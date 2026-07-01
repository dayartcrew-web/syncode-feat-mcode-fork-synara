//! Kilo adapter — real `kilo serve` provider (HTTP + SSE).
//!
//! Kilo speaks the same OpenCode-compatible local-server protocol as
//! [`crate::adapters::opencode`]: `kilo serve` exposes a REST + SSE API identical
//! to `opencode serve`. This adapter is therefore a thin re-skin of
//! [`OpenCodeAdapter`](crate::adapters::opencode::OpenCodeAdapter), differing
//! only in its [`OpenCodeCompatibleCliSpec`] (`KILO_CLI_SPEC`: binary `kilo`,
//! ready-line prefix `kilo server listening`, default agent `code`). All
//! transport + SSE decoding is shared via [`crate::opencode_server`].
//!
//! Lifecycle mapping is identical to the OpenCode adapter — see
//! [`crate::adapters::opencode`] for the full table.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, mpsc};

use super::super::trait_def::*;
use crate::opencode_server::{
    KILO_CLI_SPEC, ModelRef, OpenCodeCompatibleCliSpec, OpenCodeServerClient, TurnStatus,
};

/// Startup-wait timeout for the local `kilo serve` server (mcode uses 20s).
const SERVER_TIMEOUT_MS: u64 = 20_000;

/// Kilo-specific configuration.
#[derive(Debug, Clone)]
pub struct KiloConfig {
    /// Path to the `kilo` CLI binary (default `"kilo"`).
    pub bin_path: String,
    /// Extra args appended after `serve --hostname 127.0.0.1 --port <p>`
    /// (default empty).
    pub extra_args: Vec<String>,
    /// Full-auto mode: create the session with a blanket `*/* → allow`
    /// permission rule (and auto-approve any `permission.asked` mid-turn).
    pub full_auto: bool,
    /// Override the default agent id (`KILO_CLI_SPEC.default_agent` = `code`).
    pub agent: Option<String>,
    /// Default model (`<providerID>/<modelID>`). Empty → server default.
    pub model: String,
}

impl Default for KiloConfig {
    fn default() -> Self {
        Self {
            bin_path: "kilo".to_string(),
            extra_args: Vec::new(),
            full_auto: true,
            agent: None,
            model: String::new(),
        }
    }
}

/// The Kilo provider adapter.
pub struct KiloAdapter {
    config: Option<ProviderConfig>,
    kilo_config: KiloConfig,
    spec: &'static OpenCodeCompatibleCliSpec,
    client: Mutex<Option<OpenCodeServerClient>>,
    status: AtomicU64,
    spawned: AtomicBool,
    /// Server-assigned session id of the most recently opened session (our id).
    current_session: Mutex<Option<String>>,
    /// System prompt recorded at `start_session`, replayed on each turn.
    system_prompt: Mutex<Option<String>>,
    event_tx: broadcast::Sender<ProviderEvent>,
}

impl Default for KiloAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl KiloAdapter {
    /// Create a new Kilo adapter with default settings.
    pub fn new() -> Self {
        Self::with_kilo_config(KiloConfig::default())
    }

    /// Create a new Kilo adapter with custom kilo-specific config.
    pub fn with_kilo_config(kilo_config: KiloConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            kilo_config,
            spec: &KILO_CLI_SPEC,
            client: Mutex::new(None),
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            spawned: AtomicBool::new(false),
            current_session: Mutex::new(None),
            system_prompt: Mutex::new(None),
            event_tx,
        }
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
    }

    /// Resolve the agent id: an explicit `KiloConfig.agent` wins, else the spec
    /// default (`code`).
    fn agent(&self) -> &str {
        self.kilo_config
            .agent
            .as_deref()
            .unwrap_or(self.spec.default_agent)
    }

    /// Resolve the model for a turn: an explicit `params.model` wins, else the
    /// spawn-time `ProviderConfig.model`, else `KiloConfig.model`. Returns
    /// `None` when empty (the server picks its configured default).
    fn model_ref_for(&self, request: &ProviderRequest) -> Option<ModelRef> {
        let m = request
            .params
            .as_ref()
            .and_then(|p| p.get("model"))
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .filter(|m| !m.is_empty())
            .or_else(|| {
                self.config
                    .as_ref()
                    .map(|c| c.model.clone())
                    .filter(|m| !m.is_empty())
            })
            .or_else(|| {
                (!self.kilo_config.model.is_empty()).then(|| self.kilo_config.model.clone())
            })?;
        model_ref(&m)
    }

    /// Model resolved at `start_session` (no per-request override there).
    fn spawn_model_ref(&self) -> Option<ModelRef> {
        let m = self
            .config
            .as_ref()
            .map(|c| c.model.clone())
            .filter(|m| !m.is_empty())
            .or_else(|| {
                (!self.kilo_config.model.is_empty()).then(|| self.kilo_config.model.clone())
            })?;
        model_ref(&m)
    }

    /// Resolve the session id for a request. An explicit `params.session_id`
    /// wins; otherwise the session opened by the last `start_session`.
    async fn resolve_session(&self, params: &Option<Value>) -> Option<String> {
        if let Some(id) = params
            .as_ref()
            .and_then(|p| p.get("session_id").and_then(|v| v.as_str()))
        {
            return Some(id.to_string());
        }
        self.current_session.lock().await.clone()
    }

    /// Build the OpenCode-compatible `parts` array for a turn from a request's
    /// params. Prefers `params.input`; falls back to a textual rendering of the
    /// params object. Always a single `text` part.
    fn turn_input(params: &Option<Value>) -> Vec<Value> {
        let text = params
            .as_ref()
            .and_then(|p| match p {
                Value::Null => None,
                other => p
                    .get("input")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .or_else(|| Some(other.to_string())),
            })
            .unwrap_or_default();
        vec![json!({ "type": "text", "text": text })]
    }
}

/// Parse a Kilo/OpenCode model string into a `{providerID, id}` [`ModelRef`].
/// Accepts `<providerID>/<modelID>`; anything else → `None`.
fn model_ref(model: &str) -> Option<ModelRef> {
    let model = model.trim();
    if model.is_empty() {
        return None;
    }
    let (provider_id, id) = model.split_once('/')?;
    let (provider_id, id) = (provider_id.trim(), id.trim());
    if provider_id.is_empty() || id.is_empty() {
        return None;
    }
    Some(ModelRef {
        provider_id: provider_id.to_string(),
        id: id.to_string(),
    })
}

#[async_trait::async_trait]
impl ProviderAdapter for KiloAdapter {
    // -- Identity ----------------------------------------------------------

    fn provider_id(&self) -> &str {
        PROVIDER_KILO
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        vec![
            ProviderCapability::Streaming,
            ProviderCapability::ToolUse,
            ProviderCapability::FileSystem,
            ProviderCapability::SystemPrompt,
            ProviderCapability::CodeExecution,
        ]
    }

    fn status(&self) -> ProviderStatus {
        self.status.load(Ordering::Acquire).into()
    }

    fn available_models(&self) -> Vec<String> {
        // Kilo models are `<providerID>/<modelID>`; the live set depends on the
        // server's configured providers. A representative static list keeps the
        // trait contract populated for registry/aggregation consumers.
        vec![
            "anthropic/claude-sonnet-4-5".to_string(),
            "anthropic/claude-opus-4-1".to_string(),
            "openai/gpt-5".to_string(),
            "google/gemini-2.5-pro".to_string(),
        ]
    }

    // -- Lifecycle ---------------------------------------------------------

    async fn spawn(&mut self, config: ProviderConfig) -> Result<(), ProviderAdapterError> {
        if self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::ConfigError(
                "Kilo adapter already spawned".to_string(),
            ));
        }

        let cwd = config
            .extra
            .get("cwd")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                std::env::current_dir()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| ".".to_string())
            });
        let client = OpenCodeServerClient::spawn_with(
            self.spec,
            &self.kilo_config.bin_path,
            &self.kilo_config.extra_args,
            &cwd,
            None,
            SERVER_TIMEOUT_MS,
        )
        .await?;

        *self.client.lock().await = Some(client);
        self.config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        });
        tracing::info!(
            provider = PROVIDER_KILO,
            binary = %self.kilo_config.bin_path,
            full_auto = self.kilo_config.full_auto,
            agent = self.agent(),
            "Kilo adapter spawned + server ready",
        );
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        self.set_status(ProviderStatus::ShuttingDown);

        if let Some(client) = self.client.lock().await.take() {
            let _ = client.shutdown().await;
        }
        *self.current_session.lock().await = None;
        *self.system_prompt.lock().await = None;
        self.spawned.store(false, Ordering::Release);
        self.set_status(ProviderStatus::Disconnected);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Disconnected,
        });
        tracing::info!(provider = PROVIDER_KILO, "Kilo adapter shut down");
        Ok(())
    }

    async fn interrupt(&self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let guard = self.client.lock().await;
        let Some(client) = guard.as_ref() else {
            return Err(ProviderAdapterError::NotSpawned);
        };
        client.abort(session_id).await
    }

    // -- Session management -------------------------------------------------

    async fn start_session(&mut self, ctx: SessionContext) -> Result<String, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        let model = self.spawn_model_ref();
        let agent = self.agent().to_owned();

        let guard = self.client.lock().await;
        let Some(client) = guard.as_ref() else {
            return Err(ProviderAdapterError::NotSpawned);
        };
        let session_id = client
            .create_session(
                "syncode",
                model.as_ref(),
                Some(&agent),
                self.kilo_config.full_auto,
            )
            .await?;
        drop(guard);

        *self.current_session.lock().await = Some(session_id.clone());
        *self.system_prompt.lock().await = ctx.system_prompt.clone();
        self.set_status(ProviderStatus::Busy);
        let _ = self.event_tx.send(ProviderEvent::Started {
            session_id: session_id.clone(),
        });
        tracing::info!(
            provider = PROVIDER_KILO,
            kilo_session_id = %session_id,
            syncode_thread_id = %ctx.thread_id.as_str(),
            turn_id = %ctx.turn_id.as_str(),
            "Kilo session opened",
        );
        Ok(session_id)
    }

    async fn resume_session(&mut self, _session_id: &str) -> Result<(), ProviderAdapterError> {
        // Kilo sessions are stateful server-side: another `prompt_async` on the
        // same session id resumes it. No client-side resume RPC exists.
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        Ok(())
    }

    async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let mut cur = self.current_session.lock().await;
        if cur.as_deref() == Some(session_id) {
            *cur = None;
            *self.system_prompt.lock().await = None;
            self.set_status(ProviderStatus::Idle);
            let _ = self.event_tx.send(ProviderEvent::StatusChanged {
                status: ProviderStatus::Idle,
            });
            tracing::info!(
                provider = PROVIDER_KILO,
                session_id = session_id,
                "Kilo session stopped",
            );
            Ok(())
        } else {
            Err(ProviderAdapterError::SessionNotFound(
                session_id.to_string(),
            ))
        }
    }

    // -- Communication -----------------------------------------------------

    async fn send_request(
        &self,
        request: ProviderRequest,
    ) -> Result<ProviderResponse, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        let session_id = self.resolve_session(&request.params).await.ok_or_else(|| {
            ProviderAdapterError::SessionNotFound(
                "send_request has no session_id — call start_session first".to_string(),
            )
        })?;
        let parts = Self::turn_input(&request.params);
        let model = self.model_ref_for(&request);
        let agent = self.agent().to_owned();
        let system = self.system_prompt.lock().await.clone();

        // Bridge the turn's mpsc events onto the shared broadcast bus.
        let (fwd_tx, mut fwd_rx) = mpsc::channel::<ProviderEvent>(64);
        let bus = self.event_tx.clone();
        let forwarder = tokio::spawn(async move {
            while let Some(event) = fwd_rx.recv().await {
                let _ = bus.send(event);
            }
        });

        self.set_status(ProviderStatus::Busy);
        let turn_result = {
            let guard = self.client.lock().await;
            let Some(client) = guard.as_ref() else {
                return Err(ProviderAdapterError::NotSpawned);
            };
            client
                .start_turn(
                    &session_id,
                    parts,
                    model.as_ref(),
                    Some(&agent),
                    None,
                    system.as_deref(),
                    &fwd_tx,
                )
                .await
        };
        drop(fwd_tx); // close → forwarder drains remaining events and exits
        let _ = forwarder.await;
        let turn = turn_result?;

        match turn.status {
            TurnStatus::Completed | TurnStatus::Cancelled => {
                let _ = self.event_tx.send(ProviderEvent::Completed {
                    session_id: session_id.clone(),
                    output: turn.output.clone(),
                    usage: turn.usage.clone(),
                });
                self.set_status(ProviderStatus::Idle);
                Ok(ProviderResponse {
                    jsonrpc: "2.0".to_string(),
                    id: Some(request.id),
                    result: Some(json!({ "output": turn.output, "usage": turn.usage })),
                    error: None,
                })
            }
            TurnStatus::Failed => {
                // The Error event was already forwarded by the SSE decoder.
                let message = turn
                    .raw
                    .get("properties")
                    .and_then(|p| p.get("error"))
                    .and_then(|e| e.get("message"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("kilo turn failed")
                    .to_string();
                self.set_status(ProviderStatus::Idle);
                Ok(ProviderResponse {
                    jsonrpc: "2.0".to_string(),
                    id: Some(request.id),
                    result: None,
                    error: Some(ProviderError {
                        code: -32000,
                        message,
                        data: Some(turn.raw.clone()),
                    }),
                })
            }
        }
    }

    fn event_stream(&self, session_id: &str) -> Result<ProviderStream, ProviderAdapterError> {
        let rx = self.event_tx.subscribe();
        let sid = session_id.to_string();
        let stream = async_stream::stream! {
            let mut rx = rx;
            while let Ok(event) = rx.recv().await {
                let owned = match &event {
                    ProviderEvent::Started { session_id }
                    | ProviderEvent::Token { session_id, .. }
                    | ProviderEvent::ToolCall { session_id, .. }
                    | ProviderEvent::ToolResult { session_id, .. }
                    | ProviderEvent::Completed { session_id, .. }
                    | ProviderEvent::Error { session_id, .. } => session_id == &sid,
                    ProviderEvent::StatusChanged { .. } => true,
                };
                if owned {
                    yield Ok(event);
                }
            }
        };
        Ok(Box::pin(stream))
    }

    // -- Utility -----------------------------------------------------------

    async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Ok(false);
        }
        let guard = self.client.lock().await;
        let Some(client) = guard.as_ref() else {
            return Ok(false);
        };
        Ok(client.is_alive().await)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use syncode_core::EntityId;

    /// An adapter that is "spawned" but has no live client (so it exercises the
    /// trait guards without launching a real `kilo` binary).
    fn harness() -> KiloAdapter {
        let (event_tx, _) = broadcast::channel(256);
        KiloAdapter {
            config: None,
            kilo_config: KiloConfig::default(),
            spec: &KILO_CLI_SPEC,
            client: Mutex::new(None),
            status: AtomicU64::new(ProviderStatus::Idle.into()),
            spawned: AtomicBool::new(true),
            current_session: Mutex::new(None),
            system_prompt: Mutex::new(None),
            event_tx,
        }
    }

    fn make_ctx() -> SessionContext {
        SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp/proj".to_string(),
            system_prompt: Some("Be helpful.".to_string()),
            user_input: "fix the bug".to_string(),
            context_files: vec![],
        }
    }

    #[tokio::test]
    async fn adapter_not_spawned_initially() {
        let adapter = KiloAdapter::new();
        assert_eq!(adapter.provider_id(), PROVIDER_KILO);
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(!adapter.spawned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn double_spawn_is_rejected_before_subprocess_launch() {
        let mut provider = harness();
        let err = provider.spawn(ProviderConfig::default()).await.unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::ConfigError(ref m) if m.contains("already spawned")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn operations_before_spawn_error() {
        let mut adapter = KiloAdapter::new();
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(matches!(
            adapter.start_session(make_ctx()).await.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
        assert!(matches!(
            adapter.shutdown().await.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
    }

    #[tokio::test]
    async fn shutdown_not_spawned_fails() {
        let mut adapter = KiloAdapter::new();
        assert!(matches!(
            adapter.shutdown().await.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
    }

    #[tokio::test]
    async fn send_request_without_session_errors() {
        let provider = harness();
        let req = ProviderRequest::new("chat", Some(json!({ "input": "hi" })));
        let err = provider.send_request(req).await.unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::SessionNotFound(ref m) if m.contains("session_id")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn stop_session_unknown_errors() {
        let mut provider = harness();
        assert!(matches!(
            provider.stop_session("nope").await.unwrap_err(),
            ProviderAdapterError::SessionNotFound(_)
        ));
    }

    #[tokio::test]
    async fn health_check_no_client_returns_false() {
        let provider = harness();
        assert!(!provider.health_check().await.unwrap());
    }

    #[tokio::test]
    async fn capabilities_and_models() {
        let adapter = KiloAdapter::new();
        let caps = adapter.capabilities();
        assert!(caps.contains(&ProviderCapability::Streaming));
        assert!(caps.contains(&ProviderCapability::ToolUse));
        let models = adapter.available_models();
        assert!(!models.is_empty());
        assert!(models.iter().all(|m| m.contains('/')));
    }

    // --- pure helpers ---

    #[test]
    fn kilo_config_defaults() {
        let config = KiloConfig::default();
        assert_eq!(config.bin_path, "kilo");
        assert!(config.extra_args.is_empty());
        assert!(config.full_auto);
        assert!(config.agent.is_none());
        assert!(config.model.is_empty());
    }

    #[test]
    fn agent_prefers_config_over_spec_default() {
        let adapter = KiloAdapter::with_kilo_config(KiloConfig {
            agent: Some("custom-agent".to_string()),
            ..KiloConfig::default()
        });
        assert_eq!(adapter.agent(), "custom-agent");

        let default_adapter = KiloAdapter::new();
        assert_eq!(default_adapter.agent(), "code"); // KILO_CLI_SPEC.default_agent
    }

    #[test]
    fn turn_input_uses_input_field() {
        let input = KiloAdapter::turn_input(&Some(json!({ "input": "hi", "sequence": 2 })));
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["type"], "text");
        assert_eq!(input[0]["text"], "hi");
    }

    #[test]
    fn turn_input_falls_back_to_params_rendering() {
        let input = KiloAdapter::turn_input(&Some(json!({ "foo": "bar" })));
        assert!(input[0]["text"].as_str().unwrap().contains("foo"));
    }

    #[test]
    fn turn_input_empty_when_null() {
        let input = KiloAdapter::turn_input(&None);
        assert_eq!(input[0]["text"], "");
    }

    #[test]
    fn model_ref_parses_provider_slash_model() {
        let m = model_ref("anthropic/claude-sonnet-4-5").unwrap();
        assert_eq!(m.provider_id, "anthropic");
        assert_eq!(m.id, "claude-sonnet-4-5");
    }

    #[test]
    fn model_ref_rejects_unqualified_and_empty() {
        assert!(model_ref("claude-sonnet-4-5").is_none());
        assert!(model_ref("").is_none());
        assert!(model_ref("/").is_none());
        assert!(model_ref("anthropic/").is_none());
        assert!(model_ref("/sonnet").is_none());
    }

    #[test]
    fn model_resolution_prefers_request_then_config() {
        let mut adapter = KiloAdapter::with_kilo_config(KiloConfig {
            model: "openai/gpt-5".to_string(),
            ..KiloConfig::default()
        });
        adapter.config = Some(ProviderConfig {
            model: "anthropic/claude-opus-4-1".to_string(),
            ..ProviderConfig::default()
        });

        let req = ProviderRequest::new("chat", Some(json!({ "input": "x" })));
        let m = adapter.model_ref_for(&req).unwrap();
        assert_eq!(
            (m.provider_id.as_str(), m.id.as_str()),
            ("anthropic", "claude-opus-4-1")
        );

        let req = ProviderRequest::new(
            "chat",
            Some(json!({ "input": "x", "model": "google/gemini-2.5-pro" })),
        );
        let m = adapter.model_ref_for(&req).unwrap();
        assert_eq!(
            (m.provider_id.as_str(), m.id.as_str()),
            ("google", "gemini-2.5-pro")
        );

        adapter.config = None;
        let req = ProviderRequest::new("chat", Some(json!({ "input": "x" })));
        let m = adapter.model_ref_for(&req).unwrap();
        assert_eq!((m.provider_id.as_str(), m.id.as_str()), ("openai", "gpt-5"));

        let empty_adapter = KiloAdapter::new();
        let req = ProviderRequest::new("chat", Some(json!({ "input": "x" })));
        assert!(empty_adapter.model_ref_for(&req).is_none());
    }

    #[test]
    fn provider_status_roundtrip() {
        for status in [
            ProviderStatus::Idle,
            ProviderStatus::Busy,
            ProviderStatus::Disconnected,
            ProviderStatus::Error,
            ProviderStatus::ShuttingDown,
        ] {
            let n: u64 = status.into();
            let back: ProviderStatus = n.into();
            assert_eq!(status, back);
        }
    }
}

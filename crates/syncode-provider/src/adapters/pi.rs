//! Pi adapter — drives the `pi` CLI in headless RPC mode (`pi --mode rpc`).
//!
//! Pi (@earendil-works/pi-coding-agent) is a coding-agent CLI that ships a
//! first-class JSON-over-stdio RPC protocol explicitly designed for non-Node
//! embedding. Unlike the ACP/codex providers (JSON-RPC 2.0, reuse
//! [`JsonRpcTransport`](crate::subprocess::JsonRpcTransport)), pi speaks its own
//! `{"type":"<cmd>"}` / `{"type":"response"}` / event framing, so it uses a
//! dedicated [`PiClient`](crate::pi_rpc::PiClient) with a `type`-keyed reader.
//!
//! ## Lifecycle mapping (trait → pi RPC)
//!
//! | trait method | pi RPC |
//! |---|---|
//! | `spawn` | launch `pi --mode rpc` |
//! | `start_session` | mint a local session id (pi's default session is implicit) |
//! | `send_request` | `prompt` (submit, then drain events to terminal `agent_end`) |
//! | `interrupt` | `abort` |
//! | `health_check` | child liveness |
//! | `shutdown` | kill child |
//!
//! ## Event mapping
//!
//! pi `message_update` text/thinking deltas → `ProviderEvent::Token`;
//! `tool_execution_start/end` → `ToolCall`/`ToolResult`;
//! `agent_end` (stopReason ok) → `Completed`, else → `Error`.
//!
//! Auth: pi owns its credentials (`~/.pi/agent/auth.json` or `pi` run once
//! interactively); the spawned process inherits them. No auth on the stdio
//! channel — same model as codex/claude.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::{Mutex, broadcast, mpsc};

use super::super::pi_rpc::{PiClient, PromptStatus};
use super::super::subprocess::SubprocessSpec;
use super::super::trait_def::*;

/// Pi-specific configuration. Mirrors the codex-style config shape.
#[derive(Debug, Clone)]
pub struct PiConfig {
    /// Path to the `pi` CLI binary (default: `"pi"`).
    pub bin_path: String,
    /// Extra CLI args appended after `--mode rpc` (e.g. `["--provider","anthropic"]`).
    pub extra_args: Vec<String>,
    /// Optional `--provider` override (else pi's settings.json default).
    pub provider: Option<String>,
    /// Optional `--model` override (else pi's default).
    pub model: Option<String>,
}

impl Default for PiConfig {
    fn default() -> Self {
        Self {
            bin_path: "pi".to_string(),
            extra_args: Vec::new(),
            provider: None,
            model: None,
        }
    }
}

impl PiConfig {
    /// Build the subprocess spec for `pi --mode rpc`.
    fn spec(&self, cwd: &str) -> SubprocessSpec {
        let mut spec = SubprocessSpec::new(&self.bin_path)
            .args(["--mode", "rpc"])
            .cwd(cwd);
        if let Some(provider) = &self.provider {
            spec = spec.args(["--provider", provider]);
        }
        if let Some(model) = &self.model {
            spec = spec.args(["--model", model]);
        }
        if !self.extra_args.is_empty() {
            spec = spec.args(self.extra_args.clone());
        }
        spec
    }
}

/// The Pi provider adapter.
pub struct PiAdapter {
    config: Option<ProviderConfig>,
    pi_config: PiConfig,
    /// The pi RPC client (set on spawn).
    client: Mutex<Option<PiClient>>,
    status: AtomicU64,
    /// The active session id (pi has one implicit session per process).
    current_session: Mutex<Option<String>>,
    event_tx: broadcast::Sender<ProviderEvent>,
    spawned: AtomicBool,
}

impl PiAdapter {
    /// Create a new Pi adapter with default settings.
    pub fn new() -> Self {
        Self::with_pi_config(PiConfig::default())
    }

    /// Create a new Pi adapter with custom pi-specific config.
    pub fn with_pi_config(pi_config: PiConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            pi_config,
            client: Mutex::new(None),
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            current_session: Mutex::new(None),
            event_tx,
            spawned: AtomicBool::new(false),
        }
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged { status });
    }

    fn generate_session_id() -> String {
        format!("pi-{}", uuid::Uuid::new_v4().hyphenated())
    }
}

impl Default for PiAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ProviderAdapter for PiAdapter {
    // -- Identity ----------------------------------------------------------

    fn provider_id(&self) -> &str {
        PROVIDER_PI
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        vec![
            ProviderCapability::Streaming,
            ProviderCapability::ToolUse,
            ProviderCapability::SystemPrompt,
        ]
    }

    fn status(&self) -> ProviderStatus {
        self.status.load(Ordering::Acquire).into()
    }

    fn available_models(&self) -> Vec<String> {
        // pi resolves models at runtime via get_available_models; expose a
        // sensible default list. The actual model used is whatever pi's
        // settings.json / --model resolves to.
        vec!["default".to_string()]
    }

    // -- Lifecycle ---------------------------------------------------------

    async fn spawn(&mut self, config: ProviderConfig) -> Result<(), ProviderAdapterError> {
        if self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::ConfigError(
                "Pi adapter already spawned".to_string(),
            ));
        }

        let cwd = config
            .extra
            .get("cwd")
            .and_then(|v| v.as_str())
            .unwrap_or(".");
        let spec = self.pi_config.spec(cwd);

        let client = PiClient::spawn(&spec).await?;

        *self.client.lock().await = Some(client);
        self.config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);

        tracing::info!(provider = PROVIDER_PI, "Pi adapter spawned (pi --mode rpc)");
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        self.set_status(ProviderStatus::ShuttingDown);

        if let Some(client) = self.client.lock().await.take() {
            let _ = client.transport().shutdown().await;
        }
        *self.current_session.lock().await = None;

        self.spawned.store(false, Ordering::Release);
        self.set_status(ProviderStatus::Disconnected);
        tracing::info!(provider = PROVIDER_PI, "Pi adapter shut down");
        Ok(())
    }

    async fn interrupt(&self, _session_id: &str) -> Result<(), ProviderAdapterError> {
        let client = self.client.lock().await;
        let Some(client) = client.as_ref() else {
            return Err(ProviderAdapterError::NotSpawned);
        };
        client.abort().await
    }

    // -- Session management -------------------------------------------------

    async fn start_session(
        &mut self,
        _ctx: SessionContext,
    ) -> Result<String, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        let session_id = Self::generate_session_id();
        *self.current_session.lock().await = Some(session_id.clone());
        let _ = self.event_tx.send(ProviderEvent::Started {
            session_id: session_id.clone(),
        });
        self.set_status(ProviderStatus::Idle);
        tracing::info!(provider = PROVIDER_PI, session_id = %session_id, "Session started");
        Ok(session_id)
    }

    async fn resume_session(&mut self, _session_id: &str) -> Result<(), ProviderAdapterError> {
        // pi sessions are stateful server-side (persisted to ~/.pi/agent/sessions);
        // resuming is a no-op at the adapter level — the same process continues.
        Ok(())
    }

    async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let mut current = self.current_session.lock().await;
        if current.as_deref() == Some(session_id) {
            *current = None;
            self.set_status(ProviderStatus::Idle);
        }
        Ok(())
    }

    // -- Communication -----------------------------------------------------

    async fn send_request(
        &self,
        request: ProviderRequest,
    ) -> Result<ProviderResponse, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }

        // Resolve the session id (from params, else the active session).
        let current_session = self.current_session.lock().await.clone();
        let session_id = request
            .params
            .as_ref()
            .and_then(|p| p.get("session_id"))
            .and_then(|v| v.as_str())
            .map(String::from)
            .or(current_session)
            .ok_or_else(|| ProviderAdapterError::SessionNotFound("no active pi session".into()))?;

        let message = request
            .params
            .as_ref()
            .and_then(|p| p.get("input").or_else(|| p.get("message")))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // mpsc → broadcast forwarder: the prompt's live events flow onto the
        // shared bus while send_request returns a single response (same pattern
        // as codex). Drop the sender + await the forwarder so every event is
        // published before return.
        let (fwd_tx, mut fwd_rx) = mpsc::channel::<ProviderEvent>(64);
        let bus = self.event_tx.clone();
        let forwarder = tokio::spawn(async move {
            while let Some(ev) = fwd_rx.recv().await {
                let _ = bus.send(ev);
            }
        });

        // Drive the prompt to terminal agent_end.
        let result = {
            let client_guard = self.client.lock().await;
            let Some(client) = client_guard.as_ref() else {
                drop(fwd_tx);
                return Err(ProviderAdapterError::NotSpawned);
            };
            client.prompt(message, &session_id, &fwd_tx).await
        };

        drop(fwd_tx);
        let _ = forwarder.await;

        let result = result?;
        match result.status {
            PromptStatus::Completed | PromptStatus::Interrupted => Ok(ProviderResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(request.id),
                result: Some(serde_json::json!({
                    "output": result.output,
                    "session_id": session_id,
                })),
                error: None,
            }),
            PromptStatus::Failed => Ok(ProviderResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(request.id),
                result: None,
                error: Some(ProviderError {
                    code: -1,
                    message: result.output,
                    data: None,
                }),
            }),
        }
    }

    fn event_stream(&self, session_id: &str) -> Result<ProviderStream, ProviderAdapterError> {
        let rx = self.event_tx.subscribe();
        let sid = session_id.to_string();
        let stream = async_stream::stream! {
            let mut rx = rx;
            while let Ok(event) = rx.recv().await {
                match &event {
                    ProviderEvent::Started { session_id }
                    | ProviderEvent::Token { session_id, .. }
                    | ProviderEvent::ToolCall { session_id, .. }
                    | ProviderEvent::ToolResult { session_id, .. }
                    | ProviderEvent::Completed { session_id, .. }
                    | ProviderEvent::Error { session_id, .. } => {
                        if session_id == &sid {
                            yield Ok(event);
                        }
                    }
                    ProviderEvent::StatusChanged { .. } => {
                        yield Ok(event);
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }

    // -- Utility -----------------------------------------------------------

    async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
        let client = self.client.lock().await;
        match client.as_ref() {
            Some(c) => Ok(c.transport().is_alive().await),
            None => Ok(false),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use syncode_core::EntityId;

    fn make_ctx() -> SessionContext {
        SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp/test-pi-project".to_string(),
            system_prompt: Some("Be helpful.".to_string()),
            user_input: "Fix the bug in main.rs".to_string(),
            context_files: vec![],
        }
    }

    #[test]
    fn pi_config_defaults() {
        let config = PiConfig::default();
        assert_eq!(config.bin_path, "pi");
        assert!(config.extra_args.is_empty());
        assert!(config.provider.is_none());
        assert!(config.model.is_none());
    }

    #[test]
    fn pi_config_spec_builds_rpc_mode() {
        let config = PiConfig {
            bin_path: "/usr/local/bin/pi".into(),
            extra_args: vec!["--verbose".into()],
            provider: Some("anthropic".into()),
            model: Some("claude-sonnet-4".into()),
        };
        let spec = config.spec("/tmp/proj");
        assert_eq!(spec.command, "/usr/local/bin/pi");
        assert!(spec.args.contains(&"--mode".to_string()));
        assert!(spec.args.contains(&"rpc".to_string()));
        assert!(spec.args.contains(&"--provider".to_string()));
        assert!(spec.args.contains(&"anthropic".to_string()));
        assert!(spec.args.contains(&"--model".to_string()));
        assert!(spec.args.contains(&"--verbose".to_string()));
        assert_eq!(spec.cwd.as_deref(), Some(std::path::Path::new("/tmp/proj")));
    }

    #[tokio::test]
    async fn pi_adapter_new() {
        let adapter = PiAdapter::new();
        assert_eq!(adapter.provider_id(), PROVIDER_PI);
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(!adapter.spawned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn pi_adapter_shutdown_not_spawned_fails() {
        let mut adapter = PiAdapter::new();
        let result = adapter.shutdown().await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
    }

    #[tokio::test]
    async fn pi_adapter_capabilities() {
        let adapter = PiAdapter::new();
        let caps = adapter.capabilities();
        assert!(caps.contains(&ProviderCapability::Streaming));
        assert!(caps.contains(&ProviderCapability::ToolUse));
        assert!(caps.contains(&ProviderCapability::SystemPrompt));
    }

    #[tokio::test]
    async fn pi_adapter_available_models() {
        let adapter = PiAdapter::new();
        let models = adapter.available_models();
        assert!(!models.is_empty());
    }

    #[tokio::test]
    async fn pi_adapter_with_custom_config() {
        let pi_config = PiConfig {
            bin_path: "/custom/pi".into(),
            provider: Some("openai".into()),
            model: Some("gpt-4o".into()),
            extra_args: vec![],
        };
        let adapter = PiAdapter::with_pi_config(pi_config);
        assert_eq!(adapter.provider_id(), PROVIDER_PI);
    }

    #[tokio::test]
    async fn pi_adapter_health_check_unspawned() {
        let adapter = PiAdapter::new();
        assert!(!adapter.health_check().await.unwrap());
    }

    #[tokio::test]
    async fn pi_adapter_session_without_spawn_fails() {
        let mut adapter = PiAdapter::new();
        let result = adapter.start_session(make_ctx()).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
    }

    #[tokio::test]
    async fn pi_adapter_send_request_without_spawn_fails() {
        let adapter = PiAdapter::new();
        let req = ProviderRequest::new("chat", Some(serde_json::json!({"input": "hi"})));
        let result = adapter.send_request(req).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
    }
}

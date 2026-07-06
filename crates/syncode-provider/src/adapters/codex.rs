//! Codex adapter — real `codex app-server` provider.
//!
//! Wraps a [`CodexAppServerClient`] behind the [`ProviderAdapter`] trait so the
//! OpenAI Codex CLI (driven via its `app-server` JSON-RPC subprocess) plugs into
//! syncode's provider registry like any other adapter. The protocol client lives
//! in [`crate::codex_app_server`]; this module owns Codex's subprocess spec and
//! the trait wiring.
//!
//! Lifecycle mapping (Codex thread/turn model → trait):
//!
//! | trait method     | Codex operation                                       |
//! |------------------|-------------------------------------------------------|
//! | `spawn`          | launch `codex app-server` + `initialize` handshake    |
//! | `start_session`  | `thread/start` rooted at `ctx.working_dir` → thread id|
//! | `send_request`   | `turn/start` → streamed events + terminal `turn/...`  |
//! | `interrupt`      | `turn/interrupt` (notification)                       |
//! | `event_stream`   | subscribe to the broadcast event bus                  |
//! | `health_check`   | child liveness via the transport                      |
//! | `shutdown`       | kill child + tear down transport                      |
//!
//! # Streaming bridge
//!
//! The trait is request/response on the surface, but a Codex turn streams
//! `item/*` deltas *while* the `turn/start` response is pending and only ends on
//! a later `turn/completed` notification. [`CodexAdapter::send_request`]
//! therefore runs the turn under a short-lived `mpsc`→`broadcast` forwarder:
//! each `item/*`-derived [`ProviderEvent`] is pushed onto the shared broadcast
//! bus live, so any [`ProviderAdapter::event_stream`] subscriber observes tokens
//! / tool calls in real time, then a terminal [`ProviderEvent::Completed`] once
//! the turn's terminal notification arrives.
//!
//! # Approval policy
//!
//! By default `CodexConfig::full_auto` auto-approves every command/file-change
//! approval Codex requests mid-turn (matching Codex's own `approvalPolicy:
//! "never"` + `sandbox: "workspace-write"` full-auto mode). This keeps a headless
//! adapter from deadlocking on the first approval prompt. Set `full_auto = false`
//! to auto-*decline* approvals instead (the agent then surfaces the refusal and
//! proceeds within sandbox limits).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, Mutex};

use super::super::trait_def::*;
use crate::codex_app_server::{CodexAppServerClient, TurnStatus};
use crate::subprocess::SubprocessSpec;

/// Codex-specific configuration.
#[derive(Debug, Clone)]
pub struct CodexConfig {
    /// Path to the `codex` CLI binary (default `"codex"`).
    pub bin_path: String,
    /// Extra args appended after `app-server` (default empty).
    pub extra_args: Vec<String>,
    /// Full-auto mode: auto-approve command/file-change approvals mid-turn and
    /// run with `approvalPolicy: "never"` + `sandbox: "workspace-write"`. When
    /// `false`, approvals are auto-*declined* and the policy is `on-request`.
    pub full_auto: bool,
    /// Default model passed to `thread/start` when `ProviderConfig.model` is empty.
    pub model: String,
    /// Sandbox level: `read-only` / `workspace-write` / `danger-full-access`
    /// (default `workspace-write`).
    pub sandbox: String,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            bin_path: "codex".to_string(),
            extra_args: Vec::new(),
            full_auto: true,
            // Empty → let `codex` pick its own default from ~/.codex/config.toml.
            // Hardcoding a model here breaks ChatGPT-account auth (which forbids
            // the API-only `gpt-5-codex` family) and goes stale as OpenAI ships
            // new models. The CLI knows its own default best.
            model: String::new(),
            sandbox: "workspace-write".to_string(),
        }
    }
}

impl CodexConfig {
    /// The `approvalPolicy` value sent to `thread/start`.
    fn approval_policy(&self) -> &'static str {
        if self.full_auto {
            "never"
        } else {
            "on-request"
        }
    }

    /// Build the subprocess spec for `codex app-server [<extra_args>]`.
    fn spec(&self, cwd: &str) -> SubprocessSpec {
        let resolved = crate::bin_resolver::resolve_binary(&self.bin_path);
        let mut args = vec!["app-server".to_string()];
        args.extend(self.extra_args.iter().cloned());
        SubprocessSpec::new(&resolved)
            .args(args)
            .cwd(cwd)
            // Codex self-authenticates from its own config / env; inherit the
            // parent environment so it can find those credentials.
            .env("RUST_LOG", "info")
    }
}

/// The Codex provider adapter.
pub struct CodexAdapter {
    config: Option<ProviderConfig>,
    codex_config: CodexConfig,
    client: Mutex<Option<CodexAppServerClient>>,
    status: AtomicU64,
    spawned: AtomicBool,
    /// Codex thread id of the most recently opened thread (our session id).
    current_thread: Mutex<Option<String>>,
    /// Active turn id (for `turn/interrupt`); set when a turn starts.
    active_turn: Mutex<Option<String>>,
    event_tx: broadcast::Sender<ProviderEvent>,
}

impl Default for CodexAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexAdapter {
    /// Create a new Codex adapter with default settings.
    pub fn new() -> Self {
        Self::with_codex_config(CodexConfig::default())
    }

    /// Create a new Codex adapter with custom codex-specific config.
    pub fn with_codex_config(codex_config: CodexConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            codex_config,
            client: Mutex::new(None),
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            spawned: AtomicBool::new(false),
            current_thread: Mutex::new(None),
            active_turn: Mutex::new(None),
            event_tx,
        }
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
    }

    /// Resolve the model to pass to `thread/start`/`turn/start`: an explicit
    /// `extra.model` wins, else the spawn-time `ProviderConfig.model`, else the
    /// `CodexConfig.model` default.
    fn model_for(&self, request: &ProviderRequest) -> Option<String> {
        let extra_model = request
            .params
            .as_ref()
            .and_then(|p| p.get("model"))
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        let cfg_model = self
            .config
            .as_ref()
            .map(|c| c.model.clone())
            .filter(|m| !m.is_empty());
        extra_model.or(cfg_model).or_else(|| {
            let m = &self.codex_config.model;
            (!m.is_empty()).then(|| m.clone())
        })
    }

    /// Resolve the thread id (our session id) for a request. An explicit
    /// `params.session_id` (injected by the command reactor's dispatcher) wins;
    /// otherwise the thread opened by the last `start_session`.
    async fn resolve_thread(&self, params: &Option<Value>) -> Option<String> {
        if let Some(id) = params
            .as_ref()
            .and_then(|p| p.get("session_id").and_then(|v| v.as_str()))
        {
            return Some(id.to_string());
        }
        self.current_thread.lock().await.clone()
    }

    /// Build the Codex `input` array for a turn from a request's params.
    ///
    /// Prefers `params.input` (the orchestrator's `StartTurn` convention); falls
    /// back to a textual rendering of the params object so a turn is never
    /// silently empty. The result is always a single `text` content block.
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

#[async_trait::async_trait]
impl ProviderAdapter for CodexAdapter {
    // -- Identity ----------------------------------------------------------

    fn provider_id(&self) -> &str {
        PROVIDER_CODEX
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
        vec![
            "gpt-5.1".to_string(),
            "gpt-5.1-codex".to_string(),
            "gpt-5.1-mini".to_string(),
            "o4-mini".to_string(),
            "o3".to_string(),
            "gpt-4.1".to_string(),
            "gpt-4.1-mini".to_string(),
            "gpt-4.1-nano".to_string(),
        ]
    }

    // -- Lifecycle ---------------------------------------------------------

    async fn spawn(&mut self, config: ProviderConfig) -> Result<(), ProviderAdapterError> {
        if self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::ConfigError(
                "Codex adapter already spawned".to_string(),
            ));
        }

        // Launch `codex app-server` rooted at the config's working dir (or cwd).
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
        let spec = self.codex_config.spec(&cwd);
        let mut client = CodexAppServerClient::spawn(&spec).await?;
        client
            .initialize(&self.codex_config.bin_path, env!("CARGO_PKG_VERSION"))
            .await?;

        *self.client.lock().await = Some(client);
        self.config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        });
        tracing::info!(
            provider = PROVIDER_CODEX,
            binary = %self.codex_config.bin_path,
            full_auto = self.codex_config.full_auto,
            sandbox = %self.codex_config.sandbox,
            "Codex adapter spawned + initialize handshake complete",
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
        *self.current_thread.lock().await = None;
        *self.active_turn.lock().await = None;
        self.spawned.store(false, Ordering::Release);
        self.set_status(ProviderStatus::Disconnected);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Disconnected,
        });
        tracing::info!(provider = PROVIDER_CODEX, "Codex adapter shut down");
        Ok(())
    }

    async fn interrupt(&self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let guard = self.client.lock().await;
        let Some(client) = guard.as_ref() else {
            return Err(ProviderAdapterError::NotSpawned);
        };
        let turn_id = self.active_turn.lock().await.clone();
        client.interrupt(session_id, turn_id.as_deref()).await
    }

    // -- Session management -------------------------------------------------

    async fn start_session(&mut self, ctx: SessionContext) -> Result<String, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        let mut guard = self.client.lock().await;
        let Some(client) = guard.as_mut() else {
            return Err(ProviderAdapterError::NotSpawned);
        };
        let model = self
            .config
            .as_ref()
            .map(|c| c.model.clone())
            .filter(|m| !m.is_empty())
            .or_else(|| {
                (!self.codex_config.model.is_empty()).then(|| self.codex_config.model.clone())
            });
        let thread_id = client
            .start_thread(
                model.as_deref(),
                &ctx.working_dir,
                self.codex_config.approval_policy(),
                &self.codex_config.sandbox,
            )
            .await?;
        drop(guard);

        *self.current_thread.lock().await = Some(thread_id.clone());
        self.set_status(ProviderStatus::Busy);
        let _ = self.event_tx.send(ProviderEvent::Started {
            session_id: thread_id.clone(),
        });
        tracing::info!(
            provider = PROVIDER_CODEX,
            codex_thread_id = %thread_id,
            syncode_thread_id = %ctx.thread_id.as_str(),
            turn_id = %ctx.turn_id.as_str(),
            "Codex thread opened",
        );
        Ok(thread_id)
    }

    async fn resume_session(&mut self, _session_id: &str) -> Result<(), ProviderAdapterError> {
        // Codex threads are stateful server-side: sending another `turn/start`
        // on the same thread id resumes it. No client-side resume RPC exists.
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        Ok(())
    }

    async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let mut cur = self.current_thread.lock().await;
        if cur.as_deref() == Some(session_id) {
            *cur = None;
            *self.active_turn.lock().await = None;
            self.set_status(ProviderStatus::Idle);
            let _ = self.event_tx.send(ProviderEvent::StatusChanged {
                status: ProviderStatus::Idle,
            });
            tracing::info!(
                provider = PROVIDER_CODEX,
                thread_id = session_id,
                "Codex thread stopped",
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
        let thread_id = self.resolve_thread(&request.params).await.ok_or_else(|| {
            ProviderAdapterError::SessionNotFound(
                "send_request has no session_id — call start_session first".to_string(),
            )
        })?;
        let input = Self::turn_input(&request.params);
        let model = self.model_for(&request);
        let auto_approve = self.codex_config.full_auto;

        // Bridge the turn's mpsc events onto the shared broadcast bus so
        // event_stream subscribers observe streaming output live. The forwarder
        // is awaited before we return, guaranteeing every streamed event is
        // published (and buffered by the broadcast channel) before completion.
        let (fwd_tx, mut fwd_rx) = mpsc::channel::<ProviderEvent>(64);
        let bus = self.event_tx.clone();
        let forwarder = tokio::spawn(async move {
            while let Some(event) = fwd_rx.recv().await {
                let _ = bus.send(event);
            }
        });

        self.set_status(ProviderStatus::Busy);
        *self.active_turn.lock().await = None;
        let turn_result = {
            let mut guard = self.client.lock().await;
            let Some(client) = guard.as_mut() else {
                return Err(ProviderAdapterError::NotSpawned);
            };
            client
                .start_turn(&thread_id, input, model.as_deref(), auto_approve, &fwd_tx)
                .await
        };
        drop(fwd_tx); // close → forwarder drains remaining events and exits
        let _ = forwarder.await;
        let turn = turn_result?;
        *self.active_turn.lock().await = turn.turn_id.clone();

        // Terminal completion for this session, carrying the raw turn payload.
        let _ = self.event_tx.send(ProviderEvent::Completed {
            session_id: thread_id.clone(),
            output: turn.raw.to_string(),
            usage: turn.usage.clone(),
        });
        self.set_status(ProviderStatus::Idle);

        // Surface a failed turn as a JSON-RPC error so callers can distinguish
        // it from a clean completion.
        let error = if turn.status == TurnStatus::Failed {
            let message = turn
                .raw
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|v| v.as_str())
                .unwrap_or("codex turn failed")
                .to_string();
            let _ = self.event_tx.send(ProviderEvent::Error {
                session_id: thread_id.clone(),
                message: message.clone(),
                code: None,
            });
            Some(ProviderError {
                code: -32000,
                message,
                data: Some(turn.raw.clone()),
            })
        } else {
            None
        };

        Ok(ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(request.id),
            result: if error.is_some() {
                None
            } else {
                Some(turn.raw)
            },
            error,
        })
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
        Ok(client.transport().is_alive().await)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codex_app_server::CodexAppServerClient;
    use crate::subprocess::JsonRpcTransport;
    use syncode_core::EntityId;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio_stream::StreamExt;

    /// Wire a [`CodexAdapter`] (already "spawned") to an in-process fake *server*
    /// over two duplexes, mirroring `acp_provider::tests::provider_harness`.
    fn codex_harness() -> (
        CodexAdapter,
        tokio::io::DuplexStream, // server reads our requests
        tokio::io::DuplexStream, // server writes responses/notifications
    ) {
        let (client_writer, server_reader) = tokio::io::duplex(8192);
        let (server_writer, client_reader) = tokio::io::duplex(8192);
        let (transport, incoming) =
            JsonRpcTransport::from_streams(Box::new(client_writer), Box::new(client_reader));
        let client = CodexAppServerClient::new(transport, incoming);
        let (event_tx, _) = broadcast::channel(256);
        let provider = CodexAdapter {
            config: None,
            codex_config: CodexConfig::default(),
            client: Mutex::new(Some(client)),
            status: AtomicU64::new(ProviderStatus::Idle.into()),
            spawned: AtomicBool::new(true),
            current_thread: Mutex::new(None),
            active_turn: Mutex::new(None),
            event_tx,
        };
        (provider, server_reader, server_writer)
    }

    async fn peer_read(reader: &mut BufReader<tokio::io::DuplexStream>) -> serde_json::Value {
        let mut line = String::new();
        assert!(reader.read_line(&mut line).await.unwrap() > 0, "server EOF");
        serde_json::from_str(line.trim()).unwrap()
    }

    async fn peer_write(writer: &mut tokio::io::DuplexStream, value: &serde_json::Value) {
        writer
            .write_all(format!("{}\n", value).as_bytes())
            .await
            .unwrap();
        writer.flush().await.unwrap();
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

    /// A fake server that answers `thread/start` with a fixed thread id.
    async fn fake_thread_start(
        reader: &mut BufReader<tokio::io::DuplexStream>,
        writer: &mut tokio::io::DuplexStream,
        thread_id: &str,
    ) {
        let req = peer_read(reader).await;
        assert_eq!(req["method"], "thread/start");
        peer_write(
            writer,
            &json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "thread": { "id": thread_id } } }),
        )
        .await;
    }

    #[tokio::test]
    async fn adapter_not_spawned_initially() {
        let adapter = CodexAdapter::new();
        assert_eq!(adapter.provider_id(), PROVIDER_CODEX);
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(!adapter.spawned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn double_spawn_is_rejected_before_subprocess_launch() {
        // A harness-built adapter is already spawned; calling spawn() must hit the
        // guard and error WITHOUT attempting to launch a real `codex` binary.
        let (mut provider, _r, _w) = codex_harness();
        let err = provider.spawn(ProviderConfig::default()).await.unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::ConfigError(ref m) if m.contains("already spawned")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn operations_before_spawn_error() {
        let mut adapter = CodexAdapter::new();
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
        let mut adapter = CodexAdapter::new();
        assert!(matches!(
            adapter.shutdown().await.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
    }

    #[tokio::test]
    async fn send_request_without_session_errors() {
        let (provider, _r, _w) = codex_harness();
        let req = ProviderRequest::new("chat", Some(json!({ "input": "hi" })));
        let err = provider.send_request(req).await.unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::SessionNotFound(ref m) if m.contains("session_id")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn start_session_opens_thread_and_remembers_it() {
        let (mut provider, server_reader, server_writer) = codex_harness();

        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            let mut writer = server_writer;
            fake_thread_start(&mut reader, &mut writer, "codex-thr-1").await;
        });

        let session_id = provider
            .start_session(make_ctx())
            .await
            .expect("start_session");
        assert_eq!(session_id, "codex-thr-1");
        assert_eq!(provider.status(), ProviderStatus::Busy);
        assert_eq!(
            provider.current_thread.lock().await.as_deref(),
            Some("codex-thr-1")
        );

        server.await.unwrap();
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_request_streams_tokens_and_completes() {
        let (mut provider, server_reader, server_writer) = codex_harness();

        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            let mut writer = server_writer;
            fake_thread_start(&mut reader, &mut writer, "codex-thr-2").await;

            let req = peer_read(&mut reader).await;
            assert_eq!(req["method"], "turn/start");
            assert_eq!(req["params"]["threadId"], "codex-thr-2");
            assert_eq!(req["params"]["input"][0]["text"], "hello");
            let id = req["id"].clone();

            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "method": "item/agentMessage/delta",
                    "params": { "delta": "Hi back" } }),
            )
            .await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": id, "result": { "turn": { "id": "turn-2" } } }),
            )
            .await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "method": "turn/completed",
                    "params": { "turn": { "status": "completed", "stopReason": "end_turn" } } }),
            )
            .await;
        });

        provider.start_session(make_ctx()).await.unwrap();

        // Subscribe AFTER start_session (so Started isn't captured) but BEFORE
        // send_request, so the streamed Token + Completed are buffered for us.
        let stream = provider.event_stream("codex-thr-2").expect("event_stream");
        tokio::pin!(stream);

        let req = ProviderRequest::new(
            "chat",
            Some(json!({ "input": "hello", "session_id": "codex-thr-2" })),
        );
        let resp = provider.send_request(req).await.expect("send_request");
        assert!(resp.id.is_some());
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap()["stopReason"], "end_turn");
        assert_eq!(provider.status(), ProviderStatus::Idle);

        // Collect streamed events: expect Token("Hi back") then Completed.
        let mut events = Vec::new();
        while let Ok(Some(Ok(ev))) =
            tokio::time::timeout(std::time::Duration::from_millis(500), stream.next()).await
        {
            events.push(ev);
            if events.len() >= 2 {
                break;
            }
        }
        assert!(events.len() >= 2, "expected >=2 events, got {events:?}");
        assert!(
            matches!(&events[0], ProviderEvent::Token { content, .. } if content == "Hi back"),
            "{events:?}"
        );
        assert!(
            matches!(&events[1], ProviderEvent::Completed { .. }),
            "{events:?}"
        );

        server.await.unwrap();
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_turn_surfaces_rpc_error_and_error_event() {
        let (mut provider, server_reader, server_writer) = codex_harness();

        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            let mut writer = server_writer;
            fake_thread_start(&mut reader, &mut writer, "codex-thr-3").await;
            let req = peer_read(&mut reader).await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "turn": { "id": "t" } } }),
            )
            .await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "method": "turn/completed",
                    "params": { "turn": { "status": "failed", "error": { "message": "kaboom" } } } }),
            )
            .await;
        });

        provider.start_session(make_ctx()).await.unwrap();
        let stream = provider.event_stream("codex-thr-3").unwrap();
        tokio::pin!(stream);

        let req = ProviderRequest::new(
            "chat",
            Some(json!({ "input": "x", "session_id": "codex-thr-3" })),
        );
        let resp = provider.send_request(req).await.expect("send_request");
        assert!(resp.result.is_none());
        let err = resp.error.expect("error");
        assert_eq!(err.code, -32000);
        assert_eq!(err.message, "kaboom");

        // An Error event must also reach the bus.
        let mut saw_error = false;
        while let Ok(Some(Ok(ev))) =
            tokio::time::timeout(std::time::Duration::from_millis(500), stream.next()).await
        {
            if matches!(ev, ProviderEvent::Error { .. }) {
                saw_error = true;
                break;
            }
        }
        assert!(saw_error, "expected an Error event for the failed turn");

        server.await.unwrap();
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn interrupt_sends_turn_interrupt_notification() {
        let (mut provider, server_reader, _server_writer) = codex_harness();
        provider
            .active_turn
            .lock()
            .await
            .replace("turn-9".to_string());

        let handle = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            peer_read(&mut reader).await
        });

        provider.interrupt("codex-thr-9").await.expect("interrupt");
        let note = handle.await.unwrap();
        assert_eq!(note["method"], "turn/interrupt");
        assert_eq!(note["params"]["threadId"], "codex-thr-9");
        assert_eq!(note["params"]["turnId"], "turn-9");
        assert!(note.get("id").is_none());
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stop_session_unknown_errors() {
        let (mut provider, _r, _w) = codex_harness();
        assert!(matches!(
            provider.stop_session("nope").await.unwrap_err(),
            ProviderAdapterError::SessionNotFound(_)
        ));
    }

    #[tokio::test]
    async fn health_check_no_child_returns_false() {
        // Harness built via from_streams → no child → is_alive false.
        let (mut provider, _r, _w) = codex_harness();
        assert!(!provider.health_check().await.unwrap());
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn capabilities_and_models() {
        let adapter = CodexAdapter::new();
        let caps = adapter.capabilities();
        assert!(caps.contains(&ProviderCapability::Streaming));
        assert!(caps.contains(&ProviderCapability::ToolUse));
        let models = adapter.available_models();
        assert!(!models.is_empty());
    }

    // --- pure helpers ---

    #[test]
    fn codex_config_defaults() {
        let config = CodexConfig::default();
        assert_eq!(config.bin_path, "codex");
        assert!(config.full_auto);
        assert_eq!(config.sandbox, "workspace-write");
        assert_eq!(config.approval_policy(), "never");
    }

    #[test]
    fn codex_config_not_full_auto_uses_on_request() {
        let config = CodexConfig {
            full_auto: false,
            ..CodexConfig::default()
        };
        assert_eq!(config.approval_policy(), "on-request");
    }

    #[test]
    fn codex_config_spec_appends_app_server_and_cwd() {
        let config = CodexConfig {
            bin_path: "/usr/local/bin/codex".to_string(),
            extra_args: vec!["--foo".to_string()],
            ..CodexConfig::default()
        };
        let spec = config.spec("/tmp/work");
        assert_eq!(spec.command, "/usr/local/bin/codex");
        assert_eq!(
            spec.args,
            vec!["app-server".to_string(), "--foo".to_string()]
        );
        assert_eq!(spec.cwd.as_deref(), Some(std::path::Path::new("/tmp/work")));
    }

    #[test]
    fn turn_input_uses_input_field() {
        let input = CodexAdapter::turn_input(&Some(json!({ "input": "hi", "sequence": 2 })));
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["type"], "text");
        assert_eq!(input[0]["text"], "hi");
    }

    #[test]
    fn turn_input_falls_back_to_params_rendering() {
        let input = CodexAdapter::turn_input(&Some(json!({ "foo": "bar" })));
        assert!(input[0]["text"].as_str().unwrap().contains("foo"));
    }

    #[test]
    fn turn_input_empty_when_null() {
        let input = CodexAdapter::turn_input(&None);
        assert_eq!(input[0]["text"], "");
    }

    #[test]
    fn model_resolution_prefers_request_then_config() {
        let mut adapter = CodexAdapter::with_codex_config(CodexConfig {
            model: "default-model".to_string(),
            ..CodexConfig::default()
        });
        adapter.config = Some(ProviderConfig {
            model: "cfg-model".to_string(),
            ..ProviderConfig::default()
        });

        // No request model → falls back to spawn config model.
        let req = ProviderRequest::new("chat", Some(json!({ "input": "x" })));
        assert_eq!(adapter.model_for(&req).as_deref(), Some("cfg-model"));

        // Request model wins.
        let req = ProviderRequest::new("chat", Some(json!({ "input": "x", "model": "req-model" })));
        assert_eq!(adapter.model_for(&req).as_deref(), Some("req-model"));

        // No config model → codex_config default.
        adapter.config = None;
        let req = ProviderRequest::new("chat", Some(json!({ "input": "x" })));
        assert_eq!(adapter.model_for(&req).as_deref(), Some("default-model"));
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

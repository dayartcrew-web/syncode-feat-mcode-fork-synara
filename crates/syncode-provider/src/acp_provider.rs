//! ACP-backed [`ProviderAdapter`] — wraps an [`AcpClient`] behind the trait so
//! ACP-speaking agents (cursor, grok, gemini) plug into syncode's provider
//! registry like any other adapter.
//!
//! Each provider is configured with its ACP [`SubprocessSpec`] (command +
//! flags). Lifecycle mapping:
//!
//! | trait method        | ACP operation                                  |
//! |---------------------|------------------------------------------------|
//! | `spawn`             | launch child + `initialize` handshake          |
//! | `start_session`     | `session/new` (rooted at `ctx.working_dir`)    |
//! | `send_request`      | `session/prompt` → streamed events + response  |
//! | `interrupt`         | `session/cancel` (notification)                |
//! | `event_stream`      | subscribe to the broadcast event bus           |
//! | `health_check`      | child liveness via the transport               |
//! | `shutdown`          | kill child + tear down transport               |
//!
//! # Streaming bridge
//!
//! The trait is request/response on the surface, but ACP streams
//! `session/update` notifications *while* a `session/prompt` response is
//! pending. [`AcpProvider::send_request`] therefore runs the prompt under a
//! short-lived `mpsc`→`broadcast` forwarder: each `session/update`-derived
//! [`ProviderEvent`] is pushed onto the shared broadcast bus live, so any
//! [`ProviderAdapter::event_stream`] subscriber observes tokens/tool calls in
//! real time, then a terminal [`ProviderEvent::Completed`] once the prompt
//! response arrives. The forwarder is awaited to completion before returning,
//! guaranteeing no streamed event is dropped before the caller is notified.
//!
//! # Concurrency
//!
//! [`AcpClient`]'s mutating methods (`initialize` / `new_session` / `prompt`)
//! are guarded by a `tokio::sync::Mutex<Option<AcpClient>>` because the trait
//! only grants `&self` to `send_request` / `interrupt` / `health_check`. Only
//! one ACP turn may be in flight at a time (a prompt exclusively drains the
//! notification stream), so holding the lock for the duration of a turn is
//! correct rather than a limitation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, mpsc};

use crate::acp::AcpClient;
use crate::subprocess::SubprocessSpec;
use crate::trait_def::*;

/// Identity + spawn configuration for an [`AcpProvider`].
#[derive(Debug, Clone)]
pub struct AcpProviderConfig {
    /// Provider identifier (must match a `PROVIDER_*` constant), e.g. `"cursor"`.
    pub provider_id: String,
    /// How to launch the ACP agent subprocess.
    pub spec: SubprocessSpec,
    /// Capabilities advertised by this provider.
    pub capabilities: Vec<ProviderCapability>,
    /// Models the provider accepts (informational).
    pub available_models: Vec<String>,
    /// Client name reported during the `initialize` handshake.
    pub client_name: String,
    /// Optional ACP `authenticate` method id to send after `initialize` and
    /// before `session/new` (Cursor: `cursor_login`; Grok: `xai.api_key` /
    /// `cached_token`). `None` skips the step — for providers that
    /// self-authenticate from cached credentials (Gemini with an API key).
    pub auth_method_id: Option<String>,
    /// Optional `_meta` forwarded with the `authenticate` request (Grok sends
    /// `{"headless": true}`). Ignored when `auth_method_id` is `None`.
    pub auth_meta: Option<Value>,
}

/// A [`ProviderAdapter`] backed by an ACP-speaking agent subprocess.
///
/// Construct with [`AcpProvider::new`] (production: spawned via `spawn`) or, in
/// tests, directly over a transport built with
/// [`JsonRpcTransport::from_streams`](crate::subprocess::JsonRpcTransport::from_streams).
pub struct AcpProvider {
    config: AcpProviderConfig,
    provider_config: Option<ProviderConfig>,
    client: Mutex<Option<AcpClient>>,
    status: AtomicU64,
    spawned: AtomicBool,
    /// ACP `sessionId` of the most recently opened session. Used as the
    /// correlation fallback when a request omits an explicit `session_id`.
    current_session: Mutex<Option<String>>,
    /// Workflow context preamble captured from `SessionContext.system_prompt`
    /// during `start_session`. Prepended to every `session/prompt` as a
    /// leading text ContentBlock so the ACP-speaking agent (cursor, grok,
    /// gemini) observes syncode's active workflow state — phase, current
    /// task, total tasks, TDD/coverage constraints — without depending on
    /// the agent's own CLAUDE.md / AGENTS.md conventions. `None`/empty when
    /// the orchestrator forwards no workflow state (back-compat with prior
    /// behavior).
    workflow_preamble: Mutex<Option<String>>,
    event_tx: broadcast::Sender<ProviderEvent>,
}

impl AcpProvider {
    /// Create a new (un-spawned) ACP provider.
    pub fn new(config: AcpProviderConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config,
            provider_config: None,
            client: Mutex::new(None),
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            spawned: AtomicBool::new(false),
            current_session: Mutex::new(None),
            workflow_preamble: Mutex::new(None),
            event_tx,
        }
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
    }

    /// Build the ACP `ContentBlock` array for a prompt.
    ///
    /// When a workflow preamble was captured during `start_session`, it is
    /// emitted as the **first** text block so the downstream agent reads the
    /// workflow context (phase, current task, constraints) before the user's
    /// input. The user input itself is then appended. Prefers `params.input`
    /// (the orchestrator's `StartTurn` convention); falls back to a textual
    /// rendering of the params object so a turn is never silently empty.
    async fn prompt_blocks(&self, params: &Option<Value>) -> Vec<Value> {
        let mut blocks = Vec::with_capacity(2);
        if let Some(preamble) = self.workflow_preamble.lock().await.clone()
            && !preamble.is_empty()
        {
            blocks.push(json!({ "type": "text", "text": preamble }));
        }
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
        blocks.push(json!({ "type": "text", "text": text }));
        blocks
    }

    /// Extract the `mcpServers` array to expose to the ACP agent for a session,
    /// read from the stored [`ProviderConfig::extra`] (populated from persisted
    /// settings via `extract_provider_extras`). Returns an empty vec when unset
    /// — providers then spawn with no external MCP tool servers, matching the
    /// prior hardcoded `[]` behavior (backward compatible).
    ///
    /// The value is forwarded verbatim to ACP's `session/new` `mcpServers`
    /// param. Shape per the mcode integration: an array of
    /// `{ "name", "transport", "config" }` entries. Non-array values (mis-typed
    /// settings) are ignored defensively rather than erroring, so a malformed
    /// config never blocks session creation.
    fn mcp_servers(&self) -> Vec<Value> {
        self.provider_config
            .as_ref()
            .and_then(|c| c.extra.get("mcpServers"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
    }

    /// Resolve the session id for a request. An explicit `params.session_id`
    /// (injected by the command reactor's dispatcher) wins; otherwise the
    /// session opened by the last `start_session`.
    async fn resolve_session(&self, params: &Option<Value>) -> Option<String> {
        if let Some(id) = params
            .as_ref()
            .and_then(|p| p.get("session_id").and_then(|v| v.as_str()))
        {
            return Some(id.to_string());
        }
        self.current_session.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl ProviderAdapter for AcpProvider {
    fn provider_id(&self) -> &str {
        &self.config.provider_id
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        self.config.capabilities.clone()
    }

    fn status(&self) -> ProviderStatus {
        self.status.load(Ordering::Acquire).into()
    }

    fn available_models(&self) -> Vec<String> {
        self.config.available_models.clone()
    }

    // -- Lifecycle ---------------------------------------------------------

    async fn spawn(&mut self, config: ProviderConfig) -> Result<(), ProviderAdapterError> {
        if self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::ConfigError(format!(
                "{} adapter already spawned",
                self.config.provider_id
            )));
        }

        let mut client = AcpClient::spawn(&self.config.spec).await?;
        client
            .initialize(&self.config.client_name, env!("CARGO_PKG_VERSION"))
            .await?;

        // ACP `authenticate` step — required by agents that gate sessions
        // behind an auth method (Cursor: `cursor_login`; Grok:
        // `xai.api_key`/`cached_token`). Skipped when no method is configured
        // (Gemini self-authenticates from its cached API key). Matches the
        // mcode `AcpSessionRuntime` handshake order: initialize → authenticate
        // → session/new.
        if let Some(method_id) = &self.config.auth_method_id {
            client
                .authenticate(method_id, self.config.auth_meta.as_ref())
                .await?;
            tracing::info!(
                provider = %self.config.provider_id,
                method_id = %method_id,
                "ACP authenticate handshake complete"
            );
        }

        *self.client.lock().await = Some(client);
        self.provider_config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        });
        tracing::info!(
            provider = %self.config.provider_id,
            "ACP provider spawned + initialize handshake complete"
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
        self.spawned.store(false, Ordering::Release);
        self.set_status(ProviderStatus::Disconnected);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Disconnected,
        });
        tracing::info!(provider = %self.config.provider_id, "ACP provider shut down");
        Ok(())
    }

    async fn interrupt(&self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let guard = self.client.lock().await;
        let Some(client) = guard.as_ref() else {
            return Err(ProviderAdapterError::NotSpawned);
        };
        client.cancel(session_id).await
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
        // Forward configured MCP tool servers (from ProviderConfig.extra) to the
        // agent's `session/new` so ACP providers can use external MCP servers.
        // Empty when unconfigured — backward-compatible with the prior `[]`.
        let mcp_servers = self.mcp_servers();
        let session_id = client.new_session(&ctx.working_dir, &mcp_servers).await?;
        drop(guard);

        // Capture any workflow preamble from the orchestrator. The text is
        // generated upstream (syncode-ws `workflow_preamble::build_workflow_preamble`)
        // and forwarded via `SessionContext.system_prompt`. ACP v1 has no
        // dedicated field for workflow context, so we re-emit it on every
        // turn as a leading text ContentBlock inside `prompt_blocks`. Empty
        // system_prompts collapse to "no preamble" so legacy callers behave
        // exactly as before.
        let next_preamble = ctx
            .system_prompt
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let has_preamble = next_preamble.is_some();
        *self.workflow_preamble.lock().await = next_preamble;

        *self.current_session.lock().await = Some(session_id.clone());
        self.set_status(ProviderStatus::Busy);
        let _ = self.event_tx.send(ProviderEvent::Started {
            session_id: session_id.clone(),
        });
        tracing::info!(
            provider = %self.config.provider_id,
            session_id = %session_id,
            thread_id = %ctx.thread_id.as_str(),
            turn_id = %ctx.turn_id.as_str(),
            has_workflow_preamble = has_preamble,
            "ACP session opened"
        );
        Ok(session_id)
    }

    async fn resume_session(&mut self, _session_id: &str) -> Result<(), ProviderAdapterError> {
        // ACP sessions are resumed implicitly by sending another `session/prompt`
        // on the same sessionId — there is no client-side resume RPC.
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        Ok(())
    }

    async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let mut cur = self.current_session.lock().await;
        if cur.as_deref() == Some(session_id) {
            *cur = None;
            self.set_status(ProviderStatus::Idle);
            let _ = self.event_tx.send(ProviderEvent::StatusChanged {
                status: ProviderStatus::Idle,
            });
            tracing::info!(
                provider = %self.config.provider_id,
                session_id,
                "ACP session stopped"
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
        let blocks = self.prompt_blocks(&request.params).await;

        // Bridge the prompt's mpsc events onto the shared broadcast bus so
        // event_stream subscribers observe streaming output live. The forwarder
        // is awaited before we return, guaranteeing every streamed event is
        // published (and buffered by the broadcast channel) before completion.
        let (fwd_tx, mut fwd_rx) = mpsc::channel::<ProviderEvent>(64);
        let bus = self.event_tx.clone();
        // Accumulate the streamed Token text so the terminal `Completed` event
        // can carry the real assistant output. ACP's `session/result` only
        // carries `stopReason` + `usage` (NO content) — the text arrives via
        // streamed `agent_message_chunk` Token events. Without this, the
        // Completed output falls back to the raw result JSON and the turn's
        // `assistant_output` ends up as `{"stopReason":"end_turn"}`.
        let accumulated: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let acc_for_fwd = accumulated.clone();
        let forwarder = tokio::spawn(async move {
            while let Some(event) = fwd_rx.recv().await {
                if let ProviderEvent::Token { content, .. } = &event {
                    acc_for_fwd.lock().await.push_str(content);
                }
                let _ = bus.send(event);
            }
        });

        self.set_status(ProviderStatus::Busy);
        let prompt_result = {
            let mut guard = self.client.lock().await;
            let Some(client) = guard.as_mut() else {
                return Err(ProviderAdapterError::NotSpawned);
            };
            client.prompt(&session_id, &blocks, &fwd_tx).await
        };
        drop(fwd_tx); // close → forwarder drains remaining events and exits
        let _ = forwarder.await;

        let prompt = prompt_result?;

        // Terminal completion for this session, carrying the accumulated
        // streamed assistant text (NOT the raw result JSON, which only has
        // `stopReason`/`usage`).
        let output = accumulated.lock().await.clone();
        let _ = self.event_tx.send(ProviderEvent::Completed {
            session_id: session_id.clone(),
            output,
            usage: prompt.usage.clone(),
        });
        self.set_status(ProviderStatus::Idle);

        Ok(ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(request.id),
            result: Some(prompt.raw),
            error: None,
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
    use crate::acp::PROTOCOL_VERSION;
    use crate::subprocess::JsonRpcTransport;
    use syncode_core::EntityId;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio_stream::StreamExt;

    /// Wire an [`AcpProvider`] (already "spawned") to an in-process fake *agent*
    /// over two duplexes, mirroring `acp::tests::acp_harness`.
    fn provider_harness() -> (
        AcpProvider,
        tokio::io::DuplexStream, // agent reads our requests
        tokio::io::DuplexStream, // agent writes responses/notifications
    ) {
        let (client_writer, peer_reader) = tokio::io::duplex(8192);
        let (peer_writer, client_reader) = tokio::io::duplex(8192);
        let (transport, incoming) =
            JsonRpcTransport::from_streams(Box::new(client_writer), Box::new(client_reader));
        let client = AcpClient::new(transport, incoming);
        let (event_tx, _) = broadcast::channel(256);
        let config = AcpProviderConfig {
            provider_id: "test-acp".to_string(),
            spec: SubprocessSpec::new("fake-agent"),
            capabilities: vec![ProviderCapability::Streaming, ProviderCapability::ToolUse],
            available_models: vec!["fake-model".to_string()],
            client_name: "syncode-tests".to_string(),
            auth_method_id: None,
            auth_meta: None,
        };
        let provider = AcpProvider {
            config,
            provider_config: None,
            client: Mutex::new(Some(client)),
            status: AtomicU64::new(ProviderStatus::Idle.into()),
            spawned: AtomicBool::new(true),
            current_session: Mutex::new(None),
            workflow_preamble: Mutex::new(None),
            event_tx,
        };
        (provider, peer_reader, peer_writer)
    }

    async fn peer_read(reader: &mut BufReader<tokio::io::DuplexStream>) -> serde_json::Value {
        let mut line = String::new();
        assert!(reader.read_line(&mut line).await.unwrap() > 0, "peer EOF");
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
            // No workflow preamble by default — streaming tests assert the
            // user input shows up verbatim as the first prompt block. The
            // preamble injection path is exercised by the dedicated
            // `prompt_blocks_prepends_workflow_preamble_when_set` test.
            system_prompt: None,
            user_input: "fix the bug".to_string(),
            context_files: vec![],
        }
    }

    /// A fake agent that answers `session/new` with a fixed sessionId.
    async fn fake_new_session(
        reader: &mut BufReader<tokio::io::DuplexStream>,
        writer: &mut tokio::io::DuplexStream,
        session_id: &str,
    ) {
        let req = peer_read(reader).await;
        assert_eq!(req["method"], "session/new");
        peer_write(
            writer,
            &json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "sessionId": session_id } }),
        )
        .await;
    }

    #[tokio::test]
    async fn start_session_opens_acp_session_and_remembers_it() {
        let (mut provider, peer_reader, peer_writer) = provider_harness();

        let peer = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            let mut writer = peer_writer;
            fake_new_session(&mut reader, &mut writer, "acp-sess-1").await;
        });

        let session_id = provider
            .start_session(make_ctx())
            .await
            .expect("start_session");
        assert_eq!(session_id, "acp-sess-1");
        assert_eq!(provider.status(), ProviderStatus::Busy);
        assert_eq!(
            provider.current_session.lock().await.as_deref(),
            Some("acp-sess-1")
        );

        peer.await.unwrap();
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_request_without_session_errors() {
        let (provider, _peer_reader, _peer_writer) = provider_harness();
        let req = ProviderRequest::new("chat", Some(json!({ "input": "hi" })));
        let err = provider.send_request(req).await.unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::SessionNotFound(ref m) if m.contains("session_id")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn send_request_runs_prompt_streams_events_and_completes() {
        let (provider, peer_reader, peer_writer) = provider_harness();
        let mut provider = provider;

        let peer = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            let mut writer = peer_writer;
            fake_new_session(&mut reader, &mut writer, "acp-sess-2").await;

            // session/prompt: stream one token, then complete.
            let req = peer_read(&mut reader).await;
            assert_eq!(req["method"], "session/prompt");
            assert_eq!(req["params"]["sessionId"], "acp-sess-2");
            assert_eq!(req["params"]["prompt"][0]["text"], "hello");
            let id = req["id"].clone();

            peer_write(
                &mut writer,
                &json!({
                    "jsonrpc": "2.0", "method": "session/update",
                    "params": { "sessionId": "acp-sess-2", "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": [{ "type": "text", "text": "Hi back" }]
                    }}
                }),
            )
            .await;

            peer_write(
                &mut writer,
                &json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": {
                        "stopReason": "end_turn",
                        "usage": { "inputTokens": 3, "outputTokens": 2, "totalTokens": 5 }
                    }
                }),
            )
            .await;
        });

        provider.start_session(make_ctx()).await.unwrap();

        // Subscribe AFTER start_session (so Started isn't captured) but BEFORE
        // send_request, so the streamed Token + Completed are buffered for us.
        let stream = provider.event_stream("acp-sess-2").expect("event_stream");
        tokio::pin!(stream);

        let req = ProviderRequest::new(
            "chat",
            Some(json!({ "input": "hello", "session_id": "acp-sess-2" })),
        );
        let resp = provider.send_request(req).await.expect("send_request");
        assert!(resp.id.is_some());
        assert!(resp.result.is_some());
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
            matches!(&events[1], ProviderEvent::Completed { output, usage, .. } if output == "Hi back" && usage.as_ref().map(|u| u.total_tokens) == Some(5)),
            "Completed must carry the streamed text, not the raw result JSON: {events:?}"
        );

        peer.await.unwrap();
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn interrupt_sends_session_cancel_notification() {
        let (mut provider, peer_reader, _peer_writer) = provider_harness();

        let handle = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            peer_read(&mut reader).await
        });

        provider.interrupt("acp-sess-9").await.expect("interrupt");
        let note = handle.await.unwrap();
        assert_eq!(note["method"], "session/cancel");
        assert_eq!(note["params"]["sessionId"], "acp-sess-9");
        assert!(note.get("id").is_none(), "expected a notification: {note}");
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn event_stream_filters_other_sessions() {
        let (mut provider, _peer_reader, _peer_writer) = provider_harness();

        // Subscribe first — broadcast only delivers events sent AFTER a
        // subscriber exists.
        let mut stream = provider.event_stream("mine").unwrap();

        // A Token for a *different* session is filtered out; "mine" is yielded.
        let _ = provider.event_tx.send(ProviderEvent::Token {
            session_id: "other".to_string(),
            content: "x".to_string(),
        });
        let _ = provider.event_tx.send(ProviderEvent::Token {
            session_id: "mine".to_string(),
            content: "y".to_string(),
        });

        let ev = tokio::time::timeout(std::time::Duration::from_millis(500), stream.next())
            .await
            .expect("timed out")
            .expect("stream closed")
            .expect("stream err");
        assert!(
            matches!(ev, ProviderEvent::Token { ref content, .. } if content == "y"),
            "{ev:?}"
        );
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn health_check_with_no_child_returns_false() {
        // The harness is built via from_streams → no child → is_alive is false.
        let (mut provider, _peer_reader, _peer_writer) = provider_harness();
        assert!(!provider.health_check().await.unwrap());
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn not_spawned_operations_error() {
        let config = AcpProviderConfig {
            provider_id: "x".to_string(),
            spec: SubprocessSpec::new("nope"),
            capabilities: vec![],
            available_models: vec![],
            client_name: "syncode".to_string(),
            auth_method_id: None,
            auth_meta: None,
        };
        let mut provider = AcpProvider::new(config);
        assert_eq!(provider.status(), ProviderStatus::Disconnected);
        assert!(matches!(
            provider.start_session(make_ctx()).await.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
        assert!(matches!(
            provider.shutdown().await.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
    }

    #[tokio::test]
    async fn prompt_blocks_uses_input_field() {
        let (provider, _peer_reader, _peer_writer) = provider_harness();
        let blocks = provider
            .prompt_blocks(&Some(json!({ "input": "hi", "sequence": 2 })))
            .await;
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "hi");
    }

    #[tokio::test]
    async fn prompt_blocks_falls_back_to_params_rendering() {
        // No `input` key → textual rendering of the object.
        let (provider, _peer_reader, _peer_writer) = provider_harness();
        let blocks = provider.prompt_blocks(&Some(json!({ "foo": "bar" }))).await;
        assert!(blocks[0]["text"].as_str().unwrap().contains("foo"));
    }

    #[tokio::test]
    async fn prompt_blocks_empty_when_null() {
        let (provider, _peer_reader, _peer_writer) = provider_harness();
        let blocks = provider.prompt_blocks(&None).await;
        assert_eq!(blocks[0]["text"], "");
    }

    #[tokio::test]
    async fn prompt_blocks_prepends_workflow_preamble_when_set() {
        // When start_session captures a system_prompt, every prompt_blocks
        // call emits the preamble as a leading text block, followed by the
        // user input.
        let (mut provider, peer_reader, peer_writer) = provider_harness();
        let ctx = SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp/proj".to_string(),
            system_prompt: Some("--- WORKFLOW CONTEXT ---\nPhase: EXECUTE\n--- END ---".to_string()),
            user_input: "fix the bug".to_string(),
            context_files: vec![],
        };
        let peer = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            let mut writer = peer_writer;
            fake_new_session(&mut reader, &mut writer, "wf-sess").await;
        });
        provider.start_session(ctx).await.unwrap();
        peer.await.unwrap();

        let blocks = provider
            .prompt_blocks(&Some(json!({ "input": "do step 1" })))
            .await;
        assert_eq!(blocks.len(), 2, "preamble + input: {blocks:?}");
        assert_eq!(blocks[0]["type"], "text");
        assert!(
            blocks[0]["text"].as_str().unwrap().contains("WORKFLOW CONTEXT"),
            "preamble must be first: {blocks:?}"
        );
        assert_eq!(blocks[1]["text"], "do step 1");
        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn prompt_blocks_omits_preamble_when_system_prompt_empty() {
        // An empty/whitespace system_prompt collapses to "no preamble" so
        // legacy callers observe exactly one text block per turn.
        let (mut provider, peer_reader, peer_writer) = provider_harness();
        let ctx = SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp/proj".to_string(),
            system_prompt: Some("   \n  ".to_string()),
            user_input: "fix".to_string(),
            context_files: vec![],
        };
        let peer = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            let mut writer = peer_writer;
            fake_new_session(&mut reader, &mut writer, "empty-sess").await;
        });
        provider.start_session(ctx).await.unwrap();
        peer.await.unwrap();

        let blocks = provider.prompt_blocks(&Some(json!({ "input": "x" }))).await;
        assert_eq!(blocks.len(), 1, "no preamble when system_prompt empty: {blocks:?}");
        provider.shutdown().await.unwrap();
    }

    #[test]
    fn protocol_version_wired() {
        // Sanity: the adapter depends on the same ACP version the client speaks.
        assert_eq!(PROTOCOL_VERSION, 1);
    }
}

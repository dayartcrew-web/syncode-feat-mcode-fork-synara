//! ACP (Agent Client Protocol) client.
//!
//! Built on the protocol-agnostic [`JsonRpcTransport`] from [`crate::subprocess`].
//! Speaks ACP v0.11.3 (`PROTOCOL_VERSION = 1`) over NDJSON JSON-RPC 2.0. As the
//! ACP *client*, we drive a provider subprocess that implements the ACP *agent*
//! role: we `initialize`, open a `session/new`, then send `session/prompt` turns
//! while consuming the agent's `session/update` notifications.
//!
//! # The streaming-response concurrency problem
//!
//! `session/prompt` is a JSON-RPC *request* — it awaits a `PromptResponse` keyed
//! by its `id`. But the agent streams `session/update` notifications over the
//! SAME transport WHILE that response is pending. We therefore cannot simply
//! `await send_request(...)` and then read notifications: the notifications
//! arrive *during* the await.
//!
//! [`AcpClient::prompt`] solves this by submitting the prompt without awaiting
//! ([`JsonRpcTransport::submit_request`]) and running a `tokio::select!` loop
//! that concurrently awaits the prompt response and drains the notification
//! stream, mapping each `session/update` to a [`ProviderEvent`] forwarded live
//! to the caller. The turn completes when the prompt *response* arrives (not a
//! notification).
//!
//! Reference: `mcode/packages/effect-acp/src/client.ts` (client side) and
//! `_generated/schema.gen.ts` (`SessionNotification`, `PromptRequest`,
//! `PromptResponse`, `NewSessionRequest`, `CancelNotification`).

use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::subprocess::{IncomingMessage, JsonRpcTransport, SubprocessSpec};
use crate::trait_def::{
    ProviderAdapterError, ProviderError, ProviderEvent, ProviderResponse, UsageInfo,
};

/// ACP protocol version negotiated by `initialize`. Matches ACP v0.11.3
/// (`PROTOCOL_VERSION = 1` in `meta.gen.ts`).
pub const PROTOCOL_VERSION: u64 = 1;

/// ACP method names (client→agent requests + the `session/cancel` notification).
mod methods {
    pub const INITIALIZE: &str = "initialize";
    pub const SESSION_NEW: &str = "session/new";
    pub const SESSION_PROMPT: &str = "session/prompt";
    pub const SESSION_CANCEL: &str = "session/cancel";
    /// Agent→client notification carrying streamed turn updates.
    pub const SESSION_UPDATE: &str = "session/update";
}

/// Outcome of a completed `session/prompt` turn.
#[derive(Debug, Clone)]
pub struct PromptResult {
    /// Agent-reported stop reason (`end_turn`, `max_tokens`, `refusal`,
    /// `max_turn_requests`, `cancelled`).
    pub stop_reason: Option<String>,
    /// Token usage for the turn, if the agent reported it.
    pub usage: Option<UsageInfo>,
    /// Raw `PromptResponse.result` JSON (stop reason, usage, userMessageId, ...).
    pub raw: Value,
}

/// ACP client over a [`JsonRpcTransport`]. Owns the transport and the receiver
/// for forwarded notifications / inbound requests.
///
/// Construct with [`AcpClient::new`] (wrap existing transport) or
/// [`AcpClient::spawn`] (spawn a subprocess).
pub struct AcpClient {
    transport: JsonRpcTransport,
    incoming: mpsc::Receiver<IncomingMessage>,
}

impl AcpClient {
    /// Wrap an already-constructed transport + incoming channel.
    pub fn new(transport: JsonRpcTransport, incoming: mpsc::Receiver<IncomingMessage>) -> Self {
        Self {
            transport,
            incoming,
        }
    }

    /// Spawn the subprocess described by `spec` and build a client over its
    /// piped stdio.
    pub async fn spawn(spec: &SubprocessSpec) -> Result<Self, ProviderAdapterError> {
        let (transport, incoming) = JsonRpcTransport::spawn(spec).await?;
        Ok(Self::new(transport, incoming))
    }

    /// `initialize` handshake. Must be the first call. Returns the agent's
    /// `InitializeResponse` result (protocol version, agent info, capabilities).
    pub async fn initialize(
        &mut self,
        client_name: &str,
        client_version: &str,
    ) -> Result<Value, ProviderAdapterError> {
        let params = json!({
            "protocolVersion": PROTOCOL_VERSION,
            "clientInfo": { "name": client_name, "version": client_version },
        });
        let resp = self
            .transport
            .send_request(methods::INITIALIZE, Some(params))
            .await?;
        check_rpc_error(&resp)?;
        Ok(resp.result.unwrap_or(Value::Null))
    }

    /// `session/new` — opens a session rooted at `cwd`. Returns the agent-assigned
    /// `sessionId`. Call [`initialize`](Self::initialize) first.
    pub async fn new_session(&mut self, cwd: &str) -> Result<String, ProviderAdapterError> {
        let params = json!({ "cwd": cwd, "mcpServers": [] });
        let resp = self
            .transport
            .send_request(methods::SESSION_NEW, Some(params))
            .await?;
        check_rpc_error(&resp)?;
        resp.result
            .unwrap_or(Value::Null)
            .get("sessionId")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .ok_or_else(|| {
                ProviderAdapterError::Internal("session/new response missing sessionId".to_string())
            })
    }

    /// Send a `session/prompt` turn.
    ///
    /// `prompt` is the array of ACP `ContentBlock`s to submit
    /// (e.g. `[{"type":"text","text":"..."}]`). Events decoded from the
    /// interleaved `session/update` notifications are forwarded to `event_tx`
    /// as they arrive; the returned [`PromptResult`] reflects the final
    /// `PromptResponse`.
    ///
    /// Only one turn may be in flight at a time — this method exclusively drains
    /// the notification stream.
    pub async fn prompt(
        &mut self,
        session_id: &str,
        prompt: &[Value],
        event_tx: &mpsc::Sender<ProviderEvent>,
    ) -> Result<PromptResult, ProviderAdapterError> {
        let params = json!({ "sessionId": session_id, "prompt": prompt });
        let (_id, mut resp_rx) = self
            .transport
            .submit_request(methods::SESSION_PROMPT, Some(params))
            .await?;

        loop {
            tokio::select! {
                // `biased` + resp-first: a ready prompt response MUST win over a
                // closing incoming stream. When the agent sends the response then
                // exits, read_loop hits EOF and `incoming.recv()` would return
                // None — without this priority the select could nondeterministically
                // report ProcessExited instead of the actual response.
                biased;
                resp = &mut resp_rx => {
                    // Drain any notifications forwarded before the response but
                    // not yet consumed, so the caller never loses tail events.
                    while let Ok(msg) = self.incoming.try_recv() {
                        let _ = route_incoming(&self.transport, msg, event_tx).await;
                    }
                    return match resp {
                        Ok(r) => parse_prompt_response(r),
                        Err(_) => Err(ProviderAdapterError::ProcessExited(
                            "agent closed the prompt response channel".to_string(),
                        )),
                    };
                }
                msg = self.incoming.recv() => {
                    let Some(msg) = msg else {
                        return Err(ProviderAdapterError::ProcessExited(
                            "agent stream closed mid-turn".to_string(),
                        ));
                    };
                    route_incoming(&self.transport, msg, event_tx).await?;
                }
            }
        }
    }

    /// `session/cancel` — request the agent abort the in-flight turn. Sent as a
    /// notification (no response). Used to implement interrupt.
    pub async fn cancel(&self, session_id: &str) -> Result<(), ProviderAdapterError> {
        self.transport
            .send_notification(
                methods::SESSION_CANCEL,
                Some(json!({ "sessionId": session_id })),
            )
            .await
    }

    /// Tear down the underlying transport (kill child + abort reader). Idempotent.
    pub async fn shutdown(&self) -> Result<(), ProviderAdapterError> {
        self.transport.shutdown().await
    }

    /// Borrow the underlying transport (for adapters that need direct access).
    pub fn transport(&self) -> &JsonRpcTransport {
        &self.transport
    }
}

// ---------------------------------------------------------------------------
// incoming message routing (free fn: avoids &self-vs-&mut self.incoming borrow
// conflict inside the prompt select! loop)
// ---------------------------------------------------------------------------

/// Route one incoming message:
/// - `session/update` → mapped [`ProviderEvent`]s pushed to `event_tx`;
/// - other notifications → ignored;
/// - inbound requests (id present) → denied so the agent is not left waiting.
///
/// NOTE: the client-side RPCs (`session/request_permission`, `fs/read_text_file`,
/// `terminal/*`) are auto-denied here. A higher layer (the provider adapter) can
/// take over this policy to actually satisfy them; this just guarantees the agent
/// gets a definitive answer instead of hanging.
async fn route_incoming(
    transport: &JsonRpcTransport,
    msg: IncomingMessage,
    event_tx: &mpsc::Sender<ProviderEvent>,
) -> Result<(), ProviderAdapterError> {
    if msg.method == methods::SESSION_UPDATE {
        for event in map_session_update(&msg.params) {
            // Bounded backpressure: if the consumer lags we await rather than
            // drop tokens (losing stream fidelity would corrupt the turn).
            if event_tx.send(event).await.is_err() {
                break; // Consumer gone — turn is effectively cancelled upstream.
            }
        }
        return Ok(());
    }

    if let Some(id) = msg.id {
        transport
            .respond_to_peer(id, None, Some(method_not_handled(&msg.method)))
            .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// session/update → ProviderEvent mapping
// ---------------------------------------------------------------------------

/// Decode a `session/update` notification `params` (`{ sessionId, update }`)
/// into zero or more [`ProviderEvent`]s. Unknown update shapes are tolerated
/// (skipped) rather than erroring — agents emit update variants we don't model
/// (plan, mode changes, session info, ...).
fn map_session_update(params: &Value) -> Vec<ProviderEvent> {
    let Some(update) = params.get("update") else {
        return Vec::new();
    };
    let session_id = params
        .get("sessionId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let discriminant = update
        .get("sessionUpdate")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match discriminant {
        "agent_message_chunk" | "agent_thought_chunk" => {
            let Some(text) = extract_text(update.get("content")) else {
                return Vec::new();
            };
            vec![ProviderEvent::Token {
                session_id,
                content: text,
            }]
        }
        "tool_call" => {
            let tool_name = update
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("tool")
                .to_string();
            let tool_input = update.get("rawInput").cloned().unwrap_or(Value::Null);
            vec![ProviderEvent::ToolCall {
                session_id,
                tool_name,
                tool_input,
            }]
        }
        "tool_call_update" => {
            let status = update.get("status").and_then(|v| v.as_str()).unwrap_or("");
            if status == "completed" || status == "failed" {
                let tool_name = update
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("tool")
                    .to_string();
                let result = update.get("rawOutput").cloned().unwrap_or(Value::Null);
                vec![ProviderEvent::ToolResult {
                    session_id,
                    tool_name,
                    result,
                }]
            } else {
                // Intermediate tool progress — no ProviderEvent equivalent.
                Vec::new()
            }
        }
        // usage_update, plan, current_mode_update, available_commands_update,
        // config_option_update, session_info_update, user_message_chunk: no
        // direct ProviderEvent mapping (usage arrives in the PromptResponse).
        _ => Vec::new(),
    }
}

/// Concatenate every `text` part of an ACP content array
/// (`[{ "type":"text", "text":"..." }, ...]`). Non-text parts are ignored.
fn extract_text(content: Option<&Value>) -> Option<String> {
    let items = content?.as_array()?;
    let mut text = String::new();
    for item in items {
        if item.get("type").and_then(|v| v.as_str()) == Some("text") {
            if let Some(part) = item.get("text").and_then(|v| v.as_str()) {
                text.push_str(part);
            }
        }
    }
    if text.is_empty() { None } else { Some(text) }
}

// ---------------------------------------------------------------------------
// response helpers
// ---------------------------------------------------------------------------

/// Surface a JSON-RPC error object as [`ProviderAdapterError::RpcError`].
fn check_rpc_error(resp: &ProviderResponse) -> Result<(), ProviderAdapterError> {
    if let Some(err) = &resp.error {
        return Err(ProviderAdapterError::RpcError {
            code: err.code,
            message: err.message.clone(),
        });
    }
    Ok(())
}

/// Parse a `session/prompt` response into a [`PromptResult`].
fn parse_prompt_response(resp: ProviderResponse) -> Result<PromptResult, ProviderAdapterError> {
    check_rpc_error(&resp)?;
    let result = resp.result.unwrap_or(Value::Null);
    let stop_reason = result
        .get("stopReason")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let usage = result.get("usage").map(|u| UsageInfo {
        input_tokens: num(u, "inputTokens"),
        output_tokens: num(u, "outputTokens"),
        total_tokens: num(u, "totalTokens"),
    });
    Ok(PromptResult {
        stop_reason,
        usage,
        raw: result,
    })
}

fn num(v: &Value, key: &str) -> u32 {
    v.get(key).and_then(|x| x.as_u64()).unwrap_or(0) as u32
}

/// JSON-RPC "method not found" error for inbound requests we don't handle.
fn method_not_handled(method: &str) -> ProviderError {
    ProviderError {
        code: -32601,
        message: format!("syncode ACP client does not handle '{method}'"),
        data: None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    /// Wire an [`AcpClient`] to an in-process fake *agent* via two duplexes:
    ///   client_writer → peer_reader   (our requests reach the agent)
    ///   peer_writer   → client_reader (agent's responses/notifications reach us)
    fn acp_harness() -> (
        AcpClient,
        tokio::io::DuplexStream, // agent reads our requests here
        tokio::io::DuplexStream, // agent writes responses/notifications here
    ) {
        let (client_writer, peer_reader) = tokio::io::duplex(8192);
        let (peer_writer, client_reader) = tokio::io::duplex(8192);
        let (transport, incoming) =
            JsonRpcTransport::from_streams(Box::new(client_writer), Box::new(client_reader));
        (
            AcpClient::new(transport, incoming),
            peer_reader,
            peer_writer,
        )
    }

    /// Read one NDJSON line off the agent-side reader and parse it to a `Value`.
    async fn peer_read(reader: &mut BufReader<tokio::io::DuplexStream>) -> serde_json::Value {
        let mut line = String::new();
        assert!(reader.read_line(&mut line).await.unwrap() > 0, "peer EOF");
        serde_json::from_str(line.trim()).unwrap()
    }

    /// Write a JSON `Value` as one NDJSON line on the agent-side writer.
    async fn peer_write(writer: &mut tokio::io::DuplexStream, value: &serde_json::Value) {
        writer
            .write_all(format!("{}\n", value).as_bytes())
            .await
            .unwrap();
        writer.flush().await.unwrap();
    }

    #[tokio::test]
    async fn initialize_then_new_session_handshake() {
        let (mut client, peer_reader, peer_writer) = acp_harness();

        let peer = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            let mut writer = peer_writer;

            // initialize
            let req = peer_read(&mut reader).await;
            assert_eq!(req["method"], "initialize");
            assert_eq!(req["params"]["protocolVersion"], 1);
            peer_write(
                &mut writer,
                &json!({
                    "jsonrpc": "2.0",
                    "id": req["id"],
                    "result": {
                        "protocolVersion": 1,
                        "agentInfo": { "name": "fake-agent", "version": "0.1.0" }
                    }
                }),
            )
            .await;

            // session/new
            let req = peer_read(&mut reader).await;
            assert_eq!(req["method"], "session/new");
            assert_eq!(req["params"]["cwd"], "/tmp/proj");
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "sessionId": "sess-1" } }),
            )
            .await;
        });

        let info = client
            .initialize("syncode", "0.1.0")
            .await
            .expect("initialize");
        assert_eq!(info["agentInfo"]["name"], "fake-agent");

        let session_id = client.new_session("/tmp/proj").await.expect("new_session");
        assert_eq!(session_id, "sess-1");

        peer.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn new_session_missing_session_id_errors() {
        let (mut client, peer_reader, peer_writer) = acp_harness();
        let peer = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            let mut writer = peer_writer;
            let req = peer_read(&mut reader).await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": req["id"], "result": {} }),
            )
            .await;
        });

        let err = client.new_session("/tmp/proj").await.unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::Internal(ref m) if m.contains("sessionId")),
            "got {err:?}"
        );
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn prompt_maps_updates_and_completes() {
        let (mut client, peer_reader, peer_writer) = acp_harness();
        let (event_tx, mut event_rx) = mpsc::channel::<ProviderEvent>(64);

        let peer = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            let mut writer = peer_writer;

            let req = peer_read(&mut reader).await;
            assert_eq!(req["method"], "session/prompt");
            assert_eq!(req["params"]["sessionId"], "sess-1");
            assert_eq!(req["params"]["prompt"][0]["text"], "hi");
            let id = req["id"].clone();

            // Stream two message chunks, a tool call, and its completion.
            peer_write(
                &mut writer,
                &json!({
                    "jsonrpc": "2.0", "method": "session/update",
                    "params": { "sessionId": "sess-1", "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": [{ "type": "text", "text": "Hello " }]
                    }}
                }),
            )
            .await;
            peer_write(
                &mut writer,
                &json!({
                    "jsonrpc": "2.0", "method": "session/update",
                    "params": { "sessionId": "sess-1", "update": {
                        "sessionUpdate": "agent_thought_chunk",
                        "content": [{ "type": "text", "text": "(reasoning)" }]
                    }}
                }),
            )
            .await;
            peer_write(
                &mut writer,
                &json!({
                    "jsonrpc": "2.0", "method": "session/update",
                    "params": { "sessionId": "sess-1", "update": {
                        "sessionUpdate": "tool_call", "title": "read_file",
                        "toolCallId": "t1", "rawInput": { "path": "/x" }
                    }}
                }),
            )
            .await;
            peer_write(
                &mut writer,
                &json!({
                    "jsonrpc": "2.0", "method": "session/update",
                    "params": { "sessionId": "sess-1", "update": {
                        "sessionUpdate": "tool_call_update", "toolCallId": "t1",
                        "title": "read_file", "status": "completed", "rawOutput": { "ok": true }
                    }}
                }),
            )
            .await;

            // Final prompt response (turn complete).
            peer_write(
                &mut writer,
                &json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": {
                        "stopReason": "end_turn",
                        "usage": { "inputTokens": 10, "outputTokens": 5, "totalTokens": 15 }
                    }
                }),
            )
            .await;
        });

        let blocks = vec![json!({ "type": "text", "text": "hi" })];
        let result = client
            .prompt("sess-1", &blocks, &event_tx)
            .await
            .expect("prompt");

        assert_eq!(result.stop_reason.as_deref(), Some("end_turn"));
        let usage = result.usage.expect("usage");
        assert_eq!(
            (usage.input_tokens, usage.output_tokens, usage.total_tokens),
            (10, 5, 15)
        );

        drop(event_tx);
        let mut events = Vec::new();
        while let Some(ev) = event_rx.recv().await {
            events.push(ev);
        }
        assert_eq!(events.len(), 4, "{events:?}");
        assert!(matches!(&events[0], ProviderEvent::Token { content, .. } if content == "Hello "));
        assert!(
            matches!(&events[1], ProviderEvent::Token { content, .. } if content == "(reasoning)")
        );
        assert!(
            matches!(&events[2], ProviderEvent::ToolCall { tool_name, tool_input, .. }
            if tool_name == "read_file" && tool_input["path"] == "/x")
        );
        assert!(
            matches!(&events[3], ProviderEvent::ToolResult { tool_name, result, .. }
            if tool_name == "read_file" && result["ok"] == true)
        );

        peer.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn prompt_error_response_propagates_rpc_error() {
        let (mut client, peer_reader, peer_writer) = acp_harness();
        let (event_tx, _event_rx) = mpsc::channel::<ProviderEvent>(8);

        let peer = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            let mut writer = peer_writer;
            let req = peer_read(&mut reader).await;
            peer_write(
                &mut writer,
                &json!({
                    "jsonrpc": "2.0", "id": req["id"],
                    "error": { "code": -32000, "message": "agent blew up" }
                }),
            )
            .await;
        });

        let err = client
            .prompt("s", &[json!({ "type": "text", "text": "x" })], &event_tx)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::RpcError { code, .. } if code == -32000),
            "got {err:?}"
        );
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn agent_stream_close_mid_turn_is_reported() {
        let (mut client, peer_reader, peer_writer) = acp_harness();
        let (event_tx, _event_rx) = mpsc::channel::<ProviderEvent>(8);

        let peer = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            let writer = peer_writer;
            let _req = peer_read(&mut reader).await; // consume the prompt request
            drop(writer); // agent closes without responding
        });

        let err = client
            .prompt("s", &[json!({ "type": "text", "text": "x" })], &event_tx)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::ProcessExited(_)),
            "got {err:?}"
        );
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_sends_session_cancel_notification() {
        let (client, peer_reader, _peer_writer) = acp_harness();

        let handle = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            peer_read(&mut reader).await
        });

        client.cancel("sess-9").await.unwrap();
        let note = handle.await.unwrap();

        assert_eq!(note["method"], "session/cancel");
        assert_eq!(note["params"]["sessionId"], "sess-9");
        // A notification carries no id.
        assert!(
            note.get("id").is_none(),
            "expected a notification (no id): {note}"
        );
        client.shutdown().await.unwrap();
    }

    // NOTE: a full integration test for "inbound request auto-denied mid-turn"
    // (route_incoming's `respond_to_peer` branch) was explored but its fake-peer
    // orchestration proved timing-sensitive on the current_thread runtime. The
    // underlying `respond_to_peer` correctness is covered by
    // `subprocess::tests::respond_to_peer_sends_envelope`. End-to-end inbound
    // request handling (with a real permission/fs policy) lands in T3
    // (AcpProvider).

    #[tokio::test]
    async fn inbound_request_is_denied_so_agent_unblocks() {
        // An inbound request (e.g. session/request_permission) arriving during a
        // turn must be answered, not ignored. We verify the client replies with a
        // method-not-found error, then the turn completes normally.
        let (mut client, peer_reader, peer_writer) = acp_harness();
        let (event_tx, _event_rx) = mpsc::channel::<ProviderEvent>(8);

        let peer = tokio::spawn(async move {
            let mut reader = BufReader::new(peer_reader);
            let mut writer = peer_writer;

            let prompt_req = peer_read(&mut reader).await; // consume prompt
            let prompt_id = prompt_req["id"].clone();

            // Agent asks us a question mid-turn.
            peer_write(
                &mut writer,
                &json!({
                    "jsonrpc": "2.0", "id": 999, "method": "session/request_permission",
                    "params": { "sessionId": "s", "permission": { "type": "command" } }
                }),
            )
            .await;

            // Expect our error response to id 999.
            let reply = peer_read(&mut reader).await;
            assert_eq!(reply["id"], 999);
            assert!(
                reply.get("error").is_some(),
                "expected an error reply: {reply}"
            );
            assert_eq!(reply["error"]["code"], -32601);

            // Then complete the turn.
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": prompt_id, "result": { "stopReason": "end_turn" } }),
            )
            .await;
        });

        let result = client
            .prompt("s", &[json!({ "type": "text", "text": "x" })], &event_tx)
            .await
            .expect("prompt");
        assert_eq!(result.stop_reason.as_deref(), Some("end_turn"));
        peer.await.unwrap();
    }

    // --- pure mapping unit tests (no transport) ---

    #[test]
    fn map_skips_unknown_or_unmappable_variants() {
        assert!(
            map_session_update(
                &json!({ "sessionId": "s", "update": { "sessionUpdate": "plan", "entries": [] } })
            )
            .is_empty()
        );
        assert!(map_session_update(
            &json!({ "sessionId": "s", "update": { "sessionUpdate": "usage_update", "used": 1, "size": 2 } })
        )
        .is_empty());
        assert!(map_session_update(
            &json!({ "sessionId": "s", "update": { "sessionUpdate": "current_mode_update", "currentModeId": "m" } })
        )
        .is_empty());
    }

    #[test]
    fn map_tolerates_missing_update() {
        assert!(map_session_update(&json!({ "sessionId": "s" })).is_empty());
        assert!(map_session_update(&json!({})).is_empty());
    }

    #[test]
    fn map_message_chunk_concatenates_text_parts() {
        let events = map_session_update(&json!({
            "sessionId": "s",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": [
                    { "type": "text", "text": "foo" },
                    { "type": "image", "data": "x", "mimeType": "image/png" },
                    { "type": "text", "text": "bar" }
                ]
            }
        }));
        assert_eq!(events.len(), 1);
        assert!(
            matches!(events[0], ProviderEvent::Token { ref content, .. } if content == "foobar")
        );
    }

    #[test]
    fn map_tool_call_update_intermediate_is_skipped() {
        // In-progress tool updates (no terminal status) yield no event.
        let events = map_session_update(&json!({
            "sessionId": "s",
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": "t", "status": "in_progress" }
        }));
        assert!(events.is_empty());

        // Completed → ToolResult with rawOutput.
        let events = map_session_update(&json!({
            "sessionId": "s",
            "update": {
                "sessionUpdate": "tool_call_update", "toolCallId": "t",
                "title": "bash", "status": "completed", "rawOutput": { "stdout": "ok" }
            }
        }));
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            ProviderEvent::ToolResult { ref tool_name, ref result, .. }
            if tool_name == "bash" && result["stdout"] == "ok"
        ));
    }

    #[test]
    fn protocol_version_constant() {
        assert_eq!(PROTOCOL_VERSION, 1);
    }
}

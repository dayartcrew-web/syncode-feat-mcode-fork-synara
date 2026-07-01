//! Codex `app-server` client.
//!
//! Built on the protocol-agnostic [`JsonRpcTransport`] from [`crate::subprocess`],
//! exactly like [`crate::acp::AcpClient`] — but speaking Codex's own JSON-RPC
//! surface rather than ACP. The OpenAI Codex CLI (`codex app-server`) exposes a
//! thread/turn model that differs from ACP's session model:
//!
//! | concern            | ACP                              | Codex                                    |
//! |--------------------|----------------------------------|------------------------------------------|
//! | handshake          | `initialize` (`protocolVersion`) | `initialize` (NO version) + `initialized`|
//! | conversation root  | `session/new` → `sessionId`      | `thread/start` → `thread.id`             |
//! | one turn           | `session/prompt` (response ends) | `turn/start` then `turn/completed` notif |
//! | streamed deltas    | `session/update`                 | `item/agentMessage/delta`, `item/reasoning/*` |
//! | cancel             | `session/cancel`                 | `turn/interrupt`                         |
//! | tool approval      | (server-side policy)             | inbound `item/*/requestApproval` request |
//!
//! Reference (ground truth): `mcode/apps/server/src/codexAppServerManager.ts`
//! (subprocess + framing + RPC methods) and `mcode/apps/server/src/provider/Layers/CodexAdapter.ts`
//! (`mapToRuntimeEvents` notification→event mapping).
//!
//! # The streaming-response concurrency problem
//!
//! `turn/start` is a JSON-RPC *request* whose response merely acknowledges the
//! turn was accepted (carrying `turn.id`). The turn actually *ends* later via a
//! `turn/completed` (or `turn/aborted`) *notification* that arrives while the
//! `turn/start` response is (or was) pending, interleaved with `item/*` streaming
//! deltas. [`CodexAppServerClient::start_turn`] therefore submits the request
//! without awaiting ([`JsonRpcTransport::submit_request`]) and runs a
//! `tokio::select!` loop that concurrently consumes the `turn/start` response and
//! drains the notification stream, mapping each `item/*` delta to a live
//! [`ProviderEvent`] and returning once a terminal turn notification arrives.
//!
//! # Auth
//!
//! `codex app-server` self-authenticates: it reads its own ChatGPT/API
//! credentials from `~/.codex/` (or `$CODEX_HOME`). No token is passed on the
//! stdio channel — the spawned process simply inherits the parent environment.

use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::subprocess::{IncomingMessage, JsonRpcTransport, SubprocessSpec};
use crate::trait_def::{
    ProviderAdapterError, ProviderError, ProviderEvent, ProviderResponse, UsageInfo,
};

/// Client→Codex request methods.
mod methods {
    pub const INITIALIZE: &str = "initialize";
    /// Notification sent immediately after the `initialize` response (LSP-style).
    pub const INITIALIZED: &str = "initialized";
    pub const THREAD_START: &str = "thread/start";
    pub const TURN_START: &str = "turn/start";
    pub const TURN_INTERRUPT: &str = "turn/interrupt";
    /// Terminal notification: the turn finished (success/failure).
    pub const TURN_COMPLETED: &str = "turn/completed";
    /// Terminal notification: the turn was interrupted/aborted.
    pub const TURN_ABORTED: &str = "turn/aborted";
}

/// How a turn ended, decoded from the terminal notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnStatus {
    /// `turn/completed` with status `completed`.
    Completed,
    /// `turn/completed` with status `failed`.
    Failed,
    /// `turn/aborted`, or `cancelled`/`interrupted` status.
    Cancelled,
}

/// Outcome of a completed [`CodexAppServerClient::start_turn`] turn.
#[derive(Debug, Clone)]
pub struct TurnResult {
    /// Codex turn id (from the `turn/start` response), if reported before the
    /// turn ended.
    pub turn_id: Option<String>,
    /// Terminal status of the turn.
    pub status: TurnStatus,
    /// Codex stop reason (`turn.stopReason`), if reported.
    pub stop_reason: Option<String>,
    /// Token usage snapshot (from `thread/tokenUsage/updated` or
    /// `turn/completed.usage`), if observed.
    pub usage: Option<UsageInfo>,
    /// Raw terminal-notification payload (`turn/completed.params.turn`, or an
    /// empty object for `turn/aborted`). Surfaced to the caller verbatim.
    pub raw: Value,
}

/// Codex `app-server` client over a [`JsonRpcTransport`].
///
/// Construct with [`CodexAppServerClient::new`] (wrap an existing transport) or
/// [`CodexAppServerClient::spawn`] (spawn the `codex app-server` subprocess).
pub struct CodexAppServerClient {
    transport: JsonRpcTransport,
    incoming: mpsc::Receiver<IncomingMessage>,
}

impl CodexAppServerClient {
    /// Wrap an already-constructed transport + incoming channel.
    pub fn new(
        transport: JsonRpcTransport,
        incoming: mpsc::Receiver<IncomingMessage>,
    ) -> Self {
        Self {
            transport,
            incoming,
        }
    }

    /// Spawn the `codex app-server` subprocess described by `spec` and build a
    /// client over its piped stdio.
    pub async fn spawn(spec: &SubprocessSpec) -> Result<Self, ProviderAdapterError> {
        let (transport, incoming) = JsonRpcTransport::spawn(spec).await?;
        Ok(Self::new(transport, incoming))
    }

    /// `initialize` handshake. Must be the first call. Sends the Codex
    /// `initialize` request (no protocol version — Codex has none; capabilities
    /// advertise `experimentalApi`) and the follow-up `initialized` notification.
    /// Returns the raw `InitializeResponse` result.
    pub async fn initialize(
        &mut self,
        client_name: &str,
        client_version: &str,
    ) -> Result<Value, ProviderAdapterError> {
        let params = json!({
            "clientInfo": { "name": client_name, "title": client_name, "version": client_version },
            "capabilities": { "experimentalApi": true },
        });
        let resp = self
            .transport
            .send_request(methods::INITIALIZE, Some(params))
            .await?;
        check_rpc_error(&resp)?;
        // Codex expects an `initialized` notification immediately after the
        // initialize response (mirrors the LSP handshake Codex borrows).
        self.transport
            .send_notification(methods::INITIALIZED, None)
            .await?;
        Ok(resp.result.unwrap_or(Value::Null))
    }

    /// `thread/start` — opens a Codex thread rooted at `cwd`. Returns the
    /// agent-assigned thread id (Codex's equivalent of an ACP session id). Call
    /// [`initialize`](Self::initialize) first.
    ///
    /// `approval_policy` is one of `untrusted`/`on-failure`/`on-request`/`never`;
    /// `sandbox` is one of `read-only`/`workspace-write`/`danger-full-access`.
    pub async fn start_thread(
        &mut self,
        model: Option<&str>,
        cwd: &str,
        approval_policy: &str,
        sandbox: &str,
    ) -> Result<String, ProviderAdapterError> {
        let params = json!({
            "model": model,
            "cwd": cwd,
            "approvalPolicy": approval_policy,
            "sandbox": sandbox,
            "experimentalRawEvents": false,
        });
        let resp = self
            .transport
            .send_request(methods::THREAD_START, Some(params))
            .await?;
        check_rpc_error(&resp)?;
        let result = resp.result.unwrap_or(Value::Null);
        // Codex has reported both `{thread:{id}}` and a flat `{threadId}` shape;
        // accept either.
        let thread_id = result
            .get("thread")
            .and_then(|t| t.get("id"))
            .and_then(|v| v.as_str())
            .or_else(|| result.get("threadId").and_then(|v| v.as_str()))
            .map(str::to_owned)
            .ok_or_else(|| {
                ProviderAdapterError::Internal(
                    "thread/start response missing thread id".to_string(),
                )
            })?;
        Ok(thread_id)
    }

    /// Send a `turn/start` turn and stream it to completion.
    ///
    /// `input` is the Codex input array (e.g.
    /// `[{"type":"text","text":"..."}]`). Events decoded from the interleaved
    /// `item/*` deltas are forwarded to `event_tx` as they arrive; the returned
    /// [`TurnResult`] reflects the terminal `turn/completed` (or `turn/aborted`)
    /// notification. Inbound approval/user-input *requests* from Codex are
    /// answered inline (`auto_approve` controls approval decisions) so the agent
    /// is never left hanging mid-turn.
    ///
    /// Only one turn may be in flight at a time — this method exclusively drains
    /// the notification stream.
    pub async fn start_turn(
        &mut self,
        thread_id: &str,
        input: Vec<Value>,
        model: Option<&str>,
        auto_approve: bool,
        event_tx: &mpsc::Sender<ProviderEvent>,
    ) -> Result<TurnResult, ProviderAdapterError> {
        let mut params = json!({ "threadId": thread_id, "input": input });
        if let Some(model) = model {
            params["model"] = json!(model);
        }

        // Phase 1 — await the `turn/start` *acceptance* response. Unlike ACP's
        // `session/prompt` (whose response is terminal), Codex's `turn/start`
        // response merely acknowledges the turn; the turn actually ends later
        // via a `turn/completed`/`turn/aborted` notification. We therefore await
        // the response ONCE here (a `oneshot::Receiver` panics if re-polled
        // after completion, so it cannot live inside the drain loop's select).
        // Notifications arriving during this await are safely buffered in the
        // `incoming` channel (cap 256) and drained in phase 2.
        let (_id, mut resp_rx) = self
            .transport
            .submit_request(methods::TURN_START, Some(params))
            .await?;
        let turn_id: Option<String> = match (&mut resp_rx).await {
            Ok(r) => {
                check_rpc_error(&r)?;
                r.result
                    .as_ref()
                    .and_then(|v| v.get("turn"))
                    .and_then(|t| t.get("id"))
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            }
            Err(_) => {
                return Err(ProviderAdapterError::ProcessExited(
                    "codex closed the turn/start response channel".to_string(),
                ));
            }
        };
        drop(resp_rx);

        // Phase 2 — drain the notification stream until a terminal turn
        // notification arrives.
        let mut last_usage: Option<UsageInfo> = None;
        loop {
            match self.incoming.recv().await {
                Some(msg) => {
                    if let Some(done) = route_notification(
                        &self.transport,
                        msg,
                        thread_id,
                        &mut last_usage,
                        event_tx,
                        auto_approve,
                    )
                    .await?
                    {
                        return Ok(done_with_turn_id(done, turn_id));
                    }
                }
                None => {
                    return Err(ProviderAdapterError::ProcessExited(
                        "codex stream closed mid-turn".to_string(),
                    ));
                }
            }
        }
    }

    /// `turn/interrupt` — request the agent abort the in-flight turn. Sent as a
    /// notification (no response expected), matching ACP's `session/cancel`
    /// semantics. Used to implement interrupt.
    pub async fn interrupt(
        &self,
        thread_id: &str,
        turn_id: Option<&str>,
    ) -> Result<(), ProviderAdapterError> {
        let mut params = json!({ "threadId": thread_id });
        if let Some(turn_id) = turn_id {
            params["turnId"] = json!(turn_id);
        }
        self.transport
            .send_notification(methods::TURN_INTERRUPT, Some(params))
            .await
    }

    /// Tear down the underlying transport (kill child + abort reader). Idempotent.
    pub async fn shutdown(&self) -> Result<(), ProviderAdapterError> {
        self.transport.shutdown().await
    }

    /// Borrow the underlying transport (for adapters that need liveness checks).
    pub fn transport(&self) -> &JsonRpcTransport {
        &self.transport
    }
}

/// Route one incoming message, returning `Some(TurnResult)` when the message is a
/// terminal turn notification. A free function (not a method) so it can be called
/// from inside the `start_turn` `select!` arms without conflicting with the
/// `&mut self.incoming` borrow held by `incoming.recv()`.
///
/// - inbound requests (id present) → answered inline via [`answer_inbound`];
/// - `turn/completed` / `turn/aborted` → terminal [`TurnResult`];
/// - `item/agentMessage/delta` & `item/reasoning/*` → live [`ProviderEvent::Token`];
/// - `thread/tokenUsage/updated` → updates the `usage` snapshot;
/// - `error` → [`ProviderEvent::Error`] (non-terminal; the turn's own
///   `turn/completed` remains authoritative);
/// - other `item/*` → best-effort tool mapping via [`map_item_event`];
/// - everything else → ignored.
async fn route_notification(
    transport: &JsonRpcTransport,
    msg: IncomingMessage,
    thread_id: &str,
    usage: &mut Option<UsageInfo>,
    event_tx: &mpsc::Sender<ProviderEvent>,
    auto_approve: bool,
) -> Result<Option<TurnResult>, ProviderAdapterError> {
    // Inbound REQUESTS (approval / user-input) have an id — answer them so Codex
    // is never left waiting. Done before notification dispatch so an
    // `item/*/requestApproval` method never falls through to item-delta mapping.
    if let Some(id) = msg.id {
        answer_inbound(transport, id, &msg.method, auto_approve).await?;
        return Ok(None);
    }

    let method = msg.method.as_str();
    let params = &msg.params;

    match method {
        methods::TURN_COMPLETED => {
            let turn = params.get("turn").cloned().unwrap_or(Value::Null);
            let status = turn
                .get("status")
                .and_then(|v| v.as_str())
                .map(parse_turn_status)
                .unwrap_or(TurnStatus::Completed);
            let stop_reason = turn
                .get("stopReason")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            let resolved_usage = usage
                .clone()
                .or_else(|| turn.get("usage").and_then(parse_usage_from_value));
            return Ok(Some(TurnResult {
                turn_id: None,
                status,
                stop_reason,
                usage: resolved_usage,
                raw: turn,
            }));
        }
        methods::TURN_ABORTED => {
            return Ok(Some(TurnResult {
                turn_id: None,
                status: TurnStatus::Cancelled,
                stop_reason: None,
                usage: usage.clone(),
                raw: Value::Null,
            }));
        }
        "item/agentMessage/delta"
        | "item/reasoning/textDelta"
        | "item/reasoning/summaryTextDelta" => {
            if let Some(delta) = extract_delta(params) {
                forward(
                    event_tx,
                    ProviderEvent::Token {
                        session_id: thread_id.to_string(),
                        content: delta,
                    },
                )
                .await;
            }
        }
        "thread/tokenUsage/updated" => {
            if let Some(parsed) = parse_token_usage(params) {
                *usage = Some(parsed);
            }
        }
        "error" => {
            let message = params
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|v| v.as_str())
                .unwrap_or("codex reported an error")
                .to_string();
            forward(
                event_tx,
                ProviderEvent::Error {
                    session_id: thread_id.to_string(),
                    message,
                    code: None,
                },
            )
            .await;
        }
        m if m.starts_with("item/") => {
            for ev in map_item_event(m, params, thread_id) {
                forward(event_tx, ev).await;
            }
        }
        _ => {}
    }
    Ok(None)
}

/// Push an event onto the bounded `event_tx`, tolerating a dropped consumer
/// (consumer gone ⇒ turn is effectively cancelled upstream).
async fn forward(event_tx: &mpsc::Sender<ProviderEvent>, event: ProviderEvent) {
    let _ = event_tx.send(event).await;
}

/// Stamp a captured `turn_id` onto a terminal [`TurnResult`] produced before the
/// `turn/start` response arrived (rare ordering).
fn done_with_turn_id(mut done: TurnResult, turn_id: Option<String>) -> TurnResult {
    if done.turn_id.is_none() {
        done.turn_id = turn_id;
    }
    done
}

/// Answer an inbound Codex request so the agent unblocks:
/// - `item/*/requestApproval` → approve (`accept`) or deny (`decline`) per
///   `auto_approve`;
/// - `item/tool/requestUserInput` → empty answers (we cannot gather human input
///   from the adapter surface);
/// - anything else → JSON-RPC "method not found" (mirrors AcpClient).
async fn answer_inbound(
    transport: &JsonRpcTransport,
    id: u64,
    method: &str,
    auto_approve: bool,
) -> Result<(), ProviderAdapterError> {
    if method.ends_with("/requestApproval") {
        let decision = if auto_approve { "accept" } else { "decline" };
        transport
            .respond_to_peer(id, Some(json!({ "decision": decision })), None)
            .await
    } else if method == "item/tool/requestUserInput" {
        transport
            .respond_to_peer(id, Some(json!({ "answers": {} })), None)
            .await
    } else {
        transport
            .respond_to_peer(id, None, Some(method_not_handled(method)))
            .await
    }
}

// ---------------------------------------------------------------------------
// codex payload decoders (pure; unit-tested directly)
// ---------------------------------------------------------------------------

/// Extract a streamed text delta from a notification's params. Codex emits the
/// delta under any of `delta` / `text` / `content.text` (mcode's
/// `CodexAdapter.ts:1168-1173` tolerates all three).
fn extract_delta(params: &Value) -> Option<String> {
    let delta = params
        .get("delta")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("text").and_then(|v| v.as_str()))
        .or_else(|| {
            params
                .get("content")
                .and_then(|c| c.get("text"))
                .and_then(|v| v.as_str())
        })?;
    if delta.is_empty() {
        None
    } else {
        Some(delta.to_string())
    }
}

/// Decode a `turn/completed` status string into a [`TurnStatus`].
fn parse_turn_status(status: &str) -> TurnStatus {
    match status {
        "failed" => TurnStatus::Failed,
        "cancelled" | "interrupted" => TurnStatus::Cancelled,
        // "completed" and any unknown-but-successful status.
        _ => TurnStatus::Completed,
    }
}

/// Parse a `thread/tokenUsage/updated` notification's `tokenUsage` into a
/// [`UsageInfo`]. Tolerates both snake_case and camelCase field names (Codex has
/// emitted both); reads `last_token_usage.{input,output}_tokens` and
/// `total_token_usage.total_tokens`.
fn parse_token_usage(params: &Value) -> Option<UsageInfo> {
    let token_usage = params.get("tokenUsage").or_else(|| params.get("token_usage"))?;
    let last = token_usage
        .get("last_token_usage")
        .or_else(|| token_usage.get("lastTokenUsage"));
    let total_obj = token_usage
        .get("total_token_usage")
        .or_else(|| token_usage.get("totalTokenUsage"));

    let input_tokens = num(last, "input_tokens").or_else(|| num(last, "inputTokens"));
    let output_tokens = num(last, "output_tokens").or_else(|| num(last, "outputTokens"));
    let total_tokens = num(total_obj, "total_tokens").or_else(|| num(total_obj, "totalTokens"));

    // Only report usage if we observed something meaningful.
    if input_tokens == Some(0) && output_tokens == Some(0) && total_tokens == Some(0) {
        return None;
    }
    Some(UsageInfo {
        input_tokens: input_tokens.unwrap_or(0),
        output_tokens: output_tokens.unwrap_or(0),
        total_tokens: total_tokens.unwrap_or(0),
    })
}

/// Best-effort usage parse from an opaque `turn/completed.usage` object (Codex
/// passes it through as `unknown`). Falls back to treating the object itself.
fn parse_usage_from_value(usage: &Value) -> Option<UsageInfo> {
    parse_token_usage(&json!({ "tokenUsage": usage })).or_else(|| {
        let input = num(Some(usage), "input_tokens").or_else(|| num(Some(usage), "inputTokens"));
        let output = num(Some(usage), "output_tokens").or_else(|| num(Some(usage), "outputTokens"));
        let total = num(Some(usage), "total_tokens").or_else(|| num(Some(usage), "totalTokens"));
        if input.is_none() && output.is_none() && total.is_none() {
            None
        } else {
            Some(UsageInfo {
                input_tokens: input.unwrap_or(0),
                output_tokens: output.unwrap_or(0),
                total_tokens: total.unwrap_or(0),
            })
        }
    })
}

/// Best-effort mapping of an `item/*` notification to tool events:
/// - `item/started` with a tool-typed item → [`ProviderEvent::ToolCall`];
/// - `item/completed` with a tool-typed item → [`ProviderEvent::ToolResult`];
/// - non-tool items (reasoning summaries, file reads surfaced as items, …) → none.
///
/// The Codex item shape is rich and version-dependent; this maps the recognized
/// tool kinds (`command_execution`, `file_change`, `mcp_tool_call`,
/// `dynamic_tool_call`, `web_search`) and tolerates missing fields. Full plan /
/// diff / approval-surfacing fidelity is intentionally deferred.
fn map_item_event(method: &str, params: &Value, thread_id: &str) -> Vec<ProviderEvent> {
    let Some(item) = params.get("item") else {
        return Vec::new();
    };
    let kind = item
        .get("type")
        .and_then(|v| v.as_str())
        .or_else(|| item.get("kind").and_then(|v| v.as_str()))
        .unwrap_or("");
    let is_tool = matches!(
        kind,
        "command_execution"
            | "file_change"
            | "mcp_tool_call"
            | "dynamic_tool_call"
            | "web_search"
    );
    if !is_tool {
        return Vec::new();
    }

    let tool_name = item
        .get("command")
        .and_then(|c| c.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|s| !s.is_empty())
        .or_else(|| item.get("title").and_then(|v| v.as_str()).map(str::to_owned))
        .unwrap_or_else(|| kind.to_string());

    if method.ends_with("completed") {
        let result = item.get("result").cloned().unwrap_or_else(|| item.clone());
        vec![ProviderEvent::ToolResult {
            session_id: thread_id.to_string(),
            tool_name,
            result,
        }]
    } else {
        vec![ProviderEvent::ToolCall {
            session_id: thread_id.to_string(),
            tool_name,
            tool_input: item.clone(),
        }]
    }
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

/// Read a `u32` field from a (possibly absent) JSON object, tolerating absence.
fn num(obj: Option<&Value>, key: &str) -> Option<u32> {
    obj?.get(key).and_then(|x| x.as_u64()).map(|n| n as u32)
}

/// JSON-RPC "method not found" error for inbound requests we don't handle.
fn method_not_handled(method: &str) -> ProviderError {
    ProviderError {
        code: -32601,
        message: format!("syncode codex client does not handle '{method}'"),
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

    /// Wire a [`CodexAppServerClient`] to an in-process fake *server* via two
    /// duplexes (mirrors `acp::tests::acp_harness`):
    ///   client_writer → server_reader   (our requests reach the server)
    ///   server_writer  → client_reader  (server's responses/notifications reach us)
    fn codex_harness() -> (
        CodexAppServerClient,
        tokio::io::DuplexStream, // server reads our requests here
        tokio::io::DuplexStream, // server writes responses/notifications here
    ) {
        let (client_writer, server_reader) = tokio::io::duplex(8192);
        let (server_writer, client_reader) = tokio::io::duplex(8192);
        let (transport, incoming) =
            JsonRpcTransport::from_streams(Box::new(client_writer), Box::new(client_reader));
        (
            CodexAppServerClient::new(transport, incoming),
            server_reader,
            server_writer,
        )
    }

    /// Read one NDJSON line off the server-side reader and parse it to a `Value`.
    async fn peer_read(reader: &mut BufReader<tokio::io::DuplexStream>) -> serde_json::Value {
        let mut line = String::new();
        assert!(reader.read_line(&mut line).await.unwrap() > 0, "server EOF");
        serde_json::from_str(line.trim()).unwrap()
    }

    /// Write a JSON `Value` as one NDJSON line on the server-side writer.
    async fn peer_write(writer: &mut tokio::io::DuplexStream, value: &serde_json::Value) {
        writer
            .write_all(format!("{}\n", value).as_bytes())
            .await
            .unwrap();
        writer.flush().await.unwrap();
    }

    #[tokio::test]
    async fn initialize_handshake_sends_request_then_initialized_notification() {
        let (mut client, server_reader, server_writer) = codex_harness();

        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            let mut writer = server_writer;

            // initialize request
            let req = peer_read(&mut reader).await;
            assert_eq!(req["method"], "initialize");
            assert_eq!(req["params"]["clientInfo"]["name"], "syncode");
            assert_eq!(req["params"]["capabilities"]["experimentalApi"], true);
            assert!(
                req["params"].get("protocolVersion").is_none(),
                "codex initialize must NOT carry a protocolVersion"
            );
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": req["id"], "result": {} }),
            )
            .await;

            // initialized notification (no id)
            let note = peer_read(&mut reader).await;
            assert_eq!(note["method"], "initialized");
            assert!(note.get("id").is_none(), "initialized must be a notification");
        });

        let result = client.initialize("syncode", "0.1.0").await.expect("initialize");
        assert!(result.is_object(), "initialize result should be an object: {result}");

        server.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn start_thread_returns_thread_id_from_nested_or_flat_shape() {
        let (mut client, server_reader, server_writer) = codex_harness();

        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            let mut writer = server_writer;
            let req = peer_read(&mut reader).await;
            assert_eq!(req["method"], "thread/start");
            assert_eq!(req["params"]["cwd"], "/tmp/proj");
            assert_eq!(req["params"]["approvalPolicy"], "on-request");
            assert_eq!(req["params"]["sandbox"], "workspace-write");
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "thread": { "id": "thr-1" } } }),
            )
            .await;
        });

        let thread_id = client
            .start_thread(Some("gpt-5.5"), "/tmp/proj", "on-request", "workspace-write")
            .await
            .expect("start_thread");
        assert_eq!(thread_id, "thr-1");

        server.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn start_thread_missing_thread_id_errors() {
        let (mut client, server_reader, server_writer) = codex_harness();
        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            let mut writer = server_writer;
            let req = peer_read(&mut reader).await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": req["id"], "result": {} }),
            )
            .await;
        });

        let err = client
            .start_thread(None, "/tmp/proj", "never", "workspace-write")
            .await
            .unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::Internal(ref m) if m.contains("thread id")),
            "got {err:?}"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn start_turn_streams_deltas_and_completes() {
        let (mut client, server_reader, server_writer) = codex_harness();
        let (event_tx, mut event_rx) = mpsc::channel::<ProviderEvent>(64);

        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            let mut writer = server_writer;

            let req = peer_read(&mut reader).await;
            assert_eq!(req["method"], "turn/start");
            assert_eq!(req["params"]["threadId"], "thr-1");
            assert_eq!(req["params"]["input"][0]["text"], "hi");
            let id = req["id"].clone();

            // Stream two assistant deltas.
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "method": "item/agentMessage/delta",
                    "params": { "delta": "Hello " } }),
            )
            .await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "method": "item/reasoning/textDelta",
                    "params": { "delta": "(thinking)" } }),
            )
            .await;
            // Token usage snapshot.
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "method": "thread/tokenUsage/updated",
                    "params": { "tokenUsage": {
                        "total_token_usage": { "total_tokens": 42 },
                        "last_token_usage": { "input_tokens": 10, "output_tokens": 7 }
                    } } }),
            )
            .await;

            // turn/start response (acceptance), then turn/completed (terminal).
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": id, "result": { "turn": { "id": "turn-1" } } }),
            )
            .await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "method": "turn/completed",
                    "params": { "turn": { "status": "completed", "stopReason": "end_turn" } } }),
            )
            .await;
        });

        let input = vec![json!({ "type": "text", "text": "hi" })];
        let result = client
            .start_turn("thr-1", input, None, true, &event_tx)
            .await
            .expect("start_turn");

        assert_eq!(result.status, TurnStatus::Completed);
        assert_eq!(result.turn_id.as_deref(), Some("turn-1"));
        assert_eq!(result.stop_reason.as_deref(), Some("end_turn"));
        let usage = result.usage.expect("usage");
        assert_eq!((usage.input_tokens, usage.output_tokens, usage.total_tokens), (10, 7, 42));

        drop(event_tx);
        let mut events = Vec::new();
        while let Some(ev) = event_rx.recv().await {
            events.push(ev);
        }
        assert_eq!(events.len(), 2, "{events:?}");
        assert!(
            matches!(&events[0], ProviderEvent::Token { content, .. } if content == "Hello "),
            "{events:?}"
        );
        assert!(
            matches!(&events[1], ProviderEvent::Token { content, .. } if content == "(thinking)"),
            "{events:?}"
        );

        server.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn start_turn_aborted_is_cancelled() {
        let (mut client, server_reader, server_writer) = codex_harness();
        let (event_tx, _event_rx) = mpsc::channel::<ProviderEvent>(8);

        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            let mut writer = server_writer;
            let req = peer_read(&mut reader).await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "turn": { "id": "turn-2" } } }),
            )
            .await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "method": "turn/aborted", "params": {} }),
            )
            .await;
        });

        let result = client
            .start_turn("thr-1", vec![json!({ "type": "text", "text": "x" })], None, true, &event_tx)
            .await
            .expect("start_turn");
        assert_eq!(result.status, TurnStatus::Cancelled);

        server.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn start_turn_turn_completed_error_status_is_failed() {
        let (mut client, server_reader, server_writer) = codex_harness();
        let (event_tx, _event_rx) = mpsc::channel::<ProviderEvent>(8);

        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            let mut writer = server_writer;
            let req = peer_read(&mut reader).await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "turn": { "id": "t" } } }),
            )
            .await;
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "method": "turn/completed",
                    "params": { "turn": { "status": "failed", "error": { "message": "boom" } } } }),
            )
            .await;
        });

        let result = client
            .start_turn("thr-1", vec![json!({ "type": "text", "text": "x" })], None, true, &event_tx)
            .await
            .expect("start_turn");
        assert_eq!(result.status, TurnStatus::Failed);

        server.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn inbound_approval_is_auto_accepted_when_auto_approve() {
        let (mut client, server_reader, server_writer) = codex_harness();
        let (event_tx, _event_rx) = mpsc::channel::<ProviderEvent>(8);

        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            let mut writer = server_writer;

            let req = peer_read(&mut reader).await; // turn/start
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "turn": { "id": "t" } } }),
            )
            .await;

            // Server asks for command approval mid-turn.
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "id": 777,
                    "method": "item/commandExecution/requestApproval",
                    "params": { "turnId": "t", "itemId": "i", "threadId": "thr-1" } }),
            )
            .await;

            // Expect our approval decision.
            let reply = peer_read(&mut reader).await;
            assert_eq!(reply["id"], 777);
            assert_eq!(reply["result"]["decision"], "accept");

            // Then complete the turn.
            peer_write(
                &mut writer,
                &json!({ "jsonrpc": "2.0", "method": "turn/completed",
                    "params": { "turn": { "status": "completed" } } }),
            )
            .await;
        });

        let result = client
            .start_turn("thr-1", vec![json!({ "type": "text", "text": "x" })], None, true, &event_tx)
            .await
            .expect("start_turn");
        assert_eq!(result.status, TurnStatus::Completed);

        server.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn interrupt_sends_turn_interrupt_notification() {
        let (client, server_reader, _server_writer) = codex_harness();

        let handle = tokio::spawn(async move {
            let mut reader = BufReader::new(server_reader);
            peer_read(&mut reader).await
        });

        client.interrupt("thr-9", Some("turn-9")).await.unwrap();
        let note = handle.await.unwrap();

        assert_eq!(note["method"], "turn/interrupt");
        assert_eq!(note["params"]["threadId"], "thr-9");
        assert_eq!(note["params"]["turnId"], "turn-9");
        assert!(note.get("id").is_none(), "expected a notification: {note}");
        client.shutdown().await.unwrap();
    }

    // --- pure decoder unit tests (no transport) ---

    #[test]
    fn extract_delta_prefers_delta_field() {
        assert_eq!(
            extract_delta(&json!({ "delta": "hi" })).as_deref(),
            Some("hi")
        );
        assert_eq!(
            extract_delta(&json!({ "text": "hi2" })).as_deref(),
            Some("hi2")
        );
        assert_eq!(
            extract_delta(&json!({ "content": { "text": "hi3" } })).as_deref(),
            Some("hi3")
        );
        assert!(extract_delta(&json!({ "delta": "" })).is_none());
        assert!(extract_delta(&json!({})).is_none());
    }

    #[test]
    fn turn_status_decoding() {
        assert_eq!(parse_turn_status("completed"), TurnStatus::Completed);
        assert_eq!(parse_turn_status("failed"), TurnStatus::Failed);
        assert_eq!(parse_turn_status("cancelled"), TurnStatus::Cancelled);
        assert_eq!(parse_turn_status("interrupted"), TurnStatus::Cancelled);
        // Unknown but non-failure → completed (lenient).
        assert_eq!(parse_turn_status("unknown"), TurnStatus::Completed);
    }

    #[test]
    fn token_usage_snake_and_camel() {
        let snake = parse_token_usage(&json!({
            "tokenUsage": {
                "total_token_usage": { "total_tokens": 100 },
                "last_token_usage": { "input_tokens": 30, "output_tokens": 20 }
            }
        }))
        .unwrap();
        assert_eq!((snake.input_tokens, snake.output_tokens, snake.total_tokens), (30, 20, 100));

        let camel = parse_token_usage(&json!({
            "tokenUsage": {
                "totalTokenUsage": { "totalTokens": 5 },
                "lastTokenUsage": { "inputTokens": 2, "outputTokens": 1 }
            }
        }))
        .unwrap();
        assert_eq!((camel.input_tokens, camel.output_tokens, camel.total_tokens), (2, 1, 5));
    }

    #[test]
    fn token_usage_all_zero_is_none() {
        assert!(parse_token_usage(&json!({
            "tokenUsage": {
                "total_token_usage": { "total_tokens": 0 },
                "last_token_usage": { "input_tokens": 0, "output_tokens": 0 }
            }
        }))
        .is_none());
    }

    #[test]
    fn map_item_started_command_is_tool_call() {
        let events = map_item_event(
            "item/started",
            &json!({ "item": { "type": "command_execution", "command": ["ls", "-la"], "title": "list" } }),
            "thr-1",
        );
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], ProviderEvent::ToolCall { tool_name, tool_input, .. }
            if tool_name == "ls -la" && tool_input["type"] == "command_execution"),
            "{events:?}"
        );
    }

    #[test]
    fn map_item_completed_file_change_is_tool_result() {
        let events = map_item_event(
            "item/completed",
            &json!({ "item": { "type": "file_change", "title": "edit", "result": { "ok": true } } }),
            "thr-1",
        );
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], ProviderEvent::ToolResult { tool_name, result, .. }
            if tool_name == "edit" && result["ok"] == true),
            "{events:?}"
        );
    }

    #[test]
    fn map_item_non_tool_is_skipped() {
        assert!(map_item_event(
            "item/started",
            &json!({ "item": { "type": "reasoning" } }),
            "thr-1"
        )
        .is_empty());
        assert!(map_item_event("item/started", &json!({}), "thr-1").is_empty());
    }
}

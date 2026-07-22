//! Pi RPC client — JSON-over-stdio transport for `pi --mode rpc`.
//!
//! Drives the [`@earendil-works/pi-coding-agent`](https://agentclientprotocol.com)
//! CLI in headless RPC mode. Unlike the ACP and codex providers (which speak
//! JSON-RPC 2.0 and reuse [`JsonRpcTransport`](crate::subprocess::JsonRpcTransport)),
//! pi speaks its own `{"type":"<cmd>"}` / `{"type":"response"}` / event framing —
//! so it needs a transport with a pi-specific line classifier. The NDJSON
//! framing (one JSON object per `\n`) and subprocess machinery are identical.
//!
//! ## Turn lifecycle
//! A `prompt` command's response means "accepted", NOT "content". The actual
//! assistant output streams as interleaved events (`message_update` with
//! `text_delta`/`thinking_delta`, `tool_execution_*`, etc.) until a terminal
//! `agent_end`. A multi-step agentic turn emits multiple `turn_end` before the
//! single `agent_end` — so `agent_end` is the done-signal, not `turn_end`.
//!
//! ## Wire contract
//! See `.masday/research/custom-providers.md` and the SDK's `docs/rpc.md`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::Child;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::subprocess::SubprocessSpec;
use crate::trait_def::{ProviderAdapterError, ProviderEvent, UsageInfo};

/// Default timeout awaiting a command response (pi accepts fast; the agent run
/// is driven by events, not this). Mirrors `JsonRpcTransport`'s 30s.
const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// pi RPC command `type` strings.
pub mod methods {
    pub const PROMPT: &str = "prompt";
    pub const ABORT: &str = "abort";
    pub const NEW_SESSION: &str = "new_session";
    pub const GET_STATE: &str = "get_state";
}

/// The outcome of a completed prompt run.
#[derive(Debug, Clone)]
pub struct PromptResult {
    pub status: PromptStatus,
    /// The final assistant text (concatenated text deltas), if any.
    pub output: String,
    pub usage: Option<UsageInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptStatus {
    Completed,
    /// Turn ended via a user/network abort (mcode `piTurnFailure` "interrupted"
    /// state). Distinct from [`PromptStatus::Failed`] so callers can tell a
    /// clean cancel from a hard error — mcode drives UI state off this split.
    Interrupted,
    Failed,
}

/// NDJSON transport for `pi --mode rpc`. Classifies each stdout line by its
/// `type` field: `response` → routed to the pending awaiter keyed by string id;
/// anything else → forwarded whole to the incoming channel as an agent event.
pub struct PiTransport {
    writer: Arc<Mutex<Box<dyn AsyncWrite + Send + Unpin>>>,
    pending: Arc<StdMutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
    reader_handle: Mutex<Option<JoinHandle<()>>>,
    child: Mutex<Option<Child>>,
    next_id: AtomicU64,
}

impl PiTransport {
    /// Build from raw async I/O streams (for testing / custom wiring).
    /// Returns the transport + a receiver for streamed agent events.
    pub fn from_streams(
        writer: Box<dyn AsyncWrite + Send + Unpin>,
        reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> (Self, mpsc::Receiver<serde_json::Value>) {
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let (event_tx, event_rx) = mpsc::channel(256);
        let pending_clone = Arc::clone(&pending);
        let reader_handle = tokio::spawn(async move {
            read_loop(reader, pending_clone, event_tx).await;
        });
        (
            Self {
                writer: Arc::new(Mutex::new(writer)),
                pending,
                reader_handle: Mutex::new(Some(reader_handle)),
                child: Mutex::new(None),
                next_id: AtomicU64::new(1),
            },
            event_rx,
        )
    }

    /// Spawn `pi --mode rpc` per `spec` and build a transport over its stdio.
    pub async fn spawn(
        spec: &SubprocessSpec,
    ) -> Result<(Self, mpsc::Receiver<serde_json::Value>), ProviderAdapterError> {
        let mut cmd = tokio::process::Command::new(&spec.command);
        cmd.args(&spec.args).envs(spec.env.iter().cloned());
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        crate::subprocess::hide_console_window(&mut cmd);

        let mut child = cmd.spawn().map_err(ProviderAdapterError::Io)?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ProviderAdapterError::Internal("pi child stdin not piped".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ProviderAdapterError::Internal("pi child stdout not piped".into()))?;

        let (transport, rx) = Self::from_streams(Box::new(stdin), Box::new(stdout));
        *transport.child.lock().await = Some(child);
        Ok((transport, rx))
    }

    /// Allocate the next string request id (`"req-1"`, `"req-2"`, …).
    fn next_id(&self) -> String {
        format!("req-{}", self.next_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Send a command and await its `{"type":"response"}` ack (correlated by id).
    pub async fn send_command(
        &self,
        cmd_type: &str,
        mut payload: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, ProviderAdapterError> {
        let id = self.next_id();
        payload.insert("id".into(), id.clone().into());
        payload.insert("type".into(), cmd_type.into());
        let serialized = serde_json::to_string(&serde_json::Value::Object(payload))?;

        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);

        {
            let mut writer = self.writer.lock().await;
            writer.write_all(serialized.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
        }

        match tokio::time::timeout(DEFAULT_RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(value)) => Ok(value),
            _ => {
                self.pending.lock().unwrap().remove(&id);
                Err(ProviderAdapterError::Timeout(
                    DEFAULT_RESPONSE_TIMEOUT.as_secs(),
                ))
            }
        }
    }

    /// Send a command WITHOUT awaiting a response (for `prompt`, whose ack
    /// arrives interleaved with the event stream and isn't the content).
    /// Returns the allocated id so the caller can correlate if desired.
    pub async fn submit_command(
        &self,
        cmd_type: &str,
        mut payload: serde_json::Map<String, serde_json::Value>,
    ) -> Result<String, ProviderAdapterError> {
        let id = self.next_id();
        payload.insert("id".into(), id.clone().into());
        payload.insert("type".into(), cmd_type.into());
        let serialized = serde_json::to_string(&serde_json::Value::Object(payload))?;
        let mut writer = self.writer.lock().await;
        writer.write_all(serialized.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok(id)
    }

    /// Whether the spawned child is still running.
    pub async fn is_alive(&self) -> bool {
        let mut guard = self.child.lock().await;
        match guard.as_mut() {
            None => false,
            Some(child) => child.try_wait().map(|exit| exit.is_none()).unwrap_or(false),
        }
    }

    /// Kill the child + abort the reader. Idempotent.
    pub async fn shutdown(&self) -> Result<(), ProviderAdapterError> {
        if let Some(mut child) = self.child.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        if let Some(handle) = self.reader_handle.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }
}

/// Background reader: parse NDJSON lines, classify by `type`.
/// - `type:"response"` + `id` → route to the pending awaiter.
/// - everything else → forward the whole JSON object to the event channel.
async fn read_loop(
    reader: Box<dyn AsyncRead + Send + Unpin>,
    pending: Arc<StdMutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
    event_tx: mpsc::Sender<serde_json::Value>,
) {
    let mut buf = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        match buf.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() {
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };

        // Responses carry `type:"response"` and a string `id` correlating to a
        // pending command. Everything else (events, extension_ui_request) is
        // forwarded to the event channel.
        let is_response = value
            .get("type")
            .and_then(|v| v.as_str())
            .is_some_and(|t| t == "response");
        if is_response {
            if let Some(id) = value.get("id").and_then(|v| v.as_str()).map(String::from)
                && let Some(sender) = pending.lock().unwrap().remove(&id)
            {
                let _ = sender.send(value);
                continue;
            }
            // Stray response (no pending awaiter) — drop.
            continue;
        }
        if event_tx.send(value).await.is_err() {
            break; // consumer gone
        }
    }
}

/// pi RPC client: wraps a [`PiTransport`] + the event receiver, driving the
/// prompt lifecycle to terminal `agent_end`.
pub struct PiClient {
    transport: PiTransport,
    events: Mutex<mpsc::Receiver<serde_json::Value>>,
}

impl PiClient {
    pub fn new(transport: PiTransport, events: mpsc::Receiver<serde_json::Value>) -> Self {
        Self {
            transport,
            events: Mutex::new(events),
        }
    }

    pub async fn spawn(spec: &SubprocessSpec) -> Result<Self, ProviderAdapterError> {
        let (transport, events) = PiTransport::spawn(spec).await?;
        Ok(Self::new(transport, events))
    }

    pub fn from_streams(
        writer: Box<dyn AsyncWrite + Send + Unpin>,
        reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Self {
        let (transport, events) = PiTransport::from_streams(writer, reader);
        Self::new(transport, events)
    }

    /// Access the underlying transport (for `is_alive`/`shutdown`).
    pub fn transport(&self) -> &PiTransport {
        &self.transport
    }

    /// Abort the in-flight prompt turn.
    pub async fn abort(&self) -> Result<(), ProviderAdapterError> {
        self.transport
            .send_command(methods::ABORT, serde_json::Map::new())
            .await?;
        Ok(())
    }

    /// Send a prompt and drain the event stream to terminal `agent_end`,
    /// mapping events to [`ProviderEvent`] on `event_tx`. Returns the run outcome.
    ///
    /// The `prompt` response just means "accepted" — we submit it without
    /// awaiting and instead drive completion via the event stream. This is
    /// the same two-phase pattern codex uses (submit + drain notifications),
    /// adapted to pi's `type`-keyed framing.
    pub async fn prompt(
        &self,
        message: &str,
        session_id: &str,
        event_tx: &mpsc::Sender<ProviderEvent>,
    ) -> Result<PromptResult, ProviderAdapterError> {
        let mut payload = serde_json::Map::new();
        payload.insert("message".into(), message.into());
        // Submit without awaiting content — the response is just an ack.
        self.transport
            .submit_command(methods::PROMPT, payload)
            .await?;

        // Drain events until `agent_end`.
        let mut output = String::new();
        let mut status = PromptStatus::Completed;
        let mut usage: Option<UsageInfo> = None;
        let mut events = self.events.lock().await;
        loop {
            let Some(event) = events.recv().await else {
                // Stream closed without agent_end — treat as failure.
                status = PromptStatus::Failed;
                break;
            };
            let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if event_type == "agent_end" {
                // Classify the terminal outcome: completed vs interrupted vs
                // failed. Mirrors mcode's `classifyPiTurnFailure` — a user/
                // network abort (explicit "aborted"/"cancelled" reason, or an
                // error message matching an interruption marker) is an
                // *interrupt*, not a hard error.
                let stop_reason = extract_stop_reason(&event);
                let message = extract_failure_message(&event);
                // Keep the pre-set `Completed` on success; only override for a
                // failure/interrupt.
                let classified = classify_agent_end(&stop_reason, &message);
                if classified != PromptStatus::Completed {
                    status = classified;
                }
                if let Some(u) = extract_usage(&event) {
                    usage = Some(u);
                }
                let _ = event_tx
                    .send(terminal_event(session_id, &status, &output, usage.clone()))
                    .await;
                break;
            }
            // Map non-terminal events to ProviderEvent.
            if let Some(pe) = route_event(&event, session_id) {
                // Accumulate text deltas into the final output.
                if let ProviderEvent::Token { content, .. } = &pe {
                    output.push_str(content);
                }
                let _ = event_tx.send(pe).await;
            }
        }
        Ok(PromptResult {
            status,
            output,
            usage,
        })
    }
}

/// Map a pi agent event JSON to a [`ProviderEvent`]. Pure — unit-testable
/// with fake JSON. Returns `None` for events with no mapping (e.g. `turn_start`).
fn route_event(event: &serde_json::Value, session_id: &str) -> Option<ProviderEvent> {
    let event_type = event.get("type").and_then(|v| v.as_str())?;
    let sid = session_id.to_string();
    match event_type {
        // Streaming text/reasoning deltas, OR a per-message error delta.
        "message_update" => {
            let ame_type = event
                .pointer("/assistantMessageEvent/type")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if ame_type == "error" {
                return Some(ProviderEvent::Error {
                    session_id: sid,
                    message: "assistant message error".into(),
                    code: None,
                });
            }
            let delta = event
                .pointer("/assistantMessageEvent/delta")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    event
                        .pointer("/assistantMessageEvent/text_delta/delta")
                        .and_then(|v| v.as_str())
                });
            // thinking_delta also surfaces as /delta under thinking variant.
            let delta = delta.or_else(|| {
                event
                    .pointer("/assistantMessageEvent/thinking_delta/delta")
                    .and_then(|v| v.as_str())
            });
            delta.map(|d| ProviderEvent::Token {
                session_id: sid,
                content: d.to_string(),
            })
        }
        // Tool call start (from toolcall_end which carries the full ToolCall).
        "tool_execution_start" => {
            let name = event
                .get("toolName")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let args = event
                .get("args")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            Some(ProviderEvent::ToolCall {
                session_id: sid,
                tool_name: name,
                tool_input: args,
            })
        }
        // Tool result.
        "tool_execution_end" => {
            let name = event
                .get("toolName")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let result = event
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            Some(ProviderEvent::ToolResult {
                session_id: sid,
                tool_name: name,
                result,
            })
        }
        _ => None,
    }
}

/// Build the terminal ProviderEvent for an `agent_end`.
fn terminal_event(
    session_id: &str,
    status: &PromptStatus,
    output: &str,
    usage: Option<UsageInfo>,
) -> ProviderEvent {
    match status {
        PromptStatus::Completed | PromptStatus::Interrupted => ProviderEvent::Completed {
            session_id: session_id.to_string(),
            output: output.to_string(),
            usage,
        },
        PromptStatus::Failed => ProviderEvent::Error {
            session_id: session_id.to_string(),
            message: "pi agent ended with error".into(),
            code: None,
        },
    }
}

/// Extract the final assistant message's stopReason from an `agent_end` event.
fn extract_stop_reason(event: &serde_json::Value) -> Option<String> {
    let messages = event.get("messages").and_then(|v| v.as_array())?;
    let last = messages.last()?;
    last.get("stopReason")
        .and_then(|v| v.as_str())
        .map(String::from)
}

/// Defensively surface a failure message from an `agent_end` event. MCode reads
/// the in-process SDK's `session.agent.state.errorMessage`; in RPC mode the
/// exact carrier field is unverified, so probe the common shapes and return the
/// first text found (used to distinguish interrupt vs hard error).
fn extract_failure_message(event: &serde_json::Value) -> Option<String> {
    for key in ["error", "errorMessage", "message"] {
        if let Some(s) = event.get(key).and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
    }
    None
}

/// Message markers (case-insensitive substring) that indicate a user/network
/// abort rather than a hard error — a direct port of mcode's
/// `piTurnFailure.PI_INTERRUPTION_MARKERS`.
const PI_INTERRUPTION_MARKERS: &[&str] = &[
    "request was aborted",
    "operation was aborted",
    "aborterror",
    "interrupted by user",
    "user aborted",
];

/// Port of mcode's `piTurnFailure.isPiInterruptedMessage`.
fn is_pi_interrupted_message(message: &str) -> bool {
    let normalized = message.trim().to_ascii_lowercase();
    PI_INTERRUPTION_MARKERS
        .iter()
        .any(|marker| normalized.contains(marker))
}

/// Port of mcode's `piTurnFailure.classifyPiTurnFailure`: an interruption
/// marker in the message → `Interrupted`; otherwise a hard `Failed`.
fn classify_pi_turn_failure(message: &str) -> PromptStatus {
    if is_pi_interrupted_message(message) {
        PromptStatus::Interrupted
    } else {
        PromptStatus::Failed
    }
}

/// Classify the `agent_end` outcome from its stop reason + failure message.
///
/// A terminal `error`/`aborted`/`cancelled` reason is a failure; within that,
/// [`classify_pi_turn_failure`] splits interrupt vs hard error on the message.
/// An explicit abort/cancel reason with no message defaults to interrupted.
fn classify_agent_end(stop_reason: &Option<String>, message: &Option<String>) -> PromptStatus {
    let reason = stop_reason.as_deref().unwrap_or("");
    if !matches!(reason, "error" | "aborted" | "cancelled") {
        return PromptStatus::Completed;
    }
    match message.as_deref() {
        // mcode splits interrupted vs failed on the failure message.
        Some(m) => classify_pi_turn_failure(m),
        // No message: an explicit abort/cancel is an interrupt; a bare "error"
        // is a hard failure.
        None if matches!(reason, "aborted" | "cancelled") => PromptStatus::Interrupted,
        None => PromptStatus::Failed,
    }
}

/// Best-effort token-usage extraction from an `agent_end` event.
///
/// Extract token usage from an `agent_end` event.
///
/// Verified against the pi RPC spec (`docs/rpc.md` @ 0.80.3): the `agent_end`
/// event carries all generated messages, and each assistant message reports its
/// own usage at `usage` = `{ input, output, cacheRead, cacheWrite }` (no `total`
/// field). An agentic turn may issue several LLM calls (one per assistant
/// message), so we SUM usage across all assistant messages for an accurate
/// turn-total rather than reading only the last call. Session-wide totals are
/// also available via a separate `get_session_stats` command, but summing the
/// `agent_end` messages needs no extra round-trip. The top-level defensive
/// probes are kept as a fallback for forward-compat. Returns `None` when nothing
/// matches — the turn still completes; usage simply isn't reported.
fn extract_usage(event: &serde_json::Value) -> Option<UsageInfo> {
    // Verified path: sum `usage` across assistant messages in `agent_end`.
    if let Some(msgs) = event.get("messages").and_then(|v| v.as_array()) {
        let (mut input, mut output, mut any) = (0u32, 0u32, false);
        for m in msgs {
            if m.get("role").and_then(|v| v.as_str()) != Some("assistant") {
                continue;
            }
            if let Some(u) = m.get("usage") {
                input = input.saturating_add(
                    uint(u, &["input", "input_tokens", "inputTokens"]).unwrap_or(0),
                );
                output = output.saturating_add(
                    uint(u, &["output", "output_tokens", "outputTokens"]).unwrap_or(0),
                );
                any = true;
            }
        }
        if any {
            return Some(UsageInfo {
                input_tokens: input,
                output_tokens: output,
                total_tokens: input.saturating_add(output),
            });
        }
    }
    // Defensive fallback: top-level usage-ish objects (forward-compat).
    for obj in [
        event.get("usage"),
        event.get("tokens"),
        event.get("tokenUsage"),
        event.get("stats"),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(info) = usage_from_obj(obj) {
            return Some(info);
        }
    }
    None
}

fn usage_from_obj(u: &serde_json::Value) -> Option<UsageInfo> {
    let input = uint(u, &["input_tokens", "inputTokens", "input"])?;
    let output = uint(u, &["output_tokens", "outputTokens", "output"]).unwrap_or(0);
    let total = uint(u, &["total_tokens", "totalTokens", "total"]).unwrap_or(input + output);
    Some(UsageInfo {
        input_tokens: input,
        output_tokens: output,
        total_tokens: total,
    })
}

fn uint(v: &serde_json::Value, keys: &[&str]) -> Option<u32> {
    keys.iter()
        .find_map(|k| v.get(k).and_then(|x| x.as_u64()).map(|n| n as u32))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Duplex test harness: returns a PiClient whose stdin/stdout are wired to
    /// a peer's read/write halves. The peer scripts emit canned event sequences.
    fn pi_harness() -> (PiClient, Box<dyn AsyncWrite + Send + Unpin>) {
        let (client_io, peer_io) = tokio::io::duplex(1024);
        let (client_reader, client_writer) = tokio::io::split(client_io);
        let (_peer_reader, peer_writer) = tokio::io::split(peer_io);
        (
            PiClient::from_streams(Box::new(client_writer), Box::new(client_reader)),
            Box::new(peer_writer),
        )
    }

    async fn peer_write(writer: &mut (impl AsyncWrite + Unpin), value: &serde_json::Value) {
        let mut s = serde_json::to_string(value).unwrap();
        s.push('\n');
        writer.write_all(s.as_bytes()).await.unwrap();
        writer.flush().await.unwrap();
    }

    #[tokio::test]
    async fn prompt_drains_text_deltas_to_completed() {
        let (client, mut peer_writer) = pi_harness();
        let (event_tx, mut event_rx) = mpsc::channel::<ProviderEvent>(64);

        // The peer receives our `prompt` command, acks it, then streams events.
        let prompt_task =
            tokio::spawn(async move { client.prompt("hello", "pi-sess-1", &event_tx).await });

        // Give the transport a moment to send the prompt command to the peer.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Stream: prompt ack + two text deltas + agent_end.
        peer_write(
            &mut peer_writer,
            &json!({"id":"req-1","type":"response","command":"prompt","success":true}),
        )
        .await;
        peer_write(
            &mut peer_writer,
            &json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"Hello"}}),
        )
        .await;
        peer_write(
            &mut peer_writer,
            &json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":", world!"}}),
        )
        .await;
        peer_write(
            &mut peer_writer,
            &json!({"type":"agent_end","messages":[{"role":"assistant","stopReason":"stop"}]}),
        )
        .await;

        let result = prompt_task.await.unwrap().unwrap();
        assert_eq!(result.status, PromptStatus::Completed);
        assert_eq!(result.output, "Hello, world!");

        // Two Token events were emitted.
        let mut tokens = Vec::new();
        while let Ok(Some(ev)) =
            tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await
        {
            if let ProviderEvent::Token { content, .. } = ev {
                tokens.push(content);
            }
        }
        assert_eq!(tokens, vec!["Hello", ", world!"]);
    }

    #[tokio::test]
    async fn prompt_aborted_stop_reason_is_interrupted() {
        // mcode distinguishes a user/network abort (interrupted) from a hard
        // error (failed). An explicit "aborted" stop reason → Interrupted, not
        // Failed.
        let (client, mut peer_writer) = pi_harness();
        let (event_tx, _event_rx) = mpsc::channel::<ProviderEvent>(64);

        let prompt_task =
            tokio::spawn(async move { client.prompt("hello", "pi-sess-2", &event_tx).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        peer_write(
            &mut peer_writer,
            &json!({"id":"req-1","type":"response","command":"prompt","success":true}),
        )
        .await;
        peer_write(
            &mut peer_writer,
            &json!({"type":"agent_end","messages":[{"role":"assistant","stopReason":"aborted"}]}),
        )
        .await;

        let result = prompt_task.await.unwrap().unwrap();
        assert_eq!(result.status, PromptStatus::Interrupted);
    }

    #[tokio::test]
    async fn send_command_correlates_by_string_id() {
        let (client, mut peer_writer) = pi_harness();

        let abort_task = tokio::spawn(async move {
            client
                .transport
                .send_command(methods::ABORT, serde_json::Map::new())
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        peer_write(
            &mut peer_writer,
            &json!({"id":"req-1","type":"response","command":"abort","success":true}),
        )
        .await;

        let resp = abort_task.await.unwrap().unwrap();
        assert_eq!(resp["success"], true);
    }

    #[test]
    fn route_event_maps_text_delta() {
        let ev = json!({
            "type": "message_update",
            "assistantMessageEvent": {"type": "text_delta", "delta": "hi"}
        });
        let pe = route_event(&ev, "s1").unwrap();
        match pe {
            ProviderEvent::Token {
                content,
                session_id,
            } => {
                assert_eq!(content, "hi");
                assert_eq!(session_id, "s1");
            }
            _ => panic!("expected Token"),
        }
    }

    #[test]
    fn route_event_maps_thinking_delta() {
        let ev = json!({
            "type": "message_update",
            "assistantMessageEvent": {"type": "thinking_delta", "delta": "hmm"}
        });
        let pe = route_event(&ev, "s1").unwrap();
        assert!(matches!(pe, ProviderEvent::Token { content, .. } if content == "hmm"));
    }

    #[test]
    fn route_event_maps_tool_execution() {
        let start = json!({"type":"tool_execution_start","toolName":"bash","args":{"cmd":"ls"}});
        let pe = route_event(&start, "s1").unwrap();
        assert!(matches!(pe, ProviderEvent::ToolCall { ref tool_name, .. } if tool_name == "bash"));

        let end = json!({"type":"tool_execution_end","toolName":"bash","result":"file.txt"});
        let pe = route_event(&end, "s1").unwrap();
        assert!(
            matches!(pe, ProviderEvent::ToolResult { ref tool_name, .. } if tool_name == "bash")
        );
    }

    #[test]
    fn route_event_returns_none_for_unmapped() {
        let ev = json!({"type": "turn_start"});
        assert!(route_event(&ev, "s1").is_none());
    }

    #[test]
    fn extract_stop_reason_from_last_message() {
        let ev = json!({"type":"agent_end","messages":[{"role":"user"},{"role":"assistant","stopReason":"stop"}]});
        assert_eq!(extract_stop_reason(&ev).as_deref(), Some("stop"));

        let ev = json!({"type":"agent_end","messages":[{"role":"assistant","stopReason":"error"}]});
        assert_eq!(extract_stop_reason(&ev).as_deref(), Some("error"));
    }

    #[test]
    fn classify_pi_turn_failure_splits_interrupt_vs_error() {
        // Direct port of mcode piTurnFailure markers.
        for msg in [
            "Request was aborted by the user",
            "the operation was aborted",
            "AbortError: timed out",
            "Interrupted by user",
            "the user aborted the request",
        ] {
            assert_eq!(
                classify_pi_turn_failure(msg),
                PromptStatus::Interrupted,
                "{msg}"
            );
        }
        // Hard errors stay Failed.
        for msg in [
            "model returned an error",
            "rate limit exceeded",
            "internal server error",
        ] {
            assert_eq!(classify_pi_turn_failure(msg), PromptStatus::Failed, "{msg}");
        }
    }

    #[test]
    fn classify_agent_end_preserves_completed_vs_interrupted_vs_failed() {
        let none = Option::<String>::None;
        // Success → Completed.
        assert_eq!(
            classify_agent_end(&Some("stop".into()), &none),
            PromptStatus::Completed
        );
        assert_eq!(classify_agent_end(&none, &none), PromptStatus::Completed);
        // Explicit abort reason without message → Interrupted.
        assert_eq!(
            classify_agent_end(&Some("aborted".into()), &none),
            PromptStatus::Interrupted
        );
        assert_eq!(
            classify_agent_end(&Some("cancelled".into()), &none),
            PromptStatus::Interrupted
        );
        // Bare error reason → Failed.
        assert_eq!(
            classify_agent_end(&Some("error".into()), &none),
            PromptStatus::Failed
        );
        // Error reason + interruption message → Interrupted (message wins).
        assert_eq!(
            classify_agent_end(&Some("error".into()), &Some("request was aborted".into())),
            PromptStatus::Interrupted
        );
    }

    #[test]
    fn extract_failure_message_probes_common_fields() {
        assert_eq!(
            extract_failure_message(&json!({"error": "boom"})).as_deref(),
            Some("boom")
        );
        assert_eq!(
            extract_failure_message(&json!({"errorMessage": "x"})).as_deref(),
            Some("x")
        );
        assert_eq!(extract_failure_message(&json!({"type": "agent_end"})), None);
    }

    #[test]
    fn extract_usage_is_defensive_across_shapes() {
        // VERIFIED pi RPC spec shape: agent_end carries the final assistant
        // message's usage at messages[last].usage = {input,output,cacheRead,
        // cacheWrite} (no total — we synthesize input+output).
        let ev = json!({
            "type": "agent_end",
            "messages": [
                { "role": "user" },
                { "role": "assistant", "usage": { "input": 100, "output": 50, "cacheRead": 0, "cacheWrite": 0 }, "stopReason": "stop" }
            ]
        });
        let u = extract_usage(&ev).expect("usage from messages[last].usage");
        assert_eq!(
            (u.input_tokens, u.output_tokens, u.total_tokens),
            (100, 50, 150)
        );

        // Agentic turn: multiple assistant messages (one per LLM call) — usage
        // is SUMMED across them for an accurate turn total.
        let ev = json!({
            "type": "agent_end",
            "messages": [
                { "role": "assistant", "usage": { "input": 100, "output": 50 } },
                { "role": "user", "content": "tool result" },
                { "role": "assistant", "usage": { "input": 200, "output": 30 } }
            ]
        });
        let u = extract_usage(&ev).expect("summed usage");
        assert_eq!(
            (u.input_tokens, u.output_tokens, u.total_tokens),
            (300, 80, 380)
        );

        // snake_case under top-level `usage` (defensive fallback).
        let ev = json!({"usage": {"input_tokens": 10, "output_tokens": 4}});
        let u = extract_usage(&ev).expect("usage");
        assert_eq!(
            (u.input_tokens, u.output_tokens, u.total_tokens),
            (10, 4, 14)
        );
        // camelCase under `tokens` (defensive fallback).
        let ev = json!({"tokens": {"inputTokens": 7, "outputTokens": 3, "totalTokens": 11}});
        let u = extract_usage(&ev).expect("usage");
        assert_eq!(
            (u.input_tokens, u.output_tokens, u.total_tokens),
            (7, 3, 11)
        );
        // Nothing recognizable → None (turn still completes without usage).
        assert!(extract_usage(&json!({"type": "agent_end"})).is_none());
        // agent_end with messages but no usage on the last message → None.
        assert!(
            extract_usage(
                &json!({"type":"agent_end","messages":[{"role":"assistant","stopReason":"stop"}]})
            )
            .is_none()
        );
        // No input key → None (don't fabricate usage from output alone).
        assert!(extract_usage(&json!({"usage": {"output_tokens": 5}})).is_none());
    }
}

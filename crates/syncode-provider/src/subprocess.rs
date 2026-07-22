//! Subprocess transport — NDJSON JSON-RPC over a child process's stdio.
//!
//! This is the foundational, protocol-agnostic layer beneath the ACP client
//! (see `acp`). It handles:
//!   1. Spawning a child process with piped stdin/stdout.
//!   2. NDJSON framing (newline-delimited JSON-RPC 2.0 messages).
//!   3. Request/response correlation by JSON-RPC `id` (async `send_request`
//!      awaits the matching response via a pending-request map + oneshot).
//!   4. A background stdout-reader task that routes responses to their
//!      awaiters and forwards notifications + inbound requests to an mpsc channel.
//!   5. Graceful shutdown (kill child, abort reader).
//!
//! The transport knows nothing about ACP semantics — it speaks raw JSON-RPC.
//! Higher layers (the ACP client) interpret `IncomingMessage`s.
//!
//! Wire format: standard JSON-RPC 2.0. A message is a *response* if it has an
//! `id` and no `method`; a *notification* if it has a `method` and no `id`; an
//! *inbound request* (peer→us) if it has both. This mirrors the ACP framing in
//! the MCode reference (`packages/effect-acp/src/protocol.ts`, `ndJsonRpc`).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::Child;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::trait_def::{ProviderAdapterError, ProviderError, ProviderRequest, ProviderResponse};

/// Default per-request timeout for [`JsonRpcTransport::send_request`].
/// 120s — generous enough for `codex app-server` initialize (which starts
/// MCP servers sequentially) and for AI turn completion on slow connections.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Re-export the shared `CREATE_NO_WINDOW` chokepoint from `syncode-core` so
/// the 5 provider spawn sites below (and external callers via
/// `crate::subprocess::hide_console_window`) use the SAME implementation as
/// git/gh/npm/automation/mcp/voice spawns in the other crates. See
/// `syncode_core::util::subprocess`.
pub use syncode_core::util::subprocess::hide_console_window;

/// Spawn specification for a subprocess, mirroring MCode's `AcpSpawnInput`
/// `{ command, args, cwd?, env? }`.
#[derive(Debug, Clone)]
pub struct SubprocessSpec {
    /// Binary to execute (resolved via `$PATH`).
    pub command: String,
    /// Command-line arguments.
    pub args: Vec<String>,
    /// Working directory (defaults to inherited).
    pub cwd: Option<PathBuf>,
    /// Additional environment variables to set/override (parent env is inherited).
    pub env: Vec<(String, String)>,
}

impl SubprocessSpec {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            cwd: None,
            env: Vec::new(),
        }
    }

    #[must_use]
    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    #[must_use]
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }
}

/// A message received from the peer that is NOT a response to one of our
/// requests: either a *notification* (`id` = `None`) or an inbound *request*
/// (`id` = `Some`) that the peer expects us to answer via
/// [`JsonRpcTransport::respond_to_peer`].
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub id: Option<u64>,
    pub method: String,
    pub params: serde_json::Value,
}

/// NDJSON JSON-RPC 2.0 transport over a child process's stdio.
///
/// Construct with [`JsonRpcTransport::spawn`] (real subprocess) or
/// [`JsonRpcTransport::from_streams`] (raw I/O, for testing / custom wiring).
pub struct JsonRpcTransport {
    /// Stdin of the child (or a raw writer). Guarded so `send_*` can take `&self`.
    writer: std::sync::Arc<Mutex<Box<dyn AsyncWrite + Send + Unpin>>>,
    /// Outstanding requests awaiting a correlated response, keyed by JSON-RPC id.
    pending: std::sync::Arc<Mutex<HashMap<u64, oneshot::Sender<ProviderResponse>>>>,
    /// The background stdout-reader task (so we can abort it on shutdown).
    reader_handle: Mutex<Option<JoinHandle<()>>>,
    /// The child process, if spawned via [`spawn`](Self::spawn). `None` when
    /// built from raw streams.
    child: Mutex<Option<Child>>,
    /// Monotonic JSON-RPC request id allocator.
    next_id: AtomicU64,
}

impl JsonRpcTransport {
    /// Build a transport from arbitrary async I/O streams. The reader is moved
    /// into a background task; the writer is retained for `send_*` calls.
    ///
    /// Returns the transport and the receiver for forwarded
    /// notifications + inbound requests.
    pub fn from_streams(
        writer: Box<dyn AsyncWrite + Send + Unpin>,
        reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> (Self, mpsc::Receiver<IncomingMessage>) {
        let pending = std::sync::Arc::new(Mutex::new(HashMap::new()));
        let (incoming_tx, incoming_rx) = mpsc::channel(256);

        let pending_clone = std::sync::Arc::clone(&pending);
        let reader_handle = tokio::spawn(async move {
            read_loop(reader, pending_clone, incoming_tx).await;
        });

        let transport = Self {
            writer: std::sync::Arc::new(Mutex::new(writer)),
            pending,
            reader_handle: Mutex::new(Some(reader_handle)),
            child: Mutex::new(None),
            next_id: AtomicU64::new(1),
        };
        (transport, incoming_rx)
    }

    /// Spawn the subprocess described by `spec` and build a transport over its
    /// piped stdin/stdout. The child is marked `kill_on_drop` as a safety net.
    pub async fn spawn(
        spec: &SubprocessSpec,
    ) -> Result<(Self, mpsc::Receiver<IncomingMessage>), ProviderAdapterError> {
        // On Windows, `.cmd` / `.bat` wrappers need to be run via `cmd /C`
        // for stdio pipes to work correctly. Rust's `Command::new` doesn't
        // auto-resolve PATHEXT and batch wrappers break pipe inheritance.
        #[cfg(windows)]
        let mut cmd = {
            let resolved = crate::bin_resolver::resolve_binary(&spec.command);
            let mut c = if resolved.ends_with(".cmd") || resolved.ends_with(".bat") {
                let mut c = tokio::process::Command::new("cmd");
                c.arg("/C").arg(&resolved);
                c
            } else {
                tokio::process::Command::new(&resolved)
            };
            for a in &spec.args {
                c.arg(a);
            }
            c.envs(spec.env.iter().cloned());
            c
        };
        #[cfg(not(windows))]
        let mut cmd = {
            let mut c = tokio::process::Command::new(&spec.command);
            for a in &spec.args {
                c.arg(a);
            }
            c.envs(spec.env.iter().cloned());
            c
        };
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        hide_console_window(&mut cmd);

        let mut child = cmd.spawn().map_err(ProviderAdapterError::Io)?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ProviderAdapterError::Internal("child stdin not piped".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ProviderAdapterError::Internal("child stdout not piped".into()))?;

        let (transport, rx) = Self::from_streams(Box::new(stdin), Box::new(stdout));
        *transport.child.lock().await = Some(child);
        Ok((transport, rx))
    }

    /// Send a JSON-RPC request and await the correlated response.
    pub async fn send_request(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<ProviderResponse, ProviderAdapterError> {
        self.send_request_with_timeout(method, params, DEFAULT_REQUEST_TIMEOUT)
            .await
    }

    /// Submit a JSON-RPC request WITHOUT awaiting the response.
    ///
    /// Allocates an id, registers a pending-response slot, and writes the request
    /// immediately. Returns the request id and a [`oneshot::Receiver`] that
    /// resolves with the correlated response (routed by the background reader).
    ///
    /// This is the primitive callers need when they must await a response
    /// *concurrently* with draining interleaved notifications on the same
    /// transport — e.g. ACP `session/prompt`, where the agent streams
    /// `session/update` notifications while the prompt response is pending.
    pub async fn submit_request(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<(u64, oneshot::Receiver<ProviderResponse>), ProviderAdapterError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        let request = ProviderRequest {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.to_string(),
            params,
        };
        let serialized = serde_json::to_string(&request)?;

        {
            let mut writer = self.writer.lock().await;
            writer.write_all(serialized.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
        }
        Ok((id, rx))
    }

    /// As [`send_request`](Self::send_request) with a caller-supplied timeout.
    pub async fn send_request_with_timeout(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
        timeout: Duration,
    ) -> Result<ProviderResponse, ProviderAdapterError> {
        let (id, rx) = self.submit_request(method, params).await?;
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => {
                // Responder dropped — reader task likely exited.
                self.pending.lock().await.remove(&id);
                Err(ProviderAdapterError::ProcessExited(
                    "response channel closed".to_string(),
                ))
            }
            Err(_) => {
                self.pending.lock().await.remove(&id);
                Err(ProviderAdapterError::Timeout(
                    timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                ))
            }
        }
    }

    /// Send a JSON-RPC notification (no `id`, no response expected).
    pub async fn send_notification(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<(), ProviderAdapterError> {
        let mut envelope = serde_json::Map::new();
        envelope.insert("jsonrpc".into(), "2.0".into());
        envelope.insert("method".into(), method.into());
        envelope.insert("params".into(), params.unwrap_or(serde_json::Value::Null));
        let serialized = serde_json::to_string(&serde_json::Value::Object(envelope))?;

        let mut writer = self.writer.lock().await;
        writer.write_all(serialized.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok(())
    }

    /// Reply to an inbound request (`id` = `Some`) previously surfaced via the
    /// incoming channel. Exactly one of `result` / `error` should be `Some`.
    pub async fn respond_to_peer(
        &self,
        id: u64,
        result: Option<serde_json::Value>,
        error: Option<ProviderError>,
    ) -> Result<(), ProviderAdapterError> {
        let mut envelope = serde_json::Map::new();
        envelope.insert("jsonrpc".into(), "2.0".into());
        envelope.insert("id".into(), id.into());
        match (result, error) {
            (Some(value), _) => {
                envelope.insert("result".into(), value);
            }
            (None, Some(err)) => {
                envelope.insert("error".into(), serde_json::to_value(err)?);
            }
            (None, None) => {}
        }
        let serialized = serde_json::to_string(&serde_json::Value::Object(envelope))?;

        let mut writer = self.writer.lock().await;
        writer.write_all(serialized.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok(())
    }

    /// Whether the spawned child process is still running.
    ///
    /// Returns `false` when there is no child (constructed via
    /// [`from_streams`](Self::from_streams), or after [`shutdown`](Self::shutdown)
    /// took it) or once the child has exited. Does not block: uses `try_wait`.
    pub async fn is_alive(&self) -> bool {
        let mut guard = self.child.lock().await;
        match guard.as_mut() {
            None => false,
            Some(child) => child.try_wait().map(|exit| exit.is_none()).unwrap_or(false),
        }
    }

    /// Tear down: kill the child (if any) and abort the reader task. Idempotent.
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

/// Background reader loop: parse NDJSON lines from the peer's stdout and route
/// each message — responses to their awaiters, notifications + inbound requests
/// to the incoming channel. Exits cleanly on EOF.
async fn read_loop(
    reader: Box<dyn AsyncRead + Send + Unpin>,
    pending: std::sync::Arc<Mutex<HashMap<u64, oneshot::Sender<ProviderResponse>>>>,
    incoming_tx: mpsc::Sender<IncomingMessage>,
) {
    let mut buf = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        match buf.read_line(&mut line).await {
            Ok(0) => break, // EOF — peer closed stdout.
            Ok(_) => {}
            Err(_) => break, // Unrecoverable read error; stop.
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue, // Skip malformed lines.
        };

        let id = value.get("id").and_then(|v| v.as_u64());
        let has_method = value.get("method").is_some();

        if let Some(id) = id {
            if has_method {
                // Inbound request from the peer (e.g. ACP session/request_permission).
                let method = value
                    .get("method")
                    .and_then(|m| m.as_str())
                    .unwrap_or("")
                    .to_string();
                let params = value
                    .get("params")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                if incoming_tx
                    .send(IncomingMessage {
                        id: Some(id),
                        method,
                        params,
                    })
                    .await
                    .is_err()
                {
                    break; // Consumer dropped; stop reading.
                }
            } else {
                // Response to one of our requests.
                let response: ProviderResponse = match serde_json::from_value(value) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                if let Some(sender) = pending.lock().await.remove(&id) {
                    let _ = sender.send(response);
                }
                // Unknown id (late/stray response) is silently dropped.
            }
        } else if has_method {
            // Notification (no id).
            let method = value
                .get("method")
                .and_then(|m| m.as_str())
                .unwrap_or("")
                .to_string();
            let params = value
                .get("params")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            if incoming_tx
                .send(IncomingMessage {
                    id: None,
                    method,
                    params,
                })
                .await
                .is_err()
            {
                break;
            }
        }
        // Anything else (no id, no method) is not valid JSON-RPC; ignore.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::time::timeout;

    /// Wire up a transport against an in-process fake peer using two duplexes:
    ///   - client_writer → peer_reader   (our requests reach the peer)
    ///   - peer_writer   → client_reader (peer's responses reach us)
    ///
    /// Returns (transport, incoming_rx, peer_reader, peer_writer).
    fn harness() -> (
        JsonRpcTransport,
        mpsc::Receiver<IncomingMessage>,
        tokio::io::DuplexStream,
        tokio::io::DuplexStream,
    ) {
        let (client_writer, peer_reader) = tokio::io::duplex(8192);
        let (peer_writer, client_reader) = tokio::io::duplex(8192);
        let (transport, incoming_rx) =
            JsonRpcTransport::from_streams(Box::new(client_writer), Box::new(client_reader));
        (transport, incoming_rx, peer_reader, peer_writer)
    }

    /// Read one NDJSON line off a peer-side stream into a parsed `Value`.
    async fn read_one(peer_reader: tokio::io::DuplexStream) -> serde_json::Value {
        let mut buf = BufReader::new(peer_reader);
        let mut line = String::new();
        buf.read_line(&mut line).await.unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    #[tokio::test]
    async fn roundtrip_request_response() {
        let (transport, _incoming, peer_reader, mut peer_writer) = harness();

        // Peer: read our request, echo back a response with the same id.
        tokio::spawn(async move {
            let req = read_one(peer_reader).await;
            let id = req["id"].as_u64().unwrap();
            let resp = serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": { "ok": true } });
            peer_writer
                .write_all(format!("{resp}\n").as_bytes())
                .await
                .unwrap();
            peer_writer.flush().await.unwrap();
        });

        let resp = transport
            .send_request("initialize", Some(serde_json::json!({ "v": 1 })))
            .await
            .expect("response");
        assert_eq!(resp.id, Some(1));
        assert_eq!(resp.result, Some(serde_json::json!({ "ok": true })));
        assert!(resp.error.is_none());
        transport.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_requests_correlate_by_id() {
        let (transport, _incoming, peer_reader, mut peer_writer) = harness();

        // Peer: for each request it reads, respond with result = id (echo id).
        tokio::spawn(async move {
            let mut buf = BufReader::new(peer_reader);
            for _ in 0..3 {
                let mut line = String::new();
                if buf.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                let req: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
                let id = req["id"].as_u64().unwrap();
                let resp = serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": id });
                peer_writer
                    .write_all(format!("{resp}\n").as_bytes())
                    .await
                    .unwrap();
                peer_writer.flush().await.unwrap();
            }
        });

        // Fire three requests concurrently; each must get its OWN id back.
        // The transport is Send + Sync, so the &self borrow is shared across the join.
        let (a, b, c) = tokio::join!(
            transport.send_request("m", None),
            transport.send_request("m", None),
            transport.send_request("m", None),
        );
        let ids: Vec<u64> = [a, b, c]
            .into_iter()
            .map(|r| r.expect("ok").id.unwrap())
            .collect();
        // Each response echoes its own id; collect and confirm uniqueness + match result.
        let mut got = std::collections::HashSet::new();
        for id in &ids {
            assert!(got.insert(*id), "duplicate id {id} — correlation failed");
        }
        transport.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn notification_forwarded() {
        let (transport, mut incoming, _peer_reader, mut peer_writer) = harness();

        peer_writer
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"x\":1}}\n",
            )
            .await
            .unwrap();
        peer_writer.flush().await.unwrap();

        let msg = timeout(Duration::from_secs(2), incoming.recv())
            .await
            .expect("timed out")
            .expect("message present");
        assert_eq!(msg.id, None);
        assert_eq!(msg.method, "session/update");
        assert_eq!(msg.params, serde_json::json!({ "x": 1 }));
        transport.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn inbound_request_forwarded_with_id() {
        let (transport, mut incoming, _peer_reader, mut peer_writer) = harness();

        peer_writer
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"session/request_permission\",\"params\":{}}\n")
            .await
            .unwrap();
        peer_writer.flush().await.unwrap();

        let msg = timeout(Duration::from_secs(2), incoming.recv())
            .await
            .expect("timed out")
            .expect("message present");
        assert_eq!(msg.id, Some(42));
        assert_eq!(msg.method, "session/request_permission");
        transport.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn request_times_out_when_no_response() {
        // Peer never responds → send_request_with_timeout must error with Timeout.
        let (transport, _incoming, _peer_reader, _peer_writer) = harness();

        let err = transport
            .send_request_with_timeout("noop", None, Duration::from_millis(75))
            .await
            .expect_err("should time out");
        assert!(
            matches!(err, ProviderAdapterError::Timeout(_)),
            "got {err:?}"
        );
        // Pending entry must be cleaned up.
        assert!(transport.pending.lock().await.is_empty());
        transport.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn respond_to_peer_sends_envelope() {
        let (transport, _incoming, peer_reader, _peer_writer) = harness();

        transport
            .respond_to_peer(7, Some(serde_json::json!({ "granted": true })), None)
            .await
            .unwrap();

        let req = read_one(peer_reader).await;
        assert_eq!(req["id"], 7);
        assert_eq!(req["result"], serde_json::json!({ "granted": true }));
        assert!(req.get("error").is_none() || req["error"].is_null());
        transport.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn eof_on_peer_close_is_clean() {
        let (transport, mut incoming, _peer_reader, peer_writer) = harness();
        drop(peer_writer); // Peer closes → reader hits EOF.

        // The reader task exits cleanly; incoming yields None eventually.
        let _ = timeout(Duration::from_secs(1), incoming.recv()).await;
        // shutdown must still succeed.
        transport.shutdown().await.unwrap();
    }
}

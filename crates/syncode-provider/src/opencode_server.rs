//! OpenCode-compatible local HTTP/SSE server client.
//!
//! Drives `opencode serve` / `kilo serve` — both OpenCode-compatible local
//! servers — over REST + Server-Sent Events. The whole client is parameterized
//! by an [`OpenCodeCompatibleCliSpec`], so Kilo is *one registration* that
//! differs from OpenCode only in spawn form and identity (binary, ready-line
//! prefix, auth username, default agent).
//!
//! Unlike the ACP/codex providers (long-lived JSON-RPC subprocesses over stdio),
//! an OpenCode-compatible server is a **local HTTP server**: spawn
//! `{binary} serve --hostname 127.0.0.1 --port <p>`, wait for the
//! `<prefix> on http://127.0.0.1:<p>` ready line, then talk to it over REST
//! (`POST /session`, `POST /session/{id}/prompt_async`, `POST /.../abort`,
//! `POST /permission/{id}/reply`) and consume the streaming `GET /event` SSE
//! channel.
//!
//! Reference (ground truth):
//! - `mcode/apps/server/src/provider/opencodeRuntime.ts` — spawn / ready-line /
//!   auth / SDK-client construction.
//! - `mcode/apps/server/src/provider/Layers/OpenCodeAdapter.ts` — SSE event →
//!   runtime-event mapping (`handleSubscribedEvent`).
//! - `@opencode-ai/sdk` v2 generated types — REST/SSE shapes.
//!
//! # Auth
//!
//! A locally-spawned server runs **without** auth (mcode never sends a password
//! to a server it started itself). [`OpenCodeAuth`] is therefore optional and
//! only applied (HTTP Basic via [`reqwest::RequestBuilder::basic_auth`]) when a
//! password is supplied — reserved for an externally-managed server. Every
//! request carries the `x-opencode-directory` header and a `?directory=<cwd>`
//! query parameter (the SDK's directory scoping).
//!
//! # Streaming
//!
//! `start_turn` opens the SSE channel *before* firing `prompt_async` so no early
//! delta is lost, then drains `data:` blocks until a terminal `session.status`
//! (`idle`) or `session.idle` event arrives (success), or `session.error`
//! (failure). Decoders are split into pure free functions (`parse_ready_url`,
//! `parse_sse_event`, `map_event`, `parse_events_from_buffer`) so the full
//! SSE→[`ProviderEvent`] mapping is unit-tested with fake SSE bytes — no binary
//! and no live HTTP server required.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Child;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::StreamExt;

use crate::trait_def::{PROVIDER_KILO, PROVIDER_OPENCODE};
use crate::trait_def::{ProviderAdapterError, ProviderEvent, UsageInfo};

/// Spec parameterizing an OpenCode-compatible CLI/server. Mirrors mcode's
/// `OpenCodeCompatibleCliSpec`: the only differences between OpenCode and Kilo
/// live here.
#[derive(Debug, Clone)]
pub struct OpenCodeCompatibleCliSpec {
    /// Canonical syncode provider id this spec builds (`opencode` / `kilo`).
    pub provider_id: &'static str,
    /// Binary to execute (resolved via `$PATH`).
    pub default_binary_path: &'static str,
    /// Human-readable name (for diagnostics).
    pub display_name: &'static str,
    /// stdout prefix announcing the server is ready, e.g.
    /// `"opencode server listening"`.
    pub server_ready_prefix: &'static str,
    /// HTTP Basic auth username (`opencode` / `kilo`) — used only when a
    /// password is supplied for an external server.
    pub server_auth_username: &'static str,
    /// Default agent id passed to `session.create` (`build` / `code`).
    pub default_agent: &'static str,
}

/// OpenCode (canonical) CLI/server spec.
pub const OPENCODE_CLI_SPEC: OpenCodeCompatibleCliSpec = OpenCodeCompatibleCliSpec {
    provider_id: PROVIDER_OPENCODE,
    default_binary_path: "opencode",
    display_name: "OpenCode",
    server_ready_prefix: "opencode server listening",
    server_auth_username: "opencode",
    default_agent: "build",
};

/// Kilo CLI/server spec — OpenCode-compatible, differing only in identity.
pub const KILO_CLI_SPEC: OpenCodeCompatibleCliSpec = OpenCodeCompatibleCliSpec {
    provider_id: PROVIDER_KILO,
    default_binary_path: "kilo",
    display_name: "Kilo",
    server_ready_prefix: "kilo server listening",
    server_auth_username: "kilo",
    default_agent: "code",
};

/// Optional HTTP Basic auth for the server. A locally-spawned server has
/// `password: None` (no auth). Construct with [`OpenCodeAuth::for_external`]
/// when driving an externally-managed server that requires a password.
#[derive(Debug, Clone, Default)]
pub struct OpenCodeAuth {
    pub username: String,
    pub password: Option<String>,
}

impl OpenCodeAuth {
    /// Local spawned server: no password → no `Authorization` header is sent.
    pub fn local(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: None,
        }
    }

    /// External server with a server-generated password.
    pub fn for_external(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: Some(password.into()),
        }
    }
}

/// Reference to a model in OpenCode's `{providerID, id/modelID}` shape.
#[derive(Debug, Clone)]
pub struct ModelRef {
    pub provider_id: String,
    pub id: String,
}

/// How a turn ended, decoded from the terminal SSE event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnStatus {
    /// `session.status` idle / `session.idle`.
    Completed,
    /// `session.error`.
    Failed,
    /// `POST /abort` surfaced as the terminal signal (best-effort).
    Cancelled,
}

/// Outcome of a completed [`OpenCodeServerClient::start_turn`] turn.
#[derive(Debug, Clone)]
pub struct TurnOutcome {
    /// Terminal status of the turn.
    pub status: TurnStatus,
    /// Concatenated assistant text deltas (best-effort; richer part data is in
    /// `raw`).
    pub output: String,
    /// Token usage snapshot (from `session.next.step.ended` / `message.updated`),
    /// if observed.
    pub usage: Option<UsageInfo>,
    /// Raw terminal SSE event payload, surfaced verbatim.
    pub raw: Value,
}

/// Default startup-wait timeout for the local server (mcode uses 20s).
const DEFAULT_SERVER_TIMEOUT_MS: u64 = 20_000;

/// A reference to a running OpenCode-compatible server plus the HTTP client to
/// drive it.
pub struct OpenCodeServerClient {
    base_url: String,
    directory: String,
    auth: Option<OpenCodeAuth>,
    http: reqwest::Client,
    child: Mutex<Option<Child>>,
}

impl OpenCodeServerClient {
    /// Spawn the local `{binary} serve` server described by `spec`, wait for its
    /// ready line, and return a client rooted at `cwd`. Uses the spec's default
    /// binary path and no extra args.
    pub async fn spawn(
        spec: &OpenCodeCompatibleCliSpec,
        cwd: &str,
    ) -> Result<Self, ProviderAdapterError> {
        Self::spawn_with(
            spec,
            spec.default_binary_path,
            &[],
            cwd,
            None,
            DEFAULT_SERVER_TIMEOUT_MS,
        )
        .await
    }

    /// Like [`spawn`](Self::spawn) but with an explicit `binary` (override the
    /// spec's default), trailing `extra_args` (appended after the standard
    /// `serve --hostname … --port …`), optional auth, and a startup timeout.
    pub async fn spawn_with(
        spec: &OpenCodeCompatibleCliSpec,
        binary: &str,
        extra_args: &[String],
        cwd: &str,
        auth: Option<OpenCodeAuth>,
        timeout_ms: u64,
    ) -> Result<Self, ProviderAdapterError> {
        let port = free_port().await?;
        let port_str = port.to_string();
        let mut cmd = tokio::process::Command::new(binary);
        cmd.args([
            "serve",
            "--hostname",
            "127.0.0.1",
            "--port",
            port_str.as_str(),
        ])
        .args(extra_args)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
        // Inherit the parent env so the server finds its config/credentials.
        let mut child = cmd.spawn().map_err(ProviderAdapterError::Io)?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ProviderAdapterError::Internal("server stdout not piped".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ProviderAdapterError::Internal("server stderr not piped".into()))?;

        // Drain stderr into a shared buffer for failure diagnostics. Detached:
        // runs until the child closes stderr (i.e. exits).
        let stderr_buf: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let stderr_buf_task = {
            let sb = Arc::clone(&stderr_buf);
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    let mut g = sb.lock().await;
                    g.push_str(&line);
                    g.push('\n');
                }
            })
        };

        // Read stdout until the ready line (or the child exits / we time out).
        let mut reader = BufReader::new(stdout).lines();
        let mut stdout_capture = String::new();
        let ready = async {
            loop {
                match reader.next_line().await {
                    Ok(Some(line)) => {
                        stdout_capture.push_str(&line);
                        stdout_capture.push('\n');
                        if let Some(url) = parse_ready_url(&line, spec.server_ready_prefix) {
                            return Ok(url);
                        }
                    }
                    Ok(None) => {
                        // Child exited before the ready line.
                        let stderr = stderr_buf.lock().await.clone();
                        return Err(ProviderAdapterError::ProcessExited(format!(
                            "{} server exited before startup completed\ncommand: {} serve --hostname 127.0.0.1 --port {}\nready prefix: {:?}\nstdout:\n{}\nstderr:\n{}",
                            spec.display_name,
                            binary,
                            port,
                            spec.server_ready_prefix,
                            stdout_capture.trim(),
                            stderr.trim(),
                        )));
                    }
                    Err(e) => {
                        return Err(ProviderAdapterError::Io(e));
                    }
                }
            }
        };

        let base_url = match tokio::time::timeout(Duration::from_millis(timeout_ms), ready).await {
            Ok(Ok(url)) => url,
            Ok(Err(e)) => {
                let _ = child.start_kill();
                stderr_buf_task.abort();
                return Err(e);
            }
            Err(_) => {
                // Timed out waiting for the ready line. Kill the server and
                // surface a timeout (the captured stdout/stderr is best-effort
                // and not carried by the Timeout variant).
                let _ = child.start_kill();
                stderr_buf_task.abort();
                return Err(ProviderAdapterError::Timeout(timeout_ms));
            }
        };
        // Server is up: the stderr drain task is intentionally detached — it
        // runs until the child closes stderr (i.e. exits). Dropping the handle
        // here would cancel it, so keep it alive by binding it for the remainder
        // of this function via the guard below.
        let _detached_drain = stderr_buf_task;

        // No per-request timeout: a turn's SSE stream is long-lived. Connection
        // establishment is bounded by the OS; a turn is bounded by the caller.
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| ProviderAdapterError::Internal(format!("http client: {e}")))?;

        tracing::info!(
            provider = spec.provider_id,
            base_url = %base_url,
            "OpenCode-compatible server ready",
        );

        Ok(Self {
            base_url,
            directory: cwd.to_string(),
            auth,
            http,
            child: Mutex::new(Some(child)),
        })
    }

    /// The server's base URL (parsed from the ready line).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Whether the spawned server process is still running.
    pub async fn is_alive(&self) -> bool {
        let mut guard = self.child.lock().await;
        match guard.as_mut() {
            None => false,
            Some(child) => child.try_wait().map(|exit| exit.is_none()).unwrap_or(false),
        }
    }

    /// Build a request to `path` with directory scoping + (optional) auth.
    fn build_request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.base_url, path);
        let mut rb = self
            .http
            .request(method, &url)
            .query(&[("directory", self.directory.as_str())])
            .header("x-opencode-directory", &self.directory);
        if let Some(auth) = &self.auth
            && let Some(pw) = &auth.password
        {
            rb = rb.basic_auth(&auth.username, Some(pw));
        }
        rb
    }

    /// `POST /session` — create a session rooted at the client's directory.
    /// Returns the server-assigned session id.
    pub async fn create_session(
        &self,
        title: &str,
        model: Option<&ModelRef>,
        agent: Option<&str>,
        full_access: bool,
    ) -> Result<String, ProviderAdapterError> {
        let permission = if full_access {
            vec![json!({ "permission": "*", "pattern": "*", "action": "allow" })]
        } else {
            // ask-based default; the adapter auto-approves `permission.asked`
            // mid-turn so a headless session never deadlocks.
            vec![
                json!({ "permission": "*", "pattern": "*", "action": "ask" }),
                json!({ "permission": "bash", "pattern": "*", "action": "ask" }),
                json!({ "permission": "edit", "pattern": "*", "action": "ask" }),
                json!({ "permission": "question", "pattern": "*", "action": "allow" }),
            ]
        };
        let mut body = json!({ "title": title, "permission": permission });
        if let Some(m) = model {
            body["model"] = json!({ "providerID": m.provider_id, "id": m.id });
        }
        if let Some(a) = agent {
            body["agent"] = json!(a);
        }
        let resp = self
            .build_request(reqwest::Method::POST, "/session")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderAdapterError::Internal(format!("session.create: {e}")))?;
        let resp = ensure_ok(resp, "session.create").await?;
        let val: Value = resp
            .json()
            .await
            .map_err(|e| ProviderAdapterError::Internal(format!("session.create body: {e}")))?;
        val.get("id")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .ok_or_else(|| {
                ProviderAdapterError::Internal(format!("session.create response missing id: {val}"))
            })
    }

    /// `POST /session/{id}/prompt_async` — fire a non-blocking turn. The turn's
    /// streamed output is consumed via the SSE channel (see [`start_turn`]).
    pub async fn prompt_async(
        &self,
        session_id: &str,
        parts: Vec<Value>,
        model: Option<&ModelRef>,
        agent: Option<&str>,
        variant: Option<&str>,
        system: Option<&str>,
    ) -> Result<(), ProviderAdapterError> {
        let mut body = json!({ "parts": parts });
        if let Some(m) = model {
            body["model"] = json!({ "providerID": m.provider_id, "modelID": m.id });
        }
        if let Some(a) = agent {
            body["agent"] = json!(a);
        }
        if let Some(v) = variant {
            body["variant"] = json!(v);
        }
        if let Some(s) = system {
            body["system"] = json!(s);
        }
        let path = format!("/session/{session_id}/prompt_async");
        let resp = self
            .build_request(reqwest::Method::POST, &path)
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderAdapterError::Internal(format!("session.prompt_async: {e}")))?;
        ensure_ok(resp, "session.prompt_async").await?;
        Ok(())
    }

    /// `POST /session/{id}/abort` — interrupt the in-flight turn.
    pub async fn abort(&self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let path = format!("/session/{session_id}/abort");
        let resp = self
            .build_request(reqwest::Method::POST, &path)
            .send()
            .await
            .map_err(|e| ProviderAdapterError::Internal(format!("session.abort: {e}")))?;
        ensure_ok(resp, "session.abort").await?;
        Ok(())
    }

    /// `POST /permission/{id}/reply` — answer a `permission.asked` request
    /// (`"once"` / `"always"` / `"reject"`). Used by the turn drain to
    /// auto-approve so a headless session never deadlocks.
    pub async fn reply_permission(
        &self,
        request_id: &str,
        reply: &str,
    ) -> Result<(), ProviderAdapterError> {
        let path = format!("/permission/{request_id}/reply");
        let body = json!({ "reply": reply });
        let resp = self
            .build_request(reqwest::Method::POST, &path)
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderAdapterError::Internal(format!("permission.reply: {e}")))?;
        ensure_ok(resp, "permission.reply").await?;
        Ok(())
    }

    /// Open the SSE event channel (`GET /event`) as a streaming response. The
    /// caller consumes [`reqwest::Response::bytes_stream`] and parses `data:`
    /// blocks (see [`parse_events_from_buffer`]).
    async fn open_event_stream(&self) -> Result<reqwest::Response, ProviderAdapterError> {
        let resp = self
            .build_request(reqwest::Method::GET, "/event")
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send()
            .await
            .map_err(|e| ProviderAdapterError::Internal(format!("event.subscribe: {e}")))?;
        ensure_ok(resp, "event.subscribe").await
    }

    /// Fire `prompt_async` and drain the SSE channel for `session_id` until the
    /// turn terminates (idle / error). Streamed deltas are forwarded to
    /// `event_tx` live; the returned [`TurnOutcome`] reflects the terminal
    /// event. Inbound `permission.asked` requests are auto-approved
    /// (`"once"`) so the agent never blocks mid-turn.
    #[allow(clippy::too_many_arguments)] // mirrors the prompt_async surface
    pub async fn start_turn(
        &self,
        session_id: &str,
        parts: Vec<Value>,
        model: Option<&ModelRef>,
        agent: Option<&str>,
        variant: Option<&str>,
        system: Option<&str>,
        event_tx: &mpsc::Sender<ProviderEvent>,
    ) -> Result<TurnOutcome, ProviderAdapterError> {
        // Open the SSE channel BEFORE the prompt so early deltas are not lost.
        let resp = self.open_event_stream().await?;
        let stream = resp.bytes_stream();
        tokio::pin!(stream);

        self.prompt_async(session_id, parts, model, agent, variant, system)
            .await?;

        let mut buf = String::new();
        let mut last_usage: Option<UsageInfo> = None;
        let mut output = String::new();

        loop {
            // Parse every complete SSE block currently buffered.
            let batch = parse_events_from_buffer(&buf, session_id, &mut last_usage);
            buf.drain(..batch.consumed);
            for ev in batch.events {
                if let ProviderEvent::Token { content, .. } = &ev {
                    output.push_str(content);
                }
                let _ = event_tx.send(ev).await;
            }
            for request_id in batch.permissions {
                let _ = self.reply_permission(&request_id, "once").await;
            }
            if let Some((status, raw)) = batch.terminal {
                let usage = last_usage.take();
                return Ok(TurnOutcome {
                    status,
                    output: std::mem::take(&mut output),
                    usage,
                    raw,
                });
            }

            // Need more bytes from the SSE stream.
            match stream.next().await {
                Some(Ok(chunk)) => {
                    // SSE producers may use CRLF; normalize to LF so the "\n\n"
                    // block boundary is reliable.
                    let text = String::from_utf8_lossy(&chunk).replace('\r', "");
                    buf.push_str(&text);
                }
                Some(Err(e)) => {
                    return Err(ProviderAdapterError::Internal(format!(
                        "event stream read error: {e}"
                    )));
                }
                None => {
                    return Err(ProviderAdapterError::ProcessExited(
                        "opencode event stream closed mid-turn".to_string(),
                    ));
                }
            }
        }
    }

    /// Tear down: kill the spawned server (if any). Idempotent.
    pub async fn shutdown(&self) -> Result<(), ProviderAdapterError> {
        if let Some(mut child) = self.child.lock().await.take() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SSE + payload decoders (pure; unit-tested directly without a server)
// ---------------------------------------------------------------------------

/// One parsed batch of SSE events: the decoded [`ProviderEvent`]s, any
/// `permission.asked` request ids to auto-reply, an optional terminal turn
/// status (+ the raw terminal event), and how many bytes of the source buffer
/// were consumed as complete blocks.
#[derive(Debug, Default)]
struct ParsedBatch {
    events: Vec<ProviderEvent>,
    permissions: Vec<String>,
    terminal: Option<(TurnStatus, Value)>,
    consumed: usize,
}

/// Parse all *complete* SSE blocks (separated by blank lines) out of `buf`.
/// Returns the decoded batch and leaves any trailing partial block in `buf`
/// (caller drains `consumed` bytes).
///
/// A block is complete when `buf` contains a `"\n\n"` boundary after it. Each
/// block's `data:` lines are concatenated and parsed as one JSON `Event` object
/// (`{ "type": <name>, "properties": { ... } }`).
fn parse_events_from_buffer(
    buf: &str,
    session_id: &str,
    usage: &mut Option<UsageInfo>,
) -> ParsedBatch {
    let mut batch = ParsedBatch::default();
    let mut pos = 0usize;
    loop {
        let rest = &buf[pos..];
        match rest.find("\n\n") {
            Some(end) => {
                let block = &rest[..end];
                pos += end + 2; // skip the blank-line separator
                let Some(event) = parse_sse_event(block) else {
                    continue;
                };
                // Scope session-scoped events to our session. Events that omit
                // `properties.sessionID` (e.g. a bare `session.error`) still
                // apply — they are only emitted for an active turn by design.
                let props_session = event
                    .get("properties")
                    .and_then(|p| p.get("sessionID"))
                    .and_then(|v| v.as_str());
                if let Some(sid) = props_session
                    && sid != session_id
                {
                    continue;
                }
                let outcome = map_event(&event, session_id, usage);
                batch.events.extend(outcome.events);
                batch.permissions.extend(outcome.permission);
                if let Some(status) = outcome.terminal {
                    batch.terminal = Some((status, event.clone()));
                    break;
                }
            }
            None => break,
        }
    }
    batch.consumed = pos;
    batch
}

/// Extract the `data:` payload from one SSE block and parse it as JSON.
/// Returns `None` for comment-only / empty blocks or unparseable data.
fn parse_sse_event(block: &str) -> Option<Value> {
    let mut data = String::new();
    for line in block.lines() {
        let line = line.strip_prefix('\u{feff}').unwrap_or(line);
        if let Some(rest) = line.strip_prefix("data:") {
            let rest = rest.strip_prefix(' ').unwrap_or(rest);
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest);
        }
        // `event:`, `id:`, `retry:` and `:` comment lines are ignored — the
        // OpenCode server carries the type inside the JSON `data` payload.
    }
    if data.is_empty() {
        return None;
    }
    serde_json::from_str(&data).ok()
}

/// How a single SSE event maps to adapter output.
struct EventOutcome {
    events: Vec<ProviderEvent>,
    permission: Option<String>,
    terminal: Option<TurnStatus>,
}

/// Map one decoded SSE event to [`ProviderEvent`]s / a permission id / a
/// terminal signal. Mirrors mcode `handleSubscribedEvent` (the recognized
/// subset; richer fidelity — todos, questions, diffs — is intentionally
/// deferred).
///
/// `usage` is updated in place when the event carries token usage.
fn map_event(event: &Value, session_id: &str, usage: &mut Option<UsageInfo>) -> EventOutcome {
    let ty = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let props = event.get("properties").unwrap_or(&Value::Null);
    let mut out = EventOutcome {
        events: Vec::new(),
        permission: None,
        terminal: None,
    };

    match ty {
        // ---- streamed text / reasoning deltas ----
        "message.part.delta" | "session.next.text.delta" | "session.next.reasoning.delta" => {
            if let Some(delta) = props.get("delta").and_then(|v| v.as_str())
                && let Some(token) = token(session_id, delta)
            {
                out.events.push(token);
            }
        }

        // ---- a full part snapshot (text / reasoning / tool) ----
        "message.part.updated" => out.events.extend(map_part_updated(props, session_id)),

        // ---- tool lifecycle (newer `session.next.*` event family) ----
        "session.next.tool.called" => {
            if let Some(tool) = props.get("tool").and_then(|v| v.as_str()) {
                out.events.push(ProviderEvent::ToolCall {
                    session_id: session_id.to_string(),
                    tool_name: tool.to_string(),
                    tool_input: json!({}),
                });
            }
        }
        "session.next.tool.progress" | "session.next.tool.success" | "session.next.tool.failed" => {
            if let Some(tool) = props.get("tool").and_then(|v| v.as_str()) {
                out.events.push(ProviderEvent::ToolResult {
                    session_id: session_id.to_string(),
                    tool_name: tool.to_string(),
                    result: tool_result_payload(props),
                });
            }
        }
        "session.next.shell.started" => {
            if let Some(cmd) = props.get("command").and_then(|v| v.as_str()) {
                out.events.push(ProviderEvent::ToolCall {
                    session_id: session_id.to_string(),
                    tool_name: format!("bash: {cmd}"),
                    tool_input: json!({ "command": cmd }),
                });
            }
        }
        "session.next.shell.ended" => {
            if let Some(cmd) = props.get("command").and_then(|v| v.as_str()) {
                out.events.push(ProviderEvent::ToolResult {
                    session_id: session_id.to_string(),
                    tool_name: format!("bash: {cmd}"),
                    result: tool_result_payload(props),
                });
            }
        }

        // ---- token usage snapshots ----
        "session.next.step.ended" => {
            if let Some(u) = parse_usage(props) {
                *usage = Some(u);
            }
        }
        "message.updated" => {
            if let Some(info) = props.get("info")
                && let Some(u) = parse_usage(info)
            {
                *usage = Some(u);
            }
        }

        // ---- permission (auto-approve so the turn never blocks) ----
        "permission.asked" => {
            if let Some(id) = props.get("id").and_then(|v| v.as_str()) {
                out.permission = Some(id.to_string());
            }
        }

        // ---- terminal: turn idle (success) ----
        "session.status" => {
            let idle = props
                .get("status")
                .and_then(|s| s.get("type"))
                .and_then(|v| v.as_str())
                == Some("idle");
            if idle {
                out.terminal = Some(TurnStatus::Completed);
            }
        }
        "session.idle" => {
            out.terminal = Some(TurnStatus::Completed);
        }

        // ---- terminal: session error (failure) ----
        "session.error" => {
            let message = props
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|v| v.as_str())
                .unwrap_or("opencode session error")
                .to_string();
            out.events.push(ProviderEvent::Error {
                session_id: session_id.to_string(),
                message,
                code: None,
            });
            out.terminal = Some(TurnStatus::Failed);
        }

        _ => {}
    }

    out
}

/// Map a `message.part.updated` event's `part` to events:
/// - text/reasoning part → [`ProviderEvent::Token`] (non-empty `text`);
/// - tool part → [`ProviderEvent::ToolCall`] (pending/running) or
///   [`ProviderEvent::ToolResult`] (completed/error).
fn map_part_updated(props: &Value, session_id: &str) -> Vec<ProviderEvent> {
    let Some(part) = props.get("part") else {
        return Vec::new();
    };
    let ty = part.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match ty {
        "text" | "reasoning" => {
            if let Some(content) = part.get("text").and_then(|v| v.as_str())
                && let Some(t) = token(session_id, content)
            {
                return vec![t];
            }
            Vec::new()
        }
        "tool" => {
            let tool = part
                .get("tool")
                .and_then(|v| v.as_str())
                .unwrap_or("tool")
                .to_string();
            let state = part.get("state").unwrap_or(&Value::Null);
            let status = state.get("status").and_then(|v| v.as_str()).unwrap_or("");
            match status {
                "completed" => vec![ProviderEvent::ToolResult {
                    session_id: session_id.to_string(),
                    tool_name: tool,
                    result: state.get("output").cloned().unwrap_or(Value::Null),
                }],
                "error" => vec![ProviderEvent::ToolResult {
                    session_id: session_id.to_string(),
                    tool_name: tool,
                    result: json!({ "error": state.get("error").cloned().unwrap_or(Value::Null) }),
                }],
                _ => vec![ProviderEvent::ToolCall {
                    session_id: session_id.to_string(),
                    tool_name: tool,
                    tool_input: state.get("input").cloned().unwrap_or(Value::Null),
                }],
            }
        }
        _ => Vec::new(),
    }
}

/// Build a `ToolResult` payload from a tool/shell-ended event: prefer `error`,
/// then `output`, else an empty object.
fn tool_result_payload(props: &Value) -> Value {
    if let Some(err) = props.get("error").and_then(|v| v.as_str()) {
        return json!({ "error": err });
    }
    if let Some(output) = props.get("output") {
        return json!({ "output": output });
    }
    json!({})
}

/// Parse token usage from a `tokens` object (`{ input, output, total? }`).
/// Returns `None` when no meaningful counts are present.
fn parse_usage(value: &Value) -> Option<UsageInfo> {
    let tokens = value.get("tokens").or_else(|| value.get("tokenUsage"))?;
    let input = num_field(tokens, "input");
    let output = num_field(tokens, "output");
    let total = num_field(tokens, "total");
    if input.is_none() && output.is_none() && total.is_none() {
        return None;
    }
    let input_tokens = input.unwrap_or(0);
    let output_tokens = output.unwrap_or(0);
    let total_tokens = total.unwrap_or(input_tokens + output_tokens);
    Some(UsageInfo {
        input_tokens,
        output_tokens,
        total_tokens,
    })
}

/// Read an optional `u32` field from a JSON object.
fn num_field(obj: &Value, key: &str) -> Option<u32> {
    obj.get(key).and_then(|v| v.as_u64()).map(|n| n as u32)
}

/// Build a non-empty [`ProviderEvent::Token`].
fn token(session_id: &str, content: &str) -> Option<ProviderEvent> {
    if content.is_empty() {
        return None;
    }
    Some(ProviderEvent::Token {
        session_id: session_id.to_string(),
        content: content.to_string(),
    })
}

/// Parse the server's base URL out of a ready line like
/// `"opencode server listening on http://127.0.0.1:41239"`.
///
/// Matches mcode's `/on\s+(https?:\/\/[^\s]+)/` after the ready prefix.
pub fn parse_ready_url(line: &str, prefix: &str) -> Option<String> {
    let line = line.trim();
    if !line.starts_with(prefix) {
        return None;
    }
    let probe = "on http";
    let idx = line.find(probe)?;
    // `probe` is 7 chars; the URL begins immediately after "on " (idx + 3).
    let url_start = idx + 3;
    let rest = &line[url_start..];
    let url = rest.split_whitespace().next()?;
    if url.starts_with("http://") || url.starts_with("https://") {
        Some(url.to_string())
    } else {
        None
    }
}

/// Bind an ephemeral localhost port and release it so the spawned server can
/// claim it. (Tiny TOCTOU window — acceptable for a local, single-tenant
/// server; the ready-line wait surfaces any bind failure.)
async fn free_port() -> Result<u16, ProviderAdapterError> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(ProviderAdapterError::Io)?;
    let port = listener
        .local_addr()
        .map_err(ProviderAdapterError::Io)?
        .port();
    drop(listener);
    Ok(port)
}

/// Surface a non-2xx HTTP response as an [`ProviderAdapterError::RpcError`]
/// carrying the status code + body excerpt.
async fn ensure_ok(
    resp: reqwest::Response,
    label: &str,
) -> Result<reqwest::Response, ProviderAdapterError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let body = resp.text().await.unwrap_or_default();
    Err(ProviderAdapterError::RpcError {
        code: status.as_u16() as i64,
        message: format!("{label}: {status} {body}"),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- ready-line parsing ----

    #[test]
    fn parse_ready_url_opencode() {
        assert_eq!(
            parse_ready_url(
                "opencode server listening on http://127.0.0.1:41239",
                "opencode server listening"
            )
            .as_deref(),
            Some("http://127.0.0.1:41239"),
        );
    }

    #[test]
    fn parse_ready_url_kilo_https() {
        assert_eq!(
            parse_ready_url(
                "kilo server listening on https://127.0.0.1:9000",
                "kilo server listening"
            )
            .as_deref(),
            Some("https://127.0.0.1:9000"),
        );
    }

    #[test]
    fn parse_ready_url_rejects_wrong_prefix_and_no_url() {
        assert!(
            parse_ready_url("opencode server listening", "opencode server listening").is_none()
        );
        assert!(parse_ready_url("some other line", "opencode server listening").is_none());
        assert!(
            parse_ready_url(
                "opencode server listening on ftp://x",
                "opencode server listening"
            )
            .is_none()
        );
    }

    // ---- SSE block + event parsing ----

    #[test]
    fn parse_sse_event_extracts_data_json() {
        let block = "data: {\"type\":\"session.idle\",\"properties\":{\"sessionID\":\"s1\"}}";
        let ev = parse_sse_event(block).unwrap();
        assert_eq!(ev["type"], "session.idle");
        assert_eq!(ev["properties"]["sessionID"], "s1");
    }

    #[test]
    fn parse_sse_event_concatenates_multiline_data() {
        let block = "data: {\"type\":\"x\",\ndata: \"properties\":{}}";
        let ev = parse_sse_event(block).unwrap();
        assert_eq!(ev["type"], "x");
    }

    #[test]
    fn parse_sse_event_ignores_event_id_retry_comments() {
        let block = ": a comment\nevent: ping\nid: 5\nretry: 1000\ndata: {\"type\":\"t\"}";
        let ev = parse_sse_event(block).unwrap();
        assert_eq!(ev["type"], "t");
    }

    #[test]
    fn parse_sse_event_none_for_empty_or_bad() {
        assert!(parse_sse_event("").is_none());
        assert!(parse_sse_event(": comment only").is_none());
        assert!(parse_sse_event("data: not-json").is_none());
    }

    // ---- single-event mapping (map_event) ----

    fn props(obj: serde_json::Value) -> Value {
        json!({ "type": "__test__", "properties": obj })
    }

    fn map(
        props_obj: Value,
        ty: &str,
        session_id: &str,
        usage: &mut Option<UsageInfo>,
    ) -> EventOutcome {
        let event = json!({ "type": ty, "properties": props_obj });
        map_event(&event, session_id, usage)
    }

    #[test]
    fn map_text_delta_to_token() {
        let mut usage = None;
        let out = map(
            json!({ "delta": "Hello" }),
            "message.part.delta",
            "s1",
            &mut usage,
        );
        assert_eq!(out.events.len(), 1);
        assert!(
            matches!(&out.events[0], ProviderEvent::Token { content, .. } if content == "Hello")
        );
        assert!(out.terminal.is_none());
    }

    #[test]
    fn map_empty_delta_emits_nothing() {
        let mut usage = None;
        let out = map(
            json!({ "delta": "" }),
            "session.next.text.delta",
            "s1",
            &mut usage,
        );
        assert!(out.events.is_empty());
    }

    #[test]
    fn map_session_idle_is_terminal_completed() {
        let mut usage = None;
        let out = map(
            json!({ "sessionID": "s1" }),
            "session.idle",
            "s1",
            &mut usage,
        );
        assert_eq!(out.terminal, Some(TurnStatus::Completed));
    }

    #[test]
    fn map_session_status_idle_is_terminal() {
        let mut usage = None;
        let out = map(
            json!({ "sessionID": "s1", "status": { "type": "idle" } }),
            "session.status",
            "s1",
            &mut usage,
        );
        assert_eq!(out.terminal, Some(TurnStatus::Completed));
    }

    #[test]
    fn map_session_status_busy_is_not_terminal() {
        let mut usage = None;
        let out = map(
            json!({ "sessionID": "s1", "status": { "type": "busy" } }),
            "session.status",
            "s1",
            &mut usage,
        );
        assert!(out.terminal.is_none());
    }

    #[test]
    fn map_session_error_is_terminal_failed_with_error_event() {
        let mut usage = None;
        let out = map(
            json!({ "sessionID": "s1", "error": { "message": "boom" } }),
            "session.error",
            "s1",
            &mut usage,
        );
        assert_eq!(out.terminal, Some(TurnStatus::Failed));
        assert_eq!(out.events.len(), 1);
        assert!(
            matches!(&out.events[0], ProviderEvent::Error { message, .. } if message == "boom")
        );
    }

    #[test]
    fn map_permission_asked_yields_request_id() {
        let mut usage = None;
        let out = map(
            json!({ "sessionID": "s1", "id": "req-7" }),
            "permission.asked",
            "s1",
            &mut usage,
        );
        assert_eq!(out.permission.as_deref(), Some("req-7"));
        assert!(out.events.is_empty());
    }

    #[test]
    fn map_tool_called_and_ended() {
        let mut usage = None;
        let call = map(
            json!({ "sessionID": "s1", "tool": "edit_file" }),
            "session.next.tool.called",
            "s1",
            &mut usage,
        );
        assert!(
            matches!(&call.events[0], ProviderEvent::ToolCall { tool_name, .. } if tool_name == "edit_file")
        );

        let ended = map(
            json!({ "sessionID": "s1", "tool": "edit_file", "output": "ok" }),
            "session.next.tool.success",
            "s1",
            &mut usage,
        );
        assert!(
            matches!(&ended.events[0], ProviderEvent::ToolResult { result, .. } if result["output"] == "ok")
        );

        let failed = map(
            json!({ "sessionID": "s1", "tool": "edit_file", "error": "denied" }),
            "session.next.tool.failed",
            "s1",
            &mut usage,
        );
        assert!(
            matches!(&failed.events[0], ProviderEvent::ToolResult { result, .. } if result["error"] == "denied")
        );
    }

    #[test]
    fn map_step_ended_updates_usage() {
        let mut usage = None;
        let out = map(
            json!({ "sessionID": "s1", "tokens": { "input": 30, "output": 20 } }),
            "session.next.step.ended",
            "s1",
            &mut usage,
        );
        assert!(out.events.is_empty());
        let u = usage.expect("usage captured");
        assert_eq!(
            (u.input_tokens, u.output_tokens, u.total_tokens),
            (30, 20, 50)
        );
    }

    // ---- part-updated mapping ----

    #[test]
    fn map_part_updated_text_token() {
        // map_part_updated of a text part returns exactly one token.
        let got = map_part_updated(&json!({ "part": { "type": "text", "text": "hi" } }), "s1");
        assert_eq!(got.len(), 1);
        assert!(matches!(&got[0], ProviderEvent::Token { content, .. } if content == "hi"));
    }

    #[test]
    fn map_part_updated_tool_states() {
        // pending → ToolCall
        let call = map_part_updated(
            &json!({ "part": { "type": "tool", "tool": "bash", "state": { "status": "running", "input": {"command":"ls"} } } }),
            "s1",
        );
        assert!(
            matches!(&call[0], ProviderEvent::ToolCall { tool_name, tool_input, .. } if tool_name == "bash" && tool_input["command"] == "ls")
        );

        // completed → ToolResult(output)
        let done = map_part_updated(
            &json!({ "part": { "type": "tool", "tool": "bash", "state": { "status": "completed", "output": "a\nb" } } }),
            "s1",
        );
        assert!(matches!(&done[0], ProviderEvent::ToolResult { result, .. } if result == "a\nb"));

        // error → ToolResult({error})
        let err = map_part_updated(
            &json!({ "part": { "type": "tool", "tool": "bash", "state": { "status": "error", "error": "nope" } } }),
            "s1",
        );
        assert!(
            matches!(&err[0], ProviderEvent::ToolResult { result, .. } if result["error"] == "nope")
        );
    }

    // ---- buffer drain (parse_events_from_buffer) ----

    #[test]
    fn drain_emits_tokens_then_completes_on_idle() {
        let sse = concat!(
            "data: {\"type\":\"message.part.delta\",\"properties\":{\"sessionID\":\"s1\",\"delta\":\"Hello \"}}\n\n",
            "data: {\"type\":\"session.next.text.delta\",\"properties\":{\"sessionID\":\"s1\",\"delta\":\"world\"}}\n\n",
            "data: {\"type\":\"session.idle\",\"properties\":{\"sessionID\":\"s1\"}}\n\n",
        );
        let mut usage = None;
        let batch = parse_events_from_buffer(sse, "s1", &mut usage);
        assert_eq!(batch.consumed, sse.len());
        assert_eq!(batch.events.len(), 2);
        assert!(
            matches!(&batch.events[0], ProviderEvent::Token { content, .. } if content == "Hello ")
        );
        assert!(
            matches!(&batch.events[1], ProviderEvent::Token { content, .. } if content == "world")
        );
        assert_eq!(batch.terminal.unwrap().0, TurnStatus::Completed);
    }

    #[test]
    fn drain_auto_approves_permission_then_continues() {
        let sse = concat!(
            "data: {\"type\":\"permission.asked\",\"properties\":{\"sessionID\":\"s1\",\"id\":\"req-1\"}}\n\n",
            "data: {\"type\":\"session.idle\",\"properties\":{\"sessionID\":\"s1\"}}\n\n",
        );
        let mut usage = None;
        let batch = parse_events_from_buffer(sse, "s1", &mut usage);
        assert_eq!(batch.permissions, vec!["req-1".to_string()]);
        assert_eq!(batch.terminal.unwrap().0, TurnStatus::Completed);
    }

    #[test]
    fn drain_skips_other_session_events() {
        let sse = concat!(
            "data: {\"type\":\"message.part.delta\",\"properties\":{\"sessionID\":\"other\",\"delta\":\"not mine\"}}\n\n",
            "data: {\"type\":\"session.idle\",\"properties\":{\"sessionID\":\"s1\"}}\n\n",
        );
        let mut usage = None;
        let batch = parse_events_from_buffer(sse, "s1", &mut usage);
        assert!(
            batch.events.is_empty(),
            "other-session delta must be filtered: {:?}",
            batch.events
        );
        assert_eq!(batch.terminal.unwrap().0, TurnStatus::Completed);
    }

    #[test]
    fn drain_keeps_partial_trailing_block_unconsumed() {
        // Two complete blocks + a partial (no trailing blank line).
        let sse = "data: {\"type\":\"session.idle\",\"properties\":{\"sessionID\":\"s1\"}}\n\ndata: {\"type\":\"message.part.delta\",\"properties\":{\"delta\":\"x\"}}";
        let mut usage = None;
        let batch = parse_events_from_buffer(sse, "s1", &mut usage);
        assert!(
            batch.consumed < sse.len(),
            "partial block must remain buffered"
        );
        assert_eq!(batch.terminal.unwrap().0, TurnStatus::Completed);
        assert!(batch.events.is_empty());
    }

    #[test]
    fn drain_crlf_line_endings_parsed() {
        let sse = "data: {\"type\":\"session.idle\",\"properties\":{\"sessionID\":\"s1\"}}\r\n\r\n";
        // The CRLF is normalized by the caller (start_turn) before draining;
        // emulate that here so the boundary is LF.
        let normalized = sse.replace("\r\n", "\n");
        let mut usage = None;
        let batch = parse_events_from_buffer(&normalized, "s1", &mut usage);
        assert_eq!(batch.terminal.unwrap().0, TurnStatus::Completed);
    }

    // ---- specs / identity ----

    #[test]
    fn opencode_spec_identity() {
        assert_eq!(OPENCODE_CLI_SPEC.provider_id, PROVIDER_OPENCODE);
        assert_eq!(OPENCODE_CLI_SPEC.default_binary_path, "opencode");
        assert_eq!(
            OPENCODE_CLI_SPEC.server_ready_prefix,
            "opencode server listening"
        );
        assert_eq!(OPENCODE_CLI_SPEC.server_auth_username, "opencode");
    }

    #[test]
    fn kilo_spec_identity() {
        assert_eq!(KILO_CLI_SPEC.provider_id, PROVIDER_KILO);
        assert_eq!(KILO_CLI_SPEC.default_binary_path, "kilo");
        assert_eq!(KILO_CLI_SPEC.server_ready_prefix, "kilo server listening");
        assert_eq!(KILO_CLI_SPEC.default_agent, "code");
    }

    // allow the `props` helper to compile (keeps the test module self-contained
    // and documents the shape the mapping expects).
    #[test]
    #[allow(dead_code)]
    fn props_helper_shape_is_valid_event() {
        let _ = props(json!({ "sessionID": "s1" }));
    }
}

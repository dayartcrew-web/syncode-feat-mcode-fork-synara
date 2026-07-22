//! OpenCode adapter — real `opencode serve` provider (HTTP + SSE).
//!
//! Wraps an [`OpenCodeServerClient`] behind the [`ProviderAdapter`] trait so the
//! OpenCode CLI (driven via its local `serve` HTTP/SSE server) plugs into
//! syncode's provider registry like any other adapter. The transport client and
//! the full SSE→[`ProviderEvent`] decoding live in [`crate::opencode_server`];
//! this module owns OpenCode's CLI spec and the trait wiring.
//!
//! Unlike the ACP/codex providers (long-lived JSON-RPC subprocesses over stdio),
//! OpenCode is a **local HTTP server**: `spawn` starts `opencode serve`, then
//! the adapter talks REST (`POST /session`, `POST /session/{id}/prompt_async`,
//! `POST /session/{id}/abort`) and consumes the streaming `GET /event` SSE
//! channel. Kilo speaks the same protocol and is wired by the near-identical
//! [`crate::adapters::kilo`] adapter with a different [`OpenCodeCompatibleCliSpec`].
//!
//! Lifecycle mapping (OpenCode session/turn model → trait):
//!
//! | trait method     | OpenCode operation                                  |
//! |------------------|-----------------------------------------------------|
//! | `spawn`          | launch `opencode serve` + wait for ready line       |
//! | `start_session`  | `POST /session` rooted at `ctx.working_dir` → id    |
//! | `send_request`   | `prompt_async` + drain SSE → streamed events + idle |
//! | `interrupt`      | `POST /session/{id}/abort`                          |
//! | `event_stream`   | subscribe to the broadcast event bus                |
//! | `health_check`   | spawned-server liveness                             |
//! | `shutdown`       | kill the spawned server                             |
//!
//! # Streaming bridge
//!
//! The trait is request/response on the surface, but an OpenCode turn streams
//! SSE deltas *while* the `prompt_async` turn is in flight and only ends on a
//! terminal `session.idle` / `session.status` (idle) event. `send_request`
//! therefore runs the turn under a short-lived `mpsc`→`broadcast` forwarder:
//! each SSE-derived [`ProviderEvent`] is pushed onto the shared broadcast bus
//! live, so any [`ProviderAdapter::event_stream`] subscriber observes tokens /
//! tool calls in real time, then a terminal [`ProviderEvent::Completed`] once
//! the turn's idle event arrives (or an [`ProviderEvent::Error`] on
//! `session.error`, which the SSE decoder emits itself).
//!
//! # Permission policy
//!
//! By default `OpenCodeConfig.full_auto` creates the session with a blanket
//! `*/* → allow` permission rule, and the SSE drain auto-approves any
//! `permission.asked` request mid-turn (`"once"`), so a headless adapter never
//! deadlocks on the first permission prompt.
//!
//! # Defensive one-shot fallback
//!
//! The serve path is **primary**: auth is auto-discovered from the inherited
//! env ([`OpenCodeAuth::from_env`]), so a locally-spawned server matches its
//! own `OPENCODE_SERVER_PASSWORD` and does not 401. The one-shot
//! `opencode run --format json` subprocess remains as a defensive fallback for
//! when the serve client is unavailable or a turn errors for any other reason
//! (binary missing, no ready line, mid-flight server crash). The fallback is
//! armed in `spawn` (if the server won't start) and re-armed on the first
//! failing turn, so streaming/interrupt/health degrade gracefully to the
//! proven one-shot path instead of failing the turn. See
//! [`OpenCodeAdapter::one_shot_fallback`] and [`run_opencode_turn`].

#![allow(clippy::let_underscore_future)]
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};

use super::super::trait_def::*;
use crate::bin_resolver::resolve_binary;
use crate::opencode_server::{
    ModelRef, OPENCODE_CLI_SPEC, OpenCodeAuth, OpenCodeCompatibleCliSpec, OpenCodeServerClient,
    TurnStatus,
};

/// Startup-wait timeout for the local `opencode serve` server (mcode uses 20s).
const SERVER_TIMEOUT_MS: u64 = 20_000;

/// OpenCode-specific configuration.
#[derive(Debug, Clone)]
pub struct OpenCodeConfig {
    /// Path to the `opencode` CLI binary (default `"opencode"`).
    pub bin_path: String,
    /// Extra args appended after `serve --hostname 127.0.0.1 --port <p>`
    /// (default empty).
    pub extra_args: Vec<String>,
    /// Full-auto mode: create the session with a blanket `*/* → allow`
    /// permission rule (and auto-approve any `permission.asked` mid-turn).
    pub full_auto: bool,
    /// Override the default agent id (`OPENCODE_CLI_SPEC.default_agent` = `build`).
    pub agent: Option<String>,
    /// Default model (`<providerID>/<modelID>`, e.g. `anthropic/claude-sonnet-4-5`).
    /// Empty → let the server pick its configured default.
    pub model: String,
}

impl Default for OpenCodeConfig {
    fn default() -> Self {
        Self {
            bin_path: "opencode".to_string(),
            extra_args: Vec::new(),
            full_auto: true,
            agent: None,
            model: String::new(),
        }
    }
}

/// The OpenCode provider adapter.
pub struct OpenCodeAdapter {
    config: Option<ProviderConfig>,
    oc_config: OpenCodeConfig,
    spec: &'static OpenCodeCompatibleCliSpec,
    client: Mutex<Option<OpenCodeServerClient>>,
    status: AtomicU64,
    spawned: AtomicBool,
    /// Server-assigned session id of the most recently opened session (our id).
    current_session: Mutex<Option<String>>,
    /// System prompt recorded at `start_session`, replayed on each turn.
    system_prompt: Mutex<Option<String>>,
    event_tx: broadcast::Sender<ProviderEvent>,
    /// Armed when `opencode serve` is unavailable or a serve turn errored
    /// (e.g. 401). While set, [`send_request`](ProviderAdapter::send_request)
    /// drives a one-shot `opencode run` subprocess per turn instead of the
    /// serve SSE path. See the module-level "Defensive one-shot fallback" note.
    one_shot_fallback: AtomicBool,
}

impl Default for OpenCodeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenCodeAdapter {
    /// Create a new OpenCode adapter with default settings.
    pub fn new() -> Self {
        Self::with_opencode_config(OpenCodeConfig::default())
    }

    /// Create a new OpenCode adapter with custom opencode-specific config.
    pub fn with_opencode_config(oc_config: OpenCodeConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            oc_config,
            spec: &OPENCODE_CLI_SPEC,
            client: Mutex::new(None),
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            spawned: AtomicBool::new(false),
            current_session: Mutex::new(None),
            system_prompt: Mutex::new(None),
            event_tx,
            one_shot_fallback: AtomicBool::new(false),
        }
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
    }

    /// Resolve the agent id: an explicit `OpenCodeConfig.agent` wins, else the
    /// spec default (`build`).
    fn agent(&self) -> &str {
        self.oc_config
            .agent
            .as_deref()
            .unwrap_or(self.spec.default_agent)
    }

    /// Resolve the model for a turn: an explicit `params.model` wins, else the
    /// spawn-time `ProviderConfig.model`, else `OpenCodeConfig.model`. Returns
    /// `None` when empty (the server then picks its configured default).
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
            .or_else(|| (!self.oc_config.model.is_empty()).then(|| self.oc_config.model.clone()))?;
        model_ref(&m)
    }

    /// Model resolved at `start_session` (no per-request override there).
    fn spawn_model_ref(&self) -> Option<ModelRef> {
        let m = self
            .config
            .as_ref()
            .map(|c| c.model.clone())
            .filter(|m| !m.is_empty())
            .or_else(|| (!self.oc_config.model.is_empty()).then(|| self.oc_config.model.clone()))?;
        model_ref(&m)
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

    /// Build the OpenCode `parts` array for a turn from a request's params.
    ///
    /// Prefers `params.input` (the orchestrator's `StartTurn` convention); falls
    /// back to a textual rendering of the params object so a turn is never
    /// silently empty. The result is always a single `text` part.
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

    /// Defensive one-shot fallback turn: spawn a fresh `opencode run
    /// --format json` subprocess for a single turn, parse its NDJSON stream via
    /// [`run_opencode_turn`], and map the outcome to a [`ProviderEvent`]/response.
    /// Used when `opencode serve` is unavailable or a serve turn errored (e.g.
    /// 401). Emits `Completed`/`Error` onto the broadcast bus and returns the
    /// JSON-RPC response. `fwd_tx` is borrowed from the caller's forwarder so
    /// streamed tokens/tool events reach `event_stream` subscribers live.
    async fn one_shot_turn(
        &self,
        request_id: u64,
        params: &Option<Value>,
        session_id: &str,
        model: Option<&ModelRef>,
        system: &Option<String>,
        fwd_tx: &mpsc::Sender<ProviderEvent>,
    ) -> Result<ProviderResponse, ProviderAdapterError> {
        let parts = Self::turn_input(params);
        let prompt_text = parts
            .first()
            .and_then(|p| p.get("text"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let prompt = match system {
            Some(s) => format!("{s}\n\n{prompt_text}"),
            None => prompt_text,
        };
        let bin = resolve_binary(&self.oc_config.bin_path);
        let model_str = model
            .map(|m| format!("{}/{}", m.provider_id, m.id))
            .unwrap_or_else(|| "default".to_string());
        let working_dir = self
            .config
            .as_ref()
            .and_then(|c| c.extra.get("cwd"))
            .and_then(|v| v.as_str())
            .unwrap_or(".");
        let mut cmd = Command::new(&bin);
        cmd.args(["run", "-m", &model_str, "--format", "json", &prompt])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(working_dir)
            .kill_on_drop(true);
        for a in &self.oc_config.extra_args {
            cmd.arg(a);
        }
        // Inherit parent env (HOME/PATH must reach the process so auth.json is
        // found — tokio Command inherits env by default; do NOT env_clear).
        crate::subprocess::hide_console_window(&mut cmd);

        let mut child = cmd.spawn().map_err(|e| {
            ProviderAdapterError::ProcessExited(format!(
                "failed to spawn `opencode run` ({bin}): {e}"
            ))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            ProviderAdapterError::ProcessExited(
                "opencode subprocess stdout not captured".to_string(),
            )
        })?;
        let stderr_rx = child.stderr.take().map(|mut err| {
            let (tx, rx) = oneshot::channel::<String>();
            tokio::spawn(async move {
                let mut buf = String::new();
                drop(err.read_to_string(&mut buf).await);
                let _ = tx.send(buf);
            });
            rx
        });

        let outcome = run_opencode_turn(BufReader::new(stdout), session_id, fwd_tx).await;
        _ = child.wait().await; // reap
        let stderr_buf = match stderr_rx {
            Some(rx) => rx.await.unwrap_or_default(),
            None => String::new(),
        };

        match outcome {
            Ok(oc) => {
                let _ = self.event_tx.send(ProviderEvent::Completed {
                    session_id: session_id.to_string(),
                    output: oc.output.clone(),
                    usage: oc.usage.clone(),
                });
                self.set_status(ProviderStatus::Idle);
                Ok(ProviderResponse {
                    jsonrpc: "2.0".to_string(),
                    id: Some(request_id),
                    result: Some(json!({ "output": oc.output, "usage": oc.usage })),
                    error: None,
                })
            }
            Err(e) => {
                let message = if stderr_buf.trim().is_empty() {
                    e.to_string()
                } else {
                    format!(
                        "{e}\n--- stderr ---\n{}",
                        &stderr_buf[..stderr_buf.len().min(2048)]
                    )
                };
                let _ = self.event_tx.send(ProviderEvent::Error {
                    session_id: session_id.to_string(),
                    message: message.clone(),
                    code: None,
                });
                self.set_status(ProviderStatus::Idle);
                Ok(ProviderResponse {
                    jsonrpc: "2.0".to_string(),
                    id: Some(request_id),
                    result: None,
                    error: Some(ProviderError {
                        code: -32000,
                        message,
                        data: None,
                    }),
                })
            }
        }
    }

    /// Record the opened session id, stash the system prompt for replay, flip
    /// to `Busy`, and emit `Started`. Shared by the serve and one-shot session
    /// paths.
    async fn finalize_session(
        &self,
        ctx: SessionContext,
        session_id: String,
    ) -> Result<String, ProviderAdapterError> {
        *self.current_session.lock().await = Some(session_id.clone());
        *self.system_prompt.lock().await = ctx.system_prompt.clone();
        self.set_status(ProviderStatus::Busy);
        let _ = self.event_tx.send(ProviderEvent::Started {
            session_id: session_id.clone(),
        });
        tracing::info!(
            provider = PROVIDER_OPENCODE,
            opencode_session_id = %session_id,
            syncode_thread_id = %ctx.thread_id.as_str(),
            turn_id = %ctx.turn_id.as_str(),
            "OpenCode session opened",
        );
        Ok(session_id)
    }
}

/// Parse an OpenCode model string into a `{providerID, id}` [`ModelRef`].
///
/// Accepts `<providerID>/<modelID>` (e.g. `anthropic/claude-sonnet-4-5`); any
/// other form is rejected (`None`) so the caller lets the server pick its
/// default rather than sending a malformed ref.
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

struct OpenCodeTurnOutcome {
    output: String,
    usage: Option<UsageInfo>,
}

/// Parse the NDJSON event stream from `opencode run --format json` into
/// ProviderEvents. Used by the defensive one-shot fallback (see
/// [`OpenCodeAdapter`] module docs) when `opencode serve` is unavailable or
/// returns 401. Returns `Ok(outcome)` on clean stream end (EOF or
/// `session.ended`), `Err` on an error event (caller synthesizes an Error).
async fn run_opencode_turn<R>(
    mut reader: R,
    session_id: &str,
    fwd_tx: &mpsc::Sender<ProviderEvent>,
) -> Result<OpenCodeTurnOutcome, ProviderAdapterError>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut line = String::new();
    let mut output = String::new();
    let mut usage: Option<UsageInfo> = None;
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break; // EOF → synthesize Completed
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!(
                    provider = PROVIDER_OPENCODE,
                    line = %trimmed,
                    "skipping unparseable opencode NDJSON line"
                );
                continue;
            }
        };
        let ty = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match ty {
            "error" => {
                let err_msg = msg
                    .pointer("/error/data/message")
                    .or_else(|| msg.pointer("/error/message"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("opencode error");
                let _ = fwd_tx.send(ProviderEvent::Error {
                    session_id: session_id.to_string(),
                    message: err_msg.to_string(),
                    code: None,
                });
                return Err(ProviderAdapterError::ProcessExited(err_msg.to_string()));
            }
            "assistant" | "message" | "text" => {
                // Extract text content (try multiple shapes). opencode's
                // `--format json` emits the assistant text as a `text` event
                // whose payload lives under `part.text`:
                //   {"type":"text","part":{"type":"text","text":"<response>"}}
                // (NOT a top-level `text`/`content` field). Try `/part/text`
                // first, then fall back to the flat shapes other providers use.
                let text = msg
                    .pointer("/part/text")
                    .and_then(|v| v.as_str())
                    .or_else(|| msg.get("text").and_then(|v| v.as_str()))
                    .or_else(|| msg.pointer("/content/0/text").and_then(|v| v.as_str()))
                    .or_else(|| msg.get("content").and_then(|v| v.as_str()));
                if let Some(text) = text
                    && !text.is_empty()
                {
                    if !output.is_empty() {
                        output.push('\n');
                    }
                    output.push_str(text);
                    let _ = fwd_tx.send(ProviderEvent::Token {
                        session_id: session_id.to_string(),
                        content: text.to_string(),
                    });
                }
            }
            "tool" => {
                let _ = fwd_tx.send(ProviderEvent::ToolCall {
                    session_id: session_id.to_string(),
                    tool_name: msg
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("tool")
                        .to_string(),
                    tool_input: msg.clone(),
                });
            }
            // opencode's one-shot `--format json` emits token usage on the
            // terminal `step_finish` event: `part.tokens = {total, input,
            // output, reasoning, cache:{...}}`. Capture it so the turn's
            // `Completed.usage` carries real token counts → settings/usage +
            // profile token-stats populate (previously always None).
            "step_finish" => {
                if let Some(tokens) = msg.pointer("/part/tokens") {
                    let to_u32 = |k: &str| {
                        tokens
                            .get(k)
                            .and_then(|v| v.as_u64())
                            .map(|n| n as u32)
                            .unwrap_or(0)
                    };
                    let input = to_u32("input");
                    let total = to_u32("total");
                    usage = Some(UsageInfo {
                        input_tokens: input,
                        output_tokens: to_u32("output"),
                        total_tokens: total,
                    });
                }
            }
            "session" if msg.get("ended").and_then(|v| v.as_bool()) == Some(true) => {
                return Ok(OpenCodeTurnOutcome { output, usage });
            }
            _ => {} // ignore unknown event types (thinking_tokens, etc.)
        }
    }
    // EOF without session.ended → synthesize Completed with collected output.
    Ok(OpenCodeTurnOutcome { output, usage })
}

#[async_trait::async_trait]
impl ProviderAdapter for OpenCodeAdapter {
    // -- Identity ----------------------------------------------------------

    fn provider_id(&self) -> &str {
        PROVIDER_OPENCODE
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
        // OpenCode models are `<providerID>/<modelID>`; the live set depends on
        // the server's configured providers. A representative static list keeps
        // the trait contract populated for registry/aggregation consumers.
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
                "OpenCode adapter already spawned".to_string(),
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
        // Serve is primary. If `opencode serve` won't start (binary missing,
        // no ready line, …) arm the one-shot fallback so spawn still succeeds —
        // each turn will then drive `opencode run` directly. A serve that
        // starts but 401s at request time is detected lazily in `send_request`.
        //
        // Auth: `opencode serve` inherits this process's env, so if
        // `OPENCODE_SERVER_PASSWORD` is set the server requires HTTP Basic auth
        // (realm "Secure Area"). Discover the matching credential from the SAME
        // env via `OpenCodeAuth::from_env` — the spawner and HTTP client share
        // one source of truth, so a locally-spawned server never 401s on its
        // own inherited credential. Returns `None` when the password is unset
        // (server is unsecured; no header needed). Verified opencode v1.17.11.
        let auth = OpenCodeAuth::from_env(self.spec.server_auth_username);
        match OpenCodeServerClient::spawn_with(
            self.spec,
            &self.oc_config.bin_path,
            &self.oc_config.extra_args,
            &cwd,
            auth,
            SERVER_TIMEOUT_MS,
        )
        .await
        {
            Ok(client) => {
                *self.client.lock().await = Some(client);
                self.one_shot_fallback.store(false, Ordering::Release);
            }
            Err(e) => {
                self.one_shot_fallback.store(true, Ordering::Release);
                tracing::warn!(
                    provider = PROVIDER_OPENCODE,
                    error = %e,
                    "opencode serve unavailable; arming one-shot `opencode run` fallback \
                     (streaming/interrupt/health degraded)",
                );
            }
        }

        self.config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        });
        let mode = if self.one_shot_fallback.load(Ordering::Acquire) {
            "one-shot fallback"
        } else {
            "serve (HTTP+SSE)"
        };
        tracing::info!(
            provider = PROVIDER_OPENCODE,
            binary = %self.oc_config.bin_path,
            full_auto = self.oc_config.full_auto,
            agent = self.agent(),
            mode,
            "OpenCode adapter spawned",
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
        tracing::info!(provider = PROVIDER_OPENCODE, "OpenCode adapter shut down");
        Ok(())
    }

    async fn interrupt(&self, session_id: &str) -> Result<(), ProviderAdapterError> {
        // One-shot fallback: a turn runs as a blocking `opencode run` subprocess
        // local to its `send_request` future, so there is no tracked handle to
        // signal here — surface `NotSpawned` (the documented best-effort
        // behavior; matches the pre-fallback one-shot adapter). Serve mode
        // forwards to the server's abort endpoint.
        if self.one_shot_fallback.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
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

        // Serve mode: ask the server for a real session id. On any error
        // (notably 401 Unauthorized from auth-requiring `opencode serve` builds)
        // arm the one-shot fallback and mint a synthetic id — each turn then
        // runs `opencode run` directly. Fallback mode short-circuits to the
        // synthetic id without touching the server.
        let session_id = if self.one_shot_fallback.load(Ordering::Acquire) {
            format!("opencode-{}", syncode_core::EntityId::new())
        } else {
            let guard = self.client.lock().await;
            match guard.as_ref() {
                Some(client) => match client
                    .create_session(
                        "syncode",
                        model.as_ref(),
                        Some(&agent),
                        self.oc_config.full_auto,
                    )
                    .await
                {
                    Ok(id) => id,
                    Err(e) => {
                        drop(guard);
                        self.one_shot_fallback.store(true, Ordering::Release);
                        let id = format!("opencode-{}", syncode_core::EntityId::new());
                        tracing::warn!(
                            provider = PROVIDER_OPENCODE,
                            error = %e,
                            synthetic_session_id = %id,
                            "opencode session.create failed (likely 401); arming one-shot fallback",
                        );
                        return self.finalize_session(ctx, id).await;
                    }
                },
                None => return Err(ProviderAdapterError::NotSpawned),
            }
        };

        self.finalize_session(ctx, session_id).await
    }

    async fn resume_session(&mut self, _session_id: &str) -> Result<(), ProviderAdapterError> {
        // OpenCode sessions are stateful server-side: sending another
        // `prompt_async` on the same session id resumes it. No client-side
        // resume RPC exists.
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
                provider = PROVIDER_OPENCODE,
                session_id = session_id,
                "OpenCode session stopped",
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
        let request_id = request.id;

        // Bridge the turn's mpsc events onto the shared broadcast bus.
        let (fwd_tx, mut fwd_rx) = mpsc::channel::<ProviderEvent>(64);
        let bus = self.event_tx.clone();
        let forwarder = tokio::spawn(async move {
            while let Some(event) = fwd_rx.recv().await {
                let _ = bus.send(event);
            }
        });

        self.set_status(ProviderStatus::Busy);

        // Serve is primary; one-shot is the defensive fallback. The fallback is
        // taken immediately when armed (spawn or start_session detected an
        // unusable serve), or re-armed + retried when a serve turn errors
        // mid-flight (typically the first 401 on prompt_async).
        let result = if self.one_shot_fallback.load(Ordering::Acquire) {
            tracing::debug!(
                provider = PROVIDER_OPENCODE,
                "one-shot fallback mode: spawning `opencode run`"
            );
            self.one_shot_turn(
                request_id,
                &request.params,
                &session_id,
                model.as_ref(),
                &system,
                &fwd_tx,
            )
            .await
        } else {
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
            match turn_result {
                Ok(turn) => match turn.status {
                    TurnStatus::Completed | TurnStatus::Cancelled => {
                        let _ = self.event_tx.send(ProviderEvent::Completed {
                            session_id: session_id.clone(),
                            output: turn.output.clone(),
                            usage: turn.usage.clone(),
                        });
                        self.set_status(ProviderStatus::Idle);
                        Ok(ProviderResponse {
                            jsonrpc: "2.0".to_string(),
                            id: Some(request_id),
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
                            .unwrap_or("opencode turn failed")
                            .to_string();
                        self.set_status(ProviderStatus::Idle);
                        Ok(ProviderResponse {
                            jsonrpc: "2.0".to_string(),
                            id: Some(request_id),
                            result: None,
                            error: Some(ProviderError {
                                code: -32000,
                                message,
                                data: Some(turn.raw.clone()),
                            }),
                        })
                    }
                },
                Err(e) => {
                    // Serve turn failed (e.g. 401 Unauthorized on prompt_async).
                    // Arm the fallback for future turns and retry THIS turn via
                    // `opencode run` so the caller sees no regression.
                    self.one_shot_fallback.store(true, Ordering::Release);
                    tracing::warn!(
                        provider = PROVIDER_OPENCODE,
                        error = %e,
                        "serve turn failed; arming one-shot fallback and retrying the turn",
                    );
                    self.one_shot_turn(
                        request_id,
                        &request.params,
                        &session_id,
                        model.as_ref(),
                        &system,
                        &fwd_tx,
                    )
                    .await
                }
            }
        };
        drop(fwd_tx); // close → forwarder drains remaining events and exits
        let _ = forwarder.await;
        result
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
        // One-shot fallback: there is no long-lived server; "spawned" reflects
        // readiness (a turn will launch `opencode run` on demand).
        if self.one_shot_fallback.load(Ordering::Acquire) {
            return Ok(true);
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
    /// trait guards without launching a real `opencode` binary). Used by the
    /// lifecycle / resolution tests.
    fn harness() -> OpenCodeAdapter {
        let (event_tx, _) = broadcast::channel(256);
        OpenCodeAdapter {
            config: None,
            oc_config: OpenCodeConfig::default(),
            spec: &OPENCODE_CLI_SPEC,
            client: Mutex::new(None),
            status: AtomicU64::new(ProviderStatus::Idle.into()),
            spawned: AtomicBool::new(true),
            current_session: Mutex::new(None),
            system_prompt: Mutex::new(None),
            event_tx,
            one_shot_fallback: AtomicBool::new(false),
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
        let adapter = OpenCodeAdapter::new();
        assert_eq!(adapter.provider_id(), PROVIDER_OPENCODE);
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(!adapter.spawned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn double_spawn_is_rejected_before_subprocess_launch() {
        // A harness-built adapter is already spawned; calling spawn() must hit
        // the guard and error WITHOUT attempting to launch a real `opencode`.
        let mut provider = harness();
        let err = provider.spawn(ProviderConfig::default()).await.unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::ConfigError(ref m) if m.contains("already spawned")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn operations_before_spawn_error() {
        let mut adapter = OpenCodeAdapter::new();
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
        let mut adapter = OpenCodeAdapter::new();
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
        let adapter = OpenCodeAdapter::new();
        let caps = adapter.capabilities();
        assert!(caps.contains(&ProviderCapability::Streaming));
        assert!(caps.contains(&ProviderCapability::ToolUse));
        assert!(caps.contains(&ProviderCapability::CodeExecution));
        let models = adapter.available_models();
        assert!(!models.is_empty());
        assert!(models.iter().all(|m| m.contains('/')));
    }

    // --- pure helpers ---

    #[test]
    fn opencode_config_defaults() {
        let config = OpenCodeConfig::default();
        assert_eq!(config.bin_path, "opencode");
        assert!(config.extra_args.is_empty());
        assert!(config.full_auto);
        assert!(config.agent.is_none());
        assert!(config.model.is_empty());
    }

    #[test]
    fn agent_prefers_config_over_spec_default() {
        let adapter = OpenCodeAdapter::with_opencode_config(OpenCodeConfig {
            agent: Some("custom-agent".to_string()),
            ..OpenCodeConfig::default()
        });
        assert_eq!(adapter.agent(), "custom-agent");

        let default_adapter = OpenCodeAdapter::new();
        assert_eq!(default_adapter.agent(), "build"); // OPENCODE_CLI_SPEC.default_agent
    }

    #[test]
    fn turn_input_uses_input_field() {
        let input = OpenCodeAdapter::turn_input(&Some(json!({ "input": "hi", "sequence": 2 })));
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["type"], "text");
        assert_eq!(input[0]["text"], "hi");
    }

    #[test]
    fn turn_input_falls_back_to_params_rendering() {
        let input = OpenCodeAdapter::turn_input(&Some(json!({ "foo": "bar" })));
        assert!(input[0]["text"].as_str().unwrap().contains("foo"));
    }

    #[test]
    fn turn_input_empty_when_null() {
        let input = OpenCodeAdapter::turn_input(&None);
        assert_eq!(input[0]["text"], "");
    }

    #[tokio::test]
    async fn run_opencode_turn_captures_tokens_from_step_finish() {
        // opencode's one-shot `--format json` emits token usage on the terminal
        // `step_finish` event: `part.tokens = {total, input, output, reasoning,
        // cache:{...}}` (captured from a live opencode run). The parser must
        // populate `usage` so the turn records real token counts (was always
        // None before — settings/usage + profile token-stats were empty).
        let ndjson = concat!(
            r#"{"type":"step_start","part":{"type":"step-start"}}"#,
            "\n",
            r#"{"type":"text","part":{"type":"text","text":"hi"}}"#,
            "\n",
            r#"{"type":"step_finish","part":{"type":"step-finish","reason":"stop","tokens":{"total":195270,"input":195264,"output":6,"reasoning":0,"cache":{"write":0,"read":0}}}}"#,
            "\n",
        );
        let reader =
            tokio::io::BufReader::new(std::io::Cursor::new(ndjson.to_string().into_bytes()));
        let (tx, _rx) = tokio::sync::mpsc::channel::<ProviderEvent>(64);
        let outcome = run_opencode_turn(reader, "ses_test", &tx)
            .await
            .expect("turn should complete at EOF");
        assert_eq!(outcome.output, "hi");
        let usage = outcome
            .usage
            .expect("step_finish tokens must populate usage");
        assert_eq!(usage.input_tokens, 195264);
        assert_eq!(usage.output_tokens, 6);
        assert_eq!(usage.total_tokens, 195270);
    }

    #[tokio::test]
    async fn run_opencode_turn_extracts_part_text_from_text_event() {
        // opencode `--format json` emits the assistant response as a `text`
        // event with the payload nested under `part.text` (captured from a
        // live `opencode run -m zai-coding-plan/glm-5.2 --format json`):
        //   {"type":"step_start", ...}
        //   {"type":"text", "part":{"type":"text","text":"SYNCODE_ROUNDTRIP_OK"}}
        //   {"type":"step_finish", ...}
        // The parser MUST extract `part.text` (not a top-level text/content),
        // otherwise the turn completes with an empty assistant_output.
        let ndjson = concat!(
            r#"{"type":"step_start","part":{"type":"step-start"}}"#,
            "\n",
            r#"{"type":"text","part":{"type":"text","text":"SYNCODE_ROUNDTRIP_OK"}}"#,
            "\n",
            r#"{"type":"step_finish","part":{"type":"step-finish","reason":"stop"}}"#,
            "\n",
        );
        let reader =
            tokio::io::BufReader::new(std::io::Cursor::new(ndjson.to_string().into_bytes()));
        let (tx, _rx) = tokio::sync::mpsc::channel::<ProviderEvent>(64);
        let outcome = run_opencode_turn(reader, "ses_test", &tx)
            .await
            .expect("turn should complete at EOF");
        assert_eq!(
            outcome.output, "SYNCODE_ROUNDTRIP_OK",
            "part.text must be extracted from the opencode `text` event"
        );
    }

    #[tokio::test]
    async fn fallback_health_check_returns_spawned_when_armed() {
        // When the one-shot fallback is armed, health_check reports readiness
        // (true) even though there is no live serve client — a turn will spawn
        // `opencode run` on demand.
        let provider = harness();
        assert!(!provider.one_shot_fallback.load(Ordering::Acquire));
        assert!(!provider.health_check().await.unwrap()); // no client, not armed → false
        provider.one_shot_fallback.store(true, Ordering::Release);
        assert!(provider.health_check().await.unwrap()); // armed → readiness
    }

    #[tokio::test]
    async fn fallback_interrupt_returns_not_spawned_when_armed() {
        // In one-shot fallback mode there is no tracked subprocess to signal,
        // so interrupt surfaces NotSpawned (documented best-effort behavior).
        let provider = harness();
        provider.one_shot_fallback.store(true, Ordering::Release);
        assert!(matches!(
            provider.interrupt("ses_x").await.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
    }

    #[test]
    fn model_ref_parses_provider_slash_model() {
        let m = model_ref("anthropic/claude-sonnet-4-5").unwrap();
        assert_eq!(m.provider_id, "anthropic");
        assert_eq!(m.id, "claude-sonnet-4-5");
    }

    #[test]
    fn model_ref_rejects_unqualified_and_empty() {
        // An unqualified id has no providerID → reject (server picks default).
        assert!(model_ref("claude-sonnet-4-5").is_none());
        assert!(model_ref("").is_none());
        assert!(model_ref("/").is_none());
        assert!(model_ref("anthropic/").is_none());
        assert!(model_ref("/sonnet").is_none());
    }

    #[test]
    fn model_resolution_prefers_request_then_config() {
        let mut adapter = OpenCodeAdapter::with_opencode_config(OpenCodeConfig {
            model: "openai/gpt-5".to_string(),
            ..OpenCodeConfig::default()
        });
        adapter.config = Some(ProviderConfig {
            model: "anthropic/claude-opus-4-1".to_string(),
            ..ProviderConfig::default()
        });

        // No request model → spawn config model.
        let req = ProviderRequest::new("chat", Some(json!({ "input": "x" })));
        let m = adapter.model_ref_for(&req).unwrap();
        assert_eq!(
            (m.provider_id.as_str(), m.id.as_str()),
            ("anthropic", "claude-opus-4-1")
        );

        // Request model wins.
        let req = ProviderRequest::new(
            "chat",
            Some(json!({ "input": "x", "model": "google/gemini-2.5-pro" })),
        );
        let m = adapter.model_ref_for(&req).unwrap();
        assert_eq!(
            (m.provider_id.as_str(), m.id.as_str()),
            ("google", "gemini-2.5-pro")
        );

        // No config model → oc_config default.
        adapter.config = None;
        let req = ProviderRequest::new("chat", Some(json!({ "input": "x" })));
        let m = adapter.model_ref_for(&req).unwrap();
        assert_eq!((m.provider_id.as_str(), m.id.as_str()), ("openai", "gpt-5"));

        // Empty default → None.
        let empty_adapter = OpenCodeAdapter::new();
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

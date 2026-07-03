//! One-shot LLM invocation via a provider adapter (T6c-phase-13).
//!
//! The three LLM-backed RPCs — `provider.compactThread`, `git.summarizeDiff`,
//! `server.generateThreadRecap` — each need a single prompt → response round
//! trip through a provider CLI. They do **not** need a long-lived session or
//! streaming (the composer's compaction, the GitPanel's diff summary, and the
//! thread-recap card all consume a single text blob). This module provides the
//! shared one-shot helper plus a mock adapter for unit tests (real provider
//! CLIs may be absent in CI).
//!
//! ## One-shot flow
//!
//! 1. Look up / construct a [`SharedAdapter`] for the requested provider id.
//! 2. `spawn(config)` — boot the provider subprocess (or no-op for the mock).
//! 3. `start_session(ctx)` — bind a session id to a throwaway thread/turn.
//! 4. `send_request(prompt)` — run the prompt and await the final response.
//! 5. Extract text from `ProviderResponse.result` (provider-specific shape).
//!
//! The text-extraction step is intentionally best-effort across the provider
//! zoo: ACP providers return the raw `PromptResponse` (a JSON object whose
//! `output`/`text`/`content` fields carry the model's reply); codex/claude
//! return similarly-shaped envelopes. We probe the common keys and fall back to
//! a `to_string()` of the result so a successful round trip always yields *some*
//! text (never an empty string — a non-empty guard rejects degenerate outputs).
//!
//! ## Provider availability
//!
//! Real adapters spawn a subprocess (`claude`, `codex`, `gemini`, …). If the
//! binary is missing, `spawn()` returns a [`ProviderAdapterError::Io`] which
//! the helper surfaces as a human-readable `Err(String)`. The RPC handlers
//! convert that into a JSON-RPC error response (never a panic).

use std::sync::Arc;

use syncode_core::EntityId;
use syncode_provider::{
    ProviderAdapter, ProviderAdapterError, ProviderCapability, ProviderConfig, ProviderEvent,
    ProviderRequest, ProviderResponse, ProviderStatus, ProviderStream, SessionContext, UsageInfo,
};
use syncode_provider::{PROVIDER_CLAUDE, PROVIDER_CODEX};
use tokio::sync::RwLock;

/// A shared adapter handle (mirrors `syncode_provider::registry::SharedAdapter`).
pub type SharedAdapter = Arc<RwLock<dyn ProviderAdapter>>;

/// Default provider id used when the RPC omits a `provider` param.
///
/// `claude` is the registry default ([`ProviderRegistry::new`]); if absent we
/// fall back to `codex` so the op works on a stock syncode install that only
/// has codex configured. Resolution order: caller-provided → claude → codex.
pub const DEFAULT_PROVIDER: &str = PROVIDER_CLAUDE;
pub const FALLBACK_PROVIDER: &str = PROVIDER_CODEX;

/// Default model token (the adapter resolves the real model from its config /
/// environment; this is just the value placed into `ProviderConfig.model`).
const DEFAULT_MODEL: &str = "default";

/// Default per-request token cap. Generous enough for a compaction / summary /
/// recap; the provider CLI may impose its own tighter cap.
const DEFAULT_MAX_TOKENS: u32 = 4096;

/// A throwaway working directory for one-shot sessions. The LLM-backed RPCs do
/// not need filesystem access (the prompt carries all the content), so we pin
/// the process cwd to a stable, always-existing path. An empty string would be
/// rejected by some adapters' subprocess spawn; `/` is universally present on
/// Unix and benign on Windows (the adapter spawns the CLI which inherits its
/// own cwd resolution).
const ONESHOT_WORKING_DIR: &str = "/";

/// Error returned by the one-shot helper — a human-readable string suitable for
/// surfacing directly in a JSON-RPC error `message`. Wrapping
/// [`ProviderAdapterError`] here keeps the public surface stringly-typed (the
/// RPC layer has no need to match on provider error variants).
pub type LlmError = String;

/// Run a single prompt through a provider adapter and return the model's reply
/// text **and** any token-usage metadata the response carried.
///
/// The adapter is expected to be **unspawned** (the helper calls `spawn`). It
/// is spawned once per call — one-shot ops are infrequent (composer compaction,
/// diff summary, thread recap) so the per-call subprocess cost is acceptable
/// and avoids leaking long-lived sessions for stateless prompts. The adapter is
/// shut down before returning (best-effort; errors are logged, not surfaced —
/// the response text is already in hand).
///
/// `system` is an optional system prompt (the LLM-backed RPCs each pass a
/// task-specific instruction). `prompt` is the user content (the conversation
/// history / diff / thread body to process).
///
/// Returns `Err` if any lifecycle step fails (spawn / start_session /
/// send_request) or if the response carries a JSON-RPC error or yields no
/// extractable text. The error string is safe to return to the client.
///
/// On success the returned [`InvokeOutcome`] carries the reply text plus the
/// parsed token usage (`usage` is `None` when the provider did not report
/// any). Callers that want usage tracking persist the `usage` into a
/// [`crate::usage::UsageStore`]; callers that don't simply ignore it.
pub async fn invoke_llm_oneshot(
    adapter: &SharedAdapter,
    provider_id: &str,
    model: Option<&str>,
    system: Option<&str>,
    prompt: &str,
) -> Result<InvokeOutcome, LlmError> {
    // 1. spawn
    let config = ProviderConfig {
        provider_id: provider_id.to_string(),
        model: model.unwrap_or(DEFAULT_MODEL).to_string(),
        api_key: None,
        base_url: None,
        max_tokens: Some(DEFAULT_MAX_TOKENS),
        extra: std::collections::HashMap::new(),
    };
    {
        let mut guard = adapter.write().await;
        guard.spawn(config).await.map_err(spawn_err(provider_id))?;
    }

    // Defer shutdown so we always tear the subprocess down on exit (success or
    // error). Best-effort: a shutdown failure does not override a successful
    // response, but is logged for diagnostics.
    let model_token = model.unwrap_or(DEFAULT_MODEL).to_string();
    let result = run_session(adapter, &model_token, system, prompt).await;

    // best-effort shutdown
    if let Err(e) = adapter.write().await.shutdown().await {
        tracing::warn!(provider_id = %provider_id, error = %e, "oneshot shutdown failed");
    }

    result
}

/// The outcome of a successful one-shot invocation: the reply text plus any
/// token-usage metadata the provider reported. The model field echoes the
/// effective model token used (the value placed into `ProviderConfig.model`),
/// so the caller can record it without re-deriving it.
#[derive(Debug, Clone)]
pub struct InvokeOutcome {
    /// The reply text (non-empty; same as the historical `String` return).
    pub text: String,
    /// Token usage reported by the provider, if any. `None` when the
    /// provider's response carried no recognizable usage block.
    pub usage: Option<UsageInfo>,
    /// The effective model token for the call (the value the helper placed
    /// into `ProviderConfig.model`). Echoed back so callers don't need to
    /// recompute it when recording a usage entry.
    pub model: String,
}

impl InvokeOutcome {
    /// Convenience accessor preserving the historical `String` return shape.
    pub fn into_text(self) -> String {
        self.text
    }
}

/// Build the SessionContext + ProviderRequest, run the session, and extract
/// the reply text + usage. Factored out of [`invoke_llm_oneshot`] so the
/// shutdown guard wraps a single expression.
async fn run_session(
    adapter: &SharedAdapter,
    model: &str,
    system: Option<&str>,
    prompt: &str,
) -> Result<InvokeOutcome, LlmError> {
    let ctx = SessionContext {
        thread_id: EntityId::new(),
        turn_id: EntityId::new(),
        working_dir: ONESHOT_WORKING_DIR.to_string(),
        system_prompt: system.map(String::from),
        user_input: prompt.to_string(),
        context_files: Vec::new(),
    };

    // 2. start_session
    let session_id = {
        let mut guard = adapter.write().await;
        guard.start_session(ctx).await.map_err(|e| {
            format!("provider start_session failed: {e}")
        })?
    };

    // 3. send_request — method/params follow the ACP `session/prompt` shape
    //    (the mcode convention every adapter understands). The user input is
    //    the prompt; the system prompt was bound at session start.
    let request = ProviderRequest::new(
        "session/prompt",
        Some(serde_json::json!({
            "session_id": session_id,
            "blocks": [{ "type": "text", "text": prompt }],
        })),
    );
    let response = {
        let guard = adapter.read().await;
        guard.send_request(request).await.map_err(|e| {
            format!("provider send_request failed: {e}")
        })?
    };

    // Extract reply text first (surfaces rpc/empty errors as before). Then
    // parse any token-usage block from the SAME response.result JSON so we
    // can hand it back to the caller for usage tracking. The two extractions
    // are independent — a missing/zero usage block never blocks a successful
    // text reply (best-effort telemetry).
    let text = extract_reply_text(&response)?;
    let usage = extract_usage_from_response(&response);
    Ok(InvokeOutcome {
        text,
        usage,
        model: model.to_string(),
    })
}

/// Pull the model's reply text out of a [`ProviderResponse`].
///
/// Adapters return heterogeneous result envelopes. We probe the common keys
/// (the ACP `PromptResponse` uses `output`; codex/claude HTTP-style adapters
/// use `text`/`content`/`message`); each may be a string or an array of
/// content blocks (`[{ "text": "…" }]`). If no key matches, we fall back to
/// the raw JSON `to_string()` (still useful — the client sees *something*).
///
/// Returns `Err` if the response carries a JSON-RPC error, or if every probe
/// yields an empty string (a degenerate "success" we treat as a failure so the
/// caller surfaces a clear error rather than an empty recap/summary).
fn extract_reply_text(response: &ProviderResponse) -> Result<String, LlmError> {
    if let Some(err) = &response.error {
        return Err(format!(
            "provider rpc error: code={}, message={}",
            err.code, err.message
        ));
    }
    let result = match &response.result {
        Some(v) => v,
        None => return Err("provider returned no result".to_string()),
    };
    // If a known reply key is present, use its text (even if empty — an empty
    // model reply is a real, rejectable outcome). Only fall back to raw JSON
    // when NO known key matched at all (so the caller still sees something).
    let text = match probe_text(result) {
        ProbeOutcome::Found(t) => t,
        ProbeOutcome::KeyPresentButEmpty => {
            return Err("provider returned an empty response".to_string());
        }
        ProbeOutcome::NoKnownKey => result.to_string(),
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("provider returned an empty response".to_string());
    }
    Ok(trimmed.to_string())
}

/// Best-effort extraction of token-usage metadata from a [`ProviderResponse`].
///
/// `ProviderResponse` itself has no `usage` field — usage lives inside the
/// `result` JSON, and different providers serialize it under different keys:
///   - **ACP** (`claude`/`codex`/`cursor`/`gemini`/`grok` via AcpProvider):
///     `result.usage = { inputTokens, outputTokens, totalTokens }` (camelCase).
///   - **codex_app_server / opencode / kilo**: snake_case
///     `result.usage = { input_tokens, output_tokens, total_tokens }` (and
///     sometimes a `last_token_usage` / `lastTokenUsage` nested block — we
///     fall back to that if the top-level `usage` is absent).
///   - **anthropic HTTP adapter**: emits snake_case inside the result it
///     builds.
///
/// We probe `usage` (top-level) first, accepting either casing for the
/// token-count fields, then fall back to `last_token_usage` /
/// `lastTokenUsage`. Returns `None` if no recognizable usage block is found
/// OR every probed block yields all-zero counts (a degenerate "present but
/// empty" we treat as no-usage — recording zeros would pollute aggregates).
///
/// Never errors: a malformed usage block is silently ignored (usage is
/// best-effort telemetry; the reply text is the load-bearing extraction).
fn extract_usage_from_response(response: &ProviderResponse) -> Option<UsageInfo> {
    let result = response.result.as_ref()?;
    // Top-level `usage` block (the common case for ACP + codex).
    if let Some(usage) = result.get("usage")
        && let Some(info) = parse_usage_block(usage)
    {
        return Some(info);
    }
    // Fallback: nested `last_token_usage` / `lastTokenUsage` (codex streams
    // per-token usage under this key; the final response sometimes surfaces
    // it instead of a top-level `usage`).
    for key in ["last_token_usage", "lastTokenUsage"] {
        if let Some(usage) = result.get(key)
            && let Some(info) = parse_usage_block(usage)
        {
            return Some(info);
        }
    }
    None
}

/// Parse a JSON usage block into [`UsageInfo`], accepting either camelCase
/// (`inputTokens`) or snake_case (`input_tokens`) field names. Returns
/// `None` for an all-zero block (treated as no-usage to avoid polluting
/// aggregates with empty observations).
fn parse_usage_block(value: &serde_json::Value) -> Option<UsageInfo> {
    let input = num_field(value, "inputTokens").or_else(|| num_field(value, "input_tokens"));
    let output = num_field(value, "outputTokens").or_else(|| num_field(value, "output_tokens"));
    let total = num_field(value, "totalTokens").or_else(|| num_field(value, "total_tokens"));

    // Require at least one field to be present (otherwise this isn't a usage
    // block at all — it's just some other object that happens to sit under
    // `usage`). If only some fields are present, the missing ones default to 0
    // and total is recomputed when it's absent (a common provider omission).
    if input.is_none() && output.is_none() && total.is_none() {
        return None;
    }
    let input = input.unwrap_or(0);
    let output = output.unwrap_or(0);
    let total = total.unwrap_or(input + output);
    // Suppress degenerate all-zero blocks (provider reported usage structurally
    // but with no real counts — e.g. a failed/aborted turn that still echoed
    // the shape). Recording zeros would skew averages and inflate call counts.
    if input == 0 && output == 0 && total == 0 {
        return None;
    }
    Some(UsageInfo {
        input_tokens: input,
        output_tokens: output,
        total_tokens: total,
    })
}

/// Read a u32 field from a JSON object by key. Returns `None` if the key is
/// absent or the value isn't an unsigned integer.
fn num_field(value: &serde_json::Value, key: &str) -> Option<u32> {
    value.get(key).and_then(|v| v.as_u64()).and_then(|n| {
        if n <= u32::MAX as u64 {
            Some(n as u32)
        } else {
            None
        }
    })
}

/// Outcome of probing a response result for reply text.
enum ProbeOutcome {
    /// A known key yielded non-empty text.
    Found(String),
    /// A known key was present but its text was empty/whitespace.
    KeyPresentButEmpty,
    /// No known reply key was present at all.
    NoKnownKey,
}

/// Probe a JSON value for the common reply-text keys. Returns the first
/// non-empty extraction; distinguishes "key present but empty" from "no known
/// key" so the caller can reject degenerate empty replies.
fn probe_text(value: &serde_json::Value) -> ProbeOutcome {
    let mut any_present = false;
    for key in ["output", "text", "content", "message", "reply", "raw"] {
        if value.get(key).is_some() {
            any_present = true;
            if let Some(extracted) = extract_text_field(value, key)
                && !extracted.trim().is_empty()
            {
                return ProbeOutcome::Found(extracted);
            }
        }
    }
    if any_present {
        ProbeOutcome::KeyPresentButEmpty
    } else {
        ProbeOutcome::NoKnownKey
    }
}

/// Extract a text value from `value[key]`, handling three shapes:
///   - a plain string
///   - an array of content blocks (`[{ "type": "text", "text": "…" }, …]`)
///   - a nested object with its own `text`/`content` field
fn extract_text_field(value: &serde_json::Value, key: &str) -> Option<String> {
    let field = value.get(key)?;
    if let Some(s) = field.as_str() {
        return Some(s.to_string());
    }
    if let Some(arr) = field.as_array() {
        // Concatenate every block's `text` field (ACP/Anthropic content blocks).
        let mut out = String::new();
        for block in arr {
            if let Some(t) = block.get("text").and_then(|v| v.as_str()) {
                out.push_str(t);
                out.push('\n');
            }
        }
        if !out.is_empty() {
            return Some(out.trim_end().to_string());
        }
    }
    // Nested object: recurse one level into its `text`/`content`.
    if let Some(nested) = extract_text_field(field, "text") {
        return Some(nested);
    }
    if let Some(nested) = extract_text_field(field, "content") {
        return Some(nested);
    }
    None
}

/// Format a spawn error for surfacing to the client. The common cause is a
/// missing provider binary (`claude`/`codex`/`gemini` not on `$PATH`), so we
/// emit an actionable hint.
fn spawn_err(provider_id: &str) -> impl Fn(ProviderAdapterError) -> LlmError + '_ {
    move |e| {
        let hint = match &e {
            ProviderAdapterError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
                format!(
                    " — provider CLI '{provider_id}' not found on PATH. \
                     Install it or set the SYNCODE_{provider_id_upper}_BIN env var.",
                    provider_id_upper = provider_id.to_uppercase()
                )
            }
            _ => String::new(),
        };
        format!("provider spawn failed: {e}{hint}")
    }
}

// ---------------------------------------------------------------------------
// Mock adapter — unit-test stand-in for a real provider CLI
// ---------------------------------------------------------------------------

/// A test-only [`ProviderAdapter`] that returns a canned reply without
/// spawning any subprocess. Used by the LLM-backed RPC unit tests to prove the
/// prompt → invoke → result wiring without needing a real `claude`/`codex`
/// binary on the host.
///
/// The mock inspects the incoming prompt and, if it contains a trigger token,
/// emits a response that echoes the trigger so tests can assert the prompt was
/// built correctly (not just that *some* text flowed through). Otherwise it
/// emits a fixed canned reply.
pub struct MockLlmAdapter {
    canned: String,
    spawned: std::sync::atomic::AtomicBool,
    session_id: std::sync::Mutex<Option<String>>,
    /// Optional canned `usage` block to emit in the response JSON (so tests
    /// can exercise the usage-extraction → recording path without a real
    /// provider). `None` → no `usage` key in the response (matches the
    /// historical mock behavior — usage tracking sees nothing).
    canned_usage: Option<UsageInfo>,
}

impl MockLlmAdapter {
    /// Build a mock whose every reply is `canned`.
    pub fn new(canned: impl Into<String>) -> Self {
        Self {
            canned: canned.into(),
            spawned: std::sync::atomic::AtomicBool::new(false),
            session_id: std::sync::Mutex::new(None),
            canned_usage: None,
        }
    }

    /// A mock that echoes the prompt's `blocks[0].text` back, prefixed with a
    /// sentinel — lets a test assert the *exact* prompt reached the adapter.
    pub fn echoing() -> Self {
        Self::new("__ECHO__".to_string())
    }

    /// Attach a canned `usage` block (camelCase wire shape) to every response.
    /// Lets usage-tracking tests assert the extract → record → aggregate path
    /// without depending on a real provider.
    pub fn with_usage(mut self, usage: UsageInfo) -> Self {
        self.canned_usage = Some(usage);
        self
    }
}

#[async_trait::async_trait]
impl ProviderAdapter for MockLlmAdapter {
    fn provider_id(&self) -> &str {
        "mock-llm"
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        vec![ProviderCapability::Streaming]
    }

    fn status(&self) -> ProviderStatus {
        if self.spawned.load(std::sync::atomic::Ordering::Acquire) {
            ProviderStatus::Idle
        } else {
            ProviderStatus::Disconnected
        }
    }

    fn available_models(&self) -> Vec<String> {
        vec!["mock-model".to_string()]
    }

    async fn spawn(&mut self, _config: ProviderConfig) -> Result<(), ProviderAdapterError> {
        self.spawned
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
        self.spawned
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    async fn interrupt(&self, _session_id: &str) -> Result<(), ProviderAdapterError> {
        Ok(())
    }

    async fn start_session(
        &mut self,
        _ctx: SessionContext,
    ) -> Result<String, ProviderAdapterError> {
        let sid = format!("mock-session-{}", uuid::Uuid::new_v4().hyphenated());
        *self.session_id.lock().unwrap() = Some(sid.clone());
        Ok(sid)
    }

    async fn resume_session(&mut self, _session_id: &str) -> Result<(), ProviderAdapterError> {
        Ok(())
    }

    async fn stop_session(&mut self, _session_id: &str) -> Result<(), ProviderAdapterError> {
        Ok(())
    }

    async fn send_request(
        &self,
        request: ProviderRequest,
    ) -> Result<ProviderResponse, ProviderAdapterError> {
        // If echoing, surface the prompt text so tests can assert on it.
        let body = if self.canned == "__ECHO__" {
            let prompt_text = request
                .params
                .as_ref()
                .and_then(|p| p.get("blocks"))
                .and_then(|b| b.get(0))
                .and_then(|b| b.get("text"))
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string();
            format!("ECHO:{prompt_text}")
        } else {
            self.canned.clone()
        };

        // Build the result JSON: always an `output` key (the reply text the
        // extractor probes), plus an optional `usage` block (camelCase ACP
        // shape) when the mock was configured with canned usage. The usage
        // key is what [`extract_usage_from_response`] looks for.
        let result = match &self.canned_usage {
            Some(u) => serde_json::json!({
                "output": body,
                "usage": {
                    "inputTokens": u.input_tokens,
                    "outputTokens": u.output_tokens,
                    "totalTokens": u.total_tokens,
                }
            }),
            None => serde_json::json!({ "output": body }),
        };

        Ok(ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(request.id),
            result: Some(result),
            error: None,
        })
    }

    fn event_stream(&self, _session_id: &str) -> Result<ProviderStream, ProviderAdapterError> {
        // Yield a single status-changed event then end. Built with
        // `futures_util::stream` (a syncode-ws dependency) rather than
        // `async_stream` (not a dependency).
        use futures_util::stream;
        let s = stream::iter(vec![Ok(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        })]);
        Ok(Box::pin(s))
    }

    async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
        Ok(self.spawned.load(std::sync::atomic::Ordering::Acquire))
    }
}

// ---------------------------------------------------------------------------
// Tests — exercise the one-shot helper + text extraction directly.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_shared(canned: &str) -> SharedAdapter {
        let concrete: Arc<RwLock<MockLlmAdapter>> =
            Arc::new(RwLock::new(MockLlmAdapter::new(canned)));
        // Coerce to the trait-object form (`Arc<RwLock<dyn ProviderAdapter>>`).
        // The concrete Arc is moved into this coercion; no allocation.
        let dyn_adapter: Arc<RwLock<dyn ProviderAdapter>> = concrete;
        dyn_adapter
    }

    #[tokio::test]
    async fn oneshot_returns_canned_text() {
        let adapter = mock_shared("compacted conversation summary");
        let outcome = invoke_llm_oneshot(
            &adapter,
            "mock-llm",
            None,
            Some("You compact conversations."),
            "history: hello / hi",
        )
        .await
        .expect("oneshot should succeed");
        assert_eq!(outcome.text, "compacted conversation summary");
        assert!(outcome.usage.is_none(), "plain mock reports no usage");
        assert_eq!(outcome.model, DEFAULT_MODEL);
    }

    #[tokio::test]
    async fn oneshot_echo_reflects_built_prompt() {
        // The echoing mock surfaces the prompt body — proves the helper
        // forwards the user prompt verbatim (not the system prompt).
        let adapter: SharedAdapter = Arc::new(RwLock::new(MockLlmAdapter::echoing()));
        let outcome = invoke_llm_oneshot(
            &adapter,
            "mock-llm",
            None,
            Some("SYSTEM-INSTRUCTION"),
            "USER-PROMPT-BODY",
        )
        .await
        .unwrap();
        assert!(
            outcome.text.contains("USER-PROMPT-BODY"),
            "echo should carry the user prompt; got: {}",
            outcome.text
        );
        assert!(
            !outcome.text.contains("SYSTEM-INSTRUCTION"),
            "system prompt should not leak into the echoed user body"
        );
    }

    #[tokio::test]
    async fn oneshot_extracts_camelcase_usage_from_response() {
        // A mock configured with canned usage → the outcome carries it back,
        // parsed from the camelCase wire shape into UsageInfo.
        let adapter: SharedAdapter = Arc::new(RwLock::new(
            MockLlmAdapter::new("summary").with_usage(UsageInfo {
                input_tokens: 120,
                output_tokens: 30,
                total_tokens: 150,
            }),
        ));
        let outcome = invoke_llm_oneshot(&adapter, "mock-llm", Some("sonnet"), None, "x")
            .await
            .expect("oneshot should succeed");
        let usage = outcome.usage.expect("usage should be parsed");
        assert_eq!((usage.input_tokens, usage.output_tokens, usage.total_tokens), (120, 30, 150));
        assert_eq!(outcome.model, "sonnet", "model token echoes back");
    }

    #[test]
    fn extract_usage_handles_camelcase_and_snake_case() {
        let camel = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: Some(serde_json::json!({
                "output": "x",
                "usage": { "inputTokens": 10, "outputTokens": 5, "totalTokens": 15 }
            })),
            error: None,
        };
        let u = extract_usage_from_response(&camel).expect("camel usage");
        assert_eq!((u.input_tokens, u.output_tokens, u.total_tokens), (10, 5, 15));

        let snake = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: Some(serde_json::json!({
                "output": "x",
                "usage": { "input_tokens": 7, "output_tokens": 3, "total_tokens": 10 }
            })),
            error: None,
        };
        let u = extract_usage_from_response(&snake).expect("snake usage");
        assert_eq!(u.total_tokens, 10);

        // last_token_usage fallback (codex wire shape).
        let last = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: Some(serde_json::json!({
                "output": "x",
                "last_token_usage": { "input_tokens": 2, "output_tokens": 1 }
            })),
            error: None,
        };
        let u = extract_usage_from_response(&last).expect("last_token_usage fallback");
        assert_eq!(u.total_tokens, 3, "total recomputed when absent");
    }

    #[test]
    fn extract_usage_returns_none_for_zero_or_missing() {
        // No usage key at all.
        let no_usage = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: Some(serde_json::json!({ "output": "x" })),
            error: None,
        };
        assert!(extract_usage_from_response(&no_usage).is_none());

        // All-zero usage block → treated as no-usage (avoid polluting aggregates).
        let zero = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: Some(serde_json::json!({
                "output": "x",
                "usage": { "inputTokens": 0, "outputTokens": 0, "totalTokens": 0 }
            })),
            error: None,
        };
        assert!(
            extract_usage_from_response(&zero).is_none(),
            "all-zero usage must be suppressed"
        );

        // No result at all.
        let no_result = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: None,
            error: None,
        };
        assert!(extract_usage_from_response(&no_result).is_none());
    }

    #[tokio::test]
    async fn oneshot_surfaces_spawn_io_not_found_with_hint() {
        // An adapter whose spawn fails with NotFound yields an actionable error.
        struct MissingCli;
        #[async_trait::async_trait]
        impl ProviderAdapter for MissingCli {
            fn provider_id(&self) -> &str {
                "claude"
            }
            fn capabilities(&self) -> Vec<ProviderCapability> {
                vec![]
            }
            fn status(&self) -> ProviderStatus {
                ProviderStatus::Disconnected
            }
            fn available_models(&self) -> Vec<String> {
                vec![]
            }
            async fn spawn(&mut self, _: ProviderConfig) -> Result<(), ProviderAdapterError> {
                Err(ProviderAdapterError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no such file or directory",
                )))
            }
            async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
                Ok(())
            }
            async fn interrupt(&self, _: &str) -> Result<(), ProviderAdapterError> {
                Ok(())
            }
            async fn start_session(&mut self, _: SessionContext) -> Result<String, ProviderAdapterError> {
                unreachable!()
            }
            async fn resume_session(&mut self, _: &str) -> Result<(), ProviderAdapterError> {
                Ok(())
            }
            async fn stop_session(&mut self, _: &str) -> Result<(), ProviderAdapterError> {
                Ok(())
            }
            async fn send_request(&self, _: ProviderRequest) -> Result<ProviderResponse, ProviderAdapterError> {
                unreachable!()
            }
            fn event_stream(&self, _: &str) -> Result<ProviderStream, ProviderAdapterError> {
                unreachable!()
            }
            async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
                Ok(false)
            }
        }
        let adapter: SharedAdapter = Arc::new(RwLock::new(MissingCli));
        let err = invoke_llm_oneshot(&adapter, "claude", None, None, "x")
            .await
            .expect_err("missing CLI should error");
        assert!(err.contains("not found on PATH"), "got: {err}");
        assert!(err.contains("SYNCODE_CLAUDE_BIN"), "got: {err}");
    }

    #[test]
    fn extract_reply_text_prefers_output_key() {
        let resp = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: Some(serde_json::json!({ "output": "the reply" })),
            error: None,
        };
        assert_eq!(extract_reply_text(&resp).unwrap(), "the reply");
    }

    #[test]
    fn extract_reply_text_handles_content_block_array() {
        let resp = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: Some(serde_json::json!({
                "content": [
                    { "type": "text", "text": "part 1 " },
                    { "type": "text", "text": "part 2" },
                ]
            })),
            error: None,
        };
        assert_eq!(extract_reply_text(&resp).unwrap(), "part 1 \npart 2");
    }

    #[test]
    fn extract_reply_text_falls_back_to_raw_json() {
        // Unknown shape → to_string of the JSON (still non-empty).
        let resp = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: Some(serde_json::json!({ "weird_field": 42 })),
            error: None,
        };
        let text = extract_reply_text(&resp).unwrap();
        assert!(text.contains("weird_field"));
    }

    #[test]
    fn extract_reply_text_surfaces_rpc_error() {
        let resp = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: None,
            error: Some(syncode_provider::ProviderError {
                code: -32603,
                message: "boom".into(),
                data: None,
            }),
        };
        let err = extract_reply_text(&resp).expect_err("rpc error should surface");
        assert!(err.contains("boom") && err.contains("-32603"));
    }

    #[test]
    fn extract_reply_text_rejects_empty_output() {
        let resp = ProviderResponse {
            jsonrpc: "2.0".into(),
            id: Some(1),
            result: Some(serde_json::json!({ "output": "   " })),
            error: None,
        };
        assert!(extract_reply_text(&resp).is_err());
    }
}

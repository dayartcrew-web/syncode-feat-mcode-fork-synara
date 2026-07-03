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
    ProviderRequest, ProviderResponse, ProviderStatus, ProviderStream, SessionContext,
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
/// text.
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
pub async fn invoke_llm_oneshot(
    adapter: &SharedAdapter,
    provider_id: &str,
    model: Option<&str>,
    system: Option<&str>,
    prompt: &str,
) -> Result<String, LlmError> {
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
    let result = run_session(adapter, system, prompt).await;

    // best-effort shutdown
    if let Err(e) = adapter.write().await.shutdown().await {
        tracing::warn!(provider_id = %provider_id, error = %e, "oneshot shutdown failed");
    }

    result
}

/// Build the SessionContext + ProviderRequest, run the session, and extract
/// the reply text. Factored out of [`invoke_llm_oneshot`] so the shutdown
/// guard wraps a single expression.
async fn run_session(
    adapter: &SharedAdapter,
    system: Option<&str>,
    prompt: &str,
) -> Result<String, LlmError> {
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

    extract_reply_text(&response)
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
}

impl MockLlmAdapter {
    /// Build a mock whose every reply is `canned`.
    pub fn new(canned: impl Into<String>) -> Self {
        Self {
            canned: canned.into(),
            spawned: std::sync::atomic::AtomicBool::new(false),
            session_id: std::sync::Mutex::new(None),
        }
    }

    /// A mock that echoes the prompt's `blocks[0].text` back, prefixed with a
    /// sentinel — lets a test assert the *exact* prompt reached the adapter.
    pub fn echoing() -> Self {
        Self::new("__ECHO__".to_string())
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

        Ok(ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(request.id),
            result: Some(serde_json::json!({ "output": body })),
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
        let text = invoke_llm_oneshot(
            &adapter,
            "mock-llm",
            None,
            Some("You compact conversations."),
            "history: hello / hi",
        )
        .await
        .expect("oneshot should succeed");
        assert_eq!(text, "compacted conversation summary");
    }

    #[tokio::test]
    async fn oneshot_echo_reflects_built_prompt() {
        // The echoing mock surfaces the prompt body — proves the helper
        // forwards the user prompt verbatim (not the system prompt).
        let adapter: SharedAdapter = Arc::new(RwLock::new(MockLlmAdapter::echoing()));
        let text = invoke_llm_oneshot(
            &adapter,
            "mock-llm",
            None,
            Some("SYSTEM-INSTRUCTION"),
            "USER-PROMPT-BODY",
        )
        .await
        .unwrap();
        assert!(
            text.contains("USER-PROMPT-BODY"),
            "echo should carry the user prompt; got: {text}"
        );
        assert!(
            !text.contains("SYSTEM-INSTRUCTION"),
            "system prompt should not leak into the echoed user body"
        );
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

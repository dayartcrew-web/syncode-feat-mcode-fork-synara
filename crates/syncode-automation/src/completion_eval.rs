//! AI-evaluated completion policy (P2-2).
//!
//! Mirrors MCode's `evaluateCompletionPolicy` pattern: when an automation's
//! [`CompletionPolicy`] is [`AiEvaluated`], the system asks an LLM whether the
//! natural-language `stop_when` condition is satisfied by the run's output, and
//! trusts the verdict only when the model's reported confidence meets the
//! configured threshold — *and* the automation definition didn't change while
//! the (slow) LLM call was in flight (stale-check guard).
//!
//! ## Design — why a port, not a direct `invoke_llm_oneshot` call
//!
//! The automation crate sits below the WS layer and must not depend on
//! `syncode-ws` (where [`invoke_llm_oneshot`] lives). So the LLM round trip is
//! modeled as the [`CompletionLlmCall`] port: a single async method that takes
//! the built prompt and returns the model's reply text. Production wiring
//! supplies an adapter that calls `invoke_llm_oneshot` under the hood; unit
//! tests supply a canned responder (no real provider CLI needed). This keeps
//! the evaluation logic fully testable from within the crate.
//!
//! ## Flow
//!
//! 1. Build the prompt: the `stop_when` condition + the run's assistant output
//!    (the evidence the model reasons over).
//! 2. Call [`CompletionLlmCall::invoke`] — the one LLM round trip.
//! 3. Parse the reply for a confidence score in `[0.0, 1.0]` (the model is
//!    instructed to emit `CONFIDENCE: 0.xx`; we tolerate a few shapes).
//! 4. **Stale-check**: reload the automation def from the repository. If its
//!    `version` changed since the call started, the `stop_when` we evaluated
//!    against is no longer current — return [`CompletionVerdict::Stale`] so the
//!    caller re-evaluates instead of acting on a verdict for an old prompt.
//! 5. Otherwise return [`CompletionVerdict::Match`] / [`NoMatch`] with the
//!    parsed confidence, compared against the policy's `confidence_threshold`.
//!
//! [`AiEvaluated`]: crate::policies::CompletionPolicy::AiEvaluated
//! [`invoke_llm_oneshot`]: syncode_ws::llm::invoke_llm_oneshot

use std::sync::Arc;

use syncode_core::ports::AutomationRepository;

use crate::definition::AutomationDef;
use crate::events::RunContext;
use crate::policies::CompletionPolicy;

// ─── LLM call port ─────────────────────────────────────────────────────────

/// A single LLM round trip for the AI-evaluated completion check.
///
/// The port is deliberately minimal — one method, prompt-in / reply-out — so
/// the evaluation logic can be unit-tested with a canned responder and wired to
/// a real provider in production without dragging the WS layer into this crate.
///
/// Implementations MUST be `Send + Sync` (the check runs on a Tokio task).
/// The prompt is built by [`build_prompt`]; the reply is parsed by
/// [`parse_confidence`].
#[async_trait::async_trait]
pub trait CompletionLlmCall: Send + Sync {
    /// Run `prompt` through the model and return its reply text.
    ///
    /// Errors are surfaced as a human-readable string (mirroring
    /// [`invoke_llm_oneshot`]'s `LlmError = String`); the evaluator treats any
    /// error as a non-match (a failed check never completes a run).
    async fn invoke(&self, prompt: &str) -> Result<String, String>;
}

// ─── Result model ──────────────────────────────────────────────────────────

/// The outcome of evaluating an [`AiEvaluated`] completion policy.
///
/// [`AiEvaluated`]: crate::policies::CompletionPolicy::AiEvaluated
#[derive(Debug, Clone, PartialEq)]
pub enum CompletionVerdict {
    /// The model judged the `stop_when` condition satisfied with confidence at
    /// or above the policy's threshold.
    Match {
        /// The parsed confidence in `[0.0, 1.0]`.
        confidence: f64,
    },
    /// The model judged the condition not satisfied, OR the confidence was
    /// below the threshold, OR the LLM call / parse failed (a failed check
    /// never completes a run — the caller schedules the next evaluation).
    NoMatch {
        /// The parsed confidence, if the reply yielded one (`None` when the
        /// call errored or the reply carried no parseable score).
        confidence: Option<f64>,
        /// Why it didn't match — surfaced for diagnostics / logging.
        reason: NoMatchReason,
    },
    /// The automation definition's `version` changed while the LLM call was in
    /// flight. The `stop_when` we evaluated against is no longer the active
    /// condition, so the verdict is discarded — the caller should re-evaluate
    /// against the freshly-loaded def rather than act on this stale result.
    Stale {
        /// The version the evaluator read before the call.
        expected_version: u64,
        /// The version the def had when reloaded after the call.
        current_version: u64,
    },
}

/// Why a completion check resolved to [`CompletionVerdict::NoMatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoMatchReason {
    /// The model's confidence was below the configured threshold.
    BelowThreshold,
    /// The reply carried no parseable confidence score.
    Unparseable,
    /// The LLM call itself failed (provider error, timeout, …).
    LlmFailed,
}

/// The full result of [`evaluate_completion_policy`] — the verdict plus the
/// raw model reply (for logging / the run record).
#[derive(Debug, Clone)]
pub struct CompletionResult {
    /// The evaluation verdict.
    pub verdict: CompletionVerdict,
    /// The raw model reply text (`None` when the LLM call failed before
    /// producing any text).
    pub raw_reply: Option<String>,
}

impl CompletionResult {
    /// Whether the verdict is a [`Match`](CompletionVerdict::Match).
    pub fn is_match(&self) -> bool {
        matches!(self.verdict, CompletionVerdict::Match { .. })
    }

    /// Whether the verdict is [`Stale`](CompletionVerdict::Stale).
    pub fn is_stale(&self) -> bool {
        matches!(self.verdict, CompletionVerdict::Stale { .. })
    }
}

// ─── Prompt construction ───────────────────────────────────────────────────

/// The system instruction that frames the model as a binary completion
/// evaluator. Asks for a `CONFIDENCE:` score so [`parse_confidence`] can find
/// it. Kept as a `const` (not built per call) so the prompt is stable and
/// auditable.
const SYSTEM_INSTRUCTION: &str = "\
You are a completion-condition evaluator. Decide whether the provided run \
output satisfies the stated stop condition. Reply with a single line in the \
form `CONFIDENCE: 0.XX` where 0.XX is your confidence (0.00 to 1.00) that the \
condition is met. You may add a brief one-sentence justification on the same \
line after the score.";

/// Build the user prompt the evaluator sends to the model.
///
/// Shapes the evidence so the model reasons over the actual run output (not a
/// paraphrase): the `stop_when` condition is stated first, then the assistant
/// text the run produced. The prompt is the sole channel through which run
/// state reaches the model — nothing else about the run is leaked.
pub fn build_prompt(stop_when: &str, assistant_text: &str) -> String {
    format!(
        "Should I stop? Condition: {stop_when}\n\nRun output:\n{assistant_text}"
    )
}

/// The system instruction paired with the prompt — convenience for callers that
/// wire the port to a provider expecting both (`system` + `prompt`).
pub fn build_system_and_prompt(
    stop_when: &str,
    assistant_text: &str,
) -> ( &'static str, String ) {
    (SYSTEM_INSTRUCTION, build_prompt(stop_when, assistant_text))
}

// ─── Confidence parsing ────────────────────────────────────────────────────

/// Parse a confidence score in `[0.0, 1.0]` out of the model's reply.
///
/// Tolerates several shapes the model might emit:
///   - `CONFIDENCE: 0.92` (the instructed form)
///   - `confidence: 0.92`
///   - `0.92` (a bare number)
///   - `92%`
///
/// Probes case-insensitively for the `CONFIDENCE:` label first; if absent,
/// falls back to the first decimal-looking number in the reply. Returns `None`
/// when no parseable score is found (the evaluator treats that as a non-match).
pub fn parse_confidence(reply: &str) -> Option<f64> {
    let lower = reply.to_ascii_lowercase();
    // Preferred: a `CONFIDENCE:` label followed by the score.
    if let Some(idx) = lower.find("confidence") {
        let after = &reply[idx + "confidence".len()..];
        if let Some(score) = first_number(after) {
            return Some(clamp01(score));
        }
    }
    // Fallback: the first decimal-looking number anywhere in the reply.
    first_number(reply).map(clamp01)
}

/// Extract the first number (handling an optional leading `-` and a trailing
/// `%`-suffix) from a string slice. Recognizes decimals (`0.92`, `-0.1`) and
/// bare percentages (`92%`).
fn first_number(s: &str) -> Option<f64> {
    // Walk the string, accumulating a span of [-0-9.] characters; parse it as
    // f64. A leading `-` is included when immediately followed by a digit, so
    // `-0.1` parses as a negative number (clamped to 0.0 by the caller). A `%`
    // immediately after the span scales the value (92% → 0.92).
    let bytes = s.as_bytes();
    let mut start: Option<usize> = None;
    let mut end = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let is_digit = b.is_ascii_digit();
        let is_dot = b == b'.';
        let is_sign = b == b'-';
        if (is_digit || is_dot) && start.is_some() {
            end = i + 1;
        } else if is_sign
            && start.is_none()
            && bytes.get(i + 1).is_some_and(|nb| nb.is_ascii_digit())
        {
            // A leading `-` immediately before a digit starts the span.
            start = Some(i);
            end = i + 1;
        } else if is_digit && start.is_none() {
            start = Some(i);
            end = i + 1;
        } else if start.is_some() {
            break;
        }
    }
    let start = start?;
    let span = &s[start..end];
    let val: f64 = span.parse().ok()?;
    // A trailing `%` → scale to [0,1].
    let scaled = if bytes.get(end) == Some(&b'%') {
        val / 100.0
    } else {
        val
    };
    Some(scaled)
}

/// Clamp a score into `[0.0, 1.0]` (guards against the model emitting `1.2` or
/// a negative value — we never want to act on an out-of-range confidence).
fn clamp01(v: f64) -> f64 {
    v.clamp(0.0, 1.0)
}

// ─── Evaluator ─────────────────────────────────────────────────────────────

/// Evaluate an [`AiEvaluated`](CompletionPolicy::AiEvaluated) completion policy.
///
/// Given the automation def (read *before* the call), the run context (carries
/// the assistant text the model reasons over), and an injectable
/// [`CompletionLlmCall`] port, this:
///
/// 1. Builds the prompt from `stop_when` + the run's assistant text.
/// 2. Invokes the LLM (the single round trip).
/// 3. Parses the confidence from the reply.
/// 4. **Stale-check**: reloads the def from `repo`. If `version` changed since
///    `def` was read, returns [`CompletionVerdict::Stale`] (the `stop_when` we
///    evaluated against is no longer current).
/// 5. Compares the parsed confidence against the policy's threshold and returns
///    [`Match`](CompletionVerdict::Match) / [`NoMatch`](CompletionVerdict::NoMatch).
///
/// `assistant_text` is the run's output (the evidence). In production this is
/// the assistant turn's text for the run's target thread; tests pass a canned
/// string.
///
/// The `Arc<dyn AutomationRepository>` is required (not `&dyn`) so the function
/// is `'static`-safe for spawning on a task — the orchestrator holds the repo
/// by `Arc` already. The stale-check reload uses [`AutomationRepository::get_def`].
///
/// # Errors / non-match handling
///
/// An LLM call failure or an unparseable reply is a [`NoMatch`](CompletionVerdict::NoMatch)
/// (never a panic, never a `Match`) — a failed evaluation must not silently
/// complete a run. A repository read failure during the stale-check is treated
/// as stale-defensive: if we can't confirm the version is unchanged, we assume
/// it might have changed and return [`Stale`](CompletionVerdict::Stale) so the
/// caller re-evaluates from a clean reload rather than trusting an unverified
/// verdict.
pub async fn evaluate_completion_policy(
    def: &AutomationDef,
    run: &RunContext,
    assistant_text: &str,
    llm: &dyn CompletionLlmCall,
    repo: &Arc<dyn AutomationRepository>,
) -> CompletionResult {
    let (stop_when, threshold) = match &def.completion_policy {
        CompletionPolicy::AiEvaluated {
            stop_when,
            confidence_threshold,
        } => (stop_when.as_str(), *confidence_threshold),
        // Non-AI policies aren't evaluated here; the caller should have routed
        // them through `is_success(exit_code)` instead. Defensive: treat as
        // NoMatch so a misrouted call can't complete a run.
        _ => {
            return CompletionResult {
                verdict: CompletionVerdict::NoMatch {
                    confidence: None,
                    reason: NoMatchReason::Unparseable,
                },
                raw_reply: None,
            };
        }
    };

    let expected_version = def.version;
    let prompt = build_prompt(stop_when, assistant_text);

    // 1. The LLM round trip.
    let reply = llm.invoke(&prompt).await;
    let raw_reply = reply.as_ref().ok().cloned();

    // 2. Stale-check: reload the def and compare versions. Done AFTER the
    //    (slow) LLM call returns — that's the whole point: detect a mutation
    //    that landed while the call was in flight.
    match repo.get_def(&def.id.as_str()).await {
        Ok(Some(payload)) => match deserialize_version(&payload) {
            Some(current_version) if current_version == expected_version => {
                // Unchanged — proceed to compare the verdict.
            }
            Some(current_version) => {
                return CompletionResult {
                    verdict: CompletionVerdict::Stale {
                        expected_version,
                        current_version,
                    },
                    raw_reply,
                };
            }
            // Payload present but no version field → legacy def. Treat as
            // unchanged only if we also started at the legacy default (1);
            // otherwise the field was removed, which counts as a change.
            None if expected_version == 1 => {}
            None => {
                return CompletionResult {
                    verdict: CompletionVerdict::Stale {
                        expected_version,
                        current_version: 1,
                    },
                    raw_reply,
                };
            }
        },
        // Couldn't reload the def — stale-defensive: don't trust the verdict.
        Err(_) | Ok(None) => {
            tracing::warn!(
                automation_id = %def.id,
                "completion stale-check could not reload def; discarding verdict"
            );
            return CompletionResult {
                verdict: CompletionVerdict::Stale {
                    expected_version,
                    current_version: expected_version.wrapping_add(1),
                },
                raw_reply,
            };
        }
    }

    // 3. Parse the confidence and compare against the threshold.
    let reply_text = match reply {
        Ok(t) => t,
        Err(err) => {
            tracing::warn!(
                automation_id = %def.id,
                run_id = %run.run_id,
                error = %err,
                "completion LLM call failed"
            );
            return CompletionResult {
                verdict: CompletionVerdict::NoMatch {
                    confidence: None,
                    reason: NoMatchReason::LlmFailed,
                },
                raw_reply: None,
            };
        }
    };

    match parse_confidence(&reply_text) {
        Some(confidence) if confidence >= threshold => CompletionResult {
            verdict: CompletionVerdict::Match { confidence },
            raw_reply: Some(reply_text),
        },
        Some(confidence) => CompletionResult {
            verdict: CompletionVerdict::NoMatch {
                confidence: Some(confidence),
                reason: NoMatchReason::BelowThreshold,
            },
            raw_reply: Some(reply_text),
        },
        None => CompletionResult {
            verdict: CompletionVerdict::NoMatch {
                confidence: None,
                reason: NoMatchReason::Unparseable,
            },
            raw_reply: Some(reply_text),
        },
    }
}

/// Read the `version` field out of a serialized def payload.
///
/// The def is stored as `serde_json::Value` (camelCase — see
/// [`AutomationDef`]'s serde config). `version` is optional with a default of
/// `1`, so a legacy payload (serialized before the field existed) yields
/// `None` here, which the caller interprets as the default.
fn deserialize_version(payload: &serde_json::Value) -> Option<u64> {
    payload.get("version").and_then(|v| v.as_u64())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::events::NoopRunEventSink;
    use crate::policies::CompletionPolicy;

    // ─── Test doubles ──────────────────────────────────────────────────

    /// A [`CompletionLlmCall`] that returns a canned reply (or error),
    /// recording the prompt it received so tests can assert on it.
    struct CannedLlm {
        reply: Mutex<Result<String, String>>,
        prompt: Mutex<Option<String>>,
    }

    impl CannedLlm {
        fn ok(reply: impl Into<String>) -> Self {
            Self {
                reply: Mutex::new(Ok(reply.into())),
                prompt: Mutex::new(None),
            }
        }
        fn err(err: impl Into<String>) -> Self {
            Self {
                reply: Mutex::new(Err(err.into())),
                prompt: Mutex::new(None),
            }
        }
        fn prompt_seen(&self) -> Option<String> {
            self.prompt.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl CompletionLlmCall for CannedLlm {
        async fn invoke(&self, prompt: &str) -> Result<String, String> {
            *self.prompt.lock().unwrap() = Some(prompt.to_string());
            self.reply.lock().unwrap().clone()
        }
    }

    /// A minimal in-memory repo for the stale-check (only needs `get_def`).
    /// Holds the current serialized def payload; tests can mutate it to
    /// simulate a version bump during the call.
    struct StaleCheckRepo {
        payload: tokio::sync::Mutex<Option<serde_json::Value>>,
    }

    impl StaleCheckRepo {
        fn with(def: &AutomationDef) -> Self {
            let payload = serde_json::to_value(def).unwrap();
            Self {
                payload: tokio::sync::Mutex::new(Some(payload)),
            }
        }
        async fn set_version(&self, version: u64) {
            let mut guard = self.payload.lock().await;
            if let Some(p) = guard.as_mut()
                && let Some(obj) = p.as_object_mut()
            {
                obj.insert("version".to_string(), serde_json::json!(version));
            }
        }
        async fn remove_version(&self) {
            let mut guard = self.payload.lock().await;
            if let Some(p) = guard.as_mut()
                && let Some(obj) = p.as_object_mut()
            {
                obj.remove("version");
            }
        }
        async fn drop_payload(&self) {
            *self.payload.lock().await = None;
        }
    }

    #[async_trait::async_trait]
    impl AutomationRepository for StaleCheckRepo {
        async fn save_def(
            &self,
            _id: &str,
            _payload: serde_json::Value,
        ) -> Result<(), syncode_core::PortError> {
            Ok(())
        }
        async fn get_def(
            &self,
            _id: &str,
        ) -> Result<Option<serde_json::Value>, syncode_core::PortError> {
            Ok(self.payload.lock().await.clone())
        }
        async fn list_defs(&self) -> Result<Vec<serde_json::Value>, syncode_core::PortError> {
            Ok(Vec::new())
        }
        async fn delete_def(&self, _id: &str) -> Result<bool, syncode_core::PortError> {
            Ok(false)
        }
        async fn save_run(
            &self,
            _payload: serde_json::Value,
        ) -> Result<(), syncode_core::PortError> {
            Ok(())
        }
        async fn get_run(
            &self,
            _id: &str,
        ) -> Result<Option<serde_json::Value>, syncode_core::PortError> {
            Ok(None)
        }
        async fn list_runs(
            &self,
            _automation_id: &str,
        ) -> Result<Vec<serde_json::Value>, syncode_core::PortError> {
            Ok(Vec::new())
        }
        async fn advance_next_run_at(
            &self,
            _id: &str,
            _next_run_at: Option<String>,
        ) -> Result<(), syncode_core::PortError> {
            Ok(())
        }
    }

    fn run_ctx() -> RunContext {
        RunContext {
            run_id: "run-test".to_string(),
            automation_id: "auto-test".to_string(),
            sink: Arc::new(NoopRunEventSink) as Arc<dyn crate::events::RunEventSink>,
        }
    }

    fn ai_def(stop_when: &str, threshold: f64) -> AutomationDef {
        let mut def = AutomationDef::new(
            "ai-auto".to_string(),
            "echo".to_string(),
            crate::definition::ScheduleType::Manual,
        );
        def.completion_policy = CompletionPolicy::AiEvaluated {
            stop_when: stop_when.to_string(),
            confidence_threshold: threshold,
        };
        def
    }

    fn repo_for(def: &AutomationDef) -> Arc<dyn AutomationRepository> {
        Arc::new(StaleCheckRepo::with(def))
    }

    // ─── parse_confidence unit tests ───────────────────────────────────

    #[test]
    fn parse_confidence_instructed_form() {
        assert_eq!(parse_confidence("CONFIDENCE: 0.92"), Some(0.92));
        assert_eq!(parse_confidence("confidence: 0.50"), Some(0.5));
        assert_eq!(
            parse_confidence("CONFIDENCE: 0.99 looks done"),
            Some(0.99)
        );
    }

    #[test]
    fn parse_confidence_percentage_form() {
        assert_eq!(parse_confidence("92%"), Some(0.92));
        assert_eq!(parse_confidence("CONFIDENCE: 75%"), Some(0.75));
    }

    #[test]
    fn parse_confidence_bare_number_fallback() {
        assert_eq!(parse_confidence("0.42"), Some(0.42));
    }

    #[test]
    fn parse_confidence_clamps_out_of_range() {
        assert_eq!(parse_confidence("CONFIDENCE: 1.5"), Some(1.0));
        assert_eq!(parse_confidence("CONFIDENCE: -0.1"), Some(0.0));
    }

    #[test]
    fn parse_confidence_returns_none_when_unparseable() {
        assert_eq!(parse_confidence("no score here"), None);
        assert_eq!(parse_confidence(""), None);
    }

    // ─── build_prompt unit tests ───────────────────────────────────────

    #[test]
    fn build_prompt_carries_condition_and_output() {
        let p = build_prompt("all tests pass", "ran 50 tests, all green");
        assert!(p.contains("all tests pass"));
        assert!(p.contains("ran 50 tests, all green"));
        assert!(p.starts_with("Should I stop?"));
    }

    #[test]
    fn build_system_and_prompt_pairs_instruction_with_prompt() {
        let (sys, p) = build_system_and_prompt("cond", "out");
        assert!(sys.contains("CONFIDENCE"));
        assert!(p.contains("cond"));
        assert!(p.contains("out"));
    }

    // ─── evaluate_completion_policy: the happy path ───────────────────

    #[tokio::test]
    async fn evaluate_returns_match_when_confidence_meets_threshold() {
        let def = ai_def("all tests pass", 0.8);
        let repo = repo_for(&def);
        let llm = CannedLlm::ok("CONFIDENCE: 0.95 — all tests green");
        let run = run_ctx();

        let result =
            evaluate_completion_policy(&def, &run, "50 tests passed", &llm, &repo).await;

        assert!(result.is_match(), "expected Match, got {:?}", result.verdict);
        match result.verdict {
            CompletionVerdict::Match { confidence } => {
                assert!((confidence - 0.95).abs() < 1e-9);
            }
            _ => unreachable!(),
        }
        // The prompt the model received carried the condition + output.
        let prompt = llm.prompt_seen().expect("LLM was not invoked");
        assert!(prompt.contains("all tests pass"));
        assert!(prompt.contains("50 tests passed"));
        // Raw reply preserved for the run record.
        assert!(result.raw_reply.as_deref().unwrap().contains("0.95"));
    }

    // ─── evaluate_completion_policy: below threshold ──────────────────

    #[tokio::test]
    async fn evaluate_returns_no_match_when_confidence_below_threshold() {
        let def = ai_def("all tests pass", 0.9);
        let repo = repo_for(&def);
        let llm = CannedLlm::ok("CONFIDENCE: 0.40 — only some tests pass");
        let run = run_ctx();

        let result =
            evaluate_completion_policy(&def, &run, "some tests failed", &llm, &repo).await;

        assert!(!result.is_match());
        match result.verdict {
            CompletionVerdict::NoMatch {
                confidence,
                reason,
            } => {
                assert_eq!(confidence, Some(0.40));
                assert_eq!(reason, NoMatchReason::BelowThreshold);
            }
            _ => unreachable!(),
        }
    }

    // ─── evaluate_completion_policy: unparseable reply ────────────────

    #[tokio::test]
    async fn evaluate_returns_no_match_unparseable_when_reply_lacks_score() {
        let def = ai_def("all tests pass", 0.8);
        let repo = repo_for(&def);
        let llm = CannedLlm::ok("the tests look fine I guess");
        let run = run_ctx();

        let result =
            evaluate_completion_policy(&def, &run, "output", &llm, &repo).await;

        match result.verdict {
            CompletionVerdict::NoMatch {
                confidence,
                reason,
            } => {
                assert_eq!(confidence, None);
                assert_eq!(reason, NoMatchReason::Unparseable);
            }
            _ => unreachable!(),
        }
    }

    // ─── evaluate_completion_policy: LLM call failure ─────────────────

    #[tokio::test]
    async fn evaluate_returns_no_match_llm_failed_when_call_errors() {
        let def = ai_def("all tests pass", 0.8);
        let repo = repo_for(&def);
        let llm = CannedLlm::err("provider timeout");
        let run = run_ctx();

        let result =
            evaluate_completion_policy(&def, &run, "output", &llm, &repo).await;

        match result.verdict {
            CompletionVerdict::NoMatch {
                confidence,
                reason,
            } => {
                assert_eq!(confidence, None);
                assert_eq!(reason, NoMatchReason::LlmFailed);
            }
            _ => unreachable!(),
        }
        assert!(result.raw_reply.is_none());
    }

    // ─── evaluate_completion_policy: stale-check guard ────────────────
    //
    // The pivotal P2-2 behavior: if the def's version changed between the
    // pre-call read and the post-call reload, the verdict is discarded.

    #[tokio::test]
    async fn evaluate_returns_stale_when_version_changed_during_call() {
        // Start at version 1; the repo will report version 2 on reload.
        let def = ai_def("all tests pass", 0.8);
        let repo_inner = Arc::new(StaleCheckRepo::with(&def));
        let repo: Arc<dyn AutomationRepository> = repo_inner.clone();
        let run = run_ctx();

        // Simulate a concurrent edit landing while the (mocked, instant) LLM
        // call is in flight: bump the stored payload's version before the
        // evaluator's stale-check reads it. We do this by invoking through a
        // wrapper that bumps the version after the LLM replies.
        struct BumpingLlm {
            inner: CannedLlm,
            repo: Arc<StaleCheckRepo>,
        }
        #[async_trait::async_trait]
        impl CompletionLlmCall for BumpingLlm {
            async fn invoke(&self, prompt: &str) -> Result<String, String> {
                let r = self.inner.invoke(prompt).await;
                // The "edit" lands right after the LLM returns, before the
                // evaluator's stale-check reload — exactly the race the guard
                // protects against.
                self.repo.set_version(2).await;
                r
            }
        }
        let bumping = BumpingLlm {
            inner: CannedLlm::ok("CONFIDENCE: 0.99"),
            repo: repo_inner.clone(),
        };

        let result =
            evaluate_completion_policy(&def, &run, "output", &bumping, &repo).await;

        assert!(result.is_stale(), "expected Stale, got {:?}", result.verdict);
        match result.verdict {
            CompletionVerdict::Stale {
                expected_version,
                current_version,
            } => {
                assert_eq!(expected_version, 1);
                assert_eq!(current_version, 2);
            }
            _ => unreachable!(),
        }
        // Even a high-confidence Match is discarded when stale — the verdict
        // was computed against a `stop_when` that's no longer active.
    }

    #[tokio::test]
    async fn evaluate_returns_stale_when_def_cannot_be_reloaded() {
        let def = ai_def("all tests pass", 0.8);
        let repo_inner = Arc::new(StaleCheckRepo::with(&def));
        let repo: Arc<dyn AutomationRepository> = repo_inner.clone();
        let run = run_ctx();

        // A wrapper that drops the payload after the LLM call — simulates a
        // def deletion during the call (get_def returns None).
        struct DroppingLlm {
            repo: Arc<StaleCheckRepo>,
        }
        #[async_trait::async_trait]
        impl CompletionLlmCall for DroppingLlm {
            async fn invoke(&self, _prompt: &str) -> Result<String, String> {
                self.repo.drop_payload().await;
                Ok("CONFIDENCE: 0.95".to_string())
            }
        }
        let dropping = DroppingLlm { repo: repo_inner };

        let result =
            evaluate_completion_policy(&def, &run, "output", &dropping, &repo).await;

        // Stale-defensive: can't confirm unchanged → discard the verdict.
        assert!(result.is_stale(), "expected Stale, got {:?}", result.verdict);
    }

    // ─── evaluate_completion_policy: legacy def (no version field) ─────

    #[tokio::test]
    async fn evaluate_treats_legacy_def_without_version_as_unchanged() {
        // A def at the default version (1) whose stored payload omits the
        // `version` field (legacy serialization) is treated as unchanged.
        let mut def = ai_def("all tests pass", 0.8);
        def.version = 1; // start at the legacy default
        let repo_inner = Arc::new(StaleCheckRepo::with(&def));
        let repo: Arc<dyn AutomationRepository> = repo_inner.clone();
        let run = run_ctx();

        struct LegacyLlm {
            repo: Arc<StaleCheckRepo>,
        }
        #[async_trait::async_trait]
        impl CompletionLlmCall for LegacyLlm {
            async fn invoke(&self, _prompt: &str) -> Result<String, String> {
                // Remove the version field from the stored payload (legacy).
                self.repo.remove_version().await;
                Ok("CONFIDENCE: 0.95".to_string())
            }
        }
        let legacy = LegacyLlm { repo: repo_inner };

        let result =
            evaluate_completion_policy(&def, &run, "output", &legacy, &repo).await;

        assert!(
            result.is_match(),
            "legacy def (no version field) at default v1 should be treated as unchanged; got {:?}",
            result.verdict
        );
    }

    // ─── evaluate_completion_policy: non-AI policy routing ────────────

    #[tokio::test]
    async fn evaluate_returns_no_match_for_non_ai_policy() {
        // A def with the default ExitCodeZero policy should not be evaluated
        // here — defensive NoMatch (never complete a run via the wrong path).
        let def = AutomationDef::new(
            "plain".to_string(),
            "echo".to_string(),
            crate::definition::ScheduleType::Manual,
        );
        let repo = repo_for(&def);
        let llm = CannedLlm::ok("CONFIDENCE: 0.99");
        let run = run_ctx();

        let result =
            evaluate_completion_policy(&def, &run, "output", &llm, &repo).await;

        assert!(matches!(
            result.verdict,
            CompletionVerdict::NoMatch {
                reason: NoMatchReason::Unparseable,
                ..
            }
        ));
        // The LLM must not have been invoked for a non-AI policy.
        assert!(llm.prompt_seen().is_none());
    }

    // ─── CompletionResult helpers ─────────────────────────────────────

    #[test]
    fn completion_result_is_match_and_is_stale_helpers() {
        let m = CompletionResult {
            verdict: CompletionVerdict::Match { confidence: 0.9 },
            raw_reply: Some("x".into()),
        };
        assert!(m.is_match());
        assert!(!m.is_stale());

        let s = CompletionResult {
            verdict: CompletionVerdict::Stale {
                expected_version: 1,
                current_version: 2,
            },
            raw_reply: None,
        };
        assert!(!s.is_match());
        assert!(s.is_stale());
    }
}

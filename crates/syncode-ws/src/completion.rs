//! Production wiring for the AI-completion harness (P2-2 host side) and the
//! orchestration-backed automation run executor.
//!
//! The automation crate's [`syncode_automation::completion_harness`] provides a
//! bounded-queue + worker-pool harness that evaluates whether an automation's
//! `stop_when` condition is satisfied and disables matching automations. The
//! crate is provider-agnostic — it exposes two injectable seams:
//!
//! - [`LlmFn`] — a one-prompt → reply-text trait the harness wraps via
//!   [`ProviderCompletionLlm`] (which adapts `LlmFn` to `CompletionLlmCall`).
//! - [`CompletionDisableFn`] — the "persist the disable" callback.
//!
//! This module provides the `syncode-ws`-side implementations of both, plus the
//! [`build_automation_scheduler`] helper that wires them into the automation
//! [`Scheduler`] at WsState construction:
//!
//! - [`WsCompletionLlm`] delegates to [`crate::llm::invoke_llm_oneshot`] (the
//!   same provider-CLI one-shot path the LLM-backed RPCs use).
//! - [`WsCompletionDisableFn`] flips `enabled = false` and persists the def via
//!   the shared [`AutomationRepository`].
//! - [`OrchestrationRunExecutor`] (constructed here, lives in
//!   [`crate::orchestration_executor`]) is the `RunExecutor` impl that
//!   dispatches automation turns through [`ApplicationService`] — driving the
//!   provider adapter via the chat pipeline, capturing the assistant output
//!   into the run record. This is the MCode `dispatchRun` parity path.
//!
//! See [`build_automation_scheduler`] for the graceful-degradation + repo-root
//! decisions.

use std::sync::Arc;

use syncode_automation::completion_harness::{CompletionDisableFn, LlmFn};
use syncode_automation::{
    AutomationDef, CompletionHarness, CompletionResult, CompletionVerdict,
    InMemoryAutomationRepository, ProviderCompletionLlm, Scheduler,
};
use syncode_core::ports::AutomationRepository;
use syncode_orchestration::{ApplicationService, Orchestrator};
use syncode_provider::registry::create_by_id;

use crate::llm::{SharedAdapter, invoke_llm_oneshot};
use crate::orchestration_executor::OrchestrationRunExecutor;

/// Default provider id when `SYNCODE_DEFAULT_PROVIDER` is unset. Mirrors the
/// standalone server binary ([`crate::bin::server`]) so the library and the
/// binary resolve the same default — keeping the completion harness and the
/// chat pipeline on the same provider id by convention.
const DEFAULT_PROVIDER: &str = "claude";

// ─── WsCompletionLlm ───────────────────────────────────────────────────────

/// [`LlmFn`] impl backed by [`crate::llm::invoke_llm_oneshot`].
///
/// Holds a single [`SharedAdapter`] (constructed once from the default provider
/// id) and delegates each `call(prompt)` to a fresh one-shot spawn → session →
/// request → shutdown round trip. The adapter is a distinct instance from the
/// chat pipeline's adapter — the completion harness is a separate concern and
/// should not contend with live chat turns on the same provider subprocess.
///
/// A failed one-shot (missing CLI, spawn error, empty reply) surfaces as
/// `Err(String)`; the harness treats any error as a non-match (the run is not
/// completed — the next fire re-evaluates).
pub struct WsCompletionLlm {
    /// The provider-CLI adapter (unspawned; `invoke_llm_oneshot` spawns per call).
    adapter: SharedAdapter,
    /// The provider id this adapter was constructed for (used to label the
    /// `ProviderConfig` passed to `spawn`).
    provider_id: String,
    /// Optional model token override. `None` lets [`invoke_llm_oneshot`] pick
    /// its default (`"default"` — the adapter resolves the real model).
    model: Option<String>,
}

#[async_trait::async_trait]
impl LlmFn for WsCompletionLlm {
    async fn call(&self, prompt: &str) -> Result<String, String> {
        let outcome = invoke_llm_oneshot(
            &self.adapter,
            &self.provider_id,
            self.model.as_deref(),
            None,
            prompt,
        )
        .await?;
        Ok(outcome.text)
    }
}

// ─── WsCompletionDisableFn ─────────────────────────────────────────────────

/// [`CompletionDisableFn`] that flips `enabled = false` and persists the def.
///
/// Holds the shared [`AutomationRepository`] so the disable lands in the same
/// store the scheduler reads — the next due-evaluation cycle sees the disabled
/// def and skips it. Best-effort: a persist failure is logged and swallowed.
/// The harness contract guarantees a disable that didn't land is simply
/// re-evaluated on the next fire (it must never panic a worker).
///
/// The completion [`CompletionResult`] is logged at `info` (automation_id +
/// verdict). The `raw_reply` can be large, so it's emitted only at `debug`
/// (guarded by [`tracing::enabled!`] so the formatting is skipped entirely
/// when debug isn't active for this target).
pub struct WsCompletionDisableFn {
    /// The automation store shared with the scheduler.
    repo: Arc<dyn AutomationRepository>,
}

#[async_trait::async_trait]
impl CompletionDisableFn for WsCompletionDisableFn {
    async fn disable(&self, def: &AutomationDef, result: &CompletionResult) {
        let id = def.id.as_str();
        // The harness only invokes disable on a confident Match (≥ threshold),
        // but log any verdict shape for completeness. Confidence is cheap + useful.
        match &result.verdict {
            CompletionVerdict::Match { confidence } => {
                tracing::info!(
                    automation_id = %id,
                    confidence,
                    "completion harness disabling automation (verdict: Match)"
                );
            }
            _ => {
                tracing::info!(
                    automation_id = %id,
                    verdict = ?result.verdict,
                    "completion harness disabling automation"
                );
            }
        }
        // raw_reply can be large → DEBUG only, guarded so the (potentially big)
        // Debug format of the Option<String> is skipped when debug isn't enabled.
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                automation_id = %id,
                raw_reply = ?result.raw_reply,
                "completion raw model reply"
            );
        }
        // Flip enabled=false and persist (best-effort). `with_enabled` consumes
        // self, so clone first to keep the original def untouched for the caller.
        let updated = def.clone().with_enabled(false);
        match serde_json::to_value(&updated) {
            Ok(payload) => {
                if let Err(e) = self.repo.save_def(&id, payload).await {
                    tracing::warn!(
                        automation_id = %id,
                        error = %e,
                        "failed to persist disabled def — will re-evaluate on next fire"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    automation_id = %id,
                    error = %e,
                    "failed to serialize disabled def — disable not persisted"
                );
            }
        }
    }
}

// ─── Scheduler wiring ──────────────────────────────────────────────────────

/// Build the automation [`Scheduler`] for [`crate::WsState`], wiring the
/// [`OrchestrationRunExecutor`] (automation runs dispatch through the chat
/// pipeline) and — when the default provider is armable — the AI-completion
/// harness.
///
/// The [`InMemoryAutomationRepository`] is constructed once and shared between
/// the scheduler, the harness, and the disable fn — so a disable lands in the
/// store the scheduler reads on its next due-evaluation cycle.
///
/// # `orchestrator` handle
///
/// The orchestrator built by [`crate::bin::server`] (or `WsState::new_in_memory`
/// for tests) is wrapped in an [`ApplicationService`] and handed to the
/// [`OrchestrationRunExecutor`]. This is what makes `automation.runNow`
/// dispatch a real provider turn: the executor calls
/// `ApplicationService::create_thread` + `start_turn`, which routes through
/// the same pipeline as the chat path (`ProviderCommandReactor` → adapter →
/// stream → `TurnCompleted`). The finalized assistant text is captured into
/// the run record's `stdout`.
///
/// # Graceful degradation
///
/// If [`create_by_id`] returns `None` (the default provider id is not armable —
/// e.g. `SYNCODE_DEFAULT_PROVIDER` is set to an unknown id), the scheduler is
/// constructed WITHOUT the completion harness. Completion checks are simply
/// skipped; automations still run on schedule (and through the provider via
/// the orchestration executor when a provider is wired), they just never
/// auto-disable on an AI verdict. A `WARN` is logged so the operator knows.
///
/// Note: `create_by_id` only returns `None` for an *unknown* provider id. A
/// known id whose CLI binary is absent still returns `Some(adapter)` — the
/// adapter object is constructed without spawning. The actual CLI-absent case
/// surfaces later inside [`invoke_llm_oneshot`] as an `Err`, which the harness
/// treats as `NoMatch` (the run is not completed; the next fire re-evaluates).
///
/// # `repo_root` (P2-8 worktree isolation)
///
/// `.with_repo_root` is intentionally NOT wired here. [`crate::WsState`] has no
/// obvious, correct repo root at construction time — the read model may be
/// empty on cold start, and no config field pins a single project root (a WS
/// server can host multiple projects). Wiring a guessed path would silently
/// enable worktree isolation against the wrong directory. Worktree isolation
/// stays off by default; the P2-8 code path is still exercised in the
/// automation crate's own tests. A future caller holding a known project root
/// can rebuild the scheduler via `WsState::automation_scheduler` if needed.
pub fn build_automation_scheduler(orchestrator: Arc<Orchestrator>) -> Arc<Scheduler> {
    let repo: Arc<dyn AutomationRepository> = Arc::new(InMemoryAutomationRepository::new());
    // Orchestration-backed executor: drives each automation turn through the
    // chat pipeline (ApplicationService → ProviderCommandReactor → provider
    // adapter → stream → TurnCompleted). Captures the assistant output into
    // the run record's stdout. MCode `dispatchRun` parity.
    let service = Arc::new(ApplicationService::new(orchestrator));
    let executor: Arc<dyn syncode_core::ports::RunExecutor> =
        Arc::new(OrchestrationRunExecutor::new(service));

    let default_provider =
        std::env::var("SYNCODE_DEFAULT_PROVIDER").unwrap_or_else(|_| DEFAULT_PROVIDER.to_string());

    match create_by_id(&default_provider) {
        Some(adapter) => {
            let llm: Arc<dyn LlmFn> = Arc::new(WsCompletionLlm {
                adapter,
                provider_id: default_provider.clone(),
                model: None,
            });
            let completion_llm = Arc::new(ProviderCompletionLlm::new(llm));
            let disable_fn: Arc<dyn CompletionDisableFn> =
                Arc::new(WsCompletionDisableFn { repo: repo.clone() });
            let harness = Arc::new(CompletionHarness::start(
                repo.clone(),
                completion_llm,
                disable_fn,
            ));
            tracing::info!(
                provider = %default_provider,
                "automation completion harness armed (AI-evaluated automations will be evaluated)"
            );
            Arc::new(Scheduler::new_with_deps(repo, executor).with_completion_harness(harness))
        }
        None => {
            tracing::warn!(
                provider = %default_provider,
                "default provider not armable — automation completion harness disabled \
                 (automations run but AI-completion checks are skipped). Set \
                 SYNCODE_DEFAULT_PROVIDER to an available provider id to enable."
            );
            Arc::new(Scheduler::new_with_deps(repo, executor))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockLlmAdapter;
    use syncode_automation::definition::ScheduleType;
    use syncode_automation::policies::CompletionPolicy;
    use syncode_provider::{
        ProviderAdapter, ProviderAdapterError, ProviderCapability, ProviderConfig,
        ProviderResponse, ProviderStatus, ProviderStream, SessionContext,
    };

    /// Build a mock `SharedAdapter` backed by [`MockLlmAdapter`] (no subprocess).
    fn mock_shared(canned: &str) -> SharedAdapter {
        Arc::new(tokio::sync::RwLock::new(MockLlmAdapter::new(canned)))
    }

    /// A provider adapter whose `spawn` fails with `NotFound` — mirrors the
    /// `MissingCli` test double in `llm.rs`. Used to prove `WsCompletionLlm`
    /// surfaces spawn errors as `Err(String)` (the CLI-absent path).
    struct MissingCli;

    #[async_trait::async_trait]
    impl ProviderAdapter for MissingCli {
        fn provider_id(&self) -> &str {
            "claude"
        }
        fn capabilities(&self) -> Vec<ProviderCapability> {
            Vec::new()
        }
        fn status(&self) -> ProviderStatus {
            ProviderStatus::Disconnected
        }
        fn available_models(&self) -> Vec<String> {
            Vec::new()
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
        async fn start_session(
            &mut self,
            _: SessionContext,
        ) -> Result<String, ProviderAdapterError> {
            unreachable!()
        }
        async fn resume_session(&mut self, _: &str) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn stop_session(&mut self, _: &str) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn send_request(
            &self,
            _: syncode_provider::ProviderRequest,
        ) -> Result<ProviderResponse, ProviderAdapterError> {
            unreachable!()
        }
        fn event_stream(&self, _: &str) -> Result<ProviderStream, ProviderAdapterError> {
            unreachable!()
        }
        async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
            Ok(false)
        }
    }

    fn missing_cli_shared() -> SharedAdapter {
        Arc::new(tokio::sync::RwLock::new(MissingCli))
    }

    // ─── WsCompletionLlm ──────────────────────────────────────────────

    #[tokio::test]
    async fn ws_completion_llm_returns_canned_text() {
        // A mock adapter that replies with a fixed string → call() returns it.
        let llm = WsCompletionLlm {
            adapter: mock_shared("the model judged: all tests pass"),
            provider_id: "mock-llm".to_string(),
            model: None,
        };
        let text = llm.call("evaluate this run output").await.unwrap();
        assert_eq!(text, "the model judged: all tests pass");
    }

    #[tokio::test]
    async fn ws_completion_llm_surfaces_missing_cli_as_err() {
        // An adapter whose spawn fails with NotFound → call() returns Err with
        // an actionable hint (the harness treats this as NoMatch — run not
        // completed; the next fire re-evaluates).
        let llm = WsCompletionLlm {
            adapter: missing_cli_shared(),
            provider_id: "claude".to_string(),
            model: None,
        };
        let err = llm.call("evaluate this").await.expect_err("should error");
        assert!(err.contains("not found on PATH"), "got: {err}");
        assert!(err.contains("SYNCODE_CLAUDE_BIN"), "got: {err}");
    }

    // ─── WsCompletionDisableFn ────────────────────────────────────────

    /// Build an AI-evaluated automation def for the disable test.
    fn ai_evaluated_def() -> AutomationDef {
        let mut def = AutomationDef::new(
            "test-auto".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        );
        def.completion_policy = CompletionPolicy::AiEvaluated {
            stop_when: "all tests pass".to_string(),
            confidence_threshold: 0.8,
        };
        def
    }

    #[tokio::test]
    async fn disable_fn_flips_enabled_false_and_persists() {
        // disable() must flip enabled→false and persist via save_def so the
        // scheduler's next due-evaluation skips the def. Verified by re-reading
        // the def from the same repo.
        let repo: Arc<dyn AutomationRepository> = Arc::new(InMemoryAutomationRepository::new());
        let def = ai_evaluated_def();
        let id = def.id.as_str();
        // Seed the repo with the enabled def (as the scheduler would).
        repo.save_def(&id, serde_json::to_value(&def).unwrap())
            .await
            .unwrap();
        assert!(def.enabled, "sanity: def starts enabled");

        let disable_fn = WsCompletionDisableFn { repo: repo.clone() };
        let result = CompletionResult {
            verdict: CompletionVerdict::Match { confidence: 0.95 },
            raw_reply: Some("CONFIDENCE: 0.95".to_string()),
        };
        disable_fn.disable(&def, &result).await;

        // Re-fetch: enabled must be false (persisted).
        let stored = repo.get_def(&id).await.unwrap().unwrap();
        let stored_def: AutomationDef = serde_json::from_value(stored).unwrap();
        assert!(
            !stored_def.enabled,
            "disable must flip enabled to false and persist"
        );
        // The original def passed to disable is untouched (we cloned).
        assert!(def.enabled, "disable must not mutate the caller's def");
    }

    #[tokio::test]
    async fn disable_fn_swallows_persist_error_without_panicking() {
        // A repo whose save_def always errors → disable() logs + swallows,
        // never panics. (The harness contract: a failed disable is
        // re-evaluated on the next fire.)
        struct FailingRepo;
        #[async_trait::async_trait]
        impl AutomationRepository for FailingRepo {
            async fn save_def(
                &self,
                _: &str,
                _: serde_json::Value,
            ) -> Result<(), syncode_core::PortError> {
                Err(syncode_core::PortError::Internal("disk full".into()))
            }
            async fn get_def(
                &self,
                _: &str,
            ) -> Result<Option<serde_json::Value>, syncode_core::PortError> {
                Ok(None)
            }
            async fn list_defs(&self) -> Result<Vec<serde_json::Value>, syncode_core::PortError> {
                Ok(Vec::new())
            }
            async fn delete_def(&self, _: &str) -> Result<bool, syncode_core::PortError> {
                Ok(false)
            }
            async fn save_run(&self, _: serde_json::Value) -> Result<(), syncode_core::PortError> {
                Ok(())
            }
            async fn get_run(
                &self,
                _: &str,
            ) -> Result<Option<serde_json::Value>, syncode_core::PortError> {
                Ok(None)
            }
            async fn list_runs(
                &self,
                _: &str,
            ) -> Result<Vec<serde_json::Value>, syncode_core::PortError> {
                Ok(Vec::new())
            }
            async fn advance_next_run_at(
                &self,
                _: &str,
                _: Option<String>,
            ) -> Result<(), syncode_core::PortError> {
                Ok(())
            }
        }

        let repo: Arc<dyn AutomationRepository> = Arc::new(FailingRepo);
        let disable_fn = WsCompletionDisableFn { repo: repo.clone() };
        let def = ai_evaluated_def();
        let result = CompletionResult {
            verdict: CompletionVerdict::Match { confidence: 0.9 },
            raw_reply: None,
        };
        // Must not panic — best-effort, error swallowed.
        disable_fn.disable(&def, &result).await;
    }
}

//! Supervised agent workflow orchestrator — the sequential pipeline that drives
//! an [`AgentState`] from `Initialization` through `Completed`, with failure
//! routing at every step.
//!
//! This implements task **P1-4** (see `docs/PRD-REMAINING-GAPS.md` §6). It
//! composes the lightweight wrappers from `syncode-core::agent`:
//! - [`AgentState::new`] — step 1, initialization
//! - [`MemoryProvider::retrieve_context`] — step 2, context grounding
//! - [`execute_step`] — steps 3 & 4, planning + execution
//! - [`run_output_guardrails`] — step 5, output validation
//! - [`MemoryProvider::persist_interaction`] — step 6, persistence
//!
//! The plan/execute steps are abstracted behind the [`WorkflowExecutor`] trait
//! so the orchestrator is fully unit-testable without a real provider (the
//! concrete `ProviderAdapter` integration arrives in P1-5).
//!
//! # Pipeline
//!
//! ```text
//! Initialization → Context → Planning → Execution → Guardrails → Persistence
//!                      │          │           │
//!                      └──────────┴───────────┴──→ [Failed]  (failure routing)
//! ```
//!
//! Every step that can fail routes through [`handle_workflow_failure`], which
//! marks the state `Failed` and appends a diagnostic log entry before the
//! error is propagated to the caller via `?`.

use syncode_core::agent::{
    execute_step, run_output_guardrails, AgentState, StepResult, WorkflowError,
};
use syncode_memory::MemoryProvider;

/// Provider tag recorded alongside persisted interactions.
///
/// Used as the `provider` argument to [`MemoryProvider::persist_interaction`].
/// P1-5 will replace this with the actual provider id from the
/// `ProviderAdapter`; for now a stable sentinel keeps the persisted rows
/// attributable to the workflow orchestrator.
const WORKFLOW_PROVIDER_TAG: &str = "workflow-orchestrator";

/// Token count recorded for orchestrator-produced interactions.
///
/// The orchestrator itself does not measure tokens (that is the provider's
/// responsibility in P1-5); a fixed, documented sentinel makes the persisted
/// row self-describing and easy to filter out of token accounting until a real
/// provider supplies an accurate count.
const WORKFLOW_PLACEHOLDER_TOKENS: u32 = 0;

/// Abstraction over the two provider-bound phases of the workflow.
///
/// Both methods are **synchronous** in P1-4 so the pipeline can compose cleanly
/// with [`execute_step`] (whose action closure is `FnOnce() -> Result<...>`).
/// P1-5 will supply a concrete implementation backed by a `ProviderAdapter`;
/// if that integration needs async, an async-aware step variant can be added
/// without changing this trait's call sites in the orchestrator.
///
/// # Contract
///
/// - [`plan`](WorkflowExecutor::plan) receives the original task plus the
///   retrieved memory context and returns a plan string.
/// - [`execute`](WorkflowExecutor::execute) receives the plan string produced
///   by `plan` and returns the raw execution output.
///
/// Errors are reported as [`WorkflowError`] so they flow through the shared
/// failure-routing sink unchanged.
pub trait WorkflowExecutor: Send + Sync {
    /// Generate a plan for `task`, grounded in the retrieved `context`.
    fn plan(&self, task: &str, context: &str) -> Result<String, WorkflowError>;

    /// Execute `plan` and return the raw output string.
    fn execute(&self, plan: &str) -> Result<String, WorkflowError>;
}

/// Run the full supervised agent workflow.
///
/// Sequential pipeline (each step's failure routes the state to `Failed` and
/// propagates the error via `?`):
///
/// 1. **Initialization** — [`AgentState::new`].
/// 2. **Context** — `memory.retrieve_context(user_id, initial_task)`. The
///    returned context is stored in `state.memory.ephemeral["context"]` so
///    downstream steps (and observers) can inspect what grounded the plan.
/// 3. **Planning** — [`execute_step`] wrapping `executor.plan(...)`. The
///    plan is stored in `state.memory.ephemeral["plan"]`.
/// 4. **Execution** — [`execute_step`] wrapping `executor.execute(plan)`.
/// 5. **Guardrails** — [`run_output_guardrails`] validates the execution
///    output (rejects empty / whitespace-only payloads).
/// 6. **Persistence** — `state.mark_completed()`, append the success log, then
///    `memory.persist_interaction(...)`. Persistence is **best-effort**: a
///    failure to persist is logged but does not unwind a successfully
///    completed workflow (the output is already produced and validated).
///
/// On success the final [`AgentState`] is returned (current_step = `Completed`,
/// `is_completed = true`). On failure the state is left in the `Failed` step
/// and the [`WorkflowError`] is returned — the state is **not** returned on
/// the error path because every routing step has already mutated it in place
/// and callers can observe the final shape by cloning before the call; the
/// error itself carries the diagnostic.
pub async fn execute_workflow(
    user_id: &str,
    workflow_id: &str,
    initial_task: &str,
    memory: &dyn MemoryProvider,
    executor: &dyn WorkflowExecutor,
) -> Result<AgentState, WorkflowError> {
    // 1. Initialization — construct the deterministic state frame.
    let mut state = AgentState::new(workflow_id, user_id, initial_task);
    state.log("[Workflow] Step 1/6: Initialization complete");

    // 2. Context retrieval (memory grounding).
    state.log("[Workflow] Step 2/6: Retrieving context");
    let context = memory.retrieve_context(user_id, initial_task).await;
    state
        .memory
        .set_ephemeral("context", context.as_str());
    let context = state.memory.ephemeral["context"].clone();
    state.log("[Workflow] Context grounded");

    // Capture the task for the closure (closures borrow `executor` + need the
    // owned `context` / `plan` values; we clone the strings they reference so
    // the closure is `FnOnce` and `move`-free where possible).
    let task = state.initial_task.clone();

    // 3. Planning — execute_step advances current_step and routes failures.
    let plan_result =
        execute_step("Planning", || executor.plan(&task, &context), &mut state).await?;
    let StepResult::Success(plan) = plan_result;
    state.memory.set_ephemeral("plan", plan.as_str());

    // 4. Execution.
    let exec_result =
        execute_step("Execution", || executor.execute(&plan), &mut state).await?;
    let StepResult::Success(execution_output) = exec_result;

    // 5. Guardrails — validate the execution output before committing.
    let validated = run_output_guardrails(&execution_output, &mut state)?;

    // 6. Persistence — mark complete, then best-effort persist.
    state.mark_completed();
    state.memory.append_summary(format!(
        "Completed workflow for task: {task}"
    ));
    state.log(
        "[Workflow] Step 6/6: Workflow completed successfully with zero drift.".to_string(),
    );

    if let Err(persist_err) = memory
        .persist_interaction(
            user_id,
            &task,
            &validated,
            WORKFLOW_PROVIDER_TAG,
            WORKFLOW_PLACEHOLDER_TOKENS,
        )
        .await
    {
        // Best-effort: the workflow has already succeeded and its output is
        // validated. Surface the persistence failure via tracing so it is
        // observable without unwinding a completed run.
        tracing::warn!(
            error = %persist_err,
            "workflow interaction could not be persisted; workflow still considered completed"
        );
        state.log(format!(
            "[Workflow] Warning: persist_interaction failed ({persist_err}); output retained in-memory"
        ));
    } else {
        state.log("[Workflow] Interaction persisted to memory");
    }

    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use syncode_core::agent::WorkflowStep;
    use syncode_memory::MemoryProviderError;
    use std::sync::{Arc, Mutex};

    // ------------------------------------------------------------------
    // Test doubles
    // ------------------------------------------------------------------

    /// A [`MemoryProvider`] mock that records every call and returns canned
    /// context. Wrapped in `Arc<Mutex<…>>` so assertions can inspect the
    /// recorded calls after the workflow runs.
    #[derive(Default, Clone)]
    struct MockMemory {
        inner: Arc<Mutex<MockMemoryInner>>,
    }

    #[derive(Default)]
    struct MockMemoryInner {
        retrieved_for: Vec<String>,
        persisted: Vec<(String, String, String, String, u32)>,
        context_to_return: String,
        persist_should_fail: bool,
    }

    impl MockMemory {
        fn new_returning(context: impl Into<String>) -> Self {
            let inner = MockMemoryInner {
                context_to_return: context.into(),
                ..MockMemoryInner::default()
            };
            Self {
                inner: Arc::new(Mutex::new(inner)),
            }
        }

        fn failing_persist(context: impl Into<String>) -> Self {
            let me = Self::new_returning(context);
            me.inner.lock().unwrap().persist_should_fail = true;
            me
        }

        fn persisted_calls(&self) -> Vec<(String, String, String, String, u32)> {
            self.inner.lock().unwrap().persisted.clone()
        }

        fn retrieved_calls(&self) -> Vec<String> {
            self.inner.lock().unwrap().retrieved_for.clone()
        }
    }

    #[async_trait]
    impl MemoryProvider for MockMemory {
        async fn retrieve_context(&self, user_id: &str, _query: &str) -> String {
            self.inner
                .lock()
                .unwrap()
                .retrieved_for
                .push(user_id.to_string());
            self.inner.lock().unwrap().context_to_return.clone()
        }

        async fn persist_interaction(
            &self,
            user_id: &str,
            prompt: &str,
            response: &str,
            provider: &str,
            tokens: u32,
        ) -> Result<(), MemoryProviderError> {
            let mut inner = self.inner.lock().unwrap();
            if inner.persist_should_fail {
                // PoolTimedOut is a unit variant — a clean way to synthesise a
                // sqlx::Error in tests without a real database.
                return Err(MemoryProviderError::Store(sqlx::Error::PoolTimedOut));
            }
            inner.persisted.push((
                user_id.to_string(),
                prompt.to_string(),
                response.to_string(),
                provider.to_string(),
                tokens,
            ));
            Ok(())
        }
    }

    /// A [`WorkflowExecutor`] mock with configurable plan/execute outcomes.
    #[derive(Clone)]
    struct MockExecutor {
        plan_output: Result<String, WorkflowError>,
        execute_output: Result<String, WorkflowError>,
    }

    impl WorkflowExecutor for MockExecutor {
        fn plan(&self, _task: &str, _context: &str) -> Result<String, WorkflowError> {
            self.plan_output.clone()
        }

        fn execute(&self, _plan: &str) -> Result<String, WorkflowError> {
            self.execute_output.clone()
        }
    }

    // ------------------------------------------------------------------
    // Happy path
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn execute_workflow_happy_path_completes_and_persists() {
        let memory = MockMemory::new_returning("prior context fragment");
        let executor = MockExecutor {
            plan_output: Ok("1. do a\n2. do b".to_string()),
            execute_output: Ok("final result".to_string()),
        };

        let state = execute_workflow("user-1", "wf-1", "refactor module", &memory, &executor)
            .await
            .expect("happy path should complete");

        // Terminal state.
        assert_eq!(state.current_step, WorkflowStep::Completed);
        assert!(state.is_completed);

        // Context and plan were grounded in ephemeral memory.
        assert_eq!(
            state.memory.get_ephemeral("context"),
            Some("prior context fragment")
        );
        assert_eq!(
            state.memory.get_ephemeral("plan"),
            Some("1. do a\n2. do b")
        );

        // Every pipeline phase left a log entry.
        let logs = state.execution_logs.join("\n");
        assert!(logs.contains("Initialization complete"), "logs: {logs}");
        assert!(logs.contains("Retrieving context"), "logs: {logs}");
        assert!(logs.contains("[Harness] Starting step: Planning"), "logs: {logs}");
        assert!(
            logs.contains("[Harness] Step Planning completed"),
            "logs: {logs}"
        );
        assert!(
            logs.contains("[Harness] Starting step: Execution"),
            "logs: {logs}"
        );
        assert!(logs.contains("zero drift"), "logs: {logs}");
        assert!(logs.contains("persisted to memory"), "logs: {logs}");

        // Memory was queried and the interaction persisted exactly once.
        assert_eq!(memory.retrieved_calls(), vec!["user-1".to_string()]);
        let persisted = memory.persisted_calls();
        assert_eq!(persisted.len(), 1, "expected exactly one persist call");
        let (uid, prompt, response, provider, tokens) = &persisted[0];
        assert_eq!(uid, "user-1");
        assert_eq!(prompt, "refactor module");
        assert_eq!(response, "final result");
        assert_eq!(provider, WORKFLOW_PROVIDER_TAG);
        assert_eq!(*tokens, WORKFLOW_PLACEHOLDER_TOKENS);
    }

    // ------------------------------------------------------------------
    // Failure routing — planning
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn execute_workflow_planning_failure_routes_to_failed_and_skips_persist() {
        let memory = MockMemory::new_returning("ctx");
        let executor = MockExecutor {
            plan_output: Err(WorkflowError::ProviderError("plan model down".into())),
            execute_output: Ok("unused".to_string()),
        };

        let err = execute_workflow("user-2", "wf-2", "do thing", &memory, &executor)
            .await
            .expect_err("planning failure should propagate");

        assert!(
            matches!(err, WorkflowError::ProviderError(ref m) if m == "plan model down"),
            "unexpected error variant: {err:?}"
        );

        // We can't inspect the final state on the error path (it's consumed),
        // but we CAN assert the side effects that matter: no persistence.
        assert!(
            memory.persisted_calls().is_empty(),
            "persistence must be skipped when planning fails"
        );
        // Context retrieval happened before planning, so it was recorded.
        assert_eq!(memory.retrieved_calls().len(), 1);
    }

    // ------------------------------------------------------------------
    // Failure routing — execution
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn execute_workflow_execution_failure_routes_to_failed_and_skips_persist() {
        let memory = MockMemory::new_returning("ctx");
        let executor = MockExecutor {
            plan_output: Ok("a plan".to_string()),
            execute_output: Err(WorkflowError::StepFailed("execution crashed".into())),
        };

        let err = execute_workflow("user-3", "wf-3", "do thing", &memory, &executor)
            .await
            .expect_err("execution failure should propagate");

        assert!(
            matches!(err, WorkflowError::StepFailed(ref m) if m == "execution crashed"),
            "unexpected error variant: {err:?}"
        );
        assert!(
            memory.persisted_calls().is_empty(),
            "persistence must be skipped when execution fails"
        );
    }

    // ------------------------------------------------------------------
    // Failure routing — guardrails
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn execute_workflow_guardrail_violation_routes_to_failed_and_skips_persist() {
        let memory = MockMemory::new_returning("ctx");
        // Plan succeeds, but the execution output is empty → guardrail rejects.
        let executor = MockExecutor {
            plan_output: Ok("a plan".to_string()),
            execute_output: Ok("   \n\t ".to_string()),
        };

        let err = execute_workflow("user-4", "wf-4", "do thing", &memory, &executor)
            .await
            .expect_err("empty output should fail guardrails");

        assert!(
            matches!(err, WorkflowError::GuardrailViolation(ref m) if m.contains("Empty payload")),
            "expected guardrail violation, got: {err:?}"
        );
        assert!(
            memory.persisted_calls().is_empty(),
            "persistence must be skipped when guardrails fail"
        );
    }

    // ------------------------------------------------------------------
    // Persistence is best-effort: a persist failure does not unwind a
    // completed workflow.
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn execute_workflow_completes_even_when_persist_fails() {
        let memory = MockMemory::failing_persist("ctx");
        let executor = MockExecutor {
            plan_output: Ok("plan".to_string()),
            execute_output: Ok("good output".to_string()),
        };

        let state = execute_workflow("user-5", "wf-5", "do thing", &memory, &executor)
            .await
            .expect("persist failure must not fail a completed workflow");

        // Workflow still reports completion.
        assert_eq!(state.current_step, WorkflowStep::Completed);
        assert!(state.is_completed);

        // The persist failure was logged (best-effort observability).
        let logs = state.execution_logs.join("\n");
        assert!(
            logs.contains("persist_interaction failed"),
            "persist failure should be logged; logs: {logs}"
        );
    }

    // ------------------------------------------------------------------
    // P1-6: end-to-end with a REAL SqliteMemoryStore
    // ------------------------------------------------------------------
    //
    // The tests above exercise `execute_workflow` with an in-process mock
    // memory. P1-6's contract is that the retrieve→plan→execute→persist
    // pipeline works end-to-end against the real SQLite-backed
    // [`syncode_memory::SqliteMemoryStore`] — proving the trait wiring, the
    // SQL round-trip, and the markdown formatting all compose inside the
    // orchestrator. These two tests are the integration acceptance gate for
    // the memory-bound phases of the workflow.

    /// (1) Happy path: the workflow retrieves prior context from a real
    /// SQLite store (seeded before the run) and persists the new interaction
    /// so a *subsequent* run sees it.
    ///
    /// This proves the full chain:
    /// - `retrieve_context` reads the seeded row and grounds the plan in it
    ///   (verified via the executor receiving the prior prompt text).
    /// - `persist_interaction` writes the new prompt/response so a fresh
    ///   store instance reading the same DB sees the just-completed turn.
    #[tokio::test]
    async fn execute_workflow_end_to_end_with_sqlite_roundtrips_context() {
        use syncode_memory::SqliteMemoryStore;

        // In-memory SQLite — private pool, survives across the two calls
        // (retrieve + persist) within this test.
        let store = SqliteMemoryStore::new_in_memory()
            .await
            .expect("in-memory store should init");

        // Seed one prior interaction so retrieve_context has something to
        // return (rather than the NO_PRIOR_CONTEXT sentinel).
        store
            .persist_interaction("user-e2e", "earlier question", "earlier answer", "claude", 10)
            .await
            .expect("seed persist");

        // Capture the context the executor received so we can assert the
        // seeded row was retrieved and passed through.
        struct CtxExecutor {
            seen: Arc<Mutex<Vec<String>>>,
        }
        impl WorkflowExecutor for CtxExecutor {
            fn plan(&self, _task: &str, context: &str) -> Result<String, WorkflowError> {
                self.seen.lock().unwrap().push(context.to_string());
                Ok("plan-from-prior-context".to_string())
            }
            fn execute(&self, _plan: &str) -> Result<String, WorkflowError> {
                Ok("final-output".to_string())
            }
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let executor = CtxExecutor {
            seen: Arc::clone(&seen),
        };

        let state = execute_workflow(
            "user-e2e",
            "wf-e2e-1",
            "follow up on the earlier work",
            &store,
            &executor,
        )
        .await
        .expect("e2e happy path should complete");

        // Terminal state — the workflow completed through all 6 steps.
        assert_eq!(state.current_step, WorkflowStep::Completed);
        assert!(state.is_completed);

        // The plan step received the retrieved prior context (the seeded
        // prompt text), proving retrieve_context → execute_step plumbing
        // works against the real store.
        let captured = seen.lock().unwrap().clone();
        assert_eq!(captured.len(), 1, "plan should be called exactly once");
        assert!(
            captured[0].contains("earlier question"),
            "retrieved context must contain the seeded prompt; got: {}",
            captured[0]
        );
        assert!(
            captured[0].contains("earlier answer"),
            "retrieved context must contain the seeded response; got: {}",
            captured[0]
        );

        // The ephemeral context slot mirrors what was retrieved.
        assert!(
            state
                .memory
                .get_ephemeral("context")
                .is_some_and(|c| c.contains("earlier question")),
            "ephemeral['context'] must reflect the retrieved store data"
        );

        // The persist step wrote the new turn — a fresh retrieve now returns
        // BOTH the seed and the just-completed interaction, most-recent first.
        let after = store.retrieve_context("user-e2e", "").await;
        assert!(
            after.contains("follow up on the earlier work"),
            "persisted prompt missing from post-run retrieval: {after}"
        );
        assert!(
            after.contains("final-output"),
            "persisted response missing from post-run retrieval: {after}"
        );
        assert!(
            after.contains("earlier question"),
            "seed row must still be present after the run: {after}"
        );
        // Most-recent first: the new interaction (just persisted) should
        // appear before the seed.
        let new_pos = after
            .find("follow up")
            .expect("new interaction present");
        let seed_pos = after
            .find("earlier question")
            .expect("seed interaction present");
        assert!(
            new_pos < seed_pos,
            "new interaction must be ordered before the seed (most-recent first)"
        );
    }

    /// (2) Empty-store path: the workflow still completes when the SQLite
    /// store has no prior interactions for the user. `retrieve_context`
    /// returns the `NO_PRIOR_CONTEXT` sentinel, the workflow grounds the plan
    /// in that sentinel (no crash, no empty-string special-case at the
    /// orchestrator boundary), and the new interaction is persisted — proving
    /// the first-turn cold-start works end-to-end.
    #[tokio::test]
    async fn execute_workflow_sqlite_cold_start_persists_first_interaction() {
        use syncode_memory::{SqliteMemoryStore, NO_PRIOR_CONTEXT};

        // Brand-new in-memory store — no rows for this user.
        let store = SqliteMemoryStore::new_in_memory()
            .await
            .expect("in-memory store should init");

        // Executor that asserts it received the sentinel on a cold start.
        struct ColdStartExecutor {
            received_context: Arc<Mutex<Option<String>>>,
        }
        impl WorkflowExecutor for ColdStartExecutor {
            fn plan(&self, _task: &str, context: &str) -> Result<String, WorkflowError> {
                *self.received_context.lock().unwrap() = Some(context.to_string());
                Ok("cold-start-plan".to_string())
            }
            fn execute(&self, _plan: &str) -> Result<String, WorkflowError> {
                Ok("cold-start-output".to_string())
            }
        }

        let received = Arc::new(Mutex::new(None));
        let executor = ColdStartExecutor {
            received_context: Arc::clone(&received),
        };

        let state = execute_workflow(
            "cold-user",
            "wf-cold",
            "very first question",
            &store,
            &executor,
        )
        .await
        .expect("cold-start happy path should complete");

        assert_eq!(state.current_step, WorkflowStep::Completed);
        assert!(state.is_completed);

        // On a cold start the orchestrator receives the sentinel, not an
        // empty string — proving the store's emptiness contract holds through
        // the workflow boundary.
        let ctx = received.lock().unwrap().clone().expect("plan was called");
        assert_eq!(
            ctx, NO_PRIOR_CONTEXT,
            "cold start must surface the no-prior-context sentinel"
        );

        // The first interaction is now persisted — a second retrieve returns
        // it (no longer the sentinel).
        let after = store.retrieve_context("cold-user", "").await;
        assert_ne!(
            after, NO_PRIOR_CONTEXT,
            "after one run the store must have real context, not the sentinel"
        );
        assert!(
            after.contains("very first question"),
            "first-turn prompt must be persisted: {after}"
        );
        assert!(
            after.contains("cold-start-output"),
            "first-turn response must be persisted: {after}"
        );
        assert!(
            state
                .execution_logs
                .join("\n")
                .contains("Interaction persisted to memory"),
            "the persist-success log line must be present"
        );
    }
}

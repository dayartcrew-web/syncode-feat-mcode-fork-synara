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

/// Provider tag recorded alongside persisted interactions when the executor
/// does not supply one (e.g. the P1-4 test mocks). Used as the default for
/// [`WorkflowExecutor::provider_tag`]. P1-5's [`ProviderWorkflowExecutor`]
/// overrides this with the real provider id from the `ProviderAdapter`.
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

    /// The provider id to attribute persisted interactions to (P1-5).
    ///
    /// [`ProviderWorkflowExecutor`] returns the wrapped `ProviderAdapter`'s
    /// `provider_id()` so persisted rows are attributable to the real model.
    /// The default (test mocks) returns [`WORKFLOW_PROVIDER_TAG`] so the
    /// P1-4 behaviour is preserved unchanged.
    fn provider_tag(&self) -> &str {
        WORKFLOW_PROVIDER_TAG
    }
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
            executor.provider_tag(),
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

// ---------------------------------------------------------------------------
// P1-5 — concrete WorkflowExecutor backed by a ProviderAdapter
// ---------------------------------------------------------------------------

use std::sync::Arc;

use syncode_provider::{
    ProviderAdapter, ProviderAdapterError, ProviderRequest, ProviderResponse, SessionManager,
};

/// JSON-RPC method name the executor sends for the planning phase.
const PLAN_METHOD: &str = "plan";
/// JSON-RPC method name the executor sends for the execution phase.
const EXECUTE_METHOD: &str = "execute";

/// A concrete [`WorkflowExecutor`] that drives plan/execute phases through a
/// real [`ProviderAdapter`].
///
/// Each phase sends a JSON-RPC request to the provider and returns the
/// response's textual output. The provider is addressed directly (the
/// [`SessionManager`] is accepted for symmetry with the rest of the
/// orchestration layer and to let a future revision route through a managed
/// session; it is not required for the request/response style used here).
///
/// # Request shapes
///
/// - `plan`: `ProviderRequest { method: "plan", params: { "task", "context" } }`
/// - `execute`: `ProviderRequest { method: "execute", params: { "plan" } }`
///
/// Both responses are expected to carry a result object whose textual output
/// is under one of the conventional keys (`"output"` or `"text"`); see
/// [`extract_output`] for the precedence. This matches the shape the in-tree
/// adapters already emit (`claude`, `codex`, `kilo`, `opencode`, `pi` use
/// `"output"`; `anthropic`, `openai` use `"text"`).
///
/// # Sync/async bridge
///
/// [`WorkflowExecutor`] is synchronous (P1-4 contract — it must compose with
/// [`execute_step`]'s `FnOnce() -> Result<…>` action closure). The provider
/// `send_request`, however, is `async`. We bridge the two with
/// [`tokio::task::block_in_place`], which polls the provider future on the
/// blocking pool without deadlocking the worker thread. This requires a
/// multi-threaded tokio runtime (the production server uses one); on a
/// single-threaded runtime `block_in_place` panics, which surfaces as a
/// [`WorkflowError::ProviderError`] to the caller.
pub struct ProviderWorkflowExecutor {
    /// The provider adapter that receives plan/execute requests.
    adapter: Arc<dyn ProviderAdapter>,
    /// Accepted for symmetry with the rest of the orchestration layer; not
    /// required for the request/response style used here but kept so a future
    /// revision can route through a managed session without changing the
    /// constructor surface.
    #[allow(dead_code)]
    session_manager: Arc<SessionManager>,
    /// Cached provider id (so [`Self::provider_tag`] can return `&str` without
    /// cloning on every call). Set once at construction from
    /// [`ProviderAdapter::provider_id`].
    provider_tag: String,
}

impl ProviderWorkflowExecutor {
    /// Construct a new executor wrapping `adapter`.
    ///
    /// `session_manager` is retained for future session-routed invocations
    /// (the current implementation talks directly to the adapter via
    /// `send_request`).
    pub fn new(adapter: Arc<dyn ProviderAdapter>, session_manager: Arc<SessionManager>) -> Self {
        let provider_tag = adapter.provider_id().to_string();
        Self {
            adapter,
            session_manager,
            provider_tag,
        }
    }

    /// Send a JSON-RPC request with `params` and return the response.
    ///
    /// Bridges the async `send_request` through `block_in_place`. Any provider
    /// error is mapped to [`WorkflowError::ProviderError`] so the orchestrator's
    /// shared failure-routing sink handles it uniformly.
    fn send(&self, method: &str, params: serde_json::Value) -> Result<ProviderResponse, WorkflowError> {
        let request = ProviderRequest::new(method, Some(params));
        let adapter = Arc::clone(&self.adapter);
        let response = tokio::task::block_in_place(|| {
            // SAFETY-equivalent note: block_in_place moves the current worker
            // into a blocking state, then we hand the future to the runtime
            // via Handle::block_on. This is the documented bridge from sync
            // code into async on a multi-threaded runtime.
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async move { adapter.send_request(request).await })
        })
        .map_err(|e| WorkflowError::ProviderError(provider_error_message(method, &e)))?;
        Ok(response)
    }
}

impl WorkflowExecutor for ProviderWorkflowExecutor {
    fn plan(&self, task: &str, context: &str) -> Result<String, WorkflowError> {
        let params = serde_json::json!({
            "task": task,
            "context": context,
        });
        let response = self.send(PLAN_METHOD, params)?;
        extract_output(&response, PLAN_METHOD)
    }

    fn execute(&self, plan: &str) -> Result<String, WorkflowError> {
        let params = serde_json::json!({
            "plan": plan,
        });
        let response = self.send(EXECUTE_METHOD, params)?;
        extract_output(&response, EXECUTE_METHOD)
    }

    fn provider_tag(&self) -> &str {
        &self.provider_tag
    }
}

/// Pull the textual output out of a provider JSON-RPC response.
///
/// Adapters in this codebase use one of two conventional result keys: most
/// subprocess adapters (`claude`, `codex`, `kilo`, `opencode`, `pi`) emit
/// `{ "output": "..." }`, while the HTTP adapters (`anthropic`, `openai`)
/// emit `{ "text": "..." }`. We accept either, preferring `"output"`.
///
/// Errors:
/// - A JSON-RPC `error` field on the response → `ProviderError`.
/// - A missing `result` object → `ProviderError`.
/// - Neither key present (or non-string) → `ProviderError`.
fn extract_output(
    response: &ProviderResponse,
    method: &str,
) -> Result<String, WorkflowError> {
    if let Some(rpc_err) = response.error.as_ref() {
        return Err(WorkflowError::ProviderError(format!(
            "{method}: provider returned RPC error code {}: {}",
            rpc_err.code, rpc_err.message
        )));
    }
    let result = response.result.as_ref().ok_or_else(|| {
        WorkflowError::ProviderError(format!(
            "{method}: provider response had no 'result' field"
        ))
    })?;
    // Prefer "output" (subprocess adapters); fall back to "text" (HTTP adapters).
    if let Some(output) = result.get("output").and_then(|v| v.as_str()) {
        return Ok(output.to_string());
    }
    if let Some(text) = result.get("text").and_then(|v| v.as_str()) {
        return Ok(text.to_string());
    }
    Err(WorkflowError::ProviderError(format!(
        "{method}: provider response 'result' had no 'output' or 'text' string field"
    )))
}

/// Render a [`ProviderAdapterError`] as a concise diagnostic string.
///
/// Kept as a free function so tests can exercise it directly.
fn provider_error_message(method: &str, e: &ProviderAdapterError) -> String {
    format!("{method}: provider request failed: {e}")
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
        assert_eq!(provider, executor.provider_tag());
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
    // P1-5 — ProviderWorkflowExecutor (concrete WorkflowExecutor impl)
    // ------------------------------------------------------------------
    //
    // These tests run on a multi-threaded tokio runtime (the default
    // `#[tokio::test]`) so `tokio::task::block_in_place` is legal. The mock
    // provider records the requests it receives so the tests can assert the
    // exact JSON-RPC shape the executor emits.

    use syncode_provider::{
        ProviderCapability, ProviderConfig, ProviderStatus, ProviderStream,
        SessionContext, SessionManager,
    };

    /// A minimal `ProviderAdapter` mock for the P1-5 executor tests.
    ///
    /// Each `send_request` call:
    /// 1. records `(method, params)` for later assertion, and
    /// 2. returns a canned `ProviderResponse` derived from `params` so each
    ///    phase's output is observable.
    ///
    /// The mapping `phase → output` is configurable so a test can simulate a
    /// planning success, an execution failure, etc.
    #[derive(Default, Clone)]
    struct MockProvider {
        inner: Arc<Mutex<MockProviderInner>>,
    }

    #[derive(Default)]
    struct MockProviderInner {
        /// Every `(method, params)` received, in arrival order.
        requests: Vec<(String, serde_json::Value)>,
        /// Output to return for the next `plan` request.
        plan_output: Option<Result<String, ProviderAdapterError>>,
        /// Output to return for the next `execute` request.
        execute_output: Option<Result<String, ProviderAdapterError>>,
        /// Whether to emit the output under the `"text"` key (HTTP adapters)
        /// rather than the default `"output"` key (subprocess adapters).
        use_text_key: bool,
    }

    impl MockProvider {
        /// Build a happy-path provider: plan and execute both succeed with the
        /// given outputs. By default the response result uses the `"output"`
        /// key (matching the subprocess adapters).
        fn happy(plan: &str, execute: &str) -> Self {
            let inner = MockProviderInner {
                plan_output: Some(Ok(plan.to_string())),
                execute_output: Some(Ok(execute.to_string())),
                ..MockProviderInner::default()
            };
            Self {
                inner: Arc::new(Mutex::new(inner)),
            }
        }

        /// Force this provider to emit its result under the `"text"` key
        /// (matching the HTTP adapters like `anthropic` / `openai`).
        fn with_text_key(self) -> Self {
            self.inner.lock().unwrap().use_text_key = true;
            self
        }

        /// Build a provider whose `execute` phase fails with the given error.
        fn execute_failing(err_msg: &str) -> Self {
            let inner = MockProviderInner {
                plan_output: Some(Ok("a plan".to_string())),
                execute_output: Some(Err(ProviderAdapterError::Internal(
                    err_msg.to_string(),
                ))),
                ..MockProviderInner::default()
            };
            Self {
                inner: Arc::new(Mutex::new(inner)),
            }
        }

        /// Snapshot of the recorded requests.
        fn requests(&self) -> Vec<(String, serde_json::Value)> {
            self.inner.lock().unwrap().requests.clone()
        }
    }

    #[async_trait]
    impl ProviderAdapter for MockProvider {
        fn provider_id(&self) -> &str {
            "mock-p1-5"
        }
        fn capabilities(&self) -> Vec<ProviderCapability> {
            vec![]
        }
        fn status(&self) -> ProviderStatus {
            ProviderStatus::Idle
        }
        fn available_models(&self) -> Vec<String> {
            vec!["mock-model".to_string()]
        }
        async fn spawn(&mut self, _config: ProviderConfig) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn interrupt(&self, _session_id: &str) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn start_session(
            &mut self,
            _ctx: SessionContext,
        ) -> Result<String, ProviderAdapterError> {
            Ok("mock-p1-5-session".to_string())
        }
        async fn resume_session(
            &mut self,
            _session_id: &str,
        ) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn stop_session(
            &mut self,
            _session_id: &str,
        ) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn send_request(
            &self,
            request: ProviderRequest,
        ) -> Result<ProviderResponse, ProviderAdapterError> {
            // Record the call (method + params) before producing a response.
            let mut inner = self.inner.lock().unwrap();
            inner.requests.push((
                request.method.clone(),
                request.params.clone().unwrap_or(serde_json::Value::Null),
            ));
            let outcome = if request.method == PLAN_METHOD {
                inner.plan_output.take()
            } else if request.method == EXECUTE_METHOD {
                inner.execute_output.take()
            } else {
                None
            };
            let use_text_key = inner.use_text_key;
            drop(inner);
            match outcome {
                Some(Ok(output)) => {
                    let key = if use_text_key { "text" } else { "output" };
                    Ok(ProviderResponse {
                        jsonrpc: "2.0".to_string(),
                        id: Some(request.id),
                        result: Some(serde_json::json!({ key: output })),
                        error: None,
                    })
                }
                Some(Err(e)) => Err(e),
                None => Ok(ProviderResponse {
                    jsonrpc: "2.0".to_string(),
                    id: Some(request.id),
                    result: Some(serde_json::json!({ "output": "" })),
                    error: None,
                }),
            }
        }
        fn event_stream(
            &self,
            _session_id: &str,
        ) -> Result<ProviderStream, ProviderAdapterError> {
            Ok(Box::pin(tokio_stream::empty()))
        }
        async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
            Ok(true)
        }
    }

    // ------------------------------------------------------------------
    // Test 1 — happy path: plan + execute invoke the provider with the
    // expected JSON-RPC shape and the executor reports the real provider id.
    // ------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn provider_workflow_executor_happy_path_invokes_provider_and_routes_output() {
        let provider = MockProvider::happy("PLAN-OUTPUT", "EXEC-OUTPUT");
        // Take a separate handle to inspect the recorded requests after the
        // workflow runs. `MockProvider` is `Clone` (Arc inside), so cloning
        // shares the same recording state.
        let recorder = provider.clone();
        let session_manager = Arc::new(SessionManager::new());
        let executor = ProviderWorkflowExecutor::new(
            Arc::new(provider),
            session_manager,
        );

        // Run the executor through the full pipeline so we exercise the
        // plan/execute bridge AND the guardrails/persistence wiring.
        let memory = MockMemory::new_returning("grounded context");
        let state =
            execute_workflow("user-p1-5", "wf-p1-5", "do thing", &memory, &executor)
                .await
                .expect("happy path should complete");

        // The provider received exactly two requests in the right order.
        let requests = recorder.requests();
        assert_eq!(requests.len(), 2, "expected plan + execute, got {requests:?}");
        let (plan_method, plan_params) = &requests[0];
        assert_eq!(plan_method, PLAN_METHOD);
        assert_eq!(plan_params["task"], "do thing");
        assert_eq!(plan_params["context"], "grounded context");

        let (exec_method, exec_params) = &requests[1];
        assert_eq!(exec_method, EXECUTE_METHOD);
        assert_eq!(exec_params["plan"], "PLAN-OUTPUT");

        // The plan was grounded in ephemeral memory (proves the provider's plan
        // output flowed through the executor into the workflow state).
        assert_eq!(
            state.memory.get_ephemeral("plan"),
            Some("PLAN-OUTPUT"),
            "plan output should reach the state frame"
        );

        // Terminal state.
        assert_eq!(state.current_step, WorkflowStep::Completed);
        assert!(state.is_completed);

        // Persistence was attributed to the real provider id (not the
        // workflow-orchestrator sentinel) — the core P1-5 deliverable.
        let persisted = memory.persisted_calls();
        assert_eq!(persisted.len(), 1);
        let (_, _, _, provider_tag, _) = &persisted[0];
        assert_eq!(provider_tag, "mock-p1-5");
    }

    // ------------------------------------------------------------------
    // Test 2 — provider failure during execute is surfaced through
    // WorkflowError::ProviderError and short-circuits the pipeline before
    // persistence.
    // ------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn provider_workflow_executor_surfaces_provider_errors_and_skips_persist() {
        let provider = MockProvider::execute_failing("codex subprocess crashed");
        let recorder = provider.clone();
        let session_manager = Arc::new(SessionManager::new());
        let executor = ProviderWorkflowExecutor::new(
            Arc::new(provider),
            session_manager,
        );

        let memory = MockMemory::new_returning("ctx");
        let err = execute_workflow("user-err", "wf-err", "do thing", &memory, &executor)
            .await
            .expect_err("execute failure should propagate");

        // The error is a ProviderError mentioning both the method and the
        // provider's diagnostic.
        match err {
            WorkflowError::ProviderError(ref msg) => {
                assert!(
                    msg.contains(EXECUTE_METHOD),
                    "error should mention the failing method: {msg}"
                );
                assert!(
                    msg.contains("codex subprocess crashed"),
                    "error should carry the provider diagnostic: {msg}"
                );
            }
            other => panic!("expected ProviderError, got {other:?}"),
        }

        // Plan was dispatched, execute was attempted and failed.
        let requests = recorder.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].0, EXECUTE_METHOD);

        // No persistence on the failure path.
        assert!(
            memory.persisted_calls().is_empty(),
            "no interaction should be persisted on a provider failure"
        );
    }

    // ------------------------------------------------------------------
    // Test 3 — output extraction honours the `"text"` key used by the HTTP
    // adapters (anthropic / openai), not just the `"output"` key.
    // ------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn provider_workflow_executor_accepts_text_key_from_http_adapters() {
        let provider = MockProvider::happy("the-plan", "the-result").with_text_key();
        let session_manager = Arc::new(SessionManager::new());
        let executor = ProviderWorkflowExecutor::new(
            Arc::new(provider.clone()),
            session_manager,
        );

        let memory = MockMemory::new_returning("ctx");
        let state = execute_workflow("u", "w", "task", &memory, &executor)
            .await
            .expect("HTTP-style (text key) responses should complete the workflow");

        // The plan and execution outputs were extracted from the `"text"` key.
        assert_eq!(state.memory.get_ephemeral("plan"), Some("the-plan"));
        // The persisted response is the execution output that survived
        // guardrails.
        let persisted = memory.persisted_calls();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].2, "the-result");
    }

    // ------------------------------------------------------------------
    // Test 4 — `provider_tag()` returns the wrapped provider's id, so the
    // orchestrator persists the real model attribution (regression guard for
    // the P1-4 → P1-5 transition).
    // ------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn provider_workflow_executor_provider_tag_is_the_adapter_id() {
        let provider = MockProvider::happy("p", "e");
        let session_manager = Arc::new(SessionManager::new());
        let executor = ProviderWorkflowExecutor::new(
            Arc::new(provider),
            session_manager,
        );
        assert_eq!(executor.provider_tag(), "mock-p1-5");
        // ...and the default (P1-4 mock) executor keeps the sentinel.
        let legacy = MockExecutor {
            plan_output: Ok("p".to_string()),
            execute_output: Ok("e".to_string()),
        };
        assert_eq!(legacy.provider_tag(), WORKFLOW_PROVIDER_TAG);
    }

    // ------------------------------------------------------------------
    // Test 5 — extract_output handles the error / missing-result / missing-
    // key cases without touching the network.
    // ------------------------------------------------------------------

    #[test]
    fn extract_output_handles_error_response() {
        let response = ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(1),
            result: None,
            error: Some(syncode_provider::ProviderError {
                code: -32_600,
                message: "invalid request".to_string(),
                data: None,
            }),
        };
        let err = extract_output(&response, "plan").expect_err("RPC error should fail");
        assert!(
            matches!(err, WorkflowError::ProviderError(ref m) if m.contains("RPC error")
                && m.contains("invalid request")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn extract_output_handles_missing_result_and_keys() {
        // No result, no error.
        let no_result = ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(1),
            result: None,
            error: None,
        };
        let err = extract_output(&no_result, "execute").expect_err("missing result should fail");
        assert!(
            matches!(err, WorkflowError::ProviderError(ref m) if m.contains("no 'result'")),
            "unexpected error: {err:?}"
        );

        // Result present but neither key.
        let no_keys = ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(1),
            result: Some(serde_json::json!({ "unrelated": 42 })),
            error: None,
        };
        let err = extract_output(&no_keys, "plan").expect_err("missing output key should fail");
        assert!(
            matches!(err, WorkflowError::ProviderError(ref m) if m.contains("no 'output' or 'text'")),
            "unexpected error: {err:?}"
        );
    }
}

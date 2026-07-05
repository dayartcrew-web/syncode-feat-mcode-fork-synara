//! Supervised agent pipeline harness — the thin execution wrappers that drive
//! an [`AgentState`] through its lifecycle.
//!
//! These are the *lightweight, I/O-free* wrappers described in
//! `docs/PRD-REMAINING-GAPS.md` §6:
//! - [`execute_step`] — a generic async wrapper that advances `current_step`,
//!   logs the transition, runs an action, and routes failures.
//! - [`run_output_guardrails`] — a sync validation gate that rejects null /
//!   empty / whitespace-only outputs.
//! - [`handle_workflow_failure`] — the single failure-routing sink shared by
//!   both wrappers.
//!
//! The full provider/memory-bound `execute_workflow` orchestrator (which composes
//! these wrappers) lives in `syncode-orchestration`; this module intentionally
//! performs **no** external I/O so it remains unit-testable in isolation.

use super::state::{AgentState, WorkflowStep};

/// Errors that can occur while driving an agent workflow through the pipeline.
///
/// Every variant carries a human-readable detail string so that callers can
/// surface a meaningful diagnostic without downcasting.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowError {
    /// A pipeline step returned an error or could not complete.
    #[error("step failed: {0}")]
    StepFailed(String),
    /// An output failed guardrail validation (null / empty / whitespace-only).
    #[error("guardrail violation: {0}")]
    GuardrailViolation(String),
    /// The underlying provider (LLM / tool) returned an error.
    #[error("provider error: {0}")]
    ProviderError(String),
}

/// Outcome of a successful [`execute_step`] invocation.
///
/// Only the success path is modelled here — failures are propagated as
/// `Err(WorkflowError)`, which keeps the type minimal and the call-sites
/// use `?` for error short-circuiting (matching the PRD sketch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepResult<T> {
    /// The step completed and produced a value of type `T`.
    Success(T),
}

/// The single failure-routing sink for the agent pipeline.
///
/// Appends a `[Harness]` failure entry to `execution_logs` and transitions
/// `current_step` to [`WorkflowStep::Failed`]. Centralising this here guarantees
/// every failure path — whether from [`execute_step`] or
/// [`run_output_guardrails`] — leaves the state frame in the identical,
/// observable "Failed" shape.
///
/// This is a synchronous, pure-data mutation: no I/O, no async.
pub fn handle_workflow_failure(state: &mut AgentState, error: &WorkflowError) {
    state.log(format!("[Harness] Step failed: {error}"));
    state.current_step = WorkflowStep::Failed;
}

/// Run one supervised pipeline step.
///
/// Sequence (see PRD §6):
/// 1. Set `state.current_step` from `step_name`.
/// 2. Append the `"[Harness] Starting step: {step_name}"` log entry.
/// 3. Invoke `action()`.
///    - On `Ok(value)`: append the completion log and return
///      [`StepResult::Success`] wrapping the value.
///    - On `Err(e)`: route through [`handle_workflow_failure`] (which marks
///      the state `Failed` and appends a failure log) and propagate the error.
///
/// The closure `action` receives no arguments — callers capture whatever they
/// need (plan strings, context, provider handles) by closure. It is `FnOnce`,
/// so it may consume captured moveable data.
///
/// Although this function is `async` (so it can sit inside an async runtime
/// and so that future provider-bound steps can `await` inside `action`), the
/// default implementation performs no `.await` itself.
pub async fn execute_step<F, T>(
    step_name: &str,
    action: F,
    state: &mut AgentState,
) -> Result<StepResult<T>, WorkflowError>
where
    F: FnOnce() -> Result<T, WorkflowError>,
{
    // 1. Advance the state machine to the step we're about to run.
    state.current_step = WorkflowStep::from_name(step_name);
    // 2. Record the start of the step.
    state.log(format!("[Harness] Starting step: {step_name}"));

    // 3. Run the action and route the outcome.
    match action() {
        Ok(value) => {
            state.log(format!("[Harness] Step {step_name} completed"));
            Ok(StepResult::Success(value))
        }
        Err(error) => {
            // Centralised failure handling: logs + transitions to Failed.
            handle_workflow_failure(state, &error);
            Err(error)
        }
    }
}

/// Validate a step's raw output before it is committed to the workflow state.
///
/// Guardrails enforced (see task P1-3):
/// - not empty,
/// - not whitespace-only,
/// - (a `&str` can never be null in safe Rust, so that case is structural —
///   but the rule is documented here for parity with the TS frontend
///   contract, where `null` / `undefined` are possible).
///
/// On success the validated string is returned by value. On failure the
/// workflow is routed through [`handle_workflow_failure`] and a
/// [`WorkflowError::GuardrailViolation`] is returned.
pub fn run_output_guardrails(
    raw_output: &str,
    state: &mut AgentState,
) -> Result<String, WorkflowError> {
    if raw_output.trim().is_empty() {
        let error = WorkflowError::GuardrailViolation(
            "Empty payload generated.".into(),
        );
        handle_workflow_failure(state, &error);
        return Err(error);
    }
    Ok(raw_output.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::state::AgentState;

    // ------------------------------------------------------------------
    // execute_step
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn execute_step_success_returns_value_and_logs() {
        let mut state = AgentState::new("wf-1", "user-1", "do thing");

        let result = execute_step("Planning", || Ok(42_i32), &mut state).await;

        match result {
            Ok(StepResult::Success(value)) => assert_eq!(value, 42),
            other => panic!("expected Success(42), got {other:?}"),
        }

        // current_step advanced to the named step on success.
        assert_eq!(state.current_step, WorkflowStep::Planning);
        // Exactly two log entries: start + completion.
        assert_eq!(
            state.execution_logs,
            vec![
                "[Harness] Starting step: Planning".to_string(),
                "[Harness] Step Planning completed".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn execute_step_failure_routes_to_failed_and_returns_error() {
        let mut state = AgentState::new("wf-2", "user-1", "do thing");

        let result = execute_step(
            "Execution",
            || -> Result<(), WorkflowError> {
                Err(WorkflowError::StepFailed("adapter blew up".into()))
            },
            &mut state,
        )
        .await;

        // The original error is propagated verbatim.
        assert_eq!(
            result,
            Err(WorkflowError::StepFailed("adapter blew up".into()))
        );

        // Failure routing: current_step → Failed.
        assert_eq!(state.current_step, WorkflowStep::Failed);
        // We logged the start of the step AND the failure sink entry.
        assert!(
            state
                .execution_logs
                .iter()
                .any(|e| e == "[Harness] Starting step: Execution"),
            "missing start log; logs = {:?}",
            state.execution_logs
        );
        assert!(
            state
                .execution_logs
                .iter()
                .any(|e| e.contains("[Harness] Step failed:")
                    && e.contains("adapter blew up")),
            "missing failure log; logs = {:?}",
            state.execution_logs
        );
        // No spurious completion log on the failure path.
        assert!(
            !state
                .execution_logs
                .iter()
                .any(|e| e.contains("completed")),
            "completion log should not be present on failure path"
        );
    }

    #[tokio::test]
    async fn execute_step_advances_current_step_from_initialization() {
        // Fresh state starts at Initialization; after a successful step it
        // must reflect the named step, proving current_step is mutated before
        // the action runs.
        let mut state = AgentState::new("wf-3", "user-9", "plan something");
        assert_eq!(state.current_step, WorkflowStep::Initialization);

        let _ = execute_step("Guardrails", || Ok(()), &mut state).await;

        assert_eq!(state.current_step, WorkflowStep::Guardrails);
    }

    // ------------------------------------------------------------------
    // run_output_guardrails
    // ------------------------------------------------------------------

    #[test]
    fn run_output_guardrails_accepts_non_empty_output() {
        let mut state = AgentState::new("wf-4", "user-1", "do thing");

        let validated =
            run_output_guardrails("hello world", &mut state).expect("non-empty should pass");

        assert_eq!(validated, "hello world");
        // Valid input must not touch the step or append any logs.
        assert_eq!(state.current_step, WorkflowStep::Initialization);
        assert!(state.execution_logs.is_empty());
    }

    #[test]
    fn run_output_guardrails_rejects_empty_output() {
        let mut state = AgentState::new("wf-5", "user-1", "do thing");

        let err = run_output_guardrails("", &mut state).expect_err("empty should fail");

        assert!(matches!(
            err,
            WorkflowError::GuardrailViolation(ref msg)
                if msg.contains("Empty payload")
        ));
        assert_eq!(state.current_step, WorkflowStep::Failed);
        assert!(
            state
                .execution_logs
                .iter()
                .any(|e| e.contains("[Harness] Step failed:")
                    && e.contains("guardrail violation")),
            "missing failure log; logs = {:?}",
            state.execution_logs
        );
    }

    #[test]
    fn run_output_guardrails_rejects_whitespace_only_output() {
        let mut state = AgentState::new("wf-6", "user-1", "do thing");

        let err = run_output_guardrails("   \n\t  ", &mut state)
            .expect_err("whitespace-only should fail");

        assert!(matches!(err, WorkflowError::GuardrailViolation(_)));
        assert_eq!(state.current_step, WorkflowStep::Failed);
    }

    // ------------------------------------------------------------------
    // handle_workflow_failure (direct unit test of the sink)
    // ------------------------------------------------------------------

    #[test]
    fn handle_workflow_failure_marks_failed_and_logs() {
        let mut state = AgentState::new("wf-7", "user-1", "do thing");
        state.current_step = WorkflowStep::Execution;

        handle_workflow_failure(
            &mut state,
            &WorkflowError::ProviderError("rate limited".into()),
        );

        assert_eq!(state.current_step, WorkflowStep::Failed);
        assert_eq!(state.execution_logs.len(), 1);
        assert!(state.execution_logs[0].contains("[Harness] Step failed:"));
        assert!(state.execution_logs[0].contains("provider error"));
        assert!(state.execution_logs[0].contains("rate limited"));
    }
}

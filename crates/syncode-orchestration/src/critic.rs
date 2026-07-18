//! Post-execution critic — optional review step inserted between
//! [`crate::WorkflowExecutor::execute`] and [`crate::run_output_guardrails`].
//!
//! This is a **sibling** trait to [`WorkflowExecutor`](crate::WorkflowExecutor).
//! It is intentionally additive:
//! - The existing [`execute_workflow`](crate::execute_workflow) function is
//!   unchanged (it delegates to [`execute_workflow_with_critic`] with a
//!   [`NoOpCritic`]).
//! - All existing tests at the bottom of [`workflow`](crate::workflow) pass
//!   without modification.
//! - The 3-variant [`WorkflowError`] is unchanged (rejections reuse
//!   `StepFailed`).
//!
//! # When to supply a Critic
//!
//! A critic is useful when execution output must satisfy semantic constraints
//! beyond the structural ones the guardrails enforce (non-empty, non-whitespace).
//! Example: rule-based pre-checks for required sections, hybrid LLM critics
//! that score output quality, or domain-specific validators (e.g., code must
//! compile, JSON must parse).
//!
//! # Failure routing
//!
//! [`CriticVerdict::Rejected`] and [`CriticVerdict::NeedsInfo`] both route
//! through [`WorkflowError::StepFailed`] so the orchestrator's shared
//! failure-routing sink handles them uniformly with planning/execution
//! failures. Persistence is skipped (matching the existing failure-path
//! invariant).

use syncode_core::agent::WorkflowError;

/// Outcome of a critic review.
///
/// Each non-`Approved` variant carries structured diagnostics so callers can
/// surface actionable feedback (not just a boolean "rejected").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CriticVerdict {
    /// Execution output is acceptable; the workflow may proceed to guardrails.
    Approved {
        /// Human-readable rationale for the approval (useful for logs/audit).
        rationale: String,
    },
    /// Execution output is rejected; the workflow fails.
    Rejected {
        /// Concrete reasons for the rejection. Joined into the
        /// [`WorkflowError::StepFailed`] message.
        reasons: Vec<String>,
    },
    /// The critic could not reach a verdict without more information.
    ///
    /// Treated identically to [`Rejected`] by [`execute_workflow_with_critic`]
    /// (the workflow fails), but the structured questions let a higher-level
    /// caller re-prompt the user / provider.
    NeedsInfo {
        /// Open questions that would let the critic reach a verdict.
        questions: Vec<String>,
    },
}

/// Post-execution reviewer.
///
/// Implementations inspect the raw execution output and return a
/// [`CriticVerdict`]. The trait is sync (matching [`WorkflowExecutor`]) so it
/// composes cleanly with the existing pipeline.
///
/// # Errors
///
/// Implementations should return `Err(WorkflowError::StepFailed(...))` for
/// internal failures (e.g., an LLM critic whose verifier provider is down) so
/// the error routes through the shared failure sink.
pub trait Critic: Send + Sync {
    /// Inspect `execution_output` and return a verdict.
    fn review(&self, execution_output: &str) -> Result<CriticVerdict, WorkflowError>;
}

/// Default no-op critic that always approves.
///
/// Used by [`execute_workflow`](crate::execute_workflow) when no critic is
/// supplied, preserving the pre-critic pipeline behaviour bit-for-bit (the
/// review still runs, but the verdict is always `Approved`, so the logs gain
/// one `[Critic]` line and the workflow proceeds exactly as before).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpCritic;

impl Critic for NoOpCritic {
    fn review(&self, _execution_output: &str) -> Result<CriticVerdict, WorkflowError> {
        Ok(CriticVerdict::Approved {
            rationale: "no-op critic: auto-approved".to_string(),
        })
    }
}

/// Convert a non-Approved [`CriticVerdict`] into the workflow's failure error.
///
/// Crate-private — only [`execute_workflow_with_critic`] needs it. Returning
/// `Option<WorkflowError>` (rather than `WorkflowError` directly) lets the
/// caller use `if let Some(err) = ...` to short-circuit without an extra
/// `matches!` check.
pub(crate) fn verdict_to_failure(verdict: CriticVerdict) -> Option<WorkflowError> {
    match verdict {
        CriticVerdict::Approved { .. } => None,
        CriticVerdict::Rejected { reasons } => {
            let joined = if reasons.is_empty() {
                "no reasons provided".to_string()
            } else {
                reasons.join("; ")
            };
            Some(WorkflowError::StepFailed(format!(
                "critic rejected output: {joined}"
            )))
        }
        CriticVerdict::NeedsInfo { questions } => {
            let joined = if questions.is_empty() {
                "no questions provided".to_string()
            } else {
                questions.join("; ")
            };
            Some(WorkflowError::StepFailed(format!(
                "critic needs more information: {joined}"
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// Test doubles — crate-visible under #[cfg(test)] so workflow.rs::tests can
// use the same RecordingCritic instead of defining a parallel mock.
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod test_doubles {
    use super::{Critic, CriticVerdict, WorkflowError};
    use std::sync::{Arc, Mutex};

    /// Test double implementing [`Critic`] that records every `review` call
    /// and returns a configurable verdict.
    #[derive(Debug, Default, Clone)]
    pub(crate) struct RecordingCritic {
        inner: Arc<Mutex<RecordingCriticInner>>,
    }

    #[derive(Debug, Default)]
    struct RecordingCriticInner {
        reviewed: Vec<String>,
        next_verdict: Option<Result<CriticVerdict, WorkflowError>>,
    }

    impl RecordingCritic {
        /// Critic whose next `review` returns the given verdict.
        pub fn returning(verdict: CriticVerdict) -> Self {
            let inner = RecordingCriticInner {
                reviewed: Vec::new(),
                next_verdict: Some(Ok(verdict)),
            };
            Self {
                inner: Arc::new(Mutex::new(inner)),
            }
        }

        /// Critic whose `review` always errors with the given `WorkflowError`.
        pub fn failing(err: WorkflowError) -> Self {
            let inner = RecordingCriticInner {
                reviewed: Vec::new(),
                next_verdict: Some(Err(err)),
            };
            Self {
                inner: Arc::new(Mutex::new(inner)),
            }
        }

        /// Snapshot of every `execution_output` reviewed, in arrival order.
        pub fn reviewed(&self) -> Vec<String> {
            self.inner.lock().unwrap().reviewed.clone()
        }
    }

    impl Critic for RecordingCritic {
        fn review(&self, execution_output: &str) -> Result<CriticVerdict, WorkflowError> {
            let mut inner = self.inner.lock().unwrap();
            inner.reviewed.push(execution_output.to_string());
            inner
                .next_verdict
                .take()
                .unwrap_or(Ok(CriticVerdict::Approved {
                    rationale: "recording critic default".to_string(),
                }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::critic::test_doubles::RecordingCritic;

    // ------------------------------------------------------------------
    // NoOpCritic — always approves
    // ------------------------------------------------------------------

    #[test]
    fn noop_critic_always_approves_with_rationale() {
        let critic = NoOpCritic;
        let verdict = critic
            .review("any output")
            .expect("no-op critic must not error");
        match verdict {
            CriticVerdict::Approved { rationale } => {
                assert!(
                    rationale.contains("no-op"),
                    "rationale should mention no-op; got: {rationale}"
                );
            }
            other => panic!("expected Approved, got {other:?}"),
        }
    }

    #[test]
    fn noop_critic_approves_regardless_of_input() {
        let critic = NoOpCritic;
        for input in ["", "   ", "real output"] {
            let verdict = critic.review(input).expect("no-op must not error");
            assert!(
                matches!(verdict, CriticVerdict::Approved { .. }),
                "input {input:?} should still approve"
            );
        }
    }

    // ------------------------------------------------------------------
    // RecordingCritic (smoke test — full coverage lives in workflow.rs::tests)
    // ------------------------------------------------------------------

    #[test]
    fn recording_critic_returns_canned_verdict_and_records_input() {
        let critic = RecordingCritic::returning(CriticVerdict::Rejected {
            reasons: vec!["missing section: tests".to_string()],
        });
        let verdict = critic
            .review("the execution output")
            .expect("should not error");
        assert!(
            matches!(verdict, CriticVerdict::Rejected { ref reasons } if reasons == &vec!["missing section: tests".to_string()]),
            "got {verdict:?}"
        );
        assert_eq!(critic.reviewed(), vec!["the execution output".to_string()]);
    }

    #[test]
    fn recording_critic_propagates_errors() {
        let critic = RecordingCritic::failing(WorkflowError::ProviderError(
            "verifier LLM down".to_string(),
        ));
        let err = critic
            .review("output")
            .expect_err("should propagate the configured error");
        assert!(
            matches!(err, WorkflowError::ProviderError(ref m) if m == "verifier LLM down"),
            "got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // verdict_to_failure
    // ------------------------------------------------------------------

    #[test]
    fn verdict_to_failure_returns_none_for_approved() {
        let approved = CriticVerdict::Approved {
            rationale: "looks good".to_string(),
        };
        assert!(verdict_to_failure(approved).is_none());
    }

    #[test]
    fn verdict_to_failure_wraps_rejected_into_step_failed() {
        let rejected = CriticVerdict::Rejected {
            reasons: vec!["a".to_string(), "b".to_string()],
        };
        let err = verdict_to_failure(rejected).expect("rejected should yield Some error");
        match err {
            WorkflowError::StepFailed(msg) => {
                assert!(
                    msg.contains("critic rejected output"),
                    "missing prefix; got: {msg}"
                );
                assert!(msg.contains("a; b"), "missing joined reasons; got: {msg}");
            }
            other => panic!("expected StepFailed, got {other:?}"),
        }
    }

    #[test]
    fn verdict_to_failure_handles_empty_reasons_gracefully() {
        let rejected = CriticVerdict::Rejected { reasons: vec![] };
        let err = verdict_to_failure(rejected).expect("rejected should yield Some error");
        match err {
            WorkflowError::StepFailed(msg) => {
                assert!(msg.contains("no reasons provided"), "got: {msg}");
            }
            other => panic!("expected StepFailed, got {other:?}"),
        }
    }

    #[test]
    fn verdict_to_failure_wraps_needs_info_into_step_failed() {
        let needs = CriticVerdict::NeedsInfo {
            questions: vec!["which target?".to_string()],
        };
        let err = verdict_to_failure(needs).expect("needs-info should yield Some error");
        match err {
            WorkflowError::StepFailed(msg) => {
                assert!(msg.contains("critic needs more information"), "got: {msg}");
                assert!(msg.contains("which target?"), "got: {msg}");
            }
            other => panic!("expected StepFailed, got {other:?}"),
        }
    }
}

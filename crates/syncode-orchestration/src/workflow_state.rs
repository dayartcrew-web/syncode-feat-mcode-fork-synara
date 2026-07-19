//! Workflow state provider trait (C2 of chat-workflow bridge).
//!
//! The orchestrator's command reactor consults an implementer of
//! [`WorkflowStateProvider`] when starting a fresh provider session for a
//! thread, prepending the returned preamble text to the session's
//! `system_prompt`. This is how syncode-side workflow state (phase, current
//! task, constraints) reaches the chat AI even though ACP v1 has no native
//! field for it.
//!
//! See `workflow_preamble` in `syncode-ws` for the production implementer
//! (reads `thread_workflow_links` sidecar + builds preamble text).

/// Provider of workflow-state preambles for chat sessions.
///
/// Implementations must be cheap: the reactor awaits this on every
/// Fresh/Restarted session start, which is in the hot path of every chat
/// turn. When the thread has no active workflow, return `None` and the
/// reactor skips preamble injection (back-compat with prior behavior).
///
/// Returning an empty string is equivalent to returning `None`.
#[async_trait::async_trait]
pub trait WorkflowStateProvider: Send + Sync {
    /// Returns the workflow preamble text for a thread, or `None` when the
    /// thread has no active workflow (or the provider chooses not to inject
    /// one for this turn).
    ///
    /// `thread_id` is the chat thread's EntityId string form. `user_input`
    /// is the turn's user message — implementations may use it to populate
    /// the preamble's "current task" field when no richer state is available.
    async fn workflow_preamble(&self, thread_id: &str, user_input: &str) -> Option<String>;
}

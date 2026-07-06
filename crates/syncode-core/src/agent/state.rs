//! Agent state frame — the deterministic state passed through each step of the
//! supervised agent pipeline.
//!
//! See `docs/PRD-REMAINING-GAPS.md` §6 for the full design.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Lifecycle stages of an agent workflow.
///
/// The pipeline transitions sequentially:
/// `Initialization → Planning → Execution → Guardrails → Completed`,
/// with `Failed` reachable from any step via failure routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStep {
    /// Freshly constructed state frame; no work performed yet.
    Initialization,
    /// Generating a plan from the initial task + retrieved context.
    Planning,
    /// Executing the generated plan.
    Execution,
    /// Validating step outputs (non-empty, well-formed) before committing.
    Guardrails,
    /// All steps succeeded; workflow is finished.
    Completed,
    /// A step failed and the workflow was routed to the failure handler.
    Failed,
}

impl WorkflowStep {
    /// Parse a step name (as used in execution logs) into a [`WorkflowStep`].
    ///
    /// Unknown names fall back to [`WorkflowStep::Failed`] so that an
    /// unrecognised step label cannot leave the state machine in an
    /// inconsistent intermediate state.
    pub fn from_name(name: &str) -> Self {
        match name.trim() {
            "Initialization" => WorkflowStep::Initialization,
            "Planning" => WorkflowStep::Planning,
            "Execution" => WorkflowStep::Execution,
            "Guardrails" => WorkflowStep::Guardrails,
            "Completed" => WorkflowStep::Completed,
            "Failed" => WorkflowStep::Failed,
            _ => WorkflowStep::Failed,
        }
    }

    /// Returns `true` for the two terminal steps.
    pub fn is_terminal(&self) -> bool {
        matches!(self, WorkflowStep::Completed | WorkflowStep::Failed)
    }
}

impl std::fmt::Display for WorkflowStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkflowStep::Initialization => write!(f, "Initialization"),
            WorkflowStep::Planning => write!(f, "Planning"),
            WorkflowStep::Execution => write!(f, "Execution"),
            WorkflowStep::Guardrails => write!(f, "Guardrails"),
            WorkflowStep::Completed => write!(f, "Completed"),
            WorkflowStep::Failed => write!(f, "Failed"),
        }
    }
}

/// Agent memory — a small two-tier store carried inside [`AgentState`].
///
/// - `ephemeral`: scratch key/value pairs scoped to a single workflow run
///   (e.g. intermediate plans, retrieved context fragments).
/// - `long_term_summary`: a running prose summary that may be persisted to
///   the `MemoryProvider` (P3) after the workflow completes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMemory {
    /// Per-run scratch storage. Defaults to empty.
    pub ephemeral: HashMap<String, String>,
    /// High-level summary of the workflow, appended to as steps complete.
    /// Defaults to empty.
    pub long_term_summary: String,
}

impl AgentMemory {
    /// Construct an empty memory frame.
    pub fn new() -> Self {
        Self {
            ephemeral: HashMap::new(),
            long_term_summary: String::new(),
        }
    }

    /// Insert or overwrite an ephemeral key.
    pub fn set_ephemeral(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.ephemeral.insert(key.into(), value.into());
    }

    /// Look up an ephemeral value by key.
    pub fn get_ephemeral(&self, key: &str) -> Option<&str> {
        self.ephemeral.get(key).map(String::as_str)
    }

    /// Append a paragraph to the long-term summary (prefixed with a newline
    /// when the summary is non-empty).
    pub fn append_summary(&mut self, paragraph: impl AsRef<str>) {
        let paragraph = paragraph.as_ref();
        if self.long_term_summary.is_empty() {
            self.long_term_summary.push_str(paragraph);
        } else {
            self.long_term_summary.push('\n');
            self.long_term_summary.push_str(paragraph);
        }
    }
}

impl Default for AgentMemory {
    fn default() -> Self {
        Self::new()
    }
}

/// The deterministic state frame for the supervised agent pipeline.
///
/// Constructed once at workflow start by [`AgentState::new`] and mutated
/// in place as each pipeline step runs. This is PURE DATA — no async, no
/// I/O. The execution wrappers live in `syncode-orchestration`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentState {
    /// Current position in the workflow lifecycle.
    pub current_step: WorkflowStep,
    /// Ephemeral + long-term memory carried between steps.
    pub memory: AgentMemory,
    /// Ordered execution log; one entry per step start/finish/failure.
    pub execution_logs: Vec<String>,
    /// `true` once the workflow reaches [`WorkflowStep::Completed`].
    pub is_completed: bool,
    /// Identifier of the workflow run this state belongs to.
    pub workflow_id: String,
    /// Identifier of the user that initiated the workflow.
    pub user_id: String,
    /// The original task description supplied when the workflow was started.
    pub initial_task: String,
}

impl AgentState {
    /// Construct a fresh state frame for a new workflow run.
    ///
    /// - `current_step` is set to [`WorkflowStep::Initialization`]
    /// - `memory` and `execution_logs` are empty
    /// - `is_completed` is `false`
    pub fn new(
        workflow_id: impl Into<String>,
        user_id: impl Into<String>,
        initial_task: impl Into<String>,
    ) -> Self {
        Self {
            current_step: WorkflowStep::Initialization,
            memory: AgentMemory::new(),
            execution_logs: Vec::new(),
            is_completed: false,
            workflow_id: workflow_id.into(),
            user_id: user_id.into(),
            initial_task: initial_task.into(),
        }
    }

    /// Append a single log line to `execution_logs`.
    pub fn log(&mut self, entry: impl Into<String>) {
        self.execution_logs.push(entry.into());
    }

    /// Mark the workflow as successfully completed.
    pub fn mark_completed(&mut self) {
        self.current_step = WorkflowStep::Completed;
        self.is_completed = true;
    }

    /// Mark the workflow as failed.
    pub fn mark_failed(&mut self) {
        self.current_step = WorkflowStep::Failed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- WorkflowStep ---

    #[test]
    fn workflow_step_from_name_roundtrip() {
        for step in [
            WorkflowStep::Initialization,
            WorkflowStep::Planning,
            WorkflowStep::Execution,
            WorkflowStep::Guardrails,
            WorkflowStep::Completed,
            WorkflowStep::Failed,
        ] {
            assert_eq!(WorkflowStep::from_name(&step.to_string()), step);
        }
    }

    #[test]
    fn workflow_step_from_name_unknown_is_failed() {
        assert_eq!(WorkflowStep::from_name("nope"), WorkflowStep::Failed);
        // whitespace is trimmed
        assert_eq!(
            WorkflowStep::from_name("  Planning  "),
            WorkflowStep::Planning
        );
    }

    #[test]
    fn workflow_step_is_terminal() {
        assert!(WorkflowStep::Completed.is_terminal());
        assert!(WorkflowStep::Failed.is_terminal());
        assert!(!WorkflowStep::Initialization.is_terminal());
        assert!(!WorkflowStep::Planning.is_terminal());
        assert!(!WorkflowStep::Execution.is_terminal());
        assert!(!WorkflowStep::Guardrails.is_terminal());
    }

    // --- AgentMemory ---

    #[test]
    fn agent_memory_new_is_empty() {
        let m = AgentMemory::new();
        assert!(m.ephemeral.is_empty());
        assert!(m.long_term_summary.is_empty());
    }

    #[test]
    fn agent_memory_set_get_ephemeral() {
        let mut m = AgentMemory::new();
        m.set_ephemeral("plan", "step 1; step 2");
        assert_eq!(m.get_ephemeral("plan"), Some("step 1; step 2"));
        assert_eq!(m.get_ephemeral("missing"), None);
    }

    #[test]
    fn agent_memory_append_summary_joins_with_newline() {
        let mut m = AgentMemory::new();
        m.append_summary("First paragraph.");
        assert_eq!(m.long_term_summary, "First paragraph.");
        m.append_summary("Second paragraph.");
        assert_eq!(m.long_term_summary, "First paragraph.\nSecond paragraph.");
    }

    // --- AgentState ---

    #[test]
    fn agent_state_new_initializes_correctly() {
        let s = AgentState::new("wf-1", "user-42", "Refactor the auth module");

        assert_eq!(s.current_step, WorkflowStep::Initialization);
        assert!(s.memory.ephemeral.is_empty());
        assert!(s.memory.long_term_summary.is_empty());
        assert!(s.execution_logs.is_empty());
        assert!(!s.is_completed);
        assert_eq!(s.workflow_id, "wf-1");
        assert_eq!(s.user_id, "user-42");
        assert_eq!(s.initial_task, "Refactor the auth module");
    }

    #[test]
    fn agent_state_lifecycle_transitions() {
        let mut s = AgentState::new("wf-1", "user-1", "do thing");

        s.current_step = WorkflowStep::Planning;
        s.log("[Harness] Starting step: Planning");
        s.memory.set_ephemeral("plan", "a,b,c");

        assert_eq!(s.current_step, WorkflowStep::Planning);
        assert_eq!(s.execution_logs.len(), 1);
        assert_eq!(s.memory.get_ephemeral("plan"), Some("a,b,c"));

        s.mark_completed();
        assert_eq!(s.current_step, WorkflowStep::Completed);
        assert!(s.is_completed);

        // a fresh failed transition
        let mut s2 = AgentState::new("wf-2", "user-1", "do other");
        s2.mark_failed();
        assert_eq!(s2.current_step, WorkflowStep::Failed);
        assert!(!s2.is_completed, "Failed must not set is_completed");
    }

    #[test]
    fn agent_state_serialization_roundtrip() {
        let mut s = AgentState::new("wf-9", "user-7", "write tests");
        s.memory.set_ephemeral("ctx", "prior context");
        s.memory.append_summary("Started.");
        s.log("step 1");
        s.mark_completed();

        let json = serde_json::to_string(&s).expect("serialize");
        let back: AgentState = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(back, s);
        assert_eq!(back.current_step, WorkflowStep::Completed);
        assert!(back.is_completed);
        assert_eq!(back.memory.get_ephemeral("ctx"), Some("prior context"));
    }

    #[test]
    fn workflow_step_serde_snake_case() {
        // Verify serde uses snake_case so payloads round-trip with the
        // TS frontend's expected shape.
        let json = serde_json::to_string(&WorkflowStep::Guardrails).expect("serialize");
        assert_eq!(json, "\"guardrails\"");

        let back: WorkflowStep = serde_json::from_str("\"planning\"").expect("deserialize");
        assert_eq!(back, WorkflowStep::Planning);
    }
}

//! Workflow preamble generator.
//!
//! Produces a short text block describing the active workflow phase, current
//! task, and constraints. Intended to be prepended to chat prompts so the
//! chat AI (external claude/cursor/gemini CLI) has visibility into the
//! syncode-side workflow state machine that it cannot see otherwise.
//!
//! See: crates/syncode-ws/src/workflow_preamble.rs design doc — ACP spec
//! does not natively forward workflow state, so we inject as text.

use serde::{Deserialize, Serialize};

/// Minimal workflow state snapshot forwarded to the preamble generator.
/// Deliberately small — the preamble only needs phase + current task to be
/// useful. Richer state lives in the workflow module itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowPreambleInput {
    /// Current workflow phase (e.g., "INIT", "ANALYZE", "PLAN", "EXECUTE", "VERIFY", "DONE").
    pub phase: String,
    /// Active task title, if any (None when phase is INIT or DONE).
    pub current_task: Option<String>,
    /// Total tasks in the plan (for progress context).
    pub total_tasks: Option<u32>,
    /// Index of the current task (1-based) for progress context.
    pub current_task_index: Option<u32>,
}

/// Builds the workflow preamble text.
///
/// Returns empty string when input is None (no active workflow).
/// Output format is a compact 3-6 line block suitable for prepending to
/// a chat prompt as a leading text ContentBlock.
pub fn build_workflow_preamble(input: Option<&WorkflowPreambleInput>) -> String {
    let Some(state) = input else {
        return String::new();
    };
    let mut lines: Vec<String> = Vec::with_capacity(6);
    lines.push("--- WORKFLOW CONTEXT ---".to_string());
    lines.push(format!("Phase: {}", state.phase));
    if let Some(task) = &state.current_task {
        let task_line = match (state.current_task_index, state.total_tasks) {
            (Some(idx), Some(total)) => format!("Current task ({}/{}) — {}", idx, total, task),
            _ => format!("Current task — {}", task),
        };
        lines.push(task_line);
    }
    lines.push("Constraints: follow TDD (RED\u{2192}GREEN\u{2192}REFACTOR), maintain 80%+ coverage, zero clippy warnings, no hardcoded secrets.".to_string());
    lines.push("--- END WORKFLOW CONTEXT ---".to_string());
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_when_no_active_workflow() {
        assert_eq!(build_workflow_preamble(None), "");
    }

    #[test]
    fn includes_phase_line() {
        let input = WorkflowPreambleInput {
            phase: "EXECUTE".to_string(),
            current_task: Some("Add MCP import handler".to_string()),
            total_tasks: Some(5),
            current_task_index: Some(3),
        };
        let out = build_workflow_preamble(Some(&input));
        assert!(out.contains("Phase: EXECUTE"));
    }

    #[test]
    fn includes_current_task_with_progress() {
        let input = WorkflowPreambleInput {
            phase: "EXECUTE".to_string(),
            current_task: Some("Add MCP import handler".to_string()),
            total_tasks: Some(5),
            current_task_index: Some(3),
        };
        let out = build_workflow_preamble(Some(&input));
        assert!(out.contains("Current task (3/5) — Add MCP import handler"));
    }

    #[test]
    fn omits_current_task_line_when_none() {
        let input = WorkflowPreambleInput {
            phase: "INIT".to_string(),
            current_task: None,
            total_tasks: None,
            current_task_index: None,
        };
        let out = build_workflow_preamble(Some(&input));
        assert!(out.contains("Phase: INIT"));
        assert!(!out.contains("Current task"));
    }

    #[test]
    fn includes_constraints_reminder() {
        let input = WorkflowPreambleInput {
            phase: "PLAN".to_string(),
            current_task: None,
            total_tasks: None,
            current_task_index: None,
        };
        let out = build_workflow_preamble(Some(&input));
        assert!(out.contains("Constraints"));
        assert!(out.contains("TDD"));
    }

    #[test]
    fn bounded_output_size() {
        // Preamble should be short — never more than 1KB to avoid bloating every prompt.
        let input = WorkflowPreambleInput {
            phase: "EXECUTE".to_string(),
            current_task: Some("x".repeat(200)),
            total_tasks: Some(99),
            current_task_index: Some(50),
        };
        let out = build_workflow_preamble(Some(&input));
        assert!(out.len() < 1024, "preamble length {} exceeds 1KB", out.len());
    }
}

//! Agent layer — agentic workflow state frame model
//!
//! This module contains the deterministic state frame used by the supervised
//! sequential agent pipeline (see PRD-REMAINING-GAPS.md §6):
//! - [`WorkflowStep`] — lifecycle stages of an agent workflow
//! - [`AgentMemory`] — ephemeral key/value store + long-term summary
//! - [`AgentState`] — the full state frame passed between pipeline steps
//!
//! These types are PURE DATA: no async, no I/O. The pipeline execution
//! wrappers (`execute_step`, `run_output_guardrails`) live in [`harness`]
//! (tasks P1-2, P1-3); the provider/memory-bound `execute_workflow`
//! orchestrator is implemented in `syncode-orchestration` (task P1-4).

pub mod harness;
pub mod state;

pub use harness::{execute_step, handle_workflow_failure, run_output_guardrails, StepResult, WorkflowError};
pub use state::{AgentMemory, AgentState, WorkflowStep};

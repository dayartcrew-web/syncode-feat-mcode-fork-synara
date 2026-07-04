//! Agent layer — agentic workflow state frame model
//!
//! This module contains the deterministic state frame used by the supervised
//! sequential agent pipeline (see PRD-REMAINING-GAPS.md §6):
//! - [`WorkflowStep`] — lifecycle stages of an agent workflow
//! - [`AgentMemory`] — ephemeral key/value store + long-term summary
//! - [`AgentState`] — the full state frame passed between pipeline steps
//!
//! These types are PURE DATA: no async, no I/O. The pipeline execution
//! wrappers (`execute_step`, `run_output_guardrails`, `execute_workflow`)
//! are implemented in `syncode-orchestration` (tasks P1-2..P1-4).

pub mod state;

pub use state::{AgentMemory, AgentState, WorkflowStep};

//! Syncode Automation — Scheduled Agent Runs
//!
//! Scheduler engine, automation definition, run lifecycle,
//! retry/misfire/completion policies, heartbeat mode, and AI-evaluated completion.

pub mod completion_eval;
pub mod definition;
pub mod events;
pub mod executor;
pub mod in_memory_repo;
pub mod policies;
pub mod process_executor;
pub mod run_reactor;
pub mod runner;
pub mod schedule;
pub mod scheduler;

pub use completion_eval::{
    CompletionLlmCall, CompletionResult, CompletionVerdict, NoMatchReason,
    build_prompt, build_system_and_prompt, evaluate_completion_policy, parse_confidence,
};
pub use definition::{AutomationDef, AutomationId, ScheduleType};
pub use events::{
    NoopRunEventSink, RunContext, RunEvent, RunEventKind, RunEventSink, emit_current,
    with_run_context,
};
pub use in_memory_repo::InMemoryAutomationRepository;
pub use policies::{CompletionPolicy, MisfirePolicy, RetryPolicy};
pub use process_executor::ProcessRunExecutor;
pub use run_reactor::{AutomationRunReactor, BroadcastDomainEventStream, DomainEventStream};
pub use runner::{AutomationRun, RunStatus};
pub use scheduler::{Scheduler, SchedulerError};

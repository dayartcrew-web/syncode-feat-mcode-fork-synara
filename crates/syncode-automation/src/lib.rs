//! Syncode Automation — Scheduled Agent Runs
//!
//! Scheduler engine, automation definition, run lifecycle,
//! retry/misfire/completion policies, heartbeat mode, and AI-evaluated completion.

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

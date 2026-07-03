//! Syncode Automation — Scheduled Agent Runs
//!
//! Scheduler engine, automation definition, run lifecycle,
//! retry/misfire/completion policies, heartbeat mode, and AI-evaluated completion.

pub mod definition;
pub mod executor;
pub mod in_memory_repo;
pub mod policies;
pub mod process_executor;
pub mod runner;
pub mod schedule;
pub mod scheduler;

pub use definition::{AutomationDef, AutomationId, ScheduleType};
pub use in_memory_repo::InMemoryAutomationRepository;
pub use policies::{CompletionPolicy, MisfirePolicy, RetryPolicy};
pub use process_executor::ProcessRunExecutor;
pub use runner::{AutomationRun, RunStatus};
pub use scheduler::{Scheduler, SchedulerError};

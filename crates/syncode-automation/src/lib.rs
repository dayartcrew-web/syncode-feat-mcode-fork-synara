//! Syncode Automation — Scheduled Agent Runs
//!
//! Scheduler engine, automation definition, run lifecycle,
//! retry/misfire/completion policies, heartbeat mode, and AI-evaluated completion.

pub mod definition;
pub mod policies;
pub mod runner;
pub mod scheduler;

pub use definition::{AutomationDef, AutomationId, ScheduleType};
pub use policies::{CompletionPolicy, MisfirePolicy, RetryPolicy};
pub use runner::{AutomationRun, RunStatus};
pub use scheduler::{Scheduler, SchedulerError};

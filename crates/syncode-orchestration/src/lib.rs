//! Syncode Orchestration — CQRS/Event Sourcing Engine
//!
//! Implements the core orchestration pattern:
//! - Commands → Decider → Events (pure business logic)
//! - Events → Projector → Read Models
//! - Reactors for side effects
//! - Orchestrator pipeline wiring everything together
//! - Supervised agent workflow (`execute_workflow`)

pub mod decider;
pub mod events;
pub mod log;
pub mod pipeline;
pub mod projector;
pub mod reactors;
pub mod read_model;
pub mod use_cases;
pub mod workflow;

// Re-exports for convenience
pub use decider::{Command, Decider, DeciderError};
pub use events::DomainEvent;
pub use pipeline::{CommandResult, OrchestrationError, Orchestrator};
pub use projector::{Projector, ReadModelStore};
pub use reactors::{
    CommandReaction, CommandReactorError, EnsureOutcome, IngestionResult,
    ProviderCommandReactor, ingest_provider_event,
};
pub use read_model::{
    ActivityView, CheckpointView, MessageView, ProjectView, ThreadSessionView, ThreadView, TurnView,
};
pub use use_cases::{ApplicationService, ProjectDashboard, ThreadDetail};
pub use workflow::{execute_workflow, ProviderWorkflowExecutor, WorkflowExecutor};

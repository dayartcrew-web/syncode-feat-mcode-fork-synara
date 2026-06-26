//! Syncode Orchestration — CQRS/Event Sourcing Engine
//!
//! Implements the core orchestration pattern:
//! - Commands → Decider → Events (pure business logic)
//! - Events → Projector → Read Models
//! - Reactors for side effects

pub mod decider;
pub mod events;
pub mod projector;
pub mod read_model;
pub mod reactors;

// Re-exports for convenience
pub use decider::{Command, Decider, DeciderError};
pub use events::DomainEvent;
pub use projector::{Projector, ReadModelStore};
pub use read_model::{ProjectView, ThreadView, TurnView, MessageView, ActivityView};
pub use reactors::{CommandReaction, CommandReactorError, ProviderCommandReactor, ingest_provider_event, IngestionResult};

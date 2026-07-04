//! Syncode Core — Shared Domain Kernel
//!
//! This crate contains the fundamental domain types used across all Syncode bounded contexts:
//! - Entity identifiers, timestamps, and validated value objects
//! - Aggregate roots: Project, Thread, Turn, Message, Activity
//! - Domain events (CQRS/Event Sourcing)
//! - Port interfaces (trait definitions for external dependencies)

pub mod application;
pub mod domain;
pub mod ports;
pub mod util;

// Re-export domain primitives (EntityId, Timestamp, TrimmedString, etc.)
// Note: DomainEvent trait is NOT re-exported here to avoid name collision
// with the DomainEvent enum. Use `syncode_core::domain::primitives::DomainEvent`
// for the trait, or `syncode_core::domain::events::DomainEvent` for the enum.
pub use domain::primitives::{EntityId, Timestamp, TrimmedString, TrimmedStringError};

// Re-export the base Command trait
pub use domain::primitives::Command;

// Re-export domain event enum and envelope
pub use domain::events::{CheckpointFile, DomainEvent, Envelope};

// Re-export aggregate roots
pub use domain::activity::{Activity, ActivityType};
pub use domain::message::{ContentType, Message, MessageRole};
pub use domain::project::Project;
pub use domain::thread::{Thread, ThreadStatus};
pub use domain::turn::{Turn, TurnStatus};

// Re-export port interfaces
pub use domain::primitives::DomainEvent as DomainEventTrait;
pub use ports::{
    EventRepository, FileStatus, GitFileStatus, GitServicePort, GitStatus, PortError, ProviderPort,
    ReadModelRepository,
};

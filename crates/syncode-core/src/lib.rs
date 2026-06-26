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

// Re-export domain primitives
pub use domain::primitives::*;

// Re-export aggregate roots
pub use domain::project::Project;
pub use domain::thread::{Thread, ThreadStatus};
pub use domain::turn::{Turn, TurnStatus};
pub use domain::message::{Message, MessageRole, ContentType};
pub use domain::activity::{Activity, ActivityType};

// Re-export domain events
pub use domain::events::DomainEvent;

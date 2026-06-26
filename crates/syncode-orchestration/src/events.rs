//! Orchestration events — re-exports from syncode-core domain events
//!
//! The orchestration context uses the same DomainEvent enum from syncode-core.
//! This module provides convenience re-exports and orchestration-specific
//! event helpers.

pub use syncode_core::domain::events::DomainEvent;

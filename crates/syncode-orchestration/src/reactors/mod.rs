//! Side-effect reactors
//!
//! Reactors bridge between the domain (CQRS) and external systems (providers):
//! - `ingestion` — Provider events → domain events
//! - `command` — Domain commands → provider adapter calls

pub mod command;
pub mod ingestion;

pub use command::{CommandReaction, CommandReactorError, ProviderCommandReactor};
pub use ingestion::{ingest_provider_event, IngestionResult};

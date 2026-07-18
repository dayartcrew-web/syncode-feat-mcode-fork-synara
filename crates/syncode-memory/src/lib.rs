//! Syncode Memory — Persistent Interaction Context (P3-1).
//!
//! Implements PRD Section 5 ("Persistent Memory"). Provides a
//! [`MemoryProvider`] abstraction that retrieves and persists interaction
//! context, and a SQLite-backed [`SqliteMemoryStore`] implementation suitable
//! for injection into any agent pipeline.
//!
//! # Tables
//!
//! The `interactions` table is created additively (`CREATE TABLE IF NOT
//! EXISTS`) by [`SqliteMemoryStore::init_schema`], so it composes with an
//! existing database and is forward-compatible with future migrations.
//!
//! # Design
//!
//! The trait mirrors the PRD target architecture:
//!
//! ```text
//! MemoryProvider
//!   - retrieve_context(user_id, query) -> String
//!   - persist_interaction(user_id, prompt, response, provider, tokens)
//! ```
//!
//! Context retrieval returns a formatted markdown string of the N most
//! recent interactions (default 3) for the given user/project scope, which
//! an integrator injects into the provider session's system prompt. When no
//! interactions exist, it returns [`NO_PRIOR_CONTEXT`] instead of an empty
//! string so the result is renderable without a separate emptiness check.

pub mod backends;
pub mod hybrid;
pub mod provider;
pub mod sqlite_store;

pub use backends::EpisodicBackend;

#[cfg(feature = "pgvector")]
pub use backends::VectorBackend;

#[cfg(feature = "age")]
pub use backends::GraphBackend;
pub use hybrid::{
    HybridMemoryProvider, InMemoryBackend, MemoryBackend, MemoryEntry, MemoryRecord, Scope,
};
pub use provider::{MemoryProvider, MemoryProviderError, NO_PRIOR_CONTEXT};
pub use sqlite_store::{DEFAULT_CONTEXT_LIMIT, SqliteMemoryStore};

/// Default project identifier used when no project scope is supplied.
///
/// Real callers will pass a per-project identifier (e.g. a workspace path
/// hash or slug); this constant provides a stable fallback so the table's
/// `project_id` column is never NULL and per-project scoping remains
/// queryable even when the caller omits it.
pub const DEFAULT_PROJECT_ID: &str = "default";

//! `MemoryProvider` trait — the pluggable abstraction over interaction memory.
//!
//! See PRD Section 5. Implementations are responsible for storing
//! prompt/response pairs with metadata (provider, tokens, timestamp) and
//! for retrieving recent context formatted as markdown.

use async_trait::async_trait;
use thiserror::Error;

/// Errors surfaced by [`MemoryProvider`] implementations.
#[derive(Debug, Error)]
pub enum MemoryProviderError {
    /// Backing store (e.g. SQLite) failure.
    #[error("Memory store error: {0}")]
    Store(#[from] sqlx::Error),
}

/// Result alias used by all [`MemoryProvider`] methods that can fail.
pub type Result<T> = std::result::Result<T, MemoryProviderError>;

/// Pluggable abstraction over persistent interaction memory.
///
/// Implementations persist prompt/response pairs and return recent context
/// formatted as a markdown string for system-prompt augmentation. The trait
/// is `async` and `Send + Sync` so it can be shared across tokio tasks and
/// stored behind an `Arc<dyn MemoryProvider>`.
///
/// # Contract
///
/// - [`retrieve_context`](MemoryProvider::retrieve_context) returns a
///   formatted markdown string of the N most recent interactions for the
///   given `user_id` (and an optional project scope), ordered most-recent
///   first. It returns an empty string when no interactions are stored.
/// - [`persist_interaction`](MemoryProvider::persist_interaction) inserts a
///   new interaction row keyed by `user_id` with the supplied metadata. The
///   timestamp is recorded by the implementation (UTC ISO-8601).
#[async_trait]
pub trait MemoryProvider: Send + Sync {
    /// Retrieve formatted prior context for `user_id`, optionally scoped to
    /// a project. Returns an empty string when no interactions exist.
    async fn retrieve_context(&self, user_id: &str, query: &str) -> String;

    /// Persist a prompt/response pair with provider metadata and token count.
    async fn persist_interaction(
        &self,
        user_id: &str,
        prompt: &str,
        response: &str,
        provider: &str,
        tokens: u32,
    ) -> Result<()>;
}

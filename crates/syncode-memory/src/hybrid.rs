//! Hybrid memory backend — additive Tier-2 memory abstraction.
//!
//! Sibling to [`crate::MemoryProvider`]: introduces a new
//! [`MemoryBackend`] trait describing a single-store backend (vector /
//! graph / episodic / etc.), and a [`HybridMemoryProvider`] that composes
//! one or more backends and **implements [`crate::MemoryProvider`]** so it
//! drops into any pipeline currently using [`crate::SqliteMemoryStore`].
//!
//! # Non-conflict mapping
//!
//! - The existing [`crate::MemoryProvider`] trait (`provider.rs:45`) is
//!   **unchanged**. No method signatures modified, no method added.
//! - The existing [`crate::SqliteMemoryStore`] is **unchanged**. It remains
//!   a valid `MemoryProvider` implementation.
//! - New types live entirely in this file: `MemoryBackend`, `Scope`,
//!   `MemoryEntry`, `MemoryRecord`, `HybridMemoryProvider`,
//!   `InMemoryBackend`, plus a markdown formatter.
//!
//! # Architecture
//!
//! ```text
//! MemoryProvider (existing trait)
//!   ├── SqliteMemoryStore (existing impl, unchanged)
//!   └── HybridMemoryProvider (new impl)
//!         └── Vec<Arc<dyn MemoryBackend>>
//!               ├── VectorBackend   (future: pgvector)
//!               ├── GraphBackend    (future: Apache AGE)
//!               └── EpisodicBackend (future: JSONL append-only)
//! ```
//!
//! For T4 we ship the trait + a reference in-memory backend so the contract
//! is testable today; concrete vector/graph/episodic backends are deferred
//! to follow-up tasks (each adds a new file, none modify this one).

use crate::provider::{MemoryProvider, MemoryProviderError, NO_PRIOR_CONTEXT, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Scope at which a memory entry is stored / retrieved.
///
/// Mirrors the source project's scope taxonomy (scope.rs:17-27). Narrower
/// scopes shadow broader ones only when the backend chooses; the hybrid
/// provider passes the same scope to every backend and merges results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Scope {
    /// Per-session — ephemeral, single conversation.
    Session,
    /// Per-project — workspace-scoped long-term memory.
    Project,
    /// Per-user — cross-project user preferences and history.
    User,
    /// Global — shared across all users and projects.
    Global,
}

impl Scope {
    /// Stable string tag for serialization / logging. Lowercase kebab.
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Session => "session",
            Scope::Project => "project",
            Scope::User => "user",
            Scope::Global => "global",
        }
    }
}

/// A memory entry to be persisted. The shape mirrors the fields
/// [`MemoryProvider::persist_interaction`] accepts, plus a [`Scope`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub user_id: String,
    pub prompt: String,
    pub response: String,
    pub provider: String,
    pub tokens: u32,
    pub scope: Scope,
}

/// A retrieved memory record. `score` is backend-specific (cosine
/// similarity for vector, recency-weighted for episodic, etc.); the hybrid
/// provider sorts merged results by descending score.
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecord {
    pub prompt: String,
    pub response: String,
    pub provider: String,
    pub tokens: u32,
    /// Backend-reported relevance score in `[0.0, 1.0]`. Higher = more
    /// relevant. Backends that don't compute a score should emit `1.0` so
    /// the record isn't unfairly penalised during merge.
    pub score: f64,
}

/// Sibling trait to [`MemoryProvider`]. Describes a single-store backend.
///
/// Where [`MemoryProvider`] returns a formatted markdown string (suited for
/// system-prompt augmentation), [`MemoryBackend`] returns structured
/// [`MemoryRecord`]s so the composing provider can merge, re-rank, and
/// format. Backends are `Send + Sync` and typically wrapped in `Arc<dyn
/// MemoryBackend>` so the same instance can be shared across tasks.
#[async_trait]
pub trait MemoryBackend: Send + Sync {
    /// Stable identifier for logging / diagnostics (e.g. `"vector-pgvector"`).
    fn name(&self) -> &'static str;

    /// Persist `entry`. Idempotency is the caller's responsibility — backends
    /// should append (or upsert by their preferred key) without complaint.
    async fn store(&self, entry: &MemoryEntry) -> Result<()>;

    /// Return up to `k` records matching `query` for `user_id` at `scope`.
    /// Backends decide their own ordering; the composing provider re-sorts.
    async fn retrieve(
        &self,
        user_id: &str,
        query: &str,
        k: usize,
        scope: Scope,
    ) -> Result<Vec<MemoryRecord>>;
}

/// Error returned when a backend list is empty but a retrieve was attempted.
///
/// Distinct from [`MemoryProviderError::Store`] because "no backends
/// configured" is a programming error, not a storage failure.
#[derive(Debug, thiserror::Error)]
#[error("no memory backends configured")]
pub struct NoBackendsConfigured;

/// Composes one or more [`MemoryBackend`]s and exposes them as a
/// [`MemoryProvider`].
///
/// On [`MemoryProvider::persist_interaction`], the entry is fanned out to
/// every backend in registration order. Any backend error short-circuits
/// the remaining backends (caller-visible via the `Result`).
///
/// On [`MemoryProvider::retrieve_context`], every backend is queried in
/// parallel-by-await (sequential awaits, no `join_all` to keep the surface
/// minimal). Results are merged, sorted by descending `score`, truncated
/// to `k`, and formatted as markdown. If no backend yields any record,
/// returns [`NO_PRIOR_CONTEXT`] (matching the trait contract).
pub struct HybridMemoryProvider {
    backends: Vec<Arc<dyn MemoryBackend>>,
    default_scope: Scope,
    k: usize,
}

impl HybridMemoryProvider {
    /// Create an empty provider. Add backends via [`Self::with_backend`].
    /// Defaults: `scope = Scope::User`, `k = 3` (matches
    /// [`crate::sqlite_store::DEFAULT_CONTEXT_LIMIT`]).
    pub fn new() -> Self {
        Self {
            backends: Vec::new(),
            default_scope: Scope::User,
            k: usize::try_from(crate::sqlite_store::DEFAULT_CONTEXT_LIMIT).unwrap_or(3),
        }
    }

    /// Builder: append a backend. Chainable.
    #[must_use]
    pub fn with_backend(mut self, backend: Arc<dyn MemoryBackend>) -> Self {
        self.backends.push(backend);
        self
    }

    /// Builder: set the default scope used when persisting / retrieving
    /// without an explicit scope. Chainable.
    #[must_use]
    pub fn with_default_scope(mut self, scope: Scope) -> Self {
        self.default_scope = scope;
        self
    }

    /// Builder: set the max records returned by [`MemoryProvider::retrieve_context`].
    #[must_use]
    pub fn with_k(mut self, k: usize) -> Self {
        self.k = k;
        self
    }

    /// Number of configured backends. Diagnostics helper.
    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }
}

impl Default for HybridMemoryProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MemoryProvider for HybridMemoryProvider {
    async fn retrieve_context(&self, user_id: &str, query: &str) -> String {
        if self.backends.is_empty() {
            return NO_PRIOR_CONTEXT.to_string();
        }

        let mut merged: Vec<MemoryRecord> = Vec::new();
        for backend in &self.backends {
            // Per-backend failure degrades gracefully: skip the failed
            // backend's contribution but keep the others. The persist path
            // remains strict (errors propagate); retrieve is best-effort.
            match backend
                .retrieve(user_id, query, self.k, self.default_scope)
                .await
            {
                Ok(records) => merged.extend(records),
                Err(err) => {
                    tracing::warn!(
                        backend = backend.name(),
                        error = %err,
                        "memory backend retrieve failed; skipping"
                    );
                }
            }
        }

        if merged.is_empty() {
            return NO_PRIOR_CONTEXT.to_string();
        }

        // Sort by descending score; stable so equal-score records keep
        // backend insertion order (deterministic for tests).
        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(self.k);

        format_records_as_markdown(&merged)
    }

    async fn persist_interaction(
        &self,
        user_id: &str,
        prompt: &str,
        response: &str,
        provider: &str,
        tokens: u32,
    ) -> Result<()> {
        if self.backends.is_empty() {
            // Programming error — surface as a Store-shaped error so the
            // existing MemoryProviderError variants are reused (no new
            // variant added, preserving the enum's shape).
            return Err(MemoryProviderError::Store(sqlx::Error::Configuration(
                "no memory backends configured on HybridMemoryProvider".into(),
            )));
        }

        let entry = MemoryEntry {
            user_id: user_id.to_string(),
            prompt: prompt.to_string(),
            response: response.to_string(),
            provider: provider.to_string(),
            tokens,
            scope: self.default_scope,
        };

        for backend in &self.backends {
            backend.store(&entry).await?;
        }
        Ok(())
    }
}

/// Format retrieved records as a markdown bullet list for system-prompt
/// augmentation. Mirrors the markdown shape [`crate::SqliteMemoryStore`]
/// produces so downstream prompts can't tell the two providers apart.
fn format_records_as_markdown(records: &[MemoryRecord]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## Recent Context");
    let _ = writeln!(out);
    for r in records {
        // Truncate long fields so the formatted context stays bounded.
        let prompt_preview = truncate(r.prompt.as_str(), 80);
        let response_preview = truncate(r.response.as_str(), 160);
        let _ = writeln!(
            out,
            "- **{}** ({} tokens): {} → {}",
            r.provider, r.tokens, prompt_preview, response_preview
        );
    }
    out
}

/// Truncate `s` to `max_chars`, appending `…` if truncated. Returns a
/// borrowed slice when no truncation is needed (zero-alloc fast path).
fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_chars).collect();
        format!("{truncated}…")
    }
}

/// Reference in-memory backend for tests and trivial embed scenarios.
///
/// Stores every entry in a `Vec` behind a [`tokio::sync::Mutex`]. Retrieve
/// returns the most-recent `k` entries for the matching `user_id` (and
/// ignores `query` / `scope` — scoring is left to "real" backends). Score
/// is fixed at `1.0` so merged sorts don't unfairly deprioritise it.
pub struct InMemoryBackend {
    name: &'static str,
    entries: Mutex<Vec<MemoryEntry>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self {
            name: "in-memory",
            entries: Mutex::new(Vec::new()),
        }
    }

    /// Constructor that lets a test name the backend (useful when multiple
    /// backends are wired in and you want to tell them apart in logs).
    pub fn named(name: &'static str) -> Self {
        Self {
            name,
            entries: Mutex::new(Vec::new()),
        }
    }

    /// Snapshot count of stored entries (any user / scope). Test helper.
    pub async fn len(&self) -> usize {
        self.entries.lock().await.len()
    }

    /// True if no entries have been stored. Test helper.
    pub async fn is_empty(&self) -> bool {
        self.entries.lock().await.is_empty()
    }
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MemoryBackend for InMemoryBackend {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn store(&self, entry: &MemoryEntry) -> Result<()> {
        self.entries.lock().await.push(entry.clone());
        Ok(())
    }

    async fn retrieve(
        &self,
        user_id: &str,
        _query: &str,
        k: usize,
        _scope: Scope,
    ) -> Result<Vec<MemoryRecord>> {
        let entries = self.entries.lock().await;
        let matching: Vec<&MemoryEntry> = entries.iter().filter(|e| e.user_id == user_id).collect();

        // Take the most-recent k matching entries (last k of the Vec, since
        // store appends).
        let start = matching.len().saturating_sub(k);
        Ok(matching[start..]
            .iter()
            .map(|e| MemoryRecord {
                prompt: e.prompt.clone(),
                response: e.response.clone(),
                provider: e.provider.clone(),
                tokens: e.tokens,
                score: 1.0,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryProvider;

    // ------------------------------------------------------------------
    // Scope
    // ------------------------------------------------------------------

    #[test]
    fn scope_as_str_returns_lowercase_kebab_tags() {
        assert_eq!(Scope::Session.as_str(), "session");
        assert_eq!(Scope::Project.as_str(), "project");
        assert_eq!(Scope::User.as_str(), "user");
        assert_eq!(Scope::Global.as_str(), "global");
    }

    // ------------------------------------------------------------------
    // InMemoryBackend
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn in_memory_backend_stores_and_retrieves_by_user() {
        let backend = InMemoryBackend::new();
        let entry = MemoryEntry {
            user_id: "u1".into(),
            prompt: "what is syncode?".into(),
            response: "an event-sourced agent runtime".into(),
            provider: "anthropic".into(),
            tokens: 42,
            scope: Scope::User,
        };
        backend.store(&entry).await.expect("store");
        assert_eq!(backend.len().await, 1);

        let records = backend
            .retrieve("u1", "irrelevant-query", 3, Scope::User)
            .await
            .expect("retrieve");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].prompt, "what is syncode?");
        assert_eq!(records[0].response, "an event-sourced agent runtime");
        assert_eq!(records[0].provider, "anthropic");
        assert_eq!(records[0].tokens, 42);
        assert!((records[0].score - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn in_memory_backend_filters_by_user_id() {
        let backend = InMemoryBackend::new();
        backend
            .store(&sample_entry("alice", "p-a", "r-a"))
            .await
            .unwrap();
        backend
            .store(&sample_entry("bob", "p-b", "r-b"))
            .await
            .unwrap();

        let alice_only = backend
            .retrieve("alice", "", 10, Scope::User)
            .await
            .unwrap();
        assert_eq!(alice_only.len(), 1);
        assert_eq!(alice_only[0].prompt, "p-a");
    }

    #[tokio::test]
    async fn in_memory_backend_returns_most_recent_k() {
        let backend = InMemoryBackend::new();
        for i in 0..5 {
            backend
                .store(&sample_entry("u", &format!("prompt-{i}"), "r"))
                .await
                .unwrap();
        }

        // k=2 should return the last two stored entries (prompt-3, prompt-4).
        let records = backend.retrieve("u", "", 2, Scope::User).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].prompt, "prompt-3");
        assert_eq!(records[1].prompt, "prompt-4");
    }

    #[tokio::test]
    async fn in_memory_backend_retrieves_empty_for_unknown_user() {
        let backend = InMemoryBackend::new();
        backend.store(&sample_entry("u", "p", "r")).await.unwrap();
        let records = backend
            .retrieve("never-seen", "", 3, Scope::User)
            .await
            .unwrap();
        assert!(records.is_empty());
    }

    // ------------------------------------------------------------------
    // HybridMemoryProvider — builder + MemoryProvider impl
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn hybrid_with_no_backends_returns_no_prior_context() {
        let provider = HybridMemoryProvider::new();
        let ctx = provider.retrieve_context("u", "q").await;
        assert_eq!(ctx, NO_PRIOR_CONTEXT);
    }

    #[tokio::test]
    async fn hybrid_with_no_backends_errors_on_persist() {
        let provider = HybridMemoryProvider::new();
        let result = provider
            .persist_interaction("u", "p", "r", "anthropic", 1)
            .await;
        assert!(
            result.is_err(),
            "persisting with zero backends must surface a programming error"
        );
    }

    #[tokio::test]
    async fn hybrid_persists_to_every_backend_in_registration_order() {
        let b1 = Arc::new(InMemoryBackend::named("first"));
        let b2 = Arc::new(InMemoryBackend::named("second"));

        let b1_snap = b1.clone();
        let b2_snap = b2.clone();
        let provider = HybridMemoryProvider::new()
            .with_backend(b1)
            .with_backend(b2);

        provider
            .persist_interaction("u", "p", "r", "anthropic", 7)
            .await
            .expect("persist");

        assert_eq!(b1_snap.len().await, 1);
        assert_eq!(b2_snap.len().await, 1);
    }

    #[tokio::test]
    async fn hybrid_retrieves_and_merges_across_backends() {
        // Two backends each holding a different record for the same user.
        let b1 = Arc::new(InMemoryBackend::named("first"));
        let b2 = Arc::new(InMemoryBackend::named("second"));
        b1.store(&sample_entry("u", "p-1", "r-1")).await.unwrap();
        b2.store(&sample_entry("u", "p-2", "r-2")).await.unwrap();

        let provider = HybridMemoryProvider::new()
            .with_backend(b1)
            .with_backend(b2)
            .with_k(10);

        let ctx = provider.retrieve_context("u", "q").await;
        assert_ne!(ctx, NO_PRIOR_CONTEXT);
        assert!(
            ctx.contains("p-1"),
            "ctx must include backend-1's record: {ctx}"
        );
        assert!(
            ctx.contains("p-2"),
            "ctx must include backend-2's record: {ctx}"
        );
        assert!(ctx.contains("## Recent Context"));
    }

    #[tokio::test]
    async fn hybrid_truncates_to_k_records() {
        let b1 = Arc::new(InMemoryBackend::new());
        // Stuff 5 records.
        for i in 0..5 {
            b1.store(&sample_entry("u", &format!("p-{i}"), "r"))
                .await
                .unwrap();
        }

        let provider = HybridMemoryProvider::new().with_backend(b1).with_k(2);

        let ctx = provider.retrieve_context("u", "q").await;
        // k=2 → only the last 2 stored records (p-3, p-4) appear.
        assert!(ctx.contains("p-3"));
        assert!(ctx.contains("p-4"));
        assert!(!ctx.contains("p-0"));
        assert!(!ctx.contains("p-1"));
        assert!(!ctx.contains("p-2"));
    }

    #[tokio::test]
    async fn hybrid_retrieve_with_no_matching_records_returns_no_prior_context() {
        let b1 = Arc::new(InMemoryBackend::new());
        let provider = HybridMemoryProvider::new().with_backend(b1);

        // No entries stored at all.
        let ctx = provider.retrieve_context("anyone", "q").await;
        assert_eq!(ctx, NO_PRIOR_CONTEXT);
    }

    #[tokio::test]
    async fn hybrid_uses_default_scope_on_persist() {
        let b1 = Arc::new(InMemoryBackend::named("only"));
        let b1_snap = b1.clone();
        let provider = HybridMemoryProvider::new()
            .with_default_scope(Scope::Project)
            .with_backend(b1);

        provider
            .persist_interaction("u", "p", "r", "anthropic", 1)
            .await
            .unwrap();

        let entries = b1_snap.entries.lock().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].scope, Scope::Project);
    }

    #[tokio::test]
    async fn hybrid_is_memory_provider_object_safe() {
        // Confirms HybridMemoryProvider can be used as Box<dyn MemoryProvider>
        // (the trait-object form used throughout the orchestration crate).
        let provider: Box<dyn MemoryProvider> =
            Box::new(HybridMemoryProvider::new().with_backend(Arc::new(InMemoryBackend::new())));

        provider
            .persist_interaction("u", "p", "r", "anthropic", 1)
            .await
            .unwrap();
        let ctx = provider.retrieve_context("u", "q").await;
        assert!(ctx.contains("p"));
        assert!(ctx.contains("r"));
    }

    // ------------------------------------------------------------------
    // Formatter helpers
    // ------------------------------------------------------------------

    #[test]
    fn truncate_returns_input_when_within_limit() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("exactly5", 8), "exactly5");
    }

    #[test]
    fn truncate_appends_ellipsis_when_truncated() {
        let result = truncate("abcdefghij", 5);
        assert_eq!(result, "abcde…");
    }

    #[test]
    fn truncate_handles_multibyte_chars_on_char_boundary() {
        // Truncate must respect char boundaries (not split a UTF-8 codepoint).
        let result = truncate("héllo_WORLD_extra", 5);
        assert_eq!(result, "héllo…");
    }

    #[test]
    fn format_records_renders_markdown_with_section_header_and_bullets() {
        let records = vec![
            MemoryRecord {
                prompt: "what?".into(),
                response: "this".into(),
                provider: "anthropic".into(),
                tokens: 5,
                score: 0.9,
            },
            MemoryRecord {
                prompt: "why?".into(),
                response: "because".into(),
                provider: "openai".into(),
                tokens: 7,
                score: 0.5,
            },
        ];
        let md = format_records_as_markdown(&records);
        assert!(md.starts_with("## Recent Context"));
        assert!(md.contains("- **anthropic** (5 tokens): what? → this"));
        assert!(md.contains("- **openai** (7 tokens): why? → because"));
    }

    // ------------------------------------------------------------------
    // Test helpers
    // ------------------------------------------------------------------

    fn sample_entry(user: &str, prompt: &str, response: &str) -> MemoryEntry {
        MemoryEntry {
            user_id: user.into(),
            prompt: prompt.into(),
            response: response.into(),
            provider: "test".into(),
            tokens: 0,
            scope: Scope::User,
        }
    }
}

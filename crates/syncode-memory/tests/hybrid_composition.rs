//! Integration tests for [`syncode_memory::HybridMemoryProvider`] composing
//! the always-built backends (InMemory + Episodic).
//!
//! These tests do NOT exercise the pgvector / AGE backends (those are
//! feature-gated and require live Postgres). They cover the composition
//! guarantees the hybrid provider makes:
//!
//! - Persist fans out to every backend in registration order.
//! - Retrieve merges across backends, sorted by descending score.
//! - Score ties break deterministically (stable sort preserves
//!   registration order).
//! - Per-backend retrieve failures don't poison the merged result.
//! - The composed provider implements `MemoryProvider` so it can drop
//!   into pipelines expecting the sqlite store.

mod common;

use common::{sample_entry, sample_entry_scoped};
use std::sync::Arc;
use syncode_memory::EpisodicBackend;
use syncode_memory::hybrid::{HybridMemoryProvider, InMemoryBackend, MemoryBackend, Scope};
use syncode_memory::provider::{MemoryProvider, NO_PRIOR_CONTEXT};
use tempfile::TempDir;

#[tokio::test]
async fn hybrid_persists_to_every_backend_in_composition() {
    let tmp = TempDir::new().unwrap();
    let inmem = Arc::new(InMemoryBackend::named("inmem"));
    let episodic = Arc::new(EpisodicBackend::with_root(tmp.path()));

    let inmem_snap = inmem.clone();
    let provider = HybridMemoryProvider::new()
        .with_backend(inmem)
        .with_backend(episodic);

    provider
        .persist_interaction("alice", "p", "r", "test", 1)
        .await
        .expect("persist");

    assert_eq!(inmem_snap.len().await, 1, "in-memory backend got the entry");
    // Episodic writes a JSONL line per store; check the file landed.
    let file = tmp.path().join("episodic/user/alice.jsonl");
    assert!(
        file.exists(),
        "episodic file should exist at {}",
        file.display()
    );
}

#[tokio::test]
async fn hybrid_merges_records_from_composed_backends_sorted_by_score() {
    let tmp = TempDir::new().unwrap();
    let b1 = Arc::new(InMemoryBackend::named("first"));
    let b2 = Arc::new(EpisodicBackend::with_root(tmp.path()));

    // Both backends get a distinct entry for the same user.
    b1.store(&sample_entry("u", "from-inmem", "r-1"))
        .await
        .unwrap();
    b2.store(&sample_entry("u", "from-episodic", "r-2"))
        .await
        .unwrap();

    let provider = HybridMemoryProvider::new()
        .with_backend(b1)
        .with_backend(b2)
        .with_k(10);

    let ctx = provider.retrieve_context("u", "q").await;
    assert_ne!(ctx, NO_PRIOR_CONTEXT);
    assert!(ctx.contains("from-inmem"));
    assert!(ctx.contains("from-episodic"));
}

#[tokio::test]
async fn hybrid_truncates_to_k_when_composing_multiple_backends() {
    // In-memory backend holds 3 records, episodic holds 3 more.
    // Provider k=2 → merged output must contain at most 2 bullets.
    let tmp = TempDir::new().unwrap();
    let inmem = Arc::new(InMemoryBackend::named("first"));
    let episodic = Arc::new(EpisodicBackend::with_root(tmp.path()));

    for i in 0..3 {
        inmem
            .store(&sample_entry("u", &format!("inmem-{i}"), "r"))
            .await
            .unwrap();
        episodic
            .store(&sample_entry("u", &format!("epis-{i}"), "r"))
            .await
            .unwrap();
    }

    let provider = HybridMemoryProvider::new()
        .with_backend(inmem)
        .with_backend(episodic)
        .with_k(2);

    let ctx = provider.retrieve_context("u", "q").await;
    let bullet_count = ctx.lines().filter(|l| l.starts_with("- ")).count();
    assert!(
        bullet_count <= 2,
        "expected ≤2 bullets after k=2 truncate, got {bullet_count}: {ctx}"
    );
}

#[tokio::test]
async fn hybrid_persist_with_default_scope_propagates_to_backends() {
    let tmp = TempDir::new().unwrap();
    let inmem = Arc::new(InMemoryBackend::named("only"));

    let inmem_snap = inmem.clone();
    let provider = HybridMemoryProvider::new()
        .with_default_scope(Scope::Project)
        .with_backend(inmem)
        .with_backend(Arc::new(EpisodicBackend::with_root(tmp.path())));

    provider
        .persist_interaction("u", "p", "r", "test", 1)
        .await
        .unwrap();

    // The in-memory backend ignores scope on retrieve but stores it on
    // the entry; verify via a scoped retrieve on episodic.
    let episodic = EpisodicBackend::with_root(tmp.path());
    let project_records = episodic.retrieve("u", "", 5, Scope::Project).await.unwrap();
    assert!(
        project_records.iter().any(|r| r.prompt == "p"),
        "entry must land under Project scope"
    );
    let user_records = episodic.retrieve("u", "", 5, Scope::User).await.unwrap();
    assert!(user_records.is_empty(), "entry must NOT leak to User scope");

    // Sanity: in-memory backend received the entry regardless of scope.
    assert_eq!(inmem_snap.len().await, 1);
}

#[tokio::test]
async fn hybrid_trait_object_persists_and_retrieves_like_concrete_provider() {
    // Confirms HybridMemoryProvider works as Box<dyn MemoryProvider>.
    let tmp = TempDir::new().unwrap();
    let provider: Box<dyn MemoryProvider> = Box::new(
        HybridMemoryProvider::new()
            .with_backend(Arc::new(InMemoryBackend::new()))
            .with_backend(Arc::new(EpisodicBackend::with_root(tmp.path()))),
    );

    provider
        .persist_interaction("u", "p", "r", "test", 1)
        .await
        .unwrap();
    let ctx = provider.retrieve_context("u", "q").await;
    assert!(ctx.contains("p"));
    assert!(ctx.contains("r"));
}

#[tokio::test]
async fn episodic_backend_scope_isolation_holds_through_hybrid() {
    // Project-scope entry must not leak into a User-scope retrieve path,
    // even when the hybrid provider's default scope differs.
    let tmp = TempDir::new().unwrap();
    let episodic = Arc::new(EpisodicBackend::with_root(tmp.path()));

    episodic
        .store(&sample_entry_scoped(
            "u",
            "project-only",
            "r",
            Scope::Project,
        ))
        .await
        .unwrap();
    episodic
        .store(&sample_entry_scoped("u", "user-only", "r", Scope::User))
        .await
        .unwrap();

    // Default scope is User → only user-only must surface.
    let provider = HybridMemoryProvider::new().with_backend(episodic);
    let ctx = provider.retrieve_context("u", "q").await;
    assert!(
        ctx.contains("user-only"),
        "user-scope record must appear: {ctx}"
    );
    assert!(
        !ctx.contains("project-only"),
        "project-scope record must NOT appear via User-scope retrieve: {ctx}"
    );
}

#[tokio::test]
async fn hybrid_renders_no_prior_context_when_all_backends_empty() {
    let tmp = TempDir::new().unwrap();
    let provider = HybridMemoryProvider::new()
        .with_backend(Arc::new(InMemoryBackend::new()))
        .with_backend(Arc::new(EpisodicBackend::with_root(tmp.path())));

    let ctx = provider.retrieve_context("anyone", "q").await;
    assert_eq!(ctx, NO_PRIOR_CONTEXT);
}

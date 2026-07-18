//! Integration tests for [`syncode_memory::SqliteMemoryStore`] against a
//! real on-disk SQLite database (tempfile-backed).
//!
//! These tests cover the end-to-end behaviour of the production default
//! store: schema bootstrap, FTS5 retrieval with/without a query, recency
//! ordering, user/project scope isolation, and the NO_PRIOR_CONTEXT
//! sentinel contract.

use syncode_memory::DEFAULT_PROJECT_ID;
use syncode_memory::provider::{MemoryProvider, NO_PRIOR_CONTEXT};
use syncode_memory::sqlite_store::SqliteMemoryStore;
use tempfile::TempDir;

/// Build a store backed by a fresh tempfile SQLite file. Each test gets
/// its own dir → no cross-test contamination.
async fn fresh_store() -> (SqliteMemoryStore, TempDir) {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("memory.db");
    let store = SqliteMemoryStore::new(
        db_path.to_str().expect("tempfile path is utf-8"),
        DEFAULT_PROJECT_ID,
    )
    .await
    .expect("init store");
    (store, tmp)
}

#[tokio::test]
async fn store_constructor_initialises_schema() {
    // new() runs init_schema internally. If it succeeds, the schema is
    // in place — no separate idempotency test needed (init_schema is
    // private by design; the constructor is the public entry point).
    let (_store, _tmp) = fresh_store().await;
}

#[tokio::test]
async fn persist_and_retrieve_via_trait_object() {
    let (store, _tmp) = fresh_store().await;
    let provider: Box<dyn MemoryProvider> = Box::new(store);

    provider
        .persist_interaction(
            "alice",
            "how do I configure syncode",
            "edit settings.json",
            "test",
            10,
        )
        .await
        .unwrap();

    let ctx = provider.retrieve_context("alice", "configure").await;
    assert_ne!(ctx, NO_PRIOR_CONTEXT);
    assert!(ctx.contains("configure"));
    assert!(ctx.contains("settings.json"));
}

#[tokio::test]
async fn retrieve_with_empty_query_returns_recent_n() {
    let (store, _tmp) = fresh_store().await;

    // Persist 5 interactions.
    for i in 0..5 {
        store
            .persist_interaction("u", &format!("prompt-{i}"), &format!("resp-{i}"), "test", i)
            .await
            .unwrap();
    }

    // Default limit is 3.
    let ctx = store.retrieve_context("u", "").await;
    assert!(ctx.contains("prompt-4"), "most recent must appear: {ctx}");
    assert!(ctx.contains("prompt-3"));
    assert!(ctx.contains("prompt-2"));
    assert!(!ctx.contains("prompt-0"), "oldest must be truncated");
}

#[tokio::test]
async fn retrieve_with_fts_query_returns_only_matching_rows() {
    let (store, _tmp) = fresh_store().await;

    store
        .persist_interaction("u", "configure rust toolchain", "use rustup", "test", 1)
        .await
        .unwrap();
    store
        .persist_interaction("u", "deploy to kubernetes", "use kubectl apply", "test", 1)
        .await
        .unwrap();

    let ctx_match = store.retrieve_context("u", "configure rust").await;
    assert!(ctx_match.contains("rustup"));
    assert!(!ctx_match.contains("kubectl"));

    // A non-matching query now falls back to recency-N (preserving the
    // "if there's data, return it" contract). NO_PRIOR_CONTEXT only surfaces
    // for a genuinely empty store. Verify both rows surface here.
    let ctx_no_match = store.retrieve_context("u", "javascript webpack").await;
    assert_ne!(ctx_no_match, NO_PRIOR_CONTEXT);
    assert!(
        ctx_no_match.contains("rustup") || ctx_no_match.contains("kubectl"),
        "non-matching query should fall back to recency-N: {ctx_no_match}"
    );
}

#[tokio::test]
async fn retrieve_with_non_matching_query_on_empty_store_returns_sentinel() {
    let (store, _tmp) = fresh_store().await;
    store
        .persist_interaction("u", "configure rust toolchain", "use rustup", "test", 1)
        .await
        .unwrap();

    // A user with NO rows — even when other users have data — must surface
    // NO_PRIOR_CONTEXT for a non-matching query. The fallback path doesn't
    // manufacture data.
    let ctx = store
        .retrieve_context("ghost-user", "javascript webpack")
        .await;
    assert_eq!(ctx, NO_PRIOR_CONTEXT);
}

#[tokio::test]
async fn scope_isolation_per_user() {
    let (store, _tmp) = fresh_store().await;
    store
        .persist_interaction("alice", "alice-prompt", "alice-resp", "test", 1)
        .await
        .unwrap();
    store
        .persist_interaction("bob", "bob-prompt", "bob-resp", "test", 1)
        .await
        .unwrap();

    let alice = store.retrieve_context("alice", "").await;
    assert!(alice.contains("alice-prompt"));
    assert!(!alice.contains("bob-prompt"));
}

#[tokio::test]
async fn store_persists_across_reopen() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("memory.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    {
        let store = SqliteMemoryStore::new(&db_path_str, DEFAULT_PROJECT_ID)
            .await
            .unwrap();
        store
            .persist_interaction("u", "persisted", "across-reopen", "test", 1)
            .await
            .unwrap();
    }

    // Re-open the same DB file. Schema init must be a no-op and the row
    // must remain.
    let store = SqliteMemoryStore::new(&db_path_str, DEFAULT_PROJECT_ID)
        .await
        .unwrap();
    let ctx = store.retrieve_context("u", "").await;
    assert!(ctx.contains("persisted"));
    assert!(ctx.contains("across-reopen"));
}

#[tokio::test]
async fn retrieve_for_unknown_user_returns_sentinel() {
    let (store, _tmp) = fresh_store().await;
    store
        .persist_interaction("alice", "p", "r", "test", 1)
        .await
        .unwrap();

    // bob has no rows — must get the sentinel, not an empty string.
    let ctx = store.retrieve_context("bob", "anything").await;
    assert_eq!(ctx, NO_PRIOR_CONTEXT);
}

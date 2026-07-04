//! SQLite-backed [`MemoryProvider`] implementation (P3-1).
//!
//! Stores prompt/response pairs in an `interactions` table and retrieves
//! the N most recent rows per user/project as a formatted markdown string.
//!
//! # Table Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS interactions (
//!     id          INTEGER PRIMARY KEY AUTOINCREMENT,
//!     user_id     TEXT    NOT NULL,
//!     project_id  TEXT    NOT NULL,
//!     prompt      TEXT    NOT NULL,
//!     response    TEXT    NOT NULL,
//!     provider    TEXT    NOT NULL,
//!     tokens      INTEGER NOT NULL,
//!     timestamp   TEXT    NOT NULL
//! );
//! CREATE INDEX IF NOT EXISTS idx_interactions_user_project_ts
//!     ON interactions(user_id, project_id, timestamp DESC);
//! ```
//!
//! The schema is created idempotently via [`SqliteMemoryStore::init_schema`]
//! (called automatically by [`SqliteMemoryStore::new`]/[`new_in_memory`]).

use crate::provider::{MemoryProvider, Result};
use crate::DEFAULT_PROJECT_ID;
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;

/// Default number of recent interactions returned by
/// [`MemoryProvider::retrieve_context`]. Mirrors the PRD's "cap at N=3"
/// guidance to avoid context-window overflow.
pub const DEFAULT_CONTEXT_LIMIT: i64 = 3;

/// One row of the `interactions` table, as returned by the retrieval query.
///
/// `tokens` is decoded from the row but not currently surfaced in the
/// formatted context; it is retained so future retrieval strategies (e.g.
/// budget-aware selection) can use it without a schema change.
#[derive(Debug, Clone, sqlx::FromRow)]
struct InteractionRow {
    prompt: String,
    response: String,
    provider: String,
    #[allow(dead_code)]
    tokens: i64,
    timestamp: String,
}

/// SQLite-backed [`MemoryProvider`].
///
/// Wraps a [`SqlitePool`] and a per-instance project scope. The store is
/// cheaply cloneable (it holds an `Arc` internally via `SqlitePool`) and
/// safe to share across tasks.
pub struct SqliteMemoryStore {
    pool: SqlitePool,
    project_id: String,
    limit: i64,
}

impl SqliteMemoryStore {
    /// Open (or create) a SQLite database at `db_path`, initialize the
    /// `interactions` schema idempotently, and return a store scoped to
    /// `project_id`. An empty `db_path` selects an in-memory database.
    pub async fn new(db_path: &str, project_id: impl Into<String>) -> Result<Self> {
        let pool = connect(db_path).await?;
        let store = Self {
            pool,
            project_id: project_id.into(),
            limit: DEFAULT_CONTEXT_LIMIT,
        };
        store.init_schema().await?;
        Ok(store)
    }

    /// Convenience constructor for tests: opens an in-memory database scoped
    /// to [`DEFAULT_PROJECT_ID`].
    pub async fn new_in_memory() -> Result<Self> {
        Self::new("", DEFAULT_PROJECT_ID).await
    }

    /// Override the number of recent interactions returned by
    /// [`MemoryProvider::retrieve_context`]. Useful for tests that need to
    /// observe ordering across more than `DEFAULT_CONTEXT_LIMIT` rows.
    #[must_use]
    pub fn with_limit(mut self, limit: i64) -> Self {
        self.limit = limit.max(1);
        self
    }

    /// Borrow the underlying pool. Exposed so callers (e.g. an integrator
    /// that wants to colocate this table with other read models) can run
    /// their own queries.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Create the `interactions` table and its index if they don't exist.
    /// Idempotent — safe to call on a database that already has the schema.
    async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS interactions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     TEXT    NOT NULL,
                project_id  TEXT    NOT NULL,
                prompt      TEXT    NOT NULL,
                response    TEXT    NOT NULL,
                provider    TEXT    NOT NULL,
                tokens      INTEGER NOT NULL,
                timestamp   TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_interactions_user_project_ts
                ON interactions(user_id, project_id, timestamp DESC);
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Format `rows` (most-recent first) into the PRD's markdown shape:
    ///
    /// ```markdown
    /// ## Prior Context
    ///
    /// ### Interaction 1 (2026-07-04, claude)
    /// **User:** How do I add a new RPC handler?
    /// **Assistant:** Add a dispatch arm in rpc.rs...
    ///
    /// ### Interaction 2 (...)
    /// ```
    ///
    /// Returns an empty string when `rows` is empty (no prior context),
    /// so callers can concatenate the result unconditionally.
    fn format_context(rows: &[InteractionRow]) -> String {
        if rows.is_empty() {
            return String::new();
        }
        let mut out = String::from("## Prior Context\n");
        for (idx, row) in rows.iter().enumerate() {
            // idx is bounded by the SELECT LIMIT, so +1 cannot overflow.
            let n = idx + 1;
            out.push_str(&format!(
                "\n### Interaction {n} ({ts}, {provider})\n**User:** {prompt}\n**Assistant:** {response}\n",
                n = n,
                ts = row.timestamp,
                provider = row.provider,
                prompt = row.prompt,
                response = row.response,
            ));
        }
        out
    }
}

#[async_trait]
impl MemoryProvider for SqliteMemoryStore {
    async fn retrieve_context(&self, user_id: &str, _query: &str) -> String {
        // The `query` argument is reserved for future semantic-search retrieval
        // (PRD P3-2 lists context retrieval as a separate follow-on task).
        // For P3-1 we return the N most recent interactions for the scope,
        // which is the contract documented in the PRD target design.
        let rows: std::result::Result<Vec<InteractionRow>, sqlx::Error> = sqlx::query_as(
            r#"
            SELECT prompt, response, provider, tokens, timestamp
            FROM interactions
            WHERE user_id = ? AND project_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
            "#,
        )
        .bind(user_id)
        .bind(&self.project_id)
        .bind(self.limit)
        .fetch_all(&self.pool)
        .await;

        match rows {
            Ok(rows) => Self::format_context(&rows),
            // Surface the error via tracing and degrade to no-context rather
            // than panicking; a transient store failure should not break the
            // provider turn.
            Err(e) => {
                tracing::warn!(error = %e, "memory: retrieve_context failed");
                String::new()
            }
        }
    }

    async fn persist_interaction(
        &self,
        user_id: &str,
        prompt: &str,
        response: &str,
        provider: &str,
        tokens: u32,
    ) -> Result<()> {
        // UTC ISO-8601 timestamp, recorded by the store (not the caller) so
        // callers can't forge ordering and so the format is consistent.
        let timestamp = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO interactions
                (user_id, project_id, prompt, response, provider, tokens, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(&self.project_id)
        .bind(prompt)
        .bind(response)
        .bind(provider)
        // SQLite stores INTEGER; u32 always fits in i64.
        .bind(i64::from(tokens))
        .bind(&timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// Build a [`SqlitePool`] for `db_path` (empty = in-memory) with sensible
/// defaults: `create_if_missing`, WAL journal, and a 5s busy timeout. The
/// in-memory variant uses a private pool (max 1 connection) so the database
/// isn't dropped between queries — required for in-memory tests.
async fn connect(db_path: &str) -> Result<SqlitePool> {
    let db_url = if db_path.is_empty() {
        "sqlite::memory:".to_string()
    } else {
        format!("sqlite:{db_path}?mode=rwc")
    };

    let options = SqliteConnectOptions::from_str(&db_url)?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(5));

    let pool = if db_path.is_empty() {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?
    } else {
        SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?
    };
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `retrieve_context` on a fresh store returns an empty string — no
    /// panic, no stale data, no error. This is the PRD's "empty" case.
    #[tokio::test]
    async fn retrieve_context_returns_empty_when_no_interactions() {
        let store = SqliteMemoryStore::new_in_memory().await.unwrap();
        let ctx = store.retrieve_context("user-1", "anything").await;
        assert!(ctx.is_empty(), "fresh store should have no context");
    }

    /// Persisting one interaction and then retrieving it round-trips the
    /// prompt/response/metadata. Validates the INSERT/SELECT path and the
    /// markdown formatting.
    #[tokio::test]
    async fn persist_then_retrieve_roundtrips_interaction() {
        let store = SqliteMemoryStore::new_in_memory().await.unwrap();
        store
            .persist_interaction("user-1", "How do I add an RPC?", "Add a dispatch arm", "claude", 42)
            .await
            .unwrap();

        let ctx = store.retrieve_context("user-1", "").await;
        assert!(ctx.starts_with("## Prior Context"), "header missing: {ctx}");
        assert!(ctx.contains("Interaction 1"), "numbered entry missing: {ctx}");
        assert!(ctx.contains("How do I add an RPC?"), "prompt missing: {ctx}");
        assert!(ctx.contains("Add a dispatch arm"), "response missing: {ctx}");
        assert!(ctx.contains("claude"), "provider missing: {ctx}");
    }

    /// The store caps retrieved context at `limit` rows (default 3), most-
    /// recent first. Persisting 5 interactions should return only the 3
    /// newest, in DESC order — guarding against both the overflow risk
    /// documented in the PRD risk register and against ascending-order bugs.
    #[tokio::test]
    async fn retrieve_context_caps_at_limit_and_orders_most_recent_first() {
        let store = SqliteMemoryStore::new_in_memory().await.unwrap();
        // Persist 5 interactions with distinct prompts so we can assert
        // ordering. Tiny sleeps ensure each timestamp is unique (RFC3339
        // has sub-second precision but SQLite TEXT comparisons still need
        // strictly increasing values to be deterministic).
        for i in 1..=5 {
            store
                .persist_interaction(
                    "user-2",
                    &format!("prompt-{i}"),
                    &format!("resp-{i}"),
                    "claude",
                    10 * i as u32,
                )
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        let ctx = store.retrieve_context("user-2", "").await;
        // Only the 3 most recent should appear, newest first.
        assert!(ctx.contains("prompt-5"), "newest missing: {ctx}");
        assert!(ctx.contains("prompt-4"), "second missing: {ctx}");
        assert!(ctx.contains("prompt-3"), "third missing: {ctx}");
        assert!(!ctx.contains("prompt-2"), "older row leaked past limit: {ctx}");
        assert!(!ctx.contains("prompt-1"), "oldest row leaked past limit: {ctx}");

        // Ordering: prompt-5 must appear before prompt-3 (DESC by timestamp).
        let p5 = ctx.find("prompt-5").unwrap();
        let p3 = ctx.find("prompt-3").unwrap();
        assert!(p5 < p3, "expected newest first, got order p5={p5} p3={p3}");
    }

    /// Per-user scoping: interactions for one user must not bleed into
    /// another user's context. Guards the privacy/isolation concern raised
    /// in the PRD risk register.
    #[tokio::test]
    async fn retrieve_context_is_scoped_per_user() {
        let store = SqliteMemoryStore::new_in_memory().await.unwrap();
        store
            .persist_interaction("alice", "alice-prompt", "alice-resp", "claude", 5)
            .await
            .unwrap();
        store
            .persist_interaction("bob", "bob-prompt", "bob-resp", "claude", 5)
            .await
            .unwrap();

        let alice_ctx = store.retrieve_context("alice", "").await;
        assert!(alice_ctx.contains("alice-prompt"));
        assert!(!alice_ctx.contains("bob-prompt"), "user isolation violated");
    }

    /// A file-backed store survives across two store instances (close +
    /// reopen), proving the data is genuinely persistent rather than in-
    /// memory only.
    #[tokio::test]
    async fn file_backed_store_persists_across_reopen() {
        let tmp = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let db_path = tmp.to_str().unwrap().to_string();

        {
            let store = SqliteMemoryStore::new(&db_path, "proj").await.unwrap();
            store
                .persist_interaction("u", "persist me", "across reopen", "claude", 1)
                .await
                .unwrap();
        }

        // Reopen the same file — a brand-new store instance should see the
        // row written by the first one.
        let store = SqliteMemoryStore::new(&db_path, "proj").await.unwrap();
        let ctx = store.retrieve_context("u", "").await;
        assert!(ctx.contains("persist me"), "data not persisted to disk: {ctx}");
        assert!(ctx.contains("across reopen"));
    }
}

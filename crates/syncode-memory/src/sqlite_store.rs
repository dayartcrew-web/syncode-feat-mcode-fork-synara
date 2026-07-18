//! SQLite-backed [`MemoryProvider`] implementation (P3-1).
//!
//! Stores prompt/response pairs in an `interactions` table and retrieves
//! the N most recent rows per user/project as a formatted markdown string.
//! When the caller passes a non-empty `query`, retrieval is ranked by FTS5
//! relevance over `prompt` + `response`; an empty query falls back to the
//! most-recent-N contract.
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
//!
//! -- FTS5 sidecar (content linked to interactions so writes stay single-table).
//! CREATE VIRTUAL TABLE IF NOT EXISTS interactions_fts USING fts5(
//!     prompt, response,
//!     content='interactions', content_rowid='id'
//! );
//! -- Triggers keep the FTS index in sync with INSERTs on interactions.
//! CREATE TRIGGER IF NOT EXISTS interactions_ai AFTER INSERT ON interactions BEGIN
//!     INSERT INTO interactions_fts(rowid, prompt, response)
//!     VALUES (new.id, new.prompt, new.response);
//! END;
//! ```
//!
//! The schema is created idempotently via [`SqliteMemoryStore::init_schema`]
//! (called automatically by [`SqliteMemoryStore::new`]/[`new_in_memory`]).

use crate::DEFAULT_PROJECT_ID;
use crate::provider::{MemoryProvider, NO_PRIOR_CONTEXT, Result};
use async_trait::async_trait;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
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

    /// Create the `interactions` table, its index, and the FTS5 sidecar if
    /// they don't exist. Idempotent — safe to call on a database that already
    /// has the schema. The FTS5 virtual table + INSERT trigger enable MATCH
    /// retrieval when the caller passes a non-empty `query` to
    /// [`MemoryProvider::retrieve_context`].
    async fn init_schema(&self) -> Result<()> {
        // Base table + index — split from the FTS5 DDL because SQLite's
        // `execute` only runs the first statement when multiple are concatenated
        // with `;` depending on the driver's statement-splitting mode.
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
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS idx_interactions_user_project_ts
                ON interactions(user_id, project_id, timestamp DESC);"#,
        )
        .execute(&self.pool)
        .await?;
        // FTS5 sidecar linked to `interactions` so the base table remains the
        // source of truth and writes don't need a separate code path. The
        // AFTER INSERT trigger mirrors each row into the FTS index; we don't
        // need DELETE/UPDATE triggers because the store never mutates rows.
        sqlx::query(
            r#"CREATE VIRTUAL TABLE IF NOT EXISTS interactions_fts USING fts5(
                prompt, response,
                content='interactions', content_rowid='id'
            );"#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"CREATE TRIGGER IF NOT EXISTS interactions_ai AFTER INSERT ON interactions BEGIN
                INSERT INTO interactions_fts(rowid, prompt, response)
                VALUES (new.id, new.prompt, new.response);
            END;"#,
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
    /// Only the date portion (`YYYY-MM-DD`) of each row's RFC-3339 timestamp
    /// is rendered in the heading; the full-precision timestamp remains in
    /// the database for budget/audit queries. Returns
    /// [`NO_PRIOR_CONTEXT`] when `rows` is empty so the result can be
    /// rendered unconditionally without a separate emptiness check.
    /// Most-recent-N rows for the (user_id, project_id) scope, ordered by
    /// timestamp DESC. Shared by the empty-query path and the FTS5-zero-
    /// matches fallback so both produce identical recency-ranked output.
    ///
    /// Returns an empty `Vec` on store error (which surfaces as
    /// [`NO_PRIOR_CONTEXT`] via [`Self::format_context`]) rather than
    /// propagating the error — retrieval is best-effort.
    async fn fetch_recent(&self, user_id: &str) -> Vec<InteractionRow> {
        match sqlx::query_as(
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
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error = %e, "memory: fetch_recent failed");
                Vec::new()
            }
        }
    }

    fn format_context(rows: &[InteractionRow]) -> String {
        if rows.is_empty() {
            return String::from(NO_PRIOR_CONTEXT);
        }
        let mut out = String::from("## Prior Context\n");
        for (idx, row) in rows.iter().enumerate() {
            // idx is bounded by the SELECT LIMIT, so +1 cannot overflow.
            let n = idx + 1;
            // `timestamp` is RFC-3339 (`to_rfc3339()`), whose first 10 chars
            // are exactly `YYYY-MM-DD`. Truncating avoids pulling in another
            // parse/format pass while still rendering a stable, sortable date
            // in the heading (sub-second precision would only add noise in a
            // human-readable context block).
            let date = truncate_iso_date(&row.timestamp);
            out.push_str(&format!(
                "\n### Interaction {n} ({date}, {provider})\n**User:** {prompt}\n**Assistant:** {response}\n",
                n = n,
                date = date,
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
    async fn retrieve_context(&self, user_id: &str, query: &str) -> String {
        // Three retrieval modes:
        //   - empty `query`: most-recent-N for the scope (PRD P3-1 contract).
        //   - non-empty `query` with FTS5 matches: MATCH against prompt+response,
        //     ranked by bm25 relevance then recency, capped at N.
        //   - non-empty `query` with ZERO FTS5 matches: fall back to most-recent-N
        //     so callers passing a "context hint" rather than a strict search
        //     query still receive prior context instead of NO_PRIOR_CONTEXT.
        //
        // FTS5 query syntax errors fall through to recency-N too (via the
        // `Ok(vec![])` arm after a swallowed inner error) — a bad query never
        // breaks the provider turn.
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Self::format_context(&self.fetch_recent(user_id).await);
        }

        // FTS5 ranking: bm25() returns a relevance score (lower = more
        // relevant, hence ASC). Ties fall back to recency (timestamp DESC).
        // The MATCH pattern is parameterized — sqlx bind ensures the value
        // cannot escape into SQL syntax. We do NOT massage the query here;
        // FTS5 query syntax (e.g. `prefix*`, `"phrase"`, `OR`) is supported
        // end-to-end.
        let fts_rows: std::result::Result<Vec<InteractionRow>, sqlx::Error> = sqlx::query_as(
            r#"
            SELECT i.prompt, i.response, i.provider, i.tokens, i.timestamp
            FROM interactions i
            JOIN interactions_fts f ON f.rowid = i.id
            WHERE i.user_id = ? AND i.project_id = ?
              AND interactions_fts MATCH ?
            ORDER BY bm25(interactions_fts) ASC, i.timestamp DESC
            LIMIT ?
            "#,
        )
        .bind(user_id)
        .bind(&self.project_id)
        .bind(trimmed)
        .bind(self.limit)
        .fetch_all(&self.pool)
        .await;

        match fts_rows {
            Ok(rows) if !rows.is_empty() => Self::format_context(&rows),
            // Zero matches OR FTS5 syntax error: fall back to recency-N.
            // Recency-N returns NO_PRIOR_CONTEXT on its own when the user has
            // no rows, preserving the sentinel contract.
            Ok(_) => Self::format_context(&self.fetch_recent(user_id).await),
            Err(e) => {
                tracing::warn!(error = %e, "memory: FTS5 query failed, falling back to recency");
                Self::format_context(&self.fetch_recent(user_id).await)
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

/// Reduce an RFC-3339 timestamp like `2026-07-04T13:45:02.123+00:00` to its
/// `YYYY-MM-DD` prefix for display. Falls back to the original string if it
/// is shorter than 10 chars, which is a defensive no-op — `persist_interaction`
/// always writes a full RFC-3339 value via `chrono::Utc::now().to_rfc3339()`.
fn truncate_iso_date(timestamp: &str) -> &str {
    if timestamp.len() >= 10 {
        &timestamp[..10]
    } else {
        timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `retrieve_context` on a fresh store returns the documented
    /// [`NO_PRIOR_CONTEXT`] sentinel — no panic, no stale data, and a
    /// renderable message instead of an empty string. This is the PRD's
    /// "empty" case (P3-2).
    #[tokio::test]
    async fn retrieve_context_returns_no_prior_message_when_empty() {
        let store = SqliteMemoryStore::new_in_memory().await.unwrap();
        let ctx = store.retrieve_context("user-1", "anything").await;
        assert_eq!(
            ctx, NO_PRIOR_CONTEXT,
            "fresh store should surface the no-prior-context sentinel"
        );
    }

    /// Persisting one interaction and then retrieving it round-trips the
    /// prompt/response/metadata. Validates the INSERT/SELECT path and the
    /// markdown formatting.
    #[tokio::test]
    async fn persist_then_retrieve_roundtrips_interaction() {
        let store = SqliteMemoryStore::new_in_memory().await.unwrap();
        store
            .persist_interaction(
                "user-1",
                "How do I add an RPC?",
                "Add a dispatch arm",
                "claude",
                42,
            )
            .await
            .unwrap();

        let ctx = store.retrieve_context("user-1", "").await;
        assert!(ctx.starts_with("## Prior Context"), "header missing: {ctx}");
        assert!(
            ctx.contains("Interaction 1"),
            "numbered entry missing: {ctx}"
        );
        assert!(
            ctx.contains("How do I add an RPC?"),
            "prompt missing: {ctx}"
        );
        assert!(
            ctx.contains("Add a dispatch arm"),
            "response missing: {ctx}"
        );
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
        assert!(
            !ctx.contains("prompt-2"),
            "older row leaked past limit: {ctx}"
        );
        assert!(
            !ctx.contains("prompt-1"),
            "oldest row leaked past limit: {ctx}"
        );

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
        assert!(
            ctx.contains("persist me"),
            "data not persisted to disk: {ctx}"
        );
        assert!(ctx.contains("across reopen"));
    }

    // ─────────────────────────── P3-2: context retrieval ──────────────────

    /// The empty-store sentinel is also returned for a user who has no rows
    /// while *another* user does — scoping + emptiness must compose. Guards
    /// the contract that the no-context message surfaces per-scope, not just
    /// for a globally empty table (P3-2).
    #[tokio::test]
    async fn retrieve_context_returns_sentinel_for_unknown_user_when_others_have_data() {
        let store = SqliteMemoryStore::new_in_memory().await.unwrap();
        store
            .persist_interaction("alice", "alice-prompt", "alice-resp", "claude", 5)
            .await
            .unwrap();

        let ghost_ctx = store.retrieve_context("never-seen", "").await;
        assert_eq!(
            ghost_ctx, NO_PRIOR_CONTEXT,
            "an unknown user should get the no-prior-context sentinel, not alice's data"
        );
    }

    /// The interaction heading renders only the date portion (`YYYY-MM-DD`)
    /// of the stored RFC-3339 timestamp, matching the documented format
    /// example `### Interaction 1 (2026-07-04, claude)`. The full-precision
    /// timestamp stays out of the rendered block (P3-2).
    #[tokio::test]
    async fn retrieve_context_renders_date_only_in_heading() {
        let store = SqliteMemoryStore::new_in_memory().await.unwrap();
        store
            .persist_interaction("user-3", "p", "r", "claude", 1)
            .await
            .unwrap();

        let ctx = store.retrieve_context("user-3", "").await;
        // Today's date prefix must appear, but the full RFC-3339 `T...` part
        // must NOT leak into the heading.
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        assert!(
            ctx.contains(&today),
            "heading should contain today's date {today}: {ctx}"
        );
        // The 'T' separator only exists in the full timestamp; its absence in
        // any heading line proves date-only rendering. We check the first
        // interaction heading line specifically.
        let heading_line = ctx
            .lines()
            .find(|l| l.starts_with("### Interaction 1"))
            .expect("interaction heading present");
        assert!(
            !heading_line.contains('T'),
            "heading should be date-only, got: {heading_line}"
        );
    }

    // ─────────────────────────── P3-3: persistence ────────────────────────

    /// `persist_interaction` must round-trip the full metadata tuple —
    /// provider, token count, prompt, and response — not just the text
    /// fields. This directly exercises the P3-3 acceptance criterion that
    /// the store records provider + tokens alongside the pair.
    #[tokio::test]
    async fn persist_interaction_records_provider_and_token_metadata() {
        let store = SqliteMemoryStore::new_in_memory().await.unwrap();
        store
            .persist_interaction("meta-user", "the prompt", "the response", "openai", 1337)
            .await
            .unwrap();

        let ctx = store.retrieve_context("meta-user", "").await;
        assert!(ctx.contains("the prompt"), "prompt not stored: {ctx}");
        assert!(ctx.contains("the response"), "response not stored: {ctx}");
        assert!(
            ctx.contains("openai"),
            "provider metadata not stored: {ctx}"
        );
        // tokens are intentionally not surfaced in the rendered markdown
        // (kept for future budget-aware retrieval), so we verify them at the
        // row level instead.
        let count: (i64,) = sqlx::query_as("SELECT tokens FROM interactions WHERE user_id = ?")
            .bind("meta-user")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(count.0, 1337, "token count not persisted correctly");
    }

    /// Two interactions persisted for the same user both come back on
    /// retrieval, in most-recent-first order — proving the store doesn't
    /// overwrite previous rows and that `retrieve_context` surfaces the
    /// full recent history up to the limit (P3-3 + P3-2 ordering).
    #[tokio::test]
    async fn persist_multiple_interactions_all_retrievable_most_recent_first() {
        let store = SqliteMemoryStore::new_in_memory()
            .await
            .unwrap()
            .with_limit(5);
        store
            .persist_interaction("hist-user", "first-q", "first-a", "claude", 10)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        store
            .persist_interaction("hist-user", "second-q", "second-a", "openai", 20)
            .await
            .unwrap();

        let ctx = store.retrieve_context("hist-user", "").await;
        assert!(ctx.contains("first-q") && ctx.contains("first-a"));
        assert!(ctx.contains("second-q") && ctx.contains("second-a"));
        // Most-recent first: second (later timestamp) must precede first.
        let pos_second = ctx.find("second-q").unwrap();
        let pos_first = ctx.find("first-q").unwrap();
        assert!(
            pos_second < pos_first,
            "expected most-recent first, got second={pos_second} first={pos_first}"
        );
    }

    // ───────────────────── FTS5 query path (semantic retrieval) ──────────────

    /// A non-empty `query` exercises the FTS5 MATCH path. Rows whose
    /// prompt/response contain the query term are returned; rows that don't
    /// contain the term are filtered out even when they're more recent. This
    /// is the core production-readiness upgrade over the most-recent-N
    /// contract — agents can now pull *relevant* context, not just *recent*
    /// context.
    #[tokio::test]
    async fn retrieve_context_with_query_returns_only_matching_rows() {
        let store = SqliteMemoryStore::new_in_memory()
            .await
            .unwrap()
            .with_limit(5);
        store
            .persist_interaction(
                "fts-user",
                "How do I configure auth?",
                "Use the auth middleware",
                "claude",
                10,
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        store
            .persist_interaction(
                "fts-user",
                "Tell me a joke",
                "Why did the chicken cross the road?",
                "claude",
                5,
            )
            .await
            .unwrap();

        // Query for "auth" — only the auth-related interaction matches.
        let ctx = store.retrieve_context("fts-user", "auth").await;
        assert!(
            ctx.contains("configure auth"),
            "matching prompt should appear: {ctx}"
        );
        assert!(
            ctx.contains("auth middleware"),
            "matching response should appear: {ctx}"
        );
        assert!(
            !ctx.contains("chicken"),
            "non-matching row leaked into FTS results: {ctx}"
        );
    }

    /// An empty query still returns most-recent-N (the legacy contract).
    /// Guards against accidental breakage of existing call sites that pass "".
    #[tokio::test]
    async fn retrieve_context_with_empty_query_falls_back_to_recent_n() {
        let store = SqliteMemoryStore::new_in_memory()
            .await
            .unwrap()
            .with_limit(3);
        for prompt in ["alpha", "beta", "gamma"] {
            store
                .persist_interaction("empty-q-user", prompt, "resp", "claude", 1)
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let ctx = store.retrieve_context("empty-q-user", "").await;
        // All three rows surface (limit=3, only 3 stored) — proves the
        // FTS path is NOT engaged for empty queries.
        assert!(ctx.contains("alpha"), "missing alpha: {ctx}");
        assert!(ctx.contains("beta"), "missing beta: {ctx}");
        assert!(ctx.contains("gamma"), "missing gamma: {ctx}");
    }

    /// A non-empty query that FTS5 cannot match falls back to most-recent-N
    /// for the scope — the prior-context contract is "if there's any data,
    /// return it", and a non-matching query is treated as a hint, not a
    /// strict filter. The fallback preserves the orchestration pipeline's
    /// behavior where the query is often a task description rather than a
    /// search term.
    #[tokio::test]
    async fn retrieve_context_with_non_matching_query_falls_back_to_recent_n() {
        let store = SqliteMemoryStore::new_in_memory()
            .await
            .unwrap()
            .with_limit(3);
        store
            .persist_interaction(
                "no-match-user",
                "talks about rust",
                "rust is great",
                "claude",
                1,
            )
            .await
            .unwrap();

        let ctx = store.retrieve_context("no-match-user", "kotlin").await;
        assert!(
            ctx.contains("talks about rust"),
            "non-matching query should fall back to recency-N and return prior data: {ctx}"
        );
        assert!(
            ctx.contains("rust is great"),
            "non-matching query should also surface the response: {ctx}"
        );
    }

    /// A non-empty query against an empty store still surfaces
    /// [`NO_PRIOR_CONTEXT`] — the fallback path doesn't manufacture data.
    #[tokio::test]
    async fn retrieve_context_with_non_matching_query_on_empty_store_returns_sentinel() {
        let store = SqliteMemoryStore::new_in_memory().await.unwrap();
        let ctx = store.retrieve_context("ghost-user", "anything").await;
        assert_eq!(
            ctx, NO_PRIOR_CONTEXT,
            "empty store should surface no-prior-context sentinel, got: {ctx}"
        );
    }

    /// A query that matches some rows but not others only surfaces the
    /// matching rows — the recency-N fallback only triggers on ZERO matches.
    /// This guards against the fallback accidentally shadowing the FTS5
    /// ranking when a partial match exists.
    #[tokio::test]
    async fn retrieve_context_partial_match_does_not_trigger_fallback() {
        let store = SqliteMemoryStore::new_in_memory()
            .await
            .unwrap()
            .with_limit(5);
        store
            .persist_interaction("partial-user", "rust question", "rust answer", "claude", 1)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        store
            .persist_interaction(
                "partial-user",
                "kotlin question",
                "kotlin answer",
                "claude",
                1,
            )
            .await
            .unwrap();

        // "rust" matches row 1 only — fallback should NOT trigger, so the
        // kotlin row stays filtered out.
        let ctx = store.retrieve_context("partial-user", "rust").await;
        assert!(ctx.contains("rust question"));
        assert!(ctx.contains("rust answer"));
        assert!(
            !ctx.contains("kotlin"),
            "partial match should NOT fall back to recency-N: {ctx}"
        );
    }
}

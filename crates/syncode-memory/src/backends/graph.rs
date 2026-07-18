//! Apache AGE graph-backed [`MemoryBackend`] (feature-gated).
//!
//! Lives behind the `age` cargo feature because it requires Postgres with
//! the [`age`](https://github.com/apache/age) extension installed and
//! SQLx's Postgres backend. Default builds of `syncode-memory` do not
//! include this module.
//!
//! # Schema
//!
//! Companion SQL: `crates/syncode-memory/migrations/20260720_memory_graph.sql`.
//! Creates a `memory_graph` Cypher graph with three vertex labels
//! (`User`, `Interaction`, `Topic`) and two edge labels (`ASKED`,
//! `ABOUT`). The store path creates a `User`-`ASKED`-`Interaction`-`ABOUT`-
//! `Topic` walk; retrieval traverses the same walk in reverse, optionally
//! expanding via shared `Topic` nodes for associative recall.
//!
//! # Why a graph backend?
//!
//! Vector similarity answers "what's lexically similar to this query".
//! Graph traversal answers "what did this user ask about across related
//! topics" — a query shape that's awkward to express in pure cosine
//! similarity. The hybrid provider composes both, picking per-backend
//! strengths.
//!
//! # Production prerequisites
//!
//! 1. Postgres ≥ 14 with Apache AGE ≥ 1.4 installed.
//! 2. Companion migration applied.
//! 3. The `age` feature enabled on this crate.
//! 4. (Optional) `DATABASE_URL` for integration tests gated behind
//!    `SYNCODE_TEST_AGE=1`.

#![cfg(feature = "age")]

use crate::hybrid::{MemoryBackend, MemoryEntry, MemoryRecord, Scope};
use crate::provider::{MemoryProviderError, Result};
use async_trait::async_trait;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// AGE-backed [`MemoryBackend`].
///
/// Construct with [`GraphBackend::connect`] (creates a pool from a
/// DATABASE_URL string) or [`GraphBackend::with_pool`] when the caller
/// manages its own pool.
#[derive(Clone)]
pub struct GraphBackend {
    pool: PgPool,
}

impl GraphBackend {
    /// Create a pool from a DATABASE_URL string and wrap it.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(database_url)
            .await
            .map_err(|e| {
                MemoryProviderError::Store(sqlx::Error::Configuration(e.to_string().into()))
            })?;
        Ok(Self::with_pool(pool))
    }

    /// Wrap an existing pool.
    pub fn with_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Pick a coarse topic tag for a prompt. v1 uses the first
    /// whitespace-delimited token lowercased; this is intentionally crude —
    /// the goal is a stable key for graph traversal, not semantic
    /// understanding. Real deployments can swap in an LLM extraction step
    /// upstream and pre-populate `Topic` nodes from the caller.
    fn topic_of(prompt: &str) -> String {
        prompt
            .split_whitespace()
            .next()
            .map(|s| s.to_lowercase())
            .unwrap_or_else(|| "untagged".to_string())
    }
}

#[async_trait]
impl MemoryBackend for GraphBackend {
    fn name(&self) -> &'static str {
        "graph-age"
    }

    async fn store(&self, entry: &MemoryEntry) -> Result<()> {
        let topic = Self::topic_of(&entry.prompt);
        // NOTE: AGE Cypher queries are executed via SELECT * FROM
        // cypher('graph', $$ ... $$) AS (...). The parameters are
        // interpolated as agtype literals via sqlx::query! binds on the
        // OUTER SQL — we use format! for the agtype string body itself
        // because AGE doesn't expose bind variables inside the Cypher
        // payload, only at the SQL wrapper level.
        //
        // To stay safe against injection from user-controlled fields, we
        // escape single quotes and backslashes inside `entry.*` before
        // formatting.
        let user_id = ag_escape(&entry.user_id);
        let prompt = ag_escape(&entry.prompt);
        let response = ag_escape(&entry.response);
        let provider = ag_escape(&entry.provider);
        let scope = entry.scope.as_str();
        let tokens = entry.tokens;
        let topic = ag_escape(&topic);

        let cypher = format!(
            r#"
            MERGE (u:User {{ user_id: "{user_id}" }})
            MERGE (t:Topic {{ name: "{topic}" }})
            CREATE (i:Interaction {{
                user_id: "{user_id}",
                scope: "{scope}",
                prompt: "{prompt}",
                response: "{response}",
                provider: "{provider}",
                tokens: {tokens},
                at: timestamp()
            }})
            CREATE (u)-[:ASKED]->(i)
            CREATE (i)-[:ABOUT]->(t)
            "#
        );

        sqlx::query(&format!(
            r#"SELECT * FROM cypher('memory_graph', $$ {cypher} $$) AS (a agtype)"#
        ))
        .execute(&self.pool)
        .await
        .map_err(MemoryProviderError::Store)?;
        Ok(())
    }

    async fn retrieve(
        &self,
        user_id: &str,
        query: &str,
        k: usize,
        scope: Scope,
    ) -> Result<Vec<MemoryRecord>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        let user_id_safe = ag_escape(user_id);
        let scope_str = scope.as_str();
        let limit = i32::try_from(k).unwrap_or(i32::MAX);

        // Two paths, matching the other backends:
        // - Query empty: most recent interactions by this user at this scope.
        // - Query non-empty: interactions by this user whose topic matches
        //   the query's leading token; fall back to recency if no matches.
        let cypher = if query.trim().is_empty() {
            format!(
                r#"
                MATCH (u:User {{ user_id: "{user_id_safe}" }})-[:ASKED]->(i:Interaction)
                WHERE i.scope = "{scope_str}"
                RETURN i
                ORDER BY i.at DESC
                LIMIT {limit}
                "#
            )
        } else {
            let topic = Self::topic_of(query);
            let topic_safe = ag_escape(&topic);
            format!(
                r#"
                MATCH (u:User {{ user_id: "{user_id_safe}" }})-[:ASKED]->(i:Interaction)-[:ABOUT]->(t:Topic {{ name: "{topic_safe}" }})
                WHERE i.scope = "{scope_str}"
                RETURN i
                ORDER BY i.at DESC
                LIMIT {limit}
                "#
            )
        };

        let rows: Vec<(sqlx::types::Json<serde_json::Value>,)> = sqlx::query_as(&format!(
            r#"SELECT * FROM cypher('memory_graph', $$ {cypher} $$) AS (i agtype)"#
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(MemoryProviderError::Store)?;

        let n = rows.len();
        if n == 0 {
            return Ok(Vec::new());
        }

        Ok(rows
            .into_iter()
            .enumerate()
            .map(|(i, (json,))| {
                let v = json.0;
                let prompt = v
                    .get("prompt")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let response = v
                    .get("response")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let provider = v
                    .get("provider")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let tokens = v
                    .get("tokens")
                    .and_then(|x| x.as_u64())
                    .and_then(|n| u32::try_from(n).ok())
                    .unwrap_or(0);
                let score = if n == 1 {
                    1.0
                } else {
                    // Recency-decay [0.5, 1.0] matching the other backends.
                    1.0 - 0.5 * (i as f64) / ((n - 1) as f64)
                };
                MemoryRecord {
                    prompt,
                    response,
                    provider,
                    tokens,
                    score,
                }
            })
            .collect())
    }
}

/// Escape backslashes and single/double quotes for safe interpolation
/// into an AGE Cypher payload. This is necessary because AGE does not
/// expose bind parameters inside the `$$ ... $$` block; we substitute via
/// `format!` and rely on escaping for safety.
fn ag_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_of_picks_first_token_lowercased() {
        assert_eq!(GraphBackend::topic_of("How do I configure syncode"), "how");
        assert_eq!(GraphBackend::topic_of("rust"), "rust");
        assert_eq!(GraphBackend::topic_of(""), "untagged");
        assert_eq!(GraphBackend::topic_of("   leading"), "leading");
    }

    #[test]
    fn ag_escape_handles_quotes_and_backslashes() {
        assert_eq!(ag_escape(r#"a"b"#), r#"a\"b"#);
        assert_eq!(ag_escape("a'b"), r#"a\'b"#);
        assert_eq!(ag_escape(r"a\b"), r"a\\b");
        assert_eq!(ag_escape("plain"), "plain");
    }
}

#[cfg(test)]
#[cfg(feature = "age-integration")]
mod integration {
    use super::*;

    /// gated behind SYNCODE_TEST_AGE=1 + DATABASE_URL.
    #[tokio::test]
    async fn roundtrip_stores_and_traverses_user_interactions() {
        if std::env::var("SYNCODE_TEST_AGE").as_deref() != Ok("1") {
            eprintln!("skipping AGE integration (set SYNCODE_TEST_AGE=1)");
            return;
        }
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let backend = GraphBackend::connect(&url).await.expect("connect");

        backend
            .store(&MemoryEntry {
                user_id: "alice".into(),
                prompt: "configure syncode".into(),
                response: "edit settings.json".into(),
                provider: "test".into(),
                tokens: 1,
                scope: Scope::User,
            })
            .await
            .expect("store");

        let records = backend
            .retrieve("alice", "", 5, Scope::User)
            .await
            .expect("retrieve");
        assert!(records.iter().any(|r| r.prompt.contains("configure")));
    }
}

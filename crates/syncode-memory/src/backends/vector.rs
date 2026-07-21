//! pgvector-backed [`MemoryBackend`] (feature-gated).
//!
//! Lives behind the `pgvector` cargo feature because it pulls in
//! [`fastembed`] (~50 MB compiled; downloads a ~250 MB BERT model on first
//! embed) and [`sqlx`]'s Postgres backend. Default builds of
//! `syncode-memory` do not include this module.
//!
//! # Schema
//!
//! Companion SQL lives at `crates/syncode-memory/migrations/20260720_memory_vectors.sql`.
//! The `memory_vectors` table holds one row per embedded interaction, plus
//! an HNSW index over the `vector(384)` column (matches the default BAAI
//! `bge-small-en-v1.5` model used by fastembed).
//!
//! # Embedding model
//!
//! [`fastembed::TextEmbedding`] is wrapped in a [`tokio::sync::OnceCell`]
//! so the model loads lazily on first store/retrieve and is shared across
//! clones of the backend. Cold start is ~1 s + a one-time model download
//! (~250 MB cached under the user's HF cache dir). Subsequent embeds are
//! milliseconds.
//!
//! # Retrieval
//!
//! Two paths:
//! - **Query empty:** return the `k` most-recent rows for the `(user_id,
//!   scope)` pair, scored `[0.5, 1.0]` by recency decay (mirrors the
//!   episodic backend's scheme so merges behave consistently).
//! - **Query non-empty:** embed the query, run `ORDER BY embedding <=> $q
//!   LIMIT k`. pgvector's `<=>` returns cosine distance in `[0, 2]`; we
//!   convert to similarity `1.0 - distance/2` so higher = better, matching
//!   the rest of the crate's score convention.
//!
//! # Production prerequisites
//!
//! 1. Postgres ≥ 14 with the `vector` extension installed.
//! 2. The companion migration applied to the target database.
//! 3. The `pgvector` feature enabled on this crate (`cargo build
//!    --features syncode-memory/pgvector`).
//! 4. (Optional) `DATABASE_URL` pointing at the target Postgres for
//!    integration tests gated behind `SYNCODE_TEST_PGVECTOR=1`.

#![cfg(feature = "pgvector")]

use crate::hybrid::{MemoryBackend, MemoryEntry, MemoryRecord, Scope};
use crate::provider::{MemoryProviderError, Result};
use async_trait::async_trait;
use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

/// Embedding dim for the default BAAI/bge-small-en-v1.5 model. Must match
/// the `vector(N)` in the migration.
pub const EMBEDDING_DIM: usize = 384;

/// pgvector-backed [`MemoryBackend`].
///
/// Construct with [`VectorBackend::connect`] (creates a pool from a
/// DATABASE_URL-style string) or [`VectorBackend::with_pool`] when the
/// caller already manages a pool. Cheap to clone — everything is behind
/// `Arc` / `OnceCell`.
#[derive(Clone)]
pub struct VectorBackend {
    pool: PgPool,
    /// Lazily-loaded embedding model shared across all clones of this
    /// backend. The first `store` / `retrieve` call pays the cold-start
    /// cost; later clones reuse the same instance. The inner `Mutex` is
    /// required because fastembed 5.x's `embed` takes `&mut self`.
    model: Arc<OnceCell<Arc<Mutex<TextEmbedding>>>>,
}

impl VectorBackend {
    /// Create a pool from a DATABASE_URL string and wrap it. Caller is
    /// responsible for ensuring the `vector` extension is installed and
    /// the migration has been applied.
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

    /// Wrap an existing pool. Use when the caller already manages
    /// Postgres connections and wants to share them with the backend.
    pub fn with_pool(pool: PgPool) -> Self {
        Self {
            pool,
            model: Arc::new(OnceCell::new()),
        }
    }

    /// Lazily initialise the fastembed model. First call downloads the
    /// model to the user's HF cache (~250 MB); subsequent calls return
    /// the cached instance. Errors surface as `Store` for additive error
    /// enum compatibility.
    async fn embed_one(&self, text: &str) -> Result<Vec<f32>> {
        // fastembed 5.x's `TextEmbedding::embed` requires `&mut self`
        // (it mutates internal ONNX session state). We wrap the model in
        // a `Mutex` so the shared OnceCell can hand out clonable handles
        // that the blocking spawn can lock without holding the async runtime.
        let model = self
            .model
            .get_or_try_init(|| async {
                let init = TextEmbedding::try_new(
                    TextInitOptions::new(EmbeddingModel::default())
                        .with_show_download_progress(false),
                )
                .map_err(|e| {
                    MemoryProviderError::Store(sqlx::Error::Configuration(
                        format!("fastembed model init failed: {e}").into(),
                    ))
                })?;
                Ok::<_, MemoryProviderError>(Arc::new(Mutex::new(init)))
            })
            .await?
            .clone();

        // fastembed::TextEmbedding is blocking — spawn on a blocking
        // thread so the async runtime isn't held.
        let text_owned = text.to_string();
        let embed_result = tokio::task::spawn_blocking(move || {
            model.lock().map_err(|e| {
                MemoryProviderError::Store(sqlx::Error::Configuration(
                    format!("fastembed model lock poisoned: {e}").into(),
                ))
            })?.embed(vec![text_owned], None)
        })
        .await
        .map_err(|e| {
            MemoryProviderError::Store(sqlx::Error::Configuration(
                format!("embed task join failed: {e}").into(),
            ))
        })
        .and_then(|inner| {
            inner.map_err(|e| {
                MemoryProviderError::Store(sqlx::Error::Configuration(
                    format!("fastembed embed failed: {e}").into(),
                ))
            })
        })?;

        embed_result
            .into_iter()
            .next()
            .ok_or_else(|| {
                MemoryProviderError::Store(sqlx::Error::Configuration(
                    "fastembed returned no embedding".into(),
                ))
            })
    }
}

#[async_trait]
impl MemoryBackend for VectorBackend {
    fn name(&self) -> &'static str {
        "vector-pgvector"
    }

    async fn store(&self, entry: &MemoryEntry) -> Result<()> {
        let embedding = self.embed_one(&entry.prompt).await?;
        if embedding.len() != EMBEDDING_DIM {
            return Err(MemoryProviderError::Store(sqlx::Error::Configuration(
                format!(
                    "embedding dim mismatch: expected {EMBEDDING_DIM}, got {}",
                    embedding.len()
                )
                .into(),
            )));
        }

        // pgvector's sqlx integration accepts a &[f32] when the column is
        // vector(N). Bind by reference to avoid moving.
        sqlx::query(
            r#"INSERT INTO memory_vectors
                 (user_id, scope, prompt, response, provider, tokens, embedding)
               VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
        )
        .bind(&entry.user_id)
        .bind(entry.scope.as_str())
        .bind(&entry.prompt)
        .bind(&entry.response)
        .bind(&entry.provider)
        .bind(i32::try_from(entry.tokens).unwrap_or(i32::MAX))
        .bind(pgvector::Vector::from(embedding))
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

        let trimmed = query.trim();
        if trimmed.is_empty() {
            // Recency path — mirror episodic decay so merge ordering is
            // consistent across backends in the hybrid provider.
            let rows = sqlx::query_as::<_, VectorRow>(
                r#"SELECT prompt, response, provider, tokens
                     FROM memory_vectors
                    WHERE user_id = $1 AND scope = $2
                    ORDER BY created_at DESC
                    LIMIT $3"#,
            )
            .bind(user_id)
            .bind(scope.as_str())
            .bind(i32::try_from(k).unwrap_or(i32::MAX))
            .fetch_all(&self.pool)
            .await
            .map_err(MemoryProviderError::Store)?;

            return Ok(score_recency(rows));
        }

        // Similarity path — embed query, ORDER BY <=> ASC.
        let q_vec = self.embed_one(trimmed).await?;
        let rows = sqlx::query_as::<_, VectorRowWithDistance>(
            r#"SELECT prompt, response, provider, tokens,
                          embedding <=> $3 AS distance
                 FROM memory_vectors
                WHERE user_id = $1 AND scope = $2
                ORDER BY embedding <=> $3
                LIMIT $4"#,
        )
        .bind(user_id)
        .bind(scope.as_str())
        .bind(pgvector::Vector::from(q_vec))
        .bind(i32::try_from(k).unwrap_or(i32::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(MemoryProviderError::Store)?;

        Ok(rows
            .into_iter()
            .map(|r| {
                // Cosine distance ∈ [0, 2]; similarity = 1 - distance/2.
                // Clamp to [0.0, 1.0] to guard against fp drift.
                let similarity = (1.0 - r.distance / 2.0).clamp(0.0, 1.0);
                MemoryRecord {
                    prompt: r.prompt,
                    response: r.response,
                    provider: r.provider,
                    tokens: u32::try_from(r.tokens).unwrap_or(0),
                    score: similarity,
                }
            })
            .collect())
    }
}

#[derive(sqlx::FromRow)]
struct VectorRow {
    prompt: String,
    response: String,
    provider: String,
    tokens: i32,
}

#[derive(sqlx::FromRow)]
struct VectorRowWithDistance {
    prompt: String,
    response: String,
    provider: String,
    tokens: i32,
    distance: f64,
}

/// Map recency-ranked rows to MemoryRecord with linear decay in
/// `[0.5, 1.0]` (most recent = 1.0). Mirrors the episodic backend's
/// scheme so a hybrid provider composing both backends gets consistent
/// ordering. `rows` must already be ordered most-recent-first.
fn score_recency(rows: Vec<VectorRow>) -> Vec<MemoryRecord> {
    let n = rows.len();
    if n == 0 {
        return Vec::new();
    }
    rows.into_iter()
        .enumerate()
        .map(|(i, r)| {
            let score = if n == 1 {
                1.0
            } else {
                1.0 - 0.5 * (i as f64) / ((n - 1) as f64)
            };
            MemoryRecord {
                prompt: r.prompt,
                response: r.response,
                provider: r.provider,
                tokens: u32::try_from(r.tokens).unwrap_or(0),
                score,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_recency_returns_single_row_at_one() {
        let row = VectorRow {
            prompt: "p".into(),
            response: "r".into(),
            provider: "x".into(),
            tokens: 1,
        };
        let scored = score_recency(vec![row]);
        assert_eq!(scored.len(), 1);
        assert!((scored[0].score - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn score_recency_decays_linearly_to_half() {
        let rows: Vec<VectorRow> = (0..3)
            .map(|i| VectorRow {
                prompt: format!("p{i}"),
                response: "r".into(),
                provider: "x".into(),
                tokens: 1,
            })
            .collect();
        let scored = score_recency(rows);
        assert_eq!(scored.len(), 3);
        // i=0 → 1.0, i=1 → 0.75, i=2 → 0.5
        assert!((scored[0].score - 1.0).abs() < 1e-9);
        assert!((scored[1].score - 0.75).abs() < 1e-9);
        assert!((scored[2].score - 0.5).abs() < 1e-9);
    }

    #[test]
    fn embedding_dim_constant_matches_default_model() {
        // BAAI/bge-small-en-v1.5 = 384 dims. If you swap the model,
        // update both this constant and the migration's vector(N).
        assert_eq!(EMBEDDING_DIM, 384);
    }
}

#[cfg(test)]
#[cfg(feature = "pgvector-integration")]
mod integration {
    use super::*;

    /// gated behind SYNCODE_TEST_PGVECTOR=1 + a real DATABASE_URL.
    /// run with:
    ///   SYNCODE_TEST_PGVECTOR=1 DATABASE_URL=postgres://... \
    ///     cargo test -p syncode-memory --features pgvector,pgvector-integration
    #[tokio::test]
    async fn roundtrip_persists_and_retrieves_via_cosine() {
        if std::env::var("SYNCODE_TEST_PGVECTOR").as_deref() != Ok("1") {
            eprintln!("skipping pgvector integration (set SYNCODE_TEST_PGVECTOR=1)");
            return;
        }
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let backend = VectorBackend::connect(&url).await.expect("connect");

        let entry = MemoryEntry {
            user_id: "alice".into(),
            prompt: "how do I configure syncode?".into(),
            response: "edit ~/.syncode/settings.json".into(),
            provider: "test".into(),
            tokens: 1,
            scope: Scope::User,
        };
        backend.store(&entry).await.expect("store");

        let records = backend
            .retrieve("alice", "how to configure", 5, Scope::User)
            .await
            .expect("retrieve");
        assert!(records.iter().any(|r| r.prompt.contains("configure")));
    }
}

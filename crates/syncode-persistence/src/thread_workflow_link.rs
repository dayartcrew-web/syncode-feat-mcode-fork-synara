//! Sidecar table linking chat threads to active workflows.
//!
//! Threads in syncode are event-sourced aggregates — we don't store the
//! active workflow_id on the aggregate itself. Instead, this sidecar table
//! provides O(1) lookup for `ApplicationService::start_turn` to decide
//! whether to create a new workflow or append a task to an existing one.

use chrono::Utc;
use sqlx::SqlitePool;
use thiserror::Error;

/// Errors that can occur during thread-workflow link operations.
#[derive(Debug, Error)]
pub enum ThreadWorkflowLinkError {
    /// SQLite query/bind failure.
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Convenience `Result` alias for [`ThreadWorkflowLinkError`].
pub type Result<T> = std::result::Result<T, ThreadWorkflowLinkError>;

/// A row in `thread_workflow_links`.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct ThreadWorkflowLink {
    pub thread_id: String,
    pub workflow_id: String,
    pub linked_at: i64,
    pub updated_at: i64,
}

/// Returns the active workflow_id for a thread, if any.
pub async fn lookup(
    pool: &SqlitePool,
    thread_id: &str,
) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT workflow_id FROM thread_workflow_links WHERE thread_id = ?")
            .bind(thread_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(id,)| id))
}

/// Upserts the active workflow_id for a thread. If a row already exists,
/// its workflow_id and updated_at are replaced.
pub async fn upsert(
    pool: &SqlitePool,
    thread_id: &str,
    workflow_id: &str,
) -> Result<()> {
    let now = Utc::now().timestamp();
    sqlx::query(
        "INSERT INTO thread_workflow_links (thread_id, workflow_id, linked_at, updated_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(thread_id) DO UPDATE SET
            workflow_id = excluded.workflow_id,
            updated_at = excluded.updated_at",
    )
    .bind(thread_id)
    .bind(workflow_id)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Clears the active workflow link for a thread (e.g., when workflow is
/// completed or archived). Returns true if a row was deleted.
pub async fn clear(pool: &SqlitePool, thread_id: &str) -> Result<bool> {
    let result =
        sqlx::query("DELETE FROM thread_workflow_links WHERE thread_id = ?")
            .bind(thread_id)
            .execute(pool)
            .await?;
    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        crate::migrations::run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn lookup_returns_none_when_empty() {
        let pool = setup_pool().await;
        assert_eq!(lookup(&pool, "t1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn upsert_then_lookup_roundtrip() {
        let pool = setup_pool().await;
        upsert(&pool, "t1", "wf-1").await.unwrap();
        assert_eq!(lookup(&pool, "t1").await.unwrap(), Some("wf-1".to_string()));
    }

    #[tokio::test]
    async fn upsert_replaces_existing_workflow() {
        let pool = setup_pool().await;
        upsert(&pool, "t1", "wf-1").await.unwrap();
        upsert(&pool, "t1", "wf-2").await.unwrap();
        assert_eq!(lookup(&pool, "t1").await.unwrap(), Some("wf-2".to_string()));
    }

    #[tokio::test]
    async fn clear_removes_link() {
        let pool = setup_pool().await;
        upsert(&pool, "t1", "wf-1").await.unwrap();
        assert!(clear(&pool, "t1").await.unwrap());
        assert_eq!(lookup(&pool, "t1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn clear_returns_false_when_no_row() {
        let pool = setup_pool().await;
        assert!(!clear(&pool, "t1").await.unwrap());
    }
}

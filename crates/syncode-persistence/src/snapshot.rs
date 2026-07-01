//! Snapshot support — periodic state snapshots to avoid full event replay
//!
//! Snapshots store a serialized representation of an aggregate's state
//! at a given event sequence. When replaying, we load the latest snapshot
//! and replay only events after that point.

use crate::SqlitePool;
use syncode_core::EntityId;
use thiserror::Error;

/// Errors that can occur during snapshot operations
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Snapshot not found for aggregate {0}")]
    NotFound(String),
}

/// Save a snapshot for an aggregate at the given version.
pub async fn save_snapshot(
    pool: &SqlitePool,
    aggregate_id: EntityId,
    state: &serde_json::Value,
    version: u64,
) -> Result<(), SnapshotError> {
    let data = serde_json::to_string(state)
        .map_err(|e| SnapshotError::Serialization(e.to_string()))?;

    sqlx::query(
        r#"
        INSERT INTO snapshots (aggregate_id, sequence, data)
        VALUES (?, ?, ?)
        ON CONFLICT(aggregate_id) DO UPDATE SET sequence = ?, data = ?
        "#,
    )
    .bind(aggregate_id.to_string())
    .bind(version as i64)
    .bind(&data)
    .bind(version as i64)
    .bind(&data)
    .execute(pool)
    .await?;

    Ok(())
}

/// Load the latest snapshot for an aggregate.
///
/// Returns `None` if no snapshot exists, or `(state, version)` if found.
pub async fn load_snapshot(
    pool: &SqlitePool,
    aggregate_id: EntityId,
) -> Result<Option<(serde_json::Value, u64)>, SnapshotError> {
    let row: Option<(String, i64)> = sqlx::query_as(
        "SELECT data, sequence FROM snapshots WHERE aggregate_id = ?",
    )
    .bind(aggregate_id.to_string())
    .fetch_optional(pool)
    .await?;

    match row {
        Some((data, version)) => {
            let state: serde_json::Value = serde_json::from_str(&data)
                .map_err(|e| SnapshotError::Serialization(e.to_string()))?;
            Ok(Some((state, version as u64)))
        }
        None => Ok(None),
    }
}

/// Load every stored snapshot, across all aggregates.
///
/// Returns one `(aggregate_id, state, version)` per snapshot. Used by the
/// cold-start read-model rebuild to seed the projection and replay only each
/// aggregate's tail. An empty vec means no snapshots exist (the rebuild then
/// falls back to a full replay).
pub async fn load_all_snapshots(
    pool: &SqlitePool,
) -> Result<Vec<(EntityId, serde_json::Value, u64)>, SnapshotError> {
    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT aggregate_id, data, sequence FROM snapshots",
    )
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for (id_str, data, version) in rows {
        // aggregate_id is always stored as a valid UUID string (via
        // EntityId::to_string); a parse failure here signals corruption.
        let aggregate_id = EntityId::parse(&id_str)
            .map_err(|e| SnapshotError::Serialization(format!("invalid aggregate_id: {e}")))?;
        let state: serde_json::Value = serde_json::from_str(&data)
            .map_err(|e| SnapshotError::Serialization(e.to_string()))?;
        out.push((aggregate_id, state, version as u64));
    }
    Ok(out)
}

/// Delete a snapshot for an aggregate.
pub async fn delete_snapshot(
    pool: &SqlitePool,
    aggregate_id: EntityId,
) -> Result<bool, SnapshotError> {
    let result = sqlx::query("DELETE FROM snapshots WHERE aggregate_id = ?")
        .bind(aggregate_id.to_string())
        .execute(pool)
        .await?;

    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_database;
    use std::path::Path;

    #[tokio::test]
    async fn test_save_and_load_snapshot() {
        let pool = init_database(Path::new("")).await.expect("database");
        let agg_id = EntityId::new();

        let state = serde_json::json!({
            "name": "Test Project",
            "turn_count": 5
        });

        save_snapshot(&pool, agg_id, &state, 10)
            .await
            .expect("save snapshot");

        let loaded = load_snapshot(&pool, agg_id)
            .await
            .expect("load snapshot");

        assert!(loaded.is_some());
        let (loaded_state, version) = loaded.unwrap();
        assert_eq!(version, 10);
        assert_eq!(loaded_state["name"], "Test Project");
        assert_eq!(loaded_state["turn_count"], 5);
    }

    #[tokio::test]
    async fn test_snapshot_overwrite() {
        let pool = init_database(Path::new("")).await.expect("database");
        let agg_id = EntityId::new();

        save_snapshot(&pool, agg_id, &serde_json::json!({"v": 1}), 5)
            .await
            .expect("save v1");
        save_snapshot(&pool, agg_id, &serde_json::json!({"v": 2}), 15)
            .await
            .expect("save v2");

        let loaded = load_snapshot(&pool, agg_id)
            .await
            .expect("load");

        let (state, version) = loaded.unwrap();
        assert_eq!(version, 15);
        assert_eq!(state["v"], 2);
    }

    #[tokio::test]
    async fn test_snapshot_not_found() {
        let pool = init_database(Path::new("")).await.expect("database");
        let agg_id = EntityId::new();

        let loaded = load_snapshot(&pool, agg_id).await.expect("load");
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_delete_snapshot() {
        let pool = init_database(Path::new("")).await.expect("database");
        let agg_id = EntityId::new();

        save_snapshot(&pool, agg_id, &serde_json::json!({"x": 1}), 3)
            .await
            .expect("save");

        let deleted = delete_snapshot(&pool, agg_id).await.expect("delete");
        assert!(deleted);

        let loaded = load_snapshot(&pool, agg_id).await.expect("load");
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_load_all_snapshots() {
        // Three distinct aggregates snapshotted at different versions are all
        // returned by load_all_snapshots, with their state and version intact.
        let pool = init_database(Path::new("")).await.expect("database");
        let a = EntityId::new();
        let b = EntityId::new();
        let c = EntityId::new();

        save_snapshot(&pool, a, &serde_json::json!({"k": "a"}), 10)
            .await
            .expect("save a");
        save_snapshot(&pool, b, &serde_json::json!({"k": "b"}), 25)
            .await
            .expect("save b");
        save_snapshot(&pool, c, &serde_json::json!({"k": "c"}), 50)
            .await
            .expect("save c");

        let mut all = load_all_snapshots(&pool).await.expect("load all");
        all.sort_by_key(|(_, _, v)| *v);

        assert_eq!(all.len(), 3, "all three snapshots returned");
        let (id_a, state_a, ver_a) = &all[0];
        assert_eq!(*id_a, a);
        assert_eq!(*ver_a, 10);
        assert_eq!(state_a["k"], "a");
        let (id_b, _, ver_b) = &all[1];
        assert_eq!(*id_b, b);
        assert_eq!(*ver_b, 25);
        let (id_c, _, ver_c) = &all[2];
        assert_eq!(*id_c, c);
        assert_eq!(*ver_c, 50);
    }

    #[tokio::test]
    async fn test_load_all_snapshots_empty() {
        // A fresh database has no snapshots; load_all_snapshots returns [].
        let pool = init_database(Path::new("")).await.expect("database");
        let all = load_all_snapshots(&pool).await.expect("load all");
        assert!(all.is_empty(), "no snapshots in a fresh database");
    }
}

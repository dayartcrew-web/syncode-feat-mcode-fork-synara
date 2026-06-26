//! Event store — append-only, replay, snapshot
//!
//! Implements the core event store operations using SQLite.

use crate::SqlitePool;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur in the event store
#[derive(Debug, Error)]
pub enum EventStoreError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Concurrency conflict: expected sequence {expected}, got {actual}")]
    ConcurrencyConflict { expected: u64, actual: u64 },
    #[error("Event not found: aggregate {aggregate_id}")]
    NotFound { aggregate_id: String },
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// A persisted domain event record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedEvent {
    pub id: i64,
    pub aggregate_id: String,
    pub event_type: String,
    pub sequence: u64,
    pub data: String,
    pub timestamp: String,
    pub metadata: String,
    pub created_at: String,
}

/// Append events to the store atomically
pub async fn append_events(
    pool: &SqlitePool,
    aggregate_id: &str,
    events: &[EventToAppend],
    expected_version: u64,
) -> Result<Vec<PersistedEvent>, EventStoreError> {
    let mut tx = pool.begin().await?;

    // Verify expected version (optimistic concurrency)
    let current_seq: Option<(i64,)> =
        sqlx::query_as("SELECT COALESCE(MAX(sequence), 0) FROM domain_events WHERE aggregate_id = ?")
            .bind(aggregate_id)
            .fetch_optional(&mut *tx)
            .await?;

    let current_version = current_seq.map(|(s,)| s as u64).unwrap_or(0);
    if current_version != expected_version {
        return Err(EventStoreError::ConcurrencyConflict {
            expected: expected_version,
            actual: current_version,
        });
    }

    let mut persisted = Vec::with_capacity(events.len());
    let mut seq = expected_version;

    for event in events {
        seq += 1;

        let row: (i64, String, String, i64, String, String, String, String) = sqlx::query_as(
            r#"
            INSERT INTO domain_events (aggregate_id, event_type, sequence, data, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id, aggregate_id, event_type, sequence, data, timestamp, metadata, created_at
            "#,
        )
        .bind(aggregate_id)
        .bind(&event.event_type)
        .bind(seq as i64)
        .bind(&event.data)
        .bind(&event.timestamp)
        .bind(&event.metadata)
        .fetch_one(&mut *tx)
        .await?;

        persisted.push(PersistedEvent {
            id: row.0,
            aggregate_id: row.1,
            event_type: row.2,
            sequence: row.3 as u64,
            data: row.4,
            timestamp: row.5,
            metadata: row.6,
            created_at: row.7,
        });
    }

    tx.commit().await?;
    Ok(persisted)
}

/// An event to be appended to the store
#[derive(Debug, Clone)]
pub struct EventToAppend {
    pub event_type: String,
    pub data: String,
    pub timestamp: String,
    pub metadata: String,
}

impl EventToAppend {
    pub fn new(event_type: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            data: data.into(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            metadata: "{}".into(),
        }
    }
}

/// Replay all events for an aggregate
pub async fn replay_events(
    pool: &SqlitePool,
    aggregate_id: &str,
) -> Result<Vec<PersistedEvent>, EventStoreError> {
    let rows: Vec<(i64, String, String, i64, String, String, String, String)> = sqlx::query_as(
        r#"
        SELECT id, aggregate_id, event_type, sequence, data, timestamp, metadata, created_at
        FROM domain_events
        WHERE aggregate_id = ?
        ORDER BY sequence ASC
        "#,
    )
    .bind(aggregate_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| PersistedEvent {
            id: r.0,
            aggregate_id: r.1,
            event_type: r.2,
            sequence: r.3 as u64,
            data: r.4,
            timestamp: r.5,
            metadata: r.6,
            created_at: r.7,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_append_and_replay_events() {
        let pool = crate::init_database(std::path::Path::new(""))
            .await
            .expect("database");

        let agg_id = "test-aggregate-123";

        // Append events
        let events = vec![
            EventToAppend::new("ThreadCreated", r#"{"name":"test"}"#),
            EventToAppend::new("MessageAdded", r#"{"content":"hello"}"#),
        ];

        let persisted = append_events(&pool, agg_id, &events, 0)
            .await
            .expect("append");

        assert_eq!(persisted.len(), 2);
        assert_eq!(persisted[0].sequence, 1);
        assert_eq!(persisted[1].sequence, 2);

        // Replay
        let replayed = replay_events(&pool, agg_id)
            .await
            .expect("replay");

        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].event_type, "ThreadCreated");
        assert_eq!(replayed[1].event_type, "MessageAdded");
    }

    #[tokio::test]
    async fn test_concurrency_conflict() {
        let pool = crate::init_database(std::path::Path::new(""))
            .await
            .expect("database");

        let agg_id = "test-conflict-456";

        let events = vec![EventToAppend::new("Created", r#"{}"#)];
        append_events(&pool, agg_id, &events, 0).await.expect("first append");

        // Try to append with wrong expected version
        let result = append_events(&pool, agg_id, &events, 0).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            EventStoreError::ConcurrencyConflict {
                expected: 0,
                actual: 1,
            } => {}
            other => panic!("Expected ConcurrencyConflict, got: {:?}", other),
        }
    }
}

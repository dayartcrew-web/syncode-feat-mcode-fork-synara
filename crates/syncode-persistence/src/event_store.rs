//! Event store — append-only, replay, snapshot
//!
//! Implements the core event store operations using SQLite.

use crate::SqlitePool;
use serde::{Deserialize, Serialize};
use syncode_core::{DomainEvent, Envelope, EntityId, Timestamp, DomainEventTrait};
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

impl PersistedEvent {
    /// Deserialize the `data` field into a [`DomainEvent`].
    pub fn to_domain_event(&self) -> Result<DomainEvent, EventStoreError> {
        serde_json::from_str(&self.data).map_err(|e| EventStoreError::Serialization(e.to_string()))
    }

    /// Convert to a typed [`Envelope`] with stream metadata.
    pub fn to_envelope(&self) -> Result<Envelope, EventStoreError> {
        let event = self.to_domain_event()?;
        let timestamp = chrono::DateTime::parse_from_rfc3339(&self.timestamp)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        Ok(Envelope::with_timestamp(
            event,
            self.sequence,
            Timestamp::from_datetime(timestamp.with_timezone(&chrono::Utc)),
        ))
    }

    /// Create a `PersistedEvent` from a typed `Envelope`.
    pub fn from_envelope(envelope: &Envelope) -> Self {
        Self {
            id: 0, // will be assigned by DB
            aggregate_id: envelope.aggregate_id().to_string(),
            event_type: envelope.event_type().to_string(),
            sequence: envelope.sequence(),
            data: serde_json::to_string(&envelope.event).unwrap_or_default(),
            timestamp: envelope.timestamp().to_string(),
            metadata: "{}".to_string(),
            created_at: String::new(), // will be assigned by DB
        }
    }
}

/// Append domain events to the store atomically, returning typed `Envelope`s.
pub async fn append_domain_events(
    pool: &SqlitePool,
    aggregate_id: EntityId,
    domain_events: Vec<DomainEvent>,
    expected_version: u64,
) -> Result<Vec<Envelope>, EventStoreError> {
    let agg_id_str = aggregate_id.to_string();

    let mut tx = pool.begin().await?;

    // Verify expected version (optimistic concurrency)
    let current_seq: Option<(i64,)> =
        sqlx::query_as("SELECT COALESCE(MAX(sequence), 0) FROM domain_events WHERE aggregate_id = ?")
            .bind(&agg_id_str)
            .fetch_optional(&mut *tx)
            .await?;

    let current_version = current_seq.map(|(s,)| s as u64).unwrap_or(0);
    if current_version != expected_version {
        return Err(EventStoreError::ConcurrencyConflict {
            expected: expected_version,
            actual: current_version,
        });
    }

    let mut envelopes = Vec::with_capacity(domain_events.len());
    let mut seq = expected_version;

    for event in domain_events {
        seq += 1;
        let envelope = Envelope::new(event, seq);
        let data = serde_json::to_string(&envelope.event)
            .map_err(|e| EventStoreError::Serialization(e.to_string()))?;
        let timestamp = envelope.timestamp().to_string();

        sqlx::query(
            r#"
            INSERT INTO domain_events (aggregate_id, event_type, sequence, data, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?, '{}')
            "#,
        )
        .bind(&agg_id_str)
        .bind(envelope.event_type())
        .bind(seq as i64)
        .bind(&data)
        .bind(&timestamp)
        .execute(&mut *tx)
        .await?;

        envelopes.push(envelope);
    }

    tx.commit().await?;
    Ok(envelopes)
}

/// Replay all events for an aggregate as typed [`Envelope`]s.
pub async fn replay_envelopes(
    pool: &SqlitePool,
    aggregate_id: EntityId,
) -> Result<Vec<Envelope>, EventStoreError> {
    let persisted = replay_events(pool, &aggregate_id.to_string()).await?;
    persisted
        .into_iter()
        .map(|p| p.to_envelope())
        .collect()
}

/// Get the current version (event count) for an aggregate stream.
pub async fn current_version(
    pool: &SqlitePool,
    aggregate_id: &str,
) -> Result<u64, EventStoreError> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT COALESCE(MAX(sequence), 0) FROM domain_events WHERE aggregate_id = ?")
            .bind(aggregate_id)
            .fetch_optional(pool)
            .await?;

    Ok(row.map(|(s,)| s as u64).unwrap_or(0))
}

/// Append events to the store atomically (raw string-based API)
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

/// Replay all events across all aggregates (for global projections).
pub async fn replay_all_events(
    pool: &SqlitePool,
    since_sequence: Option<u64>,
    limit: u32,
) -> Result<Vec<PersistedEvent>, EventStoreError> {
    let rows: Vec<(i64, String, String, i64, String, String, String, String)> = if let Some(since) = since_sequence {
        sqlx::query_as(
            r#"
            SELECT id, aggregate_id, event_type, sequence, data, timestamp, metadata, created_at
            FROM domain_events
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            "#,
        )
        .bind(since as i64)
        .bind(limit as i64)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            r#"
            SELECT id, aggregate_id, event_type, sequence, data, timestamp, metadata, created_at
            FROM domain_events
            ORDER BY id ASC
            LIMIT ?
            "#,
        )
        .bind(limit as i64)
        .fetch_all(pool)
        .await?
    };

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

    #[tokio::test]
    async fn test_domain_event_roundtrip() {
        let pool = crate::init_database(std::path::Path::new(""))
            .await
            .expect("database");

        let agg_id = EntityId::new();
        let domain_events = vec![
            DomainEvent::ProjectCreated {
                id: agg_id,
                name: "Test Project".into(),
                root_path: "/tmp/test".into(),
                created_at: Timestamp::now(),
            },
            DomainEvent::ProjectUpdated {
                id: agg_id,
                provider_id: Some("anthropic".into()),
                default_model: Some("claude-3".into()),
                updated_at: Timestamp::now(),
            },
        ];

        let envelopes = append_domain_events(&pool, agg_id, domain_events, 0)
            .await
            .expect("append domain events");

        assert_eq!(envelopes.len(), 2);
        assert_eq!(envelopes[0].sequence(), 1);
        assert_eq!(envelopes[0].event_type(), "ProjectCreated");
        assert_eq!(envelopes[1].sequence(), 2);
        assert_eq!(envelopes[1].event_type(), "ProjectUpdated");

        // Replay as envelopes
        let replayed = replay_envelopes(&pool, agg_id)
            .await
            .expect("replay envelopes");

        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].aggregate_id(), agg_id);
        assert_eq!(replayed[1].event_type(), "ProjectUpdated");
    }

    #[tokio::test]
    async fn test_persisted_event_to_domain_event() {
        let persisted = PersistedEvent {
            id: 1,
            aggregate_id: "some-id".into(),
            event_type: "ThreadCreated".into(),
            sequence: 1,
            data: serde_json::to_string(&DomainEvent::ThreadCreated {
                id: EntityId::new(),
                project_id: EntityId::new(),
                provider_id: "anthropic".into(),
                model: "claude-3".into(),
                created_at: Timestamp::now(),
            }).unwrap(),
            timestamp: Timestamp::now().to_string(),
            metadata: "{}".into(),
            created_at: Timestamp::now().to_string(),
        };

        let domain_event = persisted.to_domain_event().expect("deserialize");
        assert_eq!(domain_event.event_type_name(), "ThreadCreated");
    }
}

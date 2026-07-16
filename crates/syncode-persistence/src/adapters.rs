//! Port trait adapters — concrete implementations of core ports
//!
//! These adapters bridge the syncode-persistence SQLite layer with the
//! syncode-core port traits. Currently only the live [`SqliteEventRepository`]
//! (used by the running server) is implemented here. The read-model projection
//! layer (`view_*` tables + `SqliteReadModelRepository`) was fully implemented
//! and tested but never wired into production — it has been removed to avoid
//! audit/maintenance confusion. The pipeline projects to the in-memory
//! `ReadModelStore` only; the event store is the durable surface.
//!
//! # Example
//!
//! ```ignore
//! use syncode_persistence::adapters::SqliteEventRepository;
//! let pool = syncode_persistence::init_database(std::path::Path::new("syncode.db")).await?;
//! let event_repo = SqliteEventRepository::new(pool);
//! ```

use crate::event_store::{
    EventStoreError, append_domain_events, current_version, replay_all_events as store_replay_all,
    replay_envelopes,
};
use sqlx::SqlitePool;
use syncode_core::{
    DomainEvent, EntityId, Envelope,
    ports::{EventRepository, PortError},
};

// ---------------------------------------------------------------------------
// SQLite EventRepository
// ---------------------------------------------------------------------------

/// SQLite-backed implementation of the `EventRepository` port.
///
/// Delegates to the existing `event_store` module functions. This is the LIVE
/// repository wired into the running server (`bin/server.rs`, Tauri ws_setup).
pub struct SqliteEventRepository {
    pool: SqlitePool,
}

impl SqliteEventRepository {
    /// Create a new SQLite event repository
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl EventRepository for SqliteEventRepository {
    async fn append_events(
        &self,
        aggregate_id: EntityId,
        events: Vec<DomainEvent>,
        expected_version: u64,
    ) -> Result<u64, PortError> {
        append_domain_events(&self.pool, aggregate_id, events, expected_version)
            .await
            .map(|envelopes| envelopes.last().map(|e| e.sequence).unwrap_or(0))
            .map_err(|e| match e {
                EventStoreError::ConcurrencyConflict { expected, actual } => {
                    PortError::ConcurrencyConflict { expected, actual }
                }
                _ => PortError::Internal(e.to_string()),
            })
    }

    async fn replay_events(&self, aggregate_id: EntityId) -> Result<Vec<Envelope>, PortError> {
        replay_envelopes(&self.pool, aggregate_id)
            .await
            .map_err(|e| PortError::Internal(e.to_string()))
    }

    async fn load_snapshot(
        &self,
        aggregate_id: EntityId,
    ) -> Result<Option<(serde_json::Value, u64)>, PortError> {
        crate::snapshot::load_snapshot(&self.pool, aggregate_id)
            .await
            .map_err(|e| PortError::Internal(e.to_string()))
    }

    async fn save_snapshot(
        &self,
        aggregate_id: EntityId,
        state: serde_json::Value,
        version: u64,
    ) -> Result<(), PortError> {
        crate::snapshot::save_snapshot(&self.pool, aggregate_id, &state, version)
            .await
            .map_err(|e| PortError::Internal(e.to_string()))
    }

    async fn load_all_snapshots(
        &self,
    ) -> Result<Vec<(EntityId, serde_json::Value, u64)>, PortError> {
        crate::snapshot::load_all_snapshots(&self.pool)
            .await
            .map_err(|e| PortError::Internal(e.to_string()))
    }

    async fn replay_all_events(
        &self,
        since_sequence: Option<u64>,
        limit: u32,
    ) -> Result<Vec<Envelope>, PortError> {
        let persisted = store_replay_all(&self.pool, since_sequence, limit)
            .await
            .map_err(|e| PortError::Internal(e.to_string()))?;
        // Convert PersistedEvent → Envelope
        Ok(persisted
            .into_iter()
            .filter_map(|pe| pe.to_envelope().ok())
            .collect())
    }

    async fn current_version(&self, aggregate_id: EntityId) -> Result<u64, PortError> {
        current_version(&self.pool, &aggregate_id.to_string())
            .await
            .map_err(|e| PortError::Internal(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> SqlitePool {
        crate::init_database(std::path::Path::new(""))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn sqlite_event_repo_append_and_replay() {
        let pool = setup().await;
        let repo = SqliteEventRepository::new(pool);

        let id = EntityId::new();
        let events = vec![DomainEvent::ProjectCreated {
            id,
            name: "Test".into(),
            root_path: "/test".into(),
            created_at: syncode_core::Timestamp::now(),
        }];

        // Append
        let version = repo.append_events(id, events.clone(), 0).await.unwrap();
        assert_eq!(version, 1);

        // Replay
        let envelopes = repo.replay_events(id).await.unwrap();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].event.event_type_name(), "ProjectCreated");

        // Version
        let v = repo.current_version(id).await.unwrap();
        assert_eq!(v, 1);
    }

    #[tokio::test]
    async fn sqlite_event_repo_concurrency_conflict() {
        let pool = setup().await;
        let repo = SqliteEventRepository::new(pool);

        let id = EntityId::new();
        let events = vec![DomainEvent::ActivityLogged {
            id,
            activity_type: "test".into(),
            description: "test".into(),
            thread_id: None,
            created_at: syncode_core::Timestamp::now(),
        }];

        repo.append_events(id, events.clone(), 0).await.unwrap();

        // Try to append with stale version
        let result = repo.append_events(id, events.clone(), 0).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PortError::ConcurrencyConflict { expected, actual } => {
                assert_eq!(expected, 0);
                assert_eq!(actual, 1);
            }
            _ => panic!("Expected ConcurrencyConflict"),
        }
    }

    #[tokio::test]
    async fn sqlite_event_repo_snapshot_roundtrip() {
        let pool = setup().await;
        let repo = SqliteEventRepository::new(pool);

        let id = EntityId::new();
        let state = serde_json::json!({"name": "test", "count": 42});

        // No snapshot initially
        let loaded = repo.load_snapshot(id).await.unwrap();
        assert!(loaded.is_none());

        // Save snapshot
        repo.save_snapshot(id, state.clone(), 5).await.unwrap();

        // Load snapshot
        let loaded = repo.load_snapshot(id).await.unwrap();
        assert!(loaded.is_some());
        let (loaded_state, loaded_version) = loaded.unwrap();
        assert_eq!(loaded_state["name"], "test");
        assert_eq!(loaded_version, 5);
    }
}

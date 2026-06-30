//! Port trait adapters — concrete implementations of core ports
//!
//! These adapters bridge the syncode-persistence SQLite layer with the
//! syncode-core port traits (`EventRepository`, `ReadModelRepository`).
//!
//! # Example
//!
//! ```ignore
//! use syncode_persistence::adapters::{SqliteEventRepository, SqliteReadModelRepository};
//! let pool = syncode_persistence::init_database(std::path::Path::new("syncode.db")).await?;
//! let event_repo = SqliteEventRepository::new(pool.clone());
//! let read_repo = SqliteReadModelRepository::new(pool);
//! ```

use sqlx::SqlitePool;
use syncode_core::{
    EntityId, Envelope, DomainEvent,
    ports::{EventRepository, ReadModelRepository, PortError},
};
use crate::event_store::{
    append_domain_events, replay_envelopes, current_version,
    replay_all_events as store_replay_all,
    EventStoreError,
};
use crate::projections::ProjectionManager;

// ---------------------------------------------------------------------------
// SQLite EventRepository
// ---------------------------------------------------------------------------

/// SQLite-backed implementation of the `EventRepository` port.
///
/// Delegates to the existing `event_store` module functions.
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

    async fn replay_events(
        &self,
        aggregate_id: EntityId,
    ) -> Result<Vec<Envelope>, PortError> {
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

    async fn replay_all_events(
        &self,
        since_sequence: Option<u64>,
        limit: u32,
    ) -> Result<Vec<Envelope>, PortError> {
        let persisted = store_replay_all(&self.pool, since_sequence, limit)
            .await
            .map_err(|e| PortError::Internal(e.to_string()))?;
        // Convert PersistedEvent → Envelope
        Ok(persisted.into_iter().filter_map(|pe| pe.to_envelope().ok()).collect())
    }

    async fn current_version(&self, aggregate_id: EntityId) -> Result<u64, PortError> {
        current_version(&self.pool, &aggregate_id.to_string())
            .await
            .map_err(|e| PortError::Internal(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// SQLite ReadModelRepository
// ---------------------------------------------------------------------------

/// SQLite-backed implementation of the `ReadModelRepository` port.
///
/// Uses the `ProjectionManager` for writes and queries the materialized
/// `view_*` tables for reads.
pub struct SqliteReadModelRepository {
    projection_manager: ProjectionManager,
}

impl SqliteReadModelRepository {
    /// Create a new SQLite read model repository (creates tables if needed).
    pub async fn new(pool: SqlitePool) -> Result<Self, PortError> {
        let pm = ProjectionManager::new(pool.clone())
            .await
            .map_err(|e| PortError::Internal(e.to_string()))?;
        Ok(Self { projection_manager: pm })
    }

    /// Create from an existing pool (skip table check — faster for testing).
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self {
            projection_manager: ProjectionManager::from_pool(pool),
        }
    }

    /// Get a reference to the underlying projection manager
    pub fn projection_manager(&self) -> &ProjectionManager {
        &self.projection_manager
    }
}

#[async_trait::async_trait]
impl ReadModelRepository for SqliteReadModelRepository {
    async fn refresh_projections(&self) -> Result<u32, PortError> {
        self.projection_manager
            .rebuild()
            .await
            .map_err(|e| PortError::Internal(e.to_string()))
    }

    // ─── Project queries ────────────────────────────────────────────

    async fn list_projects(&self, limit: u32, offset: u32) -> Result<Vec<serde_json::Value>, PortError> {
        let rows = sqlx::query_as::<_, (String, String, String, Option<String>, Option<String>, String, String, i64)>(
            "SELECT id, name, root_path, provider_id, default_model, created_at, updated_at, thread_count
             FROM view_projects ORDER BY created_at DESC LIMIT ? OFFSET ?"
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(self.projection_manager.pool())
        .await
        .map_err(|e| PortError::Internal(e.to_string()))?;

        Ok(rows.into_iter().map(|(id, name, root_path, provider_id, default_model, created_at, updated_at, thread_count)| {
            serde_json::json!({
                "id": id,
                "name": name,
                "rootPath": root_path,
                "providerId": provider_id,
                "defaultModel": default_model,
                "createdAt": created_at,
                "updatedAt": updated_at,
                "threadCount": thread_count,
            })
        }).collect())
    }

    async fn get_project(&self, id: EntityId) -> Result<Option<serde_json::Value>, PortError> {
        let row = sqlx::query_as::<_, (String, String, String, Option<String>, Option<String>, String, String, i64)>(
            "SELECT id, name, root_path, provider_id, default_model, created_at, updated_at, thread_count
             FROM view_projects WHERE id = ?"
        )
        .bind(id.to_string())
        .fetch_optional(self.projection_manager.pool())
        .await
        .map_err(|e| PortError::Internal(e.to_string()))?;

        Ok(row.map(|(id, name, root_path, provider_id, default_model, created_at, updated_at, thread_count)| {
            serde_json::json!({
                "id": id,
                "name": name,
                "rootPath": root_path,
                "providerId": provider_id,
                "defaultModel": default_model,
                "createdAt": created_at,
                "updatedAt": updated_at,
                "threadCount": thread_count,
            })
        }))
    }

    // ─── Thread queries ─────────────────────────────────────────────

    async fn list_threads(&self, project_id: EntityId, limit: u32, offset: u32) -> Result<Vec<serde_json::Value>, PortError> {
        let rows = sqlx::query_as::<_, (String, String, String, String, String, Option<String>, Option<String>, i64, String, String)>(
            "SELECT id, project_id, provider_id, model, status, title, git_checkpoint, turn_count, created_at, updated_at
             FROM view_threads WHERE project_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?"
        )
        .bind(project_id.to_string())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(self.projection_manager.pool())
        .await
        .map_err(|e| PortError::Internal(e.to_string()))?;

        Ok(rows.into_iter().map(|(id, project_id, provider_id, model, status, title, git_checkpoint, turn_count, created_at, updated_at)| {
            serde_json::json!({
                "id": id,
                "projectId": project_id,
                "providerId": provider_id,
                "model": model,
                "status": status,
                "title": title,
                "gitCheckpoint": git_checkpoint,
                "turnCount": turn_count,
                "createdAt": created_at,
                "updatedAt": updated_at,
            })
        }).collect())
    }

    async fn get_thread(&self, id: EntityId) -> Result<Option<serde_json::Value>, PortError> {
        let row = sqlx::query_as::<_, (String, String, String, String, String, Option<String>, Option<String>, i64, String, String)>(
            "SELECT id, project_id, provider_id, model, status, title, git_checkpoint, turn_count, created_at, updated_at
             FROM view_threads WHERE id = ?"
        )
        .bind(id.to_string())
        .fetch_optional(self.projection_manager.pool())
        .await
        .map_err(|e| PortError::Internal(e.to_string()))?;

        Ok(row.map(|(id, project_id, provider_id, model, status, title, git_checkpoint, turn_count, created_at, updated_at)| {
            serde_json::json!({
                "id": id,
                "projectId": project_id,
                "providerId": provider_id,
                "model": model,
                "status": status,
                "title": title,
                "gitCheckpoint": git_checkpoint,
                "turnCount": turn_count,
                "createdAt": created_at,
                "updatedAt": updated_at,
            })
        }))
    }

    // ─── Turn queries ──────────────────────────────────────────────

    async fn list_turns(&self, thread_id: EntityId, limit: u32, offset: u32) -> Result<Vec<serde_json::Value>, PortError> {
        let rows = sqlx::query_as::<_, (String, String, i64, String, Option<String>, String, Option<String>, Option<i64>, String, Option<String>)>(
            "SELECT id, thread_id, sequence, user_input, assistant_output, status, git_checkpoint, duration_ms, created_at, completed_at
             FROM view_turns WHERE thread_id = ? ORDER BY sequence ASC LIMIT ? OFFSET ?"
        )
        .bind(thread_id.to_string())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(self.projection_manager.pool())
        .await
        .map_err(|e| PortError::Internal(e.to_string()))?;

        Ok(rows.into_iter().map(|(id, thread_id, sequence, user_input, assistant_output, status, git_checkpoint, duration_ms, created_at, completed_at)| {
            serde_json::json!({
                "id": id,
                "threadId": thread_id,
                "sequence": sequence,
                "userInput": user_input,
                "assistantOutput": assistant_output,
                "status": status,
                "gitCheckpoint": git_checkpoint,
                "durationMs": duration_ms,
                "createdAt": created_at,
                "completedAt": completed_at,
            })
        }).collect())
    }

    async fn get_turn(&self, id: EntityId) -> Result<Option<serde_json::Value>, PortError> {
        let row = sqlx::query_as::<_, (String, String, i64, String, Option<String>, String, Option<String>, Option<i64>, String, Option<String>)>(
            "SELECT id, thread_id, sequence, user_input, assistant_output, status, git_checkpoint, duration_ms, created_at, completed_at
             FROM view_turns WHERE id = ?"
        )
        .bind(id.to_string())
        .fetch_optional(self.projection_manager.pool())
        .await
        .map_err(|e| PortError::Internal(e.to_string()))?;

        Ok(row.map(|(id, thread_id, sequence, user_input, assistant_output, status, git_checkpoint, duration_ms, created_at, completed_at)| {
            serde_json::json!({
                "id": id,
                "threadId": thread_id,
                "sequence": sequence,
                "userInput": user_input,
                "assistantOutput": assistant_output,
                "status": status,
                "gitCheckpoint": git_checkpoint,
                "durationMs": duration_ms,
                "createdAt": created_at,
                "completedAt": completed_at,
            })
        }))
    }

    // ─── Message queries ───────────────────────────────────────────

    async fn list_messages(&self, turn_id: EntityId, limit: u32, offset: u32) -> Result<Vec<serde_json::Value>, PortError> {
        let rows = sqlx::query_as::<_, (String, String, String, String, String, Option<i64>, Option<String>, Option<String>, String)>(
            "SELECT id, turn_id, role, content, content_type, token_count, tool_name, tool_call_id, created_at
             FROM view_messages WHERE turn_id = ? ORDER BY created_at ASC LIMIT ? OFFSET ?"
        )
        .bind(turn_id.to_string())
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(self.projection_manager.pool())
        .await
        .map_err(|e| PortError::Internal(e.to_string()))?;

        Ok(rows.into_iter().map(|(id, turn_id, role, content, content_type, token_count, tool_name, tool_call_id, created_at)| {
            serde_json::json!({
                "id": id,
                "turnId": turn_id,
                "role": role,
                "content": content,
                "contentType": content_type,
                "tokenCount": token_count,
                "toolName": tool_name,
                "toolCallId": tool_call_id,
                "createdAt": created_at,
            })
        }).collect())
    }

    // ─── Activity queries ─────────────────────────────────────────

    async fn list_activities(
        &self,
        project_id: Option<EntityId>,
        thread_id: Option<EntityId>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<serde_json::Value>, PortError> {
        // Build query dynamically based on filters
        let mut query = String::from(
            "SELECT id, activity_type, description, project_id, thread_id, metadata, created_at
             FROM view_activities WHERE 1=1"
        );

        if project_id.is_some() {
            query.push_str(" AND project_id = ?");
        }
        if thread_id.is_some() {
            query.push_str(" AND thread_id = ?");
        }

        query.push_str(&format!(" ORDER BY created_at DESC LIMIT {} OFFSET {}", limit, offset));

        let mut q = sqlx::query_as::<_, (String, String, String, Option<String>, Option<String>, String, String)>(&query);
        if let Some(pid) = project_id {
            q = q.bind(pid.to_string());
        }
        if let Some(tid) = thread_id {
            q = q.bind(tid.to_string());
        }

        let rows = q.fetch_all(self.projection_manager.pool())
            .await
            .map_err(|e| PortError::Internal(e.to_string()))?;

        Ok(rows.into_iter().map(|(id, activity_type, description, project_id, thread_id, metadata, created_at)| {
            serde_json::json!({
                "id": id,
                "activityType": activity_type,
                "description": description,
                "projectId": project_id,
                "threadId": thread_id,
                "metadata": serde_json::from_str::<serde_json::Value>(&metadata).unwrap_or(serde_json::Value::Object(serde_json::Map::new())),
                "createdAt": created_at,
            })
        }).collect())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> SqlitePool {
        crate::init_database(std::path::Path::new("")).await.unwrap()
    }

    #[tokio::test]
    async fn sqlite_event_repo_append_and_replay() {
        let pool = setup().await;
        let repo = SqliteEventRepository::new(pool);

        let id = EntityId::new();
        let events = vec![
            DomainEvent::ProjectCreated {
                id,
                name: "Test".into(),
                root_path: "/test".into(),
                created_at: syncode_core::Timestamp::now(),
            },
        ];

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

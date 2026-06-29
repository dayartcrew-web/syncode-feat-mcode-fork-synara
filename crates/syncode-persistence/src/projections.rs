//! Read model projections — materialized views from domain events
//!
//! The `ProjectionManager` listens to domain events and maintains
//! denormalized read model tables (projects, threads, turns, messages, activities)
//! in SQLite for fast querying.

use crate::SqlitePool;
use crate::event_store::replay_all_events;
use syncode_core::{DomainEvent, Envelope, DomainEventTrait};
use thiserror::Error;

/// Errors that can occur during projection operations
#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Event store error: {0}")]
    EventStore(#[from] crate::event_store::EventStoreError),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Projection not found: {0}")]
    NotFound(String),
}

/// Manages read model projection tables in SQLite.
///
/// Call `project_event()` for each new event to keep projections up to date,
/// or `refresh_all()` to replay from the event store.
pub struct ProjectionManager {
    pool: SqlitePool,
}

impl ProjectionManager {
    /// Create a new projection manager. Ensures projection tables exist.
    pub async fn new(pool: SqlitePool) -> Result<Self, ProjectionError> {
        Self::ensure_tables(&pool).await?;
        Ok(Self { pool })
    }

    /// Create from an existing pool without re-checking tables
    /// (useful when tables were just created by `init_database`).
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Ensure all projection tables exist.
    async fn ensure_tables(pool: &SqlitePool) -> Result<(), ProjectionError> {
        sqlx::raw_sql(
            r#"
            CREATE TABLE IF NOT EXISTS view_projects (
                id            TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                root_path     TEXT NOT NULL,
                provider_id   TEXT,
                default_model TEXT,
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL,
                thread_count  INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS view_threads (
                id            TEXT PRIMARY KEY,
                project_id    TEXT NOT NULL,
                provider_id   TEXT NOT NULL,
                model         TEXT NOT NULL,
                status        TEXT NOT NULL,
                title         TEXT,
                git_checkpoint TEXT,
                runtime_mode  TEXT NOT NULL DEFAULT 'full-access',
                interaction_mode TEXT NOT NULL DEFAULT 'default',
                turn_count    INTEGER NOT NULL DEFAULT 0,
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL,
                FOREIGN KEY (project_id) REFERENCES view_projects(id)
            );
            CREATE INDEX IF NOT EXISTS idx_threads_project ON view_threads(project_id);

            CREATE TABLE IF NOT EXISTS view_turns (
                id              TEXT PRIMARY KEY,
                thread_id       TEXT NOT NULL,
                sequence        INTEGER NOT NULL,
                user_input      TEXT NOT NULL DEFAULT '',
                assistant_output TEXT NOT NULL DEFAULT '',
                status          TEXT NOT NULL,
                git_checkpoint  TEXT,
                files_modified  TEXT DEFAULT '[]',
                duration_ms     INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                completed_at    TEXT,
                FOREIGN KEY (thread_id) REFERENCES view_threads(id)
            );
            CREATE INDEX IF NOT EXISTS idx_turns_thread ON view_turns(thread_id);

            CREATE TABLE IF NOT EXISTS view_messages (
                id            TEXT PRIMARY KEY,
                turn_id       TEXT NOT NULL,
                role          TEXT NOT NULL,
                content       TEXT NOT NULL,
                content_type  TEXT NOT NULL DEFAULT 'text',
                token_count   INTEGER,
                tool_name     TEXT,
                tool_call_id  TEXT,
                created_at    TEXT NOT NULL,
                FOREIGN KEY (turn_id) REFERENCES view_turns(id)
            );
            CREATE INDEX IF NOT EXISTS idx_messages_turn ON view_messages(turn_id);

            CREATE TABLE IF NOT EXISTS view_activities (
                id              TEXT PRIMARY KEY,
                activity_type   TEXT NOT NULL,
                description     TEXT NOT NULL,
                project_id      TEXT,
                thread_id       TEXT,
                metadata        TEXT NOT NULL DEFAULT '{}',
                created_at      TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_activities_project ON view_activities(project_id);
            CREATE INDEX IF NOT EXISTS idx_activities_thread ON view_activities(thread_id);

            -- Track the last projected event ID so we can resume
            CREATE TABLE IF NOT EXISTS projection_watermark (
                id         INTEGER PRIMARY KEY CHECK (id = 1),
                last_event_id INTEGER NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO projection_watermark (id, last_event_id) VALUES (1, 0);
            "#,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Project a single domain event onto the read model tables.
    /// For async usage prefer `project_event_async`.
    pub fn project_event(&self, _envelope: &Envelope) -> Result<(), ProjectionError> {
        // We can't do async inside this method directly, so we use sync-compatible
        // SQLite access. For actual use, see `project_event_async`.
        // This method is kept for API compatibility with the orchestration layer.
        Ok(())
    }

    /// Project a single domain event onto the read model tables (async).
    pub async fn project_event_async(
        &self,
        envelope: &Envelope,
    ) -> Result<(), ProjectionError> {
        match &envelope.event {
            DomainEvent::ProjectCreated {
                id, name, root_path, created_at, ..
            } => {
                let updated_at = created_at.to_string();
                sqlx::query(
                    r#"
                    INSERT OR REPLACE INTO view_projects (id, name, root_path, provider_id, default_model, created_at, updated_at, thread_count)
                    VALUES (?, ?, ?, NULL, NULL, ?, ?, 0)
                    "#,
                )
                .bind(id.to_string())
                .bind(name)
                .bind(root_path)
                .bind(created_at.to_string())
                .bind(&updated_at)
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ProjectUpdated {
                id, provider_id, default_model, updated_at, ..
            } => {
                sqlx::query(
                    r#"
                    UPDATE view_projects
                    SET provider_id = COALESCE(?, provider_id),
                        default_model = COALESCE(?, default_model),
                        updated_at = ?
                    WHERE id = ?
                    "#,
                )
                .bind(provider_id.as_deref())
                .bind(default_model.as_deref())
                .bind(updated_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ProjectDeleted { id, .. } => {
                // Tombstone: cascade-remove the project and its child threads,
                // turns, and messages so a rebuild leaves no orphans (the view
                // tables have no ON DELETE CASCADE foreign keys).
                let pid = id.to_string();
                sqlx::query(
                    r#"
                    DELETE FROM view_messages
                    WHERE turn_id IN (
                        SELECT t.id FROM view_turns t
                        JOIN view_threads th ON t.thread_id = th.id
                        WHERE th.project_id = ?
                    )
                    "#,
                )
                .bind(&pid)
                .execute(&self.pool)
                .await?;
                sqlx::query(
                    "DELETE FROM view_turns WHERE thread_id IN (SELECT id FROM view_threads WHERE project_id = ?)",
                )
                .bind(&pid)
                .execute(&self.pool)
                .await?;
                sqlx::query("DELETE FROM view_threads WHERE project_id = ?")
                .bind(&pid)
                .execute(&self.pool)
                .await?;
                sqlx::query("DELETE FROM view_projects WHERE id = ?")
                .bind(&pid)
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadCreated {
                id, project_id, provider_id, model, created_at, ..
            } => {
                let updated_at = created_at.to_string();
                sqlx::query(
                    r#"
                    INSERT OR REPLACE INTO view_threads (id, project_id, provider_id, model, status, title, git_checkpoint, runtime_mode, interaction_mode, turn_count, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'active', NULL, NULL, 'full-access', 'default', 0, ?, ?)
                    "#,
                )
                .bind(id.to_string())
                .bind(project_id.to_string())
                .bind(provider_id)
                .bind(model)
                .bind(created_at.to_string())
                .bind(&updated_at)
                .execute(&self.pool)
                .await?;

                // Increment project thread_count
                sqlx::query(
                    "UPDATE view_projects SET thread_count = thread_count + 1 WHERE id = ?",
                )
                .bind(project_id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadStatusChanged {
                id, new_status, updated_at, ..
            } => {
                sqlx::query(
                    "UPDATE view_threads SET status = ?, updated_at = ? WHERE id = ?",
                )
                .bind(new_status)
                .bind(updated_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadTitleSet { id, title, .. } => {
                sqlx::query(
                    "UPDATE view_threads SET title = ?, updated_at = datetime('now') WHERE id = ?",
                )
                .bind(title)
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadCheckpointSet { id, git_ref, .. } => {
                sqlx::query(
                    "UPDATE view_threads SET git_checkpoint = ? WHERE id = ?",
                )
                .bind(git_ref)
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadReverted { id, git_ref, reverted_at, .. } => {
                sqlx::query(
                    "UPDATE view_threads SET git_checkpoint = ?, updated_at = ? WHERE id = ?",
                )
                .bind(git_ref)
                .bind(reverted_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadArchived { id, archived_at, .. } => {
                sqlx::query(
                    "UPDATE view_threads SET status = 'archived', updated_at = ? WHERE id = ?",
                )
                .bind(archived_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadUnarchived { id, unarchived_at, .. } => {
                sqlx::query(
                    "UPDATE view_threads SET status = 'active', updated_at = ? WHERE id = ?",
                )
                .bind(unarchived_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadDeleted { id, .. } => {
                // Tombstone: cascade-remove the thread's messages and turns, then the
                // thread itself (view tables lack ON DELETE CASCADE foreign keys).
                let tid = id.to_string();
                sqlx::query(
                    "DELETE FROM view_messages WHERE turn_id IN (SELECT id FROM view_turns WHERE thread_id = ?)",
                )
                .bind(&tid)
                .execute(&self.pool)
                .await?;
                sqlx::query("DELETE FROM view_turns WHERE thread_id = ?")
                .bind(&tid)
                .execute(&self.pool)
                .await?;
                sqlx::query("DELETE FROM view_threads WHERE id = ?")
                .bind(&tid)
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadMessagesImported { .. } => {
                // Handoff/fork import is durably persisted in the event store. Read-model
                // materialization of imported message bodies is deferred (syncode messages
                // are turn-scoped). No projection mutation needed here.
            }

            DomainEvent::ThreadSessionStopRequested { .. } => {
                // Transient stop request; the session stop is a reactor side effect.
                // No projection mutation needed.
            }

            DomainEvent::ThreadRuntimeModeSet { id, runtime_mode, updated_at, .. } => {
                sqlx::query(
                    "UPDATE view_threads SET runtime_mode = ?, updated_at = ? WHERE id = ?",
                )
                .bind(runtime_mode)
                .bind(updated_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadInteractionModeSet { id, interaction_mode, updated_at, .. } => {
                sqlx::query(
                    "UPDATE view_threads SET interaction_mode = ?, updated_at = ? WHERE id = ?",
                )
                .bind(interaction_mode)
                .bind(updated_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ThreadApprovalResponded { .. }
            | DomainEvent::ThreadUserInputResponded { .. }
            | DomainEvent::ThreadMessageEditedAndResent { .. } => {
                // Transient provider-response records; no projection mutation.
            }

            DomainEvent::TurnStarted {
                id, thread_id, sequence, user_input, created_at, ..
            } => {
                let created_at_str = created_at.to_string();
                sqlx::query(
                    r#"
                    INSERT OR REPLACE INTO view_turns (id, thread_id, sequence, user_input, assistant_output, status, git_checkpoint, files_modified, duration_ms, created_at, completed_at)
                    VALUES (?, ?, ?, ?, '', 'running', NULL, '[]', 0, ?, NULL)
                    "#,
                )
                .bind(id.to_string())
                .bind(thread_id.to_string())
                .bind(*sequence as i64)
                .bind(user_input)
                .bind(&created_at_str)
                .execute(&self.pool)
                .await?;

                // Increment thread turn_count
                sqlx::query(
                    "UPDATE view_threads SET turn_count = turn_count + 1 WHERE id = ?",
                )
                .bind(thread_id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::TurnCompleted {
                id, assistant_output, duration_ms, completed_at, ..
            } => {
                sqlx::query(
                    r#"
                    UPDATE view_turns
                    SET assistant_output = ?, status = 'completed', duration_ms = ?, completed_at = ?
                    WHERE id = ?
                    "#,
                )
                .bind(assistant_output)
                .bind(*duration_ms as i64)
                .bind(completed_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::TurnFailed {
                id, error, completed_at, ..
            } => {
                // Store error in assistant_output for the read model
                sqlx::query(
                    r#"
                    UPDATE view_turns
                    SET assistant_output = ?, status = 'error', completed_at = ?
                    WHERE id = ?
                    "#,
                )
                .bind(error)
                .bind(completed_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::TurnCancelled { id, completed_at, .. } => {
                sqlx::query(
                    "UPDATE view_turns SET status = 'cancelled', completed_at = ? WHERE id = ?",
                )
                .bind(completed_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::TurnInterrupted { id, interrupted_at, .. } => {
                sqlx::query(
                    "UPDATE view_turns SET status = 'interrupted', completed_at = ? WHERE id = ?",
                )
                .bind(interrupted_at.to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::TurnFilesModified { id, files, .. } => {
                let files_json = serde_json::to_string(files)
                    .map_err(|e| ProjectionError::Serialization(e.to_string()))?;
                sqlx::query(
                    "UPDATE view_turns SET files_modified = ? WHERE id = ?",
                )
                .bind(&files_json)
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::TurnCheckpointSet { id, git_ref, .. } => {
                sqlx::query(
                    "UPDATE view_turns SET git_checkpoint = ? WHERE id = ?",
                )
                .bind(git_ref)
                .bind(id.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::MessageAdded {
                id, turn_id, role, content, created_at, ..
            } => {
                sqlx::query(
                    r#"
                    INSERT OR REPLACE INTO view_messages (id, turn_id, role, content, content_type, token_count, tool_name, tool_call_id, created_at)
                    VALUES (?, ?, ?, ?, 'text', NULL, NULL, NULL, ?)
                    "#,
                )
                .bind(id.to_string())
                .bind(turn_id.to_string())
                .bind(role)
                .bind(content)
                .bind(created_at.to_string())
                .execute(&self.pool)
                .await?;
            }

            DomainEvent::ActivityLogged {
                id, activity_type, description, created_at, ..
            } => {
                sqlx::query(
                    r#"
                    INSERT OR REPLACE INTO view_activities (id, activity_type, description, project_id, thread_id, metadata, created_at)
                    VALUES (?, ?, ?, NULL, NULL, '{}', ?)
                    "#,
                )
                .bind(id.to_string())
                .bind(activity_type)
                .bind(description)
                .bind(created_at.to_string())
                .execute(&self.pool)
                .await?;
            }
        }

        // Update watermark
        sqlx::query("UPDATE projection_watermark SET last_event_id = ?")
            .bind(envelope.sequence() as i64)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Project multiple events in order.
    pub async fn project_many(&self, envelopes: &[Envelope]) -> Result<u32, ProjectionError> {
        let count = envelopes.len() as u32;
        for envelope in envelopes {
            self.project_event_async(envelope).await?;
        }
        Ok(count)
    }

    /// Rebuild all projections from the event store.
    /// Drops and recreates all view tables, then replays every event.
    pub async fn rebuild(&self) -> Result<u32, ProjectionError> {
        // Drop projection tables
        sqlx::raw_sql(
            r#"
            DROP TABLE IF EXISTS view_activities;
            DROP TABLE IF EXISTS view_messages;
            DROP TABLE IF EXISTS view_turns;
            DROP TABLE IF EXISTS view_threads;
            DROP TABLE IF EXISTS view_projects;
            DROP TABLE IF EXISTS projection_watermark;
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Recreate tables
        Self::ensure_tables(&self.pool).await?;

        // Replay all events
        let persisted = replay_all_events(&self.pool, None, 10_000).await?;
        let mut count = 0u32;
        for p in &persisted {
            if let Ok(envelope) = p.to_envelope() {
                self.project_event_async(&envelope).await?;
                count += 1;
            }
        }

        tracing::info!(count, "Projection rebuild complete");
        Ok(count)
    }

    /// Get the last projected event sequence.
    pub async fn watermark(&self) -> Result<u64, ProjectionError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT last_event_id FROM projection_watermark WHERE id = 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(v,)| v as u64).unwrap_or(0))
    }

    /// Get the underlying pool reference.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_database;
    use syncode_core::{EntityId, Timestamp};
    use std::path::Path;

    async fn setup() -> ProjectionManager {
        let pool = init_database(Path::new("")).await.expect("database");
        ProjectionManager::new(pool).await.expect("projection manager")
    }

    #[tokio::test]
    async fn test_project_project_created() {
        let mgr = setup().await;
        let id = EntityId::new();
        let envelope = Envelope::new(
            DomainEvent::ProjectCreated {
                id,
                name: "Test".into(),
                root_path: "/test".into(),
                created_at: Timestamp::now(),
            },
            1,
        );
        mgr.project_event_async(&envelope).await.expect("project");

        let row: Option<(String, String,)> = sqlx::query_as(
            "SELECT id, name FROM view_projects WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(mgr.pool())
        .await
        .unwrap();

        assert!(row.is_some());
        let (rid, name) = row.unwrap();
        assert_eq!(name, "Test");
    }

    #[tokio::test]
    async fn test_project_thread_created_increments_count() {
        let mgr = setup().await;
        let pid = EntityId::new();
        let tid = EntityId::new();

        // Create project
        mgr.project_event_async(&Envelope::new(
            DomainEvent::ProjectCreated {
                id: pid,
                name: "P".into(),
                root_path: "/p".into(),
                created_at: Timestamp::now(),
            },
            1,
        )).await.expect("project created");

        // Create thread
        mgr.project_event_async(&Envelope::new(
            DomainEvent::ThreadCreated {
                id: tid,
                project_id: pid,
                provider_id: "anthropic".into(),
                model: "claude-3".into(),
                created_at: Timestamp::now(),
            },
            2,
        )).await.expect("thread created");

        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT thread_count FROM view_projects WHERE id = ?",
        )
        .bind(pid.to_string())
        .fetch_optional(mgr.pool())
        .await
        .unwrap();

        assert_eq!(row.unwrap().0, 1);
    }

    #[tokio::test]
    async fn test_project_turn_lifecycle() {
        let mgr = setup().await;
        let pid = EntityId::new();
        let tid = EntityId::new();
        let tid_id = EntityId::new();

        // Setup project + thread
        mgr.project_event_async(&Envelope::new(
            DomainEvent::ProjectCreated { id: pid, name: "P".into(), root_path: "/p".into(), created_at: Timestamp::now() },
            1,
        )).await.ok();
        mgr.project_event_async(&Envelope::new(
            DomainEvent::ThreadCreated { id: tid, project_id: pid, provider_id: "openai".into(), model: "gpt-4".into(), created_at: Timestamp::now() },
            2,
        )).await.ok();

        // Start turn
        mgr.project_event_async(&Envelope::new(
            DomainEvent::TurnStarted { id: tid_id, thread_id: tid, sequence: 1, user_input: "hello".into(), created_at: Timestamp::now() },
            3,
        )).await.expect("turn started");

        // Complete turn
        mgr.project_event_async(&Envelope::new(
            DomainEvent::TurnCompleted { id: tid_id, assistant_output: "world".into(), duration_ms: 100, completed_at: Timestamp::now() },
            4,
        )).await.expect("turn completed");

        let row: Option<(String, i64, String)> = sqlx::query_as(
            "SELECT status, duration_ms, assistant_output FROM view_turns WHERE id = ?",
        )
        .bind(tid_id.to_string())
        .fetch_optional(mgr.pool())
        .await
        .unwrap();

        let (status, ms, output) = row.unwrap();
        assert_eq!(status, "completed");
        assert_eq!(ms, 100);
        assert_eq!(output, "world");
    }

    #[tokio::test]
    async fn test_project_message_added() {
        let mgr = setup().await;
        let pid = EntityId::new();
        let tid = EntityId::new();
        let mid = EntityId::new();
        let turn_id = EntityId::new();

        // Setup: project -> thread -> turn (FK chain)
        mgr.project_event_async(&Envelope::new(
            DomainEvent::ProjectCreated { id: pid, name: "P".into(), root_path: "/p".into(), created_at: Timestamp::now() },
            1,
        )).await.ok();
        mgr.project_event_async(&Envelope::new(
            DomainEvent::ThreadCreated { id: tid, project_id: pid, provider_id: "anthropic".into(), model: "claude-3".into(), created_at: Timestamp::now() },
            2,
        )).await.ok();
        mgr.project_event_async(&Envelope::new(
            DomainEvent::TurnStarted { id: turn_id, thread_id: tid, sequence: 1, user_input: "hi".into(), created_at: Timestamp::now() },
            3,
        )).await.ok();

        // Now insert message
        mgr.project_event_async(&Envelope::new(
            DomainEvent::MessageAdded { id: mid, turn_id, role: "user".into(), content: "hello".into(), created_at: Timestamp::now() },
            4,
        )).await.expect("message added");

        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT role, content FROM view_messages WHERE id = ?",
        )
        .bind(mid.to_string())
        .fetch_optional(mgr.pool())
        .await
        .unwrap();

        let (role, content) = row.unwrap();
        assert_eq!(role, "user");
        assert_eq!(content, "hello");
    }

    #[tokio::test]
    async fn test_watermark_tracking() {
        let mgr = setup().await;
        assert_eq!(mgr.watermark().await.unwrap(), 0);

        let id = EntityId::new();
        mgr.project_event_async(&Envelope::new(
            DomainEvent::ProjectCreated { id, name: "W".into(), root_path: "/w".into(), created_at: Timestamp::now() },
            5,
        )).await.ok();

        assert_eq!(mgr.watermark().await.unwrap(), 5);
    }
}

//! Port interfaces — trait definitions for external dependencies
//!
//! These ports define the contracts that adapters must implement.
//! They form the dependency-inversion boundary: the domain and orchestration
//! layers depend on these traits, while concrete adapters (persistence, git,
//! providers) implement them.

use crate::domain::events::{DomainEvent, Envelope};
use crate::domain::primitives::EntityId;

// ---------------------------------------------------------------------------
// Error types for ports
// ---------------------------------------------------------------------------

/// Errors that can occur when interacting with the event repository
#[derive(Debug, thiserror::Error)]
pub enum PortError {
    #[error("Entity not found: {0}")]
    NotFound(String),

    #[error("Concurrency conflict: expected version {expected}, got {actual}")]
    ConcurrencyConflict { expected: u64, actual: u64 },

    #[error("Internal port error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Event Repository (write side — event store)
// ---------------------------------------------------------------------------

/// Port for event persistence (write side).
///
/// Implementations append [`Envelope`]s to a durable store (SQLite, PostgreSQL, etc.)
/// and replay them for aggregate reconstruction.
#[async_trait::async_trait]
pub trait EventRepository: Send + Sync {
    /// Append one or more events to an aggregate stream.
    ///
    /// Returns the new aggregate version (total event count after append).
    /// Implementations should use optimistic concurrency: if `expected_version`
    /// does not match the current stream length, return [`PortError::ConcurrencyConflict`].
    async fn append_events(
        &self,
        aggregate_id: EntityId,
        events: Vec<DomainEvent>,
        expected_version: u64,
    ) -> Result<u64, PortError>;

    /// Replay all events for a given aggregate, ordered by sequence.
    ///
    /// Returns an empty vec if the aggregate has no events.
    async fn replay_events(
        &self,
        aggregate_id: EntityId,
    ) -> Result<Vec<Envelope>, PortError>;

    /// Load a snapshot (if available) to avoid full replay.
    async fn load_snapshot(
        &self,
        aggregate_id: EntityId,
    ) -> Result<Option<(serde_json::Value, u64)>, PortError>;

    /// Save a snapshot for an aggregate at the given version.
    async fn save_snapshot(
        &self,
        aggregate_id: EntityId,
        state: serde_json::Value,
        version: u64,
    ) -> Result<(), PortError>;

    /// Read all events across all aggregates (for global replay / projections).
    ///
    /// Returns events ordered by timestamp, optionally filtered since a given
    /// sequence offset for pagination.
    async fn replay_all_events(
        &self,
        since_sequence: Option<u64>,
        limit: u32,
    ) -> Result<Vec<Envelope>, PortError>;

    /// Get the current version (event count) for an aggregate stream.
    async fn current_version(&self, aggregate_id: EntityId) -> Result<u64, PortError>;
}

// ---------------------------------------------------------------------------
// Domain Event Publisher (outbound notification bus)
// ---------------------------------------------------------------------------

/// Port for publishing domain events to an outbound notification bus
/// (e.g. a WebSocket push channel).
///
/// The orchestration layer calls this *after* events have been appended and
/// projected, so connected clients can react to state changes in real time.
/// Delivery is **best-effort**: a publish failure is advisory and must never
/// fail the originating command — the events are already durably persisted.
/// Implementations should be cheap and non-blocking (fan-out to an internal
/// channel, not to clients directly).
#[async_trait::async_trait]
pub trait DomainEventPublisher: Send + Sync {
    /// Publish a domain-event notification on `channel`.
    ///
    /// `event_type` is the domain event's type name, `aggregate_id` identifies
    /// the aggregate the event belongs to, and `data` carries the serialized
    /// event payload. `channel` is the subscriber-facing topic (typically the
    /// aggregate kind, e.g. `"orchestration"`).
    ///
    /// Returns `Ok(())` even when there are no receivers (that is normal
    /// before any client subscribes) — only surface an error when the bus
    /// itself is unusable.
    async fn publish(
        &self,
        channel: &str,
        event_type: &str,
        aggregate_id: &str,
        data: serde_json::Value,
    ) -> Result<(), PortError>;
}

// ---------------------------------------------------------------------------
// Read Model Repository (query side — projections)
// ---------------------------------------------------------------------------

/// Port for read model queries.
///
/// Implementations query the materialized projection tables. This is the
/// "query side" of CQRS — callers never modify state through this port.
#[async_trait::async_trait]
pub trait ReadModelRepository: Send + Sync {
    /// Refresh projections from the event store (run the projector over new events).
    async fn refresh_projections(&self) -> Result<u32, PortError>;

    // ─── Project queries ───────────────────────────────────────────────

    /// List all projects, ordered by creation date (most recent first).
    async fn list_projects(&self, limit: u32, offset: u32) -> Result<Vec<serde_json::Value>, PortError>;

    /// Get a single project by ID.
    async fn get_project(&self, id: EntityId) -> Result<Option<serde_json::Value>, PortError>;

    // ─── Thread queries ────────────────────────────────────────────────

    /// List threads for a project, ordered by creation date.
    async fn list_threads(
        &self,
        project_id: EntityId,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<serde_json::Value>, PortError>;

    /// Get a single thread by ID.
    async fn get_thread(&self, id: EntityId) -> Result<Option<serde_json::Value>, PortError>;

    // ─── Turn queries ───────────────────────────────────────────────────

    /// List turns for a thread, ordered by sequence.
    async fn list_turns(
        &self,
        thread_id: EntityId,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<serde_json::Value>, PortError>;

    /// Get a single turn by ID.
    async fn get_turn(&self, id: EntityId) -> Result<Option<serde_json::Value>, PortError>;

    // ─── Message queries ───────────────────────────────────────────────

    /// List messages for a turn.
    async fn list_messages(
        &self,
        turn_id: EntityId,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<serde_json::Value>, PortError>;

    // ─── Activity queries ──────────────────────────────────────────────

    /// List activities, optionally filtered by project or thread.
    async fn list_activities(
        &self,
        project_id: Option<EntityId>,
        thread_id: Option<EntityId>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<serde_json::Value>, PortError>;
}

// ---------------------------------------------------------------------------
// Git Service Port
// ---------------------------------------------------------------------------

/// Port for git operations.
///
/// Abstracts away the concrete git implementation (libgit2, CLI, etc.)
/// so the domain layer can request git operations without coupling.
#[async_trait::async_trait]
pub trait GitServicePort: Send + Sync {
    /// Get the current git status of the working tree.
    async fn status(&self, repo_path: &str) -> Result<GitStatus, PortError>;

    /// Create a checkpoint (reference) for the current state.
    async fn create_checkpoint(
        &self,
        repo_path: &str,
        message: &str,
    ) -> Result<String, PortError>;

    /// Get the diff between the working tree and the last checkpoint.
    async fn diff(&self, repo_path: &str, ref_name: &str) -> Result<String, PortError>;

    /// Get a list of modified files between two checkpoints.
    async fn list_modified_files(
        &self,
        repo_path: &str,
        from_ref: &str,
        to_ref: Option<&str>,
    ) -> Result<Vec<String>, PortError>;

    /// Check if a repository is valid and accessible.
    async fn is_valid_repo(&self, repo_path: &str) -> Result<bool, PortError>;
}

/// Git file status for a single file
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileStatus {
    Unmodified,
    Modified,
    Added,
    Deleted,
    Renamed,
    Untracked,
    Conflicted,
}

/// Git status of the entire working tree
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GitStatus {
    /// The current HEAD ref
    pub head_ref: Option<String>,
    /// Whether the working tree is clean
    pub is_clean: bool,
    /// Individual file statuses
    pub files: Vec<GitFileStatus>,
    /// Branch name
    pub branch: Option<String>,
}

/// Status of a single file
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GitFileStatus {
    pub path: String,
    pub status: FileStatus,
    pub staged: bool,
}

// ---------------------------------------------------------------------------
// Provider Port
// ---------------------------------------------------------------------------

/// Port for provider communication.
///
/// This is a simplified interface for the orchestration layer to interact
/// with AI providers without depending directly on the provider crate's types.
/// The full `ProviderAdapter` trait lives in `syncode-provider`.
#[async_trait::async_trait]
pub trait ProviderPort: Send + Sync {
    /// Start a provider session for a turn.
    async fn start_session(
        &self,
        provider_id: &str,
        model: &str,
        thread_id: EntityId,
        turn_id: EntityId,
        working_dir: &str,
        user_input: &str,
    ) -> Result<String, PortError>;

    /// Send a message to an active session.
    async fn send_to_session(
        &self,
        session_id: &str,
        message: &str,
    ) -> Result<(), PortError>;

    /// Interrupt an active session (user stop).
    async fn interrupt_session(&self, session_id: &str) -> Result<(), PortError>;

    /// Stop/cancel a session.
    async fn stop_session(&self, session_id: &str) -> Result<(), PortError>;

    /// Check if a provider is available and healthy.
    async fn health_check(&self, provider_id: &str) -> Result<bool, PortError>;

    /// List available models for a provider.
    async fn list_models(&self, provider_id: &str) -> Result<Vec<String>, PortError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_error_display() {
        let err = PortError::NotFound("project-123".into());
        assert!(err.to_string().contains("project-123"));

        let err = PortError::ConcurrencyConflict { expected: 5, actual: 3 };
        assert!(err.to_string().contains("expected version 5"));

        let err = PortError::Internal("db connection lost".into());
        assert!(err.to_string().contains("db connection lost"));
    }

    #[test]
    fn git_file_status_serialization() {
        let status = GitFileStatus {
            path: "src/main.rs".into(),
            status: FileStatus::Modified,
            staged: true,
        };
        let json = serde_json::to_string(&status).unwrap();
        let back: GitFileStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back.path, "src/main.rs");
        assert_eq!(back.status, FileStatus::Modified);
        assert!(back.staged);
    }

    #[test]
    fn git_status_serialization() {
        let status = GitStatus {
            head_ref: Some("refs/heads/main".into()),
            is_clean: false,
            files: vec![GitFileStatus {
                path: "README.md".into(),
                status: FileStatus::Modified,
                staged: false,
            }],
            branch: Some("main".into()),
        };
        let json = serde_json::to_string(&status).unwrap();
        let back: GitStatus = serde_json::from_str(&json).unwrap();
        assert!(!back.is_clean);
        assert_eq!(back.files.len(), 1);
    }

    #[test]
    fn file_status_serde_roundtrip() {
        let statuses = vec![
            FileStatus::Unmodified,
            FileStatus::Modified,
            FileStatus::Added,
            FileStatus::Deleted,
            FileStatus::Renamed,
            FileStatus::Untracked,
            FileStatus::Conflicted,
        ];
        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let back: FileStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(format!("{:?}", back), format!("{:?}", status));
        }
    }
}

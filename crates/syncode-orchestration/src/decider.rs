//! Command definitions and Decider — pure command→event logic
//!
//! The Decider pattern: given a command and current aggregate state,
//! produce zero or more domain events. This is the core business logic
//! of the CQRS/Event Sourcing architecture.

use syncode_core::{
    EntityId, Timestamp,
    domain::events::DomainEvent,
};
use thiserror::Error;

// ─── Imported Message (handoff/fork) ──────────────────────────────

/// A message carried over from a source thread during a handoff or fork.
///
/// Faithful to mcode's `ThreadHandoffImportedMessage` shape (messageId, role,
/// text, createdAt). We capture the essentials needed to record the import;
/// attachments are deferred.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportedMessage {
    pub source_message_id: EntityId,
    pub role: String,
    pub text: String,
}

// ─── Commands ─────────────────────────────────────────────────────

/// All commands in the orchestration bounded context.
/// Each command targets a specific aggregate.
#[derive(Debug, Clone)]
pub enum Command {
    // ─── Project Commands ────────────────────────────────────────
    CreateProject {
        name: String,
        root_path: String,
    },
    UpdateProjectConfig {
        id: EntityId,
        provider_id: Option<String>,
        default_model: Option<String>,
    },
    /// Delete a project (tombstone). Faithful to mcode `project.delete` { projectId }.
    DeleteProject {
        id: EntityId,
    },

    // ─── Thread Commands ──────────────────────────────────────────
    CreateThread {
        project_id: EntityId,
        provider_id: String,
        model: String,
    },
    PauseThread {
        id: EntityId,
    },
    ResumeThread {
        id: EntityId,
    },
    CompleteThread {
        id: EntityId,
    },
    CancelThread {
        id: EntityId,
    },
    SetThreadTitle {
        id: EntityId,
        title: String,
    },
    /// Roll a thread back to a previously-captured git checkpoint.
    RevertToCheckpoint {
        thread_id: EntityId,
        git_ref: String,
    },
    /// Archive a thread. Faithful to mcode `thread.archive`.
    ArchiveThread {
        id: EntityId,
    },
    /// Unarchive (restore) a thread. Faithful to mcode `thread.unarchive`.
    UnarchiveThread {
        id: EntityId,
    },
    /// Delete a thread (tombstone). Faithful to mcode `thread.delete`.
    DeleteThread {
        id: EntityId,
    },
    /// Create a thread by handoff from a source thread, importing its
    /// messages. Faithful to mcode `thread.handoff.create`.
    HandoffCreateThread {
        project_id: EntityId,
        provider_id: String,
        model: String,
        source_thread_id: EntityId,
        imported_messages: Vec<ImportedMessage>,
    },
    /// Create a thread by forking a source thread, importing its messages.
    /// Faithful to mcode `thread.fork.create`.
    ForkCreateThread {
        project_id: EntityId,
        provider_id: String,
        model: String,
        source_thread_id: EntityId,
        imported_messages: Vec<ImportedMessage>,
    },
    /// Stop the active provider session for a thread. Faithful to mcode
    /// `thread.session.stop`.
    StopThreadSession {
        id: EntityId,
    },

    // ─── Turn Commands ────────────────────────────────────────────
    StartTurn {
        thread_id: EntityId,
        sequence: u32,
        user_input: String,
    },
    CompleteTurn {
        id: EntityId,
        assistant_output: String,
        duration_ms: u64,
    },
    FailTurn {
        id: EntityId,
        error: String,
    },
    CancelTurn {
        id: EntityId,
    },
    /// Interrupt an in-progress (running) turn.
    InterruptTurn {
        id: EntityId,
    },
    RecordTurnFiles {
        id: EntityId,
        files: Vec<String>,
    },
    SetTurnCheckpoint {
        id: EntityId,
        git_ref: String,
    },

    // ─── Message Commands ────────────────────────────────────────
    AddMessage {
        turn_id: EntityId,
        role: String,
        content: String,
    },
}

// ─── Decider Errors ──────────────────────────────────────────────

/// Errors from command validation / business rule violations
#[derive(Debug, Error)]
pub enum DeciderError {
    #[error("Project not found: {0}")]
    ProjectNotFound(EntityId),

    #[error("Thread not found: {0}")]
    ThreadNotFound(EntityId),

    #[error("Turn not found: {0}")]
    TurnNotFound(EntityId),

    #[error("Invalid state transition: {current} → {target}")]
    InvalidStateTransition {
        aggregate: &'static str,
        current: String,
        target: String,
    },

    #[error("Project name cannot be empty")]
    EmptyProjectName,

    #[error("Project root path cannot be empty")]
    EmptyRootPath,

    #[error("Thread already completed")]
    ThreadAlreadyCompleted,

    #[error("Thread already cancelled")]
    ThreadAlreadyCancelled,

    #[error("Turn already completed")]
    TurnAlreadyCompleted,

    #[error("Turn already cancelled")]
    TurnAlreadyCancelled,

    #[error("Turn is not running; cannot interrupt (current status: {0})")]
    TurnNotRunning(String),

    #[error("Checkpoint git ref cannot be empty")]
    EmptyCheckpointRef,

    #[error("Invalid thread status for this operation: {0}")]
    InvalidThreadStatus(String),

    #[error("Invalid turn status for this operation: {0}")]
    InvalidTurnStatus(String),
}

// ─── Decider ─────────────────────────────────────────────────────

/// Pure function: (State, Command) → Result<Vec<DomainEvent>, DeciderError>
///
/// The decider contains all business rules for state transitions.
/// It is a pure function with no side effects.
pub struct Decider;

impl Decider {
    /// Decide which events to emit for a given command.
    ///
    /// `current_state` is a JSON snapshot of the aggregate (from event replay or snapshot).
    /// Returns zero or more domain events.
    pub fn decide(
        command: Command,
        current_state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        match command {
            Command::CreateProject { name, root_path } => {
                Self::decide_create_project(name, root_path)
            }
            Command::UpdateProjectConfig { id, provider_id, default_model } => {
                Self::decide_update_project(id, current_state, provider_id, default_model)
            }
            Command::DeleteProject { id } => {
                Self::decide_delete_project(id, current_state)
            }
            Command::CreateThread { project_id, provider_id, model } => {
                Self::decide_create_thread(project_id, provider_id, model)
            }
            Command::PauseThread { id } => {
                Self::decide_pause_thread(id, current_state)
            }
            Command::ResumeThread { id } => {
                Self::decide_resume_thread(id, current_state)
            }
            Command::CompleteThread { id } => {
                Self::decide_complete_thread(id, current_state)
            }
            Command::CancelThread { id } => {
                Self::decide_cancel_thread(id, current_state)
            }
            Command::SetThreadTitle { id, title } => {
                Self::decide_set_thread_title(id, current_state, title)
            }
            Command::RevertToCheckpoint { thread_id, git_ref } => {
                Self::decide_revert_to_checkpoint(thread_id, current_state, git_ref)
            }
            Command::ArchiveThread { id } => {
                Self::decide_archive_thread(id, current_state)
            }
            Command::UnarchiveThread { id } => {
                Self::decide_unarchive_thread(id, current_state)
            }
            Command::DeleteThread { id } => {
                Self::decide_delete_thread(id, current_state)
            }
            Command::HandoffCreateThread {
                project_id, provider_id, model, source_thread_id, imported_messages,
            } => {
                Self::decide_thread_with_import(
                    project_id, provider_id, model, source_thread_id, imported_messages,
                )
            }
            Command::ForkCreateThread {
                project_id, provider_id, model, source_thread_id, imported_messages,
            } => {
                Self::decide_thread_with_import(
                    project_id, provider_id, model, source_thread_id, imported_messages,
                )
            }
            Command::StopThreadSession { id } => {
                Self::decide_stop_thread_session(id, current_state)
            }
            Command::StartTurn { thread_id, sequence, user_input } => {
                Self::decide_start_turn(thread_id, sequence, user_input)
            }
            Command::CompleteTurn { id, assistant_output, duration_ms } => {
                Self::decide_complete_turn(id, current_state, assistant_output, duration_ms)
            }
            Command::FailTurn { id, error } => {
                Self::decide_fail_turn(id, current_state, error)
            }
            Command::CancelTurn { id } => {
                Self::decide_cancel_turn(id, current_state)
            }
            Command::InterruptTurn { id } => {
                Self::decide_interrupt_turn(id, current_state)
            }
            Command::RecordTurnFiles { id, files } => {
                Self::decide_record_turn_files(id, files)
            }
            Command::SetTurnCheckpoint { id, git_ref } => {
                Self::decide_set_turn_checkpoint(id, git_ref)
            }
            Command::AddMessage { turn_id, role, content } => {
                Self::decide_add_message(turn_id, role, content)
            }
        }
    }

    // ─── Project Decisions ────────────────────────────────────────

    fn decide_create_project(
        name: String,
        root_path: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let name_trimmed = name.trim().to_string();
        if name_trimmed.is_empty() {
            return Err(DeciderError::EmptyProjectName);
        }
        if root_path.trim().is_empty() {
            return Err(DeciderError::EmptyRootPath);
        }

        let id = EntityId::new();
        let now = Timestamp::now();
        Ok(vec![DomainEvent::ProjectCreated {
            id,
            name: name_trimmed,
            root_path,
            created_at: now,
        }])
    }

    fn decide_update_project(
        id: EntityId,
        state: Option<&serde_json::Value>,
        provider_id: Option<String>,
        default_model: Option<String>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        if state.is_none() {
            return Err(DeciderError::ProjectNotFound(id));
        }

        Ok(vec![DomainEvent::ProjectUpdated {
            id,
            provider_id,
            default_model,
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_delete_project(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: the project must exist. mcode rejects `project.delete` on an
        // unknown project; we surface the same invariant via ProjectNotFound.
        if state.is_none() {
            return Err(DeciderError::ProjectNotFound(id));
        }

        Ok(vec![DomainEvent::ProjectDeleted {
            id,
            deleted_at: Timestamp::now(),
        }])
    }

    // ─── Thread Decisions ─────────────────────────────────────────

    fn decide_create_thread(
        project_id: EntityId,
        provider_id: String,
        model: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let id = EntityId::new();
        let now = Timestamp::now();
        Ok(vec![DomainEvent::ThreadCreated {
            id,
            project_id,
            provider_id,
            model,
            created_at: now,
        }])
    }

    fn decide_pause_thread(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let status = Self::extract_thread_status(state, &id)?;
        Self::require_thread_active(&status)?;

        Ok(vec![DomainEvent::ThreadStatusChanged {
            id,
            old_status: status,
            new_status: "paused".to_string(),
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_resume_thread(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let status = Self::extract_thread_status(state, &id)?;
        if status != "paused" {
            return Err(DeciderError::InvalidStateTransition {
                aggregate: "Thread",
                current: status,
                target: "active".to_string(),
            });
        }

        Ok(vec![DomainEvent::ThreadStatusChanged {
            id,
            old_status: status,
            new_status: "active".to_string(),
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_complete_thread(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let status = Self::extract_thread_status(state, &id)?;
        Self::require_thread_active(&status)?;

        Ok(vec![DomainEvent::ThreadStatusChanged {
            id,
            old_status: status,
            new_status: "completed".to_string(),
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_cancel_thread(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let status = Self::extract_thread_status(state, &id)?;
        if status == "completed" {
            return Err(DeciderError::ThreadAlreadyCompleted);
        }
        if status == "cancelled" {
            return Err(DeciderError::ThreadAlreadyCancelled);
        }

        Ok(vec![DomainEvent::ThreadStatusChanged {
            id,
            old_status: status,
            new_status: "cancelled".to_string(),
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_set_thread_title(
        id: EntityId,
        state: Option<&serde_json::Value>,
        title: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let _ = Self::extract_thread_status(state, &id)?;

        Ok(vec![DomainEvent::ThreadTitleSet { id, title }])
    }

    fn decide_revert_to_checkpoint(
        thread_id: EntityId,
        state: Option<&serde_json::Value>,
        git_ref: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist (extract_thread_status errors if state is None).
        let _ = Self::extract_thread_status(state, &thread_id)?;

        // Guard: a revert target must be specified.
        let git_ref_trimmed = git_ref.trim().to_string();
        if git_ref_trimmed.is_empty() {
            return Err(DeciderError::EmptyCheckpointRef);
        }

        Ok(vec![DomainEvent::ThreadReverted {
            id: thread_id,
            git_ref: git_ref_trimmed,
            reverted_at: Timestamp::now(),
        }])
    }

    fn decide_archive_thread(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let status = Self::extract_thread_status(state, &id)?;
        if status == "archived" {
            return Err(DeciderError::InvalidStateTransition {
                aggregate: "Thread",
                current: status,
                target: "archived".to_string(),
            });
        }

        Ok(vec![DomainEvent::ThreadArchived {
            id,
            archived_at: Timestamp::now(),
        }])
    }

    fn decide_unarchive_thread(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let status = Self::extract_thread_status(state, &id)?;
        if status != "archived" {
            return Err(DeciderError::InvalidStateTransition {
                aggregate: "Thread",
                current: status,
                target: "active".to_string(),
            });
        }

        Ok(vec![DomainEvent::ThreadUnarchived {
            id,
            unarchived_at: Timestamp::now(),
        }])
    }

    fn decide_delete_thread(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. Any existing thread may be deleted.
        let _ = Self::extract_thread_status(state, &id)?;

        Ok(vec![DomainEvent::ThreadDeleted {
            id,
            deleted_at: Timestamp::now(),
        }])
    }

    /// Shared decision logic for handoff/fork thread creation.
    ///
    /// Emits a `ThreadCreated` for the new thread, then a `ThreadMessagesImported`
    /// recording the source thread and the number of imported messages — faithful
    /// to mcode's `thread.create` + internal `thread.messages.import` sequence.
    /// The decider is pure and trusts the command (project/source existence is
    /// enforced at the application layer, like `CreateThread`).
    fn decide_thread_with_import(
        project_id: EntityId,
        provider_id: String,
        model: String,
        source_thread_id: EntityId,
        imported_messages: Vec<ImportedMessage>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let id = EntityId::new();
        let now = Timestamp::now();
        let count = imported_messages.len() as u32;

        Ok(vec![
            DomainEvent::ThreadCreated {
                id,
                project_id,
                provider_id,
                model,
                created_at: now,
            },
            DomainEvent::ThreadMessagesImported {
                thread_id: id,
                source_thread_id,
                count,
                imported_at: now,
            },
        ])
    }

    fn decide_stop_thread_session(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. The actual provider-session stop is an async
        // side effect handled by the command reactor (SessionManager).
        let _ = Self::extract_thread_status(state, &id)?;

        Ok(vec![DomainEvent::ThreadSessionStopRequested {
            id,
            requested_at: Timestamp::now(),
        }])
    }

    // ─── Turn Decisions ──────────────────────────────────────────

    fn decide_start_turn(
        thread_id: EntityId,
        sequence: u32,
        user_input: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let id = EntityId::new();
        let now = Timestamp::now();
        Ok(vec![DomainEvent::TurnStarted {
            id,
            thread_id,
            sequence,
            user_input,
            created_at: now,
        }])
    }

    fn decide_complete_turn(
        id: EntityId,
        state: Option<&serde_json::Value>,
        assistant_output: String,
        duration_ms: u64,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let status = Self::extract_turn_status(state, &id)?;
        if status == "completed" {
            return Err(DeciderError::TurnAlreadyCompleted);
        }
        if status == "cancelled" {
            return Err(DeciderError::TurnAlreadyCancelled);
        }

        Ok(vec![DomainEvent::TurnCompleted {
            id,
            assistant_output,
            duration_ms,
            completed_at: Timestamp::now(),
        }])
    }

    fn decide_fail_turn(
        id: EntityId,
        state: Option<&serde_json::Value>,
        error: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let status = Self::extract_turn_status(state, &id)?;
        if status == "completed" {
            return Err(DeciderError::TurnAlreadyCompleted);
        }
        if status == "cancelled" {
            return Err(DeciderError::TurnAlreadyCancelled);
        }

        Ok(vec![DomainEvent::TurnFailed {
            id,
            error,
            completed_at: Timestamp::now(),
        }])
    }

    fn decide_cancel_turn(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let status = Self::extract_turn_status(state, &id)?;
        if status == "completed" {
            return Err(DeciderError::TurnAlreadyCompleted);
        }
        if status == "cancelled" {
            return Err(DeciderError::TurnAlreadyCancelled);
        }

        Ok(vec![DomainEvent::TurnCancelled {
            id,
            completed_at: Timestamp::now(),
        }])
    }

    fn decide_interrupt_turn(
        id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: only a running turn can be interrupted.
        let status = Self::extract_turn_status(state, &id)?;
        if status != "running" {
            return Err(DeciderError::TurnNotRunning(status));
        }

        Ok(vec![DomainEvent::TurnInterrupted {
            id,
            interrupted_at: Timestamp::now(),
        }])
    }

    fn decide_record_turn_files(
        id: EntityId,
        files: Vec<String>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        if files.is_empty() {
            return Ok(vec![]);
        }

        Ok(vec![DomainEvent::TurnFilesModified { id, files }])
    }

    fn decide_set_turn_checkpoint(
        id: EntityId,
        git_ref: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        Ok(vec![DomainEvent::TurnCheckpointSet { id, git_ref }])
    }

    fn decide_add_message(
        turn_id: EntityId,
        role: String,
        content: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let id = EntityId::new();
        let now = Timestamp::now();
        Ok(vec![DomainEvent::MessageAdded {
            id,
            turn_id,
            role,
            content,
            created_at: now,
        }])
    }

    // ─── Helpers ─────────────────────────────────────────────────

    fn extract_thread_status<'a>(
        state: Option<&'a serde_json::Value>,
        id: &EntityId,
    ) -> Result<String, DeciderError> {
        let state = state.ok_or(DeciderError::ThreadNotFound(*id))?;
        state
            .get("status")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| DeciderError::InvalidThreadStatus("unknown".to_string()))
    }

    fn extract_turn_status<'a>(
        state: Option<&'a serde_json::Value>,
        id: &EntityId,
    ) -> Result<String, DeciderError> {
        let state = state.ok_or(DeciderError::TurnNotFound(*id))?;
        state
            .get("status")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| DeciderError::InvalidTurnStatus("unknown".to_string()))
    }

    fn require_thread_active(status: &str) -> Result<(), DeciderError> {
        if status != "active" {
            return Err(DeciderError::InvalidStateTransition {
                aggregate: "Thread",
                current: status.to_string(),
                target: "target_state".to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a fake state JSON
    fn thread_state_active() -> serde_json::Value {
        serde_json::json!({ "status": "active" })
    }

    fn thread_state_paused() -> serde_json::Value {
        serde_json::json!({ "status": "paused" })
    }

    fn thread_state_completed() -> serde_json::Value {
        serde_json::json!({ "status": "completed" })
    }

    fn turn_state_pending() -> serde_json::Value {
        serde_json::json!({ "status": "pending" })
    }

    fn turn_state_running() -> serde_json::Value {
        serde_json::json!({ "status": "running" })
    }

    // ─── Project tests ───────────────────────────────────────────

    #[test]
    fn create_project_success() {
        let events = Decider::decide(
            Command::CreateProject {
                name: "my-project".to_string(),
                root_path: "/tmp/project".to_string(),
            },
            None,
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ProjectCreated { name, .. } => assert_eq!(name, "my-project"),
            _ => panic!("expected ProjectCreated"),
        }
    }

    #[test]
    fn create_project_empty_name_rejected() {
        let result = Decider::decide(
            Command::CreateProject {
                name: "  ".to_string(),
                root_path: "/tmp".to_string(),
            },
            None,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            DeciderError::EmptyProjectName => {}
            e => panic!("expected EmptyProjectName, got: {e}"),
        }
    }

    #[test]
    fn create_project_empty_path_rejected() {
        let result = Decider::decide(
            Command::CreateProject {
                name: "test".to_string(),
                root_path: "".to_string(),
            },
            None,
        );
        assert!(matches!(result.unwrap_err(), DeciderError::EmptyRootPath));
    }

    #[test]
    fn update_project_not_found() {
        let id = EntityId::new();
        let result = Decider::decide(
            Command::UpdateProjectConfig {
                id,
                provider_id: Some("anthropic".to_string()),
                default_model: None,
            },
            None,
        );
        assert!(matches!(result.unwrap_err(), DeciderError::ProjectNotFound(_)));
    }

    #[test]
    fn update_project_success() {
        let id = EntityId::new();
        let state = serde_json::json!({ "id": id.as_str() });
        let events = Decider::decide(
            Command::UpdateProjectConfig {
                id,
                provider_id: Some("anthropic".to_string()),
                default_model: Some("claude-3".to_string()),
            },
            Some(&state),
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ProjectUpdated { provider_id, default_model, .. } => {
                assert_eq!(provider_id.as_deref(), Some("anthropic"));
                assert_eq!(default_model.as_deref(), Some("claude-3"));
            }
            _ => panic!("expected ProjectUpdated"),
        }
    }

    #[test]
    fn delete_project_success() {
        let id = EntityId::new();
        let state = serde_json::json!({ "id": id.as_str() });
        let events = Decider::decide(
            Command::DeleteProject { id },
            Some(&state),
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ProjectDeleted { id: ev_id, .. } => assert_eq!(ev_id, &id),
            _ => panic!("expected ProjectDeleted"),
        }
    }

    #[test]
    fn delete_project_not_found() {
        let id = EntityId::new();
        let result = Decider::decide(Command::DeleteProject { id }, None);
        assert!(matches!(result.unwrap_err(), DeciderError::ProjectNotFound(_)));
    }

    // ─── Thread tests ────────────────────────────────────────────

    #[test]
    fn create_thread_success() {
        let pid = EntityId::new();
        let events = Decider::decide(
            Command::CreateThread {
                project_id: pid,
                provider_id: "openai".to_string(),
                model: "gpt-4".to_string(),
            },
            None,
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadCreated { project_id, provider_id, model, .. } => {
                assert_eq!(project_id, &pid);
                assert_eq!(provider_id, "openai");
                assert_eq!(model, "gpt-4");
            }
            _ => panic!("expected ThreadCreated"),
        }
    }

    #[test]
    fn pause_thread_active_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::PauseThread { id },
            Some(&thread_state_active()),
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadStatusChanged { old_status, new_status, .. } => {
                assert_eq!(old_status, "active");
                assert_eq!(new_status, "paused");
            }
            _ => panic!("expected ThreadStatusChanged"),
        }
    }

    #[test]
    fn pause_thread_not_active_fails() {
        let id = EntityId::new();
        let result = Decider::decide(
            Command::PauseThread { id },
            Some(&thread_state_paused()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn resume_thread_paused_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::ResumeThread { id },
            Some(&thread_state_paused()),
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadStatusChanged { new_status, .. } => {
                assert_eq!(new_status, "active");
            }
            _ => panic!("expected ThreadStatusChanged"),
        }
    }

    #[test]
    fn cancel_thread_completed_fails() {
        let id = EntityId::new();
        let result = Decider::decide(
            Command::CancelThread { id },
            Some(&thread_state_completed()),
        );
        assert!(matches!(result.unwrap_err(), DeciderError::ThreadAlreadyCompleted));
    }

    #[test]
    fn set_thread_title_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::SetThreadTitle {
                id,
                title: "My Thread".to_string(),
            },
            Some(&thread_state_active()),
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadTitleSet { title, .. } => assert_eq!(title, "My Thread"),
            _ => panic!("expected ThreadTitleSet"),
        }
    }

    // ─── Turn tests ──────────────────────────────────────────────

    #[test]
    fn start_turn_success() {
        let tid = EntityId::new();
        let events = Decider::decide(
            Command::StartTurn {
                thread_id: tid,
                sequence: 1,
                user_input: "Hello".to_string(),
            },
            None,
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::TurnStarted { thread_id, sequence, user_input, .. } => {
                assert_eq!(thread_id, &tid);
                assert_eq!(*sequence, 1);
                assert_eq!(user_input, "Hello");
            }
            _ => panic!("expected TurnStarted"),
        }
    }

    #[test]
    fn complete_turn_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::CompleteTurn {
                id,
                assistant_output: "Done!".to_string(),
                duration_ms: 1500,
            },
            Some(&turn_state_running()),
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::TurnCompleted { duration_ms, .. } => assert_eq!(*duration_ms, 1500),
            _ => panic!("expected TurnCompleted"),
        }
    }

    #[test]
    fn complete_turn_already_done_fails() {
        let id = EntityId::new();
        let _ = Decider::decide(
            Command::CompleteTurn {
                id,
                assistant_output: "x".to_string(),
                duration_ms: 100,
            },
            Some(&turn_state_pending()),
        ).unwrap();
        // pending → completed is allowed, so let's test against completed state
        let id2 = EntityId::new();
        let result2 = Decider::decide(
            Command::CompleteTurn {
                id: id2,
                assistant_output: "x".to_string(),
                duration_ms: 100,
            },
            // simulate completed turn
            Some(&serde_json::json!({"status": "completed"})),
        );
        assert!(matches!(result2.unwrap_err(), DeciderError::TurnAlreadyCompleted));
    }

    #[test]
    fn fail_turn_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::FailTurn {
                id,
                error: "API timeout".to_string(),
            },
            Some(&turn_state_running()),
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::TurnFailed { error, .. } => assert_eq!(error, "API timeout"),
            _ => panic!("expected TurnFailed"),
        }
    }

    #[test]
    fn record_turn_files_empty_returns_nothing() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::RecordTurnFiles { id, files: vec![] },
            None,
        ).unwrap();
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn add_message_success() {
        let tid = EntityId::new();
        let events = Decider::decide(
            Command::AddMessage {
                turn_id: tid,
                role: "user".to_string(),
                content: "Hello world".to_string(),
            },
            None,
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::MessageAdded { turn_id, role, content, .. } => {
                assert_eq!(turn_id, &tid);
                assert_eq!(role, "user");
                assert_eq!(content, "Hello world");
            }
            _ => panic!("expected MessageAdded"),
        }
    }

    // ─── InterruptTurn / RevertToCheckpoint tests ────────────────

    #[test]
    fn interrupt_turn_running_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::InterruptTurn { id },
            Some(&turn_state_running()),
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::TurnInterrupted { id: ev_id, .. } => assert_eq!(ev_id, &id),
            _ => panic!("expected TurnInterrupted"),
        }
    }

    #[test]
    fn interrupt_turn_non_running_rejected() {
        let id = EntityId::new();
        // pending turn cannot be interrupted
        let result = Decider::decide(
            Command::InterruptTurn { id },
            Some(&turn_state_pending()),
        );
        assert!(matches!(result.unwrap_err(), DeciderError::TurnNotRunning(_)));

        // completed turn cannot be interrupted
        let id2 = EntityId::new();
        let result2 = Decider::decide(
            Command::InterruptTurn { id: id2 },
            Some(&serde_json::json!({"status": "completed"})),
        );
        assert!(matches!(result2.unwrap_err(), DeciderError::TurnNotRunning(_)));
    }

    #[test]
    fn interrupt_turn_not_found() {
        let id = EntityId::new();
        let result = Decider::decide(Command::InterruptTurn { id }, None);
        assert!(matches!(result.unwrap_err(), DeciderError::TurnNotFound(_)));
    }

    #[test]
    fn revert_to_checkpoint_success() {
        let tid = EntityId::new();
        let events = Decider::decide(
            Command::RevertToCheckpoint {
                thread_id: tid,
                git_ref: "abc1234".to_string(),
            },
            Some(&thread_state_active()),
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadReverted { id, git_ref, .. } => {
                assert_eq!(id, &tid);
                assert_eq!(git_ref, "abc1234");
            }
            _ => panic!("expected ThreadReverted"),
        }
    }

    #[test]
    fn revert_to_checkpoint_unknown_thread_rejected() {
        let tid = EntityId::new();
        let result = Decider::decide(
            Command::RevertToCheckpoint {
                thread_id: tid,
                git_ref: "abc1234".to_string(),
            },
            None,
        );
        assert!(matches!(result.unwrap_err(), DeciderError::ThreadNotFound(_)));
    }

    #[test]
    fn revert_to_checkpoint_empty_ref_rejected() {
        let tid = EntityId::new();
        let result = Decider::decide(
            Command::RevertToCheckpoint {
                thread_id: tid,
                git_ref: "   ".to_string(),
            },
            Some(&thread_state_active()),
        );
        assert!(matches!(result.unwrap_err(), DeciderError::EmptyCheckpointRef));
    }

    // ─── Thread lifecycle: delete / archive / unarchive ───────────

    #[test]
    fn archive_thread_active_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::ArchiveThread { id },
            Some(&thread_state_active()),
        ).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], DomainEvent::ThreadArchived { .. }));
    }

    #[test]
    fn archive_thread_already_archived_rejected() {
        let id = EntityId::new();
        let result = Decider::decide(
            Command::ArchiveThread { id },
            Some(&serde_json::json!({"status": "archived"})),
        );
        assert!(matches!(result.unwrap_err(), DeciderError::InvalidStateTransition { .. }));
    }

    #[test]
    fn unarchive_thread_archived_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::UnarchiveThread { id },
            Some(&serde_json::json!({"status": "archived"})),
        ).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], DomainEvent::ThreadUnarchived { .. }));
    }

    #[test]
    fn unarchive_thread_non_archived_rejected() {
        let id = EntityId::new();
        let result = Decider::decide(
            Command::UnarchiveThread { id },
            Some(&thread_state_active()),
        );
        assert!(matches!(result.unwrap_err(), DeciderError::InvalidStateTransition { .. }));
    }

    #[test]
    fn delete_thread_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::DeleteThread { id },
            Some(&thread_state_active()),
        ).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], DomainEvent::ThreadDeleted { .. }));
    }

    #[test]
    fn delete_thread_not_found() {
        let id = EntityId::new();
        let result = Decider::decide(Command::DeleteThread { id }, None);
        assert!(matches!(result.unwrap_err(), DeciderError::ThreadNotFound(_)));
    }

    // ─── Stop thread session ──────────────────────────────────────

    #[test]
    fn stop_thread_session_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::StopThreadSession { id },
            Some(&thread_state_active()),
        ).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadSessionStopRequested { id: ev_id, .. } => assert_eq!(ev_id, &id),
            _ => panic!("expected ThreadSessionStopRequested"),
        }
    }

    #[test]
    fn stop_thread_session_unknown_thread_rejected() {
        let id = EntityId::new();
        let result = Decider::decide(Command::StopThreadSession { id }, None);
        assert!(matches!(result.unwrap_err(), DeciderError::ThreadNotFound(_)));
    }

    // ─── Handoff / fork thread creation ───────────────────────────

    fn imported(role: &str, text: &str) -> ImportedMessage {
        ImportedMessage {
            source_message_id: EntityId::new(),
            role: role.to_string(),
            text: text.to_string(),
        }
    }

    #[test]
    fn handoff_create_thread_emits_created_and_import() {
        let pid = EntityId::new();
        let src = EntityId::new();
        let events = Decider::decide(
            Command::HandoffCreateThread {
                project_id: pid,
                provider_id: "anthropic".into(),
                model: "claude-3".into(),
                source_thread_id: src,
                imported_messages: vec![imported("user", "hi"), imported("assistant", "hello")],
            },
            None,
        ).unwrap();

        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], DomainEvent::ThreadCreated { .. }));
        match &events[1] {
            DomainEvent::ThreadMessagesImported { source_thread_id, count, .. } => {
                assert_eq!(*source_thread_id, src);
                assert_eq!(*count, 2);
            }
            _ => panic!("expected ThreadMessagesImported"),
        }
    }

    #[test]
    fn fork_create_thread_emits_created_and_import() {
        let pid = EntityId::new();
        let src = EntityId::new();
        let events = Decider::decide(
            Command::ForkCreateThread {
                project_id: pid,
                provider_id: "openai".into(),
                model: "gpt-4".into(),
                source_thread_id: src,
                imported_messages: vec![imported("user", "q")],
            },
            None,
        ).unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], DomainEvent::ThreadCreated { .. }));
        assert!(matches!(events[1], DomainEvent::ThreadMessagesImported { count: 1, .. }));
    }

    #[test]
    fn handoff_create_thread_empty_messages_still_records_source() {
        let pid = EntityId::new();
        let src = EntityId::new();
        let events = Decider::decide(
            Command::HandoffCreateThread {
                project_id: pid,
                provider_id: "anthropic".into(),
                model: "claude-3".into(),
                source_thread_id: src,
                imported_messages: vec![],
            },
            None,
        ).unwrap();
        // Source linkage preserved even with zero imported messages.
        assert!(matches!(
            events[1],
            DomainEvent::ThreadMessagesImported { count: 0, source_thread_id, .. } if source_thread_id == src
        ));
    }
}

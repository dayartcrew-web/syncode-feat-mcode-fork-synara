//! Command definitions and Decider — pure command→event logic
//!
//! The Decider pattern: given a command and current aggregate state,
//! produce zero or more domain events. This is the core business logic
//! of the CQRS/Event Sourcing architecture.

use syncode_core::{CheckpointFile, EntityId, Timestamp, domain::events::DomainEvent};
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

/// A provider session's state, set on a thread. Faithful to mcode's
/// `OrchestrationSession` ({ threadId, status, providerName?, runtimeMode,
/// activeTurnId?, lastError?, updatedAt }). `status` is the mcode
/// `OrchestrationSessionStatus` ("idle"|"starting"|"running"|"ready"|
/// "interrupted"|"stopped"|"error"); like runtime/interaction modes, the Decider
/// trusts the supplied value rather than re-validating the enum.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadSession {
    pub status: String,
    pub provider_name: Option<String>,
    pub runtime_mode: String,
    pub active_turn_id: Option<EntityId>,
    pub last_error: Option<String>,
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
        /// Optional client-provided thread id (draft promotion / idempotency).
        /// When `Some`, the decider uses it instead of generating a new
        /// `EntityId`, so the `ThreadCreated` event and the subsequent turn
        /// reference the SAME id the frontend already holds (the frontend
        /// dispatches `thread.create` with its draft threadId and then reuses
        /// it for `thread.turn.start`). When `None`, a new id is generated.
        thread_id: Option<EntityId>,
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
    /// Update a thread's metadata (provider/model). Faithful to mcode
    /// `thread.meta.update` {threadId, modelSelection: {provider?, model?}}.
    /// Either field is `Option<String>`; `None` means "leave unchanged".
    UpdateThreadMeta {
        id: EntityId,
        provider_id: Option<String>,
        model: Option<String>,
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
    /// Set a thread's runtime mode. Faithful to mcode `thread.runtime-mode.set`
    /// {runtimeMode: "approval-required" | "full-access"}.
    SetThreadRuntimeMode {
        id: EntityId,
        runtime_mode: String,
    },
    /// Set a thread's provider interaction mode. Faithful to mcode
    /// `thread.interaction-mode.set` {interactionMode: "default" | "plan"}.
    SetThreadInteractionMode {
        id: EntityId,
        interaction_mode: String,
    },
    /// Respond to a pending provider approval request for a thread. Faithful to
    /// mcode `thread.approval.respond` {requestId, decision}.
    RespondThreadApproval {
        id: EntityId,
        request_id: String,
        decision: String,
    },
    /// Respond to a pending provider user-input request for a thread. Faithful
    /// to mcode `thread.user-input.respond` {requestId, answers}.
    RespondThreadUserInput {
        id: EntityId,
        request_id: String,
        answers: String,
    },
    /// Edit a thread message and trigger a new provider turn from it. Faithful
    /// to mcode `thread.message.edit-and-resend` {messageId, text, ...}.
    EditAndResendThreadMessage {
        id: EntityId,
        message_id: EntityId,
        text: String,
    },
    /// Set a thread's provider session state. Faithful to mcode
    /// `thread.session.set` {session: OrchestrationSession}.
    SetThreadSession {
        id: EntityId,
        session: ThreadSession,
    },
    /// Dispatch a queued turn to the provider for a thread. Faithful to mcode
    /// `thread.turn.dispatch-queued` {messageId, runtimeMode, interactionMode,
    /// dispatchMode}. The Decider records the request; the reactor dispatches it.
    DispatchQueuedTurn {
        id: EntityId,
        message_id: EntityId,
        runtime_mode: String,
        interaction_mode: String,
        dispatch_mode: String,
    },
    /// Append an activity entry to a thread. Faithful to mcode
    /// `thread.activity.append` {activity} → activity-appended payload.
    AppendThreadActivity {
        id: EntityId,
        activity_type: String,
        description: String,
    },
    /// Pin a message to a thread. Faithful to mcode `thread.pinned-message.add`.
    AddPinnedMessage {
        id: EntityId,
        message_id: EntityId,
    },
    /// Unpin a message from a thread. Faithful to mcode `thread.pinned-message.remove`.
    RemovePinnedMessage {
        id: EntityId,
        message_id: EntityId,
    },
    /// Set a pinned message's done flag. Faithful to mcode `thread.pinned-message.done.set`.
    SetPinnedMessageDone {
        id: EntityId,
        message_id: EntityId,
        done: bool,
    },
    /// Set a pinned message's label. Faithful to mcode `thread.pinned-message.label.set`.
    SetPinnedMessageLabel {
        id: EntityId,
        message_id: EntityId,
        label: Option<String>,
    },
    /// Add a marker to a thread message. Faithful to mcode `thread.marker.add`.
    AddMarker {
        id: EntityId,
        marker_id: EntityId,
        message_id: EntityId,
        start_offset: u64,
        end_offset: u64,
        selected_text: String,
        style: String,
        color: String,
    },
    /// Remove a marker from a thread. Faithful to mcode `thread.marker.remove`.
    RemoveMarker {
        id: EntityId,
        marker_id: EntityId,
    },
    /// Set a marker's done flag. Faithful to mcode `thread.marker.done.set`.
    SetMarkerDone {
        id: EntityId,
        marker_id: EntityId,
        done: bool,
    },
    /// Set a marker's label. Faithful to mcode `thread.marker.label.set`.
    SetMarkerLabel {
        id: EntityId,
        marker_id: EntityId,
        label: Option<String>,
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
    /// `thread.message.assistant.delta` — stream a chunk of an assistant message.
    /// Faithful to mcode (orchestration.ts:1257-1266): {threadId, messageId,
    /// delta, turnId}. The message id is caller-supplied (the active turn's
    /// assistant message); create-vs-append is decided by the projector.
    AppendAssistantDelta {
        thread_id: EntityId,
        message_id: EntityId,
        turn_id: EntityId,
        delta: String,
    },
    /// `thread.message.assistant.complete` — finalize a streamed assistant
    /// message. Faithful to mcode (orchestration.ts:1268-1272): {threadId,
    /// messageId, turnId}.
    FinalizeAssistantMessage {
        thread_id: EntityId,
        message_id: EntityId,
    },
    /// `thread.proposed-plan.upsert` — upsert a proposed plan on a thread.
    /// Faithful to mcode (orchestration.ts:1274-1280): {threadId, proposedPlan}.
    /// mcode enforces no count cap; the projector dedups by `thread:plan`.
    UpsertProposedPlan {
        thread_id: EntityId,
        plan_id: String,
        turn_id: Option<EntityId>,
        plan_markdown: String,
        implemented_at: Option<String>,
        implementation_thread_id: Option<EntityId>,
        created_at: Timestamp,
        updated_at: Timestamp,
    },
    /// `thread.turn.diff.complete` — record a turn's diff checkpoint summary.
    /// Faithful to mcode (orchestration.ts:1282-1294): {threadId, turnId,
    /// checkpointTurnCount, checkpointRef, status, files, assistantMessageId,
    /// completedAt}. The projector dedups by `thread:turn`.
    CompleteTurnDiff {
        thread_id: EntityId,
        turn_id: EntityId,
        checkpoint_turn_count: u32,
        checkpoint_ref: String,
        status: String,
        files: Vec<CheckpointFile>,
        assistant_message_id: Option<EntityId>,
        completed_at: Timestamp,
    },
    /// `thread.revert.complete` — truncate a thread to `turn_count` turns.
    /// Faithful to mcode (orchestration.ts:1308-1313, decider.ts:1569-1588):
    /// {threadId, turnCount}. Emits a turn-truncation event distinct from the
    /// git-checkpoint [`Command::RevertToCheckpoint`].
    CompleteRevert {
        thread_id: EntityId,
        turn_count: u32,
    },
    /// `thread.conversation.rollback` — request rolling a conversation back to
    /// a message. Faithful to mcode (orchestration.ts:1296-1306,
    /// decider.ts:1303-1340): {threadId, messageId, numTurns}. Requested-style
    /// async — syncode omits mcode's target-message invariant validation (the
    /// authoritative removed turns are carried by `rollback.complete`).
    ConversationRollback {
        thread_id: EntityId,
        message_id: EntityId,
        num_turns: u32,
    },
    /// `thread.conversation.rollback.complete` — a conversation rollback was
    /// applied. Faithful to mcode (orchestration.ts:1315-1327,
    /// decider.ts:1590-1615): {threadId, messageId, numTurns, removedTurnIds}.
    ConversationRollbackComplete {
        thread_id: EntityId,
        message_id: EntityId,
        num_turns: u32,
        removed_turn_ids: Vec<EntityId>,
    },
    /// `thread.messages.import` — import standalone messages into an existing
    /// thread. Faithful to mcode (orchestration.ts:1247-1253,
    /// decider.ts:1430-1457): {threadId, messages}. mcode emits one
    /// `thread.message-sent` per message with `turnId: null`; syncode instead
    /// records a single durable [`DomainEvent::ThreadMessagesImported`] summary
    /// (consistent with handoff/fork import — message-body materialization is
    /// deferred). The thread's own id is used as the recorded source to signal a
    /// native/standalone import with no external source thread.
    ImportMessages {
        thread_id: EntityId,
        imported_messages: Vec<ImportedMessage>,
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

    #[error("Invalid marker style {0:?}: expected \"highlight\" or \"underline\"")]
    InvalidMarkerStyle(String),

    #[error(
        "Invalid marker color {0:?}: expected one of \"yellow\", \"blue\", \"green\", \"pink\""
    )]
    InvalidMarkerColor(String),

    #[error(
        "Invalid marker offset range: start_offset ({start_offset}) must be strictly less than end_offset ({end_offset})"
    )]
    InvalidMarkerRange { start_offset: u64, end_offset: u64 },

    #[error("Pinned-message limit reached ({limit} per thread)")]
    PinnedMessageLimitReached { limit: usize },

    #[error("Marker limit reached ({limit} per thread)")]
    MarkerLimitReached { limit: usize },

    #[error("Pinned message not found in thread: {0}")]
    PinnedMessageNotFound(EntityId),

    #[error("Marker not found in thread: {0}")]
    MarkerNotFound(EntityId),
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
            Command::UpdateProjectConfig {
                id,
                provider_id,
                default_model,
            } => Self::decide_update_project(id, current_state, provider_id, default_model),
            Command::DeleteProject { id } => Self::decide_delete_project(id, current_state),
            Command::CreateThread {
                project_id,
                provider_id,
                model,
                thread_id,
            } => Self::decide_create_thread(project_id, provider_id, model, thread_id),
            Command::PauseThread { id } => Self::decide_pause_thread(id, current_state),
            Command::ResumeThread { id } => Self::decide_resume_thread(id, current_state),
            Command::CompleteThread { id } => Self::decide_complete_thread(id, current_state),
            Command::CancelThread { id } => Self::decide_cancel_thread(id, current_state),
            Command::SetThreadTitle { id, title } => {
                Self::decide_set_thread_title(id, current_state, title)
            }
            Command::UpdateThreadMeta {
                id,
                provider_id,
                model,
            } => Self::decide_update_thread_meta(id, current_state, provider_id, model),
            Command::RevertToCheckpoint { thread_id, git_ref } => {
                Self::decide_revert_to_checkpoint(thread_id, current_state, git_ref)
            }
            Command::ArchiveThread { id } => Self::decide_archive_thread(id, current_state),
            Command::UnarchiveThread { id } => Self::decide_unarchive_thread(id, current_state),
            Command::DeleteThread { id } => Self::decide_delete_thread(id, current_state),
            Command::HandoffCreateThread {
                project_id,
                provider_id,
                model,
                source_thread_id,
                imported_messages,
            } => Self::decide_thread_with_import(
                project_id,
                provider_id,
                model,
                source_thread_id,
                imported_messages,
            ),
            Command::ForkCreateThread {
                project_id,
                provider_id,
                model,
                source_thread_id,
                imported_messages,
            } => Self::decide_thread_with_import(
                project_id,
                provider_id,
                model,
                source_thread_id,
                imported_messages,
            ),
            Command::StopThreadSession { id } => {
                Self::decide_stop_thread_session(id, current_state)
            }
            Command::SetThreadRuntimeMode { id, runtime_mode } => {
                Self::decide_set_thread_runtime_mode(id, current_state, runtime_mode)
            }
            Command::SetThreadInteractionMode {
                id,
                interaction_mode,
            } => Self::decide_set_thread_interaction_mode(id, current_state, interaction_mode),
            Command::RespondThreadApproval {
                id,
                request_id,
                decision,
            } => Self::decide_respond_thread_approval(id, current_state, request_id, decision),
            Command::RespondThreadUserInput {
                id,
                request_id,
                answers,
            } => Self::decide_respond_thread_user_input(id, current_state, request_id, answers),
            Command::EditAndResendThreadMessage {
                id,
                message_id,
                text,
            } => Self::decide_edit_and_resend_thread_message(id, current_state, message_id, text),
            Command::SetThreadSession { id, session } => {
                Self::decide_set_thread_session(id, current_state, session)
            }
            Command::DispatchQueuedTurn {
                id,
                message_id,
                runtime_mode,
                interaction_mode,
                dispatch_mode,
            } => Self::decide_dispatch_queued_turn(
                id,
                current_state,
                message_id,
                runtime_mode,
                interaction_mode,
                dispatch_mode,
            ),
            Command::AppendThreadActivity {
                id,
                activity_type,
                description,
            } => Self::decide_append_thread_activity(id, current_state, activity_type, description),
            Command::AddPinnedMessage { id, message_id } => {
                Self::decide_add_pinned_message(id, current_state, message_id)
            }
            Command::RemovePinnedMessage { id, message_id } => {
                Self::decide_remove_pinned_message(id, current_state, message_id)
            }
            Command::SetPinnedMessageDone {
                id,
                message_id,
                done,
            } => Self::decide_set_pinned_message_done(id, current_state, message_id, done),
            Command::SetPinnedMessageLabel {
                id,
                message_id,
                label,
            } => Self::decide_set_pinned_message_label(id, current_state, message_id, label),
            Command::AddMarker {
                id,
                marker_id,
                message_id,
                start_offset,
                end_offset,
                selected_text,
                style,
                color,
            } => Self::decide_add_marker(
                id,
                current_state,
                marker_id,
                message_id,
                start_offset,
                end_offset,
                selected_text,
                style,
                color,
            ),
            Command::RemoveMarker { id, marker_id } => {
                Self::decide_remove_marker(id, current_state, marker_id)
            }
            Command::SetMarkerDone {
                id,
                marker_id,
                done,
            } => Self::decide_set_marker_done(id, current_state, marker_id, done),
            Command::SetMarkerLabel {
                id,
                marker_id,
                label,
            } => Self::decide_set_marker_label(id, current_state, marker_id, label),
            Command::StartTurn {
                thread_id,
                sequence,
                user_input,
            } => Self::decide_start_turn(thread_id, sequence, user_input),
            Command::CompleteTurn {
                id,
                assistant_output,
                duration_ms,
            } => Self::decide_complete_turn(id, current_state, assistant_output, duration_ms),
            Command::FailTurn { id, error } => Self::decide_fail_turn(id, current_state, error),
            Command::CancelTurn { id } => Self::decide_cancel_turn(id, current_state),
            Command::InterruptTurn { id } => Self::decide_interrupt_turn(id, current_state),
            Command::RecordTurnFiles { id, files } => Self::decide_record_turn_files(id, files),
            Command::SetTurnCheckpoint { id, git_ref } => {
                Self::decide_set_turn_checkpoint(id, git_ref)
            }
            Command::AddMessage {
                turn_id,
                role,
                content,
            } => Self::decide_add_message(turn_id, role, content),
            Command::AppendAssistantDelta {
                thread_id,
                message_id,
                turn_id,
                delta,
            } => Self::decide_append_assistant_delta(
                thread_id,
                message_id,
                turn_id,
                delta,
                current_state,
            ),
            Command::FinalizeAssistantMessage {
                thread_id,
                message_id,
            } => Self::decide_finalize_assistant_message(thread_id, message_id, current_state),
            Command::UpsertProposedPlan {
                thread_id,
                plan_id,
                turn_id,
                plan_markdown,
                implemented_at,
                implementation_thread_id,
                created_at,
                updated_at,
            } => Self::decide_upsert_proposed_plan(
                thread_id,
                plan_id,
                turn_id,
                plan_markdown,
                implemented_at,
                implementation_thread_id,
                created_at,
                updated_at,
                current_state,
            ),
            Command::CompleteTurnDiff {
                thread_id,
                turn_id,
                checkpoint_turn_count,
                checkpoint_ref,
                status,
                files,
                assistant_message_id,
                completed_at,
            } => Self::decide_complete_turn_diff(
                thread_id,
                turn_id,
                checkpoint_turn_count,
                checkpoint_ref,
                status,
                files,
                assistant_message_id,
                completed_at,
                current_state,
            ),
            Command::CompleteRevert {
                thread_id,
                turn_count,
            } => Self::decide_complete_revert(thread_id, turn_count, current_state),
            Command::ConversationRollback {
                thread_id,
                message_id,
                num_turns,
            } => {
                Self::decide_conversation_rollback(thread_id, message_id, num_turns, current_state)
            }
            Command::ConversationRollbackComplete {
                thread_id,
                message_id,
                num_turns,
                removed_turn_ids,
            } => Self::decide_conversation_rollback_complete(
                thread_id,
                message_id,
                num_turns,
                removed_turn_ids,
                current_state,
            ),
            Command::ImportMessages {
                thread_id,
                imported_messages,
            } => Self::decide_import_messages(thread_id, imported_messages, current_state),
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
        thread_id: Option<EntityId>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let id = thread_id.unwrap_or_default();
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

    fn decide_set_thread_runtime_mode(
        id: EntityId,
        state: Option<&serde_json::Value>,
        runtime_mode: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. The mode is a free-form config setting
        // (mcode RuntimeMode: "approval-required" | "full-access") — no status
        // transition constraint, so we only assert existence.
        let _ = Self::extract_thread_status(state, &id)?;

        Ok(vec![DomainEvent::ThreadRuntimeModeSet {
            id,
            runtime_mode,
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_update_thread_meta(
        id: EntityId,
        state: Option<&serde_json::Value>,
        provider_id: Option<String>,
        model: Option<String>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. Both fields are optional config values
        // (mcode modelSelection: {provider?, model?}); `None` means "unchanged".
        // No status transition constraint, so we only assert existence.
        let _ = Self::extract_thread_status(state, &id)?;

        Ok(vec![DomainEvent::ThreadMetaUpdated {
            thread_id: id,
            provider_id,
            model,
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_set_thread_interaction_mode(
        id: EntityId,
        state: Option<&serde_json::Value>,
        interaction_mode: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. The mode is a free-form config setting
        // (mcode ProviderInteractionMode: "default" | "plan") — no status
        // transition constraint.
        let _ = Self::extract_thread_status(state, &id)?;

        Ok(vec![DomainEvent::ThreadInteractionModeSet {
            id,
            interaction_mode,
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_respond_thread_approval(
        id: EntityId,
        state: Option<&serde_json::Value>,
        request_id: String,
        decision: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. The response is dispatched to the provider by
        // the command reactor (the provider approval queue is not yet modeled — gap).
        let _ = Self::extract_thread_status(state, &id)?;
        Ok(vec![DomainEvent::ThreadApprovalResponded {
            id,
            request_id,
            decision,
            responded_at: Timestamp::now(),
        }])
    }

    fn decide_respond_thread_user_input(
        id: EntityId,
        state: Option<&serde_json::Value>,
        request_id: String,
        answers: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. The response is dispatched to the provider by
        // the command reactor (the provider user-input queue is not yet modeled — gap).
        let _ = Self::extract_thread_status(state, &id)?;
        Ok(vec![DomainEvent::ThreadUserInputResponded {
            id,
            request_id,
            answers,
            responded_at: Timestamp::now(),
        }])
    }

    fn decide_edit_and_resend_thread_message(
        id: EntityId,
        state: Option<&serde_json::Value>,
        message_id: EntityId,
        text: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. The resend (a new provider turn) is triggered by
        // the command reactor (provider edit-resend is not yet modeled — gap).
        let _ = Self::extract_thread_status(state, &id)?;
        Ok(vec![DomainEvent::ThreadMessageEditedAndResent {
            id,
            message_id,
            text,
            edited_at: Timestamp::now(),
        }])
    }

    fn decide_set_thread_session(
        id: EntityId,
        state: Option<&serde_json::Value>,
        session: ThreadSession,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. The session is a free-form lifecycle/config
        // record (mcode OrchestrationSession: status, provider, active turn, last
        // error) — like runtime/interaction modes, the Decider trusts the supplied
        // values rather than re-validating the status enum.
        let _ = Self::extract_thread_status(state, &id)?;
        Ok(vec![DomainEvent::ThreadSessionSet {
            id,
            status: session.status,
            provider_name: session.provider_name,
            runtime_mode: session.runtime_mode,
            active_turn_id: session.active_turn_id,
            last_error: session.last_error,
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_dispatch_queued_turn(
        id: EntityId,
        state: Option<&serde_json::Value>,
        message_id: EntityId,
        runtime_mode: String,
        interaction_mode: String,
        dispatch_mode: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. The queued-turn dispatch is an async provider
        // side effect handled by the command reactor; this records the request
        // (Requested-style event, consistent with ThreadSessionStopRequested).
        let _ = Self::extract_thread_status(state, &id)?;
        Ok(vec![DomainEvent::TurnDispatchRequested {
            id,
            message_id,
            runtime_mode,
            interaction_mode,
            dispatch_mode,
            requested_at: Timestamp::now(),
        }])
    }

    fn decide_append_thread_activity(
        id: EntityId,
        state: Option<&serde_json::Value>,
        activity_type: String,
        description: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist. Reuses the existing ActivityLogged event, now
        // scoped to this thread (faithful to mcode `thread.activity-appended` payload
        // {threadId, activity}). thread_id lets the activity read-model filter by thread.
        let _ = Self::extract_thread_status(state, &id)?;
        Ok(vec![DomainEvent::ActivityLogged {
            id: EntityId::new(),
            activity_type,
            description,
            thread_id: Some(id),
            created_at: Timestamp::now(),
        }])
    }

    fn decide_add_pinned_message(
        id: EntityId,
        state: Option<&serde_json::Value>,
        message_id: EntityId,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Guard: thread must exist, then enforce the mcode PINNED_MESSAGES_MAX_COUNT cap.
        let _ = Self::extract_thread_status(state, &id)?;
        if Self::extract_pinned_message_ids(state).len() >= Self::MAX_PINNED_MESSAGES {
            return Err(DeciderError::PinnedMessageLimitReached {
                limit: Self::MAX_PINNED_MESSAGES,
            });
        }
        let now = Timestamp::now();
        Ok(vec![DomainEvent::PinnedMessageAdded {
            thread_id: id,
            message_id,
            label: None,
            done: false,
            pinned_at: now,
            updated_at: now,
        }])
    }

    fn decide_remove_pinned_message(
        id: EntityId,
        state: Option<&serde_json::Value>,
        message_id: EntityId,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let _ = Self::extract_thread_status(state, &id)?;
        if !Self::extract_pinned_message_ids(state).contains(&message_id.as_str()) {
            return Err(DeciderError::PinnedMessageNotFound(message_id));
        }
        Ok(vec![DomainEvent::PinnedMessageRemoved {
            thread_id: id,
            message_id,
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_set_pinned_message_done(
        id: EntityId,
        state: Option<&serde_json::Value>,
        message_id: EntityId,
        done: bool,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let _ = Self::extract_thread_status(state, &id)?;
        if !Self::extract_pinned_message_ids(state).contains(&message_id.as_str()) {
            return Err(DeciderError::PinnedMessageNotFound(message_id));
        }
        Ok(vec![DomainEvent::PinnedMessageDoneSet {
            thread_id: id,
            message_id,
            done,
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_set_pinned_message_label(
        id: EntityId,
        state: Option<&serde_json::Value>,
        message_id: EntityId,
        label: Option<String>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let _ = Self::extract_thread_status(state, &id)?;
        if !Self::extract_pinned_message_ids(state).contains(&message_id.as_str()) {
            return Err(DeciderError::PinnedMessageNotFound(message_id));
        }
        Ok(vec![DomainEvent::PinnedMessageLabelSet {
            thread_id: id,
            message_id,
            label,
            updated_at: Timestamp::now(),
        }])
    }

    // ─── Marker Decisions ──────────────────────────────────────────

    #[allow(clippy::too_many_arguments)] // domain decider: a marker carries many fields
    fn decide_add_marker(
        id: EntityId,
        state: Option<&serde_json::Value>,
        marker_id: EntityId,
        message_id: EntityId,
        start_offset: u64,
        end_offset: u64,
        selected_text: String,
        style: String,
        color: String,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        // Field validation (mcode-faithful enums + offset range). The marker-count
        // cap (THREAD_MARKERS_MAX_COUNT) needs the current marker set and is enforced
        // separately; style/color/range are pure input checks done here first.
        match style.as_str() {
            "highlight" | "underline" => {}
            other => return Err(DeciderError::InvalidMarkerStyle(other.to_string())),
        }
        match color.as_str() {
            "yellow" | "blue" | "green" | "pink" => {}
            other => return Err(DeciderError::InvalidMarkerColor(other.to_string())),
        }
        if end_offset <= start_offset {
            return Err(DeciderError::InvalidMarkerRange {
                start_offset,
                end_offset,
            });
        }

        // Guard: thread must exist, then enforce the mcode THREAD_MARKERS_MAX_COUNT cap.
        let _ = Self::extract_thread_status(state, &id)?;
        if Self::extract_marker_ids(state).len() >= Self::MAX_MARKERS {
            return Err(DeciderError::MarkerLimitReached {
                limit: Self::MAX_MARKERS,
            });
        }
        let now = Timestamp::now();
        Ok(vec![DomainEvent::MarkerAdded {
            thread_id: id,
            marker_id,
            message_id,
            start_offset,
            end_offset,
            selected_text,
            style,
            color,
            label: None,
            done: false,
            created_at: now,
            updated_at: now,
        }])
    }

    fn decide_remove_marker(
        id: EntityId,
        state: Option<&serde_json::Value>,
        marker_id: EntityId,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let _ = Self::extract_thread_status(state, &id)?;
        if !Self::extract_marker_ids(state).contains(&marker_id.as_str()) {
            return Err(DeciderError::MarkerNotFound(marker_id));
        }
        Ok(vec![DomainEvent::MarkerRemoved {
            thread_id: id,
            marker_id,
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_set_marker_done(
        id: EntityId,
        state: Option<&serde_json::Value>,
        marker_id: EntityId,
        done: bool,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let _ = Self::extract_thread_status(state, &id)?;
        if !Self::extract_marker_ids(state).contains(&marker_id.as_str()) {
            return Err(DeciderError::MarkerNotFound(marker_id));
        }
        Ok(vec![DomainEvent::MarkerDoneSet {
            thread_id: id,
            marker_id,
            done,
            updated_at: Timestamp::now(),
        }])
    }

    fn decide_set_marker_label(
        id: EntityId,
        state: Option<&serde_json::Value>,
        marker_id: EntityId,
        label: Option<String>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        let _ = Self::extract_thread_status(state, &id)?;
        if !Self::extract_marker_ids(state).contains(&marker_id.as_str()) {
            return Err(DeciderError::MarkerNotFound(marker_id));
        }
        Ok(vec![DomainEvent::MarkerLabelSet {
            thread_id: id,
            marker_id,
            label,
            updated_at: Timestamp::now(),
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
            usage: None,
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

    /// Append a streamed assistant-message chunk (mcode
    /// `thread.message.assistant.delta`). The message id is caller-supplied;
    /// the create-vs-append materialization is the projector's concern, so the
    /// decider only enforces thread existence and emits the append event.
    fn decide_append_assistant_delta(
        thread_id: EntityId,
        message_id: EntityId,
        turn_id: EntityId,
        delta: String,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        Self::extract_thread_status(state, &thread_id)?;
        let now = Timestamp::now();
        Ok(vec![DomainEvent::MessageDeltaAppended {
            id: message_id,
            turn_id,
            delta,
            created_at: now,
        }])
    }

    /// Finalize a streamed assistant message (mcode
    /// `thread.message.assistant.complete`). Only enforces thread existence.
    fn decide_finalize_assistant_message(
        thread_id: EntityId,
        message_id: EntityId,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        Self::extract_thread_status(state, &thread_id)?;
        Ok(vec![DomainEvent::MessageStreamingFinalized {
            id: message_id,
            finalized_at: Timestamp::now(),
        }])
    }

    /// Upsert a proposed plan (mcode `thread.proposed-plan.upsert`). Guards
    /// thread existence and echoes the plan; the projector dedups by id.
    #[allow(clippy::too_many_arguments)]
    fn decide_upsert_proposed_plan(
        thread_id: EntityId,
        plan_id: String,
        turn_id: Option<EntityId>,
        plan_markdown: String,
        implemented_at: Option<String>,
        implementation_thread_id: Option<EntityId>,
        created_at: Timestamp,
        updated_at: Timestamp,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        Self::extract_thread_status(state, &thread_id)?;
        Ok(vec![DomainEvent::ProposedPlanUpserted {
            thread_id,
            plan_id,
            turn_id,
            plan_markdown,
            implemented_at,
            implementation_thread_id,
            created_at,
            updated_at,
        }])
    }

    /// Record a turn's diff checkpoint (mcode `thread.turn.diff.complete`).
    /// Guards thread existence and echoes the checkpoint summary.
    #[allow(clippy::too_many_arguments)]
    fn decide_complete_turn_diff(
        thread_id: EntityId,
        turn_id: EntityId,
        checkpoint_turn_count: u32,
        checkpoint_ref: String,
        status: String,
        files: Vec<CheckpointFile>,
        assistant_message_id: Option<EntityId>,
        completed_at: Timestamp,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        Self::extract_thread_status(state, &thread_id)?;
        Ok(vec![DomainEvent::TurnDiffCompleted {
            thread_id,
            turn_id,
            checkpoint_turn_count,
            checkpoint_ref,
            status,
            files,
            assistant_message_id,
            completed_at,
        }])
    }

    /// Complete a revert, truncating the thread to `turn_count` turns (mcode
    /// `thread.revert.complete`). Guards thread existence; the projector
    /// performs the read-model truncation.
    fn decide_complete_revert(
        thread_id: EntityId,
        turn_count: u32,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        Self::extract_thread_status(state, &thread_id)?;
        Ok(vec![DomainEvent::ThreadRevertCompleted {
            thread_id,
            turn_count,
            reverted_at: Timestamp::now(),
        }])
    }

    /// Import standalone messages into an existing thread (mcode
    /// `thread.messages.import`). Guard: thread must exist. Emits a single
    /// [`DomainEvent::ThreadMessagesImported`] durable record; the thread's own
    /// id is used as the source to mark a native import (no external source
    /// thread). Message-body materialization is deferred, as for handoff/fork.
    fn decide_import_messages(
        thread_id: EntityId,
        imported_messages: Vec<ImportedMessage>,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        Self::extract_thread_status(state, &thread_id)?;
        let count = imported_messages.len() as u32;
        Ok(vec![DomainEvent::ThreadMessagesImported {
            thread_id,
            source_thread_id: thread_id,
            count,
            imported_at: Timestamp::now(),
        }])
    }

    /// Request a conversation rollback to a message (mcode
    /// `thread.conversation.rollback`). Requested-style async; syncode omits
    /// mcode's target-message invariant (removed turns come via `.complete`).
    fn decide_conversation_rollback(
        thread_id: EntityId,
        message_id: EntityId,
        num_turns: u32,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        Self::extract_thread_status(state, &thread_id)?;
        Ok(vec![DomainEvent::ConversationRollbackRequested {
            thread_id,
            message_id,
            num_turns,
            requested_at: Timestamp::now(),
        }])
    }

    /// Complete a conversation rollback (mcode
    /// `thread.conversation.rollback.complete`). The projector removes the
    /// turns in `removed_turn_ids`; the event store is untouched.
    fn decide_conversation_rollback_complete(
        thread_id: EntityId,
        message_id: EntityId,
        num_turns: u32,
        removed_turn_ids: Vec<EntityId>,
        state: Option<&serde_json::Value>,
    ) -> Result<Vec<DomainEvent>, DeciderError> {
        Self::extract_thread_status(state, &thread_id)?;
        Ok(vec![DomainEvent::ConversationRolledBack {
            thread_id,
            message_id,
            num_turns,
            removed_turn_ids,
            rolled_back_at: Timestamp::now(),
        }])
    }

    // ─── Helpers ─────────────────────────────────────────────────

    fn extract_thread_status(
        state: Option<&serde_json::Value>,
        id: &EntityId,
    ) -> Result<String, DeciderError> {
        let state = state.ok_or(DeciderError::ThreadNotFound(*id))?;
        state
            .get("status")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| DeciderError::InvalidThreadStatus("unknown".to_string()))
    }

    /// mcode PINNED_MESSAGES_MAX_COUNT: max pinned messages per thread.
    const MAX_PINNED_MESSAGES: usize = 100;
    /// Assumed mcode THREAD_MARKERS_MAX_COUNT: max markers per thread.
    const MAX_MARKERS: usize = 100;

    /// Pinned message ids currently in the thread (from the enriched state JSON).
    fn extract_pinned_message_ids(state: Option<&serde_json::Value>) -> Vec<String> {
        state
            .and_then(|s| s.get("pinned_message_ids"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Marker ids currently in the thread (from the enriched state JSON).
    fn extract_marker_ids(state: Option<&serde_json::Value>) -> Vec<String> {
        state
            .and_then(|s| s.get("marker_ids"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn extract_turn_status(
        state: Option<&serde_json::Value>,
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

    fn thread_state_with_pinned(ids: Vec<String>) -> serde_json::Value {
        serde_json::json!({ "status": "active", "pinned_message_ids": ids })
    }

    fn thread_state_with_markers(ids: Vec<String>) -> serde_json::Value {
        serde_json::json!({ "status": "active", "marker_ids": ids })
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
        )
        .unwrap();
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
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ProjectNotFound(_)
        ));
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
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ProjectUpdated {
                provider_id,
                default_model,
                ..
            } => {
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
        let events = Decider::decide(Command::DeleteProject { id }, Some(&state)).unwrap();
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
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ProjectNotFound(_)
        ));
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
                thread_id: None,
            },
            None,
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadCreated {
                project_id,
                provider_id,
                model,
                ..
            } => {
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
        let events =
            Decider::decide(Command::PauseThread { id }, Some(&thread_state_active())).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadStatusChanged {
                old_status,
                new_status,
                ..
            } => {
                assert_eq!(old_status, "active");
                assert_eq!(new_status, "paused");
            }
            _ => panic!("expected ThreadStatusChanged"),
        }
    }

    #[test]
    fn pause_thread_not_active_fails() {
        let id = EntityId::new();
        let result = Decider::decide(Command::PauseThread { id }, Some(&thread_state_paused()));
        assert!(result.is_err());
    }

    #[test]
    fn resume_thread_paused_success() {
        let id = EntityId::new();
        let events =
            Decider::decide(Command::ResumeThread { id }, Some(&thread_state_paused())).unwrap();
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
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadAlreadyCompleted
        ));
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
        )
        .unwrap();
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
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::TurnStarted {
                thread_id,
                sequence,
                user_input,
                ..
            } => {
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
        )
        .unwrap();
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
        )
        .unwrap();
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
        assert!(matches!(
            result2.unwrap_err(),
            DeciderError::TurnAlreadyCompleted
        ));
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
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::TurnFailed { error, .. } => assert_eq!(error, "API timeout"),
            _ => panic!("expected TurnFailed"),
        }
    }

    #[test]
    fn record_turn_files_empty_returns_nothing() {
        let id = EntityId::new();
        let events = Decider::decide(Command::RecordTurnFiles { id, files: vec![] }, None).unwrap();
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
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::MessageAdded {
                turn_id,
                role,
                content,
                ..
            } => {
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
        let events =
            Decider::decide(Command::InterruptTurn { id }, Some(&turn_state_running())).unwrap();
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
        let result = Decider::decide(Command::InterruptTurn { id }, Some(&turn_state_pending()));
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::TurnNotRunning(_)
        ));

        // completed turn cannot be interrupted
        let id2 = EntityId::new();
        let result2 = Decider::decide(
            Command::InterruptTurn { id: id2 },
            Some(&serde_json::json!({"status": "completed"})),
        );
        assert!(matches!(
            result2.unwrap_err(),
            DeciderError::TurnNotRunning(_)
        ));
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
        )
        .unwrap();
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
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
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
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::EmptyCheckpointRef
        ));
    }

    // ─── Thread lifecycle: delete / archive / unarchive ───────────

    #[test]
    fn archive_thread_active_success() {
        let id = EntityId::new();
        let events =
            Decider::decide(Command::ArchiveThread { id }, Some(&thread_state_active())).unwrap();
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
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::InvalidStateTransition { .. }
        ));
    }

    #[test]
    fn unarchive_thread_archived_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::UnarchiveThread { id },
            Some(&serde_json::json!({"status": "archived"})),
        )
        .unwrap();
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
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::InvalidStateTransition { .. }
        ));
    }

    #[test]
    fn delete_thread_success() {
        let id = EntityId::new();
        let events =
            Decider::decide(Command::DeleteThread { id }, Some(&thread_state_active())).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], DomainEvent::ThreadDeleted { .. }));
    }

    #[test]
    fn delete_thread_not_found() {
        let id = EntityId::new();
        let result = Decider::decide(Command::DeleteThread { id }, None);
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    // ─── Stop thread session ──────────────────────────────────────

    #[test]
    fn stop_thread_session_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::StopThreadSession { id },
            Some(&thread_state_active()),
        )
        .unwrap();
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
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
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
        )
        .unwrap();

        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], DomainEvent::ThreadCreated { .. }));
        match &events[1] {
            DomainEvent::ThreadMessagesImported {
                source_thread_id,
                count,
                ..
            } => {
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
        )
        .unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], DomainEvent::ThreadCreated { .. }));
        assert!(matches!(
            events[1],
            DomainEvent::ThreadMessagesImported { count: 1, .. }
        ));
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
        )
        .unwrap();
        // Source linkage preserved even with zero imported messages.
        assert!(matches!(
            events[1],
            DomainEvent::ThreadMessagesImported { count: 0, source_thread_id, .. } if source_thread_id == src
        ));
    }

    // ─── Thread mode settings (runtime / interaction) ──────────────

    #[test]
    fn set_thread_runtime_mode_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::SetThreadRuntimeMode {
                id,
                runtime_mode: "approval-required".to_string(),
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadRuntimeModeSet {
                id: ev_id,
                runtime_mode,
                ..
            } => {
                assert_eq!(ev_id, &id);
                assert_eq!(runtime_mode, "approval-required");
            }
            _ => panic!("expected ThreadRuntimeModeSet"),
        }
    }

    #[test]
    fn set_thread_runtime_mode_unknown_thread_rejected() {
        let id = EntityId::new();
        let result = Decider::decide(
            Command::SetThreadRuntimeMode {
                id,
                runtime_mode: "full-access".to_string(),
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    #[test]
    fn set_thread_interaction_mode_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::SetThreadInteractionMode {
                id,
                interaction_mode: "plan".to_string(),
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadInteractionModeSet {
                id: ev_id,
                interaction_mode,
                ..
            } => {
                assert_eq!(ev_id, &id);
                assert_eq!(interaction_mode, "plan");
            }
            _ => panic!("expected ThreadInteractionModeSet"),
        }
    }

    #[test]
    fn set_thread_interaction_mode_unknown_thread_rejected() {
        let id = EntityId::new();
        let result = Decider::decide(
            Command::SetThreadInteractionMode {
                id,
                interaction_mode: "default".to_string(),
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    // ─── Session set / queued-turn dispatch ────────────────────────

    fn session(status: &str) -> ThreadSession {
        ThreadSession {
            status: status.to_string(),
            provider_name: Some("anthropic".to_string()),
            runtime_mode: "full-access".to_string(),
            active_turn_id: None,
            last_error: None,
        }
    }

    #[test]
    fn set_thread_session_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::SetThreadSession {
                id,
                session: session("running"),
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadSessionSet {
                status,
                provider_name,
                runtime_mode,
                active_turn_id,
                last_error,
                ..
            } => {
                assert_eq!(status, "running");
                assert_eq!(provider_name.as_deref(), Some("anthropic"));
                assert_eq!(runtime_mode, "full-access");
                assert!(active_turn_id.is_none());
                assert!(last_error.is_none());
            }
            _ => panic!("expected ThreadSessionSet"),
        }
    }

    #[test]
    fn set_thread_session_unknown_thread_rejected() {
        let id = EntityId::new();
        let result = Decider::decide(
            Command::SetThreadSession {
                id,
                session: session("idle"),
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    #[test]
    fn dispatch_queued_turn_success() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::DispatchQueuedTurn {
                id,
                message_id: mid,
                runtime_mode: "approval-required".to_string(),
                interaction_mode: "plan".to_string(),
                dispatch_mode: "queue".to_string(),
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::TurnDispatchRequested {
                message_id,
                runtime_mode,
                interaction_mode,
                dispatch_mode,
                ..
            } => {
                assert_eq!(*message_id, mid);
                assert_eq!(runtime_mode, "approval-required");
                assert_eq!(interaction_mode, "plan");
                assert_eq!(dispatch_mode, "queue");
            }
            _ => panic!("expected TurnDispatchRequested"),
        }
    }

    #[test]
    fn dispatch_queued_turn_unknown_thread_rejected() {
        let id = EntityId::new();
        let result = Decider::decide(
            Command::DispatchQueuedTurn {
                id,
                message_id: id,
                runtime_mode: "full-access".to_string(),
                interaction_mode: "default".to_string(),
                dispatch_mode: "queue".to_string(),
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    #[test]
    fn append_assistant_delta_success() {
        let tid = EntityId::new();
        let mid = EntityId::new();
        let turn = EntityId::new();
        let events = Decider::decide(
            Command::AppendAssistantDelta {
                thread_id: tid,
                message_id: mid,
                turn_id: turn,
                delta: "Hello".to_string(),
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::MessageDeltaAppended {
                id, turn_id, delta, ..
            } => {
                assert_eq!(*id, mid);
                assert_eq!(*turn_id, turn);
                assert_eq!(delta, "Hello");
            }
            _ => panic!("expected MessageDeltaAppended"),
        }
    }

    #[test]
    fn append_assistant_delta_unknown_thread_rejected() {
        let tid = EntityId::new();
        let result = Decider::decide(
            Command::AppendAssistantDelta {
                thread_id: tid,
                message_id: tid,
                turn_id: tid,
                delta: "x".to_string(),
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    #[test]
    fn finalize_assistant_message_success() {
        let tid = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::FinalizeAssistantMessage {
                thread_id: tid,
                message_id: mid,
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::MessageStreamingFinalized { id, .. } => assert_eq!(*id, mid),
            _ => panic!("expected MessageStreamingFinalized"),
        }
    }

    #[test]
    fn finalize_assistant_message_unknown_thread_rejected() {
        let tid = EntityId::new();
        let result = Decider::decide(
            Command::FinalizeAssistantMessage {
                thread_id: tid,
                message_id: tid,
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    #[test]
    fn upsert_proposed_plan_success() {
        let tid = EntityId::new();
        let now = Timestamp::now();
        let events = Decider::decide(
            Command::UpsertProposedPlan {
                thread_id: tid,
                plan_id: "plan-1".to_string(),
                turn_id: None,
                plan_markdown: "# Plan".to_string(),
                implemented_at: None,
                implementation_thread_id: None,
                created_at: now,
                updated_at: now,
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ProposedPlanUpserted {
                thread_id,
                plan_id,
                plan_markdown,
                ..
            } => {
                assert_eq!(*thread_id, tid);
                assert_eq!(plan_id, "plan-1");
                assert_eq!(plan_markdown, "# Plan");
            }
            _ => panic!("expected ProposedPlanUpserted"),
        }
    }

    #[test]
    fn upsert_proposed_plan_unknown_thread_rejected() {
        let tid = EntityId::new();
        let result = Decider::decide(
            Command::UpsertProposedPlan {
                thread_id: tid,
                plan_id: "plan-1".to_string(),
                turn_id: None,
                plan_markdown: "# Plan".to_string(),
                implemented_at: None,
                implementation_thread_id: None,
                created_at: Timestamp::now(),
                updated_at: Timestamp::now(),
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    #[test]
    fn complete_turn_diff_success() {
        let tid = EntityId::new();
        let turn = EntityId::new();
        let now = Timestamp::now();
        let events = Decider::decide(
            Command::CompleteTurnDiff {
                thread_id: tid,
                turn_id: turn,
                checkpoint_turn_count: 3,
                checkpoint_ref: "abc123".to_string(),
                status: "ready".to_string(),
                files: vec![CheckpointFile {
                    path: "src/main.rs".to_string(),
                    kind: "modify".to_string(),
                    additions: 10,
                    deletions: 2,
                }],
                assistant_message_id: None,
                completed_at: now,
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::TurnDiffCompleted {
                turn_id,
                checkpoint_ref,
                status,
                files,
                ..
            } => {
                assert_eq!(*turn_id, turn);
                assert_eq!(checkpoint_ref, "abc123");
                assert_eq!(status, "ready");
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].additions, 10);
            }
            _ => panic!("expected TurnDiffCompleted"),
        }
    }

    #[test]
    fn complete_turn_diff_unknown_thread_rejected() {
        let tid = EntityId::new();
        let result = Decider::decide(
            Command::CompleteTurnDiff {
                thread_id: tid,
                turn_id: tid,
                checkpoint_turn_count: 0,
                checkpoint_ref: "abc".to_string(),
                status: "ready".to_string(),
                files: vec![],
                assistant_message_id: None,
                completed_at: Timestamp::now(),
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    #[test]
    fn complete_revert_success() {
        let tid = EntityId::new();
        let events = Decider::decide(
            Command::CompleteRevert {
                thread_id: tid,
                turn_count: 2,
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadRevertCompleted {
                thread_id,
                turn_count,
                ..
            } => {
                assert_eq!(*thread_id, tid);
                assert_eq!(*turn_count, 2);
            }
            _ => panic!("expected ThreadRevertCompleted"),
        }
    }

    #[test]
    fn complete_revert_unknown_thread_rejected() {
        let tid = EntityId::new();
        let result = Decider::decide(
            Command::CompleteRevert {
                thread_id: tid,
                turn_count: 0,
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    #[test]
    fn conversation_rollback_success() {
        let tid = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::ConversationRollback {
                thread_id: tid,
                message_id: mid,
                num_turns: 1,
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ConversationRollbackRequested {
                thread_id,
                message_id,
                num_turns,
                ..
            } => {
                assert_eq!(*thread_id, tid);
                assert_eq!(*message_id, mid);
                assert_eq!(*num_turns, 1);
            }
            _ => panic!("expected ConversationRollbackRequested"),
        }
    }

    #[test]
    fn conversation_rollback_unknown_thread_rejected() {
        let tid = EntityId::new();
        let result = Decider::decide(
            Command::ConversationRollback {
                thread_id: tid,
                message_id: tid,
                num_turns: 1,
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    #[test]
    fn conversation_rollback_complete_success() {
        let tid = EntityId::new();
        let mid = EntityId::new();
        let removed = vec![EntityId::new(), EntityId::new()];
        let events = Decider::decide(
            Command::ConversationRollbackComplete {
                thread_id: tid,
                message_id: mid,
                num_turns: 2,
                removed_turn_ids: removed.clone(),
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ConversationRolledBack {
                thread_id,
                removed_turn_ids,
                num_turns,
                ..
            } => {
                assert_eq!(*thread_id, tid);
                assert_eq!(*num_turns, 2);
                assert_eq!(removed_turn_ids.len(), 2);
            }
            _ => panic!("expected ConversationRolledBack"),
        }
    }

    #[test]
    fn conversation_rollback_complete_unknown_thread_rejected() {
        let tid = EntityId::new();
        let result = Decider::decide(
            Command::ConversationRollbackComplete {
                thread_id: tid,
                message_id: tid,
                num_turns: 1,
                removed_turn_ids: vec![],
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    #[test]
    fn import_messages_success() {
        let tid = EntityId::new();
        let msgs = vec![
            ImportedMessage {
                source_message_id: EntityId::new(),
                role: "user".into(),
                text: "hi".into(),
            },
            ImportedMessage {
                source_message_id: EntityId::new(),
                role: "assistant".into(),
                text: "hello".into(),
            },
        ];
        let events = Decider::decide(
            Command::ImportMessages {
                thread_id: tid,
                imported_messages: msgs,
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadMessagesImported {
                thread_id,
                source_thread_id,
                count,
                ..
            } => {
                assert_eq!(*thread_id, tid);
                // Standalone import records the thread's own id as the source.
                assert_eq!(*source_thread_id, tid);
                assert_eq!(*count, 2);
            }
            _ => panic!("expected ThreadMessagesImported"),
        }
    }

    #[test]
    fn import_messages_unknown_thread_rejected() {
        let tid = EntityId::new();
        let result = Decider::decide(
            Command::ImportMessages {
                thread_id: tid,
                imported_messages: vec![],
            },
            None,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeciderError::ThreadNotFound(_)
        ));
    }

    // ─── Turn interaction commands ────────────────────────────────

    #[test]
    fn respond_thread_approval_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::RespondThreadApproval {
                id,
                request_id: "req-1".into(),
                decision: "approved".into(),
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ThreadApprovalResponded {
                request_id,
                decision,
                ..
            } => {
                assert_eq!(request_id, "req-1");
                assert_eq!(decision, "approved");
            }
            _ => panic!("expected ThreadApprovalResponded"),
        }
    }

    #[test]
    fn respond_thread_user_input_success() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::RespondThreadUserInput {
                id,
                request_id: "req-2".into(),
                answers: "yes".into(),
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert!(matches!(
            events[0],
            DomainEvent::ThreadUserInputResponded { .. }
        ));
    }

    #[test]
    fn edit_and_resend_thread_message_success() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::EditAndResendThreadMessage {
                id,
                message_id: mid,
                text: "edited".into(),
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        match &events[0] {
            DomainEvent::ThreadMessageEditedAndResent { text, .. } => assert_eq!(text, "edited"),
            _ => panic!("expected ThreadMessageEditedAndResent"),
        }
    }

    #[test]
    fn append_thread_activity_emits_activity_logged() {
        let id = EntityId::new();
        let events = Decider::decide(
            Command::AppendThreadActivity {
                id,
                activity_type: "checkpoint".into(),
                description: "captured".into(),
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::ActivityLogged {
                activity_type,
                description,
                ..
            } => {
                assert_eq!(activity_type, "checkpoint");
                assert_eq!(description, "captured");
            }
            _ => panic!("expected ActivityLogged"),
        }
    }

    #[test]
    fn turn_interaction_commands_unknown_thread_rejected() {
        let id = EntityId::new();
        let r = Decider::decide(
            Command::RespondThreadApproval {
                id,
                request_id: "r".into(),
                decision: "approved".into(),
            },
            None,
        );
        assert!(matches!(r.unwrap_err(), DeciderError::ThreadNotFound(_)));

        let r = Decider::decide(
            Command::RespondThreadUserInput {
                id,
                request_id: "r".into(),
                answers: "a".into(),
            },
            None,
        );
        assert!(matches!(r.unwrap_err(), DeciderError::ThreadNotFound(_)));

        let r = Decider::decide(
            Command::EditAndResendThreadMessage {
                id,
                message_id: id,
                text: "t".into(),
            },
            None,
        );
        assert!(matches!(r.unwrap_err(), DeciderError::ThreadNotFound(_)));

        let r = Decider::decide(
            Command::AppendThreadActivity {
                id,
                activity_type: "t".into(),
                description: "d".into(),
            },
            None,
        );
        assert!(matches!(r.unwrap_err(), DeciderError::ThreadNotFound(_)));
    }

    // ─── Pinned messages ─────────────────────────────────────────

    #[test]
    fn add_pinned_message_success() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::AddPinnedMessage {
                id,
                message_id: mid,
            },
            Some(&thread_state_active()),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::PinnedMessageAdded {
                thread_id,
                message_id,
                label,
                done,
                ..
            } => {
                assert_eq!(*thread_id, id);
                assert_eq!(*message_id, mid);
                assert!(label.is_none());
                assert!(!*done);
            }
            _ => panic!("expected PinnedMessageAdded"),
        }
    }

    #[test]
    fn remove_pinned_message_success() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::RemovePinnedMessage {
                id,
                message_id: mid,
            },
            Some(&thread_state_with_pinned(vec![mid.as_str()])),
        )
        .unwrap();
        assert!(matches!(
            events[0],
            DomainEvent::PinnedMessageRemoved { .. }
        ));
    }

    #[test]
    fn set_pinned_message_done_success() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::SetPinnedMessageDone {
                id,
                message_id: mid,
                done: true,
            },
            Some(&thread_state_with_pinned(vec![mid.as_str()])),
        )
        .unwrap();
        match &events[0] {
            DomainEvent::PinnedMessageDoneSet { done, .. } => assert!(*done),
            _ => panic!("expected PinnedMessageDoneSet"),
        }
    }

    #[test]
    fn set_pinned_message_label_success() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::SetPinnedMessageLabel {
                id,
                message_id: mid,
                label: Some("todo".into()),
            },
            Some(&thread_state_with_pinned(vec![mid.as_str()])),
        )
        .unwrap();
        match &events[0] {
            DomainEvent::PinnedMessageLabelSet { label, .. } => {
                assert_eq!(label.as_deref(), Some("todo"))
            }
            _ => panic!("expected PinnedMessageLabelSet"),
        }
    }

    #[test]
    fn add_pinned_message_rejects_at_cap() {
        let id = EntityId::new();
        let mid = EntityId::new();
        // State already holds the maximum number of pinned messages.
        let full: Vec<String> = (0..Decider::MAX_PINNED_MESSAGES)
            .map(|i| format!("p{i}"))
            .collect();
        let err = Decider::decide(
            Command::AddPinnedMessage {
                id,
                message_id: mid,
            },
            Some(&thread_state_with_pinned(full)),
        )
        .unwrap_err();
        assert!(
            matches!(err, DeciderError::PinnedMessageLimitReached { .. }),
            "adding beyond the cap must be rejected, got: {err:?}"
        );
    }

    #[test]
    fn pinned_remove_and_set_reject_missing() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let state = Some(&thread_state_with_pinned(vec!["other".to_string()]));
        let err = Decider::decide(
            Command::RemovePinnedMessage {
                id,
                message_id: mid,
            },
            state,
        )
        .unwrap_err();
        assert!(
            matches!(err, DeciderError::PinnedMessageNotFound(_)),
            "removing a missing pin must be rejected, got: {err:?}"
        );
        let err = Decider::decide(
            Command::SetPinnedMessageDone {
                id,
                message_id: mid,
                done: true,
            },
            state,
        )
        .unwrap_err();
        assert!(
            matches!(err, DeciderError::PinnedMessageNotFound(_)),
            "set-done on a missing pin must be rejected, got: {err:?}"
        );
    }

    #[test]
    fn pinned_message_commands_unknown_thread_rejected() {
        let id = EntityId::new();
        let mid = EntityId::new();
        assert!(
            Decider::decide(
                Command::AddPinnedMessage {
                    id,
                    message_id: mid
                },
                None
            )
            .is_err()
        );
        assert!(
            Decider::decide(
                Command::RemovePinnedMessage {
                    id,
                    message_id: mid
                },
                None
            )
            .is_err()
        );
        assert!(
            Decider::decide(
                Command::SetPinnedMessageDone {
                    id,
                    message_id: mid,
                    done: true
                },
                None
            )
            .is_err()
        );
        assert!(
            Decider::decide(
                Command::SetPinnedMessageLabel {
                    id,
                    message_id: mid,
                    label: None
                },
                None
            )
            .is_err()
        );
    }

    // ─── Markers ─────────────────────────────────────────────────

    fn add_marker_cmd(id: EntityId, marker_id: EntityId, message_id: EntityId) -> Command {
        Command::AddMarker {
            id,
            marker_id,
            message_id,
            start_offset: 0,
            end_offset: 5,
            selected_text: "hello".to_string(),
            style: "highlight".to_string(),
            color: "yellow".to_string(),
        }
    }

    #[test]
    fn add_marker_success() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let msg = EntityId::new();
        let events =
            Decider::decide(add_marker_cmd(id, mid, msg), Some(&thread_state_active())).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::MarkerAdded {
                thread_id,
                marker_id,
                message_id,
                start_offset,
                end_offset,
                selected_text,
                style,
                color,
                label,
                done,
                ..
            } => {
                assert_eq!(*thread_id, id);
                assert_eq!(*marker_id, mid);
                assert_eq!(*message_id, msg);
                assert_eq!(*start_offset, 0);
                assert_eq!(*end_offset, 5);
                assert_eq!(selected_text, "hello");
                assert_eq!(style, "highlight");
                assert_eq!(color, "yellow");
                assert!(label.is_none());
                assert!(!*done);
            }
            _ => panic!("expected MarkerAdded"),
        }
    }

    #[test]
    fn add_marker_rejects_invalid_style_color_and_range() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let msg = EntityId::new();
        let state = thread_state_active();

        let mk = |style: &str, color: &str, start: u64, end: u64| Command::AddMarker {
            id,
            marker_id: mid,
            message_id: msg,
            start_offset: start,
            end_offset: end,
            selected_text: "x".to_string(),
            style: style.to_string(),
            color: color.to_string(),
        };

        // Invalid style.
        let err = Decider::decide(mk("bold", "yellow", 0, 5), Some(&state)).unwrap_err();
        assert!(
            matches!(err, DeciderError::InvalidMarkerStyle(ref s) if s.as_str() == "bold"),
            "bad style must be rejected, got: {err:?}"
        );

        // Invalid color.
        let err = Decider::decide(mk("highlight", "purple", 0, 5), Some(&state)).unwrap_err();
        assert!(
            matches!(err, DeciderError::InvalidMarkerColor(ref c) if c.as_str() == "purple"),
            "bad color must be rejected, got: {err:?}"
        );

        // Invalid range: end == start, and end < start.
        let err = Decider::decide(mk("highlight", "yellow", 5, 5), Some(&state)).unwrap_err();
        assert!(
            matches!(
                err,
                DeciderError::InvalidMarkerRange {
                    start_offset: 5,
                    end_offset: 5
                }
            ),
            "equal offsets must be rejected, got: {err:?}"
        );
        let err = Decider::decide(mk("highlight", "yellow", 9, 3), Some(&state)).unwrap_err();
        assert!(
            matches!(err, DeciderError::InvalidMarkerRange { .. }),
            "inverted offsets must be rejected, got: {err:?}"
        );

        // Sanity: a fully-valid marker still succeeds.
        Decider::decide(mk("underline", "pink", 0, 1), Some(&state)).unwrap();
    }

    #[test]
    fn remove_marker_success() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::RemoveMarker { id, marker_id: mid },
            Some(&thread_state_with_markers(vec![mid.as_str()])),
        )
        .unwrap();
        assert!(matches!(events[0], DomainEvent::MarkerRemoved { .. }));
    }

    #[test]
    fn set_marker_done_success() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::SetMarkerDone {
                id,
                marker_id: mid,
                done: true,
            },
            Some(&thread_state_with_markers(vec![mid.as_str()])),
        )
        .unwrap();
        match &events[0] {
            DomainEvent::MarkerDoneSet {
                done, marker_id, ..
            } => {
                assert!(*done);
                assert_eq!(*marker_id, mid);
            }
            _ => panic!("expected MarkerDoneSet"),
        }
    }

    #[test]
    fn set_marker_label_success() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let events = Decider::decide(
            Command::SetMarkerLabel {
                id,
                marker_id: mid,
                label: Some("note".into()),
            },
            Some(&thread_state_with_markers(vec![mid.as_str()])),
        )
        .unwrap();
        match &events[0] {
            DomainEvent::MarkerLabelSet {
                label, marker_id, ..
            } => {
                assert_eq!(label.as_deref(), Some("note"));
                assert_eq!(*marker_id, mid);
            }
            _ => panic!("expected MarkerLabelSet"),
        }
    }

    #[test]
    fn marker_commands_unknown_thread_rejected() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let msg = EntityId::new();
        assert!(Decider::decide(add_marker_cmd(id, mid, msg), None).is_err());
        assert!(Decider::decide(Command::RemoveMarker { id, marker_id: mid }, None).is_err());
        assert!(
            Decider::decide(
                Command::SetMarkerDone {
                    id,
                    marker_id: mid,
                    done: true
                },
                None
            )
            .is_err()
        );
        assert!(
            Decider::decide(
                Command::SetMarkerLabel {
                    id,
                    marker_id: mid,
                    label: None
                },
                None
            )
            .is_err()
        );
    }

    #[test]
    fn add_marker_rejects_at_cap() {
        let id = EntityId::new();
        // State already holds the maximum number of markers.
        let full: Vec<String> = (0..Decider::MAX_MARKERS).map(|i| format!("m{i}")).collect();
        let err = Decider::decide(
            add_marker_cmd(id, EntityId::new(), EntityId::new()),
            Some(&thread_state_with_markers(full)),
        )
        .unwrap_err();
        assert!(
            matches!(err, DeciderError::MarkerLimitReached { .. }),
            "adding a marker beyond the cap must be rejected, got: {err:?}"
        );
    }

    #[test]
    fn marker_remove_and_set_reject_missing() {
        let id = EntityId::new();
        let mid = EntityId::new();
        let state = Some(&thread_state_with_markers(vec!["other".to_string()]));
        let err = Decider::decide(Command::RemoveMarker { id, marker_id: mid }, state).unwrap_err();
        assert!(
            matches!(err, DeciderError::MarkerNotFound(_)),
            "removing a missing marker must be rejected, got: {err:?}"
        );
        let err = Decider::decide(
            Command::SetMarkerDone {
                id,
                marker_id: mid,
                done: true,
            },
            state,
        )
        .unwrap_err();
        assert!(
            matches!(err, DeciderError::MarkerNotFound(_)),
            "set-done on a missing marker must be rejected, got: {err:?}"
        );
    }
}

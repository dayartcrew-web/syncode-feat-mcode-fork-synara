//! Application use cases — business workflow orchestration
//!
//! The `ApplicationService` sits between the RPC/transport layer and the
//! `Orchestrator`, providing:
//!
//! - **Command use cases**: Semantic wrappers around single commands with
//!   strongly-typed params and return values.
//! - **Conversation workflow**: Multi-step orchestration (e.g., start a turn
//!   and provoke the provider, then complete with output).
//! - **Query use cases**: Read-model access with aggregated views (dashboard,
//!   thread detail).
//!
//! Layering:
//!   RPC (json parsing) → ApplicationService (business workflows) → Orchestrator (CQRS) → Decider (pure)

use std::sync::Arc;
use syncode_core::EntityId;

use crate::decider::{Command, ImportedMessage, ThreadSession};
use crate::pipeline::{CommandResult, Orchestrator, OrchestrationError};
use crate::read_model::{
    ActivityView, MessageView, ProjectView, ThreadView, TurnView,
};

// ─── Aggregated Query Types ──────────────────────────────────────

/// Project dashboard: project metadata + its threads + recent turns.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ProjectDashboard {
    pub project: ProjectView,
    pub threads: Vec<ThreadView>,
    pub recent_turns: Vec<TurnView>,
    pub total_turns: u32,
}

/// Thread detail: thread metadata + turns + messages + activities.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ThreadDetail {
    pub thread: ThreadView,
    pub turns: Vec<TurnView>,
    pub messages: Vec<MessageView>,
    pub activities: Vec<ActivityView>,
}

// ─── Application Service ──────────────────────────────────────────

/// Application service composing Orchestrator calls into business workflows.
///
/// Owned by the transport layer (WsState) and shared across connections.
#[derive(Clone)]
pub struct ApplicationService {
    orchestrator: Arc<Orchestrator>,
}

impl ApplicationService {
    /// Create a new ApplicationService wrapping an existing Orchestrator.
    pub fn new(orchestrator: Arc<Orchestrator>) -> Self {
        Self { orchestrator }
    }

    /// Get a reference to the underlying orchestrator (for advanced use).
    pub fn orchestrator(&self) -> &Arc<Orchestrator> {
        &self.orchestrator
    }

    // ─── Project Command Use Cases ───────────────────────────────

    /// Create a new project.
    pub async fn create_project(
        &self,
        name: String,
        root_path: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::CreateProject { name, root_path })
            .await
    }

    /// Update a project's configuration (provider, model).
    pub async fn update_project_config(
        &self,
        id: EntityId,
        provider_id: Option<String>,
        default_model: Option<String>,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::UpdateProjectConfig {
                id,
                provider_id,
                default_model,
            })
            .await
    }

    /// Delete a project (tombstone). Faithful to mcode `project.delete`.
    ///
    /// Rejects with [`OrchestrationError::ProjectNotFound`] if the project does
    /// not exist in the read model; the Decider mirrors the same guard.
    pub async fn delete_project(
        &self,
        id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        if self.get_project(&id.to_string()).await.is_none() {
            return Err(OrchestrationError::ProjectNotFound(id));
        }
        self.orchestrator
            .handle_command(Command::DeleteProject { id })
            .await
    }

    // ─── Thread Command Use Cases ────────────────────────────────

    /// Create a new thread within a project.
    ///
    /// Rejects with [`OrchestrationError::ProjectNotFound`] if the parent
    /// project does not exist in the read model, preventing orphan threads.
    /// (The Decider is pure and has no project state, so this precondition
    /// is enforced here at the application layer.)
    pub async fn create_thread(
        &self,
        project_id: EntityId,
        provider_id: String,
        model: String,
    ) -> Result<CommandResult, OrchestrationError> {
        if self.get_project(&project_id.to_string()).await.is_none() {
            return Err(OrchestrationError::ProjectNotFound(project_id));
        }
        self.orchestrator
            .handle_command(Command::CreateThread {
                project_id,
                provider_id,
                model,
            })
            .await
    }

    /// Pause an active thread.
    pub async fn pause_thread(
        &self,
        id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator.handle_command(Command::PauseThread { id }).await
    }

    /// Resume a paused thread.
    pub async fn resume_thread(
        &self,
        id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator.handle_command(Command::ResumeThread { id }).await
    }

    /// Archive a thread. Faithful to mcode `thread.archive`.
    pub async fn archive_thread(
        &self,
        id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator.handle_command(Command::ArchiveThread { id }).await
    }

    /// Unarchive (restore) a thread. Faithful to mcode `thread.unarchive`.
    pub async fn unarchive_thread(
        &self,
        id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator.handle_command(Command::UnarchiveThread { id }).await
    }

    /// Delete a thread (tombstone). Faithful to mcode `thread.delete`.
    pub async fn delete_thread(
        &self,
        id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator.handle_command(Command::DeleteThread { id }).await
    }

    /// Stop the active provider session for a thread. Faithful to mcode
    /// `thread.session.stop`. Emits a stop-request event; the reactor performs
    /// the actual session stop as a side effect.
    pub async fn stop_thread_session(
        &self,
        id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator.handle_command(Command::StopThreadSession { id }).await
    }

    /// Create a thread by handoff from a source thread, importing its messages.
    /// Faithful to mcode `thread.handoff.create`.
    pub async fn handoff_create_thread(
        &self,
        project_id: EntityId,
        provider_id: String,
        model: String,
        source_thread_id: EntityId,
        imported_messages: Vec<ImportedMessage>,
    ) -> Result<CommandResult, OrchestrationError> {
        self.create_thread_from_source(
            project_id,
            provider_id,
            model,
            source_thread_id,
            imported_messages,
            |project_id, provider_id, model, source_thread_id, imported_messages| {
                Command::HandoffCreateThread {
                    project_id,
                    provider_id,
                    model,
                    source_thread_id,
                    imported_messages,
                }
            },
        )
        .await
    }

    /// Create a thread by forking a source thread, importing its messages.
    /// Faithful to mcode `thread.fork.create`.
    pub async fn fork_create_thread(
        &self,
        project_id: EntityId,
        provider_id: String,
        model: String,
        source_thread_id: EntityId,
        imported_messages: Vec<ImportedMessage>,
    ) -> Result<CommandResult, OrchestrationError> {
        self.create_thread_from_source(
            project_id,
            provider_id,
            model,
            source_thread_id,
            imported_messages,
            |project_id, provider_id, model, source_thread_id, imported_messages| {
                Command::ForkCreateThread {
                    project_id,
                    provider_id,
                    model,
                    source_thread_id,
                    imported_messages,
                }
            },
        )
        .await
    }

    /// Shared precondition checks for handoff/fork thread creation: the parent
    /// project and the source thread must both exist in the read model.
    async fn create_thread_from_source(
        &self,
        project_id: EntityId,
        provider_id: String,
        model: String,
        source_thread_id: EntityId,
        imported_messages: Vec<ImportedMessage>,
        build: impl Fn(EntityId, String, String, EntityId, Vec<ImportedMessage>) -> Command,
    ) -> Result<CommandResult, OrchestrationError> {
        if self.get_project(&project_id.to_string()).await.is_none() {
            return Err(OrchestrationError::ProjectNotFound(project_id));
        }
        if self.get_thread(&source_thread_id.to_string()).await.is_none() {
            return Err(OrchestrationError::ThreadNotFound(source_thread_id));
        }
        self.orchestrator
            .handle_command(build(project_id, provider_id, model, source_thread_id, imported_messages))
            .await
    }

    /// Cancel a thread (and any in-progress turns).
    pub async fn cancel_thread(
        &self,
        id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator.handle_command(Command::CancelThread { id }).await
    }

    /// Mark a thread as complete.
    pub async fn complete_thread(
        &self,
        id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::CompleteThread { id })
            .await
    }

    /// Set a thread's title.
    pub async fn set_thread_title(
        &self,
        id: EntityId,
        title: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::SetThreadTitle { id, title })
            .await
    }

    /// Set a thread's runtime mode. Faithful to mcode `thread.runtime-mode.set`
    /// (runtimeMode: "approval-required" | "full-access"). The Decider guards
    /// thread existence.
    pub async fn set_thread_runtime_mode(
        &self,
        id: EntityId,
        runtime_mode: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::SetThreadRuntimeMode { id, runtime_mode })
            .await
    }

    /// Set a thread's provider interaction mode. Faithful to mcode
    /// `thread.interaction-mode.set` (interactionMode: "default" | "plan"). The
    /// Decider guards thread existence.
    pub async fn set_thread_interaction_mode(
        &self,
        id: EntityId,
        interaction_mode: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::SetThreadInteractionMode { id, interaction_mode })
            .await
    }

    /// Set a thread's provider session state. Faithful to mcode
    /// `thread.session.set` {session: OrchestrationSession}. The Decider guards
    /// thread existence; the session is materialized onto the thread read model.
    pub async fn set_thread_session(
        &self,
        id: EntityId,
        session: ThreadSession,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::SetThreadSession { id, session })
            .await
    }

    /// Dispatch a queued turn to the provider for a thread. Faithful to mcode
    /// `thread.turn.dispatch-queued` {messageId, runtimeMode, interactionMode,
    /// dispatchMode}. Records the request; the provider dispatch is handled by
    /// the command reactor when wired.
    pub async fn dispatch_queued_turn(
        &self,
        id: EntityId,
        message_id: EntityId,
        runtime_mode: String,
        interaction_mode: String,
        dispatch_mode: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::DispatchQueuedTurn {
                id,
                message_id,
                runtime_mode,
                interaction_mode,
                dispatch_mode,
            })
            .await
    }

    /// Stream a chunk of an assistant message. Faithful to mcode
    /// `thread.message.assistant.delta` {threadId, messageId, delta, turnId}.
    /// The first delta creates the message; subsequent deltas append. Issued by
    /// the provider stream consumer as deltas arrive.
    pub async fn append_assistant_delta(
        &self,
        thread_id: EntityId,
        message_id: EntityId,
        turn_id: EntityId,
        delta: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::AppendAssistantDelta {
                thread_id,
                message_id,
                turn_id,
                delta,
            })
            .await
    }

    /// Finalize a streamed assistant message. Faithful to mcode
    /// `thread.message.assistant.complete` {threadId, messageId, turnId}. Flips
    /// the message's `is_streaming` flag to false.
    pub async fn finalize_assistant_message(
        &self,
        thread_id: EntityId,
        message_id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::FinalizeAssistantMessage {
                thread_id,
                message_id,
            })
            .await
    }

    /// Respond to a pending provider approval request for a thread. Faithful to
    /// mcode `thread.approval.respond`. Records the response; the provider
    /// dispatch is handled by the command reactor when wired.
    pub async fn respond_thread_approval(
        &self,
        id: EntityId,
        request_id: String,
        decision: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::RespondThreadApproval { id, request_id, decision })
            .await
    }

    /// Respond to a pending provider user-input request for a thread. Faithful
    /// to mcode `thread.user-input.respond`.
    pub async fn respond_thread_user_input(
        &self,
        id: EntityId,
        request_id: String,
        answers: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::RespondThreadUserInput { id, request_id, answers })
            .await
    }

    /// Edit a thread message and trigger a new provider turn from it. Faithful
    /// to mcode `thread.message.edit-and-resend`.
    pub async fn edit_and_resend_thread_message(
        &self,
        id: EntityId,
        message_id: EntityId,
        text: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::EditAndResendThreadMessage { id, message_id, text })
            .await
    }

    /// Append an activity entry to a thread. Faithful to mcode
    /// `thread.activity.append` → activity-appended payload.
    pub async fn append_thread_activity(
        &self,
        id: EntityId,
        activity_type: String,
        description: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::AppendThreadActivity { id, activity_type, description })
            .await
    }

    /// Pin a message to a thread. Faithful to mcode `thread.pinned-message.add`.
    pub async fn add_pinned_message(
        &self,
        id: EntityId,
        message_id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::AddPinnedMessage { id, message_id })
            .await
    }

    /// Unpin a message from a thread. Faithful to mcode `thread.pinned-message.remove`.
    pub async fn remove_pinned_message(
        &self,
        id: EntityId,
        message_id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::RemovePinnedMessage { id, message_id })
            .await
    }

    /// Set a pinned message's done flag. Faithful to mcode `thread.pinned-message.done.set`.
    pub async fn set_pinned_message_done(
        &self,
        id: EntityId,
        message_id: EntityId,
        done: bool,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::SetPinnedMessageDone { id, message_id, done })
            .await
    }

    /// Set a pinned message's label. Faithful to mcode `thread.pinned-message.label.set`.
    pub async fn set_pinned_message_label(
        &self,
        id: EntityId,
        message_id: EntityId,
        label: Option<String>,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::SetPinnedMessageLabel { id, message_id, label })
            .await
    }

    /// Add a marker to a thread message. Faithful to mcode `thread.marker.add`.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_marker(
        &self,
        id: EntityId,
        marker_id: EntityId,
        message_id: EntityId,
        start_offset: u64,
        end_offset: u64,
        selected_text: String,
        style: String,
        color: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::AddMarker {
                id, marker_id, message_id, start_offset, end_offset, selected_text, style, color,
            })
            .await
    }

    /// Remove a marker from a thread. Faithful to mcode `thread.marker.remove`.
    pub async fn remove_marker(
        &self,
        id: EntityId,
        marker_id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::RemoveMarker { id, marker_id })
            .await
    }

    /// Set a marker's done flag. Faithful to mcode `thread.marker.done.set`.
    pub async fn set_marker_done(
        &self,
        id: EntityId,
        marker_id: EntityId,
        done: bool,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::SetMarkerDone { id, marker_id, done })
            .await
    }

    /// Set a marker's label. Faithful to mcode `thread.marker.label.set`.
    pub async fn set_marker_label(
        &self,
        id: EntityId,
        marker_id: EntityId,
        label: Option<String>,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::SetMarkerLabel { id, marker_id, label })
            .await
    }

    // ─── Turn Command Use Cases ──────────────────────────────────

    /// Start a conversation turn.
    ///
    /// This runs `StartTurn` through the pipeline. If a `CommandReactor` is
    /// wired into the orchestrator, it will also provoke the provider session
    /// (side effect).
    pub async fn start_turn(
        &self,
        thread_id: EntityId,
        sequence: u32,
        user_input: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::StartTurn {
                thread_id,
                sequence,
                user_input,
            })
            .await
    }

    /// Complete a turn with assistant output.
    pub async fn complete_turn(
        &self,
        turn_id: EntityId,
        assistant_output: String,
        duration_ms: u64,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::CompleteTurn {
                id: turn_id,
                assistant_output,
                duration_ms,
            })
            .await
    }

    /// Record a turn failure.
    pub async fn fail_turn(
        &self,
        turn_id: EntityId,
        error: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::FailTurn { id: turn_id, error })
            .await
    }

    /// Cancel a turn.
    pub async fn cancel_turn(
        &self,
        turn_id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::CancelTurn { id: turn_id })
            .await
    }

    /// Record files modified during a turn.
    pub async fn record_turn_files(
        &self,
        turn_id: EntityId,
        files: Vec<String>,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::RecordTurnFiles {
                id: turn_id,
                files,
            })
            .await
    }

    /// Set a turn's git checkpoint.
    pub async fn set_turn_checkpoint(
        &self,
        turn_id: EntityId,
        git_ref: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::SetTurnCheckpoint {
                id: turn_id,
                git_ref,
            })
            .await
    }

    /// Interrupt an in-progress (running) turn. Also interrupts the backing
    /// provider session when a `CommandReactor` is wired into the orchestrator.
    pub async fn interrupt_turn(
        &self,
        turn_id: EntityId,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::InterruptTurn { id: turn_id })
            .await
    }

    /// Roll a thread back to a previously-captured git checkpoint.
    pub async fn revert_to_checkpoint(
        &self,
        thread_id: EntityId,
        git_ref: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::RevertToCheckpoint {
                thread_id,
                git_ref,
            })
            .await
    }

    // ─── Message Command Use Cases ────────────────────────────────

    /// Add a message to a turn (user, assistant, system, or tool).
    pub async fn add_message(
        &self,
        turn_id: EntityId,
        role: String,
        content: String,
    ) -> Result<CommandResult, OrchestrationError> {
        self.orchestrator
            .handle_command(Command::AddMessage {
                turn_id,
                role,
                content,
            })
            .await
    }

    // ─── Query Use Cases ────────────────────────────────────────

    /// List all projects.
    pub async fn list_projects(&self) -> Vec<ProjectView> {
        let lock = self.orchestrator.read_model_ref();
        let store = lock.read().await;
        store.projects.values().cloned().collect()
    }

    /// Get a single project by ID.
    pub async fn get_project(&self, id: &str) -> Option<ProjectView> {
        let lock = self.orchestrator.read_model_ref();
        let store = lock.read().await;
        store.projects.get(id).cloned()
    }

    /// List threads, optionally filtered by project.
    pub async fn list_threads(&self, project_id: Option<&str>) -> Vec<ThreadView> {
        let lock = self.orchestrator.read_model_ref();
        let store = lock.read().await;
        match project_id {
            Some(pid) => store
                .threads
                .values()
                .filter(|t| t.project_id == pid)
                .cloned()
                .collect(),
            None => store.threads.values().cloned().collect(),
        }
    }

    /// Get a single thread by ID.
    pub async fn get_thread(&self, id: &str) -> Option<ThreadView> {
        let lock = self.orchestrator.read_model_ref();
        let store = lock.read().await;
        store.threads.get(id).cloned()
    }

    /// List turns, optionally filtered by thread.
    pub async fn list_turns(&self, thread_id: Option<&str>) -> Vec<TurnView> {
        let lock = self.orchestrator.read_model_ref();
        let store = lock.read().await;
        match thread_id {
            Some(tid) => store
                .turns
                .values()
                .filter(|t| t.thread_id == tid)
                .cloned()
                .collect(),
            None => store.turns.values().cloned().collect(),
        }
    }

    /// Get a single turn by ID.
    pub async fn get_turn(&self, id: &str) -> Option<TurnView> {
        let lock = self.orchestrator.read_model_ref();
        let store = lock.read().await;
        store.turns.get(id).cloned()
    }

    /// Get a project dashboard: project + threads + recent turns.
    pub async fn get_project_dashboard(&self, project_id: &str) -> Option<ProjectDashboard> {
        let lock = self.orchestrator.read_model_ref();
        let store = lock.read().await;
        let project = store.projects.get(project_id)?;
        let threads: Vec<ThreadView> = store
            .threads
            .values()
            .filter(|t| t.project_id == project_id)
            .cloned()
            .collect();

        // Collect recent turns across all threads in this project
        let thread_ids: Vec<&str> = threads.iter().map(|t| t.id.as_str()).collect();
        let mut recent_turns: Vec<TurnView> = store
            .turns
            .values()
            .filter(|t| thread_ids.contains(&t.thread_id.as_str()))
            .cloned()
            .collect();
        recent_turns.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        let recent_turns: Vec<TurnView> = recent_turns.into_iter().take(10).collect();

        let total_turns: u32 = threads.iter().map(|t| t.turn_count).sum();

        Some(ProjectDashboard {
            project: project.clone(),
            threads,
            recent_turns,
            total_turns,
        })
    }

    /// Get a thread detail: thread + turns + messages + activities.
    pub async fn get_thread_detail(&self, thread_id: &str) -> Option<ThreadDetail> {
        let lock = self.orchestrator.read_model_ref();
        let store = lock.read().await;
        let thread = store.threads.get(thread_id)?;

        let turns: Vec<TurnView> = store
            .turns
            .values()
            .filter(|t| t.thread_id == thread_id)
            .cloned()
            .collect();

        let turn_ids: Vec<&str> = turns.iter().map(|t| t.id.as_str()).collect();
        let messages: Vec<MessageView> = store
            .messages
            .values()
            .filter(|m| turn_ids.contains(&m.turn_id.as_str()))
            .cloned()
            .collect();

        // activities is a Vec, not a HashMap — use iter()
        let activities: Vec<ActivityView> = store
            .activities
            .iter()
            .filter(|a| {
                a.thread_id
                    .as_deref()
                    .map(|tid| tid == thread_id)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        Some(ThreadDetail {
            thread: thread.clone(),
            turns,
            messages,
            activities,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::test_helpers::InMemoryEventRepo;
    use syncode_core::ports::EventRepository;

    fn make_service() -> ApplicationService {
        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let orchestrator = Orchestrator::new(repo);
        ApplicationService::new(Arc::new(orchestrator))
    }

    #[tokio::test]
    async fn test_create_project_use_case() {
        let svc = make_service();
        let result = svc
            .create_project("My Project".into(), "/tmp/my-project".into())
            .await
            .unwrap();

        assert!(!result.events.is_empty());
        // Verify it's queryable
        let project_id = result.events[0].event.aggregate_id();
        let project = svc.get_project(&project_id.to_string()).await;
        assert!(project.is_some());
        assert_eq!(project.unwrap().name, "My Project");
    }

    #[tokio::test]
    async fn test_delete_project_use_case() {
        let svc = make_service();
        let proj_result = svc
            .create_project("Doomed".into(), "/tmp/doomed".into())
            .await
            .unwrap();
        let project_id = proj_result.events[0].event.aggregate_id();

        // Delete it
        let result = svc.delete_project(project_id).await.unwrap();
        assert_eq!(result.events.len(), 1);
        assert_eq!(result.events[0].event.event_type_name(), "ProjectDeleted");

        // Tombstone removes it from the read model
        assert!(svc.get_project(&project_id.to_string()).await.is_none());
    }

    #[tokio::test]
    async fn test_delete_project_rejects_unknown_project() {
        let svc = make_service();
        let result = svc.delete_project(EntityId::new()).await;
        assert!(matches!(
            result,
            Err(OrchestrationError::ProjectNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_thread_archive_unarchive_lifecycle() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/tmp".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();

        let thread = svc
            .create_thread(pid, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let tid = thread.events[0].event.aggregate_id();

        // Archive
        let archived = svc.archive_thread(tid).await.unwrap();
        assert_eq!(archived.events[0].event.event_type_name(), "ThreadArchived");
        assert_eq!(svc.get_thread(&tid.to_string()).await.unwrap().status, "archived");

        // Unarchive restores to active
        let unarchived = svc.unarchive_thread(tid).await.unwrap();
        assert_eq!(unarchived.events[0].event.event_type_name(), "ThreadUnarchived");
        assert_eq!(svc.get_thread(&tid.to_string()).await.unwrap().status, "active");
    }

    #[tokio::test]
    async fn test_unarchive_non_archived_rejected() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/tmp".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();
        let thread = svc
            .create_thread(pid, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let tid = thread.events[0].event.aggregate_id();

        // Active thread cannot be unarchived (Decider guard).
        let result = svc.unarchive_thread(tid).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_thread_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/tmp".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();
        let thread = svc
            .create_thread(pid, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let tid = thread.events[0].event.aggregate_id();

        let result = svc.delete_thread(tid).await.unwrap();
        assert_eq!(result.events[0].event.event_type_name(), "ThreadDeleted");
        // Tombstone removes it from the read model
        assert!(svc.get_thread(&tid.to_string()).await.is_none());
    }

    #[tokio::test]
    async fn test_stop_thread_session_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/tmp".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();
        let thread = svc
            .create_thread(pid, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let tid = thread.events[0].event.aggregate_id();

        let result = svc.stop_thread_session(tid).await.unwrap();
        assert_eq!(result.events[0].event.event_type_name(), "ThreadSessionStopRequested");
    }

    #[tokio::test]
    async fn test_stop_thread_session_unknown_thread_rejected() {
        let svc = make_service();
        let result = svc.stop_thread_session(EntityId::new()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handoff_create_thread_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/tmp".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();

        // Source thread must exist
        let source = svc
            .create_thread(pid, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let source_id = source.events[0].event.aggregate_id();

        let imported = vec![
            ImportedMessage { source_message_id: EntityId::new(), role: "user".into(), text: "hi".into() },
            ImportedMessage { source_message_id: EntityId::new(), role: "assistant".into(), text: "hello".into() },
        ];
        let result = svc
            .handoff_create_thread(pid, "anthropic".into(), "claude-3".into(), source_id, imported)
            .await
            .unwrap();

        assert_eq!(result.events.len(), 2);
        assert_eq!(result.events[0].event.event_type_name(), "ThreadCreated");
        assert_eq!(result.events[1].event.event_type_name(), "ThreadMessagesImported");
    }

    #[tokio::test]
    async fn test_handoff_create_rejects_unknown_source_thread() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/tmp".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();

        let result = svc
            .handoff_create_thread(pid, "anthropic".into(), "claude-3".into(), EntityId::new(), vec![])
            .await;
        assert!(matches!(result, Err(OrchestrationError::ThreadNotFound(_))));
    }

    #[tokio::test]
    async fn test_fork_create_thread_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/tmp".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();
        let source = svc
            .create_thread(pid, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let source_id = source.events[0].event.aggregate_id();

        let result = svc
            .fork_create_thread(pid, "openai".into(), "gpt-4".into(), source_id, vec![])
            .await
            .unwrap();
        assert_eq!(result.events.len(), 2);
        assert_eq!(result.events[1].event.event_type_name(), "ThreadMessagesImported");
    }

    #[tokio::test]
    async fn test_create_thread_use_case() {
        let svc = make_service();

        // Create project first
        let proj_result = svc
            .create_project("P".into(), "/tmp".into())
            .await
            .unwrap();
        let project_id = proj_result.events[0].event.aggregate_id();

        // Create thread
        let result = svc
            .create_thread(project_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();

        assert!(!result.events.is_empty());
        let thread_id = result.events[0].event.aggregate_id();
        let thread = svc.get_thread(&thread_id.to_string()).await;
        assert!(thread.is_some());
        assert_eq!(thread.unwrap().provider_id, "openai");
    }

    #[tokio::test]
    async fn test_create_thread_rejects_unknown_project() {
        // Orphan-thread guard: a thread cannot be created against a non-existent project.
        let svc = make_service();
        let bogus = EntityId::new();
        let result = svc
            .create_thread(bogus, "openai".into(), "gpt-4".into())
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            OrchestrationError::ProjectNotFound(_)
        ));
    }

    #[tokio::test]
    async fn test_full_turn_lifecycle() {
        let svc = make_service();

        // Project
        let proj = svc.create_project("P".into(), "/tmp".into()).await.unwrap();
        let project_id = proj.events[0].event.aggregate_id();

        // Thread
        let thr = svc
            .create_thread(project_id, "anthropic".into(), "claude-3".into())
            .await
            .unwrap();
        let thread_id = thr.events[0].event.aggregate_id();

        // Start turn
        let start = svc
            .start_turn(thread_id, 1, "Hello AI".into())
            .await
            .unwrap();
        let turn_id = start.events[0].event.aggregate_id();
        // side_effect_triggered depends on whether a CommandReactor is wired
        // (test orchestrator has no reactor, so we just verify events)
        assert!(!start.events.is_empty());

        // Complete turn
        let complete = svc
            .complete_turn(turn_id, "Hello human!".into(), 1200)
            .await
            .unwrap();

        // Verify turn state
        let turn = svc.get_turn(&turn_id.to_string()).await.unwrap();
        assert_eq!(turn.status, "completed");
        assert_eq!(turn.assistant_output.as_deref(), Some("Hello human!"));
    }

    #[tokio::test]
    async fn test_fail_turn_use_case() {
        let svc = make_service();

        let proj = svc.create_project("P".into(), "/tmp".into()).await.unwrap();
        let project_id = proj.events[0].event.aggregate_id();
        let thr = svc
            .create_thread(project_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let thread_id = thr.events[0].event.aggregate_id();
        let start = svc
            .start_turn(thread_id, 1, "test".into())
            .await
            .unwrap();
        let turn_id = start.events[0].event.aggregate_id();

        // Fail the turn
        svc.fail_turn(turn_id, "Provider timeout".into())
            .await
            .unwrap();

        let turn = svc.get_turn(&turn_id.to_string()).await.unwrap();
        assert_eq!(turn.status, "error");
    }

    #[tokio::test]
    async fn test_list_and_filter_queries() {
        let svc = make_service();

        // Create 2 projects
        let p1 = svc.create_project("Alpha".into(), "/a".into()).await.unwrap();
        let p2 = svc.create_project("Beta".into(), "/b".into()).await.unwrap();
        let p1_id = p1.events[0].event.aggregate_id();
        let p2_id = p2.events[0].event.aggregate_id();

        // Create threads in each project
        svc.create_thread(p1_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        svc.create_thread(p1_id, "anthropic".into(), "claude-3".into())
            .await
            .unwrap();
        svc.create_thread(p2_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();

        // List all projects
        let projects = svc.list_projects().await;
        assert_eq!(projects.len(), 2);

        // List threads for project 1 only
        let threads = svc.list_threads(Some(&p1_id.to_string())).await;
        assert_eq!(threads.len(), 2);

        // List threads for project 2 only
        let threads = svc.list_threads(Some(&p2_id.to_string())).await;
        assert_eq!(threads.len(), 1);
    }

    #[tokio::test]
    async fn test_project_dashboard() {
        let svc = make_service();

        let proj = svc.create_project("Dashboard".into(), "/d".into()).await.unwrap();
        let project_id = proj.events[0].event.aggregate_id();
        let thr = svc
            .create_thread(project_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let thread_id = thr.events[0].event.aggregate_id();

        // Start a turn
        svc.start_turn(thread_id, 1, "query".into())
            .await
            .unwrap();

        let dashboard = svc
            .get_project_dashboard(&project_id.to_string())
            .await
            .unwrap();

        assert_eq!(dashboard.project.name, "Dashboard");
        assert_eq!(dashboard.threads.len(), 1);
        assert_eq!(dashboard.total_turns, 1);
        assert!(!dashboard.recent_turns.is_empty());
    }

    #[tokio::test]
    async fn test_thread_detail() {
        let svc = make_service();

        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let project_id = proj.events[0].event.aggregate_id();
        let thr = svc
            .create_thread(project_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let thread_id = thr.events[0].event.aggregate_id();

        // Start and complete a turn
        let start = svc
            .start_turn(thread_id, 1, "hi".into())
            .await
            .unwrap();
        let turn_id = start.events[0].event.aggregate_id();
        svc.complete_turn(turn_id, "response".into(), 500)
            .await
            .unwrap();

        // Add a message
        svc.add_message(turn_id, "assistant".into(), "response".into())
            .await
            .unwrap();

        let detail = svc
            .get_thread_detail(&thread_id.to_string())
            .await
            .unwrap();

        assert_eq!(detail.thread.provider_id, "openai");
        assert_eq!(detail.turns.len(), 1);
        assert_eq!(detail.messages.len(), 1);
    }

    #[tokio::test]
    async fn test_pause_resume_thread_use_case() {
        let svc = make_service();

        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let project_id = proj.events[0].event.aggregate_id();
        let thr = svc
            .create_thread(project_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let thread_id = thr.events[0].event.aggregate_id();

        // Pause
        svc.pause_thread(thread_id).await.unwrap();
        let thread = svc.get_thread(&thread_id.to_string()).await.unwrap();
        assert_eq!(thread.status, "paused");

        // Resume
        svc.resume_thread(thread_id).await.unwrap();
        let thread = svc.get_thread(&thread_id.to_string()).await.unwrap();
        assert_eq!(thread.status, "active");
    }

    #[tokio::test]
    async fn test_set_thread_title_use_case() {
        let svc = make_service();

        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let project_id = proj.events[0].event.aggregate_id();
        let thr = svc
            .create_thread(project_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let thread_id = thr.events[0].event.aggregate_id();

        svc.set_thread_title(thread_id, "My Cool Thread".into())
            .await
            .unwrap();

        let thread = svc.get_thread(&thread_id.to_string()).await.unwrap();
        assert_eq!(thread.title.as_deref(), Some("My Cool Thread"));
    }

    #[tokio::test]
    async fn test_set_thread_runtime_mode_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let project_id = proj.events[0].event.aggregate_id();
        let thr = svc
            .create_thread(project_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let thread_id = thr.events[0].event.aggregate_id();

        // Default runtime mode is "full-access" (mcode DEFAULT_RUNTIME_MODE).
        let thread = svc.get_thread(&thread_id.to_string()).await.unwrap();
        assert_eq!(thread.runtime_mode, "full-access");

        let result = svc
            .set_thread_runtime_mode(thread_id, "approval-required".into())
            .await
            .unwrap();
        assert_eq!(result.events[0].event.event_type_name(), "ThreadRuntimeModeSet");

        let thread = svc.get_thread(&thread_id.to_string()).await.unwrap();
        assert_eq!(thread.runtime_mode, "approval-required");
    }

    #[tokio::test]
    async fn test_set_thread_interaction_mode_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let project_id = proj.events[0].event.aggregate_id();
        let thr = svc
            .create_thread(project_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let thread_id = thr.events[0].event.aggregate_id();

        // Default interaction mode is "default" (mcode DEFAULT_PROVIDER_INTERACTION_MODE).
        let thread = svc.get_thread(&thread_id.to_string()).await.unwrap();
        assert_eq!(thread.interaction_mode, "default");

        let result = svc
            .set_thread_interaction_mode(thread_id, "plan".into())
            .await
            .unwrap();
        assert_eq!(result.events[0].event.event_type_name(), "ThreadInteractionModeSet");

        let thread = svc.get_thread(&thread_id.to_string()).await.unwrap();
        assert_eq!(thread.interaction_mode, "plan");
    }

    #[tokio::test]
    async fn test_set_thread_modes_unknown_thread_rejected() {
        let svc = make_service();
        // No thread exists → decider existence guard rejects both.
        let result = svc
            .set_thread_runtime_mode(EntityId::new(), "approval-required".into())
            .await;
        assert!(result.is_err());

        let result = svc
            .set_thread_interaction_mode(EntityId::new(), "plan".into())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_respond_thread_approval_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();
        let thr = svc.create_thread(pid, "openai".into(), "gpt-4".into()).await.unwrap();
        let tid = thr.events[0].event.aggregate_id();

        let result = svc
            .respond_thread_approval(tid, "req-1".into(), "approved".into())
            .await
            .unwrap();
        assert_eq!(result.events[0].event.event_type_name(), "ThreadApprovalResponded");
    }

    #[tokio::test]
    async fn test_respond_thread_user_input_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();
        let thr = svc.create_thread(pid, "openai".into(), "gpt-4".into()).await.unwrap();
        let tid = thr.events[0].event.aggregate_id();

        let result = svc
            .respond_thread_user_input(tid, "req-2".into(), "yes".into())
            .await
            .unwrap();
        assert_eq!(result.events[0].event.event_type_name(), "ThreadUserInputResponded");
    }

    #[tokio::test]
    async fn test_edit_and_resend_thread_message_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();
        let thr = svc.create_thread(pid, "openai".into(), "gpt-4".into()).await.unwrap();
        let tid = thr.events[0].event.aggregate_id();

        let result = svc
            .edit_and_resend_thread_message(tid, EntityId::new(), "edited".into())
            .await
            .unwrap();
        assert_eq!(result.events[0].event.event_type_name(), "ThreadMessageEditedAndResent");
    }

    #[tokio::test]
    async fn test_append_thread_activity_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();
        let thr = svc.create_thread(pid, "openai".into(), "gpt-4".into()).await.unwrap();
        let tid = thr.events[0].event.aggregate_id();

        // activity.append reuses the existing ActivityLogged event.
        let result = svc
            .append_thread_activity(tid, "checkpoint".into(), "captured".into())
            .await
            .unwrap();
        assert_eq!(result.events[0].event.event_type_name(), "ActivityLogged");
    }

    #[tokio::test]
    async fn test_turn_interaction_commands_unknown_thread_rejected() {
        let svc = make_service();
        // All four guard thread existence at the Decider.
        assert!(svc
            .respond_thread_approval(EntityId::new(), "r".into(), "approved".into())
            .await
            .is_err());
        assert!(svc
            .respond_thread_user_input(EntityId::new(), "r".into(), "a".into())
            .await
            .is_err());
        assert!(svc
            .edit_and_resend_thread_message(EntityId::new(), EntityId::new(), "t".into())
            .await
            .is_err());
        assert!(svc
            .append_thread_activity(EntityId::new(), "t".into(), "d".into())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_pinned_message_lifecycle_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();
        let thr = svc.create_thread(pid, "openai".into(), "gpt-4".into()).await.unwrap();
        let tid = thr.events[0].event.aggregate_id();
        let mid = EntityId::new();

        let r = svc.add_pinned_message(tid, mid).await.unwrap();
        assert_eq!(r.events[0].event.event_type_name(), "PinnedMessageAdded");

        let r = svc.set_pinned_message_done(tid, mid, true).await.unwrap();
        assert_eq!(r.events[0].event.event_type_name(), "PinnedMessageDoneSet");

        let r = svc.set_pinned_message_label(tid, mid, Some("todo".into())).await.unwrap();
        assert_eq!(r.events[0].event.event_type_name(), "PinnedMessageLabelSet");

        let r = svc.remove_pinned_message(tid, mid).await.unwrap();
        assert_eq!(r.events[0].event.event_type_name(), "PinnedMessageRemoved");
    }

    #[tokio::test]
    async fn test_pinned_message_commands_unknown_thread_rejected() {
        let svc = make_service();
        let mid = EntityId::new();
        assert!(svc.add_pinned_message(EntityId::new(), mid).await.is_err());
        assert!(svc.remove_pinned_message(EntityId::new(), mid).await.is_err());
        assert!(svc.set_pinned_message_done(EntityId::new(), mid, true).await.is_err());
        assert!(svc.set_pinned_message_label(EntityId::new(), mid, None).await.is_err());
    }

    #[tokio::test]
    async fn test_marker_lifecycle_use_case() {
        let svc = make_service();
        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let pid = proj.events[0].event.aggregate_id();
        let thr = svc.create_thread(pid, "openai".into(), "gpt-4".into()).await.unwrap();
        let tid = thr.events[0].event.aggregate_id();
        let mid = EntityId::new();
        let msg = EntityId::new();

        let r = svc.add_marker(tid, mid, msg, 0, 5, "hello".into(), "highlight".into(), "yellow".into()).await.unwrap();
        assert_eq!(r.events[0].event.event_type_name(), "MarkerAdded");

        let r = svc.set_marker_done(tid, mid, true).await.unwrap();
        assert_eq!(r.events[0].event.event_type_name(), "MarkerDoneSet");

        let r = svc.set_marker_label(tid, mid, Some("note".into())).await.unwrap();
        assert_eq!(r.events[0].event.event_type_name(), "MarkerLabelSet");

        let r = svc.remove_marker(tid, mid).await.unwrap();
        assert_eq!(r.events[0].event.event_type_name(), "MarkerRemoved");
    }

    #[tokio::test]
    async fn test_marker_commands_unknown_thread_rejected() {
        let svc = make_service();
        let mid = EntityId::new();
        let msg = EntityId::new();
        assert!(svc.add_marker(EntityId::new(), mid, msg, 0, 1, "x".into(), "highlight".into(), "yellow".into()).await.is_err());
        assert!(svc.remove_marker(EntityId::new(), mid).await.is_err());
        assert!(svc.set_marker_done(EntityId::new(), mid, true).await.is_err());
        assert!(svc.set_marker_label(EntityId::new(), mid, None).await.is_err());
    }

    #[tokio::test]
    async fn test_revert_to_checkpoint_use_case() {
        let svc = make_service();

        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let project_id = proj.events[0].event.aggregate_id();
        let thr = svc
            .create_thread(project_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let thread_id = thr.events[0].event.aggregate_id();

        // Revert the thread to a captured checkpoint ref.
        let result = svc
            .revert_to_checkpoint(thread_id, "deadbeef".into())
            .await
            .unwrap();
        assert!(!result.events.is_empty());

        // Projector should have recorded the revert target as the thread's checkpoint.
        let thread = svc.get_thread(&thread_id.to_string()).await.unwrap();
        assert_eq!(thread.git_checkpoint.as_deref(), Some("deadbeef"));
    }

    #[tokio::test]
    async fn test_revert_to_checkpoint_unknown_thread_rejected() {
        let svc = make_service();
        let bogus = EntityId::new();
        let result = svc.revert_to_checkpoint(bogus, "abc".into()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_interrupt_turn_non_running_rejected() {
        // A freshly started turn is "pending" (no provider "running" transition in
        // the pure CQRS flow), so interrupt must be rejected by the decider guard.
        let svc = make_service();

        let proj = svc.create_project("P".into(), "/p".into()).await.unwrap();
        let project_id = proj.events[0].event.aggregate_id();
        let thr = svc
            .create_thread(project_id, "openai".into(), "gpt-4".into())
            .await
            .unwrap();
        let thread_id = thr.events[0].event.aggregate_id();
        let start = svc.start_turn(thread_id, 1, "hi".into()).await.unwrap();
        let turn_id = start.events[0].event.aggregate_id();

        let result = svc.interrupt_turn(turn_id).await;
        assert!(result.is_err(), "interrupting a non-running turn must fail");
    }
}

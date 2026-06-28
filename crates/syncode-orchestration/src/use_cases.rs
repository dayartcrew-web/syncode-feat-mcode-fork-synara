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

use crate::decider::Command;
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

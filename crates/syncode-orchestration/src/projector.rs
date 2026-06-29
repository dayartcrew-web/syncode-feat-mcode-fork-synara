//! Projector — projects domain events into read models
//!
//! The Projector listens to domain events and maintains denormalized
//! read models optimized for queries. It uses an in-memory store
//! for fast access, with optional persistence to SQLite.

use std::collections::HashMap;
use syncode_core::domain::events::DomainEvent;
use crate::read_model::{
    ProjectView, ThreadView, TurnView, MessageView, ActivityView,
};

/// In-memory read model store maintained by the Projector.
/// Thread-safe via interior mutability pattern.
#[derive(Debug, Clone, Default)]
pub struct ReadModelStore {
    pub projects: HashMap<String, ProjectView>,
    pub threads: HashMap<String, ThreadView>,
    pub turns: HashMap<String, TurnView>,
    pub messages: HashMap<String, MessageView>,
    pub activities: Vec<ActivityView>,
}

impl ReadModelStore {
    pub fn new() -> Self {
        Self::default()
    }
}

/// The Projector consumes domain events and updates read models.
pub struct Projector;

impl Projector {
    /// Project a single domain event into the read model store.
    pub fn project(event: &DomainEvent, store: &mut ReadModelStore) {
        match event {
            DomainEvent::ProjectCreated {
                id, name, root_path, created_at,
            } => {
                let view = ProjectView {
                    id: id.as_str(),
                    name: name.clone(),
                    root_path: root_path.clone(),
                    provider_id: None,
                    default_model: None,
                    created_at: created_at.to_string(),
                    updated_at: created_at.to_string(),
                    thread_count: 0,
                };
                store.projects.insert(view.id.clone(), view);
            }

            DomainEvent::ProjectUpdated {
                id, provider_id, default_model, updated_at,
            } => {
                if let Some(project) = store.projects.get_mut(&id.as_str()) {
                    if provider_id.is_some() {
                        project.provider_id = provider_id.clone();
                    }
                    if default_model.is_some() {
                        project.default_model = default_model.clone();
                    }
                    project.updated_at = updated_at.to_string();
                }
            }

            DomainEvent::ProjectDeleted { id, .. } => {
                // Tombstone: drop the project from the read model. Child threads
                // remain in the in-memory store (eventually consistent); the SQLite
                // projection cascades their removal on persistence.
                store.projects.remove(&id.as_str());
            }

            DomainEvent::ThreadCreated {
                id, project_id, provider_id, model, created_at,
            } => {
                let view = ThreadView {
                    id: id.as_str(),
                    project_id: project_id.as_str(),
                    provider_id: provider_id.clone(),
                    model: model.clone(),
                    status: "active".to_string(),
                    title: None,
                    git_checkpoint: None,
                    runtime_mode: "full-access".to_string(),
                    interaction_mode: "default".to_string(),
                    turn_count: 0,
                    created_at: created_at.to_string(),
                    updated_at: created_at.to_string(),
                };
                store.threads.insert(view.id.clone(), view);
                // Increment thread count on parent project
                if let Some(project) = store.projects.get_mut(&project_id.as_str()) {
                    project.thread_count += 1;
                }
            }

            DomainEvent::ThreadStatusChanged {
                id, new_status, updated_at, ..
            } => {
                if let Some(thread) = store.threads.get_mut(&id.as_str()) {
                    thread.status = new_status.clone();
                    thread.updated_at = updated_at.to_string();
                }
            }

            DomainEvent::ThreadTitleSet { id, title, .. } => {
                if let Some(thread) = store.threads.get_mut(&id.as_str()) {
                    thread.title = Some(title.clone());
                }
            }

            DomainEvent::ThreadCheckpointSet { id, git_ref, .. } => {
                if let Some(thread) = store.threads.get_mut(&id.as_str()) {
                    thread.git_checkpoint = Some(git_ref.clone());
                }
            }

            DomainEvent::ThreadReverted { id, git_ref, reverted_at } => {
                // A revert restores the thread to a captured checkpoint: record it as the
                // thread's current checkpoint and bump the updated_at watermark.
                if let Some(thread) = store.threads.get_mut(&id.as_str()) {
                    thread.git_checkpoint = Some(git_ref.clone());
                    thread.updated_at = reverted_at.to_string();
                }
            }

            DomainEvent::ThreadArchived { id, archived_at } => {
                if let Some(thread) = store.threads.get_mut(&id.as_str()) {
                    thread.status = "archived".to_string();
                    thread.updated_at = archived_at.to_string();
                }
            }

            DomainEvent::ThreadUnarchived { id, unarchived_at } => {
                if let Some(thread) = store.threads.get_mut(&id.as_str()) {
                    thread.status = "active".to_string();
                    thread.updated_at = unarchived_at.to_string();
                }
            }

            DomainEvent::ThreadDeleted { id, .. } => {
                // Tombstone: drop the thread from the read model. Child turns remain
                // in-memory (eventually consistent); the SQLite projection cascades.
                store.threads.remove(&id.as_str());
            }

            DomainEvent::ThreadMessagesImported { .. } => {
                // Handoff/fork import is recorded as a durable event (source of truth).
                // Materializing imported message bodies into the queryable read model is
                // deferred — syncode's messages are turn-scoped, unlike mcode's thread-scoped
                // imported messages.
            }

            DomainEvent::ThreadSessionStopRequested { .. } => {
                // Transient stop request; the actual session stop is a reactor side
                // effect (SessionManager). No read-model mutation needed.
            }

            DomainEvent::ThreadRuntimeModeSet { id, runtime_mode, updated_at } => {
                if let Some(thread) = store.threads.get_mut(&id.as_str()) {
                    thread.runtime_mode = runtime_mode.clone();
                    thread.updated_at = updated_at.to_string();
                }
            }

            DomainEvent::ThreadInteractionModeSet { id, interaction_mode, updated_at } => {
                if let Some(thread) = store.threads.get_mut(&id.as_str()) {
                    thread.interaction_mode = interaction_mode.clone();
                    thread.updated_at = updated_at.to_string();
                }
            }

            DomainEvent::ThreadApprovalResponded { .. }
            | DomainEvent::ThreadUserInputResponded { .. }
            | DomainEvent::ThreadMessageEditedAndResent { .. } => {
                // Transient provider-response records; the actual provider dispatch is
                // a reactor side effect (not yet wired). No read-model mutation needed.
            }

            DomainEvent::TurnStarted {
                id, thread_id, sequence, user_input, created_at,
            } => {
                let view = TurnView {
                    id: id.as_str(),
                    thread_id: thread_id.as_str(),
                    sequence: *sequence,
                    user_input: user_input.clone(),
                    assistant_output: None,
                    status: "pending".to_string(),
                    git_checkpoint: None,
                    files_modified: Vec::new(),
                    duration_ms: None,
                    created_at: created_at.to_string(),
                    completed_at: None,
                };
                store.turns.insert(view.id.clone(), view);
                // Increment turn count on parent thread
                if let Some(thread) = store.threads.get_mut(&thread_id.as_str()) {
                    thread.turn_count += 1;
                }
            }

            DomainEvent::TurnCompleted {
                id, assistant_output, duration_ms, completed_at,
            } => {
                if let Some(turn) = store.turns.get_mut(&id.as_str()) {
                    turn.assistant_output = Some(assistant_output.clone());
                    turn.status = "completed".to_string();
                    turn.duration_ms = Some(*duration_ms);
                    turn.completed_at = Some(completed_at.to_string());
                }
            }

            DomainEvent::TurnFailed {
                id, error, completed_at,
            } => {
                if let Some(turn) = store.turns.get_mut(&id.as_str()) {
                    turn.assistant_output = Some(error.clone());
                    turn.status = "error".to_string();
                    turn.completed_at = Some(completed_at.to_string());
                }
            }

            DomainEvent::TurnCancelled { id, completed_at } => {
                if let Some(turn) = store.turns.get_mut(&id.as_str()) {
                    turn.status = "cancelled".to_string();
                    turn.completed_at = Some(completed_at.to_string());
                }
            }

            DomainEvent::TurnInterrupted { id, interrupted_at } => {
                if let Some(turn) = store.turns.get_mut(&id.as_str()) {
                    turn.status = "interrupted".to_string();
                    turn.completed_at = Some(interrupted_at.to_string());
                }
            }

            DomainEvent::TurnFilesModified { id, files } => {
                if let Some(turn) = store.turns.get_mut(&id.as_str()) {
                    turn.files_modified = files.clone();
                }
            }

            DomainEvent::TurnCheckpointSet { id, git_ref, .. } => {
                if let Some(turn) = store.turns.get_mut(&id.as_str()) {
                    turn.git_checkpoint = Some(git_ref.clone());
                }
            }

            DomainEvent::MessageAdded {
                id, turn_id, role, content, created_at,
            } => {
                let view = MessageView {
                    id: id.as_str(),
                    turn_id: turn_id.as_str(),
                    role: role.clone(),
                    content: content.clone(),
                    content_type: "text".to_string(),
                    token_count: None,
                    tool_name: None,
                    tool_call_id: None,
                    created_at: created_at.to_string(),
                };
                store.messages.insert(view.id.clone(), view);
            }

            DomainEvent::ActivityLogged {
                id, activity_type, description, created_at,
            } => {
                let view = ActivityView {
                    id: id.as_str(),
                    activity_type: activity_type.clone(),
                    description: description.clone(),
                    project_id: None,
                    thread_id: None,
                    metadata: serde_json::Value::Object(serde_json::Map::new()),
                    created_at: created_at.to_string(),
                };
                store.activities.push(view);
            }
        }
    }

    /// Project multiple events in order (e.g., during replay)
    pub fn project_many(events: &[DomainEvent], store: &mut ReadModelStore) {
        for event in events {
            Self::project(event, store);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syncode_core::{EntityId, Timestamp};
    use crate::decider::Command;

    /// Helper: create a project via decider + projector
    fn create_project(name: &str, root: &str) -> (EntityId, ReadModelStore) {
        let events = crate::decider::Decider::decide(
            Command::CreateProject {
                name: name.to_string(),
                root_path: root.to_string(),
            },
            None,
        ).unwrap();
        let mut store = ReadModelStore::new();
        Projector::project_many(&events, &mut store);
        let id = events[0].aggregate_id();
        (id, store)
    }

    #[test]
    fn project_created_inserts_view() {
        let (_, store) = create_project("test-project", "/tmp/test");
        assert_eq!(store.projects.len(), 1);
        let project = store.projects.values().next().unwrap();
        assert_eq!(project.name, "test-project");
        assert_eq!(project.thread_count, 0);
    }

    #[test]
    fn project_updated_updates_view() {
        let (id, mut store) = create_project("test", "/test");
        let events = crate::decider::Decider::decide(
            Command::UpdateProjectConfig {
                id,
                provider_id: Some("anthropic".to_string()),
                default_model: Some("claude-3".to_string()),
            },
            Some(&serde_json::json!({"id": id.as_str()})),
        ).unwrap();
        Projector::project_many(&events, &mut store);
        let project = store.projects.get(&id.as_str()).unwrap();
        assert_eq!(project.provider_id.as_deref(), Some("anthropic"));
        assert_eq!(project.default_model.as_deref(), Some("claude-3"));
    }

    #[test]
    fn thread_created_increments_project_thread_count() {
        let (pid, mut store) = create_project("p", "/p");
        let events = crate::decider::Decider::decide(
            Command::CreateThread {
                project_id: pid,
                provider_id: "openai".to_string(),
                model: "gpt-4".to_string(),
            },
            None,
        ).unwrap();
        Projector::project_many(&events, &mut store);
        assert_eq!(store.projects.get(&pid.as_str()).unwrap().thread_count, 1);
        assert_eq!(store.threads.len(), 1);
    }

    #[test]
    fn thread_status_changes_reflected() {
        let (pid, mut store) = create_project("p", "/p");
        let thread_events = crate::decider::Decider::decide(
            Command::CreateThread {
                project_id: pid,
                provider_id: "openai".to_string(),
                model: "gpt-4".to_string(),
            },
            None,
        ).unwrap();
        Projector::project_many(&thread_events, &mut store);
        let tid = thread_events[0].aggregate_id();

        let pause_events = crate::decider::Decider::decide(
            Command::PauseThread { id: tid },
            Some(&serde_json::json!({"status": "active"})),
        ).unwrap();
        Projector::project_many(&pause_events, &mut store);
        let thread = store.threads.get(&tid.as_str()).unwrap();
        assert_eq!(thread.status, "paused");
    }

    #[test]
    fn turn_created_increments_thread_turn_count() {
        let (pid, mut store) = create_project("p", "/p");
        let thread_events = crate::decider::Decider::decide(
            Command::CreateThread {
                project_id: pid,
                provider_id: "openai".to_string(),
                model: "gpt-4".to_string(),
            },
            None,
        ).unwrap();
        Projector::project_many(&thread_events, &mut store);
        let tid = thread_events[0].aggregate_id();

        let turn_events = crate::decider::Decider::decide(
            Command::StartTurn {
                thread_id: tid,
                sequence: 1,
                user_input: "Hello".to_string(),
            },
            None,
        ).unwrap();
        Projector::project_many(&turn_events, &mut store);
        let thread = store.threads.get(&tid.as_str()).unwrap();
        assert_eq!(thread.turn_count, 1);
        assert_eq!(store.turns.len(), 1);
    }

    #[test]
    fn turn_completed_updates_view() {
        let (pid, mut store) = create_project("p", "/p");
        let thread_events = crate::decider::Decider::decide(
            Command::CreateThread {
                project_id: pid,
                provider_id: "openai".to_string(),
                model: "gpt-4".to_string(),
            },
            None,
        ).unwrap();
        Projector::project_many(&thread_events, &mut store);
        let tid = thread_events[0].aggregate_id();

        let turn_events = crate::decider::Decider::decide(
            Command::StartTurn {
                thread_id: tid,
                sequence: 1,
                user_input: "Hi".to_string(),
            },
            None,
        ).unwrap();
        Projector::project_many(&turn_events, &mut store);
        let turn_id = turn_events[0].aggregate_id();

        let complete_events = crate::decider::Decider::decide(
            Command::CompleteTurn {
                id: turn_id,
                assistant_output: "Response!".to_string(),
                duration_ms: 500,
            },
            Some(&serde_json::json!({"status": "running"})),
        ).unwrap();
        Projector::project_many(&complete_events, &mut store);

        let turn = store.turns.get(&turn_id.as_str()).unwrap();
        assert_eq!(turn.status, "completed");
        assert_eq!(turn.assistant_output.as_deref(), Some("Response!"));
        assert_eq!(turn.duration_ms, Some(500));
    }

    #[test]
    fn message_added_inserts_view() {
        let turn_id = EntityId::new();
        let events = crate::decider::Decider::decide(
            Command::AddMessage {
                turn_id,
                role: "user".to_string(),
                content: "Hello".to_string(),
            },
            None,
        ).unwrap();
        let mut store = ReadModelStore::new();
        Projector::project_many(&events, &mut store);
        assert_eq!(store.messages.len(), 1);
        let msg = store.messages.values().next().unwrap();
        assert_eq!(msg.role, "user");
        assert_eq!(msg.content, "Hello");
    }

    #[test]
    fn activity_logged_inserts_view() {
        let event = DomainEvent::ActivityLogged {
            id: EntityId::new(),
            activity_type: "session_started".to_string(),
            description: "User started session".to_string(),
            created_at: Timestamp::now(),
        };
        let mut store = ReadModelStore::new();
        Projector::project(&event, &mut store);
        assert_eq!(store.activities.len(), 1);
        assert_eq!(store.activities[0].activity_type, "session_started");
    }

    #[test]
    fn full_workflow_project_thread_turn_message() {
        let mut store = ReadModelStore::new();

        // 1. Create project
        let proj_events = crate::decider::Decider::decide(
            Command::CreateProject {
                name: "Full Test".to_string(),
                root_path: "/tmp/full".to_string(),
            },
            None,
        ).unwrap();
        Projector::project_many(&proj_events, &mut store);
        let pid = proj_events[0].aggregate_id();

        // 2. Create thread
        let thread_events = crate::decider::Decider::decide(
            Command::CreateThread {
                project_id: pid,
                provider_id: "anthropic".to_string(),
                model: "claude-3".to_string(),
            },
            None,
        ).unwrap();
        Projector::project_many(&thread_events, &mut store);
        let tid = thread_events[0].aggregate_id();

        // 3. Start turn
        let turn_events = crate::decider::Decider::decide(
            Command::StartTurn {
                thread_id: tid,
                sequence: 1,
                user_input: "Write tests".to_string(),
            },
            None,
        ).unwrap();
        Projector::project_many(&turn_events, &mut store);
        let turn_id = turn_events[0].aggregate_id();

        // 4. Complete turn
        let complete_events = crate::decider::Decider::decide(
            Command::CompleteTurn {
                id: turn_id,
                assistant_output: "Done!".to_string(),
                duration_ms: 2000,
            },
            Some(&serde_json::json!({"status": "running"})),
        ).unwrap();
        Projector::project_many(&complete_events, &mut store);

        // 5. Add messages
        let user_msg = crate::decider::Decider::decide(
            Command::AddMessage {
                turn_id,
                role: "user".to_string(),
                content: "Write tests".to_string(),
            },
            None,
        ).unwrap();
        let assistant_msg = crate::decider::Decider::decide(
            Command::AddMessage {
                turn_id,
                role: "assistant".to_string(),
                content: "Done!".to_string(),
            },
            None,
        ).unwrap();
        Projector::project_many(&user_msg, &mut store);
        Projector::project_many(&assistant_msg, &mut store);

        // Verify end state
        assert_eq!(store.projects.len(), 1);
        assert_eq!(store.threads.len(), 1);
        assert_eq!(store.turns.len(), 1);
        assert_eq!(store.messages.len(), 2);

        let project = store.projects.get(&pid.as_str()).unwrap();
        assert_eq!(project.thread_count, 1);

        let thread = store.threads.get(&tid.as_str()).unwrap();
        assert_eq!(thread.turn_count, 1);

        let turn = store.turns.get(&turn_id.as_str()).unwrap();
        assert_eq!(turn.status, "completed");
        assert_eq!(turn.duration_ms, Some(2000));
    }
}

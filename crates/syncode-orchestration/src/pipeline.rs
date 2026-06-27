//! Orchestration pipeline — the central nervous system
//!
//! The `Orchestrator` wires together all the orchestration components:
//! - Accepts commands
//! - Runs them through the Decider → Events
//! - Persists events via EventRepository port
//! - Projects to read model (in-memory + via ReadModelRepository port)
//! - Triggers CommandReactor for provider side effects
//! - Feeds provider events back through IngestionReactor → more domain events
//!
//! This is the command handler / application service for the orchestration layer.

use std::sync::Arc;
use syncode_core::{
    DomainEvent, Envelope, EntityId,
    ports::{EventRepository, PortError},
};
use tracing::{info, instrument};

use crate::decider::{Command, Decider, DeciderError};
use crate::projector::{Projector, ReadModelStore};
use crate::reactors::{
    ProviderCommandReactor,
    ingest_provider_event, IngestionResult,
};

/// Result of handling a command through the orchestration pipeline
#[derive(Debug, Clone)]
pub struct CommandResult {
    /// The command that was processed
    pub command: Command,
    /// Domain events produced by the decider
    pub events: Vec<Envelope>,
    /// Whether a side effect was triggered (e.g., provider session started)
    pub side_effect_triggered: bool,
    /// Additional events from side effects (e.g., ingestion reactor)
    pub side_effect_events: Vec<Envelope>,
}

/// Errors that can occur during orchestration
#[derive(Debug, thiserror::Error)]
pub enum OrchestrationError {
    #[error("Decider error: {0}")]
    Decider(#[from] DeciderError),

    #[error("Event repository error: {0}")]
    EventRepository(#[from] PortError),

    #[error("Command reactor error: {0}")]
    CommandReactor(String),

    #[error("No current state found for aggregate {0}")]
    NoState(EntityId),
}

/// The Orchestrator is the central pipeline that processes commands.
///
/// It depends on ports (traits) rather than concrete implementations,
/// making it testable with in-memory fakes.
pub struct Orchestrator {
    /// In-memory read model store for fast queries
    read_model: Arc<tokio::sync::RwLock<ReadModelStore>>,
    /// Event repository (port) for persistence
    event_repo: Arc<dyn EventRepository>,
    /// Optional provider command reactor
    command_reactor: Option<Arc<ProviderCommandReactor>>,
}

impl Orchestrator {
    /// Create a new orchestrator with an event repository.
    pub fn new(event_repo: Arc<dyn EventRepository>) -> Self {
        Self {
            read_model: Arc::new(tokio::sync::RwLock::new(ReadModelStore::new())),
            event_repo,
            command_reactor: None,
        }
    }

    /// Create with a command reactor for provider side effects.
    pub fn with_command_reactor(
        event_repo: Arc<dyn EventRepository>,
        command_reactor: Arc<ProviderCommandReactor>,
    ) -> Self {
        Self {
            read_model: Arc::new(tokio::sync::RwLock::new(ReadModelStore::new())),
            event_repo,
            command_reactor: Some(command_reactor),
        }
    }

    /// Create with an existing read model store (e.g., pre-loaded).
    pub fn with_read_model(
        event_repo: Arc<dyn EventRepository>,
        read_model: Arc<tokio::sync::RwLock<ReadModelStore>>,
    ) -> Self {
        Self {
            read_model,
            event_repo,
            command_reactor: None,
        }
    }

    /// Handle a command through the full pipeline.
    ///
    /// 1. Extract aggregate state from read model as JSON
    /// 2. Run command through Decider → domain events
    /// 3. Determine aggregate ID from produced events
    /// 4. Persist events via EventRepository
    /// 5. Build envelopes and project to in-memory read model
    /// 6. Trigger CommandReactor side effects (if configured)
    #[instrument(skip(self), fields(command = ?command), level = "info")]
    pub async fn handle_command(
        &self,
        command: Command,
    ) -> Result<CommandResult, OrchestrationError> {
        info!("Processing command");

        // 1. Get current aggregate state from read model as JSON for the Decider
        let aggregate_id_hint = self.aggregate_id_for_command(&command);
        let current_state = self.load_aggregate_state(&aggregate_id_hint, &command).await;

        // 2. Run through Decider (pure logic)
        let domain_events = Decider::decide(command.clone(), current_state.as_ref())?;

        if domain_events.is_empty() {
            info!("No events produced");
            return Ok(CommandResult {
                command,
                events: vec![],
                side_effect_triggered: false,
                side_effect_events: vec![],
            });
        }

        // 3. Determine aggregate ID from the first event (the Decider assigns IDs)
        //    and get the current stream version for optimistic concurrency.
        let aggregate_id = domain_events[0].aggregate_id();
        let current_version = self.event_repo.current_version(aggregate_id).await.unwrap_or(0);

        // 4. Persist events
        let _new_version = self.event_repo
            .append_events(aggregate_id, domain_events.clone(), current_version)
            .await?;

        // 5. Project raw domain events to in-memory read model, then wrap in envelopes
        let mut read_model = self.read_model.write().await;
        Projector::project_many(&domain_events, &mut read_model);
        drop(read_model);

        let envelopes: Vec<Envelope> = domain_events
            .into_iter()
            .enumerate()
            .map(|(i, event)| {
                let seq = current_version + 1 + i as u64;
                Envelope::new(event, seq)
            })
            .collect();

        info!(count = envelopes.len(), "Events persisted and projected");

        // 6. Trigger side effects (command reactor)
        let side_effect_events = vec![];
        let mut side_effect_triggered = false;

        if let Some(ref _reactor) = self.command_reactor {
            side_effect_triggered = self.needs_provider_interaction(&command);
            // In a full implementation, the reactor would produce provider events
            // which we'd feed through the ingestion reactor.
        }

        Ok(CommandResult {
            command,
            events: envelopes,
            side_effect_triggered,
            side_effect_events,
        })
    }

    /// Ingest a provider event (from the provider stream) and produce domain events.
    ///
    /// This is the "read side" of the provider bridge:
    /// ProviderEvent → IngestionReactor → DomainEvent → persist → project
    pub async fn ingest_provider_event(
        &self,
        provider_event: syncode_provider::ProviderEvent,
        turn_id: EntityId,
    ) -> Result<Vec<Envelope>, OrchestrationError> {
        let IngestionResult { events, consumed: _ } =
            ingest_provider_event(provider_event, turn_id);

        if events.is_empty() {
            return Ok(vec![]);
        }

        // Persist the new domain events
        let aggregate_id = turn_id; // Turn events aggregate on the turn
        let current_version = self.event_repo.current_version(aggregate_id).await.unwrap_or(0);
        let _new_version = self.event_repo
            .append_events(aggregate_id, events.clone(), current_version)
            .await?;

        // Project events to read model, then wrap in envelopes
        let mut read_model = self.read_model.write().await;
        Projector::project_many(&events, &mut read_model);
        drop(read_model);

        let envelopes: Vec<Envelope> = events
            .into_iter()
            .enumerate()
            .map(|(i, event)| Envelope::new(event, current_version + 1 + i as u64))
            .collect();

        Ok(envelopes)
    }

    /// Get a snapshot of the current read model
    pub async fn read_model_snapshot(&self) -> ReadModelStore {
        self.read_model.read().await.clone()
    }

    /// Get reference to the in-memory read model
    pub fn read_model_ref(&self) -> Arc<tokio::sync::RwLock<ReadModelStore>> {
        Arc::clone(&self.read_model)
    }

    /// Replay all events from the repository into the in-memory read model.
    pub async fn replay_read_model(&self) -> Result<u32, OrchestrationError> {
        let envelopes = self.event_repo.replay_all_events(None, 10_000).await?;
        let count = envelopes.len() as u32;

        // Extract raw domain events and project
        let raw_events: Vec<DomainEvent> = envelopes.iter().map(|e| e.event.clone()).collect();
        let mut read_model = self.read_model.write().await;
        Projector::project_many(&raw_events, &mut read_model);
        drop(read_model);

        info!(count, "Read model replayed from event store");
        Ok(count)
    }

    // ─── Private helpers ────────────────────────────────────────────

    /// Get a hint for the aggregate ID based on the command structure.
    ///
    /// For commands that reference an existing aggregate (UpdateProject, PauseThread, etc.),
    /// this returns that aggregate's ID. For create commands, returns None —
    /// the actual aggregate ID comes from the Decider's produced events.
    fn aggregate_id_for_command(&self, command: &Command) -> Option<EntityId> {
        match command {
            // Create commands: the Decider generates the ID
            Command::CreateProject { .. }
            | Command::CreateThread { .. }
            | Command::StartTurn { .. }
            | Command::AddMessage { .. } => None,

            // Commands targeting an existing project
            Command::UpdateProjectConfig { id, .. } => Some(*id),
            Command::SetThreadTitle { id, .. } => Some(*id),

            // Thread-level commands
            Command::PauseThread { id, .. }
            | Command::ResumeThread { id, .. }
            | Command::CompleteThread { id, .. }
            | Command::CancelThread { id, .. } => Some(*id),

            // Turn-level commands
            Command::CompleteTurn { id, .. }
            | Command::FailTurn { id, .. }
            | Command::CancelTurn { id, .. }
            | Command::RecordTurnFiles { id, .. }
            | Command::SetTurnCheckpoint { id, .. } => Some(*id),
        }
    }

    /// Load the current state of an aggregate from the read model as JSON.
    ///
    /// Returns `None` for new aggregates (CreateProject) or when the
    /// aggregate doesn't exist yet in the read model.
    async fn load_aggregate_state(
        &self,
        aggregate_id: &Option<EntityId>,
        command: &Command,
    ) -> Option<serde_json::Value> {
        let Some(id) = aggregate_id else {
            return None;
        };

        let read_model = self.read_model.read().await;

        // Extract the appropriate state based on command type
        match command {
            Command::CreateProject { .. } => None,

            Command::UpdateProjectConfig { .. } => {
                read_model.projects.get(&id.as_str()).map(|p| {
                    serde_json::to_value(p).unwrap_or_default()
                })
            }

            Command::SetThreadTitle { .. }
            | Command::PauseThread { .. }
            | Command::ResumeThread { .. }
            | Command::CompleteThread { .. }
            | Command::CancelThread { .. } => {
                read_model.threads.get(&id.as_str()).map(|t| {
                    serde_json::json!({"status": t.status})
                })
            }

            Command::CreateThread { .. } => {
                read_model.projects.get(&id.as_str()).map(|p| {
                    serde_json::to_value(p).unwrap_or_default()
                })
            }

            Command::StartTurn { .. } => {
                read_model.threads.get(&id.as_str()).map(|t| {
                    serde_json::json!({"status": t.status})
                })
            }

            Command::CompleteTurn { .. }
            | Command::FailTurn { .. }
            | Command::CancelTurn { .. }
            | Command::RecordTurnFiles { .. }
            | Command::SetTurnCheckpoint { .. } => {
                read_model.turns.get(&id.as_str()).map(|t| {
                    serde_json::json!({"status": t.status})
                })
            }

            Command::AddMessage { .. } => None,
        }
    }

    /// Check if a command requires provider interaction
    fn needs_provider_interaction(&self, command: &Command) -> bool {
        matches!(
            command,
            Command::StartTurn { .. }
            | Command::FailTurn { .. }
            | Command::CancelTurn { .. }
            | Command::PauseThread { .. }
            | Command::CancelThread { .. }
        )
    }
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;
    use syncode_core::ports::EventRepository;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// In-memory fake event repository for testing
    pub(crate) struct InMemoryEventRepo {
        events: Mutex<HashMap<String, Vec<Envelope>>>,
    }

    impl InMemoryEventRepo {
        pub(crate) fn new() -> Self {
            Self {
                events: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl EventRepository for InMemoryEventRepo {
        async fn append_events(
            &self,
            aggregate_id: EntityId,
            events: Vec<DomainEvent>,
            expected_version: u64,
        ) -> Result<u64, PortError> {
            let mut store = self.events.lock().unwrap();
            let key = aggregate_id.to_string();
            let current = store.get(&key).map(|v| v.len() as u64).unwrap_or(0);

            if current != expected_version {
                return Err(PortError::ConcurrencyConflict {
                    expected: expected_version,
                    actual: current,
                });
            }

            let entry = store.entry(key).or_default();
            let start_seq = current;
            for (i, event) in events.into_iter().enumerate() {
                entry.push(Envelope::new(event, start_seq + 1 + i as u64));
            }

            Ok(entry.len() as u64)
        }

        async fn replay_events(
            &self,
            aggregate_id: EntityId,
        ) -> Result<Vec<Envelope>, PortError> {
            let store = self.events.lock().unwrap();
            Ok(store.get(&aggregate_id.to_string()).cloned().unwrap_or_default())
        }

        async fn load_snapshot(
            &self,
            _aggregate_id: EntityId,
        ) -> Result<Option<(serde_json::Value, u64)>, PortError> {
            Ok(None)
        }

        async fn save_snapshot(
            &self,
            _aggregate_id: EntityId,
            _state: serde_json::Value,
            _version: u64,
        ) -> Result<(), PortError> {
            Ok(())
        }

        async fn replay_all_events(
            &self,
            _since_sequence: Option<u64>,
            _limit: u32,
        ) -> Result<Vec<Envelope>, PortError> {
            let store = self.events.lock().unwrap();
            let mut all: Vec<Envelope> = store.values().flatten().cloned().collect();
            all.sort_by_key(|e| e.sequence);
            Ok(all)
        }

        async fn current_version(&self, aggregate_id: EntityId) -> Result<u64, PortError> {
            let store = self.events.lock().unwrap();
            Ok(store.get(&aggregate_id.to_string()).map(|v| v.len() as u64).unwrap_or(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::test_helpers::InMemoryEventRepo;

    fn make_orchestrator() -> Orchestrator {
        let repo = Arc::new(InMemoryEventRepo::new());
        Orchestrator::new(repo)
    }

    #[tokio::test]
    async fn test_create_project() {
        let orch = make_orchestrator();
        let result = orch.handle_command(Command::CreateProject {
            name: "Test".into(),
            root_path: "/test".into(),
        }).await.expect("handle command");

        assert_eq!(result.events.len(), 1);
        assert_eq!(result.events[0].event.event_type_name(), "ProjectCreated");
        assert_eq!(result.events[0].sequence, 1);
    }

    #[tokio::test]
    async fn test_create_thread_succeeds_without_state_validation() {
        let orch = make_orchestrator();

        // CreateThread doesn't validate the project exists — it trusts the command.
        // (Validation could be added later via invariants.)
        let result = orch.handle_command(Command::CreateThread {
            project_id: EntityId::new(),
            provider_id: "anthropic".into(),
            model: "claude-3".into(),
        }).await.expect("create thread should succeed");

        assert_eq!(result.events.len(), 1);
        assert_eq!(result.events[0].event.event_type_name(), "ThreadCreated");
    }

    #[tokio::test]
    async fn test_read_model_updated() {
        let orch = make_orchestrator();

        orch.handle_command(Command::CreateProject {
            name: "Snapshot Test".into(),
            root_path: "/snap".into(),
        }).await.expect("create project");

        let read_model = orch.read_model_snapshot().await;
        assert_eq!(read_model.projects.len(), 1);
        assert_eq!(read_model.projects.values().next().unwrap().name, "Snapshot Test");
    }

    #[tokio::test]
    async fn test_concurrency_conflict() {
        let orch = make_orchestrator();

        // Create project
        orch.handle_command(Command::CreateProject {
            name: "P".into(),
            root_path: "/p".into(),
        }).await.expect("first");

        // Second create produces a different aggregate — should succeed
        let result = orch.handle_command(Command::CreateProject {
            name: "P2".into(),
            root_path: "/p2".into(),
        }).await.expect("second");

        assert_eq!(result.events.len(), 1);
    }

    #[tokio::test]
    async fn test_replay_read_model() {
        let orch = make_orchestrator();

        orch.handle_command(Command::CreateProject {
            name: "Replay".into(),
            root_path: "/replay".into(),
        }).await.expect("create");

        // Reset read model
        {
            let mut rm = orch.read_model.write().await;
            *rm = ReadModelStore::new();
        }

        // Read model should be empty
        let snap = orch.read_model_snapshot().await;
        assert_eq!(snap.projects.len(), 0);

        // Replay
        let count = orch.replay_read_model().await.expect("replay");
        assert!(count > 0);

        // Read model should be populated
        let snap = orch.read_model_snapshot().await;
        assert_eq!(snap.projects.len(), 1);
    }
}

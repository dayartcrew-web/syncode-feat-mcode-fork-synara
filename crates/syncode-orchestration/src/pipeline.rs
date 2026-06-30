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

    #[error("Project not found: {0}")]
    ProjectNotFound(EntityId),

    #[error("Thread not found: {0}")]
    ThreadNotFound(EntityId),
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
    /// Optional provider adapter. The reactor alone is inert — a provider
    /// adapter must also be wired for provider-interaction commands to
    /// actually dispatch (e.g. start a session on StartTurn, respond to an
    /// approval/user-input request).
    adapter: Option<syncode_provider::registry::SharedAdapter>,
}

impl Orchestrator {
    /// Create a new orchestrator with an event repository.
    pub fn new(event_repo: Arc<dyn EventRepository>) -> Self {
        Self {
            read_model: Arc::new(tokio::sync::RwLock::new(ReadModelStore::new())),
            event_repo,
            command_reactor: None,
            adapter: None,
        }
    }

    /// Create with a command reactor for provider side effects.
    ///
    /// Note: without an adapter (see [`Self::with_reactor_and_adapter`]) the
    /// reactor is present but cannot dispatch to any provider — provider
    /// side effects stay inert.
    pub fn with_command_reactor(
        event_repo: Arc<dyn EventRepository>,
        command_reactor: Arc<ProviderCommandReactor>,
    ) -> Self {
        Self {
            read_model: Arc::new(tokio::sync::RwLock::new(ReadModelStore::new())),
            event_repo,
            command_reactor: Some(command_reactor),
            adapter: None,
        }
    }

    /// Create with a command reactor AND a provider adapter, fully arming the
    /// pipeline's side effects. Provider-interaction commands (StartTurn,
    /// RespondThreadApproval, EditAndResendThreadMessage, StopThreadSession, …)
    /// dispatch through the reactor to the adapter — starting/stopping provider
    /// sessions and responding to approval/user-input requests.
    pub fn with_reactor_and_adapter(
        event_repo: Arc<dyn EventRepository>,
        command_reactor: Arc<ProviderCommandReactor>,
        adapter: syncode_provider::registry::SharedAdapter,
    ) -> Self {
        Self {
            read_model: Arc::new(tokio::sync::RwLock::new(ReadModelStore::new())),
            event_repo,
            command_reactor: Some(command_reactor),
            adapter: Some(adapter),
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
            adapter: None,
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

        // Cross-aggregate invariant: CreateThread requires its parent project to
        // exist. Handoff/Fork enforce this at the application layer, but
        // CreateThread is reachable directly through the orchestrator (WS-RPC),
        // so the engine guards it here — before the pure Decider runs.
        if let Command::CreateThread { project_id, .. } = &command {
            if current_state.is_none() {
                return Err(OrchestrationError::ProjectNotFound(*project_id));
            }
        }

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

        // 6. Trigger side effects (command reactor). When both a reactor and an
        //    adapter are wired, provider-interaction commands actually dispatch
        //    to the provider (start a session on StartTurn, respond to an
        //    approval/user-input request, stop a thread's session, …). `handled`
        //    reflects whether a provider side effect took effect. A reactor
        //    without an adapter (or no provider-interaction command) stays inert.
        let mut side_effect_events: Vec<Envelope> = Vec::new();
        let mut side_effect_triggered = false;

        if let (Some(reactor), Some(adapter)) = (&self.command_reactor, &self.adapter) {
            if self.needs_provider_interaction(&command) {
                // For StartTurn the reactor needs the freshly-assigned turn id
                // (it registers the provider session against it). Derive it from
                // the produced TurnStarted event; other commands ignore the hint.
                let turn_id_hint = envelopes.iter().find_map(|env| {
                    if let DomainEvent::TurnStarted { id, .. } = &env.event {
                        Some(*id)
                    } else {
                        None
                    }
                });
                let reaction = reactor
                    .react(&command, adapter, turn_id_hint)
                    .await
                    .map_err(|e| OrchestrationError::CommandReactor(e.to_string()))?;
                side_effect_triggered = reaction.handled;

                // Reverse bridge: feed any provider events the reactor collected
                // back through the ingestion reactor (ProviderEvent -> DomainEvent
                // -> append + project), correlated to the turn via turn_id_hint.
                // Only StartTurn yields a hint today; other provider-interaction
                // commands collect no events yet, so without a hint there is
                // nothing to ingest (the events would be uncorrelated to a turn).
                if let Some(turn_id) = turn_id_hint {
                    // Capture the session id (if the reactor created one) before
                    // moving reaction.events into the batch ingest below.
                    let session_id = reaction.session_id.clone();
                    side_effect_events = self
                        .ingest_provider_events_batch(reaction.events, turn_id)
                        .await?;

                    // Live bridge: for StartTurn the reactor created a provider
                    // session. Spawn a detached consumer that forwards that
                    // session's provider event stream back into the pipeline
                    // (append + project) under the turn. Streaming output (tokens,
                    // tool calls, completion) arrives here, not via react().
                    if let Some(sid) = session_id
                        && let Err(e) = self
                            .spawn_provider_stream_consumer(sid, turn_id)
                            .await
                    {
                        tracing::warn!(error = %e, "failed to spawn provider stream consumer");
                    }
                }
            }
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
        // Scope provider-originated activities to the turn's owning thread.
        let thread_id = thread_id_for_turn(&self.read_model, turn_id).await;
        let IngestionResult { events, consumed: _ } =
            ingest_provider_event(provider_event, turn_id, thread_id);
        // Turn events aggregate on the turn; append + project is shared with the
        // live stream consumer via the module-level `append_and_project` helper.
        append_and_project(&self.event_repo, &self.read_model, turn_id, events).await
    }

    /// Ingest a batch of provider events collected by the command reactor and
    /// produce domain-event envelopes. This closes the reverse direction of the
    /// provider bridge: a provider-interaction command may collect provider
    /// events (e.g. a `Completed` event from a synchronous response), which we
    /// turn back into domain events and append + project just like stream-sourced
    /// events. All events are correlated to the given `turn_id`.
    ///
    /// Returns the resulting envelopes (may be empty — `Started`/`Token`/
    /// `StatusChanged` produce no domain event).
    pub async fn ingest_provider_events_batch(
        &self,
        provider_events: Vec<syncode_provider::ProviderEvent>,
        turn_id: EntityId,
    ) -> Result<Vec<Envelope>, OrchestrationError> {
        let mut out = Vec::with_capacity(provider_events.len());
        for event in provider_events {
            let envelopes = self.ingest_provider_event(event, turn_id).await?;
            out.extend(envelopes);
        }
        Ok(out)
    }

    /// Spawn a detached background task that consumes a provider session's event
    /// stream and ingests each event into the pipeline (append + project),
    /// correlated to the session's turn. This is the live half of the provider
    /// bridge — the synchronous `react()` path only handles request/response;
    /// streaming output (tokens, tool calls, completion) arrives here and is fed
    /// back as domain events.
    ///
    /// The task runs until the stream ends or errors, then self-terminates.
    /// Requires an adapter to be wired (`OrchestrationError` otherwise). The
    /// returned `JoinHandle` is detached by the pipeline; tests may await it.
    pub async fn spawn_provider_stream_consumer(
        &self,
        session_id: String,
        turn_id: EntityId,
    ) -> Result<tokio::task::JoinHandle<()>, OrchestrationError> {
        let adapter = self.adapter.clone().ok_or_else(|| {
            OrchestrationError::CommandReactor(
                "no provider adapter wired for stream consumption".to_string(),
            )
        })?;

        let stream = {
            let guard = adapter.read().await;
            guard
                .event_stream(&session_id)
                .map_err(|e| OrchestrationError::CommandReactor(format!("event_stream: {e}")))?
        };

        let event_repo = Arc::clone(&self.event_repo);
        let read_model = Arc::clone(&self.read_model);

        Ok(tokio::spawn(async move {
            consume_provider_stream(stream, event_repo, read_model, turn_id, session_id).await;
        }))
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
            // CreateThread references its parent project — return the project id
            // so handle_command can load the project and enforce the cross-aggregate
            // existence guard. The new thread's own id remains event-derived
            // (`domain_events[0].aggregate_id()`), so persistence is unaffected.
            Command::CreateThread { project_id, .. } => Some(*project_id),

            // Create commands: the Decider generates the ID
            Command::CreateProject { .. }
            | Command::HandoffCreateThread { .. }
            | Command::ForkCreateThread { .. }
            | Command::StartTurn { .. }
            | Command::AddMessage { .. } => None,

            // Commands targeting an existing project
            Command::UpdateProjectConfig { id, .. }
            | Command::DeleteProject { id, .. } => Some(*id),
            Command::SetThreadTitle { id, .. } => Some(*id),

            // Thread-level commands
            Command::PauseThread { id, .. }
            | Command::ResumeThread { id, .. }
            | Command::CompleteThread { id, .. }
            | Command::CancelThread { id, .. }
            | Command::ArchiveThread { id, .. }
            | Command::UnarchiveThread { id, .. }
            | Command::DeleteThread { id, .. }
            | Command::StopThreadSession { id, .. }
            | Command::SetThreadRuntimeMode { id, .. }
            | Command::SetThreadInteractionMode { id, .. }
            | Command::RespondThreadApproval { id, .. }
            | Command::RespondThreadUserInput { id, .. }
            | Command::EditAndResendThreadMessage { id, .. }
            | Command::AppendThreadActivity { id, .. }
            | Command::AddPinnedMessage { id, .. }
            | Command::RemovePinnedMessage { id, .. }
            | Command::SetPinnedMessageDone { id, .. }
            | Command::SetPinnedMessageLabel { id, .. }
            | Command::AddMarker { id, .. }
            | Command::RemoveMarker { id, .. }
            | Command::SetMarkerDone { id, .. }
            | Command::SetMarkerLabel { id, .. } => Some(*id),

            // Revert targets the thread's checkpoint stream
            Command::RevertToCheckpoint { thread_id, .. } => Some(*thread_id),

            // Turn-level commands
            Command::CompleteTurn { id, .. }
            | Command::FailTurn { id, .. }
            | Command::CancelTurn { id, .. }
            | Command::InterruptTurn { id, .. }
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

            Command::UpdateProjectConfig { .. }
            | Command::DeleteProject { .. } => {
                read_model.projects.get(&id.as_str()).map(|p| {
                    serde_json::to_value(p).unwrap_or_default()
                })
            }

            Command::SetThreadTitle { .. }
            | Command::PauseThread { .. }
            | Command::ResumeThread { .. }
            | Command::CompleteThread { .. }
            | Command::CancelThread { .. }
            | Command::RevertToCheckpoint { .. }
            | Command::ArchiveThread { .. }
            | Command::UnarchiveThread { .. }
            | Command::DeleteThread { .. }
            | Command::StopThreadSession { .. }
            | Command::SetThreadRuntimeMode { .. }
            | Command::SetThreadInteractionMode { .. }
            | Command::RespondThreadApproval { .. }
            | Command::RespondThreadUserInput { .. }
            | Command::EditAndResendThreadMessage { .. }
            | Command::AppendThreadActivity { .. }
            | Command::AddPinnedMessage { .. }
            | Command::RemovePinnedMessage { .. }
            | Command::SetPinnedMessageDone { .. }
            | Command::SetPinnedMessageLabel { .. }
            | Command::AddMarker { .. }
            | Command::RemoveMarker { .. }
            | Command::SetMarkerDone { .. }
            | Command::SetMarkerLabel { .. } => {
                read_model.threads.get(&id.as_str()).map(|t| {
                    // Enrich with the thread's current pinned-message and marker id
                    // sets so the Decider can enforce count caps + existence checks.
                    let tid = id.as_str();
                    let pinned_message_ids: Vec<&str> = read_model
                        .pinned_messages
                        .values()
                        .filter(|p| p.thread_id == tid)
                        .map(|p| p.message_id.as_str())
                        .collect();
                    let marker_ids: Vec<&str> = read_model
                        .markers
                        .values()
                        .filter(|m| m.thread_id == tid)
                        .map(|m| m.marker_id.as_str())
                        .collect();
                    serde_json::json!({
                        "status": t.status,
                        "pinned_message_ids": pinned_message_ids,
                        "marker_ids": marker_ids,
                    })
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
            | Command::InterruptTurn { .. }
            | Command::RecordTurnFiles { .. }
            | Command::SetTurnCheckpoint { .. } => {
                read_model.turns.get(&id.as_str()).map(|t| {
                    serde_json::json!({"status": t.status})
                })
            }

            Command::AddMessage { .. } => None,

            // Thread-creation-by-import: the Decider trusts the command (project
            // and source-thread existence are enforced at the application layer).
            Command::HandoffCreateThread { .. }
            | Command::ForkCreateThread { .. } => None,
        }
    }

    /// Check if a command requires provider interaction
    fn needs_provider_interaction(&self, command: &Command) -> bool {
        matches!(
            command,
            Command::StartTurn { .. }
            | Command::FailTurn { .. }
            | Command::CancelTurn { .. }
            | Command::InterruptTurn { .. }
            | Command::PauseThread { .. }
            | Command::CancelThread { .. }
            | Command::StopThreadSession { .. }
            | Command::RespondThreadApproval { .. }
            | Command::RespondThreadUserInput { .. }
            | Command::EditAndResendThreadMessage { .. }
        )
    }
}

// ---------------------------------------------------------------------------
// Provider stream consumer — the live half of the provider bridge.
// A session's ProviderEvent stream is driven to completion, with each event
// ingested (append + project) under the session's turn.
// ---------------------------------------------------------------------------

/// Append domain events to an aggregate's stream and project them to the read
/// model, returning the sequenced envelopes. Shared by
/// [`Orchestrator::ingest_provider_event`] and the stream consumer so the
/// append+project path is defined once.
pub(crate) async fn append_and_project(
    event_repo: &Arc<dyn EventRepository>,
    read_model: &Arc<tokio::sync::RwLock<ReadModelStore>>,
    aggregate_id: EntityId,
    events: Vec<DomainEvent>,
) -> Result<Vec<Envelope>, OrchestrationError> {
    if events.is_empty() {
        return Ok(Vec::new());
    }

    let current_version = event_repo.current_version(aggregate_id).await.unwrap_or(0);
    let _new_version = event_repo
        .append_events(aggregate_id, events.clone(), current_version)
        .await?;

    {
        let mut rm = read_model.write().await;
        Projector::project_many(&events, &mut rm);
    }

    let envelopes: Vec<Envelope> = events
        .into_iter()
        .enumerate()
        .map(|(i, event)| Envelope::new(event, current_version + 1 + i as u64))
        .collect();
    Ok(envelopes)
}

/// Resolve the thread that owns a turn, from the read model. Used to scope
/// provider-originated activities (ToolCall/ToolResult) to their thread when
/// only the turn_id is known. `None` if the turn isn't projected yet (or its
/// thread_id isn't a valid UUID).
pub(crate) async fn thread_id_for_turn(
    read_model: &tokio::sync::RwLock<ReadModelStore>,
    turn_id: EntityId,
) -> Option<EntityId> {
    read_model
        .read()
        .await
        .turns
        .get(&turn_id.as_str())
        .and_then(|t| EntityId::parse(&t.thread_id).ok())
}

/// Drive a provider event stream to completion, ingesting each event into the
/// pipeline (append + project) under the given turn. A stream error or an append
/// error stops the consumer (logged). A free function so it is unit-testable
/// with a synthetic stream, independent of `tokio::spawn`.
pub(crate) async fn consume_provider_stream(
    mut stream: syncode_provider::ProviderStream,
    event_repo: Arc<dyn EventRepository>,
    read_model: Arc<tokio::sync::RwLock<ReadModelStore>>,
    turn_id: EntityId,
    session_id: String,
) {
    use tokio_stream::StreamExt;

    // Resolve the turn's owning thread once; every event on this stream shares
    // it (StartTurn emits TurnStarted before the consumer spawns, so the turn is
    // normally already projected). None if not yet projected.
    let thread_id = thread_id_for_turn(&read_model, turn_id).await;

    tracing::info!(%session_id, "provider stream consumer started");
    while let Some(result) = stream.next().await {
        match result {
            Ok(provider_event) => {
                let ingestion = ingest_provider_event(provider_event, turn_id, thread_id);
                if let Err(e) =
                    append_and_project(&event_repo, &read_model, turn_id, ingestion.events).await
                {
                    tracing::error!(%session_id, error = %e, "stream consumer ingest failed; stopping");
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(%session_id, error = %e, "provider stream error; stopping consumer");
                return;
            }
        }
    }
    tracing::info!(%session_id, "provider stream consumer ended");
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
    use syncode_provider::SessionManager;

    fn make_orchestrator() -> Orchestrator {
        let repo = Arc::new(InMemoryEventRepo::new());
        Orchestrator::new(repo)
    }

    #[tokio::test]
    async fn test_ingest_provider_events_batch_closes_reverse_bridge() {
        // The command reactor's react() always returns events: vec![] today (its
        // send_request is synchronous), so the reverse bridge is exercised
        // directly: feed a batch of provider events for a turn and assert each
        // becomes a persisted domain event with monotonic sequencing.
        // Completed -> TurnCompleted; ToolCall -> ActivityLogged.
        let orch = make_orchestrator();
        let turn_id = EntityId::new();

        let batch = vec![
            syncode_provider::ProviderEvent::ToolCall {
                session_id: "s1".into(),
                tool_name: "grep".into(),
                tool_input: serde_json::json!({"q": "foo"}),
            },
            syncode_provider::ProviderEvent::Completed {
                session_id: "s1".into(),
                output: "done".into(),
                usage: Some(syncode_provider::UsageInfo {
                    input_tokens: 10,
                    output_tokens: 20,
                    total_tokens: 30,
                }),
            },
        ];

        let envelopes = orch
            .ingest_provider_events_batch(batch, turn_id)
            .await
            .expect("batch ingest");

        // Both provider events yield exactly one domain event each.
        assert_eq!(envelopes.len(), 2, "both provider events should be ingested");
        assert!(
            envelopes
                .iter()
                .any(|env| matches!(env.event, DomainEvent::TurnCompleted { .. })),
            "Completed should produce a TurnCompleted for this turn"
        );
        assert!(
            envelopes
                .iter()
                .any(|env| matches!(env.event, DomainEvent::ActivityLogged { .. })),
            "ToolCall should produce an ActivityLogged"
        );
        // Monotonic sequencing on the turn's fresh stream (1, 2).
        let seqs: Vec<u64> = envelopes.iter().map(|e| e.sequence).collect();
        assert_eq!(seqs, vec![1, 2], "events should be sequenced 1, 2");
    }

    #[tokio::test]
    async fn consume_provider_stream_ingests_stream_events() {
        // Exercise the live half of the provider bridge directly with a synthetic
        // stream (no tokio::spawn / mock adapter): each provider event is ingested
        // (append + project) under the turn. ToolCall -> ActivityLogged, Completed
        // -> TurnCompleted; Started yields nothing.
        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let read_model: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let turn_id = EntityId::new();

        let stream: syncode_provider::ProviderStream = Box::pin(tokio_stream::iter(vec![
            Ok(syncode_provider::ProviderEvent::ToolCall {
                session_id: "s1".into(),
                tool_name: "grep".into(),
                tool_input: serde_json::json!({"q": "foo"}),
            }),
            Ok(syncode_provider::ProviderEvent::Completed {
                session_id: "s1".into(),
                output: "done".into(),
                usage: None,
            }),
            Ok(syncode_provider::ProviderEvent::Started { session_id: "s1".into() }),
        ]));

        consume_provider_stream(
            stream,
            Arc::clone(&repo),
            Arc::clone(&read_model),
            turn_id,
            "s1".into(),
        )
        .await;

        // Two domain events appended to the turn stream (ToolCall + Completed);
        // Started produces none.
        assert_eq!(
            repo.current_version(turn_id).await.unwrap(),
            2,
            "two events should be appended to the turn stream"
        );
        // ToolCall projects one activity; Completed only updates an existing turn
        // (none here — no TurnStarted), so it adds nothing to the read model.
        let rm = read_model.read().await;
        assert_eq!(rm.activities.len(), 1, "ToolCall should project one activity");
        drop(rm);
    }

    #[tokio::test]
    async fn consume_provider_stream_scopes_activities_to_thread() {
        // Pre-project a TurnStarted so the turn→thread mapping exists in the read
        // model. A provider ToolCall on that turn's stream should then produce an
        // ActivityLogged scoped to the turn's thread (follow-up #3).
        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let read_model: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let thread_id = EntityId::new();
        let turn_id = EntityId::new();

        {
            let mut rm = read_model.write().await;
            Projector::project(
                &DomainEvent::TurnStarted {
                    id: turn_id,
                    thread_id,
                    sequence: 1,
                    user_input: "hi".into(),
                    created_at: syncode_core::Timestamp::now(),
                },
                &mut rm,
            );
        }

        let stream: syncode_provider::ProviderStream = Box::pin(tokio_stream::iter(vec![
            Ok(syncode_provider::ProviderEvent::ToolCall {
                session_id: "s1".into(),
                tool_name: "grep".into(),
                tool_input: serde_json::json!({"q": "foo"}),
            }),
        ]));
        consume_provider_stream(
            stream,
            Arc::clone(&repo),
            Arc::clone(&read_model),
            turn_id,
            "s1".into(),
        )
        .await;

        let rm = read_model.read().await;
        assert_eq!(rm.activities.len(), 1, "ToolCall should produce one activity");
        assert_eq!(
            rm.activities[0].thread_id,
            Some(thread_id.as_str()),
            "provider activity should be scoped to the turn's thread"
        );
    }

    #[tokio::test]
    async fn spawn_provider_stream_consumer_errors_without_adapter() {
        // Without an adapter wired there is no stream to subscribe to.
        let orch = make_orchestrator();
        let result = orch
            .spawn_provider_stream_consumer("s1".into(), EntityId::new())
            .await;
        assert!(result.is_err(), "spawning without an adapter must error");
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
    async fn test_create_thread_rejects_unknown_project() {
        let orch = make_orchestrator();

        // CreateThread now enforces the cross-aggregate invariant that its parent
        // project must exist. A thread targeting an unknown project is rejected
        // with ProjectNotFound before any event is produced.
        let result = orch.handle_command(Command::CreateThread {
            project_id: EntityId::new(),
            provider_id: "anthropic".into(),
            model: "claude-3".into(),
        })
        .await;

        assert!(
            matches!(result, Err(OrchestrationError::ProjectNotFound(_))),
            "CreateThread on an unknown project must be rejected, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_create_thread_succeeds_for_existing_project() {
        let orch = make_orchestrator();

        let project = orch
            .handle_command(Command::CreateProject {
                name: "Guarded".into(),
                root_path: "/guarded".into(),
            })
            .await
            .expect("create project");
        let project_id = project
            .events
            .iter()
            .find_map(|env| {
                if let DomainEvent::ProjectCreated { id, .. } = &env.event {
                    Some(*id)
                } else {
                    None
                }
            })
            .expect("ProjectCreated produced");

        let result = orch
            .handle_command(Command::CreateThread {
                project_id,
                provider_id: "anthropic".into(),
                model: "claude-3".into(),
            })
            .await
            .expect("create thread for existing project");

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

    #[tokio::test]
    async fn test_reactor_fires_on_provider_interaction_command() {
        // Wire the orchestrator with a command reactor AND a recording mock
        // adapter — this is what arms the pipeline's side effects.
        let repo = Arc::new(InMemoryEventRepo::new());
        let reactor = Arc::new(ProviderCommandReactor::new(SessionManager::new()));
        let (adapter, _stopped, requests) =
            crate::reactors::command::tests::make_recorded_test_mock();
        let orch = Orchestrator::with_reactor_and_adapter(repo, reactor, adapter);

        // StartTurn is a provider-interaction command: the reactor must create
        // a provider session and dispatch the initial request.
        let result = orch
            .handle_command(Command::StartTurn {
                thread_id: EntityId::new(),
                sequence: 1,
                user_input: "hello".to_string(),
            })
            .await
            .expect("start turn");

        // react() fired and propagated `handled`.
        assert!(
            result.side_effect_triggered,
            "StartTurn should trigger a provider side effect"
        );

        // …and the reactor actually dispatched to the adapter (the initial
        // "chat" request). The old inert stub would have left this empty even
        // though it set side_effect_triggered = true.
        let recorded = requests.lock().unwrap();
        assert!(
            recorded.iter().any(|(method, _)| method == "chat"),
            "reactor should have dispatched the initial request, got {:?}",
            recorded
        );
    }

    #[tokio::test]
    async fn test_reactor_inert_without_adapter() {
        // A reactor without an adapter must NOT fire any side effect, even for
        // a provider-interaction command (previously the stub set the flag true).
        let repo = Arc::new(InMemoryEventRepo::new());
        let reactor = Arc::new(ProviderCommandReactor::new(SessionManager::new()));
        let orch = Orchestrator::with_command_reactor(repo, reactor);

        let result = orch
            .handle_command(Command::StartTurn {
                thread_id: EntityId::new(),
                sequence: 1,
                user_input: "hello".to_string(),
            })
            .await
            .expect("start turn");

        assert!(
            !result.side_effect_triggered,
            "without an adapter the reactor must stay inert"
        );
    }

    #[tokio::test]
    async fn test_e2e_provider_bridge_routes_to_active_session() {
        // Full armed pipeline: CreateProject → CreateThread → StartTurn (creates
        // + registers a Processing session for the thread) → RespondThreadApproval
        // dispatches `approval/respond` to that session; EditAndResendThreadMessage
        // dispatches `message/edit-and-resend`. This is the end-to-end proof that
        // the T6 provider bridge is wired through the orchestrator — T2's
        // activation makes StartTurn actually arm a session that the follow-up
        // turn-interaction commands dispatch into.
        let repo = Arc::new(InMemoryEventRepo::new());
        let reactor = Arc::new(ProviderCommandReactor::new(SessionManager::new()));
        let (adapter, _stopped, requests) =
            crate::reactors::command::tests::make_recorded_test_mock();
        let orch = Orchestrator::with_reactor_and_adapter(repo, reactor, adapter);

        // 1. Create project + thread (the decider assigns the thread id). The
        //    thread must reference the real project id — CreateThread now guards
        //    the parent project's existence.
        let project_result = orch
            .handle_command(Command::CreateProject {
                name: "E2E".into(),
                root_path: "/e2e".into(),
            })
            .await
            .expect("create project");
        let project_id = project_result
            .events
            .iter()
            .find_map(|env| {
                if let DomainEvent::ProjectCreated { id, .. } = &env.event {
                    Some(*id)
                } else {
                    None
                }
            })
            .expect("ProjectCreated produced");

        let thread_result = orch
            .handle_command(Command::CreateThread {
                project_id,
                provider_id: "anthropic".into(),
                model: "claude".into(),
            })
            .await
            .expect("create thread");
        let thread_id = thread_result
            .events
            .iter()
            .find_map(|env| {
                if let DomainEvent::ThreadCreated { id, .. } = &env.event {
                    Some(*id)
                } else {
                    None
                }
            })
            .expect("ThreadCreated produced");

        // 2. StartTurn — the reactor creates a Processing session for the thread.
        let start = orch
            .handle_command(Command::StartTurn {
                thread_id,
                sequence: 1,
                user_input: "fix the bug".into(),
            })
            .await
            .expect("start turn");
        assert!(
            start.side_effect_triggered,
            "StartTurn should arm a provider session"
        );

        // 3. RespondThreadApproval — dispatched to the thread's active session.
        let approval = orch
            .handle_command(Command::RespondThreadApproval {
                id: thread_id,
                request_id: "req-123".into(),
                decision: "approved".into(),
            })
            .await
            .expect("respond approval");
        assert!(
            approval.side_effect_triggered,
            "approval response should dispatch to the provider"
        );

        let reqs = requests.lock().unwrap().clone();
        let approval_dispatch = reqs
            .iter()
            .find(|(m, _)| m.as_str() == "approval/respond")
            .expect("approval/respond should have been dispatched");
        let params = approval_dispatch
            .1
            .as_ref()
            .expect("approval/respond params");
        assert_eq!(params["request_id"].as_str(), Some("req-123"));
        assert_eq!(params["decision"].as_str(), Some("approved"));
        assert!(
            params["session_id"].as_str().is_some(),
            "dispatch should carry the target session id"
        );

        // 4. EditAndResendThreadMessage — dispatched as message/edit-and-resend.
        let edit = orch
            .handle_command(Command::EditAndResendThreadMessage {
                id: thread_id,
                message_id: EntityId::new(),
                text: "edited".into(),
            })
            .await
            .expect("edit and resend");
        assert!(
            edit.side_effect_triggered,
            "edit-and-resend should dispatch to the provider"
        );

        let reqs = requests.lock().unwrap().clone();
        let edit_dispatch = reqs
            .iter()
            .find(|(m, _)| m.as_str() == "message/edit-and-resend")
            .expect("message/edit-and-resend should have been dispatched");
        let params = edit_dispatch.1.as_ref().expect("edit-and-resend params");
        assert!(params["session_id"].as_str().is_some());
    }
}

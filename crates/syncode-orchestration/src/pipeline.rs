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

use std::collections::HashMap;
use std::sync::Arc;
use syncode_core::{
    DomainEvent, EntityId, Envelope,
    ports::{DomainEventPublisher, EventRepository, PortError},
};
use syncode_provider::ProviderEvent;

use crate::decider::{Command, Decider, DeciderError};
use crate::projector::{Projector, ReadModelStore};
use crate::reactors::{IngestionResult, ProviderCommandReactor, ingest_provider_event};
use crate::read_model::{
    MarkerView, MessageView, PinnedMessageView, ProjectView, ThreadView, TurnView,
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

    /// Optimistic-concurrency conflict that did not resolve within the retry
    /// budget. A concurrent append kept racing ahead on the same aggregate.
    #[error(
        "optimistic-concurrency conflict after {attempts} attempts: expected version {expected}, actual {actual}"
    )]
    ConcurrencyConflictRetried {
        expected: u64,
        actual: u64,
        attempts: usize,
    },
}

/// Maximum number of decide+append attempts before an optimistic-concurrency
/// conflict is surfaced as [`OrchestrationError::ConcurrencyConflictRetried`].
const MAX_CONCURRENCY_ATTEMPTS: usize = 5;

/// Outcome of one successful optimistic-concurrency attempt (see
/// [`Orchestrator::decide_and_append_once`]).
struct AppendedOutcome {
    events: Vec<DomainEvent>,
    aggregate_id: EntityId,
    current_version: u64,
    new_version: u64,
}

/// Persist an aggregate snapshot every N appended events, so long-lived streams
/// can later be reconstructed from a snapshot + tail instead of full replay.
const SNAPSHOT_INTERVAL: u64 = 50;

/// Whether an aggregate that just reached `new_version` events should be
/// snapshotted. Snapshots land on non-zero multiples of `interval`.
fn should_snapshot(new_version: u64, interval: u64) -> bool {
    interval > 0 && new_version > 0 && new_version.is_multiple_of(interval)
}

/// Serialize the read-model view for the given aggregate, searching the keyed
/// read-model maps. Returns `None` for aggregates whose view is not keyed by id
/// (e.g. activities, which are stored as a flat `Vec`).
fn view_for_aggregate(read_model: &ReadModelStore, id: EntityId) -> Option<serde_json::Value> {
    let key = id.as_str();
    if let Some(view) = read_model.projects.get(&key) {
        return serde_json::to_value(view).ok();
    }
    if let Some(view) = read_model.threads.get(&key) {
        return serde_json::to_value(view).ok();
    }
    if let Some(view) = read_model.turns.get(&key) {
        return serde_json::to_value(view).ok();
    }
    if let Some(view) = read_model.messages.get(&key) {
        return serde_json::to_value(view).ok();
    }
    if let Some(view) = read_model.pinned_messages.get(&key) {
        return serde_json::to_value(view).ok();
    }
    if let Some(view) = read_model.markers.get(&key) {
        return serde_json::to_value(view).ok();
    }
    None
}

/// The aggregate kind a serialized snapshot view belongs to. Used to route a
/// snapshot's state into the correct read-model map during cold-start seeding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateKind {
    Project,
    Thread,
    Turn,
    Message,
    PinnedMessage,
    Marker,
}

/// Classify a serialized aggregate view by a field UNIQUE to each kind. Every
/// view struct has at least one field no other view defines, so this is
/// order-independent and unambiguous. Returns `None` for shapes we never
/// snapshot (activities are a flat `Vec`; unknown JSON yields nothing).
fn aggregate_kind(state: &serde_json::Value) -> Option<AggregateKind> {
    if state.get("marker_id").is_some() {
        Some(AggregateKind::Marker)
    } else if state.get("pinned_at").is_some() {
        Some(AggregateKind::PinnedMessage)
    } else if state.get("user_input").is_some() {
        Some(AggregateKind::Turn)
    } else if state.get("role").is_some() {
        Some(AggregateKind::Message)
    } else if state.get("runtime_mode").is_some() {
        Some(AggregateKind::Thread)
    } else if state.get("root_path").is_some() {
        Some(AggregateKind::Project)
    } else {
        None
    }
}

/// Seed the read model from a single aggregate snapshot: classify the stored
/// view and insert it into the matching typed map under the aggregate's id.
/// This is the inverse of [`view_for_aggregate`]. A view that fails to
/// deserialize into its classified type is skipped with a warning rather than
/// panicking — the tail replay will still rebuild it from events.
fn seed_read_model_from_snapshot(
    read_model: &mut ReadModelStore,
    id: EntityId,
    state: &serde_json::Value,
) {
    let key = id.as_str();
    match aggregate_kind(state) {
        Some(AggregateKind::Project) => {
            if let Ok(view) = serde_json::from_value::<ProjectView>(state.clone()) {
                read_model.projects.insert(key, view);
            }
        }
        Some(AggregateKind::Thread) => {
            if let Ok(view) = serde_json::from_value::<ThreadView>(state.clone()) {
                read_model.threads.insert(key, view);
            }
        }
        Some(AggregateKind::Turn) => {
            if let Ok(view) = serde_json::from_value::<TurnView>(state.clone()) {
                read_model.turns.insert(key, view);
            }
        }
        Some(AggregateKind::Message) => {
            if let Ok(view) = serde_json::from_value::<MessageView>(state.clone()) {
                read_model.messages.insert(key, view);
            }
        }
        Some(AggregateKind::PinnedMessage) => {
            if let Ok(view) = serde_json::from_value::<PinnedMessageView>(state.clone()) {
                read_model.pinned_messages.insert(key, view);
            }
        }
        Some(AggregateKind::Marker) => {
            if let Ok(view) = serde_json::from_value::<MarkerView>(state.clone()) {
                read_model.markers.insert(key, view);
            }
        }
        None => {
            crate::log::warn(&format!(
                "snapshot view of unknown kind; skipping cold-start seed (aggregate = {id:?})"
            ));
        }
    }
}

/// Channel name used for outbound domain-event notifications. All domain events
/// (project, thread, turn, message, activity, …) are published here so a client
/// subscribed to the orchestration feed sees every state change.
const PUSH_CHANNEL: &str = "orchestration";

/// Best-effort fan-out of appended domain events to the outbound bus.
///
/// Each envelope is published with its type name, owning aggregate id, and
/// serialized payload. Publishing failures (serialization or transport) are
/// logged and never propagated — by the time we publish, the events are already
/// durably appended and projected, so a push problem must not fail the command.
async fn publish_events(publisher: &Arc<dyn DomainEventPublisher>, envelopes: &[Envelope]) {
    for env in envelopes {
        let event_type = env.event.event_type_name();
        let aggregate_id = env.event.aggregate_id();
        let data = match serde_json::to_value(&env.event) {
            Ok(value) => value,
            Err(e) => {
                crate::log::warn_err(
                    &e,
                    &format!("failed to serialize domain event for push; skipping ({event_type})"),
                );
                continue;
            }
        };
        if let Err(e) = publisher
            .publish(PUSH_CHANNEL, event_type, &aggregate_id.to_string(), data)
            .await
        {
            crate::log::warn_err(
                &e,
                &format!("domain-event push failed; event remains persisted ({event_type})"),
            );
        }
    }
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
    /// Per-thread provider adapter registry. When populated, `handle_command`
    /// resolves the adapter from the thread's `provider_id` (read from the
    /// read model) instead of always using the single `adapter` fallback.
    /// This lets different threads dispatch to different providers (e.g.
    /// thread A uses Codex, thread B uses Claude). When a provider_id isn't
    /// in the registry, the `adapter` (global default) is used.
    adapter_registry: std::collections::HashMap<String, syncode_provider::registry::SharedAdapter>,
    /// Optional outbound domain-event publisher. When wired, every appended
    /// domain event (command- and provider-stream-sourced) is pushed to the
    /// bus (e.g. a WebSocket push channel) after append+project, so connected
    /// clients can react in real time. Publishing is best-effort: failures are
    /// logged and never fail the originating command (events are already
    /// persisted by the time we publish).
    event_publisher: Option<Arc<dyn DomainEventPublisher>>,
}

impl Orchestrator {
    /// Create a new orchestrator with an event repository.
    pub fn new(event_repo: Arc<dyn EventRepository>) -> Self {
        Self {
            read_model: Arc::new(tokio::sync::RwLock::new(ReadModelStore::new())),
            event_repo,
            command_reactor: None,
            adapter: None,
            adapter_registry: std::collections::HashMap::new(),
            event_publisher: None,
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
            adapter_registry: std::collections::HashMap::new(),
            event_publisher: None,
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
            adapter_registry: std::collections::HashMap::new(),
            event_publisher: None,
        }
    }

    /// Create with a command reactor, provider adapter, AND a shared read model
    /// that the reactor is already wired to (PR-1-2).
    ///
    /// This is the production constructor: the caller constructs the read model
    /// `Arc` first, wires it into the reactor via
    /// [`ProviderCommandReactor::with_read_model`], then passes both the reactor
    /// and the same `Arc` here. Sharing the handle lets the reactor resolve a
    /// thread's project root path for the session working directory in real time
    /// (the projector updates the store, the reactor reads it under a short
    /// lock). Without this sharing, the reactor falls back to the hardcoded
    /// `/tmp/syncode` working directory (unit-test path).
    pub fn with_reactor_adapter_and_read_model(
        event_repo: Arc<dyn EventRepository>,
        command_reactor: Arc<ProviderCommandReactor>,
        adapter: syncode_provider::registry::SharedAdapter,
        read_model: Arc<tokio::sync::RwLock<ReadModelStore>>,
    ) -> Self {
        Self {
            read_model,
            event_repo,
            command_reactor: Some(command_reactor),
            adapter: Some(adapter),
            adapter_registry: std::collections::HashMap::new(),
            event_publisher: None,
        }
    }

    /// Attach an outbound domain-event publisher (builder-style, consumes and
    /// returns `self` so it chains after a constructor). When attached, every
    /// appended domain event is pushed to the bus after append+project.
    ///
    /// Publishing is best-effort: a push failure is logged and never fails the
    /// originating command (the events are already persisted by publish time).
    pub fn with_event_publisher(mut self, publisher: Arc<dyn DomainEventPublisher>) -> Self {
        self.event_publisher = Some(publisher);
        self
    }

    /// Register a provider adapter under its provider id so `handle_command`
    /// can dispatch turns to the per-thread provider instead of the single
    /// global `adapter`. The global adapter remains the fallback for
    /// provider ids not in the registry (e.g. threads whose provider's CLI
    /// isn't installed, or legacy threads created before per-thread dispatch).
    /// Builder-style: chains after a constructor.
    pub fn with_adapter_registry(
        mut self,
        registry: Vec<(String, syncode_provider::registry::SharedAdapter)>,
    ) -> Self {
        self.adapter_registry = registry.into_iter().collect();
        self
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
            adapter_registry: std::collections::HashMap::new(),
            event_publisher: None,
        }
    }

    /// Borrow the configured command reactor (if any).
    ///
    /// Used by the server binary to (a) rehydrate sessions into the reactor's
    /// [`SessionManager`] on startup and (b) persist cursors to disk on
    /// shutdown. Returns `None` for the inert `Orchestrator::new` path.
    pub fn command_reactor(&self) -> Option<&Arc<ProviderCommandReactor>> {
        self.command_reactor.as_ref()
    }

    /// Borrow the configured provider adapter (if any).
    ///
    /// Pairs with [`Self::command_reactor`] for resume-cursor rehydration
    /// (which needs both the `SessionManager` and the adapter to call
    /// `resume_session`).
    pub fn adapter(&self) -> Option<&syncode_provider::registry::SharedAdapter> {
        self.adapter.as_ref()
    }

    /// Handle a command through the full pipeline.
    ///
    /// 1. Extract aggregate state from read model as JSON
    /// 2. Run command through Decider → domain events
    /// 3. Determine aggregate ID from produced events
    /// 4. Persist events via EventRepository
    /// 5. Build envelopes and project to in-memory read model
    /// 6. Trigger CommandReactor side effects (if configured)
    pub async fn handle_command(
        &self,
        command: Command,
    ) -> Result<CommandResult, OrchestrationError> {
        crate::log::info("Processing command");

        // 1. Optimistic-concurrency-controlled decide + append.
        //
        // Each attempt loads the aggregate's current state, runs the pure
        // Decider, and appends the produced events at the stream's current
        // version. If a concurrent append races ahead, `append_events` returns
        // `ConcurrencyConflict` and we retry — re-loading state and re-deciding
        // against the now-current version. Decider errors and non-concurrency
        // port errors propagate immediately; only conflicts are retried, up to
        // [`MAX_CONCURRENCY_ATTEMPTS`].
        let aggregate_id_hint = self.aggregate_id_for_command(&command);

        let mut appended: Option<AppendedOutcome> = None;
        let mut last_conflict: Option<(u64, u64)> = None;
        for _ in 0..MAX_CONCURRENCY_ATTEMPTS {
            match self
                .decide_and_append_once(&command, &aggregate_id_hint)
                .await
            {
                Ok(None) => {
                    crate::log::info("No events produced");
                    return Ok(CommandResult {
                        command,
                        events: vec![],
                        side_effect_triggered: false,
                        side_effect_events: vec![],
                    });
                }
                Ok(Some(outcome)) => {
                    appended = Some(outcome);
                    break;
                }
                Err(OrchestrationError::EventRepository(PortError::ConcurrencyConflict {
                    expected,
                    actual,
                })) => {
                    crate::log::warn(&format!(
                        "optimistic-concurrency conflict on append; retrying decide+append (expected = {expected}, actual = {actual})"
                    ));
                    last_conflict = Some((expected, actual));
                }
                Err(other) => return Err(other),
            }
        }

        let AppendedOutcome {
            events: domain_events,
            aggregate_id,
            current_version,
            new_version,
        } = match appended {
            Some(outcome) => outcome,
            None => {
                // Retry budget exhausted: surface the last conflict rather than
                // silently dropping the command.
                let (expected, actual) =
                    last_conflict.expect("retry loop exhausted without a recorded conflict");
                return Err(OrchestrationError::ConcurrencyConflictRetried {
                    expected,
                    actual,
                    attempts: MAX_CONCURRENCY_ATTEMPTS,
                });
            }
        };

        // 5. Project raw domain events to in-memory read model, then wrap in envelopes.
        let mut read_model = self.read_model.write().await;
        Projector::project_many(&domain_events, &mut read_model);
        // If this append crossed the snapshot interval, capture the aggregate's
        // just-updated view (while we hold the lock) and persist it after the
        // lock is released, so save_snapshot's await doesn't block read-model readers.
        let snapshot_to_write = if should_snapshot(new_version, SNAPSHOT_INTERVAL) {
            view_for_aggregate(&read_model, aggregate_id).map(|state| (state, new_version))
        } else {
            None
        };
        drop(read_model);

        if let Some((state, version)) = snapshot_to_write
            && let Err(e) = self
                .event_repo
                .save_snapshot(aggregate_id, state, version)
                .await
        {
            crate::log::warn_err(
                &e,
                &format!(
                    "failed to save aggregate snapshot (aggregate = {aggregate_id:?}, version = {version})"
                ),
            );
        }

        let envelopes: Vec<Envelope> = domain_events
            .into_iter()
            .enumerate()
            .map(|(i, event)| {
                let seq = current_version + 1 + i as u64;
                Envelope::new(event, seq)
            })
            .collect();

        crate::log::info(&format!(
            "Events persisted and projected (count = {})",
            envelopes.len()
        ));

        // 5b. Best-effort push of the just-appended command events to the
        //     outbound bus (e.g. WebSocket). Provider-stream events take the
        //     same path inside `append_and_project`. Never fails the command.
        if let Some(publisher) = &self.event_publisher {
            publish_events(publisher, &envelopes).await;
        }

        // 6. Trigger side effects (command reactor). When both a reactor and an
        //    adapter are wired, provider-interaction commands actually dispatch
        //    to the provider (start a session on StartTurn, respond to an
        //    approval/user-input request, stop a thread's session, …). `handled`
        //    reflects whether a provider side effect took effect. A reactor
        //    without an adapter (or no provider-interaction command) stays inert.
        let mut side_effect_events: Vec<Envelope> = Vec::new();
        let mut side_effect_triggered = false;

        if let (Some(reactor), Some(default_adapter)) = (&self.command_reactor, &self.adapter)
            && self.needs_provider_interaction(&command)
        {
            // Per-thread provider dispatch: resolve the adapter from the
            // thread's provider_id (read from the read model) instead of
            // always using the global default. When the thread's provider
            // isn't in the registry (e.g. CLI not installed, or registry
            // not populated), fall back to the global default adapter so
            // existing behavior is preserved.
            let adapter = self
                .resolve_adapter_for_command(&command, default_adapter)
                .await;
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
                .react(&command, &adapter, turn_id_hint)
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
                // moving reaction.events into the batch ingest below. Also note
                // whether the reactor already observed a terminal event: the
                // claude adapter runs `send_request` synchronously (one CLI
                // turn to completion) and the reactor pre-subscribes so its
                // captured events include the terminal Completed/Error — in
                // that case the turn is already finished and spawning the live
                // stream consumer would only leak a task that blocks forever
                // on the adapter's never-dropped broadcast sender.
                let session_id = reaction.session_id.clone();
                let reactor_captured_terminal = reaction.events.iter().any(|ev| {
                    matches!(
                        ev,
                        syncode_provider::ProviderEvent::Completed { .. }
                            | syncode_provider::ProviderEvent::Error { .. }
                    )
                });
                side_effect_events = self
                    .ingest_provider_events_batch(reaction.events, turn_id)
                    .await?;

                // Live bridge: for StartTurn the reactor created a provider
                // session. Spawn a detached consumer that forwards that
                // session's provider event stream back into the pipeline
                // (append + project) under the turn. Streaming output (tokens,
                // tool calls, completion) arrives here, not via react().
                //
                // Skipped when the reactor already captured a terminal event
                // (synchronous adapters like claude that run the whole turn
                // inside `send_request`): the turn is complete, and a fresh
                // subscriber would see no further events.
                if !reactor_captured_terminal
                    && let Some(sid) = session_id
                    && let Err(e) = self.spawn_provider_stream_consumer(sid, turn_id).await
                {
                    crate::log::warn_err(&e, "failed to spawn provider stream consumer");
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

    /// One attempt of the optimistic-concurrency loop: load the aggregate's
    /// current state, run the pure Decider, and append the produced events at
    /// the stream's current version. Returns `Ok(None)` when the Decider
    /// produced no events. The `CreateThread` project-existence guard lives
    /// here so each retry re-evaluates it against freshly-loaded state.
    async fn decide_and_append_once(
        &self,
        command: &Command,
        aggregate_id_hint: &Option<EntityId>,
    ) -> Result<Option<AppendedOutcome>, OrchestrationError> {
        let current_state = self.load_aggregate_state(aggregate_id_hint, command).await;

        // Cross-aggregate invariant: CreateThread requires its parent project to
        // exist. Handoff/Fork enforce this at the application layer, but
        // CreateThread is reachable directly through the orchestrator (WS-RPC),
        // so the engine guards it here — before the pure Decider runs.
        if let Command::CreateThread { project_id, .. } = command
            && current_state.is_none()
        {
            return Err(OrchestrationError::ProjectNotFound(*project_id));
        }

        let domain_events = Decider::decide(command.clone(), current_state.as_ref())?;
        if domain_events.is_empty() {
            return Ok(None);
        }

        let aggregate_id = domain_events[0].aggregate_id();
        let current_version = self
            .event_repo
            .current_version(aggregate_id)
            .await
            .unwrap_or(0);
        let new_version = self
            .event_repo
            .append_events(aggregate_id, domain_events.clone(), current_version)
            .await?;

        Ok(Some(AppendedOutcome {
            events: domain_events,
            aggregate_id,
            current_version,
            new_version,
        }))
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
        let IngestionResult {
            events,
            consumed: _,
        } = ingest_provider_event(provider_event, turn_id, thread_id, None);
        // Turn events aggregate on the turn; append + project is shared with the
        // live stream consumer via the module-level `append_and_project` helper.
        append_and_project(
            &self.event_repo,
            &self.read_model,
            self.event_publisher.as_ref(),
            turn_id,
            events,
        )
        .await
    }

    /// Ingest a batch of provider events collected by the command reactor and
    /// produce domain-event envelopes. This closes the reverse direction of the
    /// provider bridge: a provider-interaction command may collect provider
    /// events (e.g. a `Completed` event from a synchronous response), which we
    /// turn back into domain events and append + project just like stream-sourced
    /// events. All events are correlated to the given `turn_id`.
    ///
    /// Returns the resulting envelopes (may be empty — `Started`/
    /// `StatusChanged` produce no domain event; `Token` produces a
    /// `MessageDeltaAppended`).
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
        let event_publisher = self.event_publisher.clone();

        Ok(tokio::spawn(async move {
            consume_provider_stream(
                stream,
                event_repo,
                read_model,
                event_publisher,
                turn_id,
                session_id,
            )
            .await;
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

    /// Rebuild the in-memory read model from the event repository.
    ///
    /// Cold start: seed the projection from any stored aggregate snapshots,
    /// then replay only each aggregate's *tail* (the events appended after its
    /// snapshot) instead of the full stream. When no snapshots exist this
    /// reduces to a plain full replay — every event is projected.
    ///
    /// Correctness: a snapshot at version `V` is exactly the projection of the
    /// aggregate's first `V` events (it is captured right after projecting the
    /// `V`-th), so `seed + tail == full replay`.
    ///
    /// Returns `(replayed, seeded)` where `replayed` is the total number of
    /// events read from the repository and `seeded` is the number of aggregate
    /// snapshots used to seed the projection (0 when no snapshots exist — a
    /// plain full replay).
    pub async fn replay_read_model(&self) -> Result<(u32, usize), OrchestrationError> {
        let snapshots = self.event_repo.load_all_snapshots().await?;
        let envelopes = self.event_repo.replay_all_events(None, 10_000).await?;
        let count = envelopes.len() as u32;
        let seeded = snapshots.len();

        // Per-aggregate skip counters: a snapshot at `version` already reflects
        // the aggregate's first `version` events, so skip those and project only
        // the rest. Aggregates without a snapshot are absent here, so every one
        // of their events is projected (full replay for them).
        let mut skip: HashMap<EntityId, u64> = snapshots
            .iter()
            .map(|(id, _state, version)| (*id, *version))
            .collect();

        let mut read_model = self.read_model.write().await;

        // Seed the projection from snapshots (classified by aggregate kind).
        for (id, state, _version) in &snapshots {
            seed_read_model_from_snapshot(&mut read_model, *id, state);
        }

        // Replay events in order, skipping each snapshotted aggregate's covered
        // prefix, and project the tail onto the seeded read model. Per-aggregate
        // order is preserved within the returned stream, so each aggregate's tail
        // is applied in the correct sequence.
        let mut tail: Vec<DomainEvent> = Vec::with_capacity(envelopes.len());
        for env in &envelopes {
            let aid = env.event.aggregate_id();
            if let Some(remaining) = skip.get_mut(&aid)
                && *remaining > 0
            {
                *remaining -= 1;
                continue;
            }
            tail.push(env.event.clone());
        }
        Projector::project_many(&tail, &mut read_model);
        drop(read_model);

        crate::log::info(&format!(
            "Read model replayed (snapshot-seeded + tail) (count = {count}, seeded = {seeded})"
        ));
        Ok((count, seeded))
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
            | Command::RenameProject { id, .. }
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
            | Command::UpdateThreadMeta { id, .. }
            | Command::RespondThreadApproval { id, .. }
            | Command::RespondThreadUserInput { id, .. }
            | Command::EditAndResendThreadMessage { id, .. }
            | Command::SetThreadSession { id, .. }
            | Command::DispatchQueuedTurn { id, .. }
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

            // Streaming assistant messages target the thread (existence guard);
            // the produced event persists under the caller-supplied message id.
            Command::AppendAssistantDelta { thread_id, .. }
            | Command::FinalizeAssistantMessage { thread_id, .. }
            | Command::UpsertProposedPlan { thread_id, .. }
            | Command::CompleteTurnDiff { thread_id, .. }
            | Command::CompleteRevert { thread_id, .. }
            | Command::ConversationRollback { thread_id, .. }
            | Command::ConversationRollbackComplete { thread_id, .. }
            | Command::ImportMessages { thread_id, .. } => Some(*thread_id),

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
            | Command::RenameProject { .. }
            | Command::DeleteProject { .. } => read_model
                .projects
                .get(&id.as_str())
                .map(|p| serde_json::to_value(p).unwrap_or_default()),

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
            | Command::UpdateThreadMeta { .. }
            | Command::RespondThreadApproval { .. }
            | Command::RespondThreadUserInput { .. }
            | Command::EditAndResendThreadMessage { .. }
            | Command::SetThreadSession { .. }
            | Command::DispatchQueuedTurn { .. }
            | Command::AppendAssistantDelta { .. }
            | Command::FinalizeAssistantMessage { .. }
            | Command::UpsertProposedPlan { .. }
            | Command::CompleteTurnDiff { .. }
            | Command::CompleteRevert { .. }
            | Command::ConversationRollback { .. }
            | Command::ConversationRollbackComplete { .. }
            | Command::ImportMessages { .. }
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

            Command::CreateThread { .. } => read_model
                .projects
                .get(&id.as_str())
                .map(|p| serde_json::to_value(p).unwrap_or_default()),

            Command::StartTurn { .. } => read_model
                .threads
                .get(&id.as_str())
                .map(|t| serde_json::json!({"status": t.status})),

            Command::CompleteTurn { .. }
            | Command::FailTurn { .. }
            | Command::CancelTurn { .. }
            | Command::InterruptTurn { .. }
            | Command::RecordTurnFiles { .. }
            | Command::SetTurnCheckpoint { .. } => read_model
                .turns
                .get(&id.as_str())
                .map(|t| serde_json::json!({"status": t.status})),

            Command::AddMessage { .. } => None,

            // Thread-creation-by-import: the Decider trusts the command (project
            // and source-thread existence are enforced at the application layer).
            Command::HandoffCreateThread { .. } | Command::ForkCreateThread { .. } => None,
        }
    }

    /// Resolve the provider adapter for a command. Reads the thread's
    /// `provider_id` from the read model and looks it up in
    /// `adapter_registry`. Falls back to `default_adapter` when:
    /// - the command doesn't carry a thread id (e.g. CreateThread),
    /// - the thread isn't in the read model yet (created in this command),
    /// - the thread's provider isn't registered (CLI not installed).
    async fn resolve_adapter_for_command(
        &self,
        command: &Command,
        default_adapter: &syncode_provider::registry::SharedAdapter,
    ) -> syncode_provider::registry::SharedAdapter {
        // Extract the thread id from the command. StartTurn carries it
        // directly; most other provider-interaction commands carry it as `id`.
        let thread_id: Option<String> = match command {
            Command::StartTurn { thread_id, .. } => Some(thread_id.as_str().to_string()),
            Command::PauseThread { id, .. }
            | Command::ResumeThread { id, .. }
            | Command::StopThreadSession { id, .. }
            | Command::RespondThreadApproval { id, .. }
            | Command::RespondThreadUserInput { id, .. }
            | Command::EditAndResendThreadMessage { id, .. } => Some(id.as_str().to_string()),
            _ => None,
        };
        let Some(tid) = thread_id else {
            return default_adapter.clone();
        };
        // Look up the thread's provider_id in the read model.
        let provider_id = {
            let rm = self.read_model.read().await;
            rm.threads.get(tid.as_str()).map(|t| t.provider_id.clone())
        };
        let Some(pid) = provider_id else {
            return default_adapter.clone();
        };
        // Resolve the adapter from the registry.
        match self.adapter_registry.get(&pid) {
            Some(adapter) => adapter.clone(),
            None => default_adapter.clone(),
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
                | Command::DispatchQueuedTurn { .. }
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
    event_publisher: Option<&Arc<dyn DomainEventPublisher>>,
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

    // Best-effort push of provider-stream/batch-sourced events. Mirrors the
    // command-event push in handle_command. Never fails the append.
    if let Some(publisher) = event_publisher {
        publish_events(publisher, &envelopes).await;
    }

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

/// Maximum number of token chunks to accumulate before flushing a batched
/// `MessageDeltaAppended`, even if the time window hasn't elapsed. Caps the
/// latency of any single flush and bounds the buffer size.
const TOKEN_BATCH_MAX_COUNT: usize = 64;

/// Drive a provider event stream to completion, ingesting each event into the
/// pipeline (append + project) under the given turn. A stream error or an append
/// error stops the consumer (logged). A free function so it is unit-testable
/// with a synthetic stream, independent of `tokio::spawn`.
///
/// Token batching: incoming `ProviderEvent::Token` chunks are accumulated into a
/// buffer and flushed as a single `MessageDeltaAppended` every ~100ms (or when
/// `TOKEN_BATCH_MAX_COUNT` chunks accumulate, whichever comes first). This
/// avoids flooding the event store + WS push channel with one event per token
/// while still delivering streamed output to subscribed clients in real time.
/// Non-token events flush any pending token buffer first so ordering is
/// preserved (all tokens emitted before a ToolCall/Completed arrive before it).
pub(crate) async fn consume_provider_stream(
    mut stream: syncode_provider::ProviderStream,
    event_repo: Arc<dyn EventRepository>,
    read_model: Arc<tokio::sync::RwLock<ReadModelStore>>,
    event_publisher: Option<Arc<dyn DomainEventPublisher>>,
    turn_id: EntityId,
    session_id: String,
) {
    use tokio::time::{Duration, interval};
    use tokio_stream::StreamExt;

    // Resolve the turn's owning thread once; every event on this stream shares
    // it (StartTurn emits TurnStarted before the consumer spawns, so the turn is
    // normally already projected). None if not yet projected.
    let thread_id = thread_id_for_turn(&read_model, turn_id).await;

    // Bundle the immutable pipeline handles so the batching helpers stay
    // under clippy's argument-count limit and the call sites stay readable.
    let ctx = StreamCtx {
        turn_id,
        thread_id,
        started_at: syncode_core::Timestamp::now(),
        event_repo: &event_repo,
        read_model: &read_model,
        event_publisher: event_publisher.as_ref(),
        session_id: &session_id,
    };

    // Token batching state: accumulated text + chunk count since the last flush.
    let mut token_buf = String::new();
    let mut token_count: usize = 0;

    // 100ms flush window. The first tick completes immediately (tokio's interval
    // contract), so we skip it; subsequent ticks fire ~every 100ms and flush any
    // buffered tokens.
    let mut flush_timer = interval(Duration::from_millis(100));
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Discard the immediate first tick so we don't flush an empty buffer right away.
    flush_timer.tick().await;

    crate::log::info(&format!("provider stream consumer started: {session_id}"));

    loop {
        tokio::select! {
            // Flush the token buffer when the time window elapses, regardless of
            // stream activity, so a slow tail of tokens still reaches clients.
            _ = flush_timer.tick() => {
                if !token_buf.is_empty() {
                    let batch = std::mem::take(&mut token_buf);
                    token_count = 0;
                    ctx.flush_token_batch(batch).await;
                }
            }
            next = stream.next() => {
                match next {
                    None => break,
                    Some(Err(e)) => {
                        crate::log::warn_err(&e, &format!("provider stream error; stopping consumer ({session_id})"));
                        return;
                    }
                    Some(Ok(provider_event)) => {
                        // Tokens are accumulated into the batch buffer; everything
                        // else flushes the buffer first (ordering), then is ingested
                        // directly.
                        if let ProviderEvent::Token { content, .. } = &provider_event {
                            token_buf.push_str(content);
                            token_count += 1;
                            if token_count >= TOKEN_BATCH_MAX_COUNT {
                                let batch = std::mem::take(&mut token_buf);
                                token_count = 0;
                                ctx.flush_token_batch(batch).await;
                            }
                            continue;
                        }

                        // Non-token event: flush pending tokens first to preserve
                        // ordering (all tokens before this event reach the client
                        // before the event's own domain event does).
                        if !token_buf.is_empty() {
                            let batch = std::mem::take(&mut token_buf);
                            token_count = 0;
                            ctx.flush_token_batch(batch).await;
                        }

                        let ingestion = ingest_provider_event(
                            provider_event,
                            ctx.turn_id,
                            ctx.thread_id,
                            Some(ctx.started_at),
                        );
                        if let Err(e) = append_and_project(
                            ctx.event_repo,
                            ctx.read_model,
                            ctx.event_publisher,
                            ctx.turn_id,
                            ingestion.events,
                        )
                        .await
                        {
                            crate::log::error(&format!("stream consumer ingest failed; stopping ({session_id}: {e})"));
                            return;
                        }
                    }
                }
            }
        }
    }

    // Stream ended: flush any remaining buffered tokens so the final tail of the
    // response is not lost.
    if !token_buf.is_empty() {
        ctx.flush_token_batch(token_buf).await;
    }
    crate::log::info(&format!("provider stream consumer ended: {session_id}"));
}

/// Immutable handles the batching loop needs, bundled so the flush helper stays
/// under clippy's argument-count limit. Borrows the caller-owned `Arc`s for the
/// lifetime of one `consume_provider_stream` invocation.
struct StreamCtx<'a> {
    turn_id: EntityId,
    thread_id: Option<EntityId>,
    started_at: syncode_core::Timestamp,
    event_repo: &'a Arc<dyn EventRepository>,
    read_model: &'a Arc<tokio::sync::RwLock<ReadModelStore>>,
    event_publisher: Option<&'a Arc<dyn DomainEventPublisher>>,
    session_id: &'a str,
}

impl<'a> StreamCtx<'a> {
    /// Flush a batch of accumulated token text as a single `MessageDeltaAppended`
    /// domain event (append + project + publish). Append errors are logged and do
    /// not stop the consumer (the next batch can still succeed); the caller owns
    /// the decision to terminate on stream-level errors.
    async fn flush_token_batch(&self, batch: String) {
        let provider_event = ProviderEvent::Token {
            session_id: self.session_id.to_string(),
            content: batch,
        };
        let ingestion = ingest_provider_event(
            provider_event,
            self.turn_id,
            self.thread_id,
            Some(self.started_at),
        );
        if let Err(e) = append_and_project(
            self.event_repo,
            self.read_model,
            self.event_publisher,
            self.turn_id,
            ingestion.events,
        )
        .await
        {
            crate::log::error(&format!(
                "token-batch flush failed; dropping this batch (session_id = {}, error = {e})",
                self.session_id
            ));
        }
    }
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use syncode_core::ports::EventRepository;

    /// In-memory fake event repository for testing
    pub(crate) struct InMemoryEventRepo {
        events: Mutex<HashMap<String, Vec<Envelope>>>,
        snapshots: Mutex<HashMap<String, (serde_json::Value, u64)>>,
    }

    impl InMemoryEventRepo {
        pub(crate) fn new() -> Self {
            Self {
                events: Mutex::new(HashMap::new()),
                snapshots: Mutex::new(HashMap::new()),
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

        async fn replay_events(&self, aggregate_id: EntityId) -> Result<Vec<Envelope>, PortError> {
            let store = self.events.lock().unwrap();
            Ok(store
                .get(&aggregate_id.to_string())
                .cloned()
                .unwrap_or_default())
        }

        async fn load_snapshot(
            &self,
            aggregate_id: EntityId,
        ) -> Result<Option<(serde_json::Value, u64)>, PortError> {
            let snapshots = self.snapshots.lock().unwrap();
            Ok(snapshots.get(&aggregate_id.to_string()).cloned())
        }

        async fn save_snapshot(
            &self,
            aggregate_id: EntityId,
            state: serde_json::Value,
            version: u64,
        ) -> Result<(), PortError> {
            let mut snapshots = self.snapshots.lock().unwrap();
            snapshots.insert(aggregate_id.to_string(), (state, version));
            Ok(())
        }

        async fn load_all_snapshots(
            &self,
        ) -> Result<Vec<(EntityId, serde_json::Value, u64)>, PortError> {
            let snapshots = self.snapshots.lock().unwrap();
            // aggregate_id keys are always valid UUID strings (stored via
            // EntityId::to_string); parse failures here would indicate corruption.
            let mut out = Vec::with_capacity(snapshots.len());
            for (id_str, (state, version)) in snapshots.iter() {
                let id = EntityId::parse(id_str)
                    .map_err(|e| PortError::Internal(format!("invalid aggregate_id: {e}")))?;
                out.push((id, state.clone(), *version));
            }
            Ok(out)
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
            Ok(store
                .get(&aggregate_id.to_string())
                .map(|v| v.len() as u64)
                .unwrap_or(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_helpers::InMemoryEventRepo;
    use super::*;
    use syncode_provider::SessionManager;

    fn make_orchestrator() -> Orchestrator {
        let repo = Arc::new(InMemoryEventRepo::new());
        Orchestrator::new(repo)
    }

    /// In-memory event-repo wrapper that fails the first `conflicts`
    /// `append_events` calls with a `ConcurrencyConflict`, then delegates to
    /// the inner repo. Used to exercise the optimistic-concurrency retry loop
    /// in `handle_command` without real concurrency.
    struct FlakyEventRepo {
        inner: InMemoryEventRepo,
        conflicts_remaining: std::sync::atomic::AtomicU32,
        append_calls: std::sync::atomic::AtomicU32,
    }

    impl FlakyEventRepo {
        fn new(conflicts: u32) -> Self {
            Self {
                inner: InMemoryEventRepo::new(),
                conflicts_remaining: std::sync::atomic::AtomicU32::new(conflicts),
                append_calls: std::sync::atomic::AtomicU32::new(0),
            }
        }

        fn append_calls(&self) -> u32 {
            self.append_calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl EventRepository for FlakyEventRepo {
        async fn append_events(
            &self,
            aggregate_id: EntityId,
            events: Vec<DomainEvent>,
            expected_version: u64,
        ) -> Result<u64, PortError> {
            self.append_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Atomically consume one conflict credit if any remain. Using an
            // atomic (not a Mutex guard) keeps the future `Send`: no lock guard
            // is held across the `.await` below.
            let conflicted = self
                .conflicts_remaining
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |v| if v > 0 { Some(v - 1) } else { None },
                )
                .is_ok();
            if conflicted {
                return Err(PortError::ConcurrencyConflict {
                    expected: expected_version,
                    actual: expected_version + 1,
                });
            }
            self.inner
                .append_events(aggregate_id, events, expected_version)
                .await
        }

        async fn replay_events(&self, aggregate_id: EntityId) -> Result<Vec<Envelope>, PortError> {
            self.inner.replay_events(aggregate_id).await
        }

        async fn load_snapshot(
            &self,
            aggregate_id: EntityId,
        ) -> Result<Option<(serde_json::Value, u64)>, PortError> {
            self.inner.load_snapshot(aggregate_id).await
        }

        async fn save_snapshot(
            &self,
            aggregate_id: EntityId,
            state: serde_json::Value,
            version: u64,
        ) -> Result<(), PortError> {
            self.inner.save_snapshot(aggregate_id, state, version).await
        }

        async fn load_all_snapshots(
            &self,
        ) -> Result<Vec<(EntityId, serde_json::Value, u64)>, PortError> {
            self.inner.load_all_snapshots().await
        }

        async fn replay_all_events(
            &self,
            since_sequence: Option<u64>,
            limit: u32,
        ) -> Result<Vec<Envelope>, PortError> {
            self.inner.replay_all_events(since_sequence, limit).await
        }

        async fn current_version(&self, aggregate_id: EntityId) -> Result<u64, PortError> {
            self.inner.current_version(aggregate_id).await
        }
    }

    #[tokio::test]
    async fn handle_command_retries_concurrency_conflict_then_succeeds() {
        // The repo fails the first append with a conflict, then succeeds on the
        // retry. handle_command must re-load state + re-decide + re-append and
        // ultimately return the produced events (ProjectCreated).
        let repo = Arc::new(FlakyEventRepo::new(1));
        let repo_handle = Arc::clone(&repo);
        let orch = Orchestrator::new(repo);

        let result = orch
            .handle_command(Command::CreateProject {
                name: "Retried".into(),
                root_path: "/retried".into(),
            })
            .await
            .expect("command should succeed after retry");

        assert_eq!(result.events.len(), 1);
        assert_eq!(result.events[0].event.event_type_name(), "ProjectCreated");
        // Initial attempt (conflicted) + one successful retry.
        assert_eq!(repo_handle.append_calls(), 2);
    }

    #[tokio::test]
    async fn handle_command_surfaces_conflict_after_retry_budget_exhausted() {
        // The repo conflicts on every append. After MAX_CONCURRENCY_ATTEMPTS
        // attempts the conflict is surfaced as ConcurrencyConflictRetried rather
        // than retried indefinitely.
        let repo = Arc::new(FlakyEventRepo::new(u32::MAX));
        let repo_handle = Arc::clone(&repo);
        let orch = Orchestrator::new(repo);

        let result = orch
            .handle_command(Command::CreateProject {
                name: "Never".into(),
                root_path: "/never".into(),
            })
            .await;

        match result {
            Err(OrchestrationError::ConcurrencyConflictRetried { attempts, .. }) => {
                assert_eq!(attempts, MAX_CONCURRENCY_ATTEMPTS);
            }
            other => panic!("expected ConcurrencyConflictRetried, got: {other:?}"),
        }
        assert_eq!(repo_handle.append_calls(), MAX_CONCURRENCY_ATTEMPTS as u32);
    }

    #[test]
    fn should_snapshot_only_on_nonzero_interval_multiples() {
        assert!(!should_snapshot(0, 50));
        assert!(!should_snapshot(49, 50));
        assert!(should_snapshot(50, 50));
        assert!(should_snapshot(100, 50));
        assert!(!should_snapshot(51, 50));
        // A zero interval must never snapshot (guards against always-on / mod).
        assert!(!should_snapshot(50, 0));
    }

    #[tokio::test]
    async fn handle_command_writes_snapshot_when_aggregate_crosses_interval() {
        // Grow a single thread stream to exactly SNAPSHOT_INTERVAL events by
        // repeatedly setting its title (each appends one ThreadTitleSet). At the
        // boundary the orchestrator must persist a snapshot of the thread view.
        let orch = make_orchestrator();

        let project = orch
            .handle_command(Command::CreateProject {
                name: "Snap".into(),
                root_path: "/snap".into(),
            })
            .await
            .expect("create project");
        let project_id = match &project.events[0].event {
            DomainEvent::ProjectCreated { id, .. } => *id,
            _ => unreachable!("CreateProject yields ProjectCreated"),
        };

        let thread = orch
            .handle_command(Command::CreateThread {
                project_id,
                provider_id: "p".into(),
                model: "m".into(),
                thread_id: None,
            })
            .await
            .expect("create thread");
        let thread_id = match &thread.events[0].event {
            DomainEvent::ThreadCreated { id, .. } => *id,
            _ => unreachable!("CreateThread yields ThreadCreated"),
        };

        // Thread stream is at version 1 after creation; grow it to SNAPSHOT_INTERVAL.
        for i in 1..SNAPSHOT_INTERVAL {
            orch.handle_command(Command::SetThreadTitle {
                id: thread_id,
                title: format!("title-{i}"),
            })
            .await
            .expect("set title");
        }

        let (state, version) = orch
            .event_repo
            .load_snapshot(thread_id)
            .await
            .expect("load_snapshot")
            .expect("snapshot should exist at interval boundary");
        assert_eq!(version, SNAPSHOT_INTERVAL);
        // The snapshotted view carries the last title set.
        let title = state
            .get("title")
            .and_then(|v| v.as_str())
            .expect("snapshot state has a title");
        assert_eq!(title, format!("title-{}", SNAPSHOT_INTERVAL - 1));
    }

    #[tokio::test]
    async fn handle_command_does_not_snapshot_below_interval() {
        // A version-1 aggregate is well below the interval and must not snapshot.
        let orch = make_orchestrator();
        let project = orch
            .handle_command(Command::CreateProject {
                name: "NoSnap".into(),
                root_path: "/nosnap".into(),
            })
            .await
            .expect("create project");
        let project_id = match &project.events[0].event {
            DomainEvent::ProjectCreated { id, .. } => *id,
            _ => unreachable!("CreateProject yields ProjectCreated"),
        };

        let snap = orch
            .event_repo
            .load_snapshot(project_id)
            .await
            .expect("load_snapshot");
        assert!(snap.is_none(), "no snapshot expected below the interval");
    }

    #[tokio::test]
    async fn in_memory_repo_enumerates_all_snapshots() {
        // The in-memory event repo stores snapshots and load_all_snapshots
        // returns every one, keyed by aggregate id, with state + version intact.
        let repo = InMemoryEventRepo::new();
        let a = EntityId::new();
        let b = EntityId::new();
        let c = EntityId::new();

        repo.save_snapshot(a, serde_json::json!({"k": "a"}), 10)
            .await
            .expect("save a");
        repo.save_snapshot(b, serde_json::json!({"k": "b"}), 25)
            .await
            .expect("save b");
        repo.save_snapshot(c, serde_json::json!({"k": "c"}), 50)
            .await
            .expect("save c");

        let mut all = repo.load_all_snapshots().await.expect("load all");
        all.sort_by_key(|(_, _, v)| *v);
        assert_eq!(all.len(), 3, "all three snapshots enumerated");
        assert_eq!(all[0].0, a);
        assert_eq!(all[0].2, 10);
        assert_eq!(all[1].0, b);
        assert_eq!(all[2].0, c);
        assert_eq!(all[2].2, 50);

        // Re-saving a snapshot for an existing aggregate replaces it (no dup).
        repo.save_snapshot(a, serde_json::json!({"k": "a2"}), 12)
            .await
            .expect("update a");
        let all2 = repo.load_all_snapshots().await.expect("load all 2");
        assert_eq!(all2.len(), 3, "overwrite must not duplicate");
        let a_entry = all2
            .into_iter()
            .find(|(id, _, _)| *id == a)
            .expect("a present");
        assert_eq!(a_entry.2, 12);
        assert_eq!(a_entry.1["k"], "a2");
    }

    /// Grow a single thread stream to exactly SNAPSHOT_INTERVAL events (which
    /// triggers a snapshot at version SNAPSHOT_INTERVAL) and then append ONE tail
    /// event past the boundary. Returns the orchestrator (its incrementally
    /// projected read model is the ground truth) and the thread id. The events
    /// and snapshot live in the orchestrator's shared event repo.
    async fn build_thread_across_snapshot_boundary() -> (Orchestrator, EntityId) {
        let orch = make_orchestrator();

        let project = orch
            .handle_command(Command::CreateProject {
                name: "S".into(),
                root_path: "/s".into(),
            })
            .await
            .expect("create project");
        let project_id = match &project.events[0].event {
            DomainEvent::ProjectCreated { id, .. } => *id,
            _ => unreachable!("CreateProject yields ProjectCreated"),
        };
        let thread = orch
            .handle_command(Command::CreateThread {
                project_id,
                provider_id: "p".into(),
                model: "m".into(),
                thread_id: None,
            })
            .await
            .expect("create thread");
        let thread_id = match &thread.events[0].event {
            DomainEvent::ThreadCreated { id, .. } => *id,
            _ => unreachable!("CreateThread yields ThreadCreated"),
        };
        // version 1 -> SNAPSHOT_INTERVAL (snapshot captured at the boundary)
        for i in 1..SNAPSHOT_INTERVAL {
            orch.handle_command(Command::SetThreadTitle {
                id: thread_id,
                title: format!("title-{i}"),
            })
            .await
            .expect("set title");
        }
        // tail: one event past the snapshot boundary
        orch.handle_command(Command::SetThreadTitle {
            id: thread_id,
            title: "tail".into(),
        })
        .await
        .expect("tail title");
        (orch, thread_id)
    }

    #[tokio::test]
    async fn snapshot_replay_equals_full_replay() {
        // The ground truth is the orchestrator's incrementally-projected read
        // model (each event applied in order == a full replay). A snapshot-seeded
        // cold-start replay over the same event store must reproduce the thread
        // view exactly: seed + tail == full replay.
        let (orch, thread_id) = build_thread_across_snapshot_boundary().await;
        let truth = orch.read_model_snapshot().await;

        let fresh: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let orch2 = Orchestrator::with_read_model(Arc::clone(&orch.event_repo), Arc::clone(&fresh));
        orch2.replay_read_model().await.expect("replay");
        let replayed = fresh.read().await;

        let key = thread_id.as_str();
        let truth_view = serde_json::to_value(truth.threads.get(&key)).unwrap();
        let replayed_view = serde_json::to_value(replayed.threads.get(&key)).unwrap();
        assert_eq!(
            truth_view, replayed_view,
            "snapshot-seeded replay must equal full replay"
        );
        // Sanity: the tail event was applied (final title is the tail value).
        assert_eq!(replayed_view["title"], "tail");
    }

    #[tokio::test]
    async fn snapshot_replay_applies_tail_over_seed() {
        // The persisted snapshot covers the first SNAPSHOT_INTERVAL events, so
        // its title is the last one set inside the boundary — NOT the tail. After
        // cold-start replay the tail must be layered over the seed, yielding the
        // tail title in the read model.
        let (orch, thread_id) = build_thread_across_snapshot_boundary().await;

        let (snap_state, snap_version) = orch
            .event_repo
            .load_snapshot(thread_id)
            .await
            .expect("load_snapshot")
            .expect("snapshot exists at interval boundary");
        assert_eq!(snap_version, SNAPSHOT_INTERVAL);
        assert_eq!(
            snap_state["title"],
            format!("title-{}", SNAPSHOT_INTERVAL - 1),
            "snapshot title is the pre-tail value"
        );

        let fresh: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let orch2 = Orchestrator::with_read_model(Arc::clone(&orch.event_repo), Arc::clone(&fresh));
        orch2.replay_read_model().await.expect("replay");
        let replayed = fresh.read().await;
        let view = replayed
            .threads
            .get(&thread_id.as_str())
            .expect("thread seeded + tail applied");
        assert_eq!(
            view.title.as_deref(),
            Some("tail"),
            "tail applied over seed"
        );
    }

    #[tokio::test]
    async fn snapshot_replay_falls_back_when_no_snapshots() {
        // A version-1 aggregate is below the snapshot interval: no snapshot is
        // written, so replay_read_model must fall back to a plain full replay and
        // still reconstruct the project. load_all_snapshots() returning [] is the
        // no-snapshot path (empty skip map -> every event projected).
        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let orch = Orchestrator::new(Arc::clone(&repo));
        let project = orch
            .handle_command(Command::CreateProject {
                name: "NoSnap".into(),
                root_path: "/ns".into(),
            })
            .await
            .expect("create project");
        let project_id = match &project.events[0].event {
            DomainEvent::ProjectCreated { id, .. } => *id,
            _ => unreachable!(),
        };
        // Precondition: no snapshots stored.
        assert!(
            repo.load_all_snapshots()
                .await
                .expect("load all")
                .is_empty(),
            "no snapshot below the interval"
        );

        let fresh: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let orch2 = Orchestrator::with_read_model(Arc::clone(&repo), Arc::clone(&fresh));
        orch2.replay_read_model().await.expect("replay");
        let replayed = fresh.read().await;
        let view = replayed
            .projects
            .get(&project_id.as_str())
            .expect("project replayed via full-replay fallback");
        assert_eq!(view.name, "NoSnap");
    }

    #[test]
    fn aggregate_kind_classifies_each_view() {
        // Each view kind is identified by a field unique to it; an unknown or
        // empty shape classifies as None (never snapshotted).
        let project = serde_json::json!({
            "id": "p", "name": "n", "root_path": "/r", "thread_count": 0,
            "created_at": "t", "updated_at": "t"
        });
        assert_eq!(aggregate_kind(&project), Some(AggregateKind::Project));

        let thread = serde_json::json!({
            "id": "t", "project_id": "p", "provider_id": "pr", "model": "m",
            "status": "active", "runtime_mode": "approval-required",
            "interaction_mode": "default", "turn_count": 0
        });
        assert_eq!(aggregate_kind(&thread), Some(AggregateKind::Thread));

        let turn = serde_json::json!({"user_input": "hi", "sequence": 1});
        assert_eq!(aggregate_kind(&turn), Some(AggregateKind::Turn));

        let message = serde_json::json!({"role": "user", "content": "hi"});
        assert_eq!(aggregate_kind(&message), Some(AggregateKind::Message));

        let pinned = serde_json::json!({"pinned_at": "t", "done": false});
        assert_eq!(aggregate_kind(&pinned), Some(AggregateKind::PinnedMessage));

        let marker = serde_json::json!({"marker_id": "m", "selected_text": "x"});
        assert_eq!(aggregate_kind(&marker), Some(AggregateKind::Marker));

        // Unknown / empty -> None.
        assert_eq!(aggregate_kind(&serde_json::json!({})), None);
        assert_eq!(
            aggregate_kind(&serde_json::json!({"activity_type": "x"})),
            None,
            "activities are never snapshotted"
        );
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
        assert_eq!(
            envelopes.len(),
            2,
            "both provider events should be ingested"
        );
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
            Ok(syncode_provider::ProviderEvent::Started {
                session_id: "s1".into(),
            }),
        ]));

        consume_provider_stream(
            stream,
            Arc::clone(&repo),
            Arc::clone(&read_model),
            None,
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
        assert_eq!(
            rm.activities.len(),
            1,
            "ToolCall should project one activity"
        );
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

        let stream: syncode_provider::ProviderStream = Box::pin(tokio_stream::iter(vec![Ok(
            syncode_provider::ProviderEvent::ToolCall {
                session_id: "s1".into(),
                tool_name: "grep".into(),
                tool_input: serde_json::json!({"q": "foo"}),
            },
        )]));
        consume_provider_stream(
            stream,
            Arc::clone(&repo),
            Arc::clone(&read_model),
            None,
            turn_id,
            "s1".into(),
        )
        .await;

        let rm = read_model.read().await;
        assert_eq!(
            rm.activities.len(),
            1,
            "ToolCall should produce one activity"
        );
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

    /// A [`DomainEventPublisher`] fake that records every publish call. Used to
    /// assert the pipeline fans appended events out to the bus.
    struct RecordingPublisher {
        calls: std::sync::Mutex<Vec<(String, String, String, serde_json::Value)>>,
    }

    impl RecordingPublisher {
        fn new() -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl DomainEventPublisher for RecordingPublisher {
        async fn publish(
            &self,
            channel: &str,
            event_type: &str,
            aggregate_id: &str,
            data: serde_json::Value,
        ) -> Result<(), PortError> {
            self.calls.lock().unwrap().push((
                channel.to_string(),
                event_type.to_string(),
                aggregate_id.to_string(),
                data,
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn handle_command_pushes_domain_events_to_publisher() {
        // When an event publisher is wired, the command's produced domain events
        // must be pushed to the bus on the "orchestration" channel, in addition
        // to being persisted and projected.
        let repo = Arc::new(InMemoryEventRepo::new());
        let recorder = Arc::new(RecordingPublisher::new());
        let publisher: Arc<dyn DomainEventPublisher> = recorder.clone();
        let orch = Orchestrator::new(repo).with_event_publisher(publisher);

        let result = orch
            .handle_command(Command::CreateProject {
                name: "Push".into(),
                root_path: "/push".into(),
            })
            .await
            .expect("create project");
        assert_eq!(result.events.len(), 1);

        let calls = recorder.calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            1,
            "the single ProjectCreated event should be pushed exactly once"
        );
        assert_eq!(
            calls[0].0, "orchestration",
            "pushed on the orchestration channel"
        );
        assert_eq!(
            calls[0].1, "ProjectCreated",
            "pushed with the event type name"
        );
        // The pushed aggregate id matches the produced event's aggregate id.
        let pushed_agg = result.events[0].event.aggregate_id().to_string();
        assert_eq!(
            calls[0].2, pushed_agg,
            "pushed aggregate id matches the event's"
        );
    }

    #[tokio::test]
    async fn handle_command_publish_failure_does_not_fail_command() {
        // A publisher that always errors must NOT fail the command — publishing
        // is best-effort; the events are already persisted.
        struct FailingPublisher;
        #[async_trait::async_trait]
        impl DomainEventPublisher for FailingPublisher {
            async fn publish(
                &self,
                _: &str,
                _: &str,
                _: &str,
                _: serde_json::Value,
            ) -> Result<(), PortError> {
                Err(PortError::Internal("bus down".into()))
            }
        }

        let repo = Arc::new(InMemoryEventRepo::new());
        let orch = Orchestrator::new(repo).with_event_publisher(Arc::new(FailingPublisher));

        let result = orch
            .handle_command(Command::CreateProject {
                name: "Resilient".into(),
                root_path: "/resilient".into(),
            })
            .await;

        assert!(
            result.is_ok(),
            "command must succeed despite a publish failure"
        );
        assert_eq!(result.unwrap().events.len(), 1);
    }

    #[tokio::test]
    async fn consume_provider_stream_pushes_domain_events_to_publisher() {
        // Provider-stream-sourced events take the same push path as command
        // events: each ingested domain event is pushed on the orchestration
        // channel (ToolCall -> ActivityLogged, Completed -> TurnCompleted).
        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let read_model: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let recorder = Arc::new(RecordingPublisher::new());
        let publisher: Arc<dyn DomainEventPublisher> = recorder.clone();
        let turn_id = EntityId::new();

        let stream: syncode_provider::ProviderStream = Box::pin(tokio_stream::iter(vec![
            Ok(syncode_provider::ProviderEvent::ToolCall {
                session_id: "s1".into(),
                tool_name: "grep".into(),
                tool_input: serde_json::json!({"q": "x"}),
            }),
            Ok(syncode_provider::ProviderEvent::Completed {
                session_id: "s1".into(),
                output: "done".into(),
                usage: None,
            }),
        ]));

        consume_provider_stream(
            stream,
            Arc::clone(&repo),
            Arc::clone(&read_model),
            Some(publisher),
            turn_id,
            "s1".into(),
        )
        .await;

        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls.len(), 2, "both stream events should be pushed");
        assert!(
            calls.iter().any(|(_, et, _, _)| et == "ActivityLogged"),
            "ToolCall should push an ActivityLogged"
        );
        assert!(
            calls.iter().any(|(_, et, _, _)| et == "TurnCompleted"),
            "Completed should push a TurnCompleted"
        );
        assert!(
            calls.iter().all(|(ch, _, _, _)| ch == "orchestration"),
            "all stream events push on the orchestration channel"
        );
    }

    #[tokio::test]
    async fn consume_provider_stream_tokens_become_message_delta() {
        // P0-2: Token ProviderEvents must produce MessageDeltaAppended domain
        // events (no longer silently consumed). A short stream of tokens flushed
        // at stream end yields ONE MessageDeltaAppended whose delta is the
        // concatenation of all token text. The streamed assistant message is
        // keyed by the turn id and materialized in the read model.
        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let read_model: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let turn_id = EntityId::new();

        let stream: syncode_provider::ProviderStream = Box::pin(tokio_stream::iter(vec![
            Ok(syncode_provider::ProviderEvent::Token {
                session_id: "s1".into(),
                content: "Hello ".into(),
            }),
            Ok(syncode_provider::ProviderEvent::Token {
                session_id: "s1".into(),
                content: "world".into(),
            }),
        ]));

        consume_provider_stream(
            stream,
            Arc::clone(&repo),
            Arc::clone(&read_model),
            None,
            turn_id,
            "s1".into(),
        )
        .await;

        // Exactly ONE MessageDeltaAppended appended to the turn stream (the two
        // tokens were batched and flushed once at stream end).
        let envelopes = repo.replay_events(turn_id).await.expect("replay");
        assert_eq!(envelopes.len(), 1, "batched tokens flush as one event");
        match &envelopes[0].event {
            DomainEvent::MessageDeltaAppended {
                id,
                turn_id: tid,
                delta,
                ..
            } => {
                assert_eq!(*id, turn_id, "message id is the turn id");
                assert_eq!(*tid, turn_id);
                assert_eq!(delta, "Hello world", "token text is concatenated");
            }
            other => panic!("expected MessageDeltaAppended, got {other:?}"),
        }

        // The streamed assistant message is materialized in the read model,
        // keyed by the turn id, marked as still-streaming.
        let rm = read_model.read().await;
        let msg = rm
            .messages
            .get(&turn_id.as_str())
            .expect("streamed assistant message projected");
        assert_eq!(msg.role, "assistant");
        assert_eq!(msg.content, "Hello world");
        assert!(
            msg.is_streaming,
            "message remains streaming until finalized"
        );
    }

    #[tokio::test]
    async fn consume_provider_stream_tokens_pushed_to_publisher() {
        // P0-2: batched token deltas are pushed to subscribed WS clients via the
        // event publisher in real time. Each flushed batch produces exactly one
        // MessageDeltaAppended push on the orchestration channel.
        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let read_model: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let recorder = Arc::new(RecordingPublisher::new());
        let publisher: Arc<dyn DomainEventPublisher> = recorder.clone();
        let turn_id = EntityId::new();

        let stream: syncode_provider::ProviderStream = Box::pin(tokio_stream::iter(vec![
            Ok(syncode_provider::ProviderEvent::Token {
                session_id: "s1".into(),
                content: "chunk-".into(),
            }),
            Ok(syncode_provider::ProviderEvent::Token {
                session_id: "s1".into(),
                content: "1".into(),
            }),
        ]));

        consume_provider_stream(
            stream,
            Arc::clone(&repo),
            Arc::clone(&read_model),
            Some(publisher),
            turn_id,
            "s1".into(),
        )
        .await;

        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "one batched delta is pushed");
        assert_eq!(
            calls[0].0, "orchestration",
            "pushed on orchestration channel"
        );
        assert_eq!(
            calls[0].1, "MessageDeltaAppended",
            "pushed as MessageDeltaAppended"
        );
        assert_eq!(
            calls[0].2,
            turn_id.to_string(),
            "aggregate id is the turn id"
        );
        // The pushed payload carries the concatenated delta.
        let delta = calls[0].3["data"]["delta"]
            .as_str()
            .expect("delta present in pushed payload");
        assert_eq!(delta, "chunk-1");
    }

    #[tokio::test]
    async fn consume_provider_stream_count_threshold_flushes_early() {
        // P0-2: when TOKEN_BATCH_MAX_COUNT token chunks accumulate before the
        // 100ms window elapses, the buffer is flushed early (count threshold).
        // We feed TOKEN_BATCH_MAX_COUNT + 2 tokens with no delay and expect at
        // least TWO MessageDeltaAppended events (one at the count threshold,
        // one at stream end), proving the count-based flush fires.
        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let read_model: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let turn_id = EntityId::new();

        let total = TOKEN_BATCH_MAX_COUNT + 2;
        let events: Vec<
            Result<syncode_provider::ProviderEvent, syncode_provider::ProviderAdapterError>,
        > = (0..total)
            .map(|i| {
                Ok(syncode_provider::ProviderEvent::Token {
                    session_id: "s1".into(),
                    content: format!("t{i} "),
                })
            })
            .collect();
        let stream: syncode_provider::ProviderStream = Box::pin(tokio_stream::iter(events));

        consume_provider_stream(
            stream,
            Arc::clone(&repo),
            Arc::clone(&read_model),
            None,
            turn_id,
            "s1".into(),
        )
        .await;

        let envelopes = repo.replay_events(turn_id).await.expect("replay");
        // At least two flushes: one triggered by the count threshold, one at
        // stream end (the remaining tokens). All events are MessageDeltaAppended.
        assert!(
            envelopes.len() >= 2,
            "count threshold should trigger at least one early flush, got {} events",
            envelopes.len()
        );
        assert!(
            envelopes
                .iter()
                .all(|env| matches!(env.event, DomainEvent::MessageDeltaAppended { .. }))
        );

        // No token text is lost: concatenating every flushed delta reproduces
        // the full original stream content in order.
        let mut reconstructed = String::new();
        for env in &envelopes {
            if let DomainEvent::MessageDeltaAppended { delta, .. } = &env.event {
                reconstructed.push_str(delta);
            }
        }
        let expected: String = (0..total).map(|i| format!("t{i} ")).collect();
        assert_eq!(reconstructed, expected, "no token text lost across batches");

        // The read model reflects the full concatenated content.
        let rm = read_model.read().await;
        let msg = rm
            .messages
            .get(&turn_id.as_str())
            .expect("message projected");
        assert_eq!(msg.content, expected);
    }

    #[tokio::test]
    async fn consume_provider_stream_time_window_flushes_mid_stream() {
        // P0-2: the 100ms time window flushes buffered tokens even while the
        // stream is still open. We interleave token chunks with 120ms sleeps so
        // the timer fires between them, producing TWO batched deltas (one per
        // window) rather than one at stream end.
        use std::time::Duration;
        use tokio_stream::StreamExt;

        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let read_model: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));

        let turn_id = EntityId::new();

        // Build a stream that emits a token, sleeps 120ms, emits another token,
        // then ends. The first token should be flushed by the 100ms tick before
        // the second arrives.
        let stream: syncode_provider::ProviderStream = {
            let s = tokio_stream::iter(vec![
                Ok(syncode_provider::ProviderEvent::Token {
                    session_id: "s1".into(),
                    content: "first ".into(),
                }),
                Ok(syncode_provider::ProviderEvent::Token {
                    session_id: "s1".into(),
                    content: "second".into(),
                }),
            ]);
            // Interleave a 120ms delay after the first item using then.
            let delayed = s.then(|item| async move {
                tokio::time::sleep(Duration::from_millis(120)).await;
                item
            });
            Box::pin(delayed)
        };

        consume_provider_stream(
            stream,
            Arc::clone(&repo),
            Arc::clone(&read_model),
            None,
            turn_id,
            "s1".into(),
        )
        .await;

        let envelopes = repo.replay_events(turn_id).await.expect("replay");
        // Two separate flushes: the 100ms window fired between the two tokens.
        assert_eq!(
            envelopes.len(),
            2,
            "time window should flush the first token before the second arrives"
        );
        assert!(
            envelopes
                .iter()
                .all(|env| matches!(env.event, DomainEvent::MessageDeltaAppended { .. }))
        );
        // Ordering preserved: first delta precedes second.
        let deltas: Vec<String> = envelopes
            .iter()
            .map(|env| match &env.event {
                DomainEvent::MessageDeltaAppended { delta, .. } => delta.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(deltas[0], "first ");
        assert_eq!(deltas[1], "second");
    }

    #[tokio::test]
    async fn consume_provider_stream_flushes_tokens_before_non_token_event() {
        // P0-2: ordering — a non-token event (Completed) flushes any pending
        // token buffer first, so the streamed delta reaches clients BEFORE the
        // TurnCompleted. Two tokens then a Completed -> [delta, TurnCompleted].
        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let read_model: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let turn_id = EntityId::new();

        let stream: syncode_provider::ProviderStream = Box::pin(tokio_stream::iter(vec![
            Ok(syncode_provider::ProviderEvent::Token {
                session_id: "s1".into(),
                content: "al".into(),
            }),
            Ok(syncode_provider::ProviderEvent::Token {
                session_id: "s1".into(),
                content: "pha".into(),
            }),
            Ok(syncode_provider::ProviderEvent::Completed {
                session_id: "s1".into(),
                output: "alpha".into(),
                usage: None,
            }),
        ]));

        consume_provider_stream(
            stream,
            Arc::clone(&repo),
            Arc::clone(&read_model),
            None,
            turn_id,
            "s1".into(),
        )
        .await;

        let envelopes = repo.replay_events(turn_id).await.expect("replay");
        assert_eq!(envelopes.len(), 2, "delta flush + TurnCompleted");
        assert!(matches!(
            envelopes[0].event,
            DomainEvent::MessageDeltaAppended { .. }
        ));
        assert!(matches!(
            envelopes[1].event,
            DomainEvent::TurnCompleted { .. }
        ));
    }

    #[tokio::test]
    async fn test_create_project() {
        let orch = make_orchestrator();
        let result = orch
            .handle_command(Command::CreateProject {
                name: "Test".into(),
                root_path: "/test".into(),
            })
            .await
            .expect("handle command");

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
        let result = orch
            .handle_command(Command::CreateThread {
                project_id: EntityId::new(),
                provider_id: "anthropic".into(),
                model: "claude-3".into(),
                thread_id: None,
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
                thread_id: None,
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
        })
        .await
        .expect("create project");

        let read_model = orch.read_model_snapshot().await;
        assert_eq!(read_model.projects.len(), 1);
        assert_eq!(
            read_model.projects.values().next().unwrap().name,
            "Snapshot Test"
        );
    }

    #[tokio::test]
    async fn test_concurrency_conflict() {
        let orch = make_orchestrator();

        // Create project
        orch.handle_command(Command::CreateProject {
            name: "P".into(),
            root_path: "/p".into(),
        })
        .await
        .expect("first");

        // Second create produces a different aggregate — should succeed
        let result = orch
            .handle_command(Command::CreateProject {
                name: "P2".into(),
                root_path: "/p2".into(),
            })
            .await
            .expect("second");

        assert_eq!(result.events.len(), 1);
    }

    #[tokio::test]
    async fn test_replay_read_model() {
        let orch = make_orchestrator();

        orch.handle_command(Command::CreateProject {
            name: "Replay".into(),
            root_path: "/replay".into(),
        })
        .await
        .expect("create");

        // Reset read model
        {
            let mut rm = orch.read_model.write().await;
            *rm = ReadModelStore::new();
        }

        // Read model should be empty
        let snap = orch.read_model_snapshot().await;
        assert_eq!(snap.projects.len(), 0);

        // Replay
        let (count, _seeded) = orch.replay_read_model().await.expect("replay");
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
                thread_id: None,
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

    // ─── PR-1-3: E2E chat verification ──────────────────────────────────
    //
    // Verifies the full chat round-trip pipeline with a mocked provider
    // adapter: send message → provider responds → message appears in the read
    // model. This is the E2E proof that the StartTurn → TurnCompleted pipeline
    // works end-to-end and that the working_dir fix (PR-1-2) is in place.
    //
    // The mock adapter records every call (sessions started, requests sent) so
    // the test asserts on observable behavior without a real provider process.
    // If the `claude` CLI binary is available locally, a manual verification
    // can be run via `cargo run -p syncode-ws --bin server` + the desktop UI.

    #[tokio::test]
    async fn e2e_chat_send_message_provider_responds_message_appears() {
        // Build the full armed pipeline: orchestrator + command reactor + mock
        // adapter. The reactor is wired with the orchestrator's shared read
        // model so handle_start_turn resolves the thread's project root path
        // (the PR-1-2 fix) instead of falling back to /tmp/syncode.
        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryEventRepo::new());
        let read_model: Arc<tokio::sync::RwLock<ReadModelStore>> =
            Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
        let reactor = Arc::new(
            ProviderCommandReactor::new(SessionManager::new())
                .with_read_model(Arc::clone(&read_model)),
        );
        let (adapter, _stopped, requests, working_dirs) =
            crate::reactors::command::tests::make_working_dir_recording_mock();
        let orch =
            Orchestrator::with_reactor_adapter_and_read_model(repo, reactor, adapter, read_model);

        // 1. Create a project whose root_path is the user's actual project dir.
        let project_root = "/home/user/my-chat-project";
        let project_result = orch
            .handle_command(Command::CreateProject {
                name: "Chat E2E".into(),
                root_path: project_root.into(),
            })
            .await
            .expect("create project");
        let project_id = project_result
            .events
            .iter()
            .find_map(|env| match &env.event {
                DomainEvent::ProjectCreated { id, .. } => Some(*id),
                _ => None,
            })
            .expect("ProjectCreated produced");

        // 2. Create a thread in that project.
        let thread_result = orch
            .handle_command(Command::CreateThread {
                project_id,
                provider_id: "claude".into(),
                model: "claude-sonnet".into(),
                thread_id: None,
            })
            .await
            .expect("create thread");
        let thread_id = thread_result
            .events
            .iter()
            .find_map(|env| match &env.event {
                DomainEvent::ThreadCreated { id, .. } => Some(*id),
                _ => None,
            })
            .expect("ThreadCreated produced");

        // 3. SEND MESSAGE: StartTurn. This is the user sending a chat message.
        //    The pipeline must (a) produce TurnStarted, and (b) trigger the
        //    command reactor which starts a provider session and dispatches the
        //    initial "chat" request to the mock adapter.
        let user_message = "Hello, can you explain this codebase?";
        let start_result = orch
            .handle_command(Command::StartTurn {
                thread_id,
                sequence: 1,
                user_input: user_message.into(),
            })
            .await
            .expect("start turn");

        // The turn was started.
        assert!(
            !start_result.events.is_empty(),
            "StartTurn must produce events"
        );
        let turn_id = start_result
            .events
            .iter()
            .find_map(|env| match &env.event {
                DomainEvent::TurnStarted { id, .. } => Some(*id),
                _ => None,
            })
            .expect("TurnStarted produced");

        // The provider side effect fired (reactor → adapter).
        assert!(
            start_result.side_effect_triggered,
            "StartTurn should trigger the provider side effect (session + request)"
        );

        // 4. PROVIDER RESPONDS: the adapter received the message. The mock
        //    records the dispatched "chat" request carrying the user input.
        let recorded = requests.lock().unwrap().clone();
        let chat_dispatch = recorded
            .iter()
            .find(|(method, _)| method == "chat")
            .expect("reactor should have dispatched the initial chat request");
        let chat_params = chat_dispatch
            .1
            .as_ref()
            .expect("chat request must carry params");
        assert_eq!(
            chat_params["input"].as_str(),
            Some(user_message),
            "the dispatched request must carry the user's message"
        );

        // PR-1-2 verification: the provider session was started with the
        // project's root_path as its working directory (not the /tmp fallback).
        let dirs = working_dirs.lock().unwrap().clone();
        assert_eq!(dirs.len(), 1, "exactly one session should have started");
        assert_eq!(
            dirs[0], project_root,
            "session working_dir must be the project root_path (PR-1-2 fix), got {}",
            dirs[0]
        );

        // 5. TURN COMPLETES: this mock's `send_request` returns Ok with an empty
        //    event stream (no terminal provider event). The reactor's safety net
        //    (#184, the mcode `handleStreamExit` equivalent) therefore synthesizes
        //    a `Completed` event, which the pipeline ingests to drive `CompleteTurn`
        //    — so the turn reaches "completed" WITHOUT a manual `CompleteTurn`
        //    call. (Previously this test called `CompleteTurn` manually; after
        //    #184 that fails with `TurnAlreadyCompleted`.) The "provider response
        //    text appears" path is covered by the event-emitting mock in
        //    `start_turn_captures_events_from_synchronous_adapter`.
        let snapshot = orch.read_model_snapshot().await;
        let turn_view = snapshot
            .turns
            .get(&turn_id.as_str())
            .expect("turn exists in read model");
        assert_eq!(
            turn_view.status, "completed",
            "the safety-net Completed must complete the turn"
        );

        // And the thread's turn_count advanced.
        let thread_view = snapshot
            .threads
            .get(&thread_id.as_str())
            .expect("thread exists in read model");
        assert_eq!(
            thread_view.turn_count, 1,
            "the thread must reflect the completed turn"
        );
    }
}

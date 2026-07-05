//! Push bus — ordered event delivery to subscribed connections
//!
//! The push bus listens to the broadcast channel and delivers push events
//! to WebSocket connections that are subscribed to the relevant channel.
//! Each push event is a JSON-RPC notification (no id = no response expected).

use crate::{ConnectionId, channels::ChannelSubscription};
use serde_json::{Value, json};
use std::collections::HashMap;
use syncode_core::ports::{DomainEventPublisher, PortError};

/// Push event types that can be broadcast
#[derive(Debug, Clone)]
pub enum PushEvent {
    /// Domain event from the orchestration layer
    DomainEvent {
        channel: String,
        event_type: String,
        aggregate_id: String,
        data: Value,
    },
    /// Provider status change
    ProviderStatus {
        provider_id: String,
        status: String,
        message: Option<String>,
    },
    /// Progress update (turn processing, git operation, etc.)
    Progress {
        channel: String,
        id: String,
        progress: f64,
        message: Option<String>,
    },
    /// Terminal output
    TerminalOutput {
        session_id: String,
        data: String,
        is_error: bool,
    },
    /// Generic event
    Custom {
        channel: String,
        event_type: String,
        data: Value,
    },
}

impl PushEvent {
    /// Get the channel this event belongs to
    pub fn channel(&self) -> &str {
        match self {
            Self::DomainEvent { channel, .. }
            | Self::Progress { channel, .. }
            | Self::Custom { channel, .. } => channel,
            Self::ProviderStatus { .. } => "provider",
            Self::TerminalOutput { .. } => "terminal",
        }
    }
}

/// A [`DomainEventPublisher`] backed by the WebSocket push bus.
///
/// The orchestration pipeline calls [`DomainEventPublisher::publish`] after it
/// appends and projects a domain event. Each published event is broadcast on
/// `push_tx` as `(channel, data)`, where `data` packs the event type, aggregate
/// id, and serialized event payload. The push delivery loop then fans the
/// broadcast out to connections subscribed to `channel`.
///
/// Publishing is best-effort: if there are no receivers yet (normal before any
/// client connects), `broadcast::send` returns `SendError`, which we treat as
/// success — the event is still durably persisted and projected upstream, so the
/// absence of a live subscriber is not a publish failure.
#[derive(Clone)]
pub struct WsDomainEventPublisher {
    push_tx: tokio::sync::broadcast::Sender<(String, serde_json::Value)>,
}

impl WsDomainEventPublisher {
    /// Wrap a push-bus broadcast sender as a [`DomainEventPublisher`].
    pub fn new(push_tx: tokio::sync::broadcast::Sender<(String, serde_json::Value)>) -> Self {
        Self { push_tx }
    }
}

#[async_trait::async_trait]
impl DomainEventPublisher for WsDomainEventPublisher {
    async fn publish(
        &self,
        channel: &str,
        event_type: &str,
        aggregate_id: &str,
        data: serde_json::Value,
    ) -> Result<(), PortError> {
        // camelCase keys — wire parity with T4's `OrchestrationPushEnvelope`
        // type model (frontend/src/contracts/events.ts) and the cloned MCode
        // frontend's Effect-Schema expectations. See CONTRACTS-BRIDGE-DESIGN
        // §3.3 (camelCase canonical at the contracts boundary) + B3.
        let envelope = json!({
            "eventType": event_type,
            "aggregateId": aggregate_id,
            "data": data,
        });
        // No receivers is not an error — it is normal before any client
        // subscribes. Only an unusable bus should surface as an error.
        let _ = self.push_tx.send((channel.to_string(), envelope));
        Ok(())
    }
}

/// Subscription registry — maps connection IDs to their channel subscriptions
#[derive(Debug, Clone, Default)]
pub struct SubscriptionRegistry {
    subscriptions: HashMap<ConnectionId, ChannelSubscription>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new connection with empty subscriptions
    pub fn register(&mut self, conn_id: ConnectionId) {
        self.subscriptions
            .insert(conn_id, ChannelSubscription::new());
    }

    /// Remove a connection and its subscriptions
    pub fn unregister(&mut self, conn_id: ConnectionId) {
        self.subscriptions.remove(&conn_id);
    }

    /// Subscribe a connection to a channel
    pub fn subscribe(&mut self, conn_id: ConnectionId, channel: impl Into<String>) -> bool {
        if let Some(sub) = self.subscriptions.get_mut(&conn_id) {
            sub.subscribe(channel)
        } else {
            false
        }
    }

    /// Unsubscribe a connection from a channel
    pub fn unsubscribe(&mut self, conn_id: ConnectionId, channel: impl AsRef<str>) -> bool {
        if let Some(sub) = self.subscriptions.get_mut(&conn_id) {
            sub.unsubscribe(channel)
        } else {
            false
        }
    }

    /// Get connections subscribed to a given channel
    pub fn subscribers_for(&self, channel: &str) -> Vec<ConnectionId> {
        self.subscriptions
            .iter()
            .filter(|(_, sub)| sub.is_subscribed(channel))
            .map(|(&id, _)| id)
            .collect()
    }

    /// Get subscription info for a connection
    pub fn get_subscription(&self, conn_id: ConnectionId) -> Option<&ChannelSubscription> {
        self.subscriptions.get(&conn_id)
    }
}

// Note: push delivery is performed per-connection by `run_push_delivery` in
// `server.rs`, which subscribes to `push_tx` and forwards only the channels a
// connection has opted into. There is no central dispatcher.

// ─── Automation live event push (PUSH-1) ───────────────────────────────
//
// `automation.runNow` historically called `scheduler.trigger_with_delay`,
// which awaits `execute_run` synchronously — so the only push a subscriber
// received was the final `run-upserted` (broadcast by the RPC handler after
// the run finished). PUSH-1 adds live in-flight events (`run-started` /
// `run-progress` / `run-completed`) on `CHANNEL_AUTOMATION`, mirroring the
// terminal reader-task pattern.
//
// `AutomationPushSink` is the bridge: it implements
// `syncode_automation::RunEventSink` and forwards each `RunEvent` onto
// `push_tx` as a JSON-RPC-style push frame. The handler constructs one per
// `runNow` call (cheap — a `broadcast::Sender` clone) and passes it to
// `scheduler.trigger_with_events`, which threads it through the run context
// into `ProcessRunExecutor::dispatch_turn_live`.

/// A [`syncode_automation::RunEventSink`] that forwards automation run-events
/// onto the WebSocket push bus (PUSH-1).
///
/// Each event is broadcast on `push_tx` as `(CHANNEL_AUTOMATION, <payload>)`,
/// where `<payload>` carries the wire `type` (`run-started` / `run-progress` /
/// `run-completed`) plus the run id + automation id + the kind-specific
/// fields. Subscribers on the `automation` channel receive them via the
/// standard push delivery loop.
///
/// Best-effort like all push: `broadcast::send` errors when there are no
/// receivers (normal before any client subscribes) — swallowed, never
/// propagated. A run must never fail because nobody is listening.
#[derive(Clone)]
pub struct AutomationPushSink {
    push_tx: tokio::sync::broadcast::Sender<(String, serde_json::Value)>,
}

impl AutomationPushSink {
    /// Wrap a push-bus broadcast sender as an automation [`RunEventSink`].
    pub fn new(push_tx: tokio::sync::broadcast::Sender<(String, serde_json::Value)>) -> Self {
        Self { push_tx }
    }
}

impl syncode_automation::RunEventSink for AutomationPushSink {
    fn emit(
        &self,
        event: syncode_automation::RunEvent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        let tx = self.push_tx.clone();
        Box::pin(async move {
            use syncode_automation::RunEventKind;
            let payload = match &event.kind {
                RunEventKind::Started { started_at } => serde_json::json!({
                    "type": event.type_name(),
                    "runId": event.run_id,
                    "automationId": event.automation_id,
                    "startedAt": started_at,
                }),
                RunEventKind::Progress { progress, message } => serde_json::json!({
                    "type": event.type_name(),
                    "runId": event.run_id,
                    "automationId": event.automation_id,
                    "progress": progress,
                    "message": message,
                }),
                RunEventKind::Completed { status, exit_code } => serde_json::json!({
                    "type": event.type_name(),
                    "runId": event.run_id,
                    "automationId": event.automation_id,
                    "status": status,
                    "exitCode": exit_code,
                }),
            };
            // No receivers is normal — not a failure.
            let _ = tx.send((crate::channels::CHANNEL_AUTOMATION.to_string(), payload));
        })
    }
}

// ─── Snapshot-then-stream ─────────────────────────────────────────────
//
// When a client subscribes to a channel (or reconnects and re-subscribes),
// the server emits an initial snapshot of the current read-model state for
// that channel's scope, then continues with live deltas. This is the
// server's half of MCode's reconnect-resilience model: the client owns the
// reconnect/backoff; the server guarantees a freshly-subscribed connection
// sees current state before any live event it might otherwise miss.
//
// Ordering is race-free: `handle_push_subscribe` records the subscription
// BEFORE building the snapshot. Any event projected after the snapshot read
// is therefore guaranteed to be delivered live (the subscription was already
// in place when the event was published).

use syncode_contracts::snapshots as dto;
use syncode_orchestration::read_model as rm;

/// Map an orchestration `ProjectView` to the contracts `ProjectSummary` DTO.
fn project_summary(p: &rm::ProjectView) -> dto::ProjectSummary {
    dto::ProjectSummary {
        id: p.id.clone(),
        name: p.name.clone(),
        root_path: p.root_path.clone(),
        provider_id: p.provider_id.clone(),
        default_model: p.default_model.clone(),
        created_at: p.created_at.clone(),
        updated_at: p.updated_at.clone(),
        thread_count: p.thread_count,
    }
}

/// Map a Rust provider_id to the MCode frontend `ProviderKind` string.
/// The frontend uses "claudeAgent" while the Rust backend uses "claude".
fn to_mcode_provider_kind(provider_id: &str) -> String {
    match provider_id {
        "claude" => "claudeAgent".to_string(),
        other => other.to_string(),
    }
}

/// Map an orchestration `ThreadView` to the MCode-compatible shell snapshot
/// thread shape. The frontend expects `modelSelection: { provider, model }`
/// rather than flat `provider_id` + `model` fields.
fn thread_summary(t: &rm::ThreadView) -> serde_json::Value {
    serde_json::json!({
        "id": t.id,
        "projectId": t.project_id,
        "title": t.title.clone().unwrap_or_default(),
        "modelSelection": {
            "provider": to_mcode_provider_kind(&t.provider_id),
            "model": t.model,
        },
        "runtimeMode": t.runtime_mode,
        "interactionMode": t.interaction_mode,
        "branch": serde_json::Value::Null,
        "worktreePath": serde_json::Value::Null,
        "turn_count": t.turn_count,
        "created_at": t.created_at,
        "updated_at": t.updated_at,
        // Legacy flat fields (backward compat for code paths that still read them)
        "provider_id": t.provider_id,
        "model": t.model,
        "status": t.status,
        "git_checkpoint": t.git_checkpoint,
    })
}

/// Map an orchestration `TurnView` to the contracts `TurnSummary` DTO.
fn turn_summary(t: &rm::TurnView) -> dto::TurnSummary {
    dto::TurnSummary {
        id: t.id.clone(),
        thread_id: t.thread_id.clone(),
        sequence: t.sequence,
        user_input: t.user_input.clone(),
        assistant_output: t.assistant_output.clone(),
        status: t.status.clone(),
        git_checkpoint: t.git_checkpoint.clone(),
        files_modified: t.files_modified.clone(),
        duration_ms: t.duration_ms,
        created_at: t.created_at.clone(),
        completed_at: t.completed_at.clone(),
    }
}

/// Map an orchestration `MessageView` to the contracts `MessageSummary` DTO.
fn message_summary(m: &rm::MessageView) -> dto::MessageSummary {
    dto::MessageSummary {
        id: m.id.clone(),
        turn_id: m.turn_id.clone(),
        role: m.role.clone(),
        content: m.content.clone(),
        content_type: m.content_type.clone(),
        token_count: m.token_count,
        tool_name: m.tool_name.clone(),
        tool_call_id: m.tool_call_id.clone(),
        created_at: m.created_at.clone(),
        is_streaming: m.is_streaming,
    }
}

/// Map an orchestration `ActivityView` to the contracts `ActivitySummary` DTO.
fn activity_summary(a: &rm::ActivityView) -> dto::ActivitySummary {
    dto::ActivitySummary {
        id: a.id.clone(),
        activity_type: a.activity_type.clone(),
        description: a.description.clone(),
        project_id: a.project_id.clone(),
        thread_id: a.thread_id.clone(),
        metadata: a.metadata.clone(),
        created_at: a.created_at.clone(),
    }
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Which snapshot to build for a given (channel, optional threadId).
///
/// - `orchestration` without a threadId → `ShellSnapshot` (all projects+threads)
/// - `orchestration` with a threadId    → `ThreadDetailSnapshot` (one thread)
/// - `*` wildcard                       → `FullSnapshot` (every collection)
/// - any other channel                  → `None` (no snapshot; future work)
fn build_snapshot(
    store: &syncode_orchestration::ReadModelStore,
    channel: &str,
    thread_id: Option<&str>,
) -> Option<serde_json::Value> {
    let snapshot_at = now_iso();
    match channel {
        crate::channels::CHANNEL_ORCHESTRATION => match thread_id {
            Some(tid) => {
                // Thread-detail snapshot: the thread + its turns + messages.
                let thread = store.threads.get(tid)?;
                let turns: Vec<_> = store
                    .turns
                    .values()
                    .filter(|t| t.thread_id == tid)
                    .map(turn_summary)
                    .collect();
                let messages: Vec<_> = store
                    .messages
                    .values()
                    .filter(|m| turns.iter().any(|t: &dto::TurnSummary| t.id == m.turn_id))
                    .map(message_summary)
                    .collect();
                let snap = serde_json::json!({
                    "scope": "thread",
                    "thread": thread_summary(thread),
                    "turns": turns,
                    "messages": messages,
                    "snapshot_at": snapshot_at,
                });
                Some(snap)
            }
            None => {
                // Build shell snapshot as raw JSON to use MCode-compatible
                // thread shape (modelSelection instead of flat provider_id/model).
                let snap = serde_json::json!({
                    "scope": "shell",
                    "projects": store.projects.values().map(project_summary).collect::<Vec<_>>(),
                    "threads": store.threads.values().map(thread_summary).collect::<Vec<_>>(),
                    "snapshot_at": snapshot_at,
                });
                Some(snap)
            }
        },
        crate::channels::CHANNEL_ALL => {
            let snap = serde_json::json!({
                "scope": "full",
                "projects": store.projects.values().map(project_summary).collect::<Vec<_>>(),
                "threads": store.threads.values().map(thread_summary).collect::<Vec<_>>(),
                "turns": store.turns.values().map(turn_summary).collect::<Vec<_>>(),
                "messages": store.messages.values().map(message_summary).collect::<Vec<_>>(),
                "activities": store.activities.iter().map(activity_summary).collect::<Vec<_>>(),
                "snapshot_at": snapshot_at,
            });
            Some(snap)
        }
        // provider/git/terminal/automation: no snapshot yet (future work).
        _ => None,
    }
}

/// Emit a snapshot push notification to a single connection's `tx`.
///
/// Builds the snapshot appropriate for `channel` (and `thread_id`, if given)
/// from the current read model and sends it as a `push/<channel>` notification
/// with `event_type: "snapshot"`. No-op if the channel has no snapshot, the
/// connection is gone, or serialization fails (best-effort, like all push).
///
/// Returns whether a snapshot was emitted.
pub async fn emit_snapshot(
    state: &crate::WsState,
    conn_id: crate::ConnectionId,
    channel: &str,
    thread_id: Option<&str>,
) -> bool {
    // Look up the connection's sender first (cheap) so we don't hold the read
    // lock on the store while waiting for the connections map.
    let tx = match state.connections.read().await.get(&conn_id).cloned() {
        Some(tx) => tx,
        None => return false, // connection gone — nothing to do
    };

    // Read-lock the store only for the duration of the snapshot build.
    let snapshot_value = {
        let store = state.read_store.read().await;
        build_snapshot(&store, channel, thread_id)
    };

    let Some(data) = snapshot_value else {
        return false; // channel has no snapshot (e.g. provider/git)
    };

    let msg = serde_json::json!({
        "jsonrpc": "2.0",
        "method": format!("push/{}", channel),
        "params": {
            "eventType": "snapshot",
            "aggregateId": serde_json::Value::Null,
            "data": data,
        },
    });
    if let Ok(msg_str) = serde_json::to_string(&msg) {
        // Best-effort: a send failure means the connection dropped between
        // the lookup and now — not an error worth propagating.
        tx.send(msg_str).is_ok()
    } else {
        false
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use crate::WsState;

    /// Build a WsState with one project + one thread + one turn seeded.
    async fn seeded_state() -> WsState {
        let state = WsState::new_in_memory(16);

        // Project + thread + turn via the orchestrator (full CQRS path).
        let cmd = syncode_orchestration::Command::CreateProject {
            name: "Demo".into(),
            root_path: "/tmp/demo".into(),
        };
        let result = state.orchestrator.handle_command(cmd).await.unwrap();
        let project_id = result.events.first().unwrap().event.aggregate_id();
        let pid = syncode_core::EntityId::parse(&project_id.as_str()).unwrap();

        let cmd = syncode_orchestration::Command::CreateThread {
            project_id: pid,
            provider_id: "anthropic".into(),
            model: "claude".into(),
        };
        let result = state.orchestrator.handle_command(cmd).await.unwrap();
        let thread_id = result.events.first().unwrap().event.aggregate_id();
        let tid = syncode_core::EntityId::parse(&thread_id.as_str()).unwrap();

        let _ = state
            .orchestrator
            .handle_command(syncode_orchestration::Command::StartTurn {
                thread_id: tid,
                sequence: 1,
                user_input: "hello".into(),
            })
            .await
            .unwrap();
        state
    }

    #[tokio::test]
    async fn shell_snapshot_emitted_on_orchestration_subscribe() {
        let state = seeded_state().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;

        let emitted = emit_snapshot(&state, 1, "orchestration", None).await;
        assert!(
            emitted,
            "orchestration subscribe should emit a shell snapshot"
        );

        let msg = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
            .await
            .expect("snapshot should be delivered")
            .unwrap();
        assert!(msg.contains("push/orchestration"));
        assert!(msg.contains("\"snapshot\""));
        assert!(msg.contains("Demo"));
        assert!(msg.contains("\"shell\""));
    }

    #[tokio::test]
    async fn thread_detail_snapshot_includes_turns() {
        let state = seeded_state().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;

        // Find the seeded thread id.
        let thread_id = {
            let store = state.read_store.read().await;
            store.threads.keys().next().cloned().unwrap()
        };

        let emitted = emit_snapshot(&state, 1, "orchestration", Some(&thread_id)).await;
        assert!(emitted, "thread-detail snapshot should emit");

        let msg = rx.recv().await.unwrap();
        assert!(msg.contains("\"thread\""));
        assert!(msg.contains("turns"));
        assert!(msg.contains("hello"));
    }

    #[tokio::test]
    async fn full_snapshot_on_wildcard_channel() {
        let state = seeded_state().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;

        let emitted = emit_snapshot(&state, 1, "*", None).await;
        assert!(emitted, "wildcard should emit a full snapshot");

        let msg = rx.recv().await.unwrap();
        assert!(msg.contains("\"full\""));
        assert!(msg.contains("Demo"));
    }

    #[tokio::test]
    async fn no_snapshot_for_provider_channel() {
        let state = seeded_state().await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;

        let emitted = emit_snapshot(&state, 1, "provider", None).await;
        assert!(!emitted, "provider channel has no snapshot yet");
    }

    #[tokio::test]
    async fn no_snapshot_when_connection_gone() {
        let state = seeded_state().await;
        // No register(1, ..) — connection 1 doesn't exist.
        let emitted = emit_snapshot(&state, 1, "orchestration", None).await;
        assert!(!emitted, "should not emit for a missing connection");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_registry_lifecycle() {
        let mut reg = SubscriptionRegistry::new();
        reg.register(1);
        reg.register(2);

        assert!(reg.subscribe(1, "orchestration"));
        assert!(reg.subscribe(2, "git"));
        assert!(reg.subscribe(1, "git"));

        // Connection 1 subscribes to orchestration + git
        // Connection 2 subscribes to git
        let orch_subs = reg.subscribers_for("orchestration");
        assert_eq!(orch_subs, vec![1]);

        let git_subs = reg.subscribers_for("git");
        // Order not guaranteed, check membership
        assert_eq!(git_subs.len(), 2);
        assert!(git_subs.contains(&1));
        assert!(git_subs.contains(&2));

        reg.unregister(1);
        let orch_subs = reg.subscribers_for("orchestration");
        assert!(orch_subs.is_empty());
    }

    #[test]
    fn push_event_channel_extraction() {
        let ev = PushEvent::DomainEvent {
            channel: "orchestration".into(),
            event_type: "ThreadCreated".into(),
            aggregate_id: "abc".into(),
            data: json!({}),
        };
        assert_eq!(ev.channel(), "orchestration");

        let ev = PushEvent::ProviderStatus {
            provider_id: "anthropic".into(),
            status: "ready".into(),
            message: None,
        };
        assert_eq!(ev.channel(), "provider");

        let ev = PushEvent::TerminalOutput {
            session_id: "sess1".into(),
            data: "hello".into(),
            is_error: false,
        };
        assert_eq!(ev.channel(), "terminal");
    }

    #[tokio::test]
    async fn ws_publisher_broadcasts_packed_envelope() {
        // A published domain event is broadcast on push_tx as (channel, data),
        // where data packs the event type, aggregate id, and payload — the shape
        // a downstream push-delivery loop consumes.
        let (push_tx, mut rx) = tokio::sync::broadcast::channel::<(String, serde_json::Value)>(8);
        let publisher = WsDomainEventPublisher::new(push_tx);

        publisher
            .publish(
                "orchestration",
                "ProjectCreated",
                "agg-1",
                json!({"id": "agg-1", "name": "Demo"}),
            )
            .await
            .expect("publish should succeed");

        let (channel, data) = rx.recv().await.expect("should receive the broadcast");
        assert_eq!(channel, "orchestration");
        assert_eq!(data["eventType"], "ProjectCreated");
        assert_eq!(data["aggregateId"], "agg-1");
        assert_eq!(data["data"]["id"], "agg-1");
        assert_eq!(data["data"]["name"], "Demo");
    }

    #[tokio::test]
    async fn ws_publisher_succeeds_with_no_receivers() {
        // Before any client subscribes there are no receivers. broadcast::send
        // returns SendError in that case, but publish must still return Ok —
        // the absence of a live subscriber is not a publish failure.
        let (push_tx, _) = tokio::sync::broadcast::channel::<(String, serde_json::Value)>(8);
        let publisher = WsDomainEventPublisher::new(push_tx);

        publisher
            .publish("orchestration", "ThreadCreated", "agg-2", json!({}))
            .await
            .expect("publish with no receivers should succeed");
    }
}

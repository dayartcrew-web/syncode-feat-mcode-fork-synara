//! Push bus — ordered event delivery to subscribed connections
//!
//! The push bus listens to the broadcast channel and delivers push events
//! to WebSocket connections that are subscribed to the relevant channel.
//! Each push event is a JSON-RPC notification (no id = no response expected).

use crate::{ConnectionId, WsState, channels::ChannelSubscription};
use serde_json::{Value, json};
use std::collections::HashMap;
use syncode_core::ports::{DomainEventPublisher, PortError};
use tracing;

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
        let envelope = json!({
            "event_type": event_type,
            "aggregate_id": aggregate_id,
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
        self.subscriptions.insert(conn_id, ChannelSubscription::new());
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

/// Format a push event as a JSON-RPC notification string
fn format_push_notification(channel: &str, event_type: &str, data: &Value) -> String {
    let notification = json!({
        "jsonrpc": "2.0",
        "method": format!("push/{}", channel),
        "params": {
            "channel": channel,
            "event": event_type,
            "data": data,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }
    });
    serde_json::to_string(&notification).unwrap_or_default()
}

/// Deliver a push event to all subscribed connections via the state's sender map.
/// This is called after the event has been broadcast on the push_tx channel.
pub async fn deliver_push_event(
    state: &WsState,
    channel: &str,
    event_type: &str,
    data: &Value,
    subscriptions: &SubscriptionRegistry,
) {
    let subscribers = subscriptions.subscribers_for(channel);
    let message = format_push_notification(channel, event_type, data);

    let connections = state.connections.read().await;
    for conn_id in subscribers {
        if let Some(tx) = connections.get(&conn_id) {
            if tx.send(message.clone()).is_err() {
                tracing::warn!(conn_id, "Failed to deliver push event — connection likely closed");
            }
        }
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

    #[test]
    fn format_push_notification_structure() {
        let msg = format_push_notification("orchestration", "ThreadCreated", &json!({"id": "abc"}));
        let parsed: Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["jsonrpc"], "2.0");
        assert_eq!(parsed["method"], "push/orchestration");
        assert!(parsed.get("id").is_none()); // notification — no id
        assert_eq!(parsed["params"]["channel"], "orchestration");
        assert_eq!(parsed["params"]["event"], "ThreadCreated");
        assert_eq!(parsed["params"]["data"]["id"], "abc");
        assert!(parsed["params"]["timestamp"].is_string());
    }

    #[tokio::test]
    async fn ws_publisher_broadcasts_packed_envelope() {
        // A published domain event is broadcast on push_tx as (channel, data),
        // where data packs the event type, aggregate id, and payload — the shape
        // a downstream push-delivery loop consumes.
        let (push_tx, mut rx) =
            tokio::sync::broadcast::channel::<(String, serde_json::Value)>(8);
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
        assert_eq!(data["event_type"], "ProjectCreated");
        assert_eq!(data["aggregate_id"], "agg-1");
        assert_eq!(data["data"]["id"], "agg-1");
        assert_eq!(data["data"]["name"], "Demo");
    }

    #[tokio::test]
    async fn ws_publisher_succeeds_with_no_receivers() {
        // Before any client subscribes there are no receivers. broadcast::send
        // returns SendError in that case, but publish must still return Ok —
        // the absence of a live subscriber is not a publish failure.
        let (push_tx, _) =
            tokio::sync::broadcast::channel::<(String, serde_json::Value)>(8);
        let publisher = WsDomainEventPublisher::new(push_tx);

        publisher
            .publish("orchestration", "ThreadCreated", "agg-2", json!({}))
            .await
            .expect("publish with no receivers should succeed");
    }
}

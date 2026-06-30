//! Syncode WebSocket — Transport Layer
//!
//! WebSocket JSON-RPC server, method dispatch, push bus,
//! channel management, and connection state machine.

pub mod channels;
pub mod push;
pub mod rpc;
pub mod server;
pub mod transport;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};

/// A JSON-RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<serde_json::Value>,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

/// A JSON-RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

/// A JSON-RPC error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl JsonRpcResponse {
    pub fn success(id: serde_json::Value, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id: Some(id),
            result: Some(result),
            error: None,
        }
    }

    pub fn error(id: Option<serde_json::Value>, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }
}

/// Connection ID type
pub type ConnectionId = u64;

/// Shared state for the WebSocket server
#[derive(Clone)]
pub struct WsState {
    pub connections: Arc<RwLock<HashMap<ConnectionId, mpsc::UnboundedSender<String>>>>,
    pub push_tx: broadcast::Sender<(String, serde_json::Value)>, // (channel, data)
    pub next_connection_id: Arc<std::sync::atomic::AtomicU64>,
    /// Read model store (shared across connections via Arc)
    pub read_store: Arc<tokio::sync::RwLock<syncode_orchestration::ReadModelStore>>,
    /// Orchestrator for the CQRS pipeline (command → event → persist → project)
    pub orchestrator: Arc<syncode_orchestration::Orchestrator>,
    /// Per-connection channel subscriptions. The delivery loop consults this to
    /// decide which push broadcasts to forward to each connection (opt-in: a
    /// connection receives nothing until it subscribes via `push/subscribe`).
    pub subscriptions: Arc<RwLock<crate::push::SubscriptionRegistry>>,
}

impl WsState {
    /// Create a new WsState with an Orchestrator.
    ///
    /// The orchestrator's read model is shared with the WsState so RPC handlers
    /// can read the latest projected state.
    pub fn new(push_capacity: usize, orchestrator: syncode_orchestration::Orchestrator) -> Self {
        let (push_tx, _) = broadcast::channel(push_capacity);

        // Feed domain events from the pipeline onto the push bus: wrap the push
        // sender as a DomainEventPublisher and attach it to the orchestrator.
        // Builder-style call consumes and returns the enriched orchestrator.
        let publisher = crate::push::WsDomainEventPublisher::new(push_tx.clone());
        let orchestrator = orchestrator.with_event_publisher(Arc::new(publisher));

        let read_store = orchestrator.read_model_ref();
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            push_tx,
            next_connection_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            read_store,
            orchestrator: Arc::new(orchestrator),
            subscriptions: Arc::new(RwLock::new(crate::push::SubscriptionRegistry::new())),
        }
    }

    /// Create a simple state without persistence (in-memory only, for tests).
    /// Uses an in-memory event repository.
    pub fn new_in_memory(push_capacity: usize) -> Self {
        use syncode_core::ports::EventRepository;

        // In-memory event repo for testing
        struct InMemoryRepo {
            events: std::sync::Mutex<std::collections::HashMap<String, Vec<syncode_core::Envelope>>>,
        }
        impl InMemoryRepo {
            fn new() -> Self { Self { events: std::sync::Mutex::new(std::collections::HashMap::new()) } }
        }
        #[async_trait::async_trait]
        impl EventRepository for InMemoryRepo {
            async fn append_events(
                &self, aggregate_id: syncode_core::EntityId, events: Vec<syncode_core::DomainEvent>,
                expected_version: u64,
            ) -> Result<u64, syncode_core::PortError> {
                use syncode_core::Envelope;
                let mut store = self.events.lock().unwrap();
                let key = aggregate_id.to_string();
                let current = store.get(&key).map(|v| v.len() as u64).unwrap_or(0);
                if current != expected_version {
                    return Err(syncode_core::PortError::ConcurrencyConflict { expected: expected_version, actual: current });
                }
                let entry = store.entry(key).or_default();
                for (i, event) in events.into_iter().enumerate() {
                    entry.push(Envelope::new(event, current + 1 + i as u64));
                }
                Ok(entry.len() as u64)
            }
            async fn replay_events(&self, aggregate_id: syncode_core::EntityId) -> Result<Vec<syncode_core::Envelope>, syncode_core::PortError> {
                let store = self.events.lock().unwrap();
                Ok(store.get(&aggregate_id.to_string()).cloned().unwrap_or_default())
            }
            async fn load_snapshot(&self, _: syncode_core::EntityId) -> Result<Option<(serde_json::Value, u64)>, syncode_core::PortError> { Ok(None) }
            async fn save_snapshot(&self, _: syncode_core::EntityId, _: serde_json::Value, _: u64) -> Result<(), syncode_core::PortError> { Ok(()) }
            async fn replay_all_events(&self, _: Option<u64>, _: u32) -> Result<Vec<syncode_core::Envelope>, syncode_core::PortError> {
                let store = self.events.lock().unwrap();
                let mut all: Vec<syncode_core::Envelope> = store.values().flatten().cloned().collect();
                all.sort_by_key(|e| e.sequence);
                Ok(all)
            }
            async fn current_version(&self, aggregate_id: syncode_core::EntityId) -> Result<u64, syncode_core::PortError> {
                let store = self.events.lock().unwrap();
                Ok(store.get(&aggregate_id.to_string()).map(|v| v.len() as u64).unwrap_or(0))
            }
        }

        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryRepo::new());
        let orchestrator = syncode_orchestration::Orchestrator::new(repo);
        Self::new(push_capacity, orchestrator)
    }

    pub fn next_id(&self) -> ConnectionId {
        self.next_connection_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn register(&self, id: ConnectionId, tx: mpsc::UnboundedSender<String>) {
        self.connections.write().await.insert(id, tx);
        self.subscriptions.write().await.register(id);
        tracing::info!(connection_id = id, "WebSocket client connected");
    }

    pub async fn unregister(&self, id: ConnectionId) {
        self.connections.write().await.remove(&id);
        self.subscriptions.write().await.unregister(id);
        tracing::info!(connection_id = id, "WebSocket client disconnected");
    }
}

/// JSON-RPC standard error codes
pub mod error_codes {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;
}

//! Syncode WebSocket — Transport Layer
//!
//! WebSocket JSON-RPC server, method dispatch, push bus,
//! channel management, and connection state machine.

pub mod auth;
pub mod channels;
pub mod push;
pub mod rpc;
pub mod server;
pub mod transport;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};

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

/// Shared terminal session manager — wraps `syncode_terminal::SessionManager`
/// behind a `RwLock` (mirrors the Tauri `SharedSessionManager` pattern in
/// `crates/syncode-tauri/src/terminal_commands.rs`). The `terminal.*` RPC
/// handlers (T6c-5) read/write sessions through this handle.
pub type SharedSessionManager = Arc<RwLock<syncode_terminal::SessionManager>>;

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
    /// Auth configuration: mode (no-auth vs remote-reachable) + authenticator +
    /// shared session registry. Defaults to `UnsafeNoAuth` (backward compat).
    pub auth_config: syncode_auth::WsAuthConfig,
    /// Per-connection authenticated principals. Populated by `auth/bootstrap`,
    /// consulted by the authz gate on every protected method call.
    pub conn_auth: crate::auth::SharedConnectionAuth,
    /// Terminal PTY session manager (T6c-5). Owns the lifecycle of all
    /// terminal sessions created via `terminal.open`/`terminal.new`. Sessions
    /// are keyed by the caller-provided `terminalId` (MCode convention) so the
    /// UI's session references stay stable across calls.
    pub terminal_manager: SharedSessionManager,
    /// Automation scheduler (T6c-6). Backs the `automation.*` RPC handlers
    /// (list/create/get/update/delete/runNow/cancelRun) — manages automation
    /// definition + run-record lifecycle (mirrors the terminal_manager wiring).
    /// Subscribe/push delivery is stubbed (`automation.event` deferred).
    pub automation_scheduler: Arc<syncode_automation::Scheduler>,
}

impl WsState {
    /// Create a new WsState with an Orchestrator, in **no-auth** mode.
    ///
    /// This is the backward-compatible default: existing local-first and test
    /// deployments run without authentication. Callers building a
    /// network-reachable server should use [`WsState::new_with_auth`] instead.
    ///
    /// The orchestrator's read model is shared with the WsState so RPC handlers
    /// can read the latest projected state.
    pub fn new(push_capacity: usize, orchestrator: syncode_orchestration::Orchestrator) -> Self {
        Self::new_with_auth(
            push_capacity,
            orchestrator,
            syncode_auth::WsAuthConfig::no_auth(),
        )
    }

    /// Create a new WsState with an explicit auth configuration.
    ///
    /// Pass [`syncode_auth::WsAuthConfig::no_auth()`] for local/dev (the
    /// historical behavior), or [`syncode_auth::WsAuthConfig::remote`] to
    /// require authentication on every connection before dispatch.
    pub fn new_with_auth(
        push_capacity: usize,
        orchestrator: syncode_orchestration::Orchestrator,
        auth_config: syncode_auth::WsAuthConfig,
    ) -> Self {
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
            auth_config,
            conn_auth: crate::auth::SharedConnectionAuth::new(),
            terminal_manager: Arc::new(RwLock::new(
                syncode_terminal::SessionManager::new(),
            )),
            automation_scheduler: Arc::new(syncode_automation::Scheduler::new()),
        }
    }

    /// Create a simple state without persistence (in-memory only, for tests).
    /// Uses an in-memory event repository.
    pub fn new_in_memory(push_capacity: usize) -> Self {
        use syncode_core::ports::EventRepository;

        // In-memory event repo for testing
        struct InMemoryRepo {
            events:
                std::sync::Mutex<std::collections::HashMap<String, Vec<syncode_core::Envelope>>>,
            snapshots:
                std::sync::Mutex<std::collections::HashMap<String, (serde_json::Value, u64)>>,
        }
        impl InMemoryRepo {
            fn new() -> Self {
                Self {
                    events: std::sync::Mutex::new(std::collections::HashMap::new()),
                    snapshots: std::sync::Mutex::new(std::collections::HashMap::new()),
                }
            }
        }
        #[async_trait::async_trait]
        impl EventRepository for InMemoryRepo {
            async fn append_events(
                &self,
                aggregate_id: syncode_core::EntityId,
                events: Vec<syncode_core::DomainEvent>,
                expected_version: u64,
            ) -> Result<u64, syncode_core::PortError> {
                use syncode_core::Envelope;
                let mut store = self.events.lock().unwrap();
                let key = aggregate_id.to_string();
                let current = store.get(&key).map(|v| v.len() as u64).unwrap_or(0);
                if current != expected_version {
                    return Err(syncode_core::PortError::ConcurrencyConflict {
                        expected: expected_version,
                        actual: current,
                    });
                }
                let entry = store.entry(key).or_default();
                for (i, event) in events.into_iter().enumerate() {
                    entry.push(Envelope::new(event, current + 1 + i as u64));
                }
                Ok(entry.len() as u64)
            }
            async fn replay_events(
                &self,
                aggregate_id: syncode_core::EntityId,
            ) -> Result<Vec<syncode_core::Envelope>, syncode_core::PortError> {
                let store = self.events.lock().unwrap();
                Ok(store
                    .get(&aggregate_id.to_string())
                    .cloned()
                    .unwrap_or_default())
            }
            async fn load_snapshot(
                &self,
                aggregate_id: syncode_core::EntityId,
            ) -> Result<Option<(serde_json::Value, u64)>, syncode_core::PortError> {
                Ok(self
                    .snapshots
                    .lock()
                    .unwrap()
                    .get(&aggregate_id.to_string())
                    .cloned())
            }
            async fn save_snapshot(
                &self,
                aggregate_id: syncode_core::EntityId,
                state: serde_json::Value,
                version: u64,
            ) -> Result<(), syncode_core::PortError> {
                self.snapshots
                    .lock()
                    .unwrap()
                    .insert(aggregate_id.to_string(), (state, version));
                Ok(())
            }
            async fn load_all_snapshots(
                &self,
            ) -> Result<
                Vec<(syncode_core::EntityId, serde_json::Value, u64)>,
                syncode_core::PortError,
            > {
                let snapshots = self.snapshots.lock().unwrap();
                let mut out = Vec::with_capacity(snapshots.len());
                for (id_str, (state, version)) in snapshots.iter() {
                    let id = syncode_core::EntityId::parse(id_str).map_err(|e| {
                        syncode_core::PortError::Internal(format!("invalid aggregate_id: {e}"))
                    })?;
                    out.push((id, state.clone(), *version));
                }
                Ok(out)
            }
            async fn replay_all_events(
                &self,
                _: Option<u64>,
                _: u32,
            ) -> Result<Vec<syncode_core::Envelope>, syncode_core::PortError> {
                let store = self.events.lock().unwrap();
                let mut all: Vec<syncode_core::Envelope> =
                    store.values().flatten().cloned().collect();
                all.sort_by_key(|e| e.sequence);
                Ok(all)
            }
            async fn current_version(
                &self,
                aggregate_id: syncode_core::EntityId,
            ) -> Result<u64, syncode_core::PortError> {
                let store = self.events.lock().unwrap();
                Ok(store
                    .get(&aggregate_id.to_string())
                    .map(|v| v.len() as u64)
                    .unwrap_or(0))
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

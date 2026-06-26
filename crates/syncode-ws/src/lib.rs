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
#[derive(Debug, Clone)]
pub struct WsState {
    pub connections: Arc<RwLock<HashMap<ConnectionId, mpsc::UnboundedSender<String>>>>,
    pub push_tx: broadcast::Sender<(String, serde_json::Value)>, // (channel, data)
    pub next_connection_id: Arc<std::sync::atomic::AtomicU64>,
    /// Read model store (shared across connections via Arc)
    pub read_store: Arc<tokio::sync::RwLock<syncode_orchestration::ReadModelStore>>,
}

impl WsState {
    pub fn new(push_capacity: usize) -> Self {
        let (push_tx, _) = broadcast::channel(push_capacity);
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            push_tx,
            next_connection_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            read_store: Arc::new(tokio::sync::RwLock::new(
                syncode_orchestration::ReadModelStore::new(),
            )),
        }
    }

    pub fn next_id(&self) -> ConnectionId {
        self.next_connection_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn register(&self, id: ConnectionId, tx: mpsc::UnboundedSender<String>) {
        self.connections.write().await.insert(id, tx);
        tracing::info!(connection_id = id, "WebSocket client connected");
    }

    pub async fn unregister(&self, id: ConnectionId) {
        self.connections.write().await.remove(&id);
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

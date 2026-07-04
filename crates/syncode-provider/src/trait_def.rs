//! ProviderAdapter trait definition
//!
//! Defines the interface that all provider adapters must implement.
//! Each adapter spawns an AI coding agent as a subprocess and communicates
//! via JSON-RPC over stdin/stdout (or WebSocket for remote providers).

use std::collections::HashMap;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use syncode_core::EntityId;
use tokio_stream::Stream;

// ---------------------------------------------------------------------------
// Provider identity & capabilities
// ---------------------------------------------------------------------------

/// Unique provider identifier string (e.g., "anthropic", "openai", "codex")
pub type ProviderId = String;

/// Supported provider identifiers
pub const PROVIDER_CODEX: &str = "codex";
pub const PROVIDER_CLAUDE: &str = "claude";
pub const PROVIDER_CURSOR: &str = "cursor";
pub const PROVIDER_GEMINI: &str = "gemini";
pub const PROVIDER_GROK: &str = "grok";
pub const PROVIDER_KILO: &str = "kilo";
pub const PROVIDER_OPENCODE: &str = "opencode";
pub const PROVIDER_PI: &str = "pi";
pub const PROVIDER_ANTHROPIC: &str = "anthropic";
pub const PROVIDER_OPENAI: &str = "openai";

/// All known provider IDs
pub const ALL_PROVIDERS: &[&str] = &[
    PROVIDER_CODEX,
    PROVIDER_CLAUDE,
    PROVIDER_CURSOR,
    PROVIDER_GEMINI,
    PROVIDER_GROK,
    PROVIDER_KILO,
    PROVIDER_OPENCODE,
    PROVIDER_PI,
    PROVIDER_ANTHROPIC,
    PROVIDER_OPENAI,
];

/// Capabilities a provider may support
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCapability {
    /// Can stream token-by-token responses
    Streaming,
    /// Supports tool/function calling
    ToolUse,
    /// Can handle image inputs
    Vision,
    /// Supports code execution/sandboxing
    CodeExecution,
    /// Can work with files directly (read/write filesystem)
    FileSystem,
    /// Supports system prompts / custom instructions
    SystemPrompt,
    /// Supports steering an in-progress generation (mid-turn redirect).
    ///
    /// When a provider advertises this capability, `DispatchQueuedTurn` can
    /// "steer" an active (Processing) session instead of dropping/queuing the
    /// turn or spinning up a new session. Adapters that don't support steering
    /// fall back to the default `send_request` path.
    Steering,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Provider-specific configuration passed at adapter creation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    /// Provider identifier (e.g., "claude")
    pub provider_id: ProviderId,
    /// Model to use (e.g., "claude-3-sonnet-20240229")
    pub model: String,
    /// API key or authentication token (optional — some adapters use env vars)
    pub api_key: Option<String>,
    /// Base URL override for self-hosted / proxy setups
    pub base_url: Option<String>,
    /// Maximum tokens for a single response
    pub max_tokens: Option<u32>,
    /// Extra provider-specific parameters
    pub extra: HashMap<String, serde_json::Value>,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            provider_id: PROVIDER_CLAUDE.to_string(),
            model: "claude-3-sonnet".to_string(),
            api_key: None,
            base_url: None,
            max_tokens: Some(4096),
            extra: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// JSON-RPC message types for provider communication
// ---------------------------------------------------------------------------

/// A JSON-RPC request sent TO a provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderRequest {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

impl ProviderRequest {
    pub fn new(method: impl Into<String>, params: Option<serde_json::Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: rand_id(),
            method: method.into(),
            params,
        }
    }
}

/// A JSON-RPC response received FROM a provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProviderError>,
}

/// JSON-RPC error object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// A provider event pushed to us (not tied to a request ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ProviderEvent {
    /// Provider started processing
    Started { session_id: String },
    /// Token streaming chunk
    Token { session_id: String, content: String },
    /// Tool call emitted by the provider
    ToolCall {
        session_id: String,
        tool_name: String,
        tool_input: serde_json::Value,
    },
    /// Tool result received
    ToolResult {
        session_id: String,
        tool_name: String,
        result: serde_json::Value,
    },
    /// Provider completed a response
    Completed {
        session_id: String,
        output: String,
        usage: Option<UsageInfo>,
    },
    /// Provider reported an error
    Error {
        session_id: String,
        message: String,
        code: Option<i64>,
    },
    /// Provider status change (e.g., idle, busy, disconnected)
    StatusChanged { status: ProviderStatus },
}

/// Token usage metadata from provider responses
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UsageInfo {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
}

/// Running status of a provider connection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderStatus {
    /// Provider is idle, waiting for work
    Idle,
    /// Provider is actively processing
    Busy,
    /// Provider is disconnected / unavailable
    Disconnected,
    /// Provider encountered an error
    Error,
    /// Provider is shutting down
    ShuttingDown,
}

// Conversions to/from `u64` so adapters can store status in an `AtomicU64`.
// Centralized here (next to the type) rather than in a single adapter file, so
// every adapter shares one canonical mapping.
impl From<u64> for ProviderStatus {
    fn from(v: u64) -> Self {
        match v {
            0 => ProviderStatus::Idle,
            1 => ProviderStatus::Busy,
            2 => ProviderStatus::Disconnected,
            3 => ProviderStatus::Error,
            4 => ProviderStatus::ShuttingDown,
            _ => ProviderStatus::Error,
        }
    }
}

impl From<ProviderStatus> for u64 {
    fn from(s: ProviderStatus) -> Self {
        match s {
            ProviderStatus::Idle => 0,
            ProviderStatus::Busy => 1,
            ProviderStatus::Disconnected => 2,
            ProviderStatus::Error => 3,
            ProviderStatus::ShuttingDown => 4,
        }
    }
}

// ---------------------------------------------------------------------------
// Session context passed when creating a session
// ---------------------------------------------------------------------------

/// Context for a new provider session — ties the session to a Syncode turn
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionContext {
    /// The thread this session belongs to
    pub thread_id: EntityId,
    /// The turn this session is processing
    pub turn_id: EntityId,
    /// Working directory for the provider
    pub working_dir: String,
    /// System prompt / instructions
    pub system_prompt: Option<String>,
    /// Initial user input
    pub user_input: String,
    /// Files to include as context
    pub context_files: Vec<String>,
}

// ---------------------------------------------------------------------------
// Core trait
// ---------------------------------------------------------------------------

/// Output stream from a provider: yields [`ProviderEvent`] items.
pub type ProviderStream =
    Pin<Box<dyn Stream<Item = Result<ProviderEvent, ProviderAdapterError>> + Send>>;

/// The core trait every provider adapter must implement.
///
/// An adapter is responsible for:
/// 1. Spawning the provider as a subprocess (or connecting to a remote service)
/// 2. Sending JSON-RPC requests and receiving responses
/// 3. Streaming events (tokens, tool calls, completion)
/// 4. Lifecycle management (start, interrupt, stop)
#[async_trait::async_trait]
pub trait ProviderAdapter: Send + Sync {
    // -- Identity ----------------------------------------------------------

    /// Returns the unique provider identifier
    fn provider_id(&self) -> &str;

    /// Returns the list of capabilities this adapter supports
    fn capabilities(&self) -> Vec<ProviderCapability>;

    /// Returns the current status of this adapter
    fn status(&self) -> ProviderStatus;

    /// Returns a list of available models for this provider
    fn available_models(&self) -> Vec<String>;

    // -- Lifecycle ---------------------------------------------------------

    /// Spawn the provider process and initialize communication.
    /// Called once when the adapter is first registered.
    async fn spawn(&mut self, config: ProviderConfig) -> Result<(), ProviderAdapterError>;

    /// Gracefully shut down the provider process.
    async fn shutdown(&mut self) -> Result<(), ProviderAdapterError>;

    /// Interrupt an in-progress generation (e.g., user hit Stop).
    async fn interrupt(&self, session_id: &str) -> Result<(), ProviderAdapterError>;

    // -- Session management -------------------------------------------------

    /// Start a new session for a given turn context.
    /// Returns a session ID that the adapter uses to correlate events.
    async fn start_session(&mut self, ctx: SessionContext) -> Result<String, ProviderAdapterError>;

    /// Resume an existing session (e.g., after a pause).
    async fn resume_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError>;

    /// Stop/cancel a session.
    async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError>;

    // -- Communication -----------------------------------------------------

    /// Send a JSON-RPC request to the provider and wait for a response.
    async fn send_request(
        &self,
        request: ProviderRequest,
    ) -> Result<ProviderResponse, ProviderAdapterError>;

    /// Steer an in-progress generation: redirect an already-active session
    /// toward new input without ending the current turn.
    ///
    /// Adapters that advertise [`ProviderCapability::Steering`] override this
    /// to ship a provider-specific steer (e.g. an MCP `turn/steer` JSON-RPC
    /// method, an Anthropic in-progress edit, an OpenAI cancel-and-replay).
    /// The default implementation reports the operation as unsupported so
    /// callers can fall back to [`ProviderAdapter::send_request`] (or start a
    /// fresh turn) when steering is unavailable.
    ///
    /// `session_id` targets the active session; `payload` carries the
    /// steer-specific body (new message text, queued-turn metadata, etc.).
    async fn steer_turn(
        &self,
        _session_id: &str,
        _payload: serde_json::Value,
    ) -> Result<ProviderResponse, ProviderAdapterError> {
        Err(ProviderAdapterError::UnsupportedOperation(format!(
            "provider '{}' does not support steering",
            self.provider_id()
        )))
    }

    /// Subscribe to a stream of events from the provider.
    /// The stream yields until the session ends or the provider disconnects.
    fn event_stream(&self, session_id: &str) -> Result<ProviderStream, ProviderAdapterError>;

    // -- Utility -----------------------------------------------------------

    /// Check if the provider is healthy / reachable.
    async fn health_check(&self) -> Result<bool, ProviderAdapterError>;
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors that can occur during provider adapter operations
#[derive(Debug, thiserror::Error)]
pub enum ProviderAdapterError {
    #[error("Provider not spawned — call spawn() first")]
    NotSpawned,

    #[error("Provider process exited unexpectedly: {0}")]
    ProcessExited(String),

    #[error("JSON-RPC error from provider: code={code}, message={message}")]
    RpcError { code: i64, message: String },

    #[error("I/O error communicating with provider: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("Session already active: {0}")]
    SessionAlreadyActive(String),

    #[error("Provider configuration error: {0}")]
    ConfigError(String),

    #[error("Provider timeout after {0}ms")]
    Timeout(u64),

    #[error("Provider not supported: {0}")]
    UnsupportedProvider(String),

    /// The operation is not supported by this adapter (e.g. steering a turn
    /// on a provider that doesn't implement `steer_turn`).
    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("Provider internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate a random-ish request ID for JSON-RPC (simple counter-based for now)
fn rand_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_config_defaults() {
        let config = ProviderConfig::default();
        assert_eq!(config.provider_id, PROVIDER_CLAUDE);
        assert_eq!(config.model, "claude-3-sonnet");
        assert!(config.api_key.is_none());
        assert_eq!(config.max_tokens, Some(4096));
        assert!(config.extra.is_empty());
    }

    #[test]
    fn provider_config_custom() {
        let config = ProviderConfig {
            provider_id: PROVIDER_CODEX.to_string(),
            model: "codex-mini".to_string(),
            api_key: Some("sk-test".to_string()),
            base_url: Some("http://localhost:8080".to_string()),
            max_tokens: Some(8192),
            extra: HashMap::from([("temp".to_string(), serde_json::json!(0.7))]),
        };
        assert_eq!(config.provider_id, PROVIDER_CODEX);
        assert_eq!(config.model, "codex-mini");
        assert_eq!(config.api_key.as_deref(), Some("sk-test"));
    }

    #[test]
    fn provider_request_new() {
        let req = ProviderRequest::new("initialize", Some(serde_json::json!({"key": "val"})));
        assert_eq!(req.jsonrpc, "2.0");
        assert_eq!(req.method, "initialize");
        assert!(req.params.is_some());
        assert!(req.id > 0);
    }

    #[test]
    fn provider_request_serialization() {
        let req = ProviderRequest::new("chat", Some(serde_json::json!({"prompt": "hello"})));
        let json = serde_json::to_string(&req).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["jsonrpc"], "2.0");
        assert_eq!(parsed["method"], "chat");
        assert!(parsed["params"]["prompt"].is_string());
    }

    #[test]
    fn provider_response_deserialization() {
        let json = r#"{"jsonrpc":"2.0","id":1,"result":{"text":"hello"}}"#;
        let resp: ProviderResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, Some(1));
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
    }

    #[test]
    fn provider_response_error() {
        let json =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"invalid request"}}"#;
        let resp: ProviderResponse = serde_json::from_str(json).unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32600);
        assert_eq!(err.message, "invalid request");
    }

    #[test]
    fn provider_event_serialization_roundtrip() {
        let events = vec![
            ProviderEvent::Started {
                session_id: "sess-1".to_string(),
            },
            ProviderEvent::Token {
                session_id: "sess-1".to_string(),
                content: "hello".to_string(),
            },
            ProviderEvent::ToolCall {
                session_id: "sess-1".to_string(),
                tool_name: "read_file".to_string(),
                tool_input: serde_json::json!({"path": "/tmp/test.rs"}),
            },
            ProviderEvent::Completed {
                session_id: "sess-1".to_string(),
                output: "final answer".to_string(),
                usage: Some(UsageInfo {
                    input_tokens: 100,
                    output_tokens: 50,
                    total_tokens: 150,
                }),
            },
            ProviderEvent::StatusChanged {
                status: ProviderStatus::Busy,
            },
        ];

        for event in events {
            let json = serde_json::to_string(&event).unwrap();
            let deserialized: ProviderEvent = serde_json::from_str(&json).unwrap();
            // Can't easily compare enum variants generically, just ensure roundtrip succeeds
            let _ = deserialized;
        }
    }

    #[test]
    fn usage_info_default() {
        let usage = UsageInfo::default();
        assert_eq!(usage.input_tokens, 0);
        assert_eq!(usage.output_tokens, 0);
        assert_eq!(usage.total_tokens, 0);
    }

    #[test]
    fn provider_status_equality() {
        assert_eq!(ProviderStatus::Idle, ProviderStatus::Idle);
        assert_ne!(ProviderStatus::Idle, ProviderStatus::Busy);
    }

    #[test]
    fn all_providers_const() {
        assert_eq!(ALL_PROVIDERS.len(), 10);
        assert!(ALL_PROVIDERS.contains(&PROVIDER_CLAUDE));
        assert!(ALL_PROVIDERS.contains(&PROVIDER_CODEX));
        assert!(ALL_PROVIDERS.contains(&PROVIDER_ANTHROPIC));
        assert!(ALL_PROVIDERS.contains(&PROVIDER_OPENAI));
    }

    #[test]
    fn provider_capability_serialization() {
        let cap = ProviderCapability::Streaming;
        let json = serde_json::to_string(&cap).unwrap();
        assert_eq!(json, "\"streaming\"");
        let deserialized: ProviderCapability = serde_json::from_str(&json).unwrap();
        matches!(deserialized, ProviderCapability::Streaming);
    }

    #[test]
    fn session_context_serialization() {
        let ctx = SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp/project".to_string(),
            system_prompt: Some("You are a helpful assistant.".to_string()),
            user_input: "Fix the bug".to_string(),
            context_files: vec!["src/main.rs".to_string()],
        };
        let json = serde_json::to_string(&ctx).unwrap();
        let deserialized: SessionContext = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.working_dir, ctx.working_dir);
        assert_eq!(deserialized.user_input, ctx.user_input);
        assert_eq!(deserialized.context_files.len(), 1);
    }

    #[test]
    fn adapter_error_display() {
        let err = ProviderAdapterError::NotSpawned;
        assert!(err.to_string().contains("not spawned"));

        let err = ProviderAdapterError::Timeout(5000);
        assert!(err.to_string().contains("5000ms"));

        let err = ProviderAdapterError::SessionNotFound("sess-99".to_string());
        assert!(err.to_string().contains("sess-99"));
    }
}

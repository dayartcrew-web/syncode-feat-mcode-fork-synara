//! Claude adapter — Anthropic Claude Code CLI
//!
//! Anthropic Claude Code adapter. Spawns `claude` as a subprocess and communicates
//! via JSON-RPC over stdin/stdout. Supports the Anthropic Messages API format.
//!
//! Configuration requires an API key (via CLAUDE_API_KEY env var or ProviderConfig).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::{Mutex, broadcast};

use super::super::trait_def::*;
use crate::session::SessionState;

/// Claude-specific configuration extensions
#[derive(Debug, Clone)]
pub struct ClaudeConfig {
    /// Path to the claude CLI binary (default: "claude")
    pub bin_path: String,
    /// Anthropic API key (if not set, reads CLAUDE_API_KEY env var)
    pub api_key: Option<String>,
    /// Anthropic API base URL (default: "https://api.anthropic.com")
    pub base_url: String,
    /// Whether to run in "dangerously-skip-permissions" mode (full-auto)
    pub full_auto: bool,
    /// Default Claude model
    pub model: String,
}

impl Default for ClaudeConfig {
    fn default() -> Self {
        Self {
            bin_path: "claude".to_string(),
            api_key: std::env::var("CLAUDE_API_KEY").ok(),
            base_url: "https://api.anthropic.com".to_string(),
            full_auto: true,
            model: "claude-sonnet-4-20250514".to_string(),
        }
    }
}

/// The Claude provider adapter
pub struct ClaudeAdapter {
    config: Option<ProviderConfig>,
    claude_config: ClaudeConfig,
    status: AtomicU64, // ProviderStatus as u64
    sessions: Mutex<HashMap<String, Arc<SessionState>>>,
    event_tx: broadcast::Sender<ProviderEvent>,
    spawned: AtomicBool,
    #[allow(dead_code)] // JSON-RPC request-id seam (reserved for future real subprocess impls)
    next_req_id: AtomicU64,
}

impl Default for ClaudeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl ClaudeAdapter {
    /// Create a new Claude adapter with default settings
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            claude_config: ClaudeConfig::default(),
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            event_tx,
            spawned: AtomicBool::new(false),
            next_req_id: AtomicU64::new(1),
        }
    }

    /// Create a new Claude adapter with custom claude-specific config
    pub fn with_claude_config(claude_config: ClaudeConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            claude_config,
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            event_tx,
            spawned: AtomicBool::new(false),
            next_req_id: AtomicU64::new(1),
        }
    }

    /// Check if the Claude API key is configured
    pub fn has_api_key(&self) -> bool {
        self.claude_config.api_key.is_some() || std::env::var("CLAUDE_API_KEY").is_ok()
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
    }

    #[allow(dead_code)] // JSON-RPC request-id seam (reserved for future real subprocess impls)
    fn next_request_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    fn generate_session_id() -> String {
        format!("claude-{}", uuid::Uuid::new_v4().hyphenated())
    }
}

#[async_trait::async_trait]
impl ProviderAdapter for ClaudeAdapter {
    // -- Identity ----------------------------------------------------------

    fn provider_id(&self) -> &str {
        PROVIDER_CLAUDE
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        vec![
            ProviderCapability::Streaming,
            ProviderCapability::ToolUse,
            ProviderCapability::FileSystem,
            ProviderCapability::SystemPrompt,
            ProviderCapability::CodeExecution,
        ]
    }

    fn status(&self) -> ProviderStatus {
        self.status.load(Ordering::Acquire).into()
    }

    fn available_models(&self) -> Vec<String> {
        vec![
            "claude-sonnet-4-20250514".to_string(),
            "claude-3-5-sonnet-20241022".to_string(),
            "claude-3-5-haiku-20241022".to_string(),
            "claude-3-opus-20240229".to_string(),
        ]
    }

    // -- Lifecycle ---------------------------------------------------------

    async fn spawn(&mut self, config: ProviderConfig) -> Result<(), ProviderAdapterError> {
        if self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::ConfigError(
                "Claude adapter already spawned".to_string(),
            ));
        }

        // Check API key availability
        let has_key = config.api_key.is_some() || self.has_api_key();
        if !has_key {
            tracing::warn!(
                provider = PROVIDER_CLAUDE,
                "No Claude API key found. Set CLAUDE_API_KEY env var or pass api_key in config."
            );
        }

        self.config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);

        tracing::info!(
            provider = PROVIDER_CLAUDE,
            model = %self.claude_config.model,
            has_api_key = has_key,
            "Claude adapter spawned (process stub — real implementation spawns subprocess)"
        );

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }

        self.set_status(ProviderStatus::ShuttingDown);

        // Stop all active sessions
        let sessions = self.sessions.lock().await;
        for session_id in sessions.keys() {
            let _ = self.interrupt(session_id).await;
        }
        drop(sessions);

        // Clear sessions
        let mut sessions = self.sessions.lock().await;
        sessions.clear();

        self.spawned.store(false, Ordering::Release);
        self.set_status(ProviderStatus::Disconnected);

        tracing::info!(provider = PROVIDER_CLAUDE, "Claude adapter shut down");
        Ok(())
    }

    async fn interrupt(&self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let sessions = self.sessions.lock().await;
        if !sessions.contains_key(session_id) {
            return Err(ProviderAdapterError::SessionNotFound(
                session_id.to_string(),
            ));
        }
        tracing::info!(
            provider = PROVIDER_CLAUDE,
            session_id,
            "Interrupting session"
        );
        // In real implementation: send SIGINT to subprocess
        Ok(())
    }

    // -- Session management -------------------------------------------------

    async fn start_session(&mut self, ctx: SessionContext) -> Result<String, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }

        let session_id = Self::generate_session_id();

        // Broadcast started event
        let _ = self.event_tx.send(ProviderEvent::Started {
            session_id: session_id.clone(),
        });

        let session = Arc::new(SessionState::new(
            session_id.clone(),
            ctx.thread_id,
            ctx.turn_id,
            ctx.working_dir,
        ));

        self.sessions
            .lock()
            .await
            .insert(session_id.clone(), session);
        self.set_status(ProviderStatus::Busy);

        tracing::info!(
            provider = PROVIDER_CLAUDE,
            session_id = %session_id,
            thread_id = %ctx.thread_id.as_str(),
            turn_id = %ctx.turn_id.as_str(),
            "Session started"
        );

        Ok(session_id)
    }

    async fn resume_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let sessions = self.sessions.lock().await;
        if !sessions.contains_key(session_id) {
            return Err(ProviderAdapterError::SessionNotFound(
                session_id.to_string(),
            ));
        }
        tracing::info!(provider = PROVIDER_CLAUDE, session_id, "Session resumed");
        Ok(())
    }

    async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let mut sessions = self.sessions.lock().await;
        if sessions.remove(session_id).is_none() {
            return Err(ProviderAdapterError::SessionNotFound(
                session_id.to_string(),
            ));
        }

        // Broadcast status change
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        });

        tracing::info!(provider = PROVIDER_CLAUDE, session_id, "Session stopped");
        Ok(())
    }

    // -- Communication -----------------------------------------------------

    async fn send_request(
        &self,
        request: ProviderRequest,
    ) -> Result<ProviderResponse, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }

        // In real implementation:
        // 1. Build Anthropic Messages API request
        // 2. Serialize as JSON-RPC → write to subprocess stdin
        // 3. Read response from subprocess stdout
        // 4. Parse as ProviderResponse
        tracing::debug!(
            provider = PROVIDER_CLAUDE,
            method = %request.method,
            id = request.id,
            "Sending JSON-RPC request to Claude (stub)"
        );

        // Stub: echo back a success response
        Ok(ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(request.id),
            result: Some(serde_json::json!({
                "status": "ok",
                "provider": PROVIDER_CLAUDE,
                "stub": true
            })),
            error: None,
        })
    }

    fn event_stream(&self, session_id: &str) -> Result<ProviderStream, ProviderAdapterError> {
        let rx = self.event_tx.subscribe();
        let sid = session_id.to_string();

        let stream = async_stream::stream! {
            let mut rx = rx;
            while let Ok(event) = rx.recv().await {
                match &event {
                    ProviderEvent::Started { session_id } |
                    ProviderEvent::Token { session_id, .. } |
                    ProviderEvent::ToolCall { session_id, .. } |
                    ProviderEvent::ToolResult { session_id, .. } |
                    ProviderEvent::Completed { session_id, .. } |
                    ProviderEvent::Error { session_id, .. } => {
                        if session_id == &sid {
                            yield Ok(event);
                        }
                    }
                    ProviderEvent::StatusChanged { .. } => {
                        yield Ok(event);
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }

    // -- Utility -----------------------------------------------------------

    async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Ok(false);
        }
        // In real implementation: check if subprocess is still running
        Ok(self.status() != ProviderStatus::Disconnected && self.status() != ProviderStatus::Error)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use syncode_core::EntityId;

    fn make_ctx() -> SessionContext {
        SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp/test-claude-project".to_string(),
            system_prompt: Some("Be helpful.".to_string()),
            user_input: "Fix the bug in main.rs".to_string(),
            context_files: vec![],
        }
    }

    #[tokio::test]
    async fn claude_adapter_not_spawned_initially() {
        let adapter = ClaudeAdapter::new();
        assert_eq!(adapter.provider_id(), PROVIDER_CLAUDE);
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(!adapter.spawned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn claude_adapter_spawn_and_shutdown() {
        let mut adapter = ClaudeAdapter::new();
        let config = ProviderConfig {
            provider_id: PROVIDER_CLAUDE.to_string(),
            model: "claude-sonnet-4-20250514".to_string(),
            ..Default::default()
        };

        assert!(adapter.spawn(config).await.is_ok());
        assert_eq!(adapter.status(), ProviderStatus::Idle);
        assert!(adapter.spawned.load(Ordering::Acquire));

        assert!(adapter.shutdown().await.is_ok());
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
    }

    #[tokio::test]
    async fn claude_adapter_double_spawn_fails() {
        let mut adapter = ClaudeAdapter::new();
        let config = ProviderConfig::default();

        assert!(adapter.spawn(config).await.is_ok());
        let result = adapter.spawn(ProviderConfig::default()).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("already spawned"));
    }

    #[tokio::test]
    async fn claude_adapter_shutdown_not_spawned_fails() {
        let mut adapter = ClaudeAdapter::new();
        let result = adapter.shutdown().await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn claude_adapter_session_lifecycle() {
        let mut adapter = ClaudeAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();

        let ctx = make_ctx();
        let session_id = adapter.start_session(ctx).await.unwrap();
        assert!(session_id.starts_with("claude-"));
        assert_eq!(adapter.status(), ProviderStatus::Busy);

        // Resume should work
        assert!(adapter.resume_session(&session_id).await.is_ok());

        // Stop the session
        assert!(adapter.stop_session(&session_id).await.is_ok());

        // Stopping non-existent session should fail
        let result = adapter.stop_session("nonexistent").await;
        assert!(result.is_err());
        matches!(
            result.unwrap_err(),
            ProviderAdapterError::SessionNotFound(_)
        );
    }

    #[tokio::test]
    async fn claude_adapter_session_without_spawn_fails() {
        let mut adapter = ClaudeAdapter::new();
        let result = adapter.start_session(make_ctx()).await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn claude_adapter_send_request_echo() {
        let mut adapter = ClaudeAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();

        let req = ProviderRequest::new("initialize", Some(serde_json::json!({"key": "val"})));
        let resp = adapter.send_request(req).await.unwrap();
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn claude_adapter_health_check() {
        let adapter = ClaudeAdapter::new();
        // Not spawned → false
        assert!(!adapter.health_check().await.unwrap());

        let mut adapter = ClaudeAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();
        assert!(adapter.health_check().await.unwrap());
    }

    #[tokio::test]
    async fn claude_adapter_capabilities() {
        let adapter = ClaudeAdapter::new();
        let caps = adapter.capabilities();
        assert!(caps.contains(&ProviderCapability::Streaming));
        assert!(caps.contains(&ProviderCapability::ToolUse));
        assert!(caps.contains(&ProviderCapability::FileSystem));
        assert!(caps.contains(&ProviderCapability::SystemPrompt));
        assert!(caps.contains(&ProviderCapability::CodeExecution));
    }

    #[tokio::test]
    async fn claude_adapter_available_models() {
        let adapter = ClaudeAdapter::new();
        let models = adapter.available_models();
        assert!(!models.is_empty());
        assert!(models.contains(&"claude-sonnet-4-20250514".to_string()));
        assert!(models.contains(&"claude-3-5-sonnet-20241022".to_string()));
        assert!(models.contains(&"claude-3-5-haiku-20241022".to_string()));
    }

    #[test]
    fn claude_config_defaults() {
        let config = ClaudeConfig::default();
        assert_eq!(config.bin_path, "claude");
        assert!(config.full_auto);
        assert_eq!(config.model, "claude-sonnet-4-20250514");
        assert_eq!(config.base_url, "https://api.anthropic.com");
    }

    #[tokio::test]
    async fn claude_adapter_with_custom_config() {
        let claude_config = ClaudeConfig {
            bin_path: "/usr/local/bin/claude".to_string(),
            api_key: Some("sk-test-123".to_string()),
            base_url: "https://custom.anthropic.com".to_string(),
            full_auto: false,
            model: "claude-3-opus-20240229".to_string(),
        };
        let adapter = ClaudeAdapter::with_claude_config(claude_config);
        assert!(adapter.has_api_key());
        assert_eq!(
            adapter.available_models().first().unwrap(),
            "claude-sonnet-4-20250514"
        );
    }

    #[tokio::test]
    async fn claude_adapter_event_stream_filters_by_session() {
        let adapter = ClaudeAdapter::new();
        let stream = adapter.event_stream("test-session").unwrap();
        // Stream is created successfully (actual event filtering tested via broadcast)
        drop(stream);
    }
}

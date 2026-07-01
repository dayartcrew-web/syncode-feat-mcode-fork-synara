//! Codex adapter — reference implementation
//!
//! OpenAI Codex CLI adapter. Spawns `codex` as a subprocess and communicates
//! via JSON-RPC over stdin/stdout. This serves as the reference adapter that
//! all other adapters follow.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{broadcast, Mutex};

use super::super::trait_def::*;
use crate::session::SessionState;

/// Codex-specific configuration extensions
#[derive(Debug, Clone)]
pub struct CodexConfig {
    /// Path to the codex CLI binary (default: "codex")
    pub bin_path: String,
    /// Whether to run in "full-auto" mode (no user confirmation needed)
    pub full_auto: bool,
    /// Default model override
    pub model: String,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            bin_path: "codex".to_string(),
            full_auto: true,
            model: "o4-mini".to_string(),
        }
    }
}

/// The Codex provider adapter
pub struct CodexAdapter {
    config: Option<ProviderConfig>,
    codex_config: CodexConfig,
    status: AtomicU64, // ProviderStatus as u64
    sessions: Mutex<HashMap<String, Arc<SessionState>>>,
    event_tx: broadcast::Sender<ProviderEvent>,
    spawned: AtomicBool,
    /// Incrementing request ID
    next_req_id: AtomicU64,
}

impl CodexAdapter {
    /// Create a new Codex adapter with default settings
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            codex_config: CodexConfig::default(),
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            event_tx,
            spawned: AtomicBool::new(false),
            next_req_id: AtomicU64::new(1),
        }
    }

    /// Create a new Codex adapter with custom codex-specific config
    pub fn with_codex_config(codex_config: CodexConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            codex_config,
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            event_tx,
            spawned: AtomicBool::new(false),
            next_req_id: AtomicU64::new(1),
        }
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
    }

    fn next_request_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    fn generate_session_id() -> String {
        format!("codex-{}", uuid::Uuid::new_v4().hyphenated())
    }
}

#[async_trait::async_trait]
impl ProviderAdapter for CodexAdapter {
    // -- Identity ----------------------------------------------------------

    fn provider_id(&self) -> &str {
        PROVIDER_CODEX
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
            "o4-mini".to_string(),
            "o3".to_string(),
            "o3-mini".to_string(),
            "gpt-4.1".to_string(),
            "gpt-4.1-mini".to_string(),
            "gpt-4.1-nano".to_string(),
        ]
    }

    // -- Lifecycle ---------------------------------------------------------

    async fn spawn(&mut self, config: ProviderConfig) -> Result<(), ProviderAdapterError> {
        if self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::ConfigError(
                "Codex adapter already spawned".to_string(),
            ));
        }

        self.config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);

        tracing::info!(
            provider = PROVIDER_CODEX,
            model = %self.codex_config.model,
            "Codex adapter spawned (process stub — real implementation spawns subprocess)"
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

        tracing::info!(provider = PROVIDER_CODEX, "Codex adapter shut down");
        Ok(())
    }

    async fn interrupt(&self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let sessions = self.sessions.lock().await;
        if !sessions.contains_key(session_id) {
            return Err(ProviderAdapterError::SessionNotFound(session_id.to_string()));
        }
        tracing::info!(provider = PROVIDER_CODEX, session_id, "Interrupting session");
        // In real implementation: send SIGINT to subprocess
        Ok(())
    }

    // -- Session management -------------------------------------------------

    async fn start_session(
        &mut self,
        ctx: SessionContext,
    ) -> Result<String, ProviderAdapterError> {
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

        self.sessions.lock().await.insert(session_id.clone(), session);
        self.set_status(ProviderStatus::Busy);

        tracing::info!(
            provider = PROVIDER_CODEX,
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
            return Err(ProviderAdapterError::SessionNotFound(session_id.to_string()));
        }
        tracing::info!(provider = PROVIDER_CODEX, session_id, "Session resumed");
        Ok(())
    }

    async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let mut sessions = self.sessions.lock().await;
        if sessions.remove(session_id).is_none() {
            return Err(ProviderAdapterError::SessionNotFound(session_id.to_string()));
        }

        // Broadcast completion
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        });

        tracing::info!(provider = PROVIDER_CODEX, session_id, "Session stopped");
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

        // In real implementation: serialize request → write to subprocess stdin
        // → read response from subprocess stdout → parse as ProviderResponse
        tracing::debug!(
            provider = PROVIDER_CODEX,
            method = %request.method,
            id = request.id,
            "Sending JSON-RPC request to provider (stub)"
        );

        // Stub: echo back a success response
        Ok(ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(request.id),
            result: Some(serde_json::json!({
                "status": "ok",
                "stub": true
            })),
            error: None,
        })
    }

    fn event_stream(&self, session_id: &str) -> Result<ProviderStream, ProviderAdapterError> {
        // In real implementation: subscribe to subprocess stdout parsing
        let rx = self.event_tx.subscribe();
        let sid = session_id.to_string();

        let stream = async_stream::stream! {
            let mut rx = rx;
            while let Ok(event) = rx.recv().await {
                // Filter events for this session
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
                        // Status changes are broadcast to all subscribers
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
            working_dir: "/tmp/test-project".to_string(),
            system_prompt: Some("Be helpful.".to_string()),
            user_input: "Fix the bug in main.rs".to_string(),
            context_files: vec![],
        }
    }

    #[tokio::test]
    async fn codex_adapter_not_spawned_initially() {
        let adapter = CodexAdapter::new();
        assert_eq!(adapter.provider_id(), PROVIDER_CODEX);
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(!adapter.spawned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn codex_adapter_spawn_and_shutdown() {
        let mut adapter = CodexAdapter::new();
        let config = ProviderConfig {
            provider_id: PROVIDER_CODEX.to_string(),
            model: "o4-mini".to_string(),
            ..Default::default()
        };

        assert!(adapter.spawn(config).await.is_ok());
        assert_eq!(adapter.status(), ProviderStatus::Idle);
        assert!(adapter.spawned.load(Ordering::Acquire));

        assert!(adapter.shutdown().await.is_ok());
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
    }

    #[tokio::test]
    async fn codex_adapter_double_spawn_fails() {
        let mut adapter = CodexAdapter::new();
        let config = ProviderConfig::default();

        assert!(adapter.spawn(config).await.is_ok());
        let result = adapter.spawn(ProviderConfig::default()).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("already spawned"));
    }

    #[tokio::test]
    async fn codex_adapter_shutdown_not_spawned_fails() {
        let mut adapter = CodexAdapter::new();
        let result = adapter.shutdown().await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn codex_adapter_session_lifecycle() {
        let mut adapter = CodexAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();

        let ctx = make_ctx();
        let session_id = adapter.start_session(ctx).await.unwrap();
        assert!(session_id.starts_with("codex-"));
        assert_eq!(adapter.status(), ProviderStatus::Busy);

        // Resume should work
        assert!(adapter.resume_session(&session_id).await.is_ok());

        // Stop the session
        assert!(adapter.stop_session(&session_id).await.is_ok());

        // Stopping non-existent session should fail
        let result = adapter.stop_session("nonexistent").await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::SessionNotFound(_));
    }

    #[tokio::test]
    async fn codex_adapter_session_without_spawn_fails() {
        let mut adapter = CodexAdapter::new();
        let result = adapter.start_session(make_ctx()).await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn codex_adapter_send_request_not_spawned_fails() {
        let adapter = CodexAdapter::new();
        let req = ProviderRequest::new("test", None);
        let result = adapter.send_request(req).await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn codex_adapter_send_request_echo() {
        let mut adapter = CodexAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();

        let req = ProviderRequest::new("initialize", Some(serde_json::json!({"key": "val"})));
        let resp = adapter.send_request(req).await.unwrap();
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn codex_adapter_health_check() {
        let adapter = CodexAdapter::new();
        // Not spawned → false
        assert_eq!(adapter.health_check().await.unwrap(), false);

        let mut adapter = CodexAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();
        assert_eq!(adapter.health_check().await.unwrap(), true);
    }

    #[tokio::test]
    async fn codex_adapter_capabilities() {
        let adapter = CodexAdapter::new();
        let caps = adapter.capabilities();
        assert!(caps.contains(&ProviderCapability::Streaming));
        assert!(caps.contains(&ProviderCapability::ToolUse));
        assert!(caps.contains(&ProviderCapability::FileSystem));
    }

    #[tokio::test]
    async fn codex_adapter_available_models() {
        let adapter = CodexAdapter::new();
        let models = adapter.available_models();
        assert!(!models.is_empty());
        assert!(models.contains(&"o4-mini".to_string()));
    }

    #[test]
    fn codex_config_defaults() {
        let config = CodexConfig::default();
        assert_eq!(config.bin_path, "codex");
        assert!(config.full_auto);
        assert_eq!(config.model, "o4-mini");
    }

    #[test]
    fn provider_status_roundtrip() {
        for status in [
            ProviderStatus::Idle,
            ProviderStatus::Busy,
            ProviderStatus::Disconnected,
            ProviderStatus::Error,
            ProviderStatus::ShuttingDown,
        ] {
            let n: u64 = status.into();
            let back: ProviderStatus = n.into();
            assert_eq!(status, back);
        }
    }
}

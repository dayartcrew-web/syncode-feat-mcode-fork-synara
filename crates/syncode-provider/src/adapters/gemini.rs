//! Gemini adapter — Google Gemini Code Assist
//!
//! Gemini adapter. Spawns `gemini` CLI and communicates
//! via JSON-RPC over stdin/stdout. Supports the Google Generative AI API format.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::{Mutex, broadcast};

use super::super::trait_def::*;
use crate::session::SessionState;

/// Gemini-specific configuration
#[derive(Debug, Clone)]
pub struct GeminiConfig {
    /// Path to the gemini CLI binary (default: "gemini")
    pub bin_path: String,
    /// Google API key (if not set, reads GEMINI_API_KEY env var)
    pub api_key: Option<String>,
    /// Google API base URL (default: "https://generativelanguage.googleapis.com")
    pub base_url: String,
    /// Default Gemini model
    pub model: String,
}

impl Default for GeminiConfig {
    fn default() -> Self {
        Self {
            bin_path: "gemini".to_string(),
            api_key: std::env::var("GEMINI_API_KEY").ok(),
            base_url: "https://generativelanguage.googleapis.com".to_string(),
            model: "gemini-pro".to_string(),
        }
    }
}

/// The Gemini provider adapter
pub struct GeminiAdapter {
    config: Option<ProviderConfig>,
    gemini_config: GeminiConfig,
    status: AtomicU64,
    sessions: Mutex<HashMap<String, Arc<SessionState>>>,
    event_tx: broadcast::Sender<ProviderEvent>,
    spawned: AtomicBool,
    next_req_id: AtomicU64,
}

impl GeminiAdapter {
    /// Create a new Gemini adapter with default settings
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            gemini_config: GeminiConfig::default(),
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            event_tx,
            spawned: AtomicBool::new(false),
            next_req_id: AtomicU64::new(1),
        }
    }

    /// Create a new Gemini adapter with custom gemini-specific config
    pub fn with_gemini_config(gemini_config: GeminiConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            gemini_config,
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            event_tx,
            spawned: AtomicBool::new(false),
            next_req_id: AtomicU64::new(1),
        }
    }

    /// Check if the Gemini API key is configured
    pub fn has_api_key(&self) -> bool {
        self.gemini_config.api_key.is_some() || std::env::var("GEMINI_API_KEY").is_ok()
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
    }

    fn next_request_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    fn generate_session_id() -> String {
        format!("gemini-{}", uuid::Uuid::new_v4().hyphenated())
    }
}

impl Default for GeminiAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ProviderAdapter for GeminiAdapter {
    // -- Identity ----------------------------------------------------------

    fn provider_id(&self) -> &str {
        PROVIDER_GEMINI
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        vec![
            ProviderCapability::Streaming,
            ProviderCapability::ToolUse,
            ProviderCapability::Vision,
            ProviderCapability::SystemPrompt,
        ]
    }

    fn status(&self) -> ProviderStatus {
        self.status.load(Ordering::Acquire).into()
    }

    fn available_models(&self) -> Vec<String> {
        vec![
            "gemini-pro".to_string(),
            "gemini-ultra".to_string(),
            "gemini-flash".to_string(),
        ]
    }

    // -- Lifecycle ---------------------------------------------------------

    async fn spawn(&mut self, config: ProviderConfig) -> Result<(), ProviderAdapterError> {
        if self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::ConfigError(
                "Gemini adapter already spawned".to_string(),
            ));
        }

        let has_key = config.api_key.is_some() || self.has_api_key();
        if !has_key {
            tracing::warn!(
                provider = PROVIDER_GEMINI,
                "No Gemini API key found. Set GEMINI_API_KEY env var or pass api_key in config."
            );
        }

        self.config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);

        tracing::info!(
            provider = PROVIDER_GEMINI,
            model = %self.gemini_config.model,
            has_api_key = has_key,
            "Gemini adapter spawned (process stub)"
        );

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }

        self.set_status(ProviderStatus::ShuttingDown);

        let sessions = self.sessions.lock().await;
        for session_id in sessions.keys() {
            let _ = self.interrupt(session_id).await;
        }
        drop(sessions);

        let mut sessions = self.sessions.lock().await;
        sessions.clear();

        self.spawned.store(false, Ordering::Release);
        self.set_status(ProviderStatus::Disconnected);

        tracing::info!(provider = PROVIDER_GEMINI, "Gemini adapter shut down");
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
            provider = PROVIDER_GEMINI,
            session_id,
            "Interrupting session"
        );
        Ok(())
    }

    // -- Session management -------------------------------------------------

    async fn start_session(&mut self, ctx: SessionContext) -> Result<String, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }

        let session_id = Self::generate_session_id();

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
            provider = PROVIDER_GEMINI,
            session_id = %session_id,
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
        tracing::info!(provider = PROVIDER_GEMINI, session_id, "Session resumed");
        Ok(())
    }

    async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let mut sessions = self.sessions.lock().await;
        if sessions.remove(session_id).is_none() {
            return Err(ProviderAdapterError::SessionNotFound(
                session_id.to_string(),
            ));
        }

        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        });

        tracing::info!(provider = PROVIDER_GEMINI, session_id, "Session stopped");
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

        tracing::debug!(
            provider = PROVIDER_GEMINI,
            method = %request.method,
            id = request.id,
            "Sending JSON-RPC request to Gemini (stub)"
        );

        Ok(ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(request.id),
            result: Some(serde_json::json!({
                "status": "ok",
                "provider": PROVIDER_GEMINI,
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
            working_dir: "/tmp/test-gemini-project".to_string(),
            system_prompt: Some("Be helpful.".to_string()),
            user_input: "Fix the bug in main.rs".to_string(),
            context_files: vec![],
        }
    }

    #[test]
    fn gemini_config_defaults() {
        let config = GeminiConfig::default();
        assert_eq!(config.bin_path, "gemini");
        assert_eq!(config.model, "gemini-pro");
        assert_eq!(config.base_url, "https://generativelanguage.googleapis.com");
    }

    #[tokio::test]
    async fn gemini_adapter_new() {
        let adapter = GeminiAdapter::new();
        assert_eq!(adapter.provider_id(), PROVIDER_GEMINI);
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(!adapter.spawned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn gemini_adapter_spawn_and_shutdown() {
        let mut adapter = GeminiAdapter::new();
        let config = ProviderConfig {
            provider_id: PROVIDER_GEMINI.to_string(),
            model: "gemini-pro".to_string(),
            ..Default::default()
        };

        assert!(adapter.spawn(config).await.is_ok());
        assert_eq!(adapter.status(), ProviderStatus::Idle);
        assert!(adapter.spawned.load(Ordering::Acquire));

        assert!(adapter.shutdown().await.is_ok());
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
    }

    #[tokio::test]
    async fn gemini_adapter_double_spawn_fails() {
        let mut adapter = GeminiAdapter::new();
        assert!(adapter.spawn(ProviderConfig::default()).await.is_ok());
        let result = adapter.spawn(ProviderConfig::default()).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("already spawned"));
    }

    #[tokio::test]
    async fn gemini_adapter_shutdown_not_spawned_fails() {
        let mut adapter = GeminiAdapter::new();
        let result = adapter.shutdown().await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn gemini_adapter_session_lifecycle() {
        let mut adapter = GeminiAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();

        let session_id = adapter.start_session(make_ctx()).await.unwrap();
        assert!(session_id.starts_with("gemini-"));
        assert_eq!(adapter.status(), ProviderStatus::Busy);

        assert!(adapter.resume_session(&session_id).await.is_ok());
        assert!(adapter.stop_session(&session_id).await.is_ok());

        let result = adapter.stop_session("nonexistent").await;
        assert!(result.is_err());
        matches!(
            result.unwrap_err(),
            ProviderAdapterError::SessionNotFound(_)
        );
    }

    #[tokio::test]
    async fn gemini_adapter_session_without_spawn_fails() {
        let mut adapter = GeminiAdapter::new();
        let result = adapter.start_session(make_ctx()).await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn gemini_adapter_send_request_echo() {
        let mut adapter = GeminiAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();

        let req = ProviderRequest::new("initialize", Some(serde_json::json!({"key": "val"})));
        let resp = adapter.send_request(req).await.unwrap();
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn gemini_adapter_health_check() {
        let adapter = GeminiAdapter::new();
        assert_eq!(adapter.health_check().await.unwrap(), false);

        let mut adapter = GeminiAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();
        assert_eq!(adapter.health_check().await.unwrap(), true);
    }

    #[tokio::test]
    async fn gemini_adapter_capabilities() {
        let adapter = GeminiAdapter::new();
        let caps = adapter.capabilities();
        assert!(caps.contains(&ProviderCapability::Streaming));
        assert!(caps.contains(&ProviderCapability::ToolUse));
        assert!(caps.contains(&ProviderCapability::Vision));
    }

    #[tokio::test]
    async fn gemini_adapter_available_models() {
        let adapter = GeminiAdapter::new();
        let models = adapter.available_models();
        assert!(!models.is_empty());
        assert!(models.contains(&"gemini-pro".to_string()));
        assert!(models.contains(&"gemini-ultra".to_string()));
    }

    #[tokio::test]
    async fn gemini_adapter_with_custom_config() {
        let gemini_config = GeminiConfig {
            bin_path: "/custom/gemini".to_string(),
            api_key: Some("test-key".to_string()),
            base_url: "https://custom.gemini.com".to_string(),
            model: "gemini-custom".to_string(),
        };
        let adapter = GeminiAdapter::with_gemini_config(gemini_config);
        assert!(adapter.has_api_key());
        assert_eq!(adapter.provider_id(), PROVIDER_GEMINI);
    }
}

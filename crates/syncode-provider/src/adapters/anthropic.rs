//! Anthropic HTTP adapter — Anthropic Messages API with custom base URL
//!
//! HTTP-based adapter that calls the Anthropic Messages API directly.
//! Supports custom `base_url` for proxies, self-hosted endpoints, and
//! AWS Bedrock / Google Vertex AI Anthropic-compatible gateways.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::{Mutex, broadcast};

use super::super::trait_def::*;
use crate::session::SessionState;

// ---------------------------------------------------------------------------
// Anthropic-specific configuration
// ---------------------------------------------------------------------------

/// Anthropic Messages API configuration
#[derive(Debug, Clone)]
pub struct AnthropicConfig {
    /// Anthropic API key (if not set, reads ANTHROPIC_API_KEY env var)
    pub api_key: Option<String>,
    /// Anthropic API base URL (default: "https://api.anthropic.com")
    /// Set to a custom URL for proxies, Bedrock, or Vertex AI gateways.
    pub base_url: String,
    /// Default model to use
    pub model: String,
    /// Maximum tokens per response
    pub max_tokens: u32,
    /// Anthropic API version header (default: "2023-06-01")
    pub api_version: String,
}

impl Default for AnthropicConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("ANTHROPIC_API_KEY").ok(),
            base_url: "https://api.anthropic.com".to_string(),
            model: "claude-sonnet-4-20250514".to_string(),
            max_tokens: 4096,
            api_version: "2023-06-01".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Anthropic Messages API request/response types
// ---------------------------------------------------------------------------

/// Request body for the Anthropic Messages API
#[derive(Debug, Clone, serde::Serialize)]
struct AnthropicMessagesRequest {
    model: String,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    messages: Vec<AnthropicMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

/// A single message in the Anthropic format
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AnthropicMessage {
    role: String,
    content: String,
}

/// Response from the Anthropic Messages API (simplified)
#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)] // models the Anthropic API response shape; not all fields are read
struct AnthropicMessagesResponse {
    id: String,
    #[serde(rename = "type")]
    response_type: String,
    role: String,
    content: Vec<AnthropicContentBlock>,
    model: String,
    #[serde(default)]
    usage: AnthropicUsage,
}

/// A content block in the Anthropic response
#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)] // models the Anthropic API response shape; not all fields are read
struct AnthropicContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    text: Option<String>,
}

/// Usage info from Anthropic
#[derive(Debug, Clone, Default, serde::Deserialize)]
struct AnthropicUsage {
    #[serde(default)]
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
}

/// Anthropic API error response
#[derive(Debug, Clone, serde::Deserialize)]
struct AnthropicErrorResponse {
    error: Option<AnthropicErrorDetail>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)] // models the Anthropic error shape; not all fields are read
struct AnthropicErrorDetail {
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    message: String,
}

// ---------------------------------------------------------------------------
// AnthropicAdapter
// ---------------------------------------------------------------------------

/// HTTP-based Anthropic Messages API adapter
///
/// Communicates with Anthropic's API (or a compatible endpoint) via
/// HTTP POST. Supports custom base URLs for self-hosted setups, proxies,
/// and cloud provider gateways (AWS Bedrock, Google Vertex AI).
pub struct AnthropicAdapter {
    config: Option<ProviderConfig>,
    anthropic_config: AnthropicConfig,
    status: AtomicU64,
    sessions: Mutex<HashMap<String, Arc<SessionState>>>,
    event_tx: broadcast::Sender<ProviderEvent>,
    spawned: AtomicBool,
    #[allow(dead_code)] // JSON-RPC request-id seam (reserved for future real subprocess impls)
    next_req_id: AtomicU64,
    client: Option<reqwest::Client>,
}

impl AnthropicAdapter {
    /// Create a new Anthropic adapter with default settings
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            anthropic_config: AnthropicConfig::default(),
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            event_tx,
            spawned: AtomicBool::new(false),
            next_req_id: AtomicU64::new(1),
            client: None,
        }
    }

    /// Create a new Anthropic adapter with custom configuration
    pub fn with_anthropic_config(anthropic_config: AnthropicConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            anthropic_config,
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            event_tx,
            spawned: AtomicBool::new(false),
            next_req_id: AtomicU64::new(1),
            client: None,
        }
    }

    /// Check if the Anthropic API key is configured
    pub fn has_api_key(&self) -> bool {
        self.anthropic_config.api_key.is_some() || std::env::var("ANTHROPIC_API_KEY").is_ok()
    }

    /// Get the base URL this adapter is configured to use
    pub fn base_url(&self) -> &str {
        &self.anthropic_config.base_url
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
    }

    #[allow(dead_code)] // JSON-RPC request-id seam (reserved for future real subprocess impls)
    fn next_request_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    fn generate_session_id() -> String {
        format!("anthropic-{}", uuid::Uuid::new_v4().hyphenated())
    }

    /// Build the messages endpoint URL from the configured base URL
    fn messages_url(&self) -> String {
        let base = self.anthropic_config.base_url.trim_end_matches('/');
        format!("{}/v1/messages", base)
    }

    /// Resolve the API key from config or environment
    fn resolve_api_key(&self) -> Option<String> {
        self.anthropic_config
            .api_key
            .clone()
            .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok())
            .or_else(|| self.config.as_ref().and_then(|c| c.api_key.clone()))
    }
}

impl Default for AnthropicAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ProviderAdapter for AnthropicAdapter {
    // -- Identity ----------------------------------------------------------

    fn provider_id(&self) -> &str {
        PROVIDER_ANTHROPIC
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        // Honest advertisement: this adapter is a non-streaming, single-turn
        // HTTP client for the Anthropic Messages API. It does NOT implement
        // token streaming (sends `stream: false`), tool-use wire format,
        // vision/image inputs, code execution, or filesystem operations.
        // Only `SystemPrompt` is honoured today (passed as the top-level
        // `system` field on the outbound request body).
        vec![ProviderCapability::SystemPrompt]
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
                "Anthropic adapter already spawned".to_string(),
            ));
        }

        let api_key = config.api_key.clone().or_else(|| self.resolve_api_key());
        let has_key = api_key.is_some();

        if !has_key {
            tracing::warn!(
                provider = PROVIDER_ANTHROPIC,
                "No Anthropic API key found. Set ANTHROPIC_API_KEY env var or pass api_key in config."
            );
        }

        // Create the HTTP client
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .map_err(|e| {
                ProviderAdapterError::ConfigError(format!("Failed to create HTTP client: {e}"))
            })?;

        self.client = Some(client);

        // Apply config overrides to anthropic_config
        if let Some(base_url) = &config.base_url {
            self.anthropic_config.base_url = base_url.clone();
        }
        if !config.model.is_empty() {
            self.anthropic_config.model = config.model.clone();
        }
        if let Some(max_tokens) = config.max_tokens {
            self.anthropic_config.max_tokens = max_tokens;
        }

        self.config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);

        tracing::info!(
            provider = PROVIDER_ANTHROPIC,
            model = %self.anthropic_config.model,
            base_url = %self.anthropic_config.base_url,
            has_api_key = has_key,
            "Anthropic HTTP adapter spawned"
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

        self.client = None;
        self.spawned.store(false, Ordering::Release);
        self.set_status(ProviderStatus::Disconnected);

        tracing::info!(
            provider = PROVIDER_ANTHROPIC,
            "Anthropic HTTP adapter shut down"
        );
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
            provider = PROVIDER_ANTHROPIC,
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
            provider = PROVIDER_ANTHROPIC,
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
        tracing::info!(provider = PROVIDER_ANTHROPIC, session_id, "Session resumed");
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

        tracing::info!(provider = PROVIDER_ANTHROPIC, session_id, "Session stopped");
        Ok(())
    }

    // -- Communication (HTTP) -----------------------------------------------

    async fn send_request(
        &self,
        request: ProviderRequest,
    ) -> Result<ProviderResponse, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }

        let client = self.client.as_ref().ok_or_else(|| {
            ProviderAdapterError::Internal("HTTP client not initialized".to_string())
        })?;

        let api_key = self.resolve_api_key().ok_or_else(|| {
            ProviderAdapterError::ConfigError("No Anthropic API key configured".to_string())
        })?;

        // Extract user message from the request params
        let user_message = request
            .params
            .as_ref()
            .and_then(|p| p.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or("");

        let system_prompt = request
            .params
            .as_ref()
            .and_then(|p| p.get("system_prompt"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());

        let body = AnthropicMessagesRequest {
            model: self.anthropic_config.model.clone(),
            max_tokens: self.anthropic_config.max_tokens,
            system: system_prompt,
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: user_message.to_string(),
            }],
            stream: Some(false),
        };

        tracing::debug!(
            provider = PROVIDER_ANTHROPIC,
            method = %request.method,
            id = request.id,
            model = %self.anthropic_config.model,
            url = %self.messages_url(),
            "Sending HTTP request to Anthropic Messages API"
        );

        let response = client
            .post(self.messages_url())
            .header("x-api-key", &api_key)
            .header("anthropic-version", &self.anthropic_config.api_version)
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderAdapterError::Internal(format!("HTTP request failed: {e}")))?;

        let status = response.status();
        let response_body = response.text().await.map_err(|e| {
            ProviderAdapterError::Internal(format!("Failed to read response body: {e}"))
        })?;

        if !status.is_success() {
            // Try to parse Anthropic error format
            if let Ok(err_resp) = serde_json::from_str::<AnthropicErrorResponse>(&response_body)
                && let Some(err) = err_resp.error
            {
                return Err(ProviderAdapterError::RpcError {
                    code: status.as_u16() as i64,
                    message: err.message,
                });
            }
            return Err(ProviderAdapterError::RpcError {
                code: status.as_u16() as i64,
                message: format!("HTTP {}: {}", status.as_u16(), response_body),
            });
        }

        // Parse successful response
        let anthropic_resp: AnthropicMessagesResponse =
            serde_json::from_str(&response_body).map_err(ProviderAdapterError::Serialization)?;

        // Extract text content from response blocks
        let text_content: String = anthropic_resp
            .content
            .iter()
            .filter_map(|block| block.text.as_ref())
            .cloned()
            .collect::<Vec<_>>()
            .join("");

        let result = serde_json::json!({
            "id": anthropic_resp.id,
            "model": anthropic_resp.model,
            "text": text_content,
            "input_tokens": anthropic_resp.usage.input_tokens,
            "output_tokens": anthropic_resp.usage.output_tokens,
        });

        Ok(ProviderResponse {
            jsonrpc: "2.0".to_string(),
            id: Some(request.id),
            result: Some(result),
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

        // If we have a client and API key, try a lightweight check
        let client = match &self.client {
            Some(c) => c,
            None => return Ok(false),
        };

        let api_key = match self.resolve_api_key() {
            Some(k) => k,
            None => return Ok(false),
        };

        // Quick check: try to reach the base URL
        let base = self.anthropic_config.base_url.trim_end_matches('/');
        let result = client
            .head(format!("{}/v1/messages", base))
            .header("x-api-key", &api_key)
            .header("anthropic-version", &self.anthropic_config.api_version)
            .send()
            .await;

        match result {
            Ok(resp) => Ok(resp.status().is_success() || resp.status().as_u16() == 405),
            Err(_) => Ok(self.status() != ProviderStatus::Disconnected
                && self.status() != ProviderStatus::Error),
        }
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
            working_dir: "/tmp/test-anthropic-project".to_string(),
            system_prompt: Some("You are a helpful assistant.".to_string()),
            user_input: "Explain Rust lifetimes".to_string(),
            context_files: vec![],
        }
    }

    fn make_spawn_config() -> ProviderConfig {
        ProviderConfig {
            provider_id: PROVIDER_ANTHROPIC.to_string(),
            model: "claude-sonnet-4-20250514".to_string(),
            api_key: Some("sk-ant-test-key".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn anthropic_config_defaults() {
        let config = AnthropicConfig::default();
        assert_eq!(config.base_url, "https://api.anthropic.com");
        assert_eq!(config.model, "claude-sonnet-4-20250514");
        assert_eq!(config.max_tokens, 4096);
        assert_eq!(config.api_version, "2023-06-01");
    }

    #[tokio::test]
    async fn anthropic_adapter_new() {
        let adapter = AnthropicAdapter::new();
        assert_eq!(adapter.provider_id(), PROVIDER_ANTHROPIC);
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(!adapter.spawned.load(Ordering::Acquire));
        assert!(adapter.client.is_none());
    }

    #[tokio::test]
    async fn anthropic_adapter_spawn_and_shutdown() {
        let mut adapter = AnthropicAdapter::new();
        assert!(adapter.spawn(make_spawn_config()).await.is_ok());
        assert_eq!(adapter.status(), ProviderStatus::Idle);
        assert!(adapter.spawned.load(Ordering::Acquire));
        assert!(adapter.client.is_some());

        assert!(adapter.shutdown().await.is_ok());
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(adapter.client.is_none());
    }

    #[tokio::test]
    async fn anthropic_adapter_double_spawn_fails() {
        let mut adapter = AnthropicAdapter::new();
        assert!(adapter.spawn(make_spawn_config()).await.is_ok());
        let result = adapter.spawn(ProviderConfig::default()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already spawned"));
    }

    #[tokio::test]
    async fn anthropic_adapter_shutdown_not_spawned_fails() {
        let mut adapter = AnthropicAdapter::new();
        let result = adapter.shutdown().await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn anthropic_adapter_session_lifecycle() {
        let mut adapter = AnthropicAdapter::new();
        adapter.spawn(make_spawn_config()).await.unwrap();

        let session_id = adapter.start_session(make_ctx()).await.unwrap();
        assert!(session_id.starts_with("anthropic-"));
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
    async fn anthropic_adapter_session_without_spawn_fails() {
        let mut adapter = AnthropicAdapter::new();
        let result = adapter.start_session(make_ctx()).await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn anthropic_adapter_send_request_no_api_key_fails() {
        let mut adapter = AnthropicAdapter::new();
        // Spawn without API key
        adapter
            .spawn(ProviderConfig {
                provider_id: PROVIDER_ANTHROPIC.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let req = ProviderRequest::new(
            "message",
            Some(serde_json::json!({
                "message": "hello"
            })),
        );
        let result = adapter.send_request(req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("API key"));
    }

    #[tokio::test]
    async fn anthropic_adapter_send_request_not_spawned_fails() {
        let adapter = AnthropicAdapter::new();
        let req = ProviderRequest::new(
            "message",
            Some(serde_json::json!({
                "message": "hello"
            })),
        );
        let result = adapter.send_request(req).await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn anthropic_adapter_health_check() {
        let adapter = AnthropicAdapter::new();
        assert!(!adapter.health_check().await.unwrap());

        let mut adapter = AnthropicAdapter::new();
        adapter.spawn(make_spawn_config()).await.unwrap();
        assert!(adapter.health_check().await.unwrap());
    }

    #[tokio::test]
    async fn anthropic_adapter_capabilities() {
        let adapter = AnthropicAdapter::new();
        let caps = adapter.capabilities();
        // The HTTP adapter only does non-streaming single-turn text
        // completions with a system prompt. It must NOT advertise the
        // unimplemented features (streaming, tool-use, vision, code
        // execution, filesystem, steering).
        assert!(
            caps.contains(&ProviderCapability::SystemPrompt),
            "anthropic adapter honours system prompts, got {caps:?}"
        );
        assert!(
            !caps.contains(&ProviderCapability::Streaming),
            "anthropic HTTP adapter does not stream, got {caps:?}"
        );
        assert!(
            !caps.contains(&ProviderCapability::ToolUse),
            "anthropic HTTP adapter has no tool-use wire format, got {caps:?}"
        );
        assert!(
            !caps.contains(&ProviderCapability::Vision),
            "anthropic HTTP adapter does not handle image inputs, got {caps:?}"
        );
        assert!(
            !caps.contains(&ProviderCapability::CodeExecution),
            "anthropic HTTP adapter does not execute code, got {caps:?}"
        );
        assert!(
            !caps.contains(&ProviderCapability::FileSystem),
            "anthropic HTTP adapter does not touch the filesystem, got {caps:?}"
        );
    }

    #[tokio::test]
    async fn anthropic_adapter_available_models() {
        let adapter = AnthropicAdapter::new();
        let models = adapter.available_models();
        assert_eq!(models.len(), 4);
        assert!(models.contains(&"claude-sonnet-4-20250514".to_string()));
        assert!(models.contains(&"claude-3-5-sonnet-20241022".to_string()));
        assert!(models.contains(&"claude-3-5-haiku-20241022".to_string()));
        assert!(models.contains(&"claude-3-opus-20240229".to_string()));
    }

    #[tokio::test]
    async fn anthropic_adapter_with_custom_base_url() {
        let config = AnthropicConfig {
            base_url: "https://my-proxy.example.com".to_string(),
            api_key: Some("sk-custom".to_string()),
            model: "claude-custom".to_string(),
            max_tokens: 8192,
            api_version: "2024-01-01".to_string(),
        };
        let adapter = AnthropicAdapter::with_anthropic_config(config);
        assert_eq!(adapter.base_url(), "https://my-proxy.example.com");
        assert!(adapter.has_api_key());
        assert_eq!(adapter.provider_id(), PROVIDER_ANTHROPIC);

        // Verify messages_url strips trailing slash
        assert_eq!(
            adapter.messages_url(),
            "https://my-proxy.example.com/v1/messages"
        );
    }

    #[tokio::test]
    async fn anthropic_adapter_event_stream() {
        let adapter = AnthropicAdapter::new();
        let result = adapter.event_stream("test-session");
        assert!(result.is_ok());
    }

    #[test]
    fn anthropic_messages_request_serialization() {
        let req = AnthropicMessagesRequest {
            model: "claude-sonnet-4-20250514".to_string(),
            max_tokens: 4096,
            system: Some("Be helpful.".to_string()),
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: "Hello".to_string(),
            }],
            stream: Some(false),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"system\":\"Be helpful.\""));
        assert!(json.contains("\"role\":\"user\""));
        assert!(json.contains("\"content\":\"Hello\""));
        assert!(!json.contains("\"stream\":null"));
    }
}

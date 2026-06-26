//! OpenAI HTTP adapter — OpenAI Chat Completions API with custom base URL
//!
//! HTTP-based adapter that calls the OpenAI Chat Completions API directly.
//! Supports custom `base_url` for Azure OpenAI, self-hosted vLLM, Ollama,
//! LiteLLM, or any OpenAI-compatible endpoint.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{broadcast, Mutex};

use super::super::trait_def::*;
use crate::session::SessionState;

// ---------------------------------------------------------------------------
// OpenAI-specific configuration
// ---------------------------------------------------------------------------

/// OpenAI Chat Completions API configuration
#[derive(Debug, Clone)]
pub struct OpenAIConfig {
    /// OpenAI API key (if not set, reads OPENAI_API_KEY env var)
    pub api_key: Option<String>,
    /// OpenAI API base URL (default: "https://api.openai.com")
    /// Set to a custom URL for Azure OpenAI, vLLM, Ollama, LiteLLM, etc.
    pub base_url: String,
    /// Default model to use
    pub model: String,
    /// Maximum tokens per response
    pub max_tokens: u32,
    /// Optional OpenAI organization ID (sent as header)
    pub organization_id: Option<String>,
}

impl Default for OpenAIConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("OPENAI_API_KEY").ok(),
            base_url: "https://api.openai.com".to_string(),
            model: "gpt-4o".to_string(),
            max_tokens: 4096,
            organization_id: None,
        }
    }
}

// ---------------------------------------------------------------------------
// OpenAI Chat Completions API request/response types
// ---------------------------------------------------------------------------

/// Request body for the OpenAI Chat Completions API
#[derive(Debug, Clone, serde::Serialize)]
struct OpenAIChatRequest {
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    messages: Option<Vec<OpenAIChatMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

/// A chat message in the OpenAI format
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct OpenAIChatMessage {
    role: String,
    content: String,
}

/// Response from the OpenAI Chat Completions API (simplified)
#[derive(Debug, Clone, serde::Deserialize)]
struct OpenAIChatResponse {
    id: String,
    object: String,
    created: u64,
    model: String,
    choices: Vec<OpenAIChoice>,
    #[serde(default)]
    usage: OpenAIUsage,
}

/// A choice in the OpenAI response
#[derive(Debug, Clone, serde::Deserialize)]
struct OpenAIChoice {
    index: u32,
    message: OpenAIChatMessage,
    #[serde(default)]
    finish_reason: Option<String>,
}

/// Usage info from OpenAI
#[derive(Debug, Clone, Default, serde::Deserialize)]
struct OpenAIUsage {
    #[serde(default)]
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
    #[serde(default)]
    total_tokens: u32,
}

/// OpenAI API error response
#[derive(Debug, Clone, serde::Deserialize)]
struct OpenAIErrorResponse {
    error: Option<OpenAIErrorDetail>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct OpenAIErrorDetail {
    #[serde(default)]
    message: String,
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    code: Option<String>,
}

// ---------------------------------------------------------------------------
// OpenAIAdapter
// ---------------------------------------------------------------------------

/// HTTP-based OpenAI Chat Completions API adapter
///
/// Communicates with the OpenAI API (or any compatible endpoint) via
/// HTTP POST. Supports custom base URLs for self-hosted setups,
/// Azure OpenAI, vLLM, Ollama, LiteLLM, etc.
pub struct OpenAIAdapter {
    config: Option<ProviderConfig>,
    openai_config: OpenAIConfig,
    status: AtomicU64,
    sessions: Mutex<HashMap<String, Arc<SessionState>>>,
    event_tx: broadcast::Sender<ProviderEvent>,
    spawned: AtomicBool,
    next_req_id: AtomicU64,
    client: Option<reqwest::Client>,
}

impl OpenAIAdapter {
    /// Create a new OpenAI adapter with default settings
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            openai_config: OpenAIConfig::default(),
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            event_tx,
            spawned: AtomicBool::new(false),
            next_req_id: AtomicU64::new(1),
            client: None,
        }
    }

    /// Create a new OpenAI adapter with custom configuration
    pub fn with_openai_config(openai_config: OpenAIConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            openai_config,
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            event_tx,
            spawned: AtomicBool::new(false),
            next_req_id: AtomicU64::new(1),
            client: None,
        }
    }

    /// Check if the OpenAI API key is configured
    pub fn has_api_key(&self) -> bool {
        self.openai_config.api_key.is_some()
            || std::env::var("OPENAI_API_KEY").is_ok()
    }

    /// Get the base URL this adapter is configured to use
    pub fn base_url(&self) -> &str {
        &self.openai_config.base_url
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
    }

    fn next_request_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    fn generate_session_id() -> String {
        format!("openai-{}", uuid::Uuid::new_v4().hyphenated())
    }

    /// Build the chat completions endpoint URL from the configured base URL
    fn chat_url(&self) -> String {
        let base = self.openai_config.base_url.trim_end_matches('/');
        format!("{}/v1/chat/completions", base)
    }

    /// Resolve the API key from config or environment
    fn resolve_api_key(&self) -> Option<String> {
        self.openai_config.api_key.clone()
            .or_else(|| std::env::var("OPENAI_API_KEY").ok())
            .or_else(|| self.config.as_ref().and_then(|c| c.api_key.clone()))
    }
}

impl Default for OpenAIAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ProviderAdapter for OpenAIAdapter {
    // -- Identity ----------------------------------------------------------

    fn provider_id(&self) -> &str {
        PROVIDER_OPENAI
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        vec![
            ProviderCapability::Streaming,
            ProviderCapability::ToolUse,
            ProviderCapability::Vision,
            ProviderCapability::CodeExecution,
            ProviderCapability::FileSystem,
            ProviderCapability::SystemPrompt,
        ]
    }

    fn status(&self) -> ProviderStatus {
        self.status.load(Ordering::Acquire).into()
    }

    fn available_models(&self) -> Vec<String> {
        vec![
            "gpt-4o".to_string(),
            "gpt-4o-mini".to_string(),
            "gpt-4-turbo".to_string(),
            "gpt-3.5-turbo".to_string(),
        ]
    }

    // -- Lifecycle ---------------------------------------------------------

    async fn spawn(&mut self, config: ProviderConfig) -> Result<(), ProviderAdapterError> {
        if self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::ConfigError(
                "OpenAI adapter already spawned".to_string(),
            ));
        }

        let api_key = config.api_key.clone()
            .or_else(|| self.resolve_api_key());
        let has_key = api_key.is_some();

        if !has_key {
            tracing::warn!(
                provider = PROVIDER_OPENAI,
                "No OpenAI API key found. Set OPENAI_API_KEY env var or pass api_key in config."
            );
        }

        // Create the HTTP client
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .map_err(|e| ProviderAdapterError::ConfigError(format!(
                "Failed to create HTTP client: {e}"
            )))?;

        self.client = Some(client);

        // Apply config overrides to openai_config
        if let Some(base_url) = &config.base_url {
            self.openai_config.base_url = base_url.clone();
        }
        if !config.model.is_empty() {
            self.openai_config.model = config.model.clone();
        }
        if let Some(max_tokens) = config.max_tokens {
            self.openai_config.max_tokens = max_tokens;
        }
        if let Some(org_id) = config.extra.get("organization_id") {
            if let Some(org) = org_id.as_str() {
                self.openai_config.organization_id = Some(org.to_string());
            }
        }

        self.config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);

        tracing::info!(
            provider = PROVIDER_OPENAI,
            model = %self.openai_config.model,
            base_url = %self.openai_config.base_url,
            has_api_key = has_key,
            org_id = ?self.openai_config.organization_id,
            "OpenAI HTTP adapter spawned"
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

        tracing::info!(provider = PROVIDER_OPENAI, "OpenAI HTTP adapter shut down");
        Ok(())
    }

    async fn interrupt(&self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let sessions = self.sessions.lock().await;
        if !sessions.contains_key(session_id) {
            return Err(ProviderAdapterError::SessionNotFound(session_id.to_string()));
        }
        tracing::info!(provider = PROVIDER_OPENAI, session_id, "Interrupting session");
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
            provider = PROVIDER_OPENAI,
            session_id = %session_id,
            "Session started"
        );

        Ok(session_id)
    }

    async fn resume_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let sessions = self.sessions.lock().await;
        if !sessions.contains_key(session_id) {
            return Err(ProviderAdapterError::SessionNotFound(session_id.to_string()));
        }
        tracing::info!(provider = PROVIDER_OPENAI, session_id, "Session resumed");
        Ok(())
    }

    async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let mut sessions = self.sessions.lock().await;
        if sessions.remove(session_id).is_none() {
            return Err(ProviderAdapterError::SessionNotFound(session_id.to_string()));
        }

        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        });

        tracing::info!(provider = PROVIDER_OPENAI, session_id, "Session stopped");
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
            ProviderAdapterError::ConfigError(
                "No OpenAI API key configured".to_string(),
            )
        })?;

        // Extract messages and system prompt from request params
        let user_message = request.params
            .as_ref()
            .and_then(|p| p.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or("");

        let system_prompt = request.params
            .as_ref()
            .and_then(|p| p.get("system_prompt"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());

        // Build messages array: system prompt first (if any), then user message
        let mut messages = Vec::new();
        if let Some(sys) = system_prompt {
            messages.push(OpenAIChatMessage {
                role: "system".to_string(),
                content: sys,
            });
        }
        messages.push(OpenAIChatMessage {
            role: "user".to_string(),
            content: user_message.to_string(),
        });

        let body = OpenAIChatRequest {
            model: self.openai_config.model.clone(),
            max_tokens: Some(self.openai_config.max_tokens),
            messages: Some(messages),
            stream: Some(false),
        };

        tracing::debug!(
            provider = PROVIDER_OPENAI,
            method = %request.method,
            id = request.id,
            model = %self.openai_config.model,
            url = %self.chat_url(),
            "Sending HTTP request to OpenAI Chat Completions API"
        );

        // Build request with Bearer auth
        let mut http_req = client
            .post(self.chat_url())
            .header("Authorization", format!("Bearer {}", api_key))
            .header("content-type", "application/json");

        // Add organization header if configured
        if let Some(org_id) = &self.openai_config.organization_id {
            http_req = http_req.header("OpenAI-Organization", org_id);
        }

        let response = http_req
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderAdapterError::Internal(format!(
                "HTTP request failed: {e}"
            )))?;

        let status = response.status();
        let response_body = response.text().await.map_err(|e| {
            ProviderAdapterError::Internal(format!("Failed to read response body: {e}"))
        })?;

        if !status.is_success() {
            // Try to parse OpenAI error format
            if let Ok(err_resp) = serde_json::from_str::<OpenAIErrorResponse>(&response_body) {
                if let Some(err) = err_resp.error {
                    return Err(ProviderAdapterError::RpcError {
                        code: status.as_u16() as i64,
                        message: err.message,
                    });
                }
            }
            return Err(ProviderAdapterError::RpcError {
                code: status.as_u16() as i64,
                message: format!("HTTP {}: {}", status.as_u16(), response_body),
            });
        }

        // Parse successful response
        let openai_resp: OpenAIChatResponse =
            serde_json::from_str(&response_body).map_err(|e| {
                ProviderAdapterError::Serialization(e)
            })?;

        // Extract the assistant's message from the first choice
        let text_content = openai_resp.choices
            .first()
            .map(|c| c.message.content.clone())
            .unwrap_or_default();

        let result = serde_json::json!({
            "id": openai_resp.id,
            "model": openai_resp.model,
            "text": text_content,
            "finish_reason": openai_resp.choices.first().and_then(|c| c.finish_reason.clone()),
            "prompt_tokens": openai_resp.usage.prompt_tokens,
            "completion_tokens": openai_resp.usage.completion_tokens,
            "total_tokens": openai_resp.usage.total_tokens,
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

        // Quick check: try to reach the models endpoint
        let base = self.openai_config.base_url.trim_end_matches('/');
        let result = client
            .get(format!("{}/v1/models", base))
            .header("Authorization", format!("Bearer {}", api_key))
            .send()
            .await;

        match result {
            Ok(resp) => Ok(resp.status().is_success() || resp.status().as_u16() == 401),
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
            working_dir: "/tmp/test-openai-project".to_string(),
            system_prompt: Some("You are a helpful assistant.".to_string()),
            user_input: "Explain async Rust".to_string(),
            context_files: vec![],
        }
    }

    fn make_spawn_config() -> ProviderConfig {
        ProviderConfig {
            provider_id: PROVIDER_OPENAI.to_string(),
            model: "gpt-4o".to_string(),
            api_key: Some("sk-openai-test-key".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn openai_config_defaults() {
        let config = OpenAIConfig::default();
        assert_eq!(config.base_url, "https://api.openai.com");
        assert_eq!(config.model, "gpt-4o");
        assert_eq!(config.max_tokens, 4096);
        assert!(config.organization_id.is_none());
    }

    #[tokio::test]
    async fn openai_adapter_new() {
        let adapter = OpenAIAdapter::new();
        assert_eq!(adapter.provider_id(), PROVIDER_OPENAI);
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(!adapter.spawned.load(Ordering::Acquire));
        assert!(adapter.client.is_none());
    }

    #[tokio::test]
    async fn openai_adapter_spawn_and_shutdown() {
        let mut adapter = OpenAIAdapter::new();
        assert!(adapter.spawn(make_spawn_config()).await.is_ok());
        assert_eq!(adapter.status(), ProviderStatus::Idle);
        assert!(adapter.spawned.load(Ordering::Acquire));
        assert!(adapter.client.is_some());

        assert!(adapter.shutdown().await.is_ok());
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(adapter.client.is_none());
    }

    #[tokio::test]
    async fn openai_adapter_double_spawn_fails() {
        let mut adapter = OpenAIAdapter::new();
        assert!(adapter.spawn(make_spawn_config()).await.is_ok());
        let result = adapter.spawn(ProviderConfig::default()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already spawned"));
    }

    #[tokio::test]
    async fn openai_adapter_shutdown_not_spawned_fails() {
        let mut adapter = OpenAIAdapter::new();
        let result = adapter.shutdown().await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn openai_adapter_session_lifecycle() {
        let mut adapter = OpenAIAdapter::new();
        adapter.spawn(make_spawn_config()).await.unwrap();

        let session_id = adapter.start_session(make_ctx()).await.unwrap();
        assert!(session_id.starts_with("openai-"));
        assert_eq!(adapter.status(), ProviderStatus::Busy);

        assert!(adapter.resume_session(&session_id).await.is_ok());
        assert!(adapter.stop_session(&session_id).await.is_ok());

        let result = adapter.stop_session("nonexistent").await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::SessionNotFound(_));
    }

    #[tokio::test]
    async fn openai_adapter_session_without_spawn_fails() {
        let mut adapter = OpenAIAdapter::new();
        let result = adapter.start_session(make_ctx()).await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn openai_adapter_send_request_no_api_key_fails() {
        let mut adapter = OpenAIAdapter::new();
        adapter.spawn(ProviderConfig {
            provider_id: PROVIDER_OPENAI.to_string(),
            ..Default::default()
        }).await.unwrap();

        let req = ProviderRequest::new("message", Some(serde_json::json!({
            "message": "hello"
        })));
        let result = adapter.send_request(req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("API key"));
    }

    #[tokio::test]
    async fn openai_adapter_send_request_not_spawned_fails() {
        let adapter = OpenAIAdapter::new();
        let req = ProviderRequest::new("message", Some(serde_json::json!({
            "message": "hello"
        })));
        let result = adapter.send_request(req).await;
        assert!(result.is_err());
        matches!(result.unwrap_err(), ProviderAdapterError::NotSpawned);
    }

    #[tokio::test]
    async fn openai_adapter_health_check() {
        let adapter = OpenAIAdapter::new();
        assert_eq!(adapter.health_check().await.unwrap(), false);

        let mut adapter = OpenAIAdapter::new();
        adapter.spawn(make_spawn_config()).await.unwrap();
        assert_eq!(adapter.health_check().await.unwrap(), true);
    }

    #[tokio::test]
    async fn openai_adapter_capabilities() {
        let adapter = OpenAIAdapter::new();
        let caps = adapter.capabilities();
        assert!(caps.contains(&ProviderCapability::Streaming));
        assert!(caps.contains(&ProviderCapability::ToolUse));
        assert!(caps.contains(&ProviderCapability::Vision));
        assert!(caps.contains(&ProviderCapability::CodeExecution));
        assert!(caps.contains(&ProviderCapability::FileSystem));
        assert!(caps.contains(&ProviderCapability::SystemPrompt));
    }

    #[tokio::test]
    async fn openai_adapter_available_models() {
        let adapter = OpenAIAdapter::new();
        let models = adapter.available_models();
        assert_eq!(models.len(), 4);
        assert!(models.contains(&"gpt-4o".to_string()));
        assert!(models.contains(&"gpt-4o-mini".to_string()));
        assert!(models.contains(&"gpt-4-turbo".to_string()));
        assert!(models.contains(&"gpt-3.5-turbo".to_string()));
    }

    #[tokio::test]
    async fn openai_adapter_with_custom_base_url() {
        let config = OpenAIConfig {
            base_url: "https://my-vllm.example.com".to_string(),
            api_key: Some("sk-custom".to_string()),
            model: "llama-3".to_string(),
            max_tokens: 8192,
            organization_id: Some("org-123".to_string()),
        };
        let adapter = OpenAIAdapter::with_openai_config(config);
        assert_eq!(adapter.base_url(), "https://my-vllm.example.com");
        assert!(adapter.has_api_key());
        assert_eq!(adapter.provider_id(), PROVIDER_OPENAI);

        // Verify chat_url
        assert_eq!(
            adapter.chat_url(),
            "https://my-vllm.example.com/v1/chat/completions"
        );
    }

    #[tokio::test]
    async fn openai_adapter_with_organization_id() {
        let config = OpenAIConfig {
            organization_id: Some("org-abc456".to_string()),
            ..OpenAIConfig::default()
        };
        let adapter = OpenAIAdapter::with_openai_config(config);
        assert_eq!(adapter.openai_config.organization_id.as_deref(), Some("org-abc456"));
    }

    #[tokio::test]
    async fn openai_adapter_event_stream() {
        let adapter = OpenAIAdapter::new();
        let result = adapter.event_stream("test-session");
        assert!(result.is_ok());
    }

    #[test]
    fn openai_chat_request_serialization() {
        let req = OpenAIChatRequest {
            model: "gpt-4o".to_string(),
            max_tokens: Some(4096),
            messages: Some(vec![
                OpenAIChatMessage {
                    role: "system".to_string(),
                    content: "Be helpful.".to_string(),
                },
                OpenAIChatMessage {
                    role: "user".to_string(),
                    content: "Hello".to_string(),
                },
            ]),
            stream: Some(false),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"role\":\"system\""));
        assert!(json.contains("\"role\":\"user\""));
        assert!(json.contains("\"content\":\"Hello\""));
        assert!(!json.contains("\"stream\":null"));
    }

    #[test]
    fn openai_chat_response_deserialization() {
        let json = r#"{
            "id": "chatcmpl-abc123",
            "object": "chat.completion",
            "created": 1700000000,
            "model": "gpt-4o",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello! How can I help?"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 7,
                "total_tokens": 17
            }
        }"#;
        let resp: OpenAIChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, "chatcmpl-abc123");
        assert_eq!(resp.model, "gpt-4o");
        assert_eq!(resp.choices.len(), 1);
        assert_eq!(resp.choices[0].message.content, "Hello! How can I help?");
        assert_eq!(resp.usage.total_tokens, 17);
    }
}

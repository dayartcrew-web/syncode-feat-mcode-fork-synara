//! Provider registry — discover, configure, and report status
//!
//! The registry manages all available provider adapters. It supports:
//! - Registration of adapter instances
//! - Lookup by provider ID
//! - Bulk status querying
//! - Default provider selection

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::acp_provider::{AcpProvider, AcpProviderConfig};
use crate::trait_def::*;

/// A shared adapter instance wrapped for async access
pub type SharedAdapter = Arc<RwLock<dyn ProviderAdapter>>;

/// The provider registry — central hub for all adapter instances
pub struct ProviderRegistry {
    adapters: HashMap<String, SharedAdapter>,
    default_provider: String,
}

impl ProviderRegistry {
    /// Create a new empty registry with Claude as default
    pub fn new() -> Self {
        Self {
            adapters: HashMap::new(),
            default_provider: PROVIDER_CLAUDE.to_string(),
        }
    }

    /// Create a registry with a specific default provider
    pub fn with_default(default_provider: impl Into<String>) -> Self {
        Self {
            adapters: HashMap::new(),
            default_provider: default_provider.into(),
        }
    }

    /// Register an adapter instance
    pub async fn register(&mut self, adapter: impl ProviderAdapter + 'static) {
        let provider_id = adapter.provider_id().to_string();
        let shared: SharedAdapter = Arc::new(RwLock::new(adapter));
        self.adapters.insert(provider_id.clone(), shared);
        tracing::info!(provider_id = %provider_id, "Registered provider adapter");
    }

    /// Register a pre-wrapped shared adapter
    pub fn register_shared(&mut self, provider_id: String, adapter: SharedAdapter) {
        self.adapters.insert(provider_id.clone(), adapter);
        tracing::info!(provider_id = %provider_id, "Registered shared provider adapter");
    }

    /// Get a reference to an adapter by provider ID
    pub fn get(&self, provider_id: &str) -> Option<&SharedAdapter> {
        self.adapters.get(provider_id)
    }

    /// Get the default provider adapter
    pub fn default_adapter(&self) -> Option<&SharedAdapter> {
        self.adapters.get(&self.default_provider)
    }

    /// Get the default provider ID
    pub fn default_provider_id(&self) -> &str {
        &self.default_provider
    }

    /// Set the default provider
    pub fn set_default(&mut self, provider_id: impl Into<String>) -> Result<(), String> {
        let id = provider_id.into();
        if self.adapters.contains_key(&id) {
            self.default_provider = id;
            Ok(())
        } else {
            Err(format!("Provider '{}' not registered", id))
        }
    }

    /// List all registered provider IDs
    pub fn list_providers(&self) -> Vec<&str> {
        self.adapters.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a provider is registered
    pub fn is_registered(&self, provider_id: &str) -> bool {
        self.adapters.contains_key(provider_id)
    }

    /// Get the count of registered providers
    pub fn len(&self) -> usize {
        self.adapters.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.adapters.is_empty()
    }

    /// Collect status of all registered providers
    pub async fn status_report(&self) -> Vec<ProviderStatusEntry> {
        let mut entries = Vec::new();
        for (provider_id, adapter) in &self.adapters {
            let guard = adapter.read().await;
            entries.push(ProviderStatusEntry {
                provider_id: provider_id.clone(),
                status: guard.status(),
                capabilities: guard.capabilities(),
                available_models: guard.available_models(),
                is_default: provider_id == &self.default_provider,
            });
        }
        entries.sort_by(|a, b| a.provider_id.cmp(&b.provider_id));
        entries
    }

    /// Spawn all registered adapters with their configs
    pub async fn spawn_all(&self, configs: &HashMap<String, ProviderConfig>) -> Vec<SpawnResult> {
        let mut results = Vec::new();
        for (provider_id, adapter) in &self.adapters {
            let config = configs
                .get(provider_id)
                .cloned()
                .unwrap_or_else(|| ProviderConfig {
                    provider_id: provider_id.clone(),
                    model: "default".to_string(),
                    ..ProviderConfig::default()
                });
            let mut guard = adapter.write().await;
            match guard.spawn(config).await {
                Ok(()) => {
                    results.push(SpawnResult {
                        provider_id: provider_id.clone(),
                        success: true,
                        error: None,
                    });
                }
                Err(e) => {
                    results.push(SpawnResult {
                        provider_id: provider_id.clone(),
                        success: false,
                        error: Some(e.to_string()),
                    });
                }
            }
        }
        results
    }

    /// Shut down all registered adapters
    pub async fn shutdown_all(&self) -> Vec<ShutdownResult> {
        let mut results = Vec::new();
        for (provider_id, adapter) in &self.adapters {
            let mut guard = adapter.write().await;
            match guard.shutdown().await {
                Ok(()) => {
                    results.push(ShutdownResult {
                        provider_id: provider_id.clone(),
                        success: true,
                        error: None,
                    });
                }
                Err(e) => {
                    results.push(ShutdownResult {
                        provider_id: provider_id.clone(),
                        success: false,
                        error: Some(e.to_string()),
                    });
                }
            }
        }
        results
    }

    /// Run health check on all registered adapters
    pub async fn health_check_all(&self) -> Vec<HealthCheckResult> {
        let mut results = Vec::new();
        for (provider_id, adapter) in &self.adapters {
            let guard = adapter.read().await;
            let healthy = guard.health_check().await.unwrap_or(false);
            results.push(HealthCheckResult {
                provider_id: provider_id.clone(),
                healthy,
            });
        }
        results.sort_by(|a, b| a.provider_id.cmp(&b.provider_id));
        results
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ACP factory — construct an adapter by provider id
// ---------------------------------------------------------------------------

/// Construct a fresh (un-spawned) adapter for a known provider id.
///
/// The three ACP-speaking providers — [`PROVIDER_CURSOR`], [`PROVIDER_GROK`],
/// [`PROVIDER_GEMINI`] — are built as [`AcpProvider`]s configured with their
/// ACP subprocess spec. Returns `None` for any other id (HTTP providers and the
/// remaining stubs are constructed directly by their owners, not via this
/// factory). The caller is expected to `register_shared` the result.
pub fn create_by_id(provider_id: &str) -> Option<SharedAdapter> {
    let config = acp_config_for(provider_id)?;
    Some(Arc::new(RwLock::new(AcpProvider::new(config))))
}

/// Build the [`AcpProviderConfig`] for an ACP provider id, or `None` if `id`
/// is not one of the three ACP providers.
///
/// Command specs follow the mcode ACP integration:
/// - cursor: `cursor-agent acp`
/// - grok: `grok agent --no-leader stdio`
/// - gemini: `gemini --acp`
///
/// Provider-specific flags/env (endpoints, model, reasoning effort) are layered
/// on by the adapter owners; this captures the baseline that speaks ACP.
pub fn acp_config_for(id: &str) -> Option<AcpProviderConfig> {
    use crate::subprocess::SubprocessSpec;
    let (provider_id, spec, capabilities, available_models) = match id {
        PROVIDER_CURSOR => (
            PROVIDER_CURSOR,
            SubprocessSpec::new("cursor-agent").args(["acp"]),
            vec![
                ProviderCapability::Streaming,
                ProviderCapability::ToolUse,
                ProviderCapability::FileSystem,
                ProviderCapability::SystemPrompt,
            ],
            vec!["cursor-default".to_string()],
        ),
        PROVIDER_GROK => (
            PROVIDER_GROK,
            SubprocessSpec::new("grok").args(["agent", "--no-leader", "stdio"]),
            vec![
                ProviderCapability::Streaming,
                ProviderCapability::ToolUse,
                ProviderCapability::SystemPrompt,
            ],
            vec!["grok-default".to_string()],
        ),
        PROVIDER_GEMINI => (
            PROVIDER_GEMINI,
            SubprocessSpec::new("gemini").args(["--acp"]),
            vec![
                ProviderCapability::Streaming,
                ProviderCapability::ToolUse,
                ProviderCapability::Vision,
                ProviderCapability::FileSystem,
                ProviderCapability::SystemPrompt,
            ],
            vec!["gemini-2.5-pro".to_string(), "gemini-2.5-flash".to_string()],
        ),
        _ => return None,
    };
    Some(AcpProviderConfig {
        provider_id: provider_id.to_string(),
        spec,
        capabilities,
        available_models,
        client_name: "syncode".to_string(),
    })
}

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

/// Status report entry for a single provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderStatusEntry {
    pub provider_id: String,
    pub status: ProviderStatus,
    pub capabilities: Vec<ProviderCapability>,
    pub available_models: Vec<String>,
    pub is_default: bool,
}

/// Result of spawning a single provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnResult {
    pub provider_id: String,
    pub success: bool,
    pub error: Option<String>,
}

/// Result of shutting down a single provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownResult {
    pub provider_id: String,
    pub success: bool,
    pub error: Option<String>,
}

/// Result of health checking a single provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub provider_id: String,
    pub healthy: bool,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::codex::CodexAdapter;

    // A minimal test adapter for registry tests
    struct MockAdapter {
        id: String,
        spawned: std::sync::atomic::AtomicBool,
    }

    impl MockAdapter {
        fn new(id: &str) -> Self {
            Self {
                id: id.to_string(),
                spawned: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl ProviderAdapter for MockAdapter {
        fn provider_id(&self) -> &str {
            &self.id
        }

        fn capabilities(&self) -> Vec<ProviderCapability> {
            vec![ProviderCapability::Streaming]
        }

        fn status(&self) -> ProviderStatus {
            if self.spawned.load(std::sync::atomic::Ordering::Acquire) {
                ProviderStatus::Idle
            } else {
                ProviderStatus::Disconnected
            }
        }

        fn available_models(&self) -> Vec<String> {
            vec!["mock-model".to_string()]
        }

        async fn spawn(&mut self, _config: ProviderConfig) -> Result<(), ProviderAdapterError> {
            self.spawned
                .store(true, std::sync::atomic::Ordering::Release);
            Ok(())
        }

        async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
            self.spawned
                .store(false, std::sync::atomic::Ordering::Release);
            Ok(())
        }

        async fn interrupt(&self, _session_id: &str) -> Result<(), ProviderAdapterError> {
            Ok(())
        }

        async fn start_session(
            &mut self,
            _ctx: SessionContext,
        ) -> Result<String, ProviderAdapterError> {
            Ok("mock-session".to_string())
        }

        async fn resume_session(&mut self, _session_id: &str) -> Result<(), ProviderAdapterError> {
            Ok(())
        }

        async fn stop_session(&mut self, _session_id: &str) -> Result<(), ProviderAdapterError> {
            Ok(())
        }

        async fn send_request(
            &self,
            _request: ProviderRequest,
        ) -> Result<ProviderResponse, ProviderAdapterError> {
            Ok(ProviderResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(1),
                result: Some(serde_json::json!({"mock": true})),
                error: None,
            })
        }

        fn event_stream(&self, _session_id: &str) -> Result<ProviderStream, ProviderAdapterError> {
            let stream = async_stream::stream! {
                yield Ok(ProviderEvent::StatusChanged { status: ProviderStatus::Idle });
            };
            Ok(Box::pin(stream))
        }

        async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
            Ok(self.spawned.load(std::sync::atomic::Ordering::Acquire))
        }
    }

    #[tokio::test]
    async fn registry_new_defaults() {
        let registry = ProviderRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.default_provider_id(), PROVIDER_CLAUDE);
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test]
    async fn registry_register_and_get() {
        let mut registry = ProviderRegistry::new();
        registry.register(CodexAdapter::new()).await;

        assert_eq!(registry.len(), 1);
        assert!(registry.is_registered(PROVIDER_CODEX));
        assert!(!registry.is_registered("nonexistent"));
        assert!(registry.get(PROVIDER_CODEX).is_some());
    }

    #[tokio::test]
    async fn registry_list_providers() {
        let mut registry = ProviderRegistry::new();
        registry.register(CodexAdapter::new()).await;
        registry.register(MockAdapter::new("test-provider")).await;

        let providers = registry.list_providers();
        assert_eq!(providers.len(), 2);
        assert!(providers.contains(&PROVIDER_CODEX));
        assert!(providers.contains(&"test-provider"));
    }

    #[tokio::test]
    async fn registry_default_adapter() {
        let mut registry = ProviderRegistry::new();
        // Nothing registered → None
        assert!(registry.default_adapter().is_none());

        // Register codex but claude is default → None
        registry.register(CodexAdapter::new()).await;
        assert!(registry.default_adapter().is_none());

        // Set codex as default → Some
        registry.set_default(PROVIDER_CODEX).unwrap();
        assert!(registry.default_adapter().is_some());
    }

    #[tokio::test]
    async fn registry_set_default_not_registered_fails() {
        let mut registry = ProviderRegistry::new();
        let result = registry.set_default("nonexistent");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn registry_status_report() {
        let mut registry = ProviderRegistry::new();
        registry.register(CodexAdapter::new()).await;

        let report = registry.status_report().await;
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].provider_id, PROVIDER_CODEX);
        assert_eq!(report[0].status, ProviderStatus::Disconnected);
        assert!(!report[0].is_default);
    }

    #[tokio::test]
    async fn registry_spawn_all() {
        let mut registry = ProviderRegistry::new();
        registry.register(MockAdapter::new("mock1")).await;
        registry.register(MockAdapter::new("mock2")).await;

        let configs = HashMap::new();
        let results = registry.spawn_all(&configs).await;

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.success));
    }

    #[tokio::test]
    async fn registry_shutdown_all() {
        let mut registry = ProviderRegistry::new();
        registry.register(MockAdapter::new("mock1")).await;

        // Spawn first
        let configs = HashMap::new();
        registry.spawn_all(&configs).await;

        // Then shutdown
        let results = registry.shutdown_all().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].success);
    }

    #[tokio::test]
    async fn registry_health_check_all() {
        let mut registry = ProviderRegistry::new();
        registry.register(MockAdapter::new("mock1")).await;

        // Not spawned → unhealthy
        let results = registry.health_check_all().await;
        assert_eq!(results.len(), 1);
        assert!(!results[0].healthy);

        // Spawn → healthy
        let configs = HashMap::new();
        registry.spawn_all(&configs).await;

        let results = registry.health_check_all().await;
        assert!(results[0].healthy);
    }

    #[tokio::test]
    async fn registry_with_custom_default() {
        let registry = ProviderRegistry::with_default(PROVIDER_CODEX);
        assert_eq!(registry.default_provider_id(), PROVIDER_CODEX);
    }

    // --- ACP factory -------------------------------------------------------

    #[tokio::test]
    async fn create_by_id_builds_acp_providers() {
        for id in [PROVIDER_CURSOR, PROVIDER_GROK, PROVIDER_GEMINI] {
            let adapter = create_by_id(id).unwrap_or_else(|| panic!("no adapter for {id}"));
            let guard = adapter.read().await;
            assert_eq!(guard.provider_id(), id, "identity mismatch for {id}");
            assert_eq!(guard.status(), ProviderStatus::Disconnected);
            assert!(!guard.capabilities().is_empty());
            assert!(!guard.available_models().is_empty());
        }
    }

    #[tokio::test]
    async fn create_by_id_unknown_returns_none() {
        assert!(create_by_id("nonexistent").is_none());
        // HTTP providers and stubs are not ACP — not produced by this factory.
        assert!(create_by_id(PROVIDER_OPENAI).is_none());
        assert!(create_by_id(PROVIDER_CODEX).is_none());
    }

    #[test]
    fn acp_config_for_specs_match_mcode_acp_integration() {
        let cursor = acp_config_for(PROVIDER_CURSOR).unwrap();
        assert_eq!(cursor.provider_id, PROVIDER_CURSOR);
        assert_eq!(cursor.spec.command, "cursor-agent");
        assert_eq!(cursor.spec.args, vec!["acp".to_string()]);

        let grok = acp_config_for(PROVIDER_GROK).unwrap();
        assert_eq!(grok.spec.command, "grok");
        assert_eq!(
            grok.spec.args,
            vec!["agent", "--no-leader", "stdio"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );

        let gemini = acp_config_for(PROVIDER_GEMINI).unwrap();
        assert_eq!(gemini.spec.command, "gemini");
        assert_eq!(gemini.spec.args, vec!["--acp".to_string()]);
    }

    #[test]
    fn status_entry_serialization() {
        let entry = ProviderStatusEntry {
            provider_id: "test".to_string(),
            status: ProviderStatus::Idle,
            capabilities: vec![ProviderCapability::Streaming],
            available_models: vec!["model-a".to_string()],
            is_default: true,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: ProviderStatusEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.provider_id, "test");
        assert!(deserialized.is_default);
    }
}

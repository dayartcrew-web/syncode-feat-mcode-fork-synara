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
/// Each ACP provider owns its spec in its module
/// ([`crate::adapters::cursor::spec`] / [`crate::adapters::grok::spec`] /
/// [`crate::adapters::gemini::spec`]); cursor and grok layer provider-specific
/// flags in from `SYNICODE_*` environment variables.
///
/// Command forms follow the mcode ACP integration:
/// - cursor: `cursor-agent [-e <endpoint>] acp`
/// - grok: `grok agent [--always-approve] [-m <model>] [--reasoning-effort <effort>] --no-leader stdio`
/// - gemini: `gemini --acp`
pub fn acp_config_for(id: &str) -> Option<AcpProviderConfig> {
    use crate::adapters::{cursor, gemini, grok};
    match id {
        PROVIDER_CURSOR => Some(cursor::spec()),
        PROVIDER_GROK => Some(grok::spec()),
        PROVIDER_GEMINI => Some(gemini::spec()),
        _ => None,
    }
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
// Per-provider option descriptors (model list + capability flags)
// ---------------------------------------------------------------------------

/// Per-provider option descriptor surfaced by `provider.listOptions`. Carries
/// the real model list + capability flags read from each provider's adapter
/// (constructed un-spawned — `new()` is cheap and side-effect-free). Defaults
/// are surfaced for fields a provider does not natively expose so the shape is
/// invariant across all entries (the UI's `.supportsTemperature` etc. reads
/// must never crash on a missing field).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOptionInfo {
    /// Provider identifier (e.g. `"codex"`, `"claude"`).
    pub provider: String,
    /// Models the provider accepts (informational; sourced from the adapter's
    /// `available_models()`).
    pub models: Vec<String>,
    /// Whether the provider accepts a sampling-temperature parameter. All
    /// current adapters front LLM endpoints that accept temperature, so this is
    /// `true` for every known provider.
    pub supports_temperature: bool,
    /// Whether the provider accepts a system prompt / custom instructions.
    /// Derived from the adapter's `ProviderCapability::SystemPrompt` flag.
    pub supports_system_prompt: bool,
    /// Whether the provider accepts a tool/function-calling configuration.
    /// Derived from `ProviderCapability::ToolUse`.
    pub supports_tool_use: bool,
    /// Whether the provider can stream token-by-token responses.
    /// Derived from `ProviderCapability::Streaming`.
    pub supports_streaming: bool,
    /// Whether the provider can handle image inputs.
    /// Derived from `ProviderCapability::Vision`.
    pub supports_vision: bool,
    /// Default maximum tokens for a single response. Sourced from the adapter's
    /// default config (`4096` for all current adapters); `0` when unknown.
    pub max_tokens: u32,
    /// Raw capability identifiers (snake_case serialization of
    /// [`ProviderCapability`]), surfaced for forward compatibility.
    pub capabilities: Vec<String>,
}

impl ProviderOptionInfo {
    /// Build a descriptor for a single provider by reading its (un-spawned)
    /// adapter's live `capabilities()` + `available_models()`. Returns `None`
    /// for provider ids that have no adapter constructor (defensive — every id
    /// in [`ALL_PROVIDERS`] is covered).
    fn from_provider_id(provider_id: &str) -> Option<Self> {
        use crate::adapters::{anthropic, claude, codex, cursor, gemini, grok, kilo, openai, opencode, pi};
        use crate::trait_def::ProviderAdapter;

        // The capability flags + model list are read from a freshly-constructed
        // (un-spawned) adapter. `new()` only allocates a broadcast channel and
        // atomic state — no I/O, no subprocess — so this is safe to call from a
        // request handler. Each branch returns the adapter's real data.
        let (caps, models, max_tokens): (Vec<ProviderCapability>, Vec<String>, u32) = match provider_id {
            PROVIDER_CODEX => {
                let a = codex::CodexAdapter::new();
                (a.capabilities(), a.available_models(), 4096)
            }
            PROVIDER_CLAUDE => {
                let a = claude::ClaudeAdapter::new();
                (a.capabilities(), a.available_models(), 4096)
            }
            PROVIDER_CURSOR => {
                let a = cursor::create();
                (a.capabilities(), a.available_models(), 4096)
            }
            PROVIDER_GEMINI => {
                let a = gemini::create();
                (a.capabilities(), a.available_models(), 4096)
            }
            PROVIDER_GROK => {
                let a = grok::create();
                (a.capabilities(), a.available_models(), 4096)
            }
            PROVIDER_KILO => {
                let a = kilo::KiloAdapter::new();
                (a.capabilities(), a.available_models(), 4096)
            }
            PROVIDER_OPENCODE => {
                let a = opencode::OpenCodeAdapter::new();
                (a.capabilities(), a.available_models(), 4096)
            }
            PROVIDER_PI => {
                let a = pi::PiAdapter::new();
                (a.capabilities(), a.available_models(), 4096)
            }
            PROVIDER_ANTHROPIC => {
                let a = anthropic::AnthropicAdapter::new();
                (a.capabilities(), a.available_models(), 4096)
            }
            PROVIDER_OPENAI => {
                let a = openai::OpenAIAdapter::new();
                (a.capabilities(), a.available_models(), 4096)
            }
            _ => return None,
        };

        Some(Self {
            provider: provider_id.to_string(),
            supports_temperature: true,
            supports_system_prompt: caps.contains(&ProviderCapability::SystemPrompt),
            supports_tool_use: caps.contains(&ProviderCapability::ToolUse),
            supports_streaming: caps.contains(&ProviderCapability::Streaming),
            supports_vision: caps.contains(&ProviderCapability::Vision),
            max_tokens,
            capabilities: caps
                .iter()
                .filter_map(|c| serde_json::to_value(c).ok())
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect(),
            models,
        })
    }
}

/// Build the per-provider option descriptor list — one [`ProviderOptionInfo`]
/// per id in [`ALL_PROVIDERS`], each carrying real model + capability data read
/// from its adapter. Entries appear in [`ALL_PROVIDERS`] order. Unknown ids
/// (none currently) are skipped defensively.
pub fn all_provider_option_infos() -> Vec<ProviderOptionInfo> {
    ALL_PROVIDERS
        .iter()
        .filter_map(|id| ProviderOptionInfo::from_provider_id(id))
        .collect()
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

    // --- Per-provider option descriptors ------------------------------------

    #[test]
    fn all_provider_option_infos_covers_every_known_provider() {
        let infos = all_provider_option_infos();
        // Every id in ALL_PROVIDERS has an adapter constructor, so none are
        // dropped.
        assert_eq!(
            infos.len(),
            ALL_PROVIDERS.len(),
            "every ALL_PROVIDERS id must yield a ProviderOptionInfo"
        );
        let ids: Vec<&str> = infos.iter().map(|i| i.provider.as_str()).collect();
        for id in ALL_PROVIDERS {
            assert!(ids.contains(id), "missing option info for provider {id}");
        }
    }

    #[test]
    fn provider_option_info_carries_real_models_and_capability_flags() {
        let infos = all_provider_option_infos();
        let codex = infos
            .iter()
            .find(|i| i.provider == PROVIDER_CODEX)
            .expect("codex option info");
        // Codex's adapter advertises a multi-model list (gpt-5.1 family).
        assert!(
            !codex.models.is_empty(),
            "codex must surface a real model list"
        );
        assert!(
            codex.models.iter().any(|m| m.contains("gpt")),
            "codex models should include gpt-family entries, got {:?}",
            codex.models
        );
        // Capability flags derived from Codex's ProviderCapability set.
        assert!(codex.supports_streaming, "codex supports streaming");
        assert!(codex.supports_tool_use, "codex supports tool use");
        assert!(
            codex.supports_system_prompt,
            "codex supports system prompts"
        );
        assert!(
            !codex.supports_vision,
            "codex does not advertise vision"
        );
        // Temperature is universally supported by the LLM endpoints we front.
        assert!(codex.supports_temperature);
        // max_tokens defaults to 4096 for every current adapter.
        assert_eq!(codex.max_tokens, 4096);
        // Raw capability identifiers are surfaced (snake_case).
        assert!(codex.capabilities.contains(&"streaming".to_string()));
        assert!(codex.capabilities.contains(&"tool_use".to_string()));
    }

    #[test]
    fn provider_option_info_serializes_invariant_shape() {
        // Every descriptor must surface all fields (no provider should omit
        // capability flags or models) so the UI's reads never crash.
        let infos = all_provider_option_infos();
        for info in &infos {
            let json = serde_json::to_string(info).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
            assert!(parsed["provider"].is_string(), "missing provider field");
            assert!(parsed["models"].is_array(), "missing models field");
            assert!(
                parsed["supports_temperature"].is_boolean(),
                "missing supportsTemperature"
            );
            assert!(
                parsed["supports_system_prompt"].is_boolean(),
                "missing supportsSystemPrompt"
            );
            assert!(
                parsed["supports_tool_use"].is_boolean(),
                "missing supportsToolUse"
            );
            assert!(
                parsed["supports_streaming"].is_boolean(),
                "missing supportsStreaming"
            );
            assert!(
                parsed["supports_vision"].is_boolean(),
                "missing supportsVision"
            );
            assert!(parsed["max_tokens"].is_number(), "missing maxTokens");
            assert!(
                parsed["capabilities"].is_array(),
                "missing capabilities field"
            );
        }
    }
}

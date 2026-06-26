//! Tauri IPC Commands
//!
//! Commands exposed to the frontend via Tauri's invoke system.

use serde::{Deserialize, Serialize};

/// Application info returned to the frontend
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppInfo {
    pub name: String,
    pub version: String,
    pub tauri_version: String,
    pub mode: String,
}

/// Provider status info
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderStatusInfo {
    pub id: String,
    pub name: String,
    pub available: bool,
    pub configured: bool,
}

/// Session summary for frontend
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSummary {
    pub id: String,
    pub provider_id: String,
    pub model: String,
    pub created_at: String,
}

/// Managed provider registry state
pub struct ProviderRegistryState {
    pub providers: std::sync::Mutex<Vec<ProviderStatusInfo>>,
}

impl ProviderRegistryState {
    pub fn new() -> Self {
        let providers = vec![
            ProviderStatusInfo { id: "claude".into(), name: "Anthropic Claude".into(), available: true, configured: false },
            ProviderStatusInfo { id: "codex".into(), name: "OpenAI Codex".into(), available: true, configured: false },
            ProviderStatusInfo { id: "gemini".into(), name: "Google Gemini".into(), available: true, configured: false },
            ProviderStatusInfo { id: "grok".into(), name: "xAI Grok".into(), available: true, configured: false },
            ProviderStatusInfo { id: "cursor".into(), name: "Cursor".into(), available: true, configured: false },
            ProviderStatusInfo { id: "opencode".into(), name: "OpenCode".into(), available: true, configured: false },
            ProviderStatusInfo { id: "kilo".into(), name: "Kilo".into(), available: true, configured: false },
            ProviderStatusInfo { id: "pi".into(), name: "Pi".into(), available: true, configured: false },
        ];
        Self { providers: std::sync::Mutex::new(providers) }
    }
}

/// Managed session store state
pub struct SessionStoreState {
    pub sessions: std::sync::Mutex<Vec<SessionSummary>>,
}

impl SessionStoreState {
    pub fn new() -> Self {
        Self { sessions: std::sync::Mutex::new(Vec::new()) }
    }
}

/// Get application metadata
#[tauri::command]
pub fn get_app_info() -> AppInfo {
    AppInfo {
        name: "Syncode".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        tauri_version: tauri::VERSION.into(),
        mode: if cfg!(debug_assertions) { "development".into() } else { "production".into() },
    }
}

/// Get the application version string
#[tauri::command]
pub fn get_version() -> String {
    env!("CARGO_PKG_VERSION").into()
}

/// List all registered providers and their status
#[tauri::command]
pub fn list_providers(registry: tauri::State<'_, ProviderRegistryState>) -> Vec<ProviderStatusInfo> {
    registry.providers.lock().unwrap().clone()
}

/// Get status of a specific provider
#[tauri::command]
pub fn get_provider_status(registry: tauri::State<'_, ProviderRegistryState>, provider_id: String) -> Option<ProviderStatusInfo> {
    registry.providers.lock().unwrap().iter().find(|p| p.id == provider_id).cloned()
}

/// List all active sessions
#[tauri::command]
pub fn list_sessions(store: tauri::State<'_, SessionStoreState>) -> Vec<SessionSummary> {
    store.sessions.lock().unwrap().clone()
}

/// Create a new AI provider session
#[tauri::command]
pub fn create_session(
    store: tauri::State<'_, SessionStoreState>,
    provider_id: String,
    model: String,
) -> SessionSummary {
    let session = SessionSummary {
        id: uuid::Uuid::new_v4().to_string(),
        provider_id,
        model,
        created_at: chrono::Utc::now().to_rfc3339(),
    };
    store.sessions.lock().unwrap().push(session.clone());
    session
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_version() {
        let v = get_version();
        assert!(!v.is_empty());
    }

    #[test]
    fn test_get_app_info() {
        let info = get_app_info();
        assert_eq!(info.name, "Syncode");
        assert!(!info.version.is_empty());
    }

    #[test]
    fn test_provider_registry() {
        let registry = ProviderRegistryState::new();
        let providers = registry.providers.lock().unwrap();
        assert_eq!(providers.len(), 8);
        assert!(providers.iter().any(|p| p.id == "claude"));
        assert!(providers.iter().any(|p| p.id == "codex"));
    }

    #[test]
    fn test_session_store() {
        let store = SessionStoreState::new();
        assert!(store.sessions.lock().unwrap().is_empty());
    }
}

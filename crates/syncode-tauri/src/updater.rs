//! Auto-update
//!
//! Update checker and updater state management for the Tauri app.
//! Checks for new versions, manages download/install flow, and reports status.

use serde::{Deserialize, Serialize};

/// Update status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStatus {
    /// Not checked yet
    Idle,
    /// Currently checking for updates
    Checking,
    /// Update available
    Available { version: String, release_notes: String },
    /// Downloading update
    Downloading { progress: f64 },
    /// Update ready to install
    Ready { version: String },
    /// Update installed, restart required
    Installed { version: String },
    /// No update available
    UpToDate,
    /// Check failed
    Error { message: String },
}

impl std::fmt::Display for UpdateStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateStatus::Idle => write!(f, "idle"),
            UpdateStatus::Checking => write!(f, "checking"),
            UpdateStatus::Available { version, .. } => write!(f, "available: {}", version),
            UpdateStatus::Downloading { progress } => write!(f, "downloading: {:.0}%", progress * 100.0),
            UpdateStatus::Ready { version } => write!(f, "ready: {}", version),
            UpdateStatus::Installed { version } => write!(f, "installed: {}", version),
            UpdateStatus::UpToDate => write!(f, "up_to_date"),
            UpdateStatus::Error { message } => write!(f, "error: {}", message),
        }
    }
}

/// Updater configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdaterConfig {
    /// Base URL for update checks
    pub endpoint: String,
    /// Current version
    pub current_version: String,
    /// Whether auto-update is enabled
    pub auto_update: bool,
    /// Check interval in seconds
    pub check_interval_secs: u64,
}

impl Default for UpdaterConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://releases.syncode.dev".to_string(),
            current_version: env!("CARGO_PKG_VERSION").to_string(),
            auto_update: true,
            check_interval_secs: 3600, // 1 hour
        }
    }
}

/// Update info returned by the check
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateInfo {
    pub version: String,
    pub release_notes: String,
    pub download_url: String,
    pub release_date: String,
    pub size_bytes: u64,
}

/// Managed updater state
pub struct UpdaterState {
    pub status: std::sync::Mutex<UpdateStatus>,
    pub config: std::sync::Mutex<UpdaterConfig>,
}

impl UpdaterState {
    pub fn new() -> Self {
        Self {
            status: std::sync::Mutex::new(UpdateStatus::Idle),
            config: std::sync::Mutex::new(UpdaterConfig::default()),
        }
    }

    /// Get the current update status
    pub fn status(&self) -> UpdateStatus {
        self.status.lock().unwrap().clone()
    }

    /// Set the update status
    pub fn set_status(&self, status: UpdateStatus) {
        *self.status.lock().unwrap() = status;
    }

    /// Check if an update is available
    pub fn is_update_available(&self) -> bool {
        matches!(*self.status.lock().unwrap(), UpdateStatus::Available { .. } | UpdateStatus::Ready { .. })
    }

    /// Get the config
    pub fn config(&self) -> UpdaterConfig {
        self.config.lock().unwrap().clone()
    }

    /// Update config
    pub fn set_config(&self, config: UpdaterConfig) {
        *self.config.lock().unwrap() = config;
    }
}

impl Default for UpdaterState {
    fn default() -> Self {
        Self::new()
    }
}

/// Compare two version strings (simplified semver)
pub fn version_greater_than(current: &str, available: &str) -> bool {
    let parse = |v: &str| -> Vec<u32> {
        v.split('.')
            .filter_map(|s| s.parse().ok())
            .collect()
    };
    let current_parts = parse(current);
    let available_parts = parse(available);
    let max_len = current_parts.len().max(available_parts.len());

    for i in 0..max_len {
        let c = current_parts.get(i).unwrap_or(&0);
        let a = available_parts.get(i).unwrap_or(&0);
        if a > c {
            return true;
        }
        if a < c {
            return false;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_status_serialization() {
        let statuses = vec![
            UpdateStatus::Idle,
            UpdateStatus::Checking,
            UpdateStatus::Available { version: "1.0.0".to_string(), release_notes: "notes".to_string() },
            UpdateStatus::Downloading { progress: 0.5 },
            UpdateStatus::Ready { version: "1.0.0".to_string() },
            UpdateStatus::Installed { version: "1.0.0".to_string() },
            UpdateStatus::UpToDate,
            UpdateStatus::Error { message: "fail".to_string() },
        ];
        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let back: UpdateStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, back);
        }
    }

    #[test]
    fn update_status_display() {
        assert_eq!(UpdateStatus::Idle.to_string(), "idle");
        assert_eq!(UpdateStatus::UpToDate.to_string(), "up_to_date");
        assert_eq!(
            UpdateStatus::Downloading { progress: 0.75 }.to_string(),
            "downloading: 75%"
        );
    }

    #[test]
    fn updater_config_default() {
        let config = UpdaterConfig::default();
        assert_eq!(config.current_version, env!("CARGO_PKG_VERSION"));
        assert!(config.auto_update);
        assert_eq!(config.check_interval_secs, 3600);
    }

    #[test]
    fn updater_state_new() {
        let state = UpdaterState::new();
        assert_eq!(state.status(), UpdateStatus::Idle);
        assert!(!state.is_update_available());
    }

    #[test]
    fn updater_state_update_available() {
        let state = UpdaterState::new();
        state.set_status(UpdateStatus::Available {
            version: "2.0.0".to_string(),
            release_notes: "new stuff".to_string(),
        });
        assert!(state.is_update_available());
    }

    #[test]
    fn updater_state_set_config() {
        let state = UpdaterState::new();
        let mut config = state.config();
        config.auto_update = false;
        state.set_config(config);
        assert!(!state.config().auto_update);
    }

    #[test]
    fn version_greater_than_major() {
        assert!(version_greater_than("1.0.0", "2.0.0"));
        assert!(!version_greater_than("2.0.0", "1.0.0"));
        assert!(!version_greater_than("1.0.0", "1.0.0"));
    }

    #[test]
    fn version_greater_than_minor() {
        assert!(version_greater_than("1.0.0", "1.1.0"));
        assert!(version_greater_than("1.0.0", "1.0.1"));
        assert!(!version_greater_than("1.1.0", "1.0.0"));
    }

    #[test]
    fn version_greater_than_patch() {
        assert!(version_greater_than("1.0.0", "1.0.1"));
        assert!(!version_greater_than("1.0.1", "1.0.0"));
    }

    #[test]
    fn update_info_serialization() {
        let info = UpdateInfo {
            version: "1.0.0".to_string(),
            release_notes: "First release".to_string(),
            download_url: "https://example.com/app.tar.gz".to_string(),
            release_date: "2024-01-01".to_string(),
            size_bytes: 1024,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("downloadUrl"));
        assert!(json.contains("sizeBytes"));
        let back: UpdateInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.size_bytes, 1024);
    }

    #[test]
    fn updater_config_serialization() {
        let config = UpdaterConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("checkIntervalSecs"));
        assert!(json.contains("autoUpdate"));
    }
}

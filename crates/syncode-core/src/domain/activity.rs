//! Activity — audit log entries for tracking user and system actions

use serde::{Deserialize, Serialize};
use crate::domain::primitives::{EntityId, Timestamp};

/// Activity type classification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityType {
    // Session lifecycle
    SessionStarted,
    SessionResumed,
    SessionPaused,
    SessionCompleted,
    SessionErrored,
    SessionCancelled,

    // Provider actions
    ProviderConfigured,
    ProviderSwitched,

    // Git actions
    GitCheckpointCreated,
    GitCommitCreated,
    GitPushStarted,
    GitPushCompleted,
    GitPushFailed,

    // Automation
    AutomationStarted,
    AutomationCompleted,
    AutomationFailed,

    // General
    SettingsChanged,
    ErrorLogged,
}

/// An activity is an immutable audit log entry recording
/// a significant event in the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activity {
    pub id: EntityId,
    /// Type of activity
    pub activity_type: ActivityType,
    /// Human-readable description
    pub description: String,
    /// Related project ID (if applicable)
    pub project_id: Option<EntityId>,
    /// Related thread ID (if applicable)
    pub thread_id: Option<EntityId>,
    /// Structured metadata
    pub metadata: serde_json::Value,
    pub created_at: Timestamp,
}

impl Activity {
    pub fn new(activity_type: ActivityType, description: impl Into<String>) -> Self {
        Self {
            id: EntityId::new(),
            activity_type,
            description: description.into(),
            project_id: None,
            thread_id: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            created_at: Timestamp::now(),
        }
    }

    pub fn with_project(mut self, project_id: EntityId) -> Self {
        self.project_id = Some(project_id);
        self
    }

    pub fn with_thread(mut self, thread_id: EntityId) -> Self {
        self.thread_id = Some(thread_id);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        if let serde_json::Value::Object(ref mut map) = self.metadata {
            map.insert(key.into(), value.into());
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_new_defaults() {
        let a = Activity::new(ActivityType::SessionStarted, "Session started");
        assert_eq!(a.activity_type, ActivityType::SessionStarted);
        assert_eq!(a.description, "Session started");
        assert!(a.project_id.is_none());
        assert!(a.thread_id.is_none());
        // metadata should be an empty object, not null
        assert!(a.metadata.is_object());
    }

    #[test]
    fn activity_builder_chain() {
        let pid = EntityId::new();
        let tid = EntityId::new();
        let a = Activity::new(ActivityType::GitCommitCreated, "Committed changes")
            .with_project(pid)
            .with_thread(tid)
            .with_metadata("commit_hash", "abc123")
            .with_metadata("files_changed", 5);

        assert_eq!(a.project_id, Some(pid));
        assert_eq!(a.thread_id, Some(tid));
        assert_eq!(a.metadata["commit_hash"], "abc123");
        assert_eq!(a.metadata["files_changed"], 5);
    }

    #[test]
    fn activity_all_type_variants_constructible() {
        let variants = vec![
            ActivityType::SessionStarted,
            ActivityType::SessionResumed,
            ActivityType::SessionPaused,
            ActivityType::SessionCompleted,
            ActivityType::SessionErrored,
            ActivityType::SessionCancelled,
            ActivityType::ProviderConfigured,
            ActivityType::ProviderSwitched,
            ActivityType::GitCheckpointCreated,
            ActivityType::GitCommitCreated,
            ActivityType::GitPushStarted,
            ActivityType::GitPushCompleted,
            ActivityType::GitPushFailed,
            ActivityType::AutomationStarted,
            ActivityType::AutomationCompleted,
            ActivityType::AutomationFailed,
            ActivityType::SettingsChanged,
            ActivityType::ErrorLogged,
        ];
        for v in variants {
            let a = Activity::new(v.clone(), "test");
            assert_eq!(a.activity_type, v);
        }
    }

    #[test]
    fn activity_serialization_roundtrip() {
        let a = Activity::new(ActivityType::ProviderConfigured, "Set up OpenAI")
            .with_metadata("model", "gpt-4");
        let json = serde_json::to_string(&a).expect("serialize");
        let back: Activity = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.description, a.description);
        assert_eq!(back.activity_type, a.activity_type);
    }
}

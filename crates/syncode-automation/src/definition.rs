//! Automation definition
//!
//! An AutomationDef describes a scheduled or triggered automation run:
//! what to run, when, retry/misfire policies, and metadata.

use serde::{Deserialize, Serialize};

/// Unique automation identifier
pub type AutomationId = syncode_core::EntityId;

/// Schedule type for an automation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleType {
    /// Cron expression (e.g., "0 * * * * *")
    Cron(String),
    /// Fixed interval in seconds
    Interval(u64),
    /// Run once at a specific time
    OneShot(String),
    /// Manual trigger only
    Manual,
}

/// Automation definition — the full specification of an automation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationDef {
    /// Unique identifier
    pub id: AutomationId,
    /// Human-readable name
    pub name: String,
    /// Description
    pub description: String,
    /// Whether this automation is enabled
    pub enabled: bool,
    /// Schedule type
    pub schedule: ScheduleType,
    /// Command to execute
    pub command: String,
    /// Arguments for the command
    pub args: Vec<String>,
    /// Working directory
    pub working_dir: Option<String>,
    /// Environment variables
    pub env: std::collections::HashMap<String, String>,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Retry delay in seconds
    pub retry_delay_secs: u64,
    /// Timeout in seconds (None = no timeout)
    pub timeout_secs: Option<u64>,
    /// Tags for categorization
    pub tags: Vec<String>,
    /// Creation timestamp
    pub created_at: String,
    /// Last modification timestamp
    pub updated_at: String,
    /// Project ID this automation belongs to
    pub project_id: Option<String>,
    /// Provider ID to use for AI-assisted runs
    pub provider_id: Option<String>,
    /// Model to use for AI-assisted runs
    pub model: Option<String>,
    /// Prompt template for AI-assisted runs
    pub prompt_template: Option<String>,
    /// Whether to create a git checkpoint before running
    pub checkpoint_before: bool,
    /// Whether to auto-commit changes after running
    pub auto_commit_after: bool,
    /// Scheduling pointer — the next time this automation should fire.
    /// Mirrors MCode's `next_run_at` column (the single source of truth for
    /// due-evaluation). `None` means not yet scheduled / no future fire.
    #[serde(default)]
    pub next_run_at: Option<String>,
    /// Heartbeat target — when `Some`, runs append a turn to this thread
    /// instead of creating a new one (MCode `mode: "heartbeat"`).
    #[serde(default)]
    pub target_thread_id: Option<String>,
}

impl AutomationDef {
    /// Create a new automation with minimal fields
    pub fn new(name: String, command: String, schedule: ScheduleType) -> Self {
        let now = chrono::Utc::now().to_rfc3339();
        Self {
            id: AutomationId::new(),
            name,
            description: String::new(),
            enabled: true,
            schedule,
            command,
            args: Vec::new(),
            working_dir: None,
            env: std::collections::HashMap::new(),
            max_retries: 3,
            retry_delay_secs: 30,
            timeout_secs: None,
            tags: Vec::new(),
            created_at: now.clone(),
            updated_at: now,
            project_id: None,
            provider_id: None,
            model: None,
            prompt_template: None,
            checkpoint_before: false,
            auto_commit_after: false,
            next_run_at: None,
            target_thread_id: None,
        }
    }

    /// Builder: set description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    /// Builder: set enabled
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Builder: set working directory
    pub fn with_working_dir(mut self, dir: impl Into<String>) -> Self {
        self.working_dir = Some(dir.into());
        self
    }

    /// Builder: set project ID
    pub fn with_project_id(mut self, id: impl Into<String>) -> Self {
        self.project_id = Some(id.into());
        self
    }

    /// Builder: set provider
    pub fn with_provider(
        mut self,
        provider_id: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        self.provider_id = Some(provider_id.into());
        self.model = Some(model.into());
        self
    }

    /// Builder: set retry policy
    pub fn with_retries(mut self, max_retries: u32, delay_secs: u64) -> Self {
        self.max_retries = max_retries;
        self.retry_delay_secs = delay_secs;
        self
    }

    /// Builder: set timeout
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = Some(secs);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automation_def_new() {
        let def = AutomationDef::new(
            "test-auto".to_string(),
            "echo hello".to_string(),
            ScheduleType::Manual,
        );
        assert_eq!(def.name, "test-auto");
        assert!(def.enabled);
        assert!(!def.id.as_str().is_empty());
        assert_eq!(def.max_retries, 3);
    }

    #[test]
    fn automation_def_builder() {
        let def = AutomationDef::new(
            "build".to_string(),
            "cargo build".to_string(),
            ScheduleType::Interval(300),
        )
        .with_description("Build on schedule")
        .with_working_dir("/tmp/project")
        .with_project_id("proj-123")
        .with_provider("claude", "claude-sonnet-4-20250514")
        .with_retries(5, 60)
        .with_timeout(120);

        assert_eq!(def.description, "Build on schedule");
        assert_eq!(def.working_dir.as_deref(), Some("/tmp/project"));
        assert_eq!(def.project_id.as_deref(), Some("proj-123"));
        assert_eq!(def.provider_id.as_deref(), Some("claude"));
        assert_eq!(def.max_retries, 5);
        assert_eq!(def.timeout_secs, Some(120));
    }

    #[test]
    fn schedule_type_serialization() {
        let cron = ScheduleType::Cron("0 * * * * *".to_string());
        let json = serde_json::to_string(&cron).unwrap();
        assert!(json.contains("cron"));

        let interval = ScheduleType::Interval(60);
        let json = serde_json::to_string(&interval).unwrap();
        assert!(json.contains("interval"));

        let manual = ScheduleType::Manual;
        let json = serde_json::to_string(&manual).unwrap();
        assert!(json.contains("manual"));
    }

    #[test]
    fn automation_def_roundtrip() {
        let def = AutomationDef::new(
            "test".to_string(),
            "ls".to_string(),
            ScheduleType::Cron("*/5 * * * *".to_string()),
        );
        let json = serde_json::to_string(&def).unwrap();
        let back: AutomationDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "test");
        assert_eq!(back.command, "ls");
    }

    #[test]
    fn automation_def_camel_case() {
        let def = AutomationDef::new("test".to_string(), "ls".to_string(), ScheduleType::Manual);
        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("projectId"));
        assert!(json.contains("providerId"));
        assert!(!json.contains("project_id"));
    }

    #[test]
    fn automation_def_env() {
        let mut def = AutomationDef::new(
            "env-test".to_string(),
            "echo $FOO".to_string(),
            ScheduleType::Manual,
        );
        def.env.insert("FOO".to_string(), "bar".to_string());
        assert_eq!(def.env.get("FOO").unwrap(), "bar");
    }
}

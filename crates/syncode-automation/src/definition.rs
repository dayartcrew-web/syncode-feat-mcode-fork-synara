//! Automation definition
//!
//! An AutomationDef describes a scheduled or triggered automation run:
//! what to run, when, retry/misfire policies, and metadata.

use serde::{Deserialize, Serialize};

use crate::policies::CompletionPolicy;

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
    /// How to determine whether a run completed successfully.
    #[serde(default)]
    pub completion_policy: CompletionPolicy,
    /// Monotonic version counter — bumped on every mutation of the def.
    ///
    /// Mirrors MCode's policy-versioning guard: the AI-evaluated completion
    /// check reloads the def after the (slow) LLM round trip and discards the
    /// verdict if the version changed while the call was in flight (the
    /// `stop_when` condition the evaluator read may no longer be the active
    /// one — re-evaluating against a stale prompt would be misleading). New
    /// defs start at `1`; consumers that pre-date this field deserialize to
    /// the default (`1`) via `#[serde(default)]`.
    #[serde(default = "default_version")]
    pub version: u64,
}

/// Default value for [`AutomationDef::version`] — `1` (the first revision).
/// Used by serde for backward compatibility with payloads serialized before
/// the `version` field existed.
fn default_version() -> u64 {
    1
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
            completion_policy: CompletionPolicy::default(),
            version: 1,
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

    /// Record a mutation: bump the version counter and refresh `updated_at`.
    ///
    /// Consumers that edit a def in place (e.g. an editor RPC changing the
    /// `stop_when` condition) should call this so the AI-evaluated completion
    /// stale-check can detect the change. The version is monotonic and never
    /// resets.
    pub fn bump_version(&mut self) {
        self.version = self.version.saturating_add(1);
        self.updated_at = chrono::Utc::now().to_rfc3339();
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

    #[test]
    fn automation_def_completion_policy_default() {
        let def = AutomationDef::new(
            "comp".to_string(),
            "echo hi".to_string(),
            ScheduleType::Manual,
        );
        // New automations default to the exit-code-zero completion policy.
        assert_eq!(def.completion_policy, CompletionPolicy::ExitCodeZero);
    }

    #[test]
    fn automation_def_completion_policy_roundtrip() {
        let mut def = AutomationDef::new(
            "comp".to_string(),
            "echo hi".to_string(),
            ScheduleType::Manual,
        );
        def.completion_policy = CompletionPolicy::AiEvaluated {
            stop_when: "build is green".to_string(),
            confidence_threshold: 0.8,
        };
        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("completionPolicy"));
        let back: AutomationDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.completion_policy, def.completion_policy);
    }

    #[test]
    fn automation_def_version_defaults_to_one() {
        let def = AutomationDef::new(
            "v".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        );
        assert_eq!(def.version, 1, "new defs start at version 1");
    }

    #[test]
    fn automation_def_bump_version_is_monotonic() {
        let mut def = AutomationDef::new(
            "v".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        );
        assert_eq!(def.version, 1);
        def.bump_version();
        assert_eq!(def.version, 2);
        def.bump_version();
        assert_eq!(def.version, 3);
    }

    #[test]
    fn automation_def_version_roundtrip_and_legacy_default() {
        // A def with an explicit version round-trips the value.
        let mut def = AutomationDef::new(
            "v".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        );
        def.version = 7;
        let json = serde_json::to_string(&def).unwrap();
        let back: AutomationDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.version, 7);

        // A legacy payload (serialized before `version` existed) deserializes
        // to the default (1) — no breaking change for existing stored defs.
        // We omit `version` (the field under test) but include all other
        // required fields so deserialization succeeds.
        let legacy = serde_json::json!({
            "id": def.id.as_str(),
            "name": "x",
            "description": "",
            "enabled": true,
            "schedule": "manual",
            "command": "y",
            "args": [],
            "env": {},
            "maxRetries": 3,
            "retryDelaySecs": 30,
            "tags": [],
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z",
            "checkpointBefore": false,
            "autoCommitAfter": false,
        });
        let legacy_def: AutomationDef =
            serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy_def.version, 1, "legacy defs default to version 1");
    }
}

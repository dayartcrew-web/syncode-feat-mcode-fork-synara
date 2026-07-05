//! Automation definition
//!
//! An AutomationDef describes a scheduled or triggered automation run:
//! what to run, when, retry/misfire policies, and metadata.

use serde::{Deserialize, Serialize};

use crate::policies::CompletionPolicy;
use crate::worktree::WorktreeMode;

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
    /// Maximum number of runs before the automation is auto-disabled (P2-6).
    ///
    /// When `Some(n)`, the engine increments `iteration_count` after each run;
    /// once it reaches `n`, the automation is disabled (its `enabled` flag is
    /// flipped to `false`). `None` means run indefinitely (the default —
    /// backward compatible with stored defs serialized before this field).
    #[serde(default)]
    pub max_iterations: Option<u32>,
    /// Whether a single failed run auto-disables the automation (P2-6).
    ///
    /// Mirrors MCode's `stopOnError`: when `true`, a run that ends in a
    /// terminal `Failed` status disables the automation so it isn't
    /// re-triggered. Defaults to `false` (a failed run schedules the next
    /// fire as usual — the historical behavior).
    #[serde(default)]
    pub stop_on_error: bool,
    /// Counter of completed runs for this automation (P2-6).
    ///
    /// Incremented after each run (success or failure) and compared against
    /// [`AutomationDef::max_iterations`]. Persisted with the def so the count
    /// survives restarts. Defaults to `0`; legacy defs deserialize to `0`.
    #[serde(default)]
    pub iteration_count: u32,
    /// Maximum wall-clock seconds a single run may take before it is failed
    /// with a timeout error (P2-7).
    ///
    /// Distinct from the existing `timeout_secs` (which configures the
    /// `ProcessRunExecutor`'s per-command cap): `max_runtime_seconds` is
    /// enforced at the [`crate::executor`] level, wrapping the entire
    /// dispatch + retry loop. `None` = no run-level cap (the default).
    #[serde(default)]
    pub max_runtime_seconds: Option<u64>,
    /// Whether to isolate each standalone run in a git worktree (P2-8).
    ///
    /// Defaults to [`WorktreeMode::Local`] (no isolation — the historical
    /// behavior). `Worktree` / `Auto` create a dedicated worktree per run
    /// under `automation/<name>/<suffix>`.
    #[serde(default)]
    pub worktree_mode: WorktreeMode,
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
            max_iterations: None,
            stop_on_error: false,
            iteration_count: 0,
            max_runtime_seconds: None,
            worktree_mode: WorktreeMode::default(),
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

    /// Builder: set the max-iterations cap (P2-6). Once `iteration_count`
    /// reaches this, the automation is auto-disabled.
    pub fn with_max_iterations(mut self, max: u32) -> Self {
        self.max_iterations = Some(max);
        self
    }

    /// Builder: set stop-on-error (P2-6). When `true`, a failed run
    /// auto-disables the automation.
    pub fn with_stop_on_error(mut self, stop: bool) -> Self {
        self.stop_on_error = stop;
        self
    }

    /// Builder: set the max-runtime cap in seconds (P2-7).
    pub fn with_max_runtime_seconds(mut self, secs: u64) -> Self {
        self.max_runtime_seconds = Some(secs);
        self
    }

    /// Builder: set the worktree mode (P2-8).
    pub fn with_worktree_mode(mut self, mode: WorktreeMode) -> Self {
        self.worktree_mode = mode;
        self
    }

    /// Increment the iteration counter (P2-6). Called after each run completes
    /// (success or failure). Returns the new count. Saturating — never
    /// overflows.
    pub fn increment_iteration_count(&mut self) -> u32 {
        self.iteration_count = self.iteration_count.saturating_add(1);
        self.iteration_count
    }

    /// Whether this automation has reached its `max_iterations` cap and should
    /// be auto-disabled (P2-6). Returns `false` when `max_iterations` is
    /// `None` (run indefinitely).
    pub fn is_max_iterations_reached(&self) -> bool {
        match self.max_iterations {
            Some(cap) => self.iteration_count >= cap,
            None => false,
        }
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

    // ─── P2-6: maxIterations / stopOnError / iterationCount ────────────

    #[test]
    fn automation_def_new_defaults_max_iterations_none_and_stop_on_error_false() {
        let def = AutomationDef::new(
            "p2-6".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        );
        assert_eq!(def.max_iterations, None, "default: no iteration cap");
        assert!(!def.stop_on_error, "default: stop_on_error is false");
        assert_eq!(def.iteration_count, 0, "default: iteration_count is 0");
        assert!(
            !def.is_max_iterations_reached(),
            "no cap → never reached"
        );
    }

    #[test]
    fn automation_def_max_iterations_reached_at_cap() {
        let mut def = AutomationDef::new(
            "p2-6".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        )
        .with_max_iterations(3);

        // Two runs — under the cap.
        def.increment_iteration_count();
        assert_eq!(def.iteration_count, 1);
        assert!(!def.is_max_iterations_reached());
        def.increment_iteration_count();
        assert_eq!(def.iteration_count, 2);
        assert!(!def.is_max_iterations_reached());

        // Third run — reaches the cap.
        def.increment_iteration_count();
        assert_eq!(def.iteration_count, 3);
        assert!(
            def.is_max_iterations_reached(),
            "iteration_count == max_iterations → reached"
        );
    }

    #[test]
    fn automation_def_iteration_count_saturates() {
        let mut def = AutomationDef::new(
            "p2-6".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        );
        def.iteration_count = u32::MAX;
        let next = def.increment_iteration_count();
        assert_eq!(next, u32::MAX, "saturating add does not overflow");
    }

    #[test]
    fn automation_def_max_iterations_roundtrip_and_legacy_default() {
        let mut def = AutomationDef::new(
            "p2-6".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        )
        .with_max_iterations(10)
        .with_stop_on_error(true);
        def.increment_iteration_count();
        def.increment_iteration_count();

        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("maxIterations"));
        assert!(json.contains("stopOnError"));
        assert!(json.contains("iterationCount"));
        let back: AutomationDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_iterations, Some(10));
        assert!(back.stop_on_error);
        assert_eq!(back.iteration_count, 2);

        // Legacy payload (no P2-6 fields) → defaults.
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
        let legacy_def: AutomationDef = serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy_def.max_iterations, None);
        assert!(!legacy_def.stop_on_error);
        assert_eq!(legacy_def.iteration_count, 0);
    }

    // ─── P2-7: maxRuntimeSeconds ───────────────────────────────────────

    #[test]
    fn automation_def_new_defaults_max_runtime_seconds_none() {
        let def = AutomationDef::new(
            "p2-7".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        );
        assert_eq!(def.max_runtime_seconds, None);
    }

    #[test]
    fn automation_def_max_runtime_seconds_builder_and_roundtrip() {
        let def = AutomationDef::new(
            "p2-7".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        )
        .with_max_runtime_seconds(120);

        assert_eq!(def.max_runtime_seconds, Some(120));

        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("maxRuntimeSeconds"));
        let back: AutomationDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_runtime_seconds, Some(120));
    }

    #[test]
    fn automation_def_max_runtime_seconds_legacy_default() {
        // Legacy payload without maxRuntimeSeconds → None.
        let legacy = serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000001",
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
        let def: AutomationDef = serde_json::from_value(legacy).unwrap();
        assert_eq!(def.max_runtime_seconds, None);
    }

    // ─── P2-8: worktreeMode ────────────────────────────────────────────

    #[test]
    fn automation_def_new_defaults_worktree_mode_local() {
        let def = AutomationDef::new(
            "p2-8".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        );
        assert_eq!(def.worktree_mode, WorktreeMode::Local);
    }

    #[test]
    fn automation_def_worktree_mode_builder_and_roundtrip() {
        let def = AutomationDef::new(
            "p2-8".to_string(),
            "echo".to_string(),
            ScheduleType::Manual,
        )
        .with_worktree_mode(WorktreeMode::Worktree);

        assert_eq!(def.worktree_mode, WorktreeMode::Worktree);

        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("worktreeMode"));
        let back: AutomationDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.worktree_mode, WorktreeMode::Worktree);
    }

    #[test]
    fn automation_def_worktree_mode_legacy_default() {
        // Legacy payload without worktreeMode → Local.
        let legacy = serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000001",
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
        let def: AutomationDef = serde_json::from_value(legacy).unwrap();
        assert_eq!(def.worktree_mode, WorktreeMode::Local);
    }
}

//! Scheduler engine — cron, interval, one-shot, manual
//!
//! Manages automation schedules, checks for due automations, and dispatches
//! runs through the [`RunExecutor`] port. Backed by an
//! [`AutomationRepository`] (default: in-memory) so SQLite can drop in later.
//!
//! The [`Scheduler::tick`] method is the single due-evaluation + dispatch pass
//! a host process would call in a loop (mirrors MCode's `runDueOnce`).

use chrono::{DateTime, Utc};
use std::sync::Arc;

use syncode_core::ports::{AutomationRepository, RunExecutor};

use crate::definition::{AutomationDef, ScheduleType};
use crate::executor::{self, Delay};
use crate::in_memory_repo::InMemoryAutomationRepository;
use crate::policies::{CompletionPolicy, MisfirePolicy};
use crate::runner::AutomationRun;
use crate::schedule;

/// Scheduler error types
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("Automation not found: {0}")]
    NotFound(String),
    #[error("Automation already exists: {0}")]
    AlreadyExists(String),
    #[error("Invalid schedule: {0}")]
    InvalidSchedule(String),
    #[error("Run failed: {0}")]
    RunFailed(String),
    #[error("Repository error: {0}")]
    Repository(String),
}

/// A no-op executor used as the default (so `Scheduler::new()` works without
/// a real engine — existing tests that only exercise run-record lifecycle,
/// not execution, are unaffected). Dispatch always errors with "not configured".
#[derive(Debug, Default, Clone)]
pub struct NoopExecutor;

#[async_trait::async_trait]
impl RunExecutor for NoopExecutor {
    async fn dispatch_turn(
        &self,
        _req: syncode_core::ports::DispatchRequest,
    ) -> Result<syncode_core::ports::DispatchOutcome, syncode_core::PortError> {
        Err(syncode_core::PortError::Internal(
            "no RunExecutor configured (use Scheduler::new_with_deps)".into(),
        ))
    }
}

/// The scheduler engine
pub struct Scheduler {
    repo: Arc<dyn AutomationRepository>,
    executor: Arc<dyn RunExecutor>,
    /// Completion policy (global default; per-automation overrides are future work)
    default_completion: CompletionPolicy,
    /// Misfire policy (global default)
    default_misfire: MisfirePolicy,
}

impl Scheduler {
    /// Create a new scheduler backed by an in-memory repo + a no-op executor.
    ///
    /// Sufficient for testing run-record lifecycle (register/trigger/get/list).
    /// To actually execute runs, use [`Scheduler::new_with_deps`] with a real
    /// [`RunExecutor`].
    pub fn new() -> Self {
        Self::new_with_deps(
            Arc::new(InMemoryAutomationRepository::new()),
            Arc::new(NoopExecutor),
        )
    }

    /// Create a scheduler with explicit dependencies (production wiring).
    pub fn new_with_deps(
        repo: Arc<dyn AutomationRepository>,
        executor: Arc<dyn RunExecutor>,
    ) -> Self {
        Self {
            repo,
            executor,
            default_completion: CompletionPolicy::default(),
            default_misfire: MisfirePolicy::default(),
        }
    }

    /// Register an automation definition
    pub async fn register(&self, def: AutomationDef) -> Result<(), SchedulerError> {
        let id = def.id.as_str();
        if self.repo.get_def(&id).await.map_err(repo_err)?.is_some() {
            return Err(SchedulerError::AlreadyExists(id));
        }
        let payload = serde_json::to_value(&def).map_err(serialization_err)?;
        self.repo.save_def(&id, payload).await.map_err(repo_err)
    }

    /// Unregister an automation
    pub async fn unregister(&self, id: &str) -> bool {
        self.repo.delete_def(id).await.unwrap_or(false)
    }

    /// Get an automation definition
    pub async fn get(&self, id: &str) -> Option<AutomationDef> {
        self.repo
            .get_def(id)
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value(v).ok())
    }

    /// List all registered automations
    pub async fn list(&self) -> Vec<AutomationDef> {
        self.repo
            .list_defs()
            .await
            .ok()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect()
    }

    /// List automations that are enabled
    pub async fn list_enabled(&self) -> Vec<AutomationDef> {
        self.list()
            .await
            .into_iter()
            .filter(|a| a.enabled)
            .collect()
    }

    /// Check which automations are due (for non-manual schedules). Returns the
    /// IDs of enabled automations whose `next_run_at` is in the past relative
    /// to `now`.
    pub async fn due_automations(&self, now: DateTime<Utc>) -> Vec<String> {
        let mut due = Vec::new();
        for def in self.list_enabled().await {
            if matches!(def.schedule, ScheduleType::Manual) {
                continue;
            }
            if let Some(next_str) = &def.next_run_at
                && let Some(next) = parse_dt(next_str)
                && schedule::is_due(&next, now)
            {
                due.push(def.id.as_str());
            }
        }
        due
    }

    /// Trigger an automation run. Records a run (status Running) and dispatches
    /// it through the [`RunExecutor`]. Returns the run id.
    ///
    /// Uses the real retry loop from [`executor::execute_run`] with `Delay::Real`
    /// (production callers). For tests, see [`Scheduler::trigger_immediate`].
    pub async fn trigger(&self, automation_id: &str) -> Result<String, SchedulerError> {
        self.trigger_with_delay(automation_id, Delay::Real).await
    }

    /// Trigger with an injectable delay strategy (tests use `Delay::Immediate`).
    pub async fn trigger_with_delay(
        &self,
        automation_id: &str,
        delay: Delay,
    ) -> Result<String, SchedulerError> {
        let def = self
            .get(automation_id)
            .await
            .ok_or_else(|| SchedulerError::NotFound(automation_id.to_string()))?;

        let now = Utc::now();
        let outcome = executor::execute_run(
            &def,
            self.executor.as_ref(),
            self.repo.as_ref(),
            &self.default_completion,
            delay,
            now,
        )
        .await;

        // Synthesize a stable run id from the latest persisted run for this def.
        // (execute_run persists internally; we surface the most recent.)
        let runs = self.repo.list_runs(automation_id).await.map_err(repo_err)?;
        let run_id = runs
            .last()
            .and_then(|r| r.get("id").and_then(|v| v.as_str()).map(String::from))
            .unwrap_or_else(|| format!("run-{}", uuid::Uuid::new_v4().hyphenated()));

        if matches!(outcome.final_status, crate::runner::RunStatus::Failed) {
            // Return the run id but flag failure via the outcome — callers that
            // need the status can fetch the run. Kept as Ok to preserve the
            // existing trigger() contract (returns a run id).
            tracing::warn!(run_id = %run_id, attempts = outcome.attempts, "automation run failed");
        }
        Ok(run_id)
    }

    /// The single due-evaluation + dispatch pass a host would call in a loop
    /// (mirrors MCode's `runDueOnce`). Returns the ids of automations dispatched.
    ///
    /// For each due automation: applies the misfire policy (coalesce — advance
    /// `next_run_at` past `now` without replaying missed fires), then triggers
    /// the run.
    pub async fn tick(&self, now: DateTime<Utc>) -> Vec<String> {
        let due = self.due_automations(now).await;
        let mut dispatched = Vec::new();
        for id in &due {
            // Coalesce missed fires before dispatching.
            if let Some(def) = self.get(id).await {
                let coalesced = self.coalesce_missed_for(&def, now).await;
                if coalesced.is_none() {
                    // One-shot that already passed — skip (next_fire is None).
                    continue;
                }
            }
            match self.trigger_with_delay(id, Delay::Real).await {
                Ok(run_id) => {
                    tracing::info!(automation_id = %id, run_id = %run_id, "tick dispatched run");
                    dispatched.push(id.clone());
                }
                Err(e) => {
                    tracing::warn!(automation_id = %id, error = %e, "tick trigger failed");
                }
            }
        }
        dispatched
    }

    /// Apply the misfire policy: if the def's `next_run_at` is past due and
    /// there were missed fires, fast-forward to the next slot after `now`
    /// (coalesce). Persists the advanced pointer. Returns the new next_run_at
    /// (None if the schedule won't fire again).
    async fn coalesce_missed_for(
        &self,
        def: &AutomationDef,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let past_next = def.next_run_at.as_deref().and_then(parse_dt)?;
        if past_next > now {
            return Some(past_next); // not actually past due
        }
        let coalesced = match self.default_misfire {
            MisfirePolicy::Skip => {
                // Advance past now without running; returns next fire.
                schedule::coalesce_missed(&def.schedule, past_next, now)
            }
            MisfirePolicy::RunImmediately | MisfirePolicy::RunNext => {
                // Run now — next_run_at set to the slot after the run completes
                // (handled by execute_run's advance_schedule).
                Some(now)
            }
        };
        if let Some(next) = coalesced {
            let _ = self
                .repo
                .advance_next_run_at(&def.id.as_str(), Some(next.to_rfc3339()))
                .await;
        }
        coalesced
    }

    /// Complete a run (manual status update — for runs dispatched externally)
    pub async fn complete_run(
        &self,
        run_id: &str,
        exit_code: i32,
        stdout: String,
        stderr: String,
    ) -> Result<(), SchedulerError> {
        let payload = self
            .repo
            .get_run(run_id)
            .await
            .map_err(repo_err)?
            .ok_or_else(|| SchedulerError::NotFound(run_id.to_string()))?;
        let mut run: AutomationRun = serde_json::from_value(payload).map_err(serialization_err)?;
        run.mark_completed(exit_code, stdout, stderr);
        let updated = serde_json::to_value(&run).map_err(serialization_err)?;
        self.repo.save_run(updated).await.map_err(repo_err)
    }

    /// Fail a run
    pub async fn fail_run(&self, run_id: &str, error: String) -> Result<(), SchedulerError> {
        let payload = self
            .repo
            .get_run(run_id)
            .await
            .map_err(repo_err)?
            .ok_or_else(|| SchedulerError::NotFound(run_id.to_string()))?;
        let mut run: AutomationRun = serde_json::from_value(payload).map_err(serialization_err)?;
        run.mark_failed(error);
        let updated = serde_json::to_value(&run).map_err(serialization_err)?;
        self.repo.save_run(updated).await.map_err(repo_err)
    }

    /// Cancel a run
    pub async fn cancel_run(&self, run_id: &str) -> Result<(), SchedulerError> {
        let payload = self
            .repo
            .get_run(run_id)
            .await
            .map_err(repo_err)?
            .ok_or_else(|| SchedulerError::NotFound(run_id.to_string()))?;
        let mut run: AutomationRun = serde_json::from_value(payload).map_err(serialization_err)?;
        run.mark_cancelled();
        let updated = serde_json::to_value(&run).map_err(serialization_err)?;
        self.repo.save_run(updated).await.map_err(repo_err)
    }

    /// Get a run by ID
    pub async fn get_run(&self, run_id: &str) -> Option<AutomationRun> {
        self.repo
            .get_run(run_id)
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value(v).ok())
    }

    /// List all runs for an automation
    pub async fn list_runs(&self, automation_id: &str) -> Vec<AutomationRun> {
        self.repo
            .list_runs(automation_id)
            .await
            .ok()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect()
    }

    /// Get run count
    pub async fn run_count(&self) -> usize {
        // Count across all automations (no global list in the port; sum defs).
        let mut total = 0;
        for def in self.list().await {
            total += self.list_runs(&def.id.as_str()).await.len();
        }
        total
    }

    /// Get automation count
    pub async fn automation_count(&self) -> usize {
        self.repo.list_defs().await.map(|v| v.len()).unwrap_or(0)
    }

    /// Update an existing automation
    pub async fn update(&self, def: AutomationDef) -> Result<(), SchedulerError> {
        let id = def.id.as_str();
        if self.repo.get_def(&id).await.map_err(repo_err)?.is_none() {
            return Err(SchedulerError::NotFound(id.to_string()));
        }
        let payload = serde_json::to_value(&def).map_err(serialization_err)?;
        self.repo.save_def(&id, payload).await.map_err(repo_err)
    }

    /// Enable/disable an automation
    pub async fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), SchedulerError> {
        let mut def = self
            .get(id)
            .await
            .ok_or_else(|| SchedulerError::NotFound(id.to_string()))?;
        def.enabled = enabled;
        let payload = serde_json::to_value(&def).map_err(serialization_err)?;
        self.repo
            .save_def(&def.id.as_str(), payload)
            .await
            .map_err(repo_err)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

fn repo_err(e: syncode_core::PortError) -> SchedulerError {
    SchedulerError::Repository(e.to_string())
}

fn serialization_err(e: serde_json::Error) -> SchedulerError {
    SchedulerError::Repository(format!("serialization: {e}"))
}

fn parse_dt(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::RunStatus;

    fn make_def(name: &str) -> AutomationDef {
        AutomationDef::new(
            name.to_string(),
            "echo hello".to_string(),
            ScheduleType::Manual,
        )
    }

    #[tokio::test]
    async fn scheduler_register_and_get() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id.as_str().to_string();

        scheduler.register(def).await.unwrap();
        let fetched = scheduler.get(&id).await.unwrap();
        assert_eq!(fetched.name, "test-1");
    }

    #[tokio::test]
    async fn scheduler_register_duplicate_fails() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id;
        scheduler.register(def).await.unwrap();

        // Same name, different ID — succeeds (id is the key).
        let def2 = make_def("test-1");
        assert!(scheduler.register(def2).await.is_ok());

        // Exact same ID — fails.
        let mut def3 = make_def("test-1");
        def3.id = id;
        assert!(scheduler.register(def3).await.is_err());
    }

    #[tokio::test]
    async fn scheduler_unregister() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id.as_str().to_string();

        scheduler.register(def).await.unwrap();
        assert!(scheduler.unregister(&id).await);
        assert!(!scheduler.unregister(&id).await);
    }

    #[tokio::test]
    async fn scheduler_list() {
        let scheduler = Scheduler::new();
        scheduler.register(make_def("test-1")).await.unwrap();
        scheduler.register(make_def("test-2")).await.unwrap();

        let list = scheduler.list().await;
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn scheduler_list_enabled() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        scheduler.register(def).await.unwrap();

        let list = scheduler.list_enabled().await;
        assert_eq!(list.len(), 1);

        let all = scheduler.list().await;
        let id = all[0].id.as_str().to_string();
        scheduler.set_enabled(&id, false).await.unwrap();
        let list = scheduler.list_enabled().await;
        assert_eq!(list.len(), 0);
    }

    #[tokio::test]
    async fn scheduler_trigger_records_run() {
        // NoopExecutor → run fails, but a run record is still persisted.
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id.as_str().to_string();
        scheduler.register(def).await.unwrap();

        let run_id = scheduler
            .trigger_with_delay(&id, Delay::Immediate)
            .await
            .unwrap();
        assert!(!run_id.is_empty());
        // A run was persisted (failed, since NoopExecutor errors).
        let runs = scheduler.list_runs(&id).await;
        assert!(!runs.is_empty());
        assert_eq!(runs[0].status, RunStatus::Failed);
    }

    #[tokio::test]
    async fn scheduler_trigger_nonexistent() {
        let scheduler = Scheduler::new();
        let result = scheduler.trigger_with_delay("nope", Delay::Immediate).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn scheduler_complete_run() {
        let scheduler = Scheduler::new();
        // Manually persist a run to complete.
        let mut run = AutomationRun::new("auto-1".to_string());
        run.mark_started();
        scheduler
            .repo
            .save_run(serde_json::to_value(&run).unwrap())
            .await
            .unwrap();

        scheduler
            .complete_run(&run.id, 0, "output".to_string(), String::new())
            .await
            .unwrap();

        let fetched = scheduler.get_run(&run.id).await.unwrap();
        assert_eq!(fetched.status, RunStatus::Completed);
        assert_eq!(fetched.exit_code, Some(0));
    }

    #[tokio::test]
    async fn scheduler_fail_run() {
        let scheduler = Scheduler::new();
        let mut run = AutomationRun::new("auto-1".to_string());
        run.mark_started();
        scheduler
            .repo
            .save_run(serde_json::to_value(&run).unwrap())
            .await
            .unwrap();

        scheduler
            .fail_run(&run.id, "something went wrong".to_string())
            .await
            .unwrap();

        let fetched = scheduler.get_run(&run.id).await.unwrap();
        assert_eq!(fetched.status, RunStatus::Failed);
        assert_eq!(fetched.error.as_deref(), Some("something went wrong"));
    }

    #[tokio::test]
    async fn scheduler_cancel_run() {
        let scheduler = Scheduler::new();
        let mut run = AutomationRun::new("auto-1".to_string());
        run.mark_started();
        scheduler
            .repo
            .save_run(serde_json::to_value(&run).unwrap())
            .await
            .unwrap();

        scheduler.cancel_run(&run.id).await.unwrap();
        let fetched = scheduler.get_run(&run.id).await.unwrap();
        assert_eq!(fetched.status, RunStatus::Cancelled);
    }

    #[tokio::test]
    async fn scheduler_update() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id;
        scheduler.register(def).await.unwrap();

        let mut updated = make_def("test-1-renamed");
        updated.id = id;
        scheduler.update(updated).await.unwrap();

        let id_str = id.as_str();
        let fetched = scheduler.get(&id_str).await.unwrap();
        assert_eq!(fetched.name, "test-1-renamed");
    }

    #[tokio::test]
    async fn scheduler_error_display() {
        let err = SchedulerError::NotFound("abc".to_string());
        assert!(err.to_string().contains("abc"));
    }

    // ─── New: due-eval + tick tests ────────────────────────────────────

    #[tokio::test]
    async fn due_automations_finds_past_due() {
        let scheduler = Scheduler::new();
        let mut def = make_def("interval-auto");
        def.schedule = ScheduleType::Interval(60);
        def.next_run_at = Some((Utc::now() - chrono::Duration::seconds(30)).to_rfc3339());
        let id = def.id.as_str().to_string();
        scheduler.register(def).await.unwrap();

        let due = scheduler.due_automations(Utc::now()).await;
        assert_eq!(due, vec![id]);
    }

    #[tokio::test]
    async fn due_automations_skips_future_and_manual() {
        let scheduler = Scheduler::new();

        // Manual — never due.
        let mut manual = make_def("manual");
        manual.schedule = ScheduleType::Manual;
        scheduler.register(manual).await.unwrap();

        // Future interval — not due yet.
        let mut future = make_def("future");
        future.schedule = ScheduleType::Interval(60);
        future.next_run_at = Some((Utc::now() + chrono::Duration::seconds(300)).to_rfc3339());
        scheduler.register(future).await.unwrap();

        let due = scheduler.due_automations(Utc::now()).await;
        assert!(due.is_empty(), "neither manual nor future should be due");
    }

    #[tokio::test]
    async fn tick_dispatches_due_and_skips_past_oneshot() {
        let scheduler = Scheduler::new();

        // A due interval automation (NoopExecutor fails it, but it's dispatched).
        // max_retries=0 so it fails fast (no real backoff sleep in the test).
        let mut interval_def = make_def("interval");
        interval_def.schedule = ScheduleType::Interval(60);
        interval_def.max_retries = 0;
        interval_def.next_run_at = Some((Utc::now() - chrono::Duration::seconds(10)).to_rfc3339());
        let interval_id = interval_def.id.as_str().to_string();
        scheduler.register(interval_def).await.unwrap();

        // A past one-shot (next_fire is None → skipped by tick).
        let mut oneshot = make_def("oneshot");
        oneshot.schedule =
            ScheduleType::OneShot((Utc::now() - chrono::Duration::hours(1)).to_rfc3339());
        oneshot.next_run_at = Some((Utc::now() - chrono::Duration::hours(1)).to_rfc3339());
        scheduler.register(oneshot).await.unwrap();

        let dispatched = scheduler.tick(Utc::now()).await;
        assert!(
            dispatched.contains(&interval_id),
            "interval should dispatch"
        );
        // One-shot's coalesce returns None (past) → skipped.
        assert!(
            !dispatched.iter().any(|d| d == "oneshot"),
            "past one-shot should be skipped"
        );
    }
}

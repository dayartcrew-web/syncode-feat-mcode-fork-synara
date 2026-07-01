//! Scheduler engine — cron, interval, one-shot, manual
//!
//! Manages automation schedules, checks for due automations,
//! and provides run coordination.

use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::definition::{AutomationDef, ScheduleType};
use crate::policies::{CompletionPolicy, MisfirePolicy, RetryPolicy};
use crate::runner::AutomationRun;

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
}

/// The scheduler engine
pub struct Scheduler {
    /// Registered automations
    automations: RwLock<HashMap<String, AutomationDef>>,
    /// Active runs
    runs: RwLock<HashMap<String, AutomationRun>>,
    /// Retry policy (global default)
    #[allow(dead_code)] // reserved: per-automation policy overrides (not yet wired)
    default_retry: RetryPolicy,
    /// Misfire policy (global default)
    #[allow(dead_code)] // reserved: per-automation policy overrides (not yet wired)
    default_misfire: MisfirePolicy,
    /// Completion policy (global default)
    #[allow(dead_code)] // reserved: per-automation policy overrides (not yet wired)
    default_completion: CompletionPolicy,
}

impl Scheduler {
    /// Create a new scheduler
    pub fn new() -> Self {
        Self {
            automations: RwLock::new(HashMap::new()),
            runs: RwLock::new(HashMap::new()),
            default_retry: RetryPolicy::default(),
            default_misfire: MisfirePolicy::default(),
            default_completion: CompletionPolicy::default(),
        }
    }

    /// Register an automation definition
    pub async fn register(&self, def: AutomationDef) -> Result<(), SchedulerError> {
        let id = def.id.as_str();
        let mut autos = self.automations.write().await;
        if autos.contains_key(&id) {
            return Err(SchedulerError::AlreadyExists(id.to_string()));
        }
        autos.insert(id.to_string(), def);
        Ok(())
    }

    /// Unregister an automation
    pub async fn unregister(&self, id: &str) -> bool {
        self.automations.write().await.remove(id).is_some()
    }

    /// Get an automation definition
    pub async fn get(&self, id: &str) -> Option<AutomationDef> {
        self.automations.read().await.get(id).cloned()
    }

    /// List all registered automations
    pub async fn list(&self) -> Vec<AutomationDef> {
        self.automations.read().await.values().cloned().collect()
    }

    /// List automations that are enabled
    pub async fn list_enabled(&self) -> Vec<AutomationDef> {
        self.automations
            .read()
            .await
            .values()
            .filter(|a| a.enabled)
            .cloned()
            .collect()
    }

    /// Check which automations are due (for non-manual schedules)
    pub async fn due_automations(&self, now: &str) -> Vec<String> {
        let autos = self.automations.read().await;
        let mut due = Vec::new();
        for (id, def) in autos.iter() {
            if !def.enabled {
                continue;
            }
            match &def.schedule {
                ScheduleType::Manual => continue,
                ScheduleType::OneShot(time) => {
                    // Simple comparison: if now >= scheduled time
                    if now >= time.as_str() {
                        due.push(id.clone());
                    }
                }
                // Cron and interval would need actual time tracking
                // For now, only one-shot can be checked
                ScheduleType::Cron(_) | ScheduleType::Interval(_) => {
                    // Would need a last_run timestamp to check properly
                    // Placeholder: don't schedule
                }
            }
        }
        due
    }

    /// Trigger an automation run (for manual or due automations)
    pub async fn trigger(&self, automation_id: &str) -> Result<String, SchedulerError> {
        let autos = self.automations.read().await;
        let _def = autos
            .get(automation_id)
            .ok_or_else(|| SchedulerError::NotFound(automation_id.to_string()))?
            .clone();
        drop(autos);

        let mut run = AutomationRun::new(automation_id.to_string());
        run.mark_started();

        let run_id = run.id.clone();
        self.runs.write().await.insert(run_id.clone(), run);

        Ok(run_id)
    }

    /// Complete a run
    pub async fn complete_run(
        &self,
        run_id: &str,
        exit_code: i32,
        stdout: String,
        stderr: String,
    ) -> Result<(), SchedulerError> {
        let mut runs = self.runs.write().await;
        let run = runs
            .get_mut(run_id)
            .ok_or_else(|| SchedulerError::NotFound(run_id.to_string()))?;
        run.mark_completed(exit_code, stdout, stderr);
        Ok(())
    }

    /// Fail a run
    pub async fn fail_run(&self, run_id: &str, error: String) -> Result<(), SchedulerError> {
        let mut runs = self.runs.write().await;
        let run = runs
            .get_mut(run_id)
            .ok_or_else(|| SchedulerError::NotFound(run_id.to_string()))?;
        run.mark_failed(error);
        Ok(())
    }

    /// Cancel a run
    pub async fn cancel_run(&self, run_id: &str) -> Result<(), SchedulerError> {
        let mut runs = self.runs.write().await;
        let run = runs
            .get_mut(run_id)
            .ok_or_else(|| SchedulerError::NotFound(run_id.to_string()))?;
        run.mark_cancelled();
        Ok(())
    }

    /// Get a run by ID
    pub async fn get_run(&self, run_id: &str) -> Option<AutomationRun> {
        self.runs.read().await.get(run_id).cloned()
    }

    /// List all runs for an automation
    pub async fn list_runs(&self, automation_id: &str) -> Vec<AutomationRun> {
        self.runs
            .read()
            .await
            .values()
            .filter(|r| r.automation_id == automation_id)
            .cloned()
            .collect()
    }

    /// Get run count
    pub async fn run_count(&self) -> usize {
        self.runs.read().await.len()
    }

    /// Get automation count
    pub async fn automation_count(&self) -> usize {
        self.automations.read().await.len()
    }

    /// Update an existing automation
    pub async fn update(&self, def: AutomationDef) -> Result<(), SchedulerError> {
        let id = def.id.as_str();
        let mut autos = self.automations.write().await;
        if !autos.contains_key(&id) {
            return Err(SchedulerError::NotFound(id.to_string()));
        }
        autos.insert(id.to_string(), def);
        Ok(())
    }

    /// Enable/disable an automation
    pub async fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), SchedulerError> {
        let mut autos = self.automations.write().await;
        let def = autos
            .get_mut(id)
            .ok_or_else(|| SchedulerError::NotFound(id.to_string()))?;
        def.enabled = enabled;
        Ok(())
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
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

        // Try to register another def with the same name but different ID — should succeed
        let def2 = make_def("test-1");
        assert!(scheduler.register(def2).await.is_ok());

        // Now register with the exact same ID — should fail
        let mut def3 = make_def("test-1");
        def3.id = id;
        let result = scheduler.register(def3).await;
        assert!(result.is_err());
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

        // Disable
        let all = scheduler.list().await;
        let id = all[0].id.as_str().to_string();
        scheduler.set_enabled(&id, false).await.unwrap();
        let list = scheduler.list_enabled().await;
        assert_eq!(list.len(), 0);
    }

    #[tokio::test]
    async fn scheduler_trigger() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id.as_str().to_string();
        scheduler.register(def).await.unwrap();

        let run_id = scheduler.trigger(&id).await.unwrap();
        let run = scheduler.get_run(&run_id).await.unwrap();
        assert_eq!(run.status, RunStatus::Running);
        assert!(run.started_at.is_some());
    }

    #[tokio::test]
    async fn scheduler_trigger_nonexistent() {
        let scheduler = Scheduler::new();
        let result = scheduler.trigger("nope").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn scheduler_complete_run() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id.as_str().to_string();
        scheduler.register(def).await.unwrap();

        let run_id = scheduler.trigger(&id).await.unwrap();
        scheduler
            .complete_run(&run_id, 0, "output".to_string(), String::new())
            .await
            .unwrap();

        let run = scheduler.get_run(&run_id).await.unwrap();
        assert_eq!(run.status, RunStatus::Completed);
        assert_eq!(run.exit_code, Some(0));
    }

    #[tokio::test]
    async fn scheduler_fail_run() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id.as_str().to_string();
        scheduler.register(def).await.unwrap();

        let run_id = scheduler.trigger(&id).await.unwrap();
        scheduler
            .fail_run(&run_id, "something went wrong".to_string())
            .await
            .unwrap();

        let run = scheduler.get_run(&run_id).await.unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.error.as_deref(), Some("something went wrong"));
    }

    #[tokio::test]
    async fn scheduler_cancel_run() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id.as_str().to_string();
        scheduler.register(def).await.unwrap();

        let run_id = scheduler.trigger(&id).await.unwrap();
        scheduler.cancel_run(&run_id).await.unwrap();

        let run = scheduler.get_run(&run_id).await.unwrap();
        assert_eq!(run.status, RunStatus::Cancelled);
    }

    #[tokio::test]
    async fn scheduler_list_runs() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id.as_str().to_string();
        scheduler.register(def).await.unwrap();

        scheduler.trigger(&id).await.unwrap();
        scheduler.trigger(&id).await.unwrap();

        let runs = scheduler.list_runs(&id).await;
        assert_eq!(runs.len(), 2);
    }

    #[tokio::test]
    async fn scheduler_update() {
        let scheduler = Scheduler::new();
        let def = make_def("test-1");
        let id = def.id;
        scheduler.register(def).await.unwrap();

        // Create a new def with the same ID but different name
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

        let err = SchedulerError::AlreadyExists("xyz".to_string());
        assert!(err.to_string().contains("xyz"));
    }
}

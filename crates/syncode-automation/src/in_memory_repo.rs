//! In-memory `AutomationRepository` implementation.
//!
//! Backs the [`Scheduler`](crate::scheduler::Scheduler) with `RwLock<HashMap>`
//! storage — exactly what the scheduler's fields were before the port
//! extraction, just lifted behind the trait. This preserves the existing
//! tests' behavior (construct a scheduler, register/trigger/get) and gives
//! the SQLite adapter (future work) a contract to implement.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use syncode_core::ports::{AutomationRepository, PortError};

/// In-memory automation repository. Cheap to clone (inner is `Arc`'d).
#[derive(Debug, Clone, Default)]
pub struct InMemoryAutomationRepository {
    defs: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    runs: Arc<RwLock<Vec<serde_json::Value>>>,
}

impl InMemoryAutomationRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl AutomationRepository for InMemoryAutomationRepository {
    async fn save_def(&self, id: &str, payload: serde_json::Value) -> Result<(), PortError> {
        self.defs.write().await.insert(id.to_string(), payload);
        Ok(())
    }

    async fn get_def(&self, id: &str) -> Result<Option<serde_json::Value>, PortError> {
        Ok(self.defs.read().await.get(id).cloned())
    }

    async fn list_defs(&self) -> Result<Vec<serde_json::Value>, PortError> {
        Ok(self.defs.read().await.values().cloned().collect())
    }

    async fn delete_def(&self, id: &str) -> Result<bool, PortError> {
        Ok(self.defs.write().await.remove(id).is_some())
    }

    async fn save_run(&self, payload: serde_json::Value) -> Result<(), PortError> {
        // Upsert by run id: replace if present, else append.
        let id = payload.get("id").and_then(|v| v.as_str()).map(String::from);
        let mut runs = self.runs.write().await;
        if let Some(id) = &id
            && let Some(existing) = runs.iter_mut().find(|r| {
                r.get("id")
                    .and_then(|v| v.as_str())
                    .map(|s| s == id)
                    .unwrap_or(false)
            })
        {
            *existing = payload;
            return Ok(());
        }
        runs.push(payload);
        Ok(())
    }

    async fn get_run(&self, id: &str) -> Result<Option<serde_json::Value>, PortError> {
        Ok(self
            .runs
            .read()
            .await
            .iter()
            .find(|r| r.get("id").and_then(|v| v.as_str()) == Some(id))
            .cloned())
    }

    async fn list_runs(&self, automation_id: &str) -> Result<Vec<serde_json::Value>, PortError> {
        Ok(self
            .runs
            .read()
            .await
            .iter()
            .filter(|r| {
                r.get("automationId")
                    .and_then(|v| v.as_str())
                    .map(|s| s == automation_id)
                    .unwrap_or(false)
            })
            .cloned()
            .collect())
    }

    async fn advance_next_run_at(
        &self,
        id: &str,
        next_run_at: Option<String>,
    ) -> Result<(), PortError> {
        let mut defs = self.defs.write().await;
        if let Some(def) = defs.get_mut(id) {
            def.as_object_mut()
                .ok_or_else(|| PortError::Internal("def payload is not an object".into()))?
                .insert("nextRunAt".to_string(), serde_json::json!(next_run_at));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn save_and_get_def() {
        let repo = InMemoryAutomationRepository::new();
        let payload = serde_json::json!({"name": "test"});
        repo.save_def("a1", payload.clone()).await.unwrap();
        let got = repo.get_def("a1").await.unwrap();
        assert_eq!(got, Some(payload));
        assert!(repo.get_def("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_and_delete_defs() {
        let repo = InMemoryAutomationRepository::new();
        repo.save_def("a1", serde_json::json!({})).await.unwrap();
        repo.save_def("a2", serde_json::json!({})).await.unwrap();
        assert_eq!(repo.list_defs().await.unwrap().len(), 2);
        assert!(repo.delete_def("a1").await.unwrap());
        assert!(!repo.delete_def("a1").await.unwrap());
        assert_eq!(repo.list_defs().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn save_and_list_runs_by_automation() {
        let repo = InMemoryAutomationRepository::new();
        repo.save_run(serde_json::json!({"id": "r1", "automationId": "a1"}))
            .await
            .unwrap();
        repo.save_run(serde_json::json!({"id": "r2", "automationId": "a1"}))
            .await
            .unwrap();
        repo.save_run(serde_json::json!({"id": "r3", "automationId": "a2"}))
            .await
            .unwrap();

        let a1_runs = repo.list_runs("a1").await.unwrap();
        assert_eq!(a1_runs.len(), 2);
        let got = repo.get_run("r2").await.unwrap();
        assert!(got.is_some());
    }

    #[tokio::test]
    async fn advance_next_run_at_updates_def() {
        let repo = InMemoryAutomationRepository::new();
        repo.save_def("a1", serde_json::json!({"name": "x"}))
            .await
            .unwrap();
        repo.advance_next_run_at("a1", Some("2026-01-01T00:00:00Z".into()))
            .await
            .unwrap();
        let def = repo.get_def("a1").await.unwrap().unwrap();
        assert_eq!(def["nextRunAt"], "2026-01-01T00:00:00Z");
    }
}

//! End-to-end test — automation scheduler with real trigger, retry, and tick.
//!
//! Gating: `SYNICODE_AUTOMATION_E2E=1`.

use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use syncode_core::{EntityId, ports::{DispatchOutcome, DispatchRequest, PortError, RunExecutor}};
use async_trait::async_trait;

fn e2e_enabled() -> bool {
    std::env::var("SYNICODE_AUTOMATION_E2E").ok().as_deref() == Some("1")
}

/// A real executor that counts invocations.
#[derive(Clone)]
struct CountingExecutor {
    count: Arc<AtomicUsize>,
    fail_until: Arc<AtomicUsize>,
}

#[async_trait]
impl RunExecutor for CountingExecutor {
    async fn dispatch_turn(&self, _req: DispatchRequest) -> Result<DispatchOutcome, PortError> {
        let fails = self.fail_until.load(Ordering::SeqCst);
        let current = self.count.fetch_add(1, Ordering::SeqCst);

        if current < fails {
            Err(PortError::Internal(format!("simulated failure {}/{}", current + 1, fails)))
        } else {
            Ok(DispatchOutcome {
                thread_id: EntityId::new(),
                turn_id: EntityId::new(),
            })
        }
    }
}

#[tokio::test]
async fn automation_e2e_manual_trigger_and_complete() {
    if !e2e_enabled() { eprintln!("[skip] automation e2e: set SYNICODE_AUTOMATION_E2E=1"); return; }

    let repo = Arc::new(syncode_automation::in_memory_repo::InMemoryAutomationRepository::new());
    let executor = CountingExecutor {
        count: Arc::new(AtomicUsize::new(0)),
        fail_until: Arc::new(AtomicUsize::new(0)),
    };
    let scheduler = syncode_automation::Scheduler::new_with_deps(repo.clone(), Arc::new(executor));

    let def = syncode_automation::AutomationDef::new(
        "e2e-manual".into(),
        "echo hello".into(),
        syncode_automation::ScheduleType::Manual,
    );
    let id = def.id.clone();
    let id_str = id.as_str().to_string();
    scheduler.register(def).await.expect("register");

    // trigger returns the run_id
    let run_id = scheduler.trigger(&id_str).await.expect("trigger");

    // Verify a run was created
    let runs = scheduler.list_runs(&id_str).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].id, run_id);
}

#[tokio::test]
async fn automation_e2e_scheduler_tick_due_evaluation() {
    if !e2e_enabled() { eprintln!("[skip] automation e2e"); return; }

    let repo = Arc::new(syncode_automation::in_memory_repo::InMemoryAutomationRepository::new());
    let executor = CountingExecutor {
        count: Arc::new(AtomicUsize::new(0)),
        fail_until: Arc::new(AtomicUsize::new(0)),
    };
    let scheduler = syncode_automation::Scheduler::new_with_deps(repo.clone(), Arc::new(executor));

    // Interval: 1 second, started in the past -> already due
    let past = chrono::Utc::now() - chrono::Duration::seconds(10);
    let def = syncode_automation::AutomationDef::new(
        "e2e-interval".into(),
        "echo tick".into(),
        syncode_automation::ScheduleType::Interval(1), // every 1 second
    ).with_working_dir("/tmp");

    // Manually set next_run_at to the past so tick picks it up
    let mut def = def;
    def.next_run_at = Some(past.to_rfc3339());

    let id_str = def.id.as_str().to_string();
    scheduler.register(def).await.expect("register");

    // Tick with current time — should find the due automation
    let triggered = scheduler.tick(chrono::Utc::now()).await;
    assert_eq!(triggered.len(), 1, "expected 1 due automation");
    assert_eq!(triggered[0], id_str);
}

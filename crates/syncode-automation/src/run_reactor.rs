//! AutomationRunReactor — event-driven run-status reconciliation.
//!
//! Mirrors MCode's `automation/Services/AutomationRunReactor.ts`. The reactor
//! subscribes to the orchestration domain-event stream, filters to the small
//! set of lifecycle events that bear on an automation run's status, and
//! reconciles the corresponding run record via the [`Scheduler`].
//!
//! ## Lifecycle events → run status (MCode parity)
//!
//! | Domain event                 | Run reconciliation                          |
//! |------------------------------|---------------------------------------------|
//! | `TurnDiffCompleted`          | `Completed` (run succeeded)                 |
//! | `TurnFailed`                 | `Failed`                                    |
//! | `TurnInterrupted`            | `Interrupted`                               |
//! | `ThreadApprovalResponded`    | resume `WaitingForApproval` → `Running`     |
//! | `ThreadUserInputResponded`   | resume `WaitingForApproval` → `Running`     |
//! | `ThreadSessionSet`           | `WaitingForApproval` when status indicates  |
//! |                              | a blocked turn                              |
//!
//! ## Per-thread dedupe (MCode `makeDrainableWorker`)
//!
//! Concurrent events for a single thread are coalesced: while a reconcile is
//! in flight for thread `T`, additional events for `T` are absorbed into a
//! pending flag and reconciled once more after the in-flight call completes —
//! never concurrently. Events for *different* threads reconcile in parallel.
//! Within a coalesced burst, the **last** event wins (recorded in an intent
//! table so the final reconcile reflects the newest signal).
//!
//! This module is a pure addition. The existing poll-based [`Scheduler::tick`]
//! path and the manual `complete_run` / `fail_run` / `cancel_run` methods are
//! untouched — the reactor is an opt-in event-driven reconciliation layer that
//! a host wires up alongside the scheduler.

use std::collections::HashMap;
use std::sync::Arc;

use syncode_core::domain::events::DomainEvent;
use syncode_core::EntityId;
use tokio::sync::{Mutex, broadcast};

use crate::runner::RunStatus;
use crate::scheduler::Scheduler;

// ─── Domain event stream port ─────────────────────────────────────────────

/// A live, append-only stream of [`DomainEvent`]s.
///
/// This is the inbound seam the reactor subscribes to. In production it is
/// backed by the WebSocket push bus ([`BroadcastDomainEventStream`] wraps a
/// `tokio::sync::broadcast::Receiver` carrying the typed event). The port is
/// async-trait so a test can substitute an in-memory channel without touching
/// the WS layer.
#[async_trait::async_trait]
pub trait DomainEventStream: Send + Sync {
    /// Await the next domain event. Returns `None` when the stream is closed
    /// (sender dropped / bus shut down) — the reactor treats that as a stop
    /// signal and exits its event loop.
    async fn next_event(&self) -> Option<DomainEvent>;
}

/// A [`DomainEventStream`] backed by a `tokio::sync::broadcast` channel.
///
/// Production wiring: the WS push bus broadcasts every published domain event;
/// the server forwards orchestration-channel envelopes into a typed
/// `broadcast::Sender<DomainEvent>`. Keeping the broadcast item as the typed
/// `DomainEvent` (not the wire JSON) lets the reactor match on variants
/// directly, with no per-event deserialization.
#[derive(Clone)]
pub struct BroadcastDomainEventStream {
    rx: Arc<Mutex<broadcast::Receiver<DomainEvent>>>,
}

impl BroadcastDomainEventStream {
    /// Wrap a broadcast receiver carrying domain events.
    pub fn new(rx: broadcast::Receiver<DomainEvent>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

#[async_trait::async_trait]
impl DomainEventStream for BroadcastDomainEventStream {
    async fn next_event(&self) -> Option<DomainEvent> {
        self.rx.lock().await.recv().await.ok()
    }
}

// ─── Lifecycle event classification ───────────────────────────────────────

/// The subset of domain events the reactor reconciles against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEvent {
    /// A turn's diff was committed — the run succeeded.
    TurnDiffCompleted { thread_id: EntityId },
    /// A turn failed — the run failed.
    TurnFailed { thread_id: EntityId },
    /// A turn was interrupted — the run is interrupted.
    TurnInterrupted { thread_id: EntityId },
    /// A pending approval was responded to — resume the run if it was blocked.
    ApprovalResponded { thread_id: EntityId },
    /// The session entered a state that may require approval — mark waiting.
    SessionSet { thread_id: EntityId, status: String },
}

impl LifecycleEvent {
    /// The thread this lifecycle event pertains to (the dedupe key).
    pub fn thread_id(&self) -> EntityId {
        match self {
            Self::TurnDiffCompleted { thread_id }
            | Self::TurnFailed { thread_id }
            | Self::TurnInterrupted { thread_id }
            | Self::ApprovalResponded { thread_id }
            | Self::SessionSet { thread_id, .. } => *thread_id,
        }
    }

    /// The target run status this lifecycle event implies.
    pub fn target_status(&self) -> RunStatus {
        match self {
            Self::TurnDiffCompleted { .. } => RunStatus::Completed,
            Self::TurnFailed { .. } => RunStatus::Failed,
            Self::TurnInterrupted { .. } => RunStatus::Interrupted,
            Self::ApprovalResponded { .. } => RunStatus::Running,
            Self::SessionSet { status, .. } => {
                if is_approval_blocked_status(status) {
                    RunStatus::WaitingForApproval
                } else {
                    RunStatus::Running
                }
            }
        }
    }

    /// Classify a domain event into a lifecycle event, or `None` if it is not
    /// one the reactor reconciles against. This is the single filter.
    pub fn from_domain(ev: &DomainEvent) -> Option<Self> {
        match ev {
            DomainEvent::TurnFailed { id, .. } => Some(Self::TurnFailed { thread_id: *id }),
            DomainEvent::TurnInterrupted { id, .. } => {
                Some(Self::TurnInterrupted { thread_id: *id })
            }
            DomainEvent::TurnDiffCompleted { thread_id, .. } => {
                Some(Self::TurnDiffCompleted { thread_id: *thread_id })
            }
            DomainEvent::ThreadApprovalResponded { id, .. } => {
                Some(Self::ApprovalResponded { thread_id: *id })
            }
            DomainEvent::ThreadUserInputResponded { id, .. } => Some(Self::ApprovalResponded {
                thread_id: *id,
            }),
            DomainEvent::ThreadSessionSet { id, status, .. } => Some(Self::SessionSet {
                thread_id: *id,
                status: status.clone(),
            }),
            _ => None,
        }
    }
}

/// Statuses (from `ThreadSessionSet.status`) that indicate the turn is blocked
/// waiting for a human response. MCode inspects the session-status field;
/// syncode mirrors the vocabulary.
fn is_approval_blocked_status(status: &str) -> bool {
    matches!(
        status,
        "approval-required"
            | "user-input-required"
            | "awaiting-approval"
            | "waiting-for-approval"
    )
}

// ─── Reactor ──────────────────────────────────────────────────────────────

/// Event-driven reconciler of automation run status from orchestration
/// domain events. Mirrors MCode's `AutomationRunReactor`.
///
/// Construct with [`AutomationRunReactor::new`], then spawn
/// [`AutomationRunReactor::run`] on a Tokio task. The reactor keeps running
/// until the event stream closes (`next_event` returns `None`).
///
/// **Per-thread dedupe**: concurrent events for one thread coalesce — while a
/// reconcile is in flight for thread `T`, additional events for `T` set a
/// pending flag and trigger exactly one more reconcile after the in-flight
/// call finishes. Events for distinct threads reconcile in parallel. Within a
/// coalesced burst, the **last** event wins (its target status is what the
/// final reconcile applies).
pub struct AutomationRunReactor {
    stream: Arc<dyn DomainEventStream>,
    scheduler: Arc<Scheduler>,
    /// Per-thread coalesce state. Entry present ⇨ a reconcile loop is running
    /// for that thread. Value `true` ⇨ a follow-up reconcile is pending.
    inflight: Mutex<HashMap<EntityId, bool>>,
    /// Per-thread latest intent: the target status implied by the most recent
    /// lifecycle event for the thread. The drain loop reads (and clears) this
    /// when it runs, so a coalesced burst reconciles to the newest signal.
    intent: Mutex<HashMap<EntityId, RunStatus>>,
}

impl AutomationRunReactor {
    /// Construct a new reactor reading from `stream` and reconciling via
    /// `scheduler`.
    pub fn new(stream: Arc<dyn DomainEventStream>, scheduler: Arc<Scheduler>) -> Self {
        Self {
            stream,
            scheduler,
            inflight: Mutex::new(HashMap::new()),
            intent: Mutex::new(HashMap::new()),
        }
    }

    /// The main event loop. Reads domain events from the stream, classifies
    /// them, records the latest intent per thread, and dispatches per-thread
    /// reconcile loops with coalescing.
    ///
    /// Returns when the stream closes. Cancellation is via dropping the
    /// task/future (the next `next_event` await is the cancel point).
    pub async fn run(self: Arc<Self>) {
        tracing::info!("automation run reactor started");
        while let Some(ev) = self.stream.next_event().await {
            let Some(le) = LifecycleEvent::from_domain(&ev) else {
                continue;
            };
            let thread_id = le.thread_id();
            // Record latest intent for the thread (last-writer-wins within a burst).
            self.record_intent(thread_id, le.target_status()).await;

            // Coalesce: if a loop is already running for this thread, it will
            // pick up the new intent on its next drain iteration. Otherwise
            // spawn one.
            if self.note_pending(thread_id).await {
                let this = self.clone();
                tokio::spawn(this.reconcile_drain_loop(thread_id));
            }
        }
        tracing::info!("automation run reactor stream closed; exiting");
    }

    /// Record the latest target status implied for `thread_id`. Last writer
    /// wins within a coalesced burst — the final reconcile honors this.
    async fn record_intent(&self, thread_id: EntityId, target: RunStatus) {
        self.intent.lock().await.insert(thread_id, target);
    }

    /// Mark `thread_id` as having an unreconciled event. Returns `true` if the
    /// caller should spawn a drain loop (none is running for this thread),
    /// `false` if an existing loop will absorb the event.
    async fn note_pending(&self, thread_id: EntityId) -> bool {
        let mut inflight = self.inflight.lock().await;
        if let Some(has_pending) = inflight.get_mut(&thread_id) {
            *has_pending = true;
            false
        } else {
            inflight.insert(thread_id, false);
            true
        }
    }

    /// Drain loop for a single thread: reconcile once, then re-reconcile as
    /// long as new events arrived during the previous reconcile (coalescing a
    /// burst into a bounded number of calls). Exits when no more events are
    /// pending for the thread.
    async fn reconcile_drain_loop(self: Arc<Self>, thread_id: EntityId) {
        loop {
            // Reconcile toward the latest recorded intent for this thread.
            let target = self.intent.lock().await.remove(&thread_id);
            if let Some(target) = target {
                self.reconcile_thread(thread_id, target).await;
            }

            // Did more events arrive while we were reconciling?
            let still_pending = {
                let mut inflight = self.inflight.lock().await;
                match inflight.get_mut(&thread_id) {
                    Some(has_pending) if *has_pending => {
                        *has_pending = false; // consumed by this re-reconcile
                        true
                    }
                    _ => {
                        // Fully drained — remove the entry so a future event
                        // for this thread spawns a fresh loop.
                        inflight.remove(&thread_id);
                        false
                    }
                }
            };
            if !still_pending {
                break;
            }
        }
    }

    /// Reconcile the active run (if any) for `thread_id` toward `target`.
    /// No-op when there is no automation run bound to this thread (the common
    /// case — only heartbeat-mode automations with a `target_thread_id` are
    /// reconciled) or when the run already has the target status.
    async fn reconcile_thread(&self, thread_id: EntityId, target: RunStatus) {
        let thread_id_str = thread_id.to_string();
        let Some(mut run) = self.scheduler.find_active_run_for_thread(&thread_id_str).await
        else {
            return; // No automation run for this thread.
        };
        if run.status == target {
            return; // Already reconciled.
        }
        tracing::info!(
            thread_id = %thread_id_str,
            run_id = %run.id,
            from = %run.status,
            to = %target,
            "run reactor reconciling run status"
        );
        self.apply_target(&mut run, target).await;
    }

    /// Apply a target status to a run via the scheduler.
    async fn apply_target(&self, run: &mut crate::runner::AutomationRun, target: RunStatus) {
        let run_id = run.id.clone();
        match target {
            RunStatus::Completed => {
                let _ = self
                    .scheduler
                    .complete_run(&run_id, 0, String::new(), String::new())
                    .await;
            }
            RunStatus::Failed => {
                let _ = self.scheduler.fail_run(&run_id, "turn failed".to_string()).await;
            }
            RunStatus::Interrupted => {
                let _ = self.scheduler.interrupt_run(&run_id).await;
            }
            RunStatus::WaitingForApproval => {
                let _ = self.scheduler.set_run_waiting_for_approval(&run_id).await;
            }
            RunStatus::Running => {
                let _ = self.scheduler.resume_run_from_approval(&run_id).await;
            }
            // Pending / Retrying / TimedOut / Cancelled are not driven by
            // domain events in the initial cut.
            _ => {}
        }
        run.status = target;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use syncode_core::domain::primitives::Timestamp;
    use syncode_core::ports::{AutomationRepository, RunExecutor};
    use syncode_core::PortError;

    use crate::definition::{AutomationDef, ScheduleType};
    use crate::in_memory_repo::InMemoryAutomationRepository;
    use crate::runner::AutomationRun;

    // ── Test doubles ───────────────────────────────────────────────────────

    /// A [`DomainEventStream`] backed by a `tokio::sync::mpsc` receiver —
    /// tests push events into the sender and observe the reactor reconcile.
    struct MpscDomainEventStream {
        rx: Arc<Mutex<tokio::sync::mpsc::Receiver<DomainEvent>>>,
    }

    impl MpscDomainEventStream {
        fn new() -> (tokio::sync::mpsc::Sender<DomainEvent>, Arc<Self>) {
            let (tx, rx) = tokio::sync::mpsc::channel::<DomainEvent>(32);
            let stream = Arc::new(Self {
                rx: Arc::new(Mutex::new(rx)),
            });
            (tx, stream)
        }
    }

    #[async_trait::async_trait]
    impl DomainEventStream for MpscDomainEventStream {
        async fn next_event(&self) -> Option<DomainEvent> {
            self.rx.lock().await.recv().await
        }
    }

    /// A no-op executor (so runs stay non-terminal until the reactor
    /// reconciles them). Mirrors the scheduler tests' `NoopExecutor`.
    struct NoopExecutor;
    #[async_trait::async_trait]
    impl RunExecutor for NoopExecutor {
        async fn dispatch_turn(
            &self,
            _req: syncode_core::ports::DispatchRequest,
        ) -> Result<syncode_core::ports::DispatchOutcome, PortError> {
            Err(PortError::Internal("noop".into()))
        }
    }

    /// Build a scheduler + an automation that targets `thread_id` (heartbeat
    /// mode), with one active (Running) run persisted for it.
    async fn harness(thread_id: &str) -> (Arc<Scheduler>, String, String) {
        let repo: Arc<dyn AutomationRepository> = Arc::new(InMemoryAutomationRepository::new());
        let executor: Arc<dyn RunExecutor> = Arc::new(NoopExecutor);
        let scheduler = Arc::new(Scheduler::new_with_deps(repo, executor));

        let mut def = AutomationDef::new(
            "heartbeat-auto".to_string(),
            "echo hi".to_string(),
            ScheduleType::Manual,
        );
        def.target_thread_id = Some(thread_id.to_string());
        let auto_id = def.id.as_str().to_string();
        scheduler.register(def).await.unwrap();

        // Persist a Running run for this automation.
        let mut run = AutomationRun::new(auto_id.clone());
        run.mark_started();
        let run_id = run.id.clone();
        scheduler
            .repo
            .save_run(serde_json::to_value(&run).unwrap())
            .await
            .unwrap();

        (scheduler, auto_id, run_id)
    }

    fn mk_thread() -> EntityId {
        EntityId::new()
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    /// Test 1 — the filter: only lifecycle events are reconciled, and they map
    /// to the correct target status.
    #[test]
    fn lifecycle_filter_maps_events_to_target_statuses() {
        let thread = mk_thread();
        // TurnDiffCompleted → Completed
        let ev = DomainEvent::TurnDiffCompleted {
            thread_id: thread,
            turn_id: EntityId::new(),
            checkpoint_turn_count: 1,
            checkpoint_ref: "ref".into(),
            status: "ok".into(),
            files: vec![],
            assistant_message_id: None,
            completed_at: Timestamp::now(),
        };
        let le = LifecycleEvent::from_domain(&ev).unwrap();
        assert_eq!(le, LifecycleEvent::TurnDiffCompleted { thread_id: thread });
        assert_eq!(le.target_status(), RunStatus::Completed);

        // TurnFailed → Failed
        let le = LifecycleEvent::from_domain(&DomainEvent::TurnFailed {
            id: thread,
            error: "boom".into(),
            completed_at: Timestamp::now(),
        })
        .unwrap();
        assert_eq!(le.target_status(), RunStatus::Failed);

        // TurnInterrupted → Interrupted
        let le = LifecycleEvent::from_domain(&DomainEvent::TurnInterrupted {
            id: thread,
            interrupted_at: Timestamp::now(),
        })
        .unwrap();
        assert_eq!(le.target_status(), RunStatus::Interrupted);

        // ThreadApprovalResponded → Running (resume)
        let le = LifecycleEvent::from_domain(&DomainEvent::ThreadApprovalResponded {
            id: thread,
            request_id: "req".into(),
            decision: "approve".into(),
            responded_at: Timestamp::now(),
        })
        .unwrap();
        assert_eq!(le.target_status(), RunStatus::Running);

        // ThreadSessionSet with approval-required status → WaitingForApproval
        let le = LifecycleEvent::from_domain(&DomainEvent::ThreadSessionSet {
            id: thread,
            status: "approval-required".into(),
            provider_name: None,
            runtime_mode: "standard".into(),
            active_turn_id: None,
            last_error: None,
            updated_at: Timestamp::now(),
        })
        .unwrap();
        assert_eq!(le.target_status(), RunStatus::WaitingForApproval);

        // ThreadSessionSet with a non-blocked status → Running (no-op intent)
        let le = LifecycleEvent::from_domain(&DomainEvent::ThreadSessionSet {
            id: thread,
            status: "running".into(),
            provider_name: None,
            runtime_mode: "standard".into(),
            active_turn_id: None,
            last_error: None,
            updated_at: Timestamp::now(),
        })
        .unwrap();
        assert_eq!(le.target_status(), RunStatus::Running);

        // A non-lifecycle event (ProjectCreated) is filtered out.
        let non = DomainEvent::ProjectCreated {
            id: EntityId::new(),
            name: "p".into(),
            root_path: "/p".into(),
            created_at: Timestamp::now(),
        };
        assert!(LifecycleEvent::from_domain(&non).is_none());
    }

    /// Test 2 — end-to-end reconcile: a TurnDiffCompleted event transitions
    /// the run to Completed.
    #[tokio::test]
    async fn reactor_reconciles_diff_completed() {
        let thread = mk_thread();
        let (scheduler, _auto, run_id) = harness(&thread.to_string()).await;

        let (tx, stream) = MpscDomainEventStream::new();
        let reactor = Arc::new(AutomationRunReactor::new(stream, scheduler.clone()));
        let reactor_task = tokio::spawn(reactor.clone().run());

        tx.send(DomainEvent::TurnDiffCompleted {
            thread_id: thread,
            turn_id: EntityId::new(),
            checkpoint_turn_count: 1,
            checkpoint_ref: "ref".into(),
            status: "ok".into(),
            files: vec![],
            assistant_message_id: None,
            completed_at: Timestamp::now(),
        })
        .await
        .unwrap();

        // Give the reactor a moment to reconcile.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            if scheduler.get_run(&run_id).await.unwrap().status.is_terminal() {
                break;
            }
        }
        assert_eq!(
            scheduler.get_run(&run_id).await.unwrap().status,
            RunStatus::Completed
        );

        // Drop the sender → stream closes → reactor task exits.
        drop(tx);
        let _ = reactor_task.await;
    }

    /// Test 3 — per-thread dedupe: a burst of events for one thread coalesces
    /// so the run reflects the *last* event (Failed), while a concurrent event
    /// for a *different* thread reconciles independently (Interrupted).
    /// Asserts the dedupe invariant: inflight map fully drains.
    #[tokio::test]
    async fn reactor_coalesces_burst_and_last_event_wins() {
        let thread_a = mk_thread();
        let thread_b = mk_thread();

        let repo: Arc<dyn AutomationRepository> = Arc::new(InMemoryAutomationRepository::new());
        let executor: Arc<dyn RunExecutor> = Arc::new(NoopExecutor);
        let scheduler = Arc::new(Scheduler::new_with_deps(repo, executor));

        let mut auto_ids = Vec::new();
        let mut run_ids = Vec::new();
        for tid in [thread_a, thread_b] {
            let mut def = AutomationDef::new(
                format!("auto-{tid}"),
                "echo".into(),
                ScheduleType::Manual,
            );
            def.target_thread_id = Some(tid.to_string());
            let auto_id = def.id.as_str().to_string();
            auto_ids.push(auto_id.clone());
            scheduler.register(def).await.unwrap();
            let mut run = AutomationRun::new(auto_id);
            run.mark_started();
            run_ids.push(run.id.clone());
            scheduler
                .repo
                .save_run(serde_json::to_value(&run).unwrap())
                .await
                .unwrap();
        }

        let (tx, stream) = MpscDomainEventStream::new();
        let reactor = Arc::new(AutomationRunReactor::new(stream, scheduler.clone()));
        let reactor_task = tokio::spawn(reactor.clone().run());

        // Burst for thread A: Completed then Failed. Last wins → Failed.
        tx.send(DomainEvent::TurnDiffCompleted {
            thread_id: thread_a,
            turn_id: EntityId::new(),
            checkpoint_turn_count: 1,
            checkpoint_ref: "r".into(),
            status: "ok".into(),
            files: vec![],
            assistant_message_id: None,
            completed_at: Timestamp::now(),
        })
        .await
        .unwrap();
        tx.send(DomainEvent::TurnFailed {
            id: thread_a,
            error: "late failure".into(),
            completed_at: Timestamp::now(),
        })
        .await
        .unwrap();
        // Concurrent event for thread B → Interrupted.
        tx.send(DomainEvent::TurnInterrupted {
            id: thread_b,
            interrupted_at: Timestamp::now(),
        })
        .await
        .unwrap();

        // Wait for both runs to reach a terminal status.
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let a_done = scheduler
                .get_run(&run_ids[0])
                .await
                .is_some_and(|r| r.status.is_terminal());
            let b_done = scheduler
                .get_run(&run_ids[1])
                .await
                .is_some_and(|r| r.status.is_terminal());
            if a_done && b_done {
                break;
            }
        }

        // Thread A's run reflects the LAST event (Failed), not the first.
        assert_eq!(
            scheduler.get_run(&run_ids[0]).await.unwrap().status,
            RunStatus::Failed,
            "thread A: last event (Failed) should win after coalesce"
        );
        // Thread B's run reconciled independently to Interrupted.
        assert_eq!(
            scheduler.get_run(&run_ids[1]).await.unwrap().status,
            RunStatus::Interrupted,
            "thread B: independent reconcile to Interrupted"
        );

        // Dedupe invariant: inflight map is fully drained (no lingering entries).
        assert!(
            reactor.inflight.lock().await.is_empty(),
            "inflight map must drain fully after reconciliation"
        );

        drop(tx);
        let _ = reactor_task.await;
    }

    /// Test 4 — events for a thread with no automation run are ignored
    /// (no spurious run records created, no panics).
    #[tokio::test]
    async fn reactor_ignores_events_for_thread_without_run() {
        let repo: Arc<dyn AutomationRepository> = Arc::new(InMemoryAutomationRepository::new());
        let executor: Arc<dyn RunExecutor> = Arc::new(NoopExecutor);
        let scheduler = Arc::new(Scheduler::new_with_deps(repo.clone(), executor));

        let (tx, stream) = MpscDomainEventStream::new();
        let reactor = Arc::new(AutomationRunReactor::new(stream, scheduler.clone()));
        let reactor_task = tokio::spawn(reactor.clone().run());

        let orphan_thread = mk_thread();
        tx.send(DomainEvent::TurnFailed {
            id: orphan_thread,
            error: "nobody home".into(),
            completed_at: Timestamp::now(),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(150)).await;
        // No run records were created.
        assert_eq!(scheduler.run_count().await, 0);
        // Inflight drained.
        assert!(reactor.inflight.lock().await.is_empty());

        drop(tx);
        let _ = reactor_task.await;
    }

    /// Test 5 — the broadcast adapter round-trips events from a broadcast sender.
    #[tokio::test]
    async fn broadcast_stream_delivers_events() {
        let (btx, _) = broadcast::channel::<DomainEvent>(8);
        let stream = BroadcastDomainEventStream::new(btx.subscribe());
        btx.send(DomainEvent::TurnFailed {
            id: EntityId::new(),
            error: "x".into(),
            completed_at: Timestamp::now(),
        })
        .unwrap();
        let ev = stream.next_event().await;
        assert!(matches!(ev, Some(DomainEvent::TurnFailed { .. })));
    }

    /// Test 6 — approval-requested then approval-responded: the run goes to
    /// WaitingForApproval then resumes to Running.
    #[tokio::test]
    async fn reactor_waiting_for_approval_then_resumes() {
        let thread = mk_thread();
        let (scheduler, _auto, run_id) = harness(&thread.to_string()).await;

        let (tx, stream) = MpscDomainEventStream::new();
        let reactor = Arc::new(AutomationRunReactor::new(stream, scheduler.clone()));
        let reactor_task = tokio::spawn(reactor.clone().run());

        // SessionSet with approval-required → WaitingForApproval.
        tx.send(DomainEvent::ThreadSessionSet {
            id: thread,
            status: "approval-required".into(),
            provider_name: None,
            runtime_mode: "standard".into(),
            active_turn_id: None,
            last_error: None,
            updated_at: Timestamp::now(),
        })
        .await
        .unwrap();
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            if scheduler.get_run(&run_id).await.unwrap().status
                == RunStatus::WaitingForApproval
            {
                break;
            }
        }
        assert_eq!(
            scheduler.get_run(&run_id).await.unwrap().status,
            RunStatus::WaitingForApproval
        );

        // ApprovalResponded → resume to Running.
        tx.send(DomainEvent::ThreadApprovalResponded {
            id: thread,
            request_id: "req".into(),
            decision: "approve".into(),
            responded_at: Timestamp::now(),
        })
        .await
        .unwrap();
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            if scheduler.get_run(&run_id).await.unwrap().status == RunStatus::Running {
                break;
            }
        }
        assert_eq!(
            scheduler.get_run(&run_id).await.unwrap().status,
            RunStatus::Running
        );

        drop(tx);
        let _ = reactor_task.await;
    }
}

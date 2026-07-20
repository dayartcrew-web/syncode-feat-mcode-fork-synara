//! Thread ↔ workflow link bridge.
//!
//! C1 of the chat-workflow bridge: ensure every chat thread has an associated
//! workflow_id stored in the `thread_workflow_links` sidecar table. The link
//! is consulted by C2 (turn binding) to decide whether a new turn starts a
//! fresh workflow task or advances an existing one, and by A2's preamble
//! injection (via `workflow_preamble`) to surface the active workflow state
//! to the chat AI.
//!
//! C3 of the chat-workflow bridge: surface workflow state to the frontend
//! via the push bus. Each turn emits a `workflow.contextBound` push event
//! on [`CHANNEL_ORCHESTRATION`](crate::channels::CHANNEL_ORCHESTRATION)
//! carrying the workflow_id, phase, and current task — so the thread UI can
//! render a workflow badge without polling.
//!
//! ## Why a sidecar table
//!
//! Threads are event-sourced aggregates in syncode-orchestration — there is
//! no SQL row to attach the workflow_id to. The sidecar provides O(1)
//! lookup without touching the aggregate's event stream.
//!
//! ## Pragmatic scope (C1)
//!
//! - When a thread starts its first turn, [`ensure_link_for_thread`] checks
//!   the sidecar; if missing, it generates a fresh UUID workflow_id and
//!   upserts it.
//! - Subsequent turns reuse the existing link (no churn).
//! - When no SQLite pool is attached (in-memory mode), the function is a
//!   no-op and returns `None` — backward compatible with prior behavior.
//! - Errors are logged and swallowed: a failed sidecar write must never
//!   block the turn itself.

use syncode_orchestration::workflow_state::WorkflowStateProvider;
use syncode_persistence::SqlitePool;
use uuid::Uuid;

/// Structured snapshot of the workflow state bound to a chat thread at
/// turn time. Serialized as the `data` field of the
/// `workflow.contextBound` push event.
///
/// All v1 turns report `phase = "EXECUTE"` — chat-driven workflows run in
/// execute mode by default. Richer phase progression
/// (INIT→ANALYZE→PLAN→EXECUTE→VERIFY→DONE) requires a real workflow state
/// machine and is out of scope for v1.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkflowSnapshot {
    /// Chat thread this snapshot applies to.
    pub thread_id: String,
    /// Syncode-side workflow id bound to the thread (from the sidecar).
    pub workflow_id: String,
    /// Current workflow phase (v1 always "EXECUTE").
    pub phase: String,
    /// User input for this turn — surfaces as the current task.
    pub current_task: Option<String>,
    /// Total tasks in the plan (None when not tracked).
    pub total_tasks: Option<u32>,
    /// 1-based index of the current task (None when not tracked).
    pub current_task_index: Option<u32>,
}

/// Build a [`WorkflowSnapshot`] for a thread at turn time.
///
/// Returns `None` when:
/// - No pool is attached (in-memory mode — workflow binding is silent).
/// - Thread has no workflow link yet (orphan thread — preamble also None).
/// - Sidecar lookup fails (errors are logged + swallowed — turn proceeds).
///
/// `user_input` becomes the snapshot's `current_task`. The phase is hardcoded
/// to `"EXECUTE"` for v1 (see module docs).
pub async fn build_workflow_snapshot(
    pool: Option<&SqlitePool>,
    thread_id: &str,
    user_input: &str,
) -> Option<WorkflowSnapshot> {
    let pool = pool?;
    let workflow_id = match syncode_persistence::thread_workflow_link::lookup(pool, thread_id).await
    {
        Ok(Some(id)) => id,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(
                thread_id = thread_id,
                error = %e,
                "thread_workflow_links lookup failed — skipping workflow snapshot"
            );
            return None;
        }
    };
    Some(WorkflowSnapshot {
        thread_id: thread_id.to_string(),
        workflow_id,
        phase: "EXECUTE".to_string(),
        current_task: Some(user_input.to_string()),
        total_tasks: None,
        current_task_index: None,
    })
}

/// Emit a `WorkflowContextBound` push event on the orchestration channel.
///
/// Best-effort: `broadcast::send` errors when there are no receivers (normal
/// before any client subscribes) — swallowed, never propagated. A turn must
/// never fail because nobody is listening on the push bus.
///
/// Wire shape (camelCase outer envelope matches [`crate::push::WsDomainEventPublisher`];
/// `eventType` is PascalCase to match the existing backend convention — the
/// frontend adapter's `toCamelTag` then yields `workflowContextBound`):
/// ```json
/// {
///   "eventType": "WorkflowContextBound",
///   "aggregateId": "<thread_id>",
///   "data": {
///     "threadId":     "<thread_id>",
///     "workflowId":   "<workflow_id>",
///     "phase":        "EXECUTE",
///     "currentTask":  "<user_input>",
///     "totalTasks":   null,
///     "currentTaskIndex": null
///   }
/// }
/// ```
pub fn emit_workflow_context_push(
    push_tx: &tokio::sync::broadcast::Sender<(String, serde_json::Value)>,
    snapshot: &WorkflowSnapshot,
) {
    let data = serde_json::json!({
        "threadId":         snapshot.thread_id,
        "workflowId":       snapshot.workflow_id,
        "phase":            snapshot.phase,
        "currentTask":      snapshot.current_task,
        "totalTasks":       snapshot.total_tasks,
        "currentTaskIndex": snapshot.current_task_index,
    });
    let payload = serde_json::json!({
        "eventType": "WorkflowContextBound",
        "aggregateId": snapshot.thread_id,
        "data": data,
    });
    let _ = push_tx.send((crate::channels::CHANNEL_ORCHESTRATION.to_string(), payload));
}

/// Production [`WorkflowStateProvider`] backed by the `thread_workflow_links`
/// sidecar + the syncode-ws `workflow_preamble` generator.
///
/// For each freshly started chat session, this:
/// 1. Reads the thread's workflow_id from the sidecar (None → no preamble,
///    back-compat with no-workflow threads).
/// 2. Builds a `WorkflowPreambleInput` from the user's turn text — the input
///    itself becomes the "current task", and the phase defaults to `EXECUTE`
///    (chat-driven workflows run in execute mode by default; richer phase
///    progression is future work).
/// 3. Generates the preamble text and returns it to the reactor, which
///    prepends it to `SessionContext.system_prompt`.
pub struct ThreadWorkflowPreamble {
    pool: Option<SqlitePool>,
}

impl ThreadWorkflowPreamble {
    /// Build a preamble provider. `pool = None` is the in-memory mode: every
    /// call returns `None`, identical to having no provider attached.
    pub fn new(pool: Option<SqlitePool>) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl WorkflowStateProvider for ThreadWorkflowPreamble {
    async fn workflow_preamble(&self, thread_id: &str, user_input: &str) -> Option<String> {
        // Reuse the snapshot builder so the preamble and the push event stay
        // in lock-step — both read the same sidecar row + apply the same
        // v1 phase=EXECUTE rule.
        let snapshot = build_workflow_snapshot(self.pool.as_ref(), thread_id, user_input).await?;
        let input = crate::workflow_preamble::WorkflowPreambleInput {
            phase: snapshot.phase,
            current_task: snapshot.current_task,
            total_tasks: snapshot.total_tasks,
            current_task_index: snapshot.current_task_index,
        };
        let preamble = crate::workflow_preamble::build_workflow_preamble(Some(&input));
        if preamble.is_empty() {
            None
        } else {
            Some(preamble)
        }
    }
}

/// Ensure a thread has a workflow link. Returns the active workflow_id:
///
/// - `Some(id)` when the sidecar is backed by a pool and the link was
///   either already present or freshly created.
/// - `None` when no pool is attached (in-memory mode) or the sidecar
///   write failed (errors are logged and swallowed — the turn proceeds
///   without a workflow binding).
///
/// Idempotent: a second call with the same thread_id returns the same
/// workflow_id without creating a new row.
pub async fn ensure_link_for_thread(pool: Option<&SqlitePool>, thread_id: &str) -> Option<String> {
    let pool = pool?;

    // Fast path: existing link → no write.
    match syncode_persistence::thread_workflow_link::lookup(pool, thread_id).await {
        Ok(Some(existing)) => return Some(existing),
        Ok(None) => {} // fall through to create
        Err(e) => {
            tracing::warn!(
                thread_id = thread_id,
                error = %e,
                "thread_workflow_links lookup failed — will attempt upsert anyway"
            );
        }
    }

    let workflow_id = Uuid::new_v4().to_string();
    if let Err(e) =
        syncode_persistence::thread_workflow_link::upsert(pool, thread_id, &workflow_id).await
    {
        tracing::warn!(
            thread_id = thread_id,
            error = %e,
            "thread_workflow_links upsert failed — turn proceeds without workflow binding"
        );
        return None;
    }
    tracing::info!(
        thread_id = thread_id,
        workflow_id = %workflow_id,
        "created thread_workflow_links row for new thread"
    );
    Some(workflow_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        syncode_persistence::migrations::run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn returns_none_when_pool_is_none() {
        let result = ensure_link_for_thread(None, "t1").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn creates_link_on_first_call() {
        let pool = setup_pool().await;
        let wf = ensure_link_for_thread(Some(&pool), "t1").await;
        assert!(wf.is_some(), "expected Some(workflow_id)");
        let wf = wf.unwrap();

        // Verify it landed in the sidecar.
        let stored = syncode_persistence::thread_workflow_link::lookup(&pool, "t1")
            .await
            .unwrap();
        assert_eq!(stored.as_deref(), Some(wf.as_str()));
    }

    #[tokio::test]
    async fn reuses_existing_link_on_second_call() {
        let pool = setup_pool().await;
        let first = ensure_link_for_thread(Some(&pool), "t1").await.unwrap();
        let second = ensure_link_for_thread(Some(&pool), "t1").await.unwrap();
        assert_eq!(
            first, second,
            "idempotent: second call must return the same workflow_id"
        );
    }

    #[tokio::test]
    async fn different_threads_get_different_workflow_ids() {
        let pool = setup_pool().await;
        let a = ensure_link_for_thread(Some(&pool), "t1").await.unwrap();
        let b = ensure_link_for_thread(Some(&pool), "t2").await.unwrap();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn preamble_provider_returns_none_when_no_pool() {
        let provider = ThreadWorkflowPreamble::new(None);
        let result = provider.workflow_preamble("t1", "do thing").await;
        assert!(result.is_none(), "no pool → no preamble");
    }

    #[tokio::test]
    async fn preamble_provider_returns_none_when_no_link() {
        let pool = setup_pool().await;
        let provider = ThreadWorkflowPreamble::new(Some(pool));
        let result = provider
            .workflow_preamble("orphan-thread", "do thing")
            .await;
        assert!(result.is_none(), "no link → no preamble");
    }

    #[tokio::test]
    async fn preamble_provider_returns_text_when_link_exists() {
        let pool = setup_pool().await;
        // C1 must have seeded the link first.
        ensure_link_for_thread(Some(&pool), "t1").await.unwrap();
        let provider = ThreadWorkflowPreamble::new(Some(pool));
        let preamble = provider
            .workflow_preamble("t1", "fix the bug")
            .await
            .expect("preamble present after C1 seeds the link");
        assert!(
            preamble.contains("WORKFLOW CONTEXT"),
            "preamble missing header: {preamble}"
        );
        assert!(
            preamble.contains("EXECUTE"),
            "preamble missing phase: {preamble}"
        );
        assert!(
            preamble.contains("fix the bug"),
            "preamble must surface the user input as current task: {preamble}"
        );
    }

    // ─── build_workflow_snapshot ──────────────────────────────────────────

    #[tokio::test]
    async fn snapshot_returns_none_when_no_pool() {
        let snap = build_workflow_snapshot(None, "t1", "do thing").await;
        assert!(snap.is_none(), "no pool → no snapshot");
    }

    #[tokio::test]
    async fn snapshot_returns_none_when_no_link() {
        let pool = setup_pool().await;
        let snap = build_workflow_snapshot(Some(&pool), "orphan", "do thing").await;
        assert!(snap.is_none(), "no link → no snapshot");
    }

    #[tokio::test]
    async fn snapshot_carries_workflow_id_phase_and_task_when_link_exists() {
        let pool = setup_pool().await;
        let wf = ensure_link_for_thread(Some(&pool), "t1").await.unwrap();
        let snap = build_workflow_snapshot(Some(&pool), "t1", "ship the feature")
            .await
            .expect("snapshot present after C1 seeds the link");
        assert_eq!(snap.thread_id, "t1");
        assert_eq!(snap.workflow_id, wf);
        assert_eq!(snap.phase, "EXECUTE");
        assert_eq!(snap.current_task.as_deref(), Some("ship the feature"));
        assert!(snap.total_tasks.is_none());
        assert!(snap.current_task_index.is_none());
    }

    // ─── emit_workflow_context_push ───────────────────────────────────────

    #[tokio::test]
    async fn push_emits_context_bound_envelope_on_orchestration_channel() {
        let (push_tx, mut rx) = tokio::sync::broadcast::channel::<(String, serde_json::Value)>(8);
        let snap = WorkflowSnapshot {
            thread_id: "thread-42".to_string(),
            workflow_id: "wf-7".to_string(),
            phase: "EXECUTE".to_string(),
            current_task: Some("write tests".to_string()),
            total_tasks: None,
            current_task_index: None,
        };
        emit_workflow_context_push(&push_tx, &snap);

        let (channel, payload) = rx.recv().await.expect("should receive the broadcast");
        assert_eq!(channel, crate::channels::CHANNEL_ORCHESTRATION);
        // PascalCase wire tag — frontend adapter's toCamelTag yields "workflowContextBound".
        assert_eq!(payload["eventType"], "WorkflowContextBound");
        assert_eq!(payload["aggregateId"], "thread-42");
        // camelCase keys in data — matches the wire contract above.
        assert_eq!(payload["data"]["threadId"], "thread-42");
        assert_eq!(payload["data"]["workflowId"], "wf-7");
        assert_eq!(payload["data"]["phase"], "EXECUTE");
        assert_eq!(payload["data"]["currentTask"], "write tests");
        assert!(payload["data"]["totalTasks"].is_null());
        assert!(payload["data"]["currentTaskIndex"].is_null());
    }

    #[tokio::test]
    async fn push_succeeds_with_no_subscribers() {
        // No subscribers is normal before any client connects — broadcast
        // returns SendError but emit must not propagate it.
        let (push_tx, _) = tokio::sync::broadcast::channel::<(String, serde_json::Value)>(8);
        let snap = WorkflowSnapshot {
            thread_id: "t".to_string(),
            workflow_id: "w".to_string(),
            phase: "EXECUTE".to_string(),
            current_task: None,
            total_tasks: None,
            current_task_index: None,
        };
        // Must not panic / must not return a Result that the caller has to map.
        emit_workflow_context_push(&push_tx, &snap);
    }
}

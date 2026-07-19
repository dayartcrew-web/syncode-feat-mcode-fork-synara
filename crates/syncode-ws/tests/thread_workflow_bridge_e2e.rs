//! Integration test — chat → workflow bridge pipeline (C1 + C2 + C3).
//!
//! Walks the same path the production `StartTurn` / `thread.turn.start`
//! handlers in `rpc.rs` walk:
//!
//!   1. **C1** `ensure_link_for_thread` seeds the sidecar row on first turn
//!      and reuses it on subsequent turns (idempotent).
//!   2. **C2** `ThreadWorkflowPreamble::workflow_preamble` builds the system
//!      prompt preamble from the freshly seeded link + the user's input.
//!   3. **C3** `build_workflow_snapshot` + `emit_workflow_context_push`
//!      serialize the snapshot to the wire shape the frontend adapter
//!      consumes (`eventType: "WorkflowContextBound"`, camelCase data keys,
//!      `aggregateId: <threadId>`).
//!
//! This mirrors the production sequence in `rpc.rs` (both `StartTurn` and
//! `thread.turn.start` arms) without needing to boot a full WS server —
//! the bridge is pure-library and exercises the same code paths the
//! handlers call into.
//!
//! Gating: none — pure library calls against an in-memory SQLite pool, so
//! this runs in the default `cargo test` invocation.

use syncode_persistence::SqlitePool;
use syncode_ws::thread_workflow_bridge::{
    build_workflow_snapshot, emit_workflow_context_push, ensure_link_for_thread,
    ThreadWorkflowPreamble,
};
use syncode_orchestration::workflow_state::WorkflowStateProvider;
use tokio::sync::broadcast;

async fn setup_pool() -> SqlitePool {
    let pool = SqlitePool::connect(":memory:").await.expect("connect in-memory pool");
    syncode_persistence::migrations::run(&pool).await.expect("run migrations");
    pool
}

/// C1 + C2 + C3 happy path — the full chat→workflow→preamble→push pipeline
/// the `StartTurn` / `thread.turn.start` handlers drive in production.
#[tokio::test]
async fn chat_turn_pipeline_seeds_link_builds_preamble_emits_push() {
    let pool = setup_pool().await;
    let thread_id = "thread-chat-turn-1";
    let user_input = "Ship the C4 frontend badge";

    // ── C1: ensure_link_for_thread seeds the sidecar on first turn ───────
    let wf_id_first = ensure_link_for_thread(Some(&pool), thread_id)
        .await
        .expect("first turn must seed a workflow_id");
    assert!(!wf_id_first.is_empty(), "workflow_id must not be empty");

    // Second turn reuses the same workflow_id (idempotent — no churn).
    let wf_id_second = ensure_link_for_thread(Some(&pool), thread_id)
        .await
        .expect("second turn must still return a workflow_id");
    assert_eq!(
        wf_id_first, wf_id_second,
        "idempotent: subsequent turns must reuse the seeded workflow_id"
    );

    // ── C2: ThreadWorkflowPreamble produces text grounded on the sidecar ─
    let preamble_provider = ThreadWorkflowPreamble::new(Some(pool.clone()));
    let preamble = preamble_provider
        .workflow_preamble(thread_id, user_input)
        .await
        .expect("preamble must be present after C1 seeds the link");
    assert!(
        preamble.contains("WORKFLOW CONTEXT"),
        "preamble must include the header block: {preamble}"
    );
    assert!(
        preamble.contains("EXECUTE"),
        "preamble must include the v1 EXECUTE phase: {preamble}"
    );
    assert!(
        preamble.contains(user_input),
        "preamble must surface the user input as the current task: {preamble}"
    );
    // Note: the preamble generator (workflow_preamble.rs) emits only Phase,
    // Current task, and Constraints — workflow_id is intentionally NOT in
    // the system prompt (it would waste tokens + leak an internal id).

    // ── C3: build_workflow_snapshot + emit_workflow_context_push ─────────
    let snapshot = build_workflow_snapshot(Some(&pool), thread_id, user_input)
        .await
        .expect("snapshot must be present after C1 seeds the link");
    assert_eq!(snapshot.thread_id, thread_id);
    assert_eq!(snapshot.workflow_id, wf_id_first);
    assert_eq!(snapshot.phase, "EXECUTE");
    assert_eq!(snapshot.current_task.as_deref(), Some(user_input));

    let (push_tx, mut rx) = broadcast::channel::<(String, serde_json::Value)>(8);
    emit_workflow_context_push(&push_tx, &snapshot);

    let (channel, payload) = rx.recv().await.expect("must receive the broadcast");
    assert_eq!(
        channel,
        syncode_ws::channels::CHANNEL_ORCHESTRATION,
        "must publish on the orchestration channel"
    );
    // Wire shape contract — frontend adapter's toCamelTag maps PascalCase
    // → "workflowContextBound", which the adapter then routes as the
    // "thread.workflow-context-bound" OrchestrationEvent variant.
    assert_eq!(payload["eventType"], "WorkflowContextBound");
    assert_eq!(payload["aggregateId"], thread_id);
    assert_eq!(payload["data"]["threadId"], thread_id);
    assert_eq!(payload["data"]["workflowId"], wf_id_first);
    assert_eq!(payload["data"]["phase"], "EXECUTE");
    assert_eq!(payload["data"]["currentTask"], user_input);
    assert!(payload["data"]["totalTasks"].is_null());
    assert!(payload["data"]["currentTaskIndex"].is_null());
}

/// C1 is a no-op in-memory (no pool) — chat must never crash because the
/// sidecar is unavailable. This is the in-memory-mode backward-compat
/// guarantee.
#[tokio::test]
async fn chat_turn_pipeline_is_silent_in_in_memory_mode() {
    // No pool attached — simulate the in-memory server boot path.
    let wf_id = ensure_link_for_thread(None, "thread-mem-1").await;
    assert!(wf_id.is_none(), "no pool → no workflow binding (silent)");

    let preamble_provider = ThreadWorkflowPreamble::new(None);
    let preamble = preamble_provider.workflow_preamble("thread-mem-1", "do thing").await;
    assert!(preamble.is_none(), "no pool → no preamble (silent)");

    let snapshot = build_workflow_snapshot(None, "thread-mem-1", "do thing").await;
    assert!(snapshot.is_none(), "no pool → no snapshot (silent)");
}

/// Different threads get different workflow_ids — the sidecar must never
/// accidentally cross-link chat threads to the same workflow.
#[tokio::test]
async fn different_threads_get_different_workflow_ids() {
    let pool = setup_pool().await;
    let a = ensure_link_for_thread(Some(&pool), "thread-distinct-1")
        .await
        .expect("thread a must seed a workflow_id");
    let b = ensure_link_for_thread(Some(&pool), "thread-distinct-2")
        .await
        .expect("thread b must seed a workflow_id");
    assert_ne!(
        a, b,
        "different threads must receive different workflow_ids"
    );

    // And each snapshot carries its own workflow_id.
    let snap_a = build_workflow_snapshot(Some(&pool), "thread-distinct-1", "task A")
        .await
        .expect("snapshot a present");
    let snap_b = build_workflow_snapshot(Some(&pool), "thread-distinct-2", "task B")
        .await
        .expect("snapshot b present");
    assert_eq!(snap_a.workflow_id, a);
    assert_eq!(snap_b.workflow_id, b);
    assert_eq!(snap_a.current_task.as_deref(), Some("task A"));
    assert_eq!(snap_b.current_task.as_deref(), Some("task B"));
}

/// Push emission is best-effort: no subscribers (the early-boot window
/// before any client connects) must not crash the bridge.
#[tokio::test]
async fn push_emission_survives_no_subscribers() {
    let pool = setup_pool().await;
    // Seed the link first so the snapshot is non-None — we're testing
    // push emission resilience, not the link/snapshot path (covered above).
    ensure_link_for_thread(Some(&pool), "thread-no-sub")
        .await
        .expect("seed link");
    let snap = build_workflow_snapshot(Some(&pool), "thread-no-sub", "do thing")
        .await
        .expect("snapshot present");

    // No subscribers — broadcast::send returns Err, but the bridge must
    // swallow it and never propagate.
    let (push_tx, _) = broadcast::channel::<(String, serde_json::Value)>(8);
    emit_workflow_context_push(&push_tx, &snap);
    // Test passes if we reach this line without panic.
}

/// Orphan threads (sidecar lookup returns None for an unknown id) yield no
/// preamble and no snapshot — the chat proceeds without workflow context
/// rather than crashing. This is the "first turn before C1 has run" edge
/// case the bridge must tolerate.
#[tokio::test]
async fn orphan_thread_yields_no_preamble_and_no_snapshot() {
    let pool = setup_pool().await;
    let preamble = ThreadWorkflowPreamble::new(Some(pool.clone()))
        .workflow_preamble("never-linked-thread", "do thing")
        .await;
    assert!(preamble.is_none(), "orphan thread → no preamble");

    let snapshot = build_workflow_snapshot(Some(&pool), "never-linked-thread", "do thing").await;
    assert!(snapshot.is_none(), "orphan thread → no snapshot");
}

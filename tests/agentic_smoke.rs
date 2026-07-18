//! PR #207 agentic subsystems — end-to-end smoke tests.
//!
//! These tests exercise the four new modules shipped in PR #207 —
//! `Critic`, `DagGraph`, `HybridMemoryProvider`, and the augmented
//! `execute_workflow_with_critic` pipeline — as a single integrated stack,
//! not in isolation. They sit at the workspace root so they can wire real
//! implementations from multiple crates without mock seams at crate
//! boundaries.
//!
//! ## What's covered
//!
//! 1. **Happy path** — workflow + critic + memory persist a successful run.
//! 2. **Rejection path** — critic rejection routes through `WorkflowError::StepFailed`
//!    and persistence is skipped.
//! 3. **Memory grounding** — `retrieve_context` output flows verbatim into the
//!    planner's `context` argument.
//! 4. **DAG scheduling** — topology-order traversal drives an executor through
//!    `next_ready()` + `complete()` for a 4-node diamond graph.
//! 5. **Real-provider smoke** (`#[ignore]`) — env-gated test that runs the
//!    full pipeline against a real provider adapter (Anthropic HTTP or any
//!    CLI adapter on PATH) when `SYNCODE_SMOKE_PROVIDER` + the matching
//!    credentials are present.
//!
//! ## What's NOT covered
//!
//! `execute_dag_workflow` is currently a skeleton (returns an empty
//! `DagRunSummary`); the test below simulates the eventual contract by
//! driving `next_ready()` + `execute()` + `complete()` manually. Once the
//! skeleton is fleshed out, this test should be replaced by a single call
//! to `execute_dag_workflow(&mut graph, &executor)`.

use std::sync::{Arc, Mutex};

use syncode_core::agent::{AgentState, WorkflowError, WorkflowStep};
use syncode_memory::{HybridMemoryProvider, InMemoryBackend, MemoryProvider, NO_PRIOR_CONTEXT};
use syncode_orchestration::{
    Critic, CriticVerdict, DagGraph, EdgeKind, NoOpCritic, NodeState, TaskSpec, WorkflowExecutor,
    execute_workflow_with_critic,
};

// ---------------------------------------------------------------------------
// Test fixtures — recording executor + rule-based critic
// ---------------------------------------------------------------------------

/// Captures every call to `plan` / `execute` so tests can assert grounding,
/// ordering, and call counts without mocking the trait at crate boundary.
struct RecordingExecutor {
    calls: Mutex<Vec<(String, String)>>,
    plan_output: String,
    execute_output: String,
}

impl RecordingExecutor {
    fn new(plan_output: impl Into<String>, execute_output: impl Into<String>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            plan_output: plan_output.into(),
            execute_output: execute_output.into(),
        }
    }

    fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().expect("calls mutex poisoned").clone()
    }
}

impl WorkflowExecutor for RecordingExecutor {
    fn plan(&self, task: &str, context: &str) -> Result<String, WorkflowError> {
        self.calls
            .lock()
            .expect("calls mutex poisoned")
            .push(("plan".to_string(), format!("{task}||{context}")));
        Ok(self.plan_output.clone())
    }

    fn execute(&self, plan: &str) -> Result<String, WorkflowError> {
        self.calls
            .lock()
            .expect("calls mutex poisoned")
            .push(("execute".to_string(), plan.to_string()));
        Ok(self.execute_output.clone())
    }

    fn provider_tag(&self) -> &str {
        "recording-mock"
    }
}

/// Rule-based critic that approves outputs containing a required marker.
struct MarkerCritic {
    marker: String,
}

impl MarkerCritic {
    fn new(marker: impl Into<String>) -> Self {
        Self {
            marker: marker.into(),
        }
    }
}

impl Critic for MarkerCritic {
    fn review(&self, execution_output: &str) -> Result<CriticVerdict, WorkflowError> {
        if execution_output.contains(&self.marker) {
            Ok(CriticVerdict::Approved {
                rationale: format!("output contained required marker `{}`", self.marker),
            })
        } else {
            Ok(CriticVerdict::Rejected {
                reasons: vec![format!("output missing required marker `{}`", self.marker)],
            })
        }
    }
}

fn fresh_memory() -> Arc<dyn MemoryProvider> {
    Arc::new(HybridMemoryProvider::new().with_backend(Arc::new(InMemoryBackend::new())))
}

// ---------------------------------------------------------------------------
// 1. Happy path — workflow + NoOp critic + memory persists a successful run
// ---------------------------------------------------------------------------

#[tokio::test]
async fn workflow_with_critic_happy_path_persists_interaction_to_memory() {
    let memory = fresh_memory();
    let executor = RecordingExecutor::new("plan: do X then Y", "ANSWER: 42");

    let state = execute_workflow_with_critic(
        "user-smoke-1",
        "wf-smoke-1",
        "what is the answer",
        memory.as_ref(),
        &executor,
        &NoOpCritic,
    )
    .await
    .expect("happy-path workflow must complete");

    assert!(state.is_completed, "state.is_completed");
    assert_eq!(state.current_step, WorkflowStep::Completed);
    assert_eq!(executor.calls().len(), 2, "plan + execute called once each");

    // Memory must contain the persisted interaction.
    let ctx = memory
        .retrieve_context("user-smoke-1", "what is the answer")
        .await;
    assert_ne!(ctx, NO_PRIOR_CONTEXT, "interaction must be persisted");
    assert!(
        ctx.contains("what is the answer"),
        "persisted context must contain the prompt: {ctx}"
    );
    assert!(
        ctx.contains("ANSWER: 42"),
        "persisted context must contain the response: {ctx}"
    );
}

// ---------------------------------------------------------------------------
// 2. Rejection path — critic rejects, persistence skipped, error returned
// ---------------------------------------------------------------------------

#[tokio::test]
async fn workflow_with_critic_rejection_routes_to_step_failed_and_skips_persistence() {
    let memory = fresh_memory();
    let executor = RecordingExecutor::new("plan: emit answer", "output without marker");
    // Critic will reject — output lacks the required `ANSWER:` marker.
    let critic = MarkerCritic::new("ANSWER:");

    let err = execute_workflow_with_critic(
        "user-smoke-2",
        "wf-smoke-2",
        "produce answer",
        memory.as_ref(),
        &executor,
        &critic,
    )
    .await
    .expect_err("rejection must surface as Err");

    match err {
        WorkflowError::StepFailed(msg) => {
            assert!(
                msg.contains("critic rejected output"),
                "error message must mention critic rejection: {msg}"
            );
            assert!(
                msg.contains("ANSWER:"),
                "error message must surface the missing marker: {msg}"
            );
        }
        other => panic!("rejection must route through StepFailed, got {other:?}"),
    }

    // Persistence must be skipped — memory still empty for this user.
    let ctx = memory
        .retrieve_context("user-smoke-2", "produce answer")
        .await;
    assert_eq!(
        ctx, NO_PRIOR_CONTEXT,
        "rejected workflow must NOT persist an interaction"
    );
}

// ---------------------------------------------------------------------------
// 3. Memory grounding — retrieved context flows verbatim into planner
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retrieved_memory_context_flows_into_planner_context_argument() {
    let memory = fresh_memory();

    // Seed memory with a prior interaction we can detect in the planner call.
    memory
        .persist_interaction(
            "user-smoke-3",
            "earlier prompt",
            "earlier response with secret-token-XYZ",
            "claude",
            100,
        )
        .await
        .expect("seed persist");

    let executor = RecordingExecutor::new("plan", "ANSWER: ok");
    let _state = execute_workflow_with_critic(
        "user-smoke-3",
        "wf-smoke-3",
        "follow up prompt",
        memory.as_ref(),
        &executor,
        &NoOpCritic,
    )
    .await
    .expect("workflow completes");

    let calls = executor.calls();
    let (kind, payload) = calls.first().expect("at least one plan call recorded");
    assert_eq!(kind, "plan", "first call must be plan");
    assert!(
        payload.contains("secret-token-XYZ"),
        "planner's context arg must contain the seeded memory: {payload}"
    );
    assert!(
        payload.contains("follow up prompt"),
        "planner's task arg must be the initial_task: {payload}"
    );
}

// ---------------------------------------------------------------------------
// 4. DAG scheduling — drives nodes through executor in topology order
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dag_scheduling_drives_nodes_through_executor_in_topology_order() {
    // Build a 4-node diamond: source -> {left, right} -> sink.
    // topological ready order: source, {left, right} (any order), sink.
    let mut graph = DagGraph::new();
    let source = graph.add_node(TaskSpec::task("source", "produce input"));
    let left = graph.add_node(TaskSpec::task("left", "left branch"));
    let right = graph.add_node(TaskSpec::task("right", "right branch"));
    let sink = graph.add_node(TaskSpec::task("sink", "merge"));
    graph.add_edge(source, left, EdgeKind::Dependency).unwrap();
    graph.add_edge(source, right, EdgeKind::Dependency).unwrap();
    graph.add_edge(left, sink, EdgeKind::Dependency).unwrap();
    graph.add_edge(right, sink, EdgeKind::Dependency).unwrap();

    let executor = RecordingExecutor::new("plan", "ANSWER: executed");

    // Drive the graph the way `execute_dag_workflow` (currently a skeleton)
    // eventually will: pull next_ready, "execute" via the mock, mark complete.
    let mut executed: Vec<String> = Vec::new();
    loop {
        let ready = graph.next_ready();
        if ready.is_empty() {
            break;
        }
        for node_id in ready {
            let node = graph.node(node_id).expect("node exists");
            // Executor treats node.payload as the task.
            let _plan = executor
                .plan(&node.spec.payload, "")
                .expect("plan succeeds");
            let output = executor.execute(&_plan).expect("execute succeeds");
            assert!(
                output.contains("ANSWER"),
                "execute output must pass structural check"
            );
            executed.push(node.spec.label.clone());
            graph.complete(node_id).expect("complete succeeds");
        }
    }

    // All four nodes executed; sink last; source first.
    assert_eq!(executed.len(), 4, "all 4 nodes must execute");
    assert_eq!(executed[0], "source", "source must run first");
    assert_eq!(executed[3], "sink", "sink must run last");
    assert!(
        executed.contains(&"left".to_string()) && executed.contains(&"right".to_string()),
        "both branches must execute: {executed:?}"
    );

    // Final state: sink complete, nothing ready.
    assert_eq!(
        graph.node(sink).unwrap().state,
        NodeState::Complete,
        "sink must be complete"
    );
    assert!(graph.next_ready().is_empty(), "no nodes left ready");

    // Executor recorded 8 calls (plan + execute per node).
    assert_eq!(executor.calls().len(), 8, "2 calls × 4 nodes");
}

// ---------------------------------------------------------------------------
// 5. Real-provider smoke (#[ignore]) — env-gated live API/CLI verification
// ---------------------------------------------------------------------------

/// Live end-to-end smoke against a real provider.
///
/// Enable by setting `ANTHROPIC_API_KEY` (the adapter reads this from env at
/// request time — no explicit config wiring needed for the smoke test). The
/// adapter must already be spawned before the workflow runs; the test
/// constructs + spawns + tears down around the workflow call.
///
/// Run with:
/// ```bash
/// ANTHROPIC_API_KEY=sk-ant-... \
///   cargo test -p syncode-integration-tests --test agentic_smoke \
///   -- --ignored --nocapture real_provider_anthropic_smoke
/// ```
#[tokio::test]
#[ignore = "live Anthropic call — set ANTHROPIC_API_KEY to enable"]
async fn real_provider_anthropic_smoke() {
    use std::env;

    // Skip cleanly when the key is missing (so `cargo test --workspace` with
    // `--ignored` doesn't fail on machines without the key).
    if env::var("ANTHROPIC_API_KEY").is_err() {
        eprintln!(
            "skipping live Anthropic smoke — ANTHROPIC_API_KEY not set. \
             Set it and re-run with `--ignored real_provider_anthropic_smoke`."
        );
        return;
    }

    // Construct the adapter directly — there's no generic factory in
    // syncode-provider today (each adapter owns its constructor).
    let mut adapter = syncode_provider::adapters::anthropic::AnthropicAdapter::new();
    use syncode_provider::{ProviderAdapter, ProviderConfig};
    adapter
        .spawn(ProviderConfig::default())
        .await
        .expect("anthropic adapter spawns");

    let session_manager = Arc::new(syncode_provider::SessionManager::new());
    let executor = syncode_orchestration::ProviderWorkflowExecutor::new(
        Arc::new(adapter) as Arc<dyn syncode_provider::ProviderAdapter>,
        session_manager,
    );

    let memory: Arc<dyn MemoryProvider> =
        Arc::new(HybridMemoryProvider::new().with_backend(Arc::new(InMemoryBackend::new())));

    let state: AgentState = execute_workflow_with_critic(
        "user-live",
        "wf-live",
        "Reply with the literal token: SMOKE_OK and nothing else.",
        memory.as_ref(),
        &executor,
        &MarkerCritic::new("SMOKE_OK"),
    )
    .await
    .expect("live provider workflow must complete");

    assert!(state.is_completed, "live workflow completed");
    let ctx = memory.retrieve_context("user-live", "SMOKE_OK").await;
    assert_ne!(ctx, NO_PRIOR_CONTEXT, "live interaction must persist");
    assert!(
        ctx.contains("SMOKE_OK"),
        "live response must contain SMOKE_OK marker: {ctx}"
    );
}

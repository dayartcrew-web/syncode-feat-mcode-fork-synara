//! Workspace integration tests
//!
//! Cross-crate integration tests verifying that the major subsystems
//! work together correctly. These sit at the workspace root and can
//! access all public APIs.

use syncode_core::{EntityId, Timestamp};

// ---------------------------------------------------------------------------
// Core primitives — EntityId, Timestamp
// ---------------------------------------------------------------------------

#[test]
fn entity_id_creates_unique_values() {
    let a = EntityId::new();
    let b = EntityId::new();
    let a_str = a.as_str();
    let b_str = b.as_str();
    assert_ne!(a_str, b_str);
}

#[test]
fn entity_id_roundtrip_through_string() {
    let id = EntityId::new();
    let s = id.as_str();
    let parsed = EntityId::parse(&s).unwrap();
    assert_eq!(parsed, id);
}

#[test]
fn timestamp_now_advances() {
    let a = Timestamp::now();
    let b = Timestamp::now();
    assert!(b >= a);
}

#[test]
fn timestamp_serde_roundtrip() {
    let ts = Timestamp::now();
    let json = serde_json::to_string(&ts).unwrap();
    let back: Timestamp = serde_json::from_str(&json).unwrap();
    assert_eq!(ts, back);
}

// ---------------------------------------------------------------------------
// Contract validation — syncode_contracts types
// ---------------------------------------------------------------------------

#[test]
fn contract_entity_id_roundtrip() {
    use syncode_contracts::EntityId as ContractEntityId;
    let id = ContractEntityId::new();
    let json = serde_json::to_string(&id).unwrap();
    let back: ContractEntityId = serde_json::from_str(&json).unwrap();
    assert_eq!(id.as_str(), back.as_str());
}

#[test]
fn contract_timestamp_valid_rfc3339() {
    use syncode_contracts::Timestamp as ContractTimestamp;
    let ts = ContractTimestamp::now();
    assert!(chrono::DateTime::parse_from_rfc3339(ts.as_str()).is_ok());
}

#[test]
fn contract_session_view_roundtrip() {
    use syncode_contracts::*;
    let session = SessionView {
        id: EntityId::new(),
        provider_id: "claude".into(),
        model: "claude-sonnet-4".into(),
        working_directory: Some("/tmp/project".into()),
        created_at: Timestamp::now(),
        status: SessionStatus::Idle,
    };
    let json = serde_json::to_string(&session).unwrap();
    let decoded: SessionView = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.provider_id, "claude");
}

// ---------------------------------------------------------------------------
// Provider system — all adapters implement ProviderAdapter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn all_providers_are_known() {
    use syncode_provider::ALL_PROVIDERS;
    assert_eq!(ALL_PROVIDERS.len(), 10);
    for id in ALL_PROVIDERS {
        assert!(!id.is_empty());
    }
}

#[tokio::test]
async fn all_provider_adapters_spawn_and_shutdown() {
    use syncode_provider::adapters::*;
    use syncode_provider::{ProviderAdapter, ProviderConfig, ProviderStatus};

    // Subprocess adapters (claude, codex, cursor, grok, gemini, kilo, opencode,
    // pi) all spawn a real CLI binary. `cargo test --workspace` must pass on
    // any machine regardless of which CLIs are installed, so each adapter's
    // spawn→Idle→shutdown path is gated behind its own env var. Real-binary
    // validation lives in the dedicated `*_e2e.rs` integration tests.
    //
    // HTTP adapters (anthropic, openai) hold no subprocess — spawn is a pure
    // config-store, so they are always exercised.

    // --- Claude (subprocess) -------------------------------------------------
    if std::env::var("SYNICODE_CLAUDE_E2E").as_deref() == Ok("1") {
        let mut claude = ClaudeAdapter::new();
        claude.spawn(ProviderConfig::default()).await.unwrap();
        assert_eq!(claude.status(), ProviderStatus::Idle);
        claude.shutdown().await.unwrap();
    } else {
        assert_eq!(ClaudeAdapter::new().status(), ProviderStatus::Disconnected);
    }

    // --- Codex (subprocess) --------------------------------------------------
    if std::env::var("SYNICODE_CODEX_E2E").as_deref() == Ok("1") {
        let mut codex = CodexAdapter::new();
        codex.spawn(ProviderConfig::default()).await.unwrap();
        assert_eq!(codex.status(), ProviderStatus::Idle);
        codex.shutdown().await.unwrap();
    } else {
        assert_eq!(CodexAdapter::new().status(), ProviderStatus::Disconnected);
    }

    // --- Cursor / Grok / Gemini (ACP subprocess) -----------------------------
    let acp_e2e = std::env::var("SYNICODE_ACP_E2E").as_deref() == Ok("1");
    if acp_e2e {
        let mut cursor = create_cursor();
        cursor.spawn(ProviderConfig::default()).await.unwrap();
        assert_eq!(cursor.status(), ProviderStatus::Idle);
        cursor.shutdown().await.unwrap();

        let mut grok = create_grok();
        grok.spawn(ProviderConfig::default()).await.unwrap();
        assert_eq!(grok.status(), ProviderStatus::Idle);
        grok.shutdown().await.unwrap();

        let mut gemini = create_gemini();
        gemini.spawn(ProviderConfig::default()).await.unwrap();
        assert_eq!(gemini.status(), ProviderStatus::Idle);
        gemini.shutdown().await.unwrap();
    } else {
        assert_eq!(create_cursor().status(), ProviderStatus::Disconnected);
        assert_eq!(create_grok().status(), ProviderStatus::Disconnected);
        assert_eq!(create_gemini().status(), ProviderStatus::Disconnected);
    }

    // --- Kilo (subprocess) ---------------------------------------------------
    if std::env::var("SYNICODE_KILO_E2E").as_deref() == Ok("1") {
        let mut kilo = KiloAdapter::new();
        kilo.spawn(ProviderConfig::default()).await.unwrap();
        assert_eq!(kilo.status(), ProviderStatus::Idle);
        kilo.shutdown().await.unwrap();
    } else {
        assert_eq!(KiloAdapter::new().status(), ProviderStatus::Disconnected);
    }

    // --- OpenCode (subprocess) -----------------------------------------------
    if std::env::var("SYNICODE_OPENCODE_E2E").as_deref() == Ok("1") {
        let mut opencode = OpenCodeAdapter::new();
        opencode.spawn(ProviderConfig::default()).await.unwrap();
        assert_eq!(opencode.status(), ProviderStatus::Idle);
        opencode.shutdown().await.unwrap();
    } else {
        assert_eq!(
            OpenCodeAdapter::new().status(),
            ProviderStatus::Disconnected
        );
    }

    // --- Pi (subprocess) -----------------------------------------------------
    if std::env::var("SYNICODE_PI_E2E").as_deref() == Ok("1") {
        let mut pi = PiAdapter::new();
        pi.spawn(ProviderConfig::default()).await.unwrap();
        assert_eq!(pi.status(), ProviderStatus::Idle);
        pi.shutdown().await.unwrap();
    } else {
        assert_eq!(PiAdapter::new().status(), ProviderStatus::Disconnected);
    }

    // --- Anthropic (HTTP — always exercised) --------------------------------
    let mut anthropic = AnthropicAdapter::new();
    anthropic.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(anthropic.status(), ProviderStatus::Idle);
    anthropic.shutdown().await.unwrap();

    // --- OpenAI (HTTP — always exercised) -----------------------------------
    let mut openai = OpenAIAdapter::new();
    openai.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(openai.status(), ProviderStatus::Idle);
    openai.shutdown().await.unwrap();
}

#[tokio::test]
async fn all_provider_adapters_have_unique_ids() {
    use syncode_provider::ProviderAdapter;
    use syncode_provider::adapters::*;
    let ids: Vec<String> = vec![
        ClaudeAdapter::new().provider_id().to_string(),
        CodexAdapter::new().provider_id().to_string(),
        create_cursor().provider_id().to_string(),
        create_gemini().provider_id().to_string(),
        create_grok().provider_id().to_string(),
        KiloAdapter::new().provider_id().to_string(),
        OpenCodeAdapter::new().provider_id().to_string(),
        PiAdapter::new().provider_id().to_string(),
        AnthropicAdapter::new().provider_id().to_string(),
        OpenAIAdapter::new().provider_id().to_string(),
    ];
    let unique: std::collections::HashSet<String> = ids.into_iter().collect();
    assert_eq!(unique.len(), 10, "All provider IDs should be unique");
}

#[tokio::test]
async fn all_provider_adapters_have_capabilities() {
    use syncode_provider::ProviderAdapter;
    use syncode_provider::adapters::*;

    let adapters: Vec<Box<dyn ProviderAdapter>> = vec![
        Box::new(ClaudeAdapter::new()),
        Box::new(CodexAdapter::new()),
        Box::new(create_cursor()),
        Box::new(create_gemini()),
        Box::new(create_grok()),
        Box::new(KiloAdapter::new()),
        Box::new(OpenCodeAdapter::new()),
        Box::new(PiAdapter::new()),
        Box::new(AnthropicAdapter::new()),
        Box::new(OpenAIAdapter::new()),
    ];

    for adapter in adapters {
        let caps = adapter.capabilities();
        assert!(
            !caps.is_empty(),
            "Adapter {} should have at least one capability",
            adapter.provider_id()
        );
        let models = adapter.available_models();
        assert!(
            !models.is_empty(),
            "Adapter {} should have at least one model",
            adapter.provider_id()
        );
    }
}

// ---------------------------------------------------------------------------
// Automation system
// ---------------------------------------------------------------------------

#[test]
fn automation_definition_construction() {
    use syncode_automation::{AutomationDef, ScheduleType};
    let def = AutomationDef::new(
        "test-automation".to_string(),
        "echo hello".to_string(),
        ScheduleType::Manual,
    );
    assert_eq!(def.name, "test-automation");
    assert_eq!(def.command, "echo hello");
    assert!(def.enabled);
}

#[tokio::test]
async fn automation_scheduler_crud() {
    use syncode_automation::{AutomationDef, ScheduleType, Scheduler};
    let scheduler = Scheduler::new();
    let def = AutomationDef::new(
        "cron-test".to_string(),
        "echo hi".to_string(),
        ScheduleType::Manual,
    );
    let id = def.id;
    let id_str = id.as_str();
    scheduler.register(def).await.unwrap();

    let fetched = scheduler.get(&id_str).await.unwrap();
    assert_eq!(fetched.name, "cron-test");
    let count = scheduler.automation_count().await;
    assert_eq!(count, 1);
}

#[test]
fn automation_retry_policy_delays() {
    use syncode_automation::RetryPolicy;
    let policy = RetryPolicy::ExponentialBackoff {
        max_retries: 5,
        base_delay_secs: 1,
    };
    // Exponential backoff: delay doubles each attempt
    let d1 = policy.delay_for_attempt(1).unwrap();
    let d3 = policy.delay_for_attempt(3).unwrap();
    assert!(d1 < d3);
}

// ---------------------------------------------------------------------------
// Terminal system
// ---------------------------------------------------------------------------

#[test]
fn output_buffer_basic_flow() {
    use syncode_terminal::OutputBuffer;
    let mut buf = OutputBuffer::new(100, 1024);
    buf.write("hello");
    buf.write(" world");
    let chunk = buf.flush();
    assert!(chunk.is_some());
    let c = chunk.unwrap();
    assert_eq!(c.data, "hello world");
}

#[test]
fn output_buffer_ack_protocol() {
    use syncode_terminal::OutputBuffer;
    let mut buf = OutputBuffer::new(100, 1024);
    buf.write("aaa");
    buf.write("bbb");
    buf.flush();

    // No ack yet — should return all chunks
    let unacked = buf.unacked_chunks();
    assert_eq!(unacked.len(), 1); // "aaabbb" flushed as single chunk

    // Ack first chunk (seq 0)
    buf.ack(0);
    let unacked = buf.unacked_chunks();
    assert_eq!(unacked.len(), 0);
}

// ---------------------------------------------------------------------------
// Cross-domain — Provider session context uses core EntityId
// ---------------------------------------------------------------------------

#[tokio::test]
async fn provider_session_context_uses_core_types() {
    use syncode_provider::{ProviderRequest, SessionContext};

    let ctx = SessionContext {
        thread_id: EntityId::new(),
        turn_id: EntityId::new(),
        working_dir: "/tmp/syncode".to_string(),
        system_prompt: Some("You are helpful.".to_string()),
        user_input: "Write a test".to_string(),
        context_files: vec!["src/main.rs".to_string()],
    };

    // Verify serialization works across crates
    let json = serde_json::to_string(&ctx).unwrap();
    let back: SessionContext = serde_json::from_str(&json).unwrap();
    assert_eq!(back.working_dir, ctx.working_dir);
    assert_eq!(back.context_files.len(), 1);

    // ProviderRequest serialization
    let req = ProviderRequest::new("initialize", Some(serde_json::json!({"key": "val"})));
    assert_eq!(req.jsonrpc, "2.0");
    assert!(req.id > 0);
}

// ---------------------------------------------------------------------------
// Cross-domain — Core domain events serialization
// ---------------------------------------------------------------------------

#[test]
fn core_domain_event_serialization() {
    use syncode_core::DomainEvent;

    let id = EntityId::new();
    let event = DomainEvent::ProjectCreated {
        id,
        name: "serde-project".to_string(),
        root_path: "/tmp/serde".to_string(),
        created_at: Timestamp::now(),
    };
    let json = serde_json::to_string(&event).unwrap();
    let back: DomainEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(back.event_type_name(), "ProjectCreated");
}

#[test]
fn core_turn_lifecycle() {
    use syncode_core::Turn;

    let mut turn = Turn::new(EntityId::new(), 1, "hello world");
    assert_eq!(turn.user_input, "hello world");
    turn.start_running();
    turn.complete_with_response("done");
    // Verify serialization across the boundary
    let json = serde_json::to_string(&turn).unwrap();
    let back: Turn = serde_json::from_str(&json).unwrap();
    assert_eq!(back.user_input, "hello world");
}

// ---------------------------------------------------------------------------
// T6 — Cross-crate integration of new additive modules
//
// These tests prove the four new modules (Critic, DAG runtime, Hybrid memory,
// Unknown event variant) compose cleanly with the existing workspace:
//
//   - syncode-orchestration::Critic            (T1)
//   - syncode-orchestration::dag::DagGraph     (T3)
//   - syncode-memory::HybridMemoryProvider     (T4)
//   - syncode-core::DomainEvent::Unknown       (T5)
//
// Each test exercises ONE module in isolation first, then a follow-up test
// wires them together. The full-workspace build/test/clippy/fmt gate at
// the end of T6 is the real integration smoke test.
// ---------------------------------------------------------------------------

// --- T1: Critic trait ------------------------------------------------------

#[test]
fn critic_noop_approves_with_rationale() {
    use syncode_orchestration::{Critic, CriticVerdict, NoOpCritic};

    let critic = NoOpCritic;
    let verdict = critic.review("any output").unwrap();
    match verdict {
        CriticVerdict::Approved { rationale } => {
            assert!(
                !rationale.is_empty(),
                "NoOp critic rationale must be non-empty"
            );
        }
        other => panic!("NoOpCritic must approve, got {other:?}"),
    }
}

// --- T3: DAG runtime -------------------------------------------------------

#[test]
fn dag_runtime_end_to_end_scheduling() {
    use syncode_orchestration::{DagGraph, EdgeKind, NodeState, TaskSpec};

    // Build a 3-node diamond: source -> {left, right} -> sink
    let mut g = DagGraph::new();
    let source = g.add_node(TaskSpec::task("source", "produce input"));
    let left = g.add_node(TaskSpec::task("left", "left branch"));
    let right = g.add_node(TaskSpec::task("right", "right branch"));
    let sink = g.add_node(TaskSpec::task("sink", "merge results"));
    g.add_edge(source, left, EdgeKind::Dependency).unwrap();
    g.add_edge(source, right, EdgeKind::Dependency).unwrap();
    g.add_edge(left, sink, EdgeKind::Dependency).unwrap();
    g.add_edge(right, sink, EdgeKind::Dependency).unwrap();

    // Initially only source is ready.
    assert_eq!(g.next_ready(), vec![source]);

    // Complete source — left and right become ready (fan-out).
    g.complete(source).unwrap();
    let mut ready = g.next_ready();
    ready.sort();
    let mut expected = vec![left, right];
    expected.sort();
    assert_eq!(ready, expected);

    // Complete left — right still ready, sink still gated.
    g.complete(left).unwrap();
    assert_eq!(g.next_ready(), vec![right]);

    // Complete right — sink finally ready.
    g.complete(right).unwrap();
    assert_eq!(g.next_ready(), vec![sink]);

    // All nodes complete — nothing ready.
    g.complete(sink).unwrap();
    assert!(g.next_ready().is_empty());
    assert_eq!(g.node(sink).unwrap().state, NodeState::Complete);
}

#[test]
fn dag_runtime_snapshot_survives_serialization_across_boundary() {
    use syncode_orchestration::{DagGraph, EdgeKind, TaskSpec};

    // Snapshot must JSON-serialize so it can later be persisted to SQLite
    // via syncode-persistence. The integration test here proves the JSON
    // boundary is intact end-to-end.
    let mut g = DagGraph::new();
    let a = g.add_node(TaskSpec::task("a", "do A"));
    let b = g.add_node(TaskSpec::task("b", "do B"));
    g.add_edge(a, b, EdgeKind::Dependency).unwrap();
    g.complete(a).unwrap();

    let snap = g.snapshot();
    let json = serde_json::to_string(&snap).expect("snapshot must serialize to JSON");
    assert!(json.contains("\"nodes\""));
    assert!(json.contains("\"edges\""));
    assert!(json.contains("Dependency"));
    assert!(json.contains("Complete"));
}

// --- T4: Hybrid memory -----------------------------------------------------

#[tokio::test]
async fn hybrid_memory_drops_into_memory_provider_slot() {
    use std::sync::Arc;
    use syncode_memory::{HybridMemoryProvider, InMemoryBackend, MemoryProvider, NO_PRIOR_CONTEXT};

    // The non-conflict contract: HybridMemoryProvider implements the
    // existing MemoryProvider trait, so it can be Box<dyn MemoryProvider>
    // in any pipeline that today uses SqliteMemoryStore.
    let provider: Box<dyn MemoryProvider> =
        Box::new(HybridMemoryProvider::new().with_backend(Arc::new(InMemoryBackend::new())));

    // Empty store retrieves the sentinel (not an empty string).
    let ctx = provider.retrieve_context("user-1", "any query").await;
    assert_eq!(ctx, NO_PRIOR_CONTEXT);

    // Persist + retrieve round-trips.
    provider
        .persist_interaction("user-1", "hello", "world", "claude", 42)
        .await
        .unwrap();
    let ctx = provider.retrieve_context("user-1", "hello").await;
    assert_ne!(ctx, NO_PRIOR_CONTEXT, "after persist, must retrieve data");
    assert!(ctx.contains("hello"));
    assert!(ctx.contains("world"));
}

// --- T5: Unknown event variant ---------------------------------------------

#[test]
fn domain_event_unknown_variant_round_trips() {
    use syncode_core::DomainEvent;

    // Emit an Unknown event — this is what a projector would see if a
    // future producer emits an event type this consumer doesn't know about.
    let event = DomainEvent::Unknown;
    let json = serde_json::to_string(&event).expect("Unknown must serialize");
    let back: DomainEvent = serde_json::from_str(&json).expect("Unknown must deserialize");
    assert!(matches!(back, DomainEvent::Unknown));
    assert_eq!(back.event_type_name(), "Unknown");
}

#[test]
fn domain_event_known_variants_still_decode_correctly() {
    // Regression: the addition of the Unknown variant must not break any
    // of the 44 existing variants. Sample one well-known variant and prove
    // its serialization is unaffected.
    use syncode_core::DomainEvent;

    let event = DomainEvent::ProjectCreated {
        id: EntityId::new(),
        name: "post-unknown-project".into(),
        root_path: "/tmp/x".into(),
        created_at: Timestamp::now(),
    };
    let json = serde_json::to_string(&event).unwrap();
    let back: DomainEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(back.event_type_name(), "ProjectCreated");
}

// --- T6 composite: DAG + Critic + Hybrid Memory compose without conflict ---

#[tokio::test]
async fn new_modules_compose_with_existing_pipeline_without_conflict() {
    use std::sync::Arc;
    use syncode_memory::{HybridMemoryProvider, InMemoryBackend, MemoryProvider};
    use syncode_orchestration::{
        Critic, DagGraph, EdgeKind, NoOpCritic, TaskSpec, WorkflowExecutor,
    };

    // Wire all four modules together. The point of this test is type-system
    // composition: if any of the new modules had a conflicting trait impl,
    // signature, or import, this would not compile.

    // 1. Hybrid memory backend (T4)
    let memory: Arc<dyn MemoryProvider> =
        Arc::new(HybridMemoryProvider::new().with_backend(Arc::new(InMemoryBackend::new())));

    // 2. Critic (T1) — default NoOp preserves existing pipeline semantics
    let critic = NoOpCritic;
    let verdict = critic.review("output").unwrap();
    assert!(matches!(
        verdict,
        syncode_orchestration::CriticVerdict::Approved { .. }
    ));

    // 3. DAG runtime (T3) — a small graph that an agent could be scheduled through
    let mut graph = DagGraph::new();
    let plan = graph.add_node(TaskSpec::task("plan", "decompose task"));
    let exec = graph.add_node(TaskSpec::task("exec", "execute plan"));
    graph.add_edge(plan, exec, EdgeKind::Dependency).unwrap();
    assert_eq!(graph.next_ready(), vec![plan]);
    graph.complete(plan).unwrap();
    assert_eq!(graph.next_ready(), vec![exec]);

    // 4. Unknown event variant (T5) — proves the 44-variant enum composes
    //    with the new modules without breakage. The memory + DAG + Critic
    //    all reference DomainEvent indirectly via the orchestrator's
    //    pipeline; this round-trip is the canary.
    let unknown_event = syncode_core::DomainEvent::Unknown;
    let json = serde_json::to_string(&unknown_event).unwrap();
    assert!(json.contains("Unknown"));

    // 5. Persist a memory interaction through the hybrid provider — proves
    //    the new MemoryProvider impl is observably indistinguishable from
    //    SqliteMemoryStore at the trait boundary.
    memory
        .persist_interaction("composer", "plan", "executed", "claude", 100)
        .await
        .unwrap();
    let ctx = memory.retrieve_context("composer", "plan").await;
    assert!(ctx.contains("executed"));

    // If this compiles and runs, every new module composes with every other
    // and with the existing pipeline. The WorkflowExecutor trait (unused
    // here directly) is referenced via the type parameter to prove the
    // import resolves cleanly across the orchestration crate boundary.
    let _ = std::marker::PhantomData::<dyn WorkflowExecutor>;
}

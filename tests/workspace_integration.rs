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

    let mut claude = ClaudeAdapter::new();
    claude.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(claude.status(), ProviderStatus::Idle);
    claude.shutdown().await.unwrap();

    let mut codex = CodexAdapter::new();
    codex.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(codex.status(), ProviderStatus::Idle);
    codex.shutdown().await.unwrap();

    let mut cursor = CursorAdapter::new();
    cursor.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(cursor.status(), ProviderStatus::Idle);
    cursor.shutdown().await.unwrap();

    let mut gemini = GeminiAdapter::new();
    gemini.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(gemini.status(), ProviderStatus::Idle);
    gemini.shutdown().await.unwrap();

    let mut grok = GrokAdapter::new();
    grok.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(grok.status(), ProviderStatus::Idle);
    grok.shutdown().await.unwrap();

    let mut kilo = KiloAdapter::new();
    kilo.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(kilo.status(), ProviderStatus::Idle);
    kilo.shutdown().await.unwrap();

    let mut opencode = OpenCodeAdapter::new();
    opencode.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(opencode.status(), ProviderStatus::Idle);
    opencode.shutdown().await.unwrap();

    let mut pi = PiAdapter::new();
    pi.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(pi.status(), ProviderStatus::Idle);
    pi.shutdown().await.unwrap();

    let mut anthropic = AnthropicAdapter::new();
    anthropic.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(anthropic.status(), ProviderStatus::Idle);
    anthropic.shutdown().await.unwrap();

    let mut openai = OpenAIAdapter::new();
    openai.spawn(ProviderConfig::default()).await.unwrap();
    assert_eq!(openai.status(), ProviderStatus::Idle);
    openai.shutdown().await.unwrap();
}

#[tokio::test]
async fn all_provider_adapters_have_unique_ids() {
    use syncode_provider::adapters::*;
    use syncode_provider::ProviderAdapter;
    let ids: Vec<String> = vec![
        ClaudeAdapter::new().provider_id().to_string(),
        CodexAdapter::new().provider_id().to_string(),
        CursorAdapter::new().provider_id().to_string(),
        GeminiAdapter::new().provider_id().to_string(),
        GrokAdapter::new().provider_id().to_string(),
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
    use syncode_provider::adapters::*;
    use syncode_provider::ProviderAdapter;

    let adapters: Vec<Box<dyn ProviderAdapter>> = vec![
        Box::new(ClaudeAdapter::new()),
        Box::new(CodexAdapter::new()),
        Box::new(CursorAdapter::new()),
        Box::new(GeminiAdapter::new()),
        Box::new(GrokAdapter::new()),
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
    let id = def.id.clone();
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
    use syncode_provider::{SessionContext, ProviderRequest};

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

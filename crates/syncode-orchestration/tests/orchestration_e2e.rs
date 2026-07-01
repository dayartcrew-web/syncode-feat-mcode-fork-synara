//! End-to-end test — full CQRS pipeline with SQLite-backed persistence.
//!
//! Gating: `SYNICODE_ORCHESTRATION_E2E=1`.

use std::sync::Arc;
use syncode_core::ports::EventRepository;
use syncode_core::EntityId;
use syncode_orchestration::{Command, Orchestrator};
use tempfile::TempDir;

fn e2e_enabled() -> bool {
    std::env::var("SYNICODE_ORCHESTRATION_E2E").ok().as_deref() == Some("1")
}

async fn setup_orchestrator() -> (Orchestrator, sqlx::SqlitePool, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db_path = dir.path().join("orch-e2e.db");
    let path_str = db_path.to_str().expect("db path");

    syncode_persistence::init_database(std::path::Path::new(path_str))
        .await.expect("init_database");
    let pool = syncode_persistence::get_pool(std::path::Path::new(path_str))
        .await.expect("get_pool");

    let event_repo = syncode_persistence::adapters::SqliteEventRepository::new(pool.clone());
    let orch = Orchestrator::new(Arc::new(event_repo));
    (orch, pool, dir)
}

#[tokio::test]
async fn orchestration_real_db_project_lifecycle() {
    if !e2e_enabled() {
        eprintln!("[skip] orchestration e2e: set SYNICODE_ORCHESTRATION_E2E=1");
        return;
    }
    let (orch, _pool, _dir) = setup_orchestrator().await;

    let result = orch.handle_command(Command::CreateProject {
        name: "orch-e2e-proj".into(),
        root_path: "/tmp/orch-e2e".into(),
    }).await;
    assert!(result.is_ok(), "CreateProject failed: {:?}", result.err());

    // Hold the Arc, then the guard
    let rm_arc = orch.read_model_ref();
    let rm = rm_arc.read().await;
    assert!(!rm.projects.is_empty());
    let first = rm.projects.values().next().unwrap();
    assert_eq!(first.name, "orch-e2e-proj");
    let project_id_str = first.id.clone();
    let project_id = EntityId::parse(&project_id_str).expect("parse project id");
    drop(rm);
    drop(rm_arc);

    let result = orch.handle_command(Command::UpdateProjectConfig {
        id: project_id,
        provider_id: Some("test-provider".into()),
        default_model: None,
    }).await;
    assert!(result.is_ok(), "UpdateProjectConfig failed: {:?}", result.err());

    let rm_arc = orch.read_model_ref();
    let rm = rm_arc.read().await;
    assert_eq!(rm.projects[&project_id_str].provider_id.as_deref(), Some("test-provider"));
}

#[tokio::test]
async fn orchestration_real_db_thread_lifecycle() {
    if !e2e_enabled() { eprintln!("[skip] orchestration e2e"); return; }
    let (orch, _pool, _dir) = setup_orchestrator().await;

    orch.handle_command(Command::CreateProject {
        name: "thread-proj".into(), root_path: "/tmp".into(),
    }).await.expect("create project");

    let rm_arc = orch.read_model_ref();
    let rm = rm_arc.read().await;
    let project_id_str = rm.projects.values().next().unwrap().id.clone();
    let project_id = EntityId::parse(&project_id_str).expect("parse project id");
    drop(rm);
    drop(rm_arc);

    let result = orch.handle_command(Command::CreateThread {
        project_id,
        provider_id: "test".into(),
        model: "default".into(),
    }).await;
    assert!(result.is_ok(), "CreateThread failed: {:?}", result.err());

    let rm_arc = orch.read_model_ref();
    let rm = rm_arc.read().await;
    let thread_id_str = rm.threads.keys().next().unwrap().clone();
    let thread_id = EntityId::parse(&thread_id_str).expect("parse thread id");
    drop(rm);
    drop(rm_arc);

    orch.handle_command(Command::PauseThread { id: thread_id.clone() })
        .await.expect("PauseThread");
    orch.handle_command(Command::ResumeThread { id: thread_id.clone() })
        .await.expect("ResumeThread");
    orch.handle_command(Command::CompleteThread { id: thread_id.clone() })
        .await.expect("CompleteThread");

    let rm_arc = orch.read_model_ref();
    let rm = rm_arc.read().await;
    assert_eq!(&rm.threads[&thread_id_str].status, "completed");
}

#[tokio::test]
async fn orchestration_real_db_events_persisted_to_sqlite() {
    if !e2e_enabled() { eprintln!("[skip] orchestration e2e"); return; }
    let (orch, pool, _dir) = setup_orchestrator().await;

    orch.handle_command(Command::CreateProject {
        name: "persist-proj".into(), root_path: "/tmp".into(),
    }).await.expect("create project");

    let rm_arc = orch.read_model_ref();
    let rm = rm_arc.read().await;
    let project_id_str = rm.projects.values().next().unwrap().id.clone();
    drop(rm);
    drop(rm_arc);

    let row: Option<(i64, String)> = sqlx::query_as(
        "SELECT sequence, event_type FROM events WHERE aggregate_id = ? ORDER BY sequence"
    ).bind(&project_id_str).fetch_optional(&pool).await.expect("query events");

    assert!(row.is_some(), "no events found in SQLite");
    let (seq, et) = row.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(et, "ProjectCreated");
}

#[tokio::test]
async fn orchestration_real_db_replay_rebuilds_read_model() {
    if !e2e_enabled() { eprintln!("[skip] orchestration e2e"); return; }
    let (orch, _pool, _dir) = setup_orchestrator().await;

    orch.handle_command(Command::CreateProject {
        name: "replay-proj".into(), root_path: "/tmp".into(),
    }).await.expect("create project");

    let rm_arc = orch.read_model_ref();
    let rm = rm_arc.read().await;
    let pid_str = rm.projects.values().next().unwrap().id.clone();
    let name = rm.projects.values().next().unwrap().name.clone();
    drop(rm);
    drop(rm_arc);

    orch.replay_read_model().await.expect("replay_read_model");

    let rm_arc = orch.read_model_ref();
    let rm = rm_arc.read().await;
    assert!(rm.projects.get(&pid_str).is_some());
    assert_eq!(rm.projects[&pid_str].name, name);
}

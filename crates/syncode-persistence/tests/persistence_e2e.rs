//! End-to-end test — real SQLite file-backed database with the live persistence
//! surface (event store + snapshots). The former `view_*` projection /
//! read-model-adapter e2e cases were removed alongside the dead projection
//! layer (never wired into production).
//!
//! Gating: `SYNICODE_PERSISTENCE_E2E=1`.

use syncode_core::{DomainEvent, EntityId, Timestamp};
use tempfile::TempDir;

fn e2e_enabled() -> bool {
    std::env::var("SYNICODE_PERSISTENCE_E2E").ok().as_deref() == Some("1")
}

async fn open_db(dir: &TempDir) -> sqlx::SqlitePool {
    let db_path = dir.path().join("test.db");
    let path_str = db_path.to_str().expect("db path");
    syncode_persistence::init_database(std::path::Path::new(path_str))
        .await
        .expect("init_database");
    syncode_persistence::get_pool(std::path::Path::new(path_str))
        .await
        .expect("get_pool")
}

#[tokio::test]
async fn persistence_real_db_event_append_and_replay() {
    if !e2e_enabled() {
        eprintln!("[skip] persistence e2e: set SYNICODE_PERSISTENCE_E2E=1");
        return;
    }
    let dir = TempDir::new().expect("temp dir");
    let pool = open_db(&dir).await;

    let aggregate_id = EntityId::new();
    let event = DomainEvent::ProjectCreated {
        id: aggregate_id,
        name: "e2e-project".into(),
        root_path: "/tmp/e2e".into(),
        created_at: Timestamp::now(),
    };

    let envelopes =
        syncode_persistence::event_store::append_domain_events(&pool, aggregate_id, vec![event], 0)
            .await
            .expect("append_domain_events");
    assert_eq!(envelopes.len(), 1);

    let replayed = syncode_persistence::event_store::replay_envelopes(&pool, aggregate_id)
        .await
        .expect("replay_envelopes");
    assert_eq!(replayed.len(), 1);

    let ver =
        syncode_persistence::event_store::current_version(&pool, aggregate_id.as_str().as_str())
            .await
            .expect("current_version");
    assert_eq!(ver, 1);
}

#[tokio::test]
async fn persistence_real_db_survives_reopen() {
    if !e2e_enabled() {
        eprintln!("[skip] persistence e2e");
        return;
    }
    let dir = TempDir::new().expect("temp dir");
    let pool = open_db(&dir).await;

    let aggregate_id = EntityId::new();
    syncode_persistence::event_store::append_domain_events(
        &pool,
        aggregate_id,
        vec![DomainEvent::ProjectCreated {
            id: aggregate_id,
            name: "survive".into(),
            root_path: "/tmp".into(),
            created_at: Timestamp::now(),
        }],
        0,
    )
    .await
    .expect("append e1");
    syncode_persistence::event_store::append_domain_events(
        &pool,
        aggregate_id,
        vec![DomainEvent::ProjectUpdated {
            id: aggregate_id,
            provider_id: Some("test-provider".into()),
            default_model: None,
            updated_at: Timestamp::now(),
        }],
        1,
    )
    .await
    .expect("append e2");

    drop(pool);

    let db_path = dir.path().join("test.db");
    let path_str = db_path.to_str().expect("db path");
    let pool2 = syncode_persistence::get_pool(std::path::Path::new(path_str))
        .await
        .expect("reopen pool");

    let events = syncode_persistence::event_store::replay_envelopes(&pool2, aggregate_id)
        .await
        .expect("replay");
    assert_eq!(events.len(), 2);

    let ver =
        syncode_persistence::event_store::current_version(&pool2, aggregate_id.as_str().as_str())
            .await
            .expect("current_version");
    assert_eq!(ver, 2);
}

#[tokio::test]
async fn persistence_real_db_snapshot_save_load() {
    if !e2e_enabled() {
        eprintln!("[skip] persistence e2e");
        return;
    }
    let dir = TempDir::new().expect("temp dir");
    let pool = open_db(&dir).await;

    let aggregate_id = EntityId::new();
    let state_json = serde_json::json!({"counter": 42});

    syncode_persistence::snapshot::save_snapshot(&pool, aggregate_id, &state_json, 5)
        .await
        .expect("save_snapshot");

    let loaded = syncode_persistence::snapshot::load_snapshot(&pool, aggregate_id)
        .await
        .expect("load_snapshot");
    assert!(loaded.is_some());
    let (state, version) = loaded.unwrap();
    assert_eq!(version, 5);
    assert_eq!(state, state_json);
}

//! Standalone Syncode WebSocket server binary.
//!
//! Boots an `axum` server exposing the JSON-RPC-over-WebSocket endpoint served
//! by `syncode-ws` (`build_app` / `build_ws_router`). This is the runnable
//! backend the browser-mode web UI connects to — without it, `syncode-ws` is a
//! library that only runs inside tests (see `tests/ws_e2e.rs`) and is not
//! embedded by the Tauri desktop shell.
//!
//! # State backing
//!
//! Prefers a **SQLite-backed** `WsState` (persists events + read model across
//! restarts) via `syncode-persistence::init_database` +
//! `SqliteEventRepository`. If SQLite initialization fails, it falls back to
//! `WsState::new_in_memory` so the server always boots (graceful degradation —
//! logged at `WARN`).
//!
//! # Configuration (environment)
//!
//! - `SYNCODE_WS_HOST` — bind host (default `127.0.0.1`; set to `0.0.0.0` for
//!   remote-reachable dev).
//! - `SYNCODE_WS_PORT` — bind port (default `3000`).
//! - `SYNCODE_DB` — SQLite DB path (default `syncode.db` in cwd; empty string
//!   → in-memory).
//! - `RUST_LOG` — tracing filter (default `syncode_ws=info,info`).
//!
//! # WebSocket path
//!
//! The WS upgrade endpoint is `/ws` (see `build_ws_router`). The web UI's
//! `wsTransport.ts` resolves to `ws://<host>:<port>/ws`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use syncode_core::ports::EventRepository;
use syncode_orchestration::Orchestrator;
use syncode_persistence::adapters::SqliteEventRepository;
use syncode_ws::{WsState, server::build_app};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 3000;
const DEFAULT_DB_PATH: &str = "syncode.db";
const PUSH_CAPACITY: usize = 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tracing: honor RUST_LOG, fall back to a sane default.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("syncode_ws=info,info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let host = std::env::var("SYNCODE_WS_HOST").unwrap_or_else(|_| DEFAULT_HOST.to_string());
    let port: u16 = std::env::var("SYNCODE_WS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT);
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|e| format!("invalid bind address {host}:{port}: {e}"))?;

    let state = build_state().await;

    let app = build_app(Arc::new(state));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(
        listen_addr = %bound,
        ws_path = "/ws",
        db_mode = if std::env::var("SYNCODE_DB").as_deref() == Ok("") {
            "in-memory"
        } else {
            "sqlite"
        },
        "Syncode WebSocket server listening"
    );

    axum::serve(listener, app).await?;
    Ok(())
}

/// Build the `WsState`. Prefers SQLite; falls back to in-memory on any failure
/// so the binary always boots.
async fn build_state() -> WsState {
    let db_path = std::env::var("SYNCODE_DB").unwrap_or_else(|_| DEFAULT_DB_PATH.to_string());

    // Empty SYNCODE_DB → explicit opt-out → in-memory.
    if db_path.is_empty() {
        tracing::warn!("SYNCODE_DB is empty — using in-memory event store");
        return WsState::new_in_memory(PUSH_CAPACITY);
    }

    match syncode_persistence::init_database(&PathBuf::from(&db_path)).await {
        Ok(pool) => {
            let repo: Arc<dyn EventRepository> = Arc::new(SqliteEventRepository::new(pool));
            let orchestrator = Orchestrator::new(repo);
            tracing::info!(db_path = %db_path, "SQLite-backed event store initialized");
            WsState::new(PUSH_CAPACITY, orchestrator)
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                db_path = %db_path,
                "SQLite init failed — falling back to in-memory event store"
            );
            WsState::new_in_memory(PUSH_CAPACITY)
        }
    }
}

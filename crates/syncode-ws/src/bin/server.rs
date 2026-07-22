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
//! Prefers a **SQLite-backed** `WsState` (persists events + read model +
//! server config/settings across restarts) via
//! `syncode-persistence::init_database` + `SqliteEventRepository`. The same
//! pool is attached to the `ServerSettingsState` (SRV-1) so config/settings
//! edits write-through to the `server_config` / `server_settings` tables and
//! survive a restart. If SQLite initialization fails, it falls back to
//! `WsState::new_in_memory` so the server always boots (graceful degradation —
//! logged at `WARN`); in that mode settings are in-memory only.
//!
//! # Resume-cursor persistence (P0-4)
//!
//! Provider session resume cursors are persisted to
//! `~/.syncode/session_cursors.json` by [`FileResumeCursorStore`]. On
//! startup, after the orchestrator is built, [`SessionManager`] rehydrates
//! cursor-bearing sessions from the file and asks the provider adapter to
//! reattach via `resume_session` — so an in-flight conversation survives a
//! server restart. On shutdown (Ctrl-C / SIGINT) the live cursors are
//! re-snapshotted to the file so the next start picks them up.
//!
//! # Configuration (environment)
//!
//! - `SYNCODE_WS_HOST` — bind host (default `127.0.0.1`; set to `0.0.0.0` for
//!   remote-reachable dev).
//! - `SYNCODE_WS_PORT` — bind port (default `3000`).
//! - `SYNCODE_DB` — SQLite DB path (default `syncode.db` in cwd; empty string
//!   → in-memory).
//! - `SYNCODE_DEFAULT_PROVIDER` — provider id to arm the chat pipeline
//!   (default `claude`). When the named provider's CLI is installed, turns
//!   actually dispatch to the provider and AI responses stream back; when it
//!   is absent the orchestrator falls back to inert mode (turns are recorded
//!   but no AI response is generated, and the server still boots).
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
use syncode_persistence::adapters::SqliteEventRepository;
use syncode_provider::FileResumeCursorStore;
use syncode_ws::orchestrator_setup::build_orchestrator;
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

    let app = build_app(Arc::new(state.clone()));
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

    // P0-4: on Ctrl-C / SIGINT, snapshot live resume cursors back to disk so
    // the next start can rehydrate them. Best-effort — a failure here is
    // logged and never blocks shutdown.
    let shutdown_state = state.clone();
    let shutdown = async move {
        tokio::signal::ctrl_c()
            .await
            .unwrap_or_else(|e| tracing::warn!(error = %e, "ctrl_c signal handler failed"));
        tracing::info!("shutdown signal received — persisting session resume cursors");
        if let Some(reactor) = shutdown_state.orchestrator.command_reactor() {
            let mgr = reactor.session_manager();
            let store = FileResumeCursorStore::new();
            let n = mgr.persist_sessions(&store).await;
            tracing::info!(persisted = n, "resume cursors persisted on shutdown");
        } else {
            tracing::info!("no command reactor configured — skipping cursor persistence");
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
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
            // SRV-1: clone the pool so the settings store can persist
            // config/settings documents to the same SQLite database. The
            // original pool backs the event repository (below).
            let settings_pool = pool.clone();
            let repo: Arc<dyn EventRepository> = Arc::new(SqliteEventRepository::new(pool));
            // SRV-1 follow-up: arm the orchestrator AFTER the pool is available
            // so `build_orchestrator` can read the persisted
            // `textGenerationModelSelection` (the Settings panel's provider
            // picker) before choosing an adapter. Previously the orchestrator
            // was armed first and always fell back to the env-var default,
            // ignoring the user's pick until next restart.
            let orchestrator = build_orchestrator(repo, Some(&settings_pool), None).await;
            tracing::info!(db_path = %db_path, "SQLite-backed event store initialized");
            let state = WsState::new(PUSH_CAPACITY, orchestrator);
            // Attach the pool to the in-memory settings store: loads any
            // persisted config/settings from disk and enables write-through on
            // every subsequent mutation. Best-effort — a failure here leaves
            // the store in-memory (the server still boots).
            {
                let mut store = state.settings.write().await;
                store.attach_pool(settings_pool).await;
            }
            tracing::info!(db_path = %db_path, "server settings persistence enabled (SRV-1)");
            state
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

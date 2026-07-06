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
use syncode_orchestration::Orchestrator;
use syncode_persistence::adapters::SqliteEventRepository;
use syncode_provider::{FileResumeCursorStore, SessionManager};
use syncode_ws::{WsState, server::build_app};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 3000;
const DEFAULT_DB_PATH: &str = "syncode.db";
const PUSH_CAPACITY: usize = 1024;
/// Default provider id used when `SYNCODE_DEFAULT_PROVIDER` is unset. The
/// provider's CLI must be installed on PATH for the chat to actually generate
/// AI responses; otherwise the orchestrator falls back to inert mode.
const DEFAULT_PROVIDER: &str = "claude";

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
            let orchestrator = build_orchestrator(repo).await;
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

/// Build the orchestrator with a [`ProviderCommandReactor`] + a provider
/// adapter, so turns actually invoke a provider and AI responses stream back.
///
/// The provider id comes from `SYNCODE_DEFAULT_PROVIDER` (default `claude`).
/// When the named provider's CLI is unavailable (the adapter factory returns
/// `None`), this falls back to [`Orchestrator::new`] — turns are still
/// recorded but no AI response is generated, and the server still boots
/// (graceful degradation, logged at `WARN`).
///
/// `WsState::new` wraps the orchestrator's push bus as a
/// [`syncode_ws::push::WsDomainEventPublisher`] via
/// [`Orchestrator::with_event_publisher`], so provider-stream-sourced domain
/// events (tokens, tool calls, completion) are pushed to subscribed clients.
///
/// # P0-4: resume-cursor rehydration
///
/// After wiring the adapter the orchestrator asks the [`SessionManager`] to
/// [`rehydrate_sessions`](SessionManager::rehydrate_sessions) from
/// [`FileResumeCursorStore`]. Cursor-bearing sessions that were in flight
/// before the restart are re-registered and the provider adapter is asked to
/// `resume_session` for each — best-effort, never blocks startup.
async fn build_orchestrator(repo: Arc<dyn EventRepository>) -> Orchestrator {
    let default_provider =
        std::env::var("SYNCODE_DEFAULT_PROVIDER").unwrap_or_else(|_| DEFAULT_PROVIDER.to_string());

    // PR-1-2: construct the shared read model handle first so the reactor and
    // the orchestrator can both see it. The reactor uses it to resolve a
    // thread's project root path as the session working directory; the
    // orchestrator's projector writes to it as commands are handled. Sharing
    // the Arc (not cloning the store) keeps them in lock-step.
    let read_model: Arc<tokio::sync::RwLock<syncode_orchestration::ReadModelStore>> = Arc::new(
        tokio::sync::RwLock::new(syncode_orchestration::ReadModelStore::new()),
    );

    let session_manager = SessionManager::new();
    let reactor = Arc::new(
        syncode_orchestration::ProviderCommandReactor::new(session_manager)
            .with_read_model(Arc::clone(&read_model)),
    );

    match syncode_provider::registry::create_by_id(&default_provider) {
        Some(adapter) => {
            // Spawn the provider adapter (launches codex app-server / claude CLI).
            {
                let mut guard = adapter.write().await;
                let config = syncode_provider::ProviderConfig {
                    provider_id: default_provider.clone(),
                    model: std::env::var("SYNCODE_DEFAULT_MODEL")
                        .ok()
                        .unwrap_or_default(),
                    api_key: None,
                    base_url: None,
                    max_tokens: Some(4096),
                    extra: std::collections::HashMap::new(),
                };
                match guard.spawn(config).await {
                    Ok(()) => {
                        tracing::info!(provider = %default_provider, "provider adapter spawned")
                    }
                    Err(e) => {
                        tracing::error!(provider = %default_provider, error = %e, "failed to spawn provider adapter — turns will fail")
                    }
                }
            }

            tracing::info!(
                provider = %default_provider,
                "chat pipeline armed: turns will dispatch to the provider"
            );

            // P0-4: rehydrate persisted sessions before the orchestrator takes
            // ownership of the adapter — pass a clone of the SharedAdapter
            // (Arc<RwLock<dyn …>>) so the rehydrate path can call
            // `resume_session` without an extra lock dance.
            let store = FileResumeCursorStore::new();
            let rehydrated = reactor
                .session_manager()
                .rehydrate_sessions(&store, &adapter)
                .await;
            let reattached = rehydrated
                .iter()
                .filter(|r| matches!(r.outcome, syncode_provider::RehydrationOutcome::Reattached))
                .count();
            let failed = rehydrated.len() - reattached;
            tracing::info!(
                rehydrated = rehydrated.len(),
                reattached,
                failed,
                "session resume cursors rehydrated"
            );

            // PR-1-2: pass the shared read model so the reactor can resolve
            // the session working directory from the thread's project root.
            Orchestrator::with_reactor_adapter_and_read_model(repo, reactor, adapter, read_model)
        }
        None => {
            tracing::warn!(
                provider = %default_provider,
                "provider adapter not available — chat will be inert \
                 (turns recorded but no AI response). Install the provider CLI \
                 or set SYNCODE_DEFAULT_PROVIDER to an available provider id."
            );
            Orchestrator::new(repo)
        }
    }
}

//! WebSocket server spawn inside the Tauri desktop shell (DSK-1).
//!
//! The standalone `syncode-ws` binary (`crates/syncode-ws/src/bin/server.rs`)
//! boots the same axum WS server the web UI speaks to. Before this module the
//! Tauri desktop shell did **not** spawn that server — `main.rs` only managed
//! the in-memory `ProviderRegistryState` / `SessionStoreState` used by its own
//! IPC commands, so a browser-mode web UI pointed at the desktop binary had
//! nothing to connect to.
//!
//! This module closes that gap: it builds a [`syncode_ws::WsState`] (preferring
//! SQLite, falling back to in-memory on any init failure — identical graceful
//! degradation to the standalone binary) and spawns `axum::serve` on the Tauri
//! app's existing tokio runtime. The shared `WsState` is then handed to Tauri
//! as managed state so IPC commands and WS handlers see the same backend.
//!
//! # Configuration (environment)
//!
//! Mirrors the standalone binary's env-var contract so ops/docs stay uniform:
//! - `SYNCODE_WS_HOST` — bind host (default `127.0.0.1`).
//! - `SYNCODE_WS_PORT` — bind port (default `30101`). The desktop default
//!   differs from the standalone binary's `3000` so a developer running both
//!   simultaneously (standalone for the browser UI + desktop shell) don't fight
//!   over a port; either is overridable via env.
//! - `SYNCODE_DB` — SQLite DB path (default `syncode.db`; empty → in-memory).
//! - `SYNCODE_DEFAULT_PROVIDER` — provider id armed on the chat pipeline
//!   (default `claude`); when the CLI is absent the orchestrator falls back to
//!   inert mode and the server still boots.
//!
//! # Why this is a library, not a `main.rs` inline block
//!
//! Splitting the spawn out of `main.rs` keeps the logic unit/integration
//! testable without spinning up a full Tauri runtime (which needs a display +
//! `tauri::generate_context!`). [`boot`] / [`spawn_with_state`] are plain async
//! fns; the integration test (`tests/ws_spawn.rs`) drives them directly.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use syncode_core::ports::EventRepository;
use syncode_orchestration::{Orchestrator, ProviderCommandReactor, ReadModelStore};
use syncode_persistence::adapters::SqliteEventRepository;
use syncode_provider::SessionManager;
use syncode_ws::{WsState, server::build_app};
use tauri::Manager;
use tokio::task::JoinHandle;

/// Default bind host (loopback — the desktop shell is local-first; remote
/// reachability is the standalone binary's concern).
const DEFAULT_HOST: &str = "127.0.0.1";
/// Default bind port for the **desktop** shell. Picked distinct from the
/// standalone binary's `3000` so concurrent runs don't collide; override with
/// `SYNCODE_WS_PORT`.
const DEFAULT_PORT: u16 = 30101;
/// Default SQLite DB filename (resolved against the process cwd, matching the
/// standalone binary). Set `SYNCODE_DB=` (empty) for an in-memory store.
const DEFAULT_DB_PATH: &str = "syncode.db";
/// Capacity of the push broadcast bus (matches the standalone binary).
const PUSH_CAPACITY: usize = 1024;
/// Default provider id armed on the chat pipeline. The provider's CLI must be
/// on PATH for turns to actually generate AI responses; otherwise the
/// orchestrator falls back to inert mode.
const DEFAULT_PROVIDER: &str = "claude";

/// Configuration resolved from environment variables for the WS server.
///
/// [`WsConfig::from_env`] reads the `SYNCODE_WS_*` variables; fields are public
/// so callers (tests, future settings UI) can construct one explicitly without
/// touching the process environment.
#[derive(Debug, Clone)]
pub struct WsConfig {
    /// Bind host (e.g. `127.0.0.1`).
    pub host: String,
    /// Bind port.
    pub port: u16,
    /// SQLite DB path; empty string → in-memory event store.
    pub db_path: String,
    /// Provider id armed on the chat pipeline.
    pub default_provider: String,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            db_path: DEFAULT_DB_PATH.to_string(),
            default_provider: DEFAULT_PROVIDER.to_string(),
        }
    }
}

impl WsConfig {
    /// Resolve config from `SYNCODE_WS_*` env vars, falling back to defaults.
    pub fn from_env() -> Self {
        let host =
            std::env::var("SYNCODE_WS_HOST").unwrap_or_else(|_| DEFAULT_HOST.to_string());
        let port = std::env::var("SYNCODE_WS_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(DEFAULT_PORT);
        let db_path =
            std::env::var("SYNCODE_DB").unwrap_or_else(|_| DEFAULT_DB_PATH.to_string());
        let default_provider = std::env::var("SYNCODE_DEFAULT_PROVIDER")
            .unwrap_or_else(|_| DEFAULT_PROVIDER.to_string());
        Self {
            host,
            port,
            db_path,
            default_provider,
        }
    }

    /// Build the resolved bind address. Returns `Err` if `host:port` is not a
    /// valid `SocketAddr` (caller decides whether to abort startup).
    pub fn bind_addr(&self) -> Result<SocketAddr, String> {
        format!("{}:{}", self.host, self.port)
            .parse()
            .map_err(|e| format!("invalid bind address {}:{}: {}", self.host, self.port, e))
    }
}

/// Handle returned from [`boot`] / [`spawn_with_state`]: everything a Tauri
/// command or shutdown hook needs to reach the running server. Cloned cheaply
/// (everything is `Arc` / `String` / `JoinHandle`-by-value).
#[derive(Clone)]
pub struct WsHandle {
    /// The `ws://host:port/ws` URL clients should connect to.
    pub endpoint: String,
    /// The resolved bind address (useful for logs / `getDiagnostics`).
    pub bind_addr: SocketAddr,
    /// Shared WS state — same instance the WS handlers and Tauri commands see.
    pub state: Arc<WsState>,
    /// Background `axum::serve` task. Aborting it stops the server (called on
    /// app shutdown in a future task; today the task dies with the runtime).
    pub serve_task: Arc<JoinHandle<()>>,
}

impl WsHandle {
    /// Bound port (convenience for `getWsEndpoint` / diagnostics).
    pub fn port(&self) -> u16 {
        self.bind_addr.port()
    }
}

impl std::fmt::Debug for WsHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // JoinHandle isn't Debug; surface the parts callers care about.
        f.debug_struct("WsHandle")
            .field("endpoint", &self.endpoint)
            .field("bind_addr", &self.bind_addr)
            .finish_non_exhaustive()
    }
}

/// Build a SQLite-backed [`WsState`] (falling back to in-memory on any init
/// failure) and spawn `axum::serve` on the **current** tokio runtime.
///
/// This is the entry point `main.rs` calls inside `.setup()` — it must run on
/// the Tauri app's runtime (the calling task is already on it), so it does
/// **not** spawn its own runtime.
///
/// Returns the [`WsHandle`] (shared state + endpoint + serve task). Errors
/// only when the TCP listener cannot bind (e.g. port taken); SQLite/provider
/// failures degrade gracefully to in-memory / inert mode and are logged.
pub async fn boot(config: &WsConfig) -> Result<WsHandle, String> {
    let state = build_state(config).await;
    spawn_with_state(state, config).await
}

/// Spawn `axum::serve` for an **already-constructed** [`WsState`] on the
/// current tokio runtime.
///
/// Tests use this to boot a server backed by an in-memory state (no SQLite) so
/// they can run hermetically without touching the filesystem. Production
/// (`main.rs`) uses [`boot`] which builds the state first.
pub async fn spawn_with_state(
    state: WsState,
    config: &WsConfig,
) -> Result<WsHandle, String> {
    // Bind the listener BEFORE spawning so a bind failure surfaces to the
    // caller (rather than panicking the detached serve task where it would be
    // swallowed). Bind to the requested port; if the caller passed port 0 the
    // OS picks an ephemeral one (read back from `local_addr`).
    let addr = config.bind_addr()?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("failed to bind WS listener on {addr}: {e}"))?;
    let bound = listener
        .local_addr()
        .map_err(|e| format!("could not resolve bound WS address: {e}"))?;

    let state = Arc::new(state);
    let app = build_app(Arc::clone(&state));

    let serve_task = Arc::new(tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "Syncode desktop WS server exited with error");
        }
    }));

    let endpoint = format!("ws://{bound}/ws");
    tracing::info!(
        endpoint = %endpoint,
        bind_addr = %bound,
        ws_path = "/ws",
        "Syncode desktop WS server listening",
    );

    Ok(WsHandle {
        endpoint,
        bind_addr: bound,
        state,
        serve_task,
    })
}

/// Build the [`WsState`]: prefer SQLite (persists events + read model across
/// restarts); fall back to in-memory on any init failure so the desktop always
/// boots (graceful degradation — logged at `WARN`). Mirrors the standalone
/// binary's `build_state`.
async fn build_state(config: &WsConfig) -> WsState {
    // Empty db_path → explicit opt-out → in-memory.
    if config.db_path.is_empty() {
        tracing::warn!("SYNCODE_DB is empty — using in-memory event store");
        return WsState::new_in_memory(PUSH_CAPACITY);
    }

    match syncode_persistence::init_database(&PathBuf::from(&config.db_path)).await {
        Ok(pool) => {
            let repo: Arc<dyn EventRepository> = Arc::new(SqliteEventRepository::new(pool));
            let orchestrator = build_orchestrator(repo, &config.default_provider);
            tracing::info!(db_path = %config.db_path, "SQLite-backed event store initialized");
            WsState::new(PUSH_CAPACITY, orchestrator)
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                db_path = %config.db_path,
                "SQLite init failed — falling back to in-memory event store"
            );
            WsState::new_in_memory(PUSH_CAPACITY)
        }
    }
}

/// Build the orchestrator with a [`ProviderCommandReactor`] + a provider
/// adapter (when available), so turns actually invoke a provider and AI
/// responses stream back. When the named provider's CLI is unavailable the
/// adapter factory returns `None` and this falls back to inert mode (turns are
/// still recorded but no AI response is generated, and the server still boots).
/// Mirrors the standalone binary's `build_orchestrator`.
fn build_orchestrator(repo: Arc<dyn EventRepository>, default_provider: &str) -> Orchestrator {
    // PR-1-2: share the read model between reactor and orchestrator so the
    // reactor can resolve the session working directory from the thread's
    // project root path. Mirrors the standalone binary's build_orchestrator.
    let read_model: Arc<tokio::sync::RwLock<ReadModelStore>> =
        Arc::new(tokio::sync::RwLock::new(ReadModelStore::new()));
    let reactor = Arc::new(
        ProviderCommandReactor::new(SessionManager::new())
            .with_read_model(Arc::clone(&read_model)),
    );

    match syncode_provider::registry::create_by_id(default_provider) {
        Some(adapter) => {
            tracing::info!(
                provider = %default_provider,
                "chat pipeline armed: turns will dispatch to the provider"
            );
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

/// Tauri managed state carrying the running WS server handle so IPC commands
/// can read the endpoint / shared [`WsState`]. Stored via
/// [`tauri::Builder::manage`] / [`tauri::App::manage`] in `main.rs`.
///
/// Wrap in a `Mutex` rather than `RwLock`: write happens exactly once (during
/// setup) and reads dominate afterwards, but a `Mutex` keeps the type `Send +
/// Sync` unconditionally and avoids any async-lock footgun in sync Tauri
/// command handlers. Cheap — never contended past setup.
pub struct WsRuntimeState(pub std::sync::Mutex<Option<WsHandle>>);

impl WsRuntimeState {
    pub fn new() -> Self {
        Self(std::sync::Mutex::new(None))
    }

    /// Store the handle from `.setup()`. Panics if already set (setup runs
    /// once; a double-set is a programming error worth surfacing loudly).
    pub fn set(&self, handle: WsHandle) {
        let mut guard = self.0.lock().expect("WsRuntimeState poisoned");
        assert!(
            guard.is_none(),
            "WsRuntimeState already initialized — .setup() ran twice?"
        );
        *guard = Some(handle);
    }

    /// Snapshot the endpoint URL (cloned `String`). Returns `None` if the WS
    /// server failed to boot during setup (the rest of the app still runs —
    /// commands surface this as "WS unavailable").
    pub fn endpoint(&self) -> Option<String> {
        self.0
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|h| h.endpoint.clone()))
    }
}

impl Default for WsRuntimeState {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience: read the WS endpoint out of managed state, for Tauri commands.
///
/// Returns `None` when the WS server isn't managed (setup didn't run, or boot
/// failed). Commands should translate that into a structured error to the
/// frontend rather than panicking.
pub fn endpoint_from_app<AppT>(app: &AppT) -> Option<String>
where
    AppT: Manager<tauri::Wry>,
{
    app.try_state::<WsRuntimeState>()
        .and_then(|s| s.endpoint())
}

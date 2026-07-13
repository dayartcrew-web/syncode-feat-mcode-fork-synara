//! Syncode WebSocket — Transport Layer
//!
//! WebSocket JSON-RPC server, method dispatch, push bus,
//! channel management, and connection state machine.

// The `rpc/listMethods` payload uses a large `serde_json::json!` array literal
// (>120 entries); raise the recursion limit so the macro can expand it.
#![recursion_limit = "512"]

pub mod auth;
pub mod channels;
pub mod completion;
pub mod llm;
pub mod local_server;
pub mod orchestration_executor;
pub mod project_fs;
pub mod provider_versions;
pub mod push;
pub mod rpc;
pub mod server;
pub mod settings;
pub mod skills_catalog;
pub mod transport;
pub mod usage;
pub mod voice;

// Re-export the completion-harness host wiring so callers can construct the
// LLM/disable implementations directly (e.g. tests, alternate schedulers).
pub use completion::{WsCompletionDisableFn, WsCompletionLlm};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};

/// A JSON-RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<serde_json::Value>,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

/// A JSON-RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

/// A JSON-RPC error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl JsonRpcResponse {
    pub fn success(id: serde_json::Value, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id: Some(id),
            result: Some(result),
            error: None,
        }
    }

    pub fn error(id: Option<serde_json::Value>, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }
}

/// Connection ID type
pub type ConnectionId = u64;

/// Shared terminal session manager — wraps `syncode_terminal::SessionManager`
/// behind a `RwLock` (mirrors the Tauri `SharedSessionManager` pattern in
/// `crates/syncode-tauri/src/terminal_commands.rs`). The `terminal.*` RPC
/// handlers (T6c-5) read/write sessions through this handle.
pub type SharedSessionManager = Arc<RwLock<syncode_terminal::SessionManager>>;

/// Shared state for the WebSocket server
#[derive(Clone)]
pub struct WsState {
    pub connections: Arc<RwLock<HashMap<ConnectionId, mpsc::UnboundedSender<String>>>>,
    pub push_tx: broadcast::Sender<(String, serde_json::Value)>, // (channel, data)
    pub next_connection_id: Arc<std::sync::atomic::AtomicU64>,
    /// Read model store (shared across connections via Arc)
    pub read_store: Arc<tokio::sync::RwLock<syncode_orchestration::ReadModelStore>>,
    /// Orchestrator for the CQRS pipeline (command → event → persist → project)
    pub orchestrator: Arc<syncode_orchestration::Orchestrator>,
    /// Per-connection channel subscriptions. The delivery loop consults this to
    /// decide which push broadcasts to forward to each connection (opt-in: a
    /// connection receives nothing until it subscribes via `push/subscribe`).
    pub subscriptions: Arc<RwLock<crate::push::SubscriptionRegistry>>,
    /// Auth configuration: mode (no-auth vs remote-reachable) + authenticator +
    /// shared session registry. Defaults to `UnsafeNoAuth` (backward compat).
    pub auth_config: syncode_auth::WsAuthConfig,
    /// Per-connection authenticated principals. Populated by `auth/bootstrap`,
    /// consulted by the authz gate on every protected method call.
    pub conn_auth: crate::auth::SharedConnectionAuth,
    /// Pairing-link store (AUTH-1). Backs the `auth.createPairingCredential` /
    /// `auth.revokePairingLink` / `auth.listPairingLinks` RPCs. Defaults to an
    /// in-memory store (mirrors the `UnsafeNoAuth` opt-in posture — pairing is
    /// only persisted across restarts when the operator wires a SQLite store
    /// via [`WsState::with_pairing_links`]). All three RPCs require Write
    /// permission; in non-requiring modes the authz gate is bypassed, so this
    /// store is the single source of truth for who may bootstrap via a pairing
    /// credential regardless of auth mode.
    pub pairing_links: std::sync::Arc<dyn syncode_auth::pairing::PairingLinkStore>,
    /// Terminal PTY session manager (T6c-5). Owns the lifecycle of all
    /// terminal sessions created via `terminal.open`/`terminal.new`. Sessions
    /// are keyed by the caller-provided `terminalId` (MCode convention) so the
    /// UI's session references stay stable across calls.
    pub terminal_manager: SharedSessionManager,
    /// Per-session output-reader task handles (T6c-11). Each live terminal
    /// session has a dedicated tokio task that polls the PTY for new output
    /// (via `spawn_blocking`) and broadcasts `terminal/event` push frames onto
    /// `push_tx`. The handle is retained so `terminal.close`/`destroy` can
    /// abort the reader — otherwise the blocking `read` would outlive the
    /// session and leak a thread. Keyed by session id (the same key the
    /// `terminal_manager` uses).
    pub terminal_readers: Arc<tokio::sync::Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    /// Automation scheduler (T6c-6). Backs the `automation.*` RPC handlers
    /// (list/create/get/update/delete/runNow/cancelRun) — manages automation
    /// definition + run-record lifecycle (mirrors the terminal_manager wiring).
    /// Subscribe/unsubscribe register/deregister the calling connection on the
    /// `automation` push channel, and runNow/cancelRun broadcast
    /// `run-upserted` lifecycle events on `push_tx` (T6c-21).
    pub automation_scheduler: Arc<syncode_automation::Scheduler>,
    /// Provider adapter registry (T6c-13). Backs the LLM-backed RPCs
    /// (`provider.compactThread`, `git.summarizeDiff`,
    /// `server.generateThreadRecap`) — each one-shot op resolves a provider
    /// adapter by id from this registry, spawns it, and runs a single prompt.
    /// Starts empty in `new_in_memory` (tests register a `MockLlmAdapter`);
    /// production deployments populate it from config (claude/codex/gemini/…).
    pub provider_registry: Arc<RwLock<syncode_provider::registry::ProviderRegistry>>,
    /// In-memory server settings (T6c-18 + SRV-1). Persists `ServerConfig` +
    /// `ServerSettings` edits for the server session — the `server.*` write
    /// RPCs (`setConfig`/`updateSettings`/`patchSettings`/`updateProvider`/
    /// `upsertKeybinding`) merge into this store, the read RPCs (`getConfig`/
    /// `getSettings`) return from it, and writes broadcast push events on
    /// `push_tx` so subscribed connections receive the new state.
    ///
    /// SRV-1: when a SQLite pool is attached (via `attach_pool` after
    /// construction — the server binary does this in `build_state`), every
    /// mutation write-throughs to the `server_config` / `server_settings`
    /// tables so edits survive a restart. Without a pool (the `new_in_memory`
    /// path and tests) the store is purely in-memory (backward-compatible).
    pub settings: Arc<RwLock<crate::settings::ServerSettingsState>>,
    /// Provider token-usage log (T6c-19). Append-only record of every
    /// successful provider round trip (input/output/total tokens + provider
    /// id + model + timestamp). The `server.listProviderUsage` and
    /// `server.getProviderUsageSnapshot` RPCs aggregate over this log. The
    /// `invoke()` one-shot helper records into it whenever a provider
    /// response carries token-usage metadata. In-memory only (rebuilt from
    /// empty on each server start — mirrors the settings store's gap).
    pub usage: Arc<RwLock<crate::usage::UsageStore>>,
    /// Local-server process manager (T6c-phase-24). Backs the
    /// `server.startLocalServer` / `server.stopLocalServer` RPCs — spawns,
    /// tracks, and kills long-running server processes (e.g. `ollama serve`,
    /// LM Studio, any configurable command) via `tokio::process::Command`.
    /// Processes are keyed by an assigned server id; `start` records the
    /// child + pid, `stop` kills + removes the entry. Mirrors the
    /// `terminal_manager` wiring (Arc<RwLock<…>> shared across connections).
    pub local_servers: Arc<RwLock<crate::local_server::LocalServerManager>>,
    /// Dev-server id registry (PROJ-4). Tracks which `local_servers` ids are
    /// dev servers (started via `project.startDevServer`) so `project.
    /// listDevServers` can filter `LocalServerManager::list()` down to just the
    /// dev-server entries. The `LocalServerManager` itself has no tagging
    /// surface, so this sidecar set is the source of truth for the
    /// dev-server/non-dev-server distinction. Mirrors the `local_servers`
    /// wiring (Arc<RwLock<…>> shared across connections). An id is inserted on
    /// `startDevServer` and removed on `stopDevServer` (or implicitly dropped
    /// when the manager reports it no longer tracks it — `listDevServers`
    /// intersects the set with `list()`).
    pub dev_servers: Arc<RwLock<std::collections::HashSet<String>>>,
    /// Server start instant (T6c-phase-26). Captured in `new_with_auth` and
    /// consulted by `server.getDiagnostics` to report a real
    /// `process.uptimeSeconds` (elapsed since start). Monotonic — not wall
    /// clock, so immune to NTP skew.
    pub started_at: std::time::Instant,
}

impl WsState {
    /// Create a new WsState with an Orchestrator, in **no-auth** mode.
    ///
    /// This is the backward-compatible default: existing local-first and test
    /// deployments run without authentication. Callers building a
    /// network-reachable server should use [`WsState::new_with_auth`] instead.
    ///
    /// The orchestrator's read model is shared with the WsState so RPC handlers
    /// can read the latest projected state.
    pub fn new(push_capacity: usize, orchestrator: syncode_orchestration::Orchestrator) -> Self {
        Self::new_with_auth(
            push_capacity,
            orchestrator,
            syncode_auth::WsAuthConfig::no_auth(),
        )
    }

    /// Create a new WsState with an explicit auth configuration.
    ///
    /// Pass [`syncode_auth::WsAuthConfig::no_auth()`] for local/dev (the
    /// historical behavior), or [`syncode_auth::WsAuthConfig::remote`] to
    /// require authentication on every connection before dispatch.
    pub fn new_with_auth(
        push_capacity: usize,
        orchestrator: syncode_orchestration::Orchestrator,
        auth_config: syncode_auth::WsAuthConfig,
    ) -> Self {
        let (push_tx, _) = broadcast::channel(push_capacity);

        // Fan-out domain-event publisher: every published event lands on the
        // WS push bus (for browser clients) AND on a typed broadcast channel
        // (for in-process consumers — the AutomationRunReactor subscribes to
        // reconcile run status from turn outcomes). Both fan-outs are
        // best-effort (receiver-less is normal before any subscriber joins).
        let typed_event_tx = broadcast::channel::<syncode_core::DomainEvent>(push_capacity).0;
        let publisher =
            crate::push::FanoutDomainEventPublisher::new(push_tx.clone(), typed_event_tx.clone());
        let orchestrator = orchestrator.with_event_publisher(Arc::new(publisher));

        // Wrap the orchestrator in its Arc BEFORE building the automation
        // scheduler — the scheduler's OrchestrationRunExecutor needs a clone
        // of the Arc so each automation run can drive ApplicationService.
        let orchestrator_arc = Arc::new(orchestrator);
        let read_store = orchestrator_arc.read_model_ref();

        // Automation scheduler: dispatches each run through the chat pipeline
        // via OrchestrationRunExecutor (ApplicationService → provider adapter
        // → stream → TurnCompleted). AI-completion harness armed when the
        // default provider is armable. See [`crate::completion`].
        let automation_scheduler =
            crate::completion::build_automation_scheduler(orchestrator_arc.clone());

        // Spawn the AutomationRunReactor: subscribes to orchestration domain
        // events via the typed bus and reconciles run status from real turn
        // outcomes (running → succeeded/failed/interrupted). Mirrors MCode's
        // `AutomationRunReactorLive` layer — a host spawns it at boot so
        // automation runs transition out of `Running` based on the actual
        // turn result, not just the executor's exit code. Detached (runs
        // until the typed bus sender is dropped on WsState drop).
        spawn_automation_run_reactor(typed_event_tx.clone(), automation_scheduler.clone());

        // Usage log + the reactor that records chat-turn token usage into it
        // (subscribes to TurnCompleted on the typed bus). Created here as a
        // local so the reactor can clone it before WsState is finished.
        let usage: Arc<RwLock<crate::usage::UsageStore>> =
            Arc::new(RwLock::new(crate::usage::UsageStore::new()));
        spawn_usage_reactor(typed_event_tx.clone(), read_store.clone(), usage.clone());

        // Capture the auth mode as a kebab-case string for the in-memory
        // server-config store (the `authMode` field is surfaced to the UI as
        // an informational extra field — it's not part of the MCode schema,
        // but harmless and useful). `AuthMode` serializes kebab-case
        // (`unsafe-no-auth` | `remote-reachable` | …); fall back to the
        // no-auth default if serialization ever fails (defensive — shouldn't
        // happen for a unit enum).
        let auth_mode = serde_json::to_value(auth_config.mode)
            .ok()
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| "unsafe-no-auth".to_string());
        let settings = Arc::new(RwLock::new(crate::settings::ServerSettingsState::new(
            auth_mode,
        )));
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            push_tx,
            next_connection_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            read_store,
            orchestrator: orchestrator_arc,
            subscriptions: Arc::new(RwLock::new(crate::push::SubscriptionRegistry::new())),
            auth_config,
            conn_auth: crate::auth::SharedConnectionAuth::new(),
            // Pairing links default to in-memory (no persistence across
            // restarts). Operators wanting survival across a restart call
            // `with_pairing_links` with a `SqlitePairingLinkStore`.
            pairing_links: std::sync::Arc::new(
                syncode_auth::pairing::InMemoryPairingLinkStore::new(),
            ),
            terminal_manager: Arc::new(RwLock::new(syncode_terminal::SessionManager::new())),
            terminal_readers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            automation_scheduler,
            provider_registry: Arc::new(RwLock::new(
                syncode_provider::registry::ProviderRegistry::new(),
            )),
            settings,
            usage,
            local_servers: Arc::new(RwLock::new(crate::local_server::LocalServerManager::new())),
            dev_servers: Arc::new(RwLock::new(std::collections::HashSet::new())),
            started_at: std::time::Instant::now(),
        }
    }

    /// Override the pairing-link store (AUTH-1).
    ///
    /// The default (set by [`WsState::new_with_auth`]) is an in-memory store
    /// that does NOT survive a restart. Operators wanting pairing links to
    /// persist across restarts construct a
    /// [`syncode_auth::pairing::SqlitePairingLinkStore`] from the server's
    /// SQLite pool and pass it here.
    pub fn with_pairing_links(
        mut self,
        store: std::sync::Arc<dyn syncode_auth::pairing::PairingLinkStore>,
    ) -> Self {
        self.pairing_links = store;
        self
    }

    /// Create a simple state without persistence (in-memory only, for tests).
    /// Uses an in-memory event repository.
    pub fn new_in_memory(push_capacity: usize) -> Self {
        use syncode_core::ports::EventRepository;

        // In-memory event repo for testing
        struct InMemoryRepo {
            events:
                std::sync::Mutex<std::collections::HashMap<String, Vec<syncode_core::Envelope>>>,
            snapshots:
                std::sync::Mutex<std::collections::HashMap<String, (serde_json::Value, u64)>>,
        }
        impl InMemoryRepo {
            fn new() -> Self {
                Self {
                    events: std::sync::Mutex::new(std::collections::HashMap::new()),
                    snapshots: std::sync::Mutex::new(std::collections::HashMap::new()),
                }
            }
        }
        #[async_trait::async_trait]
        impl EventRepository for InMemoryRepo {
            async fn append_events(
                &self,
                aggregate_id: syncode_core::EntityId,
                events: Vec<syncode_core::DomainEvent>,
                expected_version: u64,
            ) -> Result<u64, syncode_core::PortError> {
                use syncode_core::Envelope;
                let mut store = self.events.lock().unwrap();
                let key = aggregate_id.to_string();
                let current = store.get(&key).map(|v| v.len() as u64).unwrap_or(0);
                if current != expected_version {
                    return Err(syncode_core::PortError::ConcurrencyConflict {
                        expected: expected_version,
                        actual: current,
                    });
                }
                let entry = store.entry(key).or_default();
                for (i, event) in events.into_iter().enumerate() {
                    entry.push(Envelope::new(event, current + 1 + i as u64));
                }
                Ok(entry.len() as u64)
            }
            async fn replay_events(
                &self,
                aggregate_id: syncode_core::EntityId,
            ) -> Result<Vec<syncode_core::Envelope>, syncode_core::PortError> {
                let store = self.events.lock().unwrap();
                Ok(store
                    .get(&aggregate_id.to_string())
                    .cloned()
                    .unwrap_or_default())
            }
            async fn load_snapshot(
                &self,
                aggregate_id: syncode_core::EntityId,
            ) -> Result<Option<(serde_json::Value, u64)>, syncode_core::PortError> {
                Ok(self
                    .snapshots
                    .lock()
                    .unwrap()
                    .get(&aggregate_id.to_string())
                    .cloned())
            }
            async fn save_snapshot(
                &self,
                aggregate_id: syncode_core::EntityId,
                state: serde_json::Value,
                version: u64,
            ) -> Result<(), syncode_core::PortError> {
                self.snapshots
                    .lock()
                    .unwrap()
                    .insert(aggregate_id.to_string(), (state, version));
                Ok(())
            }
            async fn load_all_snapshots(
                &self,
            ) -> Result<
                Vec<(syncode_core::EntityId, serde_json::Value, u64)>,
                syncode_core::PortError,
            > {
                let snapshots = self.snapshots.lock().unwrap();
                let mut out = Vec::with_capacity(snapshots.len());
                for (id_str, (state, version)) in snapshots.iter() {
                    let id = syncode_core::EntityId::parse(id_str).map_err(|e| {
                        syncode_core::PortError::Internal(format!("invalid aggregate_id: {e}"))
                    })?;
                    out.push((id, state.clone(), *version));
                }
                Ok(out)
            }
            async fn replay_all_events(
                &self,
                _: Option<u64>,
                _: u32,
            ) -> Result<Vec<syncode_core::Envelope>, syncode_core::PortError> {
                let store = self.events.lock().unwrap();
                let mut all: Vec<syncode_core::Envelope> =
                    store.values().flatten().cloned().collect();
                all.sort_by_key(|e| e.sequence);
                Ok(all)
            }
            async fn current_version(
                &self,
                aggregate_id: syncode_core::EntityId,
            ) -> Result<u64, syncode_core::PortError> {
                let store = self.events.lock().unwrap();
                Ok(store
                    .get(&aggregate_id.to_string())
                    .map(|v| v.len() as u64)
                    .unwrap_or(0))
            }
        }

        let repo: Arc<dyn EventRepository> = Arc::new(InMemoryRepo::new());
        let orchestrator = syncode_orchestration::Orchestrator::new(repo);
        Self::new(push_capacity, orchestrator)
    }

    pub fn next_id(&self) -> ConnectionId {
        self.next_connection_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn register(&self, id: ConnectionId, tx: mpsc::UnboundedSender<String>) {
        self.connections.write().await.insert(id, tx);
        self.subscriptions.write().await.register(id);
        tracing::info!(connection_id = id, "WebSocket client connected");
    }

    pub async fn unregister(&self, id: ConnectionId) {
        self.connections.write().await.remove(&id);
        self.subscriptions.write().await.unregister(id);
        tracing::info!(connection_id = id, "WebSocket client disconnected");
    }
}

// ─── Automation run reactor boot wiring ─────────────────────────────────────

/// Spawn the [`AutomationRunReactor`] as a detached Tokio task that runs until
/// the typed-domain-event broadcast sender is dropped (i.e. until [`WsState`]
/// itself is dropped).
///
/// The reactor subscribes to orchestration domain events via the typed bus and
/// reconciles automation run status from real turn outcomes — mirroring MCode's
/// `AutomationRunReactorLive` layer. Without this, automation runs only reach
/// `Completed` via the executor's own dispatch-return path (which works for
/// synchronous adapters like claude, but leaves async/ACP adapter runs stuck in
/// `Running`). With it, every domain event in the lifecycle set
/// (`TurnDiffCompleted`, `TurnFailed`, `TurnInterrupted`,
/// `ThreadApprovalResponded`, …) drives a run-status transition.
///
/// [`AutomationRunReactor`]: syncode_automation::run_reactor::AutomationRunReactor
fn spawn_automation_run_reactor(
    typed_event_tx: broadcast::Sender<syncode_core::DomainEvent>,
    scheduler: Arc<syncode_automation::Scheduler>,
) {
    use syncode_automation::run_reactor::{AutomationRunReactor, BroadcastDomainEventStream};

    let stream = Arc::new(BroadcastDomainEventStream::new(typed_event_tx.subscribe()));
    let reactor = Arc::new(AutomationRunReactor::new(stream, scheduler));
    tokio::spawn(async move {
        reactor.run().await;
    });
    tracing::info!("automation run reactor spawned (event-driven run-status reconciliation)");
}

/// Spawn a task that records chat-turn token usage into the [`UsageStore`].
///
/// Subscribes to the typed domain-event bus; on each `TurnCompleted` carrying
/// provider `usage`, it resolves the turn → thread → provider/model from the
/// read model and appends a [`UsageEntry`]. This is what makes `server/getUsage`
/// reflect real chat turns — without it only the LLM-op path
/// (`invoke_llm_oneshot`) records usage, so the settings → usage panel looks
/// empty after chatting.
///
/// Detached (runs until the typed bus sender is dropped on `WsState` drop).
fn spawn_usage_reactor(
    typed_event_tx: broadcast::Sender<syncode_core::DomainEvent>,
    read_store: Arc<tokio::sync::RwLock<syncode_orchestration::ReadModelStore>>,
    usage: Arc<RwLock<crate::usage::UsageStore>>,
) {
    let mut rx = typed_event_tx.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            let syncode_core::DomainEvent::TurnCompleted {
                id, usage: Some(u), ..
            } = event
            else {
                continue;
            };
            // Resolve turn → thread → provider/model from the read model. The
            // thread + turn exist before TurnCompleted (ThreadCreated /
            // TurnStarted project first), so the lookup is reliable.
            let (provider_id, model) = {
                let store = read_store.read().await;
                let thread = store
                    .turns
                    .get(id.as_str().as_str())
                    .and_then(|t| store.threads.get(&t.thread_id));
                let Some(thread) = thread else {
                    continue;
                };
                (thread.provider_id.clone(), thread.model.clone())
            };
            if provider_id.is_empty() {
                continue;
            }
            usage.write().await.record(crate::usage::UsageEntry {
                provider_id,
                model,
                input_tokens: u.input_tokens,
                output_tokens: u.output_tokens,
                total_tokens: u.total_tokens,
                timestamp: chrono::Utc::now(),
            });
        }
    });
    tracing::info!("usage reactor spawned (records chat-turn usage into the usage log)");
}

/// JSON-RPC standard error codes
pub mod error_codes {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;
}

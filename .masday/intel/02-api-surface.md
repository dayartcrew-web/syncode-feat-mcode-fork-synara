# 02 — API Surface

The system exposes three API layers: **WebSocket JSON-RPC** (runtime), **Tauri IPC commands** (desktop), and **Rust trait surfaces** (ports + provider adapter).

## 1. WebSocket JSON-RPC (`syncode-ws`, the primary runtime API)
Axum WS server at `GET /ws`. Each connection: unique `ConnectionId`, unbounded mpsc response sender, subscribes to a `push_tx` broadcast. `rpc.rs::dispatch_method` handles ~16 methods:

| Method | Maps to | Notes |
|--------|---------|-------|
| `ping` | — | health |
| `rpc/listMethods` | — | introspection |
| `project/create` | `Command::CreateProject` | write → CQRS pipeline |
| `project/list` | `read_store` | read |
| `thread/create` | `Command::CreateThread` | write |
| `thread/list` | `read_store` | read |
| `thread/pause`,`resume`,`cancel` | `PauseThread`/`ResumeThread`/`CancelThread` | write |
| `turn/start` | `Command::StartTurn` | write + provider side effect |
| `turn/complete` | `Command::CompleteTurn` | write |
| `push/subscribe`,`unsubscribe` | `ChannelSubscription` | channel mgmt |

(Exact names live in `ws/src/rpc.rs` dispatch match.) Read ops served from `WsState.read_store`; write ops call `orchestrator.handle_command`, then return the updated entity.

**Push channels** (`channels.rs`): `orchestration`, `provider`, `git`, `terminal`, `automation`, `*` (all). `PushEvent` variants: `DomainEvent`, `ProviderStatus`, `Progress`, `TerminalOutput`, `Custom`. Delivered as JSON-RPC **notifications** (no `id`) to subscribed connections. Best-effort (no retry).

## 2. Tauri IPC commands (`syncode-tauri`, desktop shell)
Registered in `main.rs:14-21`. **Core (6):** `get_app_info`, `get_version`, `list_providers`, `get_provider_status`, `list_sessions`, `create_session`. **Git (8):** `git_status`, `git_diff`, `git_log`, `git_branches`, `git_add`, `git_commit`, `git_create_branch`, `git_delete_branch`, `git_checkout`. **Terminal (~7):** `terminal_create_session`, `terminal_list_sessions`, `terminal_destroy_session`, `terminal_resize`, `terminal_write`, `terminal_read_output`, `terminal_ack`.

> ⚠️ No Tauri commands expose orchestration (project/thread/turn). The desktop shell does **not** wire the ws server or the engine — `ProviderRegistryState` hardcodes 8 providers; `SessionStoreState` is an in-memory `Vec`.

## 3. Rust trait surfaces

### Port traits (`syncode-core/src/ports/mod.rs`, async_trait, Send+Sync)
- **`EventRepository`** — `append_events(agg_id, events, expected_version) -> u64`, `replay_events(agg_id) -> Vec<Envelope>`, `load_snapshot` / `save_snapshot`, `replay_all_events(since, limit)`, `current_version(agg_id)`.
- **`ReadModelRepository`** — `refresh_projections() -> u32`; `list_projects`/`get_project`; `list_threads`/`get_thread`; `list_turns`/`get_turn`; `list_messages`; `list_activities(project?, thread?)`. (All return `serde_json::Value`.)
- **`GitServicePort`** — `status`, `create_checkpoint`, `diff`, `list_modified_files`, `is_valid_repo`. *(Not implemented by syncode-git — see gap above.)*
- **`ProviderPort`** — `start_session`, `send_to_session`, `interrupt_session`, `stop_session`, `health_check`, `list_models`.
- **`PortError`** — `NotFound(String)`, `ConcurrencyConflict{expected,actual}`, `Internal(String)`.

### `ProviderAdapter` trait (`syncode-provider/src/trait_def.rs:237-302`)
Identity: `provider_id`, `capabilities`, `status`, `available_models`. Lifecycle: `spawn(ProviderConfig)`, `shutdown`, `interrupt(session_id)`. Sessions: `start_session(SessionContext) -> session_id`, `resume_session`, `stop_session`. Comms: `send_request(ProviderRequest) -> ProviderResponse` (JSON-RPC 2.0), `event_stream(session_id) -> ProviderStream` (`Pin<Box<dyn Stream<Item=Result<ProviderEvent,ProviderAdapterError>>>>`). `health_check`. Error: `ProviderAdapterError` (11 variants).

### `ApplicationService` (`syncode-orchestration/src/use_cases.rs`, 24 methods)
16 commands: `create_project`, `update_project_config`, `create_thread`, `pause_thread`, `resume_thread`, `cancel_thread`, `complete_thread`, `set_thread_title`, `start_turn`, `complete_turn`, `fail_turn`, `cancel_turn`, `record_turn_files`, `set_turn_checkpoint`, `add_message` (+1). 8 queries: `list_projects`, `get_project`, `list_threads`, `get_thread`, `list_turns`, `get_turn`, `get_project_dashboard`, `get_thread_detail`.

## 4. Shared DTOs (`syncode-contracts`)
ts-rs generates `frontend/src/types/*.ts` from `#[derive(TS)]` types: `EntityId`, `Timestamp`, `ProviderConfig`, `ProviderCapabilities`, `CreateSessionRequest`, `SessionView`, `SessionStatus`, `MessageView`, `MessageRole`, `GitFileStatusView`, `FileStatusKind`, `GitStatusView`, `JsonRpcRequestView`, `JsonRpcResponseView`, `JsonRpcErrorView`, `PushEvent`. (Compare: MCode's Effect-Schema contracts add runtime validation; these are pure DTOs.)

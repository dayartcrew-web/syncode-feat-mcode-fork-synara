# syncode-ws
> WebSocket JSON-RPC 2.0 server — the primary runtime API into the CQRS engine. **L3** · 3007 LOC · 47 tests
- **Depends on (internal):** `core`, `contracts`, `orchestration`, `persistence`, `auth`.
- **External:** axum 0.8 (ws), tokio, tokio-tungstenite, serde, futures-util.

## Files
- `lib.rs` (~215 LOC) — `JsonRpcRequest/Response/Error`, `WsState` (carries `auth_config` + `conn_auth`), `ConnectionId`, error codes. Constructors: `new` (no-auth default), `new_with_auth` (opt-in auth), `new_in_memory` (tests).
- `rpc.rs` (~680 LOC) — `handle_rpc` (with **authz gate**), `dispatch_method` (~19 methods incl. `auth/*`), per-method handlers. `handle_push_subscribe` now emits a snapshot after recording the subscription (snapshot-then-stream).
- `server.rs` (~115 LOC) — axum WS upgrade + connection lifecycle; `build_ws_router()` (`/ws`), `run_push_delivery`.
- `channels.rs` (148 LOC) — `ChannelSubscription` (6 channels).
- `push.rs` (~380 LOC) — `PushEvent`, `SubscriptionRegistry`, `WsDomainEventPublisher`, `deliver_push_event`, **snapshot-then-stream** (`emit_snapshot` + view→DTO mapping helpers: `project_summary`/`thread_summary`/`turn_summary`/`message_summary`/`activity_summary` + `build_snapshot`).
- `auth.rs` — **method→permission mapping**, `ConnectionAuth`/`SharedConnectionAuth` (per-connection principals), `AuthzOutcome`, `authorize()`, `bootstrap()`, `auth_error_codes` (`UNAUTHORIZED=-32001`, `FORBIDDEN=-32003`).
- `transport.rs` — architectural note (reframed; see below).

## Public API / flow
- `WsState { connections, push_tx, orchestrator, read_store, subscriptions, auth_config, conn_auth }`.
- `handle_connection`: register → subscribe to push → RPC loop (each request passes the authz gate) → cleanup.
- **Authz gate** (`rpc::handle_rpc`): before dispatch, `conn_auth.authorize(auth_config, conn_id, method)` returns Allow/Unauthorized/Forbidden. Public methods (ping, rpc/listMethods, auth/*) bypass; read methods need `Read`; write methods need `Write`. Non-requiring mode → always Allow.
- **Auth RPC methods:** `auth/bootstrap` (credential→session token+role+subject+expiry, binds principal to conn), `auth/status` (current auth state), `auth/logout` (clears conn principal).
- **Snapshot-then-stream** (`push::emit_snapshot` + `rpc::handle_push_subscribe`): on `push/subscribe`, the server records the subscription then builds a snapshot of the channel's current read-model state and sends it to the connection's `tx` as a `push/<channel>` notification with `event_type: "snapshot"`. Ordering is race-free (subscribe → snapshot → live). `orchestration` channel → `ShellSnapshot` (projects+threads) by default, or `ThreadDetailSnapshot` (one thread) when `threadId` is passed; `*` wildcard → `FullSnapshot`. A reconnecting client re-subscribes to re-hydrate (the client owns backoff).
- **6 channels:** `orchestration`, `provider`, `git`, `terminal`, `automation`, `*`. Push delivered as JSON-RPC notifications, best-effort, subscription-filtered.

## Stubs / risks
- **`transport.rs` reframed** — the earlier "connection state machine (connecting/connected/reconnecting)" TODO was misleading: those states are client-side in MCode. The server is stateless-per-upgrade; reconnect is client-owned; the server's obligation (snapshot-on-subscribe) is in `push.rs`/`rpc.rs`. See the module doc.
- **Auth defaults to `UnsafeNoAuth`** (backward-compat) — production deployments must opt in via `new_with_auth(.., WsAuthConfig::remote(..))`.
- **No sliding-window backpressure / drop→resync** (MCode has capacity-1024 sliding buffer that signals the client to re-subscribe) — separate follow-up from snapshot-then-stream.
- No rate limiting; auth sessions in-memory (lost on restart).
- Tight coupling: `Arc<Orchestrator>` + `Command` import means orchestration changes ripple here directly.

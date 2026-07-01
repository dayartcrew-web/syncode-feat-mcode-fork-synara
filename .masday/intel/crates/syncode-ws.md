# syncode-ws
> WebSocket JSON-RPC 2.0 server — the primary runtime API into the CQRS engine. **L3** · ~1620 LOC · 37 tests
- **Depends on (internal):** `core`, `orchestration`, `persistence`, `auth`.
- **External:** axum 0.8 (ws), tokio, tokio-tungstenite, serde, futures-util.

## Files
- `lib.rs` (~215 LOC) — `JsonRpcRequest/Response/Error`, `WsState` (now carries `auth_config` + `conn_auth`), `ConnectionId`, error codes. Constructors: `new` (no-auth default), `new_with_auth` (opt-in auth), `new_in_memory` (tests).
- `rpc.rs` (~640 LOC) — `handle_rpc` (with **authz gate**), `dispatch_method` (~19 methods incl. `auth/*`), per-method handlers.
- `server.rs` (~115 LOC) — axum WS upgrade + connection lifecycle; `build_ws_router()` (`/ws`), `run_push_delivery`.
- `channels.rs` (148 LOC) — `ChannelSubscription` (6 channels).
- `push.rs` (222 LOC) — `PushEvent`, `SubscriptionRegistry`, `WsDomainEventPublisher`, `deliver_push_event`.
- `auth.rs` — **method→permission mapping**, `ConnectionAuth`/`SharedConnectionAuth` (per-connection principals), `AuthzOutcome`, `authorize()`, `bootstrap()`, `auth_error_codes` (`UNAUTHORIZED=-32001`, `FORBIDDEN=-32003`).
- `transport.rs` (3 LOC) — **stub** (connection state machine, Phase 0.4 TODO).

## Public API / flow
- `WsState { connections, push_tx, orchestrator, read_store, subscriptions, auth_config, conn_auth }`.
- `handle_connection`: register → subscribe to push → RPC loop (each request passes the authz gate) → cleanup.
- **Authz gate** (`rpc::handle_rpc`): before dispatch, `conn_auth.authorize(auth_config, conn_id, method)` returns Allow/Unauthorized/Forbidden. Public methods (ping, rpc/listMethods, auth/*) bypass; read methods need `Read`; write methods need `Write`. Non-requiring mode → always Allow.
- **Auth RPC methods:** `auth/bootstrap` (credential→session token+role+subject+expiry, binds principal to conn), `auth/status` (current auth state), `auth/logout` (clears conn principal).
- **6 channels:** `orchestration`, `provider`, `git`, `terminal`, `automation`, `*`. Push delivered as JSON-RPC notifications, best-effort, subscription-filtered.

## Stubs / risks
- **`transport.rs` is a 3-LOC stub** — connection state machine (connecting/connected/reconnecting) not implemented; no reconnect/rehydration.
- **Auth defaults to `UnsafeNoAuth`** (backward-compat) — production deployments must opt in via `new_with_auth(.., WsAuthConfig::remote(..))`.
- **No rate limiting / backpressure**; push delivery best-effort (failures logged, not retried); auth sessions in-memory (lost on restart).
- Tight coupling: `Arc<Orchestrator>` + `Command` import means orchestration changes ripple here directly.

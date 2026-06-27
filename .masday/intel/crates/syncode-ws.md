# syncode-ws
> WebSocket JSON-RPC 2.0 server — the primary runtime API into the CQRS engine. **L3** · 1188 LOC · 14 tests
- **Depends on (internal):** `core`, `orchestration`, `persistence`.
- **External:** axum 0.8 (ws), tokio, tokio-tungstenite, serde, futures-util.

## Files
- `lib.rs` (181 LOC) — `JsonRpcRequest/Response/Error`, `WsState`, `ConnectionId`, error codes.
- `rpc.rs` (545 LOC) — `handle_rpc` + `dispatch_method` (~16 methods).
- `server.rs` (88 LOC) — axum WS upgrade + connection lifecycle; `build_ws_router()` (`/ws`).
- `channels.rs` (149 LOC) — `ChannelSubscription` (6 channels).
- `push.rs` (222 LOC) — `PushEvent`, `SubscriptionRegistry`, `deliver_push_event`.
- `transport.rs` (3 LOC) — **stub** (connection state machine, Phase 0.4 TODO).

## Public API / flow
- `WsState { connections: HashMap, push_tx: broadcast, orchestrator: Arc<Orchestrator>, read_store }` (`lib.rs:77-85`).
- `handle_connection`: register → subscribe to push → RPC loop → cleanup.
- **RPC dispatch:** reads served from `read_store`; writes call `orchestrator.handle_command` then return the updated entity. Constructs `Command` variants directly (`rpc.rs:9` import).
- **6 channels:** `orchestration`, `provider`, `git`, `terminal`, `automation`, `*`. `PushEvent` variants: DomainEvent / ProviderStatus / Progress / TerminalOutput / Custom — delivered as JSON-RPC notifications (no id), best-effort.

## Stubs / risks
- **`transport.rs` is a 3-LOC stub** — connection state machine (connecting/connected/reconnecting) not implemented; no reconnect/rehydration.
- **No auth/authorization** — any WS connection can dispatch commands.
- **No rate limiting / backpressure**; push delivery best-effort (failures logged, not retried).
- Tight coupling: `Arc<Orchestrator>` + `Command` import means orchestration changes ripple here directly (`lib.rs:84,92`; `rpc.rs:9`).

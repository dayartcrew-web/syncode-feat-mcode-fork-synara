# WebSocket Transport

`syncode-ws` implements the primary bidirectional transport: a JSON-RPC 2.0
server over WebSocket, method dispatch to orchestration commands, a push bus
for server-initiated events, channel management, and a connection state machine.

## Modules

| Module | Purpose |
|--------|---------|
| `server` | WebSocket accept loop, per-connection task spawning, shutdown signal |
| `transport` | Frame read/write, ping/pong keep-alive, close handling |
| `rpc` | JSON-RPC 2.0 request/response dispatch — routes method names to handlers |
| `auth` | Auth middleware — validates `AuthenticatedSession` before dispatch |
| `channels` | Named channels for multiplexed push subscriptions |
| `push` | `PushBus` — fan-out server-initiated events to subscribed connections |
| `error_codes` | JSON-RPC error code constants |

## Key types

| Type | Description |
|------|-------------|
| `WsState` | Top-level wiring: orchestrator, read model store, push bus, auth config, connection map |
| `JsonRpcRequest` | Incoming request (id, method, params) |
| `JsonRpcResponse` | Outgoing response (id, result or error) |
| `JsonRpcError` | Structured error (code, message, data) |
| `ConnectionId` | Unique per-connection identifier |
| `PushEvent` | Server-initiated event payload (typed via `syncode-contracts`) |

## RPC method surface

| Method | Description |
|--------|-------------|
| `project/list` | List all projects |
| `project/create` | Create a new project |
| `thread/list` | List threads in a project |
| `session/create` | Create a provider session |
| `session/prompt` | Send a prompt to a session |
| `session/cancel` | Interrupt an in-flight turn |
| `git/status` | Query repository status |
| `terminal/spawn` | Open a PTY session |

## Integration points

- Dispatches commands to `syncode-orchestration::Orchestrator`.
- Reads read models from `syncode-orchestration::ReadModelStore`.
- Authenticates via `syncode-auth::Authenticator`.
- Pushes events to the frontend through the `PushBus`.
- Mounted by `syncode-tauri` at startup on a configurable port.

## Stub status

All modules contain real implementations — no stubs remain.

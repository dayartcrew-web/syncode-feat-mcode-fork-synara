# Contract Types

`syncode-contracts` holds the **Rust → TypeScript bridge types**. Every public
struct annotated with `#[derive(TS)]` (via the `ts-rs` crate) is collected into
a generated `.d.ts` file that the Tauri frontend consumes, guaranteeing that
both sides agree on the wire shape.

## Design rationale

Keeping all cross-boundary types in a dedicated crate prevents circular
dependencies: the frontend-facing DTOs live here, while the domain model lives
in `syncode-core`. The two are mapped by `syncode-orchestration` projectors.

## Type groups

| Group | Example types |
|-------|---------------|
| Primitives | `EntityId`, `Timestamp` |
| Provider | `ProviderConfig`, `ProviderCapabilities` |
| Session | `CreateSessionRequest`, `SessionView`, `SessionStatus` |
| Message | `MessageView`, `MessageRole` |
| Git | `GitFileStatusView`, `FileStatusKind`, `GitStatusView` |
| JSON-RPC | `JsonRpcRequestView`, `JsonRpcResponseView`, `JsonRpcErrorView` |
| Push events | `PushEvent` — SSE / WebSocket push payload |

## Snapshots

The `snapshots` module contains compile-time regression tests: each `#[test]`
asserts that the generated TypeScript output matches a checked-in `.snap`
string. Run `cargo test -p syncode-contracts` to verify.

## Integration points

- `syncode-tauri` IPC commands return these DTOs to the frontend.
- `syncode-ws` JSON-RPC responses are serialized from these types.
- `syncode-orchestration` projectors convert domain aggregates → contract views.

## Stub status

All types are real and actively used by the Tauri frontend.

# syncode-contracts
> Shared DTOs with ts-rs TypeScript codegen (Rust↔frontend bridge). **L0** · 265 LOC · 15 tests · single `lib.rs`
- **Depends on (internal):** none.
- **External:** serde, serde_json, ts-rs 10, uuid, chrono.

## Files
- `src/lib.rs` (265 LOC) — all DTOs in one file.

## Public API (all `#[derive(TS)]` → `frontend/src/types/*.ts`)
- **Primitives:** `EntityId`, `Timestamp` (both `#[serde(transparent)]` wrappers).
- **Provider:** `ProviderConfig`, `ProviderCapabilities`.
- **Session:** `CreateSessionRequest`, `SessionView`, `SessionStatus`.
- **Message:** `MessageView`, `MessageRole`.
- **Git:** `GitFileStatusView`, `FileStatusKind`, `GitStatusView`.
- **JSON-RPC:** `JsonRpcRequestView`, `JsonRpcResponseView`, `JsonRpcErrorView`.
- **WS:** `PushEvent`.

## How it works
`build.rs` sets `TS_RS_EXPORT_DIR = frontend/src/types/`. Test `test_generate_ts_types` (`lib.rs:247-264`) exports the `.ts` definitions. Frontend `types/` dir mirrors these names exactly.

## Stubs / risks
- **Manual TS export list** (`lib.rs:248-263`) — adding a new DTO without appending to the list = silently not exported.
- Pure DTOs, **no runtime validation** (MCode's Effect-Schema contracts carry validation; validation here is deferred to the application layer via garde).

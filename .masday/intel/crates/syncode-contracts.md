# syncode-contracts

> ⚠️ **PRE-CLONE SNAPSHOT (2026-07-02).** This intel is from before the clone+rewire arc (PR #6–#47, 48 PRs total). For the current authoritative state see [`docs/STATUS.md`](../../../docs/STATUS.md).
>
> **Key changes since this snapshot:** Now has RPC DTO module (rpc.rs, 23 request/result structs), events.rs (DomainEventDto 44-variant union + From projection), ~1100 LOC, 96 tests. Tier-3 contract symbols (139) in frontend/src/contracts/tier3/.

> Shared DTOs with ts-rs TypeScript codegen (Rust↔frontend bridge). **L0** · 571 LOC · 34 tests · `lib.rs` + `snapshots.rs`
- **Depends on (internal):** none.
- **External:** serde, serde_json, ts-rs 10, uuid, chrono.
- **Consumed by:** `syncode-ws` (snapshot DTOs for snapshot-then-stream subscriptions).

## Files
- `src/lib.rs` (~280 LOC) — primitives, provider/session/message/git/json-rpc/ws DTOs + barrel export of `snapshots`.
- `src/snapshots.rs` (~240 LOC) — snapshot DTOs for snapshot-then-stream WS subscriptions.
- `build.rs` — sets `TS_RS_EXPORT_DIR = frontend/src/types/`.

## Public API (all `#[derive(TS)]` → `frontend/src/types/*.ts`)
- **Primitives:** `EntityId`, `Timestamp` (both `#[serde(transparent)]` wrappers).
- **Provider:** `ProviderConfig`, `ProviderCapabilities`.
- **Session:** `CreateSessionRequest`, `SessionView`, `SessionStatus`.
- **Message:** `MessageView`, `MessageRole`.
- **Git:** `GitFileStatusView`, `FileStatusKind`, `GitStatusView`.
- **JSON-RPC:** `JsonRpcRequestView`, `JsonRpcResponseView`, `JsonRpcErrorView`.
- **WS:** `PushEvent`.
- **Snapshots** (`snapshots.rs`): `ProjectSummary`, `ThreadSummary`, `TurnSummary`, `MessageSummary`, `ActivitySummary` (slim views faithful to the orchestration read-model), `SnapshotScope` enum, `ShellSnapshot` (projects+threads), `ThreadDetailSnapshot` (thread+turns+messages), `FullSnapshot` (all collections).

## How it works
`build.rs` sets `TS_RS_EXPORT_DIR = frontend/src/types/`. Test `test_generate_ts_types` exports the `.ts` definitions. Frontend `types/` dir mirrors these names exactly. Snapshot DTOs intentionally mirror orchestration read-model field shapes (plain `String` ids/timestamps) so the WS layer maps views → DTOs with trivial field copies.

## Stubs / risks
- **Manual TS export list** — adding a new DTO without appending to `test_generate_ts_types` = silently not exported.
- Pure DTOs, **no runtime validation** (MCode's Effect-Schema contracts carry validation; validation here is deferred to the application layer via garde).

# B1 Tier 1 — RPC Registry + Served-Method DTOs (Progress)

**Task:** `6fe7f072-929e-46e8-b936-125ae1771451` (workflow `fa67c83a-c480-489c-8564-f1039a45a941`)
**Worktree:** `task/b1-rpc-registry-served-dtos` (from master `4f803e7`)
**Status:** DONE — all acceptance criteria met. Awaiting masday review.

## Summary

Implemented Tier 1 (the keystone) of the contracts bridge: typed request/result
DTOs for the **20 served Syncode RPC methods** (the 19 in
`syncode-ws::rpc::dispatch_method` + `ping`; `rpc/listMethods` covered too,
making 21 total entries in `SERVED_RPC`) plus a typed `UNSERVED_RPC` stub list
of **100 MCode RPC method strings** Syncode doesn't serve. The DTOs live in a
new `crates/syncode-contracts/src/rpc.rs` module (23 concrete structs + 8 type
aliases that reuse snapshot summary types), `#[derive(TS)]`, camelCase on both
serde + ts-rs, bigint-safe. A new `frontend/src/contracts/rpc.ts` exports the
typed `SERVED_RPC` registry + `UNSERVED_RPC` stub list. Both barrels
(`contracts/index.ts`, `types/index.ts`) re-export the new types.

## Files modified

- `crates/syncode-contracts/src/lib.rs` — registered `pub mod rpc;`; added 23
  `::export()` calls to `test_generate_ts_types` harness.
- `frontend/src/contracts/index.ts` — re-exported 23 new DTO types + the
  `SERVED_RPC`/`UNSERVED_RPC` registry symbols.
- `frontend/src/types/index.ts` — re-exported the 23 new DTO types.

## Files created

- `crates/syncode-contracts/src/rpc.rs` — Rust DTO mirrors (23 structs + 8
  type aliases) + 19 round-trip/wire-parity tests.
- `frontend/src/contracts/rpc.ts` — typed `SERVED_RPC` (21 entries) +
  `UNSERVED_RPC` (100 entries) + `ServedRpcMethod`/`ServedRpcRequest<M>`/
  `ServedRpcResult<M>`/`UnservedRpcMethod`/`AnyRpcMethod`/`IsServed<M>` types.
- 23 ts-rs-generated `frontend/src/types/*.ts` files (one per concrete DTO
  struct): `AuthBootstrapParams.ts`, `AuthBootstrapResult.ts`,
  `AuthLogoutResult.ts`, `AuthStatusResult.ts`, `ListMethodsResult.ts`,
  `PingResult.ts`, `ProjectCreateParams.ts`, `ProjectGetParams.ts`,
  `ProjectListResult.ts`, `PushSubscribeParams.ts`, `PushSubscribeResult.ts`,
  `PushUnsubscribeParams.ts`, `PushUnsubscribeResult.ts`,
  `ThreadCreateParams.ts`, `ThreadGetParams.ts`, `ThreadLifecycleParams.ts`,
  `ThreadListParams.ts`, `ThreadListResult.ts`, `TurnCompleteParams.ts`,
  `TurnGetParams.ts`, `TurnListParams.ts`, `TurnListResult.ts`,
  `TurnStartParams.ts`.
- `.masday/reports/progress-b1-rpc-registry-served-dtos.md` — this file.

## The 21 served methods + Request/Result type names

| Method | Request type | Result type |
|---|---|---|
| `ping` | `null` (no params) | `PingResult` |
| `rpc/listMethods` | `null` | `ListMethodsResult` |
| `project/list` | `null` | `ProjectListResult` |
| `project/get` | `ProjectGetParams` | `ProjectSummary` (alias) |
| `project/create` | `ProjectCreateParams` | `ProjectSummary` (alias) |
| `thread/list` | `ThreadListParams` | `ThreadListResult` |
| `thread/get` | `ThreadGetParams` | `ThreadSummary` (alias) |
| `thread/create` | `ThreadCreateParams` | `ThreadSummary` (alias) |
| `thread/pause` | `ThreadLifecycleParams` | `ThreadSummary` (alias) |
| `thread/resume` | `ThreadLifecycleParams` | `ThreadSummary` (alias) |
| `thread/cancel` | `ThreadLifecycleParams` | `ThreadSummary` (alias) |
| `turn/list` | `TurnListParams` | `TurnListResult` |
| `turn/get` | `TurnGetParams` | `TurnSummary` (alias) |
| `turn/start` | `TurnStartParams` | `TurnSummary` (alias) |
| `turn/complete` | `TurnCompleteParams` | `TurnSummary` (alias) |
| `auth/bootstrap` | `AuthBootstrapParams` | `AuthBootstrapResult` |
| `auth/status` | `null` | `AuthStatusResult` |
| `auth/logout` | `null` | `AuthLogoutResult` |
| `push/subscribe` | `PushSubscribeParams` | `PushSubscribeResult` |
| `push/unsubscribe` | `PushUnsubscribeParams` | `PushUnsubscribeResult` |

(21 rows; the "19 served methods" in the task brief = the 18 dispatch methods
for project/thread/turn/auth/push + ping mentioned conditionally. The
`rpc/listMethods` self-describing method is also covered.)

## UNSERVED_RPC count

**100 entries**, grouped:
- git: 22, terminal: 8, server meta: 21, provider discovery: 9, automation: 9,
  project file ops: 10, orchestration extras: 7, auth extras: 7,
  desktop/browser/filesystem: 7.

Sources: `MISSING-SYMBOLS.md` RPC-relevant groups + design doc §8 coverage
table. Method strings use MCode's **dot** convention (`git.status`) — the form
the cloned UI references; T5 transport maps to slash strings or `MethodNotFound`.

## Test results

```
$ cargo test -p syncode-contracts
test result: ok. 81 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

$ cargo clippy -p syncode-contracts --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.35s

$ npx tsc --noEmit --strict --moduleResolution node --module esnext \
          --target esnext --skipLibCheck src/contracts/rpc.ts
EXIT=0   (rpc.ts type-checks clean with --strict)
```

(81 tests = the prior 62 + 19 new rpc-module tests. Generation is idempotent:
re-running `cargo test -p syncode-contracts -- test_generate_ts_types`
produces identical TS files.)

## Deviations / assumptions

1. **`turn/start` sequence field** — the handler reads
   `params.get("sequence").as_u64().unwrap_or(0) as u32`. I typed it
   `Option<u32>` (optional on wire), matching the handler's `unwrap_or(0)`
   default. Assumption: the TS client may omit it; if present it's a u32.
2. **`turn/complete` durationMs** — handler reads as `u64` (`as_u64`), but I
   typed the DTO field `Option<u64>` with `#[ts(type = "number | null")]` to
   stay bigint-safe (the snapshot module's `TurnSummary.duration_ms` uses the
   same convention). The handler defaults to 0 if absent, but the DTO models
   it as nullable to match the snapshot type and give clients an explicit
   "unknown" sentinel. Assumption: clients treating `None` as "0" is acceptable.
3. **`auth/bootstrap` result shape** — differs by auth mode (no-auth returns
   `{authenticated, mode, note}`; requiring returns the full principal set).
   I modeled this as a single `AuthBootstrapResult` struct with all
   mode-specific fields `Option<>`. Assumption: the union shape is preferable
   to a tagged enum for TS ergonomics (the cloned UI does `result.sessionToken`
   with a null-check).
4. **`project/get`, `thread/get`, `turn/get`, lifecycle results** — the
   handlers serialize the read-model view directly via `serde_json::to_value`.
   I aliased the result types to the snapshot summary types
   (`ProjectSummary`, `ThreadSummary`, `TurnSummary`) since those DTOs are
   documented as "faithful to" the corresponding views. Assumption: the view
   shapes match the summary DTOs field-for-field (the snapshot module's own
   doc-comments assert this).
5. **`UNSERVED_RPC` method-name convention** — MCode uses dot-camelCase
   (`git.status`, `server.getConfig`). I invented plausible names from the
   MISSING-SYMBOLS symbol prefixes + design doc §8 domain lists, since MCode's
   exact `WS_METHODS` keys weren't enumerated in the worktree. Assumption:
   these are placeholders; T5 will reconcile against MCode's actual
   `packages/contracts/src/rpc.ts` `Rpc.make` keys when the transport re-wire
   runs. The list's purpose is "imports resolve to a typed literal, not `any`."
6. **Branded-ID integration deferred** — the registry uses plain `String` for
   IDs (matching the DTOs). MCode brands IDs (`ThreadId` etc.); the
   `asThreadId()` cast helper from `ids.ts` is the integration point for T5,
   not T3.

Nothing BLOCKING.

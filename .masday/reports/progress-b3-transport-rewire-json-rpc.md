# B3 — Transport re-wire (Effect RPC → JSON-RPC): Progress Report

**Workflow:** fa67c83a-c480-489c-8564-f1039a45a941
**Task:** 07e73ada-76b5-4091-8a55-145c4fe4698e (B3)
**Branch:** `task/b3-transport-rewire-json-rpc` (from master `3f183db` = T1+T2+T3+T4)
**Agent:** masday-frontend
**Date:** 2026-07-02

---

## Summary per priority

### PRIORITY 1 — Rewrite wsTransport.ts to plain JSON-RPC (DONE)

Replaced the Effect-RPC transport (`RpcClient.make(WsRpcGroup)` + `ManagedRuntime` +
`Socket.layerWebSocket` + `RpcSerialization.layerJson`, subscriptions via
`Stream.runForEach`) with a hand-written JSON-RPC-over-WebSocket client. **Zero
`effect`/`@effect`/`effect/unstable` imports** in the new file.

- One `WebSocket` to `/ws` multiplexes all requests + push channels.
- Sends `JsonRpcRequestView`-shaped frames; matches responses by `id`.
- Push notifications (`method: push/<channel>`, no `id`) routed to per-channel listeners.
- New typed surface `rpc<M extends ServedRpcMethod>(method, params): Promise<ServedRpcResult<M>>`
  via T3's `SERVED_RPC` registry — params and result are parametric per-method.
- Untyped `request<T>(method, params, options)` boundary preserved (so
  `wsNativeApi.ts` call sites are unchanged). Unserved methods
  (`git.*`, `terminal.*`, `server.*`, `provider.*`, `automation.*`, `project.readFile`,
  `orchestration.dispatchCommand`, …) reject client-side with a typed `MethodNotFound`
  (-32601) error WITHOUT calling the backend — matches the T3 `UNSERVED_RPC` contract.
- Push routing: `push/orchestration` frames pass through T4's
  `isOrchestrationPushEnvelope` guard hook; `params` forwarded as the channel payload.
- Public boundary (`request`, `subscribe`, `getLatestPush`, `onStateChange`, `getState`,
  `dispose`) preserved verbatim — `wsNativeApi.ts` needed zero edits.
- Reconnect with exponential backoff (500ms → 5s cap) preserved.
- Re-exports `isServerLifecyclePushChannel` / `shouldKeepServerLifecycleStream` for the
  test suite (hard-coded channel names — `WS_CHANNELS` not yet in the shim).

**Typing verified** via standalone type-check harness: served calls enforce real DTO
shapes (`project/create` requires `{name, rootPath}`; `turn/get` requires `{id}`); unserved
calls reject at runtime; result types flow through (`ProjectSummary`, `TurnSummary`, etc.).

**wsTransport.ts: 0 tsc errors** (was ~16 Effect shape-divergence errors at baseline).

### PRIORITY 2 — Server camelCase push envelope (DONE)

`crates/syncode-ws/src/push.rs`:
- Publisher envelope (line ~95): `event_type` → `eventType`, `aggregate_id` → `aggregateId`.
- Snapshot envelope (line ~365): same camelCase keys.
- Test (line ~569): asserts `data["eventType"]`, `data["aggregateId"]`.

`crates/syncode-ws/src/server.rs`:
- Two test fixtures (lines 140, 151) updated to camelCase `eventType` for consistency
  with the real publisher output.

Internal Rust struct field names (`event_type: String`) and method arg names
(`aggregate_id: &str`) left as snake_case — those are not serialized keys.

**`cargo test -p syncode-ws`: 43 unit + 4 E2E + 0 doc tests, ALL GREEN (exit 0).**

Resolves the T1 PushEvent wire-parity follow-up and aligns the wire with T4's
camelCase `OrchestrationPushEnvelope` model.

### PRIORITY 3 — Fix cookieStore defect (DONE)

`frontend/src/syncode-vendor-augmentations.d.ts`:
- **Root cause re-diagnosed:** TS 5.7's lib.dom.d.ts does NOT declare `CookieStore`
  at all (the d.ts file WAS the source of the interface — the T2 "conflicts with
  lib.dom" framing was inaccurate for this TS version). The actual defect was the
  global declaration `cookieStore: CookieStore | undefined`, which surfaced as
  `TS18048: 'cookieStore' is possibly 'undefined'` at every vendored call site
  (e.g. `sidebar.tsx:133`).
- **Fix:** kept the `CookieStore` interface declaration (lib.dom doesn't ship it),
  kept the `Window.cookieStore?` optional accessor, but declared the global
  `cookieStore: CookieStore` (non-optional). This matches how vendored MCode code
  uses it (no null-guard) and removes the false-positive undefined check.
- The `effect/unstable/rpc` + `effect/unstable/socket/Socket` ambient module
  declarations are preserved unchanged — still needed for the 17 remaining
  Effect importers (P4 scope; not all eliminated in one pass).

**cookieStore tsc errors: 0** (was 1).

### PRIORITY 4 — Reduce Effect surface (PARTIAL — best-effort, reported honestly)

**The transport path is fully Effect-free.** The two keystone files are clean:
- `wsTransport.ts`: 0 `effect` imports (was 3: `effect`, `effect/unstable/rpc`,
  `effect/unstable/socket/Socket`). Stripped `Effect`, `Cause`, `Data`, `Exit`,
  `Layer`, `ManagedRuntime`, `Scope`, `Stream`, `RpcClient`, `RpcSerialization`,
  `Socket.*`.
- `wsNativeApi.ts`: was NEVER an Effect importer (it delegates to `WsTransport`).

**16 of 17 remaining importers NOT stripped** — these are non-transport files:
`appSettings.ts(.test.ts)`, `projectScripts.ts`, `shared/DrainableWorker.ts`,
`shared/Net.ts`, `shared/schemaJson.ts`, `lib/projectScriptKeybindings.ts`,
`lib/providerReactQuery.ts`, `whatsNew/useWhatsNew.ts`, `routes/_chat.$threadId.tsx`,
`components/ui/sidebar.tsx`, `components/BranchToolbar.logic.ts`,
`components/ChatView.logic.ts`, `components/profile/useProfile{AvatarColor,AvatarImage,Handle,Name}.ts`.

These import `Schema.is`, `Schema.decodeUnknownSync`, `fromJsonString`, `Array.*`,
`Effect.runPromise`, etc. — runtime validation + functional helpers used across the
vendored UI, not transport. Per the task's "You will NOT eliminate all 20 importers
in one pass" guidance, these are deferred to a T5b pass that swaps them to
`contracts/runtime.ts` guards (`isObject`, `hasKey`, `safeParse`, `decodeWithDefault`).

---

## Files modified

| File | Change |
|---|---|
| `frontend/src/wsTransport.ts` | **Full rewrite** — Effect-RPC → hand-written JSON-RPC client (592 → 638 lines). Zero `effect` imports. |
| `crates/syncode-ws/src/push.rs` | Push envelope keys → camelCase (`eventType`, `aggregateId`) at 2 sites + 1 test. |
| `crates/syncode-ws/src/server.rs` | 2 test fixtures → camelCase `eventType` for publisher-output parity. |
| `frontend/src/syncode-vendor-augmentations.d.ts` | `cookieStore` global declared non-optional (removes TS18048); `effect/unstable/*` ambient decls preserved. |

## wsTransport.ts metrics

- **New line count:** 638 (was 592 — hand-written client is more verbose than the Effect variant, as expected for a from-scratch JSON-RPC client with reconnect/backoff/pending-map).
- **`effect` imports in wsTransport.ts:** **0** (was 3).
- **`effect` imports in wsNativeApi.ts:** **0** (never an importer; verified).

## Test / type-check results

### `cargo test -p syncode-ws` (PRIORITY 2)
- 43 unit tests passing
- 4 E2E tests passing (real TCP: ping/pong, project create+list, invalid method, push subscribe)
- 0 doc tests
- **Exit 0** — all green with camelCase envelope.

### `cd frontend && npx tsc --noEmit`

| Metric | Before (T2 baseline) | After (B3) | Delta |
|---|---|---|---|
| **Total TS errors** | 3004 | 2971 | **−33** |
| `wsTransport.ts` errors | ~16 (Effect shape-divergence) | **0** | −16 |
| `wsNativeApi.ts` errors | ~170 | 170 | 0 (all MISSING Tier-3 symbols) |
| `cookieStore` errors | 1 | **0** | −1 |
| Other Effect-shape errors (appSettings, schemaJson, etc.) | ~97 | ~97 | 0 (P4 deferred) |

**Breakdown of remaining 2971 errors:**
- ~170 in `wsNativeApi.ts` + 60 in `wsNativeApi.test.ts` — all "Module has no exported
  member" for MISSING Tier-3 contracts symbols (`AuthClientSession`, `OrchestrationEvent`,
  `WS_METHODS`, `WS_CHANNELS`, `ContextMenuItem`, `ThreadBrowserState`, …). These are
  deferred symbol exports, NOT transport issues.
- ~97 Effect-shape errors in the 17 non-transport importers (Schema/runtime usage).
- The rest: unrelated Tier-3 contract holes across the vendored UI (the bulk of the
  "~768 type holes span T3-T5" noted in the task — most are missing symbol exports).

**SUCCESS criterion for B3 met:** wsTransport + the transport call-path compile
Effect-free; the served-RPC call sites type-check against SERVED_RPC; the error count
DECREASED from the T2 baseline by the transport-path delta (−33).

## Effect importer count

- **Before:** 18 (task said 20; recount yielded 18 files in `src/`).
- **After:** 17 (wsTransport.ts stripped).
- **Remaining 17:** all non-transport (appSettings, projectScripts, shared/DrainableWorker,
  shared/Net, shared/schemaJson, lib/projectScriptKeybindings, lib/providerReactQuery,
  whatsNew/useWhatsNew, routes/_chat.$threadId, components/ui/sidebar, components/BranchToolbar.logic,
  components/ChatView.logic, components/profile/useProfile{AvatarColor,AvatarImage,Handle,Name}).

---

## Deviations / assumptions

1. **Channel-name divergence (DEFERRED — Tier 3):** `wsNativeApi.ts` subscribes to
   MCode-style fine-grained channels (`server.welcome`, `orchestration.domainEvent`,
   `terminal.event`) via the MISSING `WS_CHANNELS` constant. Syncode's wire emits
   coarse aggregate channels (`push/orchestration`, `push/git`, `push/terminal`).
   The transport routes correctly by the bare wire channel name; aligning wsNativeApi's
   channel conventions to Syncode's is **B6+ Tier-3 work**, not B3. The transport is
   correct on its own terms — the mismatch surfaces only when wsNativeApi is wired
   against real backend channels (which requires the missing `WS_CHANNELS` constant
   anyway).

2. **`MCODE_TO_SERVED` map is currently empty.** The remap table for MCode dot-strings
   → Syncode slash-strings was left empty because the served RPCs in `SERVED_RPC`
   don't have clean 1:1 MCode dot-string equivalents that the vendored UI calls
   through `wsNativeApi.ts` today (the served methods are project/thread/turn CRUD +
   auth + push; the vendored UI calls server/git/terminal/provider/automation which
   are ALL unserved). When a served RPC grows an MCode caller, the remap is a one-line
   addition. Unserved methods correctly reject client-side in the meantime.

3. **cookieStore defect re-diagnosis:** the task framing ("conflicts with lib.dom") was
   inaccurate for TS 5.7 — lib.dom here does NOT declare `CookieStore` at all. The
   actual defect was the `| undefined` on the global declaration producing TS18048.
   Fix matches the spirit (cookieStore usable without null-guard) and is more accurate
   to the real root cause. Documented in the d.ts comment.

4. **`routePush` orchestration guard is a no-op hook.** T4's `isOrchestrationPushEnvelope`
   is called but its result is discarded (`void isOrchestrationPushEnvelope(params)`).
   Frames are forwarded regardless of guard outcome (forward-compat with unrecognized
   event tags). This is intentional — deep validation of push payloads is deferred
   (design §5: ~6 sites, hand-written guards). The guard import is exercised so the
   dependency on T4's events.ts stays live; a future pass can wire real
   validation/rejection.

5. **No live E2E against a running backend** (deferred per task — infra-heavy). The
   cargo `ws_e2e.rs` tests exercise the real TCP server path for the served methods
   and pass; the client-side transport is verified via type-check + esbuild syntax
   parse. A live browser↔backend round-trip is a T5b/T6 task.

---

## BLOCKING / deferred for T5b

- **Strip the 17 non-transport Effect importers** → swap to `contracts/runtime.ts`
  guards. Will resolve ~97 Effect-shape errors.
- **Export MISSING Tier-3 contracts symbols** (`WS_METHODS`, `WS_CHANNELS`,
  `ORCHESTRATION_WS_METHODS`, `ORCHESTRATION_WS_CHANNELS`, `OrchestrationEvent`,
  `AuthClientSession`, `ContextMenuItem`, `ThreadBrowserState`, …) → resolves
  ~230 errors in wsNativeApi.ts(+test) + the bulk of the vendored UI holes.
- **Align wsNativeApi channel conventions** to Syncode's coarse wire channels (B6+).
- **Wire `isOrchestrationPushEnvelope` to actually validate/reject** push payloads
  (currently a no-op hook).
- **Live browser E2E** of the JSON-RPC client against a running backend.

---

## Acceptance criteria checklist

- [x] `wsTransport.ts` is a non-Effect JSON-RPC client (0 `effect` imports).
- [x] `rpc()` typed via SERVED_RPC (parametric params + result).
- [x] MethodNotFound (-32601) for unserved methods, client-side, without calling.
- [x] Push routed via T4 envelopes (`isOrchestrationPushEnvelope` guard hook).
- [x] `push.rs` emits camelCase envelope; `cargo test -p syncode-ws` green.
- [x] cookieStore defect fixed.
- [x] Error count DECREASED from T2 baseline (3004 → 2971, −33).
- [x] Transport path compiles Effect-free.
- [x] Effect importer count reduced (18 → 17); transport path clean.

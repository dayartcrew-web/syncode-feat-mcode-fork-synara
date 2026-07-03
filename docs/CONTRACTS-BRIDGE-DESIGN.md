# Contracts Bridge Design — ts-rs ↔ `@t3tools/contracts`

> **Status (2026-07-03): IMPLEMENTED (PR #6–#32).** This design was executed end-to-end — the contracts bridge is complete, the cloned UI is type-clean (tsc 0), and a standalone backend serves all RPC domains. For the authoritative current REAL-vs-STUB status see [`STATUS.md`](./STATUS.md). This document is retained as the design spec; the body (tiers, decisions, phased plan) reflects the original design.
>
> Grounded in live measurement of both sides: MCode `@t3tools/contracts` (`/home/vibe-dev/mcode/packages/contracts/`) and Syncode ts-rs output (`crates/syncode-contracts/` → `frontend/src/types/`).

---

## 1. Executive summary

The contracts bridge is a generated + hand-curated TypeScript package that **mimics the public export surface of MCode's `@t3tools/contracts`**, backed by **ts-rs output from Syncode's Rust domain** instead of Effect Schema.

Two findings dominate the design:

1. **Surface gap is ~60×, not "a remap."** MCode's contracts export **~1,557 symbols** (≈990 schemas/consts/functions + ≈567 types) across 22 modules. Syncode's ts-rs output today is **26 types** (read-model/snapshot DTOs + the JSON-RPC envelope + a generic `PushEvent`). There are **no command types, no event payloads, no RPC request/response param types, no auth/automation/provider-discovery/settings/stats/terminal/git-detail types.**

2. **The binding constraint is RPC coverage, not contract naming.** MCode's frontend invokes **~80 RPC methods**; Syncode's backend implements **~10–20**. Even a perfect contracts bridge leaves the cloned UI calling handlers that don't exist. The contracts bridge makes the *types* line up; **backend RPC growth** is what makes the UI *function*.

**Recommended strategy: tiered hybrid bridge + path-identical shim.**

- Build the bridge as a **local package aliased to the exact import path `@t3tools/contracts`** so the cloned `apps/web`'s 333 import sites need **zero import-path edits** — only type-shape mismatches surface, which the TypeScript compiler enumerates for free.
- Tier the work: **Tier 0** (the 26 existing ts-rs types, re-exported) is free; **Tier 1** (typed RPC method registry + per-method request/response params — the keystone) unblocks compilation; **Tier 2** (domain-event discriminated union) types push payloads; **Tier 3** (provider-runtime, stats, keybindings, editor, settings, …) is deferred behind stubs.
- The bridge is generated **centrally in `syncode-contracts`** (the only crate that depends on ts-rs today) using the existing **DTO-mirror pattern** — define TS-facing DTOs in the contracts crate that mirror domain types, rather than spreading `#[derive(TS)]` across `syncode-core`/`syncode-orchestration`.

**Honest effort revision:** the contracts bridge itself (Tier 0+1+2) is **~2 weeks** of Rust DTO work and *unblocks a compiling, type-correct clone fast*. A *functional* clone is gated by backend RPC coverage (§8) — that is the larger, phased effort, not the contracts layer.

---

## 2. The gap — supply vs demand

### 2.1 Demand side: what the clone imports (MCode `@t3tools/contracts`)

22 modules, ~1,557 symbols. Largest:

| Module | Approx symbols | Notes |
|---|---|---|
| `orchestration.ts` | ~400 | thread/project/message/turn/checkpoint/pinned/marker schemas, **~34 event variants**, command unions, read models, RPC I/O |
| `providerRuntime.ts` | ~300 | **~48 provider-runtime event variants** + payloads |
| `rpc.ts` | ~87 | **~85 `Rpc.make` definitions** + `WsRpcGroup` |
| `git.ts` | ~92 | 22 git operations (input + result) |
| `server.ts` | ~74 | server meta, diagnostics, usage, voice, recap |
| `automation.ts` | ~63 | schedule/retry/misfire/completion policies, run lifecycle |
| `model.ts` | ~57 | per-provider model options + reasoning efforts |
| `providerDiscovery.ts` | ~56 | skills/plugins/commands/models/agents |
| `baseSchemas.ts` | ~45 | branded IDs (ThreadId, ProjectId, TurnId, …) + primitives |
| `project.ts` | ~45 | file ops, dev servers, search |
| `stats.ts` ~30 · `provider.ts` ~27 · `ws.ts` ~26 · `ipc.ts` ~25 · `settings.ts` ~25 · `terminal.ts` ~25 · `auth.ts` ~35 · `keybindings.ts` ~14 · `environment.ts` ~9 · `editor.ts` ~7 · `filesystem.ts` ~6 · `agentMentions.ts` ~10 | | |

**Plus** `ipc.ts` defines the `NativeApi` interface (~170 lines) and `DesktopBridge` (~65 lines) — the Electron desktop-shell bridge (see §6.5).

**Import concentration:** 239 files in `apps/web` import from contracts; the **top symbols are branded IDs** (`ThreadId` ×48, `ProjectId` ×22, `TurnId` ×12, `MessageId` ×11, `ProviderKind` ×9). ~95% of imports are **type-only**.

### 2.2 Supply side: what Syncode generates today (ts-rs)

26 types, all in `crates/syncode-contracts/` (`src/lib.rs` ×16, `src/snapshots.rs` ×10), emitted to `frontend/src/types/` via `build.rs` (`TS_RS_EXPORT_DIR`) + the `test_generate_ts_types` harness.

| Exported | Purpose |
|---|---|
| `EntityId`, `Timestamp` | primitives (string aliases) |
| `ProviderConfig`, `ProviderCapabilities`, `CreateSessionRequest` | provider/session setup |
| `SessionView`, `SessionStatus`, `MessageView`, `MessageRole` | session read-model |
| `GitStatusView`, `GitFileStatusView`, `FileStatusKind` | git status read-model |
| `JsonRpcRequestView`, `JsonRpcResponseView`, `JsonRpcErrorView` | wire envelope (`params`/`result`/`data` are `Record<string, unknown>`) |
| `PushEvent` | generic push envelope (`data: Record<string, unknown>`) |
| `ProjectSummary`, `ThreadSummary`, `TurnSummary`, `MessageSummary`, `ActivitySummary`, `SnapshotScope`, `ShellSnapshot`, `ThreadDetailSnapshot`, `FullSnapshot` | snapshots |

**Not exported (the gap):** the **48 `Command` variants** (`orchestration/decider.rs`), the **44 `DomainEvent` variants** (`core/domain/events.rs`), any **RPC request/response param types**, auth/automation/provider-discovery/settings/stats/terminal/keybindings/editor types, and 5 sub-aggregate read-models (`ThreadSessionView`, `PinnedMessageView`, `MarkerView`, `ProposedPlanView`, `CheckpointView`).

### 2.3 Coverage at a glance

| Surface | MCode (demand) | Syncode (supply) | Coverage |
|---|---|---|---|
| Contracts symbols | ~1,557 | 26 | ~1.7% |
| RPC methods (frontend → backend) | ~80 | ~10–20 | ~12–25% |
| Push channels | 12 | 1 generic (`push/<channel>`) | envelope only |
| Domain events (typed) | ~40 | 44 in Rust, **0 exported** | 0% typed |

---

## 3. Bridge architecture

### 3.1 Package shape — the path-identical shim

```
frontend/
  package.json                 # adds path alias: "@t3tools/contracts" → "./src/contracts"
  src/
    contracts/                 # THE BRIDGE (drop-in for @t3tools/contracts)
      index.ts                 # barrel: re-exports every module below
      generated/               # ts-rs output lands here (replaces frontend/src/types for new types)
        SessionView.ts
        ThreadDetailSnapshot.ts
        … (all 26 + future Tier 1/2 types)
      ids.ts                   # branded IDs (ThreadId, ProjectId, …) as `type X = string & {__brand}`
      rpc.ts                   # Tier 1: RPC method registry + request/response param types
      events.ts                # Tier 2: DomainEvent discriminated union
      runtime.ts               # minimal runtime guards (replaces Schema.is / decode for the ~6 sites)
      shell.ts                 # NativeApi + DesktopBridge interfaces (Electron→Tauri boundary)
      stubs.ts                 # Tier 3: `never`/placeholder types for deferred surfaces
```

**Key trick:** alias `@t3tools/contracts` to `./src/contracts` in `tsconfig`/`vite`/`package.json`. The cloned `apps/web` keeps `import { ThreadId, type OrchestrationThread } from "@t3tools/contracts"` **verbatim** — **333 import sites need zero edits**. Whatever the bridge doesn't yet define surfaces as ordinary TS errors (`Module has no exported member 'X'`), which the compiler lists exhaustively. This turns "remap 333 files" into "fill the holes the compiler reports."

### 3.2 Generation pipeline (ts-rs, centralized)

- **One generation crate:** `syncode-contracts` remains the sole ts-rs consumer. **Do not** add ts-rs to `syncode-core`/`syncode-orchestration`/etc. — keep domain crates clean of serialization-for-TS concerns.
- **DTO-mirror pattern (already established):** contracts defines TS-facing structs that mirror domain types and derives `TS` on the mirrors (e.g. today's `ProviderConfig` DTO ≠ `syncode_provider::ProviderConfig`). Extend this: add `CommandParams`/`EventPayload`/`Rpc*` DTO mirrors in contracts, deriving `TS`, sourced from the domain enums via conversion. This keeps the generation boundary identical to the current one.
- **Generation trigger unchanged:** `cargo test -p syncode-contracts -- test_generate_ts_types` writes to `TS_RS_EXPORT_DIR`. Point `TS_RS_EXPORT_DIR` at `frontend/src/contracts/generated/` (one-line `build.rs` change).
- **Barrel generation:** add a small test that (re)writes `contracts/index.ts` re-exporting every generated file — fixes the current bug where the barrel omits the 9 snapshot types.

### 3.3 Casing normalization

ts-rs 10 emits **snake_case fields** and **PascalCase enum variants** (except where `#[serde(rename_all="snake_case")]` is set, e.g. `SnapshotScope`). MCode's frontend, written against Effect Schema, expects the names as authored in TS — Effect Schema round-trips camelCase field names.

**Decision:** canonicalize to **camelCase** at the contracts boundary.
- Add `#[ts(rename_all = "camelCase")]` (+ matching `#[serde(rename_all = "camelCase")]` for wire parity) on DTO mirrors. ts-rs 10 honors `#[ts(rename)]`/`rename_all`.
- This makes the JSON wire and the TS types agree on camelCase, matching MCode expectations and removing a class of 333-file shape errors.

### 3.4 The `bigint` problem

`TurnSummary.duration_ms: Option<u64>` → ts-rs emits `bigint | null`, but `JSON.parse` yields `number`. **Decision:** annotate `#[ts(type = "number | null")]` on the Rust field (and keep serde as-is). Audit all `u64`/`usize`/`i64` fields in exported DTOs for the same fix.

---

## 4. Tiered build-out

### Tier 0 — exists today (free)
Re-export the 26 generated types from the bridge barrel under camelCase-normalized names. **Effort: hours.**

### Tier 1 — RPC registry + param types (THE KEYSTONE)
This is what unblocks compilation of the cloned UI's transport layer. Define, in `contracts/rpc.ts`:

1. A **typed method registry** mirroring MCode's `WS_METHODS` shape but with Syncode's **slash method strings** (`project/create`, `thread/start`, …), each carrying `Request` and `Response` type params.
2. **Per-method request/response param types** for the RPCs Syncode actually serves (~20), as TS interfaces backed by ts-rs DTOs.
3. For the ~60 MCode RPCs with **no Syncode handler yet**, declare the method + types in the registry but mark them `// STUB: backend RPC not implemented` — so imports resolve and the compiler is happy, but calling them returns a typed `MethodNotFound` error at runtime (transport layer enforces).

**Rust work:** add `#[derive(TS)]` request/result DTO mirrors in `syncode-contracts` for the ~20 served RPCs (e.g. `ProjectCreateParams`, `ThreadCreateParams`, `TurnStartParams`, `PushSubscribeParams`, `AuthBootstrapResult`, …). The domain already has the data (the 48 commands); the DTO mirrors are thin projections.

**Effort: ~1 week.**

### Tier 2 — domain events (typed push payloads)
Export the **44 `DomainEvent` variants as a TS discriminated union** keyed by `event_type`. Map MCode's ~40 orchestration event types onto Syncode's 44 (more granular — modeling divergence, §7). Push payloads (`orchestration.domainEvent` channel) become typed instead of `Record<string, unknown>`.

**Rust work:** a `DomainEventDto` enum mirror in contracts (tagged by `event_type`), derived `TS`, projected from `syncode_core::DomainEvent`. ~same shape work as the existing snapshot DTOs.

**Effort: ~3–5 days.**

### Tier 3 — deferred surfaces (stubs, wire incrementally)
Large MCode surfaces with **no Syncode backend equivalent**: `providerRuntime` (~48 runtime events), `providerDiscovery` (skills/plugins/agents), `stats`, `server` meta (diagnostics, usage, voice, recap), `keybindings`, `editor`, `settings`, `automation` (the crate exists but isn't RPC-exposed).

**Decision:** declare these types in the bridge as **narrow stubs / `never`-typed or minimal interfaces** so the cloned UI *compiles*, and gate the corresponding UI features behind feature flags / "not available in Syncode" states. Implement for real only as the matching backend RPCs land (§8).

**Effort: ~2–3 days to stub; real implementation tracks backend RPC growth.**

---

## 5. Runtime validation substitute

Agent measurement: runtime Effect-Schema use in `apps/web` is **minimal** — ~6 production files, ~15 call sites: type guards (`Schema.is`), localStorage JSON serde (`fromJsonString`/`decodeSync`/`encodeSync`), and safe-decode-with-defaults (`decodeUnknownOption/Sync`). ~95% of contract usage is type-only.

**Decision:** ship a tiny `contracts/runtime.ts` with **hand-written guards + a couple of `safeParse`-style helpers** for exactly those patterns. Do **not** pull in zod/valibot globally — the surface is too small to justify the dependency. Revisit only if runtime-validation demand grows.

---

## 6. Cross-cutting decisions

### 6.1 Branded IDs
MCode brands IDs (`ThreadId`, `ProjectId`, …) via Effect Schema branding; Syncode has a single generic `EntityId = string`. **Decision:** in the bridge, declare branded aliases `type ThreadId = string & { readonly __brand: "ThreadId" }` per MCode's ID set, and provide a cast helper `asThreadId(s)`. Branded IDs are structurally strings, so they interoperate with `EntityId` at boundaries via the helper.

### 6.2 RPC method-name mapping
MCode keys RPCs as camelCase (`serverGetConfig`) over **dot** wire methods (`server.getConfig`). Syncode uses **slash** wire methods (`project/create`). The **transport re-wire** (separate doc) maps the cloned client's method keys to Syncode slash strings. The contracts `rpc.ts` registry carries the **Syncode slash strings as the source of truth**; a thin name-map translates from MCode's camelCase keys during the re-wire.

### 6.3 Push channels
MCode defines **12 typed push channels**; Syncode has **one generic** `push/<channel>` envelope with `Record<string, unknown>` data. **Decision:** keep the generic envelope on the wire; in the bridge, layer typed views per channel (`onThreadEvent`, `onTerminalEvent`, …) that narrow `PushEvent.data` using the Tier 2 event union + per-channel discriminators.

### 6.4 `MessageView` duplication
Two distinct types share the name: `syncode_contracts::MessageView` (session read-model) vs `syncode_orchestration::read_model::MessageView` vs MCode's `OrchestrationMessage`. **Decision:** disambiguate in the bridge with explicit Syncode names (`SessionMessage`, `ThreadMessage`) and alias to the MCode-expected name only where shapes align.

### 6.5 NativeApi / DesktopBridge (Electron → Tauri)
`ipc.ts` defines the `NativeApi` (~170-line) + `DesktopBridge` (~65-line) interfaces — the desktop shell API the cloned UI calls (window controls, webview/browser panels, notifications, updates, editor launch). These are **pure TS interfaces**, not Effect Schema. **Decision:** keep them verbatim in `contracts/shell.ts` (they're the stable boundary the rest of the UI consumes); implement the **Tauri** `NativeApi` in the transport re-wire via `@tauri-apps/api` `invoke`, replacing MCode's Electron `wsNativeApi.ts`. For capabilities Tauri lacks (e.g. embedded browser webview panels), stub to "unsupported."

---

## 7. Domain divergence reconciliation

Syncode's domain model diverges from MCode's (see `syncode-vs-mcode-porting-fidelity`): **Turn / Message / Activity are first-class aggregates** in Syncode; in MCode only **project + thread** are aggregates (turns/messages are thread sub-structures). Consequences for the bridge:

- **Event granularity:** Syncode's 44 events decompose message/turn/activity lifecycle more finely than MCode's ~40. The Tier 2 union maps many-to-one/one-to-many; document the mapping table during Tier 2 implementation.
- **Command surface:** Syncode's 48 commands cover project/thread/turn/message/plan/checkpoint/revert — close to MCode's orchestration command set. The gap is the **non-orchestration** RPCs (git ops, terminal, server-meta, automation, provider-discovery) which aren't commands at all in Syncode — they're separate crates with no RPC exposure.
- **Read-model shapes:** Syncode's snapshot DTOs (ShellSnapshot / ThreadDetailSnapshot / FullSnapshot) are **close but not identical** to MCode's `OrchestrationReadModel` / `OrchestrationShellSnapshot` / `OrchestrationThreadDetailSnapshot`. Provide adapter functions in the bridge that reshape Syncode snapshots into MCode-expected shapes.

---

## 8. The real cost driver — backend RPC coverage

The contracts bridge makes types line up. **A functional clone requires the backend to serve the RPCs the UI calls.** Current mapping (Syncode slash methods vs MCode dot methods):

| Domain | MCode RPCs | Syncode handlers | Gap |
|---|---|---|---|
| Projects (CRUD + file ops + dev servers + search) | ~13 | `project/list,get,create` (3) | file ops, dev servers, search **not exposed** |
| Orchestration (snapshot/diff/replay/subscribe/dispatch) | ~13 | partial (`thread/*`, `turn/*`, snapshots via `project/get`,`thread/get`) | domain-event stream, replay, diffs, repair **not exposed** |
| Git (status/diff/branch/worktree/stage/pull/PR/…) | ~22 | **0** (crate exists, no RPC) | **entire surface missing** |
| Terminal (open/write/resize/close/subscribe) | ~8 | **0** (crate exists, no RPC) | **entire surface missing** |
| Server meta (config/settings/providers/diagnostics/usage/voice/recap) | ~21 | **0** | **entire surface missing** |
| Provider discovery (skills/plugins/models/agents) | ~9 | **0** | **entire surface missing** |
| Automation (CRUD + run + subscribe) | ~9 | **0** (crate exists, no RPC) | **entire surface missing** |
| Stats / Filesystem / Shell / Auth / Push infra | ~6 | auth + push + ping (covered) | mostly covered |

**Implication:** a compiling, type-correct clone is achievable in ~2 weeks (Tier 0+1+2 + shim). A clone that **does something useful** should target a **parity subset** first — e.g. *chat + thread + project + read-only git status + terminal* — and grow backend RPCs domain-by-domain. Full MCode parity tracks the original roadmap's backend-equivalent effort; clone+rewire **saves the UI rebuild**, not the backend build-out.

---

## 9. Phased plan & revised effort

| Phase | Scope | Effort | Gate |
|---|---|---|---|
| **B0 — Shim + Tier 0** | Path-identical `@t3tools/contracts` alias, re-export 26 types (camelCase), branded IDs, barrel fix | ~2 days | clone *imports resolve* |
| **B1 — Tier 1 keystone** | RPC registry + ~20 served-method param/result DTOs + stub registry entries for the rest | ~1 week | cloned UI **compiles** (type-clean) |
| **B2 — Tier 2 events** | 44-event discriminated union + push-channel typed views | ~3–5 days | push payloads typed |
| **B3 — Transport re-wire** | (separate doc) replace Effect-RPC client with Syncode JSON-RPC client; wire the ~20 served methods; `MethodNotFound` for the rest | ~1 week | clone **runs**, served flows work |
| **B4 — Shell swap** | Electron `NativeApi` → Tauri `invoke` | ~3–5 days | desktop shell boots |
| **B5+ — Parity subset backend** | Expose git-status / terminal / domain-event-stream / automation RPCs (domain-by-domain) | **weeks–months** | functional feature growth |

**Revised headline:** clone+rewire yields a **type-correct, runnable frontend in ~3–4 weeks** (B0–B4). Feature completeness is then bounded by backend RPC coverage (B5+), not by contracts or UI work. The earlier "~3-6 weeks to parity" estimate holds **for a targeted parity subset**, not full MCode parity.

---

## 10. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| **Schema-shape divergence** (aggregates, event granularity) | High — many TS errors after clone | Maximize bridge overlap; adapter functions; map-table in Tier 2 |
| **Backend RPC coverage** (the binding constraint) | High — UI calls unimplemented handlers | Parity-subset first; `MethodNotFound` + feature flags for the rest |
| **`providerRuntime` 48 events have NO Syncode equivalent** | Medium — large deferred surface | Tier 3 stubs; gate provider-runtime-driven UI |
| **camelCase normalization drift** | Medium — wire/TS mismatch | Apply `rename_all` on both serde + ts-rs together; test round-trip |
| **bigint/number** (`duration_ms`, other `u64`) | Low–Medium | `#[ts(type="number | null")]` audit |
| **NativeApi capabilities Tauri lacks** (browser webview panels) | Medium | Stub "unsupported"; scope these features out of v1 |
| **Licensing** | Low | Confirm MCode LICENSE (same author lineage; verify before publishing) |

---

## 11. Open decisions (need your call)

1. **Target parity subset for v1** — which domains first? Recommended: *chat + thread + project + read-only git status + terminal* (covers B0–B5 minimally, ~6-8 weeks to a usable app).
2. **camelCase vs snake_case canonical** — recommend camelCase (matches MCode frontend); confirm.
3. **Runtime validation** — recommend hand-written guards (no zod); confirm, or mandate a lib.
4. **Stub strategy for deferred surfaces** — feature-flag "not available" vs hide UI entirely; recommend flag (discoverable, low-cost).
5. **Branded IDs** — adopt MCode's brand set, or stay on generic `EntityId` with loose typing? Recommend brand set for type-safety parity.

---

## Appendix A — MCode contracts module inventory (reference)

Source: `/home/vibe-dev/mcode/packages/contracts/src/index.ts` re-exports 22 modules. See agent report for the full per-module symbol tables. Headline counts: ~990 schemas/consts/functions/classes + ~567 type aliases/interfaces ≈ **1,557 symbols**; ~85 `Rpc.make` definitions in `rpc.ts`; 12 push channels in `ws.ts`; branded-ID set in `baseSchemas.ts`.

## Appendix B — Syncode ts-rs inventory (reference)

26 types in `crates/syncode-contracts/` (`lib.rs` ×16, `snapshots.rs` ×10) → `frontend/src/types/`. Config: `ts-rs 10.1.0`, features `serde-compat` + `no-serde-warnings`; `TS_RS_EXPORT_DIR` set by `build.rs`; generated by `test_generate_ts_types`. No other crate uses ts-rs. Barrel `index.ts` currently omits the 9 snapshot types (bug to fix in B0).

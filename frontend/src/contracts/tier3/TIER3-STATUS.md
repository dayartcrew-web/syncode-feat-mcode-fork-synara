# TIER3-STATUS — Tier 3 contracts export status

> Companion to `MISSING-SYMBOLS.md`. Records which of the 139 missing
> `@t3tools/contracts` symbols (as enumerated by `tsc --noEmit` on the T5b
> baseline) now have **real types** vs **stubs**, and where the remaining
> shape-error hotspots cluster for the T5c real-type-modeling pass.

## Summary

| Metric | Before (T1-T6) | After (T5b) |
|---|---|---|
| `@t3tools/contracts` `has no exported member` (TS2305/TS2724) | **609** | **0** |
| `src/contracts/` internal errors | 0 | **0** |
| Total `tsc` error lines | 3411 | **2737** (−674, −20%) |
| New module-resolution errors | 0 | **0** |

All 139 missing `@t3tools/contracts` symbols are now exported. The shim
barrel (`contracts/index.ts`) re-exports them from new `contracts/tier3/*`
modules. Where `shell.ts` previously declared opaque stubs for the same
symbols, those stubs were replaced with `import type` re-exports from the
tier3 modules so the `NativeApi`/`DesktopBridge` interfaces pick up the
real shapes too (no duplicate-export conflicts).

## Modules created

```
frontend/src/contracts/tier3/
├── base.ts           — TrimmedNonEmptyString, branded IDs (ThreadMarkerId, AutomationRunId, …)
├── orchestration.ts  — ProviderKind, ModelSelection union, RuntimeMode, Orchestration*, constants
├── model.ts          — MODEL_OPTIONS_BY_PROVIDER, MODEL_CAPABILITIES_INDEX, PROVIDER_DISPLAY_NAMES, …
├── provider.ts       — Provider*Descriptor, ProviderOption*, agent-mention aliases + functions
├── automation.ts     — AutomationSchedule, AutomationDefinition, AutomationRun, …
├── server.ts         — ServerConfig, ServerSettings(+Patch), ServerProviderStatus, lifecycle, …
├── project.ts        — ProjectEntry, ProjectDirectoryEntry, ProjectDevServer, ProjectScript, …
├── git.ts            — GitBranch, GitStatusResult, GitRunStackedActionResult, GitActionProgressEvent
├── ws.ts             — WS_METHODS, WS_CHANNELS, WsPush union, WsWelcomePayload, WsRpcGroup
├── terminal.ts       — TERMINAL_*_COLS/ROWS, TerminalSessionSnapshot, TerminalEvent (real shapes)
├── auth.ts           — AuthBootstrapInput, AuthClientSession, AuthSessionState, pairing types
├── stats.ts          — ProfileHeatmapCell, ProfileStats, ProfileTokenStats
├── keybindings.ts    — KeybindingCommand union, KeybindingRule, ResolvedKeybindingsConfig
├── misc.ts           — EDITORS, EditorId, ContextMenuItem, ToolLifecycleItemType, UploadChatAttachment
└── TIER3-STATUS.md   — this file
```

## Real-typed vs stubbed (out of 139)

**Real types — 132 symbols (95%)**. Hand-ported from
`/home/vibe-dev/mcode/packages/contracts/src/*.ts` (Effect Schema → plain
TS). Faithful field sets; the only lossy bits are nested `readonly unknown[]`
where the nested shape is itself a deferred port (chat attachments, activity
payloads, orchestration checkpoints).

**Stubs (permissive `Record<string, unknown>` / `interface X {}` / opaque
alias) — 7 symbols (5%)**:

| Symbol | Module | Reason |
|---|---|---|
| `ThreadTokenUsageSnapshot` | orchestration.ts | MCode has no direct schema; profile projection only |
| `UserInputQuestion` | misc.ts | MCode has a richer discriminated union pending port |
| `FilesystemBrowseResult` | misc.ts | Syncode's filesystem crate exposes a simpler shape than MCode's Electron browser |
| `ClientOrchestrationCommand` | orchestration.ts | 28-variant union modeled as discriminated `{ type: string; … }` — real per-variant modelling is T5c |
| `KeybindingCommand` | keybindings.ts | Union of static commands + `script.<id>.run` pattern; `script.` arm is a branded string fallback |
| `EditorId` | misc.ts | Literal union collapsed to `string` (full editor list still in `EDITORS` const) |
| `OrchestrationThread.checkpoints` | orchestration.ts (field) | `readonly unknown[]` — `OrchestrationCheckpointSummary` port is T5c |

## Files modified

- `frontend/src/contracts/index.ts` — added Tier 3 re-export block (139 symbols).
- `frontend/src/contracts/shell.ts` — replaced ~50 opaque stub declarations
  with `import type` re-exports from `./tier3/*`; kept the standalone
  opaque stubs for the Input/Result DTOs not in T5b scope. Removed the
  inline `ContextMenuItem` declaration (now re-exported from `./tier3/misc`).

## Remaining error hotspots (T5c scope)

The remaining 2737 errors break down as:

| TS code | Count (after) | vs before | Category |
|---|---|---|---|
| TS2693 | 1116 | +68 ↑ | "X only refers to a type, but is being used as a value" — Effect Schema branded-ID pattern. Pre-existing; small uptick is the cost of resolving 566 TS2305s. **T5b.2 (strip Effect)** owns this. |
| TS18046 | 308 | −83 ↓ | "X is of type unknown" — Effect `Schema.decode*` returns unknown. **T5b.2**. |
| TS2345 | 194 | −29 ↓ | assignability — real-shape tightening surfaced these; many are `unknown → typed` casts the UI needs. **T5c**. |
| TS2339 | 132 | −8 ↓ | property-access on stub fields (`ThreadTokenUsageSnapshot`, `UserInputQuestion`, …). **T5c**: port the real nested shapes. |
| TS6133 | 126 | +6 ↑ | unused locals — mostly in vendored MCode files; minor. |
| TS2322 | 106 | −5 ↓ | type-assignment — real-shape tightening. **T5c**. |
| TS7006 | 100 | −103 ↓ | implicit-any params — pre-existing; down because some sites now infer from typed contracts. |
| TS2551 | 67 | +6 ↑ | "did you mean …" — mostly `Schema.Literals → Schema.Literal` Effect drift. **T5b.2**. |

### T5c priorities (real-type-modeling pass)

1. **`ClientOrchestrationCommand` 28-variant union** — currently `{ type: string; … }`.
   Port each per-variant shape from MCode `orchestration.ts` so
   `NativeApi.orchestration.dispatchCommand` is type-safe on the command
   kind. This unblocks ~30 TS2345 errors in command-construction sites.
2. **`UserInputQuestion` discriminated union** (free-text / single-choice /
   multi-choice) — port from MCode; unblocks `pendingUserInput.ts` and
   composer prompt UI.
3. **`ThreadTokenUsageSnapshot`** — model the profile projection; unblocks
   thread-detail token-usage rendering.
4. **Nested `unknown[]` fields in `OrchestrationThread`** (checkpoints,
   attachments, activity payloads) — port `OrchestrationCheckpointSummary`,
   `ChatAttachment`, etc.
5. **`FilesystemBrowseResult`** — decide whether to mirror MCode's Electron
   shape or expose Syncode's simpler recursive listing.

### T5b.2 (strip Effect) — owns the bulk of remaining errors

The 1116 TS2693 + 308 TS18046 + 67 TS2551 = **1491 errors (~55% of the
remainder) are Effect-Schema-API drift**, NOT contracts shape errors. They
stem from the vendored UI using branded IDs / Schema refinements as runtime
values (MCode's Effect Schema branding pattern). Resolving them requires
stripping Effect from the vendored UI — out of scope for T5b per the task
spec ("Do NOT strip Effect (separate T5b.2)").

## Methodology

1. `npm install` in the worktree (node_modules absent at branch start).
2. Baseline: `npx tsc --noEmit` → 3411 lines, 609 `has no exported member`
   on `@t3tools/contracts` (139 unique symbols; the rest are Effect module
   errors + react-icons, out of scope).
3. Read MCode contracts source (`/home/vibe-dev/mcode/packages/contracts/src/`)
   for each domain; hand-port Effect Schema → plain TS in `tier3/*.ts`.
4. Re-export from `contracts/index.ts`; replace opaque stubs in `shell.ts`
   with `import type` to avoid duplicate-export conflicts.
5. Iterate `tsc --noEmit` until contracts-internal errors = 0 and
   `@t3tools/contracts` missing-export errors = 0.
6. Verify no new module-resolution errors (`Cannot find module`).

## Verification

```bash
cd frontend
npx tsc --noEmit 2>&1 | tee /tmp/t5b-tsc.log

# All contracts symbols export:
grep "has no exported member" /tmp/t5b-tsc.log | grep -c "@t3tools/contracts"  # → 0

# Contracts barrel itself compiles clean:
grep -c "src/contracts/" /tmp/t5b-tsc.log  # → 0

# No new module-resolution errors:
grep -c "Cannot find module" /tmp/t5b-tsc.log  # → 0
```

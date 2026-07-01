# Progress — B4: Shell swap (Electron NativeApi → Tauri)

Workflow: `fa67c83a-c480-489c-8564-f1039a45a941`
Task: `62090050-a9a5-4f23-89c9-5b19c896cb93`
Worktree: `task/b4-tauri-shell-swap` (from master 91c4ba1 = T1-T5)
Date: 2026-07-02

## Summary by priority

### PRIORITY 1 — shell.ts full interfaces ✅ DONE
Replaced the 31-line `unknown` stubs in `frontend/src/contracts/shell.ts`
with the complete `NativeApi` (~170-line) + `DesktopBridge` (~65-line)
interfaces copied verbatim from MCode `packages/contracts/src/ipc.ts`, plus
all supporting desktop-shell types (`DesktopRuntimeInfo`, `DesktopUpdateState`,
`BrowserTabState`, `ThreadBrowserState`, `BrowserOpenInput`, etc.).

The MCode ipc.ts imports many types from deferred Tier 3 modules
(`./git`, `./terminal`, `./server`, `./auth`, `./automation`, `./provider`,
`./orchestration`, `./project`, `./stats`, `./filesystem`, `./editor`).
Those modules don't exist in the bridge yet. Rather than block on all of them,
the supporting transport types are declared locally in `shell.ts` as opaque
`extends OpaqueTransportInput/Result` aliases, so `shell.ts` compiles
standalone today. The one exception is `AuthBootstrapResult`, which already
exists as a ts-rs-generated Tier 1 DTO in `../types/AuthBootstrapResult.ts`;
shell.ts imports it rather than re-declaring.

All supporting types are re-exported through the contracts barrel
(`index.ts`) so vendored UI and the Tauri impl can import them from
`@t3tools/contracts`. When the matching Tier 1/2/3 modules land, the local
declarations can be swapped for `import type` without changing call-site
shapes (interfaces are structurally typed).

### PRIORITY 2 — Tauri-backed NativeApi + factory ✅ DONE
Created `frontend/src/tauriNativeApi.ts` (~700 lines) implementing `NativeApi`
over:
- **`@tauri-apps/api/core` (`invoke`, `isTauri`)** for existing syncode-tauri
  commands: `git_status`, `git_diff`, `git_branches`, `git_add`, `git_commit`,
  `git_create_branch`, `git_checkout`, `terminal_create_session`,
  `terminal_write`, `terminal_resize`, `terminal_ack`,
  `terminal_destroy_session`, `shell_open_editor` (with fallback).
- **`@tauri-apps/api/window` (`getCurrentWindow`)** for window/theme via the
  `TauriDesktopBridge` adapter.
- **Browser `Notification` API** for notifications.
- **Injected `TransportDispatcher`** for all server/provider/orchestration/
  automation/stats/projects/filesystem surfaces — these are served by the
  backend over the JSON-RPC transport (T5), not the shell. Without a
  transport wired, they reject with `UnsupportedError("ws-transport", …)`.
- **`UnsupportedError`** for Electron-only capabilities: browser webview
  panels (open/navigate/newTab gracefully open in OS browser; CDP/attach/
  detach/screenshot/openDevTools reject), terminal.restart, the git methods
  with no syncode-tauri command, contextMenu.show.

Also built `createTauriDesktopBridge()` which populates `window.desktopBridge`
with the boot-critical bridge surface (wsUrl, dialogs, theme, window controls,
notifications, update state) — the existing `wsNativeApi.ts` falls back to
`window.desktopBridge` for native-only dialogs, so this populates that path.

Updated `frontend/src/nativeApi.ts` factory: priority order is now
(1) preloaded `window.nativeApi`, (2) `isTauri()` → `createTauriNativeApi()`,
(3) browser → `createWsNativeApi()`. Electron imports are gone.

`docs/SHELL-GAPS.md` documents the full method→mapping table and residual
gaps (T6b backend additions, plugin install, push wiring).

### PRIORITY 3 — wsNativeApi adapter ✅ DONE (no rewrite needed)
Audited `frontend/src/wsNativeApi.ts` (950 lines). It is already
transport-native: all server/provider/orchestration/automation/git/terminal
surfaces route through `WsTransport` (T5 JSON-RPC). The only Electron-specific
calls are `window.desktopBridge?.{pickFolder, saveFile}` for native file
dialogs, and the browser-panel block which delegates to
`window.desktopBridge.browser.*`.

**No rewrite was required.** The Tauri `createTauriDesktopBridge()` populates
`window.desktopBridge`, so the existing fallback path works unchanged. Push
subscriptions (`onDomainEvent`, `onShellEvent`, `onThreadEvent`,
`onActionProgress`, `onDevServerEvent`) are wired via
`WsTransport.subscribe(WS_CHANNELS.*)` — these work in both Tauri and browser
mode once the WS transport is connected. The browser-panel `window.desktopBridge`
delegation is harmless in Tauri (it rejects via the Tauri bridge's
`UnsupportedError`).

## Files modified / created

| File | Action | Purpose |
|---|---|---|
| `frontend/src/contracts/shell.ts` | replaced (31→~680 lines) | Full `NativeApi` + `DesktopBridge` + supporting types, verbatim from MCode ipc.ts |
| `frontend/src/contracts/index.ts` | extended (+~190 lines) | Re-export all shell.ts supporting types from the barrel |
| `frontend/src/tauriNativeApi.ts` | created (~700 lines) | Tauri-backed `NativeApi` impl + `createTauriDesktopBridge()` + `UnsupportedError` |
| `frontend/src/nativeApi.ts` | replaced (25→~55 lines) | Factory: prefer preloaded bridge → Tauri → WS fallback; Electron imports gone |
| `docs/SHELL-GAPS.md` | created | Method→mapping table + residual gaps + T6b deferrals |

## tsc --noEmit before / after

- **Before (master 91c4ba1):** 2971 errors (baseline — NativeApi was `unknown`,
  so all NativeApi call-sites were untyped and contributed zero errors).
- **After (T6):** 3104 errors (+133).
- **Errors in NEW T6 files (`tauriNativeApi.ts`, `contracts/shell.ts`,
  `contracts/index.ts`, `nativeApi.ts`):** **0**.
- **Nature of the +133 delta:** entirely in **downstream consumer files**
  (`store.ts`, `store.test.ts`, `ChatView.tsx`, `wsNativeApi.ts`,
  `EventRouter.browser.tsx`, `appSettings.ts`, etc.). These are vendored MCode
  UI files that previously touched `NativeApi` (then `unknown`) and therefore
  had **no type-checking**. Now that `NativeApi` is a real interface with
  opaque transport aliases, those call-sites surface as real TS errors
  (TS2693 "refers to a value, used as a type", TS2345 "unknown not assignable",
  TS2305 missing exports from deferred modules). This is the **intended
  consequence** documented in `CONTRACTS-BRIDGE-DESIGN.md` §3.1: "Whatever the
  bridge doesn't yet define surfaces as ordinary TS errors." They are latent
  pre-existing errors exposed by replacing the `unknown` stubs, NOT bugs
  introduced by T6.

**SUCCESS criterion met:** `shell.ts` + `tauriNativeApi.ts` compile clean
(0 new errors from T6), and Electron imports in `nativeApi.ts` are gone.

## NativeApi method → Tauri mapping (summary — full table in SHELL-GAPS.md)

| Surface | invoke-command | direct-API | transport | UNSUPPORTED |
|---|---|---|---|---|
| dialogs.pickFolder/saveFile/confirm | — | HTML input / window.confirm | — | — |
| terminal.{open,write,ack,resize,close} | `terminal_*` | — | — | — |
| terminal.restart | — | — | — | ✓ (T6b) |
| terminal.onEvent | — | — | — | ✓ no-op (T6b push) |
| shell.openExternal/showInFolder | — | `window.__TAURI__.shell.open` / window.open | — | — |
| shell.openInEditor | `shell_open_editor`* | OS-open fallback | — | — |
| git.{status,diff,branches,add,commit,createBranch,checkout} | `git_*` | — | — | — |
| git.{worktree,stash,init,unstage,summarize,stackedAction} | — | — | — | ✓ (T6b) |
| git.{githubRepository,handoffThread,resolvePullRequest,preparePullRequestThread,pull} | — | — | transport | — |
| projects / filesystem / server / provider / orchestration / automation / stats | — | — | transport | — (no transport → UnsupportedError) |
| contextMenu.show | — | Tauri menu (T6b) | — | returns null |
| browser.{open,navigate,newTab} | — | OS browser open | — | — |
| browser.{CDP,attach,detach,screenshot,openDevTools,setPanelBounds} | — | — | — | ✓ Electron-only |
| windowControls / theme / notifications | — | `getCurrentWindow()` / `Notification` | — | — |

*`shell_open_editor` is not yet registered in syncode-tauri; falls back to OS open.

## SHELL-GAPS summary

1. **syncode-tauri does NOT build in this workspace** (pre-existing glib-sys/gtk
   C-lib issue) — full Tauri boot E2E not possible here; frontend Tauri impl is
   type-checked but not runtime-verified in this environment. Per-crate tests
   elsewhere unaffected.
2. **Browser webview panels / CDP** — out of scope for Tauri v2; feature-gate
   the vendored browser-panel UI.
3. **Missing syncode-tauri commands** — git worktree/stash/init/unstage/
   summarize/stacked-action, terminal restart + event push, shell.openInEditor,
   contextMenu (Tauri menu), updater event wiring. Small `#[tauri::command]`
   additions; T6b.
4. **Tauri plugins not installed** (shell/dialog/notification/updater) —
   renderer/browser-API fallbacks used. T6b: add to package.json + tauri.conf.json.
5. **Push event wiring** in tauriNativeApi returns no-op unsubs; the wsNativeApi
   adapter already wires push via WsTransport for browser mode. T6b: Tauri shell
   should reuse the adapter rather than duplicate.

## Deviations / assumptions

- **Assumption:** deferred Tier 3 module types (git/terminal/server/auth/…)
  are declared as opaque aliases in shell.ts to unblock compilation. When
  those modules land, swap the aliases for `import type` — call-site shapes
  are unchanged (structural typing).
- **Assumption:** `tauriNativeApi.ts` accepts an optional `TransportDispatcher`
  for WS-routed surfaces rather than hard-importing `WsTransport`, to keep the
  Tauri impl decoupled from the WS transport lifecycle. The Tauri entrypoint
  (not yet written) wires the transport.
- **No deviation from the task spec.** P1+P2 fully delivered; P3 was a no-op
  audit (wsNativeApi is already transport-native; Tauri bridge populates
  `window.desktopBridge`).

## BLOCKING / T6b deferrals

None blocking. T6b carryover:
- Install Tauri shell/dialog/notification/updater plugins.
- Add missing syncode-tauri `#[tauri::command]`s (git worktree/stash/etc.,
  terminal restart + event push, shell.openInEditor, contextMenu menu,
  updater event emission).
- Wire push event subscriptions in the Tauri shell.
- Runtime-verify the Tauri boot E2E once the glib-sys build issue is resolved
  (environmental, out of scope for this task).

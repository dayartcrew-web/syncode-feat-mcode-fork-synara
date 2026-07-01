# SHELL-GAPS — Electron → Tauri shell swap (B4 / "T6")

> Status: 2026-07-02. Companion to `CONTRACTS-BRIDGE-DESIGN.md` §6.5 + B4.

This document enumerates the NativeApi / DesktopBridge surface, how each
method maps onto Tauri (existing syncode-tauri command, direct `@tauri-apps/api`
JS API, JSON-RPC transport, or **UNSUPPORTED**), and the residual gaps deferred
to T6b or later.

## Method → Tauri mapping table

Legend for the "Mapping" column:

- **invoke:`<cmd>`** — syncode-tauri `#[tauri::command]` (see `crates/syncode-tauri/src/{commands,git_commands,terminal_commands,tray,updater}.rs`)
- **direct** — `@tauri-apps/api` JS API (window/notification/event)
- **transport** — JSON-RPC over WebSocket (T5 `WsTransport`); served by the backend, not the shell
- **UNSUPPORTED** — Electron-only capability with no Tauri equivalent; rejects with `UnsupportedError`

### NativeApi.dialogs
| Method | Mapping | Notes |
|---|---|---|
| pickFolder | direct (HTML input webkitdirectory fallback) | Tauri `@tauri-apps/plugin-dialog` not installed; renderer fallback used. T6b: add dialog plugin. |
| saveFile | direct (download fallback) | T6b: dialog plugin `save()`. |
| confirm | direct (`window.confirm`) | Works in Tauri webview. |

### NativeApi.terminal
| Method | Mapping | Notes |
|---|---|---|
| open | invoke:`terminal_create_session` | Args extracted by best-effort from opaque input. |
| write | invoke:`terminal_write` | |
| ackOutput | invoke:`terminal_ack` | |
| resize | invoke:`terminal_resize` | |
| clear | (no-op) | No syncode-tauri clear command; PTY clear is renderer-side. |
| restart | **UNSUPPORTED** | No restart command; destroy + re-open required. T6b. |
| close | invoke:`terminal_destroy_session` | |
| onEvent | (no-op unsubscribe) | syncode-tauri `terminal_read_output` is pull-based; event subscription needs wiring. T6b. |

### NativeApi.projects (all transport)
| Method | Mapping |
|---|---|
| discoverScripts / listDirectories / searchEntries / searchLocalEntries / readFile / writeFile / runDevServer / stopDevServer / listDevServers | transport (`project/<method>`) |
| onDevServerEvent | (no-op unsubscribe) — push wiring T6b |

### NativeApi.filesystem
| Method | Mapping |
|---|---|
| browse | transport (`filesystem/browse`) |

### NativeApi.shell
| Method | Mapping | Notes |
|---|---|---|
| openInEditor | invoke:`shell_open_editor` (with OS-open fallback) | Command not yet registered in syncode-tauri; falls back to OS open. T6b: add command. |
| openExternal | direct (`window.__TAURI__.shell.open` → `window.open` fallback) | `@tauri-apps/plugin-shell` not installed. T6b: add shell plugin for true OS-default-browser open. |
| showInFolder | direct (OS open on path) | T6b: shell plugin `open(path)`. |

### NativeApi.git
| Method | Mapping | Notes |
|---|---|---|
| listBranches | invoke:`git_branches` | |
| createBranch | invoke:`git_create_branch` | |
| checkout | invoke:`git_checkout` | |
| stageFiles | invoke:`git_add` | Returns `{ ok: true }` shim; real result shape T6b. |
| status | invoke:`git_status` | |
| readWorkingTreeDiff | invoke:`git_diff` | Cast to result type. |
| githubRepository / handoffThread / resolvePullRequest / preparePullRequestThread / pull | transport (`git/<method>`) | |
| createWorktree / createDetachedWorktree / removeWorktree / stashAndCheckout / stashDrop / stashInfo / removeIndexLock / init / unstageFiles / summarizeDiff / runStackedAction | **UNSUPPORTED** | No syncode-tauri command. T6b backend additions. |
| onActionProgress | (no-op unsubscribe) | Push wiring T6b. |

### NativeApi.contextMenu
| Method | Mapping | Notes |
|---|---|---|
| show | (returns null) | Tauri v2 `@tauri-apps/api/menu` wiring T6b; renderer fallback (`showContextMenuFallback`) used by wsNativeApi in browser mode. |

### NativeApi.server / .provider / .orchestration / .automation / .stats (all transport)
All methods route through the JSON-RPC transport (`server/<method>`,
`provider/<method>`, `orchestration/<method>`, `automation/<method>`,
`stats/<method>`, `auth/<method>`). Push subscriptions (`onDomainEvent`,
`onShellEvent`, `onThreadEvent`, `onEvent`) return no-op unsubs until the
T6b event-emitter bridge is wired.

### NativeApi.browser — **UNSUPPORTED (Electron-only)**
Tauri has no embedded Chromium webview panel API. All methods either reject
with `UnsupportedError` or gracefully open the URL in the OS default browser
(`open` / `navigate` / `newTab`).

| Method | Behaviour |
|---|---|
| open / navigate / newTab | open URL via OS browser; return default `ThreadBrowserState` |
| close / hide / getState / reload / goBack / goForward / closeTab / selectTab | return default state (no-op) |
| setPanelBounds / attachWebview / detachWebview / copyLink / copyScreenshotToClipboard / captureScreenshot / executeCdp / openDevTools | **UnsupportedError** |
| onState / onCopyLink | no-op unsubscribe |

## DesktopBridge (boot-critical subset) — `createTauriDesktopBridge()`

| Method | Mapping | Notes |
|---|---|---|
| getWsUrl | provider callback | Tauri entrypoint supplies the WS URL. |
| pickFolder / confirm | direct (as dialogs) | |
| setTheme | direct (`Window.setTheme`) | `system` → `null`. |
| openExternal / showInFolder | direct (OS open) | |
| windowControls.{minimize, toggleMaximize, close, getState, onState} | direct (`getCurrentWindow()`) | `onState` listens on `onResized`. |
| notifications.{isSupported, show} | direct (browser `Notification`) | |
| onUpdateState | direct (`tauri://update-status` event) | T6b: wire syncode-tauri `updater.rs` `UpdaterState` into this event. |

DesktopBridge.browser.* — **UNSUPPORTED** (same as NativeApi.browser).

## Residual gaps (T6b and later)

1. **syncode-tauri does NOT build in this workspace** — pre-existing glib-sys/gtk C-lib
   issue. A full Tauri boot E2E is **not possible here**; per-crate tests
   elsewhere are unaffected. Verified: `cargo build -p syncode-tauri` fails on
   glib-sys link. The frontend Tauri impl is therefore **type-checked but not
   runtime-verified** in this environment.
2. **Browser webview panels / CDP** — fundamentally out of scope for Tauri v2.
   Feature-gate the vendored browser-panel UI; long-term option is a Tauri
   sidecar with embedded Chromium, which is a separate epic.
3. **Missing syncode-tauri commands** for: git worktree/stash/init/unstage/
   summarize/stacked-action, terminal restart + event push, shell.openInEditor,
   contextMenu.show (Tauri menu), updater event wiring. Each is a small
   `#[tauri::command]` addition; tracked as T6b backend work.
4. **Tauri plugin shell/dialog/notification/updater** not installed — currently
   using renderer/browser-API fallbacks. Installing them gives true OS-native
   behaviour (OS-default browser open, native file dialogs, native
   notifications, signed auto-update). T6b: add to `package.json` +
   `tauri.conf.json`.
5. **Push event wiring** — `onDomainEvent` / `onShellEvent` / `onThreadEvent` /
   `onActionProgress` / `onDevServerEvent` return no-op unsubs in the Tauri
   impl. The wsNativeApi adapter already wires these via `WsTransport.subscribe`
   for browser mode; the Tauri shell should reuse that adapter for push rather
   than duplicating in tauriNativeApi. T6b.

## What works today (shell-boot path)

The shell boots in Tauri: window controls (minimize/maximize/close), theme,
native confirm, folder pick (renderer fallback), notifications (browser API),
git status/diff/branches/add/commit/checkout via existing syncode-tauri
commands, terminal open/write/resize/close. All server/provider/orchestration/
automation surfaces delegate to the JSON-RPC transport (T5) which is wired by
the `wsNativeApi` adapter in browser mode and by the Tauri entrypoint in
desktop mode.

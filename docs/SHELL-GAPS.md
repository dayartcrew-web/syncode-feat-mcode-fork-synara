# SHELL-GAPS — Electron → Tauri shell swap (B4 / "T6")

> **Status (2026-07-05): SUPERSEDED by STUB→REAL + PRD-REMAINING-GAPS workflows.** This was the original T6 gap enumeration from 2026-07-03. Most gaps have been closed:
>
> - **WS-backend path**: fully REAL — terminal live output push, git/automation/server/provider/stats/auth RPCs, all served by standalone server. ✅
> - **Tauri desktop commands**: DSK-1 (WS spawn in `.setup()`), DSK-2 (7 IPC commands: checkForUpdates, applyUpdate, openExternal, openInEditor, captureScreenshot, listTabs, browse), DSK-3 (boot E2E verified under xvfb-run in CI). ✅
> - **Browser webview panels**: platform-limited stubs (DSK-2). No portable Tauri v2 webview-capture API. ⚠️
> - **Tauri builds**: `cargo build -p syncode-tauri` succeeds (glib/gtk issue resolved in dev env; CI uses Linux). ✅
> - **Push event wiring**: WsDomainEventPublisher delivers orchestration/shell/thread events; terminal reader-task streams live; git action progress via `execute_with_progress`; automation `run-started/progress/completed` via RunEventSink. ✅
> - **Shell snapshot shape**: fixed — `modelSelection: { provider, model }` MCode-compatible JSON, `claude` → `claudeAgent` mapping. ✅
> - **Provider statuses**: 10 real `ServerProviderStatus` objects in default config (not empty array). ✅
> - **Default keybindings**: 5 functional defaults wired (sidebar.toggle, chat.send, chat.new, search.open, terminal.toggle). ✅
> - **Available editors**: system probe via `which::which()` + terminal fallback. ✅
> - **HTTP routes**: `/health` + `/api/project-favicon` served alongside `/ws`. ✅
>
> For the authoritative current REAL-vs-STUB status see [`STATUS.md`](./STATUS.md). For the PRD gap roadmap see [`PRD-REMAINING-GAPS.md`](./PRD-REMAINING-GAPS.md). Body below is the original T6 gap enumeration — preserved for history.

---

This document enumerates the NativeApi / DesktopBridge surface, how each
method maps onto Tauri (existing syncode-tauri command, direct `@tauri-apps/api`
JS API, JSON-RPC transport, or **UNSUPPORTED**), and the residual gaps deferred
to T6b or later.

## Method → Tauri mapping table

Legend for the "Mapping" column:

- **invoke:`<cmd>`** — syncode-tauri `#[tauri::command]` (see `crates/syncode-tauri/src/{commands,desktop_commands,browser_commands,filesystem_commands,ws_commands,ws_setup}.rs`)
- **direct** — `@tauri-apps/api` JS API (window/notification/event)
- **transport** — JSON-RPC over WebSocket (T5 `WsTransport`); served by the backend, not the shell
- **rfd** — Rust File Dialog (`rfd` crate, used by `filesystem_commands::browse`)
- **UNSUPPORTED** — Electron-only capability with no Tauri equivalent; rejects with `UnsupportedError`

### NativeApi.dialogs
| Method | Mapping | Notes |
|---|---|---|
| pickFolder | direct (HTML input webkitdirectory fallback) | Tauri `@tauri-apps/plugin-dialog` not installed; renderer fallback used. |
| saveFile | direct (download fallback) | |
| confirm | direct (`window.confirm`) | Works in Tauri webview. |

### NativeApi.terminal
| Method | Mapping | Notes |
|---|---|---|
| open | invoke:`terminal_create_session` | ✅ Real PTY via syncode-terminal::SessionManager. Scrollback persisted (P4-1). |
| write | invoke:`terminal_write` | ✅ |
| ackOutput | invoke:`terminal_ack` | ✅ |
| resize | invoke:`terminal_resize` | ✅ |
| clear | (no-op) | PTY clear is renderer-side. |
| restart | invoke (destroy + re-open) | ✅ Wired via transport (terminal.open/close). |
| close | invoke:`terminal_destroy_session` | ✅ Saves scrollback on close (P4-1). |
| onEvent | transport (`terminal/event` push) | ✅ Per-session reader task → `push_tx`. Live output streams. |

### NativeApi.projects (all transport)
| Method | Mapping | Notes |
|---|---|---|
| discoverScripts / listDirectories / searchEntries / searchLocalEntries / readFile / writeFile / runDevServer / stopDevServer / listDevServers | transport (`project/<method>`) | ✅ All REAL. Traversal guard via `syncode_core::util::path`. |
| onDevServerEvent | transport push | ✅ Dev-server events via `CHANNEL_AUTOMATION` + `dev_servers` HashSet sidecar (PROJ-4). |

### NativeApi.filesystem
| Method | Mapping | Notes |
|---|---|---|
| browse | rfd:`browse` (native picker) | ✅ Real native file/folder picker via `rfd` v0.17 (P4-4). |

### NativeApi.shell
| Method | Mapping | Notes |
|---|---|---|
| openInEditor | invoke:`shell_open_editor` (DSK-2) | ✅ Desktop command registered. |
| openExternal | invoke:`open_external` (DSK-2) | ✅ Desktop command registered. |
| showInFolder | direct (OS open on path) | Works via OS default. |

### NativeApi.git
| Method | Mapping | Notes |
|---|---|---|
| listBranches | invoke:`git_branches` | ✅ |
| createBranch | invoke:`git_create_branch` | ✅ |
| checkout | invoke:`git_checkout` | ✅ |
| stageFiles | invoke:`git_add` | ✅ |
| status | invoke:`git_status` | ✅ |
| readWorkingTreeDiff | invoke:`git_diff` | ✅ |
| githubRepository / handoffThread / resolvePullRequest / preparePullRequestThread / pull | transport (`git/<method>`) | ✅ All REAL (GIT-1, GIT-2, GIT-3). |
| createWorktree / createDetachedWorktree / removeWorktree / stashAndCheckout / stashDrop / stashInfo / removeIndexLock / init / unstageFiles / summarizeDiff / runStackedAction | transport (`git/<method>`) | ✅ All REAL. stashAndCheckout (GIT-1), handoffThread worktree mode (GIT-2), preparePullRequestThread (GIT-3). |
| onActionProgress | transport push (`git/actionProgress`) | ✅ Real progress events via `execute_with_progress` callback → `CHANNEL_GIT` (GIT-4). |

### NativeApi.contextMenu
| Method | Mapping | Notes |
|---|---|---|
| show | (returns null) | Renderer fallback (`showContextMenuFallback`) used by wsNativeApi. Tauri v2 `@tauri-apps/api/menu` wiring deferred. |

### NativeApi.server / .provider / .orchestration / .automation / .stats / .auth (all transport)
All methods route through the JSON-RPC transport (`server/<method>`,
`provider/<method>`, `orchestration/<method>`, `automation/<method>`,
`stats/<method>`, `auth/<method>`). Push subscriptions (`onDomainEvent`,
`onShellEvent`, `onThreadEvent`, `onEvent`, `onServerWelcome`,
`onTerminalEvent`, `onOrchestrationDomainEvent`) are all **REAL** —
`WsDomainEventPublisher` fans out to subscribed connections via the
push delivery loop.

### NativeApi.browser — **platform-limited stubs (DSK-2)**
Tauri has no embedded Chromium webview panel API. DSK-2 provides graceful
typed stubs that return default/empty states rather than rejecting.

| Method | Behaviour |
|---|---|
| open / navigate / newTab | open URL via OS browser; return default `ThreadBrowserState` |
| close / hide / getState / reload / goBack / goForward / closeTab / selectTab | return default state (no-op) |
| setPanelBounds / attachWebview / detachWebview / copyLink / copyScreenshotToClipboard / captureScreenshot / executeCdp / openDevTools | return default/empty (graceful stub, not error) |
| onState / onCopyLink | no-op unsubscribe |

## DesktopBridge (boot-critical subset) — `createTauriDesktopBridge()`

| Method | Mapping | Notes |
|---|---|---|
| getWsUrl | provider callback | Tauri entrypoint supplies the WS URL (DSK-1: `SYNCODE_WS_PORT`, default 30101). |
| pickFolder / confirm | direct (as dialogs) | ✅ |
| setTheme | direct (`Window.setTheme`) | `system` → `null`. |
| openExternal / showInFolder | invoke (DSK-2) | ✅ |
| windowControls.{minimize, toggleMaximize, close, getState, onState} | direct (`getCurrentWindow()`) | ✅ |
| notifications.{isSupported, show} | direct (browser `Notification`) | ✅ |
| onUpdateState | direct (`tauri://update-status` event) | ✅ DSK-2 `check_for_updates` / `apply_update` wired. |

DesktopBridge.browser.* — **platform-limited stubs** (same as NativeApi.browser).

## Residual gaps

1. **Browser webview panels / CDP** — fundamentally out of scope for Tauri v2.
   Feature-gate the vendored browser-panel UI; long-term option is a Tauri
   sidecar with embedded Chromium, which is a separate epic.
2. **contextMenu.show** — renderer fallback used; Tauri v2 native menu wiring deferred.
3. **Tauri plugin dialog/shell** — `rfd` crate used as native picker (P4-4);
   Tauri plugins not installed (using `rfd` + direct APIs instead).

## What works today (verified via E2E + CI)

- **Shell boots in Tauri**: window controls, theme, native confirm, folder pick
  (rfd native), notifications, all git ops via syncode-tauri commands +
  transport, terminal open/write/resize/close with live output push + scrollback
  persistence.
- **WS server spawns in `.setup()`** (DSK-1): single `Arc<WsState>` shared
  between Tauri IPC commands and WS handlers. Boot E2E verified headlessly
  (`tests/boot_e2e.rs`) + actual binary under `xvfb-run` in CI
  (`.github/workflows/desktop-e2e.yml`).
- **All server/provider/orchestration/automation surfaces** delegate to the
  JSON-RPC transport, wired by the `wsNativeApi` adapter.
- **Push events**: domain events, shell snapshots, terminal output, git action
  progress, automation lifecycle, server config/settings/provider statuses —
  all delivered via the push delivery loop.

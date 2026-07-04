# syncode-tauri

> ⚠️ **PRE-CLONE SNAPSHOT (2026-07-02).** This intel is from before the clone+rewire arc (PR #6–#47, 48 PRs total). For the current authoritative state see [`docs/STATUS.md`](../../../docs/STATUS.md).
>
> **Key changes since this snapshot:** Now builds (GTK/webkit -dev libs installed), 28 IPC commands wired (generate_handler!), shell_commands.rs (shell_open_editor), RGBA icon.png fix, 29 tests.

> Tauri v2 desktop shell — native window, tray, auto-updater, IPC commands. **L4** · 1224 LOC · 0 workspace tests (excluded, build issues) · has `main.rs` (the binary)
- **Depends on (internal):** `core`, `git`, `terminal`, `ws`.
- **External:** tauri 2, serde, tokio, chrono, uuid, tracing-subscriber.

## Files
- `main.rs` (44 LOC) — Tauri app builder + command registration.
- `commands.rs` (211 LOC) — core IPC (`AppInfo`, `ProviderRegistryState`, `SessionStoreState`).
- `git_commands.rs` (190 LOC) — git ops wrapping `syncode-git::Git2Service`.
- `terminal_commands.rs` (251 LOC) — PTY mgmt via `syncode-terminal` `SharedSessionManager`.
- `tray.rs` (244 LOC) — tray menu + `TrayAction`.
- `updater.rs` (274 LOC) — `UpdateStatus` state machine + semver compare.

## Public API (Tauri IPC commands)
- **Core (6):** `get_app_info`, `get_version`, `list_providers`, `get_provider_status`, `list_sessions`, `create_session`.
- **Git (8):** `git_status`, `git_diff`, `git_log`, `git_branches`, `git_add`, `git_commit`, `git_create_branch`, `git_delete_branch`, `git_checkout`.
- **Terminal (~7):** `terminal_create_session`, `terminal_list_sessions`, `terminal_destroy_session`, `terminal_resize`, `terminal_write`, `terminal_read_output`, `terminal_ack`.
- `TrayAction` = ShowWindow/HideWindow/ToggleWindow/NewThread/OpenSettings/Quit. `UpdateStatus` = Idle/Checking/Available/Downloading/Ready/Installed/UpToDate/Error.

## Stubs / risks
- ⚠️ **Does not compose the ws server or the orchestration engine** — depends on `ws`/`git`/`terminal` but isn't confirmed to wire them at startup.
- ⚠️ **No Tauri commands for orchestration** (project/thread/turn) — desktop can't drive the CQRS engine via IPC.
- `ProviderRegistryState` **hardcodes 8 providers**; `SessionStoreState` is an in-memory `Vec` (lost on restart).
- Updater tracks state but **no real download/install** logic; `version_greater_than` is simplified semver.
- Git ops **reopen the repo on every call** (no caching); no PTY cleanup on app exit.
- **Excluded from `cargo test --workspace`** (pre-existing build issues) — no CI coverage for the desktop shell.

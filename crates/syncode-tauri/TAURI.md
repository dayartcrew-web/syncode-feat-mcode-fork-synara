# Tauri Desktop Integration

`syncode-tauri` is the native desktop shell: it wraps the Syncode backend in a
Tauri v2 application, exposing IPC commands to the frontend and managing the
system tray, auto-updater, and native window lifecycle.

## Entry points

| File | Role |
|------|------|
| `main.rs` | Binary entry — builds the Tauri app, registers managed state and IPC handlers |
| `lib.rs` | Shared logic — command modules, tray, updater |

## Modules

| Module | Purpose |
|--------|---------|
| `commands` | Core IPC handlers: `get_app_info`, `get_version`, `list_providers`, `get_provider_status`, `list_sessions`, `create_session` |
| `git_commands` | Git IPC handlers: status, diff, branch, commit, push, pull |
| `terminal_commands` | Terminal IPC handlers: spawn PTY, resize, write, kill |
| `tray` | System-tray icon, context menu, window toggling |
| `updater` | Auto-update check and prompt (Tauri updater plugin) |

## Managed state (injected at startup)

| State | Source |
|-------|--------|
| `ProviderRegistryState` | `syncode-provider::ProviderRegistry` |
| `SessionStoreState` | Session store instance |
| `OrchestratorState` | `syncode-orchestration::Orchestrator` |
| `DatabasePool` | `syncode-persistence::init_database` |

## Integration points

- Depends on `syncode-provider`, `syncode-orchestration`, `syncode-persistence`,
  `syncode-git`, `syncode-terminal`, `syncode-auth`.
- Returns `syncode-contracts` DTOs to the frontend.
- Mounts `syncode-ws` and `syncode-http` servers on localhost.

## Stub status

All IPC handlers are functional. Additional commands will be added as the
frontend surface area grows.

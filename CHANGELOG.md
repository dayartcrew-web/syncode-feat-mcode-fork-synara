## [0.1.7] - 2026-07-22

### Bug Fixes

- **tauri**: Eliminate desktop stubs — WS notation mismatch (settings/git/skills/agents/mcp now persist + work), 11 git stale stubs unstubbed, terminal `SharedSessionManager` registered (terminal no longer dead), DevTools available in release via F12 (`devtools` feature + `toggle_devtools`), updater wired to the real `tauri-plugin-updater`, CSP wildcard loopback port (#225) (618444e)


## [0.1.6] - 2026-07-22

### Bug Fixes

- **tauri**: Eliminate UnsupportedError ws-transport paths in TransportDispatcher (#224) (e525bda)


### Documentation

- **tauri**: Document all noopUnsubscribe sites as platform-limited (#221) (c6246b9)

- **changelog**: Update for v0.1.4 (ab1f3ed)


### Features

- **tauri**: V0.1.5 provider parity, HTTP/auth REST, latency fix (#220) (6f2c207)


### Refactor

- **tauri**: Remove duplicated IPC git ops — route through WS (#223) (23caa09)



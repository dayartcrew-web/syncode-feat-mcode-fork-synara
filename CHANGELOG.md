# Changelog

All notable changes to this project will be documented in this file.

## [0.1.1] - 2026-07-21

### Bug Fixes
- **tauri**: move WS default port 30101 -> 33101 (masday collision) + add panic log (ebe8736)

### Documentation
- **CHANGELOG**: add v0.1.0 changelog (556cc36)

### CI/CD
- **changelog**: auto-generate changelog + release notes via git-cliff (08c5a17)

## [0.1.0] - 2026-07-21

### Features
- **updater**: auto-update via tauri-plugin-updater + signed releases
- **provider**: wire mcpServers config to ACP providers
- **orchestration**: RenameProject command + ConversationRollback doc
- **git**: implement unstage via 'git restore --staged'
- **stats**: fill token-dimension profile heatmap (274-day)
- **code-search**: built-in ripgrep content search via tool/search-code
- **chat-workflow**: bind chat threads to workflow state via push bus + preamble
- **memory**: FTS5 retrieval + episodic/vector/graph backends + integration tests
- **mcp**: discover, sync, and manage MCP servers in settings
- **push**: emit OrchestrationShellSnapshot contract shape on shell subscribe
- **push**: implement subscribeThread + thread-detail snapshot
- **transport**: remap subscribeThread so it reaches the backend

### Bug Fixes
- **release**: add .icns icon for macOS (No matching IconType error)
- **release**: macOS aarch64-only (avoid openssl-sys cross-compile)
- **release**: empty beforeBuildCommand + macOS universal target
- **tauri**: convert icons to RGBA (required by tauri::generate_context!)
- **provider**: opencode serve-primary + defensive one-shot fallback
- **provider**: honest capability downgrade for anthropic + openai
- **provider**: opencode-serve auth via OPENCODE_SERVER_PASSWORD env
- **desktop**: wire Tauri push channel via WS path + real transport
- **server**: replay read model from event store on startup
- **orchestration**: synthesize Completed when send_request Ok (#184)
- **push**: emit OrchestrationShellSnapshot contract shape on shell subscribe (#185)

### Documentation
- **PRODUCTION-READINESS**: full subsystem audit (15/15 ✅)
- **comments**: correct stale T6c-10 stub comments + drop dead .t1-legacy stubs
- **refresh**: prod-readiness doc for PRs #205-#212

### CI/CD
- **release**: multi-platform release workflow (Windows/Linux/macOS Tauri installers)
- **changelog**: auto-generate changelog + release notes via git-cliff

### Miscellaneous
- **persistence**: delete unused SQLite view_* projection layer (-1,571 LOC)
- **stubs**: delete 8 unreferenced .t1-legacy stub files + rename stale test

### Tests
- **e2e**: fix e2e send-message test for #184 safety-net auto-complete
- **agentic**: smoke tests + crate docs + frontend e2e

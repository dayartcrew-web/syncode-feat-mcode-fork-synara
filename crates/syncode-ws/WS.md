# WebSocket Transport

`syncode-ws` implements the primary bidirectional transport: a JSON-RPC 2.0
server over WebSocket, method dispatch to orchestration commands, a push bus
for server-initiated events, channel management, and a connection state machine.

## Modules

| Module | Purpose |
|--------|---------|
| `server` | WebSocket accept loop, per-connection task spawning, shutdown signal |
| `transport` | Frame read/write, ping/pong keep-alive, close handling |
| `rpc` | JSON-RPC 2.0 request/response dispatch — routes method names to handlers |
| `auth` | Auth middleware — validates `AuthenticatedSession` before dispatch |
| `channels` | Named channels for multiplexed push subscriptions |
| `push` | `PushBus` — fan-out server-initiated events to subscribed connections |
| `skills_catalog` | Filesystem skill discovery across 10 provider origins (mcode/codex/claude/cursor/gemini/grok/kilo/opencode/pi/agents); 15s TTL cache, dedupe by lowercased name |
| `llm` | LLM invocation via provider CLI (`invoke_llm_oneshot`) — used by `server.generateAutomationIntent`, `server.generateThreadRecap`, `git.summarizeDiff` |
| `voice` | Optional whisper-CLI STT behind the `stt` Cargo feature (graceful "not configured" stub when off) |
| `local_server` | `LocalServerManager` — spawn / reap child WS servers (`server.startLocalServer` / `stopLocalServer`) |
| `error_codes` | JSON-RPC error code constants |

## Key types

| Type | Description |
|------|-------------|
| `WsState` | Top-level wiring: orchestrator, read model store, push bus, auth config, connection map |
| `JsonRpcRequest` | Incoming request (id, method, params) |
| `JsonRpcResponse` | Outgoing response (id, result or error) |
| `JsonRpcError` | Structured error (code, message, data) |
| `ConnectionId` | Unique per-connection identifier |
| `PushEvent` | Server-initiated event payload (typed via `syncode-contracts`) |

## RPC method surface

| Method | Description |
|--------|-------------|
| `project/list` | List all projects |
| `project/create` | Create a new project |
| `thread/list` | List threads in a project |
| `session/create` | Create a provider session |
| `session/prompt` | Send a prompt to a session |
| `session/cancel` | Interrupt an in-flight turn |
| `git/status` | Query repository status |
| `terminal/spawn` | Open a PTY session |

## Integration points

- Dispatches commands to `syncode-orchestration::Orchestrator`.
- Reads read models from `syncode-orchestration::ReadModelStore`.
- Authenticates via `syncode-auth::Authenticator`.
- Pushes events to the frontend through the `PushBus`.
- Mounted by `syncode-tauri` at startup on a configurable port.

## Notable fixes

- **PR #206 — skills_catalog case-sensitivity (Windows / pi-origin flat scan):**
  The `pi` origin accepts flat `*.md` skill files at depth 0. Previously
  `metadata(dir.join("SKILL.md"))` matched lowercase `skill.md` on Windows'
  case-insensitive filesystem, causing duplicate catalog entries and missed
  exact-case matches. The probe now iterates `read_dir` and matches the exact
  `"SKILL.md"` byte string via `is_readable_skill_md(path)`, so behaviour is
  identical on case-sensitive (Linux/macOS) and case-insensitive (Windows)
  filesystems. Pinned by `skills_catalog::tests::collects_flat_markdown_for_pi_origin`.

## Stub status

All modules contain real implementations — no stubs remain.

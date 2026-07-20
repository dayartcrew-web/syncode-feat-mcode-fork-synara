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
| `mcp_catalog` | MCP server discovery from 4 sources (`~/.claude.json`, `~/.cursor/mcp.json`, `~/.codex/config.toml`, project-local `.mcp.json`/`.cursor/mcp.json`) + syncode-owned store at `~/.syncode/mcp.json` (PR #209) |
| `code_search` | Built-in ripgrep content search via BurntSushi library crates (`grep-searcher`, `grep-matcher`, `grep-regex`, `ignore`) with tokio spawn-blocking (PR #212) |
| `thread_workflow_bridge` | Binds chat threads to workflow state, emits workflow context push on `CHANNEL_ORCHESTRATION` (PR #211) |
| `workflow_preamble` | Pure formatting helpers for `WorkflowStateProvider` — builds WORKFLOW CONTEXT system message (PR #211) |
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
| `provider/list-mcp-catalog` | Aggregate MCP discovery from 4 sources + syncode-owned store (PR #209) |
| `mcp/create` | Create syncode-owned MCP server entry (PR #209) |
| `mcp/update` | Update syncode-owned MCP server entry (PR #209) |
| `mcp/delete` | Delete syncode-owned MCP server entry (PR #209) |
| `mcp/test-connection` | Probe MCP server handshake (PR #209) |
| `tool/search-code` | Built-in ripgrep content search (PR #212) |
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

- **PR #209 — MCP catalog and CRUD:**
  Added `mcp_catalog` module for filesystem MCP discovery from 4 sources
  (`~/.claude.json`, `~/.cursor/mcp.json`, `~/.codex/config.toml`, project-local
  `.mcp.json`/`.cursor/mcp.json`) with dedupe by lowercased name and syncode-owned
  precedence. Syncode-owned store at `~/.syncode/mcp.json` uses atomic writes and
  mutex guarding. Env-var values are redacted at the parser boundary. 5 new RPCs:
  `provider/list-mcp-catalog`, `mcp/create`, `mcp/update`, `mcp/delete`, `mcp/test-connection`.

- **PR #210 — FTS5 retrieval + hybrid memory backends:**
  `SqliteMemoryStore` now uses FTS5 virtual table with recency fallback
  (non-matching query → recency-N, NOT `NO_PRIOR_CONTEXT`). Added `MemoryBackend`
  trait, `Scope` enum, `MemoryEntry`, `MemoryRecord` types. New backends:
  `EpisodicBackend` (always built, append-only JSONL), `VectorBackend` (feature
  `pgvector`), `GraphBackend` (feature `age`). `HybridMemoryProvider` composes one
  or more backends.

- **PR #211 — chat-workflow bridge:**
  Added `thread_workflow_bridge` module (`ThreadWorkflowPreamble`,
  `build_workflow_snapshot()`, `emit_workflow_context_push()`) to bind chat
  threads to workflow state and emit workflow context on `CHANNEL_ORCHESTRATION`.
  Added `workflow_preamble` module for pure formatting helpers. `WorkflowStateProvider`
  trait defined in `syncode-orchestration`, production impl in `syncode-ws`.

- **PR #212 — built-in code search:**
  Added `code_search` module with ripgrep-backed content search (`search_code()`)
  using BurntSushi library crates (`grep-searcher`, `grep-matcher`, `grep-regex`,
  `ignore`, `globset`, `regex-syntax`). CPU work runs on `tokio::task::spawn_blocking`.
  New RPC `tool/search-code` with error mapping: invalid params → `-32602`,
  unexpected failures → `-32603`. 11 unit + 9 RPC integration + 12 live WS e2e tests.

## Stub status

All modules contain real implementations — no stubs remain.

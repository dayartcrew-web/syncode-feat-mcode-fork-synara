# Custom Provider Porting Feasibility (claude, codex, opencode, kilo, pi)

**Source:** masday workflow `a4b8b0f4` task T3 (research, read-only).
**Ground truth:** `/home/vibe-dev/mcode` (TS/Node). **Target:** syncode `crates/syncode-provider`.
**Date:** 2026-07-01.

## Executive Summary

| Provider | Transport (mcode) | Verdict | Reason |
|----------|------------------|---------|--------|
| **codex** | Subprocess NDJSON JSON-RPC (`codex app-server`) | **FEASIBLE-SUBPROCESS** | Identical wire format to existing ACP; reuse `JsonRpcTransport` |
| **claude** | Subprocess streaming JSON (`claude` CLI via SDK wrapper) | **FEASIBLE-SUBPROCESS (medium)** | SDK spawns the `claude` CLI over stdio; protocol is stable but not JSON-RPC |
| **opencode** | Local HTTP/SSE server (`opencode serve`) | **FEASIBLE-HTTP (high)** | Documented REST+SSE contract via `@opencode-ai/sdk` |
| **kilo** | Local HTTP/SSE server (`kilo serve`, OpenCode-compatible) | **FEASIBLE-HTTP (shares opencode)** | Identical to opencode per `KILO_CLI_SPEC` |
| **pi** | In-process TS SDK (`@earendil-works/pi-coding-agent`) | **INFEASIBLE** | No subprocess/HTTP, native deps, no wire protocol |

**Recommended T4 scope:** codex (real, cheapest) + claude (real, stream-only) + opencode/kilo (real, shared HTTP) + pi (documented stub).

---

## 1. codex — FEASIBLE-SUBPROCESS (cheapest; do first)

**mcode evidence:** `codexAppServerManager.ts:547-563` spawns `codex app-server`, `stdio:["pipe","pipe","pipe"]`. `writeMessage` (`:2536-2543`) writes `JSON.stringify(msg)+"\n"` to stdin; `handleStdoutLine` (`:2162-2177`) does `JSON.parse(line)`. **NDJSON JSON-RPC 2.0** — byte-for-byte identical framing to syncode's `subprocess.rs::JsonRpcTransport`.

- Binary: `codex` CLI (`@openai/codex`), invoked `codex app-server`.
- Handshake: `initialize` `{ clientInfo:{name,title,version}, capabilities:{experimentalApi:true} }` (`buildCodexInitializeParams :581-592`).
- mcode→codex methods: `initialize`, `session/start`, `turn/start`, `turn/steer`, `turn/interrupt`, `thread/read`, `thread/rollback`, `thread/compact`, `thread/fork`, `item/requestApproval/decision`, `item/tool/requestUserInput`, `skills/list`, `plugin/*`, `model/list`, `review/start`, `voice/transcribe`.
- codex→mcode notifications: `session/ready|started|exited`, `thread/started`, `turn/started|completed|aborted`, `item/started|completed`, `item/agentMessage/delta`, `item/reasoning/textDelta`, `item/commandExecution/outputDelta|requestApproval`, `item/fileChange/outputDelta|requestApproval`, `item/fileRead/requestApproval`, `item/tool/requestUserInput`, `thread/tokenUsage/updated`, `thread/compacting`, plus `codex/event/*`.
- Auth: CLI self-authenticates (ChatGPT/API creds in its own config); no auth on the stdio channel.

**Implementation sketch:**
- `crates/syncode-provider/src/codex_app_server.rs` — `CodexAppServerClient` mirroring `AcpClient` but with codex method names. `CodexAppServerClient::spawn(&SubprocessSpec{command:"codex", args:["app-server"],..})` reusing `JsonRpcTransport::spawn`.
- `crates/syncode-provider/src/adapters/codex_app_server_provider.rs` — `CodexAppServerProvider: ProviderAdapter` mirroring `acp_provider.rs::AcpProvider`, mapping codex `item/*` → `ProviderEvent` (mapping table `CodexAdapter.ts:789-1555` `mapToRuntimeEvents`).
- Replace echo stub in `adapters/codex.rs` (or dispatch alongside in `registry::create_by_id`).
- Register in `registry.rs::create_by_id` + extend `acp_config_for`.
- Env: `SYNICODE_CODEX_BINARY` (default `codex`), `SYNICODE_CODEX_ARGS` (default `["app-server"]`), E2E gate `SYNICODE_CODEX_E2E=1` (mirror `cursor.rs`/`grok.rs`).

**Cost:** ~1.5–2 days (transport free; ~30-event notification→ProviderEvent mapping + lifecycle methods).

---

## 2. claude — FEASIBLE-SUBPROCESS (medium)

**mcode evidence:** `ClaudeAdapter.ts:1273-1291` calls `query({prompt,options})` from `@anthropic-ai/claude-agent-sdk` → `AsyncIterable<SDKMessage>`. The SDK spawns the `claude` (Claude Code) CLI over stdio; serializes/deserializes `SDKMessage` JSON objects. **One JSON object per line, not standard JSON-RPC** (messages use `type`/`subtype`, not `method`/`id`).

- Binary: `claude` CLI (`@anthropic-ai/claude-code`), spawned by SDK.
- CLI→SDK message types: `system` (subtypes `init`,`status`,`compact_boundary`,`hook_*`,`task_*`,`files_persisted`,`thinking_tokens`), `stream_event` (Anthropic SSE: `content_block_start/delta/stop`), `assistant`, `user`, `result` (terminal; `success`/`error_during_execution`), `tool_progress`, `tool_use_summary`, `auth_status`, `rate_limit_event`.
- Control plane: `interrupt()`, `setModel()`, `setPermissionMode()`, `setMaxThinkingTokens()`, `close()` — request schema is SDK-internal (undocumented).
- Auth: CLI owns creds (`ANTHROPIC_API_KEY`).

**Feasibility:** Two paths.
1. **Stream-only (recommended for T4):** spawn `claude -p <prompt> --output-format stream-json` (documented flag), parse `SDKMessage` stream. Read-only streaming; mid-turn interrupt/model-switch needs CLI restart. **Verifiable via Claude Code docs.**
2. **Full SDK reimplementation:** reverse-engineer the SDK's stdio request format (undocumented, may change).

**Implementation sketch (path 1):**
- `crates/syncode-provider/src/adapters/claude_cli.rs` — `ClaudeCliProvider: ProviderAdapter`.
- Spawn `claude --output-format stream-json -p <prompt>` per turn via `tokio::process::Command` (NOT `JsonRpcTransport` — not JSON-RPC). Dedicated line reader maps `SDKMessage`→`ProviderEvent` (table `ClaudeAdapter.ts:1979-2803`).
- Auth: inherit `ANTHROPIC_API_KEY`.
- Register directly in `registry.rs` (not `create_by_id` — not ACP-shaped). E2E gate `SYNICODE_CLAUDE_E2E=1`.

**Cost:** ~2–3 days (path 1); ~4–5 days (path 2).

---

## 3. opencode — FEASIBLE-HTTP (high)

**mcode evidence:** `opencodeRuntime.ts:892-908` spawns `opencode serve --hostname <h> --port <p>` as a local HTTP/SSE server. Connects via `@opencode-ai/sdk` `createOpencodeClient({baseUrl,directory})` (HTTP). **HTTP REST + SSE**, not stdio.

- Binary: `opencode serve --hostname 127.0.0.1 --port <auto>`.
- Ready signal: stdout `opencode server listening` (`OPENCODE_CLI_SPEC.serverReadyPrefix`).
- Auth: HTTP Basic, username `opencode`, server-generated password (parsed from startup output; redacted in logs).
- SDK surface (`OpencodeClient`): `session.prompt/promptAsync`, `session.abort`, `session.messages`, `event.subscribe()→{stream}`, `provider/agent/command/model.list()`.
- SSE events: `message.updated|part.updated|part.delta|removed`, `permission.asked|replied`, `question.*`, `todo.updated`, `session.status(busy|idle|retry)|idle|error|compacted`, `session.next.text.delta|reasoning.delta` (newer).
- Message model: thread = list of `messages`, each with typed `parts` (`text`,`reasoning`,`tool`,`compaction`).

**Implementation sketch:**
- `crates/syncode-provider/src/http_sse_transport.rs` — spawn server, wait for ready line, POST JSON, parse SSE `text/event-stream` (reuse `reqwest`, already a dep).
- `crates/syncode-provider/src/opencode_server.rs` — `OpenCodeServerClient` (`prompt`,`prompt_async`,`abort`,`messages`,`subscribe_events`,`list_*`).
- `crates/syncode-provider/src/adapters/opencode_server_provider.rs` — `OpenCodeProvider: ProviderAdapter`, SSE→ProviderEvent (`OpenCodeAdapter.ts:2508-3013` `handleSubscribedEvent`).
- Config: `OpenCodeCompatibleCliSpec` (`default_binary_path`,`server_ready_prefix`,`config_content_env_var`,`server_auth_username`).
- Register directly in `registry.rs`.

**Cost:** ~3–4 days (SSE parsing + server lifecycle is the bulk).

---

## 4. kilo — FEASIBLE-HTTP (free, shares opencode)

**mcode evidence:** `KiloAdapter.ts` docstring: *"Kilo's CLI/server API is OpenCode-compatible, so the live layer reuses the OpenCode adapter implementation with Kilo-specific process settings."* `OpenCodeAdapter.ts:66-108` parameterized by `OpenCodeCompatibleAdapterConfig`; `OPENCODE_ADAPTER_CONFIG` vs `KILO_ADAPTER_CONFIG` differ only in: `provider`, `displayName`, `defaultBinaryPath` (`opencode`/`kilo`), env-var keys, `cliSpec`.

**Implementation sketch:** Build opencode with the `OpenCodeCompatibleCliSpec` abstraction; kilo is one registration: `kilo serve`, ready line `kilo server listening`, auth username `kilo`, env `KILO_CONFIG_CONTENT`, data dir `kilo`. Register both `PROVIDER_OPENCODE` + `PROVIDER_KILO` in `registry.rs`.

**Cost:** ~0 if opencode built with the config abstraction; ~0.5 day to retrofit.

---

## 5. pi — INFEASIBLE (stay a documented stub)

**mcode evidence:** `PiAdapter.ts` imports `createAgentSessionRuntime`,`AgentSession`,`SessionManager`,`ModelRegistry`,`AuthStorage`,`ExtensionUIContext` from `@earendil-works/pi-coding-agent` / `pi-agent-core`. Calls **in-process** SDK: `SessionManager.create(cwd)`, `createAgentSessionRuntime(...)`, `session.subscribe/prompt/abort/compact/reload/bindExtensions`. **No subprocess, no HTTP server** — runs inside the Node process.

- Pure in-process TS lib (`pi-coding-agent` ^0.74 + `pi-agent-core` + `pi-ai`). Loads `models.json`, `auth.json`; sessions as local files; in-memory event emitter.
- Events: `agent_start`,`turn_start`,`message_update(text_delta|thinking_delta)`,`tool_execution_*`,`compaction_*`,`agent_end`.
- **Native Node dep** (clipboard module) — can't `require()` from non-Node.
- Even mcode no-ops ~15 `ExtensionUIContext` methods (`PiAdapter.ts:1019-1200`) — assumes a TUI mcode doesn't provide.

**Feasibility: INFEASIBLE.** No wire protocol to spawn/call; native Node deps; no public spec; a Rust port = reimplementing the entire TS codebase (model registry, tool dispatcher, extension loader, TUI framework).

**Recommendation:** Leave `pi.rs` a stub; `PROVIDERS.md` "No Rust SDK" is accurate. Document that Pi needs an official Rust SDK or an HTTP/subprocess server mode.

---

## T4 Implementation Order

1. **codex** (real, subprocess) — reuse `JsonRpcTransport`; ~1.5–2d.
2. **claude** (real, stream-only) — `claude --output-format stream-json`; ~2–3d.
3. **opencode + kilo** (real, shared HTTP+SSE with `OpenCodeCompatibleCliSpec`) — ~3–4d (kilo free).
4. **pi** — documented stub (no code; update stub doc-comment).

## Open Questions for T4

1. **Codex auth** — `codex app-server` self-authenticates? Verify with real binary in E2E.
2. **Claude CLI flags** — confirm `--output-format stream-json`; is there an interactive session mode with `interrupt` without restart? Check `claude --help`.
3. **OpenCode SSE auth password** — how does the server publish its Basic-auth password? Read `@opencode-ai/sdk` source or capture real output in E2E.
4. **Codex event coverage** — minimal subset (turn/item/delta/approval) vs full parity (~40 methods incl. `codex/event/*`,`realtime/*`).
5. **OpenCode "newer server" events** — target stable server only or include `session.next.*`.

## Sources

**mcode (ground truth):** `apps/server/src/provider/Services/{ClaudeAdapter,CodexAdapter,OpenCodeAdapter,KiloAdapter,PiAdapter}.ts`, `apps/server/src/provider/Layers/{...}.ts`, `apps/server/src/codexAppServerManager.ts`, `apps/server/src/provider/opencodeRuntime.ts`, `apps/server/package.json`, `.docs/provider-architecture.md`.
**syncode (target):** `crates/syncode-provider/{subprocess.rs,acp.rs,acp_provider.rs,registry.rs,PROVIDERS.md}`, `adapters/{anthropic,openai,codex,claude,opencode,kilo,pi}.rs`.
**Web:** Claude Agent SDK subprocess model (claude-code issues #4383, claude-agent-sdk-python #573, Archon #1030).

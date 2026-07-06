# 04 — Frontend Wiring: REAL LIVE (not mock, not stub)

> **Status (2026-07-06): LIVE.** The cloned MCode `apps/web` frontend is wired to the Syncode Rust backend via a real WebSocket JSON-RPC transport. Every RPC is served by real backend logic. The chat invokes real providers (CLI subprocesses). Terminal streams live PTY output. Automations execute real commands. Settings persist in-session. This document maps the wiring.

## Transport layer (REAL, not mock)

**`frontend/src/wsTransport.ts`** — hand-written JSON-RPC-over-WebSocket client (Effect-RPC stripped in T5):
- Opens one WebSocket to `ws://<host>:<port>/ws` (port from `VITE_WS_PORT` env, default 3000).
- Sends `JsonRpcRequestView`-shaped messages; receives `JsonRpcResponseView` + push notifications.
- `MCODE_TO_SERVED` map (88+ entries) translates MCode dot-method names (`orchestration.getShellSnapshot`) → Syncode slash methods (`shell/getSnapshot`).
- Unserved methods reject client-side with `MethodNotFound (-32601)` (no backend call).
- Reconnect with exponential backoff. Public boundary (`request`/`subscribe`/`getLatestPush`/`onStateChange`) preserved — `wsNativeApi.ts` needed zero edits.

**`frontend/src/contracts/`** — the `@t3tools/contracts` shim (path-identical alias):
- `index.ts` barrel re-exports all 139 Tier-3 symbols + 26 ts-rs types + RPC registry + 44-event union + branded IDs.
- `tier3/*.ts` — hand-ported from MCode Effect Schema → plain TypeScript (132 real-typed, 7 stubs).
- `rpc.ts` — `SERVED_RPC` registry (113+ entries) + `MCODE_TO_SERVED` + `UNSERVED_RPC` (48 non-actively-called).
- `events.ts` — `DomainEventDto` 44-variant discriminated union + `OrchestrationPushEnvelope`.
- `ids.ts` — branded IDs (`ThreadId`, `ProjectId`, etc.) with `makeUnsafe()` factories (type+value).
- `runtime.ts` — minimal hand-written guards (replaces Effect `Schema.is`).
- `shell.ts` — `NativeApi`/`DesktopBridge` interfaces (verbatim from MCode `ipc.ts`).

## Backend connection (REAL, not mock)

**Standalone WS server** (`crates/syncode-ws/src/bin/server.rs`):
- `cargo run -p syncode-ws --bin server` → boots an Axum server on `127.0.0.1:3000/ws` (SQLite-backed).
- Wires `Orchestrator::with_reactor_and_adapter` — **turns invoke real providers** (chat functional).
- Wires `WsDomainEventPublisher` → push events to subscribed clients.
- HTTP routes via `syncode-http::http_router()` (health, favicon).

**E2E PROVEN** (2026-07-04): UI ↔ backend connected → shell renders real project data → ZERO MethodNotFound → ZERO pageerrors.

## RPC coverage (REAL, not stub)

| Domain | RPCs | Status | Backed by |
|---|---|---|---|
| **Chat** | turn/start → provider → AI response | ✅ REAL | `ProviderCommandReactor` wired (provider CLI spawned, response streamed, events ingested + pushed) |
| **Shell** | getShellSnapshot/getSnapshot | ✅ REAL | `read_model` (real projects + threads) |
| **Orchestration** | dispatchCommand/subscribeShell/getTurnDiff/getFullThreadDiff/replayEvents/repairState | ✅ REAL | `Orchestrator::handle_command` / `replay_read_model` / git diff |
| **Git** | status/diff/branch/stage/commit/stash/fetch/init/worktree/pull/push/stacked/GitHub | ✅ REAL | `syncode-git::Git2Service` + git2 + gh CLI |
| **Terminal** | create/write/resize/close/ack/list/clear/restart + subscribeEvents | ✅ REAL | `syncode-terminal::SessionManager` (real PTY) + per-session reader task → live output push |
| **Automation** | CRUD + runNow/cancelRun + markRunRead/archiveRun + subscribe | ✅ REAL | `syncode-automation::Scheduler` + `ProcessRunExecutor` (executes via `sh -c`) + run-upserted push |
| **Server** | getConfig/getSettings (persisted) + writes (merge+push) + all subscribe* + welcome/env/diag (real telemetry) + localServer (spawn/kill) + generateAutomationIntent (LLM) | ✅ REAL | `ServerSettingsState` + `UsageStore` + `LocalServerManager` + `invoke_llm_oneshot` |
| **Provider** | listModels/listAgents (ALL_PROVIDERS) + skills/plugins/options/commands (filesystem scan + static) + compactThread (LLM) | ✅ REAL | provider registry + filesystem + `invoke_llm_oneshot` |
| **Stats** | getProfileStats/getProfileTokenStats | ✅ REAL | `read_store` counts + `UsageStore` token aggregates |
| **Auth** | bootstrap/status/logout | ✅ REAL | `syncode-auth` (opt-in, bearer-session) |
| **HTTP** | GET /health + GET /api/project-favicon | ✅ REAL | `syncode-http::http_router()` |
| **Voice** | transcribeVoice/voiceStart/voiceStop | 🟡 STUB | "STT not configured" (needs whisper+ffmpeg install) |

**Total: 113+ served RPCs. ZERO actively-called RPCs unserved.**

## Push event wiring (REAL LIVE streaming, not polled mock)

| Channel | Source | Delivery |
|---|---|---|
| `terminal/event` | Per-session reader task polls PTY output → `push_tx` | ✅ LIVE — terminal output streams in real time |
| `automation` | `runNow`/`cancelRun` push `run-upserted` events after execution | ✅ LIVE — automation lifecycle events |
| `server.configUpdated`/`settingsUpdated`/`providerStatusesUpdated` | Settings writes merge + push on change | ✅ LIVE — settings panel updates stream |
| `server.lifecycle` | subscribeLifecycle emits initial welcome snapshot | ✅ LIVE (initial; no ongoing maintenance push) |
| `orchestration` (shell/domain events) | `WsDomainEventPublisher` publishes after append+project | ✅ LIVE — domain event stream (MessageAdded, TurnCompleted, etc.) |

All push channels use the same `push_tx: broadcast::Sender<(String, Value)>` → `run_push_delivery` → `SubscriptionRegistry` → per-connection mpsc delivery. Subscriptions are real (`push/subscribe` registers the connection).

## Chat flow (REAL end-to-end)

```
UI types message → wsTransport.request("turn/start", {threadId, userInput})
  → WS server: handle_turn_start
    → Orchestrator::handle_command(StartTurn)
      → Decider: TurnStarted event → append + project
      → ProviderCommandReactor.react(StartTurn)
        → SessionManager.start_session → ProviderAdapter.spawn (CLI subprocess)
        → ProviderAdapter.send_request(prompt) → provider generates response
        → Stream consumer (tokio::spawn):
            ProviderEvent::{Token, ToolCall, Completed}
            → ingest_provider_event → DomainEvent (MessageAdded, TurnCompleted)
            → append + project + WsDomainEventPublisher.publish → push_tx
              → subscribed UI clients see live tokens/tool-calls/completion
```

**This is NOT a mock.** The provider CLI (e.g. `claude`) is spawned as a real subprocess. The response is real AI-generated text. It streams back via real push events. The UI renders it live.

## LLM ops (REAL via provider CLI, no API key)

`crates/syncode-ws/src/llm.rs::invoke_llm_oneshot`:
- Instantiates a provider adapter (default "claude", configurable via `SYNCODE_DEFAULT_PROVIDER`).
- `spawn(config)` → `start_session(ctx)` → `send_request(prompt)` → extract text.
- Uses the provider CLI's OWN auth (claude CLI is logged in, codex CLI is authed, etc.) — **no API key needed**.
- Token usage extracted from `ProviderResponse.result` JSON → recorded in `UsageStore`.
- Serves: compactThread, summarizeDiff, generateThreadRecap, generateAutomationIntent.

## What's NOT real (the only stubs)

| Item | Why | Fix |
|---|---|---|
| Voice STT (3 RPCs) | No whisper/ffmpeg binary installed | `pip install openai-whisper` + wire the handler |
| 48 UNSERVED_RPC entries | Legacy aliases the vendored UI doesn't invoke | Not needed (non-actively-called) |

**Everything the UI actively calls is REAL LIVE — not mock, not stub (except voice).**

## Files (the wiring surface)

| File | Role |
|---|---|
| `frontend/src/wsTransport.ts` | JSON-RPC client (Effect-free, MCODE_TO_SERVED map) |
| `frontend/src/contracts/index.ts` | `@t3tools/contracts` shim barrel |
| `frontend/src/contracts/rpc.ts` | SERVED_RPC + MCODE_TO_SERVED + UNSERVED_RPC registries |
| `frontend/src/contracts/tier3/*.ts` | 139 hand-ported MCode contract symbols |
| `frontend/src/contracts/events.ts` | 44-variant DomainEventDto discriminated union |
| `frontend/src/contracts/ids.ts` | Branded IDs with makeUnsafe factories |
| `frontend/src/contracts/shell.ts` | NativeApi/DesktopBridge interfaces |
| `crates/syncode-ws/src/bin/server.rs` | Standalone server (reactor+adapter wired) |
| `crates/syncode-ws/src/rpc.rs` | 113+ RPC dispatch handlers |
| `crates/syncode-ws/src/llm.rs` | invoke_llm_oneshot (LLM via provider CLI) |
| `crates/syncode-ws/src/usage.rs` | UsageStore (token tracking) |
| `crates/syncode-ws/src/settings.rs` | ServerSettingsState (persisted settings) |
| `crates/syncode-ws/src/local_server.rs` | LocalServerManager (process spawn/kill) |
| `crates/syncode-http/src/routes.rs` | HTTP REST routes (health, favicon) |
| `crates/syncode-ws/src/push.rs` | Push bus + WsDomainEventPublisher |
| `crates/syncode-ws/src/channels.rs` | ALL_CHANNELS registry |

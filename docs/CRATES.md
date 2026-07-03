# Syncode — Crate Reference

Quick reference for all 12 internal crates. For full detail (files, API surface, risks), see [`.masday/intel/crates/`](../.masday/intel/crates/). Layering and architecture: [ARCHITECTURE.md](ARCHITECTURE.md).

| Crate | Layer | Purpose | LOC | Tests | Status |
|---|---|---|---|---|---|
| [`syncode-core`](../.masday/intel/crates/syncode-core.md) | L0 | Domain kernel — entities, 44 events, 7 port traits | 2165 | 45 | ✅ |
| [`syncode-contracts`](../.masday/intel/crates/syncode-contracts.md) | L0 | Shared DTOs + ts-rs → `frontend/src/types/` + **snapshot DTOs** + RPC DTOs + 44-event union + Tier-3 symbols | ~1100 | 96 | ✅ |
| [`syncode-orchestration`](../.masday/intel/crates/syncode-orchestration.md) | L2 | CQRS engine — 48 Commands, Decider, Projector, Reactors, ApplicationService (56 use cases) | 10121 | 179 | ✅ |
| [`syncode-provider`](../.masday/intel/crates/syncode-provider.md) | L1 | `ProviderAdapter` trait + 10 adapters + SessionManager + registry | 13525 | 276 | ✅ |
| [`syncode-persistence`](../.masday/intel/crates/syncode-persistence.md) | L1 | SQLite event store + 7 projections + snapshots | 2476 | 25 | ✅ |
| [`syncode-git`](../.masday/intel/crates/syncode-git.md) | L1 | git2 + CLI — status/diff/branch/commit/checkpoint/worktree + **push/pull/CreatePR** (`git`/`gh` shelling-out) | 1958 | 40 | ✅ |
| [`syncode-terminal`](../.masday/intel/crates/syncode-terminal.md) | L1 | portable-pty PTY + ack-buffered output + sessions | 699 | 20 | ✅ |
| [`syncode-automation`](../.masday/intel/crates/syncode-automation.md) | L1 | Scheduler + retry/misfire/completion policies + **ProcessRunExecutor** (executes automations via `sh -c`) | ~2600 | 72 | ✅ executes (WS-wired) |
| [`syncode-ws`](../.masday/intel/crates/syncode-ws.md) | L3 | WebSocket JSON-RPC server + push bus + channels + authz + **standalone bin** (`src/bin/server.rs`) + **97 served RPCs** (all MCode domains) + terminal live-push + LLM-via-provider-CLI (`src/llm.rs`) | ~5500 | 132 | ✅ backend (clone+rewire) |
| [`syncode-tauri`](../.masday/intel/crates/syncode-tauri.md) | L4 | Tauri desktop binary — tray, updater, IPC + **28 commands wired** (git/terminal/shell) | ~1500 | 29 | ✅ builds + wired (glib unblocked) |
| [`syncode-auth`](../.masday/intel/crates/syncode-auth.md) | L1 | Credentials, auth policy, secret store, **principal/session/authenticator** | 1203 | 39 | ✅ wired into WS (opt-in) |
| [`syncode-http`](../.masday/intel/crates/syncode-http.md) | L1 | Future REST surface | 12 | 0 | 🚧 stub |

† `syncode-tauri` now builds (GTK/webkit `-dev` libs installed — env, not committed; CI must `apt install` Tauri v2 Linux prereqs). 29 tests. Workspace `cargo check --workspace` green.

## Dependency edges (complete)

```
auth, automation, git, http, persistence, provider, terminal  → core
orchestration  → core, provider
ws             → core, orchestration, persistence
tauri          → core, git, terminal, ws
contracts      → (none)
tests          → core, contracts, provider, terminal, automation
```

> **Key implication:** `syncode-core` is depended on by every other crate — changes there have the highest blast radius. The port traits in `core/ports` are the hexagonal seams; `Command`/`Orchestrator` are the sole `orchestration→ws` coupling; `ProviderAdapter` couples to all 10 adapters.

## Notable cross-cutting gaps

- **Two parallel git abstractions:** `core::ports::GitServicePort` (async) vs `syncode-git::GitService` (sync) — the latter does **not** implement the port.
- **Desktop↔backend (ADDRESSED):** `tauri` now has **28 IPC commands** wired (git/terminal/shell). The standalone WS server (`syncode-ws/src/bin/server.rs`) serves the web UI directly (SQLite-backed). Desktop boot E2E not verified (headless — needs a display). See [`STATUS.md`](./STATUS.md).
- **Automation now hosted + executing (ADDRESSED):** WS layer wires the Scheduler + `ProcessRunExecutor` (runs commands via `sh -c`); `automation.runNow`/CRUD + `markRunRead`/`archiveRun` RPCs served (latter two persist `unread`/`archived_at` on `AutomationRun` via the repo upsert). (Automation event-push still stubbed — see `STATUS.md`.)
- **WS auth is wired but opt-in** — `syncode-auth` now owns principal/session/authenticator + `AuthMode`; the WS layer authenticates connections and authorizes RPC dispatch via an authz gate (`auth/bootstrap`·`auth/status`·`auth/logout`). Default mode is `UnsafeNoAuth` (backward-compat); `WsState::new_with_auth(.., WsAuthConfig::remote(..))` opts in. No rate limiting; sessions are in-memory.

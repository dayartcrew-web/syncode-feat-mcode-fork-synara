# Syncode — Crate Reference

Quick reference for all 12 internal crates. For full detail (files, API surface, risks), see [`.masday/intel/crates/`](../.masday/intel/crates/). Layering and architecture: [ARCHITECTURE.md](ARCHITECTURE.md).

| Crate | Layer | Purpose | LOC | Tests | Status |
|---|---|---|---|---|---|
| [`syncode-core`](../.masday/intel/crates/syncode-core.md) | L0 | Domain kernel — entities, 14 events, 4 port traits | 1627 | 45 | ✅ |
| [`syncode-contracts`](../.masday/intel/crates/syncode-contracts.md) | L0 | Shared DTOs + ts-rs → `frontend/src/types/` | 265 | 15 | ✅ |
| [`syncode-orchestration`](../.masday/intel/crates/syncode-orchestration.md) | L2 | CQRS engine — 16 Commands, Decider, Projector, Reactors, ApplicationService | 3326 | 57 | ✅ |
| [`syncode-provider`](../.masday/intel/crates/syncode-provider.md) | L1 | `ProviderAdapter` trait + 10 adapters + SessionManager + registry | 7210 | 174 | ⚠️ 8/10 stubs |
| [`syncode-persistence`](../.masday/intel/crates/syncode-persistence.md) | L1 | SQLite event store + 5 projections + snapshots | 1816 | 17 | ✅ |
| [`syncode-git`](../.masday/intel/crates/syncode-git.md) | L1 | git2 — status/diff/branch/commit/checkpoint/worktree/stacked | 1201 | 22 | ⚠️ push/pull/PR stub |
| [`syncode-terminal`](../.masday/intel/crates/syncode-terminal.md) | L1 | portable-pty PTY + ack-buffered output + sessions | 714 | 38 | ✅ |
| [`syncode-automation`](../.masday/intel/crates/syncode-automation.md) | L1 | Scheduler + retry/misfire/completion policies | 1101 | 19 | ⚠️ cron/retry/exec stub |
| [`syncode-ws`](../.masday/intel/crates/syncode-ws.md) | L3 | WebSocket JSON-RPC server + push bus + channels | 1188 | 14 | ⚠️ transport stub |
| [`syncode-tauri`](../.masday/intel/crates/syncode-tauri.md) | L4 | Tauri desktop binary — tray, updater, IPC | 1129 | 0† | ⚠️ engine not wired |
| [`syncode-auth`](../.masday/intel/crates/syncode-auth.md) | L1 | Credential/policy/secret-store | 16 | 0 | 🚧 stub |
| [`syncode-http`](../.masday/intel/crates/syncode-http.md) | L1 | Future REST surface | 12 | 0 | 🚧 stub |

† `syncode-tauri` is excluded from `cargo test --workspace` (pre-existing build issues).

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
- **Engine not reachable from desktop:** `tauri` reaches orchestration only transitively via `ws`, and isn't confirmed to spawn the WS server; there are no Tauri IPC commands for project/thread/turn.
- **Automation isolated:** depends on `core` only; triggers create run records but execute nothing; not persisted.
- **No WS auth** — `syncode-auth` is the planned home but is empty.

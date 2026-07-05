# Syncode — Architecture

> **Architecture reference** (2026-06-27). The **clone+rewire arc (PR #6–#32)** since added: a standalone WS server (`syncode-ws/src/bin/server.rs`, SQLite) serving **97 RPCs** across all MCode domains, terminal live-output push, `ProcessRunExecutor` (executing automations), and LLM ops via provider CLI. For the authoritative current REAL-vs-STUB status see [`STATUS.md`](./STATUS.md). Supersedes the planning-stage `COMPARISON-*.md` docs. Generated alongside `.masday/intel/` (intel is pre-clone + stale in places — trust `STATUS.md` + live `cargo test` for current counts).

## 1. What is Syncode?

Syncode is a **Rust DDD (Domain-Driven Design) blueprint of [MCode](https://github.com/dayartcrew-web/mcode)** — a local-first AI-coding-agent desktop app. It reimplements MCode's CQRS/Event-Sourcing orchestration engine in Rust, as a deliberately slim reference skeleton (~23,300 LOC ≈ 24% of MCode's 96,870-LOC TypeScript server). It is **not** a feature-complete port.

**Lineage** (from git remotes): `synara` → `mcode` (Bun/TS/Effect) → **`syncode`** (Rust, this repo). The original MCode is the ground-truth reference at `/home/vibe-dev/mcode/`.

## 2. Stack

| Concern | Choice |
|---|---|
| Language | Rust 2024 edition, MSRV 1.85.0 |
| Workspace | Cargo, `resolver = 2`, 12 internal crates |
| Async | Tokio ("full") |
| HTTP / WebSocket | Axum 0.8, tokio-tungstenite 0.26 |
| Database | SQLx 0.8 + **SQLite** (event store + projections) |
| Git | git2 0.20 |
| Terminal | portable-pty 0.9 |
| Serialization | serde + serde_json |
| Rust → TypeScript | ts-rs 10 (exports `frontend/src/types/*.ts`) |
| Desktop | Tauri v2 |
| Frontend | React 19 + Vite 6 + TypeScript 5.7 |

## 3. System architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Tauri Desktop Shell (syncode-tauri, L4)                      │
│  window · tray · auto-updater · IPC commands (git/terminal)   │
└───────────────┬──────────────────────────────────────────────┘
                │  (transitive) 
┌───────────────▼──────────────────────────────────────────────┐
│  WebSocket JSON-RPC Server (syncode-ws, L3)   GET /ws         │
│  ~16 RPC methods · 6 push channels · best-effort push bus     │
└───────────────┬──────────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────────┐
│  CQRS / Event-Sourcing Engine (syncode-orchestration, L2)     │
│  Command → Decider → Events → [EventStore] → Projector        │
│                            → Reactors (provider side-effects)  │
└───────┬──────────────────────┬────────────────────────────────┘
        │                      │
        ▼                      ▼
┌─────────────────┐   ┌────────────────────────────────────────┐
│ syncode-provider│   │ syncode-persistence (L1)                │
│ (L1)            │   │ SQLite: domain_events + 7 projection    │
│ 10 adapters     │   │ tables + snapshots + watermark          │
│ (all real)      │   └────────────────────────────────────────┘
└─────────────────┘
        │
┌───────▼──────────────────────────────────────────────────────┐
│  syncode-core (L0) — SHARED DOMAIN KERNEL                     │
│  Entities (Project/Thread/Turn/Message/Activity) ·            │
│  DomainEvent (44) · Envelope · Port traits (7)                │
└──────────────────────────────────────────────────────────────┘
```

## 4. CQRS / Event-Sourcing pipeline

```
WS request → ws::rpc::handle_rpc
           → Orchestrator::handle_command(cmd)
               1. load_aggregate_state        (read model)
               2. Decider::decide(cmd,state)  → Vec<DomainEvent>   (pure)
               3. event_repo.append_events    (optimistic concurrency, expected_version)
               4. Projector::project_many     → ReadModelStore
               5. (optional) ProviderCommandReactor side effect
           → return updated entity + broadcast PushEvent
```

Provider read-back: `ProviderEvent → ingest_provider_event() → DomainEvent` (TurnCompleted / TurnFailed / ActivityLogged) → same append+project path.

## 5. Crate layering

| Layer | Crate | Role |
|---|---|---|
| **L0 kernel** | `syncode-core` | domain kernel — entities, 44 events, 7 port traits (**universal dependency**) |
| **L0 kernel** | `syncode-contracts` | shared DTOs + ts-rs codegen |
| **L1 leaf** | `syncode-provider` | `ProviderAdapter` trait + 10 adapters + SessionManager + registry |
| **L1 leaf** | `syncode-persistence` | SQLite event store + projections + snapshots (CQRS write/read side) |
| **L1 leaf** | `syncode-git` | git2: status/diff/branch/commit/checkpoint/worktree/stacked-actions |
| **L1 leaf** | `syncode-terminal` | portable-pty PTY + ack-buffered output + sessions |
| **L1 leaf** | `syncode-automation` | scheduler + retry/misfire/completion policies |
| **L1 leaf** | `syncode-auth` | credentials, auth policy, secret store, principal/session/authenticator (**wired into WS — opt-in**) |
| **L1 leaf** | `syncode-http` | *(stub)* future REST surface |
| **L2 engine** | `syncode-orchestration` | CQRS: 48 Commands, Decider, Orchestrator, Projector, Reactors, ApplicationService |
| **L3 transport** | `syncode-ws` | WebSocket JSON-RPC server + push bus |
| **L4 shell** | `syncode-tauri` | Tauri desktop binary (tray, updater, IPC) |

`syncode-core` is depended on by **every** other crate — it is the highest-impact surface in the repo.

## 6. Domain model

**Aggregates:** `Project → Thread → Turn → Message`, plus `Activity` (audit log). **44 domain events** (serde-tagged `{event_type, data}`), wrapped in `Envelope { event, sequence, timestamp }` (sequence = monotonic stream position for optimistic concurrency).

> **Modeling note:** Syncode treats Turn/Message/Activity as first-class aggregates. MCode only has `project` + `thread` aggregates (turns/messages nested in thread events). This is an intentional simplification.

## 7. Port traits (hexagonal seams)

Defined in `syncode-core/src/ports/mod.rs` (async, `Send+Sync`):
- **`EventRepository`** — append/replay/snapshot/version (write side)
- **`ReadModelRepository`** — refresh + list/get for project/thread/turn/message/activity (read side)
- **`GitServicePort`** — status/checkpoint/diff/modified-files/valid-repo
- **`ProviderPort`** — start/send/interrupt/stop session + health/models

## 8. Status: what's real vs stub

**Implemented & tested (~791 tests):** core domain (44 events), CQRS engine (48 Commands — all MCode client + internal commands ported; decider/projector/reactors/use-cases), SQLite persistence (7 projections), 2 HTTP provider adapters (Anthropic, OpenAI), git status/diff/branch/commit/checkpoint/worktree + **push/pull/CreatePR via CLI** (`git`/`gh` shelling-out, auth delegated to user credentials), terminal PTY + ack protocol, **automation execution engine** (cron/interval due-evaluation via `cron` crate, retry loop honoring RetryPolicy, run dispatch via `RunExecutor` port, misfire coalesce, trait-abstracted `AutomationRepository`), **WS auth wired** (principal/session/authenticator + authz gate on RPC dispatch + `auth/bootstrap`·`auth/status`·`auth/logout`), **snapshot-then-stream subscriptions** (`push/subscribe` emits a snapshot of current state, then live deltas; reconnecting clients re-subscribe to re-hydrate), WebSocket RPC + push bus, Tauri shell scaffolding.

**Stubs / not wired:**
- ~~8 subprocess provider adapters (claude/codex/cursor/gemini/grok/kilo/opencode/pi)~~ — all 8 are now REAL (claude stream-json, codex app-server, opencode/kilo HTTP+SSE, pi RPC, cursor/grok/gemini ACP). No functional stubs remain.
- `syncode-http` — empty
- `ws/transport.rs` — reframed as an architectural note (server is stateless-per-upgrade; reconnect is client-owned; the server's obligation — snapshot-on-subscribe — is in `push.rs`/`rpc.rs`). See the module doc.
- **Automation execution engine is ready but not yet hosted** — no production runtime process exists to drive `Scheduler::tick()`; storage is in-memory (SQLite `AutomationRepository` is a drop-in follow-up via the port in core).
- **Tauri shell does not compose the ws server or the orchestration engine** — desktop↔engine integration is incomplete; no IPC commands for project/thread/turn

**Known risks:** no optimistic-concurrency retry; `duration_ms` heuristic (`tokens*10`); `CreateThread` doesn't validate project exists; no automatic snapshotting; **WS auth defaults to `UnsafeNoAuth` (backward-compat) — opt in via `WsState::new_with_auth(.., WsAuthConfig::remote(..)`**; push delivery is best-effort (no sliding-window backpressure / drop→resync yet — separate follow-up); auth sessions are in-memory (not persisted across restart).

## 9. MCode parity (porting fidelity)

| | MCode (ground truth) | Syncode |
|---|---|---|
| Commands | ~39 (28 client + 11 internal) | 38 (all 28 client commands ported) |
| Event types | 35 | 35 |
| Providers | 8 CLI via ACP + Effect layers | 8 real CLI adapters (ACP×3, codex, claude, opencode, kilo, pi) + 2 HTTP |
| Server LOC | 96,870 | ~23,300 |

All 28 MCode client-orchestration commands are now ported (command-port workflow). Provider dispatch (command → provider) is wired through the command reactor (provider-bridge workflow). Still-unported MCode surfaces are the **internal** commands: conversation rollback (+complete), proposed-plan-upsert, the turn-diff pipeline (turn-diff-complete), standalone messages-import, and assistant streaming deltas. The reverse bridge — provider response events fed back into the pipeline as domain events — is not yet collected (the ingestion reactor handles the separate `ProviderEvent → DomainEvent` path).

## 10. Further reading

- **Detailed per-crate intelligence:** [`.masday/intel/`](../.masday/intel/README.md) — aggregates + one file per crate.
- **Crate quick-reference:** [CRATES.md](CRATES.md).
- **Test breakdown:** [TEST_SUMMARY.md](../TEST_SUMMARY.md).
- **MCode reference:** `/home/vibe-dev/mcode/` (authoritative domain vocabulary: `packages/contracts/src/orchestration.ts`).

## 11. Build, run, deploy

See [`../README.md`](../README.md) for the canonical Quick Start. Summary:

| Goal | Command |
|---|---|
| Run the WS server | `cargo run -p syncode-ws --bin server` (→ `ws://127.0.0.1:3000/ws`) |
| Frontend dev server | `cd frontend && npm ci && npm run dev` |
| Desktop shell | `cargo build -p syncode-tauri` (Linux needs webkit2gtk deps) |
| Docker (all-in-one) | `cp .env.example .env && docker compose up --build` |
| Workspace tests | `cargo test --workspace --exclude syncode-tauri` |
| Clippy gate | `cargo clippy --workspace --exclude syncode-tauri --all-targets -- -D warnings` |
| Frontend tests | `cd frontend && npm run typecheck && npm test` |

**Deployment paths:**
- **Docker (recommended)** — `docker compose up -d --build`. The bind-mounted
  `./data` volume holds the SQLite DB + resume cursors so state survives
  container restarts; `restart: unless-stopped` reboots on crash/host reboot.
- **Release binary** — `cargo build --release -p syncode-ws --bin server`. Run
  with the env vars from [`.env.example`](../.env.example); front with a reverse
  proxy for TLS.

**Environment variables** are documented in [`.env.example`](../.env.example).
The notable ones: `SYNCODE_WS_PORT` (default `3000`), `SYNCODE_DB` (SQLite path;
empty = in-memory), `SYNCODE_DEFAULT_PROVIDER` (default `claude`),
`RUST_LOG` (tracing filter).

**CI** (`.github/workflows/ci.yml`) runs `cargo fmt --check`, per-crate clippy
with `-D warnings`, workspace tests (excluding `syncode-tauri`, which needs
system webkit/gtk deps and is covered by `desktop-e2e.yml`), a WS-server binary
build, and frontend typecheck + vitest on every PR. `syncode-tauri` clippy is
owned by the `desktop-e2e.yml` workflow which installs the system deps first.

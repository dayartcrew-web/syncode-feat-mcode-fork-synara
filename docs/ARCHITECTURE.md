# Syncode — Architecture

> **Source of truth for the current implementation** (2026-06-27). Supersedes the planning-stage `COMPARISON-*.md` docs. Generated alongside `.masday/intel/`.

## 1. What is Syncode?

Syncode is a **Rust DDD (Domain-Driven Design) blueprint of [MCode](https://github.com/dayartcrew-web/mcode)** — a local-first AI-coding-agent desktop app. It reimplements MCode's CQRS/Event-Sourcing orchestration engine in Rust, as a deliberately slim reference skeleton (~19,600 LOC ≈ 20% of MCode's 96,870-LOC TypeScript server). It is **not** a feature-complete port.

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
│ (L1)            │   │ SQLite: domain_events + 5 projection    │
│ 10 adapters     │   │ tables + snapshots + watermark          │
│ (8 stub + 2 HTTP│   └────────────────────────────────────────┘
└─────────────────┘
        │
┌───────▼──────────────────────────────────────────────────────┐
│  syncode-core (L0) — SHARED DOMAIN KERNEL                     │
│  Entities (Project/Thread/Turn/Message/Activity) ·            │
│  DomainEvent (14) · Envelope · Port traits (4)                │
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
| **L0 kernel** | `syncode-core` | domain kernel — entities, 14 events, 4 port traits (**universal dependency**) |
| **L0 kernel** | `syncode-contracts` | shared DTOs + ts-rs codegen |
| **L1 leaf** | `syncode-provider` | `ProviderAdapter` trait + 10 adapters + SessionManager + registry |
| **L1 leaf** | `syncode-persistence` | SQLite event store + projections + snapshots (CQRS write/read side) |
| **L1 leaf** | `syncode-git` | git2: status/diff/branch/commit/checkpoint/worktree/stacked-actions |
| **L1 leaf** | `syncode-terminal` | portable-pty PTY + ack-buffered output + sessions |
| **L1 leaf** | `syncode-automation` | scheduler + retry/misfire/completion policies |
| **L1 leaf** | `syncode-auth` | *(stub)* credential/policy/secret-store |
| **L1 leaf** | `syncode-http` | *(stub)* future REST surface |
| **L2 engine** | `syncode-orchestration` | CQRS: 16 Commands, Decider, Orchestrator, Projector, Reactors, ApplicationService |
| **L3 transport** | `syncode-ws` | WebSocket JSON-RPC server + push bus |
| **L4 shell** | `syncode-tauri` | Tauri desktop binary (tray, updater, IPC) |

`syncode-core` is depended on by **every** other crate — it is the highest-impact surface in the repo.

## 6. Domain model

**Aggregates:** `Project → Thread → Turn → Message`, plus `Activity` (audit log). **14 domain events** (serde-tagged `{event_type, data}`), wrapped in `Envelope { event, sequence, timestamp }` (sequence = monotonic stream position for optimistic concurrency).

> **Modeling note:** Syncode treats Turn/Message/Activity as first-class aggregates. MCode only has `project` + `thread` aggregates (turns/messages nested in thread events). This is an intentional simplification.

## 7. Port traits (hexagonal seams)

Defined in `syncode-core/src/ports/mod.rs` (async, `Send+Sync`):
- **`EventRepository`** — append/replay/snapshot/version (write side)
- **`ReadModelRepository`** — refresh + list/get for project/thread/turn/message/activity (read side)
- **`GitServicePort`** — status/checkpoint/diff/modified-files/valid-repo
- **`ProviderPort`** — start/send/interrupt/stop session + health/models

## 8. Status: what's real vs stub

**Implemented & tested (422 tests):** core domain, CQRS engine (decider/projector/reactors/use-cases), SQLite persistence, 2 HTTP provider adapters (Anthropic, OpenAI), git status/diff/branch/commit/checkpoint/worktree, terminal PTY + ack protocol, automation policies, WebSocket RPC + push bus, Tauri shell scaffolding.

**Stubs / not wired:**
- 8 subprocess provider adapters (claude/codex/cursor/gemini/grok/kilo/opencode/pi) — non-functional
- `syncode-auth`, `syncode-http` — empty
- `ws/transport.rs` — connection state machine (no reconnect/rehydrate)
- git `push`/`pull`/`CreatePR`, worktree `prune`
- automation cron/interval due-evaluation, retry loop, execution, persistence
- **Tauri shell does not compose the ws server or the orchestration engine** — desktop↔engine integration is incomplete; no IPC commands for project/thread/turn

**Known risks:** no optimistic-concurrency retry; `duration_ms` heuristic (`tokens*10`); `CreateThread` doesn't validate project exists; no automatic snapshotting; no WS auth/rate-limiting.

## 9. MCode parity (porting fidelity)

| | MCode (ground truth) | Syncode |
|---|---|---|
| Commands | ~39 (28 client + 11 internal) | 16 |
| Event types | 35 | 14 |
| Providers | 8 CLI via ACP + Effect layers | 8 subprocess stubs + 2 HTTP |
| Server LOC | 96,870 | ~19,600 |

Unported MCode surfaces include: thread handoff/fork/archive, pinned messages, markers, runtime/interaction modes, approval/user-input interactive flow, checkpoint revert, conversation rollback, message edit-resend, proposed plans, the turn-diff pipeline, and assistant streaming deltas.

## 10. Further reading

- **Detailed per-crate intelligence:** [`.masday/intel/`](../.masday/intel/README.md) — aggregates + one file per crate.
- **Crate quick-reference:** [CRATES.md](CRATES.md).
- **Test breakdown:** [TEST_SUMMARY.md](../TEST_SUMMARY.md).
- **MCode reference:** `/home/vibe-dev/mcode/` (authoritative domain vocabulary: `packages/contracts/src/orchestration.ts`).

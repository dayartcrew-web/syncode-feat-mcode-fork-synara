# 01 — Architecture & Dependency Graph

## Architectural style
Hexagonal / Ports-and-Adapters + **CQRS with Event Sourcing**, organized as a DDD bounded-context workspace. The domain kernel (`syncode-core`) defines pure entities, domain events, and **port traits** (interfaces). Engine, persistence, and providers are adapters over those ports.

## CQRS / Event-Sourcing pipeline
```
WebSocket JSON-RPC request
   │
   ▼
syncode-ws::rpc::handle_rpc            (parse + dispatch ~16 methods)
   │  reads:  WsState.read_store (ReadModelStore)
   │  writes: WsState.orchestrator.handle_command(cmd)
   ▼
syncode-orchestration::Orchestrator::handle_command   (pipeline.rs)
   1. load_aggregate_state  (from read model JSON)
   2. Decider::decide(cmd, state)   ── pure ──▶ Vec<DomainEvent>   (or DeciderError)
   3. event_repo.append_events(agg_id, events, expected_version)   ◀ optimistic concurrency
   4. Projector::project_many(events)  ──▶ in-memory ReadModelStore
   5. (optional) ProviderCommandReactor side effect   (StartTurn/FailTurn/Cancel/Pause/CancelThread)
   ▼
syncode-persistence::SqliteEventRepository           (domain_events table, append-only)
syncode-persistence::SqliteReadModelRepository        (view_projects/threads/turns/messages/activities)
   ▼
Push bus broadcasts PushEvent(DomainEvent) ──▶ subscribed WS connections
```
**Provider read-back path:** `ProviderEvent` → `ingest_provider_event()` (reactors/ingestion.rs) → `DomainEvent` (TurnCompleted / TurnFailed / ActivityLogged) → same append+project flow.

**Replay/recovery:** `Orchestrator::replay_read_model()` re-fetches all events and re-projects them. No automatic snapshotting (caller decides).

## Crate layering (L0 → L4)
Derived from each crate's `[dependencies]`:

| Layer | Crates | Depends on (internal) |
|-------|--------|------------------------|
| **L0 kernel** | `core`, `contracts` | — (none) |
| **L1 leaf** | `auth`, `automation`, `git`, `http`, `persistence`, `provider`, `terminal` | `→ core` only |
| **L2 engine** | `orchestration` | `→ core`, `provider` |
| **L3 transport** | `ws` | `→ core`, `orchestration`, `persistence` |
| **L4 shell** | `tauri` (main binary) | `→ core`, `git`, `terminal`, `ws` |

### Complete dependency edges
```
auth       → core
automation → core
contracts  → (none)
git        → core
http       → core
persistence→ core
provider   → core
terminal   → core
orchestration → core, provider
ws         → core, orchestration, persistence
tauri      → core, git, terminal, ws
tests (integration) → core, contracts, provider, terminal, automation
```

## Blast-radius / coupling consequences
- **`syncode-core` is the universal dependency.** Any change to its domain types (`EntityId`, `Timestamp`, `DomainEvent`, port traits) forces recompile + likely edits across all 11 other crates. **Highest-impact surface in the repo.**
- **Ports are the hexagonal seams** (core/ports): `EventRepository`, `ReadModelRepository`, `GitServicePort`, `ProviderPort`. A signature change breaks the adapter impls (`persistence`, `git`, `provider`) **and** the consumers (`orchestration`, `ws`).
- **`Command` enum + `Orchestrator` type** are the sole coupling from `orchestration` → `ws`. `ws/src/lib.rs:84,92` holds `Arc<Orchestrator>`; `ws/src/rpc.rs:9` imports `Command` and constructs variants directly in RPC handlers.
- **`ProviderAdapter` trait** change breaks all 10 adapters + `orchestration` reactors (`command.rs`, `ingestion.rs`).

## Known integration gaps (architectural)
- `tauri` (composition root) does **not** depend on `orchestration`/`provider`/`persistence`/`automation` — it reaches the engine only transitively via `ws`. It is **not** confirmed to spawn the ws server or wire the engine end-to-end.
- `automation` depends on `core` only — **not wired into orchestration**; triggers only create run records, no execution loop, no persistence.
- `syncode-git`'s `GitService` trait is **synchronous** and does **not** implement `core::ports::GitServicePort` (async) — a port/impl mismatch.
- No auth/rate-limiting on the WS server; `auth` + `http` crates are empty stubs.

See per-crate files for detail. Impact hotspots also captured in the `syncode-impact-and-risk` memory.

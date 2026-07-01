# syncode-persistence
> SQLite event store + read-model projections + snapshots (concrete CQRS write/read side). **L1** · 2476 LOC · 25 tests
- **Depends on (internal):** `core`.
- **External:** sqlx 0.8 (sqlite), tokio, serde, async-trait, thiserror.

## Files
- `lib.rs` (102 LOC) — `init_database()` (creates tables via `sqlx::raw_sql`).
- `event_store.rs` (444 LOC) — append/replay with optimistic concurrency.
- `adapters.rs` (629 LOC) — `SqliteEventRepository` + `SqliteReadModelRepository` (impl the core ports).
- `projections.rs` (1070 LOC) — `ProjectionManager` (materialized views).
- `snapshot.rs` (228 LOC) — snapshot CRUD.
- `migrations/mod.rs` (3 LOC) — **stub** (Phase 0.3 TODO).

## Schema (created inline in `init_database`, not via migration files)
- **Write:** `domain_events` (append-only: event_type, aggregate_id, sequence, timestamp, JSON `data`).
- **Read (7 projection tables):** `view_projects`, `view_threads`, `view_turns`, `view_messages`, `view_activities`, `view_markers`, `view_pinned_messages`.
- `snapshots` (aggregate_id, version, JSON state) — upsert via `ON CONFLICT(aggregate_id) DO UPDATE`.
- `projection_watermark` — last processed event id.

## Public API
- `SqliteEventRepository` (`adapters.rs:34`) implements `EventRepository`; `append_domain_events` does atomic append + concurrency check.
- `SqliteReadModelRepository` (`adapters.rs:120`) implements `ReadModelRepository`.
- `ProjectionManager::project_event_async` (`projections.rs:141`) pattern-matches `DomainEvent` → SQL upserts. `rebuild()` drops/recreates projection tables and replays all events.
- `save_snapshot`/`load_snapshot` (`snapshot.rs:23`).

## Stubs / risks
- **`migrations/mod.rs` is a stub** — schema is built inline via `raw_sql`, not SQLx migrations.
- `project_event()` (sync) is a **no-op stub** — only the async projection works.
- **No automatic snapshotting** — caller decides when; infrequent snapshots = slow replays.
- **No locking on `projection_watermark`** — concurrent projection refresh could race.
- `EventStoreError→PortError` mapping loses context (all non-concurrency → `Internal`).
- FK constraints defined but SQLite may not enforce by default; basic indexes only (missing composites like project_id+status).

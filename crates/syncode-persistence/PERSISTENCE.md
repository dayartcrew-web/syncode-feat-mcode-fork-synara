# Persistence Layer

`syncode-persistence` provides the append-only event store, read-model
projection tables, SQLx migrations, and snapshot queries that back the
CQRS/Event Sourcing pipeline in `syncode-orchestration`.

## Storage backend

SQLite with WAL journal mode. The database file path is configurable at
runtime via `init_database(db_path)`.

## Modules

| Module | Purpose |
|--------|---------|
| `event_store` | Append-only `EventRepository` implementation — insert / stream-by-aggregate |
| `projections` | Read-model projection writers — maintain denormalized tables from events |
| `migrations` | SQLx embedded migrations (`migrations/*.sql`) |
| `snapshot` | Periodic aggregate-state snapshots for faster rebuild |
| `adapters` | Repository adapters bridging core ports → SQLite |

## Public API

| Function | Description |
|----------|-------------|
| `init_database(db_path)` | Run migrations and open a connection pool |
| `get_pool(db_path)` | Retrieve an existing pool or create one |

## Schema

| Table | Purpose |
|-------|---------|
| `events` | Append-only event log (aggregate_id, sequence, event_type, payload JSON) |
| `projects` | Read-model: project summaries |
| `threads` | Read-model: thread status and metadata |
| `turns` | Read-model: turn status and token usage |
| `messages` | Read-model: message content |
| `activities` | Read-model: audit trail |

## Integration points

- Implements `syncode-core::ports::EventRepository` and `ReadModelRepository`.
- Consumed by `syncode-orchestration` pipeline.
- `syncode-tauri` calls `init_database` at app startup.

## Stub status

**⚠️ One stub module.** `migrations/mod.rs` (3 lines) contains only:

```rust
//! SQLx migrations directory
// TODO: Phase 0.3 — Add migration files
```

The event store, projections, and snapshot modules are fully implemented
with real SQL queries against SQLite. The `init_database()` function uses
inline SQL rather than the empty `migrations/` directory.

# syncode-core
> Shared domain kernel — entities, domain events, port traits. **L0** · 1627 LOC · 45 tests · `lib.rs` + `domain/` + `application/` + `ports/`
- **Depends on (internal):** none — the universal dependency every other crate builds on.
- **External:** thiserror, serde, chrono, uuid, async-trait, serde_json.

## Files
- `domain/primitives.rs` — `EntityId`, `Timestamp`, `TrimmedString`(+error), `Command`/`DomainEvent` (trait) base traits.
- `domain/events.rs` — `DomainEvent` enum (14 variants) + `Envelope { event, sequence, timestamp }`.
- `domain/{project,thread,turn,message,activity}.rs` — aggregate roots + status enums (`ThreadStatus`, `TurnStatus`, `MessageRole`, `ContentType`, `ActivityType`).
- `domain/application/mod.rs` — (thin) application-layer hooks.
- `ports/mod.rs` — the 4 port traits + `PortError` + Git status DTOs.

## Public API
- **Aggregates:** `Project`, `Thread`/`ThreadStatus`, `Turn`/`TurnStatus`, `Message`/`MessageRole`/`ContentType`, `Activity`/`ActivityType`.
- **Events (14):** `ProjectCreated`, `ProjectUpdated`, `ThreadCreated`, `ThreadStatusChanged`, `ThreadTitleSet`, `ThreadCheckpointSet`, `TurnStarted`, `TurnCompleted`, `TurnFailed`, `TurnCancelled`, `TurnFilesModified`, `TurnCheckpointSet`, `MessageAdded`, `ActivityLogged`. serde-tagged `{event_type, data}`; `Envelope` adds monotonic `sequence`.
- **Ports:** `EventRepository`, `ReadModelRepository`, `GitServicePort`, `ProviderPort` (all `async_trait`, `Send+Sync`). `PortError { NotFound, ConcurrencyConflict{expected,actual}, Internal }`.

## How it works
Pure domain layer — no I/O. Events are immutable facts; `Envelope` carries stream metadata (sequence = position in the aggregate stream, used for optimistic concurrency). Port traits are the dependency-inversion boundary: orchestration depends on these traits, concrete adapters implement them.

## Stubs / risks
- **Highest blast-radius crate** — any change ripples to all 11 crates.
- Note in `lib.rs`: the `DomainEvent` *trait* vs `DomainEvent` *enum* share a name; the trait is accessed via `domain::primitives::DomainEvent` / re-exported alias `DomainEventTrait` to avoid collision.

# syncode-core

> ⚠️ **PRE-CLONE SNAPSHOT (2026-07-02).** This intel is from before the clone+rewire arc (PR #6–#47, 48 PRs total). For the current authoritative state see [`docs/STATUS.md`](../../../docs/STATUS.md).
>
> **Key changes since this snapshot:** Unchanged. 45 tests, 44 DomainEvents, 7 port traits. DomainEventDto mirror in syncode-contracts.

> Shared domain kernel — entities, domain events, port traits. **L0** · 2165 LOC · 45 tests · `lib.rs` + `domain/` + `application/` + `ports/`
- **Depends on (internal):** none — the universal dependency every other crate builds on.
- **External:** thiserror, serde, chrono, uuid, async-trait, serde_json.

## Files
- `domain/primitives.rs` — `EntityId`, `Timestamp`, `TrimmedString`(+error), `Command`/`DomainEvent` (trait) base traits.
- `domain/events.rs` — `DomainEvent` enum (44 variants) + `Envelope { event, sequence, timestamp }`.
- `domain/{project,thread,turn,message,activity}.rs` — aggregate roots + status enums (`ThreadStatus`, `TurnStatus`, `MessageRole`, `ContentType`, `ActivityType`).
- `domain/application/mod.rs` — (thin) application-layer hooks.
- `ports/mod.rs` — the 7 port traits + `PortError` + Git status DTOs.

## Public API
- **Aggregates:** `Project`, `Thread`/`ThreadStatus`, `Turn`/`TurnStatus`, `Message`/`MessageRole`/`ContentType`, `Activity`/`ActivityType`.
- **Events (44):** the `DomainEvent` enum (was 14) now spans project (3), thread lifecycle (created/status/title/checkpoint/revert/archive/delete/messages-import/session/mode/approval/edit), turn (started/completed/failed/cancelled/interrupted/files/checkpoint), message (added/delta/finalized), pinned-message (4), marker (4), plan/diff, revert/rollback, and activity events. serde-tagged `{event_type, data}`; `Envelope` adds monotonic `sequence`. (See `domain/events.rs` for the full enum.)
- **Ports (7):** `EventRepository`, `DomainEventPublisher`, `ReadModelRepository`, `GitServicePort`, `AutomationRepository`, `RunExecutor`, `ProviderPort` (all `async_trait`, `Send+Sync`). `PortError { NotFound, ConcurrencyConflict{expected,actual}, Internal }`.

## How it works
Pure domain layer — no I/O. Events are immutable facts; `Envelope` carries stream metadata (sequence = position in the aggregate stream, used for optimistic concurrency). Port traits are the dependency-inversion boundary: orchestration depends on these traits, concrete adapters implement them.

## Stubs / risks
- **Highest blast-radius crate** — any change ripples to all 11 crates.
- Note in `lib.rs`: the `DomainEvent` *trait* vs `DomainEvent` *enum* share a name; the trait is accessed via `domain::primitives::DomainEvent` / re-exported alias `DomainEventTrait` to avoid collision.

# Core Domain Kernel

`syncode-core` is the shared domain layer used by every bounded context in the
Syncode monorepo. It defines **entities**, **value objects**, **domain events**,
and **port interfaces** (traits) that higher-level crates depend on.

## Modules

| Module | Purpose |
|--------|---------|
| `application` | Application-service orchestration helpers |
| `domain` | Aggregate roots (`Project`, `Thread`, `Turn`, `Message`, `Activity`), value objects (`TrimmedString`, `EntityId`, `Timestamp`), domain events (`DomainEvent`) |
| `ports` | Trait definitions for external dependencies (`EventRepository`, `ProviderPort`, `GitServicePort`, `ReadModelRepository`) |

## Aggregate roots

| Aggregate | Root type | Key sub-types |
|----------|-----------|---------------|
| Project | `Project` | `EntityId`, `TrimmedString` |
| Thread | `Thread` | `ThreadStatus` |
| Turn | `Turn` | `TurnStatus` |
| Message | `Message` | `ContentType`, `MessageRole` |
| Activity | `Activity` | `ActivityType` |

## Port interfaces

| Port trait | Consumer |
|------------|----------|
| `EventRepository` | `syncode-persistence` |
| `ProviderPort` | `syncode-provider` |
| `GitServicePort` | `syncode-git` |
| `ReadModelRepository` | `syncode-persistence` projections |

## Integration points

- `syncode-orchestration` depends on `syncode-core::ports` and `syncode-core::domain`.
- `syncode-persistence` implements `EventRepository` and `ReadModelRepository`.
- `syncode-provider` implements `ProviderPort`.
- `syncode-git` implements `GitServicePort`.

## Stub status

**⚠️ One stub module.** `application/mod.rs` (3 lines) contains only:

```rust
// TODO: Phase 1+ -- Add use case implementations
```

All other modules (domain aggregates, events, ports) are fully implemented with
80+ unit tests. The application layer is deferred — use-case orchestration
currently lives in `syncode-orchestration::use_cases` instead.

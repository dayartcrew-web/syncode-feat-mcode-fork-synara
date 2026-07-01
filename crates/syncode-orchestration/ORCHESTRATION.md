# CQRS / Event Sourcing Engine

`syncode-orchestration` implements the core orchestration pattern that drives
the entire Syncode application:

```
Commands → Decider → Events (pure business logic)
Events   → Projector → Read Models
Events   → Reactors  → Side Effects
```

## Modules

| Module | Purpose |
|--------|---------|
| `decider` | `Decider` trait — pure function `(State, Command) → (State, Events)` |
| `events` | `DomainEvent` enum and event helpers |
| `pipeline` | `Orchestrator` — wires decider → event store → projector → reactors |
| `projector` | `Projector` trait — updates read models from events |
| `read_model` | `ReadModelStore` trait and in-memory / SQLite implementations |
| `reactors` | `CommandReactor` / `ProviderCommandReactor` — side-effect dispatch |
| `use_cases` | High-level application services (`ApplicationService`, `ProjectDashboard`, `ThreadDetail`) |

## Key types

| Type | Description |
|------|-------------|
| `Command` | Enum of all commands the system accepts |
| `Decider` | Trait: stateless decision function |
| `DeciderError` | Business-logic validation errors |
| `CommandResult` | `(Events, Reactions)` produced by a decider |
| `DomainEvent` | Enum of all domain events |
| `Orchestrator` | Top-level pipeline wiring |
| `OrchestrationError` | Unified error type |
| `Projector` | Trait: event → read-model update |
| `ReadModelStore` | Trait: typed queries on read models |

## Read-model views

| View | Description |
|------|-------------|
| `ProjectView` | Denormalized project summary |
| `ThreadView` | Thread status, message count, last activity |
| `TurnView` | Turn status, token usage, duration |
| `MessageView` | Message content and role |
| `ActivityView` | Audit-log entries |

## Integration points

- Consumes `syncode-core` domain types and port traits.
- Persists events via `syncode-persistence::event_store`.
- Updates read models via `syncode-persistence::projections`.
- Dispatches side-effects to `syncode-provider`, `syncode-git`, `syncode-automation`.
- Exposes queries to `syncode-tauri` IPC and `syncode-ws` RPC handlers.

## Stub status

All modules contain real implementations — no stubs remain.

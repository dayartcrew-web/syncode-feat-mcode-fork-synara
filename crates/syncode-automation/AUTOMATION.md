# Automation Scheduler

`syncode-automation` implements the scheduling engine for recurring and one-shot
tasks: cron expressions, fixed intervals, manual triggers, run lifecycle
management, and retry / misfire / completion policies.

## Modules

| Module | Purpose |
|--------|---------|
| `definition` | `AutomationDef`, `AutomationId`, `ScheduleType` — declarative task definition |
| `schedule` | Cron / interval / one-shot schedule parsing and due-time evaluation |
| `scheduler` | `Scheduler` — tick-based due-evaluation, spawning runs, enforcing policies |
| `runner` | `AutomationRun`, `RunStatus` — individual run lifecycle (pending → running → done / failed) |
| `executor` | Dispatch layer — hands a due run to the appropriate handler |
| `policies` | `RetryPolicy`, `MisfirePolicy`, `CompletionPolicy` — configurable guard-rails |
| `in_memory_repo` | Transient `AutomationDef` store for testing and single-process use |

## Key types

| Type | Description |
|------|-------------|
| `AutomationDef` | A named, scheduled automation entry |
| `ScheduleType` | Enum: `Cron`, `Interval`, `Once` |
| `AutomationRun` | State machine for a single execution instance |
| `RunStatus` | Enum: `Pending`, `Running`, `Completed`, `Failed`, `Cancelled` |
| `SchedulerError` | Unified error type for scheduling failures |

## Integration points

- Consumed by `syncode-orchestration` reactors that trigger side-effect runs.
- `AutomationRun` events are ingested through the CQRS pipeline as
  `DomainEvent::Automation*` variants.

## Stub status

All modules contain real implementations — no stubs remain.

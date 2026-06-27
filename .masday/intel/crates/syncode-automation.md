# syncode-automation
> Scheduler engine for scheduled agent runs — schedules, retry/misfire/completion policies, run lifecycle. **L1** · 1101 LOC · 19 tests
- **Depends on (internal):** `core` (only — **not wired to orchestration**).
- **External:** tokio, serde, chrono, uuid, thiserror, tracing.

## Files
- `lib.rs` (14 LOC) — exports.
- `definition.rs` (221 LOC) — `AutomationDef`, `ScheduleType`, `AutomationId`.
- `policies.rs` (188 LOC) — `RetryPolicy`, `MisfirePolicy`, `CompletionPolicy`.
- `runner.rs` (269 LOC) — `AutomationRun`, `RunStatus`.
- `scheduler.rs` (409 LOC) — `Scheduler` (RwLock-backed) + `SchedulerError`.

## Public API
- `ScheduleType` = Cron | Interval | OneShot | Manual.
- `AutomationDef` — full spec with builder.
- `RetryPolicy` = None | ExponentialBackoff | FixedDelay (delay calc present).
- `MisfirePolicy` = Skip | RunImmediately | RunNext.
- `CompletionPolicy` = ExitCodeZero | AllowedExitCodes | AlwaysSuccess | AiEvaluated.
- `RunStatus` = Pending→Running→(Completed|Failed|Cancelled|TimedOut|Retrying).
- `Scheduler`: register/list/trigger/update; `due_automations()`.

## Stubs / risks
- ⚠️ **Isolated from the engine** — depends on `core` only, not orchestration; triggers only create run records.
- **`due_automations` only supports OneShot** comparison (`scheduler.rs:99-114`); **Cron/Interval evaluation not implemented**.
- **No retry-loop execution** — policies compute delays but nothing runs them.
- **AI-evaluated completion** just checks `exit_code == 0` (`policies.rs:100`).
- **No heartbeat mode**, **no persistence** — all state in-memory, lost on restart.
- **No actual command execution** — `trigger` creates a run record only.

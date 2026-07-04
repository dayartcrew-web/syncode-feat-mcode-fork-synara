# syncode-automation

> ⚠️ **PRE-CLONE SNAPSHOT (2026-07-02).** This intel is from before the clone+rewire arc (PR #6–#47, 48 PRs total). For the current authoritative state see [`docs/STATUS.md`](../../../docs/STATUS.md).
>
> **Key changes since this snapshot:** Now has ProcessRunExecutor (executes automations via sh -c), AutomationRun extended (unread/archived_at fields), Scheduler wired into WS with event-push (run-upserted lifecycle events). ~78 tests.

> Scheduler engine for scheduled agent runs — schedules, retry/misfire/completion policies, run lifecycle, **execution engine**. **L1** · 2292 LOC · 67 tests
- **Depends on (internal):** `core` (ports: `AutomationRepository`, `RunExecutor`).
- **External:** tokio, serde, chrono, uuid, thiserror, tracing, **cron 0.15**, async-trait.

## Files
- `lib.rs` (16 LOC) — exports.
- `definition.rs` (~250 LOC) — `AutomationDef` (+ `next_run_at`, `target_thread_id`), `ScheduleType`, `AutomationId`.
- `policies.rs` (188 LOC) — `RetryPolicy`, `MisfirePolicy`, `CompletionPolicy`.
- `runner.rs` (269 LOC) — `AutomationRun`, `RunStatus`.
- `schedule.rs` (~200 LOC) — **real cron/interval/oneshot next-fire** (`next_fire`, `is_due`, `coalesce_missed`); 5-field cron normalized to 6-field for the `cron` crate.
- `executor.rs` (~300 LOC) — **run execution + retry loop** (`execute_run`); honors `RetryPolicy` with injectable `Delay` (Immediate for tests, Real for production); `dispatch_request_for` builds standalone vs heartbeat requests.
- `in_memory_repo.rs` (~140 LOC) — `InMemoryAutomationRepository` (the default `AutomationRepository` impl; SQLite is a drop-in follow-up).
- `scheduler.rs` (~600 LOC) — `Scheduler` (repo+executor backed) + `tick()` (due-eval+dispatch pass) + `NoopExecutor` (default) + `SchedulerError`.

## Public API
- `ScheduleType` = Cron(String) | Interval(u64) | OneShot(String) | Manual.
- `AutomationDef` — full spec with builder; `next_run_at` (scheduling pointer), `target_thread_id` (heartbeat mode).
- **Ports (in `syncode-core`):** `AutomationRepository` (save/get/list defs + runs, advance_next_run_at), `RunExecutor` (`dispatch_turn(DispatchRequest) → DispatchOutcome`).
- `Scheduler::new()` (in-memory + noop executor) / `new_with_deps(repo, executor)` (production).
- `Scheduler::tick(now)` — the due-eval+dispatch pass a host calls in a loop (mirrors MCode `runDueOnce`).
- `Scheduler::trigger(id)` — dispatches a run via `execute_run` (real retry loop).

## Status — engine ready, not yet hosted
- ✅ Real cron/interval/oneshot due-evaluation (was stubbed; only OneShot worked).
- ✅ Retry loop honoring `RetryPolicy` (MCode stubs this — "retry policies not supported yet"; Rust is ahead).
- ✅ Misfire coalesce (skip missed fires, fast-forward `next_run_at` — mirrors MCode).
- ✅ Run execution via injected `RunExecutor` trait (standalone = create+turn; heartbeat = turn on existing thread).
- ✅ `AutomationRepository` port — in-memory impl; SQLite is a drop-in follow-up.

## Stubs / risks / follow-ups
- ⚠️ **No production host process** — `tick()` is never called outside tests; needs a runtime that owns the Scheduler + spawns the tick loop.
- ⚠️ **No SQLite persistence** — `InMemoryAutomationRepository` only; port is ready.
- The `command` (subprocess) field diverges from MCode (which has no such field); execution goes through `RunExecutor`, not subprocess.
- AI-evaluated completion policy just checks `exit_code == 0` (MCode's `ai-evaluated` stop-check is future work).
- `stopOnError` (MCode's `maybeStopLoop`) not implemented — a failed run doesn't disable the automation.

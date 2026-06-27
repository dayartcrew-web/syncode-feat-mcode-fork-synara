# syncode-orchestration
> CQRS/Event-Sourcing engine — Decider, Orchestrator pipeline, Projector, Reactors, ApplicationService. **L2** · 3326 LOC · 57 tests
- **Depends on (internal):** `core`, `provider`.
- **External:** tokio, serde, thiserror, async-trait, tracing.

## Files
- `decider.rs` (25 KB) — `Command` enum (16) + `Decider::decide` pure logic + `DeciderError`.
- `events.rs` — re-exports `core::DomainEvent`.
- `pipeline.rs` (18 KB) — `Orchestrator`, `OrchestrationError`, `CommandResult`, full CQRS loop + replay.
- `projector.rs` (18 KB) — `Projector` + `ReadModelStore`.
- `read_model.rs` — `ProjectView`/`ThreadView`/`TurnView`/`MessageView`/`ActivityView`.
- `use_cases.rs` (21 KB) — `ApplicationService` (24 methods) + `ProjectDashboard`/`ThreadDetail`.
- `reactors/{command,ingestion}.rs` — side-effect bridges to providers.

## Public API
- **16 Commands:** `CreateProject`, `UpdateProjectConfig` · `CreateThread`, `PauseThread`, `ResumeThread`, `CompleteThread`, `CancelThread`, `SetThreadTitle` · `StartTurn`, `CompleteTurn`, `FailTurn`, `CancelTurn`, `RecordTurnFiles`, `SetTurnCheckpoint` · `AddMessage` (+1).
- **`Orchestrator::handle_command`** pipeline: load state → `Decider::decide` → `append_events` (optimistic concurrency, **no retry**) → `project_many` → optional `ProviderCommandReactor`. Also `ingest_provider_event`, `replay_read_model`.
- **Reactors:** `command.rs` maps `StartTurn`→start_session+send, `FailTurn`→interrupt, `CancelTurn`→stop, `Pause/CancelThread`→interrupt/stop all. `ingestion.rs` maps `ProviderEvent::{ToolCall,ToolResult}`→`ActivityLogged`, `Completed`→`TurnCompleted`, `Error`→`TurnFailed`.

## How it works
Pure decider (command+state→events, enforces invariants: non-empty names, state-transition guards, terminal-state protection, existence checks). Orchestrator is the only mutator; it persists then projects. Reactors are the seam to live providers.

## Stubs / risks
- Command-reactor side effects are **triggered but provider feedback events are not collected back** (comment `pipeline.rs ~165`) — turn completion relies solely on the ingestion path.
- **No optimistic-concurrency retry** — `ConcurrencyConflict` propagates to caller.
- `duration_ms = total_tokens * 10` **heuristic**, not wall-clock (`ingestion.rs:92`).
- **`CreateThread` does not validate the project exists** (`decider.rs ~244`) — can orphan threads.
- **No snapshot strategy** — `load_snapshot` returns None; long streams replay slowly.
- `Command` enum + `Orchestrator` are the sole coupling to `ws` (lib.rs:84,92; rpc.rs:9).

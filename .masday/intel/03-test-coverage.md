# 03 — Test Coverage

Source: `TEST_SUMMARY.md` + live `cargo test -p` (2026-07-02). **Total: 791 tests.** ~39,600 LOC / 12 crates + 1 integration package.

| Crate | Tests | Domain |
|---|---|---|
| `syncode-provider` | 276 | ProviderAdapter trait + 10 adapters (largest) |
| `syncode-orchestration` | 179 | CQRS pipeline: decider/projector/pipeline/reactors/use_cases |
| `syncode-automation` | 67 | Scheduler, retry/misfire/completion policies, run lifecycle |
| `syncode-ws` | 47 | WS server, JSON-RPC dispatch, push bus, channels, authz |
| `syncode-core` | 45 | EntityId, Timestamp, aggregates, DomainEvent, port traits |
| `syncode-git` | 40 | Git ops, checkpoint, worktree, stacked pipeline |
| `syncode-auth` | 39 | credentials, policy, principal, session, authenticator |
| `syncode-contracts` | 34 | Shared DTOs, TS bindings, JSON roundtrips |
| `syncode-persistence` | 25 | event_store, snapshot, projections, adapters |
| `syncode-terminal` | 20 | OutputBuffer, ack protocol, chunk mgmt, sessions |
| `syncode-http` | 0 | stub |
| `syncode-tauri` | 0 | excluded from workspace tests (pre-existing build issues) |
| `syncode-integration-tests` (`tests/`) | 19 | cross-crate workspace integration |

## Coverage gaps (from analysis)
- **No real provider integration tests** — provider tests use `MockSessionAdapter`; no real subprocess spawn, no real HTTP calls, no streaming, no concurrent-session tests.
- **No optimistic-concurrency path tests** in orchestration pipeline (the `ConcurrencyConflict` code path + caller retry is untested).
- **No full command→provider→ingestion E2E test** (the side-effect feedback loop is partially stubbed).
- **No integration tests run** in the default workspace test (`tests/workspace_integration.rs` exists but is a separate package).
- **`tauri` untested** at the workspace level — the desktop shell (tray, updater, IPC commands) has no CI coverage.
- **contracts:** `test_generate_ts_types` uses a **manual export list** (`lib.rs:248-263`) — new DTO types are silently missed if not added.
- **automation:** no test for cron/interval due-evaluation (it's unimplemented), no heartbeat mode, no retry-loop execution.

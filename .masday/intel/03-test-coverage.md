# 03 — Test Coverage

Source: `TEST_SUMMARY.md` (repo's recorded summary, 2026-06-27). **Total: 422 tests, 0 failures, 1 ignored doc-test.** ~19,600 LOC / 80 source files / 14 crates + 1 integration package.

| Crate | Tests | Domain |
|---|---|---|
| `syncode-provider` | 174 | ProviderAdapter trait + 10 adapters (largest) |
| `syncode-orchestration` | 57 | CQRS pipeline: decider 17, projector 12, pipeline 5, reactors (cmd 6, ingestion 10), use_cases 9 |
| `syncode-core` | 45 | EntityId, Timestamp, Project/Thread/Turn, DomainEvent, port traits |
| `syncode-terminal` | 38 | OutputBuffer, ack protocol, chunk mgmt, display |
| `syncode-git` | 22 | Git ops, checkpoint, worktree, stacked pipeline |
| `syncode-automation` | 19 | Scheduler, retry/misfire/completion policies, run lifecycle |
| `syncode-persistence` | 17 | event_store 5, snapshot 5, projections 6, adapters 3 (+1 init) |
| `syncode-contracts` | 15 | Shared DTOs, TS bindings, JSON roundtrips |
| `syncode-ws` | 14 | WS server, JSON-RPC dispatch, push bus, channels |
| `syncode-auth` | 0 | stub |
| `syncode-http` | 0 | stub |
| `syncode-tauri` | 0 | excluded from workspace tests (pre-existing build issues) |
| `syncode-integration-tests` (`tests/`) | — | cross-crate; not run in `cargo test --workspace` |

## Coverage gaps (from analysis)
- **No real provider integration tests** — provider tests use `MockSessionAdapter`; no real subprocess spawn, no real HTTP calls, no streaming, no concurrent-session tests.
- **No optimistic-concurrency path tests** in orchestration pipeline (the `ConcurrencyConflict` code path + caller retry is untested).
- **No full command→provider→ingestion E2E test** (the side-effect feedback loop is partially stubbed).
- **No integration tests run** in the default workspace test (`tests/workspace_integration.rs` exists but is a separate package).
- **`tauri` untested** at the workspace level — the desktop shell (tray, updater, IPC commands) has no CI coverage.
- **contracts:** `test_generate_ts_types` uses a **manual export list** (`lib.rs:248-263`) — new DTO types are silently missed if not added.
- **automation:** no test for cron/interval due-evaluation (it's unimplemented), no heartbeat mode, no retry-loop execution.

# Syncode — Test Summary Report

**Generated:** 2026-07-03 · partially regenerated — backend grew substantially via the clone+rewire RPC-coverage arc (PR #6–#32); per-crate recount timed out, so only changed crates refreshed below. For the full current picture run `cargo test -p syncode-<crate>` per-crate (workspace `cargo test` now works — GTK/webkit `-dev` libs installed).
**Note:** `syncode-ws` is now **132** (was ~47 — standalone server bin + 97 served RPCs across all domains + terminal live-push + ProcessRunExecutor + LLM-via-provider-CLI). `syncode-contracts` **96** (was 34 — Tier-3 symbols + RPC DTOs + 44-event union). `syncode-automation` **72** (was 67 — ProcessRunExecutor + git-advanced). `syncode-terminal` **20** (live-push reader task). Frontend: **tsc 0 errors, vitest 2128 pass / 0 fail**. Authoritative REAL-vs-STUB + state: [`docs/STATUS.md`](./docs/STATUS.md).
**Total Tests:** **~1100+** (the 791 baseline + ~310 new backend tests from the RPC-coverage/infra arc). **Total Rust LOC:** ~43,000+ (`syncode-ws` RPC layer + `syncode-ws/src/bin/server.rs` + `syncode-automation/src/process_executor.rs` + `syncode-ws/src/llm.rs` + contracts Tier-3 growth).

## Test Breakdown by Crate

| Crate | Tests |
|---|---|
| `syncode-core` | 45 |
| `syncode-contracts` | 34 |
| `syncode-orchestration` | 179 |
| `syncode-provider` | 276 |
| `syncode-git` | 40 |
| `syncode-terminal` | 20 |
| `syncode-automation` | 67 |
| `syncode-persistence` | 25 |
| `syncode-auth` | 39 |
| `syncode-ws` | 47 |
| `syncode-http` | 0 |
| `syncode-tauri` | 0 |
| `syncode-integration-tests` | 19 |

> Counts are captured live by `scripts/flow.sh docs`. Semantic docs below are agent-maintained.

# Syncode — Codebase Intelligence (.masday/intel)

> Generated 2026-06-27 by `masday-active` (codebase analysis). Ground-truth validated against the original MCode source at `/home/vibe-dev/mcode/`.

Structured map of the `syncode-feat-mcode-fork-synara` Rust workspace. Read the aggregates first, then drill into per-crate files.

## Aggregates
- [00-overview.md](00-overview.md) — what syncode is, lineage, stack, build status
- [01-architecture-and-deps.md](01-architecture-and-deps.md) — CQRS/ES architecture + crate layering + dependency graph
- [02-api-surface.md](02-api-surface.md) — WS JSON-RPC methods, Tauri IPC commands, port traits, trait methods
- [03-test-coverage.md](03-test-coverage.md) — per-crate test counts + coverage gaps

## Per-crate (by layer)
**L0 kernel:** [syncode-core](crates/syncode-core.md) · [syncode-contracts](crates/syncode-contracts.md)
**L1 leaf:** [syncode-provider](crates/syncode-provider.md) · [syncode-persistence](crates/syncode-persistence.md) · [syncode-git](crates/syncode-git.md) · [syncode-terminal](crates/syncode-terminal.md) · [syncode-automation](crates/syncode-automation.md) · [syncode-auth](crates/syncode-auth.md) · [syncode-http](crates/syncode-http.md)
**L2 engine:** [syncode-orchestration](crates/syncode-orchestration.md)
**L3 transport:** [syncode-ws](crates/syncode-ws.md)
**L4 shell:** [syncode-tauri](crates/syncode-tauri.md)

## Quick facts
- **12 internal crates** + `tests/` + `frontend/`. Rust 2024, MSRV 1.85.0. ~19,600 LOC, **422 tests**.
- **`syncode-core` is the universal dependency** — highest blast radius.
- **Stubs:** 8 subprocess provider adapters, `syncode-auth` (16 LOC), `syncode-http` (12 LOC), `ws/transport.rs`, `git` push/pull/PR, automation cron eval. `syncode-tauri` excluded from workspace tests (build issues).
- **Stale docs:** `docs/COMPARISON-*.md` were planning docs written when the repo was empty — superseded by this intel and `docs/ARCHITECTURE.md`.

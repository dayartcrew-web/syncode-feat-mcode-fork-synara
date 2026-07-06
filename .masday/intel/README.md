# Syncode — Codebase Intelligence (.masday/intel)

> Generated 2026-06-27 by `masday-active` (codebase analysis). Ground-truth validated against the original MCode source at `/home/vibe-dev/mcode/`.

Structured map of the `syncode-feat-mcode-fork-synara` Rust workspace. Read the aggregates first, then drill into per-crate files.

## Aggregates
- [00-overview.md](00-overview.md) — what syncode is, lineage, stack, build status
- [01-architecture-and-deps.md](01-architecture-and-deps.md) — CQRS/ES architecture + crate layering + dependency graph
- [02-api-surface.md](02-api-surface.md) — WS JSON-RPC methods, Tauri IPC commands, port traits, trait methods
- [03-test-coverage.md](03-test-coverage.md) — per-crate test counts + coverage gaps
- [04-frontend-wiring.md](04-frontend-wiring.md) — **REAL LIVE** frontend↔backend wiring map (transport, 113+ RPCs, push events, chat flow, LLM via provider CLI — NOT mock/stub)

## Per-crate (by layer)
**L0 kernel:** [syncode-core](crates/syncode-core.md) · [syncode-contracts](crates/syncode-contracts.md)
**L1 leaf:** [syncode-provider](crates/syncode-provider.md) · [syncode-persistence](crates/syncode-persistence.md) · [syncode-git](crates/syncode-git.md) · [syncode-terminal](crates/syncode-terminal.md) · [syncode-automation](crates/syncode-automation.md) · [syncode-auth](crates/syncode-auth.md) · [syncode-http](crates/syncode-http.md)
**L2 engine:** [syncode-orchestration](crates/syncode-orchestration.md)
**L3 transport:** [syncode-ws](crates/syncode-ws.md)
**L4 shell:** [syncode-tauri](crates/syncode-tauri.md)

## Quick facts
- **12 internal crates** + `tests/` + `frontend/`. Rust 2024, MSRV 1.85.0. ~39,600 LOC, **791 tests**.
- **`syncode-core` is the universal dependency** — highest blast radius.
- **Stubs remaining:** `syncode-http` (12 LOC), `ws/transport.rs` (reframed as an architectural note), `git` push/pull/PR CLI timeout not kill-enforced. **All 10 provider adapters are real; `syncode-auth` is wired (opt-in).** `syncode-tauri` excluded from workspace tests (build issues).
- **Stale docs:** `docs/COMPARISON-*.md` were planning docs written when the repo was empty — superseded by this intel and `docs/ARCHITECTURE.md`.

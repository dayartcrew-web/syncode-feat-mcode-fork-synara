# syncode-http

> ⚠️ **PRE-CLONE SNAPSHOT (2026-07-02).** This intel is from before the clone+rewire arc (PR #6–#47, 48 PRs total). For the current authoritative state see [`docs/STATUS.md`](../../../docs/STATUS.md).
>
> **Key changes since this snapshot:** Still a stub (12 LOC, 0 tests).

> HTTP REST API alongside the WebSocket transport. **L1** · 12 LOC · 0 tests · **STUB**
- **Depends on (internal):** `core`.
- **External:** axum 0.8, tower 0.5, tower-http 0.6, tokio, serde, thiserror, tracing (declared, unused).

## Files
- `lib.rs` (6 LOC) — barrel export of 2 modules.
- `routes.rs` (3 LOC) — TODO comment only.
- `middleware.rs` (3 LOC) — TODO comment only.

## Status
**Entire crate is a stub.** No Axum routes, no CORS/auth/logging middleware. The real runtime API today is the WebSocket server (`syncode-ws`); this crate is reserved for a future REST surface (likely mirroring WS RPC commands).

## Risks
- `syncode-tauri` does **not** compose an HTTP server — if REST is needed, it must be built here and wired into the shell.

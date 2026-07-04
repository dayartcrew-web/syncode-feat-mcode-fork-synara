# syncode-terminal

> ⚠️ **PRE-CLONE SNAPSHOT (2026-07-02).** This intel is from before the clone+rewire arc (PR #6–#47, 48 PRs total). For the current authoritative state see [`docs/STATUS.md`](../../../docs/STATUS.md).
>
> **Key changes since this snapshot:** Structurally unchanged; live output push wired in syncode-ws (per-session reader task → push_tx). PtyHandle reader uses std::Mutex (was tokio). 20 tests.

> PTY process management via portable-pty — spawn, resize, write, ack-buffered output, session lifecycle. **L1** · 699 LOC · 20 tests
- **Depends on (internal):** `core`.
- **External:** portable-pty 0.9, tokio, serde, thiserror, tracing, chrono, uuid.

## Files
- `lib.rs` (12 LOC) — barrel exports.
- `pty.rs` (244 LOC) — `PtyError`, `PtyProcessInfo`, `PtyHandle` (spawn/resize/write/read).
- `output.rs` (217 LOC) — `OutputChunk`, `OutputBuffer` (ring buffer + ack protocol).
- `session.rs` (226 LOC) — `TerminalSession`, `SessionManager`, `SessionInfo`.

## Public API
- `PtyHandle` wraps portable-pty `MasterPty` behind `Arc<Mutex<>>` (thread-safe async). `spawn()` → native PTY, `TERM=xterm-256color`, reader/writer halves; `resize`/`write`/`read_output`.
- `OutputBuffer`: `VecDeque` ring buffer, sequence-numbered chunks; `write()` chunks data at `max_chunk_size`, `ack(seq)` tracks highest acked, `unacked_chunks()` for retransmission.
- `SessionManager`: `HashMap<String, Arc<RwLock<TerminalSession>>>`, UUID session ids; `create_session`/`list_sessions`/`destroy_session`.
- DTOs (`PtyProcessInfo`, `SessionInfo`) are camelCase-serialized for the frontend.

## Stubs / risks
- **No child-process cleanup** on drop/destroy — `destroy_session` marks PTY stopped but doesn't kill the process (`session.rs:160`); child may keep running.
- `PtyProcessInfo.working_dir`/`command` are empty post-spawn (not tracked).
- Timestamps use `chrono::Utc::now().to_rfc3339()` — timezone handling to watch.

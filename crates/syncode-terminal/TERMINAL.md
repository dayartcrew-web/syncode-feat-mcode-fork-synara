# Terminal PTY

`syncode-terminal` manages pseudo-terminal (PTY) processes: spawning shells or
tools, resizing the PTY window, writing input, reading buffered output, and
tracking the lifecycle of terminal sessions.

## Modules

| Module | Purpose |
|--------|---------|
| `pty` | `PtyHandle`, `PtyProcessInfo` — low-level PTY spawn/resize/kill via `portable-pty` |
| `session` | `TerminalSession`, `SessionManager` — session lifecycle, keyed lookup, cleanup |
| `output` | `OutputBuffer`, `OutputChunk` — chunked output buffering with ack protocol for streaming to the frontend |

## Key types

| Type | Description |
|------|-------------|
| `PtyHandle` | Owning handle to a child PTY process |
| `PtyProcessInfo` | PID, command line, and cwd of a spawned process |
| `PtyError` | Unified error type for PTY operations |
| `TerminalSession` | A named session wrapping a `PtyHandle` + `OutputBuffer` |
| `SessionManager` | Registry of active sessions; create / lookup / kill / list |
| `SessionInfo` | Serialisable session metadata (id, title, pid, cwd) |
| `OutputChunk` | A chunk of raw terminal output (bytes + timestamp) |
| `OutputBuffer` | Bounded ring buffer with ack-based consumption |

## Integration points

- Consumed by `syncode-tauri` terminal IPC commands.
- Output is streamed to the frontend via `syncode-ws` push events.

## Stub status

All modules contain real implementations — no stubs remain.

# Syncode

A local-first AI-coding-agent desktop app built around a Rust CQRS / Event-Sourcing
orchestration engine. Syncode reimplements the orchestration core of
[MCode](https://github.com/dayartcrew-web/mcode) in Rust, exposing it over a
WebSocket JSON-RPC server that a Tauri desktop shell (and any other client) can
talk to.

- **Backend** — Rust 2024 edition workspace (12 crates), Tokio + Axum + SQLx/SQLite.
- **Frontend** — React 19 + Vite 6 + TypeScript 5.7, served inside the Tauri shell.
- **Provider bridge** — pluggable `ProviderAdapter` trait with real adapters for
  Claude, Codex, OpenCode, Gemini, Cursor, Grok, Kilo, and Pi, plus HTTP
  adapters for Anthropic and OpenAI.

> See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the layered design and
> [`docs/STATUS.md`](docs/STATUS.md) for the current real-vs-stub status.

## Features

- **CQRS engine** — 48 commands, 44 domain events, pure Decider, optimistic-
  concurrency-controlled event store, projector-backed read models, and reactor-
  driven provider side effects.
- **WebSocket JSON-RPC server** — ~97 RPCs across all MCode domains (project,
  thread, turn, message, git, terminal, automation, server config) plus push
  channels for live updates.
- **Provider orchestration** — start/stop/interrupt provider sessions, stream
  tokens, queued-turn pipeline, steering support, memory-augmented system prompts.
- **Git integration** — status, diff, branch, commit, checkpoint, worktree, and
  stacked PR actions via `git2` + `gh`.
- **Terminal** — portable-PTY sessions with ack-buffered live output push.
- **Automation** — cron/interval scheduler with retry, misfire, and completion
  policies.
- **Persistence** — SQLite event store + 7 projection tables + snapshots.

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Rust | 1.85.0+ (stable) | `rustup show` — edition 2024 |
| Node.js | 20+ | for the frontend |
| npm | bundled with Node | `npm ci` to install |
| SQLite | system library | only needed for the `bundled` fallback; SQLx ships its own |
| Tauri deps | Linux: `libwebkit2gtk-4.1`, `libgtk-3`, `libayatana-appindicator3-1` | only for the desktop shell build (see `docs/ARCHITECTURE.md`) |

Optional, for live chat verification:
- a provider CLI (`claude`, `codex`, …) installed and on `PATH`, **or**
- an `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` env var for the HTTP adapters.

## Quick start

### 1. Build & run the WS server (backend)

```bash
cargo run -p syncode-ws --bin server
# → listens on ws://127.0.0.1:3000/ws  (override with SYNCODE_WS_PORT)
```

### 2. Run the frontend dev server

```bash
cd frontend
npm ci
npm run dev          # Vite dev server (http://localhost:5173)
```

Point the desktop shell or any WS client at the running server.

### 3. Build the desktop app (Tauri shell)

```bash
cd frontend && npm ci && npm run build && cd ..
cargo build -p syncode-tauri          # Linux needs the webkit2gtk deps above
```

## Docker

The repo ships a multi-stage `Dockerfile` (builds backend + frontend, ships a
slim runtime image exposing port 3000) and a `docker-compose.yml` with a
bind-mounted data volume.

```bash
cp .env.example .env        # adjust defaults if needed
docker compose up --build   # build + run (foreground)
# or detached:
docker compose up -d --build
docker compose logs -f      # tail server logs
docker compose down         # stop + remove container (keeps ./data)
```

The published port is `3000:3000`. The SQLite DB lives under `./data/` on the
host (`/app/data` in the container) so it survives container restarts.

## Environment variables

All variables are optional and fall back to the documented default. See
[`.env.example`](.env.example) for the full reference.

| Variable | Default | Description |
|---|---|---|
| `SYNCODE_WS_HOST` | `0.0.0.0` | Bind host for the WS + HTTP listener. |
| `SYNCODE_WS_PORT` | `3000` | Bind port. |
| `SYNCODE_DB` | `/app/data/syncode.db` | SQLite DB path. Set empty (`SYNCODE_DB=`) for in-memory storage. |
| `SYNCODE_DEFAULT_PROVIDER` | `claude` | Provider id armed for the chat pipeline. |
| `RUST_LOG` | `syncode_ws=info,info` | tracing-subscriber filter (`syncode_ws=debug` for verbose logs). |
| `SYNCODE_SKILLS_DIR` | *(unset)* | Override the skills directory. |
| `SYNCODE_PLUGINS_DIR` | *(unset)* | Override the plugins directory. |

## Testing

```bash
# Backend — workspace tests (excludes syncode-tauri, which needs system deps)
cargo test --workspace --exclude syncode-tauri

# Clippy gate (CI enforces -D warnings per crate)
cargo clippy --workspace --exclude syncode-tauri --all-targets -- -D warnings

# Frontend
cd frontend
npm ci
npm run typecheck      # tsc --noEmit
npm test               # vitest run
```

The CI pipeline (`.github/workflows/ci.yml`) runs the same gates on every PR.

## Deploy

- **Docker (recommended)** — `docker compose up -d --build` is the deployment
  path. `restart: unless-stopped` reboots the container on crash/host reboot;
  the resume-cursor rehydration path makes this safe.
- **Binary** — `cargo build --release -p syncode-ws --bin server` produces a
  self-contained binary. Run it with the env vars above; front it with a reverse
  proxy for TLS.
- **Health** — the runtime image (Debian slim) has no `curl`/`wget`, so the
  Compose healthcheck probes PID 1 liveness. To upgrade to an HTTP probe, add
  `curl` to the Dockerfile runtime stage and point the healthcheck at
  `http://localhost:3000/`.

## Project layout

```
crates/
  syncode-core/          L0 domain kernel — entities, events, port traits
  syncode-contracts/     L0 shared DTOs + ts-rs codegen
  syncode-provider/      L1 ProviderAdapter trait + 10 adapters + registry
  syncode-persistence/   L1 SQLite event store + projections + snapshots
  syncode-git/           L1 git2: status/diff/branch/commit/checkpoint/worktree
  syncode-terminal/      L1 portable-pty PTY + ack-buffered output
  syncode-automation/    L1 scheduler + retry/misfire/completion
  syncode-auth/          L1 credentials, auth policy, secret store
  syncode-http/          L1 (stub) future REST surface
  syncode-orchestration/ L2 CQRS: Decider, Orchestrator, Projector, Reactors
  syncode-ws/            L3 WebSocket JSON-RPC server + push bus
  syncode-tauri/         L4 Tauri desktop shell
frontend/                React 19 + Vite UI
docs/                    ARCHITECTURE.md, STATUS.md, CRATES.md
tests/                   cross-crate integration tests
```

## License

MIT.

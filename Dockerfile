# syntax=docker/dockerfile:1.7
# Dockerfile for the Syncode WebSocket server.
#
# Multi-stage build:
#   1. `builder` — compiles the Rust workspace (`syncode-ws` → `server` binary,
#      release profile) and builds the frontend bundle (`frontend/dist`).
#   2. `runtime` — slim Debian image carrying only the server binary + the
#      pre-built static frontend, exposing port 3000.
#
# The runtime image intentionally excludes the Rust toolchain, Node.js, and the
# build artifacts cache (~several GB) — only what the server needs to run.
#
# Env vars consumed by the server binary (see crates/syncode-ws/src/bin/server.rs):
#   SYNCODE_WS_HOST  — bind host (default 127.0.0.1; we default to 0.0.0.0 here
#                      so the listener is container-reachable by default).
#   SYNCODE_WS_PORT  — bind port (default 3000).
#   SYNCODE_DB       — SQLite DB path (default syncode.db in cwd; "" → in-memory).
#   SYNCODE_DEFAULT_PROVIDER — provider id to arm the chat pipeline (default claude).
#   RUST_LOG         — tracing filter (default syncode_ws=info,info).

##############################################
# Stage 1: builder
##############################################
FROM rust:1-bookworm AS builder

# System dependencies for building the Rust workspace:
#   - pkg-config + libssl-dev: `reqwest` default-tls (native-tls → OpenSSL).
#   - cmake: `git2`/libgit2-sys vendor build.
# Node.js is installed via NodeSource (Debian Bookworm's apt node is too old for
# Vite 8 / the toolchain pinned in frontend/package.json).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        cmake \
        ca-certificates \
        curl \
        xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 22 LTS from NodeSource (matches the `@types/node` major used
# by the frontend toolchain — `@types/node: ^26` is forward-compatible).
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && node --version && npm --version

WORKDIR /app

# ---- Frontend build ---------------------------------------------------------
# Copy the frontend manifest + lockfile first and `npm ci` against it. This layer
# is cached unless package*.json change, so source edits don't reinstall deps.
COPY frontend/package.json frontend/package-lock.json ./frontend/
# turbo.jsonc + tsconfig.json + vite.config.ts are read by `npm run build`
# (tsc + vite) before any source is compiled, so include them up front.
COPY frontend/tsconfig.json frontend/vite.config.ts frontend/turbo.jsonc ./frontend/
COPY frontend/components.json ./frontend/

WORKDIR /app/frontend
RUN npm ci

# Now copy the rest of the frontend source and build the production bundle.
# Output: /app/frontend/dist
WORKDIR /app/frontend
COPY frontend/ ./
RUN npm run build && ls -la dist

# ---- Rust build -------------------------------------------------------------
WORKDIR /app

# Copy the workspace manifest and every member manifest first. We must copy the
# `tests/` directory too because it is a workspace member (`syncode-integration-
# tests`) — cargo resolves the entire workspace before building any package, so
# a missing member manifest breaks `cargo build -p syncode-ws`. We do NOT copy
# `src/` yet, so this dummy layer builds dependencies only and is cached across
# source edits (cargo sees no `src` and bails after compiling deps, but the
# dependency crate cache is preserved).
COPY Cargo.toml ./
COPY crates/syncode-core/Cargo.toml        ./crates/syncode-core/Cargo.toml
COPY crates/syncode-contracts/Cargo.toml    ./crates/syncode-contracts/Cargo.toml
COPY crates/syncode-orchestration/Cargo.toml ./crates/syncode-orchestration/Cargo.toml
COPY crates/syncode-provider/Cargo.toml     ./crates/syncode-provider/Cargo.toml
COPY crates/syncode-git/Cargo.toml          ./crates/syncode-git/Cargo.toml
COPY crates/syncode-terminal/Cargo.toml     ./crates/syncode-terminal/Cargo.toml
COPY crates/syncode-automation/Cargo.toml   ./crates/syncode-automation/Cargo.toml
COPY crates/syncode-persistence/Cargo.toml  ./crates/syncode-persistence/Cargo.toml
COPY crates/syncode-memory/Cargo.toml       ./crates/syncode-memory/Cargo.toml
COPY crates/syncode-auth/Cargo.toml         ./crates/syncode-auth/Cargo.toml
COPY crates/syncode-ws/Cargo.toml           ./crates/syncode-ws/Cargo.toml
COPY crates/syncode-http/Cargo.toml         ./crates/syncode-http/Cargo.toml
COPY crates/syncode-tauri/Cargo.toml        ./crates/syncode-tauri/Cargo.toml
COPY tests/Cargo.toml                        ./tests/Cargo.toml

# Pre-create stub source files so cargo can resolve the workspace layout for the
# dependency-fetch step without erroring on missing bin/lib targets. Cargo
# refuses to load a member manifest that has neither src/lib.rs, src/main.rs,
# nor an explicit [lib]/[[bin]] section, so every member needs a stub. These
# stubs are overwritten by the next COPY when the real sources arrive.
#
# All listed members are libraries (src/lib.rs) EXCEPT `syncode-tauri`, which is
# a binary crate (src/main.rs + src/lib.rs), and `tests`, which is an integration
# test target ([[test]] → tests/*.rs). `syncode-ws` ships a binary at
# src/bin/server.rs but also has src/lib.rs — for the deps-warm phase we only
# need the lib stub; the real source COPY below supplies the bin.
RUN mkdir -p tests \
    crates/syncode-core/src \
    crates/syncode-contracts/src \
    crates/syncode-orchestration/src \
    crates/syncode-provider/src \
    crates/syncode-git/src \
    crates/syncode-terminal/src \
    crates/syncode-automation/src \
    crates/syncode-persistence/src \
    crates/syncode-memory/src \
    crates/syncode-auth/src \
    crates/syncode-ws/src \
    crates/syncode-http/src \
    crates/syncode-tauri/src \
    && for c in core contracts orchestration provider git terminal automation \
                persistence memory auth ws http tauri; do \
        echo "" > "crates/syncode-$c/src/lib.rs"; \
    done \
    && echo "fn main() {}" > crates/syncode-tauri/src/main.rs \
    && echo "" > tests/dummy.rs

# Fetch + build dependency crates only. `--locked` is intentionally omitted
# because Cargo.lock is gitignored and will be generated fresh here; this step
# warms the registry + build cache. Cache mounts (BuildKit) persist cargo's
# git/registry across builds without bloating the final image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo fetch

# Now copy the actual source for the whole workspace. The dependency cache from
# the step above survives because the manifest-only layers below it are
# unchanged by source edits.
COPY crates/ ./crates/
COPY tests/  ./tests/

# Build the release binary. Cache mounts keep `/app/target` and the cargo
# registries out of the image and persistent across rebuilds.
# `--package syncode-ws --bin server` selects just the WS server binary from the
# workspace (other members — syncode-tauri desktop, integration tests — are not
# required for the server image and are skipped).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release -p syncode-ws --bin server \
    && cp /app/target/release/server /app/server

##############################################
# Stage 2: runtime
##############################################
FROM debian:bookworm-slim AS runtime

# Runtime system deps:
#   - ca-certificates: HTTPS calls from reqwest / git operations.
#   - libssl3: native-tls (OpenSSL) loaded by reqwest at runtime.
# `tini` provides PID 1 reaping + signal forwarding so the server's Ctrl-C
# graceful-shutdown handler actually receives SIGINT from `docker stop`.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the release binary from the builder stage.
COPY --from=builder /app/server /app/server

# Copy the pre-built frontend bundle. The server binary's `build_app` currently
# exposes only /ws, /health, and /api/project-favicon — it does not yet serve
# these static files. Including the bundle keeps the image self-contained for a
# future static-file mount / reverse proxy and matches the deployment intent
# (server + web client in one image).
COPY --from=builder /app/frontend/dist /app/frontend/dist

# Create a writable directory for the SQLite DB (default path: /app/data/syncode.db)
# so the server can persist events when SYNCODE_DB points there.
RUN mkdir -p /app/data
ENV SYNCODE_DB=/app/data/syncode.db

# Bind on all interfaces by default so the port is reachable from outside the
# container. The server binary's own default is 127.0.0.1 (loopback-only),
# which is unreachable from `docker run -p` port-forwarding — overriding here.
ENV SYNCODE_WS_HOST=0.0.0.0
ENV SYNCODE_WS_PORT=3000
ENV RUST_LOG=syncode_ws=info,info

EXPOSE 3000

# Persist the SQLite DB across container restarts.
VOLUME ["/app/data"]

# `tini` as PID 1 → forwards SIGINT/SIGTERM to the server, which triggers the
# graceful-shutdown handler in crates/syncode-ws/src/bin/server.rs (persists
# session resume cursors to disk before exiting).
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/app/server"]

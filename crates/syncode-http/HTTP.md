# HTTP API

`syncode-http` provides REST routes and middleware that complement the primary
WebSocket transport in `syncode-ws`. It is intended for health checks,
configuration endpoints, and any HTTP-only integrations.

## Modules

| Module | Purpose |
|--------|---------|
| `routes` | HTTP endpoint definitions (Axum handlers) |
| `middleware` | Request logging, CORS, auth extraction, rate-limiting |

## Design notes

The WebSocket layer (`syncode-ws`) is the primary transport for bidirectional
communication. `syncode-http` covers the remaining HTTP surface:

- `GET /health` — liveness probe for orchestration / load-balancer
- `GET /version` — build-info endpoint
- Future: file-upload endpoints, webhook receivers

## Integration points

- Mounted by the Tauri shell (`syncode-tauri`) alongside the WebSocket server.
- Shares `syncode-auth` middleware for authenticated endpoints.

## Stub status

**⛔ ENTIRE CRATE IS A STUB.** Both `routes.rs` and `middleware.rs` contain only
TODO comments with no implementation:

```rust
// routes.rs — 3 lines total:
//! HTTP routes
// TODO: Implement routes

// middleware.rs — 3 lines total:
//! Middleware — CORS, auth, logging
// TODO: Implement middleware
```

No routes, no middleware, no Axum handlers, no tests. This crate is a
placeholder awaiting a future HTTP surface alongside the WebSocket transport.

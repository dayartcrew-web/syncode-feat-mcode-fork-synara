//! Syncode HTTP — REST API surface for the standalone server.
//!
//! Owns the stateless REST routes (`/health`, `/api/project-favicon`) and
//! reusable HTTP middleware (CORS, tracing, request-id). The WebSocket server
//! binary (`syncode-ws`) merges [`routes::http_router`] into its Axum app so
//! the standalone backend serves both transports from one process.
//!
//! # Why a leaf crate?
//!
//! These routes carry no orchestrator/session state, so they belong in the L1
//! leaf rather than the L4 WS crate. The WS crate (higher layer) depends on
//! this crate (lower layer) — direction-correct layering.
//!
//! # Quick start
//!
//! ```no_run
//! use syncode_http::routes::http_router;
//!
//! let router = http_router(); // GET /health, GET /api/project-favicon
//! ```

pub mod middleware;
pub mod routes;

// Convenience re-exports at crate root for the most-used items.
pub use routes::http_router;

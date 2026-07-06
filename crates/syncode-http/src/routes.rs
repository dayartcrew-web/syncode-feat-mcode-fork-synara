//! HTTP routes for the standalone Syncode server.
//!
//! This crate owns the stateless REST surface (`/health`, `/api/project-favicon`)
//! so the WebSocket server binary (`syncode-ws`) can merge it via
//! [`http_router`] instead of inlining the handlers. Routes here are
//! intentionally state-free: they need no `WsState` / orchestrator handle, so
//! they live in the L1 leaf rather than the L4 WS crate.
//!
//! # Routes
//!
//! | Method | Path                     | Purpose                                   |
//! |--------|--------------------------|-------------------------------------------|
//! | GET    | `/health`                | Liveness + version + uptime JSON          |
//! | GET    | `/api/project-favicon`   | 1x1 transparent PNG (browser placeholder) |
//!
//! Wiring: `syncode-ws::server::build_app` calls [`http_router`] and
//! `Router::merge`s it with the WS router, so the standalone server exposes
//! both transports from one Axum app.

use std::sync::OnceLock;
use std::time::Instant;

use axum::Router;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;

/// Crate version (compile-time, from `Cargo.toml`).
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Process-wide start timestamp, captured on first access (lazy via `OnceLock`)
/// so uptime is reported relative to the first request rather than to crate
/// load time — robust against binary restarts and test process reuse.
fn start_time() -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now)
}

/// Build the stateless HTTP router.
///
/// The returned [`Router<()>`] carries no shared state and is safe to
/// `Router::merge` into a larger app (e.g. the WS server's `build_app`).
/// Re-invoking this function builds a fresh router; the lazily-initialised
/// start-time singleton is shared across routers in the same process so
/// uptime remains monotonic.
pub fn http_router() -> Router<()> {
    Router::new()
        .route("/health", get(health_handler))
        .route("/api/project-favicon", get(project_favicon_handler))
}

/// `GET /health` — liveness probe with version + uptime.
///
/// Returns JSON: `{ "status": "ok", "version": "<pkg>", "uptime_secs": <f64> }`.
/// The `status` field lets load balancers do a simple body check; `version`
/// aids release identification; `uptime_secs` helps diagnose restart loops.
async fn health_handler() -> impl IntoResponse {
    let uptime_secs = start_time().elapsed().as_secs_f64();
    axum::Json(serde_json::json!({
        "status": "ok",
        "version": VERSION,
        "uptime_secs": uptime_secs,
    }))
}

/// `GET /api/project-favicon` — 1x1 transparent PNG placeholder.
///
/// The MCode frontend requests this to display a project icon; without it the
/// browser logs a 404. A real implementation would probe the project's website
/// for a favicon, but a placeholder is sufficient for dev/test. Returns
/// `Content-Type: image/png` so the browser renders it inline.
async fn project_favicon_handler() -> impl IntoResponse {
    // 1x1 transparent PNG (67 bytes). Sourced from the canonical minimal PNG;
    // any browser treats this as a valid transparent pixel.
    const TRANSPARENT_PNG: &[u8] = &[
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F,
        0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00,
        0x01, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49,
        0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    ];
    ([(header::CONTENT_TYPE, "image/png")], TRANSPARENT_PNG)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// Helper: dispatch a GET against the router and return (status, body bytes).
    async fn get_route(path: &str) -> (StatusCode, Vec<u8>) {
        let response = http_router()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .expect("router dispatch must not error");
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body collect must not error")
            .to_bytes();
        (status, bytes.to_vec())
    }

    #[tokio::test]
    async fn health_returns_ok_with_version_and_uptime() {
        let (status, body) = get_route("/health").await;
        assert_eq!(status, StatusCode::OK);

        let json: serde_json::Value =
            serde_json::from_slice(&body).expect("health body must be valid JSON");
        assert_eq!(json["status"], "ok");
        assert_eq!(
            json["version"],
            serde_json::Value::from(VERSION),
            "version must match crate version"
        );
        let uptime = json["uptime_secs"]
            .as_f64()
            .expect("uptime_secs must be a number");
        assert!(uptime >= 0.0, "uptime must be non-negative, got {uptime}");
    }

    #[tokio::test]
    async fn favicon_serves_png_with_correct_content_type() {
        let response = http_router()
            .oneshot(
                Request::builder()
                    .uri("/api/project-favicon")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("router dispatch must not error");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/png",
            "favicon must be served as image/png"
        );

        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body collect must not error")
            .to_bytes();
        // PNG magic bytes — sanity check the payload is a real PNG.
        assert!(bytes.len() > 8, "favicon body must be non-trivial");
        assert_eq!(
            &bytes[..8],
            &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]
        );
    }

    #[tokio::test]
    async fn unknown_path_returns_404() {
        let (status, _body) = get_route("/does-not-exist").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn router_builds_without_panicking() {
        // Smoke test: building the router must be cheap and panic-free.
        let _router = http_router();
    }
}

//! HTTP middleware — CORS, tracing, request-id.
//!
//! Provides reusable Axum layers the server binary can apply to the merged
//! router. Kept minimal and stateless: no auth (handled in `syncode-auth` for
//! the WS RPC path), no rate limiting (deferred).

use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue, Response};
use axum::middleware::Next;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

/// Standard `X-Request-Id` header name.
pub const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// A permissive CORS layer suitable for dev servers (browser frontends on a
/// different origin). Allows any origin, method, and header so the Tauri shell
/// and standalone web UI can reach the REST endpoints during development.
///
/// For production, tighten this to the known frontend origin.
pub fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any)
}

/// A `tracing`-backed HTTP layer that spans every request with method + URI.
pub fn trace_layer() -> TraceLayer<
    tower_http::classify::SharedClassifier<tower_http::classify::ServerErrorsAsFailures>,
> {
    TraceLayer::new_for_http()
}

/// Middleware that stamps an `X-Request-Id` echo header on every response.
///
/// If the inbound request carries an `X-Request-Id`, it is mirrored back; if
/// not, the response still carries the header (empty value) so downstream
/// proxies see the field exists. This is a minimal correlation aid — the WS
/// RPC layer already does richer per-message correlation.
pub async fn request_id_layer(request: Request, next: Next) -> Response<axum::body::Body> {
    let inbound = request
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&inbound) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::{Router, middleware};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn ok_handler() -> &'static str {
        "ok"
    }

    fn app_with_request_id() -> Router {
        Router::new()
            .route("/ping", get(ok_handler))
            .layer(middleware::from_fn(request_id_layer))
    }

    #[tokio::test]
    async fn request_id_is_mirrored_when_present() {
        let response = app_with_request_id()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/ping")
                    .header(&REQUEST_ID_HEADER, "abc-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(&REQUEST_ID_HEADER).unwrap(),
            "abc-123"
        );
    }

    #[tokio::test]
    async fn request_id_layer_preserves_body() {
        let response = app_with_request_id()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/ping")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(&bytes[..], b"ok");
    }

    #[test]
    fn cors_and_trace_layers_build_without_panicking() {
        let _cors = cors_layer();
        let _trace = trace_layer();
    }
}

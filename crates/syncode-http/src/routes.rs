//! HTTP routes for the standalone Syncode server.
//!
//! This crate owns the stateless REST surface so the WebSocket server binary
//! (`syncode-ws`) can merge it via [`http_router`] instead of inlining the
//! handlers. Routes here are intentionally state-free: they need no `WsState`
//! / orchestrator handle, so they live in the L1 leaf rather than the L4 WS
//! crate.
//!
//! # Routes
//!
//! | Method | Path                     | Purpose                                       |
//! |--------|--------------------------|-----------------------------------------------|
//! | GET    | `/health`                | Liveness + version + uptime JSON              |
//! | GET    | `/api/project-favicon`   | 1x1 transparent PNG (browser placeholder)     |
//! | GET    | `/api/editor-icon`       | 1x1 transparent PNG placeholder (per editor)  |
//! | GET    | `/api/local-image`       | Serves images from disk (path-traversal-guard)|
//! | GET    | `/api/site-favicon`      | Probes `<url>/favicon.ico`, falls back to PNG |
//!
//! Wiring: `syncode-ws::server::build_app` calls [`http_router`] and
//! `Router::merge`s it with the WS router, so the standalone server exposes
//! both transports from one Axum app.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Instant;

use axum::Router;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use serde::Deserialize;

/// Crate version (compile-time, from `Cargo.toml`).
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 1x1 transparent PNG (67 bytes). Used as a generic placeholder for
/// `/api/project-favicon`, `/api/editor-icon`, and `/api/site-favicon`
/// fallback. Any browser treats this as a valid transparent pixel.
const TRANSPARENT_PNG: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
    0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
    0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
    0x42, 0x60, 0x82,
];

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
        .route("/api/editor-icon", get(editor_icon_handler))
        .route("/api/local-image", get(local_image_handler))
        .route("/api/site-favicon", get(site_favicon_handler))
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
    ([(header::CONTENT_TYPE, "image/png")], TRANSPARENT_PNG)
}

/// `GET /api/editor-icon?id={editorId}` — 1x1 transparent PNG placeholder.
///
/// v0.1.5: the frontend's `resolveNativeEditorIcon` already falls back to a
/// React component when the `<image>` load fails (which it will for this
/// placeholder). Returns the same PNG regardless of `id` — no SVG asset
/// pipeline to maintain. Cheap and ship-safe.
async fn editor_icon_handler(Query(params): Query<EditorIconParams>) -> impl IntoResponse {
    // `id` is currently ignored — every editor gets the same placeholder. Kept
    // in the query string for forward compatibility (a future iteration could
    // dispatch on `id` to serve per-editor SVGs).
    let _ = params.id;
    ([(header::CONTENT_TYPE, "image/png")], TRANSPARENT_PNG)
}

#[derive(Debug, Deserialize)]
struct EditorIconParams {
    #[serde(default)]
    id: Option<String>,
}

/// `GET /api/local-image?path={absPath}` — serves images from disk (e.g.
/// pasted screenshots stored under the server's data dir).
///
/// **Path-traversal guard:** canonicalizes the requested path and rejects any
/// that don't begin with one of the allowed roots (server data dir, temp dir,
/// current working dir). Returns 403 with a generic message on rejection —
/// never echoes the canonical path back (it could leak filesystem layout).
async fn local_image_handler(Query(params): Query<LocalImageParams>) -> impl IntoResponse {
    let Some(path_str) = params.path else {
        return (StatusCode::BAD_REQUEST, "missing path").into_response();
    };
    let requested = PathBuf::from(&path_str);
    match validate_local_image_path(&requested).await {
        Ok(()) => match tokio::fs::read(&requested).await {
            Ok(bytes) => {
                let mime = sniff_image_mime(&requested);
                (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, mime)],
                    bytes.to_vec(),
                )
                    .into_response()
            }
            Err(e) => {
                tracing::warn!(error = %e, path = %path_str, "local-image read failed");
                (StatusCode::NOT_FOUND, "image not found").into_response()
            }
        },
        Err(()) => (StatusCode::FORBIDDEN, "path outside allowed roots").into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct LocalImageParams {
    #[serde(default)]
    path: Option<String>,
}

/// `GET /api/site-favicon?url={url}` — proxies favicon fetch (CSP forbids
/// direct cross-origin from the webview).
///
/// Fetches `<url>/favicon.ico` via `reqwest` with a 5s timeout. On any failure
/// (DNS, timeout, non-2xx, transport) returns the transparent PNG placeholder
/// so the UI always gets an image. The browser's `<img onError>` will surface
/// the failure visually if desired.
async fn site_favicon_handler(Query(params): Query<SiteFaviconParams>) -> impl IntoResponse {
    let Some(url) = params.url else {
        return (StatusCode::BAD_REQUEST, "missing url").into_response();
    };
    let fetch_url = match derive_favicon_url(&url) {
        Some(u) => u,
        None => {
            return (StatusCode::BAD_REQUEST, "invalid url").into_response();
        }
    };
    match fetch_favicon(&fetch_url).await {
        Ok(bytes) => {
            let mime = sniff_favicon_mime(&bytes);
            (StatusCode::OK, [(header::CONTENT_TYPE, mime)], bytes).into_response()
        }
        Err(e) => {
            tracing::debug!(error = %e, url = %fetch_url, "favicon fetch failed — returning placeholder");
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "image/png")],
                TRANSPARENT_PNG.to_vec(),
            )
                .into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct SiteFaviconParams {
    #[serde(default)]
    url: Option<String>,
}

/// Resolve which favicon URL to fetch from the user-supplied string. Accepts
/// bare hosts (`example.com`), origin URLs (`https://example.com`), and full
/// paths (`https://example.com/blog`). Always appends `/favicon.ico`.
fn derive_favicon_url(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }
    // If the caller already supplied a full path with a scheme, treat as-is.
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        let origin = trimmed.split('?').next().unwrap_or(trimmed);
        let origin = origin.trim_end_matches('/');
        return Some(format!("{origin}/favicon.ico"));
    }
    // Bare host — assume https (browsers do this too for address-bar favicons).
    let host = trimmed.split('/').next().unwrap_or(trimmed);
    if host.is_empty() {
        return None;
    }
    Some(format!("https://{host}/favicon.ico"))
}

/// Fetch favicon bytes via a short-lived `reqwest` client. 5s timeout —
/// adequate for well-behaved sites, caps damage from slow / hanging origins.
async fn fetch_favicon(url: &str) -> Result<Vec<u8>, reqwest::Error> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;
    let resp = client.get(url).send().await?.error_for_status()?;
    resp.bytes().await.map(|b| b.to_vec())
}

/// Sniff Content-Type from image file extension. Conservative — anything we
/// can't recognize falls back to `application/octet-stream` so the browser
/// forces a download rather than misrendering.
fn sniff_image_mime(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
        .as_deref()
    {
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("svg") => "image/svg+xml",
        Some("bmp") => "image/bmp",
        _ => "application/octet-stream",
    }
}

/// Sniff Content-Type from favicon magic bytes. Browsers usually ship
/// `image/x-icon` (ICO container) or `image/png`; both render via `<img>`.
fn sniff_favicon_mime(bytes: &[u8]) -> &'static str {
    if bytes.len() >= 8 && bytes.starts_with(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]) {
        return "image/png";
    }
    if bytes.len() >= 6 && (&bytes[0..2] == b"BM") {
        return "image/bmp";
    }
    if bytes.len() >= 6 && bytes[0..6] == [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10] {
        return "image/jpeg";
    }
    // Default for ICOs and anything else — browsers handle it.
    "image/x-icon"
}

/// Validate that `path` is inside one of the allowed roots. Returns `Ok(())`
/// if so, `Err(())` otherwise. Canonicalizes the path and compares it against
/// canonical forms of the allowed roots — symlinks pointing outside are
/// rejected by the canonical-prefix check.
///
/// Allowed roots (whitelist — NOT a blacklist):
/// - `%APPDATA%\syncode\images\` (Windows) / `~/.local/share/syncode/images/` (Linux/macOS)
/// - System temp dir (e.g. `%TEMP%` / `/tmp`)
/// - Current working directory (for Vite dev server's asset paths)
async fn validate_local_image_path(path: &PathBuf) -> Result<(), ()> {
    let canonical = match tokio::fs::canonicalize(path).await {
        Ok(c) => c,
        Err(_) => return Err(()),
    };

    let allowed_roits = allowed_image_roots();
    for root in &allowed_roits {
        if canonical.starts_with(root) {
            return Ok(());
        }
    }
    Err(())
}

/// Compute the allowed image roots (canonical where possible). Lazily
/// initialized per-call rather than cached — these paths rarely change and
/// the cost is negligible compared to file IO.
fn allowed_image_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();

    // Server data dir (cross-platform).
    if let Some(data_dir) = syncode_data_dir() {
        roots.push(data_dir.join("images"));
    }

    // System temp dir (covers pasted-screenshot flows).
    if let Ok(temp) = std::env::temp_dir().canonicalize() {
        roots.push(temp);
    }

    // Current working dir (Vite dev server's asset paths).
    if let Ok(cwd) = std::env::current_dir().and_then(|p| p.canonicalize()) {
        roots.push(cwd);
    }

    roots
}

/// Resolve the per-user data directory in a platform-aware way. Mirrors the
/// Tauri `paths.rs` logic without depending on Tauri (this crate is the L1
/// leaf — no Tauri dependency).
fn syncode_data_dir() -> Option<PathBuf> {
    if cfg!(target_os = "windows") {
        std::env::var_os("APPDATA").map(|p| PathBuf::from(p).join("syncode"))
    } else if cfg!(target_os = "macos") {
        std::env::var_os("HOME")
            .map(|p| PathBuf::from(p).join("Library/Application Support/syncode"))
    } else {
        let xdg = std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".local/share")));
        xdg.map(|p| p.join("syncode"))
    }
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

    #[tokio::test]
    async fn editor_icon_returns_placeholder_png() {
        // `id` is currently ignored — every editor gets the same 1x1 PNG. The
        // frontend's `<image onError>` falls back to a React icon.
        let (status, body) = get_route("/api/editor-icon?id=cursor").await;
        assert_eq!(status, StatusCode::OK);
        // PNG magic bytes.
        assert_eq!(
            &body[..8],
            &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]
        );
    }

    #[tokio::test]
    async fn editor_icon_works_without_id_param() {
        // Frontend always passes `?id=...`; missing id should still respond
        // (defensive — never 400 a placeholder).
        let (status, _body) = get_route("/api/editor-icon").await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn local_image_rejects_missing_path_param() {
        let (status, _body) = get_route("/api/local-image").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn local_image_rejects_nonexistent_path_outside_allowed_roots() {
        // A bare path like `/etc/passwd` (Linux) or a Windows root path will
        // either fail canonicalization (→ 403) or fall outside the allowed
        // roots (→ 403). Either outcome is correct — the key assertion is
        // that we DO NOT serve the file.
        let (status, _body) = get_route("/api/local-image?path=/etc/passwd").await;
        assert!(
            status == StatusCode::FORBIDDEN || status == StatusCode::NOT_FOUND,
            "expected 403/404 for path outside allowed roots, got {status}"
        );
    }

    #[tokio::test]
    async fn local_image_serves_file_inside_temp_dir() {
        // Write a test file under the system temp dir (one of the allowed
        // roots) and confirm we can serve it.
        let dir = std::env::temp_dir().join("syncode-http-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.png");
        std::fs::write(&path, [0x89, 0x50, 0x4E, 0x47]).unwrap();

        let uri = format!(
            "/api/local-image?path={}",
            url_encode(&path.to_string_lossy())
        );
        let (status, body) = get_route(&uri).await;
        assert_eq!(status, StatusCode::OK, "uri: {uri}");
        assert_eq!(&body[..4], &[0x89, 0x50, 0x4E, 0x47]);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[tokio::test]
    async fn site_favicon_returns_placeholder_on_invalid_url() {
        // Invalid URL → 400 (missing/invalid). Should never crash.
        let (status, _body) = get_route("/api/site-favicon?url=").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn site_favicon_returns_placeholder_on_unreachable_host() {
        // Reserved-docs TLD — guaranteed not to resolve. Should fall back to
        // the placeholder rather than 5xx.
        let (status, body) =
            get_route("/api/site-favicon?url=https://nonexistent.invalid.favicon.test").await;
        assert_eq!(status, StatusCode::OK);
        // PNG magic bytes — placeholder fallback.
        assert_eq!(
            &body[..8],
            &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]
        );
    }

    #[test]
    fn router_builds_without_panicking() {
        // Smoke test: building the router must be cheap and panic-free.
        let _router = http_router();
    }

    /// Minimal URL-encoder for test query strings (avoids a `urlencoding`
    /// dependency just for tests). Only handles characters likely to appear
    /// in paths (backslashes, colons on Windows).
    fn url_encode(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'/' | b'\\' | b':' | b'?' | b'#' | b' ' => {
                    out.push_str(&format!("%{:02X}", b));
                }
                _ if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') => {
                    out.push(b as char);
                }
                _ => out.push_str(&format!("%{:02X}", b)),
            }
        }
        out
    }
}

//! v0.1.5 smoke E2E — full server parity (providers + HTTP + auth REST).
//!
//! Bo the exact `syncode_tauri::ws_setup::boot()` path the desktop `.setup()`
//! runs, then drives a single booted server through every surface v0.1.5
//! changed:
//!
//! 1. **JSON-RPC over WS** — project + thread + settings + welcome cycles
//!    (proves the unified `build_orchestrator` wires provider dispatch and
//!    settings persistence into the Tauri shell).
//! 2. **HTTP routes** — `/health`, `/api/editor-icon`, `/api/local-image`
//!    (path-traversal guard), `/api/site-favicon` (placeholder fallback).
//! 3. **Auth REST** — `/api/auth/session`, `/api/auth/pairing-token`,
//!    `/api/auth/pairing-links`, `/api/auth/clients`, and
//!    `/api/auth/clients/revoke-others`.
//!
//! The test is hermetic: ephemeral port + in-memory DB + no provider CLI
//! required. The orchestrator falls back to inert mode when the provider
//! binary is missing, so thread/pause/resume cycle the state machine without
//! generating AI responses.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use syncode_tauri::ws_setup::{WsConfig, WsRuntimeState, boot};
use tokio_tungstenite::tungstenite::Message;

/// Boot the desktop WS/HTTP server on an ephemeral port backed by an
/// in-memory store, mirroring `main.rs::setup()`. Returns:
/// - `ws_url`  — `ws://host:port/ws` for JSON-RPC traffic
/// - `http_origin` — `http://host:port` for HTTP / auth REST traffic
/// - `serve_task` — abort to tear the server down
async fn boot_server() -> (String, String, std::sync::Arc<tokio::task::JoinHandle<()>>) {
    let config = WsConfig {
        host: "127.0.0.1".into(),
        port: 0,                // ephemeral — no port collision across tests
        db_path: String::new(), // in-memory → hermetic
        default_provider: "claude".into(),
    };
    let handle = boot(&config)
        .await
        .expect("ws_setup::boot() on an ephemeral port must succeed");

    // Mirror main.rs: thread the handle through WsRuntimeState exactly the way
    // `.setup()` does — proves the v0.1.5 wiring is what the IPC layer sees.
    let runtime_state = WsRuntimeState::new();
    runtime_state.set(handle.clone());
    let ws_url = runtime_state
        .endpoint()
        .expect("endpoint readable via WsRuntimeState");

    // The same Axum app serves WS at /ws and HTTP elsewhere on the same port
    // (see crates/syncode-ws/src/server.rs::build_app — merges ws_router +
    // auth_rest_router + http_router). Derive HTTP origin from the WS URL.
    let http_origin = ws_url
        .strip_suffix("/ws")
        .and_then(|s| s.strip_prefix("ws://"))
        .map(|host| format!("http://{host}"))
        .unwrap_or_else(|| {
            ws_url
                .replace("ws://", "http://")
                .trim_end_matches("/ws")
                .to_string()
        });

    (ws_url, http_origin, handle.serve_task)
}

/// Send a JSON-RPC request frame and read back the matching response (matched
/// by `id`). 5s timeout — hung server → test failure, not suite hang.
async fn rpc_call(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    method: &str,
    params: Value,
) -> Value {
    let id = json!(uuid::Uuid::new_v4().to_string());
    let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    stream
        .send(Message::Text(request.to_string().into()))
        .await
        .expect("send JSON-RPC frame");

    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(msg)) = stream.next().await {
            if let Message::Text(text) = msg {
                let v: Value = serde_json::from_str(&text).expect("parse json");
                if v.get("id").is_some() {
                    return v;
                }
            }
        }
        panic!("stream closed without a response");
    })
    .await
    .expect("timeout reading JSON-RPC response")
}

/// The full v0.1.5 smoke matrix. One [`tokio::test`] drives every surface so
/// the test report points at the first broken contract (rather than spreading
/// across many tests); assertions are scoped with sub-function boundaries for
/// readability. Boots the real server once, runs the WS-RPC matrix, then
/// re-uses the same server for HTTP and auth REST assertions.
///
/// # Live-binary mode
///
/// When `DESKTOP_LIVE_WS_URL=ws://host:port/ws` is set, the test skips the
/// in-process `boot()` and drives the **actual desktop binary** through the
/// same matrix. Pair with the manual UI smoke steps in `README.md` — this
/// covers the boot/WS/HTTP surface headlessly against the real .exe, the
/// manual steps cover what a human needs to see in the window.
#[tokio::test]
async fn v015_full_server_parity_smoke() {
    // Live-binary mode: skip boot, use the env-provided endpoint + derived
    // HTTP origin. Serve task is None — we don't own the binary.
    let (ws_url, http_origin, serve): (
        String,
        String,
        Option<std::sync::Arc<tokio::task::JoinHandle<()>>>,
    ) = match std::env::var("DESKTOP_LIVE_WS_URL")
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(url) => {
            let http_origin = url
                .strip_suffix("/ws")
                .and_then(|s| s.strip_prefix("ws://"))
                .map(|host| format!("http://{host}"))
                .unwrap_or_else(|| {
                    url.replace("ws://", "http://")
                        .trim_end_matches("/ws")
                        .to_string()
                });
            (url, http_origin, None)
        }
        None => {
            let (ws, http, serve) = boot_server().await;
            (ws, http, Some(serve))
        }
    };

    // ─── Phase 1: JSON-RPC over WS (orchestrator + settings) ──────────────
    let (mut stream, resp) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("WS connect to booted desktop endpoint");
    assert_eq!(resp.status(), 101, "WS upgrade must succeed");

    // ping — server is dispatching JSON-RPC.
    let ping = rpc_call(&mut stream, "ping", json!({})).await;
    assert!(
        ping.get("error").is_none(),
        "ping error: {:?}",
        ping["error"]
    );

    rpc_project_cycle(&mut stream).await;
    let project_id = rpc_project_id(&mut stream).await;
    rpc_thread_cycle(&mut stream, &project_id).await;
    rpc_settings_cycle(&mut stream).await;
    rpc_welcome_payload(&mut stream).await;

    // Disconnect cleanly so the connection registry reaps the entry.
    let _ = stream.close(None).await;

    // ─── Phase 2: HTTP routes (syncode-http) ──────────────────────────────
    http_routes_smoke(&http_origin).await;

    // ─── Phase 3: Auth REST (syncode-ws::auth_rest) ───────────────────────
    auth_rest_smoke(&http_origin).await;

    // Tear down only the server we own (live-binary mode leaves the .exe
    // running so the human smoke can continue against the same instance).
    if let Some(serve) = serve {
        serve.abort();
    }
}

/// Project create → list → get cycle.
async fn rpc_project_cycle(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let created = rpc_call(
        stream,
        "project/create",
        json!({ "name": "v015-smoke", "rootPath": "/tmp/v015-smoke" }),
    )
    .await;
    assert!(
        created.get("error").is_none(),
        "project/create failed: {:?}",
        created["error"]
    );

    let listed = rpc_call(stream, "project/list", json!({})).await;
    let projects = listed["result"]["projects"]
        .as_array()
        .expect("project/list returns a projects array");
    assert!(
        projects.iter().any(|p| p["name"] == "v015-smoke"),
        "project/list missing v015-smoke: {listed}"
    );

    // project/get — round-trip on the freshly-created project.
    let project_id = projects
        .iter()
        .find(|p| p["name"] == "v015-smoke")
        .and_then(|p| p["id"].as_str())
        .expect("project has an id")
        .to_string();
    let got = rpc_call(stream, "project/get", json!({ "id": project_id })).await;
    assert!(
        got.get("error").is_none(),
        "project/get failed: {:?}",
        got["error"]
    );
}

/// Look up the v015-smoke project id via project/list (used by the thread
/// cycle that needs a real projectId).
async fn rpc_project_id(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> String {
    let listed = rpc_call(stream, "project/list", json!({})).await;
    listed["result"]["projects"]
        .as_array()
        .expect("project/list returns a projects array")
        .iter()
        .find(|p| p["name"] == "v015-smoke")
        .and_then(|p| p["id"].as_str())
        .expect("v015-smoke project id")
        .to_string()
}

/// Thread create (with projectId / providerId / model) → list → pause →
/// resume cycle. Proves the unified orchestrator wires provider dispatch
/// into the Tauri shell — before v0.1.5 the desktop orchestrator was a stub
/// and `thread/create` 404'd.
async fn rpc_thread_cycle(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    project_id: &str,
) {
    let created = rpc_call(
        stream,
        "thread/create",
        json!({
            "projectId": project_id,
            "providerId": "claude",
            "model": "sonnet",
        }),
    )
    .await;
    assert!(
        created.get("error").is_none(),
        "thread/create failed: {:?}",
        created["error"]
    );
    let thread_id = created["result"]["id"]
        .as_str()
        .expect("thread/create returns an id")
        .to_string();

    let listed = rpc_call(stream, "thread/list", json!({ "projectId": project_id })).await;
    let threads = listed["result"]["threads"]
        .as_array()
        .expect("thread/list returns a threads array");
    assert!(
        threads.iter().any(|t| t["id"] == thread_id),
        "thread/list missing the created thread: {listed}"
    );

    // pause / resume — exercises the orchestrator command channel (proves
    // adapter spawn happened; the stub orchestrator pre-v0.1.5 had no
    // command adapter and these would error out).
    let paused = rpc_call(stream, "thread/pause", json!({ "id": thread_id })).await;
    assert!(
        paused.get("error").is_none(),
        "thread/pause failed: {:?}",
        paused["error"]
    );

    let resumed = rpc_call(stream, "thread/resume", json!({ "id": thread_id })).await;
    assert!(
        resumed.get("error").is_none(),
        "thread/resume failed: {:?}",
        resumed["error"]
    );
}

/// server/getSettings → server/updateSettings → server/getSettings cycle.
/// Proves the deep-merge patch landed and the settings store is the same one
/// the WS handlers see (the v0.1.5 fix that attached the SQLite pool to the
/// in-memory store — without that, writes were lost).
async fn rpc_settings_cycle(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    // Baseline: settings is a JSON object.
    let before = rpc_call(stream, "server/getSettings", json!({})).await;
    assert!(
        before.get("error").is_none(),
        "server/getSettings failed: {:?}",
        before["error"]
    );
    assert!(
        before["result"].is_object(),
        "settings must be an object: {}",
        before["result"]
    );

    // Pick a key the frontend uses — theme is always present + always a
    // string. Patching it should land a new value the next get reads.
    let patch = json!({ "theme": "v015-smoke-theme" });
    let updated = rpc_call(stream, "server/update-settings", patch).await;
    assert!(
        updated.get("error").is_none(),
        "server/update-settings failed: {:?}",
        updated["error"]
    );

    // Re-read — patch must be visible (proves the store the WS handler reads
    // is the same one updateSettings mutated).
    let after = rpc_call(stream, "server/getSettings", json!({})).await;
    assert_eq!(
        after["result"]["theme"].as_str(),
        Some("v015-smoke-theme"),
        "settings patch did not land: {}",
        after["result"]
    );
}

/// server/welcome returns the homeDir / cwd / serverVersion payload the
/// frontend boot flow needs.
async fn rpc_welcome_payload(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let welcome = rpc_call(stream, "server/welcome", json!({})).await;
    assert!(
        welcome.get("error").is_none(),
        "server/welcome failed: {:?}",
        welcome["error"]
    );
    let result = &welcome["result"];
    assert!(
        !result["homeDir"].as_str().unwrap_or("").trim().is_empty(),
        "welcome.homeDir must be non-empty: {result}"
    );
    assert!(
        !result["cwd"].as_str().unwrap_or("").trim().is_empty(),
        "welcome.cwd must be non-empty: {result}"
    );
    assert_eq!(result["authRequired"], json!(false));
}

/// HTTP routes added/fixed in v0.1.5: /health, /api/editor-icon,
/// /api/local-image (traversal guard), /api/site-favicon (placeholder
/// fallback).
async fn http_routes_smoke(http_origin: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client");

    // /health — liveness + version.
    let health = client
        .get(format!("{http_origin}/health"))
        .send()
        .await
        .expect("GET /health");
    assert_eq!(health.status(), reqwest::StatusCode::OK);
    let health_json: Value = health.json().await.expect("health json");
    assert_eq!(health_json["status"], "ok");
    assert!(
        !health_json["version"].as_str().unwrap_or("").is_empty(),
        "health.version must be non-empty"
    );

    // /api/editor-icon — placeholder PNG regardless of id.
    let editor = client
        .get(format!("{http_origin}/api/editor-icon?id=cursor"))
        .send()
        .await
        .expect("GET /api/editor-icon");
    assert_eq!(editor.status(), reqwest::StatusCode::OK);
    assert_eq!(
        editor
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap_or("")),
        Some("image/png"),
        "editor-icon must be served as image/png"
    );
    let bytes = editor.bytes().await.expect("editor-icon body");
    assert!(
        bytes.len() > 8 && bytes.starts_with(&[0x89, 0x50, 0x4E, 0x47]),
        "editor-icon body must be a PNG"
    );

    // /api/local-image — path-traversal guard. An absolute path outside the
    // allowlist must be rejected (403 or 404 — either is safe; the contract
    // is "do NOT serve the file").
    let blocked = client
        .get(format!("{http_origin}/api/local-image"))
        .query(&[("path", "/etc/passwd")])
        .send()
        .await
        .expect("GET /api/local-image outside allowlist");
    assert!(
        blocked.status() == reqwest::StatusCode::FORBIDDEN
            || blocked.status() == reqwest::StatusCode::NOT_FOUND,
        "expected 403/404 for path outside allowlist, got {}",
        blocked.status()
    );

    // /api/local-image — a file inside the temp-dir allowlist must serve.
    let dir = std::env::temp_dir().join("syncode-v015-smoke");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let file_path = dir.join("pixel.png");
    std::fs::write(&file_path, [0x89u8, 0x50, 0x4E, 0x47]).expect("write temp png");
    let allowed = client
        .get(format!("{http_origin}/api/local-image"))
        .query(&[("path", file_path.to_string_lossy().as_ref())])
        .send()
        .await
        .expect("GET /api/local-image inside allowlist");
    assert_eq!(
        allowed.status(),
        reqwest::StatusCode::OK,
        "temp-dir file should serve"
    );
    let body = allowed.bytes().await.expect("local-image body");
    assert_eq!(&body[..4], &[0x89, 0x50, 0x4E, 0x47]);
    let _ = std::fs::remove_file(&file_path);
    let _ = std::fs::remove_dir(&dir);

    // /api/site-favicon — invalid host → placeholder PNG (200 OK with PNG
    // body, never a 5xx — the frontend's <img onError> can render the
    // fallback).
    let placeholder = client
        .get(format!("{http_origin}/api/site-favicon"))
        .query(&[("url", "https://nonexistent.invalid.favicon.test")])
        .send()
        .await
        .expect("GET /api/site-favicon");
    assert_eq!(placeholder.status(), reqwest::StatusCode::OK);
    let placeholder_bytes = placeholder.bytes().await.expect("favicon body");
    assert!(
        placeholder_bytes.len() > 8
            && placeholder_bytes.starts_with(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]),
        "favicon fallback must be a PNG"
    );
}

/// Auth REST routes added in v0.1.5: 5 representative endpoints out of the
/// 10 mounted. The full surface is unit-tested in `auth_rest.rs`; this proves
/// they are reachable through the booted desktop server.
async fn auth_rest_smoke(http_origin: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client");

    // /api/auth/session — bare object with `authenticated`.
    let session = client
        .get(format!("{http_origin}/api/auth/session"))
        .send()
        .await
        .expect("GET /api/auth/session");
    assert_eq!(session.status(), reqwest::StatusCode::OK);
    let session_json: Value = session.json().await.expect("session json");
    assert!(
        session_json.get("authenticated").is_some(),
        "session must include `authenticated`: {session_json}"
    );

    // /api/auth/pairing-token — POST {} returns a credential.
    let paired = client
        .post(format!("{http_origin}/api/auth/pairing-token"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("POST /api/auth/pairing-token");
    assert_eq!(paired.status(), reqwest::StatusCode::OK);
    let paired_json: Value = paired.json().await.expect("pairing-token json");
    assert!(
        paired_json.get("credential").is_some(),
        "pairing-token must return a credential: {paired_json}"
    );

    // /api/auth/pairing-links — bare array (frontend contract).
    let links = client
        .get(format!("{http_origin}/api/auth/pairing-links"))
        .send()
        .await
        .expect("GET /api/auth/pairing-links");
    assert_eq!(links.status(), reqwest::StatusCode::OK);
    let links_json: Value = links.json().await.expect("pairing-links json");
    assert!(
        links_json.is_array(),
        "pairing-links must be a bare array: {links_json}"
    );

    // /api/auth/clients — bare array (frontend contract).
    let clients = client
        .get(format!("{http_origin}/api/auth/clients"))
        .send()
        .await
        .expect("GET /api/auth/clients");
    assert_eq!(clients.status(), reqwest::StatusCode::OK);
    let clients_json: Value = clients.json().await.expect("clients json");
    assert!(
        clients_json.is_array(),
        "clients must be a bare array: {clients_json}"
    );

    // /api/auth/clients/revoke-others — POST returns revokedCount.
    let revoke = client
        .post(format!("{http_origin}/api/auth/clients/revoke-others"))
        .send()
        .await
        .expect("POST /api/auth/clients/revoke-others");
    assert_eq!(revoke.status(), reqwest::StatusCode::OK);
    let revoke_json: Value = revoke.json().await.expect("revoke json");
    assert!(
        revoke_json.get("revokedCount").is_some(),
        "revoke-others must return revokedCount: {revoke_json}"
    );
}

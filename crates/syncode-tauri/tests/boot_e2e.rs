//! DSK-3 — Desktop boot end-to-end tests.
//!
//! Closes the "boot E2E not verified" gap from `docs/STATUS.md` by exercising
//! the boot path at two layers:
//!
//! 1. **WS-layer boot E2E** (headless, always runs) —
//!    [`ws_setup_boot_wiring_e2e`]: calls the exact [`syncode_tauri::ws_setup::boot`]
//!    the desktop `.setup()` calls, stores the handle in a
//!    [`WsRuntimeState`] (mirroring `main.rs`), reads the endpoint back via the
//!    same accessor the `getWsEndpoint` IPC command uses, then connects a real
//!    WS client over TCP and round-trips JSON-RPC (ping + project create/list +
//!    disconnect). Proves the **complete `.setup()` wiring** — boot → managed
//!    state → endpoint accessor → WS handler — boots and serves without a
//!    display. This is the closest a headless test can get to the real setup
//!    path; `tests/ws_spawn.rs` (DSK-1) covers only the spawn primitives.
//!
//! 2. **Full-shell boot E2E** (CI-only, against a running binary) —
//!    [`desktop_binary_boot_connects_ws`]: connects to the **actual desktop
//!    binary** that CI started under `xvfb-run`. The binary's `.setup()` creates
//!    the webview window THEN boots the WS server, so a successful WS handshake
//!    proves the full Tauri shell (window + IPC + WS) came up. Gated by the
//!    `DESKTOP_E2E_WS_URL` env var: when unset the test passes with a documented
//!    skip (so `cargo test` stays green locally without the binary); CI sets the
//!    var in [`.github/workflows/desktop-e2e.yml`].
//!
//! # Manual procedure (full GUI verification)
//!
//! The full-shell test needs a display + the webview shared libs + the frontend
//! dist (for `tauri::generate_context!()`). On Linux:
//!
//! ```sh
//! # 0. Install Tauri Linux system deps (one-time):
//! #    sudo apt install -y libwebkit2gtk-4.1-dev libgtk-3-dev \
//! #       libayatana-appindicator3-dev librsvg2-dev patchelf xvfb
//! # 1. Build the frontend (generate_context! embeds the dist at compile time)
//! (cd frontend && npm ci && npm run build)
//! # 2. Build the desktop binary
//! cargo build -p syncode-tauri
//! # 3. Boot under a virtual display — xvfb-run provides the X server headless
//! SYNCODE_DB= SYNCODE_WS_PORT=30142 \
//!   xvfb-run -a ./target/debug/syncode-tauri &
//! # 4. Run the full-shell boot E2E against the live binary
//! DESKTOP_E2E_WS_URL=ws://127.0.0.1:30142/ws \
//!   cargo test -p syncode-tauri --test boot_e2e desktop_binary_boot_connects_ws -- --nocapture
//! # 5. Tear down
//! kill %1
//! ```
//!
//! On Windows the binary boots against the installed WebView2 runtime (no
//! virtual display needed); run steps 1-2 then start the binary directly and
//! point `DESKTOP_E2E_WS_URL` at its port.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use syncode_tauri::ws_setup::{WsConfig, WsRuntimeState, boot};
use tokio_tungstenite::tungstenite::Message;

/// Boot a server on an ephemeral port backed by an in-memory `WsState`, then
/// hand the handle to a fresh [`WsRuntimeState`] exactly like `main.rs::setup`
/// does (`app.state::<WsRuntimeState>().set(handle)`). Returns the endpoint URL
/// read back FROM the managed state (exercising the `getWsEndpoint` accessor
/// path) + the serve task handle (aborted by the caller to stop the server).
async fn boot_and_manage() -> (String, std::sync::Arc<tokio::task::JoinHandle<()>>) {
    let config = WsConfig {
        host: "127.0.0.1".into(),
        port: 0, // ephemeral → no port collision across tests
        db_path: String::new(), // in-memory → hermetic, no filesystem
        default_provider: "claude".into(),
    };
    let handle = boot(&config)
        .await
        .expect("ws_setup::boot() should succeed on an ephemeral port");

    // Mirror main.rs: store the handle in managed WsRuntimeState. A real App
    // would do `app.state::<WsRuntimeState>().set(handle)`; here we exercise the
    // same type directly (it is the single source of truth the IPC command
    // reads, so this proves the wiring end-to-end minus the Tauri App wrapper).
    let runtime_state = WsRuntimeState::new();
    runtime_state.set(handle.clone());

    // Read the endpoint back via the same accessor `getWsEndpoint` uses
    // (`WsRuntimeState::endpoint`). A mismatch here would mean setup wrote a
    // different endpoint than the one IPC commands surface to the frontend.
    let endpoint = runtime_state
        .endpoint()
        .expect("endpoint should be readable from WsRuntimeState after set()");
    assert_eq!(
        endpoint, handle.endpoint,
        "managed-state endpoint must match the booted handle's endpoint"
    );

    (endpoint, handle.serve_task)
}

/// Send a JSON-RPC request and read back the matching response (matched by
/// `id`). Times out after 5s so a hung server fails the test instead of
/// hanging the suite.
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

/// **WS-layer boot E2E (headless).** Boots via the same `ws_setup::boot()`
/// `main.rs::setup()` calls, threads the handle through `WsRuntimeState`
/// (the managed state `getWsEndpoint` reads), then drives a real WS client
/// through ping → project/create → project/list → disconnect.
///
/// This is the canonical "desktop boot E2E" for the WS layer: it proves the
/// complete wiring path that runs inside `.setup()` (minus the Tauri App /
/// window, which need a display — see [`desktop_binary_boot_connects_ws`]).
#[tokio::test]
async fn ws_setup_boot_wiring_e2e() {
    let (url, serve) = boot_and_manage().await;

    // A WS handshake succeeding against the endpoint surfaced by managed state
    // is the core "WS connects" assertion.
    let (mut stream, resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect to the managed-state endpoint");
    assert_eq!(
        resp.status(),
        101,
        "server should upgrade to WebSocket (101 Switching Protocols)"
    );

    // ping — server dispatches JSON-RPC.
    let ping = rpc_call(&mut stream, "ping", json!({})).await;
    assert!(ping.get("error").is_none(), "ping error: {:?}", ping["error"]);
    assert!(ping.get("result").is_some(), "ping must return a result: {ping}");

    // project/create + project/list — exercises the shared WsState's
    // orchestrator through the booted server (proves the managed WsState is the
    // same one serving WS traffic, which is the DSK-1 invariant DSK-3 relies
    // on for "WS connects to the real backend").
    let created = rpc_call(
        &mut stream,
        "project/create",
        json!({ "name": "dsk-3-boot-e2e", "rootPath": "/tmp/dsk-3-boot-e2e" }),
    )
    .await;
    assert!(
        created.get("error").is_none(),
        "project/create failed: {:?}",
        created["error"]
    );

    let listed = rpc_call(&mut stream, "project/list", json!({})).await;
    let projects = listed["result"]["projects"]
        .as_array()
        .expect("project/list returns a projects array");
    assert!(
        projects.iter().any(|p| p["name"] == "dsk-3-boot-e2e"),
        "booted server should serve the created project: {listed}"
    );

    // Disconnect cleanly so the connection registry reaps the entry.
    let _ = stream.close(None).await;
    serve.abort();
}

/// **Full-shell boot E2E (CI-only).** Connects to the actual desktop binary
/// that CI booted under `xvfb-run` (Linux) or directly (Windows + WebView2).
///
/// The binary's `.setup()` creates the webview window **before** booting the WS
/// server, so reaching the WS handshake proves the entire Tauri shell came up:
/// app context → window creation → `ws_setup::boot()` → managed state →
/// `/ws` listener. That closes the "boot E2E not verified" gap for real
/// (window + WS) rather than just the WS layer.
///
/// # When this runs
///
/// - In CI: `.github/workflows/desktop-e2e.yml` builds the binary, starts it
///   under `xvfb-run` with `SYNCODE_WS_PORT`, and exports
///   `DESKTOP_E2E_WS_URL=ws://127.0.0.1:<port>/ws` before invoking this test.
/// - Locally without the binary: the test **passes with a documented skip**
///   when `DESKTOP_E2E_WS_URL` is unset, so `cargo test` stays green. Follow
///   the manual procedure in the module docs to run it for real.
#[tokio::test]
async fn desktop_binary_boot_connects_ws() {
    let Some(url) = std::env::var("DESKTOP_E2E_WS_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!(
            "skip: DESKTOP_E2E_WS_URL unset — desktop binary not running. \
             See tests/boot_e2e.rs module docs for the xvfb-run procedure."
        );
        return;
    };

    // The binary may still be coming up (xvfb-run + webview init adds latency).
    // Retry the handshake for up to ~15s before declaring failure.
    let mut stream = None;
    for attempt in 0..30 {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((s, resp)) => {
                assert_eq!(
                    resp.status(),
                    101,
                    "desktop binary should upgrade to WebSocket (got {})",
                    resp.status()
                );
                stream = Some(s);
                break;
            }
            Err(e) => {
                eprintln!("boot E2E connect attempt {attempt} failed: {e}; retrying…");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    let mut stream = stream.expect(
        "could not connect to the desktop binary's WS endpoint within ~15s — \
         the shell/window likely failed to boot (check xvfb + webkit2gtk + \
         frontend dist)",
    );

    // Reaching the WS handler means setup() completed and the window was
    // created. Pin the contract with a ping round-trip.
    let ping = rpc_call(&mut stream, "ping", json!({})).await;
    assert!(
        ping.get("error").is_none(),
        "desktop binary WS ping returned an error: {:?}",
        ping["error"]
    );
    assert!(ping.get("result").is_some(), "ping must return a result: {ping}");

    let _ = stream.close(None).await;
}

//! Integration test — DSK-1: WS server spawn inside the Tauri shell.
//!
//! Boots the same `ws_setup::spawn_with_state` the desktop binary's `.setup()`
//! calls, then connects a real WS client over TCP and round-trips a JSON-RPC
//! frame (boot → connect → ping/pong → disconnect). Proves the spawn wiring is
//! correct without needing a full Tauri runtime (which requires a display +
//! `tauri::generate_context!`).
//!
//! Uses an in-memory `WsState` (no SQLite) so the test is hermetic — no
//! filesystem, no port assumptions (binds port 0 → OS-assigned).

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use syncode_tauri::ws_setup::{WsConfig, boot, spawn_with_state};
use syncode_ws::WsState;
use tokio_tungstenite::tungstenite::Message;

/// Boot a server on an ephemeral port (port 0 → OS-assigned) backed by an
/// in-memory `WsState`. Returns the client-reachable `ws://…/ws` URL + the
/// serve task handle (aborted on drop / at end of test to stop the server).
async fn boot_in_memory() -> (String, std::sync::Arc<tokio::task::JoinHandle<()>>) {
    let state = WsState::new_in_memory(256);
    // port 0 → OS picks an ephemeral free port, sidestepping collisions.
    let config = WsConfig {
        host: "127.0.0.1".into(),
        port: 0,
        db_path: String::new(), // in-memory
        default_provider: "claude".into(),
    };
    let handle = spawn_with_state(state, &config)
        .await
        .expect("WS server should boot on an ephemeral port");
    (handle.endpoint, handle.serve_task)
}

/// End-to-end boot via the same `boot()` entry point `main.rs::setup()` calls.
///
/// Where the other tests use `spawn_with_state` (passing a pre-built in-memory
/// `WsState`), this exercises the full `boot()` flow — i.e. `build_state` +
/// `spawn_with_state` chained — with `db_path` empty so it stays hermetic
/// (in-memory, no filesystem). This is the closest a headless test can get to
/// the real `.setup()` boot path.
#[tokio::test]
async fn ws_boot_full_path_boots_and_serves() {
    let config = WsConfig {
        host: "127.0.0.1".into(),
        port: 0,
        db_path: String::new(), // in-memory → hermetic
        default_provider: "claude".into(),
    };
    let handle = boot(&config).await.expect("boot() should succeed");

    let mut stream = tokio_tungstenite::connect_async(&handle.endpoint)
        .await
        .expect("connect")
        .0;
    let resp = rpc_call(&mut stream, "ping", json!({})).await;
    assert!(
        resp.get("error").is_none(),
        "boot() server should answer ping"
    );

    let _ = stream.close(None).await;
    handle.serve_task.abort();
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

/// **Boot** — the server starts and accepts a connection at `/ws`.
#[tokio::test]
async fn ws_spawn_boots_and_accepts_connection() {
    let (url, serve) = boot_in_memory().await;

    // Connect a real WS client — if the server didn't boot / isn't listening
    // on the reported endpoint, this fails.
    let (stream, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect to /ws");
    assert_eq!(
        _resp.status(),
        101,
        "server should upgrade to WebSocket (101 Switching Protocols)"
    );

    drop(stream);
    serve.abort();
}

/// **Connect + ping round-trip** — the spawned server dispatches JSON-RPC.
///
/// `ping` returns an empty result object (`{}`) — the standalone
/// `ws_e2e.rs::ws_real_tcp_ping_pong` asserts `"pong"`, but that test is gated
/// behind `SYNCODE_WS_E2E=1` and never runs; the real handler
/// (`rpc.rs:81`) returns `Value::Object(empty)`. This test pins the actual
/// contract: a successful response with no error.
#[tokio::test]
async fn ws_spawn_ping_round_trip() {
    let (url, serve) = boot_in_memory().await;
    let mut stream = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect")
        .0;

    let response = rpc_call(&mut stream, "ping", json!({})).await;
    assert_eq!(response["jsonrpc"], "2.0");
    // `ping` returns an empty result object — the key assertion is that a
    // response is dispatched with no error (the server round-tripped the frame
    // through the JSON-RPC handler).
    assert!(
        response.get("error").is_none(),
        "unexpected error: {:?}",
        response["error"]
    );
    assert!(
        response.get("result").is_some(),
        "ping must return a result field: {response}"
    );

    let _ = stream.close(None).await;
    serve.abort();
}

/// **Connect + project create + list** — exercises the shared WsState's
/// orchestrator end-to-end through the spawned server.
#[tokio::test]
async fn ws_spawn_project_create_and_list() {
    let (url, serve) = boot_in_memory().await;
    let mut stream = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect")
        .0;

    let create_resp = rpc_call(
        &mut stream,
        "project/create",
        json!({ "name": "dsk-1-test", "rootPath": "/tmp/dsk-1-test" }),
    )
    .await;
    assert!(
        create_resp.get("error").is_none(),
        "project/create failed: {:?}",
        create_resp["error"]
    );

    let list_resp = rpc_call(&mut stream, "project/list", json!({})).await;
    let projects = list_resp["result"]["projects"]
        .as_array()
        .expect("project/list returns a projects array");
    assert!(
        projects.iter().any(|p| p["name"] == "dsk-1-test"),
        "created project should appear in list: {:?}",
        list_resp
    );

    let _ = stream.close(None).await;
    serve.abort();
}

/// **Disconnect** — after the client closes, the server's connection registry
/// drops the entry (no leak). Verified via the shared `WsState`: clone it for
/// the server, keep one for inspection (clones share the same `Arc`'d
/// connection map, so mutations the server makes are visible to the test).
#[tokio::test]
async fn ws_spawn_disconnect_removes_connection() {
    let state = WsState::new_in_memory(256);
    // The server takes one clone; we keep another to inspect the connection
    // registry. Because every WsState field is an Arc, both clones observe the
    // SAME connection map — this is exactly the "shared WsState" property DSK-1
    // requires between Tauri commands and WS handlers.
    let inspector = state.clone();
    let config = WsConfig {
        host: "127.0.0.1".into(),
        port: 0,
        db_path: String::new(),
        default_provider: "claude".into(),
    };
    let handle = spawn_with_state(state, &config).await.expect("boot");

    let mut stream = tokio_tungstenite::connect_async(&handle.endpoint)
        .await
        .expect("connect")
        .0;

    // Drive an RPC so registration definitely completes before we measure.
    let _ = rpc_call(&mut stream, "ping", json!({})).await;

    // After at least one RPC the connection is registered.
    let registered = inspector.connections.read().await;
    assert!(
        !registered.is_empty(),
        "connection should be registered after an RPC"
    );
    drop(registered);

    // Disconnect.
    let _ = stream.close(None).await;
    // Give the server a beat to run its unregister cleanup (the read loop
    // observes the close and calls `unregister`).
    tokio::time::sleep(Duration::from_millis(150)).await;

    let after = inspector.connections.read().await;
    assert!(
        after.is_empty(),
        "connection registry should be empty after disconnect (leaked: {})",
        after.len()
    );

    handle.serve_task.abort();
}

//! End-to-end test — real TCP WebSocket + JSON-RPC round-trip.
//!
//! Gating: `SYNICODE_WS_E2E=1`.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

fn e2e_enabled() -> bool {
    std::env::var("SYNICODE_WS_E2E").ok().as_deref() == Some("1")
}

async fn boot_server() -> (String, tokio::task::JoinHandle<()>) {
    let state = Arc::new(syncode_ws::WsState::new_in_memory(256));
    let app = syncode_ws::server::build_ws_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let url = format!("ws://127.0.0.1:{}/ws", port);

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (url, handle)
}

async fn connect(
    url: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let (stream, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("connect");
    stream
}

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
        .expect("send");

    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(msg)) = stream.next().await {
            if let Message::Text(text) = msg {
                let v: Value = serde_json::from_str(&text).expect("parse json");
                if v.get("id").is_some() {
                    return v;
                }
            }
        }
        panic!("stream closed without response");
    })
    .await
    .expect("timeout reading response")
}

#[tokio::test]
async fn ws_real_tcp_ping_pong() {
    if !e2e_enabled() {
        eprintln!("[skip] ws e2e: set SYNICODE_WS_E2E=1");
        return;
    }
    let (url, handle) = boot_server().await;
    let mut stream = connect(&url).await;
    let response = rpc_call(&mut stream, "ping", json!({})).await;
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["result"], "pong");
    assert!(
        response.get("error").is_none(),
        "unexpected error: {:?}",
        response["error"]
    );
    let _ = stream.close(None).await;
    handle.abort();
}

#[tokio::test]
async fn ws_real_tcp_project_create_and_list() {
    if !e2e_enabled() {
        eprintln!("[skip] ws e2e");
        return;
    }
    let (url, handle) = boot_server().await;
    let mut stream = connect(&url).await;

    let create_resp = rpc_call(
        &mut stream,
        "project/create",
        json!({
            "name": "e2e-project", "rootPath": "/tmp/e2e"
        }),
    )
    .await;
    assert!(
        create_resp.get("error").is_none(),
        "project/create failed: {:?}",
        create_resp["error"]
    );

    let list_resp = rpc_call(&mut stream, "project/list", json!({})).await;
    let projects = list_resp["result"]["projects"].as_array().unwrap();
    assert!(!projects.is_empty());

    let _ = stream.close(None).await;
    handle.abort();
}

#[tokio::test]
async fn ws_real_tcp_invalid_method_returns_error() {
    if !e2e_enabled() {
        eprintln!("[skip] ws e2e");
        return;
    }
    let (url, handle) = boot_server().await;
    let mut stream = connect(&url).await;
    let response = rpc_call(&mut stream, "nonexistent/method", json!({})).await;
    assert!(
        response.get("error").is_some(),
        "expected error for unknown method"
    );
    assert_eq!(response["error"]["code"], -32601);
    let _ = stream.close(None).await;
    handle.abort();
}

#[tokio::test]
async fn ws_real_tcp_push_subscribe() {
    if !e2e_enabled() {
        eprintln!("[skip] ws e2e");
        return;
    }
    let (url, handle) = boot_server().await;
    let mut stream = connect(&url).await;
    let sub_resp = rpc_call(
        &mut stream,
        "push/subscribe",
        json!({
            "channels": ["*"]
        }),
    )
    .await;
    assert!(
        sub_resp.get("error").is_none(),
        "push/subscribe failed: {:?}",
        sub_resp["error"]
    );
    let _ = stream.close(None).await;
    handle.abort();
}

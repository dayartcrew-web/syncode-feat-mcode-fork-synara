//! WebSocket server (axum)

use crate::rpc::handle_rpc;
use crate::WsState;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Build the WebSocket router
pub fn build_ws_router(state: Arc<WsState>) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state)
}

/// Build the full app router (WS + optional HTTP)
pub fn build_app(state: Arc<WsState>) -> Router {
    build_ws_router(state)
}

/// WebSocket upgrade handler
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<WsState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_connection(socket, state))
}

/// Handle a single WebSocket connection
async fn handle_connection(socket: WebSocket, state: Arc<WsState>) {
    let conn_id = state.next_id();
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Create a channel for sending messages to this connection
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let push_tx_clone = tx.clone();
    state.register(conn_id, tx).await;

    // Subscribe to push events
    let mut push_rx = state.push_tx.subscribe();

    // Spawn a task to forward push events to this connection
    let push_handle = tokio::spawn(async move {
        while let Ok((channel, data)) = push_rx.recv().await {
            let msg = serde_json::json!({
                "jsonrpc": "2.0",
                "method": format!("push/{}", channel),
                "params": data,
            });
            if let Ok(msg_str) = serde_json::to_string(&msg) {
                if push_tx_clone.send(msg_str).is_err() {
                    break;
                }
            }
        }
    });

    // Spawn a task to send queued messages via WebSocket
    let send_handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    // Receive messages from WebSocket and handle RPC
    while let Some(Ok(msg)) = ws_receiver.next().await {
        if let Message::Text(text) = msg {
            let response = handle_rpc(&state, &text).await;
            if let Some(resp_str) = response {
                if let Some(sender) = state.connections.read().await.get(&conn_id) {
                    let _ = sender.send(resp_str);
                }
            }
        }
    }

    // Cleanup
    state.unregister(conn_id).await;
    push_handle.abort();
    send_handle.abort();
}

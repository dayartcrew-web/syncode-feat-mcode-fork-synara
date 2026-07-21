//! WebSocket server (axum)

use crate::auth_rest::auth_rest_router;
use crate::rpc::handle_rpc;
use crate::{ConnectionId, WsState};
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Build the WebSocket router
pub fn build_ws_router(state: Arc<WsState>) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state)
}

/// Build the full app router (WS + HTTP REST routes from `syncode-http` +
/// stateful auth REST shims from `auth_rest`).
///
/// The stateless REST surface (`/health`, `/api/project-favicon`) lives in the
/// `syncode-http` leaf crate and is merged here so the standalone server
/// binary exposes both transports from one Axum app. The WS router carries
/// `Arc<WsState>` as its state; the HTTP router is stateless (`Router<()>`),
/// so `Router::merge` composes them without a state conflict.
///
/// v0.1.5: the 10 `/api/auth/*` REST routes live in [`crate::auth_rest`]
/// (they delegate to RPC handlers that need `&WsState`, so they can't live
/// in the leaf). Mounted here via the same `Router::merge` pattern.
pub fn build_app(state: Arc<WsState>) -> Router {
    build_ws_router(state.clone())
        .merge(auth_rest_router(state))
        .merge(syncode_http::http_router())
}

/// WebSocket upgrade handler
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<WsState>>) -> impl IntoResponse {
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

    // Emit the welcome push directly on this connection's mpsc — bypasses the
    // subscription opt-in (the welcome is unconditional on connect, matching
    // MCode's `onServerWelcome` semantics). The frontend's listener binds to
    // the `server.welcome` channel name and uses the payload's `homeDir` to
    // resolve the splash screen + populate `workspaceStore.homeDir`. Without
    // this push the welcome gap blocks "New chat" (homeDir stays null).
    //
    // Sent BEFORE the push-delivery task spawns so the welcome is the first
    // message buffered in the channel; the send task drains in FIFO order.
    if !crate::rpc::emit_welcome_on_connect(&push_tx_clone, &state) {
        tracing::warn!(
            conn_id,
            "welcome push not delivered (connection sender closed)"
        );
    }

    // Forward subscribed push-bus broadcasts to this connection (honors the
    // connection's channel subscriptions; see run_push_delivery).
    let push_handle = tokio::spawn(run_push_delivery(
        Arc::clone(&state),
        conn_id,
        push_tx_clone,
    ));

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
            let response = handle_rpc(&state, conn_id, &text).await;
            if let Some(resp_str) = response
                && let Some(sender) = state.connections.read().await.get(&conn_id)
            {
                let _ = sender.send(resp_str);
            }
        }
    }

    // Cleanup
    state.unregister(conn_id).await;
    push_handle.abort();
    send_handle.abort();
}

/// Forward push-bus broadcasts for `conn_id`'s subscribed channels to `tx`.
///
/// Subscribes to the push bus and, for each broadcast, forwards it to `tx` only
/// if the connection is currently subscribed to that channel. Subscriptions are
/// opt-in: a fresh connection receives nothing until it calls `push/subscribe`.
/// Runs until the push bus closes or `tx` is dropped (connection gone). A
/// standalone function (not a closure) so delivery/filtering is unit-testable
/// without a live WebSocket.
pub async fn run_push_delivery(
    state: Arc<WsState>,
    conn_id: ConnectionId,
    tx: mpsc::UnboundedSender<String>,
) {
    let mut rx = state.push_tx.subscribe();
    while let Ok((channel, data)) = rx.recv().await {
        // Opt-in: only forward channels this connection has subscribed to.
        let subscribed = state
            .subscriptions
            .read()
            .await
            .get_subscription(conn_id)
            .is_some_and(|sub| sub.is_subscribed(&channel));
        if !subscribed {
            continue;
        }
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": format!("push/{}", channel),
            "params": data,
        });
        if let Ok(msg_str) = serde_json::to_string(&msg)
            && tx.send(msg_str).is_err()
        {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn delivery_honors_subscriptions() {
        // Cleaner: register conn, keep rx, spawn delivery with a cloned tx.
        let state = Arc::new(WsState::new_in_memory(16));
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        state.register(1, tx.clone()).await;
        state
            .subscriptions
            .write()
            .await
            .subscribe(1, "orchestration");

        let _handle = tokio::spawn(run_push_delivery(Arc::clone(&state), 1, tx));

        // Let the delivery task subscribe to the push bus before we publish
        // (broadcast only reaches receivers that exist at send time).
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Subscribed channel → delivered.
        let _ = state.push_tx.send((
            "orchestration".to_string(),
            serde_json::json!({"eventType": "ProjectCreated"}),
        ));
        let received = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
        assert!(received.is_ok(), "subscribed channel should be delivered");
        let msg = received.unwrap().unwrap();
        assert!(msg.contains("push/orchestration"));
        assert!(msg.contains("ProjectCreated"));

        // Unsubscribed channel → filtered out (recv times out).
        let _ = state.push_tx.send((
            "git".to_string(),
            serde_json::json!({"eventType": "GitPushed"}),
        ));
        let filtered = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(
            filtered.is_err(),
            "unsubscribed channel must NOT be delivered"
        );
    }

    #[tokio::test]
    async fn delivery_isolates_connections() {
        // Conn 1 subscribes to orchestration; conn 2 subscribes to git. Each
        // receives only its own channel.
        let state = Arc::new(WsState::new_in_memory(16));

        let (tx1, mut rx1) = mpsc::unbounded_channel::<String>();
        state.register(1, tx1.clone()).await;
        state
            .subscriptions
            .write()
            .await
            .subscribe(1, "orchestration");
        let _h1 = tokio::spawn(run_push_delivery(Arc::clone(&state), 1, tx1));

        let (tx2, mut rx2) = mpsc::unbounded_channel::<String>();
        state.register(2, tx2.clone()).await;
        state.subscriptions.write().await.subscribe(2, "git");
        let _h2 = tokio::spawn(run_push_delivery(Arc::clone(&state), 2, tx2));

        // Let both delivery tasks subscribe before publishing.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let _ = state
            .push_tx
            .send(("orchestration".to_string(), serde_json::json!({})));
        let _ = state
            .push_tx
            .send(("git".to_string(), serde_json::json!({})));

        // Conn 1 gets orchestration, not git.
        assert!(
            tokio::time::timeout(Duration::from_millis(200), rx1.recv())
                .await
                .is_ok()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), rx1.recv())
                .await
                .is_err()
        );

        // Conn 2 gets git, not orchestration.
        assert!(
            tokio::time::timeout(Duration::from_millis(200), rx2.recv())
                .await
                .is_ok()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), rx2.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn domain_event_reaches_subscribed_connection_e2e() {
        // End-to-end "live consumer" proof: orchestrator command -> domain event
        // published via the WsDomainEventPublisher -> push_tx -> run_push_delivery
        // forwards it to a connection subscribed to the orchestration channel.
        // This closes the deferred robustness-hardening gap (a real consumer for
        // the publisher's push_tx feed) by asserting the whole loop is wired.
        let state = Arc::new(WsState::new_in_memory(16));

        // Register a connection and opt it into the orchestration channel.
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        state.register(1, tx.clone()).await;
        state
            .subscriptions
            .write()
            .await
            .subscribe(1, "orchestration");

        // Spawn the per-connection push delivery consumer.
        let _handle = tokio::spawn(run_push_delivery(Arc::clone(&state), 1, tx));

        // Let the delivery task subscribe to the push bus BEFORE the command
        // runs — broadcast only reaches receivers that exist at send time.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Issue a command -> the pipeline appends + projects a ProjectCreated
        // event, then best-effort publishes it to push_tx.
        let cmd = syncode_orchestration::Command::CreateProject {
            name: "PushDemo".into(),
            root_path: "/tmp/push-demo".into(),
        };
        state
            .orchestrator
            .handle_command(cmd)
            .await
            .expect("command should succeed");

        // The subscribed connection should receive a push/orchestration
        // notification carrying the just-published ProjectCreated event.
        let received = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
        assert!(
            received.is_ok(),
            "subscribed connection should receive the pushed domain event"
        );
        let msg = received.unwrap().unwrap();
        assert!(
            msg.contains("push/orchestration"),
            "method should be push/orchestration: {msg}"
        );
        assert!(
            msg.contains("ProjectCreated"),
            "should carry the ProjectCreated event type: {msg}"
        );
        assert!(
            msg.contains("PushDemo"),
            "should carry the serialized event payload (project name): {msg}"
        );
    }
}

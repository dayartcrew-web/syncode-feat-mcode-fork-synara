//! DSK-4 — Live provider event push end-to-end over real WS TCP.
//!
//! Closes the "live push wiring only verified at unit level" gap by exercising
//! the full path the desktop binary uses: in-memory `WsState` →
//! `ws_setup::spawn_with_state` → real WS handshake → JSON-RPC `subscribeShell`
//! → server-side publish on `push_tx` → `run_push_delivery` forwards the frame
//! to the subscribed WS client.
//!
//! The unit-level proof already lives in
//! `syncode-ws::server::tests::domain_event_reaches_subscribed_connection_e2e`
//! (in-process mpsc, no real socket). These tests add the WS-TCP boundary so a
//! regression in the Tauri shell's wiring (e.g. `spawn_with_state` mis-threading
//! `push_tx`, the WS send loop dropping broadcast frames) shows up here first —
//! before it can ship and surface as a "Working… stuck" symptom in the desktop
//! UI.
//!
//! # Coverage
//!
//! Three orthogonal cases:
//!
//! 1. **Command-driven live event** — `orchestration.dispatchCommand
//!    CreateProject` flows through the orchestrator's projector →
//!    `WsDomainEventPublisher` → `push_tx` → subscribed WS client. This is the
//!    same domain event the unit test asserts, but over a real socket.
//!
//! 2. **Provider-activity live event** — simulate the wire shape ingestion
//!    emits when a provider streams a `Reasoning` frame (PR #240/#241): publish
//!    an `ActivityLogged` event with `activity_type = "provider_reasoning"`
//!    through the same `WsDomainEventPublisher`. Asserts the WS client sees the
//!    `push/orchestration` frame carrying the reasoning payload — closing the
//!    loop from "backend emits new activity variant" to "frontend-observable
//!    wire frame".
//!
//! 3. **Subscription filter** — a second client that does NOT call
//!    `subscribeShell` must not receive `push/orchestration` frames. Guards
//!    against an accidental broadcast-to-all regression in `run_push_delivery`.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use syncode_core::ports::DomainEventPublisher;
use syncode_tauri::ws_setup::{WsConfig, spawn_with_state};
use syncode_ws::WsState;
use syncode_ws::push::WsDomainEventPublisher;
use tokio_tungstenite::tungstenite::Message;

/// Boot the server on an ephemeral port backed by an in-memory `WsState`.
/// Returns the WS endpoint URL, the serve task handle, AND a clone of the
/// `WsState` so the test can drive the orchestrator / publish directly on
/// `push_tx` (mirroring what ingestion does in production).
async fn boot_with_state_handle() -> (
    String,
    std::sync::Arc<tokio::task::JoinHandle<()>>,
    std::sync::Arc<WsState>,
) {
    // Clone-on-clone: both `Arc`s point at the same `WsState`, so the test's
    // copy observes every mutation the server makes (e.g. new connections).
    let state = std::sync::Arc::new(WsState::new_in_memory(256));
    let config = WsConfig {
        host: "127.0.0.1".into(),
        port: 0, // ephemeral → no port collision across tests
        db_path: String::new(),
        default_provider: "claude".into(),
    };
    let handle = spawn_with_state((*state).clone(), &config)
        .await
        .expect("WS server should boot on an ephemeral port");
    (handle.endpoint, handle.serve_task, state)
}

/// Send a JSON-RPC request and read back the matching response (matched by
/// `id`). Times out after 5s so a hung server fails the test instead of
/// hanging the suite. Mirrors the helper in `tests/boot_e2e.rs`.
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

/// Read the next non-response WS frame (i.e. a server-initiated push like
/// `push/orchestration`), skipping snapshot frames (`eventType == "snapshot"`).
/// Times out after 3s so a missing push fails the test fast instead of
/// hanging the suite.
///
/// Snapshot frames are skipped because `subscribeShell` emits an initial
/// `ShellSnapshot` between the request and response on the wire — `rpc_call`'s
/// response-matching loop consumes it during the call, but re-subscribes or
/// replays can emit additional snapshots that we want to look past when
/// asserting on live provider activity frames.
async fn next_push_skipping_snapshots(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Value {
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(Ok(msg)) = stream.next().await {
            if let Message::Text(text) = msg {
                let v: Value = serde_json::from_str(&text).expect("parse json");
                if v.get("id").is_some() || v.get("method").is_none() {
                    continue;
                }
                if v["params"]["eventType"] == "snapshot" {
                    continue;
                }
                return v;
            }
        }
        panic!("stream closed without a non-snapshot push frame");
    })
    .await
    .expect("timeout waiting for a live (non-snapshot) push frame")
}

/// **Command-driven live event push.** End-to-end over real WS: subscribe →
/// dispatch `CreateProject` → receive `push/orchestration` carrying
/// `ProjectCreated`. Proves the orchestrator → publisher → push_tx →
/// `run_push_delivery` → WS frame loop is wired inside `spawn_with_state`
/// (the exact path the Tauri binary uses).
#[tokio::test]
async fn command_event_reaches_subscribed_ws_client() {
    let (url, serve, state) = boot_with_state_handle().await;

    let mut stream = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect")
        .0;

    // Register on the orchestration push channel (the same call the frontend's
    // `__root.tsx::ensureScopedSubscriptions` makes on welcome).
    //
    // NOTE: the server emits the initial ShellSnapshot BETWEEN the request and
    // the response on the wire. `rpc_call`'s response-matching loop skips
    // any frame without an `id`, so the snapshot is silently consumed during
    // the call. By the time `rpc_call` returns, the subscription is fully
    // registered and any subsequent `push_tx.send()` is delivered to us.
    let sub = rpc_call(&mut stream, "orchestration.subscribeShell", json!({})).await;
    assert!(
        sub.get("error").is_none(),
        "subscribeShell failed: {:?}",
        sub["error"]
    );
    assert_eq!(sub["result"]["subscribed"], true);
    assert_eq!(sub["result"]["channel"], "orchestration");

    // Drive the orchestrator directly via the shared `WsState`. The
    // orchestrator's projector publishes ProjectCreated through
    // `WsDomainEventPublisher` → `push_tx` → delivery loop.
    state
        .orchestrator
        .handle_command(syncode_orchestration::Command::CreateProject {
            name: "PushE2E".into(),
            root_path: "/tmp/push-e2e".into(),
        })
        .await
        .expect("CreateProject command should succeed");

    // The subscribed WS client must receive the ProjectCreated push. Skip
    // any intervening snapshot frames (defensive — should be none here).
    let push = next_push_skipping_snapshots(&mut stream).await;
    assert_eq!(
        push["method"], "push/orchestration",
        "method should be push/orchestration: {push}"
    );
    assert!(
        push["params"]["eventType"] == "ProjectCreated",
        "push should carry ProjectCreated: {push}"
    );
    let payload_str = push.to_string();
    assert!(
        payload_str.contains("PushE2E"),
        "push should carry the project name: {payload_str}"
    );

    let _ = stream.close(None).await;
    serve.abort();
}

/// **Provider-activity live event push.** Simulates the wire shape ingestion
/// emits when a provider streams a `Reasoning` frame (PR #240/#241): publish an
/// `ActivityLogged` event with `activity_type = "provider_reasoning"` through
/// the same `WsDomainEventPublisher` the orchestrator uses. Asserts a
/// subscribed WS client receives the `push/orchestration` frame carrying the
/// reasoning payload — closing the loop from "backend emits new activity
/// variant" to "frontend-observable wire frame".
#[tokio::test]
async fn provider_reasoning_event_reaches_subscribed_ws_client() {
    let (url, serve, state) = boot_with_state_handle().await;

    let mut stream = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect")
        .0;

    let sub = rpc_call(&mut stream, "orchestration.subscribeShell", json!({})).await;
    assert!(
        sub.get("error").is_none(),
        "subscribeShell error: {:?}",
        sub["error"]
    );

    // Publish directly via the WS-domain event publisher — exactly what the
    // ingestion translator does when it maps a `ProviderEvent::Reasoning` to a
    // `DomainEvent::ActivityLogged`. The activity_type is the namespaced token
    // the frontend's `classifyActivityTone` (adaptPushEvent.ts) keys off.
    //
    // This MUST happen AFTER `subscribeShell` returns — broadcast frames are
    // delivered only to receivers whose subscription is registered at send
    // time. Calling publish concurrently with the subscribe RPC would race
    // the registration and silently drop the push.
    let publisher = WsDomainEventPublisher::new(state.push_tx.clone());
    publisher
        .publish(
            "orchestration",
            "ActivityLogged",
            "thread-provider-reasoning-e2e",
            json!({
                "activity_type": "provider_reasoning",
                "description": "thinking: design a tiny REST API",
                "thread_id": "thread-provider-reasoning-e2e",
            }),
        )
        .await
        .expect("publish should succeed");

    let push = next_push_skipping_snapshots(&mut stream).await;
    assert_eq!(push["method"], "push/orchestration");
    let payload_str = push.to_string();
    assert!(
        payload_str.contains("ActivityLogged"),
        "push should carry the ActivityLogged event type: {payload_str}"
    );
    assert!(
        payload_str.contains("provider_reasoning"),
        "push should carry the provider_reasoning activity_type: {payload_str}"
    );
    assert!(
        payload_str.contains("design a tiny REST API"),
        "push should carry the reasoning description: {payload_str}"
    );

    let _ = stream.close(None).await;
    serve.abort();
}

/// **Subscription filter.** A second client that does NOT call `subscribeShell`
/// must not receive `push/orchestration` frames — only registered subscribers
/// get live events. Guards against an accidental broadcast-to-all regression
/// in `run_push_delivery`.
#[tokio::test]
async fn unsubscribed_client_does_not_receive_push() {
    let (url, serve, state) = boot_with_state_handle().await;

    // Subscribed client — sanity check.
    let mut sub_stream = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect subscribed")
        .0;
    let sub = rpc_call(&mut sub_stream, "orchestration.subscribeShell", json!({})).await;
    assert!(sub.get("error").is_none());

    // Unsubscribed client — connects but never calls subscribeShell.
    let mut unsub_stream = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect unsubscribed")
        .0;

    // Drain the unsubscribed client's welcome frame so the subsequent
    // "must not receive" assertion is comparing against push frames only,
    // not the welcome that every connection gets on connect.
    let _welcome = tokio::time::timeout(Duration::from_millis(500), async {
        unsub_stream.next().await
    })
    .await
    .expect("unsubscribed client should at least receive a welcome frame on connect");

    // Publish a provider activity event on push_tx.
    let publisher = WsDomainEventPublisher::new(state.push_tx.clone());
    publisher
        .publish(
            "orchestration",
            "ActivityLogged",
            "thread-filter-test",
            json!({
                "activity_type": "provider_skill_dispatched",
                "description": "Skill dispatched: kmr-build",
            }),
        )
        .await
        .expect("publish should succeed");

    // Subscribed client receives it.
    let push = next_push_skipping_snapshots(&mut sub_stream).await;
    assert_eq!(push["method"], "push/orchestration");

    // Unsubscribed client should NOT receive any push within a short window.
    let filtered = tokio::time::timeout(Duration::from_millis(500), async {
        unsub_stream.next().await
    })
    .await;
    assert!(
        filtered.is_err(),
        "unsubscribed client must not receive push/orchestration frames, but got: {filtered:?}"
    );

    let _ = sub_stream.close(None).await;
    let _ = unsub_stream.close(None).await;
    serve.abort();
}

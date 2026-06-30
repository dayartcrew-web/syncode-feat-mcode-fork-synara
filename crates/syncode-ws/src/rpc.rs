//! JSON-RPC handler — orchestration methods
//!
//! All command-handling methods route through `WsState.orchestrator.handle_command()`,
//! which runs the full CQRS pipeline:
//!   Decider → Events → EventRepository persist → Projector → ReadModelStore

use crate::{ConnectionId, JsonRpcRequest, JsonRpcResponse, WsState};
use serde_json::Value;
use syncode_orchestration::Command;

/// Handle an incoming JSON-RPC message.
///
/// `conn_id` identifies the connection the request originated from, so
/// per-connection state (push subscriptions) can be mutated by the handler.
pub async fn handle_rpc(state: &WsState, conn_id: ConnectionId, raw: &str) -> Option<String> {
    // Parse the request
    let request: JsonRpcRequest = match serde_json::from_str(raw) {
        Ok(req) => req,
        Err(_) => {
            let resp = JsonRpcResponse::error(
                None,
                crate::error_codes::PARSE_ERROR,
                "Parse error",
            );
            return Some(serde_json::to_string(&resp).unwrap_or_default());
        }
    };

    tracing::debug!(method = %request.method, "RPC request");

    // Dispatch to method handler
    let response = dispatch_method(state, conn_id, &request).await;

    // Only respond if the request has an id (notifications don't get responses)
    if request.id.is_some() {
        Some(serde_json::to_string(&response).unwrap_or_default())
    } else {
        None
    }
}

/// Dispatch to the appropriate method handler
async fn dispatch_method(state: &WsState, conn_id: ConnectionId, request: &JsonRpcRequest) -> JsonRpcResponse {
    let id = request.id.clone().unwrap_or(Value::Null);

    match request.method.as_str() {
        // ─── System Methods ──────────────────────────────────────
        "ping" => JsonRpcResponse::success(id, Value::Object(serde_json::Map::new())),

        "rpc/listMethods" => JsonRpcResponse::success(
            id,
            serde_json::json!({
                "methods": [
                    "ping",
                    "rpc/listMethods",
                    "push/subscribe",
                    "push/unsubscribe",
                    "project/list",
                    "project/get",
                    "project/create",
                    "thread/list",
                    "thread/get",
                    "thread/create",
                    "thread/pause",
                    "thread/resume",
                    "thread/cancel",
                    "turn/list",
                    "turn/get",
                    "turn/start",
                    "turn/complete",
                ]
            }),
        ),

        // ─── Project Methods ──────────────────────────────────────
        "project/list" => {
            let store = state.read_store.read().await;
            let projects: Vec<Value> = store
                .projects
                .values()
                .filter_map(|p| serde_json::to_value(p).ok())
                .collect();
            JsonRpcResponse::success(id, serde_json::json!({ "projects": projects }))
        }

        "project/get" => handle_project_get(state, id, &request.params).await,

        "project/create" => handle_project_create(state, id, &request.params).await,

        // ─── Thread Methods ───────────────────────────────────────
        "thread/list" => {
            let store = state.read_store.read().await;
            let project_id = request.params.get("projectId")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let threads: Vec<Value> = store
                .threads
                .values()
                .filter(|t| project_id.is_empty() || t.project_id == project_id)
                .filter_map(|t| serde_json::to_value(t).ok())
                .collect();
            JsonRpcResponse::success(id, serde_json::json!({ "threads": threads }))
        }

        "thread/get" => handle_thread_get(state, id, &request.params).await,

        "thread/create" => handle_thread_create(state, id, &request.params).await,

        "thread/pause" => handle_thread_pause(state, id, &request.params).await,

        "thread/resume" => handle_thread_resume(state, id, &request.params).await,

        "thread/cancel" => handle_thread_cancel(state, id, &request.params).await,

        // ─── Turn Methods ────────────────────────────────────────
        "turn/list" => {
            let store = state.read_store.read().await;
            let thread_id = request.params.get("threadId")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let turns: Vec<Value> = store
                .turns
                .values()
                .filter(|t| thread_id.is_empty() || t.thread_id == thread_id)
                .filter_map(|t| serde_json::to_value(t).ok())
                .collect();
            JsonRpcResponse::success(id, serde_json::json!({ "turns": turns }))
        }

        "turn/get" => handle_turn_get(state, id, &request.params).await,

        "turn/start" => handle_turn_start(state, id, &request.params).await,

        "turn/complete" => handle_turn_complete(state, id, &request.params).await,

        // ─── Push Subscription Methods ───────────────────────────
        "push/subscribe" => handle_push_subscribe(state, conn_id, id, &request.params).await,

        "push/unsubscribe" => handle_push_unsubscribe(state, conn_id, id, &request.params).await,

        // ─── Unknown ────────────────────────────────────────────
        method => {
            tracing::warn!(method, "Unknown RPC method");
            JsonRpcResponse::error(
                Some(id),
                crate::error_codes::METHOD_NOT_FOUND,
                format!("Method not found: {}", method),
            )
        }
    }
}

// ─── Push Subscription Handlers ───────────────────────────────────

/// Record a channel subscription for the originating connection. The "*"
/// wildcard expands to all known channels. Subscriptions are opt-in: a
/// connection receives no pushes until it subscribes. Idempotent — `added`
/// reports whether this created a new subscription.
async fn handle_push_subscribe(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let channel = match params.get("channel").and_then(|v| v.as_str()) {
        Some(c) => c,
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'channel' parameter",
            )
        }
    };
    if !crate::channels::ChannelSubscription::is_valid(channel) {
        return JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            format!("Unknown channel: {}", channel),
        );
    }
    // Record against this connection. Returns false if the connection isn't
    // registered (shouldn't happen for a live socket) or was already subscribed.
    let added = state.subscriptions.write().await.subscribe(conn_id, channel);
    JsonRpcResponse::success(
        id,
        serde_json::json!({ "subscribed": true, "channel": channel, "added": added }),
    )
}

/// Remove a channel subscription for the originating connection. The "*"
/// wildcard clears all subscriptions.
async fn handle_push_unsubscribe(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let channel = params.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    let removed = state
        .subscriptions
        .write()
        .await
        .unsubscribe(conn_id, channel);
    JsonRpcResponse::success(
        id,
        serde_json::json!({ "unsubscribed": true, "channel": channel, "removed": removed }),
    )
}

// ─── Project Handlers ────────────────────────────────────────────

async fn handle_project_get(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let project_id = match params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'id' parameter"),
    };

    let store = state.read_store.read().await;
    match store.projects.get(&project_id) {
        Some(project) => {
            let val = serde_json::to_value(project).unwrap_or(Value::Null);
            JsonRpcResponse::success(id, val)
        }
        None => JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            format!("Project not found: {}", project_id),
        ),
    }
}

async fn handle_project_create(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let name = match params.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'name' parameter"),
    };
    let root_path = match params.get("rootPath").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'rootPath' parameter"),
    };

    let cmd = Command::CreateProject { name, root_path };
    match state.orchestrator.handle_command(cmd).await {
        Ok(result) => {
            // Read the updated entity from the read model
            if let Some(envelope) = result.events.first() {
                let project_id = envelope.event.aggregate_id();
                let store = state.read_store.read().await;
                let project = store.projects.get(&project_id.as_str())
                    .cloned()
                    .map(|p| serde_json::to_value(p).unwrap_or(Value::Null));
                JsonRpcResponse::success(id, project.unwrap_or(Value::Null))
            } else {
                JsonRpcResponse::error(Some(id), crate::error_codes::INTERNAL_ERROR, "No events produced")
            }
        }
        Err(e) => JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            e.to_string(),
        ),
    }
}

// ─── Thread Handlers ───────────────────────────────────────────────

async fn handle_thread_get(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let thread_id = match params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'id' parameter"),
    };

    let store = state.read_store.read().await;
    match store.threads.get(&thread_id) {
        Some(thread) => {
            let val = serde_json::to_value(thread).unwrap_or(Value::Null);
            JsonRpcResponse::success(id, val)
        }
        None => JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            format!("Thread not found: {}", thread_id),
        ),
    }
}

async fn handle_thread_create(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let project_id_str = match params.get("projectId").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'projectId'"),
    };
    let provider_id = match params.get("providerId").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'providerId'"),
    };
    let model = match params.get("model").and_then(|v| v.as_str()) {
        Some(m) => m.to_string(),
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'model'"),
    };

    let project_id = match syncode_core::EntityId::parse(project_id_str) {
        Ok(id) => id,
        Err(_) => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Invalid projectId format"),
    };

    let cmd = Command::CreateThread { project_id, provider_id, model };
    match state.orchestrator.handle_command(cmd).await {
        Ok(result) => {
            if let Some(envelope) = result.events.first() {
                let thread_id = envelope.event.aggregate_id();
                let store = state.read_store.read().await;
                let thread = store.threads.get(&thread_id.as_str())
                    .cloned()
                    .map(|t| serde_json::to_value(t).unwrap_or(Value::Null));
                JsonRpcResponse::success(id, thread.unwrap_or(Value::Null))
            } else {
                JsonRpcResponse::error(Some(id), crate::error_codes::INTERNAL_ERROR, "No events produced")
            }
        }
        Err(e) => JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, e.to_string()),
    }
}

async fn handle_thread_pause(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    apply_thread_command(state, id, params, |tid| Command::PauseThread { id: tid }).await
}

async fn handle_thread_resume(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    apply_thread_command(state, id, params, |tid| Command::ResumeThread { id: tid }).await
}

async fn handle_thread_cancel(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    apply_thread_command(state, id, params, |tid| Command::CancelThread { id: tid }).await
}

async fn apply_thread_command(
    state: &WsState,
    id: Value,
    params: &Value,
    cmd_fn: impl FnOnce(syncode_core::EntityId) -> Command,
) -> JsonRpcResponse {
    let thread_id_str = match params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'id' parameter"),
    };
    let thread_id = match syncode_core::EntityId::parse(thread_id_str) {
        Ok(id) => id,
        Err(_) => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Invalid id format"),
    };

    let cmd = cmd_fn(thread_id);
    match state.orchestrator.handle_command(cmd).await {
        Ok(_result) => {
            // The orchestrator already projected to read model, read the updated thread
            let store = state.read_store.read().await;
            let thread = store.threads.get(thread_id_str)
                .cloned()
                .map(|t| serde_json::to_value(t).unwrap_or(Value::Null));
            JsonRpcResponse::success(id, thread.unwrap_or(Value::Null))
        }
        Err(e) => JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, e.to_string()),
    }
}

// ─── Turn Handlers ────────────────────────────────────────────────

async fn handle_turn_get(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let turn_id = match params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'id' parameter"),
    };

    let store = state.read_store.read().await;
    match store.turns.get(&turn_id) {
        Some(turn) => {
            let val = serde_json::to_value(turn).unwrap_or(Value::Null);
            JsonRpcResponse::success(id, val)
        }
        None => JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            format!("Turn not found: {}", turn_id),
        ),
    }
}

async fn handle_turn_start(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let thread_id_str = match params.get("threadId").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'threadId'"),
    };
    let sequence = params.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    let user_input = match params.get("userInput").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'userInput'"),
    };

    let thread_id = match syncode_core::EntityId::parse(thread_id_str) {
        Ok(id) => id,
        Err(_) => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Invalid threadId format"),
    };

    let cmd = Command::StartTurn { thread_id, sequence, user_input };
    match state.orchestrator.handle_command(cmd).await {
        Ok(result) => {
            if let Some(envelope) = result.events.first() {
                let turn_id = envelope.event.aggregate_id();
                let store = state.read_store.read().await;
                let turn = store.turns.get(&turn_id.as_str())
                    .cloned()
                    .map(|t| serde_json::to_value(t).unwrap_or(Value::Null));
                JsonRpcResponse::success(id, turn.unwrap_or(Value::Null))
            } else {
                JsonRpcResponse::error(Some(id), crate::error_codes::INTERNAL_ERROR, "No events produced")
            }
        }
        Err(e) => JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, e.to_string()),
    }
}

async fn handle_turn_complete(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let turn_id_str = match params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'id'"),
    };
    let assistant_output = match params.get("assistantOutput").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Missing 'assistantOutput'"),
    };
    let duration_ms = params.get("durationMs").and_then(|v| v.as_u64()).unwrap_or(0);

    let turn_id = match syncode_core::EntityId::parse(turn_id_str) {
        Ok(id) => id,
        Err(_) => return JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, "Invalid id format"),
    };

    let cmd = Command::CompleteTurn { id: turn_id, assistant_output, duration_ms };
    match state.orchestrator.handle_command(cmd).await {
        Ok(_result) => {
            let store = state.read_store.read().await;
            let turn = store.turns.get(turn_id_str)
                .cloned()
                .map(|t| serde_json::to_value(t).unwrap_or(Value::Null));
            JsonRpcResponse::success(id, turn.unwrap_or(Value::Null))
        }
        Err(e) => JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handle_ping() {
        let state = WsState::new_in_memory(16);
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "ping"
        });

        let response = handle_rpc(&state, 1, &request.to_string()).await;
        assert!(response.is_some());
        let resp: JsonRpcResponse = serde_json::from_str(&response.unwrap()).unwrap();
        assert!(resp.error.is_none());
        assert_eq!(resp.id, Some(serde_json::json!(1)));
    }

    #[tokio::test]
    async fn test_handle_unknown_method() {
        let state = WsState::new_in_memory(16);
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "nonexistent/method"
        });

        let response = handle_rpc(&state, 1, &request.to_string()).await;
        assert!(response.is_some());
        let resp: JsonRpcResponse = serde_json::from_str(&response.unwrap()).unwrap();
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn test_notification_no_response() {
        let state = WsState::new_in_memory(16);
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "ping"
        });

        let response = handle_rpc(&state, 1, &request.to_string()).await;
        assert!(response.is_none());
    }

    #[tokio::test]
    async fn test_project_create_and_list() {
        let state = WsState::new_in_memory(16);

        // Create project
        let create_req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "project/create",
            "params": { "name": "Test Project", "rootPath": "/tmp/test" }
        });
        let response = handle_rpc(&state, 1, &create_req.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&response).unwrap();
        assert!(resp.error.is_none(), "Create failed: {:?}", resp.error);
        let project = resp.result.unwrap();
        let project_id = project["id"].as_str().unwrap().to_string();

        // List projects
        let list_req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "project/list"
        });
        let response = handle_rpc(&state, 1, &list_req.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&response).unwrap();
        let result = resp.result.unwrap();
        let projects = result["projects"].as_array().unwrap();
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0]["name"], "Test Project");
        assert_eq!(projects[0]["id"], project_id);
    }

    #[tokio::test]
    async fn test_project_create_validation() {
        let state = WsState::new_in_memory(16);

        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "project/create",
            "params": { "name": "   ", "rootPath": "/tmp" }
        });
        let response = handle_rpc(&state, 1, &req.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&response).unwrap();
        assert!(resp.error.is_some());
        assert!(resp.error.unwrap().message.contains("empty"));
    }

    #[tokio::test]
    async fn test_push_subscribe_records_subscription() {
        let state = WsState::new_in_memory(16);
        // Register connection 1 (subscribe requires a registered conn_id).
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "push/subscribe",
            "params": { "channel": "orchestration" }
        });
        let resp = handle_rpc(&state, 1, &req.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&resp).unwrap();
        assert!(resp.error.is_none(), "{:?}", resp.error);
        assert_eq!(resp.result.unwrap()["subscribed"], true);

        // The registry now records conn 1 subscribed to orchestration.
        let subs = state.subscriptions.read().await;
        assert!(subs.get_subscription(1).unwrap().is_subscribed("orchestration"));
        assert!(!subs.get_subscription(1).unwrap().is_subscribed("git"));
    }

    #[tokio::test]
    async fn test_push_subscribe_rejects_unknown_channel() {
        let state = WsState::new_in_memory(16);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "push/subscribe",
            "params": { "channel": "bogus" }
        });
        let resp = handle_rpc(&state, 1, &req.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&resp).unwrap();
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_push_unsubscribe_removes_subscription() {
        let state = WsState::new_in_memory(16);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;

        // Subscribe then unsubscribe orchestration.
        let sub = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "push/subscribe",
            "params": { "channel": "orchestration" }
        });
        let _ = handle_rpc(&state, 1, &sub.to_string()).await;
        let unsub = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "push/unsubscribe",
            "params": { "channel": "orchestration" }
        });
        let resp = handle_rpc(&state, 1, &unsub.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&resp).unwrap();
        assert_eq!(resp.result.unwrap()["removed"], true);

        let subs = state.subscriptions.read().await;
        assert!(!subs.get_subscription(1).unwrap().is_subscribed("orchestration"));
    }
}

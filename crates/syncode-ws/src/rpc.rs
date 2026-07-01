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
/// per-connection state (push subscriptions, authenticated principal) can be
/// consulted and mutated by the handler.
///
/// **Authorization:** before dispatch, the request is checked against the
/// server's [`WsAuthConfig`](syncode_auth::WsAuthConfig). In non-requiring
/// modes (the default) every method is allowed. In requiring mode,
/// protected methods (those with a [`Permission`](syncode_auth::policy::Permission))
/// are rejected with `UNAUTHORIZED` (-32001) until the connection calls
/// `auth/bootstrap`, and `FORBIDDEN` (-32003) if the principal's role lacks
/// the required permission. Bootstrap/system methods are always callable.
pub async fn handle_rpc(state: &WsState, conn_id: ConnectionId, raw: &str) -> Option<String> {
    // Parse the request
    let request: JsonRpcRequest = match serde_json::from_str(raw) {
        Ok(req) => req,
        Err(_) => {
            let resp = JsonRpcResponse::error(None, crate::error_codes::PARSE_ERROR, "Parse error");
            return Some(serde_json::to_string(&resp).unwrap_or_default());
        }
    };

    tracing::debug!(method = %request.method, "RPC request");

    // Authorization gate — runs before dispatch. Public methods (ping,
    // auth/*, rpc/listMethods) bypass; protected methods require an
    // authenticated principal with sufficient permission in requiring mode.
    match state
        .conn_auth
        .authorize(&state.auth_config, conn_id, &request.method)
        .await
    {
        crate::auth::AuthzOutcome::Allow => { /* proceed */ }
        blocked => {
            let id = request.id.clone().unwrap_or(Value::Null);
            let resp = crate::auth::authz_error_response(id, &blocked);
            return respond(request.id, resp);
        }
    }

    // Dispatch to method handler
    let response = dispatch_method(state, conn_id, &request).await;

    // Only respond if the request has an id (notifications don't get responses)
    respond(request.id, response)
}

/// Serialize a response only if the request carried an id (notifications don't
/// get responses).
fn respond(id: Option<Value>, response: JsonRpcResponse) -> Option<String> {
    if id.is_some() {
        Some(serde_json::to_string(&response).unwrap_or_default())
    } else {
        None
    }
}

/// Dispatch to the appropriate method handler
async fn dispatch_method(
    state: &WsState,
    conn_id: ConnectionId,
    request: &JsonRpcRequest,
) -> JsonRpcResponse {
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
                    "auth/bootstrap",
                    "auth/status",
                    "auth/logout",
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
            let project_id = request
                .params
                .get("projectId")
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
            let thread_id = request
                .params
                .get("threadId")
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

        // ─── Auth Methods (always callable — they're the bootstrap path) ──
        "auth/bootstrap" => handle_auth_bootstrap(state, conn_id, id, &request.params).await,
        "auth/status" => handle_auth_status(state, conn_id, id).await,
        "auth/logout" => handle_auth_logout(state, conn_id, id).await,

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

/// Record a channel subscription for the originating connection, then emit a
/// snapshot of the channel's current state (snapshot-then-stream).
///
/// The "*"
/// wildcard expands to all known channels. Subscriptions are opt-in: a
/// connection receives no pushes until it subscribes. Idempotent — `added`
/// reports whether this created a new subscription.
///
/// **Snapshot:** after the subscription is recorded, the server builds a
/// snapshot of the channel's current read-model state and sends it to this
/// connection as a `push/<channel>` notification with `event_type: "snapshot"`.
/// The subscribe-then-snapshot ordering is race-free: any event projected
/// after the snapshot read is delivered live (the subscription was already in
/// place). For the `orchestration` channel, an optional `threadId` param
/// selects a thread-detail snapshot (one thread + turns + messages) instead
/// of the default shell snapshot (all projects + threads).
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
            );
        }
    };
    if !crate::channels::ChannelSubscription::is_valid(channel) {
        return JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            format!("Unknown channel: {}", channel),
        );
    }
    // Optional threadId for the orchestration channel (thread-detail snapshot).
    let thread_id = params.get("threadId").and_then(|v| v.as_str());

    // Record against this connection. Returns false if the connection isn't
    // registered (shouldn't happen for a live socket) or was already subscribed.
    let added = state
        .subscriptions
        .write()
        .await
        .subscribe(conn_id, channel);

    // Snapshot-then-stream: emit current state BEFORE returning, so the
    // client has an immediate basis to apply live deltas against. Ordering
    // is safe because the subscription is already recorded above.
    let snapshot_emitted = crate::push::emit_snapshot(state, conn_id, channel, thread_id).await;

    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "channel": channel,
            "added": added,
            "snapshotEmitted": snapshot_emitted,
        }),
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

// ─── Auth Handlers ───────────────────────────────────────────────

/// Exchange a credential for a session, binding the resulting principal to
/// the originating connection. In no-auth mode this is a no-op success (the
/// connection is already trusted); in requiring mode it validates the
/// credential via the configured [`Authenticator`].
///
/// Returns the session token, role, subject, and expiry. The token is
/// opaque and should be treated as a bearer secret by the client.
async fn handle_auth_bootstrap(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let credential = match params.get("credential").and_then(|v| v.as_str()) {
        Some(c) => c,
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'credential' parameter",
            );
        }
    };

    // Non-requiring mode: no credential check. Acknowledge as authenticated
    // (owner) so clients that always bootstrap work uniformly.
    if !state.auth_config.requires_authentication() {
        return JsonRpcResponse::success(
            id,
            serde_json::json!({
                "authenticated": true,
                "mode": state.auth_config.mode,
                "note": "server does not require authentication",
            }),
        );
    }

    match crate::auth::bootstrap(&state.auth_config, &state.conn_auth, conn_id, credential).await {
        Ok(result) => {
            let principal = result.principal;
            JsonRpcResponse::success(
                id,
                serde_json::json!({
                    "authenticated": true,
                    "sessionToken": result.token.as_str(),
                    "role": principal.role,
                    "subject": principal.subject,
                    "expiresAt": principal.expires_at,
                }),
            )
        }
        Err((code, msg)) => JsonRpcResponse::error(Some(id), code, msg),
    }
}

/// Report the connection's current authentication state.
async fn handle_auth_status(state: &WsState, conn_id: ConnectionId, id: Value) -> JsonRpcResponse {
    let requires = state.auth_config.requires_authentication();
    let principal = state.conn_auth.get(conn_id).await;

    let result = if let Some(p) = principal {
        serde_json::json!({
            "authenticated": true,
            "requiresAuthentication": requires,
            "role": p.role,
            "subject": p.subject,
            "expiresAt": p.expires_at,
        })
    } else {
        serde_json::json!({
            "authenticated": !requires, // open if no auth required
            "requiresAuthentication": requires,
            "role": null,
            "subject": null,
        })
    };
    JsonRpcResponse::success(id, result)
}

/// Clear the connection's bound principal. Idempotent. Subsequent protected
/// calls in requiring mode will return `UNAUTHORIZED`.
async fn handle_auth_logout(state: &WsState, conn_id: ConnectionId, id: Value) -> JsonRpcResponse {
    let cleared = state.conn_auth.clear(conn_id).await;
    JsonRpcResponse::success(
        id,
        serde_json::json!({ "loggedOut": true, "hadSession": cleared }),
    )
}

// ─── Project Handlers ────────────────────────────────────────────

async fn handle_project_get(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let project_id = match params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'id' parameter",
            );
        }
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

async fn handle_project_create(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let name = match params.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'name' parameter",
            );
        }
    };
    let root_path = match params.get("rootPath").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'rootPath' parameter",
            );
        }
    };

    let cmd = Command::CreateProject { name, root_path };
    match state.orchestrator.handle_command(cmd).await {
        Ok(result) => {
            // Read the updated entity from the read model
            if let Some(envelope) = result.events.first() {
                let project_id = envelope.event.aggregate_id();
                let store = state.read_store.read().await;
                let project = store
                    .projects
                    .get(&project_id.as_str())
                    .cloned()
                    .map(|p| serde_json::to_value(p).unwrap_or(Value::Null));
                JsonRpcResponse::success(id, project.unwrap_or(Value::Null))
            } else {
                JsonRpcResponse::error(
                    Some(id),
                    crate::error_codes::INTERNAL_ERROR,
                    "No events produced",
                )
            }
        }
        Err(e) => {
            JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, e.to_string())
        }
    }
}

// ─── Thread Handlers ───────────────────────────────────────────────

async fn handle_thread_get(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let thread_id = match params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'id' parameter",
            );
        }
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

async fn handle_thread_create(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let project_id_str = match params.get("projectId").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'projectId'",
            );
        }
    };
    let provider_id = match params.get("providerId").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'providerId'",
            );
        }
    };
    let model = match params.get("model").and_then(|v| v.as_str()) {
        Some(m) => m.to_string(),
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'model'",
            );
        }
    };

    let project_id = match syncode_core::EntityId::parse(project_id_str) {
        Ok(id) => id,
        Err(_) => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Invalid projectId format",
            );
        }
    };

    let cmd = Command::CreateThread {
        project_id,
        provider_id,
        model,
    };
    match state.orchestrator.handle_command(cmd).await {
        Ok(result) => {
            if let Some(envelope) = result.events.first() {
                let thread_id = envelope.event.aggregate_id();
                let store = state.read_store.read().await;
                let thread = store
                    .threads
                    .get(&thread_id.as_str())
                    .cloned()
                    .map(|t| serde_json::to_value(t).unwrap_or(Value::Null));
                JsonRpcResponse::success(id, thread.unwrap_or(Value::Null))
            } else {
                JsonRpcResponse::error(
                    Some(id),
                    crate::error_codes::INTERNAL_ERROR,
                    "No events produced",
                )
            }
        }
        Err(e) => {
            JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, e.to_string())
        }
    }
}

async fn handle_thread_pause(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    apply_thread_command(state, id, params, |tid| Command::PauseThread { id: tid }).await
}

async fn handle_thread_resume(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    apply_thread_command(state, id, params, |tid| Command::ResumeThread { id: tid }).await
}

async fn handle_thread_cancel(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
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
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'id' parameter",
            );
        }
    };
    let thread_id = match syncode_core::EntityId::parse(thread_id_str) {
        Ok(id) => id,
        Err(_) => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Invalid id format",
            );
        }
    };

    let cmd = cmd_fn(thread_id);
    match state.orchestrator.handle_command(cmd).await {
        Ok(_result) => {
            // The orchestrator already projected to read model, read the updated thread
            let store = state.read_store.read().await;
            let thread = store
                .threads
                .get(thread_id_str)
                .cloned()
                .map(|t| serde_json::to_value(t).unwrap_or(Value::Null));
            JsonRpcResponse::success(id, thread.unwrap_or(Value::Null))
        }
        Err(e) => {
            JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, e.to_string())
        }
    }
}

// ─── Turn Handlers ────────────────────────────────────────────────

async fn handle_turn_get(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let turn_id = match params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'id' parameter",
            );
        }
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

async fn handle_turn_start(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let thread_id_str = match params.get("threadId").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'threadId'",
            );
        }
    };
    let sequence = params.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    let user_input = match params.get("userInput").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'userInput'",
            );
        }
    };

    let thread_id = match syncode_core::EntityId::parse(thread_id_str) {
        Ok(id) => id,
        Err(_) => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Invalid threadId format",
            );
        }
    };

    let cmd = Command::StartTurn {
        thread_id,
        sequence,
        user_input,
    };
    match state.orchestrator.handle_command(cmd).await {
        Ok(result) => {
            if let Some(envelope) = result.events.first() {
                let turn_id = envelope.event.aggregate_id();
                let store = state.read_store.read().await;
                let turn = store
                    .turns
                    .get(&turn_id.as_str())
                    .cloned()
                    .map(|t| serde_json::to_value(t).unwrap_or(Value::Null));
                JsonRpcResponse::success(id, turn.unwrap_or(Value::Null))
            } else {
                JsonRpcResponse::error(
                    Some(id),
                    crate::error_codes::INTERNAL_ERROR,
                    "No events produced",
                )
            }
        }
        Err(e) => {
            JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, e.to_string())
        }
    }
}

async fn handle_turn_complete(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let turn_id_str = match params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'id'",
            );
        }
    };
    let assistant_output = match params.get("assistantOutput").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'assistantOutput'",
            );
        }
    };
    let duration_ms = params
        .get("durationMs")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let turn_id = match syncode_core::EntityId::parse(turn_id_str) {
        Ok(id) => id,
        Err(_) => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Invalid id format",
            );
        }
    };

    let cmd = Command::CompleteTurn {
        id: turn_id,
        assistant_output,
        duration_ms,
    };
    match state.orchestrator.handle_command(cmd).await {
        Ok(_result) => {
            let store = state.read_store.read().await;
            let turn = store
                .turns
                .get(turn_id_str)
                .cloned()
                .map(|t| serde_json::to_value(t).unwrap_or(Value::Null));
            JsonRpcResponse::success(id, turn.unwrap_or(Value::Null))
        }
        Err(e) => {
            JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
        assert_eq!(
            resp.error.unwrap().code,
            crate::error_codes::METHOD_NOT_FOUND
        );
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
        let response = handle_rpc(&state, 1, &create_req.to_string())
            .await
            .unwrap();
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
        assert!(
            subs.get_subscription(1)
                .unwrap()
                .is_subscribed("orchestration")
        );
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
        assert!(
            !subs
                .get_subscription(1)
                .unwrap()
                .is_subscribed("orchestration")
        );
    }

    // ─── Auth integration tests ──────────────────────────────────

    /// Helper: build a remote-requiring WsState with a known owner secret.
    fn make_remote_state() -> WsState {
        use std::sync::Mutex;
        use syncode_auth::OWNER_TOKEN_KEY;
        use syncode_auth::authenticator::SharedSecretAuthenticator;
        use syncode_auth::secret_store::{InMemorySecretStore, SecretStore};

        let mut store = InMemorySecretStore::new();
        store.store(OWNER_TOKEN_KEY, "sk-owner-secret");
        let store: Arc<Mutex<dyn SecretStore>> = Arc::new(Mutex::new(store));
        let sessions = Arc::new(syncode_auth::session::SessionRegistry::new());
        let auth = SharedSecretAuthenticator::new(store, sessions);
        let orchestrator = syncode_orchestration::Orchestrator::new(in_memory_repo());
        WsState::new_with_auth(
            16,
            orchestrator,
            syncode_auth::WsAuthConfig::remote(Arc::new(auth)),
        )
    }

    /// Minimal in-memory EventRepository for tests that need a real Orchestrator.
    fn in_memory_repo() -> Arc<dyn syncode_core::ports::EventRepository> {
        Arc::new(InlineInMemoryRepo::new())
    }

    /// Send an RPC request and parse the response.
    async fn rpc(state: &WsState, conn: ConnectionId, req: &serde_json::Value) -> JsonRpcResponse {
        let raw = handle_rpc(state, conn, &req.to_string()).await;
        serde_json::from_str(&raw.unwrap_or_default()).unwrap()
    }

    #[tokio::test]
    async fn no_auth_mode_project_create_unaffected() {
        // Default (no-auth) state: create works with no bootstrap. Backward compat.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "project/create",
            "params": { "name": "X", "rootPath": "/tmp/x" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(
            resp.error.is_none(),
            "no-auth create should succeed: {:?}",
            resp.error
        );
    }

    #[tokio::test]
    async fn remote_unauth_write_is_unauthorized() {
        let state = make_remote_state();
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "project/create",
            "params": { "name": "X", "rootPath": "/tmp/x" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some());
        assert_eq!(
            resp.error.unwrap().code,
            crate::auth::auth_error_codes::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn remote_public_methods_callable_pre_auth() {
        let state = make_remote_state();
        // ping + auth/status must work before bootstrap.
        for method in ["ping", "auth/status", "rpc/listMethods"] {
            let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": method });
            let resp = rpc(&state, 1, &req).await;
            assert!(
                resp.error.is_none(),
                "{} should be public: {:?}",
                method,
                resp.error
            );
        }
    }

    #[tokio::test]
    async fn bootstrap_then_write_succeeds() {
        let state = make_remote_state();

        // Bootstrap with the correct credential.
        let boot = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "auth/bootstrap",
            "params": { "credential": "sk-owner-secret" }
        });
        let resp = rpc(&state, 1, &boot).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        assert_eq!(resp.result.unwrap()["authenticated"], true);

        // Now a write method works (owner role).
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "project/create",
            "params": { "name": "Post-Auth", "rootPath": "/tmp/p" }
        });
        let resp = rpc(&state, 1, &create).await;
        assert!(
            resp.error.is_none(),
            "post-bootstrap create failed: {:?}",
            resp.error
        );
    }

    #[tokio::test]
    async fn bootstrap_wrong_credential_rejected() {
        let state = make_remote_state();
        let boot = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "auth/bootstrap",
            "params": { "credential": "wrong" }
        });
        let resp = rpc(&state, 1, &boot).await;
        assert!(resp.error.is_some());
        assert_eq!(
            resp.error.unwrap().code,
            crate::auth::auth_error_codes::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn auth_status_reports_state() {
        let state = make_remote_state();
        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "auth/status" });

        // Pre-auth: requiresAuthentication true, authenticated false.
        let resp = rpc(&state, 1, &req).await;
        let result = resp.result.unwrap();
        assert_eq!(result["requiresAuthentication"], true);
        assert_eq!(result["authenticated"], false);

        // Bootstrap then re-check.
        let _ = rpc(
            &state,
            1,
            &serde_json::json!({
                "jsonrpc": "2.0", "id": 2, "method": "auth/bootstrap",
                "params": { "credential": "sk-owner-secret" }
            }),
        )
        .await;
        let resp = rpc(&state, 1, &req).await;
        let result = resp.result.unwrap();
        assert_eq!(result["authenticated"], true);
        assert_eq!(result["role"], "owner");
    }

    #[tokio::test]
    async fn logout_clears_session() {
        let state = make_remote_state();
        // Bootstrap.
        rpc(
            &state,
            1,
            &serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "auth/bootstrap",
                "params": { "credential": "sk-owner-secret" }
            }),
        )
        .await;

        // Write works while authenticated.
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "project/create",
            "params": { "name": "Before", "rootPath": "/tmp/b" }
        });
        let resp = rpc(&state, 1, &create).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);

        // Logout.
        let out = rpc(
            &state,
            1,
            &serde_json::json!({ "jsonrpc": "2.0", "id": 3, "method": "auth/logout" }),
        )
        .await;
        assert_eq!(out.result.unwrap()["hadSession"], true);

        // Write now unauthorized again.
        let resp = rpc(&state, 1, &create).await;
        assert_eq!(
            resp.error.unwrap().code,
            crate::auth::auth_error_codes::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn bootstrap_no_auth_mode_is_noop_success() {
        // In no-auth mode, bootstrap returns authenticated:true without checking.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "auth/bootstrap",
            "params": { "credential": "literally-anything" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap()["authenticated"], true);
    }

    // ── Test-only in-memory EventRepository ────────────────────────────
    // (self-contained so rpc auth tests don't depend on the WsState internals)

    use std::collections::HashMap as StdHashMap;
    use std::sync::Mutex as StdMutex;

    struct InlineInMemoryRepo {
        events: StdMutex<StdHashMap<String, Vec<syncode_core::Envelope>>>,
        snapshots: StdMutex<StdHashMap<String, (serde_json::Value, u64)>>,
    }

    impl InlineInMemoryRepo {
        fn new() -> Self {
            Self {
                events: StdMutex::new(StdHashMap::new()),
                snapshots: StdMutex::new(StdHashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl syncode_core::ports::EventRepository for InlineInMemoryRepo {
        async fn append_events(
            &self,
            aggregate_id: syncode_core::EntityId,
            events: Vec<syncode_core::DomainEvent>,
            expected_version: u64,
        ) -> Result<u64, syncode_core::PortError> {
            let mut store = self.events.lock().unwrap();
            let key = aggregate_id.to_string();
            let current = store.get(&key).map(|v| v.len() as u64).unwrap_or(0);
            if current != expected_version {
                return Err(syncode_core::PortError::ConcurrencyConflict {
                    expected: expected_version,
                    actual: current,
                });
            }
            let entry = store.entry(key).or_default();
            for (i, event) in events.into_iter().enumerate() {
                entry.push(syncode_core::Envelope::new(event, current + 1 + i as u64));
            }
            Ok(entry.len() as u64)
        }
        async fn replay_events(
            &self,
            aggregate_id: syncode_core::EntityId,
        ) -> Result<Vec<syncode_core::Envelope>, syncode_core::PortError> {
            let store = self.events.lock().unwrap();
            Ok(store
                .get(&aggregate_id.to_string())
                .cloned()
                .unwrap_or_default())
        }
        async fn load_snapshot(
            &self,
            aggregate_id: syncode_core::EntityId,
        ) -> Result<Option<(serde_json::Value, u64)>, syncode_core::PortError> {
            Ok(self
                .snapshots
                .lock()
                .unwrap()
                .get(&aggregate_id.to_string())
                .cloned())
        }
        async fn save_snapshot(
            &self,
            aggregate_id: syncode_core::EntityId,
            state: serde_json::Value,
            version: u64,
        ) -> Result<(), syncode_core::PortError> {
            self.snapshots
                .lock()
                .unwrap()
                .insert(aggregate_id.to_string(), (state, version));
            Ok(())
        }
        async fn load_all_snapshots(
            &self,
        ) -> Result<Vec<(syncode_core::EntityId, serde_json::Value, u64)>, syncode_core::PortError>
        {
            let snapshots = self.snapshots.lock().unwrap();
            let mut out = Vec::with_capacity(snapshots.len());
            for (id_str, (state, version)) in snapshots.iter() {
                let id = syncode_core::EntityId::parse(id_str).map_err(|e| {
                    syncode_core::PortError::Internal(format!("invalid aggregate_id: {e}"))
                })?;
                out.push((id, state.clone(), *version));
            }
            Ok(out)
        }
        async fn replay_all_events(
            &self,
            _: Option<u64>,
            _: u32,
        ) -> Result<Vec<syncode_core::Envelope>, syncode_core::PortError> {
            let store = self.events.lock().unwrap();
            let mut all: Vec<syncode_core::Envelope> = store.values().flatten().cloned().collect();
            all.sort_by_key(|e| e.sequence);
            Ok(all)
        }
        async fn current_version(
            &self,
            aggregate_id: syncode_core::EntityId,
        ) -> Result<u64, syncode_core::PortError> {
            let store = self.events.lock().unwrap();
            Ok(store
                .get(&aggregate_id.to_string())
                .map(|v| v.len() as u64)
                .unwrap_or(0))
        }
    }
}

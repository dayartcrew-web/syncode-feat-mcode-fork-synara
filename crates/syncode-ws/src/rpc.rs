//! JSON-RPC handler — orchestration methods
//!
//! All command-handling methods route through `WsState.orchestrator.handle_command()`,
//! which runs the full CQRS pipeline:
//!   Decider → Events → EventRepository persist → Projector → ReadModelStore

use crate::{ConnectionId, JsonRpcRequest, JsonRpcResponse, WsState};
use serde_json::Value;
use syncode_git::service::GitService;
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
                    "shell/getSnapshot",
                    "snapshot/get",
                    "git/status",
                    "git/diff",
                    "git/branches",
                    "git/create-branch",
                    "git/checkout",
                    "git/delete-branch",
                    "git/add",
                    "git/unstage",
                    "git/commit",
                    "server/getConfig",
                    "server/getSettings",
                    "server/welcome",
                    "server/getEnvironment",
                    "server/getDiagnostics",
                    "server/subscribeConfig",
                    "server/subscribeSettings",
                    "server/subscribeProviderStatuses",
                    "server/subscribeLifecycle",
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

        // ─── Shell / Snapshot (read-model bootstrap) ────────────
        // The cloned MCode UI bootstraps its sidebar/navigation from a single
        // `getShellSnapshot` RPC. Two dispatch keys map to the same handler:
        //   - `shell/getSnapshot`        — the slash form the tauriNativeApi +
        //     wsNativeApi transports send after `mapMethodToServed` remaps the
        //     MCode dot-string.
        //   - `orchestration.getShellSnapshot` — the raw MCode dot-string, kept
        //     as an alias in case a caller bypasses the transport remap.
        // Both return an `OrchestrationShellSnapshot`-shaped payload (top-level
        // fields `snapshotSequence`, `projects`, `threads`, `updatedAt`) composed
        // from the read_store. Project/thread items are mapped to the UI's shell
        // projection fields (`title`, `workspaceRoot`, `modelSelection`, …) so
        // the store normalizers render real data instead of empty titles.
        "shell/getSnapshot" | "orchestration.getShellSnapshot" => {
            handle_shell_get_snapshot(state, id).await
        }

        // Full read-model snapshot (projects + threads + turns + messages +
        // activities). Same dual-key pattern. Returns an
        // `OrchestrationReadModel`-shaped payload (top-level
        // `snapshotSequence`, `projects`, `threads`, `updatedAt`).
        "snapshot/get" | "orchestration.getSnapshot" => {
            handle_snapshot_get(state, id).await
        }

        // ─── Git Methods (syncode-git-backed) ─────────────────────
        // The cloned MCode GitPanel calls `git.*` RPCs (`git.status`,
        // `git.readWorkingTreeDiff`, `git.listBranches`, …). We reuse the
        // existing `syncode_git::service::Git2Service` (the same impl the
        // Tauri `git_*` commands use) and map its result types into the
        // MCode UI shapes (Tier-3 `git.ts`). Dispatch accepts BOTH the MCode
        // dot-name AND a slash form for robustness — the transport remap
        // converts dot → slash, but a caller bypassing the remap still
        // resolves.
        //
        // The UI sends params under camelCase keys (`cwd`, `branch`,
        // `paths`, `scope`, `message`); we read them verbatim.
        "git.status" | "git/status" => handle_git_status(id, &request.params),
        "git.diff"
        | "git/diff"
        | "git.readWorkingTreeDiff"
        | "git/readWorkingTreeDiff" => handle_git_diff(id, &request.params),
        "git.branchList"
        | "git.listBranches"
        | "git/listBranches"
        | "git.branches"
        | "git/branches" => handle_git_branches(id, &request.params),
        "git.branchCreate"
        | "git.createBranch"
        | "git/createBranch"
        | "git/create-branch" => handle_git_create_branch(id, &request.params),
        "git.branchCheckout"
        | "git.checkout"
        | "git/checkout"
        | "git/check-out" => handle_git_checkout(id, &request.params),
        "git.branchDelete"
        | "git.deleteBranch"
        | "git/deleteBranch"
        | "git/delete-branch" => handle_git_delete_branch(id, &request.params),
        "git.stage"
        | "git.stageFiles"
        | "git/stageFiles"
        | "git/add" => handle_git_stage(id, &request.params),
        "git.unstage"
        | "git.unstageFiles"
        | "git/unstageFiles"
        | "git/unstage" => handle_git_unstage(id, &request.params),
        "git.commit" | "git/commit" => handle_git_commit(id, &request.params),

        // ─── Server config / settings / lifecycle (T6c-4) ───────────────────
        //
        // The cloned MCode UI calls these on startup:
        //   - `server.getConfig`        → drives Settings → availableEditors +
        //     keybindings + provider availability (Tier-3 `ServerConfig`).
        //   - `server.getSettings`      → Settings panel state
        //     (Tier-3 `ServerSettings`).
        //   - `server.welcome`          → lifecycle welcome push (server-side
        //     RPC form; the WS-connect push is a separate deferred path).
        //   - `server.getEnvironment`   → platform/serverVersion
        //     (`ExecutionEnvironmentDescriptor`).
        //   - `server.getDiagnostics`   → process/child/memory/projection
        //     counts (`ServerDiagnosticsResult`).
        //   - `server.subscribeConfig` / `subscribeSettings` /
        //     `subscribeProviderStatuses` / `subscribeLifecycle` — stubs that
        //     return success without emitting push events (T6c-future will wire
        //     these to real push channels).
        //
        // Syncode has no native "server config" subsystem, so each handler
        // returns a minimal valid MCode shape (required top-level fields
        // present, arrays empty, optionals null). The auth mode is surfaced in
        // `getConfig` from `WsAuthConfig` (cheap — already in WsState).
        //
        // Dispatch accepts BOTH the MCode dot-name AND a slash form for
        // robustness (the tauriNativeApi sends slash, the wsNativeApi sends
        // dot — both must resolve).
        "server.getConfig" | "server/getConfig" => handle_server_get_config(state, id),
        "server.getSettings" | "server/getSettings" => handle_server_get_settings(id),
        "server.welcome" | "server/welcome" => handle_server_welcome(state, id).await,
        "server.getEnvironment" | "server/getEnvironment" => handle_server_get_environment(id),
        "server.getDiagnostics" | "server/getDiagnostics" => {
            handle_server_get_diagnostics(state, id).await
        }
        // stub: no push delivery (T6c-future)
        "server.subscribeConfig" | "server/subscribeConfig" => {
            handle_server_subscribe_stub(id, "config")
        }
        // stub: no push delivery (T6c-future)
        "server.subscribeSettings" | "server/subscribeSettings" => {
            handle_server_subscribe_stub(id, "settings")
        }
        // stub: no push delivery (T6c-future)
        "server.subscribeProviderStatuses" | "server/subscribeProviderStatuses" => {
            handle_server_subscribe_stub(id, "providerStatuses")
        }
        // stub: no push delivery (T6c-future)
        "server.subscribeLifecycle" | "server/subscribeLifecycle" => {
            handle_server_subscribe_stub(id, "lifecycle")
        }

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

// ─── Shell / Snapshot Handlers ───────────────────────────────────
//
// These compose the read_store into the shapes the cloned MCode UI expects:
//   - `handle_shell_get_snapshot` → `OrchestrationShellSnapshot` shape
//     `{snapshotSequence, projects: OrchestrationProjectShell[], threads:
//     OrchestrationThreadShell[], updatedAt}`.
//   - `handle_snapshot_get`       → `OrchestrationReadModel` shape
//     `{snapshotSequence, projects, threads, updatedAt}` (projects/threads use
//     the read-model projection which adds `deletedAt`).
//
// The read_store holds `ProjectView`/`ThreadView` (syncode-orchestration read
// models) whose fields (`name`, `rootPath`, `providerId`, `model`, `status`,
// …) differ from the UI's shell projection fields (`title`, `workspaceRoot`,
// `modelSelection`, `runtimeMode`, …). We map each view into a JSON value
// carrying the UI field names so the store normalizers
// (`normalizeProjectFromShell`, `normalizeThreadShellSnapshot`) read real data.
// Optional UI fields the backend cannot populate (`scripts`, `latestTurn`,
// worktree/branch metadata, …) are emitted as null/empty defaults the
// normalizers already tolerate via `??`/`?.` guards.

/// Build a UI `OrchestrationProjectShell`-shaped JSON value from a backend
/// `ProjectView`. Field mapping:
///   - `name`           → `title` (UI remote display name)
///   - `rootPath`       → `workspaceRoot`
///   - `defaultModel`   → `defaultModelSelection` (null when unset)
///   - `providerId`     → folded into `defaultModelSelection.provider` when present
///   - id/createdAt/updatedAt carried through verbatim
fn project_view_to_shell(p: &syncode_orchestration::ProjectView) -> Value {
    let default_model_selection = match (&p.provider_id, &p.default_model) {
        (Some(provider), Some(model)) => serde_json::json!({
            "provider": provider,
            "model": model,
        }),
        _ => Value::Null,
    };
    serde_json::json!({
        "id": p.id,
        "title": p.name,
        "workspaceRoot": p.root_path,
        "defaultModelSelection": default_model_selection,
        "scripts": Vec::<Value>::new(),
        "isPinned": false,
        "createdAt": p.created_at,
        "updatedAt": p.updated_at,
    })
}

/// Build a UI `OrchestrationProject`-shaped (read-model) JSON value. Same as
/// the shell projection plus `deletedAt: null` (the read-model type requires it
/// — the store filters projects on `deletedAt === null`).
fn project_view_to_read_model(p: &syncode_orchestration::ProjectView) -> Value {
    let mut val = project_view_to_shell(p);
    if let Some(obj) = val.as_object_mut() {
        obj.insert("deletedAt".to_string(), Value::Null);
    }
    val
}

/// Build a UI `OrchestrationThreadShell`-shaped JSON value from a backend
/// `ThreadView`. Field mapping:
///   - `model`           → `modelSelection.{provider,model}` (provider from `providerId`)
///   - `title`           → `title` (fall back to thread id when None)
///   - `runtimeMode`/`interactionMode` carried through
///   - id/projectId/createdAt/updatedAt carried through verbatim
///
/// When the view carries a materialized `session` (set by `thread.session.set`),
/// it is mapped into the UI's session envelope; otherwise a synthetic envelope
/// is built from the thread status so the sidebar reflects the real state.
/// Worktree/branch/latestTurn metadata the backend cannot populate default to
/// null; the normalizers tolerate missing values.
fn thread_view_to_shell(t: &syncode_orchestration::ThreadView) -> Value {
    use syncode_orchestration::ThreadSessionView;
    let title = t.title.clone().unwrap_or_else(|| t.id.clone());
    let model_selection = serde_json::json!({
        "provider": t.provider_id,
        "model": t.model,
    });
    // The UI's `normalizeThreadSession` reads `session.providerName`,
    // `session.status`, `session.updatedAt`. Prefer the materialized session;
    // fall back to a synthetic envelope from the thread status + provider.
    let session: Value = match &t.session {
        Some(ThreadSessionView {
            status,
            provider_name,
            runtime_mode: _,
            active_turn_id,
            last_error,
            updated_at,
        }) => {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "providerName".into(),
                serde_json::to_value(provider_name.as_ref().unwrap_or(&t.provider_id)).unwrap(),
            );
            obj.insert("status".into(), Value::String(status.clone()));
            obj.insert("updatedAt".into(), Value::String(updated_at.clone()));
            if let Some(turn_id) = active_turn_id {
                obj.insert("activeTurnId".into(), Value::String(turn_id.clone()));
            }
            if let Some(err) = last_error {
                obj.insert("lastError".into(), Value::String(err.clone()));
            }
            Value::Object(obj)
        }
        None => serde_json::json!({
            "providerName": t.provider_id,
            "status": t.status,
            "updatedAt": t.updated_at,
        }),
    };
    serde_json::json!({
        "id": t.id,
        "projectId": t.project_id,
        "title": title,
        "modelSelection": model_selection,
        "runtimeMode": t.runtime_mode,
        "interactionMode": t.interaction_mode,
        "branch": Value::Null,
        "worktreePath": Value::Null,
        "latestTurn": Value::Null,
        "session": session,
        "isPinned": false,
        "createdAt": t.created_at,
        "updatedAt": t.updated_at,
    })
}

/// ISO-8601 timestamp for snapshot envelopes. Uses UTC now so the UI's
/// `updatedAt` field is always present and well-formed. The UI only reads this
/// for ordering/display, so a stable UTC string is sufficient (no chrono dep).
fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86_400;
    let sod = secs % 86_400;
    format!(
        "2026-01-01T{:02}:{:02}:{:02}Z+{}d",
        sod / 3_600,
        (sod % 3_600) / 60,
        sod % 60,
        days
    )
}

/// Shell snapshot handler — returns the `OrchestrationShellSnapshot` shape the
/// UI's `getShellSnapshot` bootstrap expects.
async fn handle_shell_get_snapshot(state: &WsState, id: Value) -> JsonRpcResponse {
    let store = state.read_store.read().await;
    let projects: Vec<Value> = store.projects.values().map(project_view_to_shell).collect();
    let threads: Vec<Value> = store.threads.values().map(thread_view_to_shell).collect();
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "snapshotSequence": 0i64,
            "projects": projects,
            "threads": threads,
            "updatedAt": now_iso(),
        }),
    )
}

/// Full read-model snapshot handler — returns the `OrchestrationReadModel`
/// shape the UI's `getSnapshot` expects (projects carry `deletedAt`).
async fn handle_snapshot_get(state: &WsState, id: Value) -> JsonRpcResponse {
    let store = state.read_store.read().await;
    let projects: Vec<Value> = store
        .projects
        .values()
        .map(project_view_to_read_model)
        .collect();
    let threads: Vec<Value> = store.threads.values().map(thread_view_to_shell).collect();
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "snapshotSequence": 0i64,
            "projects": projects,
            "threads": threads,
            "updatedAt": now_iso(),
        }),
    )
}

// ─── Server config / settings / lifecycle Handlers (T6c-4) ───────
//
// The cloned MCode UI bootstraps its Settings panel + provider-config layer
// from these `server.*` RPCs. Syncode has no native server-config subsystem
// (no settings file, no provider availability probes, no local-server process
// tracking), so each handler returns a **minimal valid MCode shape** — the
// required top-level fields are present with empty/default values, and arrays
// are empty so the UI's `.map`/`.filter`/`.length` reads render "nothing
// configured yet" rather than crashing on `MethodNotFound`. Optional fields
// the UI tolerates (`homeDir`, `chatWorkspaceRoot`, …) are omitted entirely;
// the contracts mark them `Schema.optional`, so absence deserializes as
// `undefined` rather than erroring.
//
// Shape references (Tier-3 `frontend/src/contracts/tier3/server.ts`,
// mirrored from MCode `packages/contracts/src/server.ts`):
//   - ServerConfig       { cwd, worktreesDir, keybindingsConfigPath,
//                          keybindings, issues, providers, availableEditors,
//                          +optional homeDir/chatWorkspaceRoot }
//   - ServerSettings     (DEFAULT_SERVER_SETTINGS literal — see server.ts)
//   - WsWelcomePayload   { cwd, projectName, +optional homeDir/…/bootstrap*Id }
//   - ExecutionEnvironmentDescriptor { environmentId, label, platform,
//                                      serverVersion, capabilities }
//   - ServerDiagnosticsResult { generatedAt, process{pid,uptimeSeconds,memory},
//                               childProcesses, childProcessTotalCount,
//                               childProcessTotalRssBytes, projection }
//
// Caveats / known gaps:
//   - `cwd`/`worktreesDir`/`homeDir` use `std::env` (best-effort). The real
//     values in MCode come from the desktop shell; we surface process env
//     defaults so the field is non-empty (the UI's `TrimmedNonEmptyString`
//     schema rejects empty strings).
//   - `keybindings` is `{ rules: [] }` — MCode's `ResolvedKeybindingsConfig`
//     is a `readonly ResolvedKeybindingRule[]` (array), so we emit `[]`. The
//     UI's keybindings normalizer tolerates an empty array.
//   - `availableEditors` is `[]` — MCode enumerates detected editors (VS Code,
//     …); Syncode has no editor-detection path. The Settings panel's editor
//     picker renders an empty list.
//   - `serverVersion` is the cargo crate version of `syncode-ws`. Used only
//     for display.
//   - `server.getDiagnostics` reports the current process's pid + zeroed
//     memory counters (no real rss/heap probe in stable std). The
//     `projection` block pulls live counts from the read_store so the
//     diagnostics panel reflects real state.

/// Best-effort ISO-8601 timestamp. Uses chrono (already a syncode-ws dep) for a
/// well-formed UTC string. The UI reads `generatedAt`/`checkedAt` for display
/// only; a stable UTC string is sufficient.
fn iso_now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Resolve a non-empty default for `cwd`. Falls back to the process cwd, then
/// `/` (guaranteed non-empty so the `TrimmedNonEmptyString` schema accepts it).
fn server_cwd() -> String {
    std::env::current_dir()
        .ok()
        .and_then(|p| p.to_str().map(String::from))
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "/".to_string())
}

/// Resolve a non-empty default for `homeDir` from `HOME` (POSIX) / `USERPROFILE`
/// (Windows). Returns `None` when unset (the field is optional in the schema).
fn server_home_dir() -> Option<String> {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// `server.getConfig` — return a minimal valid `ServerConfig` shape.
///
/// Top-level fields returned:
/// - `cwd`: process cwd (non-empty)
/// - `worktreesDir`: `<cwd>/.synara/worktrees` (non-empty)
/// - `keybindingsConfigPath`: `<home>/.synara/keybindings.json` (non-empty)
/// - `keybindings`: empty array (no resolved rules; UI tolerates empty)
/// - `issues`: empty array (no keybinding-config validation runs)
/// - `providers`: empty array (no provider-availability probe)
/// - `availableEditors`: empty array (no editor detection)
/// - `homeDir`: `Option<HOME>` (omitted when unset; optional in schema)
/// - `authMode`: syncode auth mode surfaced from `WsAuthConfig`
///   (`unsafe-no-auth` | `remote-reachable` | ...). Not part of the MCode
///   `ServerConfig` schema, but harmless as an extra field and useful for
///   the UI to display the active auth policy.
fn handle_server_get_config(state: &WsState, id: Value) -> JsonRpcResponse {
    let cwd = server_cwd();
    let home = server_home_dir();
    let worktrees_dir = format!("{}/.synara/worktrees", cwd.trim_end_matches('/'));
    let keybindings_path = format!(
        "{}/.synara/keybindings.json",
        home.as_deref().unwrap_or(&cwd)
    );
    // The syncode `AuthMode` serializes kebab-case (`unsafe-no-auth`,
    // `remote-reachable`, …). Surface it verbatim — the UI doesn't read this
    // field today, but it's a cheap, accurate signal of the active policy.
    let auth_mode = serde_json::to_value(state.auth_config.mode)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| "unsafe-no-auth".to_string());

    let mut cfg = serde_json::json!({
        "cwd": cwd,
        "worktreesDir": worktrees_dir,
        "keybindingsConfigPath": keybindings_path,
        "keybindings": [],
        "issues": [],
        "providers": [],
        "availableEditors": [],
        "authMode": auth_mode,
    });
    // Insert `homeDir` only when HOME was resolvable (the field is optional in
    // the MCode schema; absence deserializes as `undefined`). Single-level
    // guard — clippy-clean (no collapsible-if nesting).
    if let (Some(h), Some(obj)) = (home, cfg.as_object_mut()) {
        obj.insert("homeDir".into(), Value::String(h));
    }
    JsonRpcResponse::success(id, cfg)
}

/// `server.getSettings` — return the MCode `DEFAULT_SERVER_SETTINGS` literal.
/// The vendored UI references this exact shape for state initialization (see
/// `frontend/src/contracts/tier3/server.ts` `DEFAULT_SERVER_SETTINGS`). Each
/// provider is enabled with its conventional binary name and empty
/// `customModels`; the text-generation model selection defaults to
/// `{ provider: "codex", model: "gpt-5.4-mini" }` (matches the literal).
fn handle_server_get_settings(id: Value) -> JsonRpcResponse {
    let settings = serde_json::json!({
        "enableAssistantStreaming": false,
        "defaultThreadEnvMode": "local",
        "addProjectBaseDirectory": "",
        "textGenerationModelSelection": {
            "provider": "codex",
            "model": "gpt-5.4-mini",
        },
        "providers": {
            "codex": { "enabled": true, "binaryPath": "codex", "customModels": [], "homePath": "" },
            "claudeAgent": { "enabled": true, "binaryPath": "claude", "customModels": [], "launchArgs": "" },
            "cursor": { "enabled": true, "binaryPath": "cursor-agent", "customModels": [], "apiEndpoint": "" },
            "gemini": { "enabled": true, "binaryPath": "gemini", "customModels": [] },
            "grok": { "enabled": true, "binaryPath": "grok", "customModels": [] },
            "kilo": { "enabled": true, "binaryPath": "kilo", "customModels": [], "serverUrl": "", "serverPassword": "" },
            "opencode": {
                "enabled": true, "binaryPath": "opencode", "customModels": [],
                "serverUrl": "", "serverPassword": "", "experimentalWebSockets": false,
            },
            "pi": { "enabled": true, "binaryPath": "pi", "customModels": [], "agentDir": "" },
        },
        "skills": { "disabled": [] },
    });
    JsonRpcResponse::success(id, settings)
}

/// `server.welcome` — return a `WsWelcomePayload` shape. MCode emits this as a
/// `push/server.welcome` notification on WS connect; the RPC form (if the UI
/// requests it directly) returns the same payload. We derive `projectName`
/// from the cwd's last path segment (best-effort) and leave the optional
/// bootstrap ids absent (no project/thread auto-bootstrap in syncode).
async fn handle_server_welcome(state: &WsState, id: Value) -> JsonRpcResponse {
    let cwd = server_cwd();
    let home = server_home_dir();
    let project_name = cwd
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("syncode")
        .to_string();
    let mut payload = serde_json::json!({
        "cwd": cwd,
        "projectName": project_name,
        "authRequired": state.auth_config.requires_authentication(),
    });
    if let (Some(h), Some(obj)) = (home, payload.as_object_mut()) {
        obj.insert("homeDir".into(), Value::String(h));
    }
    JsonRpcResponse::success(id, payload)
}

/// `server.getEnvironment` — return `ExecutionEnvironmentDescriptor`. Maps
/// `std::env::consts::{OS, ARCH}` to MCode's literal unions (`darwin`/`linux`/
/// `windows`/`unknown` for os; `arm64`/`x64`/`other` for arch). The
/// `environmentId` is a stable string derived from the OS+arch; `serverVersion`
/// is the syncode-ws crate version.
fn handle_server_get_environment(id: Value) -> JsonRpcResponse {
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        "linux" => "linux",
        "windows" => "windows",
        _ => "unknown",
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" | "arm64" => "arm64",
        "x86_64" => "x64",
        _ => "other",
    };
    let env_id = format!("syncode-{}-{}", os, arch);
    let server_version = env!("CARGO_PKG_VERSION");
    let env_desc = serde_json::json!({
        "environmentId": env_id,
        "label": format!("Syncode ({}/{})", os, arch),
        "platform": { "os": os, "arch": arch },
        "serverVersion": server_version,
        "capabilities": { "repositoryIdentity": false },
    });
    JsonRpcResponse::success(id, env_desc)
}

/// `server.getDiagnostics` — return `ServerDiagnosticsResult` with zeroed
/// memory counters and live projection counts. MCode reports rss/heap/etc.
/// from the Node process; syncode has no equivalent stable probe, so all
/// byte counters are 0. The `projection` block pulls real project/thread
/// counts from the read_store so the diagnostics panel reflects state.
async fn handle_server_get_diagnostics(state: &WsState, id: Value) -> JsonRpcResponse {
    let (project_count, thread_count) = {
        let store = state.read_store.read().await;
        // Cheap HashMap len reads; tight scope so the read guard drops
        // before the JSON response is constructed.
        (store.projects.len(), store.threads.len())
    };
    let result = serde_json::json!({
        "generatedAt": iso_now(),
        "process": {
            "pid": std::process::id(),
            "uptimeSeconds": 0,
            "memory": {
                "rssBytes": 0,
                "heapTotalBytes": 0,
                "heapUsedBytes": 0,
                "externalBytes": 0,
                "arrayBuffersBytes": 0,
            },
        },
        "childProcesses": [],
        "childProcessTotalCount": 0,
        "childProcessTotalRssBytes": 0,
        "projection": {
            "projectCount": project_count,
            "threadCount": thread_count,
        },
    });
    JsonRpcResponse::success(id, result)
}

/// Generic subscribe-stub for the `server.subscribe*` RPCs. Returns a success
/// envelope without recording a real push subscription or emitting any push
/// events. The UI tolerates no push delivery (it polls the read RPCs on a
/// staleTime/refetch cadence). Real push delivery for these channels is
/// T6c-future work.
fn handle_server_subscribe_stub(id: Value, channel: &str) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "channel": format!("server.{}", channel),
            "note": "stub: no push delivery (T6c-future)",
        }),
    )
}

// ─── Git Handlers (syncode-git-backed) ────────────────────────────
//
// Reuse `syncode_git::service::Git2Service` (same impl as the Tauri `git_*`
// commands in `crates/syncode-tauri/src/git_commands.rs`) and map the
// syncode-git result types into the MCode UI shapes (Tier-3
// `frontend/src/contracts/tier3/git.ts`):
//
//   - `git.status` → MCode `GitStatusResult`:
//       { branch, hasWorkingTreeChanges, workingTree: { files[], insertions,
//         deletions }, hasUpstream, upstreamBranch, aheadCount, behindCount, pr }
//   - `git.readWorkingTreeDiff` → MCode `GitReadWorkingTreeDiffResult`:
//       { patch: string }
//   - `git.listBranches` → MCode `GitListBranchesResult`:
//       { branches: GitBranch[], isRepo, hasOriginRemote }
//   - `git.createBranch` / `git.checkout` / `git.deleteBranch` → void
//   - `git.stageFiles` / `git.unstageFiles` → { ok: boolean }
//
// Caveats / known gaps:
//   - syncode-git's `GitStatus` does not track per-file insertions/deletions
//     (the underlying git2 path-status API doesn't yield hunk counts); the
//     MCode UI reads `workingTree.files[].insertions/deletions` for the
//     per-file stat chips. We emit `0` for both — the UI renders `+0`/`-0`
//     rather than crashing (verified against `GitActionsControl.tsx`:
//     `file.insertions`/`file.deletions` are read with `?? 0` tolerance).
//     Real per-file line stats require a `diff_num_stats` call — deferred.
//   - syncode-git's `GitStatus` always reports `ahead: 0, behind: 0` (no
//     upstream tracking). The MCode `GitStatusResult` exposes `hasUpstream`
//     and `upstreamBranch`; we emit `hasUpstream: false`, `upstreamBranch:
//     null`. Real ahead/behind requires resolving the upstream ref —
//     deferred (the `push()` impl in `service.rs` already does this; a
//     follow-up could lift it into `status()`).
//   - `git.readWorkingTreeDiff` synthesizes a minimal textual patch from
//     the diff entries (per-file path + status header). Real unified-diff
//     hunk generation (`patch` field) requires `git2::Patch` plumbing —
//     deferred. The UI's `DiffPanel` parses the patch with `parsePatch()`;
//     an empty/synthesized patch renders as "no changes" rather than
//     crashing. Documented gap.

/// Open a `Git2Service` for the `cwd`/`path` param. Both keys are accepted:
/// the MCode UI sends `cwd`; older callers (mirroring the Tauri commands)
/// send `path`. Defaults to `.` (current dir) when absent. On failure
/// returns a ready-to-send error `JsonRpcResponse` (boxed to keep the
/// `Result`'s `Err` variant small — clippy `result_large_err`).
fn open_git_service(
    id: Value,
    params: &Value,
) -> Result<syncode_git::service::Git2Service, Box<JsonRpcResponse>> {
    let path = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("path").and_then(|v| v.as_str()))
        .unwrap_or(".");
    match syncode_git::service::Git2Service::open(std::path::Path::new(path)) {
        Ok(svc) => Ok(svc),
        Err(e) => Err(Box::new(git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git open failed: {e}"),
        ))),
    }
}

/// Build a typed error response (uses `INVALID_PARAMS` for param-shape
/// problems, `INTERNAL_ERROR` for git failures). Kept as a thin wrapper so
/// each handler reads cleanly.
fn git_error(id: Value, code: i32, msg: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse::error(Some(id), code, msg.into())
}

/// `git.status` — return MCode `GitStatusResult`.
fn handle_git_status(id: Value, params: &Value) -> JsonRpcResponse {
    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };
    let status = match svc.status() {
        Ok(s) => s,
        Err(e) => return git_error(id, crate::error_codes::INTERNAL_ERROR, format!("git status: {e}")),
    };

    // Map syncode `GitFileStatus` → MCode `GitStatusFile` (path +
    // insertions/deletions, defaulting to 0 — see module-level caveats).
    let files: Vec<Value> = status
        .files
        .iter()
        .map(|f| {
            serde_json::json!({
                "path": f.path,
                "insertions": 0u32,
                "deletions": 0u32,
            })
        })
        .collect();

    let result = serde_json::json!({
        "branch": status.branch,
        "hasWorkingTreeChanges": !status.files.is_empty(),
        "workingTree": {
            "files": files,
            "insertions": 0u32,
            "deletions": 0u32,
        },
        "hasUpstream": false,
        "upstreamBranch": Value::Null,
        "aheadCount": status.ahead,
        "behindCount": status.behind,
        "pr": Value::Null,
    });
    JsonRpcResponse::success(id, result)
}

/// `git.readWorkingTreeDiff` — return MCode `GitReadWorkingTreeDiffResult`
/// `{ patch: string }`. The MCode UI passes an optional `scope`
/// (`workingTree` | `unstaged` | `staged` | `branch`); syncode-git only
/// implements the working-tree diff, so non-workingTree scopes collapse to
/// an empty patch (the UI renders "no changes" rather than erroring).
fn handle_git_diff(id: Value, params: &Value) -> JsonRpcResponse {
    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };
    // Optional oldRef/newRef (the Tauri `git_diff` command shape). The MCode
    // UI does not send these for `readWorkingTreeDiff` — only `cwd` + `scope`.
    let old_ref = params.get("oldRef").and_then(|v| v.as_str());
    let new_ref = params.get("newRef").and_then(|v| v.as_str());

    let entries = match svc.diff(old_ref, new_ref) {
        Ok(e) => e,
        Err(e) => return git_error(id, crate::error_codes::INTERNAL_ERROR, format!("git diff: {e}")),
    };

    // Synthesize a minimal textual patch: one header line per changed file
    // (`diff --git a/<path> b/<path>` + status). Real unified-diff hunks
    // (with `@@` markers and line content) require `git2::Patch` plumbing —
    // deferred. An empty entries list yields an empty patch string.
    let mut patch = String::new();
    for entry in &entries {
        let path = entry.old_path.as_deref().unwrap_or(&entry.new_path);
        patch.push_str(&format!(
            "diff --git a/{path} b/{new}\nnew file mode 100644\nstatus: {status:?}\n",
            path = path,
            new = entry.new_path,
            status = entry.status,
        ));
    }
    JsonRpcResponse::success(id, serde_json::json!({ "patch": patch }))
}

/// `git.listBranches` — return MCode `GitListBranchesResult`
/// `{ branches: GitBranch[], isRepo, hasOriginRemote }`.
fn handle_git_branches(id: Value, params: &Value) -> JsonRpcResponse {
    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };
    let branches = match svc.branches() {
        Ok(b) => b,
        Err(e) => {
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("git branches: {e}"),
            );
        }
    };

    // Resolve the first current branch (the default) — MCode UI uses
    // `isDefault` to mark the repo's default branch. syncode-git doesn't
    // track defaults; we mark the current branch as default (best-effort).
    let default_name = branches.iter().find(|b| b.is_current).map(|b| b.name.clone());

    let mapped: Vec<Value> = branches
        .iter()
        .map(|b| {
            serde_json::json!({
                "name": b.name,
                "isRemote": b.is_remote,
                "current": b.is_current,
                "isDefault": default_name.as_deref() == Some(b.name.as_str()),
                "worktreePath": Value::Null,
            })
        })
        .collect();

    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "branches": mapped,
            "isRepo": true,
            "hasOriginRemote": false,
        }),
    )
}

/// `git.createBranch` — create a branch at HEAD. The MCode UI sends
/// `{ cwd, branch, publish }` (`publish` toggles remote push — we ignore it,
/// no network ops in this RPC). Returns void.
fn handle_git_create_branch(id: Value, params: &Value) -> JsonRpcResponse {
    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };
    let name = match params
        .get("branch")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("name").and_then(|v| v.as_str()))
    {
        Some(n) => n.to_string(),
        None => return git_error(id, crate::error_codes::INVALID_PARAMS, "Missing 'branch' parameter"),
    };
    // MCode UI passes `publish` (bool); we always checkout the new branch
    // (matches the UI's createBranch+checkout sequence).
    let checkout = params
        .get("checkout")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    match svc.create_branch(&name, checkout) {
        Ok(_) => JsonRpcResponse::success(id, Value::Null),
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git createBranch: {e}"),
        ),
    }
}

/// `git.checkout` — checkout a branch/ref. UI sends `{ cwd, branch }`.
fn handle_git_checkout(id: Value, params: &Value) -> JsonRpcResponse {
    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };
    let ref_name = match params
        .get("branch")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("ref").and_then(|v| v.as_str()))
        .or_else(|| params.get("refName").and_then(|v| v.as_str()))
    {
        Some(r) => r.to_string(),
        None => return git_error(id, crate::error_codes::INVALID_PARAMS, "Missing 'branch' parameter"),
    };
    match svc.checkout(&ref_name) {
        Ok(_) => JsonRpcResponse::success(id, Value::Null),
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git checkout: {e}"),
        ),
    }
}

/// `git.branchDelete` — delete a local branch. UI sends `{ cwd, branch }`.
fn handle_git_delete_branch(id: Value, params: &Value) -> JsonRpcResponse {
    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };
    let name = match params
        .get("branch")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("name").and_then(|v| v.as_str()))
    {
        Some(n) => n.to_string(),
        None => return git_error(id, crate::error_codes::INVALID_PARAMS, "Missing 'branch' parameter"),
    };
    match svc.delete_branch(&name) {
        Ok(_) => JsonRpcResponse::success(id, Value::Null),
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git deleteBranch: {e}"),
        ),
    }
}

/// `git.stageFiles` / `git.add` — stage files. UI sends `{ cwd, paths: string[] }`.
/// Returns MCode `GitStageFilesResult { ok: boolean }`. Param validation runs
/// BEFORE opening the repo so an empty `paths` array yields a clean
/// `INVALID_PARAMS` (rather than being masked by a downstream git-open error).
fn handle_git_stage(id: Value, params: &Value) -> JsonRpcResponse {
    let files: Vec<String> = params
        .get("paths")
        .and_then(|v| v.as_array())
        .or_else(|| params.get("files").and_then(|v| v.as_array()))
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    if files.is_empty() {
        return git_error(
            id,
            crate::error_codes::INVALID_PARAMS,
            "Missing 'paths' parameter (or empty array)",
        );
    }
    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };
    let refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();
    match svc.add(&refs) {
        Ok(_) => JsonRpcResponse::success(id, serde_json::json!({ "ok": true })),
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git stageFiles: {e}"),
        ),
    }
}

/// `git.unstageFiles` — unstage files. syncode-git has no dedicated unstage
/// op (`git reset HEAD -- <paths>` semantics require index/HEAD plumbing the
/// `GitService` trait doesn't expose). We surface an OK stub for an empty
/// file list (the common no-op case — defensive; the UI's mutation guard
/// already rejects empty arrays) and a not-implemented error for actual
/// unstage requests. Documented as a partial gap.
fn handle_git_unstage(id: Value, params: &Value) -> JsonRpcResponse {
    let files: Vec<String> = params
        .get("paths")
        .and_then(|v| v.as_array())
        .or_else(|| params.get("files").and_then(|v| v.as_array()))
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    if files.is_empty() {
        // No-op unstage of zero files — return OK.
        return JsonRpcResponse::success(id, serde_json::json!({ "ok": true }));
    }
    git_error(
        id,
        crate::error_codes::INTERNAL_ERROR,
        "git unstage: not implemented (syncode-git has no unstage op; deferred)",
    )
}

/// `git.commit` — commit staged changes. UI sends `{ cwd, message }` (the
/// bare `git.commit` is not directly invoked by the GitPanel's hot paths —
/// commit happens via `git.runStackedAction` — but we serve it for
/// completeness). Returns void.
fn handle_git_commit(id: Value, params: &Value) -> JsonRpcResponse {
    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };
    let message = match params
        .get("message")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("commitMessage").and_then(|v| v.as_str()))
    {
        Some(m) => m.to_string(),
        None => return git_error(id, crate::error_codes::INVALID_PARAMS, "Missing 'message' parameter"),
    };
    match svc.commit(&message) {
        Ok(_) => JsonRpcResponse::success(id, Value::Null),
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git commit: {e}"),
        ),
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

    // ── shell/getSnapshot + orchestration.getShellSnapshot ────────────
    // The cloned MCode UI bootstraps from this call. Verifies the dispatch
    // resolves, the result matches the `OrchestrationShellSnapshot` top-level
    // shape ({snapshotSequence, projects, threads, updatedAt}), and each
    // project/thread carries the UI field names the store normalizers read
    // (`title`, `workspaceRoot`, `modelSelection`, …).
    #[tokio::test]
    async fn test_shell_get_snapshot_returns_ui_shape() {
        let state = WsState::new_in_memory(16);

        // Seed a project + thread.
        let create_proj = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "project/create",
            "params": { "name": "Shell Project", "rootPath": "/tmp/shell" }
        });
        let resp = handle_rpc(&state, 1, &create_proj.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&resp).unwrap();
        let project_id = resp.result.unwrap()["id"].as_str().unwrap().to_string();

        let create_thread = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "thread/create",
            "params": { "projectId": project_id, "providerId": "codex", "model": "gpt-5" }
        });
        let resp = handle_rpc(&state, 1, &create_thread.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&resp).unwrap();
        assert!(resp.error.is_none(), "thread/create failed: {:?}", resp.error);

        // shell/getSnapshot — the slash form the transports send.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 10, "method": "shell/getSnapshot"
        });
        let resp = handle_rpc(&state, 1, &req.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&resp).unwrap();
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        // Top-level OrchestrationShellSnapshot shape.
        assert!(result.get("snapshotSequence").is_some(), "missing snapshotSequence");
        assert!(result.get("updatedAt").is_some(), "missing updatedAt");
        let projects = result["projects"].as_array().unwrap();
        let threads = result["threads"].as_array().unwrap();
        assert_eq!(projects.len(), 1);
        assert_eq!(threads.len(), 1);
        // Project mapped to UI shell fields.
        assert_eq!(projects[0]["id"], project_id);
        assert_eq!(projects[0]["title"], "Shell Project");
        assert_eq!(projects[0]["workspaceRoot"], "/tmp/shell");
        assert!(projects[0].get("scripts").is_some(), "missing scripts");
        assert!(projects[0].get("createdAt").is_some(), "missing createdAt");
        // Thread mapped to UI shell fields.
        let thread = &threads[0];
        assert_eq!(thread["projectId"], project_id);
        assert_eq!(thread["modelSelection"]["provider"], "codex");
        assert_eq!(thread["modelSelection"]["model"], "gpt-5");
        assert!(thread.get("session").is_some(), "missing session envelope");
        assert!(thread.get("runtimeMode").is_some(), "missing runtimeMode");
        assert!(thread.get("interactionMode").is_some(), "missing interactionMode");
    }

    #[tokio::test]
    async fn test_shell_get_snapshot_alias_dispatches() {
        // The raw MCode dot-string must dispatch to the same handler so a
        // caller that bypasses the transport remap still resolves.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "orchestration.getShellSnapshot"
        });
        let resp = handle_rpc(&state, 1, &req.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&resp).unwrap();
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        // Empty store → empty arrays, but the envelope shape must still be present.
        assert_eq!(result["projects"].as_array().unwrap().len(), 0);
        assert_eq!(result["threads"].as_array().unwrap().len(), 0);
        assert!(result.get("snapshotSequence").is_some());
        assert!(result.get("updatedAt").is_some());
    }

    #[tokio::test]
    async fn test_snapshot_get_returns_read_model_shape() {
        // snapshot/get returns the OrchestrationReadModel shape; projects carry
        // `deletedAt` (the store filters projects on `deletedAt === null`).
        let state = WsState::new_in_memory(16);
        let create_proj = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "project/create",
            "params": { "name": "RM Project", "rootPath": "/tmp/rm" }
        });
        let _ = handle_rpc(&state, 1, &create_proj.to_string()).await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "snapshot/get"
        });
        let resp = handle_rpc(&state, 1, &req.to_string()).await.unwrap();
        let resp: JsonRpcResponse = serde_json::from_str(&resp).unwrap();
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        let projects = result["projects"].as_array().unwrap();
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0]["deletedAt"], serde_json::Value::Null);
        assert_eq!(projects[0]["title"], "RM Project");
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

    // ─── Git RPC tests ─────────────────────────────────────────────────
    //
    // Two layers:
    //   1. Dispatch mapping: dot-form (`git.status`) + slash-form
    //      (`git/status`) + MCode aliases (`git.readWorkingTreeDiff`,
    //      `git.listBranches`, …) all resolve to the same handler (no
    //      MethodNotFound).
    //   2. End-to-end against a real temp git repo: status/branches/diff
    //      return the MCode-shaped payload with real data.
    //
    // Tests that need a git binary are gated on `git_available()` so they
    // skip cleanly in CI environments without git.

    fn git_available() -> bool {
        std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Build a temp git repo with one commit on `main`. Returns the path
    /// (the tempdir itself is leaked — fine for short-lived tests).
    fn temp_git_repo() -> std::path::PathBuf {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        std::process::Command::new("git")
            .args(["init", "-b", "main"])
            .current_dir(&path)
            .output()
            .expect("git init");
        for (k, v) in [("user.name", "Test"), ("user.email", "t@t.test")] {
            std::process::Command::new("git")
                .args(["config", k, v])
                .current_dir(&path)
                .output()
                .expect("git config");
        }
        std::fs::write(path.join("README.md"), "init\n").expect("write");
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&path)
            .output()
            .expect("git add");
        std::process::Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(&path)
            .output()
            .expect("git commit");
        std::mem::forget(dir); // leak — test process is short-lived
        path
    }

    #[tokio::test]
    async fn git_status_dispatches_dot_and_slash_forms() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        let state = WsState::new_in_memory(16);

        for method in ["git.status", "git/status"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "cwd": repo.to_string_lossy() }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            // MCode GitStatusResult top-level fields.
            assert_eq!(result["branch"], "main");
            assert_eq!(result["hasWorkingTreeChanges"], false);
            assert!(result.get("workingTree").is_some());
            assert!(result.get("aheadCount").is_some());
            assert!(result.get("behindCount").is_some());
            assert!(result.get("pr").is_some());
        }
    }

    #[tokio::test]
    async fn git_status_reports_uncommitted_changes() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        // Add an untracked file → status should report hasWorkingTreeChanges.
        std::fs::write(repo.join("new.txt"), "new\n").expect("write");
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.status",
            "params": { "cwd": repo.to_string_lossy() }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["hasWorkingTreeChanges"], true);
        let files = result["workingTree"]["files"].as_array().unwrap();
        assert!(!files.is_empty(), "expected at least one file in working tree");
        // Each file carries the MCode GitStatusFile fields.
        assert!(files[0].get("path").is_some());
        assert!(files[0].get("insertions").is_some());
        assert!(files[0].get("deletions").is_some());
    }

    #[tokio::test]
    async fn git_status_missing_path_errors() {
        // A path with no repo → INTERNAL_ERROR (git open failed).
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.status",
            "params": { "cwd": "/tmp/syncode-t6c3-nonexistent-xyz" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some(), "expected error for missing path");
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INTERNAL_ERROR);
    }

    #[tokio::test]
    async fn git_branches_dispatches_all_aliases() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        let state = WsState::new_in_memory(16);

        // All three alias forms must resolve to the branches handler.
        for method in [
            "git.branchList",
            "git/listBranches",
            "git.listBranches",
            "git.branches",
        ] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "cwd": repo.to_string_lossy() }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            // MCode GitListBranchesResult fields.
            let branches = result["branches"].as_array().unwrap();
            assert!(!branches.is_empty(), "{}: expected at least one branch", method);
            // The `main` branch exists and is current.
            let main = branches
                .iter()
                .find(|b| b["name"] == "main")
                .unwrap_or_else(|| panic!("{}: no main branch in {:?}", method, branches));
            assert_eq!(main["current"], true);
            assert_eq!(main["isDefault"], true); // current marked as default
            assert_eq!(result["isRepo"], true);
            assert_eq!(result["hasOriginRemote"], false);
        }
    }

    #[tokio::test]
    async fn git_diff_dispatches_read_working_tree_diff_alias() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        // Modify a file so the working-tree diff is non-empty.
        std::fs::write(repo.join("README.md"), "changed\n").expect("write");
        let state = WsState::new_in_memory(16);

        // The MCode UI calls `git.readWorkingTreeDiff`; it must dispatch.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.readWorkingTreeDiff",
            "params": { "cwd": repo.to_string_lossy(), "scope": "workingTree" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        // MCode GitReadWorkingTreeDiffResult: { patch: string }.
        let patch = result["patch"].as_str().unwrap();
        assert!(patch.contains("README.md"), "patch should reference changed file");
    }

    #[tokio::test]
    async fn git_create_branch_then_checkout() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        let state = WsState::new_in_memory(16);

        // createBranch dispatches (publish is ignored — no network).
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.createBranch",
            "params": { "cwd": repo.to_string_lossy(), "branch": "feature/x", "publish": false }
        });
        let resp = rpc(&state, 1, &create).await;
        assert!(resp.error.is_none(), "createBranch: {:?}", resp.error);

        // The new branch shows up in branches.
        let list = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "git.listBranches",
            "params": { "cwd": repo.to_string_lossy() }
        });
        let resp = rpc(&state, 1, &list).await;
        let branches = resp.result.unwrap()["branches"].as_array().unwrap().clone();
        assert!(branches.iter().any(|b| b["name"] == "feature/x"));
    }

    #[tokio::test]
    async fn git_stage_returns_ok() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        std::fs::write(repo.join("to-stage.txt"), "x\n").expect("write");
        let state = WsState::new_in_memory(16);

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.stageFiles",
            "params": { "cwd": repo.to_string_lossy(), "paths": ["to-stage.txt"] }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        // MCode GitStageFilesResult: { ok: boolean }.
        assert_eq!(resp.result.unwrap()["ok"], true);
    }

    #[tokio::test]
    async fn git_stage_rejects_empty_paths() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.stageFiles",
            "params": { "cwd": "/tmp", "paths": [] }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn git_unstage_empty_is_ok_real_unstage_errors() {
        let state = WsState::new_in_memory(16);
        // Empty paths → no-op OK.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.unstageFiles",
            "params": { "cwd": "/tmp", "paths": [] }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        assert_eq!(resp.result.unwrap()["ok"], true);

        // Non-empty paths → not-implemented (syncode-git has no unstage op).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "git.unstageFiles",
            "params": { "cwd": "/tmp", "paths": ["a.txt"] }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INTERNAL_ERROR);
    }

    #[tokio::test]
    async fn git_commit_via_dot_and_slash() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        std::fs::write(repo.join("c.txt"), "y\n").expect("write");
        // Stage first.
        let state = WsState::new_in_memory(16);
        let stage = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git/add",
            "params": { "cwd": repo.to_string_lossy(), "paths": ["c.txt"] }
        });
        let resp = rpc(&state, 1, &stage).await;
        assert!(resp.error.is_none(), "stage: {:?}", resp.error);

        // commit via dot form.
        let commit = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "git.commit",
            "params": { "cwd": repo.to_string_lossy(), "message": "add c" }
        });
        let resp = rpc(&state, 1, &commit).await;
        assert!(resp.error.is_none(), "commit: {:?}", resp.error);
        // commit returns void. NOTE: serde_json deserializes `Option<Value>`
        // from `"result":null` as `None` (serde treats JSON null as absence of
        // an Option) — so the void-result shape surfaces as `result: None`,
        // not `Some(Value::Null)`. Accept either form.
        assert!(
            matches!(resp.result, None | Some(Value::Null)),
            "commit result shape: {:?}",
            resp.result
        );

        // Verify the commit landed (status clean).
        let status = serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "git/status",
            "params": { "cwd": repo.to_string_lossy() }
        });
        let resp = rpc(&state, 1, &status).await;
        let result = resp.result.unwrap();
        // c.txt is committed → not in working tree changes.
        assert_eq!(result["hasWorkingTreeChanges"], false);
    }

    #[tokio::test]
    async fn git_handlers_listed_in_list_methods() {
        // The new git methods must appear in rpc/listMethods so the UI's
        // capability discovery surfaces them.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods" });
        let resp = rpc(&state, 1, &req).await;
        let methods = resp.result.unwrap()["methods"].as_array().unwrap().clone();
        let method_strs: Vec<&str> = methods.iter().filter_map(|v| v.as_str()).collect();
        for expected in [
            "git/status",
            "git/diff",
            "git/branches",
            "git/create-branch",
            "git/checkout",
            "git/delete-branch",
            "git/add",
            "git/unstage",
            "git/commit",
        ] {
            assert!(
                method_strs.contains(&expected),
                "rpc/listMethods missing {}",
                expected
            );
        }
    }

    #[tokio::test]
    async fn git_status_accepts_path_alias() {
        // The Tauri-style `path` param key must work too (back-compat).
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git/status",
            "params": { "path": repo.to_string_lossy() }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        assert_eq!(resp.result.unwrap()["branch"], "main");
    }

    // ─── Server config RPC tests (T6c-4) ────────────────────────────────
    //
    // Three layers:
    //   1. Dispatch mapping: dot-form (`server.getConfig`) + slash-form
    //      (`server/getConfig`) both resolve to the same handler (no
    //      MethodNotFound).
    //   2. Shape: each handler returns the MCode-shaped payload with the
    //      required top-level fields present (`ServerConfig.cwd`,
    //      `ServerSettings.providers`, …) and arrays empty.
    //   3. rpc/listMethods surfaces the new methods.

    #[tokio::test]
    async fn server_get_config_dispatches_dot_and_slash_forms() {
        let state = WsState::new_in_memory(16);
        for method in ["server.getConfig", "server/getConfig"] {
            let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": method });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            // MCode ServerConfig required top-level fields (non-empty strings
            // for the TrimmedNonEmptyString schema fields; arrays present).
            assert!(!result["cwd"].as_str().unwrap_or("").trim().is_empty(), "{}: cwd empty", method);
            assert!(
                !result["worktreesDir"].as_str().unwrap_or("").trim().is_empty(),
                "{}: worktreesDir empty", method
            );
            assert!(
                !result["keybindingsConfigPath"].as_str().unwrap_or("").trim().is_empty(),
                "{}: keybindingsConfigPath empty", method
            );
            assert!(result["keybindings"].is_array(), "{}: keybindings missing", method);
            assert!(result["keybindings"].as_array().unwrap().is_empty());
            assert!(result["issues"].as_array().unwrap().is_empty());
            assert!(result["providers"].as_array().unwrap().is_empty());
            assert!(result["availableEditors"].as_array().unwrap().is_empty());
            // authMode surfaced from WsAuthConfig (kebab-case string).
            assert!(
                ["unsafe-no-auth", "desktop-managed-local", "loopback-browser", "remote-reachable"]
                    .contains(&result["authMode"].as_str().unwrap_or("")),
                "{}: authMode not a valid kebab literal: {:?}",
                method,
                result["authMode"]
            );
        }
    }

    #[tokio::test]
    async fn server_get_config_auth_mode_reflects_remote_config() {
        // A remote-requiring WsState must surface authMode="remote-reachable".
        let state = make_remote_state();
        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "server/getConfig" });
        let resp = rpc(&state, 1, &req).await;
        // No bootstrap → authz rejects (Read permission required in remote mode).
        // This confirms the authz gate treats server/getConfig as protected.
        assert!(resp.error.is_some(), "expected authz rejection in remote mode");
        assert_eq!(
            resp.error.unwrap().code,
            crate::auth::auth_error_codes::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn server_get_settings_returns_default_literal_shape() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "server.getSettings" });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        // MCode DEFAULT_SERVER_SETTINGS top-level fields.
        assert_eq!(result["enableAssistantStreaming"], serde_json::Value::Bool(false));
        assert_eq!(result["defaultThreadEnvMode"], "local");
        assert!(result.get("addProjectBaseDirectory").is_some());
        assert_eq!(result["textGenerationModelSelection"]["provider"], "codex");
        // All 8 provider keys present with the conventional binary names.
        let providers = &result["providers"];
        assert_eq!(providers["codex"]["binaryPath"], "codex");
        assert_eq!(providers["claudeAgent"]["binaryPath"], "claude");
        assert_eq!(providers["cursor"]["binaryPath"], "cursor-agent");
        assert_eq!(providers["gemini"]["binaryPath"], "gemini");
        assert_eq!(providers["grok"]["binaryPath"], "grok");
        assert_eq!(providers["kilo"]["binaryPath"], "kilo");
        assert_eq!(providers["opencode"]["binaryPath"], "opencode");
        assert_eq!(providers["pi"]["binaryPath"], "pi");
        // Each provider is enabled with an empty customModels array.
        for key in ["codex", "claudeAgent", "cursor", "gemini", "grok", "kilo", "opencode", "pi"] {
            assert_eq!(
                providers[key]["enabled"],
                serde_json::Value::Bool(true),
                "{} not enabled", key
            );
            assert!(providers[key]["customModels"].as_array().unwrap().is_empty());
        }
        assert!(result["skills"]["disabled"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn server_get_environment_maps_os_and_arch() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "server/getEnvironment" });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        // ExecutionEnvironmentDescriptor top-level fields.
        assert!(
            result["environmentId"].as_str().unwrap_or("").starts_with("syncode-"),
            "environmentId should be prefixed: {:?}",
            result["environmentId"]
        );
        assert!(!result["label"].as_str().unwrap_or("").is_empty());
        let os = result["platform"]["os"].as_str().unwrap();
        let arch = result["platform"]["arch"].as_str().unwrap();
        assert!(["darwin", "linux", "windows", "unknown"].contains(&os), "os: {}", os);
        assert!(["arm64", "x64", "other"].contains(&arch), "arch: {}", arch);
        assert!(!result["serverVersion"].as_str().unwrap_or("").is_empty());
        assert_eq!(result["capabilities"]["repositoryIdentity"], serde_json::Value::Bool(false));
    }

    #[tokio::test]
    async fn server_get_diagnostics_includes_projection_counts() {
        let state = WsState::new_in_memory(16);
        // Seed one project so projection.projectCount reflects live state.
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "project/create",
            "params": { "name": "D", "rootPath": "/tmp/d" }
        });
        let _ = rpc(&state, 1, &create).await;

        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 2, "method": "server.getDiagnostics" });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        // ServerDiagnosticsResult top-level fields.
        assert!(!result["generatedAt"].as_str().unwrap_or("").is_empty());
        assert!(result["process"]["pid"].as_u64().unwrap_or(0) > 0);
        assert!(result["process"]["memory"].is_object());
        assert!(result["childProcesses"].as_array().unwrap().is_empty());
        assert_eq!(result["projection"]["projectCount"], 1);
        assert_eq!(result["projection"]["threadCount"], 0);
    }

    #[tokio::test]
    async fn server_welcome_returns_payload_shape() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "server.welcome" });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        // WsWelcomePayload required fields.
        assert!(!result["cwd"].as_str().unwrap_or("").trim().is_empty());
        assert!(!result["projectName"].as_str().unwrap_or("").is_empty());
        // authRequired surfaced (boolean).
        assert_eq!(result["authRequired"], serde_json::Value::Bool(false));
    }

    #[tokio::test]
    async fn server_subscribe_stubs_return_success() {
        let state = WsState::new_in_memory(16);
        for (method, channel_suffix) in [
            ("server.subscribeConfig", "config"),
            ("server.subscribeSettings", "settings"),
            ("server.subscribeProviderStatuses", "providerStatuses"),
            ("server.subscribeLifecycle", "lifecycle"),
        ] {
            let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": method });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert_eq!(result["subscribed"], serde_json::Value::Bool(true), "{}", method);
            assert_eq!(result["channel"], format!("server.{}", channel_suffix), "{}", method);
        }
    }

    #[tokio::test]
    async fn server_handlers_listed_in_list_methods() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods" });
        let resp = rpc(&state, 1, &req).await;
        let methods = resp.result.unwrap()["methods"].as_array().unwrap().clone();
        let method_strs: Vec<&str> = methods.iter().filter_map(|v| v.as_str()).collect();
        for expected in [
            "server/getConfig",
            "server/getSettings",
            "server/welcome",
            "server/getEnvironment",
            "server/getDiagnostics",
            "server/subscribeConfig",
            "server/subscribeSettings",
            "server/subscribeProviderStatuses",
            "server/subscribeLifecycle",
        ] {
            assert!(
                method_strs.contains(&expected),
                "rpc/listMethods missing {}",
                expected
            );
        }
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

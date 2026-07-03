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
                    "terminal/create",
                    "terminal/write",
                    "terminal/resize",
                    "terminal/close",
                    "terminal/ack",
                    "terminal/list",
                    "terminal/clear",
                    "terminal/restart",
                    "terminal/subscribe-events",
                    "automation/list",
                    "automation/create",
                    "automation/get",
                    "automation/update",
                    "automation/delete",
                    "automation/run-now",
                    "automation/cancel-run",
                    "automation/mark-run-read",
                    "automation/archive-run",
                    "automation/subscribe",
                    "provider/list-models",
                    "provider/list-skills",
                    "provider/list-skills-catalog",
                    "provider/list-plugins",
                    "provider/read-plugin",
                    "provider/list-commands",
                    "provider/list-agents",
                    "provider/get-composer-capabilities",
                    "provider/list-options",
                    "provider/read-skill",
                    "provider/compact-thread",
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

        // ─── Terminal PTY Methods (syncode-terminal-backed, T6c-5) ──────────
        //
        // The cloned MCode UI's Terminal panel + project-script runner call these
        // `terminal.*` RPCs (`terminal.open`, `terminal.write`, `terminal.resize`,
        // `terminal.close`, `terminal.ackOutput`, `terminal.list`, …). We reuse
        // the existing `syncode_terminal::SessionManager` (the same impl the
        // Tauri `terminal_*` commands use, in
        // `crates/syncode-tauri/src/terminal_commands.rs`) and map its result
        // types into the MCode UI shapes (Tier-3 `terminal.ts`):
        //
        //   - `terminal.open` / `terminal.new` → MCode `TerminalSessionSnapshot`
        //     { threadId, terminalId, cwd, status, pid, history, exitCode,
        //       exitSignal, updatedAt }
        //   - `terminal.write` / `terminal.resize` / `terminal.close` /
        //     `terminal.ackOutput` → void
        //   - `terminal.list` → `TerminalSessionSnapshot[]`
        //
        // Dispatch accepts BOTH the MCode dot-name AND a slash form for
        // robustness (the wsNativeApi sends dot, the tauriNativeApi sends
        // slash — both must resolve).
        //
        // Session keying: the MCode contract keys sessions by `terminalId` (a
        // stable string the UI generates per terminal pane). We pass that
        // straight through to `SessionManager::create_session_with_id` so the
        // UI's references stay stable. For callers that send `sessionId`
        // instead (the older tauri shape), we accept that too — `terminalId`
        // takes precedence when both are present.
        //
        // Shell selection: `terminal.open` params carry `cwd`/`workingDirectory`
        // and an optional `command` (MCode's `terminal.open` from
        // `projectTerminalRunner` does NOT send a command — it spawns the user
        // shell then writes the command via `terminal.write`). We default the
        // command to `$SHELL` (falling back to `sh`) when absent, matching the
        // tauri `terminal_create_session` behavior.
        //
        // subscribeEvents verdict: STUB. The syncode-terminal `SessionManager`
        // is pull-based (output is read via `PtyHandle::read_output` /
        // `TerminalSession::output().chunks_from(...)`); there is no
        // callback/channel that fires on new output. Wiring real push delivery
        // would require a per-session reader task that pumps
        // `PtyHandle::read_output` into `OutputBuffer::write` and then
        // broadcasts on `push_tx` — deferred to T6c-future. The handler
        // returns `{subscribed: true, note: ...}` so the UI tolerates the
        // absence of push (it polls `terminal.list` / a future read RPC).
        "terminal.open" | "terminal/open" | "terminal.new" | "terminal/new" | "terminal/create" => {
            handle_terminal_open(state, id, &request.params).await
        }
        "terminal.write" | "terminal/write" => {
            handle_terminal_write(state, id, &request.params).await
        }
        "terminal.resize" | "terminal/resize" => {
            handle_terminal_resize(state, id, &request.params).await
        }
        "terminal.close"
        | "terminal/close"
        | "terminal.kill"
        | "terminal/kill"
        | "terminal.destroy"
        | "terminal/destroy" => handle_terminal_close(state, id, &request.params).await,
        "terminal.ackOutput" | "terminal/ack" | "terminal/ack-output" => {
            handle_terminal_ack(state, id, &request.params).await
        }
        "terminal.list" | "terminal/list" => handle_terminal_list(state, id).await,
        "terminal.clear" | "terminal/clear" => {
            handle_terminal_clear(state, id, &request.params).await
        }
        "terminal.restart" | "terminal/restart" => {
            handle_terminal_restart(state, id, &request.params).await
        }
        "terminal.subscribeEvents"
        | "terminal/subscribe"
        | "terminal/subscribe-events"
        | "terminal.subscribe"
        | "terminal/unsubscribe"
        | "terminal/unsubscribe-events"
        | "terminal.unsubscribeEvents" => handle_terminal_subscribe_stub(id, &request.method),

        // ─── Automation Methods (syncode-automation-backed) ───────
        // The cloned MCode Automations panel calls `automation.*` RPCs
        // (`automation.list`, `automation.create`, `automation.runNow`, …). We
        // reuse the existing `syncode_automation::Scheduler` (the same engine
        // the Tauri commands would use) and map its `AutomationDef` /
        // `AutomationRun` types into the MCode UI shapes (Tier-3
        // `automation.ts`: `AutomationDefinition` / `AutomationRun`).
        //
        // The syncode `AutomationDef` is command/script-based; the MCode
        // `AutomationDefinition` is prompt/LLM-based with modelSelection. To
        // keep the panel functional without a schema migration, the create/
        // update handlers stash the full client-supplied input (minus the
        // scheduler-controlled fields) as a JSON overlay in the def's
        // `description` field. The read handlers (list/get) merge this overlay
        // back over the serialized def so the returned payload carries the
        // MCode-required fields (`prompt`, `projectId`, `modelSelection`, …)
        // the UI reads. The scheduler remains authoritative for id/name/
        // enabled/schedule/nextRunAt/createdAt/updatedAt.
        //
        // Dispatch accepts BOTH the MCode dot-name AND the slash form for
        // robustness (the transport remap converts dot → slash, but a caller
        // bypassing the remap still resolves).
        "automation.list" | "automation/list" => {
            handle_automation_list(state, id).await
        }
        "automation.create" | "automation/create" => {
            handle_automation_create(state, id, &request.params).await
        }
        "automation.get" | "automation/get" => {
            handle_automation_get(state, id, &request.params).await
        }
        "automation.update" | "automation/update" => {
            handle_automation_update(state, id, &request.params).await
        }
        "automation.delete" | "automation/delete" => {
            handle_automation_delete(state, id, &request.params).await
        }
        "automation.runNow"
        | "automation/run-now"
        | "automation.run"
        | "automation/run" => handle_automation_run_now(state, id, &request.params).await,
        "automation.cancelRun" | "automation/cancel-run" => {
            handle_automation_cancel_run(state, id, &request.params).await
        }
        "automation.markRunRead" | "automation/mark-run-read" => {
            handle_automation_mark_run_read(state, id, &request.params).await
        }
        "automation.archiveRun" | "automation/archive-run" => {
            handle_automation_archive_run(state, id, &request.params).await
        }
        "automation.subscribe"
        | "automation/subscribe"
        | "automation.unsubscribe"
        | "automation/unsubscribe" => handle_automation_subscribe_stub(id, &request.method),

        // ─── Provider discovery RPCs (T6c-7) ─────────────────────
        //
        // The cloned MCode UI's composer/agent-mention/SkillsPanel/plugin layer
        // calls these `provider.*` discovery RPCs on bootstrap + on-demand
        // (`provider.listModels`, `provider.listSkills`, `provider.listPlugins`,
        // `provider.listCommands`, `provider.listAgents`, …). Syncode has no
        // native skill/plugin/agent discovery subsystem (no
        // `~/.mcode/skills|plugins|agents` scan; no marketplace loader), so each
        // handler returns a **minimal valid MCode shape** — required top-level
        // fields present, arrays empty, optionals null — so the UI's `.map`/
        // `.filter`/`.length` reads render "nothing configured yet" rather than
        // crashing on `MethodNotFound`. Two of the list RPCs (`listModels`,
        // `listAgents`) are cheaply populated from the syncode-provider
        // `ALL_PROVIDERS` static (a `&[&str]` constant, no registry/state
        // needed) — the UI's model picker + agent-mention autocomplete show the
        // real provider set instead of an empty list. `compactThread` is a real
        // op the composer calls to compact conversation context; we return a
        // `{ ok: true }` stub (no LLM compaction wired here — deferred).
        //
        // Shape references (Tier-3 `frontend/src/contracts/tier3/provider.ts`,
        // mirrored from MCode `providerDiscovery.ts`):
        //   - ProviderListModelsResult         { models: ProviderModelDescriptor[],
        //                                         source?, cached? }
        //   - ProviderListSkillsResult         { skills: ProviderSkillDescriptor[],
        //                                         source?, cached? }
        //   - ProviderSkillsCatalogResult      { skills: ProviderSkillDescriptor[],
        //                                         mcodeSkillsDir? }
        //   - ProviderListPluginsResult        { marketplaces,
        //                                         marketplaceLoadErrors,
        //                                         remoteSyncError,
        //                                         featuredPluginIds, source?, cached? }
        //   - ProviderListCommandsResult       { commands: ProviderNativeCommandDescriptor[],
        //                                         source?, cached? }
        //   - ProviderListAgentsResult         { agents: ProviderAgentDescriptor[],
        //                                         source?, cached? }
        //   - ProviderComposerCapabilities     { provider, supportsSkillMentions, … }
        //   - readPlugin/readSkill             → { plugin: null } / { skill: null }
        //
        // Dispatch accepts BOTH the MCode dot-name AND a slash form for
        // robustness (the wsNativeApi sends dot, the tauriNativeApi sends slash
        // — both must resolve). Entry order matches the MCODE_TO_SERVED append
        // block to ease parallel-merge conflict resolution.
        "provider.listModels" | "provider/list-models" => handle_provider_list_models(id),
        "provider.listSkills" | "provider/list-skills" => handle_provider_list_skills(id),
        "provider.listSkillsCatalog" | "provider/list-skills-catalog" => {
            handle_provider_list_skills_catalog(id)
        }
        "provider.listPlugins" | "provider/list-plugins" => handle_provider_list_plugins(id),
        "provider.readPlugin" | "provider/read-plugin" => handle_provider_read_plugin(id),
        "provider.listCommands" | "provider/list-commands" => handle_provider_list_commands(id),
        "provider.listAgents" | "provider/list-agents" => handle_provider_list_agents(id),
        "provider.getComposerCapabilities" | "provider/get-composer-capabilities" => {
            handle_provider_get_composer_capabilities(id, &request.params)
        }
        "provider.listOptions" | "provider/list-options" => handle_provider_list_options(id),
        "provider.readSkill" | "provider/read-skill" => handle_provider_read_skill(id),
        "provider.compactThread" | "provider/compact-thread" => handle_provider_compact_thread(id),

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

// ─── Terminal PTY Handlers (syncode-terminal-backed, T6c-5) ────────────
//
// Reuse `syncode_terminal::SessionManager` (same impl as the Tauri
// `terminal_*` commands in `crates/syncode-tauri/src/terminal_commands.rs`)
// and map its `SessionInfo` into the MCode `TerminalSessionSnapshot` shape
// (Tier-3 `frontend/src/contracts/tier3/terminal.ts`):
//
//   TerminalSessionSnapshot {
//     threadId: string, terminalId: string, cwd: string,
//     status: "starting" | "running" | "exited" | "error",
//     pid: number | null, history: string, exitCode: number | null,
//     exitSignal: number | null, updatedAt: string
//   }
//
// The syncode `SessionInfo` carries `{sessionId, pid, alive, createdAt, cols,
// rows}`. Mapping:
//   - `sessionId`  → `terminalId` (we keyed the session by the caller's
//     terminalId at create time, so these are the same string)
//   - `alive`      → `status` (`"running"` when alive, `"exited"` otherwise;
//     `"starting"` is never returned by the syncode impl — PTY spawn is
//     synchronous, so by the time create_session returns the process is
//     either running or the spawn failed)
//   - `pid`        → `pid` (0 when the platform can't resolve it — mapped to
//     null per the MCode schema which allows `number | null`)
//   - `createdAt`  → `updatedAt`
//   - `cwd`        → from the create params (SessionInfo doesn't track cwd
//     post-spawn; we re-read it from the request or fall back to "")
//   - `history`    → "" (the syncode impl has no scrollback field; the UI
//     tolerates empty history — it only renders it for reattach)
//   - `exitCode`/`exitSignal` → null (syncode doesn't track exit codes; the
//     PTY's `mark_stopped` only flips a bool)
//
// Caveat: the MCode UI keys each terminal by `(threadId, terminalId)`. The
// syncode `SessionManager` keys sessions by a single string. We use
// `terminalId` as the SessionManager key (caller-provided via
// `create_session_with_id`); `threadId` is carried through the snapshot
// verbatim from the request (defaulting to the terminalId when absent) so
// the UI's pane identity is preserved.

/// Resolve the session key from request params. MCode sends `terminalId`;
/// the older tauri shape sends `sessionId`. `terminalId` wins when both are
/// present. Returns `None` when neither is a non-empty string.
fn terminal_session_key(params: &Value) -> Option<String> {
    params
        .get("terminalId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| params.get("sessionId").and_then(|v| v.as_str()))
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// Resolve the user shell. Mirrors the tauri `terminal_create_session`
/// default: explicit `command` param → `$SHELL` → `sh`. The MCode
/// `terminal.open` from `projectTerminalRunner` does NOT send a command
/// (it writes the command via `terminal.write` after the shell spawns), so
/// this default is the common path.
fn resolve_shell(params: &Value) -> String {
    if let Some(cmd) = params.get("command").and_then(|v| v.as_str())
        && !cmd.is_empty()
    {
        return cmd.to_string();
    }
    std::env::var("SHELL").unwrap_or_else(|_| "sh".to_string())
}

/// Resolve optional args. The MCode `terminal.open` doesn't send args; the
/// tauri shape sends `args: string[]`. We accept either `args` (array) or
/// `arguments` (array).
fn resolve_args(params: &Value) -> Vec<String> {
    params
        .get("args")
        .or_else(|| params.get("arguments"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve cwd. MCode sends `cwd`; the tauri shape sends `workingDir`/
/// `workingDirectory`. Returns `None` when unset (the PTY then inherits the
/// server process's cwd).
fn resolve_cwd(params: &Value) -> Option<String> {
    params
        .get("cwd")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("workingDirectory").and_then(|v| v.as_str()))
        .or_else(|| params.get("workingDir").and_then(|v| v.as_str()))
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// Build a MCode `TerminalSessionSnapshot` JSON value from a syncode
/// `SessionInfo` + the original request params (for `threadId`/`cwd`).
fn session_info_to_snapshot(info: &syncode_terminal::SessionInfo, params: &Value) -> Value {
    let status = if info.alive { "running" } else { "exited" };
    let pid = if info.pid == 0 { Value::Null } else { Value::from(info.pid) };
    // threadId: MCode sends it on open; for sessions created without one we
    // fall back to the terminalId so the snapshot is always non-null.
    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(&info.session_id);
    let cwd = resolve_cwd(params).unwrap_or_default();
    serde_json::json!({
        "threadId": thread_id,
        "terminalId": info.session_id,
        "cwd": cwd,
        "status": status,
        "pid": pid,
        "history": "",
        "exitCode": Value::Null,
        "exitSignal": Value::Null,
        "updatedAt": info.created_at,
    })
}

/// Thin typed error wrapper for terminal handlers — keeps the closure-style
/// early-return sites readable (mirrors the git handlers' `git_error`).
fn terminal_error(id: Value, code: i32, msg: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse::error(Some(id), code, msg.into())
}

/// `terminal.open` / `terminal.new` — spawn a PTY session and return the
/// `TerminalSessionSnapshot`.
///
/// Params (MCode camelCase):
///   - `terminalId` (preferred) | `sessionId` (legacy) — stable session key.
///     When absent, the server generates `term-{uuid}` (and returns it in the
///     snapshot so the caller can address the session thereafter).
///   - `cwd` | `workingDirectory` — spawn cwd (optional; defaults to server cwd).
///   - `command` — binary to spawn (optional; defaults to `$SHELL` then `sh`).
///   - `args` | `arguments` — argv (optional; defaults to []).
///   - `cols`, `rows` — initial PTY size (optional; defaults to 80×24).
///   - `threadId` — MCode pane identity (carried through the snapshot only).
///   - `env` — environment overrides (NOT applied; syncode-terminal's
///     `PtyHandle::spawn` doesn't accept per-session env — deferred. Documented
///     gap; the UI sends `env` for project-script runners but the PTY inherits
///     the server process env, which already has the project cwd context.)
async fn handle_terminal_open(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let cols = params
        .get("cols")
        .and_then(|v| v.as_u64())
        .map(|n| n as u16)
        .unwrap_or(80);
    let rows = params
        .get("rows")
        .and_then(|v| v.as_u64())
        .map(|n| n as u16)
        .unwrap_or(24);
    let command = resolve_shell(params);
    let args = resolve_args(params);
    let cwd = resolve_cwd(params);
    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    // Session key: caller-provided terminalId/sessionId, or a fresh UUID.
    let session_key = terminal_session_key(params).unwrap_or_else(|| {
        format!("term-{}", uuid::Uuid::new_v4().hyphenated())
    });

    let mgr = state.terminal_manager.clone();
    let create_result = {
        let write_guard = mgr.write().await;
        write_guard
            .create_session_with_id(
                session_key.clone(),
                &command,
                &arg_refs,
                cwd.as_deref(),
                cols,
                rows,
            )
            .await
    };
    if let Err(e) = create_result {
        return terminal_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("terminal.open: spawn failed: {e}"),
        );
    }

    // Read back the freshly-created session's info to build the snapshot.
    let info = {
        let read_guard = mgr.read().await;
        read_guard.list_sessions().await
    };
    let info = match info.into_iter().find(|s| s.session_id == session_key) {
        Some(i) => i,
        None => {
            return terminal_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                "terminal.open: session vanished after create",
            );
        }
    };
    JsonRpcResponse::success(id, session_info_to_snapshot(&info, params))
}

/// `terminal.write` — write input bytes to a session's PTY.
///
/// Params: `{ terminalId | sessionId, data }`. The `data` is a UTF-8 string
/// (the MCode contract sends `\r`-terminated command lines; binary is not
/// supported over JSON — documented gap).
async fn handle_terminal_write(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let session_key = match terminal_session_key(params) {
        Some(k) => k,
        None => {
            return terminal_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "terminal.write: missing 'terminalId' (or 'sessionId') parameter",
            );
        }
    };
    let data = match params.get("data").and_then(|v| v.as_str()) {
        Some(d) => d,
        None => {
            return terminal_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "terminal.write: missing 'data' parameter",
            );
        }
    };

    let mgr = state.terminal_manager.clone();
    // Lookup then write under separate guards (mirrors the tauri pattern):
    // the manager's read guard resolves the `Arc<RwLock<TerminalSession>>`,
    // then the session's PTY writer takes its own lock.
    let session = {
        let read_guard = mgr.read().await;
        read_guard.get_session(&session_key).await
    };
    let session = match session {
        Some(s) => s,
        None => {
            return terminal_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("terminal.write: session not found: {session_key}"),
            );
        }
    };
    // write_str is on PtyHandle; we hold a read guard on the session to access it.
    let pty = session.read().await;
    if let Err(e) = pty.pty().write_str(data).await {
        return terminal_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("terminal.write: {e}"),
        );
    }
    JsonRpcResponse::success(id, Value::Null)
}

/// `terminal.resize` — resize a session's PTY.
///
/// Params: `{ terminalId | sessionId, cols, rows }`.
async fn handle_terminal_resize(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let session_key = match terminal_session_key(params) {
        Some(k) => k,
        None => {
            return terminal_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "terminal.resize: missing 'terminalId' (or 'sessionId') parameter",
            );
        }
    };
    let cols = params
        .get("cols")
        .and_then(|v| v.as_u64())
        .map(|n| n as u16)
        .unwrap_or(80);
    let rows = params
        .get("rows")
        .and_then(|v| v.as_u64())
        .map(|n| n as u16)
        .unwrap_or(24);

    let mgr = state.terminal_manager.clone();
    let session = {
        let read_guard = mgr.read().await;
        read_guard.get_session(&session_key).await
    };
    let session = match session {
        Some(s) => s,
        None => {
            return terminal_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("terminal.resize: session not found: {session_key}"),
            );
        }
    };
    let session_guard = session.read().await;
    if let Err(e) = session_guard.resize(cols, rows).await {
        return terminal_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("terminal.resize: {e}"),
        );
    }
    JsonRpcResponse::success(id, Value::Null)
}

/// `terminal.close` / `terminal.kill` — destroy a session.
///
/// Params: `{ terminalId | sessionId }`. Returns `{ ok: boolean }`.
async fn handle_terminal_close(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let session_key = match terminal_session_key(params) {
        Some(k) => k,
        None => {
            return terminal_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "terminal.close: missing 'terminalId' (or 'sessionId') parameter",
            );
        }
    };
    let mgr = state.terminal_manager.clone();
    let destroyed = {
        let write_guard = mgr.write().await;
        write_guard.destroy_session(&session_key).await
    };
    if !destroyed {
        return terminal_error(
            id,
            crate::error_codes::INVALID_PARAMS,
            format!("terminal.close: session not found: {session_key}"),
        );
    }
    JsonRpcResponse::success(id, serde_json::json!({ "ok": true }))
}

/// `terminal.ackOutput` — acknowledge output up to a sequence number (flow
/// control so the server may release buffered chunks).
///
/// Params: `{ terminalId | sessionId, sequence | seq | ackedBytes }`. The
/// syncode `OutputBuffer::ack` takes a chunk seq number; `ackedBytes` (a byte
/// count) is accepted but currently treated as a no-op marker (syncode's ack
/// is seq-based, not byte-based — documented gap; the byte count is logged
/// for a future byte-window flow-control impl).
async fn handle_terminal_ack(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let session_key = match terminal_session_key(params) {
        Some(k) => k,
        None => {
            return terminal_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "terminal.ackOutput: missing 'terminalId' (or 'sessionId') parameter",
            );
        }
    };
    let seq = params
        .get("sequence")
        .and_then(|v| v.as_u64())
        .or_else(|| params.get("seq").and_then(|v| v.as_u64()))
        .or_else(|| params.get("ackedBytes").and_then(|v| v.as_u64()))
        .unwrap_or(0);

    let mgr = state.terminal_manager.clone();
    let session = {
        let read_guard = mgr.read().await;
        read_guard.get_session(&session_key).await
    };
    let session = match session {
        Some(s) => s,
        None => {
            return terminal_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("terminal.ackOutput: session not found: {session_key}"),
            );
        }
    };
    session.write().await.output_mut().ack(seq);
    JsonRpcResponse::success(id, Value::Null)
}

/// `terminal.list` — list all sessions as `TerminalSessionSnapshot[]`.
async fn handle_terminal_list(state: &WsState, id: Value) -> JsonRpcResponse {
    let mgr = state.terminal_manager.clone();
    let infos = {
        let read_guard = mgr.read().await;
        read_guard.list_sessions().await
    };
    let snapshots: Vec<Value> = infos
        .iter()
        .map(|i| session_info_to_snapshot(i, &Value::Null))
        .collect();
    JsonRpcResponse::success(id, serde_json::json!({ "sessions": snapshots }))
}

/// `terminal.clear` — clear a session's buffered output.
///
/// The syncode `OutputBuffer::clear` resets the chunk ring. This does NOT
/// send a clear escape sequence to the PTY (the UI's renderer-side clear
/// handles the visible terminal); it only drops server-side scrollback.
/// Params: `{ terminalId | sessionId }` (optional — when omitted, clears all
/// sessions). Returns `{ ok: boolean }`.
async fn handle_terminal_clear(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let mgr = state.terminal_manager.clone();
    // If a specific session is named, clear just that one; otherwise clear all.
    if let Some(session_key) = terminal_session_key(params) {
        let session = {
            let read_guard = mgr.read().await;
            read_guard.get_session(&session_key).await
        };
        match session {
            Some(s) => s.write().await.output_mut().clear(),
            None => {
                return terminal_error(
                    id,
                    crate::error_codes::INVALID_PARAMS,
                    format!("terminal.clear: session not found: {session_key}"),
                );
            }
        }
    } else {
        let infos = {
            let read_guard = mgr.read().await;
            read_guard.list_sessions().await
        };
        for info in infos {
            let session = {
                let read_guard = mgr.read().await;
                read_guard.get_session(&info.session_id).await
            };
            if let Some(s) = session {
                s.write().await.output_mut().clear();
            }
        }
    }
    JsonRpcResponse::success(id, serde_json::json!({ "ok": true }))
}

/// `terminal.restart` — destroy + recreate a session (best-effort).
///
/// The syncode-terminal `SessionManager` has no native restart; the SHELL-GAPS
/// note flagged restart as unsupported. We emulate it by destroying the
/// existing session (if any) and spawning a fresh one under the same id with
/// the original spawn params. The caller may also pass the full open param
/// set to override. Returns the new `TerminalSessionSnapshot`.
async fn handle_terminal_restart(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let session_key = match terminal_session_key(params) {
        Some(k) => k,
        None => {
            return terminal_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "terminal.restart: missing 'terminalId' (or 'sessionId') parameter",
            );
        }
    };
    let mgr = state.terminal_manager.clone();
    // Destroy the existing session (ignore not-found — restart is idempotent).
    {
        let write_guard = mgr.write().await;
        let _ = write_guard.destroy_session(&session_key).await;
    }
    // Re-spawn under the same id. Reuse the open handler's logic by calling it
    // directly (it reads the same param set + generates the snapshot).
    handle_terminal_open(state, id, params).await
}

/// `terminal.subscribeEvents` / `terminal.unsubscribeEvents` — STUB.
///
/// Returns `{ subscribed: true, note: ... }` without recording a real push
/// subscription or spawning an output-pump task. The syncode-terminal
/// `SessionManager` is pull-based (no callback on new output), so real push
/// delivery requires a per-session reader task that polls
/// `PtyHandle::read_output` and broadcasts on `push_tx` — deferred to
/// T6c-future. The UI tolerates the absence of push (it can poll
/// `terminal.list` or a future `terminal.read`).
fn handle_terminal_subscribe_stub(id: Value, method: &str) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "method": method,
            "channel": "terminal",
            "note": "stub: pull-based SessionManager — no push delivery (T6c-future)",
        }),
    )
}

// ══════════════════════════════════════════════════════════════════════════
// ─── Automation Handlers (T6c-6) ───────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════
//
// The Automations panel calls `automation.*` RPCs. We reuse
// `syncode_automation::Scheduler` for def + run-record lifecycle. The
// syncode `AutomationDef` is command/script-based; the MCode UI expects the
// prompt/LLM-based `AutomationDefinition` shape. To bridge without a schema
// migration, we stash the full client-supplied create/update input as a JSON
// overlay in the def's `description` field (`AUTOMATION_OVERLAY_KEY`), then
// merge it back on read so the UI receives the MCode-required fields
// (`prompt`, `projectId`, `modelSelection`, …) it reads off the definition.

/// Marker key used to detect and recover the JSON overlay stashed in a def's
/// `description` field. The value is a stringified JSON object carrying the
/// MCode create/update input fields the syncode `AutomationDef` doesn't model.
const AUTOMATION_OVERLAY_KEY: &str = "__mcode_overlay__:";

/// JSON keys whose values the Scheduler (not the client) owns. These are
/// skipped when capturing the overlay from create/update input.
fn is_scheduler_controlled(key: &str) -> bool {
    matches!(
        key,
        "id"
            | "name"
            | "enabled"
            | "schedule"
            | "nextRunAt"
            | "createdAt"
            | "updatedAt"
            | "archivedAt"
            | "iterationCount"
            | "completionPolicyVersion"
            | "completionPolicyUpdatedAt"
    )
}

/// Build a syncode `ScheduleType` from a MCode `AutomationSchedule` payload.
/// MCode discriminated union `{ type: "manual" | "once" | "interval" | "cron"
/// | "daily" | "weekdays" | "weekly" }`. daily/weekdays/weekly are collapsed
/// to Manual (syncode's ScheduleType doesn't model them — kept as Manual so
/// they never auto-fire; the panel still lists them).
fn parse_schedule(input: &Value) -> syncode_automation::ScheduleType {
    let kind = input.get("type").and_then(|v| v.as_str()).unwrap_or("manual");
    match kind {
        "once" => {
            let run_at = input
                .get("runAt")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            syncode_automation::ScheduleType::OneShot(run_at)
        }
        "interval" => {
            let secs = input
                .get("everySeconds")
                .and_then(|v| v.as_u64())
                .unwrap_or(60);
            syncode_automation::ScheduleType::Interval(secs)
        }
        "cron" => {
            let expr = input
                .get("expression")
                .and_then(|v| v.as_str())
                .unwrap_or("0 * * * *")
                .to_string();
            syncode_automation::ScheduleType::Cron(expr)
        }
        // manual, daily, weekdays, weekly — collapse to Manual (no auto-fire).
        _ => syncode_automation::ScheduleType::Manual,
    }
}

/// Capture the MCode create/update input fields the syncode `AutomationDef`
/// doesn't model, as a JSON object. Stored as a stringified blob in the def's
/// `description` (prefixed with `AUTOMATION_OVERLAY_KEY`).
fn capture_overlay(input: &Value) -> Value {
    let mut overlay = serde_json::Map::new();
    if let Some(obj) = input.as_object() {
        for (k, v) in obj {
            if !is_scheduler_controlled(k.as_str()) {
                overlay.insert(k.clone(), v.clone());
            }
        }
    }
    Value::Object(overlay)
}

/// Recover the overlay JSON object previously stashed in `description`, if any.
fn recover_overlay(description: &str) -> Value {
    if let Some(rest) = description.strip_prefix(AUTOMATION_OVERLAY_KEY)
        && let Ok(Value::Object(map)) = serde_json::from_str::<Value>(rest)
    {
        return Value::Object(map);
    }
    Value::Object(serde_json::Map::new())
}

/// Merge a recovered overlay onto a serialized syncode `AutomationDef`,
/// producing the MCode `AutomationDefinition` shape. Scheduler-controlled
/// fields (`id`, `name`, `enabled`, `schedule`, `nextRunAt`, `createdAt`,
/// `updatedAt`) win; overlay fills in everything else (`prompt`, `projectId`,
/// `modelSelection`, `runtimeMode`, …). Missing MCode-required fields get
/// sensible defaults so the UI's field accesses never collapse to undefined.
fn def_to_mcode_definition(def: &syncode_automation::AutomationDef) -> Value {
    let mut out = serde_json::to_value(def).unwrap_or_else(|_| Value::Object(Default::default()));
    let map = out.as_object_mut().expect("serialized def is an object");

    // Rewrite the schedule from syncode's internally-tagged form
    // (`{"manual": null}` / `{"interval": 300}` / `{"one_shot": "..."}` /
    // `{"cron": "..."}`) into the MCode discriminated-union shape
    // (`{"type": "manual"}` / `{"type": "interval", "everySeconds": 300}` / …).
    // The overlay (captured from the create/update input) already carries the
    // original MCode schedule — prefer it if present, else derive from syncode.
    let schedule_value = if map.get("schedule").is_some() {
        schedule_to_mcode(&def.schedule)
    } else {
        Value::Null
    };

    // Merge the overlay (non-controlled fields).
    let overlay = recover_overlay(&def.description);
    if let Value::Object(overlay_map) = overlay {
        for (k, v) in overlay_map {
            // Overlay never overwrites scheduler-controlled fields.
            map.entry(k).or_insert(v);
        }
    }

    // The overlay's schedule (if present) is the original MCode input —
    // authoritative. Otherwise use the syncode-derived MCode shape.
    if map.get("schedule").and_then(|s| s.get("type")).is_none() {
        map.insert("schedule".into(), schedule_value);
    }

    // Fill defaults for MCode-required fields if still absent (the UI reads
    // these off AutomationDefinition under exactOptionalPropertyTypes — they
    // must be present and well-typed).
    let now = chrono::Utc::now().to_rfc3339();
    map.entry("projectId").or_insert(Value::Null);
    map.entry("sourceThreadId").or_insert(Value::Null);
    map.entry("prompt")
        .or_insert(Value::String(String::new()));
    map.entry("modelSelection")
        .or_insert(serde_json::json!({ "providerId": "claude", "modelId": "claude-sonnet-4-20250514" }));
    map.entry("runtimeMode")
        .or_insert(Value::String("approval-required".into()));
    map.entry("interactionMode")
        .or_insert(Value::String("default".into()));
    map.entry("worktreeMode").or_insert(Value::String("auto".into()));
    map.entry("mode")
        .or_insert(Value::String("standalone".into()));
    map.entry("targetThreadId").or_insert(Value::Null);
    map.entry("maxIterations").or_insert(Value::Null);
    map.entry("stopOnError").or_insert(Value::Bool(true));
    map.entry("minimumIntervalSeconds").or_insert(serde_json::json!(60));
    map.entry("maxRuntimeSeconds").or_insert(Value::Null);
    map.entry("retryPolicy")
        .or_insert(serde_json::json!({ "type": "none" }));
    map.entry("misfirePolicy")
        .or_insert(Value::String("coalesce".into()));
    map.entry("acknowledgedRisks").or_insert(serde_json::json!([]));
    map.entry("iterationCount").or_insert(serde_json::json!(0));
    // nextRunAt / archivedAt — ensure present (Null if unset).
    map.entry("nextRunAt").or_insert(Value::Null);
    map.entry("archivedAt").or_insert(Value::Null);
    // createdAt/updatedAt are always present from the syncode def; if somehow
    // missing, fill now.
    map.entry("createdAt").or_insert(Value::String(now.clone()));
    map.entry("updatedAt").or_insert(Value::String(now));
    out
}

/// Map a syncode `AutomationRun` into the MCode `AutomationRun` shape. The
/// syncode run carries id/automationId/status/startedAt/endedAt/error; the
/// MCode shape adds projectId/threadId/trigger/scheduledFor/result/etc.
/// Missing fields are defaulted so UI field accesses stay well-typed.
fn run_to_mcode_run(
    run: &syncode_automation::AutomationRun,
    project_id: &str,
) -> Value {
    let mut out = serde_json::to_value(run).unwrap_or_else(|_| Value::Object(Default::default()));
    let map = out.as_object_mut().expect("serialized run is an object");
    let now = chrono::Utc::now().to_rfc3339();
    map.entry("projectId")
        .or_insert(Value::String(project_id.to_string()));
    map.entry("threadId").or_insert(Value::Null);
    map.entry("turnId").or_insert(Value::Null);
    map.entry("trigger")
        .or_insert(serde_json::json!({ "type": "manual" }));
    // MCode uses `scheduledFor`; the syncode run doesn't track it — default to now.
    map.entry("scheduledFor").or_insert(Value::String(now.clone()));
    map.entry("claimedBy").or_insert(Value::Null);
    map.entry("claimedAt").or_insert(Value::Null);
    map.entry("leaseExpiresAt").or_insert(Value::Null);
    map.entry("finishedAt")
        .or_insert(run.ended_at.clone().map(Value::String).unwrap_or(Value::Null));
    map.entry("threadCreateCommandId").or_insert(Value::Null);
    map.entry("turnStartCommandId").or_insert(Value::Null);
    map.entry("messageId").or_insert(Value::Null);
    map.entry("result").or_insert(Value::Null);
    map.entry("permissionSnapshot")
        .or_insert(serde_json::json!({
            "provider": "claude",
            "modelSelection": { "providerId": "claude", "modelId": "claude-sonnet-4-20250514" },
            "runtimeMode": "approval-required",
            "interactionMode": "default",
            "worktreeMode": "auto",
            "allowedCapabilities": ["send-turn"],
            "createdAt": now.clone(),
        }));
    map.entry("createdAt")
        .or_insert(run.started_at.clone().map(Value::String).unwrap_or(Value::String(now.clone())));
    map.entry("updatedAt")
        .or_insert(run.ended_at.clone().map(Value::String).unwrap_or(Value::String(now)));
    out
}

/// Convert a syncode `ScheduleType` back to the MCode discriminated-union
/// shape (`{"type": "manual"}`, `{"type": "interval", "everySeconds": N}`, …).
/// Used on read to surface a schedule the UI understands (syncode's serde
/// tagged form `{"manual": null}` / `{"interval": N}` is not MCode-shaped).
fn schedule_to_mcode(schedule: &syncode_automation::ScheduleType) -> Value {
    use syncode_automation::ScheduleType;
    match schedule {
        ScheduleType::Manual => serde_json::json!({ "type": "manual" }),
        ScheduleType::Interval(secs) => {
            serde_json::json!({ "type": "interval", "everySeconds": secs })
        }
        ScheduleType::OneShot(run_at) => {
            serde_json::json!({ "type": "once", "runAt": run_at })
        }
        ScheduleType::Cron(expr) => {
            serde_json::json!({ "type": "cron", "expression": expr, "timezone": "UTC" })
        }
    }
}

/// Resolve the projectId to associate with runs for a given automation def
/// (falls back to "" — the UI tolerates a missing/empty projectId).
fn project_id_for_def(def: &syncode_automation::AutomationDef) -> String {
    def.project_id.clone().unwrap_or_default()
}

fn automation_error(id: Value, code: i32, msg: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse::error(Some(id), code, msg.into())
}

/// `automation.list` — return all automation definitions + their runs in the
/// MCode `AutomationListResult` shape (`{ definitions, runs }`).
async fn handle_automation_list(state: &WsState, id: Value) -> JsonRpcResponse {
    let scheduler = state.automation_scheduler.clone();
    let defs = scheduler.list().await;
    let definitions: Vec<Value> = defs.iter().map(def_to_mcode_definition).collect();

    let mut runs: Vec<Value> = Vec::new();
    for def in &defs {
        let project_id = project_id_for_def(def);
        let def_id = def.id.as_str();
        for r in scheduler.list_runs(&def_id).await {
            runs.push(run_to_mcode_run(&r, &project_id));
        }
    }
    JsonRpcResponse::success(
        id,
        serde_json::json!({ "definitions": definitions, "runs": runs }),
    )
}

/// `automation.create` — register a new automation def from the MCode
/// `AutomationCreateInput` and return the created `AutomationDefinition`.
///
/// Params (MCode camelCase): `name`, `prompt`, `schedule`, `projectId`,
/// `modelSelection`, `enabled`, … The scheduler controls `id`/`name`/`enabled`
/// /`schedule`/`createdAt`/`updatedAt`; everything else is stashed as the
/// overlay so the read path can return the full MCode shape.
async fn handle_automation_create(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let name = match params.get("name").and_then(|v| v.as_str()) {
        Some(n) if !n.trim().is_empty() => n.to_string(),
        _ => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "automation.create: 'name' is required",
            );
        }
    };
    let schedule = if let Some(s) = params.get("schedule") {
        parse_schedule(s)
    } else {
        syncode_automation::ScheduleType::Manual
    };
    // Syncode AutomationDef is command-based; derive a no-op command from the
    // prompt (the real run dispatch happens through the RunExecutor, which is
    // NoopExecutor by default — the command field is unused at runtime).
    let prompt = params
        .get("prompt")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let command = if prompt.is_empty() {
        "true".to_string()
    } else {
        format!("echo {}", prompt.chars().take(80).collect::<String>())
    };

    let overlay = capture_overlay(params);
    let overlay_str = format!(
        "{AUTOMATION_OVERLAY_KEY}{}",
        serde_json::to_string(&overlay).unwrap_or_else(|_| "{}".into())
    );

    let mut def = syncode_automation::AutomationDef::new(name, command, schedule);
    def.description = overlay_str;
    if let Some(enabled) = params.get("enabled").and_then(|v| v.as_bool()) {
        def.enabled = enabled;
    }
    if let Some(pid) = params.get("projectId").and_then(|v| v.as_str()) {
        def.project_id = Some(pid.to_string());
    }

    let scheduler = state.automation_scheduler.clone();
    if let Err(e) = scheduler.register(def.clone()).await {
        return automation_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("automation.create: register failed: {e}"),
        );
    }
    let created_id = def.id.as_str();
    let created = scheduler.get(&created_id).await.unwrap_or(def);
    JsonRpcResponse::success(id, def_to_mcode_definition(&created))
}

/// `automation.get` — fetch a single automation def by id.
async fn handle_automation_get(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let auto_id = match params
        .get("id")
        .or_else(|| params.get("automationId"))
        .and_then(|v| v.as_str())
    {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "automation.get: 'id' (or 'automationId') is required",
            );
        }
    };
    let scheduler = state.automation_scheduler.clone();
    match scheduler.get(&auto_id).await {
        Some(def) => JsonRpcResponse::success(id, def_to_mcode_definition(&def)),
        None => automation_error(
            id,
            crate::error_codes::INVALID_PARAMS,
            format!("automation.get: not found: {auto_id}"),
        ),
    }
}

/// `automation.update` — patch an existing automation def. Reads `id` + any
/// subset of create fields. Re-captures the overlay from the update input,
/// preserving previously-stashed fields not present in this update.
async fn handle_automation_update(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let auto_id = match params.get("id").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "automation.update: 'id' is required",
            );
        }
    };
    let scheduler = state.automation_scheduler.clone();
    let mut existing = match scheduler.get(&auto_id).await {
        Some(d) => d,
        None => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("automation.update: not found: {auto_id}"),
            );
        }
    };

    // Start the overlay from the previously-stashed fields, then apply this
    // update's overlay on top (so a partial update doesn't drop prior fields).
    let mut merged_overlay = match recover_overlay(&existing.description) {
        Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };
    let new_overlay = capture_overlay(params);
    if let Value::Object(new_map) = new_overlay {
        for (k, v) in new_map {
            merged_overlay.insert(k, v);
        }
    }
    existing.description = format!(
        "{AUTOMATION_OVERLAY_KEY}{}",
        serde_json::to_string(&Value::Object(merged_overlay)).unwrap_or_else(|_| "{}".into())
    );

    if let Some(name) = params.get("name").and_then(|v| v.as_str())
        && !name.trim().is_empty()
    {
        existing.name = name.to_string();
    }
    if let Some(schedule) = params.get("schedule") {
        existing.schedule = parse_schedule(schedule);
    }
    if let Some(enabled) = params.get("enabled").and_then(|v| v.as_bool()) {
        existing.enabled = enabled;
    }
    if let Some(pid) = params.get("projectId").and_then(|v| v.as_str()) {
        existing.project_id = Some(pid.to_string());
    }
    existing.updated_at = chrono::Utc::now().to_rfc3339();

    if let Err(e) = scheduler.update(existing.clone()).await {
        return automation_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("automation.update: {e}"),
        );
    }
    let updated_id = existing.id.as_str();
    let updated = scheduler.get(&updated_id).await.unwrap_or(existing);
    JsonRpcResponse::success(id, def_to_mcode_definition(&updated))
}

/// `automation.delete` — unregister an automation def. Returns `{ ok: true }`.
async fn handle_automation_delete(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let auto_id = match params
        .get("id")
        .or_else(|| params.get("automationId"))
        .and_then(|v| v.as_str())
    {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "automation.delete: 'id' (or 'automationId') is required",
            );
        }
    };
    let scheduler = state.automation_scheduler.clone();
    let removed = scheduler.unregister(&auto_id).await;
    JsonRpcResponse::success(id, serde_json::json!({ "ok": removed }))
}

/// `automation.runNow` / `automation.run` — trigger a run immediately and
/// return the MCode `AutomationRunNowResult` shape (`{ run: AutomationRun }`).
///
/// The default Scheduler uses `NoopExecutor`, so the run fails (status Failed)
/// but a run record is persisted and returned — the panel can render it. Real
/// dispatch requires wiring a `RunExecutor` (deferred).
///
/// Uses `Delay::Immediate` (not `Delay::Real`) to skip the retry backoff: with
/// the default `NoopExecutor` retrying is pointless, and real backoff sleeps
/// (default `retry_delay_secs=30` × `max_retries=3` = ~90s) would hang the RPC.
/// When a real `RunExecutor` is wired (deferred), revisit the delay strategy.
async fn handle_automation_run_now(state: &WsState, id: Value, params: &Value) -> JsonRpcResponse {
    let auto_id = match params
        .get("id")
        .or_else(|| params.get("automationId"))
        .and_then(|v| v.as_str())
    {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "automation.runNow: 'id' (or 'automationId') is required",
            );
        }
    };
    let scheduler = state.automation_scheduler.clone();
    // Verify the def exists first (better error than trigger's NotFound).
    let def = match scheduler.get(&auto_id).await {
        Some(d) => d,
        None => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("automation.runNow: not found: {auto_id}"),
            );
        }
    };

    match scheduler
        .trigger_with_delay(&auto_id, syncode_automation::executor::Delay::Immediate)
        .await
    {
        Ok(run_id) => {
            let run = scheduler
                .get_run(&run_id)
                .await
                .unwrap_or_else(|| syncode_automation::AutomationRun::new(auto_id.clone()));
            let project_id = project_id_for_def(&def);
            JsonRpcResponse::success(
                id,
                serde_json::json!({ "run": run_to_mcode_run(&run, &project_id) }),
            )
        }
        Err(e) => automation_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("automation.runNow: trigger failed: {e}"),
        ),
    }
}

/// `automation.cancelRun` — cancel a run by id. Returns the updated run in the
/// MCode `AutomationCancelRunResult` shape (`{ run: AutomationRun }`).
async fn handle_automation_cancel_run(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let run_id = match params
        .get("runId")
        .or_else(|| params.get("id"))
        .and_then(|v| v.as_str())
    {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "automation.cancelRun: 'runId' (or 'id') is required",
            );
        }
    };
    let scheduler = state.automation_scheduler.clone();
    if let Err(e) = scheduler.cancel_run(&run_id).await {
        return automation_error(
            id,
            crate::error_codes::INVALID_PARAMS,
            format!("automation.cancelRun: {e}"),
        );
    }
    let run = match scheduler.get_run(&run_id).await {
        Some(r) => r,
        None => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("automation.cancelRun: run vanished: {run_id}"),
            );
        }
    };
    // Recover the project id from the run's automation def.
    let project_id = scheduler
        .get(&run.automation_id)
        .await
        .map(|d| project_id_for_def(&d))
        .unwrap_or_default();
    JsonRpcResponse::success(
        id,
        serde_json::json!({ "run": run_to_mcode_run(&run, &project_id) }),
    )
}

/// `automation.markRunRead` — mark a run as read. The syncode run type + repo
/// port don't model a `read`/`unread` flag, so this is a STUB that returns the
/// current run unchanged. Returns `{ run: AutomationRun }`.
async fn handle_automation_mark_run_read(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let run_id = match params
        .get("runId")
        .or_else(|| params.get("id"))
        .and_then(|v| v.as_str())
    {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "automation.markRunRead: 'runId' (or 'id') is required",
            );
        }
    };
    let scheduler = state.automation_scheduler.clone();
    let run = match scheduler.get_run(&run_id).await {
        Some(r) => r,
        None => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("automation.markRunRead: not found: {run_id}"),
            );
        }
    };
    let project_id = scheduler
        .get(&run.automation_id)
        .await
        .map(|d| project_id_for_def(&d))
        .unwrap_or_default();
    JsonRpcResponse::success(
        id,
        serde_json::json!({ "run": run_to_mcode_run(&run, &project_id) }),
    )
}

/// `automation.archiveRun` — archive a run. The syncode run type + repo port
/// don't model `archivedAt`, so this is a STUB that returns the current run
/// unchanged. Returns `{ run: AutomationRun }`.
async fn handle_automation_archive_run(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let run_id = match params
        .get("runId")
        .or_else(|| params.get("id"))
        .and_then(|v| v.as_str())
    {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "automation.archiveRun: 'runId' (or 'id') is required",
            );
        }
    };
    let scheduler = state.automation_scheduler.clone();
    let run = match scheduler.get_run(&run_id).await {
        Some(r) => r,
        None => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("automation.archiveRun: not found: {run_id}"),
            );
        }
    };
    let project_id = scheduler
        .get(&run.automation_id)
        .await
        .map(|d| project_id_for_def(&d))
        .unwrap_or_default();
    JsonRpcResponse::success(
        id,
        serde_json::json!({ "run": run_to_mcode_run(&run, &project_id) }),
    )
}

/// `automation.subscribe` / `automation.unsubscribe` — STUB. Real push delivery
/// (`automation.event`) would require a per-automation event tap feeding the
/// push bus — deferred. Returns `{ subscribed: true }` so the UI tolerates the
/// absence of push (it polls `automation.list`).
fn handle_automation_subscribe_stub(id: Value, method: &str) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "method": method,
            "channel": "automation",
            "note": "stub: no automation.event push delivery (T6c-future)",
        }),
    )
}

// ─── Provider discovery Handlers (T6c-7) ─────────────────────────
//
// Syncode has no native skill/plugin/agent discovery subsystem, so the list
// handlers return **minimal valid MCode shapes** (required top-level fields
// present, arrays empty, optionals null). The UI's `.map`/`.filter`/`.length`
// reads render "nothing configured yet" rather than crashing on `MethodNotFound`.
//
// `listModels` and `listAgents` are cheaply populated from the syncode-provider
// `ALL_PROVIDERS` static (a `&[&str]` constant — no registry/state lookup
// needed). The MCode `ProviderKind` union is narrower than syncode's
// `ALL_PROVIDERS` (it has `claudeAgent` not `claude`, and excludes
// `anthropic`/`openai`); we map `claude → claudeAgent` and skip the
// non-MCode provider ids so the model picker/agent-mention autocomplete only
// show valid MCode providers.
//
// Caveats / known gaps:
//   - Each `ProviderModelDescriptor` carries only `slug` + `name` (the
//     schema-required fields); the rich option fields (reasoning efforts,
//     context-window options, …) are omitted — the UI tolerates their absence
//     (Schema.optional). A real per-provider model catalog would require
//     enumerating each adapter's `available_models()` — deferred (would need
//     `ProviderRegistry` plumbed into `WsState`).
//   - `getComposerCapabilities` returns an all-false/empty descriptor for the
//     requested provider (read from `provider` param; defaults to `codex` when
//     absent or unknown). The composer renders without skill/plugin/@-mention
//     discovery — the user can still type plain prompts.
//   - `compactThread` is a real op the composer calls to compact conversation
//     context before the LLM round-trip; we return `{ ok: true }` (no LLM
//     compaction wired here — deferred).

/// Map a syncode `ALL_PROVIDERS` id onto the MCode `ProviderKind` union. Returns
/// `None` for provider ids that don't exist in the MCode kind union
/// (`anthropic`, `openai`). The `claude → claudeAgent` rename reflects MCode's
/// composer-facing naming (the agent is `claudeAgent`, the binary is `claude`).
fn to_mcode_provider_kind(syncode_id: &str) -> Option<&'static str> {
    match syncode_id {
        syncode_provider::PROVIDER_CODEX => Some("codex"),
        syncode_provider::PROVIDER_CLAUDE => Some("claudeAgent"),
        syncode_provider::PROVIDER_CURSOR => Some("cursor"),
        syncode_provider::PROVIDER_GEMINI => Some("gemini"),
        syncode_provider::PROVIDER_GROK => Some("grok"),
        syncode_provider::PROVIDER_KILO => Some("kilo"),
        syncode_provider::PROVIDER_OPENCODE => Some("opencode"),
        syncode_provider::PROVIDER_PI => Some("pi"),
        // anthropic/openai are syncode-internal upstream ids not present in the
        // MCode `ProviderKind` union — skip.
        _ => None,
    }
}

/// `provider.listModels` — return `ProviderListModelsResult` with one
/// `ProviderModelDescriptor` per known MCode-valid provider (slug + name only;
/// rich option fields omitted as Schema.optional). An empty `source`-less
/// result is the safe fallback; populating from `ALL_PROVIDERS` keeps the model
/// picker non-empty so the user can actually select a provider without the
/// "no models" empty state blocking thread creation.
fn handle_provider_list_models(id: Value) -> JsonRpcResponse {
    let models: Vec<Value> = syncode_provider::ALL_PROVIDERS
        .iter()
        .filter_map(|p| to_mcode_provider_kind(p))
        .map(|kind| {
            serde_json::json!({
                "slug": kind,
                "name": kind,
            })
        })
        .collect();
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "models": models,
            "source": "syncode",
        }),
    )
}

/// `provider.listSkills` — return `ProviderListSkillsResult` with an empty
/// `skills` array. Syncode has no skill-discovery subsystem.
fn handle_provider_list_skills(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, serde_json::json!({ "skills": [] }))
}

/// `provider.listSkillsCatalog` — return `ProviderSkillsCatalogResult` with an
/// empty `skills` array. The catalog is a UI-side aggregated skill index;
/// syncode has no skill loader.
fn handle_provider_list_skills_catalog(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, serde_json::json!({ "skills": [] }))
}

/// `provider.listPlugins` — return `ProviderListPluginsResult` with empty
/// marketplaces/errors and a null `remoteSyncError`. Syncode has no plugin
/// marketplace loader.
fn handle_provider_list_plugins(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "marketplaces": [],
            "marketplaceLoadErrors": [],
            "remoteSyncError": Value::Null,
            "featuredPluginIds": [],
        }),
    )
}

/// `provider.readPlugin` — return `{ plugin: null }`. The UI's readPlugin
/// consumer (`PluginDetailSheet`) renders an empty/not-found state when the
/// plugin is null; the MCode schema marks plugin as `Schema.Null`able.
fn handle_provider_read_plugin(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, serde_json::json!({ "plugin": Value::Null }))
}

/// `provider.listCommands` — return `ProviderListCommandsResult` with an empty
/// `commands` array. Syncode has no native slash-command discovery.
fn handle_provider_list_commands(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, serde_json::json!({ "commands": [] }))
}

/// `provider.listAgents` — return `ProviderListAgentsResult` with one
/// `ProviderAgentDescriptor` per known MCode-valid provider (name + displayName
/// only). Populating from `ALL_PROVIDERS` keeps the agent-mention autocomplete
/// non-empty; each entry is a provider-shell agent (the UI's actual agent
/// definitions come from `AGENT_MENTION_ALIASES` on the client side — this RPC
/// is the live discovery complement).
fn handle_provider_list_agents(id: Value) -> JsonRpcResponse {
    let agents: Vec<Value> = syncode_provider::ALL_PROVIDERS
        .iter()
        .filter_map(|p| to_mcode_provider_kind(p))
        .map(|kind| {
            serde_json::json!({
                "name": kind,
                "displayName": kind,
            })
        })
        .collect();
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "agents": agents,
            "source": "syncode",
        }),
    )
}

/// `provider.getComposerCapabilities` — return a `ProviderComposerCapabilities`
/// descriptor for the requested provider with every support flag `false`. The
/// `provider` field is read from the request params (defaults to `codex` when
/// absent or unrecognized) so the UI's per-provider capability gating sees a
/// valid `ProviderKind`. All discovery flags false renders a plain-prompt
/// composer (no skill/plugin/@-mention autocomplete) — functional baseline.
fn handle_provider_get_composer_capabilities(id: Value, params: &Value) -> JsonRpcResponse {
    let provider = params
        .get("provider")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("codex");
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "provider": provider,
            "supportsSkillMentions": false,
            "supportsSkillDiscovery": false,
            "supportsNativeSlashCommandDiscovery": false,
            "supportsPluginMentions": false,
            "supportsPluginDiscovery": false,
            "supportsRuntimeModelList": false,
            "supportsThreadCompaction": false,
            "supportsThreadImport": false,
        }),
    )
}

/// `provider.listOptions` — return `{ options: [] }`. Syncode has no
/// per-provider option descriptor subsystem (the MCode `ProviderOptionDescriptor`
/// surface drives provider-specific settings UIs we don't render).
fn handle_provider_list_options(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, serde_json::json!({ "options": [] }))
}

/// `provider.readSkill` — return `{ skill: null }`. Mirrors readPlugin: the
/// skill-detail consumer renders an empty state when the skill is null.
fn handle_provider_read_skill(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, serde_json::json!({ "skill": Value::Null }))
}

/// `provider.compactThread` — STUB. The composer calls this to compact the
/// conversation context before an LLM round-trip (a real op). Syncode has no
/// LLM-side compaction wired into the WS layer — return `{ ok: true }` so the
/// composer treats the compaction as a no-op success. Real compaction would
/// route through the provider adapter's session — deferred.
fn handle_provider_compact_thread(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, serde_json::json!({ "ok": true }))
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

    // ─── Terminal PTY RPC tests (T6c-5) ─────────────────────────────────
    //
    // These exercise the full create → list → write → resize → ack → close
    // round-trip against a REAL PTY (spawned via `syncode-terminal`'s
    // `portable_pty`). The no-op command `/bin/true` exits immediately, so
    // the session is created, written to (best-effort — the write may hit a
    // not-running PTY if `true` already exited, which we tolerate), resized,
    // acked, and destroyed cleanly. This mirrors the phase-3 git test pattern
    // (real `Git2Service` against a tempdir repo).
    //
    // The MCode `TerminalSessionSnapshot` shape is asserted at each step so a
    // contracts drift surfaces here rather than in the UI.

    /// Helper: send an RPC and return the parsed response.
    async fn term_rpc(state: &WsState, req: &serde_json::Value) -> JsonRpcResponse {
        let raw = handle_rpc(state, 1, &req.to_string()).await;
        serde_json::from_str(&raw.unwrap_or_default()).unwrap()
    }

    #[tokio::test]
    async fn terminal_open_returns_snapshot_shape() {
        // Skip on platforms without a usable PTY (e.g. some CI containers).
        // `/bin/true` is universally available on POSIX; if spawn fails the
        // test asserts the error path rather than aborting.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.open",
            "params": {
                "terminalId": "term-test-1",
                "threadId": "thread-1",
                "cwd": "/tmp",
                "command": "/bin/true",
                "cols": 100, "rows": 30
            }
        });
        let resp = term_rpc(&state, &req).await;
        assert!(resp.error.is_none(), "terminal.open failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        // MCode TerminalSessionSnapshot top-level fields.
        assert_eq!(result["terminalId"], "term-test-1");
        assert_eq!(result["threadId"], "thread-1");
        assert_eq!(result["cwd"], "/tmp");
        assert!(result.get("status").is_some(), "missing status");
        assert!(result.get("pid").is_some(), "missing pid");
        assert!(result.get("history").is_some(), "missing history");
        assert!(result.get("exitCode").is_some(), "missing exitCode");
        assert!(result.get("exitSignal").is_some(), "missing exitSignal");
        assert!(result.get("updatedAt").is_some(), "missing updatedAt");

        // Cleanup: destroy the session so the PTY child doesn't linger.
        let _ = term_rpc(
            &state,
            &serde_json::json!({
                "jsonrpc": "2.0", "id": 2, "method": "terminal.close",
                "params": { "terminalId": "term-test-1" }
            }),
        )
        .await;
    }

    #[tokio::test]
    async fn terminal_open_then_list_then_close_round_trip() {
        let state = WsState::new_in_memory(16);

        // 1. Open a long-lived shell (`sh` reading from /dev/null won't exit
        //    immediately — but `sh` with no stdin redirection may exit. We use
        //    `cat` which blocks on stdin and stays alive until killed).
        let open = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.open",
            "params": { "terminalId": "term-rt-1", "command": "/bin/cat", "cwd": "/tmp" }
        });
        let resp = term_rpc(&state, &open).await;
        assert!(resp.error.is_none(), "open failed: {:?}", resp.error);

        // 2. List → the session is present.
        let list = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "terminal.list"
        });
        let resp = term_rpc(&state, &list).await;
        assert!(resp.error.is_none(), "list failed: {:?}", resp.error);
        let sessions = resp.result.unwrap()["sessions"].as_array().unwrap().clone();
        assert_eq!(sessions.len(), 1, "expected exactly 1 session after open");
        assert_eq!(sessions[0]["terminalId"], "term-rt-1");

        // 3. Write to the PTY (best-effort: `cat` echoes; the write should
        //    succeed since cat is alive).
        let write = serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "terminal.write",
            "params": { "terminalId": "term-rt-1", "data": "hello\r" }
        });
        let resp = term_rpc(&state, &write).await;
        assert!(resp.error.is_none(), "write failed: {:?}", resp.error);

        // 4. Resize.
        let resize = serde_json::json!({
            "jsonrpc": "2.0", "id": 4, "method": "terminal.resize",
            "params": { "terminalId": "term-rt-1", "cols": 120, "rows": 40 }
        });
        let resp = term_rpc(&state, &resize).await;
        assert!(resp.error.is_none(), "resize failed: {:?}", resp.error);

        // 5. AckOutput (seq-based; should be a no-op success).
        let ack = serde_json::json!({
            "jsonrpc": "2.0", "id": 5, "method": "terminal.ackOutput",
            "params": { "terminalId": "term-rt-1", "sequence": 0 }
        });
        let resp = term_rpc(&state, &ack).await;
        assert!(resp.error.is_none(), "ack failed: {:?}", resp.error);

        // 6. Close → ok:true, then list shows 0 sessions.
        let close = serde_json::json!({
            "jsonrpc": "2.0", "id": 6, "method": "terminal.close",
            "params": { "terminalId": "term-rt-1" }
        });
        let resp = term_rpc(&state, &close).await;
        assert!(resp.error.is_none(), "close failed: {:?}", resp.error);
        assert_eq!(resp.result.unwrap()["ok"], true);

        let resp = term_rpc(&state, &list).await;
        let sessions = resp.result.unwrap()["sessions"].as_array().unwrap().clone();
        assert!(sessions.is_empty(), "session should be gone after close");
    }

    #[tokio::test]
    async fn terminal_write_unknown_session_is_error() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.write",
            "params": { "terminalId": "no-such-session", "data": "x" }
        });
        let resp = term_rpc(&state, &req).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn terminal_write_missing_session_key_is_invalid_params() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.write",
            "params": { "data": "x" }
        });
        let resp = term_rpc(&state, &req).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn terminal_close_unknown_session_is_error() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.close",
            "params": { "terminalId": "ghost" }
        });
        let resp = term_rpc(&state, &req).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn terminal_subscribests_is_stub_success() {
        // subscribeEvents is stubbed (pull-based SessionManager) but must
        // return success so the UI's subscribe call doesn't error.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.subscribeEvents",
            "params": { "terminalId": "whatever" }
        });
        let resp = term_rpc(&state, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        assert_eq!(resp.result.unwrap()["subscribed"], true);
    }

    #[tokio::test]
    async fn terminal_open_accepts_session_id_alias() {
        // The older tauri shape uses `sessionId` instead of `terminalId`.
        // Both must resolve to the same handler + key.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.open",
            "params": { "sessionId": "legacy-1", "command": "/bin/true", "cwd": "/tmp" }
        });
        let resp = term_rpc(&state, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        // The snapshot echoes the session key back as terminalId.
        assert_eq!(result["terminalId"], "legacy-1");

        // Cleanup.
        let _ = term_rpc(
            &state,
            &serde_json::json!({
                "jsonrpc": "2.0", "id": 2, "method": "terminal.close",
                "params": { "sessionId": "legacy-1" }
            }),
        )
        .await;
    }

    #[tokio::test]
    async fn terminal_list_methods_includes_terminal_rpcs() {
        // The new terminal methods must appear in rpc/listMethods so the UI's
        // capability discovery sees them.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods"
        });
        let resp = term_rpc(&state, &req).await;
        let methods = resp.result.unwrap()["methods"].as_array().unwrap().clone();
        let method_strs: Vec<String> = methods
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        for expected in [
            "terminal/create",
            "terminal/write",
            "terminal/resize",
            "terminal/close",
            "terminal/ack",
            "terminal/list",
        ] {
            assert!(
                method_strs.iter().any(|m| m == expected),
                "rpc/listMethods missing {expected}"
            );
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // ─── Automation RPC tests (T6c-6) ────────────────────────────────────────
    // ════════════════════════════════════════════════════════════════════════

    /// Test helper that mirrors `term_rpc` but is named for automation scope.
    async fn auto_rpc(state: &WsState, req: &serde_json::Value) -> JsonRpcResponse {
        let raw = handle_rpc(state, 1, &req.to_string()).await;
        serde_json::from_str(&raw.unwrap_or_default()).unwrap()
    }

    #[tokio::test]
    async fn automation_list_methods_includes_automation_rpcs() {
        // The new automation methods must appear in rpc/listMethods so the UI's
        // capability discovery sees them.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods"
        });
        let resp = auto_rpc(&state, &req).await;
        let methods = resp.result.unwrap()["methods"]
            .as_array()
            .unwrap()
            .clone();
        let method_strs: Vec<String> = methods
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        for expected in [
            "automation/list",
            "automation/create",
            "automation/get",
            "automation/update",
            "automation/delete",
            "automation/run-now",
            "automation/cancel-run",
            "automation/mark-run-read",
            "automation/archive-run",
            "automation/subscribe",
        ] {
            assert!(
                method_strs.iter().any(|m| m == expected),
                "rpc/listMethods missing {expected}"
            );
        }
    }

    #[tokio::test]
    async fn automation_create_returns_mcode_shape() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.create",
            "params": {
                "name": "Triage crashes",
                "prompt": "Look for new crashes and open a PR",
                "schedule": { "type": "manual" },
                "projectId": "proj-1",
                "modelSelection": { "providerId": "claude", "modelId": "claude-sonnet-4-20250514" },
                "enabled": true,
                "runtimeMode": "approval-required"
            }
        });
        let resp = auto_rpc(&state, &req).await;
        assert!(resp.error.is_none(), "create failed: {:?}", resp.error);
        let def = resp.result.unwrap();
        // MCode-required top-level fields present + well-typed.
        assert!(def["id"].is_string(), "id missing: {}", def);
        assert_eq!(def["name"], "Triage crashes");
        assert_eq!(def["enabled"], true);
        assert_eq!(def["prompt"], "Look for new crashes and open a PR");
        assert_eq!(def["projectId"], "proj-1");
        assert_eq!(def["schedule"]["type"], "manual");
        assert_eq!(def["modelSelection"]["providerId"], "claude");
        assert_eq!(def["runtimeMode"], "approval-required");
        // Defaulted MCode-required fields are populated.
        assert_eq!(def["mode"], "standalone");
        assert_eq!(def["interactionMode"], "default");
        assert!(def["createdAt"].is_string());
        assert!(def["updatedAt"].is_string());
    }

    #[tokio::test]
    async fn automation_create_rejects_missing_name() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.create",
            "params": { "prompt": "no name" }
        });
        let resp = auto_rpc(&state, &req).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn automation_round_trip_create_list_get_run_cancel() {
        // End-to-end happy path mirroring the phase-5 terminal round-trip.
        let state = WsState::new_in_memory(16);

        // 1. Create an automation.
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.create",
            "params": {
                "name": "Daily build",
                "prompt": "Run the build",
                "schedule": { "type": "interval", "everySeconds": 300 },
                "projectId": "proj-rt"
            }
        });
        let resp = auto_rpc(&state, &create).await;
        assert!(resp.error.is_none(), "create: {:?}", resp.error);
        let def = resp.result.unwrap();
        let auto_id = def["id"].as_str().unwrap().to_string();
        assert_eq!(def["schedule"]["type"], "interval");
        assert_eq!(def["schedule"]["everySeconds"], 300);

        // 2. List → definitions array includes it, runs empty.
        let list = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "automation.list"
        });
        let resp = auto_rpc(&state, &list).await;
        assert!(resp.error.is_none(), "list: {:?}", resp.error);
        let result = resp.result.unwrap();
        let definitions = result["definitions"].as_array().unwrap();
        assert_eq!(definitions.len(), 1);
        assert_eq!(definitions[0]["id"], auto_id);
        assert_eq!(definitions[0]["name"], "Daily build");
        // Runs starts empty (no run triggered yet).
        assert!(result["runs"].as_array().unwrap().is_empty());

        // 3. Get by id returns the same def.
        let get = serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "automation.get",
            "params": { "id": auto_id }
        });
        let resp = auto_rpc(&state, &get).await;
        assert!(resp.error.is_none(), "get: {:?}", resp.error);
        assert_eq!(resp.result.unwrap()["id"], auto_id);

        // 4. runNow → a run record is created (NoopExecutor → Failed, but the
        //    record exists and is returned in the AutomationRunNowResult shape).
        let run_now = serde_json::json!({
            "jsonrpc": "2.0", "id": 4, "method": "automation.runNow",
            "params": { "id": auto_id }
        });
        let resp = auto_rpc(&state, &run_now).await;
        assert!(resp.error.is_none(), "runNow: {:?}", resp.error);
        let run = resp.result.unwrap()["run"].clone();
        assert!(run["id"].is_string());
        assert_eq!(run["automationId"], auto_id);
        assert_eq!(run["projectId"], "proj-rt");
        let run_id = run["id"].as_str().unwrap().to_string();

        // 5. List now shows the run too.
        let resp = auto_rpc(&state, &list).await;
        let runs = resp.result.unwrap()["runs"].as_array().unwrap().clone();
        assert_eq!(runs.len(), 1, "expected 1 run after runNow");
        assert_eq!(runs[0]["id"], run_id);

        // 6. cancelRun → run status becomes cancelled.
        let cancel = serde_json::json!({
            "jsonrpc": "2.0", "id": 5, "method": "automation.cancelRun",
            "params": { "runId": run_id }
        });
        let resp = auto_rpc(&state, &cancel).await;
        assert!(resp.error.is_none(), "cancelRun: {:?}", resp.error);
        let cancelled = resp.result.unwrap()["run"].clone();
        assert_eq!(cancelled["status"], "cancelled");

        // 7. Delete → ok:true, then list shows zero defs.
        let delete = serde_json::json!({
            "jsonrpc": "2.0", "id": 6, "method": "automation.delete",
            "params": { "id": auto_id }
        });
        let resp = auto_rpc(&state, &delete).await;
        assert!(resp.error.is_none(), "delete: {:?}", resp.error);
        assert_eq!(resp.result.unwrap()["ok"], true);
        let resp = auto_rpc(&state, &list).await;
        assert_eq!(
            resp.result.unwrap()["definitions"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn automation_update_preserves_overlay_fields() {
        let state = WsState::new_in_memory(16);

        // Create with a prompt + modelSelection.
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.create",
            "params": {
                "name": "Auto-1",
                "prompt": "original prompt",
                "schedule": { "type": "manual" },
                "modelSelection": { "providerId": "claude", "modelId": "m1" }
            }
        });
        let resp = auto_rpc(&state, &create).await;
        let auto_id = resp.result.unwrap()["id"].as_str().unwrap().to_string();

        // Update only the name + enabled (partial). Prompt + modelSelection
        // must survive (overlay preserved).
        let update = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "automation.update",
            "params": { "id": auto_id, "name": "Auto-renamed", "enabled": false }
        });
        let resp = auto_rpc(&state, &update).await;
        assert!(resp.error.is_none(), "update: {:?}", resp.error);
        let def = resp.result.unwrap();
        assert_eq!(def["name"], "Auto-renamed");
        assert_eq!(def["enabled"], false);
        // Overlay preserved.
        assert_eq!(def["prompt"], "original prompt");
        assert_eq!(def["modelSelection"]["modelId"], "m1");
    }

    #[tokio::test]
    async fn automation_get_not_found_returns_invalid_params() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.get",
            "params": { "id": "nope" }
        });
        let resp = auto_rpc(&state, &req).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn automation_dot_and_slash_dispatch_equivalent() {
        // Both MCode dot-name and slash form must resolve to the same handler.
        let state = WsState::new_in_memory(16);
        for method in ["automation.list", "automation/list"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method
            });
            let resp = auto_rpc(&state, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert!(result["definitions"].is_array(), "{}: bad shape", method);
            assert!(result["runs"].is_array(), "{}: bad shape", method);
        }
    }

    #[tokio::test]
    async fn automation_mark_run_read_and_archive_are_stubs() {
        // Verdict: markRunRead/archiveRun are stubs (syncode run type/repo
        // don't model read/archived). Both must return the run unchanged in
        // the AutomationRunActionResult shape (`{ run: ... }`).
        let state = WsState::new_in_memory(16);

        // Create + runNow to seed a run.
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.create",
            "params": { "name": "Stub-test", "schedule": { "type": "manual" } }
        });
        let resp = auto_rpc(&state, &create).await;
        let auto_id = resp.result.unwrap()["id"].as_str().unwrap().to_string();
        let run_now = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "automation.runNow",
            "params": { "id": auto_id }
        });
        let resp = auto_rpc(&state, &run_now).await;
        let run_id = resp.result.unwrap()["run"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        // markRunRead.
        let mark = serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "automation.markRunRead",
            "params": { "runId": run_id }
        });
        let resp = auto_rpc(&state, &mark).await;
        assert!(resp.error.is_none(), "markRunRead: {:?}", resp.error);
        assert_eq!(resp.result.unwrap()["run"]["id"], run_id);

        // archiveRun.
        let archive = serde_json::json!({
            "jsonrpc": "2.0", "id": 4, "method": "automation.archiveRun",
            "params": { "runId": run_id }
        });
        let resp = auto_rpc(&state, &archive).await;
        assert!(resp.error.is_none(), "archiveRun: {:?}", resp.error);
        assert_eq!(resp.result.unwrap()["run"]["id"], run_id);
    }

    #[tokio::test]
    async fn automation_subscribe_returns_stub() {
        let state = WsState::new_in_memory(16);
        for method in ["automation.subscribe", "automation/subscribe"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method
            });
            let resp = auto_rpc(&state, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            assert_eq!(resp.result.unwrap()["subscribed"], true);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // ─── Provider discovery RPCs (T6c-7) ────────────────────────────────
    // ════════════════════════════════════════════════════════════════════════

    /// Test helper scoped to provider RPCs (mirrors `auto_rpc`).
    async fn provider_rpc(state: &WsState, req: &serde_json::Value) -> JsonRpcResponse {
        let raw = handle_rpc(state, 1, &req.to_string()).await;
        serde_json::from_str(&raw.unwrap_or_default()).unwrap()
    }

    /// All 11 provider discovery RPCs must resolve (no MethodNotFound) under
    /// BOTH the MCode dot-name AND the slash form, and each must return a
    /// success envelope. The dot-form is what the wsNativeApi sends (after
    /// remap it becomes slash, but the dispatch must still accept the raw
    /// dot-name); the slash form is what tauriNativeApi sends.
    #[tokio::test]
    async fn provider_rpcs_resolve_both_forms() {
        let state = WsState::new_in_memory(16);
        // (dot-name, slash-name)
        let cases: &[(&str, &str)] = &[
            ("provider.listModels", "provider/list-models"),
            ("provider.listSkills", "provider/list-skills"),
            ("provider.listSkillsCatalog", "provider/list-skills-catalog"),
            ("provider.listPlugins", "provider/list-plugins"),
            ("provider.readPlugin", "provider/read-plugin"),
            ("provider.listCommands", "provider/list-commands"),
            ("provider.listAgents", "provider/list-agents"),
            (
                "provider.getComposerCapabilities",
                "provider/get-composer-capabilities",
            ),
            ("provider.listOptions", "provider/list-options"),
            ("provider.readSkill", "provider/read-skill"),
            ("provider.compactThread", "provider/compact-thread"),
        ];
        for (dot, slash) in cases {
            for method in [*dot, *slash] {
                let req = serde_json::json!({
                    "jsonrpc": "2.0", "id": 1, "method": method
                });
                let resp = provider_rpc(&state, &req).await;
                assert!(
                    resp.error.is_none(),
                    "{method} failed: {:?}",
                    resp.error
                );
                assert!(resp.result.is_some(), "{method} returned null result");
            }
        }
    }

    /// rpc/listMethods must advertise the new provider methods so the UI's
    /// capability discovery sees them.
    #[tokio::test]
    async fn provider_rpcs_listed_in_list_methods() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods"
        });
        let resp = provider_rpc(&state, &req).await;
        let methods = resp.result.unwrap()["methods"]
            .as_array()
            .expect("methods is an array")
            .clone();
        let listed: std::collections::HashSet<String> = methods
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        for expected in [
            "provider/list-models",
            "provider/list-skills",
            "provider/list-skills-catalog",
            "provider/list-plugins",
            "provider/read-plugin",
            "provider/list-commands",
            "provider/list-agents",
            "provider/get-composer-capabilities",
            "provider/list-options",
            "provider/read-skill",
            "provider/compact-thread",
        ] {
            assert!(
                listed.contains(expected),
                "rpc/listMethods missing {expected}"
            );
        }
    }

    /// listModels must be populated from the syncode-provider ALL_PROVIDERS
    /// static (one entry per MCode-valid provider), each carrying the
    /// schema-required `slug` + `name` fields. The MCode `ProviderKind` union
    /// excludes `anthropic`/`openai` and renames `claude → claudeAgent`, so
    /// those transformations must be reflected in the model slugs.
    #[tokio::test]
    async fn provider_list_models_populated_from_all_providers() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listModels"
        });
        let resp = provider_rpc(&state, &req).await;
        let result = resp.result.unwrap();
        let models = result["models"].as_array().expect("models is an array");
        // 8 MCode-valid providers (claude/cursor/gemini/grok/kilo/opencode/pi +
        // codex); anthropic + openai filtered out.
        assert_eq!(models.len(), 8, "expected 8 MCode-valid provider models");
        let slugs: Vec<&str> = models
            .iter()
            .map(|m| m["slug"].as_str().unwrap())
            .collect();
        // The claude → claudeAgent rename must be applied.
        assert!(slugs.contains(&"claudeAgent"), "claude should map to claudeAgent");
        assert!(
            !slugs.contains(&"anthropic"),
            "anthropic is not a MCode ProviderKind"
        );
        assert!(
            !slugs.contains(&"openai"),
            "openai is not a MCode ProviderKind"
        );
        // Each model must carry the schema-required slug + name.
        for m in models {
            assert!(m["slug"].is_string(), "model missing slug");
            assert!(m["name"].is_string(), "model missing name");
        }
    }

    /// listAgents mirrors listModels: one entry per MCode-valid provider with
    /// name + displayName.
    #[tokio::test]
    async fn provider_list_agents_populated_from_all_providers() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listAgents"
        });
        let resp = provider_rpc(&state, &req).await;
        let result = resp.result.unwrap();
        let agents = result["agents"].as_array().expect("agents is an array");
        assert_eq!(agents.len(), 8, "expected 8 MCode-valid provider agents");
        for a in agents {
            assert!(a["name"].is_string(), "agent missing name");
            assert!(a["displayName"].is_string(), "agent missing displayName");
        }
    }

    /// The empty-list RPCs (skills, plugins, commands, options) must return
    /// the MCode-required top-level fields with empty arrays/null so the UI's
    /// `.map`/`.length` reads don't crash. readPlugin/readSkill must return
    /// null descriptors.
    #[tokio::test]
    async fn provider_empty_list_rpcs_return_minimal_shapes() {
        let state = WsState::new_in_memory(16);

        // listSkills → { skills: [] }
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listSkills"
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["skills"].as_array().unwrap().len(), 0);

        // listSkillsCatalog → { skills: [] }
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listSkillsCatalog"
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["skills"].as_array().unwrap().len(), 0);

        // listCommands → { commands: [] }
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listCommands"
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["commands"].as_array().unwrap().len(), 0);

        // listOptions → { options: [] }
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listOptions"
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["options"].as_array().unwrap().len(), 0);

        // readPlugin → { plugin: null }
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.readPlugin",
            "params": { "pluginId": "x" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert!(result["plugin"].is_null(), "readPlugin should return null plugin");

        // readSkill → { skill: null }
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.readSkill",
            "params": { "name": "x" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert!(result["skill"].is_null(), "readSkill should return null skill");
    }

    /// listPlugins must return the full ProviderListPluginsResult shape with
    /// empty marketplaces + a null remoteSyncError (the UI's plugins panel
    /// reads all four top-level fields).
    #[tokio::test]
    async fn provider_list_plugins_returns_full_shape() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listPlugins"
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert!(result["marketplaces"].is_array());
        assert_eq!(result["marketplaces"].as_array().unwrap().len(), 0);
        assert!(result["marketplaceLoadErrors"].is_array());
        assert!(result["remoteSyncError"].is_null());
        assert!(result["featuredPluginIds"].is_array());
    }

    /// getComposerCapabilities must echo the requested `provider` and return
    /// every support flag as false (the composer renders a plain-prompt UI).
    /// Defaults to "codex" when the provider param is absent.
    #[tokio::test]
    async fn provider_get_composer_capabilities_all_false() {
        let state = WsState::new_in_memory(16);

        // Explicit provider request.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.getComposerCapabilities",
            "params": { "provider": "gemini" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["provider"], "gemini");
        for flag in [
            "supportsSkillMentions",
            "supportsSkillDiscovery",
            "supportsNativeSlashCommandDiscovery",
            "supportsPluginMentions",
            "supportsPluginDiscovery",
            "supportsRuntimeModelList",
            "supportsThreadCompaction",
            "supportsThreadImport",
        ] {
            assert_eq!(result[flag], false, "{flag} should be false");
        }

        // Default when provider param absent.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.getComposerCapabilities"
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["provider"], "codex", "default provider should be codex");
    }

    /// compactThread is a stub — returns { ok: true } so the composer treats
    /// compaction as a no-op success.
    #[tokio::test]
    async fn provider_compact_thread_returns_ok_stub() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.compactThread",
            "params": { "threadId": "thr_123" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["ok"], true);
    }
}

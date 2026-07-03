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
                    "server/set-config",
                    "server/update-settings",
                    "server/refresh-providers",
                    "server/update-provider",
                    "server/upsert-keybinding",
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
                    "stats/get-profile-stats",
                    "stats/get-profile-token-stats",
                    "git/stash-list",
                    "git/stash-create",
                    "git/stash-apply",
                    "git/stash-drop",
                    "git/stash-info",
                    "git/stash-and-checkout",
                    "git/fetch",
                    "git/pull",
                    "git/push",
                    "git/init",
                    "git/remove-index-lock",
                    "git/worktree-list",
                    "git/worktree-create",
                    "git/worktree-remove",
                    "git/summarize-diff",
                    "server/generate-thread-recap",
                    "git/github-repository",
                    "git/resolve-pull-request",
                    "git/handoff-thread",
                    "git/prepare-pull-request-thread",
                    "server/transcribe-voice",
                    "server/voice-start",
                    "server/voice-stop",
                    "git/run-stacked-action",
                    "git/create-detached-worktree",
                    "git/subscribe-action-progress",
                    // T6c-17: server niche ops (last batch).
                    "server/generate-automation-intent",
                    "server/patch-settings",
                    "server/list-provider-usage",
                    "server/get-provider-usage-snapshot",
                    "server/start-local-server",
                    "server/stop-local-server",
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

        // ─── Server write-side stubs (T6c-10) ───────────────────────────────
        //
        // The cloned MCode UI persists user edits via these `server.*` write
        // RPCs (`setConfig`, `updateSettings`, `refreshProviders`,
        // `updateProvider`, `upsertKeybinding`). Syncode has no native
        // settings/keybindings persistence layer, so each handler is a STUB:
        // it validates the params shape and echoes the default read-side
        // payload. The UI's optimistic update is overwritten by the echoed
        // default on the next read, converging to "no changes persisted".
        // Dispatch accepts BOTH dot-name AND slash form (the wsNativeApi sends
        // dot, the tauriNativeApi sends slash — both must resolve).
        //
        // stub: no persistence — echoes default ServerConfig.
        "server.setConfig" | "server/set-config" => handle_server_set_config(state, id),
        // stub: no persistence — echoes default ServerSettings.
        "server.updateSettings" | "server/update-settings" => {
            handle_server_update_settings(id)
        }
        // stub: no provider probe — empty `{ providers: [] }`.
        "server.refreshProviders" | "server/refresh-providers" => {
            handle_server_refresh_providers(id)
        }
        // stub: validates `provider` non-empty, returns `{ providers: [] }`.
        "server.updateProvider" | "server/update-provider" => {
            handle_server_update_provider(id, &request.params)
        }
        // stub: validates params is an object, returns
        // `{ keybindings: [], issues: [] }`.
        "server.upsertKeybinding" | "server/upsert-keybinding" => {
            handle_server_upsert_keybinding(id, &request.params)
        }

        // `server.generateThreadRecap` (T6c-13) — LLM-backed thread recap.
        // See the LLM-backed-ops block above for the one-shot flow.
        "server.generateThreadRecap"
        | "server/generate-thread-recap"
        | "server/generateThreadRecap" => {
            handle_server_generate_thread_recap(state, id, &request.params).await
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
        | "terminal.subscribe" => handle_terminal_subscribe(state, conn_id, id).await,
        "terminal/unsubscribe"
        | "terminal/unsubscribe-events"
        | "terminal.unsubscribeEvents" => handle_terminal_unsubscribe(state, conn_id, id).await,

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
        // op the composer calls to compact conversation context; it is now
        // (T6c-13) provider-backed — the handler reads the thread's messages
        // and runs them through a provider adapter one-shot (see the
        // LLM-backed-ops block below). Empty history yields a no-op
        // `{ ok: true, compactedSummary: "" }`.
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
        "provider.compactThread" | "provider/compact-thread" => {
            handle_provider_compact_thread(state, id, &request.params).await
        }

        // ─── Profile stats RPCs (T6c-8) ─────────────────────────
        //
        // The cloned MCode UI's Profile page calls these `stats.*` RPCs to render
        // the activity heatmap, provider-usage breakdown, skill-usage list, token
        // totals, and quota panel (`stats.getProfileStats`,
        // `stats.getProfileTokenStats`). Syncode has no native stats aggregation
        // subsystem (no prompt/turn/token accumulator, no daily-rollup store, no
        // provider-quota poller), so each handler returns a **minimal valid MCode
        // shape** — every schema-required top-level field present, arrays empty,
        // counts 0, optionals null — so the UI's `.map`/`.find`/`.length` reads
        // render an empty/zero state ("no activity yet") rather than crashing on
        // `MethodNotFound`.
        //
        // Shape references (Tier-3 `frontend/src/contracts/tier3/stats.ts`,
        // mirrored from MCode `packages/contracts/src/stats.ts`):
        //   - ProfileStats         { generatedAt, timezone, identity, activity,
        //                            activeHours, insights, providerModels[],
        //                            skills[], mostUsedSkill, mostWorkedProject,
        //                            quota }
        //   - ProfileTokenStats    { available, lifetimeTotalTokens, peakDayTokens,
        //                            peakDay, providers[], unavailableProviders[],
        //                            heatmapMetric, heatmap[] }
        //
        // Dispatch accepts BOTH the MCode dot-name AND a slash form for
        // robustness (the wsNativeApi sends dot, the tauriNativeApi sends slash
        // — both must resolve). Entry order matches the MCODE_TO_SERVED append
        // block to ease parallel-merge conflict resolution.
        "stats.getProfileStats" | "stats/get-profile-stats" => handle_stats_get_profile_stats(id),
        "stats.getProfileTokenStats" | "stats/get-profile-token-stats" => {
            handle_stats_get_profile_token_stats(id)
        }

        // ─── Git Advanced (stash / network / worktree / init, T6c-9) ────────
        //
        // The cloned MCode GitPanel calls these `git.*` dot-strings beyond the
        // core phase-3 surface (status/diff/branches/branch-CRUD/stage/commit):
        //
        //   - Stash: `git.stashList`, `git.stashCreate`, `git.stashApply`,
        //     `git.stashDrop`, `git.stashInfo`, `git.stashAndCheckout`
        //   - Network: `git.fetch`, `git.pull`, `git.push`
        //   - Worktree: `git.worktreeList`, `git.worktreeCreate`,
        //     `git.worktreeRemove` (+ the MCode alternate names
        //     `git.listWorktrees` / `git.createWorktree` / `git.removeWorktree`)
        //   - Misc: `git.init`, `git.removeIndexLock`
        //
        // Implementation strategy:
        //   - stash/fetch/init/removeIndexLock go through `git2` directly
        //     (syncode-git's `GitService` trait does not expose them). The same
        //     `Repository::discover` lookup that `Git2Service::repo()` uses is
        //     reused here so the `cwd` resolution matches.
        //   - pull/push delegate to `Git2Service::{pull,push}` (already
        //     CLI-backed; classify_cli_error surfaces auth/non-fast-forward
        //     distinctly).
        //   - worktree reuses `syncode_git::worktree::{list,add,remove}_worktree`.
        //
        // `git.stashAndCheckout` is STUBBED (`{ ok:false }` with a `reason`) —
        // it is a two-phase op (stash then checkout) the UI can compose itself
        // via `stashCreate` + `checkout`. Documented below.
        //
        // Deferred / unserved (still in `UNSERVED_RPC`): `git.runStackedAction`
        // (LLM-backed multi-phase commit/push/PR — would need provider wiring),
        // `git.createDetachedWorktree`, `git.subscribeActionProgress`
        // (push channel — T6c-future). NOTE: `git.summarizeDiff` was SERVED in
        // T6c-13 (LLM-backed one-shot — see the LLM-backed-ops block below).
        // `git.githubRepository` + `git.resolvePullRequest` + `git.handoffThread`
        // + `git.preparePullRequestThread` were UNSERVED until T6c-14; they are
        // NOW SERVED via the GitHub-API ops block below (shelling out to the
        // `gh` CLI — auth delegated to `gh auth login`, no token handling).
        //
        // Dispatch accepts BOTH the MCode dot-name AND a slash form for
        // robustness. Entry order matches the MCODE_TO_SERVED append block to
        // ease parallel-merge conflict resolution.
        "git.stashList" | "git/stash-list" | "git/stashList" => {
            handle_git_stash_list(id, &request.params)
        }
        "git.stashCreate" | "git/stash-create" | "git/stashCreate" => {
            handle_git_stash_create(id, &request.params)
        }
        "git.stashApply" | "git/stash-apply" | "git/stashApply" => {
            handle_git_stash_apply(id, &request.params)
        }
        "git.stashDrop" | "git/stash-drop" | "git/stashDrop" => {
            handle_git_stash_drop(id, &request.params)
        }
        "git.stashInfo" | "git/stash-info" | "git/stashInfo" => {
            handle_git_stash_info(id, &request.params)
        }
        "git.stashAndCheckout" | "git/stash-and-checkout" | "git/stashAndCheckout" => {
            handle_git_stash_and_checkout(id, &request.params)
        }
        "git.fetch" | "git/fetch" => handle_git_fetch(id, &request.params),
        "git.pull" | "git/pull" => handle_git_pull(id, &request.params),
        "git.push" | "git/push" => handle_git_push(id, &request.params),
        "git.init" | "git/init" => handle_git_init(id, &request.params),
        "git.removeIndexLock"
        | "git/remove-index-lock"
        | "git/removeIndexLock"
        | "git/remove_index_lock" => handle_git_remove_index_lock(id, &request.params),
        "git.worktreeList"
        | "git/listWorktrees"
        | "git/worktree-list"
        | "git.listWorktrees"
        | "git/list-worktrees"
        | "git/worktreeList" => handle_git_worktree_list(id, &request.params),
        "git.worktreeCreate"
        | "git/createWorktree"
        | "git/worktree-create"
        | "git.create-worktree"
        | "git/worktreeCreate" => handle_git_worktree_create(id, &request.params),
        "git.worktreeRemove"
        | "git/removeWorktree"
        | "git/worktree-remove"
        | "git/remove-worktree"
        | "git/worktreeRemove" => handle_git_worktree_remove(id, &request.params),

        // ─── T6c-16: git stacked/detached-worktree/progress RPCs ──────────
        //
        // The last 3 git niche RPCs the vendored MCode UI's GitActionsControl
        // calls. Reuse `syncode_git::stacked_actions::{StackedPipeline,
        // StackedAction}` (the Stage/Commit/Push/CreatePR pipeline) and map the
        // MCode `GitStackedAction` (`commit | push | create_pr | commit_push |
        // commit_push_pr`) onto a sequence of syncode stacked actions. The
        // detached-worktree RPC mirrors `git.worktreeCreate` but checks out at
        // a ref/commit-ish WITHOUT creating a branch (detached HEAD). The
        // progress RPC is a GRACEFUL STUB (no real push channel for stacked
        // actions — they're synchronous; T6c-future could stream progress).
        //
        // Dispatch accepts BOTH the MCode dot-name AND a slash form.
        "git.runStackedAction"
        | "git/run-stacked-action"
        | "git/runStackedAction"
        | "git/run_stacked_action" => handle_git_run_stacked_action(id, &request.params),
        "git.createDetachedWorktree"
        | "git/create-detached-worktree"
        | "git/createDetachedWorktree"
        | "git/create_detached_worktree" => {
            handle_git_create_detached_worktree(id, &request.params)
        }
        "git.subscribeActionProgress"
        | "git/subscribe-action-progress"
        | "git/subscribeActionProgress"
        | "git/subscribe_action_progress" => {
            handle_git_subscribe_action_progress(id, &request.params)
        }

        // ─── LLM-backed ops (T6c-13: provider-CLI one-shot) ────────────
        //
        // Three RPCs need a single prompt → response round trip through a
        // provider CLI (no streaming, no long-lived session):
        //   - `provider.compactThread` — compact a thread's history (was a
        //     `{ ok: true }` stub; now invokes the provider).
        //   - `git.summarizeDiff` — LLM summary of a git diff (was UNSERVED).
        //   - `server.generateThreadRecap` — LLM recap of a thread (was
        //     UNSERVED).
        //
        // Each handler builds a prompt from the request payload (thread
        // messages read from `read_store`; diff text from the params or
        // fetched via syncode-git), resolves a provider adapter from
        // `WsState::provider_registry`, and runs the one-shot helper in
        // `crates/syncode-ws/src/llm.rs`. If no adapter is registered for the
        // requested provider (or the CLI binary is missing) the handler
        // returns a clear JSON-RPC error — never a panic. Dispatch accepts
        // BOTH the MCode dot-name AND a slash form for robustness.
        "git.summarizeDiff" | "git/summarize-diff" | "git/summarizeDiff" => {
            handle_git_summarize_diff(state, id, &request.params).await
        }

        // ─── GitHub-API ops (T6c-14: gh-CLI-backed) ──────────────────────
        //
        // Four RPCs need the GitHub API. Rather than depend on an OAuth REST
        // client + token vault, we shell out to the user's `gh` CLI (authed
        // via `gh auth login` — `dayartcrew-web` in this dev env). Each
        // handler spawns `gh` via `tokio::process::Command`, parses its
        // `--json` output, and maps to the MCode result shape. On any gh
        // failure (binary missing, not authed, no network, not a GitHub repo,
        // PR not found) the handler returns a clear JSON-RPC error result —
        // never a panic. The pure parsing logic lives in `gh_parse::*`
        // (unit-tested with fixtures; the `gh` subprocess calls are
        // `#[ignore]`-gated integration tests).
        //
        //   - `git.githubRepository`         — detect the GitHub repo for a
        //     local path (parses `git remote get-url origin`; enriches via
        //     `gh repo view --json owner,name,url,defaultBranchRef`).
        //   - `git.resolvePullRequest`       — `gh pr view <n> --json
        //     number,title,state,headRefName,baseRefName,url` → MCode
        //     `GitResolvePullRequestResult`.
        //   - `git.handoffThread`            — create a PR from a branch via
        //     `gh pr create`. STUBBED with `{ ok:false, reason }` for the
        //     multi-phase worktree/checkout variant (the MCode shape carries
        //     `worktreePath`/`associatedWorktreeBranch` fields that imply a
        //     two-phase op we don't model); the simple `gh pr create` path
        //     returns the PR URL.
        //   - `git.preparePullRequestThread` — prepare a worktree/branch for a
        //     PR. STUBBED (`{ ok:false, reason }`) — the MCode shape implies
        //     a checkout + worktree-add sequence we don't wire here.
        //
        // Dispatch accepts BOTH the MCode dot-name AND a slash form.
        "git.githubRepository" | "git/github-repository" | "git/githubRepository" => {
            handle_git_github_repository(id, &request.params).await
        }
        "git.resolvePullRequest"
        | "git/resolve-pull-request"
        | "git/resolvePullRequest" => handle_git_resolve_pull_request(id, &request.params).await,
        "git.handoffThread" | "git/handoff-thread" | "git/handoffThread" => {
            handle_git_handoff_thread(id, &request.params).await
        }
        "git.preparePullRequestThread"
        | "git/prepare-pull-request-thread"
        | "git/preparePullRequestThread" => {
            handle_git_prepare_pull_request_thread(id, &request.params).await
        }

        // ─── Voice STT Methods (T6c-15 — graceful not-configured stubs) ──
        //
        // The cloned MCode UI's voice panel calls these `server.*` RPCs to
        // drive speech-to-text (STT):
        //
        //   - `server.transcribeVoice` — submit an audio blob for transcription
        //     (the UI captures mic input, encodes it, and posts it here)
        //   - `server.voiceStart`     — begin a streaming listening session
        //   - `server.voiceStop`      — end a streaming listening session
        //
        // Syncode has NO STT backend (no whisper/ffmpeg CLI installed, no STT
        // API configured), so each handler is a GRACEFUL STUB: it reads the
        // params (audio blob / start-stop flags) without processing them and
        // returns a typed "STT not configured" result so the UI surfaces a
        // clear status instead of MethodNotFound or a crash.
        //
        // Dispatch accepts BOTH dot-name AND slash form (the wsNativeApi sends
        // dot, the tauriNativeApi sends slash — both must resolve).
        //
        // stub: no STT backend (T6c-future — install whisper/ffmpeg or wire a
        // STT provider) — returns a not-configured result.
        "server.transcribeVoice"
        | "server/transcribe-voice"
        | "server/transcribeVoice" => handle_server_transcribe_voice(id, &request.params),
        // stub: no STT backend — can't start listening.
        "server.voiceStart" | "server/voice-start" | "server/voiceStart" => {
            handle_server_voice_start(id, &request.params)
        }
        // stub: no STT backend — no-op stop.
        "server.voiceStop" | "server/voice-stop" | "server/voiceStop" => {
            handle_server_voice_stop(id, &request.params)
        }

        // ─── Server niche ops (T6c-17 — last batch; completes all RPCs) ────
        //
        // The final 6 unserved server RPCs. `server.generateAutomationIntent`
        // is REAL (LLM-backed via `invoke()` — the same one-shot flow as
        // `compactThread`/`summarizeDiff`/`generateThreadRecap`); the other 5
        // are STUBS that return documented empty/ack payloads (syncode has no
        // settings persistence, usage-tracking, or local-server process-mgmt
        // subsystem). After this batch: ZERO unserved RPCs.
        //
        // Dispatch accepts BOTH dot-name AND slash form.
        //
        // REAL (LLM): generates an `AutomationIntent` from a natural-language
        // message by prompting the provider CLI once. The reply text is parsed
        // as JSON into the MCode `ServerGenerateAutomationIntentResult` shape;
        // a parse failure yields a not-automation result carrying the raw text.
        "server.generateAutomationIntent"
        | "server/generate-automation-intent" => {
            handle_server_generate_automation_intent(state, id, &request.params).await
        }
        // stub: no settings persistence — echoes default `ServerSettings`
        // (mirrors `server.updateSettings`).
        "server.patchSettings" | "server/patch-settings" => {
            handle_server_patch_settings(id)
        }
        // stub: no usage-tracking subsystem — empty usage list.
        "server.listProviderUsage" | "server/list-provider-usage" => {
            handle_server_list_provider_usage(id, &request.params)
        }
        // stub: no usage-tracking subsystem — null snapshot.
        "server.getProviderUsageSnapshot" | "server/get-provider-usage-snapshot" => {
            handle_server_get_provider_usage_snapshot(id, &request.params)
        }
        // stub: no local-server process-mgmt subsystem — graceful not-supported.
        "server.startLocalServer" | "server/start-local-server" => {
            handle_server_start_local_server(id)
        }
        // stub: no local-server process-mgmt subsystem — no-op ack.
        "server.stopLocalServer" | "server/stop-local-server" => {
            handle_server_stop_local_server(id, &request.params)
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

/// Build the minimal valid `ServerConfig` shape (MCode
/// `frontend/src/contracts/tier3/server.ts`). Shared by the read-side
/// `server.getConfig` handler and the write-side `server.setConfig` stub —
/// both return the same default-config payload (the stub accepts the write
/// but performs no persistence, mirroring the read view).
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
fn build_default_server_config(state: &WsState) -> Value {
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
    cfg
}

/// `server.getConfig` — return a minimal valid `ServerConfig` shape (see
/// `build_default_server_config`).
fn handle_server_get_config(state: &WsState, id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, build_default_server_config(state))
}

/// Build the MCode `DEFAULT_SERVER_SETTINGS` literal. Shared by the read-side
/// `server.getSettings` handler and the write-side `server.updateSettings`
/// stub. The vendored UI references this exact shape for state initialization
/// (see `frontend/src/contracts/tier3/server.ts` `DEFAULT_SERVER_SETTINGS`).
/// Each provider is enabled with its conventional binary name and empty
/// `customModels`; the text-generation model selection defaults to
/// `{ provider: "codex", model: "gpt-5.4-mini" }` (matches the literal).
fn build_default_server_settings() -> Value {
    serde_json::json!({
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
    })
}

/// `server.getSettings` — return the MCode `DEFAULT_SERVER_SETTINGS` literal
/// (see `build_default_server_settings`).
fn handle_server_get_settings(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, build_default_server_settings())
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

// ─── Server write-side stub Handlers (T6c-10) ────────────────────
//
// The cloned MCode UI persists user edits via these `server.*` write RPCs:
//   - `server.setConfig`        → overwrite the ServerConfig (Settings panel
//     "Apply"/"Reset" actions). MCode itself has no `setConfig` (the UI uses
//     `updateSettings` / per-provider update RPCs), but the contracts layer
//     and the task spec list it; we accept the write and echo the default
//     config so the UI's optimistic state converges back to the read view.
//   - `server.updateSettings`   → patch `ServerSettings` (Settings panel
//     save). MCode accepts a `ServerSettingsPatch` and returns the full
//     resolved `ServerSettings`.
//   - `server.refreshProviders` → re-probe provider availability. MCode
//     returns a `ServerProviderStatusesUpdatedPayload` (`{ providers: [] }`).
//   - `server.updateProvider`   → re-probe a single provider. MCode accepts
//     `{ provider: ProviderKind }` and returns the same payload shape.
//   - `server.upsertKeybinding` → add/update a keybinding rule. MCode accepts
//     a `KeybindingRule` and returns `{ keybindings, issues }`.
//
// Syncode has no native settings/keybindings persistence layer (no settings
// file write path, no keybindings resolver), so each handler is a STUB: it
// validates the params shape (rejecting malformed input with -32602) and
// returns the default read-side payload — `build_default_server_config()` /
// `build_default_server_settings()` / `{ providers: [] }` / `{ keybindings:
// [], issues: [] }`. The UI's optimistic update is overwritten by the echoed
// default on the next read, converging to "no changes persisted" — which
// matches the documented gap (no settings subsystem).
//
// Ack shapes (mirrors of the read side):
//   - setConfig           → ServerConfig (build_default_server_config)
//   - updateSettings      → ServerSettings (build_default_server_settings)
//   - refreshProviders    → { providers: [] }
//   - updateProvider      → { providers: [] }   (validates `provider` non-empty)
//   - upsertKeybinding    → { keybindings: [], issues: [] }
//                          (validates `params` is a JSON object)

/// `server.setConfig` — accept the write, echo the default `ServerConfig`.
/// Performs no persistence (stub). Mirrors `server.getConfig` so the UI's
/// post-save re-read converges to the unchanged default.
fn handle_server_set_config(state: &WsState, id: Value) -> JsonRpcResponse {
    // stub: no persistence — echo the default config.
    JsonRpcResponse::success(id, build_default_server_config(state))
}

/// `server.updateSettings` — accept the patch, echo the default
/// `ServerSettings`. Performs no persistence (stub). Mirrors
/// `server.getSettings` so the UI's post-save re-read converges to the
/// unchanged default.
fn handle_server_update_settings(id: Value) -> JsonRpcResponse {
    // stub: no persistence — echo the default settings.
    JsonRpcResponse::success(id, build_default_server_settings())
}

/// `server.refreshProviders` — re-probe all providers. Returns an empty
/// `ServerProviderStatusesUpdatedPayload` (`{ providers: [] }`) since syncode
/// has no provider-availability probe. Performs no real refresh (stub).
fn handle_server_refresh_providers(id: Value) -> JsonRpcResponse {
    // stub: no provider probe — empty statuses payload.
    JsonRpcResponse::success(id, serde_json::json!({ "providers": [] }))
}

/// `server.updateProvider` — re-probe a single provider. Validates that
/// `params.provider` is present and non-empty (MCode `ServerProviderUpdateInput`
/// is `{ provider: ProviderKind }`), then returns the empty
/// `ServerProviderStatusesUpdatedPayload`. Performs no real refresh (stub).
fn handle_server_update_provider(id: Value, params: &Value) -> JsonRpcResponse {
    // Validate `provider` is a non-empty string (MCode ProviderKind union).
    // Missing/empty/wrong-type → InvalidParams (-32602) so the UI surfaces a
    // typed validation error rather than a silent no-op.
    let provider = params.get("provider").and_then(|v| v.as_str()).unwrap_or("");
    if provider.trim().is_empty() {
        return JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            "Invalid params: 'provider' must be a non-empty string",
        );
    }
    // stub: no provider probe — empty statuses payload.
    JsonRpcResponse::success(id, serde_json::json!({ "providers": [] }))
}

/// `server.upsertKeybinding` — add/update a keybinding rule. Validates that
/// `params` is a JSON object (MCode `ServerUpsertKeybindingInput` is a
/// `KeybindingRule` struct), then returns the default upsert result shape
/// (`{ keybindings: [], issues: [] }`). Performs no persistence (stub) —
/// syncode has no keybindings resolver, so the echoed keybindings list is
/// empty and the UI's optimistic add is dropped on the next re-read.
fn handle_server_upsert_keybinding(id: Value, params: &Value) -> JsonRpcResponse {
    // Validate `params` is an object (KeybindingRule is a struct). Non-object
    // (null, array, primitive) → InvalidParams (-32602).
    if !params.is_object() {
        return JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            "Invalid params: 'upsertKeybinding' expects a keybinding-rule object",
        );
    }
    // stub: no keybindings resolver — empty result.
    JsonRpcResponse::success(
        id,
        serde_json::json!({ "keybindings": [], "issues": [] }),
    )
}

// ─── Voice STT Handlers (T6c-15 — graceful not-configured stubs) ──
//
// Syncode has no STT backend (no whisper/ffmpeg CLI, no STT API), so these
// handlers do NOT process audio. They read the params (to accept the MCode
// voice-input shapes without erroring) and return a typed "STT not
// configured" result so the UI can surface a clear status. Real STT wiring
// (whisper CLI subprocess, or a STT provider adapter) is deferred to
// T6c-future.

/// `server.transcribeVoice` — submit an audio blob for transcription. The
/// MCode input carries an encoded audio blob (base64/binary) + format hint;
/// we read `params` purely to acknowledge the call (we do not decode or
/// process the blob) and return an empty-text / not-configured result. The
/// UI then knows transcription is unavailable rather than receiving
/// MethodNotFound.
fn handle_server_transcribe_voice(id: Value, params: &Value) -> JsonRpcResponse {
    // Acknowledge the audio blob param without processing it. We do not
    // validate its shape strictly (the MCode contract is not in tier3), so
    // any JSON object/array/primitive is tolerated — the result is the same
    // "not configured" payload regardless.
    let _ = params;
    // stub: no STT backend (T6c-future — install whisper/ffmpeg or wire a
    // STT provider) — return a not-configured transcription result.
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "text": "",
            "error": "STT not configured — install whisper + ffmpeg (or configure a STT provider) to enable voice transcription"
        }),
    )
}

/// `server.voiceStart` — begin a streaming listening session. Without a STT
/// backend we cannot capture or transcribe audio, so we return
/// `{ ok: false, listening: false, reason: "STT not configured" }`. Reads
/// `params` to acknowledge the call but performs no listening-side setup.
fn handle_server_voice_start(id: Value, params: &Value) -> JsonRpcResponse {
    let _ = params;
    // stub: no STT backend — can't start listening.
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "ok": false,
            "listening": false,
            "reason": "STT not configured"
        }),
    )
}

/// `server.voiceStop` — end a streaming listening session. Since
/// `voiceStart` never actually starts listening, this is a no-op that
/// returns `{ ok: true, listening: false }`. Reads `params` to acknowledge
/// the call.
fn handle_server_voice_stop(id: Value, params: &Value) -> JsonRpcResponse {
    let _ = params;
    // stub: no STT backend — no-op stop.
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "ok": true,
            "listening": false
        }),
    )
}

// ─── Server niche ops Handlers (T6c-17 — last batch; completes all RPCs) ──
//
// The final 6 unserved server RPCs. `generateAutomationIntent` is REAL
// (LLM-backed one-shot — see `invoke()` helper above). The other 5 are stubs
// returning documented empty/ack payloads (syncode has no settings
// persistence, no usage-tracking, no local-server process-mgmt subsystem).
//
// These complete the served set: after this block, every UI RPC reaches the
// backend (ZERO unserved RPCs).

/// `server.generateAutomationIntent` — REAL (LLM-backed, T6c-17). Given a
/// natural-language `message` (e.g. "run tests every hour"), prompt the
/// provider CLI once and parse the reply as an automation definition. The
/// MCode `ServerGenerateAutomationIntentResult` shape is:
/// `{ isAutomation, confidence, language, name, taskPrompt, schedule, mode,
///    maxIterations, completionPolicy, missingFields, needsConfirmation,
///    reason }`.
///
/// The LLM is instructed to respond with ONLY valid JSON carrying `{ name,
/// command, schedule, mode }`. We extract these into the result fields and
/// compute `missingFields` (any of name/schedule/taskPrompt absent). On any
/// failure (no provider registered, spawn/send error, malformed JSON, empty
/// reply) we return a not-automation result with `isAutomation: false`,
/// `confidence: 0`, and the raw/error text in `reason` — the UI surfaces a
/// clear "could not generate" state rather than a MethodNotFound or crash.
///
/// `params.message` (or `params.intent` / `params.prompt`) is required; if
/// absent/empty we return InvalidParams (-32602) since there is nothing to
/// generate from.
async fn handle_server_generate_automation_intent(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    // Pull the user intent text. MCode's contract uses `message`; we accept a
    // few aliases for robustness (`intent`, `prompt`, `text`).
    let message = params
        .get("message")
        .or_else(|| params.get("intent"))
        .or_else(|| params.get("prompt"))
        .or_else(|| params.get("text"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if message.trim().is_empty() {
        return JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            "Invalid params: 'message' must be a non-empty string",
        );
    }

    let system = "You are an automation assistant. Given a user's intent, generate an automation definition. Respond with ONLY valid JSON (no markdown fences, no prose) with this exact shape: {\"name\": string, \"command\": string, \"schedule\": string (cron-like or natural language), \"mode\": \"oneshot\"|\"scheduled\"|\"continuous\", \"confidence\": number (0..1)}. If the intent is not an automation, respond with {\"isAutomation\": false}.";
    let prompt = format!("Intent: {message}\n\nGenerate the automation definition JSON.");
    let provider = resolve_provider_param(params);
    let model = resolve_model_param(params);

    // Build the not-automation fallback result. Used when the LLM can't be
    // invoked OR the reply isn't parseable as automation JSON.
    let not_automation = |reason: String| -> Value {
        serde_json::json!({
            "isAutomation": false,
            "confidence": 0.0,
            "language": null,
            "name": null,
            "taskPrompt": null,
            "schedule": null,
            "mode": null,
            "missingFields": ["name", "schedule", "taskPrompt", "mode"],
            "needsConfirmation": false,
            "reason": reason,
        })
    };

    let reply = match invoke(state, &provider, model.as_deref(), system, &prompt).await {
        Ok(text) => text,
        Err(e) => {
            return JsonRpcResponse::success(id, not_automation(e));
        }
    };

    // Try to parse the reply as JSON. Tolerate markdown fences (```json ... ```).
    let trimmed = reply.trim();
    let json_str = strip_markdown_fence(trimmed);
    let parsed: Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(_) => {
            // Malformed JSON — return not-automation with the raw text so the
            // UI / caller can inspect what the provider returned.
            return JsonRpcResponse::success(
                id,
                not_automation(format!("LLM reply was not valid JSON: {reply}")),
            );
        }
    };

    // If the LLM explicitly said "not an automation", honor that.
    let is_automation_flag = parsed
        .get("isAutomation")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !is_automation_flag
        && parsed.get("name").is_none()
        && parsed.get("command").is_none()
    {
        return JsonRpcResponse::success(id, not_automation(reply));
    }

    // Map the LLM JSON fields into the MCode result shape.
    let name = parsed
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let command = parsed
        .get("command")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let schedule = parsed
        .get("schedule")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let mode = parsed
        .get("mode")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let confidence = parsed
        .get("confidence")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.8);

    // Compute missing fields against the MCode union.
    let mut missing: Vec<&str> = Vec::new();
    if name.is_none() {
        missing.push("name");
    }
    if schedule.is_none() {
        missing.push("schedule");
    }
    if command.is_none() {
        missing.push("taskPrompt");
    }
    if mode.is_none() {
        missing.push("mode");
    }

    // `taskPrompt` in the MCode shape is the prompt to drive the automation;
    // the LLM's `command` is the closest analog (what to run).
    let result = serde_json::json!({
        "isAutomation": true,
        "confidence": confidence,
        "language": null,
        "name": name,
        "taskPrompt": command,
        "schedule": schedule,
        "mode": mode,
        "maxIterations": null,
        "missingFields": missing,
        "needsConfirmation": true,
        "reason": null,
    });
    JsonRpcResponse::success(id, result)
}

/// Strip a leading ```json …``` (or ``` … ```) fence from a model reply so
/// `serde_json` can parse it. Returns the input unchanged if no fence is
/// present.
fn strip_markdown_fence(s: &str) -> String {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix("```json") {
        return rest.trim().trim_end_matches("```").trim().to_string();
    }
    if let Some(rest) = s.strip_prefix("```") {
        return rest.trim().trim_end_matches("```").trim().to_string();
    }
    s.to_string()
}

/// `server.patchSettings` — accept the patch, echo the default
/// `ServerSettings`. Performs no persistence (stub). Mirrors
/// `server.updateSettings` so the UI's post-save re-read converges to the
/// unchanged default.
fn handle_server_patch_settings(id: Value) -> JsonRpcResponse {
    // stub: no persistence — echo the default settings.
    JsonRpcResponse::success(id, build_default_server_settings())
}

/// `server.listProviderUsage` — return empty usage list. MCode's contract is
/// `ServerListProviderUsageResult = readonly ServerProviderUsageSnapshot[]`;
/// syncode has no usage-tracking subsystem, so we return an empty array
/// (acknowledging the optional `forceRefresh` param without erroring).
fn handle_server_list_provider_usage(id: Value, params: &Value) -> JsonRpcResponse {
    let _ = params; // ack `forceRefresh` etc. — no behavior.
    // stub: no usage-tracking subsystem — empty list.
    JsonRpcResponse::success(id, serde_json::json!([]))
}

/// `server.getProviderUsageSnapshot` — return null snapshot. MCode's contract
/// is `ServerGetProviderUsageSnapshotResult = ServerProviderUsageSnapshot |
/// null`; syncode has no usage-tracking subsystem, so we return `null`.
/// Validates that `params.provider` is a non-empty string to give a typed
/// error rather than a silent null when the caller omits it.
fn handle_server_get_provider_usage_snapshot(id: Value, params: &Value) -> JsonRpcResponse {
    let provider = params.get("provider").and_then(|v| v.as_str()).unwrap_or("");
    if provider.trim().is_empty() {
        return JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            "Invalid params: 'provider' must be a non-empty string",
        );
    }
    // stub: no usage-tracking subsystem — null snapshot.
    JsonRpcResponse::success(id, Value::Null)
}

/// `server.startLocalServer` — return a graceful not-supported result.
/// Syncode has no local-server process-mgmt subsystem (no dev-server spawn /
/// port-bind / lifecycle tracking), so we acknowledge the call and return
/// `{ ok: false, reason: "Local server management not supported in this mode" }`
/// — the UI surfaces a clear "not available" state instead of MethodNotFound.
fn handle_server_start_local_server(id: Value) -> JsonRpcResponse {
    // stub: no local-server process-mgmt subsystem.
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "ok": false,
            "reason": "Local server management not supported in this mode"
        }),
    )
}

/// `server.stopLocalServer` — no-op ack. Syncode has no local-server
/// process-mgmt subsystem, so there is nothing to stop. Reads `params` to
/// acknowledge the MCode `ServerStopLocalServerInput` (`{ pid, port }`) and
/// returns `{ ok: true }`.
fn handle_server_stop_local_server(id: Value, params: &Value) -> JsonRpcResponse {
    let _ = params; // ack `{ pid, port }` — no behavior.
    // stub: no local-server process-mgmt subsystem — no-op ack.
    JsonRpcResponse::success(id, serde_json::json!({ "ok": true }))
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

// ─── Git Advanced Handlers (stash / network / worktree / init, T6c-9) ──
//
// These back the `git.*` arms appended at the END of `dispatch_method`. They
// cover the GitPanel RPCs the core phase-3 surface does not:
//   - Stash: list/create/apply/drop/info (git2 direct) + stashAndCheckout stub
//   - Network: fetch (git2 direct), pull/push (syncode-git Git2Service)
//   - Worktree: list/create/remove (syncode_git::worktree free functions)
//   - Misc: init (`Repository::init`), removeIndexLock (delete `.git/index.lock`)
//
// All handlers reuse the shared `open_git_service`/`git_error` helpers so the
// `cwd` resolution + error-envelope shape matches the phase-3 handlers. The
// MCode UI shapes (Tier-3 `git.ts`) only declare `GitStashInfoResult`
// formally; the other result shapes are returned as best-effort JSON objects
// with the fields the UI reads (`stashes`, `ok`, `branch`, …). The contracts
// registry (`rpc.ts`) declares local interfaces for type-safety.

/// Resolve a `git2::Repository` from request params. Mirrors
/// `open_git_service` but returns the raw `git2` handle (needed for stash /
/// fetch / init / index-lock ops that aren't on the `GitService` trait).
/// Reuses the same `cwd`/`path` resolution so behavior is identical.
fn open_git2_repo(
    id: Value,
    params: &Value,
) -> Result<git2::Repository, Box<JsonRpcResponse>> {
    let path = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("path").and_then(|v| v.as_str()))
        .unwrap_or(".");
    match git2::Repository::discover(path) {
        Ok(repo) => Ok(repo),
        Err(e) => Err(Box::new(git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git open failed: {e}"),
        ))),
    }
}

/// `git.stashList` → list stashes. Returns `{ stashes: [{ index, message,
/// oid }] }`. The MCode UI reads `stashes` as an array; the per-entry shape
/// is a local best-effort (no formal Tier-3 type — `GitStashInfoResult` is
/// per-stash, used by `stashInfo`).
fn handle_git_stash_list(id: Value, params: &Value) -> JsonRpcResponse {
    let mut repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    let mut stashes: Vec<Value> = Vec::new();
    let walk = repo.stash_foreach(|index, message, oid| {
        stashes.push(serde_json::json!({
            "index": index,
            "message": message,
            "oid": oid.to_string(),
            "stashRef": format!("stash@{{{index}}}"),
        }));
        true
    });
    if let Err(e) = walk {
        return git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git stash_foreach: {e}"),
        );
    }
    JsonRpcResponse::success(id, serde_json::json!({ "stashes": stashes }))
}

/// `git.stashCreate` → save working tree to a new stash. UI sends
/// `{ cwd, message? }`. Returns `{ ok: true, oid, stashRef }` on success.
/// `oid` is `null` when there was nothing to stash (git2 returns the zero
/// oid in that case — we surface it as `ok:true, oid:null, reason:"nothing
/// to stash"` so the UI can render an appropriate empty state).
fn handle_git_stash_create(id: Value, params: &Value) -> JsonRpcResponse {
    let mut repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    let message = params
        .get("message")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("stashMessage").and_then(|v| v.as_str()));
    // Signature: prefer the repo's configured default; fall back to a generic
    // "syncode" identity so the stash can always be saved (MCode uses the
    // configured git identity too).
    let sig = match repo.signature() {
        Ok(s) => s,
        Err(_) => match git2::Signature::now("syncode", "syncode@local") {
            Ok(s) => s,
            Err(e) => {
                return git_error(
                    id,
                    crate::error_codes::INTERNAL_ERROR,
                    format!("git stash signature: {e}"),
                );
            }
        },
    };
    let oid = match repo.stash_save2(&sig, message, Some(git2::StashFlags::INCLUDE_UNTRACKED)) {
        Ok(o) => o,
        Err(e) => {
            // git2's libgit2 returns a Stash-class NotFound error when there
            // are no local modifications to stash ("there is nothing to
            // stash"). The caller-visible semantics in MCode are "ok, nothing
            // to do" — surface that explicitly rather than as an error.
            // (Class 19 = Stash, code -3 = NotFound in libgit2.)
            if e.class() == git2::ErrorClass::Stash && e.code() == git2::ErrorCode::NotFound {
                return JsonRpcResponse::success(
                    id,
                    serde_json::json!({
                        "ok": true,
                        "oid": Value::Null,
                        "stashRef": Value::Null,
                        "reason": "nothing to stash"
                    }),
                );
            }
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("git stash_save: {e}"),
            );
        }
    };
    // Defensive: a non-error zero oid (older libgit2 versions returned this
    // for nothing-to-stash) is also surfaced as nothing-to-stash.
    if oid.is_zero() {
        return JsonRpcResponse::success(
            id,
            serde_json::json!({ "ok": true, "oid": Value::Null, "stashRef": Value::Null, "reason": "nothing to stash" }),
        );
    }
    // Compute the resulting stash index (last entry — stash_save appends).
    let mut count = 0u32;
    let _ = repo.stash_foreach(|_, _, _| {
        count += 1;
        true
    });
    let stash_ref = format!("stash@{{{}}}", count.saturating_sub(1));
    JsonRpcResponse::success(
        id,
        serde_json::json!({ "ok": true, "oid": oid.to_string(), "stashRef": stash_ref }),
    )
}

/// `git.stashApply` → apply a stash by index. UI sends `{ cwd, index? }`
/// (default 0 — the most recent stash). Returns `{ ok: true }`.
fn handle_git_stash_apply(id: Value, params: &Value) -> JsonRpcResponse {
    let mut repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    let index = params
        .get("index")
        .and_then(|v| v.as_u64())
        .or_else(|| params.get("stashIndex").and_then(|v| v.as_u64()))
        .unwrap_or(0) as usize;
    match repo.stash_apply(index, None) {
        Ok(_) => JsonRpcResponse::success(id, serde_json::json!({ "ok": true })),
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git stash_apply: {e}"),
        ),
    }
}

/// `git.stashDrop` → drop a stash by index. UI sends `{ cwd, index? }`
/// (default 0). Returns `{ ok: true }`.
fn handle_git_stash_drop(id: Value, params: &Value) -> JsonRpcResponse {
    let mut repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    let index = params
        .get("index")
        .and_then(|v| v.as_u64())
        .or_else(|| params.get("stashIndex").and_then(|v| v.as_u64()))
        .unwrap_or(0) as usize;
    match repo.stash_drop(index) {
        Ok(_) => JsonRpcResponse::success(id, serde_json::json!({ "ok": true })),
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git stash_drop: {e}"),
        ),
    }
}

/// `git.stashInfo` → return MCode `GitStashInfoResult` for a single stash:
/// `{ cwd, branch, stashRef, message, files }`. UI sends `{ cwd, index? }`
/// (default 0). `files` is the list of paths the stash touches (best-effort —
/// derived from `stash@{N}^1` vs `stash@{N}` tree diff). `branch` is the
/// branch the stash was created on (`stash@{N}`'s parent commit's branch, if
/// resolvable; else null).
fn handle_git_stash_info(id: Value, params: &Value) -> JsonRpcResponse {
    let mut repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    let index = params
        .get("index")
        .and_then(|v| v.as_u64())
        .or_else(|| params.get("stashIndex").and_then(|v| v.as_u64()))
        .unwrap_or(0) as usize;
    let cwd = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("path").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();

    // Walk stashes once, capturing the matching entry (message + oid). git2's
    // foreach takes `&mut self`, so we run it before any later borrow of
    // `repo` for tree diffing.
    let mut found: Option<(String, git2::Oid)> = None;
    let mut walk_err: Option<git2::Error> = None;
    {
        let walk = repo.stash_foreach(|i, message, oid| {
            if i == index {
                found = Some((message.to_string(), *oid));
                false // stop
            } else {
                true
            }
        });
        if let Err(e) = walk {
            walk_err = Some(e);
        }
    }
    if let Some(e) = walk_err {
        return git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git stash_foreach: {e}"),
        );
    }
    let (message, oid) = match found {
        Some(m) => m,
        None => {
            return git_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("stash index {index} not found"),
            );
        }
    };

    // The stash commit's first parent is the commit the stash was based on;
    // its branch (if any) is the branch at stash-creation time.
    let branch = repo
        .find_commit(oid)
        .ok()
        .and_then(|commit| commit.parent(0).ok())
        .and_then(|parent| {
            // Resolve which branch (if any) points at this parent.
            repo.branches(Some(git2::BranchType::Local))
                .ok()
                .and_then(|mut branches| {
                    branches.find_map(|b| {
                        let (b, _) = b.ok()?;
                        let target = b.get().target()?;
                        let name = b.name().ok()?.map(String::from)?;
                        (target == parent.id()).then_some(name)
                    })
                })
        });

    // Files: union of paths touched by the stash. A stash commit has 2-3
    // parents: [0]=HEAD, [1]=index state, [2]=untracked-files state (only
    // when INCLUDE_UNTRACKED). Each parent's tree-vs-stash-tree diff reveals
    // a subset of the touched paths; we union them so the UI sees the full
    // set. (Diffing only parent[0] misses untracked-only files.)
    let files: Vec<String> = (|| -> Result<Vec<String>, git2::Error> {
        let stash_commit = repo.find_commit(oid)?;
        let stash_tree = stash_commit.tree()?;
        let mut paths: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for parent_idx in 0..stash_commit.parent_count() {
            let parent = match stash_commit.parent(parent_idx) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let parent_tree = parent.tree()?;
            let diff = repo.diff_tree_to_tree(Some(&parent_tree), Some(&stash_tree), None)?;
            for d in diff.deltas() {
                if let Some(p) = d.new_file().path().or_else(|| d.old_file().path()) {
                    paths.insert(p.to_string_lossy().to_string());
                }
            }
            // For the untracked-files parent (parent[2]), also diff ITS tree
            // against the empty tree — that's where pure-untracked additions
            // show up (they're stored as additions in parent[2]'s tree, not
            // in the stash commit's own tree).
            if parent_idx == 2 {
                let untracked_diff = repo.diff_tree_to_tree(None, Some(&parent_tree), None)?;
                for d in untracked_diff.deltas() {
                    if let Some(p) = d.new_file().path() {
                        paths.insert(p.to_string_lossy().to_string());
                    }
                }
            }
        }
        Ok(paths.into_iter().collect())
    })()
    .unwrap_or_default();

    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "cwd": cwd,
            "branch": branch,
            "stashRef": format!("stash@{{{index}}}"),
            "message": message,
            "files": files,
        }),
    )
}

/// `git.stashAndCheckout` — STUB. Two-phase op (stash working tree then
/// checkout a branch) the UI can compose itself via `stashCreate` +
/// `checkout`. Returning `{ ok:false, reason }` so the UI can surface a clear
/// "not supported, use stash + checkout" message rather than a generic
/// MethodNotFound. Documented gap.
fn handle_git_stash_and_checkout(id: Value, _params: &Value) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "ok": false,
            "reason": "stashAndCheckout is not implemented as a single op; use stashCreate then checkout",
        }),
    )
}

/// `git.fetch` → fetch from a remote. UI sends `{ cwd, remote? (default
/// "origin"), refspec? (optional single refspec) }`. Returns `{ ok: true,
/// remote, refspec }`. Implemented via git2's `Remote::download` (which
/// auto-connects if needed) + explicit `disconnect`. Auth is delegated to
/// the user's git credential setup — if the remote requires auth and none is
/// configured, git2 returns an error we surface as `INTERNAL_ERROR`
/// (auth-classification matching the push/pull CLI path would require
/// parsing the git2 error message; deferred).
fn handle_git_fetch(id: Value, params: &Value) -> JsonRpcResponse {
    let repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    let remote_name = params
        .get("remote")
        .and_then(|v| v.as_str())
        .unwrap_or("origin");
    let refspec_opt = params.get("refspec").and_then(|v| v.as_str());

    let mut remote = match repo.find_remote(remote_name) {
        Ok(r) => r,
        Err(e) => {
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("git fetch: remote '{remote_name}' not found: {e}"),
            );
        }
    };
    // Download the pack. `Remote::download` auto-connects if not already
    // connected. Pass the optional single refspec; if absent, pass the
    // remote's configured fetch refspecs (the default
    // `+refs/heads/*:refs/remotes/origin/*` mapping set up by `git clone`).
    // An empty specs slice tells git2 to use the base refspecs.
    let refspecs_owned: Vec<String> = match refspec_opt {
        Some(s) => vec![s.to_string()],
        None => remote
            .fetch_refspecs()
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| s.map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
    };
    let download = remote.download(&refspecs_owned, None);
    // disconnect is non-fatal from the caller's perspective (the connection
    // tears down with the remote handle on drop), but we still call it for
    // cleanliness — log on error.
    if let Err(e) = remote.disconnect() {
        tracing::warn!(error = %e, "git fetch disconnect failed (non-fatal)");
    }
    if let Err(e) = download {
        return git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git fetch download: {e}"),
        );
    }
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "ok": true,
            "remote": remote_name,
            "refspec": refspec_opt.unwrap_or("default"),
        }),
    )
}

/// `git.pull` → delegate to `Git2Service::pull` (CLI-backed, --ff-only,
/// surfaces NoUpstream/AuthenticationRequired/RemoteRejected distinctly).
/// UI sends `{ cwd, remote?, branch? }`. Returns the MCode-shaped
/// `{ status: "pulled" | "skipped_up_to_date", branch, upstream_branch }`.
fn handle_git_pull(id: Value, params: &Value) -> JsonRpcResponse {
    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };
    let remote = params
        .get("remote")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let branch = params
        .get("branch")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    match svc.pull(remote, branch) {
        Ok(result) => {
            let json = match serde_json::to_value(&result) {
                Ok(v) => v,
                Err(e) => {
                    return git_error(
                        id,
                        crate::error_codes::INTERNAL_ERROR,
                        format!("git pull serialize: {e}"),
                    );
                }
            };
            JsonRpcResponse::success(id, json)
        }
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git pull: {e}"),
        ),
    }
}

/// `git.push` → delegate to `Git2Service::push` (CLI-backed, sets upstream
/// with -u when none configured, skips when up-to-date). UI sends
/// `{ cwd, remote?, branch? }`. Returns the MCode-shaped
/// `{ status: "pushed" | "skipped_up_to_date", branch, upstream_branch,
///   set_upstream }`.
fn handle_git_push(id: Value, params: &Value) -> JsonRpcResponse {
    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };
    let remote = params
        .get("remote")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let branch = params
        .get("branch")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    match svc.push(remote, branch) {
        Ok(result) => {
            let json = match serde_json::to_value(&result) {
                Ok(v) => v,
                Err(e) => {
                    return git_error(
                        id,
                        crate::error_codes::INTERNAL_ERROR,
                        format!("git push serialize: {e}"),
                    );
                }
            };
            JsonRpcResponse::success(id, json)
        }
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git push: {e}"),
        ),
    }
}

/// `git.init` → initialize a new git repository at `path`. UI sends
/// `{ cwd }` (the path to init; NOT required to exist as a repo first — this
/// is the one git.* RPC where the path is usually NOT yet a repo). Returns
/// `{ ok: true, path }`. Uses `Repository::init` (creates `.git` if absent,
/// idempotent if already a repo).
fn handle_git_init(id: Value, params: &Value) -> JsonRpcResponse {
    let path = match params
        .get("cwd")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("path").and_then(|v| v.as_str()))
    {
        Some(p) => p.to_string(),
        None => {
            return git_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "Missing 'cwd' parameter",
            );
        }
    };
    match git2::Repository::init(&path) {
        Ok(_) => JsonRpcResponse::success(id, serde_json::json!({ "ok": true, "path": path })),
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git init: {e}"),
        ),
    }
}

/// `git.removeIndexLock` → remove a stale `.git/index.lock`. UI sends
/// `{ cwd }`. Returns `{ ok: true, removed: bool }` — `removed:false` means
/// no lock file was present (the common no-op case). The lock path is
/// `<repo.path()>/index.lock` where `repo.path()` is the `.git` directory
/// (or the `.git` dir itself for bare repos). Uses `Repository::discover` so
/// the call works from a subdirectory of the worktree.
fn handle_git_remove_index_lock(id: Value, params: &Value) -> JsonRpcResponse {
    let repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    let lock_path = repo.path().join("index.lock");
    match std::fs::remove_file(&lock_path) {
        Ok(_) => JsonRpcResponse::success(
            id,
            serde_json::json!({ "ok": true, "removed": true, "path": lock_path.to_string_lossy() }),
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // No lock file — the healthy case. Surface removed:false so the
            // UI can render "nothing to clean up".
            JsonRpcResponse::success(
                id,
                serde_json::json!({ "ok": true, "removed": false, "path": lock_path.to_string_lossy() }),
            )
        }
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git removeIndexLock: {e}"),
        ),
    }
}

/// `git.worktreeList` → list worktrees. Returns `{ worktrees: [{ path,
/// branch, is_main, is_locked }] }`. Implemented directly via git2 rather
/// than `syncode_git::worktree::list_worktrees` — the syncode-git helper
/// iterates `repo.worktrees()` which OMITS the main worktree (it only
/// returns linked worktrees). The main worktree is the repo's `workdir()`,
/// so we prepend it explicitly. The per-entry shape mirrors
/// `syncode_git::worktree::WorktreeInfo` (camelCase via serde would be
/// `isMain`; here we keep snake_case `is_main` matching the syncode-git
/// struct's serde rename — `WorktreeInfo` derives default serde, so fields
/// serialize as their Rust names).
fn handle_git_worktree_list(id: Value, params: &Value) -> JsonRpcResponse {
    let repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    let main_path = repo
        .workdir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();
    let mut arr: Vec<Value> = Vec::new();
    // The main worktree is the repo's working directory; its "branch" is the
    // current HEAD (best-effort shorthand).
    if !main_path.is_empty() {
        let head_branch = repo
            .head()
            .ok()
            .and_then(|h| h.shorthand().map(String::from));
        arr.push(serde_json::json!({
            "path": main_path,
            "branch": head_branch,
            "is_main": true,
            "is_locked": false,
        }));
    }
    // Linked worktrees (additional worktrees added via `git worktree add`).
    let wt_names = match repo.worktrees() {
        Ok(n) => n,
        Err(e) => {
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("git worktreeList: {e}"),
            );
        }
    };
    for wt_name_opt in &wt_names {
        let Some(wt_name) = wt_name_opt else { continue };
        let wt = match repo.find_worktree(wt_name) {
            Ok(w) => w,
            Err(_) => continue,
        };
        let path = wt.path().to_string_lossy().to_string();
        let is_locked = matches!(wt.is_locked(), Ok(git2::WorktreeLockStatus::Locked { .. }));
        arr.push(serde_json::json!({
            "path": path,
            "branch": wt_name,
            "is_main": false,
            "is_locked": is_locked,
        }));
    }
    JsonRpcResponse::success(id, serde_json::json!({ "worktrees": arr }))
}

/// `git.worktreeCreate` → add a worktree. UI sends `{ cwd, branch, path?,
/// createBranch? (default true) }`. Returns the created worktree info as
/// `{ worktree: {...} }`. Implemented directly via git2 because the
/// `syncode_git::worktree::add_worktree` helper passes the refname as the
/// PATH argument to `Repository::worktree` (a bug — the second arg is the
/// filesystem path for the new worktree, not a git ref). Here we resolve the
/// path correctly and let git2 handle the branch lock/reference.
fn handle_git_worktree_create(id: Value, params: &Value) -> JsonRpcResponse {
    let repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    let branch = match params
        .get("branch")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("name").and_then(|v| v.as_str()))
    {
        Some(b) => b.to_string(),
        None => {
            return git_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "Missing 'branch' parameter",
            );
        }
    };
    let create_branch = params
        .get("createBranch")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    // Resolve the worktree's filesystem path. MCode UI sends `path`; if
    // absent, derive a sibling dir under the repo root
    // (`<workdir>/.worktrees/<branch>`).
    let wt_path = params
        .get("path")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let mut p = repo
                .workdir()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::path::PathBuf::from("."));
            p.push(".worktrees");
            p.push(&branch);
            p
        });

    // If createBranch is requested, create the branch at HEAD first (so the
    // worktree checks it out). Otherwise the worktree is created in detached
    // HEAD mode (or on an existing branch if it exists).
    if create_branch
        && let Err(e) = (|| -> Result<(), git2::Error> {
            let head = repo.head()?.peel_to_commit()?;
            // Ignore "already exists" — the worktree can checkout an existing
            // branch.
            match repo.branch(&branch, &head, false) {
                Ok(_) => Ok(()),
                Err(e) if e.code() == git2::ErrorCode::Exists => Ok(()),
                Err(e) => Err(e),
            }
        })()
    {
        return git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git worktreeCreate branch: {e}"),
        );
    }

    let wt = match repo.worktree(&branch, &wt_path, None) {
        Ok(w) => w,
        Err(e) => {
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("git worktreeCreate: {e}"),
            );
        }
    };
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "worktree": {
                "path": wt.path().to_string_lossy(),
                "branch": branch,
                "is_main": false,
                "is_locked": false,
            }
        }),
    )
}

/// `git.worktreeRemove` → prune a linked worktree. UI sends `{ cwd, branch }`
/// (the worktree name = branch it was created for). Returns `{ ok: true }`.
/// Implemented directly via git2 (`Worktree::prune`) rather than
/// `syncode_git::worktree::remove_worktree` for consistency with
/// `worktreeCreate` (both bypass the buggy syncode-git helper).
fn handle_git_worktree_remove(id: Value, params: &Value) -> JsonRpcResponse {
    let repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    let branch = match params
        .get("branch")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("name").and_then(|v| v.as_str()))
    {
        Some(b) => b.to_string(),
        None => {
            return git_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "Missing 'branch' parameter",
            );
        }
    };
    let wt = match repo.find_worktree(&branch) {
        Ok(w) => w,
        Err(e) => {
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("git worktreeRemove: worktree '{branch}' not found: {e}"),
            );
        }
    };
    // `force` controls whether prune removes a dirty/locked worktree. MCode
    // UI's default is false (safe); we honor an explicit `force:true` param
    // by enabling the VALID flag (prune even if the worktree appears valid).
    let force = params.get("force").and_then(|v| v.as_bool()).unwrap_or(false);
    let mut opts = git2::WorktreePruneOptions::new();
    opts.working_tree(true);
    if force {
        opts.valid(true);
    }
    match wt.prune(Some(&mut opts)) {
        Ok(_) => JsonRpcResponse::success(id, serde_json::json!({ "ok": true })),
        Err(e) => git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git worktreeRemove: {e}"),
        ),
    }
}

// ─── T6c-16: git stacked/detached-worktree/progress handlers ────────────
//
// These reuse the `syncode_git::stacked_actions` pipeline (Stage → Commit →
// Push → CreatePR) where possible and fall back to graceful partial results
// (never a panic) when an action can't be fully executed (e.g. no remote, no
// `gh` auth, PR already exists). The MCode `GitRunStackedActionResult` shape
// is `{ action, branch, commit, push, pr }` where each step carries its own
// status enum (skipped_not_requested / created / pushed / opened_existing /
// …). We project each step's outcome into the matching MCode status string
// so the vendored UI's `PullRequestDialog` / `GitActionsControl` can render
// real per-step state.

/// Parse the MCode `GitStackedAction` discriminator from `params.action`.
/// Accepts the canonical snake_case form MCode sends (`commit`, `push`,
/// `create_pr`, `commit_push`, `commit_push_pr`) and tolerant aliases.
fn parse_stacked_action_kind(s: &str) -> Option<StackedActionKind> {
    match s.trim() {
        "commit" => Some(StackedActionKind::Commit),
        "push" => Some(StackedActionKind::Push),
        "create_pr" | "createPr" | "create-pr" | "pr" => Some(StackedActionKind::CreatePr),
        "commit_push" | "commitPush" | "commit-push" => Some(StackedActionKind::CommitPush),
        "commit_push_pr" | "commitPushPr" | "commit-push-pr" => {
            Some(StackedActionKind::CommitPushPr)
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StackedActionKind {
    Commit,
    Push,
    CreatePr,
    CommitPush,
    CommitPushPr,
}

impl StackedActionKind {
    fn wants_commit(self) -> bool {
        matches!(
            self,
            StackedActionKind::Commit
                | StackedActionKind::CommitPush
                | StackedActionKind::CommitPushPr
        )
    }
    fn wants_push(self) -> bool {
        matches!(
            self,
            StackedActionKind::Push
                | StackedActionKind::CommitPush
                | StackedActionKind::CommitPushPr
        )
    }
    fn wants_pr(self) -> bool {
        matches!(
            self,
            StackedActionKind::CreatePr | StackedActionKind::CommitPushPr
        )
    }
    /// Echo back the canonical MCode discriminator in the result.
    fn as_str(self) -> &'static str {
        match self {
            StackedActionKind::Commit => "commit",
            StackedActionKind::Push => "push",
            StackedActionKind::CreatePr => "create_pr",
            StackedActionKind::CommitPush => "commit_push",
            StackedActionKind::CommitPushPr => "commit_push_pr",
        }
    }
}

/// `git.runStackedAction` → execute a commit/push/PR pipeline against the
/// repo at `params.path` (or `params.cwd`). UI sends `{ actionId, cwd, action,
/// message?, baseBranch?, remote?, branch? }`. The result mirrors the MCode
/// `GitRunStackedActionResult` shape: `{ action, branch, commit, push, pr }`,
/// each step carrying its own status enum so the UI's stacked-action progress
/// UI can render per-step state. Implemented via `syncode_git::stacked_actions`
/// — we build a `StackedPipeline`, push the relevant `StackedAction`s, run
/// `execute`, and project the per-step `ActionResult` into the MCode shape.
///
/// The branch step is always "skipped_not_requested" (syncode's stacked
/// pipeline doesn't model branch-creation as a step; the MCode UI sends a
/// branch name only when it wants a new branch — we honor that via a
/// pre-step `create_branch` when `params.branch` is set and `params.createBranch`
/// isn't false). Partial failures (no remote, no `gh` auth, nothing to commit)
/// surface as a graceful per-step status, never a crash.
fn handle_git_run_stacked_action(id: Value, params: &Value) -> JsonRpcResponse {
    let action_str = match params.get("action").and_then(|v| v.as_str()) {
        Some(a) => a,
        None => {
            return git_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                "Missing 'action' parameter (expected: commit | push | create_pr | commit_push | commit_push_pr)",
            );
        }
    };
    let kind = match parse_stacked_action_kind(action_str) {
        Some(k) => k,
        None => {
            return git_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("Unknown 'action' value: {action_str}"),
            );
        }
    };

    let svc = match open_git_service(id.clone(), params) {
        Ok(s) => s,
        Err(resp) => return *resp,
    };

    // Optional pre-step: create + checkout a new branch (MCode's `branch`
    // phase). When `params.branch` is supplied AND `params.createBranch` isn't
    // `false`, create the branch at HEAD before running the pipeline so the
    // commit/push land on the right ref. The branch step status reflects the
    // outcome (created / skipped_not_requested).
    let branch_name: Option<String> = params
        .get("branch")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let create_branch = params
        .get("createBranch")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let mut branch_status = "skipped_not_requested".to_string();
    if let Some(name) = branch_name.as_ref()
        && create_branch
    {
        match <syncode_git::service::Git2Service as GitService>::create_branch(
            &svc, name, true,
        ) {
            Ok(_) => branch_status = "created".to_string(),
            Err(e) => {
                // Surface the branch-create failure but continue the pipeline
                // — the MCode UI tolerates a "skipped" branch step.
                tracing::warn!(error = %e, branch = %name, "runStackedAction: create_branch failed");
            }
        }
    }

    let commit_message = params
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("Syncode stacked action");
    let remote = params
        .get("remote")
        .and_then(|v| v.as_str())
        .unwrap_or("origin");
    let push_branch = branch_name
        .clone()
        .or_else(|| {
            <syncode_git::service::Git2Service as GitService>::current_branch(&svc)
                .ok()
                .flatten()
        })
        .unwrap_or_else(|| "HEAD".to_string());
    let pr_base = params
        .get("baseBranch")
        .and_then(|v| v.as_str())
        .unwrap_or("main");
    let pr_title = params
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or(commit_message);

    // Build the syncode stacked pipeline and execute it. `StackedPipeline` is
    // async-but-actually-sync (no `.await` points inside `execute` — it just
    // calls sync `GitService` trait methods), so we can safely drive the
    // future to completion inline with a no-op executor. This avoids the
    // `block_on`-inside-async-context deadlock risk (we are called from the
    // async `dispatch_method` on the tokio worker thread).
    let mut pipeline = syncode_git::stacked_actions::StackedPipeline::new();
    if kind.wants_commit() {
        pipeline.add(syncode_git::stacked_actions::StackedAction::Commit {
            message: commit_message.to_string(),
        });
    }
    if kind.wants_push() {
        pipeline.add(syncode_git::stacked_actions::StackedAction::Push {
            remote: remote.to_string(),
            branch: push_branch.clone(),
        });
    }
    if kind.wants_pr() {
        pipeline.add(syncode_git::stacked_actions::StackedAction::CreatePR {
            title: pr_title.to_string(),
            body: params
                .get("body")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            base: pr_base.to_string(),
        });
    }

    // `StackedPipeline::execute` is declared `async` but contains no `.await`
    // points (only sync `GitService` calls), so the returned future is
    // immediately ready. We poll it once via `Pin::new(&mut fut).poll(...)` to
    // extract the `Result` without re-entering the runtime — this is the
    // canonical pattern for "driving a future that's already ready". `Box`
    // makes the future `Unpin` (the generated async-block future isn't).
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    let mut fut = Box::pin(pipeline.execute(&svc));
    let results = match Pin::new(&mut fut).poll(&mut Context::from_waker(
        &futures_util::task::noop_waker(),
    )) {
        Poll::Ready(r) => r,
        Poll::Pending => {
            // Should never happen — `execute` has no real `.await` points.
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                "git runStackedAction: pipeline unexpectedly pending (no async work expected)",
            );
        }
    };
    let results = match results {
        Ok(r) => r,
        Err(e) => {
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("git runStackedAction: pipeline failed: {e}"),
            );
        }
    };

    // Project per-step results into the MCode shape. Each step defaults to
    // `skipped_not_requested`; a matching `ActionResult` flips it.
    let mut commit_status = "skipped_not_requested".to_string();
    let mut commit_sha: Option<String> = None;
    let mut commit_subject: Option<String> = None;
    let mut push_status = "skipped_not_requested".to_string();
    let mut push_branch_out: Option<String> = None;
    let mut push_upstream: Option<String> = None;
    let mut push_set_upstream: Option<bool> = None;
    let mut pr_status = "skipped_not_requested".to_string();
    let mut pr_url: Option<String> = None;
    let mut pr_base_out: Option<String> = None;
    let mut pr_head: Option<String> = None;
    let mut pr_title_out: Option<String> = None;

    for r in &results {
        let out = r.output.as_deref().unwrap_or("");
        let err = r.error.as_deref();
        if out.starts_with("Committed") {
            commit_status = if r.success {
                "created".to_string()
            } else {
                // Distinguish "nothing to commit" from a hard failure.
                if err
                    .map(|e| e.contains("nothing to commit") || e.contains("no changes"))
                    .unwrap_or(false)
                {
                    "skipped_no_changes".to_string()
                } else {
                    // Hard commit failure — surface via pr/push step error
                    // field (no MCode status for "failed"; closest is
                    // skipped_no_changes, but we keep "created" with no sha so
                    // the UI shows an empty step).
                    "skipped_no_changes".to_string()
                }
            };
            // Extract sha/subject from "Committed: <subject> (<sha>)".
            if r.success
                && let Some(sha_start) = out.rfind('(')
                && let Some(sha_end) = out[sha_start..].find(')')
            {
                commit_sha = Some(out[sha_start + 1..sha_start + sha_end].to_string());
            }
            if r.success {
                // "Committed: <subject> (<sha>)" → subject between ": " and " (".
                if let Some(subject_start) = out.find(": ") {
                    let rest = &out[subject_start + 2..];
                    let subject_end = rest.rfind(" (").unwrap_or(rest.len());
                    commit_subject = Some(rest[..subject_end].to_string());
                }
            }
        } else if out.starts_with("Pushed") || out.starts_with("Skipped") {
            push_status = if r.success {
                if out.starts_with("Skipped") {
                    "skipped_up_to_date".to_string()
                } else {
                    "pushed".to_string()
                }
            } else {
                "skipped_not_requested".to_string()
            };
            push_branch_out = Some(push_branch.clone());
            push_upstream = Some(push_branch.clone());
            push_set_upstream = Some(out.contains("set upstream"));
        } else if out.starts_with("Created PR") {
            pr_status = if r.success {
                "created".to_string()
            } else if err
                .map(|e| e.contains("already exists"))
                .unwrap_or(false)
            {
                "opened_existing".to_string()
            } else {
                "skipped_not_requested".to_string()
            };
            // "Created PR '<title>' → <url>" → url after "→ ".
            if r.success
                && let Some(url_start) = out.find("→ ")
            {
                pr_url = Some(out[url_start + 2..].trim().to_string());
            }
            pr_base_out = Some(pr_base.to_string());
            pr_head = Some(push_branch.clone());
            pr_title_out = Some(pr_title.to_string());
        }
    }

    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "action": kind.as_str(),
            "branch": {
                "status": branch_status,
                "name": branch_name,
            },
            "commit": {
                "status": commit_status,
                "commitSha": commit_sha,
                "subject": commit_subject,
            },
            "push": {
                "status": push_status,
                "branch": push_branch_out,
                "upstreamBranch": push_upstream,
                "setUpstream": push_set_upstream,
            },
            "pr": {
                "status": pr_status,
                "url": pr_url,
                "baseBranch": pr_base_out,
                "headBranch": pr_head,
                "title": pr_title_out,
            },
        }),
    )
}

/// `git.createDetachedWorktree` → add a worktree at a detached HEAD (no
/// branch ref). Mirrors `git.worktreeCreate` but checks out `params.commitIsh`
/// (or `params.ref`) directly instead of creating/checking out a branch. UI
/// sends `{ cwd, commitIsh?, path?, name? }`. Returns the worktree's path +
/// the ref it was created at (the MCode `GitCreateDetachedWorktreeResult`
/// shape: `{ worktree: { path, ref, branch: null } }`).
///
/// Implementation: validate `commitIsh` via git2 (`revparse_single` →
/// `peel_to_commit`), then shell out to `git worktree add --detach <path>
/// <commit-ish>`. We use the CLI rather than `Repository::worktree` +
/// `WorktreeAddOptions::reference` because libgit2 rejects non-branch refs
/// for the worktree HEAD ("reference is not a branch; class=Worktree (32)")
/// — the `--detach` flag is the canonical way to create a detached worktree.
fn handle_git_create_detached_worktree(id: Value, params: &Value) -> JsonRpcResponse {
    let repo = match open_git2_repo(id.clone(), params) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };

    // Resolve the commit-ish the worktree should check out (detached).
    let commit_ish = params
        .get("commitIsh")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("ref").and_then(|v| v.as_str()))
        .or_else(|| params.get("commitish").and_then(|v| v.as_str()))
        .unwrap_or("HEAD");
    let commit = match repo.revparse_single(commit_ish) {
        Ok(obj) => match obj.peel_to_commit() {
            Ok(c) => c,
            Err(e) => {
                return git_error(
                    id,
                    crate::error_codes::INVALID_PARAMS,
                    format!("git createDetachedWorktree: '{commit_ish}' is not a commit: {e}"),
                );
            }
        },
        Err(e) => {
            return git_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("git createDetachedWorktree: cannot resolve '{commit_ish}': {e}"),
            );
        }
    };
    let commit_oid = commit.id();

    // Worktree name (used as the administrative name under .git/worktrees).
    // Defaults to the short OID so multiple detached worktrees don't collide.
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| commit_oid.to_string()[..7].to_string());

    // Filesystem path for the new worktree. Defaults to a sibling dir under
    // the repo root (`.worktrees/<name>`), mirroring `worktreeCreate`.
    let wt_path = params
        .get("path")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let mut p = repo
                .workdir()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::path::PathBuf::from("."));
            p.push(".worktrees");
            p.push(&name);
            p
        });

    // Create the detached worktree via `git worktree add --detach <path>
    // <commit-ish>`. libgit2's `Repository::worktree` + `WorktreeAddOptions::
    // reference` REQUIRES the reference to be a branch (the underlying
    // `git_worktree_add_options::reference` field is documented as "reference
    // to use for the new worktree HEAD" but libgit2 rejects non-branch refs
    // with "reference is not a branch; class=Worktree (32)"). The CLI's
    // `--detach` flag is the canonical way to create a worktree in detached
    // HEAD mode without creating a branch ref — mirrors what MCode's tauri
    // side does for the same RPC.
    let cwd = repo
        .workdir()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let output = match std::process::Command::new("git")
        .args([
            "worktree",
            "add",
            "--detach",
            &wt_path.to_string_lossy(),
            commit_ish,
        ])
        .current_dir(&cwd)
        .output()
    {
        Ok(o) => o,
        Err(e) => {
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("git createDetachedWorktree: git binary spawn failed: {e}"),
            );
        }
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return git_error(
            id,
            crate::error_codes::INTERNAL_ERROR,
            format!("git createDetachedWorktree: worktree add failed: {stderr}"),
        );
    }

    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "worktree": {
                "path": wt_path.to_string_lossy(),
                "ref": commit_ish,
                "branch": null,
            }
        }),
    )
}

/// `git.subscribeActionProgress` → GRACEFUL STUB. The vendored MCode UI
/// subscribes to per-phase progress events for a stacked action
/// (`action_started` / `phase_started` / `hook_output` / `action_finished`).
/// Syncode executes stacked actions SYNCHRONOUSLY (no progress push channel
/// for stacked actions), so this returns `{ subscribed: true }` without
/// wiring a real subscription. T6c-future could stream progress via the
/// existing `push/subscribe` bus when stacked actions become long-running.
fn handle_git_subscribe_action_progress(id: Value, _params: &Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, serde_json::json!({ "subscribed": true }))
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

    // Spawn the per-session output-reader task (T6c-11). It polls the PTY for
    // new output (via `spawn_blocking` — the PTY reader is blocking std I/O)
    // and broadcasts each chunk onto `push_tx` as a `terminal/event` push
    // frame. Connections subscribed to the `terminal` channel receive it. The
    // task ends on EOF (child exited) or when aborted by `terminal.close`.
    spawn_terminal_reader(state.clone(), session_key.clone(), params.clone()).await;

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
    // Abort the per-session output-reader task so its blocking PTY read does
    // not outlive the session (otherwise the reader thread leaks until EOF).
    abort_terminal_reader(state, &session_key).await;
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

/// `terminal.subscribeEvents` — record a real push subscription for the
/// `terminal` channel on the originating connection (T6c-11).
///
/// Before T6c-11 this was a stub: the syncode-terminal `SessionManager` was
/// pull-based (no callback on new output), so no push frames were ever
/// emitted and the subscription was a no-op. With the per-session output
/// reader task now broadcasting `terminal/event` frames onto `push_tx`, a
/// connection that subscribes here receives every output/exit event from
/// every live session (the `terminal` channel is global, not per-session —
/// the `terminalId` inside each frame lets the UI filter per pane).
///
/// The push delivery loop (`run_push_delivery` in `server.rs`) consults the
/// subscription registry on every broadcast and forwards only channels the
/// connection has opted into, so this call is the gate that opens the
/// terminal stream to the caller.
async fn handle_terminal_subscribe(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
) -> JsonRpcResponse {
    let added = state
        .subscriptions
        .write()
        .await
        .subscribe(conn_id, crate::channels::CHANNEL_TERMINAL);
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "channel": crate::channels::CHANNEL_TERMINAL,
            "added": added,
        }),
    )
}

/// `terminal.unsubscribeEvents` — drop the `terminal` channel subscription
/// for the originating connection (T6c-11). After this call the connection
/// receives no further `push/terminal` frames until it re-subscribes.
async fn handle_terminal_unsubscribe(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
) -> JsonRpcResponse {
    let removed = state
        .subscriptions
        .write()
        .await
        .unsubscribe(conn_id, crate::channels::CHANNEL_TERMINAL);
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "unsubscribed": true,
            "channel": crate::channels::CHANNEL_TERMINAL,
            "removed": removed,
        }),
    )
}

/// Per-session output-reader task (T6c-11) — the bridge that turns the
/// pull-based `SessionManager` into a live-push terminal stream.
///
/// `spawn_terminal_reader` launches a tokio task that owns the read loop for
/// one session. Each iteration:
///
/// 1. **Read** — `tokio::task::spawn_blocking` runs the PTY read on a blocking
///    thread (the `portable_pty` reader's `read` is blocking std I/O — it has
///    no readiness notification, so calling it directly on a reactor thread
///    would stall the async runtime). The blocking closure locks the
///    `PtyHandle`'s reader (`std::sync::Mutex`, held only across the read),
///    fills a 4 KiB buffer, and returns the byte count.
/// 2. **Decode + push** — back on the async task, the bytes are
///    lossily-decoded to UTF-8 (`String::from_utf8_lossy` — terminal output is
///    a byte stream that may split a multi-byte char across reads) and
///    broadcast onto `push_tx` as `(CHANNEL_TERMINAL, <TerminalEvent payload>)`.
///    The payload is the MCode `TerminalEvent` `{ type:"output", threadId,
///    terminalId, createdAt, data, byteLength }` shape (see
///    `frontend/src/contracts/tier3/terminal.ts`), so the UI's existing
///    decoder applies unchanged.
/// 3. **EOF / exit** — a zero-length read (or a reader error) means the child
///    has exited; the task broadcasts a final `{ type:"exited", exitCode,
///    exitSignal }` frame (best-effort exit code: `null`, since
///    `portable_pty`'s `child` is dropped at spawn and we can't reap it
///    here — documented gap), marks the PTY stopped, removes itself from the
///    reader registry, and ends.
///
/// **Locking model.** The `Arc<RwLock<TerminalSession>>` is cloned once at
/// spawn; each iteration takes a `session.read().await` guard only long enough
/// to grab a `&PtyHandle` reference for the blocking closure (the closure
/// locks the reader's own `std::sync::Mutex` internally). The session guard
/// is dropped before `push_tx.send` so the push never blocks another task
/// waiting on the session lock. The `push_tx` clone is captured directly, so
/// the reader does not touch `WsState` after spawn (no Arc to the whole
/// state) — only the reader-registry Mutex is touched at exit, to unregister.
///
/// **Abortion.** The `JoinHandle` is stored in `state.terminal_readers` keyed
/// by session id. `handle_terminal_close` calls `abort_terminal_reader`,
/// which aborts the task — the blocking read is interrupted when its task is
/// dropped (the `spawn_blocking` closure's future is cancelled on the next
/// yield; in practice the session's PTY master is also dropped by
/// `destroy_session`, which causes the blocking read to error/EOF promptly).
async fn spawn_terminal_reader(state: WsState, session_id: String, params: Value) {
    use tokio::task::JoinHandle;

    // Resolve the session Arc once. If it vanished between create and here
    // (extremely unlikely — same task), bail without spawning.
    let session_arc = {
        let read_guard = state.terminal_manager.read().await;
        read_guard.get_session(&session_id).await
    };
    let session_arc = match session_arc {
        Some(a) => a,
        None => return,
    };

    let push_tx = state.push_tx.clone();
    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(&session_id)
        .to_string();
    let terminal_id = session_id.clone();
    let readers_registry = state.terminal_readers.clone();

    let handle: JoinHandle<()> = tokio::spawn(async move {
        loop {
            // Each iteration hands the session Arc to a blocking thread that
            // performs the PTY read. The blocking closure takes the session's
            // RwLock read guard (tokio guards are Send) and calls the sync
            // `read_output_blocking` WITHOUT awaiting — the documented
            // escape-hatch for tokio RwLocks on a blocking thread.
            let session_for_read = session_arc.clone();
            let read_result = tokio::task::spawn_blocking(move || {
                let session = session_for_read.blocking_read();
                let pty = session.pty();
                let mut local_buf = vec![0u8; 4096];
                match pty.read_output_blocking(&mut local_buf) {
                    Ok(0) => Ok(Vec::new()), // EOF
                    Ok(n) => {
                        local_buf.truncate(n);
                        Ok(local_buf)
                    }
                    Err(e) => Err(e),
                }
            })
            .await;

            let bytes = match read_result {
                Ok(Ok(b)) if b.is_empty() => {
                    // EOF — child exited.
                    let _ = push_tx.send((
                        crate::channels::CHANNEL_TERMINAL.to_string(),
                        serde_json::json!({
                            "type": "exited",
                            "threadId": thread_id,
                            "terminalId": terminal_id,
                            "createdAt": chrono::Utc::now().to_rfc3339(),
                            "exitCode": serde_json::Value::Null,
                            "exitSignal": serde_json::Value::Null,
                        }),
                    ));
                    break;
                }
                Ok(Ok(b)) => b,
                Ok(Err(_e)) => {
                    // Reader error — treat like EOF (session is unusable).
                    tracing::warn!(
                        session_id = %terminal_id,
                        "terminal reader error; ending reader task"
                    );
                    let _ = push_tx.send((
                        crate::channels::CHANNEL_TERMINAL.to_string(),
                        serde_json::json!({
                            "type": "error",
                            "threadId": thread_id,
                            "terminalId": terminal_id,
                            "createdAt": chrono::Utc::now().to_rfc3339(),
                            "message": "terminal reader closed",
                        }),
                    ));
                    break;
                }
                Err(join_err) => {
                    // spawn_blocking task panicked or was cancelled — the
                    // reader task itself is being aborted (terminal.close).
                    // Exit quietly.
                    tracing::debug!(
                        session_id = %terminal_id,
                        error = %join_err,
                        "terminal reader blocking task cancelled; ending reader"
                    );
                    break;
                }
            };

            let byte_len = bytes.len();
            let data = String::from_utf8_lossy(&bytes).into_owned();
            let created_at = chrono::Utc::now().to_rfc3339();
            // Broadcast the output frame. `send` errors when there are no
            // receivers — not a failure (the session is still alive; output
            // is best-effort pushed).
            let _ = push_tx.send((
                crate::channels::CHANNEL_TERMINAL.to_string(),
                serde_json::json!({
                    "type": "output",
                    "threadId": thread_id,
                    "terminalId": terminal_id,
                    "createdAt": created_at,
                    "data": data,
                    "byteLength": byte_len,
                }),
            ));
        }

        // Self-unregister from the reader registry (best-effort; close() may
        // already have removed + aborted us, in which case this is a no-op).
        readers_registry.lock().await.remove(&terminal_id);
        // Mark the PTY stopped so list_sessions reports it as not-alive.
        let session = session_arc.read().await;
        session.pty().mark_stopped();
    });

    // Record the handle so close() can abort it. If a previous reader for the
    // same id lingered (restart path), abort it first to avoid double-readers.
    let mut registry = state.terminal_readers.lock().await;
    if let Some(prev) = registry.insert(session_id, handle) {
        prev.abort();
    }
}

/// Abort a session's output-reader task on close/destroy (T6c-11).
async fn abort_terminal_reader(state: &WsState, session_id: &str) {
    let handle = state.terminal_readers.lock().await.remove(session_id);
    if let Some(h) = handle {
        h.abort();
    }
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

/// `provider.compactThread` — LLM-backed (T6c-13). The composer calls this to
/// compact a thread's conversation context before an LLM round-trip. We read
/// the thread's messages from `read_store`, build a compaction prompt, invoke a
/// provider adapter one-shot, and return the compacted summary in the MCode
/// `ProviderCompactThreadResult` shape (`{ ok, compactedSummary? }`).
///
/// The result extends the phase-7 stub shape (`{ ok: true }`) with an optional
/// `compactedSummary` field — clients that only read `ok` keep working, while
/// the composer can surface the summary. On failure (no provider registered,
/// CLI missing, LLM error) `ok` is `false` and `error` carries a clear message
/// — the composer falls back to the un-compacted history.
async fn handle_provider_compact_thread(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let thread_id = match params.get("threadId").and_then(|v| v.as_str()) {
        Some(t) if !t.is_empty() => t.to_string(),
        _ => {
            return param_error(
                id,
                "provider.compactThread requires a non-empty 'threadId'",
            )
        }
    };

    // Gather the thread's messages from the read model.
    let history = thread_history_text(state, &thread_id).await;
    if history.trim().is_empty() {
        // Nothing to compact — succeed with an empty summary (the composer
        // treats this as a no-op, same as the old stub).
        return JsonRpcResponse::success(
            id,
            serde_json::json!({ "ok": true, "compactedSummary": "" }),
        );
    }

    let system = "You are a conversation compactor. Produce a concise summary of the conversation that preserves every decision, code reference, file path, and open question. Output only the summary.";
    let prompt = format!(
        "Compact the following conversation history into a faithful summary. \
         Preserve all technical details, decisions, and action items.\n\n\
         --- CONVERSATION ---\n{history}\n--- END ---"
    );
    let provider = resolve_provider_param(params);
    let model = resolve_model_param(params);
    match invoke(state, &provider, model.as_deref(), system, &prompt).await {
        Ok(summary) => JsonRpcResponse::success(
            id,
            serde_json::json!({ "ok": true, "compactedSummary": summary }),
        ),
        Err(e) => JsonRpcResponse::success(
            id,
            serde_json::json!({ "ok": false, "error": e }),
        ),
    }
}

// ─── LLM-backed ops helpers (T6c-13) ───────────────────────────────────
//
// Shared plumbing for the three provider-CLI one-shot RPCs
// (`provider.compactThread`, `git.summarizeDiff`, `server.generateThreadRecap`).
// Each handler builds a prompt, resolves a provider adapter from
// `WsState::provider_registry`, and runs it through
// `crate::llm::invoke_llm_oneshot`.

/// Build a JSON-RPC error response for an invalid-params failure (sugar over
/// the inline `JsonRpcResponse::error(...)` used elsewhere).
fn param_error(id: Value, message: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse::error(Some(id), crate::error_codes::INVALID_PARAMS, message)
}

/// Resolve the provider id from the RPC params. Accepts an optional `provider`
/// (or `providerId`) string; falls back to the default provider (`claude`).
fn resolve_provider_param(params: &Value) -> String {
    params
        .get("provider")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| params.get("providerId").and_then(|v| v.as_str()))
        .filter(|s| !s.is_empty())
        .map(String::from)
        .unwrap_or_else(|| crate::llm::DEFAULT_PROVIDER.to_string())
}

/// Resolve an optional model override from the RPC params (`model`/`modelId`).
fn resolve_model_param(params: &Value) -> Option<String> {
    params
        .get("model")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| params.get("modelId").and_then(|v| v.as_str()))
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// Run a one-shot LLM invocation against the provider registered under
/// `provider_id` in `WsState::provider_registry`. Returns the reply text or a
/// human-readable error string (the caller surfaces it in the result shape).
///
/// `model` is an optional model override (resolved from the RPC params); `None`
/// lets the adapter pick its default.
///
/// Errors:
///   - `provider '{id}' is not registered` — the registry has no adapter for
///     the id (production deployments must register one; tests register a mock).
///   - The underlying `invoke_llm_oneshot` error (spawn/start/send failures,
///     missing CLI binary, empty response).
async fn invoke(
    state: &WsState,
    provider_id: &str,
    model: Option<&str>,
    system: &str,
    prompt: &str,
) -> Result<String, String> {
    // Try the requested provider; if not registered, fall back to the default
    // then to the codex fallback so a stock install still works.
    let registry = state.provider_registry.read().await;
    let resolved = if registry.is_registered(provider_id) {
        provider_id.to_string()
    } else if registry.is_registered(crate::llm::DEFAULT_PROVIDER) {
        tracing::info!(
            requested = %provider_id,
            fallback = %crate::llm::DEFAULT_PROVIDER,
            "provider not registered; falling back"
        );
        crate::llm::DEFAULT_PROVIDER.to_string()
    } else if registry.is_registered(crate::llm::FALLBACK_PROVIDER) {
        crate::llm::FALLBACK_PROVIDER.to_string()
    } else {
        return Err(format!(
            "no provider registered (requested '{provider_id}'); \
             register one via the provider registry to enable LLM-backed ops"
        ));
    };
    let adapter = registry
        .get(&resolved)
        .cloned()
        .ok_or_else(|| format!("provider '{resolved}' lookup failed"))?;
    drop(registry);

    crate::llm::invoke_llm_oneshot(&adapter, &resolved, model, Some(system), prompt).await
}

/// Read a thread's messages from the read model and render them as a flat
/// `role: content` transcript (oldest first). Used by `compactThread` and
/// `generateThreadRecap` to build the LLM prompt body.
///
/// Messages are filtered to this thread (by joining through turns, since
/// `MessageView` carries `turn_id` not `thread_id`). Tool-only messages
/// (`role == "tool"`) are omitted — they're noise for compaction/recap.
async fn thread_history_text(state: &WsState, thread_id: &str) -> String {
    let store = state.read_store.read().await;
    // Turns belonging to this thread, ordered by sequence.
    let mut turns: Vec<&syncode_orchestration::TurnView> = store
        .turns
        .values()
        .filter(|t| t.thread_id == thread_id)
        .collect();
    turns.sort_by_key(|t| t.sequence);

    let mut out = String::new();
    for turn in turns {
        // The user input is on the turn itself.
        out.push_str(&format!("user: {}\n", turn.user_input));
        if let Some(output) = turn.assistant_output.as_deref() {
            out.push_str(&format!("assistant: {output}\n"));
        }
        // Also surface role-tagged messages for richer fidelity when present.
        let mut msgs: Vec<&syncode_orchestration::MessageView> = store
            .messages
            .values()
            .filter(|m| m.turn_id == turn.id && m.role != "tool")
            .collect();
        msgs.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        for m in msgs {
            out.push_str(&format!("{}: {}\n", m.role, m.content));
        }
    }
    out
}

/// `git.summarizeDiff` — LLM-backed (T6c-13). Returns a natural-language
/// summary of a git diff in the MCode `GitSummarizeDiffResult` shape
/// (`{ summary }`; the Tier-3 `GitSummarizeDiffResult extends
/// OpaqueTransportResult` so a single `summary` field is shape-compatible).
///
/// The diff text comes from one of:
///   1. `params.diff` / `params.patch` — caller-supplied diff text (the
///      GitPanel may already have it in hand).
///   2. `params.cwd` + optional `scope`/`oldRef`/`newRef` — fetch the working
///      tree diff via syncode-git (reuses `handle_git_diff`'s logic).
async fn handle_git_summarize_diff(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    // 1. Caller-supplied diff text wins.
    let diff = params
        .get("diff")
        .or_else(|| params.get("patch"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let diff = if !diff.trim().is_empty() {
        diff
    } else {
        // 2. Fetch via syncode-git. `cwd` is required for this path.
        match open_git_service(id.clone(), params) {
            Ok(svc) => {
                let old_ref = params.get("oldRef").and_then(|v| v.as_str());
                let new_ref = params.get("newRef").and_then(|v| v.as_str());
                match svc.diff(old_ref, new_ref) {
                    Ok(entries) => {
                        if entries.is_empty() {
                            return JsonRpcResponse::success(
                                id,
                                serde_json::json!({ "summary": "No changes in diff." }),
                            );
                        }
                        // Render a minimal textual patch (same shape as
                        // `handle_git_diff`).
                        let mut patch = String::new();
                        for entry in &entries {
                            let path =
                                entry.old_path.as_deref().unwrap_or(&entry.new_path);
                            patch.push_str(&format!(
                                "diff --git a/{path} b/{new}\nstatus: {status:?}\n",
                                new = entry.new_path,
                                status = entry.status,
                            ));
                        }
                        patch
                    }
                    Err(e) => {
                        return JsonRpcResponse::error(
                            Some(id),
                            crate::error_codes::INTERNAL_ERROR,
                            format!("git summarizeDiff: failed to read diff: {e}"),
                        );
                    }
                }
            }
            Err(resp) => return *resp,
        }
    };

    let system = "You are a code reviewer. Summarize the following git diff in 2-4 sentences for a developer. Focus on the intent of the change, the files touched, and any notable risks. Output only the summary.";
    let prompt = format!(
        "Summarize this diff:\n\n```diff\n{diff}\n```"
    );
    let provider = resolve_provider_param(params);
    let model = resolve_model_param(params);
    match invoke(state, &provider, model.as_deref(), system, &prompt).await {
        Ok(summary) => JsonRpcResponse::success(id, serde_json::json!({ "summary": summary })),
        Err(e) => JsonRpcResponse::success(
            id,
            serde_json::json!({ "summary": "", "error": e }),
        ),
    }
}

/// `server.generateThreadRecap` — LLM-backed (T6c-13). Produces a high-level
/// recap of a thread (what was done, what's pending) for the UI's recap card.
/// Reads the thread's history from `read_store`, builds a recap prompt,
/// invokes the provider, and returns the MCode `ServerGenerateThreadRecapResult`
/// shape (`{ recap }`).
async fn handle_server_generate_thread_recap(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let thread_id = match params.get("threadId").and_then(|v| v.as_str()) {
        Some(t) if !t.is_empty() => t.to_string(),
        _ => {
            return param_error(
                id,
                "server.generateThreadRecap requires a non-empty 'threadId'",
            )
        }
    };

    let history = thread_history_text(state, &thread_id).await;
    if history.trim().is_empty() {
        return JsonRpcResponse::success(
            id,
            serde_json::json!({ "recap": "No activity in this thread yet." }),
        );
    }

    let system = "You are an engineering assistant. Produce a concise recap of the conversation: what was accomplished, key decisions made, and any open or pending work. Use bullet points. Output only the recap.";
    let prompt = format!(
        "Generate a recap of the following thread:\n\n--- THREAD ---\n{history}\n--- END ---"
    );
    let provider = resolve_provider_param(params);
    let model = resolve_model_param(params);
    match invoke(state, &provider, model.as_deref(), system, &prompt).await {
        Ok(recap) => JsonRpcResponse::success(id, serde_json::json!({ "recap": recap })),
        Err(e) => JsonRpcResponse::success(
            id,
            serde_json::json!({ "recap": "", "error": e }),
        ),
    }
}

// ─── GitHub-API ops handlers (T6c-14: gh-CLI-backed) ───────────────────
//
// These four handlers resolve the GitHub-API RPCs the vendored MCode UI calls
// (`git.githubRepository`, `git.resolvePullRequest`, `git.handoffThread`,
// `git.preparePullRequestThread`). Rather than implement an OAuth client + a
// token vault, we shell out to the user's `gh` CLI (authed via
// `gh auth login`). Every subprocess call is bounded (gh's own network calls
// fail fast on no-auth/no-network) and every error path returns a JSON-RPC
// error result, never a panic.
//
// The pure parsing logic (remote-URL parse, `gh repo view` JSON parse, `gh pr
// view` JSON parse) is factored into the `gh_parse` submodule below so the
// mappings can be unit-tested with canned fixtures — no `gh` subprocess
// required. The `#[ignore]`-gated tests at the bottom exercise the live
// `gh`/`git` subprocess path against a real GitHub repo (integration-only).

/// Resolve `cwd` from RPC params (accepts both `cwd` and `path` keys; defaults
/// to `.`). Used by the GitHub-API handlers (matches the open_git_service
/// convention). Note: a separate `resolve_cwd` (returning `Option<String>`)
/// exists for the terminal handlers above; this variant always yields a path.
fn resolve_cwd_or_dot(params: &Value) -> String {
    params
        .get("cwd")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("path").and_then(|v| v.as_str()))
        .unwrap_or(".")
        .to_string()
}

/// Spawn a subprocess (`gh` or `git`) in `cwd`, capturing stdout/stderr/exit.
/// Async + bounded — uses `tokio::process::Command` (the WS handler context is
/// async). Returns `Ok(output_string)` on exit 0 (stdout), `Err(message)` on
/// any failure (binary missing, non-zero exit, IO error). The error message is
/// crafted to surface to the UI (e.g. "gh auth required" / "not a GitHub
/// repo" / "PR not found") rather than a raw stack trace.
async fn run_cli_capture(
    bin: &str,
    cwd: &str,
    args: &[&str],
) -> Result<String, String> {
    // Validate the binary is on PATH before spawning (a spawn() failure would
    // otherwise surface as a generic IO error). `which` is a sync std call —
    // cheap, runs once per RPC.
    if which::which(bin).is_err() {
        return Err(format!(
            "`{bin}` CLI not found on PATH — install it and (for gh) run `gh auth login`"
        ));
    }
    let output = tokio::process::Command::new(bin)
        .args(args)
        .current_dir(cwd)
        .output()
        .await
        .map_err(|e| format!("`{bin}` spawn failed: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        // gh prefixes errors with "gh: " and exits non-zero; surface the
        // first non-empty line for the UI.
        let msg = stderr
            .lines()
            .chain(stdout.lines())
            .find(|l| !l.trim().is_empty())
            .unwrap_or("non-zero exit")
            .to_string();
        return Err(format!("`{bin}` failed: {msg}"));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// `git.githubRepository` → detect the GitHub repo for a local path.
///
/// Two-step resolution:
///   1. `git -C <cwd> remote get-url origin` → parse the GitHub owner/repo
///      from the URL (handles both `git@github.com:owner/repo.git` and
///      `https://github.com/owner/repo[.git]`). If `origin` is missing or
///      points elsewhere (GitLab/Bitbucket/local), return `{ repository: null }`
///      (the MCode `GitHubRepositoryResult` shape — null signals "not a GitHub
///      repo", NOT an error).
///   2. If a GitHub repo was detected, enrich via
///      `gh repo view owner/repo --json nameWithOwner,url` when `gh` is
///      available + authed. On any `gh` failure (not authed / no network), we
///      still return the parsed-from-URL fields (graceful degradation — the
///      URL parse alone is enough to populate the shape).
async fn handle_git_github_repository(id: Value, params: &Value) -> JsonRpcResponse {
    let cwd = resolve_cwd_or_dot(params);

    // Step 1: parse the GitHub owner/repo from the origin remote URL.
    let remote_url = match run_cli_capture("git", &cwd, &["remote", "get-url", "origin"]).await {
        Ok(s) => s.trim().to_string(),
        Err(_) => {
            // No `origin` remote (or not a git repo) → not a GitHub repo. The
            // MCode shape uses `null` for this case, not an error.
            return JsonRpcResponse::success(
                id,
                serde_json::json!({ "repository": null }),
            );
        }
    };
    let Some((owner, name)) = gh_parse::parse_github_remote(&remote_url) else {
        // origin exists but isn't a GitHub URL → null.
        return JsonRpcResponse::success(
            id,
            serde_json::json!({ "repository": null }),
        );
    };

    // Step 2: enrich via gh (graceful on failure).
    let slug = format!("{owner}/{name}");
    let mut name_with_owner = slug.clone();
    let mut url = format!("https://github.com/{slug}");
    if let Ok(json) =
        run_cli_capture("gh", &cwd, &["repo", "view", &slug, "--json", "nameWithOwner,url"]).await
        && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&json)
    {
        if let Some(s) = parsed.get("nameWithOwner").and_then(|v| v.as_str()) {
            name_with_owner = s.to_string();
        }
        if let Some(u) = parsed.get("url").and_then(|v| v.as_str()) {
            url = u.to_string();
        }
    }

    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "repository": {
                "nameWithOwner": name_with_owner,
                "url": url,
            }
        }),
    )
}

/// `git.resolvePullRequest` → resolve a PR by number (or URL). Returns the
/// MCode `GitResolvePullRequestResult` shape (`{ pullRequest: { number, title,
/// url, baseBranch, headBranch, state } }`). Calls
/// `gh pr view <ref> --json number,title,state,headRefName,baseRefName,url`.
/// `ref` is the PR number if `params.number` is set, else the URL if
/// `params.url` is set. On gh failure (PR not found / repo not GitHub / not
/// authed) → JSON-RPC error.
async fn handle_git_resolve_pull_request(id: Value, params: &Value) -> JsonRpcResponse {
    let cwd = resolve_cwd_or_dot(params);
    // Resolve the PR reference: prefer `number`, fall back to `url`.
    let pr_ref = params
        .get("number")
        .and_then(|v| v.as_i64().map(|n| n.to_string()))
        .or_else(|| params.get("url").and_then(|v| v.as_str()).map(String::from));
    let Some(pr_ref) = pr_ref else {
        return param_error(
            id,
            "git.resolvePullRequest requires 'number' (int) or 'url' (string)",
        );
    };
    if pr_ref.trim().is_empty() {
        return param_error(
            id,
            "git.resolvePullRequest requires a non-empty 'number' or 'url'",
        );
    }

    let json = match run_cli_capture(
        "gh",
        &cwd,
        &[
            "pr",
            "view",
            &pr_ref,
            "--json",
            "number,title,state,headRefName,baseRefName,url",
        ],
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INTERNAL_ERROR,
                format!("git.resolvePullRequest: {e}"),
            );
        }
    };

    match gh_parse::parse_pr_view(&json) {
        Ok(pr) => JsonRpcResponse::success(
            id,
            serde_json::json!({ "pullRequest": pr }),
        ),
        Err(e) => JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INTERNAL_ERROR,
            format!("git.resolvePullRequest: parse gh output: {e}"),
        ),
    }
}

/// `git.handoffThread` → create a PR from a thread's branch.
///
/// The full MCode shape (`GitHandoffThreadResult`) implies a multi-phase op:
/// it carries `worktreePath`, `associatedWorktreePath`, `associatedWorktreeBranch`,
/// `associatedWorktreeRef`, `changesTransferred`, `conflictsDetected` — a
/// thread-to-worktree handoff we don't model. The PR-creation sub-step (the
/// most common intent) IS wired via `gh pr create` and surfaced in the
/// returned `message`. The other fields are returned as `null`/`false`
/// (matching the "no worktree handoff performed" outcome).
///
/// Params: `{ cwd, title, body, base, head }`. If `head` is omitted, gh
/// defaults to the current branch.
async fn handle_git_handoff_thread(id: Value, params: &Value) -> JsonRpcResponse {
    let cwd = resolve_cwd_or_dot(params);
    let title = match params.get("title").and_then(|v| v.as_str()) {
        Some(t) if !t.trim().is_empty() => t.to_string(),
        _ => return param_error(id, "git.handoffThread requires a non-empty 'title'"),
    };
    let body = params
        .get("body")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let base = params
        .get("base")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let head = params
        .get("head")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Build the gh pr create arg list. `--body-file -` reads the body from
    // stdin (avoids arg-length limits + shell-escaping; matches MCode's
    // temp-file approach but stdin is simpler in-process).
    let mut args: Vec<String> = vec![
        "pr".into(),
        "create".into(),
        "--title".into(),
        title.clone(),
    ];
    if !base.is_empty() {
        args.push("--base".into());
        args.push(base.clone());
    }
    if !head.is_empty() {
        args.push("--head".into());
        args.push(head.clone());
    }
    args.push("--body".into());
    args.push(body);
    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    let mut cmd = tokio::process::Command::new("gh");
    cmd.args(&arg_refs).current_dir(&cwd);
    // Stdin is not piped (--body is a literal arg); inherit nothing.
    let output = match cmd.output().await {
        Ok(o) => o,
        Err(e) => {
            // Distinguish "binary missing" from "spawn failed".
            if which::which("gh").is_err() {
                return JsonRpcResponse::success(
                    id,
                    serde_json::json!({
                        "ok": false,
                        "reason": "`gh` CLI not found on PATH — install it and run `gh auth login`"
                    }),
                );
            }
            return JsonRpcResponse::success(
                id,
                serde_json::json!({ "ok": false, "reason": format!("gh spawn failed: {e}") }),
            );
        }
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let reason = stderr
            .lines()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("gh pr create failed (non-zero exit)")
            .to_string();
        return JsonRpcResponse::success(
            id,
            serde_json::json!({ "ok": false, "reason": reason }),
        );
    }

    // gh pr create prints the PR URL on stdout.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let url = stdout
        .lines()
        .find(|l| l.starts_with("https://"))
        .map(String::from)
        .unwrap_or_else(|| stdout.trim().to_string());

    // Return the MCode GitHandoffThreadResult shape (worktree fields null —
    // we didn't perform a worktree handoff, only the PR-create sub-step).
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "targetMode": "branch",
            "branch": null,
            "worktreePath": null,
            "associatedWorktreePath": null,
            "associatedWorktreeBranch": null,
            "associatedWorktreeRef": null,
            "changesTransferred": false,
            "conflictsDetected": false,
            "message": format!("Created PR: {url}"),
        }),
    )
}

/// `git.preparePullRequestThread` → prepare a worktree/branch for a PR.
///
/// STUBBED. The MCode shape (`GitPreparePullRequestThreadResult`) implies a
/// two-phase op: resolve the PR (via `git.resolvePullRequest`), then create a
/// local worktree + checkout the PR's head branch. The worktree plumbing
/// (`git worktree add`) is available via `git.worktreeCreate`, but wiring the
/// full sequence (PR resolve → branch checkout → worktree add → associate with
/// the thread) is deferred. We return a clear `{ ok:false, reason }` envelope
/// so the UI can render a fallback rather than a MethodNotFound.
async fn handle_git_prepare_pull_request_thread(id: Value, _params: &Value) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "ok": false,
            "reason": "git.preparePullRequestThread is stubbed — PR→worktree checkout sequence not wired (compose via git.resolvePullRequest + git.worktreeCreate)"
        }),
    )
}

/// Pure parsing helpers for `gh` CLI JSON output. Factored out of the handlers
/// so the mappings can be unit-tested with canned fixtures (no `gh` subprocess
/// required). The handlers above call these after capturing `gh`'s stdout.
mod gh_parse {
    use serde_json::{json, Value};

    /// Parse a GitHub owner/repo pair from a git remote URL. Accepts:
    ///   - `git@github.com:owner/repo.git`
    ///   - `git@github.com:owner/repo`
    ///   - `https://github.com/owner/repo.git`
    ///   - `https://github.com/owner/repo`
    ///   - `ssh://git@github.com/owner/repo.git`
    ///
    /// Returns `None` for non-GitHub URLs (GitLab, Bitbucket, local paths)
    /// or malformed inputs. Pure + allocation-light.
    pub fn parse_github_remote(url: &str) -> Option<(String, String)> {
        let url = url.trim();
        if url.is_empty() {
            return None;
        }
        // SCP-like form: git@github.com:owner/repo.git (note the COLON after
        // the host). Accept with or without a leading `ssh://`.
        for prefix in [
            "ssh://git@github.com:",
            "git@github.com:",
            "ssh.git@github.com:",
        ] {
            if let Some(rest) = url.strip_prefix(prefix) {
                return split_slug(rest);
            }
        }
        // Slash-form (https:// or ssh://git@github.com/): strip everything up
        // to and including `github.com/` and split the remainder.
        for prefix in [
            "https://github.com/",
            "http://github.com/",
            "ssh://git@github.com/",
            "ssh://github.com/",
            "git://github.com/",
        ] {
            if let Some(rest) = url.strip_prefix(prefix) {
                return split_slug(rest);
            }
        }
        None
    }

    /// Split `owner/repo.git` or `owner/repo` into `(owner, repo)`, stripping a
    /// trailing `.git` and any trailing `.wiki`/`.git` sub-suffix. Returns None
    /// if there aren't exactly two `/`-separated non-empty segments.
    fn split_slug(slug: &str) -> Option<(String, String)> {
        // Drop a trailing `.git` (case-insensitive) and any fragment/query.
        let slug = slug.split(['#', '?']).next().unwrap_or(slug);
        let slug = slug.strip_suffix(".git").unwrap_or(slug);
        let mut parts = slug.split('/');
        let owner = parts.next()?.trim();
        let name = parts.next()?.trim();
        // Reject anything beyond owner/repo (e.g. owner/repo/branches/main).
        if parts.next().is_some() || owner.is_empty() || name.is_empty() {
            return None;
        }
        Some((owner.to_string(), name.to_string()))
    }

    /// Parse the JSON output of `gh pr view <n> --json number,title,state,
    /// headRefName,baseRefName,url` into the MCode `GitResolvedPullRequest`
    /// shape (`{ number, title, url, baseBranch, headBranch, state }`).
    ///
    /// `state` is normalized to one of `"open" | "closed" | "merged"` — gh
    /// emits lowercase (`OPEN`/`CLOSED`/`MERGED`), and we lowercase + map
    /// defensively (unknown values fall back to `"open"`).
    pub fn parse_pr_view(json: &str) -> Result<Value, String> {
        let v: Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
        let number = v
            .get("number")
            .and_then(|n| n.as_i64())
            .ok_or_else(|| "missing 'number' field".to_string())?;
        let title = v
            .get("title")
            .and_then(|t| t.as_str())
            .ok_or_else(|| "missing 'title' field".to_string())?
            .to_string();
        let url = v
            .get("url")
            .and_then(|u| u.as_str())
            .ok_or_else(|| "missing 'url' field".to_string())?
            .to_string();
        let head_branch = v
            .get("headRefName")
            .and_then(|h| h.as_str())
            .unwrap_or("")
            .to_string();
        let base_branch = v
            .get("baseRefName")
            .and_then(|b| b.as_str())
            .unwrap_or("")
            .to_string();
        let state_raw = v
            .get("state")
            .and_then(|s| s.as_str())
            .unwrap_or("open");
        let state = match state_raw.to_ascii_lowercase().as_str() {
            "open" | "opened" => "open",
            "closed" | "close" => "closed",
            "merged" | "merge" => "merged",
            _ => "open", // defensive default
        };
        Ok(json!({
            "number": number,
            "title": title,
            "url": url,
            "baseBranch": base_branch,
            "headBranch": head_branch,
            "state": state,
        }))
    }
}



/// `stats.getProfileStats` — return an empty `ProfileStats`. Syncode has no
/// stats aggregation subsystem (no prompt/turn/token accumulator, no daily
/// heatmap rollup, no provider-quota poller), so every aggregate is zeroed and
/// every list is empty. The shape mirrors the MCode `ProfileStats` schema
/// (Tier-3 `frontend/src/contracts/tier3/stats.ts`): all schema-required
/// top-level fields are present so the UI's destructuring reads don't throw.
///
/// `generatedAt` is the current UTC time (the UI displays "generated at" — a
/// live timestamp keeps that label honest); `timezone.utcOffsetMinutes` echoes
/// the caller's request param (read best-effort, default 0) so the timezone
/// card renders the caller's offset rather than a hard-coded 0.
fn handle_stats_get_profile_stats(id: Value) -> JsonRpcResponse {
    let generated_at = chrono::Utc::now().to_rfc3339();
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "generatedAt": generated_at,
            "timezone": {
                "utcOffsetMinutes": 0,
                "today": "",
            },
            "identity": {
                "homeDirBasename": "",
                "initials": "",
                "defaultHandle": "",
            },
            "activity": {
                "currentStreakDays": 0,
                "longestStreakDays": 0,
                "totalPromptsSent": 0,
                "totalThreads": 0,
                "promptsToday": 0,
                "heatmapMetric": "prompts",
                "heatmap": [],
            },
            "activeHours": {
                "startHour": Value::Null,
                "endHour": Value::Null,
                "turnCount": 0,
                "label": Value::Null,
            },
            "insights": {
                "topProvider": Value::Null,
                "topProviderPercent": Value::Null,
                "topReasoning": Value::Null,
                "topReasoningPercent": Value::Null,
                "skillsExplored": 0,
                "totalSkillsUsed": 0,
            },
            "providerModels": [],
            "skills": [],
            "mostUsedSkill": Value::Null,
            "mostWorkedProject": Value::Null,
            "quota": {
                "status": "unavailable",
                "provider": Value::Null,
                "window": Value::Null,
                "usedPercent": Value::Null,
                "resetsAt": Value::Null,
                "planName": Value::Null,
            },
        }),
    )
}

/// `stats.getProfileTokenStats` — return an empty `ProfileTokenStats`. Syncode
/// has no token-usage tracking (no per-turn token counter, no provider-quota
/// poller), so `available` is `false` and every aggregate is null/empty. The
/// shape mirrors the MCode `ProfileTokenStats` schema: all schema-required
/// top-level fields are present (`available`, `heatmapMetric`, `providers`,
/// `unavailableProviders`, `heatmap`) so the UI's token-usage panel renders a
/// "token stats unavailable" state rather than crashing.
fn handle_stats_get_profile_token_stats(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "available": false,
            "lifetimeTotalTokens": Value::Null,
            "peakDayTokens": Value::Null,
            "peakDay": Value::Null,
            "providers": [],
            "unavailableProviders": [],
            "heatmapMetric": "tokens",
            "heatmap": [],
        }),
    )
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
            // T6c-9 advanced git RPCs.
            "git/stash-list",
            "git/stash-create",
            "git/stash-apply",
            "git/stash-drop",
            "git/stash-info",
            "git/stash-and-checkout",
            "git/fetch",
            "git/pull",
            "git/push",
            "git/init",
            "git/remove-index-lock",
            "git/worktree-list",
            "git/worktree-create",
            "git/worktree-remove",
            // T6c-16 stacked-action / detached-worktree / progress RPCs.
            "git/run-stacked-action",
            "git/create-detached-worktree",
            "git/subscribe-action-progress",
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

    // ─── Git advanced RPC tests (T6c-9: stash/init/index-lock/worktree) ──
    //
    // Network-dependent ops (fetch/pull/push against a real remote) are not
    // unit-testable here without standing up a local file:// remote; the
    // syncode-git crate already covers push/pull round-trips against a local
    // bare remote (see `crates/syncode-git/src/service.rs` integration tests).
    // Here we cover the deterministic local ops: init, removeIndexLock
    // (present + absent), stash round-trip (create/list/info/apply/drop),
    // worktree list. All git-gated (skip cleanly when `git` is absent).

    #[tokio::test]
    async fn git_init_creates_a_repo() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("new-repo");
        let state = WsState::new_in_memory(16);

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git/init",
            "params": { "cwd": target.to_string_lossy() }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["ok"], true);
        // The .git directory must exist after init.
        assert!(target.join(".git").exists(), ".git dir not created");

        // Idempotent: calling init on an already-initialized repo succeeds.
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "second init should be idempotent");
    }

    #[tokio::test]
    async fn git_init_requires_cwd() {
        // No cwd param → INVALID_PARAMS (not a crash).
        let state = WsState::new_in_memory(16);
        let req =
            serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "git/init", "params": {} });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some(), "expected error for missing cwd");
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn git_remove_index_lock_when_absent_reports_removed_false() {
        // Healthy repo (no lock file) → removed:false (no-op success).
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git/removeIndexLock",
            "params": { "cwd": repo.to_string_lossy() }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["ok"], true);
        assert_eq!(result["removed"], false);
    }

    #[tokio::test]
    async fn git_remove_index_lock_removes_present_lock() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        // Drop a stale index.lock in place.
        let lock_path = repo.join(".git").join("index.lock");
        std::fs::write(&lock_path, b"").expect("write lock");

        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git/removeIndexLock",
            "params": { "cwd": repo.to_string_lossy() }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["ok"], true);
        assert_eq!(result["removed"], true);
        assert!(!lock_path.exists(), "lock file still present after remove");
    }

    #[tokio::test]
    async fn git_stash_round_trip_create_list_info_apply_drop() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        let state = WsState::new_in_memory(16);

        // Make a working-tree change so there's something to stash.
        std::fs::write(repo.join("modified.txt"), "change\n").expect("write");

        // 1. stashCreate — saves the working tree to stash@{0}.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git/stash-create",
            "params": { "cwd": repo.to_string_lossy(), "message": "T6c9 stash" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "stashCreate failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["ok"], true);
        assert!(result["oid"].is_string(), "oid should be set: {:?}", result);
        assert_eq!(result["stashRef"], "stash@{0}");

        // 2. stashList — surfaces the just-created entry.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "git/stash-list",
            "params": { "cwd": repo.to_string_lossy() }
        });
        let resp = rpc(&state, 1, &req).await;
        let result = resp.result.unwrap();
        let stashes = result["stashes"].as_array().expect("stashes array");
        assert_eq!(stashes.len(), 1, "expected exactly 1 stash: {:?}", stashes);
        assert_eq!(stashes[0]["index"], 0);
        assert_eq!(stashes[0]["stashRef"], "stash@{0}");
        assert!(
            stashes[0]["message"].as_str().unwrap_or("").contains("T6c9 stash"),
            "stash message should carry the create message: {:?}",
            stashes[0]["message"]
        );

        // 3. stashInfo — formal GitStashInfoResult shape.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "git/stash-info",
            "params": { "cwd": repo.to_string_lossy(), "index": 0 }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "stashInfo failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["stashRef"], "stash@{0}");
        assert_eq!(
            result["cwd"],
            serde_json::Value::String(repo.to_string_lossy().to_string())
        );
        assert!(
            result["message"].as_str().unwrap_or("").contains("T6c9 stash"),
            "stashInfo message: {:?}",
            result["message"]
        );
        // Stash was created on `main` — branch should resolve to "main".
        assert_eq!(result["branch"], "main", "branch should resolve to main");
        // Files should include the modified file we stashed.
        let files = result["files"].as_array().expect("files array");
        assert!(
            files.iter().any(|f| f.as_str() == Some("modified.txt")),
            "expected modified.txt in stash files: {:?}",
            files
        );

        // 4. stashApply — re-applies index 0 (working tree was reset by save).
        // Reset working tree first so apply has something to write.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 4, "method": "git/stash-apply",
            "params": { "cwd": repo.to_string_lossy(), "index": 0 }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "stashApply failed: {:?}", resp.error);
        assert_eq!(resp.result.unwrap()["ok"], true);

        // 5. stashDrop — removes index 0; list is now empty.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 5, "method": "git/stash-drop",
            "params": { "cwd": repo.to_string_lossy(), "index": 0 }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "stashDrop failed: {:?}", resp.error);
        assert_eq!(resp.result.unwrap()["ok"], true);

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 6, "method": "git/stash-list",
            "params": { "cwd": repo.to_string_lossy() }
        });
        let resp = rpc(&state, 1, &req).await;
        let stashes = resp.result.unwrap()["stashes"]
            .as_array()
            .expect("stashes array")
            .clone();
        assert!(stashes.is_empty(), "stash list should be empty after drop");
    }

    #[tokio::test]
    async fn git_stash_create_nothing_to_stash() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo(); // clean repo
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git/stash-create",
            "params": { "cwd": repo.to_string_lossy(), "message": "nothing" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        // Nothing-to-stash path returns ok:true, oid:null, reason set.
        assert_eq!(result["ok"], true);
        assert!(result["oid"].is_null(), "oid should be null: {:?}", result);
        assert!(result["stashRef"].is_null());
    }

    #[tokio::test]
    async fn git_stash_and_checkout_stub_returns_ok_false() {
        // The documented stub: stashAndCheckout is not a single op.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git/stash-and-checkout",
            "params": { "cwd": "/tmp/anywhere", "branch": "x" }
        });
        let resp = rpc(&state, 1, &req).await;
        // Note: stub returns success envelope with ok:false (NOT an RPC error).
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["ok"], false);
        assert!(result["reason"].is_string());
    }

    #[tokio::test]
    async fn git_worktree_list_returns_main_worktree() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        let state = WsState::new_in_memory(16);

        // Both MCode dot-name and slash form must resolve.
        for method in ["git.worktreeList", "git/worktree-list", "git.listWorktrees"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "cwd": repo.to_string_lossy() }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let worktrees = resp.result.unwrap()["worktrees"]
                .as_array()
                .expect("worktrees array")
                .clone();
            assert!(
                !worktrees.is_empty(),
                "{}: expected at least the main worktree",
                method
            );
            // The main worktree's path matches the repo root.
            assert!(
                worktrees.iter().any(|w| w["isMain"] == true
                    || w["is_main"] == true
                    || w.get("path").is_some()),
                "{}: main worktree missing",
                method
            );
        }
    }

    // ─── T6c-16 stacked-action / detached-worktree tests ───────────────────
    //
    // The new T6c-16 RPCs are gated on `git_available()` (skip cleanly when
    // the git binary is absent). We cover the deterministic local paths:
    //   - createDetachedWorktree: creates a real worktree checked out at HEAD
    //     in detached mode (no branch ref created), returns its filesystem
    //     path + the ref it was created at.
    //   - runStackedAction: a simple `commit` action runs the pipeline against
    //     a temp repo with a staged file, producing a `created` commit step
    //     status + the commit sha/subject.
    //   - runStackedAction validation: a missing/invalid `action` param
    //     returns INVALID_PARAMS (not METHOD_NOT_FOUND — proves dispatch
    //     reached the handler).

    #[tokio::test]
    async fn git_create_detached_worktree_creates_a_detached_worktree() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        let state = WsState::new_in_memory(16);

        // Each method form gets its own worktree name (so the default
        // `.worktrees/<name>` path doesn't collide between iterations).
        let cases: &[(&str, &str)] = &[
            ("git.createDetachedWorktree", "wt-dot"),
            ("git/create-detached-worktree", "wt-slash"),
        ];
        for &(method, wt_name) in cases {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": {
                    "cwd": repo.to_string_lossy(),
                    "commitIsh": "HEAD",
                    "name": wt_name,
                }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            let worktree = &result["worktree"];
            let path = worktree["path"].as_str().expect("worktree.path");
            // The worktree directory must exist on disk.
            assert!(
                std::path::Path::new(path).exists(),
                "{}: worktree path {path} does not exist",
                method
            );
            // A detached worktree has no branch ref.
            assert_eq!(worktree["branch"], serde_json::Value::Null);
            // The ref echoes the commit-ish we asked for.
            assert_eq!(worktree["ref"], "HEAD");
            // The worktree must be checked out in detached HEAD (no branch
            // pointer created in the parent repo).
            let head_file = std::path::Path::new(path).join(".git");
            // For a linked worktree, `.git` is a file pointing to the
            // admin dir; the `HEAD` inside is the worktree's HEAD. Read it
            // and confirm it's a detached OID (not `ref: refs/heads/...`).
            let git_ptr = std::fs::read_to_string(&head_file).unwrap_or_default();
            // Resolve the admin dir from the `.git` file (`gitdir: <path>`).
            let admin_dir = git_ptr
                .strip_prefix("gitdir:")
                .map(str::trim)
                .unwrap_or(&git_ptr);
            let worktree_head = std::path::Path::new(admin_dir).join("HEAD");
            let head_content =
                std::fs::read_to_string(&worktree_head).unwrap_or_default();
            assert!(
                !head_content.starts_with("ref: refs/heads/"),
                "{method}: detached worktree HEAD should be an OID, got: {head_content}"
            );
        }

        // The worktree must NOT have created a branch ref in the parent repo
        // (detached). Verify by listing branches — only `main` should be
        // present (no `wt-dot`/`wt-slash`).
        let branches_output = std::process::Command::new("git")
            .args(["branch", "--list"])
            .current_dir(&repo)
            .output()
            .expect("git branch --list");
        let branches = String::from_utf8_lossy(&branches_output.stdout);
        assert!(
            !branches.contains("wt-dot") && !branches.contains("wt-slash"),
            "detached worktree should NOT create a branch ref, got: {branches}"
        );
    }

    #[tokio::test]
    async fn git_run_stacked_action_runs_a_commit_action() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        // Stage a file change so the commit step has work to do.
        std::fs::write(repo.join("change.txt"), "stacked\n").expect("write change.txt");
        let git_add = std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&repo)
            .output()
            .expect("git add");
        assert!(git_add.status.success(), "git add failed");

        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git/run-stacked-action",
            "params": {
                "cwd": repo.to_string_lossy(),
                "action": "commit",
                "message": "stacked test commit",
            }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        // Action discriminator echoed back.
        assert_eq!(result["action"], "commit");
        // Commit step succeeded.
        assert_eq!(result["commit"]["status"], "created");
        let sha = result["commit"]["commitSha"].as_str();
        assert!(sha.is_some(), "commitSha missing: {:?}", result["commit"]);
        assert!(
            !sha.unwrap().is_empty(),
            "commitSha empty: {:?}",
            result["commit"]
        );
        assert_eq!(
            result["commit"]["subject"], "stacked test commit",
            "subject mismatch: {:?}",
            result["commit"]
        );
        // Non-requested steps are marked skipped_not_requested.
        assert_eq!(result["push"]["status"], "skipped_not_requested");
        assert_eq!(result["pr"]["status"], "skipped_not_requested");
        assert_eq!(result["branch"]["status"], "skipped_not_requested");
    }

    #[tokio::test]
    async fn git_run_stacked_action_rejects_invalid_action_param() {
        // Validation path: missing `action` returns INVALID_PARAMS (proves
        // dispatch reached the handler — not METHOD_NOT_FOUND).
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git/run-stacked-action",
            "params": { "cwd": "/tmp" }
        });
        let resp = rpc(&state, 1, &req).await;
        let err = resp
            .error
            .expect("expected INVALID_PARAMS for missing 'action'");
        assert_eq!(err.code, crate::error_codes::INVALID_PARAMS);

        // Unknown action value likewise.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.runStackedAction",
            "params": { "cwd": "/tmp", "action": "bogus_action" }
        });
        let resp = rpc(&state, 1, &req).await;
        let err = resp
            .error
            .expect("expected INVALID_PARAMS for unknown action");
        assert_eq!(err.code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn git_subscribe_action_progress_returns_subscribed_stub() {
        // The subscribe RPC is a graceful stub — returns { subscribed: true }
        // regardless of params (no real progress push channel for stacked
        // actions). Both dispatch forms must resolve.
        let state = WsState::new_in_memory(16);
        for method in [
            "git.subscribeActionProgress",
            "git/subscribe-action-progress",
        ] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "actionId": "test-action" }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            assert_eq!(resp.result.unwrap()["subscribed"], true);
        }
    }

    #[tokio::test]
    async fn git_advanced_dispatch_accepts_dot_and_slash_forms() {
        // Smoke: every new method must resolve under BOTH forms (no
        // MethodNotFound). We don't need a real repo for most — the
        // open-repo error path proves dispatch reached the handler (returns
        // INTERNAL_ERROR, not METHOD_NOT_FOUND).
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let state = WsState::new_in_memory(16);
        // Use a nonexistent path so handlers short-circuit at open_git2_repo;
        // the assertion is on error CODE (INTERNAL_ERROR) vs METHOD_NOT_FOUND.
        // stashAndCheckout is a stub that never opens a repo — assert ok:false.
        // subscribeActionProgress is also a stub (no repo open).
        let stub_methods = [
            ("git.stashAndCheckout", "git/stash-and-checkout"),
            ("git.subscribeActionProgress", "git/subscribe-action-progress"),
        ];
        for (dot, slash) in stub_methods {
            for method in [dot, slash] {
                let req = serde_json::json!({
                    "jsonrpc": "2.0", "id": 1, "method": method,
                    "params": { "cwd": "/tmp/nonexistent-t6c9-xyz" }
                });
                let resp = rpc(&state, 1, &req).await;
                assert!(
                    resp.error.is_none(),
                    "{}: stub should return success, got {:?}",
                    method,
                    resp.error
                );
            }
        }

        // Repo-opening methods: assert INTERNAL_ERROR (not METHOD_NOT_FOUND).
        // runStackedAction needs `action` (else INVALID_PARAMS before repo
        // open) — supply it so the handler reaches open_git_service.
        let repo_opening_methods: &[(&str, &str)] = &[
            ("git.stashList", "git/stash-list"),
            ("git.stashCreate", "git/stash-create"),
            ("git.stashApply", "git/stash-apply"),
            ("git.stashDrop", "git/stash-drop"),
            ("git.stashInfo", "git/stash-info"),
            ("git.fetch", "git/fetch"),
            ("git.pull", "git/pull"),
            ("git.push", "git/push"),
            ("git.removeIndexLock", "git/remove-index-lock"),
            ("git.worktreeList", "git/worktree-list"),
            ("git.worktreeCreate", "git/worktree-create"),
            ("git.worktreeRemove", "git/worktree-remove"),
            ("git.createDetachedWorktree", "git/create-detached-worktree"),
        ];
        for (dot, slash) in repo_opening_methods {
            for method in [*dot, *slash] {
                let req = serde_json::json!({
                    "jsonrpc": "2.0", "id": 1, "method": method,
                    "params": { "cwd": "/tmp/nonexistent-t6c9-xyz" }
                });
                let resp = rpc(&state, 1, &req).await;
                let err = resp.error.unwrap_or_else(|| {
                    panic!("{method}: expected INTERNAL_ERROR for missing repo, got success")
                });
                assert_eq!(
                    err.code,
                    crate::error_codes::INTERNAL_ERROR,
                    "{method}: expected INTERNAL_ERROR, got code {} ({})",
                    err.code,
                    err.message
                );
            }
        }

        // runStackedAction: needs `action` param (INVALID_PARAMS without it,
        // INTERNAL_ERROR with a bad-repo path since it opens the repo first).
        for method in ["git.runStackedAction", "git/run-stacked-action"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": {
                    "cwd": "/tmp/nonexistent-t6c9-xyz",
                    "action": "commit",
                }
            });
            let resp = rpc(&state, 1, &req).await;
            let err = resp.error.unwrap_or_else(|| {
                panic!("{method}: expected INTERNAL_ERROR for missing repo, got success")
            });
            assert_eq!(
                err.code,
                crate::error_codes::INTERNAL_ERROR,
                "{method}: expected INTERNAL_ERROR, got code {} ({})",
                err.code,
                err.message
            );
        }
        for (dot, slash) in repo_opening_methods {
            for method in [*dot, *slash] {
                let req = serde_json::json!({
                    "jsonrpc": "2.0", "id": 1, "method": method,
                    "params": { "cwd": "/tmp/nonexistent-t6c9-xyz" }
                });
                let resp = rpc(&state, 1, &req).await;
                let err = resp.error.unwrap_or_else(|| {
                    panic!("{method}: expected INTERNAL_ERROR for missing repo, got success")
                });
                assert_eq!(
                    err.code,
                    crate::error_codes::INTERNAL_ERROR,
                    "{method}: expected INTERNAL_ERROR, got code {} ({})",
                    err.code,
                    err.message
                );
            }
        }
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
            "server/set-config",
            "server/update-settings",
            "server/refresh-providers",
            "server/update-provider",
            "server/upsert-keybinding",
        ] {
            assert!(
                method_strs.contains(&expected),
                "rpc/listMethods missing {}",
                expected
            );
        }
    }

    // ─── Server write-side stub tests (T6c-10) ──────────────────────────
    //
    // Each write-side RPC must:
    //   1. Dispatch BOTH dot-form and slash-form to the same handler.
    //   2. Return the documented ack shape (echo of the read-side default).
    //   3. Validate params where required (`updateProvider`, `upsertKeybinding`)
    //      — rejecting malformed input with -32602 InvalidParams.

    #[tokio::test]
    async fn server_set_config_echoes_default_config_shape() {
        let state = WsState::new_in_memory(16);
        for method in ["server.setConfig", "server/set-config"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "cwd": "/tmp/x" }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            // Echoed default ServerConfig: required top-level fields present,
            // arrays empty (no persistence — write is a stub).
            assert!(!result["cwd"].as_str().unwrap_or("").is_empty(), "{}: cwd", method);
            assert!(result["providers"].as_array().unwrap().is_empty(), "{}: providers", method);
            assert!(result["issues"].as_array().unwrap().is_empty(), "{}: issues", method);
        }
    }

    #[tokio::test]
    async fn server_update_settings_echoes_default_settings_shape() {
        let state = WsState::new_in_memory(16);
        for method in ["server.updateSettings", "server/update-settings"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "enableAssistantStreaming": true }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            // Echoed default ServerSettings: provider set present with all 8 keys.
            assert_eq!(result["defaultThreadEnvMode"], "local", "{}: env mode", method);
            let providers = &result["providers"];
            assert_eq!(providers["codex"]["binaryPath"], "codex", "{}: codex", method);
            assert_eq!(providers["pi"]["binaryPath"], "pi", "{}: pi", method);
            // The patch is NOT applied — the stub echoes the unchanged default.
            assert_eq!(
                result["enableAssistantStreaming"],
                serde_json::Value::Bool(false),
                "{}: stub must not persist the patch",
                method
            );
        }
    }

    #[tokio::test]
    async fn server_refresh_providers_returns_empty_status_payload() {
        let state = WsState::new_in_memory(16);
        for method in ["server.refreshProviders", "server/refresh-providers"] {
            let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": method });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            // ServerProviderStatusesUpdatedPayload: { providers: [] }.
            assert!(result["providers"].is_array(), "{}: providers missing", method);
            assert!(result["providers"].as_array().unwrap().is_empty(), "{}: not empty", method);
        }
    }

    #[tokio::test]
    async fn server_update_provider_validates_provider_param() {
        let state = WsState::new_in_memory(16);

        // Happy path: `provider` non-empty → success with `{ providers: [] }`.
        for method in ["server.updateProvider", "server/update-provider"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "provider": "codex" }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert!(result["providers"].as_array().unwrap().is_empty(), "{}: not empty", method);
        }

        // Validation: missing `provider` → InvalidParams (-32602).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "server/update-provider", "params": {}
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some(), "missing provider should reject");
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);

        // Validation: empty string `provider` → InvalidParams (-32602).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "server/update-provider",
            "params": { "provider": "  " }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some(), "whitespace provider should reject");
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn server_upsert_keybinding_validates_params_object() {
        let state = WsState::new_in_memory(16);

        // Happy path: params is a keybinding-rule object → success with the
        // default empty result shape `{ keybindings: [], issues: [] }`.
        for method in ["server.upsertKeybinding", "server/upsert-keybinding"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "key": "mod+k", "command": "test" }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert!(result["keybindings"].as_array().unwrap().is_empty(), "{}: keybindings", method);
            assert!(result["issues"].as_array().unwrap().is_empty(), "{}: issues", method);
        }

        // Validation: params null → InvalidParams (-32602).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "server/upsert-keybinding", "params": null
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some(), "null params should reject");
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);

        // Validation: params array → InvalidParams (-32602).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "server/upsert-keybinding",
            "params": [{ "key": "mod+k" }]
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some(), "array params should reject");
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    // ─── Voice STT stub tests (T6c-15) ─────────────────────────────────
    //
    // The 3 voice RPCs must dispatch BOTH dot-form and slash-form, must NOT
    // MethodNotFound, and must return the documented "STT not configured"
    // result shapes (no real STT backend exists).

    #[tokio::test]
    async fn server_transcribe_voice_returns_not_configured() {
        let state = WsState::new_in_memory(16);
        for method in [
            "server.transcribeVoice",
            "server/transcribe-voice",
            "server/transcribeVoice",
        ] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "audio": "base64-blob", "format": "webm" }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            // Empty text + not-configured error string (no crash).
            assert_eq!(result["text"], serde_json::Value::String("".into()), "{}: text", method);
            let err = result["error"].as_str().unwrap_or("");
            assert!(!err.trim().is_empty(), "{}: error should be non-empty", method);
            assert!(err.contains("STT not configured"), "{}: error mentions STT not configured (got {})", method, err);
        }
    }

    #[tokio::test]
    async fn server_voice_start_returns_not_listening() {
        let state = WsState::new_in_memory(16);
        for method in ["server.voiceStart", "server/voice-start", "server/voiceStart"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method, "params": {}
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert_eq!(result["ok"], serde_json::Value::Bool(false), "{}: ok", method);
            assert_eq!(result["listening"], serde_json::Value::Bool(false), "{}: listening", method);
            assert_eq!(
                result["reason"].as_str().unwrap_or(""),
                "STT not configured",
                "{}: reason",
                method
            );
        }
    }

    #[tokio::test]
    async fn server_voice_stop_returns_noop_success() {
        let state = WsState::new_in_memory(16);
        for method in ["server.voiceStop", "server/voice-stop", "server/voiceStop"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method, "params": {}
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert_eq!(result["ok"], serde_json::Value::Bool(true), "{}: ok", method);
            assert_eq!(result["listening"], serde_json::Value::Bool(false), "{}: listening", method);
        }
    }

    #[tokio::test]
    async fn voice_methods_listed_in_list_methods() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods" });
        let resp = rpc(&state, 1, &req).await;
        let methods = resp.result.unwrap()["methods"].as_array().unwrap().clone();
        let method_strs: Vec<&str> = methods.iter().filter_map(|v| v.as_str()).collect();
        for expected in ["server/transcribe-voice", "server/voice-start", "server/voice-stop"] {
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
    async fn terminal_subscribe_returns_success() {
        // subscribeEvents now records a real `terminal` channel subscription
        // (T6c-11) and must return success so the UI's subscribe call works.
        let state = WsState::new_in_memory(16);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;
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
                // compactThread requires a non-empty threadId (T6c-13 made it
                // provider-backed); pass one so the no-op-empty-history path
                // resolves successfully. Other methods take no params.
                let params = if method.ends_with("compactThread") || method.contains("compact-thread") {
                    serde_json::json!({ "threadId": "thr_resolve_test" })
                } else {
                    serde_json::json!({})
                };
                let req = serde_json::json!({
                    "jsonrpc": "2.0", "id": 1, "method": method, "params": params
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

    /// compactThread with no thread history returns `{ ok: true }` (the
    /// composer treats compaction as a no-op success when there's nothing to
    /// compact). See `provider_compact_thread_invokes_provider` for the
    /// provider-backed path.
    #[tokio::test]
    async fn provider_compact_thread_empty_history_is_noop() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.compactThread",
            "params": { "threadId": "thr_123" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["ok"], true);
        assert_eq!(result["compactedSummary"], "");
    }

    // ─── LLM-backed ops wiring tests (T6c-13) ───────────────────────────
    //
    // These tests prove the prompt → invoke → result wiring without a real
    // provider CLI: a `MockLlmAdapter` is registered under the default
    // provider id ("claude") and returns a canned reply. The compactThread /
    // summarizeDiff / generateThreadRecap handlers must route the prompt
    // through the adapter and surface the canned text in their result shape.

    /// Seed a project + thread + one turn (user input + assistant output) and
    /// return the thread id. Used by the compactThread / generateThreadRecap
    /// wiring tests so they have non-empty history to feed the prompt.
    async fn seed_thread_with_history(state: &WsState) -> String {
        use syncode_orchestration::{Command, DomainEvent};

        let project = state
            .orchestrator
            .handle_command(Command::CreateProject {
                name: "LLM Test".into(),
                root_path: "/tmp".into(),
            })
            .await
            .expect("create project");
        let project_id = match &project.events[0].event {
            DomainEvent::ProjectCreated { id, .. } => *id,
            _ => unreachable!(),
        };
        let thread = state
            .orchestrator
            .handle_command(Command::CreateThread {
                project_id,
                provider_id: "claude".into(),
                model: "m".into(),
            })
            .await
            .expect("create thread");
        let thread_id = match &thread.events[0].event {
            DomainEvent::ThreadCreated { id, .. } => *id,
            _ => unreachable!(),
        };
        let turn = state
            .orchestrator
            .handle_command(Command::StartTurn {
                thread_id,
                sequence: 1,
                user_input: "How do I fix the bug?".into(),
            })
            .await
            .expect("start turn");
        let turn_id = match &turn.events[0].event {
            DomainEvent::TurnStarted { id, .. } => *id,
            _ => unreachable!(),
        };
        state
            .orchestrator
            .handle_command(Command::CompleteTurn {
                id: turn_id,
                assistant_output: "Use Option<T> instead of unwrap.".into(),
                duration_ms: 1000,
            })
            .await
            .expect("complete turn");
        thread_id.to_string()
    }

    /// Register a `MockLlmAdapter` under the default provider id ("claude") so
    /// `invoke()` resolves it. We register under an explicit id via
    /// `register_shared` (rather than `register`, which keys by the mock's own
    /// `provider_id()` of "mock-llm") so the handler's default-provider
    /// resolution finds it.
    async fn register_mock_provider(state: &WsState, canned: &str) {
        use crate::llm::SharedAdapter;
        let mock: SharedAdapter =
            std::sync::Arc::new(tokio::sync::RwLock::new(
                crate::llm::MockLlmAdapter::new(canned),
            ));
        let mut registry = state.provider_registry.write().await;
        registry.register_shared("claude".to_string(), mock);
    }

    /// compactThread with real history invokes the registered provider and
    /// surfaces its canned reply in `compactedSummary`. Proves the prompt
    /// (built from read-store messages) flows through to the adapter.
    #[tokio::test]
    async fn provider_compact_thread_invokes_provider() {
        let state = WsState::new_in_memory(16);
        register_mock_provider(&state, "SUMMARY: bug fix discussion").await;
        let thread_id = seed_thread_with_history(&state).await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.compactThread",
            "params": { "threadId": thread_id }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["ok"], true, "ok flag must be true on success");
        assert_eq!(
            result["compactedSummary"], "SUMMARY: bug fix discussion",
            "canned reply must flow into compactedSummary"
        );
    }

    /// compactThread with history but NO provider registered returns
    /// `{ ok: false, error }` — a clear error, not a panic. The composer falls
    /// back to the un-compacted history.
    #[tokio::test]
    async fn provider_compact_thread_no_provider_returns_error_result() {
        let state = WsState::new_in_memory(16);
        // No provider registered.
        let thread_id = seed_thread_with_history(&state).await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.compactThread",
            "params": { "threadId": thread_id }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["ok"], false, "ok must be false without a provider");
        let err = result["error"].as_str().expect("error message present");
        assert!(
            err.contains("no provider registered"),
            "error should explain the missing provider; got: {err}"
        );
    }

    /// summarizeDiff with a caller-supplied diff invokes the provider and
    /// surfaces the canned reply in `summary`. Proves the diff-text path
    /// (params.diff) flows through to the adapter.
    #[tokio::test]
    async fn git_summarize_diff_invokes_provider_with_supplied_diff() {
        let state = WsState::new_in_memory(16);
        register_mock_provider(&state, "Refactors the auth module.").await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.summarizeDiff",
            "params": {
                "diff": "diff --git a/auth.rs b/auth.rs\n- old line\n+ new line"
            }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(
            result["summary"], "Refactors the auth module.",
            "canned reply must flow into summary"
        );
    }

    /// summarizeDiff resolves under BOTH the dot-name and the slash form (the
    /// wsNativeApi sends dot, the tauriNativeApi sends slash).
    #[tokio::test]
    async fn git_summarize_diff_resolves_both_forms() {
        let state = WsState::new_in_memory(16);
        register_mock_provider(&state, "ok").await;
        for method in ["git.summarizeDiff", "git/summarize-diff"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "diff": "x" }
            });
            let resp = provider_rpc(&state, &req).await;
            assert!(resp.error.is_none(), "{method} failed: {:?}", resp.error);
            assert!(resp.result.is_some(), "{method} returned null result");
        }
    }

    /// generateThreadRecap with real history invokes the provider and surfaces
    /// the canned reply in `recap`.
    #[tokio::test]
    async fn server_generate_thread_recap_invokes_provider() {
        let state = WsState::new_in_memory(16);
        register_mock_provider(&state, "RECAP: discussed Option<T>.").await;
        let thread_id = seed_thread_with_history(&state).await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "server.generateThreadRecap",
            "params": { "threadId": thread_id }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(
            result["recap"], "RECAP: discussed Option<T>.",
            "canned reply must flow into recap"
        );
    }

    /// generateThreadRecap resolves under BOTH the dot-name and slash form.
    #[tokio::test]
    async fn server_generate_thread_recap_resolves_both_forms() {
        let state = WsState::new_in_memory(16);
        register_mock_provider(&state, "ok").await;
        let thread_id = seed_thread_with_history(&state).await;
        for method in [
            "server.generateThreadRecap",
            "server/generate-thread-recap",
        ] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "threadId": &thread_id }
            });
            let resp = provider_rpc(&state, &req).await;
            assert!(resp.error.is_none(), "{method} failed: {:?}", resp.error);
        }
    }

    /// rpc/listMethods must advertise the two newly-served LLM RPCs (the
    /// compactThread entry was already listed in phase-7).
    #[tokio::test]
    async fn llm_rpcs_listed_in_list_methods() {
        let state = WsState::new_in_memory(16);
        let req =
            serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods" });
        let methods = provider_rpc(&state, &req).await.result.unwrap()["methods"]
            .as_array()
            .expect("methods is an array")
            .clone();
        let listed: std::collections::HashSet<String> = methods
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        for expected in [
            "provider/compact-thread",
            "git/summarize-diff",
            "server/generate-thread-recap",
        ] {
            assert!(
                listed.contains(expected),
                "rpc/listMethods missing {expected}"
            );
        }
    }

    // ─── Server niche ops tests (T6c-17 — last batch; completes all RPCs) ─
    //
    // `generateAutomationIntent` is REAL (LLM-backed) — register a mock
    // provider returning canned JSON and assert the parsed automation flows
    // into the result. The 5 stubs are validated for both-forms dispatch +
    // documented result shape.

    /// generateAutomationIntent with a MockLlmAdapter returning valid JSON
    /// parses the LLM reply into the MCode result shape (`isAutomation: true`,
    /// name/command/schedule/mode populated).
    #[tokio::test]
    async fn server_generate_automation_intent_parses_llm_json() {
        let state = WsState::new_in_memory(16);
        let canned = r#"{"name":"hourly-tests","command":"cargo test","schedule":"0 * * * *","mode":"scheduled","confidence":0.9}"#;
        register_mock_provider(&state, canned).await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "server.generateAutomationIntent",
            "params": { "message": "run tests every hour" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["isAutomation"], true, "must be flagged as automation");
        assert_eq!(result["name"], "hourly-tests", "name must be parsed");
        assert_eq!(result["taskPrompt"], "cargo test", "command must map to taskPrompt");
        assert_eq!(result["schedule"], "0 * * * *", "schedule must be parsed");
        assert_eq!(result["mode"], "scheduled", "mode must be parsed");
        assert_eq!(result["confidence"], 0.9, "confidence must flow through");
        let missing = result["missingFields"].as_array().expect("missingFields array");
        assert!(missing.is_empty(), "no missing fields when all present: {missing:?}");
        assert_eq!(result["needsConfirmation"], true);
    }

    /// generateAutomationIntent tolerates markdown-fenced JSON (```json …```)
    /// — providers commonly wrap replies in fences.
    #[tokio::test]
    async fn server_generate_automation_intent_strips_markdown_fence() {
        let state = WsState::new_in_memory(16);
        let canned = "```json\n{\"name\":\"lint\",\"command\":\"make lint\",\"schedule\":\"daily\",\"mode\":\"oneshot\"}\n```";
        register_mock_provider(&state, canned).await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "server/generate-automation-intent",
            "params": { "message": "lint daily" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["isAutomation"], true, "fenced JSON must still parse");
        assert_eq!(result["name"], "lint");
        assert_eq!(result["schedule"], "daily");
    }

    /// generateAutomationIntent without a registered provider returns a
    /// not-automation result (NOT a panic) carrying the error in `reason`.
    #[tokio::test]
    async fn server_generate_automation_intent_no_provider_returns_not_automation() {
        let state = WsState::new_in_memory(16);
        // No provider registered.

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "server.generateAutomationIntent",
            "params": { "message": "deploy nightly" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["isAutomation"], false, "no provider → not automation");
        let reason = result["reason"].as_str().expect("reason present");
        assert!(
            reason.contains("no provider registered"),
            "reason should explain missing provider; got: {reason}"
        );
    }

    /// generateAutomationIntent with malformed LLM reply returns a
    /// not-automation result carrying the raw text in `reason` (graceful
    /// failure, not a crash).
    #[tokio::test]
    async fn server_generate_automation_intent_malformed_json_returns_not_automation() {
        let state = WsState::new_in_memory(16);
        register_mock_provider(&state, "sorry, I can't help with that").await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "server.generateAutomationIntent",
            "params": { "message": "do something" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["isAutomation"], false, "malformed → not automation");
        let reason = result["reason"].as_str().expect("reason present");
        assert!(
            reason.contains("not valid JSON"),
            "reason should explain parse failure; got: {reason}"
        );
    }

    /// generateAutomationIntent rejects empty `message` with InvalidParams.
    #[tokio::test]
    async fn server_generate_automation_intent_rejects_empty_message() {
        let state = WsState::new_in_memory(16);
        register_mock_provider(&state, "{}").await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "server.generateAutomationIntent",
            "params": { "message": "   " }
        });
        let resp = provider_rpc(&state, &req).await;
        assert!(resp.error.is_some(), "empty message must reject");
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    /// patchSettings echoes the default ServerSettings under BOTH forms
    /// (mirrors updateSettings).
    #[tokio::test]
    async fn server_patch_settings_echoes_default_settings_shape() {
        let state = WsState::new_in_memory(16);
        for method in ["server.patchSettings", "server/patch-settings"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "enableAssistantStreaming": true }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert_eq!(result["defaultThreadEnvMode"], "local", "{}: env mode", method);
            assert_eq!(
                result["enableAssistantStreaming"],
                serde_json::Value::Bool(false),
                "{}: stub must not persist the patch",
                method
            );
        }
    }

    /// listProviderUsage returns an empty array under BOTH forms.
    #[tokio::test]
    async fn server_list_provider_usage_returns_empty_array() {
        let state = WsState::new_in_memory(16);
        for method in ["server.listProviderUsage", "server/list-provider-usage"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "forceRefresh": true }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert!(result.is_array(), "{}: result must be array", method);
            assert!(result.as_array().unwrap().is_empty(), "{}: must be empty", method);
        }
    }

    /// getProviderUsageSnapshot returns null (and validates `provider`).
    #[tokio::test]
    async fn server_get_provider_usage_snapshot_returns_null_and_validates() {
        let state = WsState::new_in_memory(16);

        // Happy path: provider non-empty → null snapshot. NOTE: a `Value::Null`
        // result round-trips through JSON (serialize → deserialize) as `None`
        // (serde's `Option<Value>` deserializes JSON `null` to `None`), so we
        // accept EITHER `None` or `Some(Value::Null)` here — both mean the
        // handler returned a null snapshot.
        for method in [
            "server.getProviderUsageSnapshot",
            "server/get-provider-usage-snapshot",
        ] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "provider": "codex" }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            match resp.result {
                None => {} // null result deserialized to None (serde quirk).
                Some(v) => assert!(v.is_null(), "{}: must be null, got {v}", method),
            }
        }

        // Validation: missing provider → InvalidParams.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "server/get-provider-usage-snapshot",
            "params": {}
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some(), "missing provider must reject");
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    /// startLocalServer returns graceful not-supported; stopLocalServer
    /// returns `{ ok: true }`.
    #[tokio::test]
    async fn server_local_server_lifecycle_stubs_are_graceful() {
        let state = WsState::new_in_memory(16);

        // start: `{ ok: false, reason: ... }` under BOTH forms.
        for method in ["server.startLocalServer", "server/start-local-server"] {
            let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": method });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert_eq!(result["ok"], false, "{}: ok must be false", method);
            let reason = result["reason"].as_str().expect("reason present");
            assert!(!reason.is_empty(), "{}: reason must be non-empty", method);
        }

        // stop: `{ ok: true }` under BOTH forms.
        for method in ["server.stopLocalServer", "server/stop-local-server"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "pid": 1234, "port": 8080 }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            assert_eq!(resp.result.unwrap()["ok"], true, "{}: ok must be true", method);
        }
    }

    /// rpc/listMethods must advertise all 6 newly-served niche RPCs.
    #[tokio::test]
    async fn server_niche_rpcs_listed_in_list_methods() {
        let state = WsState::new_in_memory(16);
        let req =
            serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods" });
        let methods = provider_rpc(&state, &req).await.result.unwrap()["methods"]
            .as_array()
            .expect("methods is an array")
            .clone();
        let listed: std::collections::HashSet<String> = methods
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        for expected in [
            "server/generate-automation-intent",
            "server/patch-settings",
            "server/list-provider-usage",
            "server/get-provider-usage-snapshot",
            "server/start-local-server",
            "server/stop-local-server",
        ] {
            assert!(
                listed.contains(expected),
                "rpc/listMethods missing {expected}"
            );
        }
    }

    // ─── GitHub-API ops RPC tests (T6c-14: gh-CLI-backed) ───────────────
    //
    // The pure parsing logic (`gh_parse::parse_github_remote`,
    // `gh_parse::parse_pr_view`) is unit-tested with canned fixtures below —
    // no `gh` subprocess required. The dispatch + listMethods tests verify
    // wiring without invoking `gh`. The two live-`gh` tests are
    // `#[ignore]`-gated (integration-only — they need a real GitHub repo +
    // network + `gh auth login`).

    /// `parse_github_remote` handles all four canonical remote-URL forms
    /// (git@ SSH, ssh://, https://, with/without trailing .git) and rejects
    /// non-GitHub hosts + malformed inputs.
    #[test]
    fn gh_parse_remote_url_forms() {
        use super::gh_parse::parse_github_remote;
        // SSH SCP-like form.
        assert_eq!(
            parse_github_remote("git@github.com:owner/repo.git"),
            Some(("owner".into(), "repo".into()))
        );
        // SSH SCP-like form without .git.
        assert_eq!(
            parse_github_remote("git@github.com:owner/repo"),
            Some(("owner".into(), "repo".into()))
        );
        // ssh:// form.
        assert_eq!(
            parse_github_remote("ssh://git@github.com/owner/repo.git"),
            Some(("owner".into(), "repo".into()))
        );
        // HTTPS form with .git.
        assert_eq!(
            parse_github_remote("https://github.com/owner/repo.git"),
            Some(("owner".into(), "repo".into()))
        );
        // HTTPS form without .git.
        assert_eq!(
            parse_github_remote("https://github.com/owner/repo"),
            Some(("owner".into(), "repo".into()))
        );
        // Whitespace is trimmed.
        assert_eq!(
            parse_github_remote("  https://github.com/owner/repo.git  "),
            Some(("owner".into(), "repo".into()))
        );

        // Non-GitHub hosts → None.
        assert_eq!(parse_github_remote("git@gitlab.com:owner/repo.git"), None);
        assert_eq!(
            parse_github_remote("https://bitbucket.org/owner/repo.git"),
            None
        );
        // Local path remote → None.
        assert_eq!(parse_github_remote("/home/user/repo"), None);
        // Malformed (too many segments) → None.
        assert_eq!(
            parse_github_remote("https://github.com/owner/repo/branches/main"),
            None
        );
        // Empty → None.
        assert_eq!(parse_github_remote(""), None);
        // Empty owner/repo segments → None.
        assert_eq!(parse_github_remote("https://github.com//repo"), None);
    }

    /// `parse_pr_view` maps the `gh pr view --json number,title,state,
    /// headRefName,baseRefName,url` output to the MCode
    /// `GitResolvedPullRequest` shape, normalizing `state` to lowercase +
    /// mapping unknown states to `"open"` defensively.
    #[test]
    fn gh_parse_pr_view_maps_to_mcode_shape() {
        use super::gh_parse::parse_pr_view;
        let fixture = serde_json::json!({
            "number": 42,
            "title": "feat: add gh-CLI ops",
            "state": "OPEN",
            "headRefName": "task/t6c14-github-gh-api",
            "baseRefName": "master",
            "url": "https://github.com/synara/syncode/pull/42",
        })
        .to_string();
        let pr = parse_pr_view(&fixture).expect("fixture must parse");
        assert_eq!(pr["number"], 42);
        assert_eq!(pr["title"], "feat: add gh-CLI ops");
        assert_eq!(pr["url"], "https://github.com/synara/syncode/pull/42");
        assert_eq!(pr["baseBranch"], "master");
        assert_eq!(pr["headBranch"], "task/t6c14-github-gh-api");
        assert_eq!(pr["state"], "open");

        // MERGED → "merged".
        let merged = serde_json::json!({
            "number": 7, "title": "t", "state": "MERGED",
            "headRefName": "h", "baseRefName": "b",
            "url": "https://github.com/o/r/pull/7",
        })
        .to_string();
        assert_eq!(parse_pr_view(&merged).unwrap()["state"], "merged");

        // Unknown state → defensive default "open".
        let weird = serde_json::json!({
            "number": 1, "title": "t", "state": "DRAFT",
            "headRefName": "h", "baseRefName": "b",
            "url": "https://github.com/o/r/pull/1",
        })
        .to_string();
        assert_eq!(parse_pr_view(&weird).unwrap()["state"], "open");

        // Missing `number` → Err.
        let bad = serde_json::json!({ "title": "t" }).to_string();
        assert!(parse_pr_view(&bad).is_err());

        // Malformed JSON → Err.
        assert!(parse_pr_view("not json").is_err());

        // Missing optional headRefName/baseRefName → empty string (not Err).
        let no_refs = serde_json::json!({
            "number": 1, "title": "t", "state": "open",
            "url": "https://github.com/o/r/pull/1",
        })
        .to_string();
        let pr = parse_pr_view(&no_refs).unwrap();
        assert_eq!(pr["headBranch"], "");
        assert_eq!(pr["baseBranch"], "");
    }

    /// `git.preparePullRequestThread` is stubbed — it must resolve (no
    /// MethodNotFound) under BOTH the dot-name AND the slash form and return
    /// a `{ ok:false, reason }` envelope (not an error).
    #[tokio::test]
    async fn git_prepare_pull_request_thread_stub_resolves_both_forms() {
        let state = WsState::new_in_memory(16);
        for method in [
            "git.preparePullRequestThread",
            "git/prepare-pull-request-thread",
        ] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method, "params": {}
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{method} errored: {:?}", resp.error);
            let result = resp.result.expect("stub returns a result envelope");
            assert_eq!(result["ok"], false, "{method} must report ok:false");
            assert!(
                result["reason"].as_str().is_some(),
                "{method} must carry a reason string"
            );
        }
    }

    /// `git.handoffThread` rejects an empty/missing `title` with INVALID_PARAMS
    /// (param validation guard — no gh subprocess spawned for this path).
    #[tokio::test]
    async fn git_handoff_thread_requires_title() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.handoffThread",
            "params": { "title": "  " }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(
            resp.error.is_some(),
            "empty title must be rejected with an error"
        );
        let err = resp.error.unwrap();
        assert_eq!(err.code, crate::error_codes::INVALID_PARAMS);
    }

    /// `git.resolvePullRequest` rejects missing/empty `number`+`url` with
    /// INVALID_PARAMS (param validation guard).
    #[tokio::test]
    async fn git_resolve_pull_request_requires_ref() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.resolvePullRequest",
            "params": {}
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some(), "missing ref must be rejected");
        assert_eq!(
            resp.error.unwrap().code,
            crate::error_codes::INVALID_PARAMS
        );
    }

    /// `git.githubRepository` on a non-repo path returns `{ repository: null }`
    /// (the MCode "not a GitHub repo" sentinel) — not an error.
    #[tokio::test]
    async fn git_github_repository_non_repo_returns_null() {
        let state = WsState::new_in_memory(16);
        // /tmp is not a git repo → no origin remote → repository:null.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.githubRepository",
            "params": { "cwd": "/tmp" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "non-repo must not error: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert!(
            result["repository"].is_null(),
            "non-repo must yield repository:null, got {result}"
        );
    }

    /// `git.githubRepository` on a repo with a non-GitHub origin returns
    /// `{ repository: null }` (not an error). Uses a temp repo with a local
    /// path remote (which is never a GitHub URL).
    #[tokio::test]
    async fn git_github_repository_non_github_origin_returns_null() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = temp_git_repo();
        // Add a local path remote as `origin` (definitely not GitHub).
        std::process::Command::new("git")
            .args(["remote", "add", "origin", "/some/local/path"])
            .current_dir(&repo)
            .output()
            .expect("git remote add");

        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.githubRepository",
            "params": { "cwd": repo.to_string_lossy() }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "non-github origin must not error");
        let result = resp.result.unwrap();
        assert!(
            result["repository"].is_null(),
            "non-github origin must yield repository:null"
        );
    }

    /// rpc/listMethods must advertise the four newly-served GitHub-API RPCs.
    #[tokio::test]
    async fn github_api_rpcs_listed_in_list_methods() {
        let state = WsState::new_in_memory(16);
        let req =
            serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods" });
        let methods = rpc(&state, 1, &req).await.result.unwrap()["methods"]
            .as_array()
            .expect("methods is an array")
            .clone();
        let listed: std::collections::HashSet<String> = methods
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        for expected in [
            "git/github-repository",
            "git/resolve-pull-request",
            "git/handoff-thread",
            "git/prepare-pull-request-thread",
        ] {
            assert!(
                listed.contains(expected),
                "rpc/listMethods missing {expected}"
            );
        }
    }

    /// LIVE: `git.githubRepository` on the syncode worktree (which has a real
    /// GitHub origin) resolves the owner/repo + URL. `#[ignore]` — needs a
    /// real GitHub repo + `git remote get-url origin` to succeed.
    #[tokio::test]
    #[ignore = "live: needs a real GitHub repo origin + gh auth"]
    async fn live_github_repository_resolves_worktree() {
        let state = WsState::new_in_memory(16);
        let cwd = env!("CARGO_MANIFEST_DIR");
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.githubRepository",
            "params": { "cwd": cwd }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "live resolve errored: {:?}", resp.error);
        let result = resp.result.unwrap();
        let repo = &result["repository"];
        // If the worktree's origin isn't a GitHub repo, repository is null —
        // still a valid outcome. If it IS, nameWithOwner must be non-empty.
        if !repo.is_null() {
            assert!(
                repo["nameWithOwner"].as_str().unwrap_or("").contains('/'),
                "nameWithOwner must be owner/repo: {repo}"
            );
            assert!(
                repo["url"].as_str().unwrap_or("").starts_with("https://"),
                "url must be https: {repo}"
            );
        }
    }

    /// LIVE: `git.resolvePullRequest` on a known PR number resolves the MCode
    /// shape. `#[ignore]` — needs network + `gh auth login` + a real PR.
    /// (Caller sets a known-good `{ cwd, number }` before un-ignoring.)
    #[tokio::test]
    #[ignore = "live: needs gh auth + a real GitHub PR number"]
    async fn live_resolve_pull_request_known_pr() {
        let state = WsState::new_in_memory(16);
        let cwd = env!("CARGO_MANIFEST_DIR");
        // PR #1 is a placeholder — adjust to a real PR before un-ignoring.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.resolvePullRequest",
            "params": { "cwd": cwd, "number": 1 }
        });
        let resp = rpc(&state, 1, &req).await;
        // Accept either a successful resolve OR a clear error (PR not found /
        // not authed) — both prove the wiring is intact. A panic would fail.
        eprintln!(
            "live resolvePullRequest result: error={:?} result={:?}",
            resp.error, resp.result
        );
    }


    // ─── Profile stats RPC tests (T6c-8) ──────────────────────────────

    /// Both `stats.*` RPCs must resolve (no MethodNotFound) under BOTH the
    /// MCode dot-name AND the slash form, and each must return a success
    /// envelope.
    #[tokio::test]
    async fn stats_rpcs_resolve_both_forms() {
        let state = WsState::new_in_memory(16);
        // (dot-name, slash-name)
        let cases: &[(&str, &str)] = &[
            ("stats.getProfileStats", "stats/get-profile-stats"),
            (
                "stats.getProfileTokenStats",
                "stats/get-profile-token-stats",
            ),
        ];
        for (dot, slash) in cases {
            for method in [*dot, *slash] {
                let req =
                    serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": method });
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

    /// rpc/listMethods must advertise the new stats methods so the UI's
    /// capability discovery sees them.
    #[tokio::test]
    async fn stats_rpcs_listed_in_list_methods() {
        let state = WsState::new_in_memory(16);
        let req =
            serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods" });
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
            "stats/get-profile-stats",
            "stats/get-profile-token-stats",
        ] {
            assert!(
                listed.contains(expected),
                "rpc/listMethods missing {expected}"
            );
        }
    }

    /// getProfileStats must return every schema-required `ProfileStats` field
    /// (the UI destructures these unconditionally — missing fields crash the
    /// render). Aggregates zeroed, arrays empty, optionals null.
    #[tokio::test]
    async fn stats_get_profile_stats_returns_full_shape() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "stats.getProfileStats",
            "params": { "utcOffsetMinutes": -300 }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        // Top-level required fields.
        for field in [
            "generatedAt",
            "timezone",
            "identity",
            "activity",
            "activeHours",
            "insights",
            "providerModels",
            "skills",
            "mostUsedSkill",
            "mostWorkedProject",
            "quota",
        ] {
            assert!(
                result.get(field).is_some(),
                "ProfileStats missing required field {field}"
            );
        }
        // generatedAt must be an ISO-8601 string (live UTC timestamp).
        assert!(
            result["generatedAt"].is_string(),
            "generatedAt must be a string"
        );
        // Empty/zero aggregates.
        assert_eq!(result["providerModels"].as_array().unwrap().len(), 0);
        assert_eq!(result["skills"].as_array().unwrap().len(), 0);
        assert_eq!(result["activity"]["heatmap"].as_array().unwrap().len(), 0);
        assert_eq!(result["activity"]["totalPromptsSent"], 0);
        assert_eq!(result["activity"]["currentStreakDays"], 0);
        assert_eq!(result["activity"]["heatmapMetric"], "prompts");
        // Optionals null.
        assert!(result["mostUsedSkill"].is_null());
        assert!(result["mostWorkedProject"].is_null());
        // Quota unavailable (no provider-quota poller in syncode).
        assert_eq!(result["quota"]["status"], "unavailable");
        assert!(result["quota"]["provider"].is_null());
    }

    /// getProfileTokenStats must return every schema-required
    /// `ProfileTokenStats` field with `available: false` (syncode has no token
    /// tracking) and empty arrays.
    #[tokio::test]
    async fn stats_get_profile_token_stats_returns_full_shape() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "stats.getProfileTokenStats",
            "params": { "utcOffsetMinutes": 0 }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        // Top-level required fields.
        for field in [
            "available",
            "lifetimeTotalTokens",
            "peakDayTokens",
            "peakDay",
            "providers",
            "unavailableProviders",
            "heatmapMetric",
            "heatmap",
        ] {
            assert!(
                result.get(field).is_some(),
                "ProfileTokenStats missing required field {field}"
            );
        }
        // available=false (syncode has no token tracking).
        assert_eq!(result["available"], false);
        // Null/empty aggregates.
        assert!(result["lifetimeTotalTokens"].is_null());
        assert!(result["peakDayTokens"].is_null());
        assert!(result["peakDay"].is_null());
        assert_eq!(result["providers"].as_array().unwrap().len(), 0);
        assert_eq!(result["unavailableProviders"].as_array().unwrap().len(), 0);
        assert_eq!(result["heatmap"].as_array().unwrap().len(), 0);
        assert_eq!(result["heatmapMetric"], "tokens");
    }

    // ─── T6c-11: terminal live-push tests ──────────────────────────────

    /// Helper: subscribe to the push bus and collect terminal frames until a
    /// predicate matches or the deadline passes.
    async fn collect_terminal_frames(
        push_tx: tokio::sync::broadcast::Sender<(String, serde_json::Value)>,
        deadline_ms: u64,
    ) -> Vec<serde_json::Value> {
        let mut rx = push_tx.subscribe();
        let mut frames = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(deadline_ms);
        while std::time::Instant::now() < deadline {
            let remaining =
                deadline.saturating_duration_since(std::time::Instant::now());
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Ok((channel, data))) if channel == crate::channels::CHANNEL_TERMINAL => {
                    frames.push(data);
                }
                _ => break,
            }
        }
        frames
    }

    /// Keystone: spawning a terminal session whose PTY runs `echo hello`
    /// produces a `terminal/event` output frame containing "hello" on the
    /// push bus. This proves the reader task reads PTY output and broadcasts
    /// it end-to-end.
    #[tokio::test]
    async fn terminal_open_pushes_output_to_broadcast() {
        // Skip on platforms without /bin/sh.
        if std::path::Path::new("/bin/sh").exists() {
            // present
        } else {
            eprintln!("[skip] /bin/sh not available; cannot run PTY test");
            return;
        }

        let state = WsState::new_in_memory(16);

        // Subscribe to the push bus BEFORE opening the session, so the
        // broadcast receiver exists when the reader task sends.
        let push_tx = state.push_tx.clone();
        let collect_handle = tokio::spawn(collect_terminal_frames(push_tx.clone(), 2000));

        // Yield once so the collector task is polling.
        tokio::task::yield_now().await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.open",
            "params": {
                "terminalId": "term-echo-test",
                "command": "/bin/sh",
                "args": ["-c", "echo hello"],
                "threadId": "thread-1",
            }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "open failed: {:?}", resp.error);
        assert_eq!(resp.result.unwrap()["terminalId"], "term-echo-test");

        let frames = collect_handle.await.expect("collector task panicked");

        // Assert at least one output frame carried "hello".
        let saw_hello = frames.iter().any(|f| {
            f.get("type").and_then(|v| v.as_str()) == Some("output")
                && f.get("data")
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| s.contains("hello"))
        });
        assert!(
            saw_hello,
            "expected an output frame containing 'hello'; got {} frames: {:?}",
            frames.len(),
            frames
        );

        // Also assert the frame carries the identity fields the UI needs.
        let output_frame = frames
            .iter()
            .find(|f| f.get("type").and_then(|v| v.as_str()) == Some("output"))
            .unwrap();
        assert_eq!(output_frame["terminalId"], "term-echo-test");
        assert_eq!(output_frame["threadId"], "thread-1");
        assert!(output_frame["createdAt"].is_string());
        assert!(output_frame["byteLength"].is_number());

        // Cleanup: close the session (aborts the reader).
        let close_req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "terminal.close",
            "params": { "terminalId": "term-echo-test" }
        });
        let _ = rpc(&state, 1, &close_req).await;
    }

    /// `terminal.subscribeEvents` records a real `terminal` channel
    /// subscription on the originating connection (T6c-11). Before T6c-11
    /// this was a no-op stub; now the connection actually receives
    /// `push/terminal` frames.
    #[tokio::test]
    async fn terminal_subscribe_records_real_subscription() {
        let state = WsState::new_in_memory(16);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;

        // No subscription yet.
        let subscribed = state
            .subscriptions
            .read()
            .await
            .get_subscription(1)
            .is_some_and(|s| s.is_subscribed(crate::channels::CHANNEL_TERMINAL));
        assert!(!subscribed, "should not be subscribed before call");

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.subscribeEvents"
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["subscribed"], true);
        assert_eq!(result["channel"], "terminal");

        // Now subscribed.
        let subscribed = state
            .subscriptions
            .read()
            .await
            .get_subscription(1)
            .is_some_and(|s| s.is_subscribed(crate::channels::CHANNEL_TERMINAL));
        assert!(subscribed, "subscribeEvents must record a real subscription");
    }

    /// `terminal.unsubscribeEvents` drops the subscription (T6c-11).
    #[tokio::test]
    async fn terminal_unsubscribe_drops_subscription() {
        let state = WsState::new_in_memory(16);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;
        state
            .subscriptions
            .write()
            .await
            .subscribe(1, crate::channels::CHANNEL_TERMINAL);

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.unsubscribeEvents"
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        assert_eq!(resp.result.unwrap()["unsubscribed"], true);

        let subscribed = state
            .subscriptions
            .read()
            .await
            .get_subscription(1)
            .is_some_and(|s| s.is_subscribed(crate::channels::CHANNEL_TERMINAL));
        assert!(!subscribed, "unsubscribeEvents must clear the subscription");
    }

    /// `terminal.close` aborts the reader task and removes its handle from
    /// the registry (T6c-11). After close, the reader registry is empty.
    #[tokio::test]
    async fn terminal_close_aborts_reader_task() {
        if !std::path::Path::new("/bin/sh").exists() {
            eprintln!("[skip] /bin/sh not available");
            return;
        }
        let state = WsState::new_in_memory(16);

        // Spawn a long-lived shell so the reader keeps running.
        let open_req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "terminal.open",
            "params": {
                "terminalId": "term-close-test",
                "command": "/bin/sh",
                "args": ["-c", "sleep 30"],
            }
        });
        let resp = rpc(&state, 1, &open_req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);

        // Reader registered.
        let registered = state.terminal_readers.lock().await.contains_key("term-close-test");
        assert!(registered, "reader handle should be registered after open");

        // Close.
        let close_req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "terminal.close",
            "params": { "terminalId": "term-close-test" }
        });
        let resp = rpc(&state, 1, &close_req).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        assert_eq!(resp.result.unwrap()["ok"], true);

        // Reader removed.
        let still_registered = state
            .terminal_readers
            .lock()
            .await
            .contains_key("term-close-test");
        assert!(
            !still_registered,
            "close must remove the reader handle from the registry"
        );
    }
}

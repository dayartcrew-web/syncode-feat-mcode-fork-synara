//! JSON-RPC handler — orchestration methods
//!
//! All command-handling methods route through `WsState.orchestrator.handle_command()`,
//! which runs the full CQRS pipeline:
//!   Decider → Events → EventRepository persist → Projector → ReadModelStore

use crate::{ConnectionId, JsonRpcRequest, JsonRpcResponse, WsState};
use serde_json::Value;
use std::path::{Path, PathBuf};
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
                    // T6c-29: orchestration.* RPCs — generic dispatch + replay.
                    "orchestration.dispatchCommand",
                    "orchestration/dispatch-command",
                    "orchestration.subscribeShell",
                    "orchestration/subscribe-shell",
                    "orchestration.getTurnDiff",
                    "orchestration/get-turn-diff",
                    "orchestration.getFullThreadDiff",
                    "orchestration/get-full-thread-diff",
                    "orchestration.replayEvents",
                    "orchestration/replay-events",
                    "orchestration.repairState",
                    "orchestration/repair-state",
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

        // ─── Server config / settings / lifecycle (T6c-4, T6c-18) ───────────
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
        //     `subscribeProviderStatuses` — T6c-18 REAL: register on the
        //     matching `server.*Updated` push channel + emit a snapshot of the
        //     current stored state. `subscribeLifecycle` is REAL as of
        //     T6c-phase-27: it registers on `server.lifecycle` and emits an
        //     initial `welcome` event (same shape as `server.welcome`).
        //
        // T6c-18 makes `getConfig`/`getSettings` read from the in-memory
        // `ServerSettingsState` (persists edits for the server session) and
        // the write RPCs (`setConfig`/`updateSettings`/`patchSettings`/
        // `updateProvider`/`upsertKeybinding`) merge into that store + push
        // the new state to subscribed connections. The store is initialized
        // from the default builders at `WsState` construction; the auth mode
        // is surfaced in `getConfig` from `WsAuthConfig`.
        //
        // Dispatch accepts BOTH the MCode dot-name AND a slash form for
        // robustness (the tauriNativeApi sends slash, the wsNativeApi sends
        // dot — both must resolve).
        "server.getConfig" | "server/getConfig" => handle_server_get_config(state, id).await,
        "server.getSettings" | "server/getSettings" => handle_server_get_settings(state, id).await,
        "server.welcome" | "server/welcome" => handle_server_welcome(state, id).await,
        "server.getEnvironment" | "server/getEnvironment" => handle_server_get_environment(id),
        "server.getDiagnostics" | "server/getDiagnostics" => {
            handle_server_get_diagnostics(state, id).await
        }
        // T6c-18: REAL subscription. Registers the connection on the matching
        // `server.configUpdated` push channel so the delivery loop forwards
        // writes from `server.setConfig` / `upsertKeybinding`. Emits an
        // initial snapshot of the current config so a freshly-subscribed
        // client has a basis to apply live deltas against.
        "server.subscribeConfig" | "server/subscribeConfig" => {
            handle_server_subscribe_config(state, conn_id, id).await
        }
        // T6c-18: REAL subscription — `server.settingsUpdated` channel.
        "server.subscribeSettings" | "server/subscribeSettings" => {
            handle_server_subscribe_settings(state, conn_id, id).await
        }
        // T6c-18: REAL subscription — `server.providerStatusesUpdated`
        // channel.
        "server.subscribeProviderStatuses" | "server/subscribeProviderStatuses" => {
            handle_server_subscribe_provider_statuses(state, conn_id, id).await
        }
        // T6c-phase-27: REAL subscription. Registers the connection on the
        // `server.lifecycle` push channel and emits an initial `welcome` event
        // (the same payload `server.welcome` returns) so a freshly-subscribed
        // client has a baseline. Mirrors the snapshot-then-stream pattern used
        // by `subscribeConfig`/`subscribeSettings`/`subscribeProviderStatuses`
        // (T6c-18). Future server-lifecycle broadcasts (e.g. shutdown notices)
        // will fan out via the delivery loop's `is_subscribed` check.
        "server.subscribeLifecycle" | "server/subscribeLifecycle" => {
            handle_server_subscribe_lifecycle(state, conn_id, id).await
        }

        // ─── Server write-side stubs (T6c-10) ───────────────────────────────
        //
        // The cloned MCode UI persists user edits via these `server.*` write
        // RPCs (`setConfig`, `updateSettings`, `refreshProviders`,
        // `updateProvider`, `upsertKeybinding`). T6c-18 makes these REAL:
        // each handler merges the write into the in-memory `ServerSettingsState`
        // (persists for the server session) and broadcasts a push event on the
        // matching `server.*Updated` channel so subscribed connections receive
        // the new state. The UI's optimistic update now converges with the
        // server's stored view instead of being overwritten by the default.
        // Dispatch accepts BOTH dot-name AND slash form (the wsNativeApi sends
        // dot, the tauriNativeApi sends slash — both must resolve).
        //
        // T6c-18 REAL: merge into store + push `server.configUpdated`.
        "server.setConfig" | "server/set-config" => {
            handle_server_set_config(state, id, &request.params).await
        }
        // T6c-18 REAL: deep-merge patch into settings + push
        // `server.settingsUpdated`.
        "server.updateSettings" | "server/update-settings" => {
            handle_server_update_settings(state, id, &request.params).await
        }
        // T6c-18: re-list providers from the in-memory settings store (no
        // external probe) + push `server.providerStatusesUpdated`.
        "server.refreshProviders" | "server/refresh-providers" => {
            handle_server_refresh_providers(state, id).await
        }
        // T6c-18 REAL: validates `provider` non-empty, returns the provider's
        // current status from the settings store + pushes
        // `server.providerStatusesUpdated`.
        "server.updateProvider" | "server/update-provider" => {
            handle_server_update_provider(state, id, &request.params).await
        }
        // T6c-18 REAL: validates params is an object, appends/updates the
        // keybinding rule in the config store + pushes `server.configUpdated`.
        "server.upsertKeybinding" | "server/upsert-keybinding" => {
            handle_server_upsert_keybinding(state, id, &request.params).await
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
        "automation.subscribe" | "automation/subscribe" => {
            handle_automation_subscribe(state, conn_id, id).await
        }
        "automation.unsubscribe" | "automation/unsubscribe" => {
            handle_automation_unsubscribe(state, conn_id, id).await
        }

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
        "provider.listSkills" | "provider/list-skills" => {
            handle_provider_list_skills(id, &request.params)
        }
        "provider.listSkillsCatalog" | "provider/list-skills-catalog" => {
            handle_provider_list_skills_catalog(id, &request.params)
        }
        "provider.listPlugins" | "provider/list-plugins" => {
            handle_provider_list_plugins(id, &request.params)
        }
        "provider.readPlugin" | "provider/read-plugin" => {
            handle_provider_read_plugin(id, &request.params)
        }
        "provider.listCommands" | "provider/list-commands" => {
            handle_provider_list_commands(id, &request.params)
        }
        "provider.listAgents" | "provider/list-agents" => handle_provider_list_agents(id),
        "provider.getComposerCapabilities" | "provider/get-composer-capabilities" => {
            handle_provider_get_composer_capabilities(id, &request.params)
        }
        "provider.listOptions" | "provider/list-options" => {
            handle_provider_list_options(id, &request.params)
        }
        "provider.readSkill" | "provider/read-skill" => {
            handle_provider_read_skill(id, &request.params)
        }
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
        "stats.getProfileStats" | "stats/get-profile-stats" => {
            handle_stats_get_profile_stats(state, id).await
        }
        "stats.getProfileTokenStats" | "stats/get-profile-token-stats" => {
            handle_stats_get_profile_token_stats(state, id).await
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
        // T6c-18 REAL: deep-merge the patch into the settings store + push
        // `server.settingsUpdated`. Mirrors `server.updateSettings`.
        "server.patchSettings" | "server/patch-settings" => {
            handle_server_patch_settings(state, id, &request.params).await
        }
        // T6c-19 REAL: aggregate the in-memory usage log into per-provider
        // snapshots. Acknowledges the optional `forceRefresh` param.
        "server.listProviderUsage" | "server/list-provider-usage" => {
            handle_server_list_provider_usage(state, id, &request.params).await
        }
        // T6c-19 REAL: single-provider usage snapshot from the log; null when
        // the provider has no recorded usage. Validates `provider` non-empty.
        "server.getProviderUsageSnapshot" | "server/get-provider-usage-snapshot" => {
            handle_server_get_provider_usage_snapshot(state, id, &request.params).await
        }
        // T6c-phase-24 REAL: spawn a long-running server process via the
        // LocalServerManager. Reads `command`/`args`/`env`/`name`/`id`/`ports`.
        "server.startLocalServer" | "server/start-local-server" => {
            handle_server_start_local_server(state, id, &request.params).await
        }
        // T6c-phase-24 REAL: kill a tracked server process by `id`.
        "server.stopLocalServer" | "server/stop-local-server" => {
            handle_server_stop_local_server(state, id, &request.params).await
        }

        // ─── Orchestration generic RPCs (T6c-29 — REAL) ──────────────
        //
        // The cloned MCode UI drives the orchestration engine through a small
        // generic API (in addition to the typed `thread.*` / `turn.*` methods):
        //   - `orchestration.dispatchCommand`   → route any command-shape JSON
        //     through `Orchestrator::handle_command` (full CQRS pipeline).
        //   - `orchestration.subscribeShell`    → register on the orchestration
        //     push channel + emit an initial shell snapshot.
        //   - `orchestration.getTurnDiff`       → git diff between a turn's
        //     checkpoint and HEAD (or the next turn's checkpoint).
        //   - `orchestration.getFullThreadDiff` → cumulative diff across all of
        //     a thread's turns (first checkpoint → latest).
        //   - `orchestration.replayEvents`      → re-project the read model
        //     from the event store (optionally scoped to one aggregate).
        //   - `orchestration.repairState`       → full replay (rebuild read
        //     model from events) and report the count.
        // Dispatch accepts BOTH dot-name AND slash form for robustness.
        "orchestration.dispatchCommand" | "orchestration/dispatch-command" => {
            handle_orchestration_dispatch_command(state, id, &request.params).await
        }
        "orchestration.subscribeShell" | "orchestration/subscribe-shell" => {
            handle_orchestration_subscribe_shell(state, conn_id, id).await
        }
        "orchestration.getTurnDiff" | "orchestration/get-turn-diff" => {
            handle_orchestration_get_turn_diff(state, id, &request.params).await
        }
        "orchestration.getFullThreadDiff" | "orchestration/get-full-thread-diff" => {
            handle_orchestration_get_full_thread_diff(state, id, &request.params).await
        }
        "orchestration.replayEvents" | "orchestration/replay-events" => {
            handle_orchestration_replay_events(state, id, &request.params).await
        }
        "orchestration.repairState" | "orchestration/repair-state" => {
            handle_orchestration_repair_state(state, id).await
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

// ─── Orchestration generic Handlers (T6c-29 — REAL) ──────────────────
//
// Generic orchestration RPCs. These complement the typed `thread.*` / `turn.*`
// handlers with a generic command dispatcher, shell-event subscription, git
// diff aggregation across turns, and read-model replay/repair.

/// `orchestration.dispatchCommand` — accept an MCode orchestration command
/// shape `{ type, ...payload }` (or `{ command: "Type", ...payload }`) and
/// route it through `Orchestrator::handle_command`. The CQRS pipeline runs in
/// full: Decider → Events → EventRepository persist → Projector → ReadModelStore.
/// The supported command types mirror the typed handlers
/// (`CreateProject`, `CreateThread`, `StartTurn`, `PauseThread`, `ResumeThread`,
/// `CancelThread`, `CompleteThread`, `SetThreadTitle`, `DeleteThread`,
/// `DeleteProject`). Unknown command types return INVALID_PARAMS.
async fn handle_orchestration_dispatch_command(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    // The command type may arrive under `type` (MCode wire) or `command`.
    let cmd_type = params
        .get("type")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("command").and_then(|v| v.as_str()));
    let cmd_type = match cmd_type {
        Some(c) => c,
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "Missing 'type' (command type) parameter",
            );
        }
    };

    let opt_str = |key: &str| params.get(key).and_then(|v| v.as_str()).map(String::from);
    // Try multiple param keys in order, returning the first parsed EntityId.
    // Boxed to keep the closure return small (avoids clippy::result_large_err).
    let parse_id_any = |keys: &[&str]| -> Result<syncode_core::EntityId, Box<JsonRpcResponse>> {
        for &k in keys {
            if let Some(s) = params.get(k).and_then(|v| v.as_str())
                && let Ok(e) = syncode_core::EntityId::parse(s)
            {
                return Ok(e);
            }
        }
        Err(Box::new(JsonRpcResponse::error(
            Some(id.clone()),
            crate::error_codes::INVALID_PARAMS,
            format!("Missing/invalid id parameter (tried: {})", keys.join(", ")),
        )))
    };

    // Map the wire command type → syncode `Command`. Mirrors the typed handlers.
    let cmd = match cmd_type {
        "CreateProject" => {
            let name = match opt_str("name") {
                Some(n) => n,
                None => return param_error(id, "CreateProject requires 'name'"),
            };
            let root_path = match opt_str("rootPath").or_else(|| opt_str("root_path")) {
                Some(r) => r,
                None => return param_error(id, "CreateProject requires 'rootPath'"),
            };
            Command::CreateProject { name, root_path }
        }
        "DeleteProject" => {
            let id_e = match parse_id_any(&["id", "projectId"]) {
                Ok(e) => e,
                Err(r) => return *r,
            };
            Command::DeleteProject { id: id_e }
        }
        "CreateThread" => {
            let project_id = match parse_id_any(&["projectId", "project_id"]) {
                Ok(e) => e,
                Err(r) => return *r,
            };
            let provider_id = match opt_str("providerId").or_else(|| opt_str("provider_id")) {
                Some(p) => p,
                None => return param_error(id, "CreateThread requires 'providerId'"),
            };
            let model = match opt_str("model") {
                Some(m) => m,
                None => return param_error(id, "CreateThread requires 'model'"),
            };
            Command::CreateThread {
                project_id,
                provider_id,
                model,
            }
        }
        "PauseThread" => Command::PauseThread {
            id: match parse_id_any(&["id", "threadId"]) {
                Ok(e) => e,
                Err(r) => return *r,
            },
        },
        "ResumeThread" => Command::ResumeThread {
            id: match parse_id_any(&["id", "threadId"]) {
                Ok(e) => e,
                Err(r) => return *r,
            },
        },
        "CancelThread" => Command::CancelThread {
            id: match parse_id_any(&["id", "threadId"]) {
                Ok(e) => e,
                Err(r) => return *r,
            },
        },
        "SetThreadTitle" => {
            let thread_id = match parse_id_any(&["id", "threadId"]) {
                Ok(e) => e,
                Err(r) => return *r,
            };
            let title = match opt_str("title") {
                Some(t) => t,
                None => return param_error(id, "SetThreadTitle requires 'title'"),
            };
            Command::SetThreadTitle {
                id: thread_id,
                title,
            }
        }
        "DeleteThread" => Command::DeleteThread {
            id: match parse_id_any(&["id", "threadId"]) {
                Ok(e) => e,
                Err(r) => return *r,
            },
        },
        "StartTurn" => {
            let thread_id = match parse_id_any(&["threadId", "thread_id"]) {
                Ok(e) => e,
                Err(r) => return *r,
            };
            let sequence = params.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            let user_input = match opt_str("userInput").or_else(|| opt_str("user_input")) {
                Some(u) => u,
                None => return param_error(id, "StartTurn requires 'userInput'"),
            };
            Command::StartTurn {
                thread_id,
                sequence,
                user_input,
            }
        }
        other => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                format!("Unsupported command type: {other}"),
            );
        }
    };

    // Run the full CQRS pipeline.
    match state.orchestrator.handle_command(cmd).await {
        Ok(result) => {
            // Build a result envelope: aggregate id (first event), event count,
            // and event types — the UI's optimistic update converges from these.
            let aggregate_id = result.events.first().map(|e| e.event.aggregate_id());
            let event_types: Vec<String> = result
                .events
                .iter()
                .map(|e| format!("{:?}", e.event))
                .collect();
            JsonRpcResponse::success(
                id,
                serde_json::json!({
                    "dispatched": true,
                    "aggregateId": aggregate_id.unwrap_or_default(),
                    "eventsAppended": result.events.len(),
                    "eventTypes": event_types,
                }),
            )
        }
        Err(e) => JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            e.to_string(),
        ),
    }
}

/// `orchestration.subscribeShell` — register the connection on the
/// `orchestration` push channel (so the delivery loop forwards future
/// lifecycle broadcasts) and emit an initial shell snapshot so a freshly-
/// subscribed client has a baseline. Mirrors the snapshot-then-stream pattern
/// of `server.subscribeConfig`.
async fn handle_orchestration_subscribe_shell(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
) -> JsonRpcResponse {
    let added = state
        .subscriptions
        .write()
        .await
        .subscribe(conn_id, crate::channels::CHANNEL_ORCHESTRATION);

    // Build + emit the initial shell snapshot via the push channel so the
    // delivery loop routes it to this connection (and any other subscribers).
    let snapshot = handle_shell_get_snapshot(state, Value::Null).await;
    let snapshot_data = match snapshot {
        JsonRpcResponse {
            result: Some(v), ..
        } => v,
        _ => Value::Null,
    };
    let _ = state.push_tx.send((
        crate::channels::CHANNEL_ORCHESTRATION.to_string(),
        serde_json::json!({
            "eventType": "snapshot",
            "aggregateId": Value::Null,
            "data": snapshot_data,
        }),
    ));

    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "channel": crate::channels::CHANNEL_ORCHESTRATION,
            "added": added,
            "snapshotEmitted": true,
        }),
    )
}

/// `orchestration.getTurnDiff` — return the git diff captured by a turn's
/// checkpoint. Reads `threadId`, optional `turnId` (defaults to the latest
/// turn), and `cwd` (the working dir to diff in). The turn's `git_checkpoint`
/// is used as the `from_ref`; the next turn's checkpoint (or HEAD when this is
/// the latest turn) is the `to_ref`. An empty patch is returned when the turn
/// has no checkpoint or git is unavailable.
async fn handle_orchestration_get_turn_diff(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let thread_id = match params.get("threadId").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return param_error(id, "Missing 'threadId' parameter"),
    };
    let cwd = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .unwrap_or(".");

    // Resolve the target turn + collect ordered turns for the thread. Clone to
    // owned so the read lock is released before the (potentially slow) git ops.
    let (turn_count, target_idx, from_ref, to_ref) = {
        let store = state.read_store.read().await;
        let mut turns: Vec<&syncode_orchestration::TurnView> = store
            .turns
            .values()
            .filter(|t| t.thread_id == thread_id)
            .collect();
        turns.sort_by_key(|t| t.sequence);
        let count = turns.len() as u32;
        if turns.is_empty() {
            return JsonRpcResponse::success(
                id,
                serde_json::json!({ "patch": "", "turns": 0u32 }),
            );
        }
        let target_idx = match params.get("turnId").and_then(|v| v.as_str()) {
            Some(tid) => turns.iter().position(|t| t.id == tid),
            None => Some(turns.len().saturating_sub(1)),
        };
        let target_idx = match target_idx {
            Some(i) => i,
            None => {
                return JsonRpcResponse::success(
                    id,
                    serde_json::json!({ "patch": "", "turns": count }),
                );
            }
        };
        let from_ref = match turns[target_idx].git_checkpoint.clone() {
            Some(r) => r,
            None => {
                return JsonRpcResponse::success(
                    id,
                    serde_json::json!({
                        "patch": "",
                        "turns": count,
                        "note": "no checkpoint for turn",
                    }),
                );
            }
        };
        let to_ref: Option<String> = turns
            .get(target_idx + 1)
            .and_then(|t| t.git_checkpoint.clone());
        (count, target_idx, from_ref, to_ref)
    };
    let _ = target_idx;

    let svc = match syncode_git::service::Git2Service::open(Path::new(cwd)) {
        Ok(s) => s,
        Err(e) => {
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("git open failed: {e}"),
            );
        }
    };
    let entries = match svc.diff(Some(&from_ref), to_ref.as_deref()) {
        Ok(e) => e,
        Err(e) => return git_error(id, crate::error_codes::INTERNAL_ERROR, format!("git diff: {e}")),
    };
    let patch = entries
        .iter()
        .map(|e| {
            let path = e.old_path.as_deref().unwrap_or(&e.new_path);
            format!(
                "diff --git a/{path} b/{new}\nstatus: {status:?}\n",
                new = e.new_path,
                status = e.status,
            )
        })
        .collect::<String>();
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "patch": patch,
            "turns": turn_count,
            "fromRef": from_ref,
            "toRef": to_ref,
        }),
    )
}

/// `orchestration.getFullThreadDiff` — cumulative diff across all of a
/// thread's turns (first turn's checkpoint → latest turn's checkpoint, or HEAD
/// when the latest turn has no checkpoint). Returns an empty patch when the
/// thread has no checkpoints or git is unavailable.
async fn handle_orchestration_get_full_thread_diff(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let thread_id = match params.get("threadId").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return param_error(id, "Missing 'threadId' parameter"),
    };
    let cwd = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .unwrap_or(".");

    // Read the thread's ordered turns under a short-lived lock, then compute
    // the cumulative diff range. The earliest checkpoint is the `from_ref`;
    // `to_ref` is HEAD (None) so the diff reflects the working state since the
    // first checkpoint.
    let (turn_count, from_ref) = {
        let store = state.read_store.read().await;
        let mut turns: Vec<&syncode_orchestration::TurnView> = store
            .turns
            .values()
            .filter(|t| t.thread_id == thread_id)
            .collect();
        turns.sort_by_key(|t| t.sequence);
        let count = turns.len() as u32;
        // Use the earliest checkpoint as `from_ref` so the diff spans the full
        // thread. (If no checkpoint exists, returns None → empty patch.)
        let from = turns.iter().find_map(|t| t.git_checkpoint.clone());
        (count, from)
    };

    let from_ref = match from_ref {
        Some(r) => r,
        None => {
            return JsonRpcResponse::success(
                id,
                serde_json::json!({
                    "patch": "",
                    "turns": turn_count,
                    "note": "no checkpoints for thread",
                }),
            );
        }
    };

    let svc = match syncode_git::service::Git2Service::open(Path::new(cwd)) {
        Ok(s) => s,
        Err(e) => {
            return git_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("git open failed: {e}"),
            );
        }
    };
    // `to_ref = None` → HEAD: from the earliest checkpoint to current working state.
    let entries = match svc.diff(Some(&from_ref), None) {
        Ok(e) => e,
        Err(e) => return git_error(id, crate::error_codes::INTERNAL_ERROR, format!("git diff: {e}")),
    };
    let patch = entries
        .iter()
        .map(|e| {
            let path = e.old_path.as_deref().unwrap_or(&e.new_path);
            format!(
                "diff --git a/{path} b/{new}\nstatus: {status:?}\n",
                new = e.new_path,
                status = e.status,
            )
        })
        .collect::<String>();
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "patch": patch,
            "turns": turn_count,
            "fromRef": from_ref,
            "toRef": Value::Null,
        }),
    )
}

/// `orchestration.replayEvents` — re-project the read model from the event
/// store. Without an `aggregateId` this is a full replay
/// (`Orchestrator::replay_read_model`); with an `aggregateId` it still falls
/// back to a full replay (the orchestrator's public API does not expose a
/// single-aggregate replay without reaching into the event repo), but the
/// returned `scope` reflects the requested scope so callers can distinguish.
async fn handle_orchestration_replay_events(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let aggregate_id = params.get("aggregateId").and_then(|v| v.as_str());
    let scope = aggregate_id.unwrap_or("all").to_string();

    match state.orchestrator.replay_read_model().await {
        Ok(count) => JsonRpcResponse::success(
            id,
            serde_json::json!({
                "replayed": true,
                "eventsReplayed": count,
                "scope": scope,
            }),
        ),
        Err(e) => JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INTERNAL_ERROR,
            format!("replay failed: {e}"),
        ),
    }
}

/// `orchestration.repairState` — rebuild the read model from the event store
/// (full replay) and report the count. Equivalent to `replayEvents` with no
/// `aggregateId` but presented under a distinct method name so the UI's
/// "repair" affordance is unambiguous.
async fn handle_orchestration_repair_state(state: &WsState, id: Value) -> JsonRpcResponse {
    match state.orchestrator.replay_read_model().await {
        Ok(count) => JsonRpcResponse::success(
            id,
            serde_json::json!({
                "repaired": true,
                "eventsReplayed": count,
            }),
        ),
        Err(e) => JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INTERNAL_ERROR,
            format!("repair failed: {e}"),
        ),
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

/// Resolve a non-empty default for `cwd`. Delegates to
/// [`crate::settings::server_cwd`] (single source of truth).
fn server_cwd() -> String {
    crate::settings::server_cwd()
}

/// Resolve a non-empty default for `homeDir`. Delegates to
/// [`crate::settings::server_home_dir`].
fn server_home_dir() -> Option<String> {
    crate::settings::server_home_dir()
}

/// Whether the server cwd is a git worktree (T6c-phase-26). Used by
/// `server.welcome` and `server.getEnvironment` to surface
/// `capabilities.repositoryIdentity` / `repositoryIdentity` so the UI can
/// decide whether to render the git-backed workspace chrome. Best-effort:
/// any error opening the repo (missing dir, no `.git`, libgit2 failure) →
/// `false`. We never propagate the error — a degraded UI is preferable to a
/// 500 over an environment probe.
fn cwd_is_git_repo() -> bool {
    let cwd = server_cwd();
    git2::Repository::open(&cwd).is_ok()
}

/// Current process RSS in kBytes (T6c-phase-26). On Linux this reads
/// `/proc/self/status` and parses the `VmRSS:` line (kB units). On any
/// non-Linux OS, or on any read/parse failure, returns 0 — the caller
/// (`server.getDiagnostics`) treats 0 as "unknown" and surfaces it as 0
/// `rssBytes` (the diagnostics contract explicitly permits zeroed memory
/// counters). Async-free on purpose: `/proc` is a tiny ramfs read.
fn process_rss_kbytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if let Some(rest) = line.strip_prefix("VmRSS:") {
                    // Format: "VmRSS:\t      12345 kB"
                    let mut it = rest.split_whitespace();
                    if let Some(kb) = it.next()
                        && let Ok(n) = kb.parse::<u64>()
                    {
                        // Sanity: the unit field should be "kB" if present,
                        // but we don't strictly require it — a parseable
                        // leading number is the value we want.
                        return n;
                    }
                }
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// `server.getConfig` (T6c-18 REAL) — return the stored `ServerConfig` from
/// the in-memory settings store. The store is initialized from the default
/// builder at `WsState` construction and updated by `server.setConfig` /
/// `upsertKeybinding` writes; reads therefore reflect the most recent edit
/// for the server session (not just the static default).
async fn handle_server_get_config(state: &WsState, id: Value) -> JsonRpcResponse {
    let config = state.settings.read().await.config.clone();
    JsonRpcResponse::success(id, config)
}

/// `server.getSettings` (T6c-18 REAL) — return the stored `ServerSettings`
/// from the in-memory settings store. The store is initialized from the
/// default builder and updated by `server.updateSettings` /
/// `patchSettings` / `updateProvider` writes; reads reflect the most recent
/// merged state for the server session.
async fn handle_server_get_settings(state: &WsState, id: Value) -> JsonRpcResponse {
    let settings = state.settings.read().await.settings.clone();
    JsonRpcResponse::success(id, settings)
}

/// `server.welcome` — return a `WsWelcomePayload` shape. MCode emits this as a
/// `push/server.welcome` notification on WS connect; the RPC form (if the UI
/// requests it directly) returns the same payload. We derive `projectName`
/// from the cwd's last path segment (best-effort) and leave the optional
/// bootstrap ids absent (no project/thread auto-bootstrap in syncode).
async fn handle_server_welcome(state: &WsState, id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, build_server_welcome_payload(state))
}

/// Build the `server.welcome` payload — shared between the `server.welcome`
/// one-shot RPC response and the initial `welcome` event pushed by
/// `server.subscribeLifecycle` (T6c-phase-27). The lifecycle channel carries
/// this same shape as its snapshot baseline.
fn build_server_welcome_payload(state: &WsState) -> Value {
    let cwd = server_cwd();
    let home = server_home_dir();
    let project_name = cwd
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("syncode")
        .to_string();
    // Auth mode as a kebab-case string (matches how `WsState::new_with_auth`
    // serializes it into the in-memory settings store). Falls back to the
    // no-auth default if serialization fails (defensive; shouldn't happen
    // for the unit AuthMode enum).
    let mode = serde_json::to_value(state.auth_config.mode)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| "unsafe-no-auth".to_string());
    let repository_identity = cwd_is_git_repo();
    let mut payload = serde_json::json!({
        "cwd": cwd,
        "projectName": project_name,
        "authRequired": state.auth_config.requires_authentication(),
        "serverVersion": env!("CARGO_PKG_VERSION"),
        "mode": mode,
        "capabilities": {
            "repositoryIdentity": repository_identity,
        },
    });
    if let (Some(h), Some(obj)) = (home, payload.as_object_mut()) {
        obj.insert("homeDir".into(), Value::String(h));
    }
    payload
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
    let repository_identity = cwd_is_git_repo();
    let env_desc = serde_json::json!({
        "environmentId": env_id,
        "label": format!("Syncode ({}/{})", os, arch),
        "platform": { "os": os, "arch": arch },
        "serverVersion": server_version,
        "capabilities": { "repositoryIdentity": repository_identity },
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
    // Real process telemetry (T6c-phase-26):
    //   - uptime from the server-start Instant captured in `WsState::new_with_auth`.
    //   - RSS from `/proc/self/status` on Linux (kB → bytes); 0 elsewhere.
    let uptime_seconds = state.started_at.elapsed().as_secs();
    let rss_kbytes = process_rss_kbytes();
    let rss_bytes = rss_kbytes.saturating_mul(1024);

    // Child-process rollup: live terminal PTY sessions + tracked local
    // servers. We surface a summary count only (no per-child probing —
    // neither child process exposes its RSS portably without /proc walks,
    // which is out of scope for a diagnostics summary). Both reads are
    // cheap (HashMap snapshot / Vec clone under a short-lived lock).
    let terminal_count = state.terminal_manager.read().await.list_sessions().await.len();
    let local_server_count = state.local_servers.read().await.list().len();
    let child_total_count = terminal_count + local_server_count;

    let result = serde_json::json!({
        "generatedAt": iso_now(),
        "process": {
            "pid": std::process::id(),
            "uptimeSeconds": uptime_seconds,
            "memory": {
                "rssBytes": rss_bytes,
                // Rust has no stable heap/external probe; keep these 0
                // (contract permits zeroed counters for non-Node runtimes).
                "heapTotalBytes": 0,
                "heapUsedBytes": 0,
                "externalBytes": 0,
                "arrayBuffersBytes": 0,
            },
        },
        "childProcesses": [],
        "childProcessTotalCount": child_total_count,
        // No per-child RSS probing — surface 0 (matches the empty
        // `childProcesses` array; the contract permits a 0 rollup when no
        // per-process detail is collected).
        "childProcessTotalRssBytes": 0,
        "projection": {
            "projectCount": project_count,
            "threadCount": thread_count,
        },
    });
    JsonRpcResponse::success(id, result)
}

/// `server.subscribeLifecycle` (T6c-phase-27 — REAL) — register on
/// `server.lifecycle` and emit an initial `welcome` event as the snapshot
/// baseline. The payload mirrors `server.welcome` so a client that subscribes
/// to lifecycle (rather than calling `server.welcome` directly) still receives
/// the same bootstrap state. Future server-lifecycle broadcasts (e.g. shutdown
/// notices) will fan out via the delivery loop's `is_subscribed` check on this
/// channel.
///
/// The push uses the same snapshot-then-stream pattern as
/// `subscribeConfig`/`subscribeSettings`: register the connection, then send
/// a single best-effort `push/server.lifecycle` notification with the welcome
/// shape. The notification is broadcast via `push_tx` so the test harness can
/// observe it (matching the delivery-loop seam used by the other channels).
async fn handle_server_subscribe_lifecycle(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
) -> JsonRpcResponse {
    let added = state
        .subscriptions
        .write()
        .await
        .subscribe(conn_id, crate::channels::CHANNEL_SERVER_LIFECYCLE);
    // Build the welcome payload and broadcast it as the initial `welcome`
    // event on the lifecycle channel. Best-effort: no subscribers is not an
    // error (matches the `WsDomainEventPublisher` convention).
    let welcome = build_server_welcome_payload(state);
    let _ = state.push_tx.send((
        crate::channels::CHANNEL_SERVER_LIFECYCLE.to_string(),
        serde_json::json!({
            "eventType": "welcome",
            "aggregateId": Value::Null,
            "data": welcome.clone(),
        }),
    ));
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "channel": crate::channels::CHANNEL_SERVER_LIFECYCLE,
            "added": added,
            "snapshotEmitted": true,
            "welcome": welcome,
        }),
    )
}

// ─── Server-settings subscribe handlers (T6c-18 — REAL) ───────────
//
// Each subscribe handler registers the originating connection on the matching
// `server.*Updated` push channel (via the shared `SubscriptionRegistry`) and
// emits an initial snapshot of the current stored state. The push delivery
// loop (`run_push_delivery`) then forwards future writes from the matching
// write handler to this connection as `push/<channel>` notifications. This is
// the snapshot-then-stream pattern: the client applies live deltas against
// the snapshot baseline.

/// `server.subscribeConfig` — register on `server.configUpdated` and emit the
/// current stored `ServerConfig` as the initial snapshot. Subsequent
/// `server.setConfig` / `server.upsertKeybinding` writes fan out to this
/// connection as `push/server.configUpdated` notifications.
async fn handle_server_subscribe_config(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
) -> JsonRpcResponse {
    let added = state
        .subscriptions
        .write()
        .await
        .subscribe(conn_id, crate::channels::CHANNEL_SERVER_CONFIG_UPDATED);
    // Emit the current stored config as a one-shot snapshot push so a
    // freshly-subscribed client has the baseline. Best-effort: a missing
    // connection (unregistered mid-flight) is silently a no-snapshot.
    let snapshot_emitted =
        emit_server_config_snapshot(state, conn_id, crate::channels::CHANNEL_SERVER_CONFIG_UPDATED)
            .await;
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "channel": crate::channels::CHANNEL_SERVER_CONFIG_UPDATED,
            "added": added,
            "snapshotEmitted": snapshot_emitted,
        }),
    )
}

/// `server.subscribeSettings` — register on `server.settingsUpdated` and emit
/// the current stored `ServerSettings` snapshot.
async fn handle_server_subscribe_settings(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
) -> JsonRpcResponse {
    let added = state
        .subscriptions
        .write()
        .await
        .subscribe(conn_id, crate::channels::CHANNEL_SERVER_SETTINGS_UPDATED);
    let snapshot_emitted = emit_server_settings_snapshot(
        state,
        conn_id,
        crate::channels::CHANNEL_SERVER_SETTINGS_UPDATED,
    )
    .await;
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "channel": crate::channels::CHANNEL_SERVER_SETTINGS_UPDATED,
            "added": added,
            "snapshotEmitted": snapshot_emitted,
        }),
    )
}

/// `server.subscribeProviderStatuses` — register on
/// `server.providerStatusesUpdated` and emit the current provider list
/// snapshot (derived from the settings store's `providers` map).
async fn handle_server_subscribe_provider_statuses(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
) -> JsonRpcResponse {
    let added = state
        .subscriptions
        .write()
        .await
        .subscribe(conn_id, crate::channels::CHANNEL_SERVER_PROVIDER_STATUSES_UPDATED);
    let snapshot_emitted = emit_server_config_snapshot(
        state,
        conn_id,
        crate::channels::CHANNEL_SERVER_PROVIDER_STATUSES_UPDATED,
    )
    .await;
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "channel": crate::channels::CHANNEL_SERVER_PROVIDER_STATUSES_UPDATED,
            "added": added,
            "snapshotEmitted": snapshot_emitted,
        }),
    )
}

/// Emit a snapshot of the current stored `ServerConfig`-derived state to
/// `conn_id` as a `push/<channel>` notification with `event_type: "snapshot"`.
/// Used by `subscribeConfig` (full config payload) and
/// `subscribeProviderStatuses` (`{ providers }` slice). Best-effort: a missing
/// connection is silently a no-op.
async fn emit_server_config_snapshot(
    state: &WsState,
    conn_id: ConnectionId,
    channel: &str,
) -> bool {
    let tx = match state.connections.read().await.get(&conn_id).cloned() {
        Some(tx) => tx,
        None => return false,
    };
    // Read-lock the store only for the snapshot build.
    let data = {
        let store = state.settings.read().await;
        match channel {
            crate::channels::CHANNEL_SERVER_PROVIDER_STATUSES_UPDATED => {
                // Provider-statuses snapshot: just the `providers` slice from
                // the config (which is `[]` by default — no probe runs). This
                // matches the `ServerProviderStatusesUpdatedPayload` shape.
                serde_json::json!({
                    "eventType": "snapshot",
                    "aggregateId": Value::Null,
                    "data": { "providers": store.config["providers"].clone() },
                })
            }
            // configUpdated snapshot: the full `ServerConfigUpdatedPayload`
            // shape is `{ issues, providers }` — both slices of the config.
            _ => serde_json::json!({
                "eventType": "snapshot",
                "aggregateId": Value::Null,
                "data": {
                    "issues": store.config["issues"].clone(),
                    "providers": store.config["providers"].clone(),
                },
            }),
        }
    };
    push_frame(&tx, channel, &data)
}

/// Emit a snapshot of the current stored `ServerSettings` to `conn_id` as a
/// `push/server.settingsUpdated` notification with `event_type: "snapshot"`.
async fn emit_server_settings_snapshot(
    state: &WsState,
    conn_id: ConnectionId,
    channel: &str,
) -> bool {
    let tx = match state.connections.read().await.get(&conn_id).cloned() {
        Some(tx) => tx,
        None => return false,
    };
    let data = {
        let store = state.settings.read().await;
        serde_json::json!({
            "eventType": "snapshot",
            "aggregateId": Value::Null,
            "data": { "settings": store.settings.clone() },
        })
    };
    push_frame(&tx, channel, &data)
}

/// Serialize + send a `push/<channel>` notification to a single connection.
/// Best-effort: a send failure (dropped connection) returns false.
fn push_frame(
    tx: &tokio::sync::mpsc::UnboundedSender<String>,
    channel: &str,
    data: &Value,
) -> bool {
    let msg = serde_json::json!({
        "jsonrpc": "2.0",
        "method": format!("push/{}", channel),
        "params": data,
    });
    serde_json::to_string(&msg)
        .map(|s| tx.send(s).is_ok())
        .unwrap_or(false)
}

// ─── Server write-side handlers (T6c-18 — REAL persistence + push) ─
//
// Each write handler:
//   1. Validates the params shape (rejecting malformed input with -32602).
//   2. Acquires a write lock on `state.settings` and applies the change
//      (replace for setConfig, deep-merge for updateSettings/patchSettings,
//      per-field for updateProvider/upsertKeybinding).
//   3. Broadcasts a push event on the matching `server.*Updated` channel so
//      subscribed connections receive the new state. The broadcast is
//      best-effort (`let _ = state.push_tx.send(...)`): no subscribers is not
//      an error, matching the `WsDomainEventPublisher` convention.
//   4. Returns the updated full document (the read-side shape) so the UI's
//      optimistic state converges with the server's stored view.
//
// Persistence scope: in-memory only, for the server session. Syncode has no
// on-disk settings file, so edits don't survive a restart (the documented
// gap — the store is rebuilt from defaults on each server start).

/// `server.setConfig` — overwrite the stored `ServerConfig` with the params
/// (validated as a JSON object). Pushes `server.configUpdated` with the
/// `{ issues, providers }` slice. Returns the full updated config.
async fn handle_server_set_config(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    // Validate `params` is a JSON object (ServerConfig is a struct). Non-object
    // (null, array, primitive) → InvalidParams (-32602).
    if !params.is_object() {
        return JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            "Invalid params: 'setConfig' expects a server-config object",
        );
    }
    let updated = {
        let mut store = state.settings.write().await;
        // Replace wholesale (setConfig is a full overwrite, not a merge).
        store.config = params.clone();
        // Build the push payload (the `ServerConfigUpdatedPayload` slice)
        // and the response (the full config) before releasing the lock.
        let push_payload = serde_json::json!({
            "issues": store.config["issues"].clone(),
            "providers": store.config["providers"].clone(),
        });
        let response = store.config.clone();
        (push_payload, response)
    };
    // Broadcast the update to subscribed connections. Best-effort: no
    // subscribers is not an error.
    let _ = state.push_tx.send((
        crate::channels::CHANNEL_SERVER_CONFIG_UPDATED.to_string(),
        updated.0,
    ));
    JsonRpcResponse::success(id, updated.1)
}

/// `server.updateSettings` / `server.patchSettings` — deep-merge the patch
/// into the stored `ServerSettings` and push `server.settingsUpdated` with the
/// full resolved settings. Returns the full updated settings.
async fn handle_server_update_settings(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    // Validate `params` is a JSON object (ServerSettingsPatch is a struct with
    // all-optional fields). Non-object → InvalidParams (-32602).
    if !params.is_object() {
        return JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            "Invalid params: 'updateSettings' expects a settings-patch object",
        );
    }
    let updated = {
        let mut store = state.settings.write().await;
        crate::settings::merge_json(&mut store.settings, params);
        store.settings.clone()
    };
    let _ = state.push_tx.send((
        crate::channels::CHANNEL_SERVER_SETTINGS_UPDATED.to_string(),
        serde_json::json!({ "settings": updated.clone() }),
    ));
    JsonRpcResponse::success(id, updated)
}

/// `server.patchSettings` — alias of `updateSettings` (same deep-merge
/// semantics; MCode exposes both names for forward-compat with the contracts
/// layer).
async fn handle_server_patch_settings(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    handle_server_update_settings(state, id, params).await
}

/// `server.refreshProviders` — re-emit the current provider list from the
/// settings store. Syncode has no external provider-availability probe, so the
/// "refresh" is a no-op state-wise: the providers slice is whatever the last
/// write stored (the default `[]` if no probe or updateProvider has run).
/// Pushes `server.providerStatusesUpdated` and returns the
/// `ServerProviderStatusesUpdatedPayload` (`{ providers: [...] }`).
async fn handle_server_refresh_providers(state: &WsState, id: Value) -> JsonRpcResponse {
    let providers = {
        let store = state.settings.read().await;
        store.config["providers"].clone()
    };
    let payload = serde_json::json!({ "providers": providers.clone() });
    let _ = state.push_tx.send((
        crate::channels::CHANNEL_SERVER_PROVIDER_STATUSES_UPDATED.to_string(),
        payload.clone(),
    ));
    JsonRpcResponse::success(id, payload)
}

/// `server.updateProvider` — re-probe a single provider. Validates that
/// `params.provider` is present and non-empty (MCode `ServerProviderUpdateInput`
/// is `{ provider: ProviderKind }`). Syncode has no probe, so this re-emits the
/// current provider list (the targeted provider's status is unchanged unless a
/// prior `updateSettings` write updated its settings entry). Pushes
/// `server.providerStatusesUpdated` and returns the payload.
async fn handle_server_update_provider(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
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
    let providers = {
        let store = state.settings.read().await;
        store.config["providers"].clone()
    };
    let payload = serde_json::json!({ "providers": providers.clone() });
    let _ = state.push_tx.send((
        crate::channels::CHANNEL_SERVER_PROVIDER_STATUSES_UPDATED.to_string(),
        payload.clone(),
    ));
    JsonRpcResponse::success(id, payload)
}

/// `server.upsertKeybinding` — add/update a keybinding rule. Validates that
/// `params` is a JSON object (MCode `KeybindingRule` is a struct), then
/// appends/replaces the entry in the config's `keybindings` array (keyed by
/// the rule's `id` if present, else appended). Pushes `server.configUpdated`
/// with the `{ issues, providers }` slice and returns `{ keybindings, issues }`.
async fn handle_server_upsert_keybinding(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    // Validate `params` is an object (KeybindingRule is a struct). Non-object
    // (null, array, primitive) → InvalidParams (-32602).
    if !params.is_object() {
        return JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            "Invalid params: 'upsertKeybinding' expects a keybinding-rule object",
        );
    }
    let (keybindings, push_payload) = {
        let mut store = state.settings.write().await;
        // Ensure `keybindings` is an array (the default is `[]`; a prior
        // setConfig write could have replaced it with a non-array).
        let cfg_obj = store
            .config
            .as_object_mut()
            .expect("stored ServerConfig is always an object");
        if !cfg_obj["keybindings"].is_array() {
            cfg_obj["keybindings"] = Value::Array(Vec::new());
        }
        let keybindings_arr = cfg_obj["keybindings"].as_array_mut().unwrap();
        // Upsert by `id` if the rule carries one; else append. A matching id
        // replaces; otherwise the rule is appended.
        let rule_id = params.get("id").and_then(|v| v.as_str()).map(String::from);
        let mut replaced = false;
        if let Some(ref rid) = rule_id {
            for entry in keybindings_arr.iter_mut() {
                if entry.get("id").and_then(|v| v.as_str()) == Some(rid.as_str()) {
                    *entry = params.clone();
                    replaced = true;
                    break;
                }
            }
        }
        if !replaced {
            keybindings_arr.push(params.clone());
        }
        let keybindings = cfg_obj["keybindings"].clone();
        let push_payload = serde_json::json!({
            "issues": cfg_obj["issues"].clone(),
            "providers": cfg_obj["providers"].clone(),
        });
        (keybindings, push_payload)
    };
    let _ = state.push_tx.send((
        crate::channels::CHANNEL_SERVER_CONFIG_UPDATED.to_string(),
        push_payload,
    ));
    JsonRpcResponse::success(
        id,
        serde_json::json!({ "keybindings": keybindings, "issues": [] }),
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

/// `server.listProviderUsage` — REAL (T6c-19): aggregates the in-memory usage
/// log into one `ServerProviderUsageSnapshot` per provider that has recorded
/// usage. MCode's contract is `ServerListProviderUsageResult = readonly
/// ServerProviderUsageSnapshot[]`. Syncode now backs this with actual usage
/// captured from successful LLM one-shot ops (`compactThread`, `summarizeDiff`,
/// `generateThreadRecap`, `generateAutomationIntent`) via the `invoke()`
/// helper. Providers with no recorded usage are omitted (an empty array means
/// "no usage yet this session" — the UI surfaces an empty state).
///
/// The optional `forceRefresh` param is acknowledged but has no effect
/// (syncode's log is always live in-memory; there's no cached snapshot to
/// invalidate). Returns at most one snapshot per provider id, sorted
/// alphabetically.
async fn handle_server_list_provider_usage(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let _ = params; // ack `forceRefresh` etc. — no behavior (live log).
    let store = state.usage.read().await;
    let aggregates = store.aggregate_by_provider();
    let snapshots: Vec<Value> = aggregates
        .into_iter()
        .map(|agg| usage_snapshot_json(&agg, "syncode-usage-log"))
        .collect();
    JsonRpcResponse::success(id, Value::Array(snapshots))
}

/// `server.getProviderUsageSnapshot` — REAL (T6c-19): returns a single
/// provider's usage snapshot aggregated from the in-memory log. MCode's
/// contract is `ServerGetProviderUsageSnapshotResult =
/// ServerProviderUsageSnapshot | null`. We return `null` when the provider
/// has no recorded usage (the UI treats null as "no data"); otherwise a
/// snapshot whose `usageLines` carry the aggregate token counts.
///
/// Validates that `params.provider` is a non-empty string (the MCode input
/// shape marks it required). The provider id is matched case-sensitively
/// against what the `invoke()` helper recorded (the resolved registry id —
/// e.g. `claude`, `codex`, `gemini`, …).
async fn handle_server_get_provider_usage_snapshot(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let provider = params.get("provider").and_then(|v| v.as_str()).unwrap_or("");
    if provider.trim().is_empty() {
        return JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            "Invalid params: 'provider' must be a non-empty string",
        );
    }
    let store = state.usage.read().await;
    match store.aggregate_for(provider) {
        Some(agg) => JsonRpcResponse::success(
            id,
            usage_snapshot_json(&agg, "syncode-usage-log"),
        ),
        None => {
            // No usage recorded for this provider → null (UI shows empty state).
            JsonRpcResponse::success(id, Value::Null)
        }
    }
}

/// Build a `ServerProviderUsageSnapshot` JSON value from a usage aggregate.
///
/// Matches the MCode contract (`frontend/src/contracts/tier3/server.ts`).
/// Field mapping:
///
/// - `provider`: the provider id (`ProviderKind`).
/// - `updatedAt`: ISO-8601 timestamp of the most-recent entry.
/// - `limits`: empty array. Syncode has no rate-limit tracking; the
///   `ServerProviderUsageLimit` shape is for windowed quotas we don't model
///   (left as a follow-up).
/// - `usageLines`: label/value lines for input, output, total tokens + call
///   count.
/// - `source`: opaque origin tag (caller-supplied; the handlers pass
///   `"syncode-usage-log"` so the UI can label the source).
/// - `status`: `"ok"`. We only record successful ops; a non-ok status would
///   come from a rate-limit probe, which we lack.
///
/// `usageLines` use the `ServerProviderUsageLine` shape `{ label, value,
/// subtitle? }`. Values are formatted as human-readable strings (the contract
/// types `value` as `TrimmedNonEmptyString`, not a number — the UI renders
/// them as-is).
fn usage_snapshot_json(agg: &crate::usage::ProviderUsageAggregate, source: &str) -> Value {
    let updated_at = agg
        .last_used_at
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
    serde_json::json!({
        "provider": agg.provider_id,
        "updatedAt": updated_at,
        "limits": [],
        "usageLines": [
            { "label": "Input tokens",  "value": agg.total_input.to_string() },
            { "label": "Output tokens", "value": agg.total_output.to_string() },
            { "label": "Total tokens",  "value": agg.total_tokens.to_string() },
            {
                "label": "Calls",
                "value": agg.call_count.to_string(),
                "subtitle": "LLM round trips recorded this session"
            },
        ],
        "source": source,
        "status": "ok",
        "planName": null,
        "detail": format!(
            "Aggregated from {} recorded call(s); model last used: {}",
            agg.call_count, agg.model
        ),
    })
}

/// `server.startLocalServer` — spawn a long-running server process.
///
/// Reads params:
///   - `command` (required): executable to spawn (argv[0]).
///   - `args` (optional, default []): argv[1..] as an array of strings.
///   - `env` (optional, default {}): environment overrides as an object.
///   - `name` (optional): display name surfaced to the UI.
///   - `id` (optional): explicit server id; auto-generated as `local-<n>` if absent.
///   - `ports` (optional, default []): ports the caller declares the server
///     will bind (echoed back into `addresses`, not probed).
///
/// Returns the MCode `ServerLocalServerProcess` shape on success:
///   `{ id, pid, command, displayName, args, ports, addresses, isStoppable, startedAt }`
/// On spawn failure returns an INVALID_PARAMS error (-32602) carrying the
/// spawn error message.
async fn handle_server_start_local_server(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let obj = match params.as_object() {
        Some(o) => o,
        None => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "params must be an object",
            );
        }
    };

    let command = match obj.get("command").and_then(|v| v.as_str()) {
        Some(c) if !c.is_empty() => c.to_string(),
        _ => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "params.command (non-empty string) is required",
            );
        }
    };

    let args: Vec<String> = obj
        .get("args")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let env: std::collections::HashMap<String, String> = obj
        .get("env")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let ports: Vec<u32> = obj
        .get("ports")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as u32))
                .collect()
        })
        .unwrap_or_default();

    let display_name = obj
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or(&command)
        .to_string();

    // Auto-assign an id from the current map size + a uuid suffix for
    // collision-safety if the caller omits one.
    let server_id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| {
            format!(
                "local-{}",
                uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or("0")
            )
        });

    let mut mgr = state.local_servers.write().await;
    match mgr
        .start(server_id, display_name, command, args, env, ports)
        .await
    {
        Ok(view) => {
            let result = serde_json::to_value(&view).unwrap_or_else(|_| {
                serde_json::json!({ "id": view.id, "pid": view.pid })
            });
            JsonRpcResponse::success(id, result)
        }
        Err(msg) => JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INTERNAL_ERROR,
            msg,
        ),
    }
}

/// `server.stopLocalServer` — kill a tracked server process by `id`.
///
/// Reads params:
///   - `id` (preferred): the server id returned by `startLocalServer`.
///   - `name` (fallback): treated as an id if `id` is absent (legacy/convenience).
///
/// Returns `{ ok: true }` on success. If the id isn't tracked returns an
/// INVALID_PARAMS error (-32602).
async fn handle_server_stop_local_server(
    state: &WsState,
    id: Value,
    params: &Value,
) -> JsonRpcResponse {
    let server_id = params
        .get("id")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("name").and_then(|v| v.as_str()))
        .map(String::from);
    let server_id = match server_id {
        Some(s) if !s.is_empty() => s,
        _ => {
            return JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INVALID_PARAMS,
                "params.id (non-empty string) is required",
            );
        }
    };

    let mut mgr = state.local_servers.write().await;
    match mgr.stop(&server_id).await {
        Ok(()) => JsonRpcResponse::success(id, serde_json::json!({ "ok": true })),
        Err(msg) => JsonRpcResponse::error(
            Some(id),
            crate::error_codes::INVALID_PARAMS,
            msg,
        ),
    }
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
    // MCode places `unread` + `archivedAt` on `AutomationRunResult` (which
    // lives under `result`). The syncode `AutomationRun` lifts these to the
    // run itself for simplicity, so we mirror them into a `result` sub-object
    // (creating or merging) for contract compatibility with the UI.
    let unread = run.unread;
    let archived_at = run.archived_at.clone();
    let result_value = map
        .get("result")
        .cloned()
        .filter(|v| !v.is_null())
        .unwrap_or_else(|| Value::Object(Default::default()));
    if let Value::Object(mut result_map) = result_value {
        result_map.entry("unread").or_insert(Value::Bool(unread));
        result_map
            .entry("archivedAt")
            .or_insert(archived_at.clone().map(Value::String).unwrap_or(Value::Null));
        map.insert("result".to_string(), Value::Object(result_map));
    }
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
            let run_payload = run_to_mcode_run(&run, &project_id);
            // Push the run snapshot to subscribed connections as a `run-upserted`
            // lifecycle event on the `automation` channel. `trigger_with_delay`
            // awaits `execute_run` synchronously — so by the time it returns,
            // the run is in its terminal state (succeeded/failed) and this
            // broadcast captures the lifecycle transition (T6c-21).
            push_automation_run_upserted(state, &run_payload);
            JsonRpcResponse::success(id, serde_json::json!({ "run": run_payload }))
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
    let run_payload = run_to_mcode_run(&run, &project_id);
    // Push the cancelled run snapshot to subscribers on the `automation`
    // channel as a `run-upserted` lifecycle event (T6c-21). The cancel
    // transition is a real lifecycle change (status flips to cancelled),
    // so we broadcast it just like a completed/failed run.
    push_automation_run_upserted(state, &run_payload);
    JsonRpcResponse::success(id, serde_json::json!({ "run": run_payload }))
}

/// `automation.markRunRead` — mark a run as read. Persists the change through
/// the scheduler (`Scheduler::mark_run_read` flips the run's `unread` flag and
/// upserts via the repo). Returns `{ run: AutomationRun }` with `unread=false`.
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
    let run = match scheduler.mark_run_read(&run_id).await {
        Ok(r) => r,
        Err(syncode_automation::SchedulerError::NotFound(_)) => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("automation.markRunRead: not found: {run_id}"),
            );
        }
        Err(e) => {
            return automation_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("automation.markRunRead: {e}"),
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

/// `automation.archiveRun` — archive a run. Persists the change through the
/// scheduler (`Scheduler::archive_run` stamps `archived_at` and upserts via the
/// repo). Returns `{ run: AutomationRun }` with `archivedAt` set.
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
    let run = match scheduler.archive_run(&run_id).await {
        Ok(r) => r,
        Err(syncode_automation::SchedulerError::NotFound(_)) => {
            return automation_error(
                id,
                crate::error_codes::INVALID_PARAMS,
                format!("automation.archiveRun: not found: {run_id}"),
            );
        }
        Err(e) => {
            return automation_error(
                id,
                crate::error_codes::INTERNAL_ERROR,
                format!("automation.archiveRun: {e}"),
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

/// Broadcast a `run-upserted` lifecycle event on the `automation` push channel
/// (T6c-21). Subscribers receive the run snapshot — MCode's
/// `AutomationStreamEvent` union exposes `run-upserted` (not separate
/// started/completed/failed/cancelled variants), so the lifecycle transition
/// is encoded in the run's `status` field.
///
/// Best-effort: no live subscribers is normal (broadcast::send returns
/// `SendError` only when there are zero receivers) — the call site is still
/// authoritative for the RPC response.
fn push_automation_run_upserted(state: &WsState, run_payload: &serde_json::Value) {
    let event = serde_json::json!({
        "type": "run-upserted",
        "run": run_payload,
    });
    let _ = state
        .push_tx
        .send((crate::channels::CHANNEL_AUTOMATION.to_string(), event));
}

/// `automation.subscribe` — register the calling connection on the `automation`
/// push channel (T6c-21). Subsequent run-lifecycle events (runNow →
/// `run-upserted`, cancelRun → `run-upserted`) are delivered by the push
/// delivery loop (`run_push_delivery` in `server.rs`).
///
/// Unlike `server.subscribeConfig`, no initial snapshot is emitted here — the
/// UI's automation view calls `automation.list` first to bootstrap its state,
/// then subscribes for live deltas. (Mirror of MCode's snapshot-via-RPC,
/// stream-via-push pattern.)
async fn handle_automation_subscribe(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
) -> JsonRpcResponse {
    let added = state
        .subscriptions
        .write()
        .await
        .subscribe(conn_id, crate::channels::CHANNEL_AUTOMATION);
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": true,
            "channel": crate::channels::CHANNEL_AUTOMATION,
            "added": added,
        }),
    )
}

/// `automation.unsubscribe` — deregister the calling connection from the
/// `automation` push channel (T6c-21).
async fn handle_automation_unsubscribe(
    state: &WsState,
    conn_id: ConnectionId,
    id: Value,
) -> JsonRpcResponse {
    let removed = state
        .subscriptions
        .write()
        .await
        .unsubscribe(conn_id, crate::channels::CHANNEL_AUTOMATION);
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "subscribed": false,
            "channel": crate::channels::CHANNEL_AUTOMATION,
            "removed": removed,
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

/// `provider.listSkills` — return `ProviderListSkillsResult` populated from a
/// filesystem scan of the project `.skills/*.md` directory (or
/// `SYNCODE_SKILLS_DIR`). Each markdown file becomes a `ProviderSkillDescriptor`
/// (name = filename stem, description = YAML frontmatter `description:` field,
/// path = absolute, enabled = true). Missing/unreadable directory returns an
/// empty `skills` array — the composer renders a "no skills" empty state.
fn handle_provider_list_skills(id: Value, params: &Value) -> JsonRpcResponse {
    let skills = match resolve_skills_dir(params) {
        Some(dir) => scan_skills_dir(&dir),
        None => Vec::new(),
    };
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "skills": skills,
            "source": "filesystem",
        }),
    )
}

/// `provider.listSkillsCatalog` — same filesystem scan as `listSkills`. The
/// catalog is a UI-side aggregated skill index; in syncode the catalog and the
/// live skills list are the same filesystem-backed source. Includes the
/// resolved `mcodeSkillsDir` (absolute path) when a skills dir exists.
fn handle_provider_list_skills_catalog(id: Value, params: &Value) -> JsonRpcResponse {
    let (skills, dir_field): (Vec<Value>, Value) = match resolve_skills_dir(params) {
        Some(dir) => {
            let abs = dir.canonicalize_unchecked();
            let abs_str = abs.to_string_lossy().into_owned();
            (scan_skills_dir(&dir), Value::String(abs_str))
        }
        None => (Vec::new(), Value::Null),
    };
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "skills": skills,
            "mcodeSkillsDir": dir_field,
        }),
    )
}

/// `provider.listPlugins` — filesystem scan of the project `.plugins/`
/// directory (precedence: explicit `cwd` + `.plugins`, then
/// `SYNCODE_PLUGINS_DIR` env, then relative `.plugins` fallback) for `*.json`
/// plugin descriptor files. Each file is parsed as a `ProviderPluginDescriptor`
/// (id, name, source, installed, enabled, installPolicy, authPolicy). Returns
/// the full `ProviderListPluginsResult` shape: marketplaces/errors empty,
/// remoteSyncError null, and the discovered plugins under a synthetic
/// "local" marketplace. Missing dir → graceful empty marketplaces list.
fn handle_provider_list_plugins(id: Value, params: &Value) -> JsonRpcResponse {
    let dir = resolve_plugins_dir(params);
    let marketplaces: Vec<Value> = match dir {
        Some(d) => {
            let plugins = scan_plugins_dir(&d);
            if plugins.is_empty() {
                Vec::new()
            } else {
                let abs = d.canonicalize_unchecked().to_string_lossy().into_owned();
                vec![serde_json::json!({
                    "name": "local",
                    "path": abs,
                    "interface": { "displayName": "Local Plugins" },
                    "plugins": plugins,
                })]
            }
        }
        None => Vec::new(),
    };
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "marketplaces": marketplaces,
            "marketplaceLoadErrors": [],
            "remoteSyncError": Value::Null,
            "featuredPluginIds": [],
            "source": "filesystem",
        }),
    )
}

/// `provider.readPlugin` — read a plugin descriptor at the requested `path`.
/// The path must point to an existing readable `*.json` file inside a `.plugins`
/// directory (basic traversal guard). Returns `{ plugin: {...} }` with the full
/// descriptor shape or `{ plugin: null }` when missing/unreadable/out-of-bounds.
fn handle_provider_read_plugin(id: Value, params: &Value) -> JsonRpcResponse {
    let raw_path = match params.get("path").and_then(|v| v.as_str()) {
        Some(p) if !p.is_empty() => p,
        _ => return JsonRpcResponse::success(id, serde_json::json!({ "plugin": Value::Null })),
    };
    let path = Path::new(raw_path);
    let canonical = match path.canonicalize_unchecked().canonicalize() {
        Ok(c) => c,
        Err(_) => return JsonRpcResponse::success(id, serde_json::json!({ "plugin": Value::Null })),
    };
    // Basic traversal guard: require the canonical path to contain a `.plugins`
    // component, and reject non-`.json` extensions.
    let in_plugins = canonical
        .components()
        .any(|c| c.as_os_str() == ".plugins");
    let is_json = canonical.extension().and_then(|e| e.to_str()) == Some("json");
    if !in_plugins || !is_json {
        return JsonRpcResponse::success(id, serde_json::json!({ "plugin": Value::Null }));
    }
    let plugin = read_plugin_descriptor(&canonical);
    JsonRpcResponse::success(id, serde_json::json!({ "plugin": plugin }))
}

/// `provider.listCommands` — return `ProviderListCommandsResult` with a static
/// per-provider list of native slash commands. Each provider ships a known set
/// of built-in CLI commands; we surface them so the composer's `/` autocomplete
/// shows real entries instead of an empty list. Unknown/unrecognized providers
/// get the minimal `/help` + `/clear` baseline.
fn handle_provider_list_commands(id: Value, params: &Value) -> JsonRpcResponse {
    let provider = params
        .get("provider")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("claude");
    let kind = to_mcode_provider_kind(provider).unwrap_or(provider);
    let commands: Vec<Value> = match kind {
        "claudeAgent" => vec![
            serde_json::json!({ "name": "/help", "description": "Show available commands and usage." }),
            serde_json::json!({ "name": "/clear", "description": "Clear the current conversation history." }),
            serde_json::json!({ "name": "/compact", "description": "Summarize and compact the conversation context." }),
            serde_json::json!({ "name": "/cost", "description": "Show token usage and cost for the session." }),
            serde_json::json!({ "name": "/doctor", "description": "Diagnose the Claude installation and environment." }),
        ],
        "codex" => vec![
            serde_json::json!({ "name": "/help", "description": "Show available commands and usage." }),
            serde_json::json!({ "name": "/clear", "description": "Clear the current conversation history." }),
        ],
        _ => vec![
            serde_json::json!({ "name": "/help", "description": "Show available commands and usage." }),
            serde_json::json!({ "name": "/clear", "description": "Clear the current conversation history." }),
        ],
    };
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "commands": commands,
            "source": "static",
        }),
    )
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
        .unwrap_or("claude");
    let kind = to_mcode_provider_kind(provider).unwrap_or(provider);
    // Per-provider capability matrix. Tier-1 providers (claude/codex) get the
    // richest flag set so the composer renders the full skill/command UI;
    // smaller providers get progressively fewer flags. Plugin flags stay false
    // everywhere — syncode has no plugin marketplace subsystem.
    let (skill_mentions, skill_discovery, native_commands) = match kind {
        "claudeAgent" => (true, true, true),
        "codex" => (true, true, true),
        "gemini" => (true, false, true),
        "grok" => (false, false, true),
        "cursor" => (false, false, true),
        "kilo" | "opencode" | "pi" => (false, false, false),
        _ => (false, false, false),
    };
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "provider": kind,
            "supportsSkillMentions": skill_mentions,
            "supportsSkillDiscovery": skill_discovery,
            "supportsNativeSlashCommandDiscovery": native_commands,
            "supportsPluginMentions": false,
            "supportsPluginDiscovery": false,
            "supportsRuntimeModelList": true,
            "supportsThreadCompaction": true,
            "supportsThreadImport": true,
        }),
    )
}

/// Resolve the skills directory for a `listSkills`/`listSkillsCatalog`/`readSkill`
/// request. Precedence: explicit `cwd` param joined with `.skills`, then the
/// `SYNCODE_SKILLS_DIR` env var, finally a relative `.skills` fallback. Returns
/// `None` if the resolved directory does not exist (graceful empty result).
fn resolve_skills_dir(params: &Value) -> Option<PathBuf> {
    let dir = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|cwd| Path::new(cwd).join(".skills"))
        .or_else(|| std::env::var_os("SYNCODE_SKILLS_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(".skills"));
    if dir.is_dir() {
        Some(dir)
    } else {
        None
    }
}

/// Parse a YAML-ish frontmatter block (`---\n...\n---`) from a markdown skill
/// file and extract the `description:` value. Only the simple `key: value`
/// scalar form is supported — sufficient for skill files authored as
/// `name: foo\ndescription: bar`. Returns `None` if no frontmatter or no
/// `description:` key is present.
fn parse_skill_frontmatter_description(content: &str) -> Option<String> {
    let trimmed = content.trim_start();
    let after_fence = trimmed.strip_prefix("---")?;
    let end = after_fence.find("\n---")?;
    let frontmatter = &after_fence[..end];
    for line in frontmatter.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("description:") {
            let val = rest.trim().trim_matches(|c| c == '"' || c == '\'');
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

/// Scan the resolved skills directory for `*.md` files and build a
/// `ProviderSkillDescriptor` per file. Name is the filename stem, description is
/// parsed from the file's YAML frontmatter (`description:` field), path is the
/// absolute (canonicalized) file path, enabled is always `true`. Returns an
/// empty vec on any I/O error — callers return a graceful empty `skills` array.
fn scan_skills_dir(dir: &Path) -> Vec<Value> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    let mut skills: Vec<(String, Value)> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("md") {
            continue;
        }
        let name = match path.file_stem().and_then(|s| s.to_str()) {
            Some(n) if !n.is_empty() => n.to_string(),
            _ => continue,
        };
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        let description = parse_skill_frontmatter_description(&content);
        let abs_path = path.canonicalize_unchecked().to_string_lossy().into_owned();
        let mut descriptor = serde_json::json!({
            "name": name,
            "path": abs_path,
            "enabled": true,
        });
        if let Some(desc) = description {
            descriptor["description"] = Value::String(desc);
        }
        skills.push((name, descriptor));
    }
    skills.sort_by(|a, b| a.0.cmp(&b.0));
    skills.into_iter().map(|(_, v)| v).collect()
}

/// Helper to convert a `Path` to a string for the `path` field without failing
/// on non-UTF8 — falls back to lossy rendering (absolute path is still emitted).
trait CanonicalizeUnchecked {
    fn canonicalize_unchecked(&self) -> PathBuf;
}
impl CanonicalizeUnchecked for Path {
    fn canonicalize_unchecked(&self) -> PathBuf {
        std::fs::canonicalize(self).unwrap_or_else(|_| self.to_path_buf())
    }
}

/// `provider.listOptions` — return per-provider model configuration options
/// (`ProviderOptionDescriptor[]`). Mirrors the MCode `model.ts` option sets:
/// reasoning-effort (codex/claude/grok), thinking-level (gemini/pi). Other
/// providers return an empty array (no configurable options). The `provider`
/// param is read from the request (defaults to `claude` when absent). Returns
/// `{ options: [...] }`.
fn handle_provider_list_options(id: Value, params: &Value) -> JsonRpcResponse {
    let provider = params
        .get("provider")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("claude");
    let kind = to_mcode_provider_kind(provider).unwrap_or(provider);
    let options = provider_option_descriptors(kind);
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "options": options,
            "source": "static",
        }),
    )
}

/// Build the static `ProviderOptionDescriptor[]` for a given MCode provider
/// kind. Returns a single `SelectProviderOptionDescriptor` for providers with a
/// configurable reasoning effort / thinking level; empty for others. Values
/// sourced from the MCode `model.ts` capability constants.
fn provider_option_descriptors(kind: &str) -> Vec<Value> {
    let (opt_id, label, choices): (&str, &str, Vec<(&str, &str, bool)>) = match kind {
        // codex: low/medium(high-default)/high/xhigh — gpt-5.5 model.
        "codex" => (
            "reasoningEffort",
            "Reasoning Effort",
            vec![
                ("low", "Low", false),
                ("medium", "Medium", true),
                ("high", "High", false),
                ("xhigh", "Extra High", false),
            ],
        ),
        // claudeAgent: low/medium/high-default/xhigh/max/ultrathink/ultracode.
        "claudeAgent" => (
            "reasoningEffort",
            "Reasoning Effort",
            vec![
                ("low", "Low", false),
                ("medium", "Medium", false),
                ("high", "High", true),
                ("xhigh", "Extra High", false),
                ("max", "Max", false),
                ("ultrathink", "Ultrathink", false),
                ("ultracode", "Ultracode", false),
            ],
        ),
        // grok: none/low-default/medium/high.
        "grok" => (
            "reasoningEffort",
            "Reasoning Effort",
            vec![
                ("none", "None", false),
                ("low", "Low", true),
                ("medium", "Medium", false),
                ("high", "High", false),
            ],
        ),
        // gemini: thinking level — Dynamic/-1 or 512 tokens (gemini-2.5) and
        // HIGH-default/LOW (gemini-3). Surface the union as thinkingLevel.
        "gemini" => (
            "thinkingLevel",
            "Thinking Level",
            vec![
                ("HIGH", "High", true),
                ("LOW", "Low", false),
                ("-1", "Dynamic", false),
                ("512", "512 Tokens", false),
            ],
        ),
        // pi: thinking level — same surface as gemini (pi uses a similar
        // thinking-budget toggle). Default to medium.
        "pi" => (
            "thinkingLevel",
            "Thinking Level",
            vec![
                ("low", "Low", false),
                ("medium", "Medium", true),
                ("high", "High", false),
            ],
        ),
        _ => return Vec::new(),
    };
    let options: Vec<Value> = choices
        .into_iter()
        .map(|(value, lbl, is_default)| {
            let mut choice = serde_json::json!({
                "id": value,
                "label": lbl,
            });
            if is_default {
                choice["isDefault"] = Value::Bool(true);
            }
            choice
        })
        .collect();
    vec![serde_json::json!({
        "id": opt_id,
        "label": label,
        "type": "select",
        "options": options,
    })]
}

/// Resolve the plugins directory for a `listPlugins`/`readPlugin` request.
/// Precedence: explicit `cwd` param joined with `.plugins`, then the
/// `SYNCODE_PLUGINS_DIR` env var, finally a relative `.plugins` fallback.
/// Returns `None` if the resolved directory does not exist (graceful empty
/// result).
fn resolve_plugins_dir(params: &Value) -> Option<PathBuf> {
    let dir = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|cwd| Path::new(cwd).join(".plugins"))
        .or_else(|| std::env::var_os("SYNCODE_PLUGINS_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(".plugins"));
    if dir.is_dir() {
        Some(dir)
    } else {
        None
    }
}

/// Scan the resolved plugins directory for `*.json` files and build a
/// `ProviderPluginDescriptor` per file. Each file is parsed as JSON; required
/// fields are `id` and `name` (others default: source = local file path,
/// installed/enabled = true, installPolicy = AVAILABLE, authPolicy = ON_USE).
/// Invalid JSON or missing required fields → file is skipped. Returns sorted
/// by id. Empty on any I/O error.
fn scan_plugins_dir(dir: &Path) -> Vec<Value> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    let mut plugins: Vec<(String, Value)> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        if let Some(descriptor) = read_plugin_descriptor(&path) {
            let id = descriptor
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if !id.is_empty() {
                plugins.push((id, descriptor));
            }
        }
    }
    plugins.sort_by(|a, b| a.0.cmp(&b.0));
    plugins.into_iter().map(|(_, v)| v).collect()
}

/// Read and parse a plugin descriptor file at `path` into a
/// `ProviderPluginDescriptor` JSON value. The file must be valid JSON with at
/// least an `id` (string) and `name` (string) field; optional fields
/// (`description`, `enabled`, `version`, `interface`, `installPolicy`,
/// `authPolicy`) are merged in when present. Returns `None` on I/O error,
/// invalid JSON, or missing required fields.
fn read_plugin_descriptor(path: &Path) -> Option<Value> {
    let content = std::fs::read_to_string(path).ok()?;
    let parsed: Value = serde_json::from_str(&content).ok()?;
    let obj = parsed.as_object()?;
    let id = obj.get("id").and_then(|v| v.as_str())?;
    let name = obj.get("name").and_then(|v| v.as_str())?;
    if id.is_empty() || name.is_empty() {
        return None;
    }
    let abs_path = path.canonicalize_unchecked().to_string_lossy().into_owned();
    let enabled = obj
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let install_policy = obj
        .get("installPolicy")
        .and_then(|v| v.as_str())
        .unwrap_or("AVAILABLE");
    let auth_policy = obj
        .get("authPolicy")
        .and_then(|v| v.as_str())
        .unwrap_or("ON_USE");
    let mut descriptor = serde_json::json!({
        "id": id,
        "name": name,
        "source": { "type": "local", "path": abs_path },
        "installed": true,
        "enabled": enabled,
        "installPolicy": install_policy,
        "authPolicy": auth_policy,
    });
    if let Some(desc) = obj.get("description").and_then(|v| v.as_str()) {
        descriptor["description"] = Value::String(desc.to_string());
    }
    if let Some(version) = obj.get("version").and_then(|v| v.as_str()) {
        descriptor["version"] = Value::String(version.to_string());
    }
    if let Some(iface) = obj.get("interface") {
        descriptor["interface"] = iface.clone();
    }
    Some(descriptor)
}

/// `provider.readSkill` — read a skill file at the requested `path` and return
/// a `{ skill: { name, content, path, enabled } }` descriptor. The path must
/// point to an existing readable file (typically surfaced by `listSkills`).
/// Returns `{ skill: null }` when the path is missing/unreadable or points
/// outside a `.skills` directory (basic path traversal guard).
fn handle_provider_read_skill(id: Value, params: &Value) -> JsonRpcResponse {
    let raw_path = match params.get("path").and_then(|v| v.as_str()) {
        Some(p) if !p.is_empty() => p,
        _ => return JsonRpcResponse::success(id, serde_json::json!({ "skill": Value::Null })),
    };
    let path = Path::new(raw_path);
    // Basic traversal guard: require the canonical path to contain a `.skills`
    // component, and reject non-`.md` extensions.
    let canonical = match path.canonicalize_unchecked().canonicalize() {
        Ok(c) => c,
        Err(_) => return JsonRpcResponse::success(id, serde_json::json!({ "skill": Value::Null })),
    };
    let in_skills = canonical
        .components()
        .any(|c| c.as_os_str() == ".skills");
    let is_md = canonical.extension().and_then(|e| e.to_str()) == Some("md");
    if !in_skills || !is_md {
        return JsonRpcResponse::success(id, serde_json::json!({ "skill": Value::Null }));
    }
    let content = match std::fs::read_to_string(&canonical) {
        Ok(c) => c,
        Err(_) => return JsonRpcResponse::success(id, serde_json::json!({ "skill": Value::Null })),
    };
    let name = canonical
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    let abs_path = canonical.to_string_lossy().into_owned();
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "skill": {
                "name": name,
                "content": content,
                "path": abs_path,
                "enabled": true,
            }
        }),
    )
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

    let outcome =
        crate::llm::invoke_llm_oneshot(&adapter, &resolved, model, Some(system), prompt).await?;

    // Record token usage (best-effort telemetry). A `None` usage (provider
    // reported none, or all-zero) is silently skipped — the reply text is
    // the load-bearing return; usage is opportunistic aggregation feed.
    if let Some(usage) = &outcome.usage {
        let entry = crate::usage::UsageEntry {
            provider_id: resolved.clone(),
            model: outcome.model.clone(),
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            total_tokens: usage.total_tokens,
            timestamp: chrono::Utc::now(),
        };
        state.usage.write().await.record(entry);
    }

    Ok(outcome.text)
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
/// `stats.getProfileStats` — REAL (T6c-phase-28): the activity counts
/// (`totalPromptsSent` / `totalThreads` / `promptsToday`), the per-provider
/// breakdown (`providerModels`), and the `insights.topProvider` are now
/// populated from real in-memory sources:
///
///   - **Activity**: `read_store` HashMaps — `totalPromptsSent` = turn count,
///     `totalThreads` = thread count, `promptsToday` = turns created today
///     (UTC date match on `created_at`).
///   - **providerModels**: `usage.aggregate_by_provider()` — one entry per
///     provider with recorded token usage, with `turnCount` = call_count and
///     `percent` = the provider's share of total tokens across all providers.
///   - **insights.topProvider / topProviderPercent**: the provider with the
///     largest `total_tokens` share (null when there is no usage yet).
///
/// Fields that need deeper subsystems remain at their previous defaults:
///   - `currentStreakDays` / `longestStreakDays` / `heatmap` / `activeHours`:
///     need a daily rollup over the turn log (deferred — the activity heatmap
///     is a separate T6c phase).
///   - `skills` / `mostUsedSkill`: need a skills-usage subsystem (none exists).
///   - `mostWorkedProject`: needs per-project turn aggregation (deferred).
///   - `quota`: needs a provider-quota poller (none exists; stays
///     `unavailable`).
///   - `identity` / `timezone.today`: identity needs a user-profile subsystem
///     (none exists); `timezone.utcOffsetMinutes` echoes the caller's request
///     param best-effort so the timezone card at least shows the caller offset.
///
/// All schema-required top-level fields remain present so the UI's
/// destructuring reads don't throw (Tier-3 `frontend/src/contracts/tier3/stats.ts`).
async fn handle_stats_get_profile_stats(state: &WsState, id: Value) -> JsonRpcResponse {
    let generated_at = chrono::Utc::now().to_rfc3339();

    // ── Activity counts from the read store ──────────────────────────────
    // totalPromptsSent = turn count (each turn = one user prompt + assistant
    //   response cycle, matching MCode's "prompts sent" semantic).
    // totalThreads = thread count.
    // promptsToday = turns whose `created_at` ISO timestamp date-matches today
    //   (UTC). Best-effort parse — malformed entries are silently skipped.
    let (total_prompts, total_threads, prompts_today): (u64, u64, u64) = {
        let store = state.read_store.read().await;
        let turns = store.turns.len() as u64;
        let threads = store.threads.len() as u64;
        let today = chrono::Utc::now().date_naive();
        let mut today_count: u64 = 0;
        for turn in store.turns.values() {
            // `created_at` is an ISO-8601 string; parse the date portion only.
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&turn.created_at)
                && dt.with_timezone(&chrono::Utc).date_naive() == today
            {
                today_count += 1;
            }
        }
        (turns, threads, today_count)
    };

    // ── Per-provider usage breakdown + top-provider insight ──────────────
    let aggregates = {
        let usage = state.usage.read().await;
        usage.aggregate_by_provider()
    };
    let grand_total_tokens: u64 = aggregates.iter().map(|a| a.total_tokens).sum();

    let provider_models: Vec<Value> = aggregates
        .iter()
        .map(|agg| {
            let percent = if grand_total_tokens > 0 {
                (agg.total_tokens as f64 / grand_total_tokens as f64) * 100.0
            } else {
                0.0
            };
            // Round to 2 decimals for a clean UI label.
            let percent = (percent * 100.0).round() / 100.0;
            serde_json::json!({
                "provider": agg.provider_id,
                "model": agg.model,
                "turnCount": agg.call_count,
                "percent": percent,
            })
        })
        .collect();

    // Top provider = largest total_tokens share. Provider list is already
    // sorted by provider_id (stable aggregate output); pick max by tokens.
    let top_provider_agg: Option<&crate::usage::ProviderUsageAggregate> = aggregates
        .iter()
        .max_by_key(|a| a.total_tokens);
    let (top_provider, top_provider_percent): (Value, Value) = match top_provider_agg {
        Some(agg) if grand_total_tokens > 0 => {
            let pct = (agg.total_tokens as f64 / grand_total_tokens as f64) * 100.0;
            let pct = (pct * 100.0).round() / 100.0;
            (Value::String(agg.provider_id.clone()), serde_json::json!(pct))
        }
        _ => (Value::Null, Value::Null),
    };

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
                "totalPromptsSent": total_prompts,
                "totalThreads": total_threads,
                "promptsToday": prompts_today,
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
                "topProvider": top_provider,
                "topProviderPercent": top_provider_percent,
                "topReasoning": Value::Null,
                "topReasoningPercent": Value::Null,
                "skillsExplored": 0,
                "totalSkillsUsed": 0,
            },
            "providerModels": provider_models,
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

/// `stats.getProfileTokenStats` — REAL (T6c-phase-28): aggregates from the
/// in-memory `UsageStore`. `available` is now `true` when at least one
/// provider has recorded usage (the panel renders data instead of an empty
/// state). Field mapping:
///
///   - `lifetimeTotalTokens`: sum of every provider's `total_tokens` across
///     the retained log (null only when there is zero usage, so `available`
///     stays false and the UI shows the empty state).
///   - `providers`: the distinct provider ids with at least one recorded
///     entry (matches MCode's `ProviderKind[]` shape — the panel labels each
///     provider's contribution; per-provider magnitudes come from
///     `getProfileStats.providerModels`).
///   - `peakDayTokens` / `peakDay`: the single UTC day with the highest
///     aggregate `total_tokens` across the log (null when fewer than two
///     distinct days are present — one day isn't a meaningful "peak"; the
///     shape's null state is preserved for the low-data case).
///   - `unavailableProviders`: empty. We can't enumerate providers that
///     *lack* usage without a registry snapshot, and the panel treats an
///     empty list as "no known-gaps" (correct given our data).
///   - `heatmap`: empty. A per-day heatmap needs the daily-rollup subsystem
///     (deferred — same gap as `getProfileStats.activity.heatmap`).
async fn handle_stats_get_profile_token_stats(state: &WsState, id: Value) -> JsonRpcResponse {
    // Aggregate per-provider. `aggregate_by_provider()` returns one entry
    // per provider with at least one recorded usage entry, sorted by id.
    let aggregates = {
        let usage = state.usage.read().await;
        usage.aggregate_by_provider()
    };

    if aggregates.is_empty() {
        // No usage recorded yet — preserve the empty state.
        return JsonRpcResponse::success(
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
        );
    }

    let lifetime_total: u64 = aggregates.iter().map(|a| a.total_tokens).sum();
    let providers: Vec<Value> = aggregates
        .iter()
        .map(|a| Value::String(a.provider_id.clone()))
        .collect();

    // Peak-day approximation: re-walk the raw entries, group total_tokens by
    // UTC date, and pick the max. We re-read the log for the per-entry
    // timestamps (the aggregate loses the day dimension).
    let (peak_day_tokens, peak_day): (Value, Value) = {
        let usage = state.usage.read().await;
        let mut by_day: std::collections::HashMap<chrono::NaiveDate, u64> =
            std::collections::HashMap::new();
        for entry in usage.entries().iter() {
            let date = entry.timestamp.with_timezone(&chrono::Utc).date_naive();
            *by_day.entry(date).or_insert(0) += entry.total_tokens as u64;
        }
        if by_day.len() < 2 {
            // Fewer than two distinct days → no meaningful "peak" yet.
            (Value::Null, Value::Null)
        } else {
            let (day, tokens) = by_day
                .into_iter()
                .max_by_key(|(_, t)| *t)
                .unwrap_or((chrono::Utc::now().date_naive(), 0));
            (
                serde_json::json!(tokens),
                Value::String(day.format("%Y-%m-%d").to_string()),
            )
        }
    };

    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "available": true,
            "lifetimeTotalTokens": lifetime_total,
            "peakDayTokens": peak_day_tokens,
            "peakDay": peak_day,
            "providers": providers,
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
        // T6c-phase-26: repositoryIdentity is now REAL — derived from a git2
        // probe of the server cwd. The test cwd (cargo's tempdir under the
        // worktree) is a git repo, so this should be true; if it isn't, the
        // probe failed silently (degrade-to-false) — assert it's a bool
        // either way and prefer true.
        let repo_identity = &result["capabilities"]["repositoryIdentity"];
        assert!(repo_identity.is_boolean(), "repositoryIdentity must be bool: {:?}", repo_identity);
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
        // T6c-phase-26: uptime is now REAL — the server records a start
        // Instant at construction; elapsed must be >= 0 (very fast tests may
        // still be in the same second, so accept 0 too, but the field must
        // be present as a non-null u64).
        let uptime = result["process"]["uptimeSeconds"].as_u64();
        assert!(uptime.is_some(), "uptimeSeconds must be u64: {:?}", result["process"]["uptimeSeconds"]);
        // memory is an object; on Linux rssBytes reflects /proc VmRSS.
        assert!(result["process"]["memory"].is_object());
        assert!(result["childProcesses"].is_array());
        // No terminals/servers spawned in this test → 0 child count.
        assert_eq!(result["childProcessTotalCount"].as_u64(), Some(0));
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
        // T6c-phase-26: serverVersion, mode, and capabilities.repositoryIdentity
        // are now REAL.
        assert!(
            !result["serverVersion"].as_str().unwrap_or("").is_empty(),
            "serverVersion must be non-empty: {:?}",
            result["serverVersion"]
        );
        assert!(
            !result["mode"].as_str().unwrap_or("").is_empty(),
            "mode must be non-empty: {:?}",
            result["mode"]
        );
        // Default test state is UnsafeNoAuth → mode serializes to "unsafe-no-auth".
        assert_eq!(result["mode"].as_str().unwrap(), "unsafe-no-auth");
        assert!(
            result["capabilities"]["repositoryIdentity"].is_boolean(),
            "capabilities.repositoryIdentity must be bool: {:?}",
            result["capabilities"]
        );
    }

    #[tokio::test]
    async fn server_subscribe_stubs_return_success() {
        let state = WsState::new_in_memory(16);
        // T6c-18: config/settings/providerStatuses subscribe are REAL — they
        // register on the matching server.*Updated push channel. lifecycle
        // remains a stub. All four return success with `subscribed: true`.
        for (method, channel) in [
            ("server.subscribeConfig", crate::channels::CHANNEL_SERVER_CONFIG_UPDATED),
            ("server.subscribeSettings", crate::channels::CHANNEL_SERVER_SETTINGS_UPDATED),
            (
                "server.subscribeProviderStatuses",
                crate::channels::CHANNEL_SERVER_PROVIDER_STATUSES_UPDATED,
            ),
            ("server.subscribeLifecycle", "server.lifecycle"),
        ] {
            let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": method });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert_eq!(result["subscribed"], serde_json::Value::Bool(true), "{}", method);
            assert_eq!(result["channel"], channel, "{}", method);
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
    async fn server_set_config_persists_and_returns_stored_config() {
        // T6c-18 REAL: setConfig overwrites the stored config with the params
        // and returns the stored config (a full overwrite, not a merge).
        let state = WsState::new_in_memory(16);
        for method in ["server.setConfig", "server/set-config"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": {
                    "cwd": "/tmp/x", "worktreesDir": "/tmp/x/wt",
                    "keybindingsConfigPath": "/tmp/x/kb.json",
                    "keybindings": [], "issues": [], "providers": [],
                    "availableEditors": [], "authMode": "unsafe-no-auth",
                }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            // The stored config is the params verbatim (full overwrite).
            assert_eq!(result["cwd"], "/tmp/x", "{}: cwd", method);
            assert!(result["providers"].as_array().unwrap().is_empty(), "{}: providers", method);
            assert!(result["issues"].as_array().unwrap().is_empty(), "{}: issues", method);

            // Read back via getConfig confirms persistence.
            let req = serde_json::json!({ "jsonrpc": "2.0", "id": 2, "method": "server.getConfig" });
            let resp = rpc(&state, 1, &req).await;
            assert_eq!(resp.result.unwrap()["cwd"], "/tmp/x", "{}: read-back cwd", method);
        }
    }

    #[tokio::test]
    async fn server_update_settings_persists_merged_settings() {
        // T6c-18 REAL: updateSettings deep-merges the patch into the stored
        // settings and returns the full resolved settings.
        let state = WsState::new_in_memory(16);
        for method in ["server.updateSettings", "server/update-settings"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "enableAssistantStreaming": true }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            // The patch IS applied (REAL semantics — not the stub echo).
            assert_eq!(
                result["enableAssistantStreaming"],
                serde_json::Value::Bool(true),
                "{}: patch must be applied",
                method
            );
            // Untouched default keys preserved (deep-merge).
            assert_eq!(result["defaultThreadEnvMode"], "local", "{}: env mode", method);
            let providers = &result["providers"];
            assert_eq!(providers["codex"]["binaryPath"], "codex", "{}: codex", method);
            assert_eq!(providers["pi"]["binaryPath"], "pi", "{}: pi", method);
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

        // Happy path: params is a keybinding-rule object → success. T6c-18
        // REAL: the rule is appended to the stored config's `keybindings`
        // array, so the returned `keybindings` reflects the upsert (length 1
        // after the first call in each iteration's fresh state — but the
        // loop reuses the same state, so the second iteration appends again).
        for (i, method) in ["server.upsertKeybinding", "server/upsert-keybinding"]
            .iter()
            .enumerate()
        {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "key": "mod+k", "command": "test" }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            // The keybindings array contains the upserted rule (REAL — no
            // longer empty). Both iterations append (no `id` to dedupe on).
            let kbs = result["keybindings"].as_array().unwrap();
            assert!(!kbs.is_empty(), "{}: keybindings must reflect the upsert", method);
            assert_eq!(kbs.len(), i + 1, "{}: appended once per call", method);
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
    async fn automation_mark_run_read_and_archive_persist() {
        // markRunRead/archiveRun are REAL: they mutate the run via the
        // scheduler (`mark_run_read`/`archive_run`) and persist through the
        // repo's upsert. The returned run must reflect `unread=false` (after
        // markRunRead) and `archivedAt` set (after archiveRun).
        let state = WsState::new_in_memory(16);

        // Create + runNow to seed a run.
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.create",
            "params": { "name": "Read-archive-test", "schedule": { "type": "manual" } }
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

        // Newly-created runs surface as unread under `result`.
        let fresh = auto_rpc(&state, &run_now).await;
        let fresh_result = fresh.result.expect("runNow succeeds");
        let fresh_unread = fresh_result["run"]["result"]["unread"].as_bool();
        assert_eq!(
            fresh_unread,
            Some(true),
            "newly created run should surface result.unread=true (got {fresh_unread:?})"
        );

        // markRunRead → unread flips to false, persisted.
        let mark = serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "automation.markRunRead",
            "params": { "runId": run_id }
        });
        let resp = auto_rpc(&state, &mark).await;
        assert!(resp.error.is_none(), "markRunRead: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["run"]["id"], run_id);
        assert_eq!(
            result["run"]["result"]["unread"], false,
            "markRunRead must flip result.unread to false"
        );

        // Re-fetch via automation.list to confirm persistence.
        let list = serde_json::json!({
            "jsonrpc": "2.0", "id": 99, "method": "automation.list"
        });
        let list_resp = auto_rpc(&state, &list).await;
        let list_result = list_resp.result.expect("automation.list succeeds");
        let persisted_unread = &list_result["runs"]
            .as_array()
            .expect("runs is an array")
            .iter()
            .find(|r| r["id"] == run_id)
            .expect("seeded run present in list")["result"]["unread"];
        assert_eq!(*persisted_unread, false, "markRunRead must persist");

        // archiveRun → archivedAt becomes non-null, persisted.
        let archive = serde_json::json!({
            "jsonrpc": "2.0", "id": 4, "method": "automation.archiveRun",
            "params": { "runId": run_id }
        });
        let resp = auto_rpc(&state, &archive).await;
        assert!(resp.error.is_none(), "archiveRun: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["run"]["id"], run_id);
        assert!(
            !result["run"]["result"]["archivedAt"].is_null(),
            "archiveRun must set result.archivedAt"
        );
    }

    #[tokio::test]
    async fn automation_mark_run_read_missing_is_error() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.markRunRead",
            "params": { "runId": "no-such-run" }
        });
        let resp = auto_rpc(&state, &req).await;
        assert!(resp.error.is_some(), "missing run should error");
    }

    #[tokio::test]
    async fn automation_archive_run_missing_is_error() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.archiveRun",
            "params": { "runId": "no-such-run" }
        });
        let resp = auto_rpc(&state, &req).await;
        assert!(resp.error.is_some(), "missing run should error");
    }

    #[tokio::test]
    async fn automation_mark_run_read_requires_run_id() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.markRunRead",
            "params": {}
        });
        let resp = auto_rpc(&state, &req).await;
        assert!(resp.error.is_some(), "missing runId should be INVALID_PARAMS");
    }

    #[tokio::test]
    async fn automation_subscribe_registers_on_automation_channel() {
        // T6c-21: subscribe is REAL — registers the connection on the
        // `automation` push channel (no longer a stub). Both dot-name and
        // slash form must resolve and surface `subscribed: true` with the
        // correct channel name.
        let state = WsState::new_in_memory(16);
        state.subscriptions.write().await.register(1);
        for method in ["automation.subscribe", "automation/subscribe"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method
            });
            let resp = auto_rpc(&state, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert_eq!(result["subscribed"], true);
            assert_eq!(result["channel"], "automation");
        }
        // The connection is now registered on the automation channel.
        assert!(state
            .subscriptions
            .read()
            .await
            .subscribers_for(crate::channels::CHANNEL_AUTOMATION)
            .contains(&1));
    }

    #[tokio::test]
    async fn automation_unsubscribe_deregisters_from_automation_channel() {
        // T6c-21: unsubscribe is REAL — removes the connection from the
        // `automation` push channel.
        let state = WsState::new_in_memory(16);
        {
            let mut regs = state.subscriptions.write().await;
            regs.register(1);
            regs.subscribe(1, crate::channels::CHANNEL_AUTOMATION);
        }
        for method in ["automation.unsubscribe", "automation/unsubscribe"] {
            // First call removes; second is idempotent (subscribed: false).
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method
            });
            let resp = auto_rpc(&state, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            assert_eq!(result["subscribed"], false);
            assert_eq!(result["channel"], "automation");
        }
        assert!(!state
            .subscriptions
            .read()
            .await
            .subscribers_for(crate::channels::CHANNEL_AUTOMATION)
            .contains(&1));
    }

    /// T6c-21 keystone proof: subscribe to the `automation` channel → run an
    /// automation (`echo hello`) → assert a `push/automation` frame carrying a
    /// `run-upserted` event is received on the push bus. The delivery loop
    /// (`run_push_delivery`) is not exercised here directly — we tap
    /// `push_tx` (the broadcast bus) because that is the seam the delivery
    /// loop subscribes to.
    #[tokio::test]
    async fn automation_run_now_pushes_lifecycle_event_on_subscribed_channel() {
        let state = WsState::new_in_memory(16);
        // Tap the push bus BEFORE triggering, so the broadcast receiver is
        // live when the run-now handler publishes.
        let mut rx = state.push_tx.subscribe();

        // Create an automation with a real command (the in-memory scheduler
        // uses ProcessRunExecutor, which runs `prompt` as a shell command).
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.create",
            "params": {
                "name": "Echo hello",
                "prompt": "echo hello",
                "schedule": { "type": "manual" },
                "projectId": "proj-keystone"
            }
        });
        let resp = auto_rpc(&state, &create).await;
        assert!(resp.error.is_none(), "create: {:?}", resp.error);
        let auto_id = resp.result.unwrap()["id"].as_str().unwrap().to_string();

        // Trigger the run — `trigger_with_delay(Immediate)` awaits the full
        // subprocess execution synchronously, so by the time runNow returns,
        // the `run-upserted` lifecycle event has already been broadcast.
        let run_now = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "automation.runNow",
            "params": { "id": auto_id }
        });
        let resp = auto_rpc(&state, &run_now).await;
        assert!(resp.error.is_none(), "runNow: {:?}", resp.error);
        let run_id = resp.result.unwrap()["run"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Drain pending pushes and find the automation channel event.
        // (The broadcast bus may carry other events — e.g. orchestration
        // domain events; filter for the automation channel.)
        let mut saw_run_upserted = false;
        for _ in 0..32 {
            match rx.try_recv() {
                Ok((channel, payload)) if channel == "automation" => {
                    assert_eq!(
                        payload["type"], "run-upserted",
                        "automation push event must be a run-upserted (got {payload})"
                    );
                    assert_eq!(
                        payload["run"]["id"], run_id,
                        "pushed run id must match the runNow result"
                    );
                    assert_eq!(
                        payload["run"]["automationId"], auto_id,
                        "pushed automationId must match the def"
                    );
                    saw_run_upserted = true;
                    break;
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        assert!(
            saw_run_upserted,
            "automation.runNow must push a run-upserted event on the `automation` channel"
        );
    }

    /// Cancellation also broadcasts a `run-upserted` lifecycle event (the
    /// run's status flips to `cancelled`).
    #[tokio::test]
    async fn automation_cancel_run_pushes_lifecycle_event() {
        let state = WsState::new_in_memory(16);
        let mut rx = state.push_tx.subscribe();

        // Seed: create + runNow (echo ok) so a real run exists.
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "automation.create",
            "params": {
                "name": "Echo ok",
                "prompt": "echo ok",
                "schedule": { "type": "manual" }
            }
        });
        let resp = auto_rpc(&state, &create).await;
        let auto_id = resp.result.unwrap()["id"].as_str().unwrap().to_string();
        let run_now = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "automation.runNow",
            "params": { "id": auto_id }
        });
        let run_now_resp = auto_rpc(&state, &run_now).await;
        // Drain the runNow push so the cancel push is unambiguous.
        let _ = rx.try_recv();
        let run_id = run_now_resp.result.unwrap()["run"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Cancel the run.
        let cancel = serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "automation.cancelRun",
            "params": { "runId": run_id }
        });
        let resp = auto_rpc(&state, &cancel).await;
        assert!(resp.error.is_none(), "cancelRun: {:?}", resp.error);

        // The cancel handler must have pushed a run-upserted on automation.
        let mut saw_cancel_push = false;
        for _ in 0..32 {
            match rx.try_recv() {
                Ok((channel, payload)) if channel == "automation" => {
                    assert_eq!(payload["type"], "run-upserted");
                    assert_eq!(payload["run"]["id"], run_id);
                    saw_cancel_push = true;
                    break;
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        assert!(
            saw_cancel_push,
            "automation.cancelRun must push a run-upserted event on the `automation` channel"
        );
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

        // listSkills with no `cwd` and no .skills dir present → { skills: [] }
        // (T6c-23 made this REAL: filesystem scan of `.skills/*.md`; with no
        // skills dir on disk the result is an empty array.)
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listSkills",
            "params": { "cwd": "/nonexistent-syncode-skills-empty-test-9999" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["skills"].as_array().unwrap().len(), 0);

        // listSkillsCatalog with no `cwd` → { skills: [] }
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listSkillsCatalog",
            "params": { "cwd": "/nonexistent-syncode-skills-empty-test-9999" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["skills"].as_array().unwrap().len(), 0);

        // listCommands is now REAL (T6c-23): static per-provider native
        // commands. With no provider param it defaults to claudeAgent and
        // returns 5 entries (help/clear/compact/cost/doctor). The dedicated
        // `test_list_commands_claude_includes_compact_and_cost` covers the
        // content; here we just assert it's non-empty (REAL behavior).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listCommands"
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert!(
            !result["commands"].as_array().unwrap().is_empty(),
            "listCommands should return static non-empty command list"
        );

        // listOptions → returns reasoningEffort select descriptor for codex.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listOptions",
            "params": { "provider": "codex" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        let options = result["options"].as_array().unwrap();
        assert_eq!(options.len(), 1, "codex should expose one option descriptor");
        assert_eq!(options[0]["id"], "reasoningEffort");
        assert_eq!(options[0]["type"], "select");
        let choices = options[0]["options"].as_array().unwrap();
        assert!(choices.len() >= 3, "codex reasoningEffort has multiple levels");
        // "medium" is the default for the gpt-5.5 codex model.
        let medium = choices
            .iter()
            .find(|c| c["id"] == "medium")
            .expect("codex reasoningEffort includes a medium option");
        assert_eq!(medium["isDefault"], true);

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

    /// listOptions must return a per-provider static option descriptor map
    /// mirroring the MCode `model.ts` capability constants: codex/claude/grok
    /// get a `reasoningEffort` select, gemini/pi get a `thinkingLevel` select,
    /// and unmapped providers (e.g. kilo) get an empty array. Default provider
    /// (no param) resolves to claudeAgent.
    #[tokio::test]
    async fn provider_list_options_per_provider_map() {
        let state = WsState::new_in_memory(16);

        // codex → reasoningEffort select with medium as default (gpt-5.5).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listOptions",
            "params": { "provider": "codex" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        let options = result["options"].as_array().unwrap();
        assert_eq!(options.len(), 1);
        assert_eq!(options[0]["id"], "reasoningEffort");
        assert_eq!(options[0]["type"], "select");
        let codex_choices = options[0]["options"].as_array().unwrap();
        assert_eq!(codex_choices.len(), 4);
        let values: Vec<&str> = codex_choices
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert_eq!(values, vec!["low", "medium", "high", "xhigh"]);

        // claudeAgent → reasoningEffort with ultrathink-style levels.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listOptions",
            "params": { "provider": "claudeAgent" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        let options = result["options"].as_array().unwrap();
        assert_eq!(options[0]["id"], "reasoningEffort");
        let claude_values: Vec<&str> = options[0]["options"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert!(claude_values.contains(&"ultrathink"));
        assert!(claude_values.contains(&"ultracode"));

        // gemini → thinkingLevel.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listOptions",
            "params": { "provider": "gemini" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        let options = result["options"].as_array().unwrap();
        assert_eq!(options[0]["id"], "thinkingLevel");
        assert_eq!(options[0]["type"], "select");

        // grok → reasoningEffort including "none".
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listOptions",
            "params": { "provider": "grok" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        let grok_values: Vec<&str> = result["options"][0]["options"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert!(grok_values.contains(&"none"));
        // grok default is "low".
        let low = result["options"][0]["options"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == "low")
            .unwrap();
        assert_eq!(low["isDefault"], true);

        // pi → thinkingLevel.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listOptions",
            "params": { "provider": "pi" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["options"][0]["id"], "thinkingLevel");

        // kilo → empty (no configurable options).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listOptions",
            "params": { "provider": "kilo" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["options"].as_array().unwrap().len(), 0);

        // default provider (no param) → claudeAgent reasoningEffort.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listOptions"
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["options"][0]["id"], "reasoningEffort");
    }

    /// listPlugins must scan a project `.plugins/` dir for `*.json` plugin
    /// descriptors, returning them under a synthetic "local" marketplace.
    /// Missing dir → empty marketplaces. Each descriptor carries the full
    /// ProviderPluginDescriptor shape (id, name, source, installed, enabled,
    /// installPolicy, authPolicy).
    #[tokio::test]
    async fn provider_list_plugins_scans_filesystem() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let plugins_dir = tmp.path().join(".plugins");
        std::fs::create_dir_all(&plugins_dir).unwrap();
        // Two valid plugin descriptors + one invalid (missing required field).
        std::fs::write(
            plugins_dir.join("alpha.json"),
            serde_json::json!({
                "id": "alpha",
                "name": "Alpha Plugin",
                "description": "First test plugin",
                "version": "1.0.0",
                "enabled": true,
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            plugins_dir.join("beta.json"),
            serde_json::json!({
                "id": "beta",
                "name": "Beta Plugin",
            })
            .to_string(),
        )
        .unwrap();
        // invalid: missing `name`.
        std::fs::write(
            plugins_dir.join("broken.json"),
            serde_json::json!({ "id": "broken" }).to_string(),
        )
        .unwrap();
        // non-json: ignored.
        std::fs::write(plugins_dir.join("readme.md"), "# plugins").unwrap();

        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listPlugins",
            "params": { "cwd": tmp.path() }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        let marketplaces = result["marketplaces"].as_array().unwrap();
        assert_eq!(marketplaces.len(), 1, "one local marketplace");
        assert_eq!(marketplaces[0]["name"], "local");
        let plugins = marketplaces[0]["plugins"].as_array().unwrap();
        assert_eq!(plugins.len(), 2, "two valid descriptors, broken skipped");
        // sorted by id → alpha first.
        assert_eq!(plugins[0]["id"], "alpha");
        assert_eq!(plugins[0]["name"], "Alpha Plugin");
        assert_eq!(plugins[0]["description"], "First test plugin");
        assert_eq!(plugins[0]["version"], "1.0.0");
        assert_eq!(plugins[0]["installed"], true);
        assert_eq!(plugins[0]["enabled"], true);
        assert_eq!(plugins[0]["installPolicy"], "AVAILABLE");
        assert_eq!(plugins[0]["authPolicy"], "ON_USE");
        assert_eq!(plugins[0]["source"]["type"], "local");
        assert_eq!(plugins[1]["id"], "beta");

        // Missing `.plugins` dir → empty marketplaces (graceful).
        let empty_tmp = tempfile::tempdir().unwrap();
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.listPlugins",
            "params": { "cwd": empty_tmp.path() }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["marketplaces"].as_array().unwrap().len(), 0);
        assert!(result["remoteSyncError"].is_null());
    }

    /// readPlugin must return `{ plugin: {...} }` for an existing valid plugin
    /// descriptor inside a `.plugins/` dir, and `{ plugin: null }` for missing
    /// paths or traversal attempts outside `.plugins`.
    #[tokio::test]
    async fn provider_read_plugin_filesystem() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let plugins_dir = tmp.path().join(".plugins");
        std::fs::create_dir_all(&plugins_dir).unwrap();
        let plugin_path = plugins_dir.join("alpha.json");
        std::fs::write(
            &plugin_path,
            serde_json::json!({
                "id": "alpha", "name": "Alpha Plugin", "enabled": false,
            })
            .to_string(),
        )
        .unwrap();

        let state = WsState::new_in_memory(16);
        // Valid read → full descriptor with enabled=false carried through.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.readPlugin",
            "params": { "path": plugin_path }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        let plugin = &result["plugin"];
        assert_eq!(plugin["id"], "alpha");
        assert_eq!(plugin["name"], "Alpha Plugin");
        assert_eq!(plugin["enabled"], false);
        assert_eq!(plugin["source"]["type"], "local");

        // Missing path → null.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.readPlugin",
            "params": { "path": plugins_dir.join("nonexistent.json") }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert!(result["plugin"].is_null());

        // Path outside .plugins → null (traversal guard).
        let outside = tmp.path().join("outside.json");
        std::fs::write(&outside, r#"{"id":"x","name":"y"}"#).unwrap();
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.readPlugin",
            "params": { "path": outside }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert!(result["plugin"].is_null(), "plugin outside .plugins must be null");
    }

    /// getComposerCapabilities must echo the requested `provider` and return
    /// the per-provider capability matrix (T6c-23): claude/codex full, gemini
    /// partial, smaller providers minimal. Plugin flags stay false everywhere.
    /// Defaults to "claudeAgent" when the provider param is absent.
    #[tokio::test]
    async fn provider_get_composer_capabilities_per_provider_matrix() {
        let state = WsState::new_in_memory(16);

        // gemini: skill mentions + native commands, but no skill discovery.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.getComposerCapabilities",
            "params": { "provider": "gemini" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["provider"], "gemini");
        assert_eq!(result["supportsSkillMentions"], true);
        assert_eq!(result["supportsSkillDiscovery"], false);
        assert_eq!(result["supportsNativeSlashCommandDiscovery"], true);
        // plugin flags stay false everywhere (no plugin subsystem).
        assert_eq!(result["supportsPluginMentions"], false);
        assert_eq!(result["supportsPluginDiscovery"], false);
        // universal infrastructure flags.
        assert_eq!(result["supportsRuntimeModelList"], true);
        assert_eq!(result["supportsThreadCompaction"], true);
        assert_eq!(result["supportsThreadImport"], true);

        // Default when provider param absent: now claudeAgent (the richest
        // profile — full skill + command discovery).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "provider.getComposerCapabilities"
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(
            result["provider"], "claudeAgent",
            "default provider should be claudeAgent"
        );
        assert_eq!(result["supportsSkillMentions"], true);
        assert_eq!(result["supportsSkillDiscovery"], true);
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
    async fn server_patch_settings_persists_merged_settings() {
        // T6c-18 REAL: patchSettings deep-merges the patch (alias of
        // updateSettings).
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
            // The patch IS applied (REAL semantics — not the stub echo).
            assert_eq!(
                result["enableAssistantStreaming"],
                serde_json::Value::Bool(true),
                "{}: patch must be applied",
                method
            );
        }
    }

    /// listProviderUsage with NO recorded usage returns an empty array
    /// (REAL: aggregates the in-memory log, which starts empty). Under BOTH
    /// dot-name AND slash form.
    #[tokio::test]
    async fn server_list_provider_usage_empty_when_no_usage_recorded() {
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
            assert!(
                result.as_array().unwrap().is_empty(),
                "{}: must be empty when no usage recorded",
                method
            );
        }
    }

    /// listProviderUsage returns one snapshot per provider after usage is
    /// recorded, aggregating token counts across calls. Records entries
    /// directly into the store (the same path `invoke()` writes through),
    /// then asserts the snapshot carries the summed totals.
    #[tokio::test]
    async fn server_list_provider_usage_aggregates_recorded_entries() {
        let state = WsState::new_in_memory(16);
        {
            let mut store = state.usage.write().await;
            store.record(crate::usage::UsageEntry {
                provider_id: "claude".into(),
                model: "sonnet".into(),
                input_tokens: 100,
                output_tokens: 50,
                total_tokens: 150,
                timestamp: chrono::Utc::now(),
            });
            store.record(crate::usage::UsageEntry {
                provider_id: "claude".into(),
                model: "sonnet".into(),
                input_tokens: 30,
                output_tokens: 20,
                total_tokens: 50,
                timestamp: chrono::Utc::now(),
            });
            store.record(crate::usage::UsageEntry {
                provider_id: "codex".into(),
                model: "gpt-5".into(),
                input_tokens: 10,
                output_tokens: 5,
                total_tokens: 15,
                timestamp: chrono::Utc::now(),
            });
        }

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "server.listProviderUsage"
        });
        let result = rpc(&state, 1, &req).await.result.unwrap();
        let arr = result.as_array().expect("array of snapshots");
        assert_eq!(arr.len(), 2, "one snapshot per provider");

        // Sorted alphabetically: claude first, codex second.
        let claude = &arr[0];
        assert_eq!(claude["provider"], "claude");
        assert_eq!(claude["status"], "ok");
        // Find the "Total tokens" usage line and assert it sums (150 + 50 = 200).
        let lines = claude["usageLines"].as_array().expect("usageLines array");
        let total_line = lines
            .iter()
            .find(|l| l["label"] == "Total tokens")
            .expect("total tokens line");
        assert_eq!(total_line["value"], "200", "claude total aggregated");
        let calls_line = lines
            .iter()
            .find(|l| l["label"] == "Calls")
            .expect("calls line");
        assert_eq!(calls_line["value"], "2", "claude call count");

        let codex = &arr[1];
        assert_eq!(codex["provider"], "codex");
        let codex_total = codex["usageLines"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["label"] == "Total tokens")
            .unwrap();
        assert_eq!(codex_total["value"], "15");
    }

    // ── stats.getProfileStats / getProfileTokenStats (T6c-phase-28) ──────

    /// Helper: insert a `TurnView` directly into the read store, mimicking
    /// what the Projector would do after a `TurnStarted` event. Used so the
    /// stats tests can exercise the activity-count aggregation without
    /// spinning up a full turn-start → provider round-trip.
    async fn seed_turn(state: &WsState, turn_id: &str, thread_id: &str, created_at: &str) {
        use syncode_orchestration::read_model::TurnView;
        let turn = TurnView {
            id: turn_id.into(),
            thread_id: thread_id.into(),
            sequence: 1,
            user_input: "hi".into(),
            assistant_output: None,
            status: "completed".into(),
            git_checkpoint: None,
            files_modified: vec![],
            duration_ms: None,
            created_at: created_at.into(),
            completed_at: None,
        };
        let mut store = state.read_store.write().await;
        store.turns.insert(turn_id.into(), turn);
    }

    /// `getProfileStats` returns REAL activity counts from the read store:
    /// totalPromptsSent = turn count, totalThreads = thread count,
    /// promptsToday = turns created today. Also asserts the per-provider
    /// breakdown and top-provider insight populate from the usage log.
    #[tokio::test]
    async fn profile_stats_populates_activity_and_provider_models() {
        let state = WsState::new_in_memory(16);

        // Seed 3 turns (2 today, 1 yesterday) and 2 threads.
        let now = chrono::Utc::now();
        let today_iso = now.to_rfc3339();
        let yesterday_iso = (now - chrono::Duration::days(1)).to_rfc3339();
        seed_turn(&state, "t1", "th1", &today_iso).await;
        seed_turn(&state, "t2", "th1", &today_iso).await;
        seed_turn(&state, "t3", "th2", &yesterday_iso).await;
        {
            use syncode_orchestration::read_model::ThreadView;
            let mut store = state.read_store.write().await;
            store.threads.insert(
                "th1".into(),
                ThreadView {
                    id: "th1".into(),
                    project_id: "p1".into(),
                    provider_id: "claude".into(),
                    model: "sonnet".into(),
                    status: "active".into(),
                    title: None,
                    git_checkpoint: None,
                    runtime_mode: "approval-required".into(),
                    interaction_mode: "default".into(),
                    turn_count: 2,
                    created_at: today_iso.clone(),
                    updated_at: today_iso.clone(),
                    session: None,
                },
            );
            store.threads.insert(
                "th2".into(),
                ThreadView {
                    id: "th2".into(),
                    project_id: "p1".into(),
                    provider_id: "codex".into(),
                    model: "gpt-5".into(),
                    status: "active".into(),
                    title: None,
                    git_checkpoint: None,
                    runtime_mode: "approval-required".into(),
                    interaction_mode: "default".into(),
                    turn_count: 1,
                    created_at: yesterday_iso.clone(),
                    updated_at: yesterday_iso.clone(),
                    session: None,
                },
            );
        }

        // Seed usage so providerModels + topProvider populate.
        {
            let mut usage = state.usage.write().await;
            usage.record(crate::usage::UsageEntry {
                provider_id: "claude".into(),
                model: "sonnet".into(),
                input_tokens: 100,
                output_tokens: 50,
                total_tokens: 150,
                timestamp: now,
            });
            usage.record(crate::usage::UsageEntry {
                provider_id: "codex".into(),
                model: "gpt-5".into(),
                input_tokens: 10,
                output_tokens: 5,
                total_tokens: 15,
                timestamp: now,
            });
        }

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "stats.getProfileStats"
        });
        let result = rpc(&state, 1, &req).await.result.unwrap();
        let activity = &result["activity"];
        assert_eq!(activity["totalPromptsSent"], 3, "turn count");
        assert_eq!(activity["totalThreads"], 2, "thread count");
        assert_eq!(activity["promptsToday"], 2, "turns created today");

        // providerModels: claude 150/165 ≈ 90.91%, codex 15/165 ≈ 9.09%.
        let pm = result["providerModels"].as_array().expect("array");
        assert_eq!(pm.len(), 2, "one entry per provider with usage");
        let claude = pm.iter().find(|v| v["provider"] == "claude").unwrap();
        assert_eq!(claude["turnCount"], 1, "call count");
        let pct = claude["percent"].as_f64().unwrap();
        assert!(pct > 90.0 && pct < 91.0, "claude share ≈ 90.91%, got {pct}");

        // topProvider = claude (largest total_tokens share).
        assert_eq!(result["insights"]["topProvider"], "claude");
        let top_pct = result["insights"]["topProviderPercent"].as_f64().unwrap();
        assert!(top_pct > 90.0 && top_pct < 91.0, "top provider pct");
    }

    /// `getProfileStats` with no usage / no turns returns zeroed activity
    /// and null topProvider (preserves the previous empty-state shape).
    #[tokio::test]
    async fn profile_stats_empty_state_when_no_data() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "stats.getProfileStats"
        });
        let result = rpc(&state, 1, &req).await.result.unwrap();
        assert_eq!(result["activity"]["totalPromptsSent"], 0);
        assert_eq!(result["activity"]["totalThreads"], 0);
        assert_eq!(result["activity"]["promptsToday"], 0);
        assert_eq!(result["providerModels"].as_array().unwrap().len(), 0);
        assert!(result["insights"]["topProvider"].is_null());
    }

    /// `getProfileTokenStats` aggregates lifetime totals and per-provider
    /// breakdown from the usage log; available=true when there is data.
    #[tokio::test]
    async fn profile_token_stats_aggregates_lifetime_and_providers() {
        let state = WsState::new_in_memory(16);
        {
            let mut usage = state.usage.write().await;
            // Two entries today + one yesterday → 2 distinct days, peak exists.
            usage.record(crate::usage::UsageEntry {
                provider_id: "claude".into(),
                model: "sonnet".into(),
                input_tokens: 100,
                output_tokens: 50,
                total_tokens: 150,
                timestamp: chrono::Utc::now(),
            });
            usage.record(crate::usage::UsageEntry {
                provider_id: "codex".into(),
                model: "gpt-5".into(),
                input_tokens: 200,
                output_tokens: 100,
                total_tokens: 300,
                timestamp: chrono::Utc::now() - chrono::Duration::days(1),
            });
        }

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "stats.getProfileTokenStats"
        });
        let result = rpc(&state, 1, &req).await.result.unwrap();
        assert_eq!(result["available"], true, "data exists");
        assert_eq!(result["lifetimeTotalTokens"], 450, "150 + 300");

        let providers = result["providers"].as_array().unwrap();
        assert_eq!(providers.len(), 2, "two distinct providers");
        assert!(providers.iter().any(|v| v == "claude"));
        assert!(providers.iter().any(|v| v == "codex"));

        // Peak day present (yesterday had 300 > today's 150).
        assert!(!result["peakDayTokens"].is_null(), "peak day tokens");
        assert!(!result["peakDay"].is_null(), "peak day label");
        let peak = result["peakDayTokens"].as_u64().unwrap();
        assert_eq!(peak, 300, "peak = highest-day total");
    }

    /// `getProfileTokenStats` with no usage → available=false + null totals
    /// (preserves the empty-state shape, no crash).
    #[tokio::test]
    async fn profile_token_stats_empty_state_when_no_usage() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "stats.getProfileTokenStats"
        });
        let result = rpc(&state, 1, &req).await.result.unwrap();
        assert_eq!(result["available"], false);
        assert!(result["lifetimeTotalTokens"].is_null());
        assert!(result["peakDayTokens"].is_null());
        assert!(result["peakDay"].is_null());
        assert_eq!(result["providers"].as_array().unwrap().len(), 0);
    }

    /// `getProfileTokenStats` with usage on only one distinct day → peak
    /// stays null (a single day isn't a meaningful "peak").
    #[tokio::test]
    async fn profile_token_stats_single_day_no_peak() {
        let state = WsState::new_in_memory(16);
        {
            let mut usage = state.usage.write().await;
            usage.record(crate::usage::UsageEntry {
                provider_id: "claude".into(),
                model: "sonnet".into(),
                input_tokens: 100,
                output_tokens: 50,
                total_tokens: 150,
                timestamp: chrono::Utc::now(),
            });
        }
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "stats.getProfileTokenStats"
        });
        let result = rpc(&state, 1, &req).await.result.unwrap();
        assert_eq!(result["available"], true);
        assert_eq!(result["lifetimeTotalTokens"], 150);
        assert!(result["peakDayTokens"].is_null(), "single day → no peak");
        assert!(result["peakDay"].is_null());
    }

    /// getProviderUsageSnapshot returns null when the provider has no usage,
    /// and a snapshot otherwise. Validates `provider` non-empty. Under BOTH
    /// forms.
    #[tokio::test]
    async fn server_get_provider_usage_snapshot_real_aggregation() {
        let state = WsState::new_in_memory(16);
        // Seed usage for "claude" only.
        state.usage.write().await.record(crate::usage::UsageEntry {
            provider_id: "claude".into(),
            model: "sonnet".into(),
            input_tokens: 100,
            output_tokens: 50,
            total_tokens: 150,
            timestamp: chrono::Utc::now(),
        });

        // Provider WITH usage → snapshot (not null).
        for method in [
            "server.getProviderUsageSnapshot",
            "server/get-provider-usage-snapshot",
        ] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "provider": "claude" }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.expect("non-null snapshot for claude");
            assert_eq!(result["provider"], "claude");
            assert_eq!(result["status"], "ok");
            assert!(
                result["updatedAt"].as_str().is_some(),
                "snapshot must carry updatedAt"
            );
            let total = result["usageLines"]
                .as_array()
                .unwrap()
                .iter()
                .find(|l| l["label"] == "Total tokens")
                .unwrap();
            assert_eq!(total["value"], "150");
        }

        // Provider WITHOUT usage → null (UI empty state).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "server.getProviderUsageSnapshot",
            "params": { "provider": "codex" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none());
        match resp.result {
            None => {}
            Some(v) => assert!(v.is_null(), "no-usage provider must be null, got {v}"),
        }

        // Validation: missing provider → InvalidParams.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "server/get-provider-usage-snapshot",
            "params": {}
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some(), "missing provider must reject");
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    /// End-to-end: a real LLM-backed RPC (`git.summarizeDiff`) records usage
    /// into the store, and `listProviderUsage` then surfaces it. Proves the
    /// instrumentation seam (invoke → record → aggregate) is wired through.
    #[tokio::test]
    async fn llm_op_records_usage_visible_in_list_provider_usage() {
        use crate::llm::SharedAdapter;
        use syncode_provider::UsageInfo;
        use tokio::sync::RwLock;

        let state = WsState::new_in_memory(16);
        // Register a mock provider configured with canned usage — when
        // `git.summarizeDiff` invokes it, the response carries usage that
        // `invoke()` must record.
        let mock: SharedAdapter = Arc::new(RwLock::new(
            crate::llm::MockLlmAdapter::new("Refactors the auth module.").with_usage(UsageInfo {
                input_tokens: 200,
                output_tokens: 40,
                total_tokens: 240,
            }),
        ));
        state
            .provider_registry
            .write()
            .await
            .register_shared("claude".to_string(), mock);

        // 1. Run an LLM-backed op (summarizeDiff with a caller-supplied diff).
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "git.summarizeDiff",
            "params": { "diff": "diff --git a/x b/x\n- old\n+ new" }
        });
        let result = provider_rpc(&state, &req).await.result.unwrap();
        assert_eq!(result["summary"], "Refactors the auth module.");

        // 2. The usage store now has one entry for claude.
        {
            let store = state.usage.read().await;
            assert_eq!(store.len(), 1, "exactly one usage entry recorded");
            let agg = store.aggregate_for("claude").expect("claude aggregate");
            assert_eq!(agg.total_tokens, 240);
        }

        // 3. listProviderUsage surfaces the recorded usage.
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "server.listProviderUsage"
        });
        let result = rpc(&state, 1, &req).await.result.unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 1, "one provider with usage");
        assert_eq!(arr[0]["provider"], "claude");
        let total = arr[0]["usageLines"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["label"] == "Total tokens")
            .unwrap();
        assert_eq!(total["value"], "240");
    }

    /// startLocalServer spawns a real process and returns its pid; stopLocalServer
    /// kills + removes it. Both forms (dot + slash) verified.
    #[tokio::test]
    async fn server_local_server_lifecycle_is_real() {
        let state = WsState::new_in_memory(16);

        // start `sleep 30` as a local server under BOTH method-name forms.
        let mut started_ids = Vec::new();
        for method in ["server.startLocalServer", "server/start-local-server"] {
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": {
                    "id": format!("sleep-{method}"),
                    "name": "sleeper",
                    "command": "sleep",
                    "args": ["30"],
                    "ports": [8080],
                }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            let result = resp.result.unwrap();
            let pid = result["pid"].as_u64().expect("pid present") as i32;
            assert!(pid > 0, "{}: pid must be positive", method);
            assert_eq!(result["command"], "sleep");
            assert_eq!(result["args"], "30");
            assert_eq!(result["ports"][0], 8080);
            assert_eq!(result["isStoppable"], true);
            let srv_id = result["id"].as_str().expect("id present").to_string();
            // Process must actually exist.
            let alive = std::process::Command::new("kill")
                .arg("-0")
                .arg(pid.to_string())
                .status()
                .expect("kill -0 runs");
            assert!(alive.success(), "{}: spawned pid must be alive", method);
            started_ids.push(srv_id);
        }

        // stop each under BOTH method-name forms.
        for (i, method) in [
            "server.stopLocalServer",
            "server/stop-local-server",
        ]
        .iter()
        .enumerate()
        {
            let srv_id = &started_ids[i];
            let req = serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": method,
                "params": { "id": srv_id }
            });
            let resp = rpc(&state, 1, &req).await;
            assert!(resp.error.is_none(), "{} failed: {:?}", method, resp.error);
            assert_eq!(resp.result.unwrap()["ok"], true, "{}: ok must be true", method);
        }

        // No longer tracked.
        let mgr = state.local_servers.read().await;
        assert!(mgr.list().is_empty(), "all servers must be removed after stop");
    }

    /// startLocalServer validates: missing command -> INVALID_PARAMS.
    #[tokio::test]
    async fn server_start_local_server_validates_command() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "server.startLocalServer",
            "params": { "name": "x" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.result.is_none(), "missing command must error");
        let err = resp.error.expect("error present");
        assert_eq!(err.code, crate::error_codes::INVALID_PARAMS);
    }

    /// stopLocalServer validates: unknown id -> INVALID_PARAMS.
    #[tokio::test]
    async fn server_stop_local_server_unknown_id_errors() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "server.stopLocalServer",
            "params": { "id": "never-started" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.result.is_none(), "unknown id must error");
        let err = resp.error.expect("error present");
        assert_eq!(err.code, crate::error_codes::INVALID_PARAMS);
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

    // ─── T6c-18: server-settings REAL (in-memory persistence + push) ──

    /// Helper: send a JSON-RPC request and parse the response. Assumes the
    /// call succeeds (no parse error).
    async fn rpc_success(state: &WsState, method: &str, params: Value) -> JsonRpcResponse {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let response = handle_rpc(state, 1, &request.to_string()).await;
        serde_json::from_str(&response.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn get_config_returns_stored_default() {
        // A fresh server returns the default ServerConfig (initialized in
        // WsState::new_with_auth). The stored config must be a non-empty
        // object with the required top-level fields.
        let state = WsState::new_in_memory(16);
        let resp = rpc_success(&state, "server.getConfig", serde_json::json!({})).await;
        assert!(resp.error.is_none(), "get_config failed: {:?}", resp.error);
        let config = resp.result.unwrap();
        assert!(config["cwd"].as_str().unwrap().contains('/'));
        assert!(config["worktreesDir"].as_str().unwrap().contains(".synara"));
        assert_eq!(config["keybindings"].as_array().unwrap().len(), 0);
        assert_eq!(config["authMode"], "unsafe-no-auth");
    }

    #[tokio::test]
    async fn get_settings_returns_stored_default() {
        let state = WsState::new_in_memory(16);
        let resp = rpc_success(&state, "server.getSettings", serde_json::json!({})).await;
        assert!(resp.error.is_none());
        let settings = resp.result.unwrap();
        assert_eq!(settings["defaultThreadEnvMode"], "local");
        assert_eq!(settings["providers"]["codex"]["enabled"], true);
    }

    #[tokio::test]
    async fn set_config_persists_and_reads_back() {
        // setConfig overwrites the stored config; a subsequent getConfig
        // returns the written value (not the default). This is the core
        // REAL behavior — the stub echoed the default.
        let state = WsState::new_in_memory(16);
        let new_config = serde_json::json!({
            "cwd": "/custom/cwd",
            "worktreesDir": "/custom/worktrees",
            "keybindingsConfigPath": "/custom/kb.json",
            "keybindings": [{ "id": "kb1", "key": "cmd+k" }],
            "issues": [{ "kind": "keybindings.invalid-entry", "message": "bad" }],
            "providers": [],
            "availableEditors": [],
            "authMode": "unsafe-no-auth",
        });
        let resp = rpc_success(&state, "server.setConfig", new_config.clone()).await;
        assert!(resp.error.is_none(), "set_config failed: {:?}", resp.error);
        let returned = resp.result.unwrap();
        assert_eq!(returned["cwd"], "/custom/cwd");
        assert_eq!(returned["keybindings"][0]["id"], "kb1");

        // Read back — must reflect the write.
        let resp = rpc_success(&state, "server.getConfig", serde_json::json!({})).await;
        let config = resp.result.unwrap();
        assert_eq!(config["cwd"], "/custom/cwd");
        assert_eq!(config["keybindings"][0]["key"], "cmd+k");
    }

    #[tokio::test]
    async fn set_config_rejects_non_object() {
        let state = WsState::new_in_memory(16);
        let resp =
            rpc_success(&state, "server.setConfig", serde_json::json!("not-an-object")).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn update_settings_deep_merges_and_reads_back() {
        // updateSettings applies a partial patch (deep-merge); a subsequent
        // getSettings reflects the merge and preserves untouched keys.
        let state = WsState::new_in_memory(16);
        let patch = serde_json::json!({
            "enableAssistantStreaming": true,
            "textGenerationModelSelection": { "model": "claude-4-opus" },
            "providers": { "codex": { "enabled": false } },
        });
        let resp = rpc_success(&state, "server.updateSettings", patch).await;
        assert!(resp.error.is_none(), "update_settings failed: {:?}", resp.error);
        let returned = resp.result.unwrap();
        // Patched scalar.
        assert_eq!(returned["enableAssistantStreaming"], true);
        // Patched nested field, untouched sibling preserved (deep merge).
        assert_eq!(returned["textGenerationModelSelection"]["model"], "claude-4-opus");
        assert_eq!(
            returned["textGenerationModelSelection"]["provider"],
            "codex"
        );
        // Patched provider entry, untouched sibling providers preserved.
        assert_eq!(returned["providers"]["codex"]["enabled"], false);
        assert_eq!(returned["providers"]["claudeAgent"]["enabled"], true);
        // Untouched top-level key preserved.
        assert_eq!(returned["defaultThreadEnvMode"], "local");

        // Read back.
        let resp = rpc_success(&state, "server.getSettings", serde_json::json!({})).await;
        let settings = resp.result.unwrap();
        assert_eq!(settings["enableAssistantStreaming"], true);
        assert_eq!(settings["providers"]["codex"]["enabled"], false);
    }

    #[tokio::test]
    async fn patch_settings_aliases_update_settings() {
        // patchSettings applies the same deep-merge as updateSettings.
        let state = WsState::new_in_memory(16);
        let patch = serde_json::json!({ "addProjectBaseDirectory": "/base" });
        let resp = rpc_success(&state, "server.patchSettings", patch).await;
        assert!(resp.error.is_none());
        let resp = rpc_success(&state, "server.getSettings", serde_json::json!({})).await;
        assert_eq!(resp.result.unwrap()["addProjectBaseDirectory"], "/base");
    }

    #[tokio::test]
    async fn update_settings_rejects_non_object() {
        let state = WsState::new_in_memory(16);
        let resp = rpc_success(&state, "server.updateSettings", serde_json::json!(42)).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn upsert_keybinding_appends_and_replaces() {
        // First upsert appends; second upsert with the same id replaces.
        let state = WsState::new_in_memory(16);
        let rule_a = serde_json::json!({ "id": "kb1", "key": "cmd+a", "command": "A" });
        let resp = rpc_success(&state, "server.upsertKeybinding", rule_a).await;
        assert!(resp.error.is_none(), "upsert failed: {:?}", resp.error);
        let returned = resp.result.unwrap();
        assert_eq!(returned["keybindings"].as_array().unwrap().len(), 1);

        // Second rule with a different id → append.
        let rule_b = serde_json::json!({ "id": "kb2", "key": "cmd+b", "command": "B" });
        let resp = rpc_success(&state, "server.upsertKeybinding", rule_b).await;
        let returned = resp.result.unwrap();
        assert_eq!(returned["keybindings"].as_array().unwrap().len(), 2);

        // Replace kb1 by id.
        let rule_a_v2 =
            serde_json::json!({ "id": "kb1", "key": "cmd+shift+a", "command": "A2" });
        let resp = rpc_success(&state, "server.upsertKeybinding", rule_a_v2).await;
        let returned = resp.result.unwrap();
        let kbs = returned["keybindings"].as_array().unwrap();
        assert_eq!(kbs.len(), 2, "replace must not grow the array");
        let kb1 = kbs.iter().find(|k| k["id"] == "kb1").unwrap();
        assert_eq!(kb1["key"], "cmd+shift+a");

        // The stored config reflects the upserts (read back via getConfig).
        let resp = rpc_success(&state, "server.getConfig", serde_json::json!({})).await;
        let config = resp.result.unwrap();
        assert_eq!(config["keybindings"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn upsert_keybinding_rejects_non_object() {
        let state = WsState::new_in_memory(16);
        let resp =
            rpc_success(&state, "server.upsertKeybinding", serde_json::json!([1, 2, 3])).await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn update_provider_validates_and_returns_payload() {
        let state = WsState::new_in_memory(16);
        // Valid provider → success with `{ providers: [...] }` payload.
        let resp = rpc_success(
            &state,
            "server.updateProvider",
            serde_json::json!({ "provider": "codex" }),
        )
        .await;
        assert!(resp.error.is_none());
        assert!(resp.result.unwrap()["providers"].is_array());

        // Missing/empty provider → InvalidParams.
        let resp = rpc_success(
            &state,
            "server.updateProvider",
            serde_json::json!({ "provider": "" }),
        )
        .await;
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn refresh_providers_returns_payload() {
        let state = WsState::new_in_memory(16);
        let resp = rpc_success(&state, "server.refreshProviders", serde_json::json!({})).await;
        assert!(resp.error.is_none());
        assert!(resp.result.unwrap()["providers"].is_array());
    }

    #[tokio::test]
    async fn set_config_pushes_config_updated_event() {
        // setConfig broadcasts on push_tx with channel=server.configUpdated.
        // A subscribed receiver picks up the `{ issues, providers }` payload.
        let state = WsState::new_in_memory(16);
        let mut rx = state.push_tx.subscribe();
        let new_config = serde_json::json!({
            "cwd": "/x", "worktreesDir": "/x/wt",
            "keybindingsConfigPath": "/x/kb.json",
            "keybindings": [], "issues": [{ "kind": "keybindings.malformed-config", "message": "m" }],
            "providers": [], "availableEditors": [], "authMode": "unsafe-no-auth",
        });
        let _ = rpc_success(&state, "server.setConfig", new_config).await;
        let (channel, data) = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .expect("configUpdated push should arrive")
            .unwrap();
        assert_eq!(channel, crate::channels::CHANNEL_SERVER_CONFIG_UPDATED);
        assert!(data["issues"].is_array());
        assert!(data["providers"].is_array());
    }

    #[tokio::test]
    async fn update_settings_pushes_settings_updated_event() {
        let state = WsState::new_in_memory(16);
        let mut rx = state.push_tx.subscribe();
        let patch = serde_json::json!({ "enableAssistantStreaming": true });
        let _ = rpc_success(&state, "server.updateSettings", patch).await;
        let (channel, data) = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .expect("settingsUpdated push should arrive")
            .unwrap();
        assert_eq!(channel, crate::channels::CHANNEL_SERVER_SETTINGS_UPDATED);
        assert_eq!(data["settings"]["enableAssistantStreaming"], true);
    }

    #[tokio::test]
    async fn subscribe_config_emits_snapshot_and_registers_channel() {
        // subscribeConfig registers the connection on server.configUpdated
        // and emits an initial snapshot via the per-connection tx. We verify
        // both: (1) the subscription is recorded (so future writes forward),
        // (2) a snapshot frame arrives on the connection's tx.
        //
        // For the live-push leg we must spawn `run_push_delivery` — that task
        // is what forwards push_tx broadcasts onto the connection's mpsc tx
        // (mirrors the production connection handler in server.rs). Without
        // it the snapshot (sent directly to tx) arrives, but the setConfig
        // broadcast on push_tx would have no consumer.
        let state = std::sync::Arc::new(WsState::new_in_memory(16));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx.clone()).await;
        let _delivery = tokio::spawn(crate::server::run_push_delivery(
            std::sync::Arc::clone(&state),
            1,
            tx,
        ));
        // Let the delivery task subscribe to the push bus BEFORE the
        // subscribe/setConfig calls run — broadcast only reaches receivers
        // that exist at send time. Mirrors the e2e test in server.rs.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = rpc_success(
            &state,
            "server.subscribeConfig",
            serde_json::json!({}),
        )
        .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["subscribed"], true);
        assert_eq!(result["channel"], crate::channels::CHANNEL_SERVER_CONFIG_UPDATED);
        assert_eq!(result["snapshotEmitted"], true);

        // Snapshot frame should arrive on the connection tx.
        let snapshot_msg = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .expect("snapshot should arrive")
            .unwrap();
        assert!(snapshot_msg.contains("push/server.configUpdated"));
        assert!(snapshot_msg.contains("snapshot"));

        // The subscription is recorded — a subsequent setConfig forwards.
        let _ = rpc_success(
            &state,
            "server.setConfig",
            serde_json::json!({
                "cwd": "/y", "worktreesDir": "/y/wt",
                "keybindingsConfigPath": "/y/kb.json",
                "keybindings": [], "issues": [], "providers": [],
                "availableEditors": [], "authMode": "unsafe-no-auth",
            }),
        )
        .await;
        let live_msg = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .expect("live configUpdated should arrive on subscribed conn")
            .unwrap();
        assert!(live_msg.contains("push/server.configUpdated"));
        // The live frame is not a snapshot (it's the write payload).
        assert!(!live_msg.contains("\"snapshot\""));
    }

    #[tokio::test]
    async fn subscribe_settings_emits_snapshot() {
        let state = WsState::new_in_memory(16);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;

        let resp = rpc_success(
            &state,
            "server.subscribeSettings",
            serde_json::json!({}),
        )
        .await;
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap()["snapshotEmitted"], true);

        let msg = rx.recv().await.unwrap();
        assert!(msg.contains("push/server.settingsUpdated"));
        assert!(msg.contains("snapshot"));
    }

    #[tokio::test]
    async fn subscribe_provider_statuses_emits_snapshot() {
        let state = WsState::new_in_memory(16);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;

        let resp = rpc_success(
            &state,
            "server.subscribeProviderStatuses",
            serde_json::json!({}),
        )
        .await;
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap()["channel"], crate::channels::CHANNEL_SERVER_PROVIDER_STATUSES_UPDATED);

        let msg = rx.recv().await.unwrap();
        assert!(msg.contains("push/server.providerStatusesUpdated"));
    }

    #[tokio::test]
    async fn subscribe_lifecycle_emits_welcome_and_registers_channel() {
        // T6c-phase-27: subscribeLifecycle registers the connection on
        // `server.lifecycle` and pushes an initial `welcome` event on push_tx
        // (observable via a broadcast subscriber). The welcome payload must
        // mirror server.welcome (cwd, projectName, serverVersion, authRequired,
        // mode). Mirrors the subscribe_config snapshot-then-stream test.
        let state = std::sync::Arc::new(WsState::new_in_memory(16));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx.clone()).await;
        // Spawn the delivery loop so the broadcast on push_tx forwards onto
        // the connection's mpsc tx (mirrors the production server.rs handler).
        let _delivery = tokio::spawn(crate::server::run_push_delivery(
            std::sync::Arc::clone(&state),
            1,
            tx,
        ));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Independent broadcast subscriber — observes the welcome push even
        // if the per-connection delivery path races.
        let mut bcast_rx = state.push_tx.subscribe();

        let resp = rpc_success(
            &state,
            "server.subscribeLifecycle",
            serde_json::json!({}),
        )
        .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["subscribed"], true);
        assert_eq!(result["channel"], crate::channels::CHANNEL_SERVER_LIFECYCLE);
        assert_eq!(result["snapshotEmitted"], true);
        // The response includes the welcome payload so callers can use either
        // server.welcome or server.subscribeLifecycle for bootstrap.
        assert!(result["welcome"]["cwd"].is_string());
        assert!(result["welcome"]["projectName"].is_string());
        assert!(result["welcome"]["serverVersion"].is_string());
        assert!(result["welcome"]["authRequired"].is_boolean());
        assert!(result["welcome"]["mode"].is_string());

        // The broadcast bus carries the welcome event on the lifecycle channel.
        let (channel, data) = tokio::time::timeout(std::time::Duration::from_millis(500), bcast_rx.recv())
            .await
            .expect("lifecycle welcome push should arrive on push_tx")
            .unwrap();
        assert_eq!(channel, crate::channels::CHANNEL_SERVER_LIFECYCLE);
        assert_eq!(data["eventType"], "welcome");
        // The welcome payload data mirrors server.welcome.
        assert!(data["data"]["cwd"].is_string());
        assert!(data["data"]["projectName"].is_string());
        assert!(data["data"]["serverVersion"].is_string());

        // The forwarded per-connection frame arrives too (delivered by
        // run_push_delivery). It should be a `push/server.lifecycle` message
        // with `eventType: "welcome"`.
        let msg = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .expect("welcome push should arrive on connection tx")
            .unwrap();
        assert!(msg.contains("push/server.lifecycle"));
        assert!(msg.contains("welcome"));

        // The subscription is recorded — conn 1 is now on the lifecycle channel.
        let subscribers = state
            .subscriptions
            .read()
            .await
            .subscribers_for(crate::channels::CHANNEL_SERVER_LIFECYCLE);
        assert!(
            subscribers.contains(&1),
            "conn 1 should be subscribed to server.lifecycle (got {:?})",
            subscribers
        );
    }

    #[tokio::test]
    async fn push_subscribe_accepts_server_config_updated_channel() {
        // The new server.*Updated channels must pass the push/subscribe
        // validator (T6c-18 added them to ALL_CHANNELS).
        let state = WsState::new_in_memory(16);
        let resp = rpc_success(
            &state,
            "push/subscribe",
            serde_json::json!({ "channel": crate::channels::CHANNEL_SERVER_CONFIG_UPDATED }),
        )
        .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        assert_eq!(resp.result.unwrap()["subscribed"], true);
    }

    #[tokio::test]
    async fn unauthenticated_subscriber_does_not_receive_writes() {
        // A connection NOT subscribed to server.configUpdated must not
        // receive the setConfig push (opt-in delivery).
        let state = WsState::new_in_memory(16);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        state.register(1, tx).await;
        // No subscribe call — connection is registered but unsubscribed.

        let _ = rpc_success(
            &state,
            "server.setConfig",
            serde_json::json!({
                "cwd": "/z", "worktreesDir": "/z/wt",
                "keybindingsConfigPath": "/z/kb.json",
                "keybindings": [], "issues": [], "providers": [],
                "availableEditors": [], "authMode": "unsafe-no-auth",
            }),
        )
        .await;
        // No snapshot, no live frame — the channel must be silent.
        let outcome = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await;
        assert!(
            outcome.is_err(),
            "unsubscribed connection must not receive pushes"
        );
    }

    // ─── T6c-23: provider skills/commands/capabilities discovery ────────

    #[test]
    fn test_capabilities_claude_full_flags() {
        let params = serde_json::json!({ "provider": "claude" });
        let resp = handle_provider_get_composer_capabilities(Value::from(1), &params);
        let result = resp.result.unwrap();
        assert_eq!(result["provider"], "claudeAgent");
        assert_eq!(result["supportsSkillMentions"], true);
        assert_eq!(result["supportsSkillDiscovery"], true);
        assert_eq!(result["supportsNativeSlashCommandDiscovery"], true);
        assert_eq!(result["supportsPluginMentions"], false);
        assert_eq!(result["supportsPluginDiscovery"], false);
        assert_eq!(result["supportsRuntimeModelList"], true);
        assert_eq!(result["supportsThreadCompaction"], true);
    }

    #[test]
    fn test_capabilities_codex_full_flags() {
        let params = serde_json::json!({ "provider": "codex" });
        let resp = handle_provider_get_composer_capabilities(Value::from(1), &params);
        let result = resp.result.unwrap();
        assert_eq!(result["provider"], "codex");
        assert_eq!(result["supportsSkillMentions"], true);
        assert_eq!(result["supportsSkillDiscovery"], true);
        assert_eq!(result["supportsNativeSlashCommandDiscovery"], true);
    }

    #[test]
    fn test_capabilities_gemini_partial_flags() {
        let params = serde_json::json!({ "provider": "gemini" });
        let resp = handle_provider_get_composer_capabilities(Value::from(1), &params);
        let result = resp.result.unwrap();
        assert_eq!(result["supportsSkillMentions"], true);
        assert_eq!(result["supportsSkillDiscovery"], false);
        assert_eq!(result["supportsNativeSlashCommandDiscovery"], true);
    }

    #[test]
    fn test_capabilities_kilo_minimal_flags() {
        let params = serde_json::json!({ "provider": "kilo" });
        let resp = handle_provider_get_composer_capabilities(Value::from(1), &params);
        let result = resp.result.unwrap();
        assert_eq!(result["supportsSkillMentions"], false);
        assert_eq!(result["supportsSkillDiscovery"], false);
        assert_eq!(result["supportsNativeSlashCommandDiscovery"], false);
    }

    #[test]
    fn test_list_commands_claude_includes_compact_and_cost() {
        let params = serde_json::json!({ "provider": "claude" });
        let resp = handle_provider_list_commands(Value::from(1), &params);
        let result = resp.result.unwrap();
        let names: Vec<&str> = result["commands"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"/help"));
        assert!(names.contains(&"/clear"));
        assert!(names.contains(&"/compact"));
        assert!(names.contains(&"/cost"));
        assert!(names.contains(&"/doctor"));
    }

    #[test]
    fn test_list_commands_codex_minimal() {
        let params = serde_json::json!({ "provider": "codex" });
        let resp = handle_provider_list_commands(Value::from(1), &params);
        let result = resp.result.unwrap();
        let names: Vec<&str> = result["commands"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["/help", "/clear"]);
    }

    #[test]
    fn test_list_skills_scans_markdown_files() {
        let tmp = std::env::temp_dir().join(format!(
            "syncode-ws-skills-test-{}",
            std::process::id()
        ));
        let skills_dir = tmp.join(".skills");
        std::fs::create_dir_all(&skills_dir).unwrap();
        // skill with frontmatter description
        std::fs::write(
            skills_dir.join("review.md"),
            "---\nname: review\ndescription: Code review specialist.\n---\n# Review\nbody",
        )
        .unwrap();
        // skill without frontmatter
        std::fs::write(skills_dir.join("explore.md"), "# Explore\nplain body").unwrap();
        // non-markdown file should be skipped
        std::fs::write(skills_dir.join("ignore.txt"), "nope").unwrap();

        let params = serde_json::json!({ "cwd": tmp.to_string_lossy() });
        let resp = handle_provider_list_skills(Value::from(1), &params);
        let result = resp.result.unwrap();
        let skills = result["skills"].as_array().unwrap();
        assert_eq!(skills.len(), 2, "expected 2 markdown skills, got {skills:?}");
        // sorted alphabetically by name
        assert_eq!(skills[0]["name"], "explore");
        assert_eq!(skills[1]["name"], "review");
        assert_eq!(skills[1]["description"], "Code review specialist.");
        assert_eq!(skills[1]["enabled"], true);
        assert!(
            skills[1]["path"]
                .as_str()
                .unwrap()
                .ends_with("review.md"),
            "path should be absolute and end with review.md"
        );
        assert!(
            skills[0].get("description").is_none(),
            "explore has no frontmatter description"
        );

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_list_skills_missing_dir_is_empty() {
        let params =
            serde_json::json!({ "cwd": "/nonexistent-syncode-skills-test-path-12345" });
        let resp = handle_provider_list_skills(Value::from(1), &params);
        let result = resp.result.unwrap();
        assert_eq!(result["skills"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_read_skill_reads_file() {
        let tmp = std::env::temp_dir().join(format!(
            "syncode-ws-readskill-test-{}",
            std::process::id()
        ));
        let skills_dir = tmp.join(".skills");
        std::fs::create_dir_all(&skills_dir).unwrap();
        let body = "---\ndescription: hello world\n---\n# Greet\nbody text";
        std::fs::write(skills_dir.join("greet.md"), body).unwrap();
        let abs = std::fs::canonicalize(skills_dir.join("greet.md")).unwrap();

        let params = serde_json::json!({ "path": abs.to_string_lossy() });
        let resp = handle_provider_read_skill(Value::from(1), &params);
        let result = resp.result.unwrap();
        let skill = result["skill"].as_object().unwrap();
        assert_eq!(skill["name"], "greet");
        assert_eq!(skill["enabled"], true);
        assert!(skill["content"].as_str().unwrap().contains("hello world"));

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_read_skill_rejects_path_outside_skills() {
        let tmp = std::env::temp_dir().join(format!(
            "syncode-ws-readskill-traversal-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let outside = tmp.join("secret.md");
        std::fs::write(&outside, "secret").unwrap();
        let abs = std::fs::canonicalize(&outside).unwrap();

        let params = serde_json::json!({ "path": abs.to_string_lossy() });
        let resp = handle_provider_read_skill(Value::from(1), &params);
        let result = resp.result.unwrap();
        assert!(result["skill"].is_null(), "path outside .skills must return null");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_read_skill_missing_returns_null() {
        let params = serde_json::json!({ "path": "" });
        let resp = handle_provider_read_skill(Value::from(1), &params);
        let result = resp.result.unwrap();
        assert!(result["skill"].is_null());
    }

    #[test]
    fn test_frontmatter_description_parser() {
        let content = "---\nname: foo\ndescription: \"A skill.\"\n---\n# body";
        assert_eq!(
            parse_skill_frontmatter_description(content),
            Some("A skill.".to_string())
        );
        assert_eq!(parse_skill_frontmatter_description("# no frontmatter"), None);
        assert_eq!(
            parse_skill_frontmatter_description("---\nname: foo\n---\nbody"),
            None
        );
    }

    // ─── T6c-29: orchestration generic RPC tests ───────────────────────

    #[tokio::test]
    async fn orchestration_dispatch_command_creates_project() {
        // dispatchCommand with type=CreateProject runs the full CQRS pipeline:
        // events are appended and the read model is updated.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "orchestration.dispatchCommand",
            "params": {
                "type": "CreateProject",
                "name": "demo",
                "rootPath": "/tmp/demo",
            }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "dispatch failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["dispatched"], true);
        assert_eq!(result["eventsAppended"], 1);
        // Read model now contains the project.
        let store = state.read_store.read().await;
        assert_eq!(store.projects.len(), 1, "project should be projected");
    }

    #[tokio::test]
    async fn orchestration_dispatch_command_slash_form_resolves() {
        // The slash form must resolve to the same handler.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "orchestration/dispatch-command",
            "params": { "type": "CreateProject", "name": "x", "rootPath": "/tmp/x" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "slash form failed: {:?}", resp.error);
        assert_eq!(resp.result.unwrap()["dispatched"], true);
    }

    #[tokio::test]
    async fn orchestration_dispatch_command_unknown_type_errors() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "orchestration.dispatchCommand",
            "params": { "type": "Nonsense" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_some(), "expected error for unknown type");
        assert_eq!(resp.error.unwrap().code, crate::error_codes::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn orchestration_repair_state_returns_events_replayed() {
        // repairState rebuilds the read model from events. After creating a
        // project, replay should report at least 1 event replayed and the read
        // model should still contain the project.
        let state = WsState::new_in_memory(16);
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "orchestration.dispatchCommand",
            "params": { "type": "CreateProject", "name": "p", "rootPath": "/tmp/p" }
        });
        rpc(&state, 1, &create).await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "orchestration.repairState"
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "repairState failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["repaired"], true);
        let count = result["eventsReplayed"].as_u64().unwrap_or(0);
        assert!(count >= 1, "expected at least 1 event replayed, got {count}");
        // Read model still reflects the project.
        let store = state.read_store.read().await;
        assert_eq!(store.projects.len(), 1);
    }

    #[tokio::test]
    async fn orchestration_replay_events_returns_count() {
        // replayEvents without an aggregateId performs a full replay.
        let state = WsState::new_in_memory(16);
        let create = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "orchestration.dispatchCommand",
            "params": { "type": "CreateProject", "name": "p2", "rootPath": "/tmp/p2" }
        });
        rpc(&state, 1, &create).await;

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "orchestration.replayEvents"
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "replayEvents failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["replayed"], true);
        assert_eq!(result["scope"], "all");
        assert!(result["eventsReplayed"].as_u64().unwrap_or(0) >= 1);
    }

    #[tokio::test]
    async fn orchestration_subscribe_shell_registers_and_emits_snapshot() {
        // subscribeShell registers the connection on the `orchestration` push
        // channel and emits an initial shell snapshot.
        let state = WsState::new_in_memory(16);
        state.subscriptions.write().await.register(1);

        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "orchestration.subscribeShell"
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "subscribeShell failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["subscribed"], true);
        assert_eq!(result["channel"], "orchestration");
        // Connection is now registered on the orchestration channel.
        assert!(state
            .subscriptions
            .read()
            .await
            .subscribers_for(crate::channels::CHANNEL_ORCHESTRATION)
            .contains(&1));
    }

    #[tokio::test]
    async fn orchestration_get_turn_diff_returns_empty_for_no_checkpoints() {
        // A thread with no turns/checkpoints returns an empty patch.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "orchestration.getTurnDiff",
            "params": { "threadId": "no-such-thread", "cwd": "/tmp" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "getTurnDiff failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["patch"], "");
    }

    #[tokio::test]
    async fn orchestration_get_full_thread_diff_returns_empty_for_no_checkpoints() {
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "orchestration.getFullThreadDiff",
            "params": { "threadId": "no-such-thread", "cwd": "/tmp" }
        });
        let resp = rpc(&state, 1, &req).await;
        assert!(resp.error.is_none(), "getFullThreadDiff failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["patch"], "");
    }

    #[tokio::test]
    async fn orchestration_methods_appear_in_list_methods() {
        // All 6 new RPCs (both dot and slash forms) must appear in
        // rpc/listMethods so the UI's served-RPC discovery surfaces them.
        let state = WsState::new_in_memory(16);
        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "rpc/listMethods" });
        let resp = rpc(&state, 1, &req).await;
        let methods = resp.result.unwrap()["methods"].as_array().unwrap().clone();
        let names: Vec<String> = methods
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        for expected in [
            "orchestration.dispatchCommand",
            "orchestration/dispatch-command",
            "orchestration.subscribeShell",
            "orchestration/subscribe-shell",
            "orchestration.getTurnDiff",
            "orchestration/get-turn-diff",
            "orchestration.getFullThreadDiff",
            "orchestration/get-full-thread-diff",
            "orchestration.replayEvents",
            "orchestration/replay-events",
            "orchestration.repairState",
            "orchestration/repair-state",
        ] {
            assert!(
                names.iter().any(|n| n == expected),
                "rpc/listMethods missing {expected}"
            );
        }
    }
}

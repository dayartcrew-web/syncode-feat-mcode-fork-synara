# Syncode — Clone+Rewire Status & REAL-vs-STUB Matrix

> **Status (2026-07-04): COMPREHENSIVELY FUNCTIONAL.** Authoritative accounting of what is **REAL** (backed by real logic/data) vs **STUB** (default/empty/no-persistence) vs **UNSERVED** across the cloned MCode web UI ↔ Syncode Rust backend. Updated through PR #32. **Server (config/Settings) section re-audited 2026-07-04** against code post-sync to `7789fa9`. **Desktop WS spawn (DSK-1) added 2026-07-04** — in-process WS server now boots inside Tauri `.setup()`.
>
> This is the single source of truth for "mana yang masih stub vs real app-wired." Other docs (`COMPARISON-FRONTEND`, `CONTRACTS-BRIDGE-DESIGN`, `SHELL-GAPS`, `TEST_SUMMARY`, `CRATES`, `ARCHITECTURE`) carry detail/history; this file carries current status.

---

## TL;DR
- **Frontend**: MCode `apps/web` cloned + rewired — **type-clean (tsc 0)**, suite **2128/0 pass**, vite build green.
- **Backend**: standalone WS server (`crates/syncode-ws/src/bin/server.rs`, SQLite) — **113 served RPCs** dispatching MCode dot-names + slash forms. **ZERO actively-called UI RPCs unserved.** ws **227 tests**.
- **Every UI panel's RPCs reach the backend — ALL REAL.** Chat works (ProviderCommandReactor wired). Voice (STT) is REAL behind the `stt` Cargo feature (SRV-4: whisper-CLI transcription; default build retains the graceful "STT not configured" stub); all other domains return real data/logic.

## Legend
- ✅ **REAL** — backed by real Syncode logic/data (git2, syncode-terminal, scheduler, read_model, provider CLI, …).
- 🟡 **STUB** — served (no MethodNotFound) but returns default/empty/no-persistence (UI renders a valid empty state; writes are accepted but not persisted).
- ⛔ **UNSERVED** — no handler (client-stub MethodNotFound; UI feature non-functional).

---

## REAL-vs-STUB matrix (per RPC domain)

### Shell / orchestration
| RPC | Status | Backed by |
|---|---|---|
| `orchestration.getShellSnapshot` / `getSnapshot` | ✅ REAL | `read_model` (real projects + threads; E2E-proven: shell lists "Demo Project") |
| `orchestration.subscribeShell` | ✅ REAL (T6c-29) | registers on `orchestration` push channel + emits initial shell snapshot |
| `orchestration.dispatchCommand` | ✅ REAL (T6c-29) | generic dispatcher → `Orchestrator::handle_command` (CreateProject/CreateThread/StartTurn/Pause/Resume/Cancel/SetTitle/Delete) |
| `orchestration.getTurnDiff` | ✅ REAL (T6c-29) | git diff between turn checkpoint and next/HEAD (`syncode_git::Git2Service`) |
| `orchestration.getFullThreadDiff` | ✅ REAL (T6c-29) | cumulative diff across all thread turns (earliest checkpoint → HEAD) |
| `orchestration.replayEvents` | ✅ REAL (T6c-29) | full read-model replay via `Orchestrator::replay_read_model`; returns count |
| `orchestration.repairState` | ✅ REAL (T6c-29) | full replay (rebuild read model); returns `{ repaired, eventsReplayed }` |

### Git (GitPanel)
| RPC | Status | Backed by |
|---|---|---|
| `git.status` / `diff` / `readWorkingTreeDiff` / `log` / `branches` / `add`(stage) / `commit` / `createBranch` / `deleteBranch` / `checkout` | ✅ REAL | `syncode-git::Git2Service` |
| `git.stashList` / `stashCreate` / `stashApply` / `stashDrop` / `stashInfo` / `fetch` / `init` / `removeIndexLock` / `worktreeList` / `worktreeCreate` / `worktreeRemove` / `pull` / `push` | ✅ REAL | git2 (stash/fetch/init/worktree) + syncode-git CLI (pull/push) |
| `git.stashAndCheckout` | ✅ REAL (GIT-1) | git2 two-phase: `stash_save2` → `checkout_tree`; best-effort `stash_apply` rollback on checkout failure |
| `git.summarizeDiff` | ✅ REAL | provider CLI one-shot (LLM) |
| `git.githubRepository` / `resolvePullRequest` | ✅ REAL | gh CLI (no token — uses gh auth; `gh repo view` / `gh pr view`) |
| `git.handoffThread` | ✅ REAL | gh pr create (branch mode) + git2 worktree add (worktree mode, `targetMode:"worktree"` populates `worktreePath`/`associatedWorktreeBranch`/`changesTransferred`/`conflictsDetected`) |
| `git.preparePullRequestThread` | 🟡 STUB | composable via resolvePullRequest + worktreeCreate |
| `git.runStackedAction` / `createDetachedWorktree` | ✅ REAL | syncode-git StackedPipeline (action mapping) + `git worktree add --detach` |
| `git.subscribeActionProgress` | ✅ REAL (GIT-4) | registers connection on `git` push channel + emits initial `subscribed` event; `runStackedAction` drives `StackedPipeline::execute_with_progress` and broadcasts per-stage `{stage,percent,message}` events on `CHANNEL_GIT` (only when ≥1 subscriber; default sync path unchanged) |

### Server (config / Settings)
| RPC | Status | Backed by |
|---|---|---|
| `server.getConfig` / `getSettings` | ✅ REAL | `ServerSettingsState` (`Arc<RwLock<Value>>` on `WsState`) — **on-disk persisted** (SRV-1): when the server binary attaches the SQLite pool, config/settings load from `server_config`/`server_settings` tables on startup and every mutation write-throughs; reads return live state. In-memory/test deployments (no pool) retain the session-scoped behavior. |
| `server.setConfig` / `updateSettings` / `patchSettings` / `updateProvider` / `upsertKeybinding` / `refreshProviders` | ✅ REAL | mutate store (`setConfig` replaces `config`; `updateSettings`/`patchSettings` deep-merge `settings` via `merge_json`; `upsertKeybinding` upserts by id; `updateProvider`/`refreshProviders` re-emit providers) + push on change (`configUpdated`/`settingsUpdated`/`providerStatusesUpdated`) + **write-through to SQLite** (SRV-1: `server_config`/`server_settings` tables; no-op without an attached pool) |
| `server.subscribeConfig` / `subscribeSettings` / `subscribeProviderStatuses` | ✅ REAL | register on channel + initial snapshot push + live push delivery |
| `server.welcome` | ✅ REAL | derived payload: cwd→`projectName`, real `authRequired`/`authMode` from `WsAuthConfig`, `serverVersion`, git-repo identity |
| `server.getEnvironment` | ✅ REAL | real `os`/`arch` from `std::env::consts` + server version |
| `server.getDiagnostics` | ✅ REAL | live `read_store` project/thread counts + `pid` + uptime + RSS (Linux `/proc`) + terminal/local-server child counts (heap/external memory counters hardcoded 0) |
| `server.subscribeLifecycle` | ✅ REAL (SRV-3) | registers on `server.lifecycle` + emits initial `welcome` snapshot + **ongoing broadcasts** on lifecycle event sources: `startLocalServer`/`stopLocalServer` (`local-server-started`/`local-server-stopped`), `setConfig` (`config-changed`), `updateSettings`/`patchSettings` (`settings-changed`), `refreshProviders`/`updateProvider` (`providers-refreshed`); delivered via `run_push_delivery` to subscribed connections |
| `server.transcribeVoice` / `voiceStart` / `voiceStop` | ✅ REAL (feature-gated) | `crate::voice` module — **real whisper-CLI STT behind the `stt` Cargo feature** (SRV-4). `stt` ON + `whisper` on PATH → `transcribeVoice` decodes base64 audio → temp file → shells out to `whisper --model tiny --output_fmt txt` → returns transcript text; `voiceStart` probes binary (`ok:true`+`engine:"whisper"`); `voiceStop` no-op. `stt` OFF (default) OR binary missing → graceful "STT not configured" stub (byte-identical to pre-SRV-4). Optional deps: `base64`, `tempfile` (only under `stt`). 5 tests (4 always-run fallback + 1 `#[ignore]`d real-whisper). |
| `server.generateAutomationIntent` | ✅ REAL | LLM via provider CLI (`invoke()`→`invoke_llm_oneshot`) — prompt → AutomationDef JSON (markdown-fence tolerant; malformed JSON falls back to raw text) |
| `server.generateThreadRecap` | ✅ REAL | LLM via provider CLI (`invoke()`→`invoke_llm_oneshot`) — thread → recap text |
| `server.listProviderUsage` / `getProviderUsageSnapshot` | ✅ REAL | `UsageStore` (in-memory; FIFO-capped 10k entries) — usage recorded in `invoke()` wrapper at rpc.rs:6808 (not inside `invoke_llm_oneshot`); aggregates per-provider totals, call count, last-seen model, last-used-at (**no peak-day/windowed breakdown**; `limits: []`) |
| `server.startLocalServer` / `stopLocalServer` | ✅ REAL | `LocalServerManager` (local_server.rs) — real `tokio::process::Command` spawn + `Child::kill`/`wait` reap; tracks pid (T6c-phase-24; tests verify real spawn/kill) |
| _(8 server.* in frontend `UNSERVED_RPC`)_ | ⛔ non-actively-called | `listProviders`, `getProviderStatuses`, `getProviderAuthStatus`, `getUsage`, `getRecap`, `listLocalServers`, `listLocalServerProcesses`, `listWorktrees` — legacy aliases the vendored UI doesn't invoke |

### Terminal (Terminal panel)
| RPC | Status | Backed by |
|---|---|---|
| `terminal.open`/`new` / `write` / `resize` / `close`/`kill` / `ackOutput` / `list` / `clear` / `restart` | ✅ REAL | `syncode-terminal::SessionManager` (real PTY; round-trip verified vs `/bin/cat`) |
| `terminal.subscribeEvents` (live output) | ✅ REAL | per-session reader task → `push_tx` `terminal/event` (terminal **streams live output**) |
| `terminal.split` / `toggle` / `splitRight` / `splitDown` / `splitUp` / `splitLeft` | (UI-internal) | pane-layout, not backend RPCs |

### Automation (Automations panel)
| RPC | Status | Backed by |
|---|---|---|
| `automation.list` / `create` / `get` / `update` / `delete` / `runNow` / `cancelRun` | ✅ REAL | `syncode-automation::Scheduler` + **`ProcessRunExecutor`** (automations **actually execute** via `sh -c`) |
| `automation.markRunRead` / `archiveRun` | ✅ REAL | `Scheduler::mark_run_read` / `archive_run` (persisted via repo upsert; `AutomationRun` carries `unread` + `archived_at`) |
| `automation.subscribe` / `unsubscribe` / `automation.event` (push) | ✅ REAL | register on CHANNEL_AUTOMATION; `runNow`/`cancelRun` push `run-upserted` lifecycle events via `push_tx` (trigger synchronous — awaits full execution) |
| `automation.get` (single) | ✅ REAL | Scheduler.get |

### Provider (discovery + LLM)
| RPC | Status | Backed by |
|---|---|---|
| `provider.listModels` / `listAgents` | ✅ REAL | `ALL_PROVIDERS` static (8 real provider descriptors) |
| `provider.getComposerCapabilities` / `listSkills` / `listSkillsCatalog` / `listCommands` / `readSkill` | ✅ REAL | per-provider capability flags + filesystem `.skills/*.md` scan + static native commands + skill file read (traversal-guarded) |
| `provider.listPlugins` / `readPlugin` / `listOptions` | 🟡 STUB-empty | no plugin marketplace/options subsystem |
| `provider.compactThread` | ✅ REAL | provider CLI one-shot (LLM compaction) |

### Stats (Profile)
| RPC | Status | Backed by |
|---|---|---|
| `stats.getProfileStats` / `getProfileTokenStats` | ✅ REAL | `read_store` (project/thread/turn/message counts) + `UsageStore` (per-provider token totals, lifetime tokens, peak day, provider breakdown) |

### Auth / infra
| RPC | Status | Backed by |
|---|---|---|
| `auth.bootstrap` / `auth.status` / `auth.logout` | ✅ REAL | `syncode-auth` (opt-in; bearer-session) |
| `push.subscribe` / `push.unsubscribe` / `ping` / `rpc.listMethods` | ✅ REAL | WS infra |

---

## Component status (the app-wiring)
| Component | Status |
|---|---|
| Cloned MCode UI (`apps/web` → `frontend/`) | ✅ vendored (753 files + 35 shared modules), type-clean |
| **Chat (turn → provider → AI response)** | ✅ **ProviderCommandReactor wired** — turns invoke providers (default `claude`, configurable via `SYNCODE_DEFAULT_PROVIDER`); responses stream back → push to subscribed clients. Graceful fallback if CLI absent. |
| Contracts bridge (`@t3tools/contracts` shim) | ✅ complete — 139 Tier-3 symbols + RPC registry + 44-event union + branded IDs |
| Transport (`wsTransport` JSON-RPC) | ✅ Effect-free; `MCODE_TO_SERVED` (88 mappings) |
| Standalone WS backend | ✅ `cargo run -p syncode-ws --bin server` (SQLite, env-configurable) |
| **In-process WS server (Tauri)** | ✅ **DSK-1** — desktop `.setup()` spawns the same axum WS server (`ws_setup::boot`) on `SYNCODE_WS_PORT` (default **30101**); shared `WsState` managed by Tauri → IPC commands + WS handlers see the same backend. `/ws` endpoint verified reachable (101 upgrade + JSON-RPC ping round-trip). 5 integration tests (`tests/ws_spawn.rs`). |
| Terminal live output | ✅ reader-task → push bus |
| Automation execution | ✅ `ProcessRunExecutor` (sh -c) |
| LLM ops | ✅ provider CLI one-shot (`llm.rs::invoke_llm_oneshot`) — **no API key** (providers use CLI auth) |
| Desktop shell (Tauri) | ✅ builds + **29 commands** wired (added `getWsEndpoint`); WS server spawned in `.setup()` (DSK-1). **GUI window boot still needs a display** (headless-blocked), but WS-layer boot E2E verified. |

## Test/quality state
- **Frontend**: tsc **0 errors**, vitest **2128 pass / 0 fail**, vite build green.
- **Backend**: `cargo test -p syncode-ws` **132** (128 lib + 4 e2e); `syncode-contracts` 96; `syncode-automation` 72; `syncode-terminal` 20. Per-crate `cargo clippy -- -D warnings` green. (Workspace `cargo test` works now — glib/libs installed.)
- **~27 PRs** (PR #6–#32) across the clone+rewire + RPC-coverage + infra arc.

## Genuinely remaining (marginal / niche / config-gated)
- **git/automation live event-push** — extend terminal reader-task pattern; git ops are synchronous so progress is limited.
- **GitHub-API ops** — achievable via `gh api` subprocess (gh CLI authed); niche PR-handoff flow.
- **voice ops** (transcribeVoice/…) — STT subsystem (different from LLM-text).
- **Real persistence for server settings** — ✅ **DONE (SRV-1)**: `ServerSettingsState` now write-throughs to SQLite `server_config`/`server_settings` tables; edits survive a restart (server binary attaches the pool in `build_state`).
- **Desktop GUI boot E2E** — needs a display (headless-blocked). **WS-layer boot is verified** (DSK-1: server spawns in `.setup()`, `/ws` reachable, JSON-RPC round-trips); only the webview window rendering is unverified without a display.

---

*For the design/spec history see [`CONTRACTS-BRIDGE-DESIGN.md`](./CONTRACTS-BRIDGE-DESIGN.md); for the frontend comparison/lineage see [`COMPARISON-FRONTEND-MCODE-vs-SYNCODE.md`](./COMPARISON-FRONTEND-MCODE-vs-SYNCODE.md).*

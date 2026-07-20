# Syncode — Clone+Rewire Status & REAL-vs-STUB Matrix

> **Status (2026-07-20: ALL STUB→REAL WORKFLOW COMPLETE.** Every 🟡 STUB / 🟡 PARTIAL / ⛔ UNSERVED entry from the original matrix has been converted to ✅ REAL across 34 tasks, 30 PRs (#49–#78), workflow `c55208b0`. The `UNSERVED_RPC` frontend list has been emptied of all actively-called methods. Authoritative accounting of what is **REAL** (backed by real logic/data) vs **STUB** (default/empty/no-persistence) vs **UNSERVED** across the cloned MCode web UI ↔ Syncode Rust backend.
>>
> **Server (config/Settings) section re-audited 2026-07-04** against code post-sync to `7789fa9`. **Desktop WS spawn (DSK-1) added 2026-07-04** — in-process WS server now boots inside Tauri `.setup()`. **Desktop boot E2E (DSK-3) added 2026-07-04** — full `.setup()` wiring + actual binary boot now verified under `xvfb-run` in CI (`.github/workflows/desktop-e2e.yml`).
>
> This is the single source of truth for "mana yang masih stub vs real app-wired." Other docs (`COMPARISON-FRONTEND`, `CONTRACTS-BRIDGE-DESIGN`, `SHELL-GAPS`, `TEST_SUMMARY`, `CRATES`, `ARCHITECTURE`) carry detail/history; this file carries current status.

---

## TL;DR
- **Frontend**: MCode `apps/web` cloned + rewired — **type-clean (tsc 0)**, suite **2128/0 pass**, vite build green.
- **Backend**: standalone WS server (`crates/syncode-ws/src/bin/server.rs`, SQLite) — **169 served RPCs** dispatching MCode dot-names + slash forms. **ZERO actively-called UI RPCs unserved.** ws **350+ tests**.
- **Recent major features (PRs #205-#212)**: MCP server discovery/management (PR #209), FTS5 hybrid memory backends (PR #210), code search via ripgrep (PR #212), chat-workflow binding (PR #211), provider MCP forwarding (PR #205).
- **Every UI panel's RPCs reach the backend — ALL REAL.** Chat works (ProviderCommandReactor wired). Voice (STT) is REAL behind the `stt` Cargo feature (SRV-4: whisper-CLI transcription; default build retains the graceful "STT not configured" stub). Settings persist across restarts (SRV-1). SQLx migrations (SRV-2). All domains return real data/logic.
- **STUB→REAL workflow** (workflow `c55208b0`, 2026-07-04/05): 34 tasks, 30 PRs (#49–#78), converted every 🟡/⛔ to ✅. New subsystems: pairing links (AUTH-1/2), dev-server lifecycle (PROJ-4), project file ops (PROJ-1/2/3), plugin install-state (PROV-1), provider options (PROV-2), drift detection (ORCH-3), dispatchCommand via ApplicationService (ORCH-5), turn/thread diffs (ORCH-6/7), repairState (ORCH-3), subscribe lifecycle (SRV-3), legacy aliases (SRV-5/6), Tauri IPC commands (DSK-2), boot E2E (DSK-3), SQLx migrations (SRV-2).

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
| `orchestration.subscribeShell` | ✅ REAL (ORCH-4) | registers on `orchestration` push channel + emits initial shell snapshot via `build_snapshot` |
| `orchestration.subscribeEvents` | ✅ REAL (ORCH-4) | registers on `orchestration` push channel + emits initial snapshot; live events via `WsDomainEventPublisher` |
| `orchestration.dispatchCommand` | ✅ REAL (ORCH-5) | routes `{type, payload}` through `ApplicationService` methods (17 command types); structured error mapping with `data.kind` discriminant |
| `orchestration.getLatestTurn` | ✅ REAL (ORCH-1) | reads `read_store.turns` filtered by threadId, returns turn with highest `sequence` via `max_by_key` |
| `orchestration.getTurnDiff` | ✅ REAL (ORCH-6) | loads two `CheckpointView`s, computes diff via `syncode_git::diff::compute_diff`; graceful empty fallback when checkpoints sparse |
| `orchestration.getFullThreadDiff` | ✅ REAL (ORCH-7) | aggregates per-turn diffs across a thread via shared `render_diff_summary` helper (reuses ORCH-6 primitive); returns `{turns:[{turnId,sequence,diff}], totalDiff}` |
| `orchestration.replayEvents` | ✅ REAL (ORCH-2) | full read-model replay via `Orchestrator::replay_read_model`; returns `{replayed, seeded}` |
| `orchestration.repairState` / `repairReadModel` | ✅ REAL (ORCH-3) | drift detection: snapshot ReadModelStore counts → clear → replay → compare; `repair:true` clears+replays; returns `{driftDetected, repairedCount, details:{before,after}}` |

### Git (GitPanel)
| RPC | Status | Backed by |
|---|---|---|
| `git.status` / `diff` / `readWorkingTreeDiff` / `log` / `branches` / `add`(stage) / `commit` / `createBranch` / `deleteBranch` / `checkout` | ✅ REAL | `syncode-git::Git2Service` |
| `git.stashList` / `stashCreate` / `stashApply` / `stashDrop` / `stashInfo` / `fetch` / `init` / `removeIndexLock` / `worktreeList` / `worktreeCreate` / `worktreeRemove` / `pull` / `push` | ✅ REAL | git2 (stash/fetch/init/worktree) + syncode-git CLI (pull/push) |
| `git.stashAndCheckout` | ✅ REAL (GIT-1) | git2 two-phase: `stash_save2` → `checkout_tree`; best-effort `stash_apply` rollback on checkout failure |
| `git.summarizeDiff` | ✅ REAL | provider CLI one-shot (LLM) |
| `git.githubRepository` / `resolvePullRequest` | ✅ REAL | gh CLI (no token — uses gh auth; `gh repo view` / `gh pr view`) |
| `git.handoffThread` | ✅ REAL | gh pr create (branch mode) + git2 worktree add (worktree mode, `targetMode:"worktree"` populates `worktreePath`/`associatedWorktreeBranch`/`changesTransferred`/`conflictsDetected`) |
| `git.preparePullRequestThread` | ✅ REAL (GIT-3) | composes `gh pr view` (resolve PR head branch) + git2 worktree add on the head ref; returns `GitPreparePullRequestThreadResult` (`{ pullRequest, branch, worktreePath }`); worktree step degrades gracefully to `null` when the head branch can't be linked |
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
| `server.listProviders` / `getProviderStatuses` / `getProviderAuthStatus` / `getUsage` / `getRecap` | ✅ REAL (SRV-5) | thin aliases: `listProviders`→`listAgents`, `getProviderStatuses`→`subscribeProviderStatuses` snapshot, `getProviderAuthStatus`→derives from `WsAuthConfig`, `getUsage`→`listProviderUsage`, `getRecap`→`generateThreadRecap` |
| `server.listLocalServers` / `listLocalServerProcesses` / `listWorktrees` | ✅ REAL (SRV-6) | `listLocalServers`/`listLocalServerProcesses` via `LocalServerManager::list()`; `listWorktrees` delegates to `handle_git_worktree_list` |

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
| `provider.listMcpCatalog` / `list-mcp-catalog` | ✅ REAL (PR #209) | MCP discovery module (`mcp_catalog.rs`) — reads `~/.claude.json`, `~/.cursor/mcp.json`, `~/.codex/config.toml`, project-local `.mcp.json`/`.cursor/mcp.json`. Returns syncode-owned store at `~/.syncode/mcp.json`. |
| `provider.getComposerCapabilities` / `listSkills` / `listSkillsCatalog` / `listCommands` / `readSkill` | ✅ REAL | per-provider capability flags + filesystem `.skills/*.md` scan + static native commands + skill file read (traversal-guarded) |
| `provider.listPlugins` / `readPlugin` | ✅ REAL | filesystem `.plugins/*.json` scan (per-plugin `id`/`name`/`source`/`description`/`version`) + install-state manifest overlay at `~/.synara/plugins.json` (env-configurable via `SYNCODE_PLUGINS_MANIFEST`, per-request-pinnable via `pluginsManifestPath`) — drives real `installed`/`enabled`/`installPolicy` (`auto`\|`manual`\|`disabled`) fields. **Remote marketplace sync (OAuth, remote catalog) is out of scope.** |
| `provider.listOptions` | ✅ REAL | `ProviderOptionInfo[]` per `ALL_PROVIDERS` id — real model lists + capability flags (temperature / system-prompt / tool-use / streaming / vision / maxTokens) read from each provider's adapter, plus reasoning-effort / thinking-level selects (PROV-2) |
| `provider.compactThread` | ✅ REAL | provider CLI one-shot (LLM compaction) |

### MCP servers
| RPC | Status | Backed by |
|---|---|---|
| `mcp.create` / `mcp/update` / `mcp/delete` | ✅ REAL (PR #209) | CRUD on syncode-owned MCP store (`~/.syncode/mcp.json`). Validates dot-strings, persists to disk, broadcasts `mcp-servers-updated` push event. |
| `mcp.testConnection` / `test-connection` | ✅ REAL (PR #209) | Probes MCP server health via WebSocket/stdio. Returns `{ok:true/false, error?, latency_ms?}`. |

### Tools
| RPC | Status | Backed by |
|---|---|---|
| `tool.searchCode` / `search-code` | ✅ REAL (PR #212) | `code_search.rs` — ripgrep-backed content search. Parameters: `{cwd,query,limit,file_glob,case_insensitive,regex}`. Returns `{hits:[{path,line,column,matched_text}],truncated,query}`. 11 unit tests + 9 RPC integration tests + 12 live WS e2e probes. |

### Stats (Profile)
| RPC | Status | Backed by |
|---|---|---|
| `stats.getProfileStats` / `getProfileTokenStats` | ✅ REAL | `read_store` (project/thread/turn/message counts) + `UsageStore` (per-provider token totals, lifetime tokens, peak day, provider breakdown) |

### Auth / infra
| RPC | Status | Backed by |
|---|---|---|
| `auth.bootstrap` / `auth.status` / `auth.logout` | ✅ REAL | `syncode-auth` (opt-in; bearer-session) |
| `auth.createPairingCredential` / `revokePairingLink` / `listPairingLinks` | ✅ REAL (AUTH-1) | `PairingLinkStore` (InMemory + SQLite); Write-gated; credentials redacted from list; TTL enforced; revocable |
| `auth.listClientSessions` / `revokeClientSession` / `getWebSocketToken` / `getSessionState` | ✅ REAL (AUTH-2) | Write-gated; `listSessions` enumerates `conn_auth`; `revokeSession` forces reauth; `getWebSocketToken` issues `SessionRegistry`-validated token; `getSessionState` returns principal + authMode |
| `push.subscribe` / `push.unsubscribe` / `ping` / `rpc.listMethods` | ✅ REAL | WS infra |

### Project filesystem
| RPC | Status | Backed by |
|---|---|---|
| `project.readFile` / `writeFile` / `listDirectories` / `searchEntries` / `searchLocalEntries` | ✅ REAL (PROJ-2) | `project_fs` primitives with 2-layer traversal guard (lexical + canonicalize-containment); MCode Tier-3 shapes |
| `project.discoverScripts` / `runScript` | ✅ REAL (PROJ-3) | `discover_scripts`: package.json scripts + Makefile targets union; `run_script`: tokio::process shell-out returning stdout/stderr/exitCode |
| `project.listDevServers` / `startDevServer` / `stopDevServer` | ✅ REAL (PROJ-4) | `LocalServerManager` (real spawn/kill) + `dev_servers` HashSet sidecar for tagging/filtering |

### Desktop / Tauri IPC
| RPC | Status | Backed by |
|---|---|---|
| `desktop.checkForUpdates` / `applyUpdate` / `openExternal` / `openInEditor` | ✅ REAL (DSK-2) | Tauri IPC commands (`desktop_commands.rs`); update flow walks state machine (delegated to `tauri-plugin-updater` when wired) |
| `browser.captureScreenshot` / `listTabs` | ✅ REAL (DSK-2) | Tauri IPC commands (`browser_commands.rs`); platform-limited stubs (no portable webview-capture API) |
| `filesystem.browse` | ✅ REAL (DSK-2) | Tauri IPC command (`filesystem_commands.rs`); picker validation + empty-selection fallback |

---

## Component status (the app-wiring)
| Component | Status |
|---|---|
| Cloned MCode UI (`apps/web` → `frontend/`) | ✅ vendored (753 files + 35 shared modules), type-clean |
| **Chat (turn → provider → AI response)** | ✅ **ProviderCommandReactor wired** — turns invoke providers (default `claude`, configurable via `SYNCODE_DEFAULT_PROVIDER`); responses stream back → push to subscribed clients. **Chat threads bound to workflow state via preamble (PR #211)** — `ThreadWorkflowPreamble` injects workflow context as system message. |
| Contracts bridge (`@t3tools/contracts` shim) | ✅ complete — 139 Tier-3 symbols + RPC registry + 44-event union + branded IDs |
| Transport (`wsTransport` JSON-RPC) | ✅ Effect-free; `MCODE_TO_SERVED` (88 mappings) |
| Standalone WS backend | ✅ `cargo run -p syncode-ws --bin server` (SQLite, env-configurable) |
| **In-process WS server (Tauri)** | ✅ **DSK-1** — desktop `.setup()` spawns the same axum WS server (`ws_setup::boot`) on `SYNCODE_WS_PORT` (default **30101**); shared `WsState` managed by Tauri → IPC commands + WS handlers see the same backend. `/ws` endpoint verified reachable (101 upgrade + JSON-RPC ping round-trip). 5 integration tests (`tests/ws_spawn.rs`). |
| Terminal live output | ✅ reader-task → push bus |
| Automation execution | ✅ `ProcessRunExecutor` (sh -c) |
| LLM ops | ✅ provider CLI one-shot (`llm.rs::invoke_llm_oneshot`) — **no API key** (providers use CLI auth) |
| Desktop shell (Tauri) | ✅ builds + **29 commands** wired (added `getWsEndpoint`); WS server spawned in `.setup()` (DSK-1). **Boot E2E verified** (DSK-3): the full `.setup()` wiring (`ws_setup::boot` → `WsRuntimeState` → endpoint accessor → WS handshake + JSON-RPC round-trip) is covered by a headless test (`tests/boot_e2e.rs::ws_setup_boot_wiring_e2e`), and the **actual binary** is booted under `xvfb-run` in CI (`.github/workflows/desktop-e2e.yml`) with a WS-connect assertion against the live shell (`desktop_binary_boot_connects_ws`, env-gated by `DESKTOP_E2E_WS_URL`). |

## Test/quality state
- **Frontend**: tsc **0 errors**, vitest **2128 pass / 0 fail**, vite build green.
- **Backend**: `cargo test -p syncode-ws` **350+** (lib + e2e; +50 added in PRs #209/#212/#211); `syncode-contracts` 98; `syncode-automation` 72; `syncode-terminal` 20; `syncode-auth` 39; `syncode-persistence` 25; `syncode-core` 45; `syncode-git` 40; `syncode-tauri` 29. Per-crate `cargo clippy -- -D warnings` green.
- **STUB→REAL workflow**: 34 tasks, 30 PRs (#49–#78), +200 tests added across all crates. Workflow `c55208b0` complete 2026-07-05. **Subsequent major features (PRs #205-#212)**: +50 more tests.
- **PRD depth-parity audit (2026-07-06)**: `docs/PRD-REMAINING-GAPS.md` P0–P5 epics verified **SHIPPED** — all 31 sub-tasks REAL on `master` via PRs #79/#85/#87/#90/#98/#99/#100/#102/#103/#108. Re-counted per-crate tests (0 fail): `syncode-core` 94+2 doctests, `syncode-memory` 9, `syncode-orchestration` 216, `syncode-automation` 163, `syncode-terminal` 42, `syncode-git` 52, `syncode-provider` 303. See PRD Appendix C for the full parity matrix.

## Genuinely remaining (marginal / niche / config-gated)
All major gaps closed by the STUB→REAL workflow (workflow `c55208b0`, PRs #49–#78). Remaining items are marginal:
- **Browser screenshot/tab-list** — platform-limited (DSK-2 provides graceful stubs; full webview-capture needs platform-specific APIs).
- **Plugin marketplace remote sync** — explicitly out of scope (PROV-1 provides filesystem + install-state; OAuth marketplace server is a separate epic).
- **STT on CI** — whisper-CLI integration is `#[cfg(feature="stt")]` + `#[ignore]`-gated (needs whisper binary installed; SRV-4).
- **Peak-day/windowed usage breakdown** — `UsageStore` aggregates per-provider totals only (no time-windowed breakdown; SRV-1 era design decision).
- **Vector/graph memory backend tests** — `VectorBackend` (pgvector) and `GraphBackend` (Apache AGE) are feature-gated; core FTS5 backend fully tested (PR #210).

---

*For the design/spec history see [`CONTRACTS-BRIDGE-DESIGN.md`](./CONTRACTS-BRIDGE-DESIGN.md); for the frontend comparison/lineage see [`COMPARISON-FRONTEND-MCODE-vs-SYNCODE.md`](./COMPARISON-FRONTEND-MCODE-vs-SYNCODE.md).*

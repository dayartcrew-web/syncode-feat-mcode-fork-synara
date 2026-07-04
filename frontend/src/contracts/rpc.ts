/**
 * Tier 1 — Typed RPC method registry (served + unserved).
 *
 * This is the keystone artifact of the contracts bridge (see
 * `CONTRACTS-BRIDGE-DESIGN.md` §4). It maps each JSON-RPC method string to its
 * `Request` (params) and `Result` types, so the transport layer (T5) and the
 * cloned MCode UI are type-safe on every served call.
 *
 * ## Two registries
 *
 * 1. **`SERVED_RPC`** — the methods `syncode-ws::rpc::dispatch_method`
 *    actually handles (system + project + thread + turn + shell/snapshot +
 *    git + server + terminal + auth + push; plus `ping` + `rpc/listMethods`),
 *    each carrying concrete `Request`/`Result` types from the ts-rs-generated
 *    DTOs in `../types/*` and the Tier-3 domain types. Calling these succeeds
 *    at runtime (T5 wires the transport).
 *
 * 2. **`UNSERVED_RPC`** — the ~60 MCode RPC methods Syncode does NOT serve
 *    (git ops, terminal, server-meta, provider-discovery, automation, …).
 *    These return `MethodNotFound (-32601)` at runtime (the T5 transport layer
 *    enforces this). They're enumerated here so the cloned UI's imports of
 *    their method names resolve to a typed `string` literal rather than `any`.
 *
 * ## Type-level registry trick
 *
 * Each served entry uses `null as unknown as T` for its `request`/`result`
 * fields. This makes the registry a **type-level** construct — the values are
 * `null` at runtime, fully erased by the TS compiler. The *types* of the
 * fields are what matter, surfaced via `ServedRpcRequest<M>` /
 * `ServedRpcResult<M>`. (No runtime RPC happens through this object.)
 *
 * ## Slash vs dot convention
 *
 * Syncode's wire uses **slash** method strings (`project/create`); MCode's UI
 * uses **dot** camelCase keys (`projectCreate`). The contracts registry is the
 * source of truth for **Syncode slash strings**. A thin name-map in the T5
 * transport re-wire translates MCode keys to these slash strings.
 *
 * @see CONTRACTS-BRIDGE-DESIGN.md §4 (Tier 1), §6.2 (method-name mapping), §8 (RPC coverage)
 */

// ─── Served DTO imports (ts-rs-generated, camelCase) ────────────────────
import type { ListMethodsResult } from "../types/ListMethodsResult";
import type { PingResult } from "../types/PingResult";
import type { ProjectCreateParams } from "../types/ProjectCreateParams";
import type { ProjectGetParams } from "../types/ProjectGetParams";
import type { OrchestrationReadModel } from "./tier3/orchestration";
import type { OrchestrationShellSnapshot } from "./tier3/orchestration";
import type { ProjectListResult } from "../types/ProjectListResult";
import type { ProjectSummary } from "../types/ProjectSummary";
import type { ThreadCreateParams } from "../types/ThreadCreateParams";
import type { ThreadGetParams } from "../types/ThreadGetParams";
import type { ThreadLifecycleParams } from "../types/ThreadLifecycleParams";
import type { ThreadListParams } from "../types/ThreadListParams";
import type { ThreadListResult } from "../types/ThreadListResult";
import type { ThreadSummary } from "../types/ThreadSummary";
import type { TurnCompleteParams } from "../types/TurnCompleteParams";
import type { TurnGetParams } from "../types/TurnGetParams";
import type { TurnListParams } from "../types/TurnListParams";
import type { TurnListResult } from "../types/TurnListResult";
import type { TurnStartParams } from "../types/TurnStartParams";
import type { TurnSummary } from "../types/TurnSummary";
import type { AuthBootstrapParams } from "../types/AuthBootstrapParams";
import type { AuthBootstrapResult } from "../types/AuthBootstrapResult";
import type { AuthLogoutResult } from "../types/AuthLogoutResult";
import type { AuthStatusResult } from "../types/AuthStatusResult";
import type { PushSubscribeParams } from "../types/PushSubscribeParams";
import type { PushSubscribeResult } from "../types/PushSubscribeResult";
import type { PushUnsubscribeParams } from "../types/PushUnsubscribeParams";
import type { PushUnsubscribeResult } from "../types/PushUnsubscribeResult";
// Git Tier-3 result/input types (T6c-3 git RPC exposure). The backend maps
// syncode-git's types into these MCode shapes — see `crates/syncode-ws/src/rpc.rs`
// `handle_git_*` handlers. T6c-9 adds `GitStashInfoResult` for the advanced
// stash RPCs.
import type {
  GitBranch,
  GitReadWorkingTreeDiffInput,
  GitRunStackedActionResult,
  GitStashInfoResult,
  GitStatusResult,
} from "./tier3/git";
// Server Tier-3 result/input types (T6c-4 server config RPC exposure). The
// backend `crates/syncode-ws/src/rpc.rs` `handle_server_*` handlers return
// minimal valid MCode shapes (arrays empty, optionals null) — see
// `frontend/src/contracts/tier3/server.ts` for the canonical shapes.
import type {
  ServerConfig,
  ServerConfigIssue,
  ServerGenerateAutomationIntentInput,
  ServerGenerateAutomationIntentResult,
  ServerGetProviderUsageSnapshotInput,
  ServerGetProviderUsageSnapshotResult,
  ServerListProviderUsageInput,
  ServerListProviderUsageResult,
  ServerProviderStatus,
  ServerSettings,
  ServerSettingsPatch,
  ServerStopLocalServerInput,
} from "./tier3/server";
// Terminal Tier-3 result type (T6c-5 terminal PTY RPC exposure). The backend
// `crates/syncode-ws/src/rpc.rs` `handle_terminal_*` handlers reuse
// `syncode-terminal::SessionManager` and map its `SessionInfo` into the MCode
// `TerminalSessionSnapshot` shape — see `frontend/src/contracts/tier3/terminal.ts`.
import type { TerminalSessionSnapshot } from "./tier3/terminal";
// Automation Tier-3 result/input types (T6c-6 automation RPC exposure). The
// backend `crates/syncode-ws/src/rpc.rs` `handle_automation_*` handlers reuse
// `syncode-automation::Scheduler` and map its `AutomationDef`/`AutomationRun`
// into the MCode `AutomationDefinition`/`AutomationRun` shapes — see
// `frontend/src/contracts/tier3/automation.ts` for the canonical shapes + the
// `AutomationListResult`/`AutomationRunNowResult`/`AutomationRunActionResult`
// input/result types from `./shell.ts`.
import type {
  AutomationCreateInput,
  AutomationDefinition,
  AutomationListResult,
  AutomationUpdateInput,
} from "./tier3/automation";
import type {
  AutomationArchiveRunInput,
  AutomationCancelRunInput,
  AutomationCancelRunResult,
  AutomationDeleteInput,
  AutomationListInput,
  AutomationMarkRunReadInput,
  AutomationRunActionResult,
  AutomationRunNowInput,
  AutomationRunNowResult,
  GitSummarizeDiffInput,
  GitSummarizeDiffResult,
  ServerGenerateThreadRecapInput,
  ServerGenerateThreadRecapResult,
  // T6c-14 GitHub-API ops (gh-CLI-backed).
  GitHubRepositoryInput,
  GitHubRepositoryResult,
  GitPullRequestRefInput,
  GitResolvePullRequestResult,
  GitHandoffThreadInput,
  GitHandoffThreadResult,
  GitPreparePullRequestThreadInput,
  GitPreparePullRequestThreadResult,
  // T6c-16 git stacked-action / detached-worktree / progress RPCs.
  GitCreateDetachedWorktreeInput,
  GitCreateDetachedWorktreeResult,
  GitRunStackedActionInput,
} from "./shell";
import type { WsWelcomePayload } from "./tier3/ws";
// Provider discovery Tier-3 result types (T6c-7 provider RPC exposure). The
// backend `crates/syncode-ws/src/rpc.rs` `handle_provider_*` handlers return
// minimal valid MCode shapes (empty arrays/null descriptors, except listModels/
// listAgents which are cheaply populated from the syncode-provider
// `ALL_PROVIDERS` static) — see `frontend/src/contracts/tier3/provider.ts` for
// the canonical shapes.
import type {
  ProviderComposerCapabilities,
  ProviderListAgentsResult,
  ProviderListCommandsResult,
  ProviderListModelsResult,
  ProviderListPluginsResult,
  ProviderListSkillsResult,
  ProviderPluginDetail,
  ProviderSkillsCatalogResult,
  ProviderSkillDescriptor,
} from "./tier3/provider";
// Profile stats Tier-3 result/input types (T6c-8 stats RPC exposure). The
// backend `crates/syncode-ws/src/rpc.rs` `handle_stats_*` handlers return
// minimal valid MCode shapes (aggregates zeroed, arrays empty, optionals null
// — syncode has no stats aggregation subsystem) — see
// `frontend/src/contracts/tier3/stats.ts` for the canonical shapes.
import type {
  ProfileStats,
  ProfileTokenStats,
  StatsGetProfileStatsInput,
} from "./tier3/stats";

// Minimal git input shapes for the served slash dispatch keys. The MCode UI
// sends params under these camelCase keys (`cwd`, `branch`, `paths`,
// `message`); the backend reads them verbatim (see `handle_git_*`).
interface GitCwdInput {
  cwd: string;
}
interface GitCreateBranchInput {
  cwd: string;
  branch: string;
  publish?: boolean;
}
interface GitCheckoutInput {
  cwd: string;
  branch: string;
}
interface GitStageFilesInput {
  cwd: string;
  paths: readonly string[];
}
interface GitCommitInput {
  cwd: string;
  message: string;
}
interface GitStageFilesResult {
  ok: boolean;
}
// readWorkingTreeDiff returns `{ patch: string }` (MCode GitReadWorkingTreeDiffResult).
interface GitReadWorkingTreeDiffResult {
  patch: string;
}
// listBranches returns the MCode GitListBranchesResult.
interface GitListBranchesResult {
  branches: readonly GitBranch[];
  isRepo: boolean;
  hasOriginRemote: boolean;
}

// ─── Git Advanced input/result shapes (T6c-9 stash/network/worktree/init) ─
//
// The backend `crates/syncode-ws/src/rpc.rs` `handle_git_*` advanced handlers
// return these shapes. `GitStashInfoResult` is the canonical Tier-3 type
// (vendored from MCode); the other result shapes are local best-effort
// interfaces mirroring what the handlers emit (the backend serializes
// syncode-git/git2 types directly + small ad-hoc JSON objects). The UI
// surface that consumes these is the GitPanel's stash/worktree/network menus;
// they read the top-level fields declared here.

interface GitStashCreateInput {
  cwd: string;
  message?: string;
}
interface GitStashCreateResult {
  ok: boolean;
  /** null when there was nothing to stash. */
  oid: string | null;
  /** `stash@{N}` form, or null when nothing to stash. */
  stashRef: string | null;
  reason?: string;
}
interface GitStashIndexInput {
  cwd: string;
  /** stash index (0 = most recent). Defaults to 0 when omitted. */
  index?: number;
}
interface GitStashEntry {
  index: number;
  message: string;
  oid: string;
  /** `stash@{N}` form. */
  stashRef: string;
}
interface GitStashListResult {
  stashes: readonly GitStashEntry[];
}
interface GitStashActionResult {
  ok: boolean;
}
interface GitStashAndCheckoutResult {
  /** Always false — this op is stubbed (use stashCreate + checkout). */
  ok: boolean;
  reason?: string;
}
interface GitNetworkInput {
  cwd: string;
  remote?: string;
  branch?: string;
  /** fetch-only: optional single refspec. */
  refspec?: string;
}
interface GitFetchResult {
  ok: boolean;
  remote: string;
  refspec: string;
}
// pull/push results reuse the syncode-git PushResult/PullResult wire shape:
// `{ status: "pushed" | "skipped_up_to_date", branch, upstream_branch,
//   set_upstream }` (push) / `{ status: "pulled" | "skipped_up_to_date",
//   branch, upstream_branch }` (pull). Declared as local interfaces so the
// served registry is type-safe without importing syncode-git's Rust types.
interface GitPushResult {
  status: "pushed" | "skipped_up_to_date";
  branch: string;
  upstream_branch: string;
  set_upstream?: boolean;
}
interface GitPullResult {
  status: "pulled" | "skipped_up_to_date";
  branch: string;
  upstream_branch: string;
}
interface GitInitInput {
  /** Path to initialize (need not yet be a repo). */
  cwd: string;
}
interface GitInitResult {
  ok: boolean;
  path: string;
}
interface GitRemoveIndexLockResult {
  ok: boolean;
  /** true when a lock file was removed, false when none was present. */
  removed: boolean;
  path: string;
}
interface GitWorktreeListResult {
  worktrees: readonly {
    path: string;
    branch: string | null;
    is_main: boolean;
    is_locked: boolean;
  }[];
}
interface GitWorktreeCreateInput {
  cwd: string;
  branch: string;
  /** Optional filesystem path for the new worktree. */
  path?: string;
  /** Create the branch at HEAD if it doesn't exist (default true). */
  createBranch?: boolean;
}
interface GitWorktreeCreateResult {
  worktree: {
    path: string;
    branch: string;
    is_main: boolean;
    is_locked: boolean;
  };
}
interface GitWorktreeRemoveInput {
  cwd: string;
  /** Worktree name (= branch it was created for). */
  branch: string;
  /** Force-remove a dirty/locked worktree. */
  force?: boolean;
}
interface GitWorktreeRemoveResult {
  ok: boolean;
}

// ─── Server config/env/diagnostics/subscribe shapes (T6c-4) ────────────
//
// `ServerConfig` and `ServerSettings` are imported from Tier-3 `server.ts`
// (canonical MCode shapes). The diagnostics/environment/subscribe shapes are
// NOT vendored in Tier-3 (MCode derives them from Effect schemas in
// `packages/contracts/src/{server,environment}.ts`); we declare local
// interfaces mirroring the MCode top-level field set so the served registry
// is type-safe. The backend returns these exact shapes (see
// `handle_server_*` in `crates/syncode-ws/src/rpc.rs`).

// ServerGetEnvironmentResult = ExecutionEnvironmentDescriptor (MCode
// `environment.ts`). The backend surfaces `std::env::consts::{OS,ARCH}` +
// the syncode-ws crate version.
interface ServerGetEnvironmentResult {
  environmentId: string;
  label: string;
  platform: { os: string; arch: string };
  serverVersion: string;
  capabilities: { repositoryIdentity: boolean };
}

// ServerDiagnosticsResult (MCode `server.ts` ~L232). The backend zeroes the
// memory counters (no stable rss/heap probe) and pulls live project/thread
// counts into `projection`.
interface ServerDiagnosticsMemory {
  rssBytes: number;
  heapTotalBytes: number;
  heapUsedBytes: number;
  externalBytes: number;
  arrayBuffersBytes: number;
}
interface ServerDiagnosticsResult {
  generatedAt: string;
  process: {
    pid: number;
    uptimeSeconds: number;
    memory: ServerDiagnosticsMemory;
  };
  childProcesses: readonly unknown[];
  childProcessTotalCount: number;
  childProcessTotalRssBytes: number;
  projection: { projectCount: number; threadCount: number };
}

// Server subscribe* stubs (T6c-4). The backend returns a success envelope
// without recording a real push subscription or emitting push events —
// real push delivery is T6c-future.
interface ServerSubscribeStubResult {
  subscribed: boolean;
  channel: string;
  note?: string;
}

// ─── Server write-side stub shapes (T6c-10) ────────────────────────────
//
// The cloned MCode UI persists user edits via these `server.*` write RPCs.
// Syncode has no native settings/keybindings persistence layer, so each
// backend handler is a STUB: it validates the params shape and echoes the
// default read-side payload. The ack result types mirror MCode's contracts:
//
//   - `server.setConfig`        → echoes `ServerConfig` (default).
//   - `server.updateSettings`   → echoes `ServerSettings` (default; the
//                                 patch is NOT applied).
//   - `server.refreshProviders` → `{ providers: [] }` (empty status payload).
//   - `server.updateProvider`   → `{ providers: [] }` (validates `provider`).
//   - `server.upsertKeybinding` → `{ keybindings: [], issues: [] }` (validates
//                                 params is an object).
//
// `ServerConfig`/`ServerSettings` are reused from Tier-3 `server.ts`. The
// provider-status and keybindings ack shapes get local interfaces below.

// MCode `ServerProviderStatusesUpdatedPayload` (the ack shape for both
// `refreshProviders` and `updateProvider`). The stub returns an empty
// `providers` array (no provider-availability probe in syncode).
interface ServerProvidersStatusResult {
  providers: readonly ServerProviderStatus[];
}

// MCode `ServerProviderUpdateInput`: `{ provider: ProviderKind }`. The backend
// validates `provider` is a non-empty string before returning the ack.
interface ServerProviderUpdateInput {
  provider: string;
}

// MCode `ServerUpsertKeybindingResult`: `{ keybindings, issues }`. The stub
// echoes empty arrays (no keybindings resolver in syncode). `keybindings` is
// `readonly unknown[]` because the MCode `ResolvedKeybindingsConfig` shape is
// an array of resolved rules the UI tolerates empty; `issues` mirrors the
// `ServerConfigIssue` shape from Tier-3 `server.ts`.
interface ServerUpsertKeybindingResult {
  keybindings: readonly unknown[];
  issues: readonly ServerConfigIssue[];
}

// MCode `ServerUpsertKeybindingInput` = `KeybindingRule` (a struct). The
// backend validates params is a JSON object; the exact field set is not
// enforced server-side (the stub accepts any object and returns the empty
// default). Typed loosely as `Record<string, unknown>` to match.
type ServerUpsertKeybindingInput = Record<string, unknown>;

// `server.setConfig` accepts a full `ServerConfig` overwrite. The stub ignores
// the payload and echoes the default.
type ServerSetConfigInput = Partial<ServerConfig>;

// `server.updateSettings` accepts a `ServerSettingsPatch`. The stub ignores
// the patch and echoes the default `ServerSettings`.
type ServerUpdateSettingsInput = Record<string, unknown>;

// ─── Terminal PTY input/result shapes (T6c-5) ──────────────────────────
//
// `TerminalSessionSnapshot` is imported from Tier-3 `terminal.ts` (the
// canonical MCode shape). The input shapes mirror the camelCase keys the
// backend reads in `handle_terminal_*` (`terminalId`/`sessionId`, `cwd`,
// `command`, `cols`/`rows`, `data`, `sequence`). The backend accepts both
// `terminalId` (MCode convention) and `sessionId` (legacy tauri shape) as
// the session key — `terminalId` wins when both are present.
//
// `terminal.env` (sent by `projectTerminalRunner` for project-script
// terminals) is declared optional but NOT applied by the backend today
// (syncode-terminal's `PtyHandle::spawn` doesn't accept per-session env —
// documented gap; the PTY inherits the server process env).

interface TerminalOpenInput {
  terminalId?: string;
  sessionId?: string;
  threadId?: string;
  cwd?: string;
  command?: string;
  args?: readonly string[];
  env?: Readonly<Record<string, string>>;
  cols?: number;
  rows?: number;
}
interface TerminalWriteInput {
  terminalId?: string;
  sessionId?: string;
  data: string;
}
interface TerminalResizeInput {
  terminalId?: string;
  sessionId?: string;
  cols?: number;
  rows?: number;
}
interface TerminalCloseInput {
  terminalId?: string;
  sessionId?: string;
}
interface TerminalAckOutputInput {
  terminalId?: string;
  sessionId?: string;
  sequence?: number;
  seq?: number;
  ackedBytes?: number;
}
interface TerminalClearInput {
  terminalId?: string;
  sessionId?: string;
}
interface TerminalRestartInput {
  terminalId?: string;
  sessionId?: string;
  cwd?: string;
  command?: string;
  cols?: number;
  rows?: number;
}
interface TerminalCloseResult {
  ok: boolean;
}
interface TerminalClearResult {
  ok: boolean;
}
interface TerminalListResult {
  sessions: readonly TerminalSessionSnapshot[];
}
// subscribeEvents (T6c-11): records a real `terminal` channel subscription
// on the originating connection. A per-session output-reader task broadcasts
// `terminal/event` push frames (output/exited/error) onto the push bus;
// connections subscribed here receive them via `push/terminal`.
interface TerminalSubscribeResult {
  subscribed: boolean;
  channel: string;
  /** True if this call newly added the subscription (false = already subscribed). */
  added?: boolean;
  /** Present on unsubscribe responses. */
  unsubscribed?: boolean;
  removed?: boolean;
}

// ─── Provider discovery minimal input/result shapes (T6c-7) ────────────
// The backend handlers return minimal MCode shapes: arrays empty, optionals
// null. `readPlugin`/`readSkill` return a null descriptor (the UI renders an
// empty/not-found state); `compactThread` returns `{ ok: true }` (stub — no
// LLM-side compaction wired in the WS layer). The list RPCs reuse the full
// Tier-3 result types from `./tier3/provider` directly. See
// `handle_provider_*` in `crates/syncode-ws/src/rpc.rs`.
interface ProviderReadPluginInput {
  /** MCode marketplace + plugin id pair (the UI sends the marketplace name +
   * plugin id it wants detail for; backend ignores both — returns null). */
  marketplaceName?: string;
  pluginId?: string;
}
interface ProviderReadPluginResult {
  plugin: ProviderPluginDetail | null;
}
interface ProviderGetComposerCapabilitiesInput {
  /** MCode `ProviderKind` the composer is querying capabilities for. */
  provider: string;
}
interface ProviderListOptionsResult {
  options: readonly never[];
}
interface ProviderReadSkillInput {
  /** Skill name/path the UI wants detail for (backend ignores — returns null). */
  name?: string;
  path?: string;
}
interface ProviderReadSkillResult {
  skill: ProviderSkillDescriptor | null;
}
interface ProviderCompactThreadInput {
  /** Thread id to compact. The backend reads its messages from the read model
   * and runs them through a provider adapter one-shot (T6c-13). */
  threadId?: string;
  /** Optional provider override (default "claude", falls back to "codex"). */
  provider?: string;
  /** Optional model override. */
  model?: string;
}
interface ProviderCompactThreadResult {
  /** `true` on success (compacted summary produced, or no-op for empty
   * history); `false` if the provider couldn't be invoked (CLI missing,
   * not registered, LLM error). */
  ok: boolean;
  /** The compacted summary text (empty string for the no-op-empty-history
   * path). Present when `ok` is true. */
  compactedSummary?: string;
  /** Human-readable error message. Present when `ok` is false. */
  error?: string;
}

// ════════════════════════════════════════════════════════════════════════
// ─── SERVED_RPC — 102 entries (latest: T6c-16 git stacked/detached-worktree/progress +3) ──
// ════════════════════════════════════════════════════════════════════════

/**
 * Registry of every JSON-RPC method the Syncode WS backend serves. Keys are
 * the slash-method strings from `rpc/listMethods` in
 * `crates/syncode-ws/src/rpc.rs`. Each value carries `request` (params DTO or
 * `null` if the method takes no params) and `result` (the result DTO).
 *
 * Methods with no params use `null` as the request type — these are the
 * RPCs whose handler ignores `request.params` (`project/list`, `auth/status`,
 * `auth/logout`, `ping`, `rpc/listMethods`).
 *
 * Result types that are projections of a read-model view reuse the snapshot
 * summary types directly: `ProjectGetResult = ProjectSummary`,
 * `ThreadGetResult = ThreadSummary`, `TurnGetResult = TurnSummary`, and the
 * lifecycle results (`thread/pause|resume|cancel`, `turn/start|complete`,
 * `project/create`, `thread/create`) likewise return the matching summary.
 */
export const SERVED_RPC = {
  // ─── System ──────────────────────────────────────────────────────────
  ping: { request: null as unknown as null, result: null as unknown as PingResult },
  "rpc/listMethods": {
    request: null as unknown as null,
    result: null as unknown as ListMethodsResult,
  },

  // ─── Project ─────────────────────────────────────────────────────────
  "project/list": { request: null as unknown as null, result: null as unknown as ProjectListResult },
  "project/get": { request: null as unknown as ProjectGetParams, result: null as unknown as ProjectSummary },
  "project/create": {
    request: null as unknown as ProjectCreateParams,
    result: null as unknown as ProjectSummary,
  },

  // ─── Thread ──────────────────────────────────────────────────────────
  "thread/list": { request: null as unknown as ThreadListParams, result: null as unknown as ThreadListResult },
  "thread/get": { request: null as unknown as ThreadGetParams, result: null as unknown as ThreadSummary },
  "thread/create": {
    request: null as unknown as ThreadCreateParams,
    result: null as unknown as ThreadSummary,
  },
  "thread/pause": {
    request: null as unknown as ThreadLifecycleParams,
    result: null as unknown as ThreadSummary,
  },
  "thread/resume": {
    request: null as unknown as ThreadLifecycleParams,
    result: null as unknown as ThreadSummary,
  },
  "thread/cancel": {
    request: null as unknown as ThreadLifecycleParams,
    result: null as unknown as ThreadSummary,
  },

  // ─── Turn ────────────────────────────────────────────────────────────
  "turn/list": { request: null as unknown as TurnListParams, result: null as unknown as TurnListResult },
  "turn/get": { request: null as unknown as TurnGetParams, result: null as unknown as TurnSummary },
  "turn/start": { request: null as unknown as TurnStartParams, result: null as unknown as TurnSummary },
  "turn/complete": {
    request: null as unknown as TurnCompleteParams,
    result: null as unknown as TurnSummary,
  },

  // ─── Shell / Snapshot (read-model bootstrap) ─────────────────────────
  // The cloned MCode UI bootstraps its sidebar from `getShellSnapshot`. The
  // transport remaps the MCode dot-strings (`orchestration.getShellSnapshot`,
  // `orchestration.getSnapshot`) onto these slash methods. Results use the
  // UI's own projection types — the backend composes the read_store into the
  // matching field shapes (title/workspaceRoot/modelSelection/session/…).
  "shell/getSnapshot": {
    request: null as unknown as null,
    result: null as unknown as OrchestrationShellSnapshot,
  },
  "snapshot/get": {
    request: null as unknown as null,
    result: null as unknown as OrchestrationReadModel,
  },

  // ─── Git (syncode-git-backed, T6c-3) ────────────────────────────────
  // The cloned MCode GitPanel calls `git.*` RPCs. The transport remaps the
  // MCode dot-strings (`git.status`, `git.readWorkingTreeDiff`,
  // `git.listBranches`, …) onto these slash keys (see `MCODE_TO_SERVED` in
  // `wsTransport.ts`). The backend `crates/syncode-ws/src/rpc.rs`
  // `handle_git_*` handlers reuse `syncode-git::Git2Service` and map the
  // results into the MCode shapes (Tier-3 `git.ts`).
  //
  // Known gaps (documented in `handle_git_*`):
  //   - per-file insertions/deletions are 0 (syncode-git lacks line stats)
  //   - `git.unstage` with non-empty paths is not implemented (no syncode-git
  //     unstage op) → INTERNAL_ERROR. Empty-paths is a no-op OK.
  //   - `git.readWorkingTreeDiff` returns a synthesized minimal patch (real
  //     unified-diff hunks require git2::Patch plumbing — deferred).
  "git/status": { request: null as unknown as GitCwdInput, result: null as unknown as GitStatusResult },
  "git/diff": {
    request: null as unknown as GitReadWorkingTreeDiffInput,
    result: null as unknown as GitReadWorkingTreeDiffResult,
  },
  "git/branches": { request: null as unknown as GitCwdInput, result: null as unknown as GitListBranchesResult },
  "git/create-branch": {
    request: null as unknown as GitCreateBranchInput,
    result: null as unknown as null,
  },
  "git/checkout": { request: null as unknown as GitCheckoutInput, result: null as unknown as null },
  "git/delete-branch": { request: null as unknown as GitCheckoutInput, result: null as unknown as null },
  "git/add": {
    request: null as unknown as GitStageFilesInput,
    result: null as unknown as GitStageFilesResult,
  },
  "git/unstage": {
    request: null as unknown as GitStageFilesInput,
    result: null as unknown as GitStageFilesResult,
  },
  "git/commit": { request: null as unknown as GitCommitInput, result: null as unknown as null },

  // ─── Server config / settings / lifecycle (T6c-4) ───────────────────
  // The cloned MCode UI calls these `server.*` RPCs on startup (Settings
  // panel + provider-config initialization). The transport remaps the MCode
  // dot-strings (`server.getConfig`, `server.getSettings`, …) onto these
  // slash keys (see `MCODE_TO_SERVED` in `wsTransport.ts`). The backend
  // `crates/syncode-ws/src/rpc.rs` `handle_server_*` handlers return minimal
  // valid MCode shapes (required fields present, arrays empty, optionals
  // null). `ServerConfig`/`ServerSettings` use the canonical Tier-3 types;
  // diagnostics/environment/subscribe use local interfaces mirroring MCode.
  //
  // Known gaps (documented in `handle_server_*`):
  //   - `server.getConfig` returns empty `providers`/`availableEditors`/
  //     `keybindings`/`issues` (no provider probe / editor detection).
  //   - `server.getDiagnostics` zeroes memory counters (no stable probe).
  //   - `server.subscribe*` are stubs (no push delivery — T6c-future).
  "server/getConfig": { request: null as unknown as null, result: null as unknown as ServerConfig },
  "server/getSettings": {
    request: null as unknown as null,
    result: null as unknown as ServerSettings,
  },
  "server/welcome": {
    request: null as unknown as null,
    result: null as unknown as WsWelcomePayload,
  },
  "server/getEnvironment": {
    request: null as unknown as null,
    result: null as unknown as ServerGetEnvironmentResult,
  },
  "server/getDiagnostics": {
    request: null as unknown as null,
    result: null as unknown as ServerDiagnosticsResult,
  },
  "server/subscribeConfig": {
    request: null as unknown as null,
    result: null as unknown as ServerSubscribeStubResult,
  },
  "server/subscribeSettings": {
    request: null as unknown as null,
    result: null as unknown as ServerSubscribeStubResult,
  },
  "server/subscribeProviderStatuses": {
    request: null as unknown as null,
    result: null as unknown as ServerSubscribeStubResult,
  },
  "server/subscribeLifecycle": {
    request: null as unknown as null,
    result: null as unknown as ServerSubscribeStubResult,
  },

  // ─── Terminal PTY (syncode-terminal-backed, T6c-5) ───────────────────
  // The cloned MCode UI's Terminal panel + project-script runner call these
  // `terminal.*` RPCs. The transport remaps the MCode dot-strings
  // (`terminal.open`, `terminal.write`, `terminal.resize`, `terminal.close`,
  // `terminal.ackOutput`, `terminal.list`, `terminal.clear`,
  // `terminal.restart`, `terminal.subscribeEvents`) onto these slash keys
  // (see `MCODE_TO_SERVED` in `wsTransport.ts`). The backend
  // `crates/syncode-ws/src/rpc.rs` `handle_terminal_*` handlers reuse
  // `syncode-terminal::SessionManager` and map `SessionInfo` into the MCode
  // `TerminalSessionSnapshot` shape.
  //
  // Known gaps (documented in `handle_terminal_*`):
  //   - `terminal.env` (project-script runtime env) is NOT applied — the PTY
  //     inherits the server process env (syncode-terminal's spawn has no
  //     per-session env hook). Documented gap.
  //   - `terminal.subscribeEvents` (T6c-11): records a real `terminal` channel
  //     subscription. A per-session output-reader task broadcasts `terminal/event`
  //     push frames (output/exited/error) onto the push bus; subscribed
  //     connections receive them via `push/terminal`.
  //   - `exitCode`/`exitSignal`/`history` in the snapshot are null/empty
  //     (syncode-terminal doesn't track exit codes or scrollback).
  "terminal/create": {
    request: null as unknown as TerminalOpenInput,
    result: null as unknown as TerminalSessionSnapshot,
  },
  "terminal/write": {
    request: null as unknown as TerminalWriteInput,
    result: null as unknown as null,
  },
  "terminal/resize": {
    request: null as unknown as TerminalResizeInput,
    result: null as unknown as null,
  },
  "terminal/close": {
    request: null as unknown as TerminalCloseInput,
    result: null as unknown as TerminalCloseResult,
  },
  "terminal/ack": {
    request: null as unknown as TerminalAckOutputInput,
    result: null as unknown as null,
  },
  "terminal/list": {
    request: null as unknown as null,
    result: null as unknown as TerminalListResult,
  },
  "terminal/clear": {
    request: null as unknown as TerminalClearInput,
    result: null as unknown as TerminalClearResult,
  },
  "terminal/restart": {
    request: null as unknown as TerminalRestartInput,
    result: null as unknown as TerminalSessionSnapshot,
  },
  "terminal/subscribe-events": {
    request: null as unknown as null,
    result: null as unknown as TerminalSubscribeResult,
  },

  // ─── Automation (T6c-6 — syncode-automation-backed) ───────────────────
  // The Automations panel calls `automation.*` RPCs. The backend reuses
  // `syncode-automation::Scheduler` for def + run-record lifecycle, mapping
  // `AutomationDef`/`AutomationRun` into the MCode shapes. Notes:
  //   - `subscribe` is a STUB (no real `automation.event` push — deferred).
  //   - `markRunRead`/`archiveRun` are STUBS (syncode run type/repo don't
  //     model `unread`/`archivedAt` — return the run unchanged).
  //   - `runNow` uses `Delay::Immediate` (no-op executor retries are pointless;
  //     real executor wiring is deferred).
  "automation/list": {
    request: null as unknown as AutomationListInput,
    result: null as unknown as AutomationListResult,
  },
  "automation/create": {
    request: null as unknown as AutomationCreateInput,
    result: null as unknown as AutomationDefinition,
  },
  "automation/get": {
    request: null as unknown as AutomationListInput,
    result: null as unknown as AutomationDefinition,
  },
  "automation/update": {
    request: null as unknown as AutomationUpdateInput,
    result: null as unknown as AutomationDefinition,
  },
  "automation/delete": {
    request: null as unknown as AutomationDeleteInput,
    result: null as unknown as { ok: boolean },
  },
  "automation/run-now": {
    request: null as unknown as AutomationRunNowInput,
    result: null as unknown as AutomationRunNowResult,
  },
  "automation/cancel-run": {
    request: null as unknown as AutomationCancelRunInput,
    result: null as unknown as AutomationCancelRunResult,
  },
  "automation/mark-run-read": {
    request: null as unknown as AutomationMarkRunReadInput,
    result: null as unknown as AutomationRunActionResult,
  },
  "automation/archive-run": {
    request: null as unknown as AutomationArchiveRunInput,
    result: null as unknown as AutomationRunActionResult,
  },
  "automation/subscribe": {
    request: null as unknown as null,
    result: null as unknown as TerminalSubscribeResult,
  },

  // ─── Auth ────────────────────────────────────────────────────────────
  "auth/bootstrap": {
    request: null as unknown as AuthBootstrapParams,
    result: null as unknown as AuthBootstrapResult,
  },
  "auth/status": { request: null as unknown as null, result: null as unknown as AuthStatusResult },
  "auth/logout": { request: null as unknown as null, result: null as unknown as AuthLogoutResult },

  // ─── Push subscription ───────────────────────────────────────────────
  "push/subscribe": {
    request: null as unknown as PushSubscribeParams,
    result: null as unknown as PushSubscribeResult,
  },
  "push/unsubscribe": {
    request: null as unknown as PushUnsubscribeParams,
    result: null as unknown as PushUnsubscribeResult,
  },

  // ─── Provider discovery (syncode-provider-backed, T6c-7) ─────────────
  // The cloned MCode UI's composer/agent-mention/SkillsPanel/plugin layer
  // calls these `provider.*` dot-strings (`wsNativeApi.ts` →
  // `callTransport("provider.listModels", …)`, `provider.getComposerCapabili-
  // ties`, …). The transport remaps the MCode dot-strings onto these slash
  // keys (see `MCODE_TO_SERVED` in `wsTransport.ts`). The backend
  // `crates/syncode-ws/src/rpc.rs` `handle_provider_*` handlers return minimal
  // valid MCode shapes (empty arrays/null descriptors, except listModels/
  // listAgents which are cheaply populated from the syncode-provider
  // `ALL_PROVIDERS` static). Entries appended at the END to ease parallel-
  // merge conflict resolution.
  //
  // `provider` param type for getComposerCapabilities: the MCode `ProviderKind`
  // union (string literal). We use `ProviderComposerCapabilities` itself as
  // the request carrier isn't right (it's the result); the handler reads a
  // loose `{ provider?: string }` so a minimal local input type suffices — but
  // reusing the schema-required shape keeps the contract honest.
  "provider/list-models": {
    request: null as unknown as null,
    result: null as unknown as ProviderListModelsResult,
  },
  "provider/list-skills": {
    request: null as unknown as null,
    result: null as unknown as ProviderListSkillsResult,
  },
  "provider/list-skills-catalog": {
    request: null as unknown as null,
    result: null as unknown as ProviderSkillsCatalogResult,
  },
  "provider/list-plugins": {
    request: null as unknown as null,
    result: null as unknown as ProviderListPluginsResult,
  },
  "provider/read-plugin": {
    request: null as unknown as ProviderReadPluginInput,
    result: null as unknown as ProviderReadPluginResult,
  },
  "provider/list-commands": {
    request: null as unknown as null,
    result: null as unknown as ProviderListCommandsResult,
  },
  "provider/list-agents": {
    request: null as unknown as null,
    result: null as unknown as ProviderListAgentsResult,
  },
  "provider/get-composer-capabilities": {
    request: null as unknown as ProviderGetComposerCapabilitiesInput,
    result: null as unknown as ProviderComposerCapabilities,
  },
  "provider/list-options": {
    request: null as unknown as null,
    result: null as unknown as ProviderListOptionsResult,
  },
  "provider/read-skill": {
    request: null as unknown as ProviderReadSkillInput,
    result: null as unknown as ProviderReadSkillResult,
  },
  "provider/compact-thread": {
    request: null as unknown as ProviderCompactThreadInput,
    result: null as unknown as ProviderCompactThreadResult,
  },

  // ─── Profile stats (T6c-8 stats RPC exposure) ────────────────────────
  // The cloned MCode UI's Profile page calls these `stats.*` dot-strings
  // (`wsNativeApi.ts` → `callTransport("stats.getProfileStats", …)`) to render
  // the activity heatmap, provider-usage breakdown, skill-usage list, token
  // totals, and quota panel. The transport remaps the MCode dot-strings onto
  // these slash keys (see `MCODE_TO_SERVED` in `wsTransport.ts`). The backend
  // `crates/syncode-ws/src/rpc.rs` `handle_stats_*` handlers return minimal
  // valid MCode shapes (aggregates zeroed, arrays empty, optionals null) since
  // syncode has no stats aggregation subsystem. Entries appended at the END to
  // ease parallel-merge conflict resolution.
  "stats/get-profile-stats": {
    request: null as unknown as StatsGetProfileStatsInput,
    result: null as unknown as ProfileStats,
  },
  "stats/get-profile-token-stats": {
    request: null as unknown as StatsGetProfileStatsInput,
    result: null as unknown as ProfileTokenStats,
  },

  // ─── Git Advanced (stash / network / worktree / init, T6c-9) ─────────
  // The cloned MCode GitPanel calls these `git.*` dot-strings beyond the
  // core phase-3 surface (status/diff/branches/branch-CRUD/stage/commit).
  // The transport remaps the MCode dot-strings onto these slash keys (see
  // MCODE_TO_SERVED in `wsTransport.ts`). The backend
  // `crates/syncode-ws/src/rpc.rs` `handle_git_*` advanced handlers use
  // git2 directly (stash, fetch, init, removeIndexLock, worktree list/create/
  // remove) and delegate to syncode-git's `Git2Service::{push,pull}` for the
  // network ops. `git.stashAndCheckout` is a stub (`{ ok:false }` — the UI
  // composes stash+checkout itself).
  //
  // Known gaps (documented in `handle_git_*`):
  //   - `git.fetch` surfaces auth failures as generic INTERNAL_ERROR (no
  //     auth-class classification; the CLI-backed push/pull paths classify
  //     AuthenticationRequired distinctly).
  //   - `git.stashAndCheckout` is a stub (compose via stashCreate + checkout).
  //   - `git.pull` is --ff-only (no merge commits; fails on divergence).
  "git/stash-list": {
    request: null as unknown as GitCwdInput,
    result: null as unknown as GitStashListResult,
  },
  "git/stash-create": {
    request: null as unknown as GitStashCreateInput,
    result: null as unknown as GitStashCreateResult,
  },
  "git/stash-apply": {
    request: null as unknown as GitStashIndexInput,
    result: null as unknown as GitStashActionResult,
  },
  "git/stash-drop": {
    request: null as unknown as GitStashIndexInput,
    result: null as unknown as GitStashActionResult,
  },
  "git/stash-info": {
    request: null as unknown as GitStashIndexInput,
    result: null as unknown as GitStashInfoResult,
  },
  "git/stash-and-checkout": {
    request: null as unknown as GitNetworkInput,
    result: null as unknown as GitStashAndCheckoutResult,
  },
  "git/fetch": {
    request: null as unknown as GitNetworkInput,
    result: null as unknown as GitFetchResult,
  },
  "git/pull": {
    request: null as unknown as GitNetworkInput,
    result: null as unknown as GitPullResult,
  },
  "git/push": {
    request: null as unknown as GitNetworkInput,
    result: null as unknown as GitPushResult,
  },
  "git/init": {
    request: null as unknown as GitInitInput,
    result: null as unknown as GitInitResult,
  },
  "git/remove-index-lock": {
    request: null as unknown as GitCwdInput,
    result: null as unknown as GitRemoveIndexLockResult,
  },
  "git/worktree-list": {
    request: null as unknown as GitCwdInput,
    result: null as unknown as GitWorktreeListResult,
  },
  "git/worktree-create": {
    request: null as unknown as GitWorktreeCreateInput,
    result: null as unknown as GitWorktreeCreateResult,
  },
  "git/worktree-remove": {
    request: null as unknown as GitWorktreeRemoveInput,
    result: null as unknown as GitWorktreeRemoveResult,
  },

  // ─── Server write-side stubs (T6c-10) ─────────────────────────────────
  // The cloned MCode UI persists user edits via these `server.*` write RPCs
  // (Settings panel "Apply"/"Reset", provider re-probe buttons, keybinding
  // editor). The transport remaps the MCode dot-strings (`server.setConfig`,
  // `server.updateSettings`, `server.refreshProviders`, `server.updateProvider`,
  // `server.upsertKeybinding`) onto these slash keys (see `MCODE_TO_SERVED` in
  // `wsTransport.ts`). The backend handlers in
  // `crates/syncode-ws/src/rpc.rs` `handle_server_*` are STUBS: they validate
  // the params shape (where required) and echo the default read-side payload
  // — no persistence (syncode has no settings/keybindings subsystem). The
  // UI's optimistic update is overwritten by the echoed default on the next
  // read, converging to "no changes persisted".
  //
  // Known gaps (documented in `handle_server_*`):
  //   - `server/set-config` echoes the default `ServerConfig` (write ignored).
  //   - `server/update-settings` echoes the default `ServerSettings` (patch
  //     NOT applied).
  //   - `server/refresh-providers` returns `{ providers: [] }` (no probe).
  //   - `server/update-provider` validates `provider` non-empty, then returns
  //     `{ providers: [] }` (no probe).
  //   - `server/upsert-keybinding` validates params is an object, then returns
  //     `{ keybindings: [], issues: [] }` (no resolver).
  // Entries appended at the END to ease parallel-merge conflict resolution.
  "server/set-config": {
    request: null as unknown as ServerSetConfigInput,
    result: null as unknown as ServerConfig,
  },
  "server/update-settings": {
    request: null as unknown as ServerUpdateSettingsInput,
    result: null as unknown as ServerSettings,
  },
  "server/refresh-providers": {
    request: null as unknown as null,
    result: null as unknown as ServerProvidersStatusResult,
  },
  "server/update-provider": {
    request: null as unknown as ServerProviderUpdateInput,
    result: null as unknown as ServerProvidersStatusResult,
  },
  "server/upsert-keybinding": {
    request: null as unknown as ServerUpsertKeybindingInput,
    result: null as unknown as ServerUpsertKeybindingResult,
  },

  // ─── LLM-backed ops (T6c-13: provider-CLI one-shot) ──────────────────
  // The cloned MCode UI's composer (`provider.compactThread`), GitPanel
  // (`git.summarizeDiff`), and thread-recap card (`server.generateThreadRecap`)
  // call these. The backend `crates/syncode-ws/src/rpc.rs` handlers build a
  // prompt (thread messages from the read model; diff text from params or
  // syncode-git), invoke a provider adapter one-shot, and surface the reply
  // text. The transport remaps the MCode dot-strings onto these slash keys
  // (see MCODE_TO_SERVED in `wsTransport.ts`). `provider.compactThread` was
  // listed in the T6c-7 block; `git/summarize-diff` and
  // `server/generate-thread-recap` are newly served. If no provider is
  // registered (or the CLI binary is missing), the result carries an `error`
  // field / empty text rather than throwing — the UI renders a fallback.
  "git/summarize-diff": {
    request: null as unknown as GitSummarizeDiffInput,
    result: null as unknown as GitSummarizeDiffResult,
  },
  "server/generate-thread-recap": {
    request: null as unknown as ServerGenerateThreadRecapInput,
    result: null as unknown as ServerGenerateThreadRecapResult,
  },

  // ─── GitHub-API ops (T6c-14: gh-CLI-backed) ──────────────────────────
  // Four RPCs the vendored MCode UI's PR-handoff surface calls. The backend
  // `crates/syncode-ws/src/rpc.rs` handlers shell out to the user's `gh` CLI
  // (authed via `gh auth login` — no OAuth/token handling in-process). The
  // transport remaps the MCode dot-strings onto these slash keys (see
  // MCODE_TO_SERVED in `wsTransport.ts`).
  //   - `git/github-repository`         — detect the GitHub repo for a local
  //     path (parses `git remote get-url origin`; enriches via `gh repo view`
  //     when available). Returns `{ repository: { nameWithOwner, url } | null }`
  //     (null = not a GitHub repo).
  //   - `git/resolve-pull-request`      — `gh pr view <n> --json …` → MCode
  //     `GitResolvePullRequestResult` shape.
  //   - `git/handoff-thread`            — `gh pr create` from a branch. The
  //     full worktree-handoff variant is STUBBED (returns a `GitHandoffThreadResult`
  //     with worktree fields null + a `message` carrying the PR URL when the
  //     PR-create sub-step succeeds).
  //   - `git/prepare-pull-request-thread` — STUBBED (`{ ok:false, reason }` —
  //     the PR→worktree checkout sequence isn't wired; compose via
  //     `resolvePullRequest` + `worktreeCreate`).
  "git/github-repository": {
    request: null as unknown as GitHubRepositoryInput,
    result: null as unknown as GitHubRepositoryResult,
  },
  "git/resolve-pull-request": {
    request: null as unknown as GitPullRequestRefInput,
    result: null as unknown as GitResolvePullRequestResult,
  },
  "git/handoff-thread": {
    request: null as unknown as GitHandoffThreadInput,
    result: null as unknown as GitHandoffThreadResult,
  },
  "git/prepare-pull-request-thread": {
    request: null as unknown as GitPreparePullRequestThreadInput,
    result: null as unknown as GitPreparePullRequestThreadResult,
  },

  // ─── Voice STT (T6c-15: graceful not-configured stubs) ───────────────
  // Three RPCs the vendored MCode UI's voice panel calls for speech-to-text.
  // The syncode-ws backend has NO STT backend (no whisper/ffmpeg CLI, no STT
  // API), so each handler is a GRACEFUL STUB: it acknowledges the call and
  // returns a typed "STT not configured" result (no crash). The transport
  // remaps the MCode dot-strings onto these slash keys (see MCODE_TO_SERVED
  // in `wsTransport.ts`). Result shapes are opaque (`Record<string, unknown>`)
  // since the MCode voice-input contracts are not in tier3; the backend's
  // not-configured payloads are documented in
  // `crates/syncode-ws/src/rpc.rs::handle_server_transcribe_voice` et al.:
  //   - `server/transcribe-voice` → `{ text: "", error: "STT not configured — install whisper + ffmpeg (or configure a STT provider) to enable voice transcription" }`
  //   - `server/voice-start`      → `{ ok: false, listening: false, reason: "STT not configured" }`
  //   - `server/voice-stop`       → `{ ok: true, listening: false }`
  "server/transcribe-voice": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },
  "server/voice-start": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },
  "server/voice-stop": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },

  // ─── Git stacked actions / detached worktree / progress (T6c-16) ──────
  // The last 3 git niche RPCs the vendored MCode UI's GitActionsControl
  // calls. The backend `crates/syncode-ws/src/rpc.rs` handlers:
  //   - `git/run-stacked-action` — REUSES `syncode_git::stacked_actions`
  //     (StackedPipeline / StackedAction) to execute a commit/push/PR
  //     pipeline. Maps MCode `GitStackedAction` (`commit | push | create_pr |
  //     commit_push | commit_push_pr`) onto a sequence of syncode stacked
  //     actions and projects per-step `ActionResult` into the MCode
  //     `GitRunStackedActionResult` shape (`{ action, branch, commit, push,
  //     pr }` with per-step status enums).
  //   - `git/create-detached-worktree` — REAL: `git worktree add --detach`
  //     (libgit2 rejects non-branch refs for the worktree HEAD, so the CLI is
  //     the canonical path). Returns MCode `GitCreateDetachedWorktreeResult`
  //     (`{ worktree: { path, ref, branch: null } }`).
  //   - `git/subscribe-action-progress` — GRACEFUL STUB: stacked actions are
  //     synchronous (no real progress push channel); returns
  //     `{ subscribed: true }`. T6c-future could stream progress via the
  //     existing `push/subscribe` bus when stacked actions become long-running.
  "git/run-stacked-action": {
    request: null as unknown as GitRunStackedActionInput,
    result: null as unknown as GitRunStackedActionResult,
  },
  "git/create-detached-worktree": {
    request: null as unknown as GitCreateDetachedWorktreeInput,
    result: null as unknown as GitCreateDetachedWorktreeResult,
  },
  "git/subscribe-action-progress": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },

  // ─── Server niche ops (T6c-17 — LAST batch; completes all RPCs) ──────
  // The final 6 unserved server RPCs the vendored MCode UI calls.
  // `server/generate-automation-intent` is REAL (LLM-backed one-shot): the
  // backend `crates/syncode-ws/src/rpc.rs::handle_server_generate_automation_intent`
  // prompts a registered provider CLI once, parses the JSON reply into the
  // MCode `ServerGenerateAutomationIntentResult` shape, and returns it. If
  // no provider is registered, the spawn fails, or the reply isn't JSON, the
  // handler returns a not-automation result with the error/raw text in
  // `reason` (never a panic). The other 5 are GRACEFUL STUBS returning
  // documented empty/ack payloads:
  //   - `server/patch-settings`              → echoes default `ServerSettings`
  //     (no persistence — mirrors `server/update-settings`).
  //   - `server/list-provider-usage`         → `[]` (no usage-tracking).
  //   - `server/get-provider-usage-snapshot` → `null` (validates `provider`).
  //   - `server/start-local-server`          → `{ ok:false, reason:"..." }`
  //     (no local-server process-mgmt subsystem).
  //   - `server/stop-local-server`           → `{ ok:true }` (no-op ack).
  // The transport remaps the MCode dot-strings onto these slash keys (see
  // MCODE_TO_SERVED in `wsTransport.ts`). After this batch: ZERO unserved
  // RPCs at the transport layer.
  "server/generate-automation-intent": {
    request: null as unknown as ServerGenerateAutomationIntentInput,
    result: null as unknown as ServerGenerateAutomationIntentResult,
  },
  "server/patch-settings": {
    request: null as unknown as ServerSettingsPatch,
    result: null as unknown as ServerSettings,
  },
  "server/list-provider-usage": {
    request: null as unknown as ServerListProviderUsageInput,
    result: null as unknown as ServerListProviderUsageResult,
  },
  "server/get-provider-usage-snapshot": {
    request: null as unknown as ServerGetProviderUsageSnapshotInput,
    result: null as unknown as ServerGetProviderUsageSnapshotResult,
  },
  "server/start-local-server": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },
  "server/stop-local-server": {
    request: null as unknown as ServerStopLocalServerInput,
    result: null as unknown as Record<string, unknown>,
  },
  // ─── T6c-29: orchestration generic RPCs ───────────────────────────
  // All 6 accept a generic params object and return a generic result object;
  // the typed shapes live on the wire but are intentionally loose here (the
  // engine-side Command/Result enums are Rust-only). The transport remaps the
  // MCode dot-strings onto these slash keys (see MCODE_TO_SERVED in
  // wsTransport.ts).
  "orchestration/dispatch-command": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },
  "orchestration/subscribe-shell": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },
  "orchestration/get-turn-diff": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },
  "orchestration/get-full-thread-diff": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },
  "orchestration/replay-events": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },
  "orchestration/repair-state": {
    request: null as unknown as Record<string, unknown>,
    result: null as unknown as Record<string, unknown>,
  },
} as const;

/** Union of all served JSON-RPC method strings. */
export type ServedRpcMethod = keyof typeof SERVED_RPC;

/**
 * The request (params) type for a served method. `null` for methods that
 * take no params (`project/list`, `auth/status`, `auth/logout`, `ping`,
 * `rpc/listMethods`).
 *
 * @example
 *   type P = ServedRpcRequest<"project/create">; // ProjectCreateParams
 *   type N = ServedRpcRequest<"project/list">;   // null
 */
export type ServedRpcRequest<M extends ServedRpcMethod> =
  (typeof SERVED_RPC)[M]["request"];

/** The result type for a served method. @example `ServedRpcResult<"turn/get">` → `TurnSummary`. */
export type ServedRpcResult<M extends ServedRpcMethod> =
  (typeof SERVED_RPC)[M]["result"];

// ════════════════════════════════════════════════════════════════════════
// ─── UNSERVED_RPC — MCode RPCs Syncode does NOT serve ───────────────────
// ════════════════════════════════════════════════════════════════════════

/**
 * The MCode RPC methods Syncode's WS backend does **not** implement. Calling
 * any of these returns a typed `MethodNotFound (-32601)` error at runtime
 * (the T5 transport layer enforces this — these strings never reach a
 * handler). Enumerated from `MISSING-SYMBOLS.md` RPC groups +
 * `CONTRACTS-BRIDGE-DESIGN.md` §8 coverage table.
 *
 * Domains (counts per `CONTRACTS-BRIDGE-DESIGN.md` §8):
 * - **git** (~22 ops) — status/diff/branch/worktree/stage/pull/PR/…
 * - **terminal** (~8 ops) — open/write/resize/close/subscribe
 * - **server meta** (~21 ops) — config/settings/providers/diagnostics/usage/voice/recap
 * - **provider discovery** (~9 ops) — skills/plugins/models/agents/commands
 * - **automation** (~9 ops) — CRUD + run + subscribe
 * - **project file ops** (~10 ops) — readFile/listDirectories/search/discoverScripts/devServers
 * - **orchestration** (~7 ops) — snapshot/diff/replay/subscribe/dispatchCommand (not in served set)
 *
 * Method strings use MCode's **dot** convention (`git.status`) — the form
 * the cloned UI references. The T5 transport re-wire maps these to either a
 * served slash-string equivalent (where one exists) or routes them to the
 * `MethodNotFound` path.
 *
 * NOTE: this is a stub list. Each entry is a plain string literal so the
 * cloned UI's `import { WS_METHODS }` from `@t3tools/contracts` resolves.
 * The full typed request/response shapes for these are Tier 3 (deferred).
 */
export const UNSERVED_RPC = [
  // ─── Git (core ops SERVED in T6c-3; stash/network/worktree/init SERVED in T6c-9) ──
  // The core GitPanel ops (status/diff/listBranches/createBranch/checkout/
  // branchDelete/stage/unstage/commit) are SERVED — see SERVED_RPC. The
  // advanced ops (stash/network/worktree/init/removeIndexLock) are NOW ALSO
  // SERVED as of T6c-9 (mapped via MCODE_TO_SERVED to `git/stash-*`,
  // `git/fetch`, `git/pull`, `git/push`, `git/worktree-*`, `git/init`,
  // `git/remove-index-lock`). ALL git.* RPCs the vendored UI calls are now
  // SERVED as of T6c-16:
  //   - `git.runStackedAction` + `git.createDetachedWorktree` +
  //     `git.subscribeActionProgress` were the last 3 unserved git RPCs;
  //     they are NOW SERVED as of T6c-16 (see SERVED_RPC).
  //   (NOTE: `git.summarizeDiff` was served in T6c-13 — LLM-backed one-shot.
  //   `git.githubRepository` + `git.resolvePullRequest` + `git.handoffThread`
  //   + `git.preparePullRequestThread` were UNSERVED until T6c-14; they are
  //   NOW SERVED via the gh-CLI-backed GitHub-API ops — see SERVED_RPC.)
  // No git.* RPCs remain unserved at the transport layer — this git block is
  // intentionally empty (kept as a marker for the domain).

  // ─── Terminal (CORE ops SERVED in T6c-5, advanced deferred) ──────────
  // The core Terminal panel ops (open/new, write, resize, close/kill, ack,
  // list, clear, restart, subscribeEvents) are SERVED — see SERVED_RPC. The
  // backend reuses `syncode-terminal::SessionManager`. The pane-layout ops
  // below (split/toggle/…) are UI-internal and never reach the backend; they
  // stay client-side. No terminal.* RPCs remain unserved at the transport
  // layer — this section is intentionally empty (kept as a marker for the
  // domain).

  // ─── Server meta — read-side SERVED in T6c-4, write-side SERVED in T6c-10 ──
  // The core read RPCs (`server.getConfig`, `server.getSettings`,
  // `server.getEnvironment`, `server.getDiagnostics`, `server.welcome`) and
  // the four `server.subscribe*` stubs are SERVED — see SERVED_RPC. The
  // write-side RPCs (`server.setConfig`, `server.updateSettings`,
  // `server.refreshProviders`, `server.updateProvider`,
  // `server.upsertKeybinding`) are ALSO SERVED as of T6c-10 (stubs that echo
  // the default read-side payload — no persistence). The advanced server
  // RPCs below remain unserved (MethodNotFound):
  "server.listProviders",
  "server.getProviderStatuses",
  "server.getProviderAuthStatus",
  "server.getUsage",
  "server.getRecap",
  // NOTE: `server.generateThreadRecap` was SERVED in T6c-13 (LLM-backed recap).
  "server.listLocalServers",
  "server.listLocalServerProcesses",
  "server.listWorktrees",
  // NOTE: `server.transcribeVoice` / `server.voiceStart` / `server.voiceStop`
  // were SERVED in T6c-15 (graceful STT not-configured stubs).
  // NOTE: the final 6 server niche RPCs were SERVED in T6c-17 —
  // `server.patchSettings`, `server.listProviderUsage`,
  // `server.getProviderUsageSnapshot`, `server.startLocalServer`,
  // `server.stopLocalServer`, and `server.generateAutomationIntent` (the
  // last one is REAL via an LLM one-shot; the rest are graceful stubs).
  // After T6c-17 the only remaining server.* unserved entries are
  // list-only/process-list RPCs the vendored UI does not actively call
  // (`listLocalServers`, `listLocalServerProcesses`, `listWorktrees`) plus
  // the legacy `getUsage`/`getRecap`/`listProviders`/`getProviderStatuses`/
  // `getProviderAuthStatus` aliases (superseded by the served equivalents).

  // ─── Provider discovery (no backend surface) — ~9 ───────────────────
  "provider.listSkills",
  "provider.readSkill",
  "provider.listPlugins",
  "provider.readPlugin",
  "provider.listCommands",
  "provider.listModels",
  "provider.listAgents",
  "provider.listOptions",
  "provider.listSkillsCatalog",

  // ─── Automation ──────────────────────────────────────────────────────
  // The core automation CRUD + run RPCs (`automation.list`, `automation.create`,
  // `automation.get`, `automation.update`, `automation.delete`, `automation.run`,
  // `automation.cancelRun`, `automation.subscribe`, `automation.unsubscribe`)
  // are NOW SERVED as of T6c-6 (mapped via MCODE_TO_SERVED to `automation/list`,
  // `automation/create`, …; subscribe/unsubscribe share a stub arm returning
  // `{subscribed:true}`). `automation.runNow`/`markRunRead`/`archiveRun` are
  // also served (markRunRead/archiveRun are no-op stubs).

  // ─── Project file ops (CRUD served, file ops not) — ~10 ─────────────
  "project.readFile",
  "project.writeFile",
  "project.listDirectories",
  "project.searchEntries",
  "project.searchLocalEntries",
  "project.discoverScripts",
  "project.runScript",
  "project.listDevServers",
  "project.startDevServer",
  "project.stopDevServer",

  // ─── Orchestration extras (beyond served thread/turn set) ────────────
  // T6c-29 made 4 of these SERVED (dispatchCommand, getFullThreadDiff,
  // getTurnDiff, replayEvents — mapped to slash forms via MCODE_TO_SERVED).
  // subscribeShell / repairState are NEW served RPCs (no MCode dot-name in
  // UNSERVED_RPC). The remaining ~3 are still unserved.
  "orchestration.subscribeEvents",
  "orchestration.repairReadModel",
  "orchestration.getLatestTurn",

  // ─── Auth extras (bootstrap/status/logout served; these are not) ─────
  "auth.createPairingCredential",
  "auth.revokePairingLink",
  "auth.listPairingLinks",
  "auth.listClientSessions",
  "auth.revokeClientSession",
  "auth.getWebSocketToken",
  "auth.getSessionState",

  // ─── Desktop / browser / editor (Tauri-shell scope, no RPC) ─────────
  "desktop.checkForUpdates",
  "desktop.applyUpdate",
  "desktop.openExternal",
  "desktop.openInEditor",
  "browser.captureScreenshot",
  "browser.listTabs",
  "filesystem.browse",
] as const;

/** Union of unserved MCode RPC method strings (typed `MethodNotFound` set). */
export type UnservedRpcMethod = (typeof UNSERVED_RPC)[number];

/**
 * Total registry union — every RPC method the cloned UI may invoke. Used by
 * the T5 transport to type the JSON-RPC client's `call(method, params)`
 * surface exhaustively.
 */
export type AnyRpcMethod = ServedRpcMethod | UnservedRpcMethod;

/**
 * Type-level predicate: did a given method resolve to a served handler?
 * Useful for the transport re-wire to narrow the `MethodNotFound` branch.
 */
export type IsServed<M extends string> = M extends ServedRpcMethod ? true : false;

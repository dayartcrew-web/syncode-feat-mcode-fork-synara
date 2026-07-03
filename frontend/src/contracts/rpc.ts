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
 * 1. **`SERVED_RPC`** — the 19 methods `syncode-ws::rpc::dispatch_method`
 *    actually handles (plus `ping` + `rpc/listMethods`), each carrying concrete
 *    `Request`/`Result` types from the ts-rs-generated DTOs in
 *    `../types/*`. Calling these succeeds at runtime (T5 wires the transport).
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

// ════════════════════════════════════════════════════════════════════════
// ─── SERVED_RPC — 23 entries (21 in dispatch + ping + rpc/listMethods) ──
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
  // ─── Git (crate exists, no RPC exposure) — ~22 ───────────────────────
  "git.status",
  "git.diff",
  "git.branchList",
  "git.branchCreate",
  "git.branchCheckout",
  "git.branchDelete",
  "git.worktreeList",
  "git.worktreeCreate",
  "git.worktreeRemove",
  "git.stage",
  "git.unstage",
  "git.stashList",
  "git.stashCreate",
  "git.stashApply",
  "git.stashDrop",
  "git.commit",
  "git.pull",
  "git.push",
  "git.fetch",
  "git.resolvePullRequest",
  "git.runStackedAction",
  "git.readWorkingTreeDiff",

  // ─── Terminal (crate exists, no RPC exposure) — ~8 ───────────────────
  "terminal.open",
  "terminal.write",
  "terminal.resize",
  "terminal.close",
  "terminal.kill",
  "terminal.list",
  "terminal.subscribe",
  "terminal.unsubscribe",

  // ─── Server meta (no backend surface) — ~21 ─────────────────────────
  "server.getConfig",
  "server.setConfig",
  "server.getSettings",
  "server.patchSettings",
  "server.listProviders",
  "server.getProviderStatuses",
  "server.getProviderAuthStatus",
  "server.getProviderUsageSnapshot",
  "server.listProviderUsage",
  "server.getDiagnostics",
  "server.getUsage",
  "server.getRecap",
  "server.startLocalServer",
  "server.stopLocalServer",
  "server.listLocalServerProcesses",
  "server.generateAutomationIntent",
  "server.voiceStart",
  "server.voiceStop",
  "server.subscribeConfig",
  "server.subscribeProviderStatuses",
  "server.subscribeSettings",

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

  // ─── Automation (crate exists, not RPC-exposed) — ~9 ────────────────
  "automation.list",
  "automation.get",
  "automation.create",
  "automation.update",
  "automation.delete",
  "automation.run",
  "automation.cancelRun",
  "automation.subscribe",
  "automation.unsubscribe",

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

  // ─── Orchestration extras (beyond served thread/turn set) — ~7 ──────
  "orchestration.dispatchCommand",
  "orchestration.getFullThreadDiff",
  "orchestration.getTurnDiff",
  "orchestration.replayEvents",
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

// FILE: wsTransport.ts
// Purpose: Browser-side JSON-RPC-over-WebSocket client for the Syncode backend.
// Layer: Web transport
// Exports: WsTransport plus stream-selection helpers used by tests.
//
// ## History — B3 (Transport re-wire)
//
// This file was rewritten from an Effect-RPC transport (`RpcClient.make(WsRpcGroup)` +
// `ManagedRuntime` + `Socket.layerWebSocket` + `RpcSerialization.layerJson`,
// subscriptions via `Stream.runForEach`) to a **plain hand-written JSON-RPC
// client** with zero `effect` runtime dependencies. The public boundary that
// `wsNativeApi.ts` consumes (`request`, `subscribe`, `getLatestPush`,
// `onStateChange`, `getState`, `dispose`) is preserved to minimize call-site
// churn; only the implementation changed.
//
// ## Wire format
//
// - Outgoing requests: `JsonRpcRequestView` shape
//   `{jsonrpc:"2.0", id, method, params}`.
// - Incoming responses: `JsonRpcResponseView` shape
//   `{jsonrpc, id, result, error}`.
// - Incoming push notifications: `{jsonrpc:"2.0", method:"push/<channel>",
//   params:{eventType, aggregateId, data, sequence?, timestamp?}}` (camelCase
//   envelope emitted by `crates/syncode-ws/src/push.rs` as of B3).
//
// ## Typing (Tier 1 keystone — T3)
//
// Two call surfaces coexist:
//   1. `rpc<M extends ServedRpcMethod>(method, params)` — **typed** via the T3
//      `SERVED_RPC` registry (`frontend/src/contracts/rpc.ts`). This is the
//      canonical surface new code should use; it is parametric over the 21
//      served slash-method strings.
//   2. `request<T>(method, params, options)` — the **untyped string** boundary
//      `wsNativeApi.ts` calls with MCode dot-strings (`server.getConfig`,
//      `orchestration.dispatchCommand`, …). Internally this routes via
//      `mapMethodToServed`: served methods get JSON-RPC dispatched; unserved
//      methods (git ops, terminal, server-meta, …) reject with a typed
//      `MethodNotFound (-32601)` error WITHOUT calling the backend — the
//      backend doesn't serve them.
//
// ## Push routing (Tier 2 — T4)
//
// Push frames arrive as `push/<channel>` notifications. The
// `push/orchestration` channel is routed through T4's
// `isOrchestrationPushEnvelope` guard (`OrchestrationPushEnvelope` /
// `DomainEventDto` discriminated union). Other channels forward `params` as
// the push `data` payload (forward-compat — typed views land Tier 3).

import {
  isObject,
  hasKey,
  safeParse,
  isOrchestrationPushEnvelope,
  type JsonRpcRequestView,
  type JsonRpcResponseView,
  type JsonRpcErrorView,
  SERVED_RPC,
  type ServedRpcMethod,
  type ServedRpcRequest,
  type ServedRpcResult,
  type WsPushChannel,
  type WsPushData,
} from "@t3tools/contracts";

import type { WsTransportState } from "./wsTransportEvents";

// ─── JSON-RPC error codes (spec + transport-local) ─────────────────────
// JSONRPC_PARSE_ERROR reserved for future server-side parse-error mapping.
const JSONRPC_METHOD_NOT_FOUND = -32601;

// ─── Push-channel wire shape ───────────────────────────────────────────
/** A JSON-RPC 2.0 notification frame (`method` set, no `id`). */
interface PushNotification {
  readonly jsonrpc: "2.0";
  readonly method: string; // e.g. "push/orchestration"
  readonly params: unknown;
}

/**
 * The push-channel name embedded in a `push/<channel>` notification's
 * `method` field. MCode keys channels by name (`serverWelcome`,
 * `terminalEvent`, …); the wire uses `push/<channel>` method strings. This
 * union mirrors the channel names the consumers subscribe to via
 * `transport.subscribe(channel, …)` from `wsNativeApi.ts`.
 */
// `PushChannel` is the contracts' channel-keyed union (`WsPushChannel`) so
// `WsPushMessage<C>.data` narrows to the typed payload per channel (e.g.
// `WsWelcomePayload` for `serverWelcome`). Previously this was `string` with
// `data: unknown`, which forced ~12 call-site casts in `wsNativeApi.ts` and
// surfaced TS2345 (`unknown` not assignable to typed payload).
type PushChannel = WsPushChannel;

/**
 * Push message surfaced to subscribers. Shape kept compatible with the
 * previous Effect-based transport (so `wsNativeApi.ts` and tests need no
 * changes): `{ type:"push", sequence, channel, data }`.
 */
export interface WsPushMessage<C extends PushChannel = PushChannel> {
  readonly type: "push";
  readonly sequence: number;
  readonly channel: C;
  readonly data: WsPushData<C>;
}

type PushListener = (message: WsPushMessage) => void;

// ─── MCode dot-method → Syncode slash-method mapping ────────────────────
//
// MCode's frontend keys RPCs as dot camelCase (`server.getConfig`,
// `orchestration.dispatchCommand`, …); Syncode's backend serves slash
// strings (`project/create`, `thread/start`, …). The contracts `SERVED_RPC`
// registry is the source of truth for the **slash** strings.
//
// `mapMethodToServed` resolves an incoming dot/other method string to the
// served slash string it maps onto, or `null` when no handler exists. Most
// MCode RPCs (git, terminal, server-meta, provider-discovery, automation,
// project file-ops) have NO Syncode handler — these return `null` and the
// transport rejects them client-side with `MethodNotFound (-32601)`,
// matching the contracts `UNSERVED_RPC` contract (T3).
const MCODE_TO_SERVED: Readonly<Record<string, ServedRpcMethod>> = {
  // MCode `ping`-style server-meta calls are not served; only the literal
  // `ping` and `rpc/listMethods` slash methods are. The served slash names
  // are passed through directly by the dispatcher below.

  // Orchestration bootstrap (T6c-2): the cloned MCode UI calls these
  // dot-strings (`wsNativeApi.ts` getShellSnapshot / getSnapshot). They map to
  // the served `shell/getSnapshot` / `snapshot/get` handlers, which compose
  // the read_store into the UI's snapshot projection shapes. Without this
  // remap the calls would fall through to the `null` branch and be
  // client-stubbed with MethodNotFound — the shell would never load real data.
  "orchestration.getShellSnapshot": "shell/getSnapshot",
  "orchestration.getSnapshot": "snapshot/get",

  // Git RPCs (T6c-3): the cloned MCode GitPanel calls `git.*` dot-strings
  // (`git.status`, `git.readWorkingTreeDiff`, `git.listBranches`, …). The
  // syncode-ws backend now serves these via `syncode-git` handlers — map
  // every MCode dot-name the UI uses to the served slash dispatch key. The
  // backend also accepts the dot-name directly (dispatch arms cover both
  // forms) so this remap is belt-and-braces robustness, not strictly
  // required — but keeping it lets the SERVED_RPC type registry govern the
  // call shape (typed params/result).
  "git.status": "git/status",
  "git.diff": "git/diff",
  "git.readWorkingTreeDiff": "git/diff",
  "git.branchList": "git/branches",
  "git.listBranches": "git/branches",
  "git.branchCreate": "git/create-branch",
  "git.createBranch": "git/create-branch",
  "git.branchCheckout": "git/checkout",
  "git.checkout": "git/checkout",
  "git.branchDelete": "git/delete-branch",
  "git.deleteBranch": "git/delete-branch",
  "git.stage": "git/add",
  "git.stageFiles": "git/add",
  "git.unstage": "git/unstage",
  "git.unstageFiles": "git/unstage",
  "git.commit": "git/commit",

  // Server config/settings/lifecycle RPCs (T6c-4): the cloned MCode UI calls
  // these `server.*` dot-strings on startup (`wsNativeApi.ts` →
  // `WS_METHODS.serverGetConfig` = "server.getConfig", …). The syncode-ws
  // backend now serves minimal valid MCode shapes for the read side (config,
  // settings, welcome, environment, diagnostics) + subscribe* stubs. Map every
  // MCode dot-name the UI uses to the served slash dispatch key so the calls
  // reach the backend instead of being client-stubbed with MethodNotFound
  // (which would leave the Settings/provider-config layer uninitialized).
  // The `tauriNativeApi.ts` already sends slash forms (`server/getConfig`),
  // so the dot→slash remap here only affects the `wsNativeApi` path; both
  // resolve because the backend dispatch accepts both forms.
  "server.getConfig": "server/getConfig",
  "server.getSettings": "server/getSettings",
  "server.welcome": "server/welcome",
  "server.getEnvironment": "server/getEnvironment",
  "server.getDiagnostics": "server/getDiagnostics",
  "server.subscribeConfig": "server/subscribeConfig",
  "server.subscribeSettings": "server/subscribeSettings",
  "server.subscribeProviderStatuses": "server/subscribeProviderStatuses",
  "server.subscribeLifecycle": "server/subscribeLifecycle",

  // Terminal PTY RPCs (T6c-5): the cloned MCode UI's Terminal panel +
  // project-script runner call these `terminal.*` dot-strings
  // (`wsNativeApi.ts` → `WS_METHODS.terminalOpen` = "terminal.open", …). The
  // syncode-ws backend now serves them via `syncode-terminal::SessionManager`
  // handlers — map every MCode dot-name the UI uses to the served slash
  // dispatch key so the calls reach the backend instead of being
  // client-stubbed with MethodNotFound (which would leave the Terminal panel
  // inert). The backend dispatch also accepts the dot-name directly (arms
  // cover both forms) so this remap is belt-and-braces robustness.
  "terminal.open": "terminal/create",
  "terminal.new": "terminal/create",
  "terminal.write": "terminal/write",
  "terminal.resize": "terminal/resize",
  "terminal.close": "terminal/close",
  "terminal.kill": "terminal/close",
  "terminal.ackOutput": "terminal/ack",
  "terminal.list": "terminal/list",
  "terminal.clear": "terminal/clear",
  "terminal.restart": "terminal/restart",
  "terminal.subscribeEvents": "terminal/subscribe-events",
  "terminal.subscribe": "terminal/subscribe-events",

  // Automation RPCs (T6c-6): the cloned MCode Automations panel calls these
  // `automation.*` dot-strings (`tauriNativeApi.ts` →
  // `callTransport("automation/list", …)`, `callTransport("automation/create")`,
  // …). The syncode-ws backend now serves them via
  // `syncode-automation::Scheduler` handlers — map every MCode dot-name the UI
  // uses to the served slash dispatch key so the calls reach the backend
  // instead of being client-stubbed with MethodNotFound (which would leave the
  // Automations panel empty). The backend dispatch also accepts the dot-name
  // directly (arms cover both forms) so this remap is belt-and-braces
  // robustness. Note: `automation.runNow` (MCode UI form) and `automation.run`
  // (alternate form) both map to `automation/run-now`.
  "automation.list": "automation/list",
  "automation.create": "automation/create",
  "automation.get": "automation/get",
  "automation.update": "automation/update",
  "automation.delete": "automation/delete",
  "automation.runNow": "automation/run-now",
  "automation.run": "automation/run-now",
  "automation.cancelRun": "automation/cancel-run",
  "automation.markRunRead": "automation/mark-run-read",
  "automation.archiveRun": "automation/archive-run",
  "automation.subscribe": "automation/subscribe",
  "automation.unsubscribe": "automation/subscribe",

  // Provider discovery RPCs (T6c-7): the cloned MCode UI's composer/agent-
  // mention/SkillsPanel/plugin layer calls these `provider.*` dot-strings
  // (`wsNativeApi.ts` → `callTransport("provider.listModels", …)`,
  // `provider.getComposerCapabilities`, …). The syncode-ws backend now serves
  // minimal valid MCode shapes (empty arrays/null descriptors, except
  // `listModels`/`listAgents` which are cheaply populated from the
  // syncode-provider `ALL_PROVIDERS` static) — map every MCode dot-name the UI
  // uses to the served slash dispatch key so the calls reach the backend
  // instead of being client-stubbed with MethodNotFound (which would leave the
  // model picker/agent-mention autocomplete/SkillsPanel empty). The backend
  // dispatch also accepts the dot-name directly (arms cover both forms) so this
  // remap is belt-and-braces robustness. Entries appended at the END to ease
  // parallel-merge conflict resolution.
  "provider.listModels": "provider/list-models",
  "provider.listSkills": "provider/list-skills",
  "provider.listSkillsCatalog": "provider/list-skills-catalog",
  "provider.listPlugins": "provider/list-plugins",
  "provider.readPlugin": "provider/read-plugin",
  "provider.listCommands": "provider/list-commands",
  "provider.listAgents": "provider/list-agents",
  "provider.getComposerCapabilities": "provider/get-composer-capabilities",
  "provider.listOptions": "provider/list-options",
  "provider.readSkill": "provider/read-skill",
  "provider.compactThread": "provider/compact-thread",

  // Profile stats RPCs (T6c-8): the cloned MCode UI's Profile page calls these
  // `stats.*` dot-strings (`wsNativeApi.ts` →
  // `callTransport("stats.getProfileStats", …)`) to render the activity
  // heatmap, provider-usage breakdown, skill-usage list, token totals, and
  // quota panel. The syncode-ws backend now serves minimal valid MCode shapes
  // (aggregates zeroed, arrays empty, optionals null — syncode has no stats
  // aggregation subsystem) — map every MCode dot-name the UI uses to the
  // served slash dispatch key so the calls reach the backend instead of being
  // client-stubbed with MethodNotFound (which would crash the Profile page).
  // The backend dispatch also accepts the dot-name directly (arms cover both
  // forms) so this remap is belt-and-braces robustness. Entries appended at
  // the END to ease parallel-merge conflict resolution.
  "stats.getProfileStats": "stats/get-profile-stats",
  "stats.getProfileTokenStats": "stats/get-profile-token-stats",

  // Git Advanced RPCs (T6c-9): the cloned MCode GitPanel's stash/worktree/
  // network/init menus call these `git.*` dot-strings beyond the core phase-3
  // surface. The syncode-ws backend now serves them (stash/fetch/init/
  // removeIndexLock via git2; pull/push via syncode-git Git2Service; worktree
  // list/create/remove via git2; stashAndCheckout is a stub `{ ok:false }`).
  // Map every MCode dot-name the UI uses to the served slash dispatch key so
  // the calls reach the backend instead of being client-stubbed with
  // MethodNotFound. The backend dispatch also accepts the dot-name directly
  // (arms cover both forms) so this remap is belt-and-braces robustness.
  // Entries appended at the END to ease parallel-merge conflict resolution.
  "git.stashList": "git/stash-list",
  "git.stashCreate": "git/stash-create",
  "git.stashApply": "git/stash-apply",
  "git.stashDrop": "git/stash-drop",
  "git.stashInfo": "git/stash-info",
  "git.stashAndCheckout": "git/stash-and-checkout",
  "git.fetch": "git/fetch",
  "git.pull": "git/pull",
  "git.push": "git/push",
  "git.init": "git/init",
  "git.removeIndexLock": "git/remove-index-lock",
  "git.worktreeList": "git/worktree-list",
  "git.listWorktrees": "git/worktree-list",
  "git.worktreeCreate": "git/worktree-create",
  "git.createWorktree": "git/worktree-create",
  "git.worktreeRemove": "git/worktree-remove",
  "git.removeWorktree": "git/worktree-remove",

  // Server write-side stub RPCs (T6c-10): the cloned MCode UI's Settings
  // panel "Apply"/"Reset", provider re-probe buttons, and keybinding editor
  // call these `server.*` dot-strings. The syncode-ws backend serves them as
  // stubs — they validate the params shape and echo the default read-side
  // payload (no persistence: syncode has no settings/keybindings subsystem).
  // Map every MCode dot-name the UI uses to the served slash dispatch key so
  // the calls reach the backend instead of being client-stubbed with
  // MethodNotFound (which would leave the Settings panel's save buttons
  // erroring). The backend dispatch also accepts the dot-name directly (arms
  // cover both forms) so this remap is belt-and-braces robustness. Entries
  // appended at the END to ease parallel-merge conflict resolution.
  "server.setConfig": "server/set-config",
  "server.updateSettings": "server/update-settings",
  "server.refreshProviders": "server/refresh-providers",
  "server.updateProvider": "server/update-provider",
  "server.upsertKeybinding": "server/upsert-keybinding",

  // LLM-backed RPCs (T6c-13): the cloned MCode UI's composer (compactThread),
  // GitPanel (summarizeDiff), and thread-recap card (generateThreadRecap) call
  // these dot-strings. The syncode-ws backend now serves them by invoking a
  // provider adapter one-shot (prompt → response). Map every MCode dot-name to
  // the served slash dispatch key so the calls reach the backend instead of
  // being client-stubbed with MethodNotFound. The backend dispatch also
  // accepts the dot-name directly (arms cover both forms) so this remap is
  // belt-and-braces robustness. `provider.compactThread` was already mapped in
  // the T6c-7 block above; `git.summarizeDiff` and `server.generateThreadRecap`
  // are newly served. Entries appended at the END to ease parallel-merge
  // conflict resolution.
  "git.summarizeDiff": "git/summarize-diff",
  "server.generateThreadRecap": "server/generate-thread-recap",
};

/**
 * Resolve a method string from the untyped `request()` boundary to a served
 * Syncode slash method (one of `SERVED_RPC`'s keys), or `null` if the
 * backend doesn't serve it.
 *
 * Resolution order:
 *   1. If `method` is already a served slash string → return it verbatim.
 *   2. If `method` is a known MCode dot-string remap → return the remap.
 *   3. Otherwise → `null` (the backend doesn't serve it).
 */
function mapMethodToServed(method: string): ServedRpcMethod | null {
  if (method in SERVED_RPC) return method as ServedRpcMethod;
  const remapped = MCODE_TO_SERVED[method];
  return remapped ?? null;
}

/**
 * Build a typed `MethodNotFound` JSON-RPC error object (-32601). Used for
 * unserved methods so the call sites get the canonical spec error shape
 * rather than a thrown exception — matches the T3 `UNSERVED_RPC` contract.
 */
function methodNotFound(method: string): JsonRpcErrorView {
  return {
    code: JSONRPC_METHOD_NOT_FOUND,
    message: `Method not found: ${method}`,
  };
}

/**
 * Convert any thrown value to an `Error`. Mirrors the prior
 * `causeToError` behavior from the Effect transport.
 */
function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

// ─── Transport URL resolution (preserved from Effect variant) ───────────

function resolveRpcUrl(rawUrl: string): string {
  const url = new URL(rawUrl);
  url.pathname = "/ws";
  return url.toString();
}

function makeSocketUrl(explicitUrl: string | null): string {
  if (explicitUrl) return resolveRpcUrl(explicitUrl);
  // `desktopBridge` is the Electron/Tauri shell bridge; in browser/dev it's
  // absent. The contracts shim types it loosely, so we narrow here.
  const bridge = (window as unknown as { desktopBridge?: { getWsUrl?: () => string } })
    .desktopBridge;
  const bridgeUrl = bridge?.getWsUrl?.();
  const envUrl = import.meta.env.VITE_WS_URL as string | undefined;
  // Port-only override for browser/dev mode: the web UI is typically served
  // by Vite on a different port (e.g. :5173) than the standalone WS backend
  // (`cargo run -p syncode-ws --bin server`, default :3000). `VITE_WS_PORT`
  // targets that backend while keeping the page hostname/protocol — without
  // needing a full `VITE_WS_URL`. Lower priority than the desktop bridge and
  // `VITE_WS_URL`; higher than the page-port fallback.
  const envPort = import.meta.env.VITE_WS_PORT as string | undefined;
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const host = window.location.hostname;
  const envPortUrl =
    envPort && envPort.length > 0
      ? `${protocol}://${host}:${envPort}`
      : `${protocol}://${host}:${window.location.port}`;
  const rawUrl =
    bridgeUrl && bridgeUrl.length > 0
      ? bridgeUrl
      : envUrl && envUrl.length > 0
        ? envUrl
        : envPortUrl;
  return resolveRpcUrl(rawUrl);
}

/**
 * Strip `null`/`undefined` values from a `thread.user-input.respond`
 * command's `answers` map before dispatch. Preserved verbatim from the
 * Effect variant — the backend rejects null answers. Only applies to the
 * orchestration dispatch path (the `request()` boundary passes the bare
 * command in `{command}`).
 */
function omitNullUserInputAnswers(input: unknown): unknown {
  if (!isObject(input)) return input;
  if (input["type"] !== "thread.user-input.respond") return input;
  const answers = input["answers"];
  if (!isObject(answers)) return input;
  const filtered: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(answers)) {
    if (value !== null && value !== undefined) filtered[key] = value;
  }
  return { ...input, answers: filtered };
}

// ─── Per-request pending entry ─────────────────────────────────────────

interface PendingRequest {
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: unknown) => void;
  readonly method: string;
}

/**
 * Browser-side JSON-RPC-over-WebSocket transport for the Syncode backend.
 *
 * One WebSocket to the `/ws` endpoint multiplexes all requests and push
 * channels. Requests are matched to responses by `id`; push notifications
 * (frames with a `method` and no `id`) are routed to per-channel listeners.
 *
 * Public surface is preserved from the prior Effect-based transport so
 * `wsNativeApi.ts` call sites need no edits — only the implementation
 * changed (Effect-RPC internals → hand-written JSON-RPC client).
 */
export class WsTransport {
  private readonly explicitUrl: string | null;
  private readonly listeners = new Map<PushChannel, Set<PushListener>>();
  private readonly stateListeners = new Set<(state: WsTransportState) => void>();
  private readonly latestPushByChannel = new Map<PushChannel, WsPushMessage>();
  private readonly pending = new Map<number, PendingRequest>();
  private sequence = 0;
  private nextId = 1;
  private state: WsTransportState = "connecting";
  private disposed = false;
  private socket: WebSocket | null = null;
  private connectPromise: Promise<void> | null = null;
  private reconnectFailures = 0;

  constructor(url?: string) {
    this.explicitUrl = url ?? null;
    void this.openSession();
  }

  // ─── Typed RPC surface (T3 SERVED_RPC keystone) ──────────────────────

  /**
   * Typed JSON-RPC call over the served method registry. Use this for new
   * code: `method` is constrained to a served slash string, `params` is
   * typed per `ServedRpcRequest<M>`, and the result is typed per
   * `ServedRpcResult<M>`.
   *
   * @example
   *   const project = await transport.rpc("project/create", { name: "x" });
   *   //    ^? ProjectSummary
   */
  async rpc<M extends ServedRpcMethod>(
    method: M,
    params: ServedRpcRequest<M>,
  ): Promise<ServedRpcResult<M>> {
    const served = (SERVED_RPC as Record<string, unknown>)[method];
    const requestParams = served === null || params === null ? {} : params;
    return (await this.sendJsonRpc(method, requestParams)) as ServedRpcResult<M>;
  }

  // ─── Untyped request boundary (consumed by wsNativeApi.ts) ───────────

  /**
   * Send an RPC request by method string. This is the boundary
   * `wsNativeApi.ts` calls with MCode dot-strings (`server.getConfig`,
   * `orchestration.dispatchCommand`, …) and the served slash strings.
   *
   * Served methods are JSON-RPC dispatched to the backend. **Unserved**
   * methods (git ops, terminal, server-meta, provider-discovery,
   * automation, project file-ops — see `UNSERVED_RPC` in
   * `@t3tools/contracts`) reject client-side with a `MethodNotFound`
   * (-32601) error WITHOUT calling the backend — the backend doesn't serve
   * them.
   *
   * The `dispatchCommand` orchestration method passes its inner `command`
   * through `omitNullUserInputAnswers` (the backend rejects null answers).
   */
  async request<T = unknown>(
    method: string,
    params?: unknown,
    _options?: { readonly timeoutMs?: number | null },
  ): Promise<T> {
    if (this.disposed) throw new Error("Transport disposed");

    // Orchestration dispatch: unwrap `{command}` and clean null answers.
    // This maps to the served `turn/start` / `thread/*` handlers in a real
    // flow; for now the bare command is forwarded as-is so any future
    // dispatch handler receives the same shape the prior transport sent.
    const isDispatch = method === "orchestration.dispatchCommand";
    const payload = isDispatch
      ? omitNullUserInputAnswers((params as { command: unknown } | undefined)?.command)
      : params;

    const served = mapMethodToServed(method);
    if (served === null) {
      // Unserved method — reject client-side, never reach the backend.
      const err = methodNotFound(method);
      throw new Error(err.message);
    }

    const requestParams =
      payload === undefined || payload === null ? {} : payload;
    return (await this.sendJsonRpc(served, requestParams)) as T;
  }

  // ─── Push subscription surface ───────────────────────────────────────

  /**
   * Subscribe to push frames on a channel. Callers pass the bare channel
   * name (e.g. `"orchestration"`, `"server.welcome"`) — the same name the
   * wire `push/<channel>` method embeds. The consumers in
   * `wsNativeApi.ts` subscribe using MCode channel-name conventions
   * (`serverWelcome`, `terminalEvent`, `orchestration.domainEvent`, …);
   * `wsNativeApi.ts` is responsible for translating those to the bare
   * channel names this method expects.
   */
  subscribe<C extends PushChannel>(
    channel: C,
    listener: (message: WsPushMessage<C>) => void,
    options?: { readonly replayLatest?: boolean },
  ): () => void {
    let channelListeners = this.listeners.get(channel);
    if (!channelListeners) {
      channelListeners = new Set<PushListener>();
      this.listeners.set(channel, channelListeners);
    }

    const wrapped: PushListener = (message) =>
      listener(message as WsPushMessage<C>);
    channelListeners.add(wrapped);

    if (options?.replayLatest) {
      const latest = this.latestPushByChannel.get(channel);
      if (latest) wrapped(latest);
    }

    return () => {
      channelListeners?.delete(wrapped);
      if (channelListeners?.size === 0) {
        this.listeners.delete(channel);
      }
    };
  }

  getLatestPush<C extends PushChannel>(channel: C): WsPushMessage<C> | null {
    const latest = this.latestPushByChannel.get(channel);
    return latest ? (latest as WsPushMessage<C>) : null;
  }

  // ─── Transport state surface ─────────────────────────────────────────

  onStateChange(
    listener: (state: WsTransportState) => void,
    options?: { readonly replayCurrent?: boolean },
  ): () => void {
    this.stateListeners.add(listener);
    if (options?.replayCurrent) listener(this.state);
    return () => {
      this.stateListeners.delete(listener);
    };
  }

  getState(): WsTransportState {
    return this.state;
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.setState("disposed");
    // Reject any in-flight requests so callers don't hang.
    const pendingError = new Error("Transport disposed");
    for (const entry of this.pending.values()) {
      try {
        entry.reject(pendingError);
      } catch {
        // Reject must never throw out of dispose.
      }
    }
    this.pending.clear();
    // Tear down the socket without waiting for the close handshake.
    const socket = this.socket;
    this.socket = null;
    if (socket) {
      socket.onopen = null;
      socket.onmessage = null;
      socket.onerror = null;
      socket.onclose = null;
      try {
        socket.close();
      } catch {
        // Close can throw if already in CLOSING/CLOSED — ignore.
      }
    }
  }

  // ─── Internals: connection lifecycle ─────────────────────────────────

  private openSession(): Promise<void> {
    if (this.connectPromise) return this.connectPromise;
    this.setState("connecting");

    this.connectPromise = new Promise<void>((resolveSession, rejectSession) => {
      let url: string;
      try {
        url = makeSocketUrl(this.explicitUrl);
      } catch (error) {
        rejectSession(toError(error));
        this.connectPromise = null;
        this.setState("closed");
        return;
      }

      let socket: WebSocket;
      try {
        socket = new WebSocket(url);
      } catch (error) {
        rejectSession(toError(error));
        this.connectPromise = null;
        this.setState("closed");
        return;
      }
      this.socket = socket;

      socket.onopen = () => {
        this.reconnectFailures = 0;
        this.setState("open");
        resolveSession();
        this.connectPromise = null;
      };

      socket.onmessage = (event: MessageEvent) => {
        this.handleIncoming(event.data);
      };

      socket.onerror = () => {
        // Errors are reflected via onclose; nothing to do here but log at
        // debug. Avoid noisy console output (matches the Effect variant's
        // silent treatment of transient socket errors during reconnect).
      };

      socket.onclose = () => {
        this.socket = null;
        // If the open never resolved, reject the session promise.
        if (this.connectPromise) {
          this.connectPromise = null;
          rejectSession(new Error("WebSocket closed before open"));
        }
        if (this.disposed) {
          this.setState("closed");
          return;
        }
        this.setState("connecting");
        void this.scheduleReconnect();
      };
    });
    return this.connectPromise;
  }

  private async scheduleReconnect(): Promise<void> {
    if (this.disposed) return;
    const delayMs = Math.min(500 * 2 ** this.reconnectFailures, 5_000);
    this.reconnectFailures += 1;
    await new Promise<void>((resolve) => window.setTimeout(resolve, delayMs));
    if (this.disposed) return;
    this.connectPromise = null;
    try {
      await this.openSession();
    } catch (error) {
      if (!this.disposed) {
        // Reconnect failed — schedule another attempt.
        void this.scheduleReconnect();
      }
    }
  }

  // ─── Internals: send / receive ───────────────────────────────────────

  /**
   * Wait for the socket to be open (resolving the connect promise), then
   * send a JSON-RPC request frame and await the matching response. Throws
   * on socket-not-open, on send failure, and on a JSON-RPC `error` reply.
   */
  private async sendJsonRpc(method: string, params: unknown): Promise<unknown> {
    if (this.disposed) throw new Error("Transport disposed");
    await this.ensureOpen();

    const id = this.nextId++;
    const frame: JsonRpcRequestView = {
      jsonrpc: "2.0",
      id: String(id),
      method,
      params: isObject(params) ? (params as Record<string, unknown>) : {},
    };

    return new Promise<unknown>((resolve, reject) => {
      this.pending.set(id, { resolve, reject, method });
      try {
        this.socket?.send(JSON.stringify(frame));
      } catch (error) {
        this.pending.delete(id);
        reject(toError(error));
      }
    });
  }

  private async ensureOpen(): Promise<void> {
    if (this.socket && this.socket.readyState === WebSocket.OPEN) return;
    if (this.connectPromise) {
      await this.connectPromise;
      return;
    }
    await this.openSession();
  }

  /**
   * Handle one incoming WebSocket frame. Parses JSON, then dispatches:
   *   - responses (`id` present, no `method`) → resolve/reject the pending
   *     request by id;
   *   - notifications (`method` present, no `id`) → route as a push frame.
   *
   * Invalid JSON or unparseable frames are dropped (matches the Effect
   * variant's tolerance of malformed frames during reconnect races).
   */
  private handleIncoming(raw: unknown): void {
    if (typeof raw !== "string") return;
    const parsed = safeParse<unknown>(raw);
    if (!isObject(parsed)) return;

    // ── Response (has `id`, no `method`) ──
    if (hasKey(parsed, "id") && !hasKey(parsed, "method")) {
      const response = parsed as unknown as JsonRpcResponseView;
      const id = response.id;
      const numericId = id === undefined ? NaN : Number(id);
      const entry = this.pending.get(numericId);
      if (!entry) return; // unknown id (duplicate / late reply) — drop
      this.pending.delete(numericId);
      if (response.error !== undefined && response.error !== null) {
        const err = response.error;
        entry.reject(
          Object.assign(new Error(err.message), { code: err.code, data: err.data }),
        );
      } else {
        entry.resolve(response.result ?? {});
      }
      return;
    }

    // ── Push notification (has `method`, no `id`) ──
    if (hasKey(parsed, "method") && !hasKey(parsed, "id")) {
      const notification = parsed as unknown as PushNotification;
      this.routePush(notification);
    }
  }

  /**
   * Route a `push/<channel>` notification to subscribers. The wire method
   * is `push/<channel>`; the bare channel name is extracted and matched
   * against registered listeners. The orchestration channel is validated
   * via T4's `isOrchestrationPushEnvelope` guard before dispatch — but
   * unrecognized shapes are still forwarded (forward-compat with future
   * server event tags not yet mirrored in `DomainEventDto`).
   */
  private routePush(notification: PushNotification): void {
    const method = notification.method;
    if (!method.startsWith("push/")) return;
    const channel = method.slice("push/".length);
    const params = notification.params;

    // The orchestration channel is validated via T4's envelope guard. A
    // guard failure is informational only — we still forward the frame so
    // consumers can react to unrecognized event tags (forward-compat).
    if (channel === "orchestration" && params !== null && params !== undefined) {
      void isOrchestrationPushEnvelope(params); // type narrowing hook (no-op drop)
    }

    this.emit(channel as PushChannel, params);
  }

  /**
   * Deliver a push payload to all listeners on `channel`, and cache it as
   * the latest for late-subscriber replay. Listener errors are swallowed
   * so one bad handler can't break the dispatch loop.
   */
  private emit(channel: PushChannel, data: unknown): void {
    const message: WsPushMessage = {
      type: "push",
      sequence: ++this.sequence,
      channel,
      data: data as WsPushData<typeof channel>,
    };
    this.latestPushByChannel.set(channel, message);
    const listeners = this.listeners.get(channel);
    if (!listeners) return;
    for (const listener of listeners) {
      try {
        listener(message);
      } catch {
        // Listener errors must not break transport push dispatch.
      }
    }
  }

  private setState(state: WsTransportState): void {
    if (this.state === state) return;
    this.state = state;
    for (const listener of this.stateListeners) {
      try {
        listener(state);
      } catch {
        // Listener errors must not break reconnect or RPC transitions.
      }
    }
  }
}

// ─── Server-lifecycle channel helpers (preserved for tests) ─────────────
//
// These mirror the prior Effect variant's exports used by the test suite
// (`isServerLifecyclePushChannel`, `shouldKeepServerLifecycleStream`). The
// MCode `WS_CHANNELS` constant is not yet in the contracts shim (T6+), so
// we hard-code the channel names the prior implementation referenced —
// these are stable wire strings from `push/server.welcome` /
// `push/server.maintenanceUpdated`.

const SERVER_LIFECYCLE_CHANNELS = new Set<string>([
  "server.welcome",
  "server.maintenanceUpdated",
]);

export function isServerLifecyclePushChannel(channel: string): boolean {
  return SERVER_LIFECYCLE_CHANNELS.has(channel);
}

export function shouldKeepServerLifecycleStream(activeChannels: ReadonlySet<string>): boolean {
  for (const channel of SERVER_LIFECYCLE_CHANNELS) {
    if (activeChannels.has(channel)) return true;
  }
  return false;
}

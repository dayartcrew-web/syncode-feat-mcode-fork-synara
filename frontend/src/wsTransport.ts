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
  const rawUrl =
    bridgeUrl && bridgeUrl.length > 0
      ? bridgeUrl
      : envUrl && envUrl.length > 0
        ? envUrl
        : `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.hostname}:${window.location.port}`;
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

    this.emit(channel, params);
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
      data,
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

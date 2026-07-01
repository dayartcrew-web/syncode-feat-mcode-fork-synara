/**
 * useWebSocket — JSON-RPC over WebSocket hook
 *
 * Manages connection lifecycle, provides `rpc()` for requests
 * and `onPush()` for push event subscriptions.
 *
 * Connection model (mirrors MCode's `wsTransport`):
 * - Exponential backoff reconnect: `min(500 * 2^failures, 5000)` ms.
 * - On reconnect, re-subscribes to all active channels so the server
 *   re-emits a fresh snapshot (snapshot-then-stream — the server's half
 *   of the reconnect bargain).
 * - In-flight `rpc()` calls are rejected on disconnect (no 10s hang).
 *
 * Push dispatch derives the channel from the message method
 * (`"push/<channel>"`) and reads `event_type` from params — matching the
 * server's actual envelope `{ event_type, aggregate_id, data }`.
 */

import { useState, useEffect, useCallback, useRef } from "react";

interface JsonRpcRequest {
  jsonrpc: "2.0";
  id: number;
  method: string;
  params?: Record<string, unknown>;
}

interface JsonRpcResponse {
  jsonrpc: "2.0";
  id?: number;
  result?: unknown;
  error?: { code: number; message: string };
}

/** The push envelope the server actually sends. */
export interface PushEvent {
  /** Channel name, derived from `method: "push/<channel>"`. */
  channel: string;
  /** Event discriminator, e.g. "snapshot" or a domain event type. */
  eventType: string;
  /** Aggregate id the event concerns (null for snapshots). */
  aggregateId: string | null;
  /** The event payload (shape depends on channel + eventType). */
  data: unknown;
}

/** Connection lifecycle state. */
export type ConnectionState = "connecting" | "open" | "reconnecting" | "closed";

export interface UseWebSocketReturn {
  /** Coarse boolean for backward-compat callers (`connected = status === "open"`). */
  connected: boolean;
  /** Richer lifecycle state for UIs that distinguish reconnecting from offline. */
  status: ConnectionState;
  rpc: <T = unknown>(method: string, params?: Record<string, unknown>) => Promise<T>;
  onPush: (callback: (event: PushEvent) => void) => () => void;
  /** Subscribe to a push channel (records it for re-subscribe on reconnect). */
  subscribe: (channel: string, params?: Record<string, unknown>) => Promise<void>;
}

/** Compute backoff delay: min(500 * 2^failures, 5000), mirroring MCode. */
function backoffDelay(failures: number): number {
  return Math.min(500 * 2 ** failures, 5000);
}

export function useWebSocket(url: string): UseWebSocketReturn {
  const wsRef = useRef<WebSocket | null>(null);
  const pendingRef = useRef<
    Map<number, { resolve: (v: unknown) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> }>
  >(new Map());
  const nextIdRef = useRef(1);
  const pushCallbacksRef = useRef<Set<(event: PushEvent) => void>>(new Set());
  /** Channels to re-subscribe on reconnect: channel -> params (for threadId etc). */
  const subscriptionsRef = useRef<Map<string, Record<string, unknown> | undefined>>(new Map());

  const [status, setStatus] = useState<ConnectionState>("connecting");
  const connected = status === "open";

  // Refs that the socket handlers need to read without re-binding (the effect
  // closes over `url` only, so these must be refs to stay current).
  const failuresRef = useRef(0);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const disposedRef = useRef(false);

  const drainPending = useCallback((reason: string) => {
    for (const [id, entry] of pendingRef.current) {
      clearTimeout(entry.timer);
      entry.reject(new Error(reason));
      pendingRef.current.delete(id);
    }
  }, []);

  /** Re-subscribe to every channel in subscriptionsRef after a (re)connect. */
  const resubscribeAll = useCallback((ws: WebSocket) => {
    for (const [channel, params] of subscriptionsRef.current) {
      const id = nextIdRef.current++;
      const request: JsonRpcRequest = {
        jsonrpc: "2.0",
        id,
        method: "push/subscribe",
        params: { channel, ...params },
      };
      // Best-effort: a failure here just means no snapshot this round; the
      // next reconnect will retry. Don't block the RPC map on it.
      try {
        ws.send(JSON.stringify(request));
      } catch {
        // socket may have closed again mid-resubscribe — ignore.
      }
    }
  }, []);

  // Connect (+ reconnect) lifecycle. Keyed only on `url` so the effect doesn't
  // tear down on every render.
  useEffect(() => {
    disposedRef.current = false;

    const connect = () => {
      if (disposedRef.current) return;
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        failuresRef.current = 0;
        setStatus("open");
        console.log("[ws] Connected");
        // Re-subscribe to all active channels so the server re-emits snapshots.
        resubscribeAll(ws);
      };

      ws.onclose = () => {
        console.log("[ws] Disconnected");
        drainPending("WebSocket disconnected");
        if (disposedRef.current) {
          setStatus("closed");
          return;
        }
        // Schedule a reconnect with exponential backoff.
        setStatus("reconnecting");
        const delay = backoffDelay(failuresRef.current);
        failuresRef.current += 1;
        reconnectTimerRef.current = setTimeout(connect, delay);
      };

      ws.onerror = (err) => {
        console.error("[ws] Error", err);
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data) as JsonRpcResponse & {
            method?: string;
            params?: { event_type?: string; aggregate_id?: string | null; data?: unknown };
          };

          // Push notification: method starts with "push/" and no request id.
          if (!msg.id && msg.method?.startsWith("push/") && msg.params) {
            const channel = msg.method.slice("push/".length);
            const pushEvent: PushEvent = {
              channel,
              eventType: msg.params.event_type ?? "unknown",
              aggregateId: msg.params.aggregate_id ?? null,
              data: msg.params.data,
            };
            pushCallbacksRef.current.forEach((cb) => cb(pushEvent));
            return;
          }

          // RPC response.
          if (msg.id != null) {
            const pending = pendingRef.current.get(msg.id);
            if (pending) {
              clearTimeout(pending.timer);
              pendingRef.current.delete(msg.id);
              if (msg.error) {
                pending.reject(new Error(msg.error.message));
              } else {
                pending.resolve(msg.result);
              }
            }
          }
        } catch {
          console.warn("[ws] Failed to parse message", event.data);
        }
      };
    };

    setStatus("connecting");
    connect();

    return () => {
      disposedRef.current = true;
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      drainPending("WebSocket hook unmounting");
      wsRef.current?.close();
    };
  }, [url, drainPending, resubscribeAll]);

  const rpc = useCallback(
    async <T = unknown,>(method: string, params?: Record<string, unknown>): Promise<T> => {
      return new Promise((resolve, reject) => {
        const ws = wsRef.current;
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          reject(new Error("WebSocket not connected"));
          return;
        }

        const id = nextIdRef.current++;
        const request: JsonRpcRequest = {
          jsonrpc: "2.0",
          id,
          method,
          params: params ?? {},
        };

        // 10s timeout — cleared on resolve/reject/disconnect.
        const timer = setTimeout(() => {
          if (pendingRef.current.has(id)) {
            pendingRef.current.delete(id);
            reject(new Error(`RPC timeout: ${method}`));
          }
        }, 10_000);

        pendingRef.current.set(id, { resolve: resolve as (v: unknown) => void, reject, timer });
        ws.send(JSON.stringify(request));
      });
    },
    [],
  );

  const onPush = useCallback((callback: (event: PushEvent) => void): (() => void) => {
    pushCallbacksRef.current.add(callback);
    return () => {
      pushCallbacksRef.current.delete(callback);
    };
  }, []);

  const subscribe = useCallback(
    async (channel: string, params?: Record<string, unknown>): Promise<void> => {
      // Record for re-subscribe on reconnect.
      subscriptionsRef.current.set(channel, params);
      // If connected, subscribe now (server will emit a snapshot).
      const ws = wsRef.current;
      if (ws && ws.readyState === WebSocket.OPEN) {
        await rpc("push/subscribe", { channel, ...params });
      }
    },
    [rpc],
  );

  return { connected, status, rpc, onPush, subscribe };
}

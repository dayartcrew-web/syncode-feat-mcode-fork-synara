/**
 * useWebSocket — JSON-RPC over WebSocket hook
 *
 * Manages connection lifecycle, provides `rpc()` for requests
 * and `onPush()` for push event subscriptions.
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

interface PushParams {
  channel: string;
  event: string;
  data: unknown;
  timestamp: string;
}

export interface UseWebSocketReturn {
  connected: boolean;
  rpc: <T = unknown>(method: string, params?: Record<string, unknown>) => Promise<T>;
  onPush: (callback: (params: PushParams) => void) => () => void;
}

export function useWebSocket(url: string): UseWebSocketReturn {
  const wsRef = useRef<WebSocket | null>(null);
  const pendingRef = useRef<Map<number, { resolve: (v: unknown) => void; reject: (e: Error) => void }>>(new Map());
  const nextIdRef = useRef(1);
  const pushCallbacksRef = useRef<Set<(params: PushParams) => void>>(new Set());
  const [connected, setConnected] = useState(false);

  // Connect on mount
  useEffect(() => {
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      console.log("[ws] Connected");
    };

    ws.onclose = () => {
      setConnected(false);
      console.log("[ws] Disconnected");
    };

    ws.onerror = (err) => {
      console.error("[ws] Error", err);
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data) as JsonRpcResponse & {
          method?: string;
          params?: PushParams;
        };

        // Push notification (no id)
        if (!msg.id && msg.method?.startsWith("push/") && msg.params) {
          pushCallbacksRef.current.forEach((cb) => cb(msg.params!));
          return;
        }

        // RPC response
        if (msg.id != null) {
          const pending = pendingRef.current.get(msg.id);
          if (pending) {
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

    return () => {
      ws.close();
    };
  }, [url]);

  const rpc = useCallback(async <T = unknown>(method: string, params?: Record<string, unknown>): Promise<T> => {
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

      pendingRef.current.set(id, { resolve: resolve as (v: unknown) => void, reject });
      ws.send(JSON.stringify(request));

      // Timeout after 10s
      setTimeout(() => {
        if (pendingRef.current.has(id)) {
          pendingRef.current.delete(id);
          reject(new Error(`RPC timeout: ${method}`));
        }
      }, 10_000);
    });
  }, []);

  const onPush = useCallback((callback: (params: PushParams) => void): (() => void) => {
    pushCallbacksRef.current.add(callback);
    return () => {
      pushCallbacksRef.current.delete(callback);
    };
  }, []);

  return { connected, rpc, onPush };
}

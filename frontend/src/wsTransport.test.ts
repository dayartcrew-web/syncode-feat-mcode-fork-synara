// FILE: wsTransport.test.ts
// Purpose: Verifies browser WebSocket construction around the Effect RPC transport.
// Layer: Web transport tests
// Depends on: the global WebSocket constructor shim and desktop bridge URL contract.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { WS_CHANNELS } from "@t3tools/contracts";

import { shouldKeepServerLifecycleStream, WsTransport } from "./wsTransport";

type WsEventType = "open" | "message" | "close" | "error";
type WsListener = (event?: { data?: unknown }) => void;

const sockets: MockWebSocket[] = [];

/** Yield to the microtask queue a few times so async send paths flush. */
function flushMicrotasks(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState = MockWebSocket.CONNECTING;
  readonly sent: unknown[] = [];
  private readonly listeners = new Map<WsEventType, Set<WsListener>>();
  // Direct-property handlers — the real WebSocket API WsTransport uses
  // (`socket.onopen`, `socket.onmessage`, …). Kept in addition to the
  // addEventListener surface so existing tests are unaffected.
  onopen: WsListener | null = null;
  onmessage: WsListener | null = null;
  onclose: WsListener | null = null;
  onerror: WsListener | null = null;

  constructor(readonly url: string) {
    sockets.push(this);
  }

  addEventListener(type: WsEventType, listener: WsListener) {
    const listeners = this.listeners.get(type) ?? new Set<WsListener>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: WsEventType, listener: WsListener) {
    this.listeners.get(type)?.delete(listener);
  }

  send(data: unknown) {
    this.sent.push(data);
  }

  close() {
    this.readyState = MockWebSocket.CLOSED;
    this.emit("close");
  }

  /** Test-only: deliver a frame to listeners registered for `type`. */
  emit(type: WsEventType, event?: { data?: unknown }) {
    const listeners = this.listeners.get(type);
    if (listeners) {
      for (const listener of listeners) {
        listener(event);
      }
    }
    // Also fire the direct-property handler (WsTransport uses these).
    const handler = this[`on${type}` as "onopen" | "onmessage" | "onclose" | "onerror"];
    handler?.(event);
  }
}

const originalWebSocket = globalThis.WebSocket;

beforeEach(() => {
  sockets.length = 0;
  vi.stubEnv("VITE_WS_URL", "");

  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: {
      location: { protocol: "http:", hostname: "localhost", port: "3020" },
      desktopBridge: undefined,
    },
  });

  globalThis.WebSocket = MockWebSocket as unknown as typeof WebSocket;
});

afterEach(() => {
  globalThis.WebSocket = originalWebSocket;
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("WsTransport", () => {
  it("keeps the shared lifecycle stream while either lifecycle channel is active", () => {
    expect(shouldKeepServerLifecycleStream(new Set([WS_CHANNELS.serverWelcome]))).toBe(true);
    expect(shouldKeepServerLifecycleStream(new Set([WS_CHANNELS.serverMaintenanceUpdated]))).toBe(
      true,
    );
    expect(
      shouldKeepServerLifecycleStream(
        new Set([WS_CHANNELS.serverWelcome, WS_CHANNELS.serverMaintenanceUpdated]),
      ),
    ).toBe(true);
    expect(shouldKeepServerLifecycleStream(new Set([WS_CHANNELS.serverConfigUpdated]))).toBe(false);
  });

  it("normalizes explicit websocket URLs to the RPC endpoint", () => {
    const transport = new WsTransport("ws://localhost:3020");

    expect(sockets[0]?.url).toBe("ws://localhost:3020/ws");
    expect(transport.getState()).toBe("connecting");

    transport.dispose();
  });

  it("uses the desktop bridge URL before falling back to the browser location", () => {
    const getWsUrl = vi.fn().mockReturnValue("ws://127.0.0.1:53036/?token=old");
    Object.defineProperty(globalThis, "window", {
      configurable: true,
      value: {
        location: { protocol: "http:", hostname: "localhost", port: "3020" },
        desktopBridge: { getWsUrl },
      },
    });

    const transport = new WsTransport();

    expect(getWsUrl).toHaveBeenCalledTimes(1);
    expect(sockets[0]?.url).toBe("ws://127.0.0.1:53036/ws?token=old");

    transport.dispose();
  });

  it("falls back to the current browser host when no desktop bridge URL exists", () => {
    const transport = new WsTransport();

    expect(sockets[0]?.url).toBe("ws://localhost:3020/ws");

    transport.dispose();
  });

  it("targets the standalone WS backend port via VITE_WS_PORT in browser mode", () => {
    // Browser dev: page served by Vite on :5173, standalone WS backend on :3000.
    // VITE_WS_PORT must override the page port while keeping the hostname.
    Object.defineProperty(globalThis, "window", {
      configurable: true,
      value: {
        location: { protocol: "http:", hostname: "localhost", port: "5173" },
        desktopBridge: undefined,
      },
    });
    vi.stubEnv("VITE_WS_PORT", "3000");

    const transport = new WsTransport();

    expect(sockets[0]?.url).toBe("ws://localhost:3000/ws");

    transport.dispose();
  });

  it("prefers VITE_WS_URL over VITE_WS_PORT when both are set", () => {
    // A full-URL override wins over the port-only override.
    vi.stubEnv("VITE_WS_URL", "ws://staging.example:9000");
    vi.stubEnv("VITE_WS_PORT", "3000");

    const transport = new WsTransport();

    expect(sockets[0]?.url).toBe("ws://staging.example:9000/ws");

    transport.dispose();
  });

  it("notifies state listeners and replays the current state on demand", () => {
    const transport = new WsTransport();
    const listener = vi.fn();

    const unsubscribe = transport.onStateChange(listener, { replayCurrent: true });

    expect(listener).toHaveBeenCalledWith("connecting");

    listener.mockClear();
    transport.dispose();

    expect(listener).toHaveBeenCalledWith("disposed");

    listener.mockClear();
    unsubscribe();
    transport.dispose();

    expect(listener).not.toHaveBeenCalled();
  });

  // ── T6c-2: orchestration bootstrap remap ───────────────────────────
  // The cloned MCode UI calls `orchestration.getShellSnapshot` /
  // `orchestration.getSnapshot` (dot-strings). The transport must remap these
  // onto the served slash methods (`shell/getSnapshot`, `snapshot/get`) and
  // SEND them to the backend, rather than client-stubbing them with
  // MethodNotFound. Without the remap the shell would show "Loading projects…"
  // forever (the bootstrap call never reached the backend).
  it("remaps orchestration.getShellSnapshot to the served shell/getSnapshot method", async () => {
    const transport = new WsTransport("ws://localhost:3020");
    const socket = sockets[0]!;
    // Open the socket so sendJsonRpc proceeds past ensureOpen().
    socket.readyState = MockWebSocket.OPEN;
    socket.emit("open");

    // Issue the bootstrap call the UI makes (wsNativeApi.ts).
    const pending = transport.request("orchestration.getShellSnapshot");

    // Wait for the request to flush to the wire (ensureOpen awaits a microtask).
    await flushMicrotasks();

    // The wire frame must carry the served slash method, not the dot-string.
    expect(socket.sent).toHaveLength(1);
    const sentFrame = JSON.parse(socket.sent[0] as string);
    expect(sentFrame.method).toBe("shell/getSnapshot");

    // Resolve so the pending promise doesn't leak across tests.
    const responseFrame = {
      jsonrpc: "2.0",
      id: sentFrame.id,
      result: { snapshotSequence: 0, projects: [], threads: [], updatedAt: "2026-01-01T00:00:00Z" },
    };
    socket.emit("message", { data: JSON.stringify(responseFrame) });
    await expect(pending).resolves.toEqual({
      snapshotSequence: 0,
      projects: [],
      threads: [],
      updatedAt: "2026-01-01T00:00:00Z",
    });

    transport.dispose();
  });

  it("remaps orchestration.getSnapshot to the served snapshot/get method", async () => {
    const transport = new WsTransport("ws://localhost:3020");
    const socket = sockets[0]!;
    socket.readyState = MockWebSocket.OPEN;
    socket.emit("open");

    const pending = transport.request("orchestration.getSnapshot");

    await flushMicrotasks();

    expect(socket.sent).toHaveLength(1);
    const sentFrame = JSON.parse(socket.sent[0] as string);
    expect(sentFrame.method).toBe("snapshot/get");

    const responseFrame = {
      jsonrpc: "2.0",
      id: sentFrame.id,
      result: { snapshotSequence: 0, projects: [], threads: [], updatedAt: "2026-01-01T00:00:00Z" },
    };
    socket.emit("message", { data: JSON.stringify(responseFrame) });
    await expect(pending).resolves.toEqual({
      snapshotSequence: 0,
      projects: [],
      threads: [],
      updatedAt: "2026-01-01T00:00:00Z",
    });

    transport.dispose();
  });

  it("rejects genuinely unserved orchestration methods client-side", async () => {
    // Sanity: the remap is specific. An orchestration method the backend does
    // NOT serve must still reject with MethodNotFound without reaching the wire.
    const transport = new WsTransport("ws://localhost:3020");
    const socket = sockets[0]!;
    socket.readyState = MockWebSocket.OPEN;
    socket.emit("open");

    await expect(transport.request("orchestration.repairReadModel")).rejects.toThrow(
      /Method not found/,
    );
    expect(socket.sent).toHaveLength(0);

    transport.dispose();
  });
});

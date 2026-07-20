/**
 * NativeApi factory — B4 / "T6" shell swap.
 *
 * Resolves the active `NativeApi` in priority order:
 *   1. A preloaded desktop bridge on `window.nativeApi` (set by the Tauri
 *      entrypoint or an Electron preload script if present).
 *   2. The Tauri-backed implementation when running inside Tauri
 *      (`isTauri()`). Boot-critical shell surfaces are wired; WS-routed
 *      surfaces (server/provider/orchestration/automation/stats/projects/
 *      filesystem) AND live push delivery delegate to a real `WsTransport`
 *      pointing at the desktop's embedded WS server
 *      (`crates/syncode-tauri/src/ws_setup.rs`, default 127.0.0.1:33101).
 *   3. The browser/WebSocket implementation (`createWsNativeApi`) as the final
 *      fallback for browser-mode dev/test.
 *
 * The Electron-specific path (MCode's `wsNativeApi.ts` reading
 * `window.desktopBridge`) is replaced: in Tauri, `window.nativeApi` is set by
 * `createTauriNativeApi()`; in browser, `createWsNativeApi()` builds the
 * full transport-backed facade.
 */

import { invoke, isTauri } from "@tauri-apps/api/core";

import type { NativeApi } from "@t3tools/contracts";

import {
  createTauriDesktopBridge,
  createTauriNativeApi,
  type TauriDesktopBridge,
  type TransportDispatcher,
} from "./tauriNativeApi";
import { createWsNativeApi } from "./wsNativeApi";
import { WsTransport } from "./wsTransport";

// Desktop WS server default bind — mirrors
// `crates/syncode-tauri/src/ws_setup.rs::DEFAULT_PORT` (33101) so the eager
// `WsTransport.openSession()` has a usable URL even before the
// `get_ws_endpoint` invoke resolves (typically <1 frame; this is just a
// safety net for the boot race).
const DESKTOP_WS_DEFAULT_URL = "ws://127.0.0.1:33101";

let cachedDesktopApi: NativeApi | undefined;
let desktopTransport: WsTransport | undefined;

/**
 * Install `window.desktopBridge` (if absent) with a `getWsUrl` that returns
 * the desktop's embedded WS server endpoint.
 *
 * The endpoint is discovered via the `get_ws_endpoint` Tauri command (single
 * source of truth — respects `SYNCODE_WS_PORT` overrides on the Rust side).
 * The invoke resolves async into a sync cache; `getWsUrl` returns the cache
 * or the default URL until the invoke lands, so the transport's eager connect
 * always has a target. Only `getWsUrl` is required by `wsTransport.ts`; the
 * rest of the bridge (dialogs, theme, window controls) is wired for parity
 * with the Electron shell contract.
 */
function installDesktopBridgeIfNeeded(): void {
  if (typeof window === "undefined") return;
  const existing = (window as unknown as { desktopBridge?: { getWsUrl?: () => string | null } })
    .desktopBridge;
  if (existing?.getWsUrl) return;

  let cachedEndpoint: string | null = null;
  void invoke<{ endpoint: string; available: boolean }>("get_ws_endpoint")
    .then((info) => {
      if (info?.available && typeof info.endpoint === "string" && info.endpoint.length > 0) {
        cachedEndpoint = info.endpoint;
      }
    })
    .catch(() => {
      // Server may still be booting or the command unavailable; the default
      // URL fallback in getWsUrl keeps the transport retrying.
    });

  const bridge: TauriDesktopBridge = createTauriDesktopBridge(
    () => cachedEndpoint ?? DESKTOP_WS_DEFAULT_URL,
  );
  (window as unknown as { desktopBridge: TauriDesktopBridge }).desktopBridge = bridge;
}

/**
 * Adapt a `WsTransport` instance to the `TransportDispatcher` shape
 * `createTauriNativeApi` consumes. `WsTransport` names its RPC method
 * `request` and types `subscribe` by `PushChannel`; the dispatcher uses the
 * looser `call` / string-channel signatures so `tauriNativeApi` stays
 * decoupled from the transport class. The wrappers are trivial pass-throughs.
 */
function wrapWsTransportAsDispatcher(transport: WsTransport): TransportDispatcher {
  return {
    call: <R>(method: string, params?: unknown) => transport.request<R>(method, params),
    subscribe: (channel, listener) =>
      transport.subscribe(
        channel as Parameters<typeof transport.subscribe>[0],
        (message) => listener({ data: message.data }),
      ),
  };
}

export function readNativeApi(): NativeApi | undefined {
  if (typeof window === "undefined") return undefined;
  if (cachedDesktopApi && window.nativeApi === cachedDesktopApi) return cachedDesktopApi;

  // 1. Preloaded bridge (Tauri entrypoint or Electron preload).
  if (window.nativeApi) {
    cachedDesktopApi = window.nativeApi;
    return cachedDesktopApi;
  }

  // 2. Tauri webview: build the Tauri-backed impl over a real WsTransport.
  //    The desktop shell boots its own axum WS server (ws_setup.rs); the
  //    frontend reaches it via the SAME WsTransport the browser uses. The
  //    desktop bridge exposes the endpoint via getWsUrl (read first by
  //    wsTransport.makeSocketUrl). The transport drives both RPC dispatch
  //    (server/provider/orchestration/…) and live push delivery (demux
  //    registered inside createTauriNativeApi mirrors wsNativeApi).
  if (isTauri()) {
    installDesktopBridgeIfNeeded();
    if (!desktopTransport) {
      desktopTransport = new WsTransport();
    }
    const transport = wrapWsTransportAsDispatcher(desktopTransport);
    const tauriApi = createTauriNativeApi(transport);
    cachedDesktopApi = tauriApi;
    return tauriApi;
  }

  // 3. Browser mode (dev/test): full WS transport facade.
  return createWsNativeApi();
}

export function ensureNativeApi(): NativeApi {
  const api = readNativeApi();
  if (!api) {
    throw new Error("Native API not found");
  }
  return api;
}

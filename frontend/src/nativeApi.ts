/**
 * NativeApi factory — B4 / "T6" shell swap.
 *
 * Resolves the active `NativeApi` in priority order:
 *   1. A preloaded desktop bridge on `window.nativeApi` (set by the Tauri
 *      entrypoint or an Electron preload script if present).
 *   2. The Tauri-backed implementation when running inside Tauri
 *      (`isTauri()`). Boot-critical shell surfaces are wired; WS-routed
 *      surfaces delegate to the JSON-RPC transport via `createWsNativeApi`.
 *   3. The browser/WebSocket implementation (`createWsNativeApi`) as the final
 *      fallback for browser-mode dev/test.
 *
 * The Electron-specific path (MCode's `wsNativeApi.ts` reading
 * `window.desktopBridge`) is replaced: in Tauri, `window.nativeApi` is set by
 * `createTauriNativeApi()`; in browser, `createWsNativeApi()` builds the
 * full transport-backed facade.
 */

import { isTauri } from "@tauri-apps/api/core";

import type { NativeApi } from "@t3tools/contracts";

import { createTauriNativeApi } from "./tauriNativeApi";
import { createWsNativeApi } from "./wsNativeApi";

let cachedDesktopApi: NativeApi | undefined;

export function readNativeApi(): NativeApi | undefined {
  if (typeof window === "undefined") return undefined;
  if (cachedDesktopApi && window.nativeApi === cachedDesktopApi) return cachedDesktopApi;

  // 1. Preloaded bridge (Tauri entrypoint or Electron preload).
  if (window.nativeApi) {
    cachedDesktopApi = window.nativeApi;
    return cachedDesktopApi;
  }

  // 2. Tauri webview: build the Tauri-backed impl. The WS-routed surfaces
  //    (server/provider/orchestration/…) delegate to createWsNativeApi's
  //    transport, which the Tauri shell wires in parallel. Until that wiring
  //    lands, those surfaces reject with UnsupportedError("ws-transport").
  if (isTauri()) {
    const tauriApi = createTauriNativeApi(null);
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

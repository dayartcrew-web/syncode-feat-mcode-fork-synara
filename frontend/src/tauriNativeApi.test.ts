// FILE: tauriNativeApi.test.ts
// Purpose: Verifies the Tauri-backed NativeApi adapter routes user confirmations
// through the DOM `showConfirmDialogFallback` rather than the native
// `window.confirm`, which the Tauri v2 webview suppresses (silently returning
// false), aborting every delete/archive/checkpoint-revert flow.
// Layer: Tauri transport adapter tests
// Depends on: confirmDialogFallback mock + a minimal TransportDispatcher.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { TransportDispatcher } from "./tauriNativeApi";

const showConfirmDialogFallbackMock = vi.fn(() => Promise.resolve(true));

vi.mock("./confirmDialogFallback", () => ({
  showConfirmDialogFallback: showConfirmDialogFallbackMock,
}));

// Stub a `window` whose `confirm` is a spy. The Tauri implementation must NOT
// reach this; if it regresses to `window.confirm`, the spy stays uncalled and
// the assertion below fails loudly. In the real Tauri webview this call is
// suppressed (returns false) and every confirmation-gated op silently no-ops —
// the root cause of "delete threads/chats does nothing in the desktop app".
const windowConfirmSpy = vi.fn(() => true);

function makeTransport(): TransportDispatcher {
  return {
    // `vi.fn` erases the generic `<R>` in its declared type, so cast to the
    // dispatcher's `call` signature — the runtime behavior (resolve undefined)
    // is compatible, only TS cannot prove the generic round-trip.
    call: vi.fn(
      <R>(_method: string, _params?: unknown): Promise<R> =>
        Promise.resolve(undefined as unknown as R),
    ) as unknown as TransportDispatcher["call"],
    subscribe: vi.fn(() => () => undefined),
  };
}

beforeEach(() => {
  vi.resetModules();
  showConfirmDialogFallbackMock.mockReset();
  showConfirmDialogFallbackMock.mockResolvedValue(true);
  windowConfirmSpy.mockClear();
  vi.stubGlobal("window", { confirm: windowConfirmSpy });
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("createTauriNativeApi — dialogs.confirm", () => {
  it("routes confirm through showConfirmDialogFallback, not window.confirm", async () => {
    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    const result = await api.dialogs.confirm("Delete thread?");

    expect(showConfirmDialogFallbackMock).toHaveBeenCalledTimes(1);
    expect(showConfirmDialogFallbackMock).toHaveBeenCalledWith("Delete thread?");
    expect(windowConfirmSpy).not.toHaveBeenCalled();
    expect(result).toBe(true);
  });

  it("propagates the fallback's resolved value (false → caller aborts)", async () => {
    showConfirmDialogFallbackMock.mockResolvedValue(false);

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    const result = await api.dialogs.confirm("Delete 3 threads?");

    expect(result).toBe(false);
  });
});

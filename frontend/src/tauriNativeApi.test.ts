// FILE: tauriNativeApi.test.ts
// Purpose: Verifies the Tauri-backed NativeApi adapter routes user confirmations
// through the DOM `showConfirmDialogFallback` rather than the native
// `window.confirm`, which the Tauri v2 webview suppresses (silently returning
// false), aborting every delete/archive/checkpoint-revert flow. Also covers
// the six real `invoke<T>()` calls (`terminal_create_session`, `terminal_write`,
// `terminal_ack`, `terminal_resize`, `terminal_destroy_session`,
// `shell_open_editor`) via `@tauri-apps/api/mocks` mockIPC — the load-bearing
// IPC surface that, if regressed (renamed command, dropped param), breaks the
// desktop shell silently.
// Layer: Tauri transport adapter tests
// Depends on: confirmDialogFallback mock + a minimal TransportDispatcher +
// `@tauri-apps/api/mocks` for IPC interception.

import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
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
// `open` spy for the shell.openInEditor fallback path — the production code
// falls through to `openExternalImpl` → `window.open(target, "_blank", ...)`
// when the backend has no `shell_open_editor` command registered.
const windowOpenSpy = vi.fn(() => null);

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
  windowOpenSpy.mockClear();
  vi.stubGlobal("window", {
    confirm: windowConfirmSpy,
    open: windowOpenSpy,
  });
});

afterEach(() => {
  clearMocks();
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

// ─── terminal IPC surface ───────────────────────────────────────────────
//
// The terminal surface forwards every op to a syncode-tauri `terminal_*`
// command via `invoke()`. These tests pin the wire contract: command name,
// parameter shape, and return-value pass-through. A regression here (renamed
// command, dropped param, swapped order) is a silent break — the desktop
// terminal panel stops responding and the only signal is "nothing happens".
describe("createTauriNativeApi — terminal IPC", () => {
  it("terminal.open invokes terminal_create_session with shaped params and returns the snapshot", async () => {
    let capturedCmd: string | undefined;
    let capturedArgs: unknown;
    mockIPC((cmd, args) => {
      capturedCmd = cmd;
      capturedArgs = args;
      return {
        threadId: "thread-1",
        terminalId: "term-1",
        cwd: "/proj",
        status: "running",
        pid: 4242,
        history: "",
        exitCode: null,
        exitSignal: null,
        updatedAt: "2026-08-14T00:00:00Z",
      };
    });

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    const snapshot = await api.terminal.open({
      command: "bash",
      args: ["-l"],
      cwd: "/proj",
      cols: 120,
      rows: 40,
    });

    expect(capturedCmd).toBe("terminal_create_session");
    expect(capturedArgs).toEqual({
      command: "bash",
      args: ["-l"],
      workingDir: "/proj",
      cols: 120,
      rows: 40,
    });
    expect(snapshot).toMatchObject({
      threadId: "thread-1",
      terminalId: "term-1",
      pid: 4242,
      status: "running",
    });
  });

  it("terminal.open applies defaults when input fields are absent (cols/rows)", async () => {
    let capturedArgs: unknown;
    mockIPC((_cmd, args) => {
      capturedArgs = args;
      return {
        threadId: "t",
        terminalId: "d",
        cwd: "",
        status: "starting",
        pid: null,
        history: "",
        exitCode: null,
        exitSignal: null,
        updatedAt: "",
      };
    });

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    await api.terminal.open({} as never);

    expect(capturedArgs).toMatchObject({
      args: [],
      workingDir: null,
      cols: 80,
      rows: 24,
    });
  });

  it("terminal.write invokes terminal_write with sessionId + data", async () => {
    let capturedCmd: string | undefined;
    let capturedArgs: unknown;
    mockIPC((cmd, args) => {
      capturedCmd = cmd;
      capturedArgs = args;
      return undefined;
    });

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    await api.terminal.write({ sessionId: "s1", data: "ls -la\r" });

    expect(capturedCmd).toBe("terminal_write");
    expect(capturedArgs).toEqual({ sessionId: "s1", data: "ls -la\r" });
  });

  it("terminal.ackOutput invokes terminal_ack with sessionId + seq", async () => {
    let capturedCmd: string | undefined;
    let capturedArgs: unknown;
    mockIPC((cmd, args) => {
      capturedCmd = cmd;
      capturedArgs = args;
      return undefined;
    });

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    await api.terminal.ackOutput({ sessionId: "s1", seq: 99 });

    expect(capturedCmd).toBe("terminal_ack");
    expect(capturedArgs).toEqual({ sessionId: "s1", seq: 99 });
  });

  it("terminal.resize invokes terminal_resize with sessionId + cols + rows", async () => {
    let capturedCmd: string | undefined;
    let capturedArgs: unknown;
    mockIPC((cmd, args) => {
      capturedCmd = cmd;
      capturedArgs = args;
      return undefined;
    });

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    await api.terminal.resize({ sessionId: "s1", cols: 200, rows: 50 });

    expect(capturedCmd).toBe("terminal_resize");
    expect(capturedArgs).toEqual({ sessionId: "s1", cols: 200, rows: 50 });
  });

  it("terminal.close invokes terminal_destroy_session with sessionId", async () => {
    let capturedCmd: string | undefined;
    let capturedArgs: unknown;
    mockIPC((cmd, args) => {
      capturedCmd = cmd;
      capturedArgs = args;
      return true;
    });

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    await api.terminal.close({ sessionId: "s1" });

    expect(capturedCmd).toBe("terminal_destroy_session");
    expect(capturedArgs).toEqual({ sessionId: "s1" });
  });

  it("terminal.restart destroys then rejects with UnsupportedError (no restart command)", async () => {
    const destroyCalls: string[] = [];
    mockIPC((cmd, args) => {
      if (cmd === "terminal_destroy_session") {
        destroyCalls.push((args as { sessionId?: string }).sessionId ?? "");
        return true;
      }
      return undefined;
    });

    const { createTauriNativeApi, UnsupportedError } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    // restart destroys the existing session, then rejects because there is no
    // native `terminal_restart_session` command — the UI must re-open() with
    // fresh params to fully restart.
    await expect(api.terminal.restart({ sessionId: "s1" })).rejects.toThrow(
      UnsupportedError,
    );
    expect(destroyCalls).toEqual(["s1"]);
  });
});

// ─── shell.openInEditor IPC + fallback ───────────────────────────────────
//
// `shell.openInEditor` first tries the syncode-tauri `shell_open_editor`
// command. If the command is unregistered (older backend, plugin missing),
// the error message contains "command" or "not" and we fall through to
// `openExternalImpl(cwd)` → `window.open(cwd, "_blank", ...)`. Any other
// error re-throws so genuine permission failures don't get masked.
describe("createTauriNativeApi — shell.openInEditor", () => {
  it("invokes shell_open_editor with cwd + editor on the happy path", async () => {
    let capturedCmd: string | undefined;
    let capturedArgs: unknown;
    mockIPC((cmd, args) => {
      capturedCmd = cmd;
      capturedArgs = args;
      return null;
    });

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    await api.shell.openInEditor("/proj", "vscode");

    expect(capturedCmd).toBe("shell_open_editor");
    expect(capturedArgs).toEqual({ cwd: "/proj", editor: "vscode" });
    expect(windowOpenSpy).not.toHaveBeenCalled();
  });

  it("falls back to window.open when the command is unregistered (error contains 'command')", async () => {
    mockIPC(() => {
      throw new Error("command shell_open_editor not found");
    });

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    await api.shell.openInEditor("/proj", "vscode");

    expect(windowOpenSpy).toHaveBeenCalledTimes(1);
    expect(windowOpenSpy).toHaveBeenCalledWith(
      "/proj",
      "_blank",
      "noopener,noreferrer",
    );
  });

  it("falls back when the error message contains 'not' (Tauri 'not allowed' phrasing)", async () => {
    mockIPC(() => {
      throw new Error("plugin not allowed on this scope");
    });

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    await api.shell.openInEditor("/proj", "vscode");

    expect(windowOpenSpy).toHaveBeenCalledTimes(1);
  });

  it("re-throws errors that are not command-registration failures", async () => {
    mockIPC(() => {
      throw new Error("permission denied");
    });

    const { createTauriNativeApi } = await import("./tauriNativeApi");
    const api = createTauriNativeApi(makeTransport());

    await expect(api.shell.openInEditor("/proj", "vscode")).rejects.toThrow(
      "permission denied",
    );
    expect(windowOpenSpy).not.toHaveBeenCalled();
  });
});

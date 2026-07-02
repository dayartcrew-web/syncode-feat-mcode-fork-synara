/**
 * Tier 3 — Terminal domain.
 *
 * Hand-ported from MCode `packages/contracts/src/terminal.ts` (Effect Schema
 * → plain TS types). Adds the runtime constants + the real
 * `TerminalSessionSnapshot` / `TerminalEvent` shapes (shell.ts has them
 * opaque). Source of truth: /home/vibe-dev/mcode/packages/contracts/src/terminal.ts
 */

// PTY window dimension bounds (unsigned 16-bit OS winsize caps, kept well
// below 65535).
export const TERMINAL_MIN_COLS = 20;
export const TERMINAL_MAX_COLS = 2000;
export const TERMINAL_MIN_ROWS = 5;
export const TERMINAL_MAX_ROWS = 1000;
export const DEFAULT_TERMINAL_ID = "default";

export type TerminalSessionStatus = "starting" | "running" | "exited" | "error";

export interface TerminalSessionSnapshot {
  threadId: string;
  terminalId: string;
  cwd: string;
  status: TerminalSessionStatus;
  pid: number | null;
  history: string;
  replayPreamble?: string;
  exitCode: number | null;
  exitSignal: number | null;
  updatedAt: string;
}

interface TerminalEventBase {
  readonly threadId: string;
  readonly terminalId: string;
  readonly createdAt: string;
}

export type TerminalEvent =
  | (TerminalEventBase & {
      readonly type: "started";
      readonly snapshot: TerminalSessionSnapshot;
    })
  | (TerminalEventBase & {
      readonly type: "output";
      readonly data: string;
      readonly byteLength?: number;
    })
  | (TerminalEventBase & {
      readonly type: "exited";
      readonly exitCode: number | null;
      readonly exitSignal: number | null;
    })
  | (TerminalEventBase & {
      readonly type: "error";
      readonly message: string;
    })
  | (TerminalEventBase & { readonly type: "cleared" })
  | (TerminalEventBase & {
      readonly type: "restarted";
      readonly snapshot: TerminalSessionSnapshot;
    })
  | (TerminalEventBase & {
      readonly type: "activity";
      readonly hasRunningSubprocess: boolean;
      readonly cliKind: "codex" | "claude" | null;
      readonly agentState: "running" | "attention" | "review" | null;
    });

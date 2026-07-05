import { describe, expect, it } from "vitest";

import type { ServerProviderStatus } from "@t3tools/contracts";
import {
  isProviderUsable,
  normalizeProviderStatusForLocalConfig,
  normalizeServerProviderStatuses,
  providerUnavailableReason,
} from "./providerAvailability";

const BASE_STATUS: ServerProviderStatus = {
  provider: "gemini",
  status: "error",
  available: false,
  authStatus: "unknown",
  checkedAt: "2026-04-17T10:00:00.000Z",
  message: "Gemini CLI (`gemini`) is not installed or not on PATH.",
};

describe("normalizeProviderStatusForLocalConfig", () => {
  it("keeps Gemini interactive when a custom binary path is configured locally", () => {
    expect(
      normalizeProviderStatusForLocalConfig({
        provider: "gemini",
        status: BASE_STATUS,
        customBinaryPath: "/opt/homebrew/bin/gemini",
      }),
    ).toEqual({
      ...BASE_STATUS,
      available: true,
      status: "warning",
      message:
        "Gemini uses a custom local binary path in this app. Availability will be confirmed when you start a session.",
    });
  });

  it("applies the same custom-path fallback to Claude", () => {
    expect(
      normalizeProviderStatusForLocalConfig({
        provider: "claudeAgent",
        status: {
          ...BASE_STATUS,
          provider: "claudeAgent",
          message: "Claude Code CLI (`claude`) is not installed or not on PATH.",
        },
        customBinaryPath: "/opt/homebrew/bin/claude",
      }),
    ).toEqual({
      ...BASE_STATUS,
      provider: "claudeAgent",
      available: true,
      status: "warning",
      message:
        "Claude uses a custom local binary path in this app. Availability will be confirmed when you start a session.",
    });
  });

  it("marks a custom-path provider ready after a successful session confirms it", () => {
    expect(
      normalizeProviderStatusForLocalConfig({
        provider: "opencode",
        status: {
          ...BASE_STATUS,
          provider: "opencode",
          message: "OpenCode CLI (`opencode`) is not installed or not on PATH.",
        },
        customBinaryPath: "/custom/bin/opencode",
        confirmedCustomBinaryPath: "/custom/bin/opencode",
      }),
    ).toEqual({
      provider: "opencode",
      authStatus: "unknown",
      available: true,
      checkedAt: BASE_STATUS.checkedAt,
      status: "ready",
    });
  });

  it("keeps warning when a different custom path was confirmed", () => {
    expect(
      normalizeProviderStatusForLocalConfig({
        provider: "opencode",
        status: {
          ...BASE_STATUS,
          provider: "opencode",
          message: "OpenCode CLI (`opencode`) is not installed or not on PATH.",
        },
        customBinaryPath: "/custom/bin/opencode-next",
        confirmedCustomBinaryPath: "/custom/bin/opencode",
      }),
    ).toEqual({
      ...BASE_STATUS,
      provider: "opencode",
      available: true,
      status: "warning",
      message:
        "OpenCode uses a custom local binary path in this app. Availability will be confirmed when you start a session.",
    });
  });

  it("preserves authenticated and unauthenticated statuses", () => {
    expect(
      normalizeProviderStatusForLocalConfig({
        provider: "gemini",
        status: { ...BASE_STATUS, available: true, status: "ready", authStatus: "authenticated" },
        customBinaryPath: "/opt/homebrew/bin/gemini",
      }),
    ).toEqual({ ...BASE_STATUS, available: true, status: "ready", authStatus: "authenticated" });

    expect(
      normalizeProviderStatusForLocalConfig({
        provider: "gemini",
        status: { ...BASE_STATUS, authStatus: "unauthenticated" },
        customBinaryPath: "/opt/homebrew/bin/gemini",
      }),
    ).toEqual({ ...BASE_STATUS, authStatus: "unauthenticated" });
  });
});

describe("isProviderUsable", () => {
  it("blocks unavailable or unauthenticated providers", () => {
    expect(isProviderUsable(null)).toBe(false);
    expect(isProviderUsable(undefined)).toBe(false);
    expect(isProviderUsable(BASE_STATUS)).toBe(false);
    expect(
      isProviderUsable({ ...BASE_STATUS, available: true, authStatus: "unauthenticated" }),
    ).toBe(false);
    expect(isProviderUsable({ ...BASE_STATUS, available: true, authStatus: "authenticated" })).toBe(
      true,
    );
  });
});

describe("providerUnavailableReason", () => {
  it("returns provider-specific guidance", () => {
    expect(providerUnavailableReason({ ...BASE_STATUS, authStatus: "unauthenticated" })).toBe(
      "Gemini is not authenticated yet.",
    );
    expect(providerUnavailableReason(BASE_STATUS)).toBe(BASE_STATUS.message);
  });
});

describe("normalizeServerProviderStatuses", () => {
  // PR-4-2: the server registry uses "claude" as the provider id
  // (PROVIDER_CLAUDE = "claude"), but the frontend contract uses
  // "claudeAgent". This is the claude → claudeAgent mapping the picker
  // depends on; without it, Claude shows as "Checking" (no live status).
  it("maps the server-side claude id to the frontend claudeAgent kind", () => {
    const input = {
      provider: "claude",
      status: "ready",
      available: true,
      authStatus: "authenticated",
      checkedAt: "2026-07-05T10:00:00.000Z",
    } as unknown as ServerProviderStatus;
    const result = normalizeServerProviderStatuses([input]);
    expect(result).toHaveLength(1);
    expect(result[0]?.provider).toBe("claudeAgent");
    // Non-provider fields are preserved.
    expect(result[0]?.available).toBe(true);
    expect(result[0]?.authStatus).toBe("authenticated");
  });

  it("drops non-picker providers (anthropic, openai) the server emits", () => {
    // The server's ALL_PROVIDERS emits 10 entries; the frontend ProviderKind
    // union only covers 8. anthropic/openai are upstream model hosts surfaced
    // inside other providers' model lists, not standalone picker entries.
    // The cast mirrors the runtime reality: the server emits provider ids the
    // TS union doesn't cover, and normalizeServerProviderStatuses is the gate
    // that filters them out.
    const statuses = [
      {
        provider: "codex",
        status: "ready",
        available: true,
        authStatus: "authenticated",
        checkedAt: "2026-07-05T10:00:00.000Z",
      },
      {
        provider: "claude",
        status: "ready",
        available: true,
        authStatus: "authenticated",
        checkedAt: "2026-07-05T10:00:00.000Z",
      },
      {
        provider: "anthropic",
        status: "ready",
        available: true,
        authStatus: "authenticated",
        checkedAt: "2026-07-05T10:00:00.000Z",
      },
      {
        provider: "openai",
        status: "ready",
        available: true,
        authStatus: "authenticated",
        checkedAt: "2026-07-05T10:00:00.000Z",
      },
    ] as unknown as ServerProviderStatus[];
    const result = normalizeServerProviderStatuses(statuses);
    expect(result.map((s) => s.provider)).toEqual(["codex", "claudeAgent"]);
  });

  it("passes already-correct frontend ids through unchanged", () => {
    const statuses: ServerProviderStatus[] = [
      {
        provider: "gemini",
        status: "ready",
        available: true,
        authStatus: "authenticated",
        checkedAt: "2026-07-05T10:00:00.000Z",
      },
      {
        provider: "claudeAgent",
        status: "ready",
        available: true,
        authStatus: "authenticated",
        checkedAt: "2026-07-05T10:00:00.000Z",
      },
    ];
    const result = normalizeServerProviderStatuses(statuses);
    expect(result).toEqual(statuses);
  });

  it("does not mutate the input array", () => {
    const input = [
      {
        provider: "claude",
        status: "ready",
        available: true,
        authStatus: "authenticated",
        checkedAt: "2026-07-05T10:00:00.000Z",
      },
    ] as unknown as ServerProviderStatus[];
    const snapshot = input.map((s) => ({ ...s }));
    normalizeServerProviderStatuses(input);
    expect(input).toEqual(snapshot);
  });
});

import {
  PROVIDER_DISPLAY_NAMES,
  type ProviderKind,
  type ServerProviderStatus,
} from "@t3tools/contracts";

/**
 * The full set of {@link ProviderKind} union values as a runtime set.
 *
 * The server's provider registry emits statuses for 10 providers
 * (`syncode_provider::ALL_PROVIDERS`), but only the 8 in the `ProviderKind`
 * union are real composer providers — `anthropic` and `openai` are upstream
 * model hosts surfaced inside other providers' model lists, not standalone
 * picker entries. This set is the runtime gate used by
 * {@link normalizeServerProviderStatuses} to filter them out.
 */
const PICKER_PROVIDER_KINDS: ReadonlySet<string> = new Set<ProviderKind>([
  "codex",
  "claudeAgent",
  "cursor",
  "gemini",
  "grok",
  "kilo",
  "opencode",
  "pi",
]);

/**
 * Map a raw server-side provider id to the frontend {@link ProviderKind}.
 *
 * The server registry uses `"claude"` as the provider id
 * (`PROVIDER_CLAUDE = "claude"`), but the frontend contract uses
 * `"claudeAgent"` (the MCode `ProviderKind` union). Every other id is
 * already identical on both sides. Centralizing this mapping here means the
 * picker keeps working regardless of which server path emitted the statuses
 * (`getConfig`, the snapshot, or `refreshProviders`) — defensive against any
 * path that forgets the mapping.
 */
function toPickerProviderKind(rawProvider: string): ProviderKind | null {
  if (rawProvider === "claude") {
    return "claudeAgent";
  }
  if (PICKER_PROVIDER_KINDS.has(rawProvider)) {
    return rawProvider as ProviderKind;
  }
  // Unknown provider ids (e.g. "anthropic", "openai" — upstream model hosts
  // that are not standalone picker entries) are dropped.
  return null;
}

/**
 * Normalize a raw server provider-statuses array for picker consumption.
 *
 * - Maps `"claude"` → `"claudeAgent"` (the server ↔ frontend id gap).
 * - Drops entries whose provider id is not a real picker {@link ProviderKind}
 *   (e.g. `"anthropic"` / `"openai"`, which the server emits but the picker
 *   has no entry for — they show up inside other providers' model lists).
 *
 * Returns a new array; the input is not mutated. Safe to call on every render
 * (the work is O(n) over the ≤10 statuses).
 */
export function normalizeServerProviderStatuses(
  statuses: readonly ServerProviderStatus[],
): ServerProviderStatus[] {
  const result: ServerProviderStatus[] = [];
  for (const status of statuses) {
    const pickerKind = toPickerProviderKind(status.provider);
    if (pickerKind === null) {
      continue;
    }
    if (pickerKind === status.provider) {
      result.push(status);
    } else {
      // Rewrite the provider id to the frontend kind (claude → claudeAgent).
      result.push({ ...status, provider: pickerKind });
    }
  }
  return result;
}

export interface ProviderSendAvailability {
  readonly provider: ProviderKind;
  readonly status: ServerProviderStatus | null;
  readonly usable: boolean;
  readonly unavailableReason: string;
}

export function normalizeCustomBinaryPath(value: string | null | undefined): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export function normalizeProviderStatusForLocalConfig(input: {
  provider: ProviderKind;
  status: ServerProviderStatus | null | undefined;
  customBinaryPath?: string | null | undefined;
  confirmedCustomBinaryPath?: string | null | undefined;
}): ServerProviderStatus | null {
  const status = input.status ?? null;
  if (!status) {
    return null;
  }

  const customBinaryPath = normalizeCustomBinaryPath(input.customBinaryPath);
  if (!customBinaryPath) {
    return status;
  }

  if (status.available || status.authStatus !== "unknown") {
    return status;
  }

  if (normalizeCustomBinaryPath(input.confirmedCustomBinaryPath) === customBinaryPath) {
    // Only the exact path used by a successful session can suppress the warning.
    return {
      provider: status.provider,
      available: true,
      status: "ready",
      authStatus: status.authStatus,
      checkedAt: status.checkedAt,
      ...(status.authType ? { authType: status.authType } : {}),
      ...(status.authLabel ? { authLabel: status.authLabel } : {}),
      ...(status.voiceTranscriptionAvailable !== undefined
        ? { voiceTranscriptionAvailable: status.voiceTranscriptionAvailable }
        : {}),
    };
  }

  return {
    ...status,
    available: true,
    status: "warning",
    message: `${PROVIDER_DISPLAY_NAMES[input.provider]} uses a custom local binary path in this app. Availability will be confirmed when you start a session.`,
  };
}

export function isProviderUsable(status: ServerProviderStatus | null | undefined): boolean {
  if (!status) {
    // Missing status means the health check has not confirmed an installed provider yet.
    return false;
  }
  return status.available && status.authStatus !== "unauthenticated";
}

export function providerUnavailableReason(status: ServerProviderStatus | null | undefined): string {
  if (!status) {
    return "Provider status is still loading.";
  }
  const providerLabel = PROVIDER_DISPLAY_NAMES[status.provider] ?? status.provider;
  if (status.authStatus === "unauthenticated") {
    return `${providerLabel} is not authenticated yet.`;
  }
  if (!status.available) {
    return status.message ?? `${providerLabel} is unavailable right now.`;
  }
  return status.message ?? `${providerLabel} has limited availability right now.`;
}

export function findProviderStatus(
  statuses: readonly ServerProviderStatus[],
  provider: ProviderKind,
): ServerProviderStatus | null {
  return statuses.find((status) => status.provider === provider) ?? null;
}

// Shared send gate used by chat, Kanban, shortcuts, and handoff flows.
export function resolveProviderSendAvailability(input: {
  readonly provider: ProviderKind;
  readonly statuses: readonly ServerProviderStatus[];
}): ProviderSendAvailability {
  const status = findProviderStatus(input.statuses, input.provider);
  return {
    provider: input.provider,
    status,
    usable: isProviderUsable(status),
    unavailableReason: providerUnavailableReason(status),
  };
}

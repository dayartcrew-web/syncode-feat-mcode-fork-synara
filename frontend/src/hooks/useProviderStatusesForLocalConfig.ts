// FILE: useProviderStatusesForLocalConfig.ts
// Purpose: Normalize server provider health against local binary overrides for composer-like sends.
// Layer: Web hook
// Depends on: server config query, app settings, and provider availability normalization.

import type { ServerProviderStatus } from "@t3tools/contracts";
import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";

import { getCustomBinaryPathForProvider, useAppSettings } from "../appSettings";
import {
  normalizeProviderStatusForLocalConfig,
  normalizeServerProviderStatuses,
} from "../lib/providerAvailability";
import { serverConfigQueryOptions } from "../lib/serverReactQuery";

const EMPTY_PROVIDER_STATUSES: ServerProviderStatus[] = [];

export function useProviderStatusesForLocalConfig(): readonly ServerProviderStatus[] {
  const { settings } = useAppSettings();
  const serverConfigQuery = useQuery(serverConfigQueryOptions());

  return useMemo(
    () =>
      // PR-4-2: normalize the raw server statuses first — map "claude" →
      // "claudeAgent" and drop non-picker providers (anthropic/openai) — so the
      // downstream per-provider lookup by ProviderKind always matches.
      normalizeServerProviderStatuses(
        serverConfigQuery.data?.providers ?? EMPTY_PROVIDER_STATUSES,
      )
        .map((status) =>
          normalizeProviderStatusForLocalConfig({
            provider: status.provider,
            status,
            customBinaryPath: getCustomBinaryPathForProvider(settings, status.provider),
          }),
        )
        .flatMap((status) => (status ? [status] : [])),
    [serverConfigQuery.data?.providers, settings],
  );
}

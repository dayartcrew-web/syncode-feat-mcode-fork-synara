// FILE: providerReactQuery.ts
// Purpose: Builds React Query options for provider-backed orchestration RPC calls.
// Layer: Web data fetching helpers
// Depends on: native API bridge, orchestration contracts, and React Query.

import {
  type OrchestrationGetFullThreadDiffInput,
  type OrchestrationGetTurnDiffInput,
  type ThreadId,
} from "@t3tools/contracts";
import { queryOptions } from "@tanstack/react-query";
import { ensureNativeApi } from "../nativeApi";

interface CheckpointDiffQueryInput {
  threadId: ThreadId | null;
  fromTurnCount: number | null;
  toTurnCount: number | null;
  ignoreWhitespace: boolean;
  cacheScope?: string | null;
  enabled?: boolean;
}

// Minimal Option stand-in — replaces the Effect `Option` shape used by the
// former `Schema.decodeUnknownOption(...).pipe(Option.map(...))` chain. Only
// the `_tag` discriminator and `value` are consumed by this module.
interface Some<T> {
  readonly _tag: "Some";
  readonly value: T;
}
interface None {
  readonly _tag: "None";
}
type Option_<T> = Some<T> | None;

const some = <T>(value: T): Option_<T> => ({ _tag: "Some", value });
const none = (): None => ({ _tag: "None" });

export const providerQueryKeys = {
  all: ["providers"] as const,
  checkpointDiff: (input: CheckpointDiffQueryInput) =>
    [
      "providers",
      "checkpointDiff",
      input.threadId,
      input.fromTurnCount,
      input.toTurnCount,
      input.ignoreWhitespace,
      input.cacheScope ?? null,
    ] as const,
};

/** Keep polling while placeholder checkpoints are still being written. */
export const CHECKPOINT_DIFF_PENDING_REFETCH_INTERVAL_MS = 2_000;
export const CHECKPOINT_DIFF_PENDING_REFETCH_MAX_ATTEMPTS = 12;

function shouldUseFullThreadDiffApi(input: CheckpointDiffQueryInput): boolean {
  return (
    input.fromTurnCount === 0 &&
    typeof input.cacheScope === "string" &&
    input.cacheScope.startsWith("conversation:")
  );
}

type CheckpointDiffRequest =
  | { readonly kind: "fullThreadDiff"; readonly input: OrchestrationGetFullThreadDiffInput }
  | { readonly kind: "turnDiff"; readonly input: OrchestrationGetTurnDiffInput };

function decodeCheckpointDiffRequest(input: CheckpointDiffQueryInput): Option_<CheckpointDiffRequest> {
  // The original Effect Schema decode validated the field shapes and returned
  // None when invalid. The transport input types are now opaque records; the
  // fields below are already typed by the caller, so we only need a presence
  // check on the required `threadId` to mirror the former guard.
  if (!input.threadId) {
    return none();
  }
  if (shouldUseFullThreadDiffApi(input)) {
    return some({
      kind: "fullThreadDiff" as const,
      input: {
        threadId: input.threadId,
        toTurnCount: input.toTurnCount,
        ignoreWhitespace: input.ignoreWhitespace,
      } as OrchestrationGetFullThreadDiffInput,
    });
  }
  return some({
    kind: "turnDiff" as const,
    input: {
      threadId: input.threadId,
      fromTurnCount: input.fromTurnCount,
      toTurnCount: input.toTurnCount,
      ignoreWhitespace: input.ignoreWhitespace,
    } as OrchestrationGetTurnDiffInput,
  });
}

function asCheckpointErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === "string") {
    return error;
  }
  return "";
}

function normalizeCheckpointErrorMessage(error: unknown): string {
  const message = asCheckpointErrorMessage(error).trim();
  if (message.length === 0) {
    return "Failed to load checkpoint diff.";
  }

  const lower = message.toLowerCase();
  if (lower.includes("not a git repository")) {
    return "Turn diffs are unavailable because this project is not a git repository.";
  }

  if (
    lower.includes("checkpoint unavailable for thread") ||
    lower.includes("checkpoint invariant violation")
  ) {
    const separatorIndex = message.indexOf(":");
    if (separatorIndex >= 0) {
      const detail = message.slice(separatorIndex + 1).trim();
      if (detail.length > 0) {
        return detail;
      }
    }
  }

  return message;
}

export function isCheckpointTemporarilyUnavailable(error: unknown): boolean {
  const message = asCheckpointErrorMessage(error).toLowerCase();
  return (
    message.includes("exceeds current turn count") ||
    // Placeholder checkpoint rows can arrive before the checkpoint writer finishes.
    message.includes("checkpoint diff is not available yet")
  );
}

export function resolveCheckpointDiffQueryDisplayState(input: {
  isLoading: boolean;
  isFetching: boolean;
  data: unknown;
  error: unknown;
}): { isLoading: boolean; error: string | null } {
  const hasData = input.data != null;
  return {
    isLoading: input.isLoading || (input.isFetching && !hasData),
    error:
      input.isFetching || input.error == null ? null : normalizeCheckpointErrorMessage(input.error),
  };
}

export function checkpointDiffQueryOptions(input: CheckpointDiffQueryInput) {
  const decodedRequest = decodeCheckpointDiffRequest(input);

  return queryOptions({
    queryKey: providerQueryKeys.checkpointDiff(input),
    queryFn: async () => {
      const api = ensureNativeApi();
      if (!input.threadId || decodedRequest._tag === "None") {
        throw new Error("Checkpoint diff is unavailable.");
      }
      try {
        if (decodedRequest.value.kind === "fullThreadDiff") {
          return await api.orchestration.getFullThreadDiff(decodedRequest.value.input);
        }
        return await api.orchestration.getTurnDiff(decodedRequest.value.input);
      } catch (error) {
        throw new Error(normalizeCheckpointErrorMessage(error), { cause: error });
      }
    },
    enabled: (input.enabled ?? true) && !!input.threadId && decodedRequest._tag === "Some",
    staleTime: Infinity,
    retry: (failureCount, error) => {
      if (isCheckpointTemporarilyUnavailable(error)) {
        return failureCount < 12;
      }
      return failureCount < 3;
    },
    retryDelay: (attempt, error) =>
      isCheckpointTemporarilyUnavailable(error)
        ? Math.min(5_000, 250 * 2 ** (attempt - 1))
        : Math.min(1_000, 100 * 2 ** (attempt - 1)),
    refetchInterval: (query) => {
      const temporaryError = query.state.error;
      if (!temporaryError || !isCheckpointTemporarilyUnavailable(temporaryError)) {
        return false;
      }
      const temporaryErrorCount = query.state.errorUpdateCount ?? 0;
      return temporaryErrorCount < CHECKPOINT_DIFF_PENDING_REFETCH_MAX_ATTEMPTS
        ? CHECKPOINT_DIFF_PENDING_REFETCH_INTERVAL_MS
        : false;
    },
  });
}

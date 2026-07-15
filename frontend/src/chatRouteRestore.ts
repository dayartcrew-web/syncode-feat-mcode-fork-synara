// FILE: chatRouteRestore.ts
// Purpose: Validates saved chat routes before restoring them from startup or sidebar navigation.
// Layer: Route helper
// Exports: last-thread route resolver plus empty-startup fallback policy helpers.

export type LastThreadRoute = {
  threadId: string;
  splitViewId?: string | undefined;
};

export type EmptyRouteRestoreRecoveryState = "idle" | "pending" | "done";

export const EMPTY_ROUTE_RESTORE_FALLBACK_DELAY_MS = 1_800;

export function resolveRestorableThreadRoute(input: {
  lastThreadRoute: LastThreadRoute | null;
  availableThreadIds: ReadonlySet<string>;
  availableSplitViewIds?: ReadonlySet<string>;
}): LastThreadRoute | null {
  const { lastThreadRoute, availableThreadIds, availableSplitViewIds } = input;
  if (!lastThreadRoute) {
    return null;
  }

  if (!availableThreadIds.has(lastThreadRoute.threadId)) {
    return null;
  }

  if (
    lastThreadRoute.splitViewId &&
    availableSplitViewIds &&
    !availableSplitViewIds.has(lastThreadRoute.splitViewId)
  ) {
    return { threadId: lastThreadRoute.threadId };
  }

  return lastThreadRoute;
}

// Route fallback guards separate a stale URL from a temporarily empty startup snapshot.
export function shouldStartRememberedRouteRecovery(input: {
  lastThreadRoute: LastThreadRoute | null;
  availableThreadCount: number;
  recoveryState: EmptyRouteRestoreRecoveryState;
}): boolean {
  return Boolean(
    input.lastThreadRoute && input.availableThreadCount === 0 && input.recoveryState === "idle",
  );
}

export function shouldHoldRememberedRouteFallback(input: {
  lastThreadRoute: LastThreadRoute | null;
  availableThreadCount: number;
  recoveryState: EmptyRouteRestoreRecoveryState;
}): boolean {
  return Boolean(
    input.lastThreadRoute && input.availableThreadCount === 0 && input.recoveryState !== "done",
  );
}

export function shouldStartMissingThreadRouteRecovery(input: {
  hasKnownServerThreads: boolean;
  recoveryState: EmptyRouteRestoreRecoveryState;
  routeThreadExists: boolean;
}): boolean {
  // Recovery should run whenever the route thread is missing, regardless of
  // whether other threads exist. The previous `!hasKnownServerThreads` gate
  // skipped recovery once ANY thread was in the store — but the shell snapshot
  // may not include the route thread (pagination, race, deep link). That caused
  // existing threads opened by URL to redirect to welcome.
  return !input.routeThreadExists && input.recoveryState === "idle";
}

export function shouldHoldMissingThreadRouteFallback(input: {
  hasKnownServerThreads: boolean;
  recoveryState: EmptyRouteRestoreRecoveryState;
  routeThreadExists: boolean;
}): boolean {
  return !input.routeThreadExists && input.recoveryState !== "done";
}

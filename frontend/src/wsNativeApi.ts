// FILE: wsNativeApi.ts
// Purpose: NativeApi implementation backed by the browser WebSocket RPC transport.
// Layer: Web transport adapter
// Exports: createWsNativeApi and event subscription helpers for server push channels.

import {
  type AuthBearerBootstrapResult,
  type AuthBootstrapInput,
  type AuthBootstrapResult,
  type AuthClientSession,
  type AuthCreatePairingCredentialInput,
  type AuthPairingCredentialResult,
  type AuthPairingLink,
  type AuthRevokeClientSessionInput,
  type AuthRevokePairingLinkInput,
  type AuthSessionState,
  type AuthWebSocketTokenResult,
  type ThreadId,
  type ThreadBrowserState,
  type GitActionProgressEvent,
  type OrchestrationEvent,
  type OrchestrationShellStreamItem,
  type OrchestrationThreadStreamItem,
  type ProjectDevServerEvent,
  type ServerProviderStatusesUpdatedPayload,
  type ServerLifecycleStreamEvent,
  type ServerSettingsUpdatedPayload,
  type TerminalEvent,
  ORCHESTRATION_WS_METHODS,
  type ContextMenuItem,
  type NativeApi,
  ServerConfigUpdatedPayload,
  WS_CHANNELS,
  WS_METHODS,
  type WsWelcomePayload,
  type AutomationStreamEvent,
} from "@t3tools/contracts";

import { showConfirmDialogFallback } from "./confirmDialogFallback";
import { showContextMenuFallback } from "./contextMenuFallback";
import { adaptPushEnvelope, createPushAdaptContext } from "./contracts/adaptPushEvent";
import { WsTransport } from "./wsTransport";
import { emitWsTransportState } from "./wsTransportEvents";

let instance: { api: NativeApi; transport: WsTransport } | null = null;
// Transport wired to the server-lifecycle push channels (welcome +
// configUpdated + providerStatusesUpdated + maintenanceUpdated +
// settingsUpdated). Used for two purposes:
//   1. Fanout: each lifecycle push fans out to the matching `*Listeners` set
//      that the exported `on*` helpers consume.
//   2. Replay: when an `on*` listener is registered AFTER the push already
//      arrived, the transport's `latestPushByChannel` cache lets us replay it.
//
// Set by BOTH `createWsNativeApi` (browser/dev mode) AND `nativeApi.ts`
// (Tauri shell mode) via `bindServerLifecycleTransport`. Without the Tauri
// path, the `on*` helpers are no-ops in the desktop app — `instance` is only
// set inside `createWsNativeApi`, which never runs in Tauri mode — so the
// welcome push never reaches `__root.tsx`'s `onServerWelcome` listener and
// the splash screen hangs on "Home folder is not available yet.".
let serverLifecycleTransport: WsTransport | null = null;
const welcomeListeners = new Set<(payload: WsWelcomePayload) => void>();
const serverConfigUpdatedListeners = new Set<(payload: ServerConfigUpdatedPayload) => void>();
const serverProviderStatusesUpdatedListeners = new Set<
  (payload: ServerProviderStatusesUpdatedPayload) => void
>();
const serverMaintenanceUpdatedListeners = new Set<(payload: ServerLifecycleStreamEvent) => void>();
const serverSettingsUpdatedListeners = new Set<(payload: ServerSettingsUpdatedPayload) => void>();
const gitActionProgressListeners = new Set<(payload: GitActionProgressEvent) => void>();

export function omitNullUserInputAnswers(
  command: Parameters<NativeApi["orchestration"]["dispatchCommand"]>[0],
) {
  if (command.type !== "thread.user-input.respond") {
    return command;
  }

  return {
    ...command,
    answers: Object.fromEntries(
      Object.entries(command.answers as Record<string, unknown>).filter(
        ([, answer]) => answer !== null && answer !== undefined,
      ),
    ),
  };
}
const terminalEventListeners = new Set<(payload: TerminalEvent) => void>();
const projectDevServerEventListeners = new Set<(payload: ProjectDevServerEvent) => void>();
const automationEventListeners = new Set<(payload: AutomationStreamEvent) => void>();
const orchestrationDomainEventListeners = new Set<(payload: OrchestrationEvent) => void>();
const orchestrationShellEventListeners = new Set<(payload: OrchestrationShellStreamItem) => void>();
const orchestrationThreadEventListeners = new Set<
  (payload: OrchestrationThreadStreamItem) => void
>();
// Per-connection push adapter context (resets on page reload; reconnect
// mid-turn is a known edge case — see adaptPushEvent.ts risks).
const pushAdaptCtx = createPushAdaptContext();
const fallbackBrowserStateListeners = new Set<(state: ThreadBrowserState) => void>();
const fallbackBrowserStates = new Map<ThreadId, ThreadBrowserState>();

function defaultBrowserState(threadId: ThreadId): ThreadBrowserState {
  return {
    threadId,
    version: 0,
    open: false,
    activeTabId: null,
    tabs: [],
    lastError: null,
  };
}

function defaultBrowserTitle(url: string): string {
  if (url === "about:blank") {
    return "New tab";
  }
  try {
    return new URL(url).hostname || url;
  } catch {
    return url;
  }
}

async function requestAuthJson<T>(
  path: string,
  options: {
    readonly method?: "GET" | "POST";
    readonly body?: unknown;
  } = {},
): Promise<T> {
  const hasBody = options.body !== undefined;
  const response = await fetch(path, {
    method: options.method ?? "GET",
    credentials: "same-origin",
    ...(hasBody
      ? {
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(options.body),
        }
      : {}),
  });
  const payload = (await response.json().catch(() => null)) as unknown;
  if (!response.ok) {
    const message =
      payload &&
      typeof payload === "object" &&
      "error" in payload &&
      typeof payload.error === "string"
        ? payload.error
        : `Auth request failed with status ${response.status}`;
    throw new Error(message);
  }
  return payload as T;
}

function createFallbackTab(url = "about:blank") {
  return {
    id: crypto.randomUUID(),
    url,
    title: defaultBrowserTitle(url),
    status: "live" as const,
    isLoading: false,
    canGoBack: false,
    canGoForward: false,
    faviconUrl: null,
    lastCommittedUrl: url,
    lastError: null,
  };
}

function cloneBrowserState(state: ThreadBrowserState): ThreadBrowserState {
  return {
    ...state,
    tabs: state.tabs.map((tab) => ({ ...tab })),
  };
}

function getFallbackBrowserState(threadId: ThreadId): ThreadBrowserState {
  const existing = fallbackBrowserStates.get(threadId);
  if (existing) {
    return existing;
  }
  const initial = defaultBrowserState(threadId);
  fallbackBrowserStates.set(threadId, initial);
  return initial;
}

function emitFallbackBrowserState(threadId: ThreadId): ThreadBrowserState {
  const state = cloneBrowserState(getFallbackBrowserState(threadId));
  for (const listener of fallbackBrowserStateListeners) {
    listener(state);
  }
  return state;
}

function markFallbackBrowserStateChanged(state: ThreadBrowserState): void {
  state.version += 1;
}

function ensureFallbackBrowserWorkspace(threadId: ThreadId): ThreadBrowserState {
  const state = getFallbackBrowserState(threadId);
  if (state.tabs.length === 0) {
    const tab = createFallbackTab();
    state.tabs = [tab];
    state.activeTabId = tab.id;
  }
  state.open = true;
  return state;
}

function resolveFallbackBrowserTab(state: ThreadBrowserState, tabId?: string) {
  const existing =
    (tabId ? state.tabs.find((tab) => tab.id === tabId) : undefined) ??
    (state.activeTabId ? state.tabs.find((tab) => tab.id === state.activeTabId) : undefined) ??
    state.tabs[0];
  if (existing) {
    return existing;
  }
  const tab = createFallbackTab();
  state.tabs = [tab];
  state.activeTabId = tab.id;
  state.open = true;
  return tab;
}

/**
 * Wire a `WsTransport` to the 5 server-lifecycle push channels (welcome,
 * configUpdated, providerStatusesUpdated, maintenanceUpdated, settingsUpdated)
 * and register it as the replay source for the exported `on*` helpers.
 *
 * Called from BOTH code paths that own a real `WsTransport`:
 *   - `createWsNativeApi` (browser/dev mode) wires its in-file transport.
 *   - `nativeApi.ts` (Tauri shell mode) wires the desktop transport it creates
 *     for `createTauriNativeApi`.
 *
 * Without the Tauri-side bind, the `on*` helpers never fire — `instance` is
 * only set inside `createWsNativeApi`, which never runs in Tauri mode, so
 * the welcome push arrives at the transport but never fans out to listeners
 * and the replay cache is never consulted.
 *
 * Safe to call multiple times: the transport reference is replaced (prior
 * transport's subscriptions live until that transport is disposed).
 */
export function bindServerLifecycleTransport(transport: WsTransport): void {
  // Idempotent: binding the SAME transport again would re-subscribe every
  // lifecycle channel (welcome/configUpdated/settingsUpdated/…), so each push
  // would fan out N times. `nativeApi.ts` calls this once per cached API build
  // — guard defensively in case a caller re-invokes it.
  if (serverLifecycleTransport === transport) return;
  serverLifecycleTransport = transport;
  transport.subscribe(WS_CHANNELS.serverWelcome, (message) => {
    fanoutLifecycle(welcomeListeners, message.data as WsWelcomePayload);
  });
  transport.subscribe(WS_CHANNELS.serverConfigUpdated, (message) => {
    fanoutLifecycle(
      serverConfigUpdatedListeners,
      message.data as ServerConfigUpdatedPayload,
    );
  });
  transport.subscribe(WS_CHANNELS.serverProviderStatusesUpdated, (message) => {
    fanoutLifecycle(
      serverProviderStatusesUpdatedListeners,
      message.data as ServerProviderStatusesUpdatedPayload,
    );
  });
  transport.subscribe(WS_CHANNELS.serverMaintenanceUpdated, (message) => {
    fanoutLifecycle(
      serverMaintenanceUpdatedListeners,
      message.data as ServerLifecycleStreamEvent,
    );
  });
  transport.subscribe(WS_CHANNELS.serverSettingsUpdated, (message) => {
    fanoutLifecycle(
      serverSettingsUpdatedListeners,
      message.data as ServerSettingsUpdatedPayload,
    );
  });
}

function fanoutLifecycle<T>(listeners: Set<(payload: T) => void>, payload: T): void {
  for (const listener of listeners) {
    try {
      listener(payload);
    } catch {
      // Swallow listener errors — one bad callback must not break the dispatch loop.
    }
  }
}

/**
 * Subscribe to the server welcome message. If a welcome was already received
 * before this call, the listener fires synchronously with the cached payload.
 * This avoids the race between WebSocket connect and React effect registration.
 */
export function onServerWelcome(listener: (payload: WsWelcomePayload) => void): () => void {
  welcomeListeners.add(listener);

  const latestWelcome =
    serverLifecycleTransport?.getLatestPush(WS_CHANNELS.serverWelcome)?.data ?? null;
  if (latestWelcome) {
    try {
      listener(latestWelcome);
    } catch {
      // Swallow listener errors
    }
  }

  return () => {
    welcomeListeners.delete(listener);
  };
}

/**
 * Subscribe to server config update events. Replays the latest update for
 * late subscribers to avoid missing config validation feedback.
 */
export function onServerConfigUpdated(
  listener: (payload: ServerConfigUpdatedPayload) => void,
): () => void {
  serverConfigUpdatedListeners.add(listener);

  const latestConfig =
    serverLifecycleTransport?.getLatestPush(WS_CHANNELS.serverConfigUpdated)?.data ?? null;
  if (latestConfig) {
    try {
      listener(latestConfig);
    } catch {
      // Swallow listener errors
    }
  }

  return () => {
    serverConfigUpdatedListeners.delete(listener);
  };
}

/**
 * Subscribe to provider status updates without forcing a full config reload.
 */
export function onServerProviderStatusesUpdated(
  listener: (payload: ServerProviderStatusesUpdatedPayload) => void,
): () => void {
  serverProviderStatusesUpdatedListeners.add(listener);

  const latestProviderStatuses =
    serverLifecycleTransport?.getLatestPush(WS_CHANNELS.serverProviderStatusesUpdated)?.data ??
    null;
  if (latestProviderStatuses) {
    try {
      listener(latestProviderStatuses);
    } catch {
      // Swallow listener errors
    }
  }

  return () => {
    serverProviderStatusesUpdatedListeners.delete(listener);
  };
}

export function onServerMaintenanceUpdated(
  listener: (payload: ServerLifecycleStreamEvent) => void,
): () => void {
  serverMaintenanceUpdatedListeners.add(listener);

  const latestMaintenance =
    serverLifecycleTransport?.getLatestPush(WS_CHANNELS.serverMaintenanceUpdated)?.data ?? null;
  if (latestMaintenance) {
    try {
      listener(latestMaintenance);
    } catch {
      // Swallow listener errors
    }
  }

  return () => {
    serverMaintenanceUpdatedListeners.delete(listener);
  };
}

export function onServerSettingsUpdated(
  listener: (payload: ServerSettingsUpdatedPayload) => void,
): () => void {
  serverSettingsUpdatedListeners.add(listener);

  const latestSettings =
    serverLifecycleTransport?.getLatestPush(WS_CHANNELS.serverSettingsUpdated)?.data ?? null;
  if (latestSettings) {
    try {
      listener(latestSettings);
    } catch {
      // Swallow listener errors
    }
  }

  return () => {
    serverSettingsUpdatedListeners.delete(listener);
  };
}

export function createWsNativeApi(): NativeApi {
  if (instance) {
    if (instance.transport.getState() !== "disposed") {
      return instance.api;
    }
    instance = null;
  }

  const transport = new WsTransport();
  transport.onStateChange((state) => emitWsTransportState(state));

  // Wire the 5 server-lifecycle channels (welcome, configUpdated,
  // providerStatusesUpdated, maintenanceUpdated, settingsUpdated) and register
  // the transport as the replay source for the exported `on*` helpers.
  bindServerLifecycleTransport(transport);
  transport.subscribe(WS_CHANNELS.gitActionProgress, (message) => {
    const payload = message.data;
    for (const listener of gitActionProgressListeners) {
      try {
        listener(payload);
      } catch {
        // Swallow listener errors
      }
    }
  });
  transport.subscribe(WS_CHANNELS.terminalEvent, (message) => {
    const payload = message.data;
    for (const listener of terminalEventListeners) {
      try {
        listener(payload);
      } catch {
        // Swallow listener errors
      }
    }
  });
  transport.subscribe(WS_CHANNELS.projectDevServerEvent, (message) => {
    const payload = message.data;
    for (const listener of projectDevServerEventListeners) {
      try {
        listener(payload);
      } catch {
        // Swallow listener errors
      }
    }
  });
  transport.subscribe(WS_CHANNELS.automationEvent, (message) => {
    const payload = message.data;
    for (const listener of automationEventListeners) {
      try {
        listener(payload);
      } catch {
        // Swallow listener errors
      }
    }
  });
  // The backend publishes every domain event on the bare `orchestration`
  // channel (`push/orchestration`); wsTransport derives `channel =
  // method.slice("push/".length)` and emits on that exact key. The previous
  // subscribes on `orchestration.domainEvent`/`.shellEvent`/`.threadEvent`
  // never matched → zero live delivery. Subscribe on the bare channel and
  // demux: snapshot frames → shell listeners; domain frames → adapt → domain
  // listeners. (The thread/shell "event" branches never fire from the real
  // backend; only snapshots route through the shell branch, best-effort.)
  transport.subscribe("orchestration", (message) => {
    const env = message.data as { eventType?: string; data?: unknown } | undefined;
    if (env && env.eventType === "snapshot") {
      const snap = env.data as
        | { scope?: string; thread?: unknown; snapshotSequence?: number }
        | undefined;
      // Thread-detail snapshot (subscribeThread) → onThreadEvent, which hydrates
      // the conversation on thread open/reload. Distinguished from the shell
      // snapshot by `scope === "thread"` (the shell snapshot carries no `scope`).
      if (snap && snap.scope === "thread" && snap.thread !== undefined) {
        for (const listener of orchestrationThreadEventListeners) {
          try {
            listener({
              kind: "snapshot",
              snapshot: {
                snapshotSequence: snap.snapshotSequence ?? 0,
                thread: snap.thread,
              },
            } as OrchestrationThreadStreamItem);
          } catch {
            // Swallow listener errors
          }
        }
        return;
      }
      // Shell snapshot → onShellEvent (sidebar bootstrap).
      for (const listener of orchestrationShellEventListeners) {
        try {
          listener({ kind: "snapshot", snapshot: env.data } as OrchestrationShellStreamItem);
        } catch {
          // Swallow listener errors
        }
      }
      return;
    }
    if (!env || typeof env.eventType !== "string") return;
    const events = adaptPushEnvelope(
      { eventType: env.eventType, aggregateId: (env as { aggregateId?: string }).aggregateId ?? null, data: env.data },
      pushAdaptCtx,
    );
    for (const event of events) {
      for (const listener of orchestrationDomainEventListeners) {
        try {
          listener(event);
        } catch {
          // Swallow listener errors
        }
      }
    }
  });
  const api: NativeApi = {
    dialogs: {
      pickFolder: async () => {
        if (!window.desktopBridge) return null;
        return window.desktopBridge.pickFolder();
      },
      saveFile: async (input) => {
        if (window.desktopBridge?.saveFile) {
          return window.desktopBridge.saveFile(input);
        }
        const blob = new Blob([input.contents], { type: "text/markdown;charset=utf-8" });
        const url = URL.createObjectURL(blob);
        try {
          const anchor = document.createElement("a");
          anchor.href = url;
          anchor.download = input.defaultFilename;
          anchor.click();
        } finally {
          URL.revokeObjectURL(url);
        }
        return null;
      },
      confirm: async (message) => {
        return showConfirmDialogFallback(message);
      },
    },
    terminal: {
      open: (input) => transport.request(WS_METHODS.terminalOpen, input),
      write: (input) => transport.request(WS_METHODS.terminalWrite, input),
      ackOutput: (input) => transport.request(WS_METHODS.terminalAckOutput, input),
      resize: (input) => transport.request(WS_METHODS.terminalResize, input),
      clear: (input) => transport.request(WS_METHODS.terminalClear, input),
      restart: (input) => transport.request(WS_METHODS.terminalRestart, input),
      close: (input) => transport.request(WS_METHODS.terminalClose, input),
      onEvent: (callback) => {
        terminalEventListeners.add(callback);
        return () => {
          terminalEventListeners.delete(callback);
        };
      },
    },
    projects: {
      // Register a folder as a backend project (ProjectCreated event). Used by
      // the "Work in a project" picker so a selected folder persists as a real
      // sidebar project. Params: { name, rootPath } (absolute).
      create: (input) => transport.request(WS_METHODS.projectCreate, input),
      discoverScripts: (input) => transport.request(WS_METHODS.projectsDiscoverScripts, input),
      listDirectories: (input) => transport.request(WS_METHODS.projectsListDirectories, input),
      searchEntries: (input) => transport.request(WS_METHODS.projectsSearchEntries, input),
      searchLocalEntries: (input) =>
        transport.request(WS_METHODS.projectsSearchLocalEntries, input),
      readFile: (input) => transport.request(WS_METHODS.projectsReadFile, input),
      writeFile: (input) => transport.request(WS_METHODS.projectsWriteFile, input),
      runDevServer: (input) => transport.request(WS_METHODS.projectsRunDevServer, input),
      stopDevServer: (input) => transport.request(WS_METHODS.projectsStopDevServer, input),
      listDevServers: () => transport.request(WS_METHODS.projectsListDevServers),
      onDevServerEvent: (callback) => {
        projectDevServerEventListeners.add(callback);
        return () => {
          projectDevServerEventListeners.delete(callback);
        };
      },
    },
    filesystem: {
      browse: (input) => transport.request(WS_METHODS.filesystemBrowse, input),
    },
    shell: {
      openInEditor: (cwd, editor) =>
        transport.request(WS_METHODS.shellOpenInEditor, { cwd, editor }),
      openExternal: async (url) => {
        if (window.desktopBridge) {
          const opened = await window.desktopBridge.openExternal(url);
          if (!opened) {
            throw new Error("Unable to open link.");
          }
          return;
        }

        // Some mobile browsers can return null here even when the tab opens.
        // Avoid false negatives and let the browser handle popup policy.
        window.open(url, "_blank", "noopener,noreferrer");
      },
      showInFolder: async (path) => {
        if (window.desktopBridge) {
          await window.desktopBridge.showInFolder(path);
          return;
        }
        // Browser/dev fallback: ask the server to open the folder in the OS
        // file manager via the existing shellOpenInEditor RPC with the
        // "file-manager" pseudo-editor (resolves to explorer/open/xdg-open).
        await transport.request(WS_METHODS.shellOpenInEditor, {
          cwd: path,
          editor: "file-manager",
        });
      },
    },
    git: {
      githubRepository: (input) => transport.request(WS_METHODS.gitGithubRepository, input),
      pull: (input) => transport.request(WS_METHODS.gitPull, input),
      status: (input) => transport.request(WS_METHODS.gitStatus, input),
      readWorkingTreeDiff: (input) => transport.request(WS_METHODS.gitReadWorkingTreeDiff, input),
      summarizeDiff: (input) =>
        transport.request(WS_METHODS.gitSummarizeDiff, input, {
          timeoutMs: null,
        }),
      runStackedAction: (input) =>
        transport.request(WS_METHODS.gitRunStackedAction, input, {
          timeoutMs: null,
        }),
      listBranches: (input) => transport.request(WS_METHODS.gitListBranches, input),
      createWorktree: (input) => transport.request(WS_METHODS.gitCreateWorktree, input),
      createDetachedWorktree: (input) =>
        transport.request(WS_METHODS.gitCreateDetachedWorktree, input),
      removeWorktree: (input) => transport.request(WS_METHODS.gitRemoveWorktree, input),
      createBranch: (input) => transport.request(WS_METHODS.gitCreateBranch, input),
      checkout: (input) => transport.request(WS_METHODS.gitCheckout, input),
      stashAndCheckout: (input) => transport.request(WS_METHODS.gitStashAndCheckout, input),
      stashDrop: (input) => transport.request(WS_METHODS.gitStashDrop, input),
      stashInfo: (input) => transport.request(WS_METHODS.gitStashInfo, input),
      removeIndexLock: (input) => transport.request(WS_METHODS.gitRemoveIndexLock, input),
      init: (input) => transport.request(WS_METHODS.gitInit, input),
      stageFiles: (input) => transport.request(WS_METHODS.gitStageFiles, input),
      unstageFiles: (input) => transport.request(WS_METHODS.gitUnstageFiles, input),
      handoffThread: (input) => transport.request(WS_METHODS.gitHandoffThread, input),
      resolvePullRequest: (input) => transport.request(WS_METHODS.gitResolvePullRequest, input),
      preparePullRequestThread: (input) =>
        transport.request(WS_METHODS.gitPreparePullRequestThread, input),
      onActionProgress: (callback) => {
        gitActionProgressListeners.add(callback);
        return () => {
          gitActionProgressListeners.delete(callback);
        };
      },
    },
    contextMenu: {
      show: async <T extends string>(
        items: readonly ContextMenuItem<T>[],
        position?: { x: number; y: number },
      ): Promise<T | null> => {
        if (window.desktopBridge) {
          return window.desktopBridge.showContextMenu(items, position);
        }
        return showContextMenuFallback(items, position);
      },
    },
    server: {
      getConfig: () => transport.request(WS_METHODS.serverGetConfig),
      getEnvironment: () => transport.request(WS_METHODS.serverGetEnvironment),
      getSettings: () => transport.request(WS_METHODS.serverGetSettings),
      updateSettings: (input) => transport.request(WS_METHODS.serverUpdateSettings, input),
      getAuthSession: () => requestAuthJson<AuthSessionState>("/api/auth/session"),
      bootstrapAuth: (input: AuthBootstrapInput) =>
        requestAuthJson<AuthBootstrapResult>("/api/auth/bootstrap", {
          method: "POST",
          body: input,
        }),
      bootstrapBearerAuth: (input: AuthBootstrapInput) =>
        requestAuthJson<AuthBearerBootstrapResult>("/api/auth/bootstrap/bearer", {
          method: "POST",
          body: input,
        }),
      issueAuthWebSocketToken: () =>
        requestAuthJson<AuthWebSocketTokenResult>("/api/auth/ws-token", { method: "POST" }),
      createAuthPairingToken: (input?: AuthCreatePairingCredentialInput) =>
        requestAuthJson<AuthPairingCredentialResult>("/api/auth/pairing-token", {
          method: "POST",
          ...(input ? { body: input } : {}),
        }),
      listAuthPairingLinks: () =>
        requestAuthJson<ReadonlyArray<AuthPairingLink>>("/api/auth/pairing-links"),
      revokeAuthPairingLink: (input: AuthRevokePairingLinkInput) =>
        requestAuthJson<{ revoked: boolean }>("/api/auth/pairing-links/revoke", {
          method: "POST",
          body: input,
        }),
      listAuthClients: () => requestAuthJson<ReadonlyArray<AuthClientSession>>("/api/auth/clients"),
      revokeAuthClient: (input: AuthRevokeClientSessionInput) =>
        requestAuthJson<{ revoked: boolean }>("/api/auth/clients/revoke", {
          method: "POST",
          body: input,
        }),
      revokeOtherAuthClients: () =>
        requestAuthJson<{ revokedCount: number }>("/api/auth/clients/revoke-others", {
          method: "POST",
        }),
      refreshProviders: () => transport.request(WS_METHODS.serverRefreshProviders),
      updateProvider: (input) => transport.request(WS_METHODS.serverUpdateProvider, input),
      listWorktrees: () => transport.request(WS_METHODS.serverListWorktrees),
      listLocalServers: () => transport.request(WS_METHODS.serverListLocalServers),
      stopLocalServer: (input) => transport.request(WS_METHODS.serverStopLocalServer, input),
      getProviderUsageSnapshot: (input) =>
        transport.request(WS_METHODS.serverGetProviderUsageSnapshot, input),
      listProviderUsage: (input) => transport.request(WS_METHODS.serverListProviderUsage, input),
      getDiagnostics: () => transport.request(WS_METHODS.serverGetDiagnostics),
      generateThreadRecap: (input) =>
        transport.request(WS_METHODS.serverGenerateThreadRecap, input, {
          timeoutMs: null,
        }),
      generateAutomationIntent: (input) =>
        transport.request(WS_METHODS.serverGenerateAutomationIntent, input, {
          timeoutMs: null,
        }),
      transcribeVoice: (input) => {
        if (window.desktopBridge?.server?.transcribeVoice) {
          return window.desktopBridge.server.transcribeVoice(input);
        }
        return transport.request(WS_METHODS.serverTranscribeVoice, input);
      },
      upsertKeybinding: (input) => transport.request(WS_METHODS.serverUpsertKeybinding, input),
    },
    stats: {
      getProfileStats: (input) => transport.request(WS_METHODS.statsGetProfileStats, input),
      getProfileTokenStats: (input) =>
        transport.request(WS_METHODS.statsGetProfileTokenStats, input),
    },
    provider: {
      getComposerCapabilities: (input) =>
        transport.request(WS_METHODS.providerGetComposerCapabilities, input),
      compactThread: (input) => transport.request(WS_METHODS.providerCompactThread, input),
      listCommands: (input) => transport.request(WS_METHODS.providerListCommands, input),
      listSkills: (input) => transport.request(WS_METHODS.providerListSkills, input),
      listSkillsCatalog: (input) => transport.request(WS_METHODS.providerListSkillsCatalog, input),
      listPlugins: (input) => transport.request(WS_METHODS.providerListPlugins, input),
      readPlugin: (input) => transport.request(WS_METHODS.providerReadPlugin, input),
      listModels: (input) => transport.request(WS_METHODS.providerListModels, input),
      listAgents: (input) => transport.request(WS_METHODS.providerListAgents, input),
      listMcpCatalog: (input) => transport.request(WS_METHODS.providerListMcpCatalog, input),
    },
    mcp: {
      create: (input) => transport.request(WS_METHODS.mcpCreate, input),
      update: (input) => transport.request(WS_METHODS.mcpUpdate, input),
      delete: (input) => transport.request(WS_METHODS.mcpDelete, input),
      testConnection: (input) => transport.request(WS_METHODS.mcpTestConnection, input),
    },
    orchestration: {
      getSnapshot: () => transport.request(ORCHESTRATION_WS_METHODS.getSnapshot),
      getShellSnapshot: () => transport.request(ORCHESTRATION_WS_METHODS.getShellSnapshot),
      dispatchCommand: (command) => {
        return transport.request(ORCHESTRATION_WS_METHODS.dispatchCommand, {
          command: omitNullUserInputAnswers(command),
        });
      },
      importThread: (input) => transport.request(ORCHESTRATION_WS_METHODS.importThread, input),
      repairState: () => transport.request(ORCHESTRATION_WS_METHODS.repairState),
      getTurnDiff: (input) => transport.request(ORCHESTRATION_WS_METHODS.getTurnDiff, input),
      getFullThreadDiff: (input) =>
        transport.request(ORCHESTRATION_WS_METHODS.getFullThreadDiff, input),
      replayEvents: (fromSequenceExclusive) =>
        transport.request(ORCHESTRATION_WS_METHODS.replayEvents, {
          fromSequenceExclusive,
        }),
      subscribeShell: () => transport.request<void>(ORCHESTRATION_WS_METHODS.subscribeShell, {}),
      unsubscribeShell: () =>
        transport.request<void>(ORCHESTRATION_WS_METHODS.unsubscribeShell, {}),
      subscribeThread: (input) =>
        transport.request<void>(ORCHESTRATION_WS_METHODS.subscribeThread, input),
      unsubscribeThread: (input) =>
        transport.request<void>(ORCHESTRATION_WS_METHODS.unsubscribeThread, input),
      onDomainEvent: (callback) => {
        orchestrationDomainEventListeners.add(callback);
        return () => {
          orchestrationDomainEventListeners.delete(callback);
        };
      },
      onShellEvent: (callback) => {
        orchestrationShellEventListeners.add(callback);
        return () => {
          orchestrationShellEventListeners.delete(callback);
        };
      },
      onThreadEvent: (callback) => {
        orchestrationThreadEventListeners.add(callback);
        return () => {
          orchestrationThreadEventListeners.delete(callback);
        };
      },
    },
    automation: {
      list: (input) => transport.request(WS_METHODS.automationList, input),
      create: (input) => transport.request(WS_METHODS.automationCreate, input),
      update: (input) => transport.request(WS_METHODS.automationUpdate, input),
      delete: (input) => transport.request(WS_METHODS.automationDelete, input),
      runNow: (input) => transport.request(WS_METHODS.automationRunNow, input),
      cancelRun: (input) => transport.request(WS_METHODS.automationCancelRun, input),
      markRunRead: (input) => transport.request(WS_METHODS.automationMarkRunRead, input),
      archiveRun: (input) => transport.request(WS_METHODS.automationArchiveRun, input),
      onEvent: (callback) => {
        automationEventListeners.add(callback);
        return () => {
          automationEventListeners.delete(callback);
        };
      },
    },
    browser: {
      open: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.open(input);
        }
        const state = ensureFallbackBrowserWorkspace(input.threadId);
        if (input.initialUrl && state.tabs.length > 0) {
          const activeTab = resolveFallbackBrowserTab(state);
          activeTab.url = input.initialUrl;
          activeTab.title = defaultBrowserTitle(input.initialUrl);
          activeTab.lastCommittedUrl = input.initialUrl;
        }
        markFallbackBrowserStateChanged(state);
        return emitFallbackBrowserState(input.threadId);
      },
      close: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.close(input);
        }
        const state = getFallbackBrowserState(input.threadId);
        state.open = false;
        state.activeTabId = null;
        state.tabs = [];
        state.lastError = null;
        markFallbackBrowserStateChanged(state);
        return emitFallbackBrowserState(input.threadId);
      },
      hide: async (input) => {
        if (window.desktopBridge) {
          await window.desktopBridge.browser.hide(input);
        }
      },
      getState: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.getState(input);
        }
        return cloneBrowserState(getFallbackBrowserState(input.threadId));
      },
      setPanelBounds: async (input) => {
        if (window.desktopBridge) {
          await window.desktopBridge.browser.setPanelBounds(input);
          return;
        }
      },
      attachWebview: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.attachWebview(input);
        }
        return cloneBrowserState(getFallbackBrowserState(input.threadId));
      },
      detachWebview: async (input) => {
        if (window.desktopBridge) {
          await window.desktopBridge.browser.detachWebview(input);
        }
      },
      copyLink: async (input) => {
        if (window.desktopBridge) {
          await window.desktopBridge.browser.copyLink(input);
          return;
        }
        throw new Error("Copying the browser link requires the desktop app.");
      },
      copyScreenshotToClipboard: async (input) => {
        if (window.desktopBridge) {
          await window.desktopBridge.browser.copyScreenshotToClipboard(input);
          return;
        }
        throw new Error("Browser screenshots require the desktop app.");
      },
      captureScreenshot: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.captureScreenshot(input);
        }
        throw new Error("Browser screenshots require the desktop app.");
      },
      executeCdp: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.executeCdp(input);
        }
        throw new Error("Browser automation requires the desktop app.");
      },
      navigate: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.navigate(input);
        }
        const state = ensureFallbackBrowserWorkspace(input.threadId);
        const tab = resolveFallbackBrowserTab(state, input.tabId);
        tab.url = input.url;
        tab.title = defaultBrowserTitle(input.url);
        tab.lastCommittedUrl = input.url;
        tab.lastError = null;
        tab.status = "live";
        state.activeTabId = tab.id;
        markFallbackBrowserStateChanged(state);
        return emitFallbackBrowserState(input.threadId);
      },
      reload: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.reload(input);
        }
        return cloneBrowserState(getFallbackBrowserState(input.threadId));
      },
      goBack: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.goBack(input);
        }
        return cloneBrowserState(getFallbackBrowserState(input.threadId));
      },
      goForward: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.goForward(input);
        }
        return cloneBrowserState(getFallbackBrowserState(input.threadId));
      },
      newTab: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.newTab(input);
        }
        const state = ensureFallbackBrowserWorkspace(input.threadId);
        const tab = createFallbackTab(input.url);
        state.tabs = [...state.tabs, tab];
        if (input.activate !== false || !state.activeTabId) {
          state.activeTabId = tab.id;
        }
        markFallbackBrowserStateChanged(state);
        return emitFallbackBrowserState(input.threadId);
      },
      closeTab: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.closeTab(input);
        }
        const state = ensureFallbackBrowserWorkspace(input.threadId);
        const nextTabs = state.tabs.filter((tab) => tab.id !== input.tabId);
        if (nextTabs.length === state.tabs.length) {
          return cloneBrowserState(state);
        }
        state.tabs = nextTabs;
        if (nextTabs.length === 0) {
          const replacementTab = createFallbackTab();
          state.tabs = [replacementTab];
          state.activeTabId = replacementTab.id;
          state.lastError = null;
        } else if (!state.tabs.some((tab) => tab.id === state.activeTabId)) {
          state.activeTabId = state.tabs[0]?.id ?? null;
        }
        markFallbackBrowserStateChanged(state);
        return emitFallbackBrowserState(input.threadId);
      },
      selectTab: async (input) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.selectTab(input);
        }
        const state = ensureFallbackBrowserWorkspace(input.threadId);
        const tab = resolveFallbackBrowserTab(state, input.tabId);
        state.activeTabId = tab.id;
        markFallbackBrowserStateChanged(state);
        return emitFallbackBrowserState(input.threadId);
      },
      openDevTools: async (input) => {
        if (window.desktopBridge) {
          await window.desktopBridge.browser.openDevTools(input);
        }
      },
      onState: (callback) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.onState(callback);
        }
        fallbackBrowserStateListeners.add(callback);
        return () => {
          fallbackBrowserStateListeners.delete(callback);
        };
      },
      onCopyLink: (callback) => {
        if (window.desktopBridge) {
          return window.desktopBridge.browser.onBrowserCopyLink(callback);
        }
        return () => {};
      },
    },
  };

  instance = { api, transport };
  return api;
}

// Browser-mode tests mount full app roots repeatedly in one page; reset the
// singleton so each test gets a fresh WebSocket stream and cached push state.
export function resetWsNativeApiForTest(): void {
  instance?.transport.dispose();
  instance = null;
  serverLifecycleTransport = null;
  welcomeListeners.clear();
  serverConfigUpdatedListeners.clear();
  serverProviderStatusesUpdatedListeners.clear();
  serverMaintenanceUpdatedListeners.clear();
  serverSettingsUpdatedListeners.clear();
  gitActionProgressListeners.clear();
  terminalEventListeners.clear();
  projectDevServerEventListeners.clear();
  automationEventListeners.clear();
  orchestrationDomainEventListeners.clear();
  orchestrationShellEventListeners.clear();
  orchestrationThreadEventListeners.clear();
  fallbackBrowserStateListeners.clear();
  fallbackBrowserStates.clear();
}

if (import.meta.hot) {
  import.meta.hot.dispose(() => {
    instance?.transport.dispose();
    instance = null;
    serverLifecycleTransport = null;
    welcomeListeners.clear();
    serverConfigUpdatedListeners.clear();
    serverProviderStatusesUpdatedListeners.clear();
    serverSettingsUpdatedListeners.clear();
    gitActionProgressListeners.clear();
    terminalEventListeners.clear();
    projectDevServerEventListeners.clear();
    orchestrationDomainEventListeners.clear();
    orchestrationShellEventListeners.clear();
    orchestrationThreadEventListeners.clear();
    fallbackBrowserStateListeners.clear();
  });
}

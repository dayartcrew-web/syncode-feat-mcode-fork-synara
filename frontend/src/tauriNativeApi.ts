/**
 * Tauri-backed `NativeApi` implementation (B4 / "T6" shell swap).
 *
 * Implements the boot-critical subset of the `NativeApi` interface over
 * `@tauri-apps/api` + existing syncode-tauri `invoke` commands, and stubs
 * Electron-only capabilities with no Tauri equivalent (embedded browser
 * webview panels, CDP, attach/detach webview, desktop Chrome) to a typed
 * `UnsupportedError`. See `docs/SHELL-GAPS.md` for the full gap list.
 *
 * Mapping strategy (see the method→command table in `SHELL-GAPS.md`):
 *  - Window/theme/notifications: `@tauri-apps/api` direct JS APIs
 *    (`getCurrentWindow`, `Window.theme`, browser `Notification`).
 *  - Git ops: existing syncode-tauri `git_*` commands via `invoke`.
 *  - Terminal PTY: existing syncode-tauri `terminal_*` commands via `invoke`.
 *  - Server/provider/orchestration/automation/stats/projects/filesystem: these
 *    are NOT shell capabilities — they are served by the backend over the
 *    JSON-RPC WebSocket transport (T5). The `wsNativeApi` adapter delegates
 *    those to `WsTransport`; this Tauri impl routes them through an injected
 *    `transport` callback so the same Rust handlers serve both modes.
 *    If no transport is wired, callers get `UnsupportedError` ("requires ws
 *    transport") — the shell still boots (window/dialogs/shell/git/terminal).
 *  - Browser panels / CDP: `UnsupportedError` (Electron-only; Tauri has no
 *    embedded Chromium webview panel API).
 *
 * Detection: `isTauri()` from `@tauri-apps/api/core` is the gate. The factory
 * in `nativeApi.ts` prefers this impl when `isTauri()` returns true and no
 * preloaded `window.nativeApi` exists.
 */

import { invoke, isTauri } from "@tauri-apps/api/core";
import { getCurrentWindow } from "@tauri-apps/api/window";
import type { UnlistenFn } from "@tauri-apps/api/event";

import { adaptPushEnvelope, createPushAdaptContext } from "./contracts/adaptPushEvent";
import { WS_CHANNELS } from "./contracts/tier3/ws";

import type {
  AuthBootstrapInput,
  AuthBootstrapResult,
  AuthBearerBootstrapResult,
  AuthCreatePairingCredentialInput,
  AuthClientSession,
  AuthPairingCredentialResult,
  AuthPairingLink,
  AuthRevokeClientSessionInput,
  AuthRevokePairingLinkInput,
  AuthSessionState,
  AuthWebSocketTokenResult,
  AutomationArchiveRunInput,
  AutomationCancelRunInput,
  AutomationCancelRunResult,
  AutomationCreateInput,
  AutomationDefinition,
  AutomationDeleteInput,
  AutomationListInput,
  AutomationListResult,
  AutomationMarkRunReadInput,
  AutomationRunActionResult,
  AutomationRunNowInput,
  AutomationRunNowResult,
  AutomationStreamEvent,
  AutomationUpdateInput,
  BrowserAttachWebviewInput,
  BrowserCaptureScreenshotResult,
  BrowserCopyLinkEvent,
  BrowserDetachWebviewInput,
  BrowserExecuteCdpInput,
  BrowserNavigateInput,
  BrowserNewTabInput,
  BrowserOpenInput,
  BrowserSetPanelBoundsInput,
  BrowserTabInput,
  BrowserThreadInput,
  ClientOrchestrationCommand,
  ContextMenuItem,
  DesktopNotificationInput,
  EditorId,
  FilesystemBrowseInput,
  FilesystemBrowseResult,
  GitActionProgressEvent,
  GitHubRepositoryInput,
  GitHubRepositoryResult,
  GitCheckoutInput,
  GitCreateBranchInput,
  GitCreateDetachedWorktreeInput,
  GitCreateDetachedWorktreeResult,
  GitCreateWorktreeInput,
  GitCreateWorktreeResult,
  GitHandoffThreadInput,
  GitHandoffThreadResult,
  GitInitInput,
  GitListBranchesInput,
  GitListBranchesResult,
  GitPreparePullRequestThreadInput,
  GitPreparePullRequestThreadResult,
  GitPullInput,
  GitPullResult,
  GitPullRequestRefInput,
  GitReadWorkingTreeDiffInput,
  GitReadWorkingTreeDiffResult,
  GitRemoveIndexLockInput,
  GitRemoveWorktreeInput,
  GitResolvePullRequestResult,
  GitRunStackedActionInput,
  GitRunStackedActionResult,
  GitStageFilesInput,
  GitStageFilesResult,
  GitStashAndCheckoutInput,
  GitStashDropInput,
  GitStashInfoInput,
  GitStashInfoResult,
  GitStatusInput,
  GitStatusResult,
  GitSummarizeDiffInput,
  GitSummarizeDiffResult,
  GitUnstageFilesInput,
  GitUnstageFilesResult,
  NativeApi,
  OrchestrationEvent,
  OrchestrationGetFullThreadDiffInput,
  OrchestrationGetFullThreadDiffResult,
  OrchestrationGetTurnDiffInput,
  OrchestrationGetTurnDiffResult,
  OrchestrationImportThreadInput,
  OrchestrationImportThreadResult,
  OrchestrationReadModel,
  OrchestrationShellSnapshot,
  OrchestrationShellStreamItem,
  OrchestrationSubscribeThreadInput,
  OrchestrationThreadStreamItem,
  ProjectDevServerEvent,
  ProjectCreateParams,
  ProjectDiscoverScriptsInput,
  ProjectDiscoverScriptsResult,
  ProjectListDevServersResult,
  ProjectListDirectoriesInput,
  ProjectListDirectoriesResult,
  ProjectReadFileInput,
  ProjectReadFileResult,
  ProjectRunDevServerInput,
  ProjectRunDevServerResult,
  ProjectSearchEntriesInput,
  ProjectSearchEntriesResult,
  ProjectSearchLocalEntriesInput,
  ProjectSearchLocalEntriesResult,
  ProjectSummary,
  ProjectStopDevServerInput,
  ProjectStopDevServerResult,
  ProjectWriteFileInput,
  ProjectWriteFileResult,
  ProviderCompactThreadInput,
  ProviderComposerCapabilities,
  ProviderGetComposerCapabilitiesInput,
  ProviderListAgentsInput,
  ProviderListAgentsResult,
  ProviderListMcpCatalogInput,
  ProviderListCommandsInput,
  ProviderListCommandsResult,
  ProviderListModelsInput,
  ProviderListModelsResult,
  McpCatalogResponse,
  McpCreateResult,
  McpDeleteInput,
  McpDeleteResult,
  McpServerInput,
  McpTestConnectionInput,
  McpTestConnectionResult,
  McpUpdateInput,
  McpUpdateResult,
  ProviderListPluginsInput,
  ProviderListPluginsResult,
  ProviderListSkillsInput,
  ProviderListSkillsResult,
  ProviderReadPluginInput,
  ProviderReadPluginResult,
  ProviderSkillsCatalogInput,
  ProviderSkillsCatalogResult,
  ServerConfig,
  ServerDiagnosticsResult,
  ServerGenerateAutomationIntentInput,
  ServerGenerateAutomationIntentResult,
  ServerGenerateThreadRecapInput,
  ServerGenerateThreadRecapResult,
  ServerGetEnvironmentResult,
  ServerGetProviderUsageSnapshotInput,
  ServerGetProviderUsageSnapshotResult,
  ServerGetSettingsResult,
  ServerListLocalServersResult,
  ServerListProviderUsageInput,
  ServerListProviderUsageResult,
  ServerListWorktreesResult,
  ServerProviderUpdateInput,
  ServerProviderUpdateResult,
  ServerRefreshProvidersResult,
  ServerStopLocalServerInput,
  ServerStopLocalServerResult,
  ServerUpdateSettingsInput,
  ServerUpdateSettingsResult,
  ServerUpsertKeybindingInput,
  ServerUpsertKeybindingResult,
  ServerVoiceTranscriptionInput,
  ServerVoiceTranscriptionResult,
  StatsGetProfileStatsInput,
  StatsGetProfileStatsResult,
  StatsGetProfileTokenStatsInput,
  StatsGetProfileTokenStatsResult,
  TerminalAckOutputInput,
  TerminalCloseInput,
  TerminalEvent,
  TerminalOpenInput,
  TerminalResizeInput,
  TerminalRestartInput,
  TerminalSessionSnapshot,
  TerminalWriteInput,
  ThreadBrowserState,
} from "@t3tools/contracts";

// ─── Auth types (imported from contracts shell.ts supporting aliases) ───

/**
 * Typed error raised for capabilities Tauri cannot serve. UI can detect this
 * via `instanceof UnsupportedError` and feature-gate the corresponding surface.
 */
export class UnsupportedError extends Error {
  readonly capability: string;
  readonly reason: string;

  constructor(capability: string, reason: string) {
    super(`Unsupported capability "${capability}": ${reason}`);
    this.name = "UnsupportedError";
    this.capability = capability;
    this.reason = reason;
  }
}

function unsupported<T>(capability: string, reason: string): Promise<T> {
  return Promise.reject(new UnsupportedError(capability, reason));
}

/**
 * Optional transport hook for the WS-routed NativeApi surfaces
 * (server/provider/orchestration/automation/stats/projects/filesystem).
 * When omitted, those methods reject with `UnsupportedError("ws-transport",
 * "requires JSON-RPC WebSocket transport")`. The `wsNativeApi` adapter
 * supplies this hook by delegating to its `WsTransport` instance.
 *
 * `subscribe` is optional: when present, `createTauriNativeApi` registers a
 * one-time push demux (mirroring `wsNativeApi`) so live push frames from the
 * embedded desktop WS server reach `onDomainEvent`/`onShellEvent`/
 * `onThreadEvent`/`onEvent`/`onActionProgress`/`onDevServerEvent`. Without
 * it, those subscriptions stay no-ops (the desktop shell receives no live
 * push and chat appears stuck).
 */
export interface TransportDispatcher {
  call: <R = unknown>(method: string, params?: unknown) => Promise<R>;
  subscribe?: (
    channel: string,
    listener: (message: { readonly data: unknown }) => void,
  ) => () => void;
}

const NO_TRANSPORT_REASON =
  "requires JSON-RPC WebSocket transport — wire createTauriNativeApi({ transport }) from wsNativeApi";

// ─── Push delivery (mirrors wsNativeApi demux) ───────────────────────────
//
// The desktop shell boots its own embedded axum WS server
// (`crates/syncode-tauri/src/ws_setup.rs`, default 127.0.0.1:30101) and the
// frontend reaches it via the SAME `WsTransport` the browser uses. Push
// frames therefore arrive on `WsTransport`'s channel-keyed subscribe surface
// — this module demuxes them exactly like `wsNativeApi.ts` and routes to the
// per-surface listener Sets consumed by the NativeApi callbacks.
//
// Listener sets are module-level so a single demux registration (per transport
// lifetime) fans out to every callback added via the returned `api`.

const orchestrationDomainEventListeners = new Set<(event: OrchestrationEvent) => void>();
const orchestrationShellEventListeners = new Set<
  (event: OrchestrationShellStreamItem) => void
>();
const orchestrationThreadEventListeners = new Set<
  (event: OrchestrationThreadStreamItem) => void
>();
const terminalEventListeners = new Set<(event: TerminalEvent) => void>();
const gitActionProgressListeners = new Set<(event: GitActionProgressEvent) => void>();
const automationEventListeners = new Set<(event: AutomationStreamEvent) => void>();
const projectDevServerEventListeners = new Set<(event: ProjectDevServerEvent) => void>();

// Per-connection push adapter context (resets on page reload; reconnect
// mid-turn is a known edge case — see adaptPushEvent.ts risks). Mirrors
// wsNativeApi.
const pushAdaptCtx = createPushAdaptContext();

// Guard so the demux is wired at most once per transport (idempotent across
// repeat createTauriNativeApi calls during hot reload / singleton reset).
let demuxRegistered = false;

function fanout<T>(listeners: Set<(event: T) => void>, event: T): void {
  for (const listener of listeners) {
    try {
      listener(event);
    } catch {
      // Swallow listener errors so one bad subscriber can't break the demux.
    }
  }
}

/**
 * Register the push channel demux on the supplied transport. Idempotent —
 * repeated calls with the same (or a new) transport are a no-op once the
 * demux is wired. Mirrors the channel subscription block in
 * `wsNativeApi.ts::createWsNativeApi` exactly (same channels, same
 * snapshot/domain demux on the bare `orchestration` channel, same
 * `adaptPushEnvelope` for domain events).
 */
function registerPushDemux(transport: TransportDispatcher): void {
  if (demuxRegistered || !transport.subscribe) return;
  demuxRegistered = true;

  const subscribe = transport.subscribe.bind(transport);

  subscribe(WS_CHANNELS.terminalEvent, (message) => {
    fanout(terminalEventListeners, message.data as TerminalEvent);
  });
  subscribe(WS_CHANNELS.gitActionProgress, (message) => {
    fanout(gitActionProgressListeners, message.data as GitActionProgressEvent);
  });
  subscribe(WS_CHANNELS.projectDevServerEvent, (message) => {
    fanout(projectDevServerEventListeners, message.data as ProjectDevServerEvent);
  });
  subscribe(WS_CHANNELS.automationEvent, (message) => {
    fanout(automationEventListeners, message.data as AutomationStreamEvent);
  });

  // The backend publishes every domain event on the bare `orchestration`
  // channel (`push/orchestration`); wsTransport emits on that exact key.
  // Demux: snapshot frames → shell/thread listeners; domain frames → adapt
  // → domain listeners. Mirrors wsNativeApi's branch logic verbatim.
  subscribe("orchestration", (message) => {
    const env = message.data as { eventType?: string; data?: unknown } | undefined;
    if (env && env.eventType === "snapshot") {
      const snap = env.data as
        | { scope?: string; thread?: unknown; snapshotSequence?: number }
        | undefined;
      // Thread-detail snapshot (subscribeThread) → onThreadEvent (hydrates
      // the conversation on thread open/reload). Distinguished from the
      // shell snapshot by `scope === "thread"`.
      if (snap && snap.scope === "thread" && snap.thread !== undefined) {
        fanout(orchestrationThreadEventListeners, {
          kind: "snapshot",
          snapshot: {
            snapshotSequence: snap.snapshotSequence ?? 0,
            thread: snap.thread,
          },
        } as OrchestrationThreadStreamItem);
        return;
      }
      // Shell snapshot → onShellEvent (sidebar bootstrap).
      fanout(orchestrationShellEventListeners, {
        kind: "snapshot",
        snapshot: env.data,
      } as OrchestrationShellStreamItem);
      return;
    }
    if (!env || typeof env.eventType !== "string") return;
    const events = adaptPushEnvelope(
      {
        eventType: env.eventType,
        aggregateId: (env as { aggregateId?: string }).aggregateId ?? null,
        data: env.data,
      },
      pushAdaptCtx,
    );
    for (const event of events) {
      fanout(orchestrationDomainEventListeners, event);
    }
  });
}

function noTransport<T>(): Promise<T> {
  return unsupported<T>("ws-transport", NO_TRANSPORT_REASON);
}

// ─── Window state helpers ────────────────────────────────────────────────

async function getWindowState(): Promise<{
  isMaximized: boolean;
  isFullscreen: boolean;
}> {
  const win = getCurrentWindow();
  const [isMaximized, isFullscreen] = await Promise.all([
    win.isMaximized(),
    win.isFullscreen(),
  ]);
  return { isMaximized, isFullscreen };
}

function noopUnsubscribe(): () => void {
  return () => {};
}

// ─── Browser fallback state (in-renderer, since Tauri has no webview panel) ──

function defaultBrowserState(threadId: BrowserOpenInput["threadId"]): ThreadBrowserState {
  return {
    threadId,
    version: 0,
    open: false,
    activeTabId: null,
    tabs: [],
    lastError: null,
  };
}

/**
 * Construct a Tauri-backed `NativeApi`. Boot-critical shell surfaces
 * (dialogs/shell/git/terminal/notifications/window) are wired; WS-routed
 * surfaces delegate to the optional `transport`. Browser panels reject with
 * `UnsupportedError`.
 *
 * @param transport Optional JSON-RPC transport for server-side surfaces.
 */
export function createTauriNativeApi(
  transport: TransportDispatcher | null = null,
): NativeApi {
  const callTransport = <R>(method: string, params?: unknown): Promise<R> => {
    if (!transport) return noTransport<R>();
    return transport.call<R>(method, params);
  };

  // Wire the one-time push demux if the transport exposes `subscribe`. Until
  // a transport is supplied the push callbacks below stay no-op (chat stuck).
  if (transport) {
    registerPushDemux(transport);
  }

  const api: NativeApi = makeTauriNativeApi(callTransport);

  return api;
}

/**
 * Build the NativeApi surface over the supplied transport dispatcher. Split
 * from `createTauriNativeApi` so the demux registration stays co-located
 * with the listener Sets above and the surface assembly reads as a flat
 * block (no nested-function indentation drift).
 */
function makeTauriNativeApi(
  callTransport: <R>(method: string, params?: unknown) => Promise<R>,
): NativeApi {
  const api: NativeApi = {
    // ─── dialogs ────────────────────────────────────────────────────────
    dialogs: {
      // Tauri v2 has @tauri-apps/plugin-dialog but it isn't installed; we use
      // the HTML <input type="file"> fallback for folder picking in the
      // webview until the dialog plugin is wired. This keeps the shell booting.
      pickFolder: async () => {
        // Feature-detect the webkitdirectory input fallback.
        if (typeof document === "undefined") return null;
        return new Promise<string | null>((resolve) => {
          const input = document.createElement("input");
          input.type = "file";
          // webkitdirectory is non-standard but works in Tauri webview + browsers.
          input.setAttribute("webkitdirectory", "");
          input.setAttribute("directory", "");
          input.onchange = () => {
            const file = input.files?.[0];
            // webkitRelativePath gives the selected folder path.
            const rel = file?.webkitRelativePath ?? "";
            const folder = rel.includes("/") ? rel.split("/")[0] : rel;
            resolve(folder || null);
          };
          // If the user cancels, change never fires; we resolve null on blur.
          input.addEventListener("cancel", () => resolve(null));
          input.click();
        });
      },
      saveFile: async (input) => {
        if (typeof document === "undefined") return null;
        // Fallback: trigger a download with the default filename.
        const blob = new Blob([input.contents], { type: "text/plain" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = input.defaultFilename;
        a.click();
        URL.revokeObjectURL(url);
        return input.defaultFilename;
      },
      confirm: async (message: string) => {
        // window.confirm works in the Tauri webview.
        if (typeof window === "undefined" || typeof window.confirm !== "function") {
          return false;
        }
        return window.confirm(message);
      },
    },

    // ─── terminal (existing syncode-tauri terminal_* commands) ──────────
    terminal: {
      open: async (input: TerminalOpenInput) => {
        // MCode input is opaque here; the syncode-tauri command takes
        // (command, args, workingDir, cols, rows). We extract by best-effort.
        const params = input as unknown as {
          command?: string;
          args?: string[];
          cwd?: string;
          cols?: number;
          rows?: number;
        };
        return invoke<TerminalSessionSnapshot>("terminal_create_session", {
          command: params.command ?? (typeof process !== "undefined" ? process.env.SHELL ?? "sh" : "sh"),
          args: params.args ?? [],
          workingDir: params.cwd ?? null,
          cols: params.cols ?? 80,
          rows: params.rows ?? 24,
        });
      },
      write: async (input: TerminalWriteInput) => {
        const params = input as unknown as { sessionId?: string; data?: string };
        await invoke<void>("terminal_write", {
          sessionId: params.sessionId ?? "",
          data: params.data ?? "",
        });
      },
      ackOutput: async (input: TerminalAckOutputInput) => {
        const params = input as unknown as { sessionId?: string; seq?: number };
        await invoke<void>("terminal_ack", {
          sessionId: params.sessionId ?? "",
          seq: params.seq ?? 0,
        });
      },
      resize: async (input: TerminalResizeInput) => {
        const params = input as unknown as { sessionId?: string; cols?: number; rows?: number };
        await invoke<void>("terminal_resize", {
          sessionId: params.sessionId ?? "",
          cols: params.cols ?? 80,
          rows: params.rows ?? 24,
        });
      },
      clear: async () => {
        // No direct syncode-tauri clear command; PTY clear is renderer-side.
        // Acknowledged no-op at the shell layer.
      },
      restart: async (input: TerminalRestartInput) => {
        // No direct restart command; emulate via destroy + open.
        const params = input as unknown as { sessionId?: string };
        if (params.sessionId) {
          await invoke<boolean>("terminal_destroy_session", { sessionId: params.sessionId }).catch(
            () => false,
          );
        }
        return unsupported<TerminalSessionSnapshot>(
          "terminal.restart",
          "no syncode-tauri restart command — destroy + re-open required (T6b)",
        );
      },
      close: async (input: TerminalCloseInput) => {
        const params = input as unknown as { sessionId?: string };
        await invoke<boolean>("terminal_destroy_session", {
          sessionId: params.sessionId ?? "",
        });
      },
      onEvent: (callback: (event: TerminalEvent) => void) => {
        terminalEventListeners.add(callback);
        return () => {
          terminalEventListeners.delete(callback);
        };
      },
    },

    // ─── projects (WS transport — no syncode-tauri command) ─────────────
    projects: {
      create: (input: ProjectCreateParams) =>
        callTransport<ProjectSummary>("project/create", input),
      discoverScripts: (input: ProjectDiscoverScriptsInput) =>
        callTransport<ProjectDiscoverScriptsResult>("project/discoverScripts", input),
      listDirectories: (input: ProjectListDirectoriesInput) =>
        callTransport<ProjectListDirectoriesResult>("project/listDirectories", input),
      searchEntries: (input: ProjectSearchEntriesInput) =>
        callTransport<ProjectSearchEntriesResult>("project/searchEntries", input),
      searchLocalEntries: (input: ProjectSearchLocalEntriesInput) =>
        callTransport<ProjectSearchLocalEntriesResult>("project/searchLocalEntries", input),
      readFile: (input: ProjectReadFileInput) =>
        callTransport<ProjectReadFileResult>("project/readFile", input),
      writeFile: (input: ProjectWriteFileInput) =>
        callTransport<ProjectWriteFileResult>("project/writeFile", input),
      runDevServer: (input: ProjectRunDevServerInput) =>
        callTransport<ProjectRunDevServerResult>("project/runDevServer", input),
      stopDevServer: (input: ProjectStopDevServerInput) =>
        callTransport<ProjectStopDevServerResult>("project/stopDevServer", input),
      listDevServers: () =>
        callTransport<ProjectListDevServersResult>("project/listDevServers"),
      onDevServerEvent: (callback: (event: ProjectDevServerEvent) => void) => {
        projectDevServerEventListeners.add(callback);
        return () => {
          projectDevServerEventListeners.delete(callback);
        };
      },
    },

    // ─── filesystem (WS transport) ──────────────────────────────────────
    filesystem: {
      browse: (input: FilesystemBrowseInput) =>
        callTransport<FilesystemBrowseResult>("filesystem/browse", input),
    },

    // ─── shell (openExternal via Tauri open / openInEditor via opener) ──
    shell: {
      openInEditor: async (cwd: string, _editor: EditorId) => {
        // No syncode-tauri command opens an external editor today. We open
        // the project folder in the OS file manager as a graceful fallback
        // and document the gap. A future syncode-tauri `shell_open_editor`
        // command can replace this.
        await invoke<unknown>("shell_open_editor", { cwd, editor: _editor }).catch(
          (err: unknown) => {
            // If the command isn't registered, fall back to OS open of cwd.
            const message = err instanceof Error ? err.message : String(err);
            if (message.includes("command") || message.includes("not")) {
              return openExternalImpl(cwd);
            }
            throw err;
          },
        );
      },
      openExternal: (url: string) => openExternalImpl(url).then(() => undefined),
      showInFolder: async (path: string) => {
        // No syncode-tauri command; use OS open on the parent path.
        await openExternalImpl(path);
      },
    },

    // ─── git (existing syncode-tauri git_* commands where available) ────
    git: {
      // syncode-tauri has: git_status, git_diff, git_log, git_branches,
      // git_add, git_commit, git_create_branch, git_delete_branch, git_checkout.
      githubRepository: (input: GitHubRepositoryInput) =>
        callTransport<GitHubRepositoryResult>("git/githubRepository", input),
      listBranches: async (input: GitListBranchesInput) => {
        const params = input as unknown as { path?: string };
        return invoke<GitListBranchesResult>("git_branches", { path: params.path ?? "" });
      },
      createWorktree: (_input: GitCreateWorktreeInput) =>
        unsupported<GitCreateWorktreeResult>(
          "git.createWorktree",
          "no syncode-tauri command — needs T6b backend addition",
        ),
      createDetachedWorktree: (_input: GitCreateDetachedWorktreeInput) =>
        unsupported<GitCreateDetachedWorktreeResult>(
          "git.createDetachedWorktree",
          "no syncode-tauri command — needs T6b backend addition",
        ),
      removeWorktree: (_input: GitRemoveWorktreeInput) =>
        unsupported<void>(
          "git.removeWorktree",
          "no syncode-tauri command — needs T6b backend addition",
        ),
      createBranch: async (input: GitCreateBranchInput) => {
        const params = input as unknown as { path?: string; name?: string; checkout?: boolean };
        await invoke<unknown>("git_create_branch", {
          path: params.path ?? "",
          name: params.name ?? "",
          checkout: params.checkout ?? true,
        });
      },
      checkout: async (input: GitCheckoutInput) => {
        const params = input as unknown as { path?: string; ref?: string };
        await invoke<void>("git_checkout", { path: params.path ?? "", refName: params.ref ?? "" });
      },
      stashAndCheckout: (_input: GitStashAndCheckoutInput) =>
        unsupported<void>(
          "git.stashAndCheckout",
          "no syncode-tauri command — needs T6b backend addition",
        ),
      stashDrop: (_input: GitStashDropInput) =>
        unsupported<void>(
          "git.stashDrop",
          "no syncode-tauri command — needs T6b backend addition",
        ),
      stashInfo: (_input: GitStashInfoInput) =>
        unsupported<GitStashInfoResult>(
          "git.stashInfo",
          "no syncode-tauri command — needs T6b backend addition",
        ),
      removeIndexLock: (_input: GitRemoveIndexLockInput) =>
        unsupported<void>(
          "git.removeIndexLock",
          "no syncode-tauri command — needs T6b backend addition",
        ),
      init: (_input: GitInitInput) =>
        unsupported<void>("git.init", "no syncode-tauri command — needs T6b backend addition"),
      stageFiles: async (input: GitStageFilesInput) => {
        const params = input as unknown as { path?: string; files?: string[] };
        await invoke<void>("git_add", { path: params.path ?? "", files: params.files ?? [] });
        return { ok: true } as unknown as GitStageFilesResult;
      },
      unstageFiles: (_input: GitUnstageFilesInput) =>
        unsupported<GitUnstageFilesResult>(
          "git.unstageFiles",
          "no syncode-tauri command — needs T6b backend addition",
        ),
      handoffThread: (input: GitHandoffThreadInput) =>
        callTransport<GitHandoffThreadResult>("git/handoffThread", input),
      resolvePullRequest: (input: GitPullRequestRefInput) =>
        callTransport<GitResolvePullRequestResult>("git/resolvePullRequest", input),
      preparePullRequestThread: (input: GitPreparePullRequestThreadInput) =>
        callTransport<GitPreparePullRequestThreadResult>(
          "git/preparePullRequestThread",
          input,
        ),
      pull: (input: GitPullInput) => callTransport<GitPullResult>("git/pull", input),
      status: async (input: GitStatusInput) => {
        const params = input as unknown as { path?: string };
        return invoke<GitStatusResult>("git_status", { path: params.path ?? "" });
      },
      readWorkingTreeDiff: async (input: GitReadWorkingTreeDiffInput) => {
        const params = input as unknown as {
          path?: string;
          oldRef?: string;
          newRef?: string;
        };
        const result = await invoke<unknown>("git_diff", {
          path: params.path ?? "",
          oldRef: params.oldRef ?? null,
          newRef: params.newRef ?? null,
        });
        return result as unknown as GitReadWorkingTreeDiffResult;
      },
      summarizeDiff: (_input: GitSummarizeDiffInput) =>
        unsupported<GitSummarizeDiffResult>(
          "git.summarizeDiff",
          "no syncode-tauri command — needs T6b backend addition",
        ),
      runStackedAction: (_input: GitRunStackedActionInput) =>
        unsupported<GitRunStackedActionResult>(
          "git.runStackedAction",
          "no syncode-tauri command — needs T6b backend addition",
        ),
      onActionProgress: (callback: (event: GitActionProgressEvent) => void) => {
        gitActionProgressListeners.add(callback);
        return () => {
          gitActionProgressListeners.delete(callback);
        };
      },
    },

    // ─── contextMenu (renderer fallback) ────────────────────────────────
    contextMenu: {
      show: async <T extends string>(
        _items: readonly ContextMenuItem<T>[],
        _position?: { x: number; y: number },
      ): Promise<T | null> => {
        // Tauri v2 has @tauri-apps/api/menu but a full context-menu popover
        // is non-trivial; renderer fallback (the existing showContextMenuFallback)
        // is used by wsNativeApi. Here we return null to signal "no selection."
        return null;
      },
    },

    // ─── server (WS transport) ──────────────────────────────────────────
    server: {
      getConfig: () => callTransport<ServerConfig>("server/getConfig"),
      getEnvironment: () => callTransport<ServerGetEnvironmentResult>("server/getEnvironment"),
      getSettings: () => callTransport<ServerGetSettingsResult>("server/getSettings"),
      updateSettings: (input: ServerUpdateSettingsInput) =>
        callTransport<ServerUpdateSettingsResult>("server/updateSettings", input),
      getAuthSession: () => callTransport<AuthSessionState>("auth/status"),
      bootstrapAuth: (input: AuthBootstrapInput) =>
        callTransport<AuthBootstrapResult>("auth/bootstrap", input),
      bootstrapBearerAuth: (input: AuthBootstrapInput) =>
        callTransport<AuthBearerBootstrapResult>("auth/bootstrapBearer", input),
      issueAuthWebSocketToken: () =>
        callTransport<AuthWebSocketTokenResult>("auth/issueWsToken"),
      createAuthPairingToken: (input?: AuthCreatePairingCredentialInput) =>
        callTransport<AuthPairingCredentialResult>("auth/createPairingToken", input),
      listAuthPairingLinks: () =>
        callTransport<ReadonlyArray<AuthPairingLink>>("auth/listPairingLinks"),
      revokeAuthPairingLink: (input: AuthRevokePairingLinkInput) =>
        callTransport<{ revoked: boolean }>("auth/revokePairingLink", input),
      listAuthClients: () => callTransport<ReadonlyArray<AuthClientSession>>("auth/listClients"),
      revokeAuthClient: (input: AuthRevokeClientSessionInput) =>
        callTransport<{ revoked: boolean }>("auth/revokeClient", input),
      revokeOtherAuthClients: () =>
        callTransport<{ revokedCount: number }>("auth/revokeOtherClients"),
      refreshProviders: () =>
        callTransport<ServerRefreshProvidersResult>("server/refreshProviders"),
      updateProvider: (input: ServerProviderUpdateInput) =>
        callTransport<ServerProviderUpdateResult>("server/updateProvider", input),
      listWorktrees: () => callTransport<ServerListWorktreesResult>("server/listWorktrees"),
      listLocalServers: () =>
        callTransport<ServerListLocalServersResult>("server/listLocalServers"),
      stopLocalServer: (input: ServerStopLocalServerInput) =>
        callTransport<ServerStopLocalServerResult>("server/stopLocalServer", input),
      getProviderUsageSnapshot: (input: ServerGetProviderUsageSnapshotInput) =>
        callTransport<ServerGetProviderUsageSnapshotResult>(
          "server/getProviderUsageSnapshot",
          input,
        ),
      listProviderUsage: (input: ServerListProviderUsageInput) =>
        callTransport<ServerListProviderUsageResult>("server/listProviderUsage", input),
      getDiagnostics: () => callTransport<ServerDiagnosticsResult>("server/getDiagnostics"),
      generateThreadRecap: (input: ServerGenerateThreadRecapInput) =>
        callTransport<ServerGenerateThreadRecapResult>(
          "server/generateThreadRecap",
          input,
        ),
      generateAutomationIntent: (input: ServerGenerateAutomationIntentInput) =>
        callTransport<ServerGenerateAutomationIntentResult>(
          "server/generateAutomationIntent",
          input,
        ),
      transcribeVoice: (input: ServerVoiceTranscriptionInput) =>
        callTransport<ServerVoiceTranscriptionResult>("server/transcribeVoice", input),
      upsertKeybinding: (input: ServerUpsertKeybindingInput) =>
        callTransport<ServerUpsertKeybindingResult>("server/upsertKeybinding", input),
    },

    // ─── stats (WS transport) ───────────────────────────────────────────
    stats: {
      getProfileStats: (input: StatsGetProfileStatsInput) =>
        callTransport<StatsGetProfileStatsResult>("stats/getProfileStats", input),
      getProfileTokenStats: (input: StatsGetProfileTokenStatsInput) =>
        callTransport<StatsGetProfileTokenStatsResult>("stats/getProfileTokenStats", input),
    },

    // ─── provider (WS transport) ────────────────────────────────────────
    provider: {
      getComposerCapabilities: (input: ProviderGetComposerCapabilitiesInput) =>
        callTransport<ProviderComposerCapabilities>(
          "provider/getComposerCapabilities",
          input,
        ),
      compactThread: (input: ProviderCompactThreadInput) =>
        callTransport<void>("provider/compactThread", input),
      listCommands: (input: ProviderListCommandsInput) =>
        callTransport<ProviderListCommandsResult>("provider/listCommands", input),
      listSkills: (input: ProviderListSkillsInput) =>
        callTransport<ProviderListSkillsResult>("provider/listSkills", input),
      listSkillsCatalog: (input: ProviderSkillsCatalogInput) =>
        callTransport<ProviderSkillsCatalogResult>(
          "provider/listSkillsCatalog",
          input,
        ),
      listPlugins: (input: ProviderListPluginsInput) =>
        callTransport<ProviderListPluginsResult>("provider/listPlugins", input),
      readPlugin: (input: ProviderReadPluginInput) =>
        callTransport<ProviderReadPluginResult>("provider/readPlugin", input),
      listModels: (input: ProviderListModelsInput) =>
        callTransport<ProviderListModelsResult>("provider/listModels", input),
      listAgents: (input: ProviderListAgentsInput) =>
        callTransport<ProviderListAgentsResult>("provider/listAgents", input),
      listMcpCatalog: (input: ProviderListMcpCatalogInput) =>
        callTransport<McpCatalogResponse>("provider/listMcpCatalog", input),
    },
    mcp: {
      create: (input: McpServerInput) =>
        callTransport<McpCreateResult>("mcp/create", input),
      update: (input: McpUpdateInput) =>
        callTransport<McpUpdateResult>("mcp/update", input),
      delete: (input: McpDeleteInput) =>
        callTransport<McpDeleteResult>("mcp/delete", input),
      testConnection: (input: McpTestConnectionInput) =>
        callTransport<McpTestConnectionResult>("mcp/testConnection", input),
    },

    // ─── orchestration (WS transport — primary served surface) ──────────
    orchestration: {
      getSnapshot: () => callTransport<OrchestrationReadModel>("project/get"),
      getShellSnapshot: () => callTransport<OrchestrationShellSnapshot>("shell/getSnapshot"),
      dispatchCommand: (command: ClientOrchestrationCommand) =>
        callTransport<{ sequence: number }>("orchestration/dispatch", command),
      importThread: (input: OrchestrationImportThreadInput) =>
        callTransport<OrchestrationImportThreadResult>("orchestration/importThread", input),
      repairState: () => callTransport<OrchestrationReadModel>("orchestration/repair"),
      getTurnDiff: (input: OrchestrationGetTurnDiffInput) =>
        callTransport<OrchestrationGetTurnDiffResult>("orchestration/getTurnDiff", input),
      getFullThreadDiff: (input: OrchestrationGetFullThreadDiffInput) =>
        callTransport<OrchestrationGetFullThreadDiffResult>(
          "orchestration/getFullThreadDiff",
          input,
        ),
      replayEvents: (fromSequenceExclusive: number) =>
        callTransport<OrchestrationEvent[]>("orchestration/replayEvents", {
          fromSequenceExclusive,
        }),
      subscribeShell: () => callTransport<void>("push/subscribe", { channel: "shell" }),
      unsubscribeShell: () => callTransport<void>("push/unsubscribe", { channel: "shell" }),
      subscribeThread: (input: OrchestrationSubscribeThreadInput) =>
        callTransport<void>("push/subscribe", { ...input, channel: "thread" }),
      unsubscribeThread: (input: OrchestrationSubscribeThreadInput) =>
        callTransport<void>("push/unsubscribe", { ...input, channel: "thread" }),
      onDomainEvent: (callback: (event: OrchestrationEvent) => void) => {
        orchestrationDomainEventListeners.add(callback);
        return () => {
          orchestrationDomainEventListeners.delete(callback);
        };
      },
      onShellEvent: (callback: (event: OrchestrationShellStreamItem) => void) => {
        orchestrationShellEventListeners.add(callback);
        return () => {
          orchestrationShellEventListeners.delete(callback);
        };
      },
      onThreadEvent: (callback: (event: OrchestrationThreadStreamItem) => void) => {
        orchestrationThreadEventListeners.add(callback);
        return () => {
          orchestrationThreadEventListeners.delete(callback);
        };
      },
    },

    // ─── automation (WS transport) ──────────────────────────────────────
    automation: {
      list: (input?: AutomationListInput) =>
        callTransport<AutomationListResult>("automation/list", input),
      create: (input: AutomationCreateInput) =>
        callTransport<AutomationDefinition>("automation/create", input),
      update: (input: AutomationUpdateInput) =>
        callTransport<AutomationDefinition>("automation/update", input),
      delete: (input: AutomationDeleteInput) =>
        callTransport<void>("automation/delete", input),
      runNow: (input: AutomationRunNowInput) =>
        callTransport<AutomationRunNowResult>("automation/runNow", input),
      cancelRun: (input: AutomationCancelRunInput) =>
        callTransport<AutomationCancelRunResult>("automation/cancelRun", input),
      markRunRead: (input: AutomationMarkRunReadInput) =>
        callTransport<AutomationRunActionResult>("automation/markRunRead", input),
      archiveRun: (input: AutomationArchiveRunInput) =>
        callTransport<AutomationRunActionResult>("automation/archiveRun", input),
      onEvent: (callback: (event: AutomationStreamEvent) => void) => {
        automationEventListeners.add(callback);
        return () => {
          automationEventListeners.delete(callback);
        };
      },
    },

    // ─── browser (UNSUPPORTED — Electron-only embedded webview panels) ──
    browser: {
      open: (input: BrowserOpenInput) =>
        // Tauri has no embedded Chromium webview panel; open in OS browser.
        openExternalImpl(input.initialUrl ?? "about:blank").then(() =>
          defaultBrowserState(input.threadId),
        ),
      close: (input: BrowserThreadInput) =>
        Promise.resolve(defaultBrowserState(input.threadId)),
      hide: (_input: BrowserThreadInput) => Promise.resolve(),
      getState: (input: BrowserThreadInput) =>
        Promise.resolve(defaultBrowserState(input.threadId)),
      setPanelBounds: (_input: BrowserSetPanelBoundsInput) =>
        unsupported<void>("browser.setPanelBounds", "no embedded webview panel in Tauri"),
      attachWebview: (_input: BrowserAttachWebviewInput) =>
        unsupported<ThreadBrowserState>(
          "browser.attachWebview",
          "Electron webContents attach — no Tauri equivalent",
        ),
      detachWebview: (_input: BrowserDetachWebviewInput) =>
        unsupported<void>(
          "browser.detachWebview",
          "Electron webContents detach — no Tauri equivalent",
        ),
      copyLink: (_input: BrowserTabInput) =>
        unsupported<void>("browser.copyLink", "no embedded webview panel in Tauri"),
      copyScreenshotToClipboard: (_input: BrowserTabInput) =>
        unsupported<void>(
          "browser.copyScreenshotToClipboard",
          "no embedded webview panel in Tauri",
        ),
      captureScreenshot: (_input: BrowserTabInput) =>
        unsupported<BrowserCaptureScreenshotResult>(
          "browser.captureScreenshot",
          "no embedded webview panel in Tauri",
        ),
      executeCdp: (_input: BrowserExecuteCdpInput) =>
        unsupported<unknown>("browser.executeCdp", "CDP requires embedded Chromium"),
      navigate: (input: BrowserNavigateInput) =>
        openExternalImpl(input.url).then(() => defaultBrowserState(input.threadId)),
      reload: (input: BrowserTabInput) =>
        Promise.resolve(defaultBrowserState(input.threadId)),
      goBack: (input: BrowserTabInput) =>
        Promise.resolve(defaultBrowserState(input.threadId)),
      goForward: (input: BrowserTabInput) =>
        Promise.resolve(defaultBrowserState(input.threadId)),
      newTab: (input: BrowserNewTabInput) =>
        openExternalImpl(input.url ?? "about:blank").then(() =>
          defaultBrowserState(input.threadId),
        ),
      closeTab: (input: BrowserTabInput) =>
        Promise.resolve(defaultBrowserState(input.threadId)),
      selectTab: (input: BrowserTabInput) =>
        Promise.resolve(defaultBrowserState(input.threadId)),
      openDevTools: (_input: BrowserTabInput) =>
        unsupported<void>("browser.openDevTools", "no embedded webview panel in Tauri"),
      onState: (_callback: (state: ThreadBrowserState) => void) => noopUnsubscribe(),
      onCopyLink: (_callback: (event: BrowserCopyLinkEvent) => void) => noopUnsubscribe(),
    },
  };

  return api;
}

// ─── Internal helpers (not part of the NativeApi surface) ───────────────

/**
 * Open a URL/path externally. Tauri v2 typically uses the shell plugin's
 * `open`, but `@tauri-apps/plugin-shell` isn't installed. We use the
 * `window.__TAURI__` opener if present, else fall back to `window.open`.
 */
async function openExternalImpl(target: string): Promise<boolean> {
  if (typeof window === "undefined") return false;
  // Tauri v2 exposes opener via the shell plugin; without it, window.open
  // opens in a Tauri webview window rather than the OS default browser, but
  // is the best graceful degradation available.
  try {
    const tauriOpen = (window as unknown as {
      __TAURI__?: { shell?: { open?: (t: string) => Promise<void> } };
    }).__TAURI__?.shell?.open;
    if (tauriOpen) {
      await tauriOpen(target);
      return true;
    }
  } catch {
    // fall through to window.open
  }
  if (typeof window.open === "function") {
    window.open(target, "_blank", "noopener,noreferrer");
    return true;
  }
  return false;
}

/**
 * Convenience: also expose a `DesktopBridge`-shaped adapter for code paths
 * that read `window.desktopBridge`. This implements the boot-critical bridge
 * surface (wsUrl, dialogs, theme, window controls, notifications, update
 * state) over Tauri; browser/webview methods reject with `UnsupportedError`.
 *
 * Returned by `installTauriDesktopBridge()` which the Tauri entrypoint calls
 * to populate `window.desktopBridge` before the React app boots.
 */
export interface TauriDesktopBridge {
  getWsUrl: () => string | null;
  pickFolder: () => Promise<string | null>;
  confirm: (message: string) => Promise<boolean>;
  setTheme: (theme: "light" | "dark" | "system") => Promise<void>;
  openExternal: (url: string) => Promise<boolean>;
  showInFolder: (path: string) => Promise<void>;
  windowControls: {
    minimize: () => Promise<void>;
    toggleMaximize: () => Promise<{ isMaximized: boolean; isFullscreen: boolean }>;
    close: () => Promise<void>;
    getState: () => Promise<{ isMaximized: boolean; isFullscreen: boolean }>;
    onState: (
      listener: (state: { isMaximized: boolean; isFullscreen: boolean }) => void,
    ) => () => void;
  };
  notifications: {
    isSupported: () => Promise<boolean>;
    show: (input: DesktopNotificationInput) => Promise<boolean>;
  };
  onUpdateState: (
    listener: (state: unknown) => void,
  ) => () => void;
}

/**
 * Build and (optionally) install the Tauri-backed DesktopBridge onto
 * `window.desktopBridge`. Returns the bridge object.
 */
export function createTauriDesktopBridge(
  wsUrlProvider: () => string | null = () => null,
): TauriDesktopBridge {
  let updateUnlisten: UnlistenFn | undefined;
  const updateListeners = new Set<(state: unknown) => void>();

  const bridge: TauriDesktopBridge = {
    getWsUrl: wsUrlProvider,
    pickFolder: async () => {
      // Delegate to the NativeApi dialogs.pickFolder implementation.
      if (typeof document === "undefined") return null;
      return new Promise<string | null>((resolve) => {
        const input = document.createElement("input");
        input.type = "file";
        input.setAttribute("webkitdirectory", "");
        input.onchange = () => {
          const file = input.files?.[0];
          const rel = file?.webkitRelativePath ?? "";
          const folder = rel.includes("/") ? rel.split("/")[0] ?? rel : rel;
          resolve(folder || null);
        };
        input.addEventListener("cancel", () => resolve(null));
        input.click();
      });
    },
    confirm: async (message: string) => {
      if (typeof window === "undefined" || typeof window.confirm !== "function") return false;
      return window.confirm(message);
    },
    setTheme: async (theme: "light" | "dark" | "system") => {
      if (!isTauri()) return;
      const win = getCurrentWindow();
      if (theme === "system") {
        await win.setTheme(null);
      } else {
        await win.setTheme(theme);
      }
    },
    openExternal: (url: string) => openExternalImpl(url),
    showInFolder: (path: string) => openExternalImpl(path).then(() => undefined),
    windowControls: {
      minimize: async () => {
        if (isTauri()) await getCurrentWindow().minimize();
      },
      toggleMaximize: async () => {
        if (isTauri()) await getCurrentWindow().toggleMaximize();
        return getWindowState();
      },
      close: async () => {
        if (isTauri()) await getCurrentWindow().close();
      },
      getState: () => getWindowState(),
      onState: (listener) => {
        if (!isTauri()) return noopUnsubscribe();
        const win = getCurrentWindow();
        let unlisten: UnlistenFn | undefined;
        void win
          .onResized(() => {
            void getWindowState().then(listener);
          })
          .then((fn) => {
            unlisten = fn;
          });
        return () => {
          unlisten?.();
        };
      },
    },
    notifications: {
      isSupported: async () => {
        return typeof Notification !== "undefined";
      },
      show: async (input: DesktopNotificationInput) => {
        if (typeof Notification === "undefined") return false;
        if (Notification.permission === "denied") return false;
        if (Notification.permission !== "granted") {
          const perm = await Notification.requestPermission();
          if (perm !== "granted") return false;
        }
        try {
          // Build options conditionally to satisfy exactOptionalPropertyTypes.
          const options: NotificationOptions = {};
          if (input.body !== undefined) options.body = input.body;
          if (input.silent !== undefined) options.silent = input.silent;
          new Notification(input.title, options);
          return true;
        } catch {
          return false;
        }
      },
    },
    onUpdateState: (listener) => {
      updateListeners.add(listener);
      if (!updateUnlisten && isTauri()) {
        void getCurrentWindow()
          .listen("tauri://update-status", (event) => {
            for (const l of updateListeners) {
              try {
                l(event.payload);
              } catch {
                // Swallow listener errors
              }
            }
          })
          .then((fn) => {
            updateUnlisten = fn;
          });
      }
      return () => {
        updateListeners.delete(listener);
        if (updateListeners.size === 0) {
          updateUnlisten?.();
          updateUnlisten = undefined;
        }
      };
    },
  };

  return bridge;
}

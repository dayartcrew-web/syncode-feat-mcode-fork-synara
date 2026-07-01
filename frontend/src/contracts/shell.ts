/**
 * NativeApi + DesktopBridge — desktop shell bridge interfaces (B4 / "T6").
 *
 * Copied verbatim from MCode `packages/contracts/src/ipc.ts` (the Electron
 * desktop-shell surface the vendored MCode `apps/web` consumes). These are
 * pure TypeScript interfaces — NOT Effect Schema — and are the stable boundary
 * the rest of the UI imports.
 *
 * Implementation: `../tauriNativeApi.ts` realises `NativeApi` over Tauri
 * `invoke` + `@tauri-apps/api` direct JS APIs (window, theme, …) and stubs
 * Electron-only capabilities (embedded browser webview panels, CDP, desktop
 * Chrome) to a typed `UnsupportedError`. See `docs/SHELL-GAPS.md`.
 *
 * References to MCode contracts types (terminal/git/project/server/…/auth/
 * automation/provider/orchestration) that live in deferred Tier 3 modules are
 * declared locally below as self-contained types so this file compiles
 * standalone today. When the corresponding bridge modules land (Tier 1/2/3),
 * the local declarations can be replaced by `import type` from those modules
 * without changing call-site shapes (interfaces are structurally typed).
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/ipc.ts
 */

import type { ThreadId } from "./ids";
// `AuthBootstrapResult` is the ts-rs-generated concrete Tier 1 DTO; import it
// rather than re-declaring an opaque alias, so `NativeApi.server.bootstrapAuth`
// shares the canonical backend type with the served RPC surface.
import type { AuthBootstrapResult } from "../types/AuthBootstrapResult";

// ─── Self-contained supporting type aliases (deferred Tier 3 modules) ────
// These narrow the surface this file pulls in. Each is a placeholder for the
// matching MCode contracts module; method signatures use them only as opaque
// input/result shapes. Replace with `import type` when the bridge module lands.

/** Opaque stand-in for inputs/results routed over the JSON-RPC transport. */
export type OpaqueTransportInput = Readonly<Record<string, unknown>>;
/** Opaque stand-in for inputs/results routed over the JSON-RPC transport. */
export type OpaqueTransportResult = Readonly<Record<string, unknown>>;

/** MCode `editor.ts` — `EditorId`. */
export type EditorId = string;

/** MCode `terminal.ts` types (terminal PTY session surface). */
export interface TerminalOpenInput extends OpaqueTransportInput {}
export interface TerminalWriteInput extends OpaqueTransportInput {}
export interface TerminalAckOutputInput extends OpaqueTransportInput {}
export interface TerminalResizeInput extends OpaqueTransportInput {}
export interface TerminalClearInput extends OpaqueTransportInput {}
export interface TerminalRestartInput extends OpaqueTransportInput {}
export interface TerminalCloseInput extends OpaqueTransportInput {}
export interface TerminalSessionSnapshot extends OpaqueTransportResult {}
export interface TerminalEvent extends OpaqueTransportResult {}

/** MCode `project.ts` types (project file/dev-server surface). */
export interface ProjectDiscoverScriptsInput extends OpaqueTransportInput {}
export interface ProjectDiscoverScriptsResult extends OpaqueTransportResult {}
export interface ProjectListDirectoriesInput extends OpaqueTransportInput {}
export interface ProjectListDirectoriesResult extends OpaqueTransportResult {}
export interface ProjectSearchEntriesInput extends OpaqueTransportInput {}
export interface ProjectSearchEntriesResult extends OpaqueTransportResult {}
export interface ProjectSearchLocalEntriesInput extends OpaqueTransportInput {}
export interface ProjectSearchLocalEntriesResult extends OpaqueTransportResult {}
export interface ProjectReadFileInput extends OpaqueTransportInput {}
export interface ProjectReadFileResult extends OpaqueTransportResult {}
export interface ProjectWriteFileInput extends OpaqueTransportInput {}
export interface ProjectWriteFileResult extends OpaqueTransportResult {}
export interface ProjectRunDevServerInput extends OpaqueTransportInput {}
export interface ProjectRunDevServerResult extends OpaqueTransportResult {}
export interface ProjectStopDevServerInput extends OpaqueTransportInput {}
export interface ProjectStopDevServerResult extends OpaqueTransportResult {}
export interface ProjectListDevServersResult extends OpaqueTransportResult {}
export interface ProjectDevServerEvent extends OpaqueTransportResult {}

/** MCode `filesystem.ts` types (filesystem browser surface). */
export interface FilesystemBrowseInput extends OpaqueTransportInput {}
export interface FilesystemBrowseResult extends OpaqueTransportResult {}

/** MCode `git.ts` types (git branch/worktree/stage/diff/PR surface). */
export interface GitHubRepositoryInput extends OpaqueTransportInput {}
export interface GitHubRepositoryResult extends OpaqueTransportResult {}
export interface GitListBranchesInput extends OpaqueTransportInput {}
export interface GitListBranchesResult extends OpaqueTransportResult {}
export interface GitCreateWorktreeInput extends OpaqueTransportInput {}
export interface GitCreateWorktreeResult extends OpaqueTransportResult {}
export interface GitCreateDetachedWorktreeInput extends OpaqueTransportInput {}
export interface GitCreateDetachedWorktreeResult extends OpaqueTransportResult {}
export interface GitRemoveWorktreeInput extends OpaqueTransportInput {}
export interface GitCreateBranchInput extends OpaqueTransportInput {}
export interface GitCheckoutInput extends OpaqueTransportInput {}
export interface GitStashAndCheckoutInput extends OpaqueTransportInput {}
export interface GitStashDropInput extends OpaqueTransportInput {}
export interface GitStashInfoInput extends OpaqueTransportInput {}
export interface GitStashInfoResult extends OpaqueTransportResult {}
export interface GitRemoveIndexLockInput extends OpaqueTransportInput {}
export interface GitInitInput extends OpaqueTransportInput {}
export interface GitStageFilesInput extends OpaqueTransportInput {}
export interface GitStageFilesResult extends OpaqueTransportResult {}
export interface GitUnstageFilesInput extends OpaqueTransportInput {}
export interface GitUnstageFilesResult extends OpaqueTransportResult {}
export interface GitHandoffThreadInput extends OpaqueTransportInput {}
export interface GitHandoffThreadResult extends OpaqueTransportResult {}
export interface GitPullRequestRefInput extends OpaqueTransportInput {}
export interface GitResolvePullRequestResult extends OpaqueTransportResult {}
export interface GitPreparePullRequestThreadInput extends OpaqueTransportInput {}
export interface GitPreparePullRequestThreadResult extends OpaqueTransportResult {}
export interface GitPullInput extends OpaqueTransportInput {}
export interface GitPullResult extends OpaqueTransportResult {}
export interface GitStatusInput extends OpaqueTransportInput {}
export interface GitStatusResult extends OpaqueTransportResult {}
export interface GitReadWorkingTreeDiffInput extends OpaqueTransportInput {}
export interface GitReadWorkingTreeDiffResult extends OpaqueTransportResult {}
export interface GitSummarizeDiffInput extends OpaqueTransportInput {}
export interface GitSummarizeDiffResult extends OpaqueTransportResult {}
export interface GitRunStackedActionInput extends OpaqueTransportInput {}
export interface GitRunStackedActionResult extends OpaqueTransportResult {}
export interface GitActionProgressEvent extends OpaqueTransportResult {}

/** MCode `server.ts` types (server meta/settings/providers/diagnostics surface). */
export interface ServerConfig extends OpaqueTransportResult {}
export interface ServerGetEnvironmentResult extends OpaqueTransportResult {}
export interface ServerGetSettingsResult extends OpaqueTransportResult {}
export interface ServerUpdateSettingsInput extends OpaqueTransportInput {}
export interface ServerUpdateSettingsResult extends OpaqueTransportResult {}
export interface ServerDiagnosticsResult extends OpaqueTransportResult {}
export interface ServerGenerateAutomationIntentInput extends OpaqueTransportInput {}
export interface ServerGenerateAutomationIntentResult extends OpaqueTransportResult {}
export interface ServerGenerateThreadRecapInput extends OpaqueTransportInput {}
export interface ServerGenerateThreadRecapResult extends OpaqueTransportResult {}
export interface ServerGetProviderUsageSnapshotInput extends OpaqueTransportInput {}
export interface ServerGetProviderUsageSnapshotResult extends OpaqueTransportResult {}
export interface ServerListProviderUsageInput extends OpaqueTransportInput {}
export interface ServerListProviderUsageResult extends OpaqueTransportResult {}
export interface ServerListLocalServersResult extends OpaqueTransportResult {}
export interface ServerListWorktreesResult extends OpaqueTransportResult {}
export interface ServerProviderUpdateInput extends OpaqueTransportInput {}
export interface ServerProviderUpdateResult extends OpaqueTransportResult {}
export interface ServerRefreshProvidersResult extends OpaqueTransportResult {}
export interface ServerStopLocalServerInput extends OpaqueTransportInput {}
export interface ServerStopLocalServerResult extends OpaqueTransportResult {}
export interface ServerUpsertKeybindingInput extends OpaqueTransportInput {}
export interface ServerUpsertKeybindingResult extends OpaqueTransportResult {}
export interface ServerVoiceTranscriptionInput extends OpaqueTransportInput {}
export interface ServerVoiceTranscriptionResult extends OpaqueTransportResult {}

/** MCode `auth.ts` types.
 *  NOTE: `AuthBootstrapResult` is intentionally NOT re-declared here — it is
 *  the ts-rs-generated concrete DTO in `../types/AuthBootstrapResult.ts`
 *  (Tier 1). The other auth types remain opaque aliases pending the matching
 *  bridge module.
 */
export interface AuthBootstrapInput extends OpaqueTransportInput {}
export interface AuthBearerBootstrapResult extends OpaqueTransportResult {}
export interface AuthWebSocketTokenResult extends OpaqueTransportResult {}
export interface AuthSessionState extends OpaqueTransportResult {}
export interface AuthCreatePairingCredentialInput extends OpaqueTransportInput {}
export interface AuthPairingCredentialResult extends OpaqueTransportResult {}
export interface AuthPairingLink extends OpaqueTransportResult {}
export interface AuthRevokePairingLinkInput extends OpaqueTransportInput {}
export interface AuthClientSession extends OpaqueTransportResult {}
export interface AuthRevokeClientSessionInput extends OpaqueTransportInput {}

/** MCode `automation.ts` types. */
export interface AutomationListInput extends OpaqueTransportInput {}
export interface AutomationListResult extends OpaqueTransportResult {}
export interface AutomationCreateInput extends OpaqueTransportInput {}
export interface AutomationDefinition extends OpaqueTransportResult {}
export interface AutomationUpdateInput extends OpaqueTransportInput {}
export interface AutomationDeleteInput extends OpaqueTransportInput {}
export interface AutomationRunNowInput extends OpaqueTransportInput {}
export interface AutomationRunNowResult extends OpaqueTransportResult {}
export interface AutomationCancelRunInput extends OpaqueTransportInput {}
export interface AutomationCancelRunResult extends OpaqueTransportResult {}
export interface AutomationMarkRunReadInput extends OpaqueTransportInput {}
export interface AutomationRunActionResult extends OpaqueTransportResult {}
export interface AutomationArchiveRunInput extends OpaqueTransportInput {}
export interface AutomationStreamEvent extends OpaqueTransportResult {}

/** MCode `providerDiscovery.ts` + `provider.ts` types. */
export interface ProviderGetComposerCapabilitiesInput extends OpaqueTransportInput {}
export interface ProviderComposerCapabilities extends OpaqueTransportResult {}
export interface ProviderCompactThreadInput extends OpaqueTransportInput {}
export interface ProviderListCommandsInput extends OpaqueTransportInput {}
export interface ProviderListCommandsResult extends OpaqueTransportResult {}
export interface ProviderListSkillsInput extends OpaqueTransportInput {}
export interface ProviderListSkillsResult extends OpaqueTransportResult {}
export interface ProviderSkillsCatalogInput extends OpaqueTransportInput {}
export interface ProviderSkillsCatalogResult extends OpaqueTransportResult {}
export interface ProviderListPluginsInput extends OpaqueTransportInput {}
export interface ProviderListPluginsResult extends OpaqueTransportResult {}
export interface ProviderReadPluginInput extends OpaqueTransportInput {}
export interface ProviderReadPluginResult extends OpaqueTransportResult {}
export interface ProviderListModelsInput extends OpaqueTransportInput {}
export interface ProviderListModelsResult extends OpaqueTransportResult {}
export interface ProviderListAgentsInput extends OpaqueTransportInput {}
export interface ProviderListAgentsResult extends OpaqueTransportResult {}

/** MCode `stats.ts` types. */
export interface StatsGetProfileStatsInput extends OpaqueTransportInput {}
export interface StatsGetProfileStatsResult extends OpaqueTransportResult {}
export interface StatsGetProfileTokenStatsInput extends OpaqueTransportInput {}
export interface StatsGetProfileTokenStatsResult extends OpaqueTransportResult {}

/** MCode `orchestration.ts` types (the aggregate stream surface). */
export interface OrchestrationReadModel extends OpaqueTransportResult {}
export interface OrchestrationShellSnapshot extends OpaqueTransportResult {}
export type ClientOrchestrationCommand = Readonly<Record<string, unknown>> & { type: string };
export interface OrchestrationImportThreadInput extends OpaqueTransportInput {}
export interface OrchestrationImportThreadResult extends OpaqueTransportResult {}
export interface OrchestrationGetTurnDiffInput extends OpaqueTransportInput {}
export interface OrchestrationGetTurnDiffResult extends OpaqueTransportResult {}
export interface OrchestrationGetFullThreadDiffInput extends OpaqueTransportInput {}
export interface OrchestrationGetFullThreadDiffResult extends OpaqueTransportResult {}
export interface OrchestrationEvent extends OpaqueTransportResult {}
export interface OrchestrationShellStreamItem extends OpaqueTransportResult {}
export interface OrchestrationThreadStreamItem extends OpaqueTransportResult {}
export interface OrchestrationSubscribeThreadInput extends OpaqueTransportInput {}

// ─── Desktop-shell types (defined inline, verbatim from MCode ipc.ts) ────

export interface ContextMenuItem<T extends string = string> {
  id: T;
  label: string;
  /** Starts a new visual group before this actionable row. */
  separatorBefore?: boolean;
  destructive?: boolean;
}

export type DesktopUpdateStatus =
  | "disabled"
  | "idle"
  | "checking"
  | "up-to-date"
  | "available"
  | "downloading"
  | "downloaded"
  | "error";

export type DesktopRuntimeArch = "arm64" | "x64" | "other";
export type DesktopTheme = "light" | "dark" | "system";

export interface DesktopRuntimeInfo {
  hostArch: DesktopRuntimeArch;
  appArch: DesktopRuntimeArch;
  runningUnderArm64Translation: boolean;
}

export interface DesktopUpdateState {
  enabled: boolean;
  status: DesktopUpdateStatus;
  currentVersion: string;
  hostArch: DesktopRuntimeArch;
  appArch: DesktopRuntimeArch;
  runningUnderArm64Translation: boolean;
  availableVersion: string | null;
  downloadedVersion: string | null;
  downloadPercent: number | null;
  checkedAt: string | null;
  message: string | null;
  errorContext: "check" | "download" | "install" | null;
  canRetry: boolean;
  // Public URL where the user can manually download the release when the
  // in-app updater cannot apply it (silent installer failure, unsigned build,
  // read-only install location, unsupported platform). Null when no GitHub
  // update source is configured.
  releaseUrl: string | null;
}

export interface DesktopUpdateActionResult {
  accepted: boolean;
  completed: boolean;
  state: DesktopUpdateState;
}

export interface BrowserTabState {
  id: string;
  url: string;
  title: string;
  status: "live" | "suspended";
  isLoading: boolean;
  canGoBack: boolean;
  canGoForward: boolean;
  faviconUrl: string | null;
  lastCommittedUrl: string | null;
  lastError: string | null;
}

export interface ThreadBrowserState {
  threadId: ThreadId;
  version: number;
  open: boolean;
  activeTabId: string | null;
  tabs: BrowserTabState[];
  lastError: string | null;
}

export interface BrowserOpenInput {
  threadId: ThreadId;
  initialUrl?: string;
}

export interface BrowserThreadInput {
  threadId: ThreadId;
}

export interface BrowserTabInput {
  threadId: ThreadId;
  tabId: string;
}

export interface BrowserNavigateInput {
  threadId: ThreadId;
  tabId?: string;
  url: string;
}

export interface BrowserNewTabInput {
  threadId: ThreadId;
  url?: string;
  activate?: boolean;
}

export interface BrowserPanelBounds {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface BrowserSetPanelBoundsInput {
  threadId: ThreadId;
  bounds: BrowserPanelBounds | null;
  surface?: "native" | "renderer";
}

export interface BrowserAttachWebviewInput extends BrowserTabInput {
  webContentsId: number;
}

export interface BrowserDetachWebviewInput extends BrowserTabInput {
  webContentsId: number;
}

export interface BrowserCaptureScreenshotResult {
  name: string;
  mimeType: "image/png";
  sizeBytes: number;
  bytes: Uint8Array;
}

export interface BrowserExecuteCdpInput extends BrowserTabInput {
  method: string;
  params?: Record<string, unknown>;
}

// Pushed from the desktop main process when the in-app browser copy-link chord fires
// while the native page (not the React chrome) holds keyboard focus.
export interface BrowserCopyLinkEvent {
  threadId: ThreadId;
  url: string;
}

export interface DesktopNotificationInput {
  title: string;
  body?: string;
  silent?: boolean;
  threadId?: ThreadId;
}

export interface DesktopWindowState {
  isMaximized: boolean;
  isFullscreen: boolean;
}

export interface DesktopBridge {
  getWsUrl: () => string | null;
  pickFolder: () => Promise<string | null>;
  saveFile?: (input: {
    defaultFilename: string;
    contents: string;
    filters?: ReadonlyArray<{ name: string; extensions: ReadonlyArray<string> }>;
  }) => Promise<string | null>;
  confirm: (message: string) => Promise<boolean>;
  setTheme: (theme: DesktopTheme) => Promise<void>;
  showContextMenu: <T extends string>(
    items: readonly ContextMenuItem<T>[],
    position?: { x: number; y: number },
  ) => Promise<T | null>;
  openExternal: (url: string) => Promise<boolean>;
  showInFolder: (path: string) => Promise<void>;
  shell?: {
    showInFolder: (path: string) => Promise<void>;
  };
  windowControls?: {
    minimize: () => Promise<void>;
    toggleMaximize: () => Promise<DesktopWindowState>;
    close: () => Promise<void>;
    getState: () => Promise<DesktopWindowState>;
    onState: (listener: (state: DesktopWindowState) => void) => () => void;
  };
  onMenuAction: (listener: (action: string) => void) => () => void;
  /** Current `webContents` page zoom (1 = 100%). Used to keep macOS traffic-light gutter aligned. */
  getZoomFactor: () => number;
  onZoomFactorChange: (listener: (zoomFactor: number) => void) => () => void;
  getUpdateState: () => Promise<DesktopUpdateState>;
  checkForUpdates: () => Promise<DesktopUpdateState>;
  downloadUpdate: () => Promise<DesktopUpdateActionResult>;
  installUpdate: () => Promise<DesktopUpdateActionResult>;
  onUpdateState: (listener: (state: DesktopUpdateState) => void) => () => void;
  notifications: {
    isSupported: () => Promise<boolean>;
    show: (input: DesktopNotificationInput) => Promise<boolean>;
  };
  server?: {
    transcribeVoice: (
      input: ServerVoiceTranscriptionInput,
    ) => Promise<ServerVoiceTranscriptionResult>;
  };
  browser: {
    open: (input: BrowserOpenInput) => Promise<ThreadBrowserState>;
    close: (input: BrowserThreadInput) => Promise<ThreadBrowserState>;
    hide: (input: BrowserThreadInput) => Promise<void>;
    getState: (input: BrowserThreadInput) => Promise<ThreadBrowserState>;
    setPanelBounds: (input: BrowserSetPanelBoundsInput) => Promise<void>;
    attachWebview: (input: BrowserAttachWebviewInput) => Promise<ThreadBrowserState>;
    detachWebview: (input: BrowserDetachWebviewInput) => Promise<void>;
    copyLink: (input: BrowserTabInput) => Promise<void>;
    copyScreenshotToClipboard: (input: BrowserTabInput) => Promise<void>;
    captureScreenshot: (input: BrowserTabInput) => Promise<BrowserCaptureScreenshotResult>;
    executeCdp: (input: BrowserExecuteCdpInput) => Promise<unknown>;
    navigate: (input: BrowserNavigateInput) => Promise<ThreadBrowserState>;
    reload: (input: BrowserTabInput) => Promise<ThreadBrowserState>;
    goBack: (input: BrowserTabInput) => Promise<ThreadBrowserState>;
    goForward: (input: BrowserTabInput) => Promise<ThreadBrowserState>;
    newTab: (input: BrowserNewTabInput) => Promise<ThreadBrowserState>;
    closeTab: (input: BrowserTabInput) => Promise<ThreadBrowserState>;
    selectTab: (input: BrowserTabInput) => Promise<ThreadBrowserState>;
    openDevTools: (input: BrowserTabInput) => Promise<void>;
    onState: (listener: (state: ThreadBrowserState) => void) => () => void;
    onBrowserUseOpenPanelRequest: (listener: () => void) => () => void;
    onBrowserCopyLink: (listener: (event: BrowserCopyLinkEvent) => void) => () => void;
  };
}

export interface NativeApi {
  dialogs: {
    pickFolder: () => Promise<string | null>;
    saveFile?: (input: {
      defaultFilename: string;
      contents: string;
      filters?: ReadonlyArray<{ name: string; extensions: ReadonlyArray<string> }>;
    }) => Promise<string | null>;
    confirm: (message: string) => Promise<boolean>;
  };
  terminal: {
    open: (input: TerminalOpenInput) => Promise<TerminalSessionSnapshot>;
    write: (input: TerminalWriteInput) => Promise<void>;
    ackOutput: (input: TerminalAckOutputInput) => Promise<void>;
    resize: (input: TerminalResizeInput) => Promise<void>;
    clear: (input: TerminalClearInput) => Promise<void>;
    restart: (input: TerminalRestartInput) => Promise<TerminalSessionSnapshot>;
    close: (input: TerminalCloseInput) => Promise<void>;
    onEvent: (callback: (event: TerminalEvent) => void) => () => void;
  };
  projects: {
    discoverScripts: (input: ProjectDiscoverScriptsInput) => Promise<ProjectDiscoverScriptsResult>;
    listDirectories: (input: ProjectListDirectoriesInput) => Promise<ProjectListDirectoriesResult>;
    searchEntries: (input: ProjectSearchEntriesInput) => Promise<ProjectSearchEntriesResult>;
    searchLocalEntries: (
      input: ProjectSearchLocalEntriesInput,
    ) => Promise<ProjectSearchLocalEntriesResult>;
    readFile: (input: ProjectReadFileInput) => Promise<ProjectReadFileResult>;
    writeFile: (input: ProjectWriteFileInput) => Promise<ProjectWriteFileResult>;
    runDevServer: (input: ProjectRunDevServerInput) => Promise<ProjectRunDevServerResult>;
    stopDevServer: (input: ProjectStopDevServerInput) => Promise<ProjectStopDevServerResult>;
    listDevServers: () => Promise<ProjectListDevServersResult>;
    onDevServerEvent: (callback: (event: ProjectDevServerEvent) => void) => () => void;
  };
  filesystem: {
    browse: (input: FilesystemBrowseInput) => Promise<FilesystemBrowseResult>;
  };
  shell: {
    openInEditor: (cwd: string, editor: EditorId) => Promise<void>;
    openExternal: (url: string) => Promise<void>;
    showInFolder: (path: string) => Promise<void>;
  };
  git: {
    // Existing branch/worktree API
    githubRepository: (input: GitHubRepositoryInput) => Promise<GitHubRepositoryResult>;
    listBranches: (input: GitListBranchesInput) => Promise<GitListBranchesResult>;
    createWorktree: (input: GitCreateWorktreeInput) => Promise<GitCreateWorktreeResult>;
    createDetachedWorktree: (
      input: GitCreateDetachedWorktreeInput,
    ) => Promise<GitCreateDetachedWorktreeResult>;
    removeWorktree: (input: GitRemoveWorktreeInput) => Promise<void>;
    createBranch: (input: GitCreateBranchInput) => Promise<void>;
    checkout: (input: GitCheckoutInput) => Promise<void>;
    stashAndCheckout: (input: GitStashAndCheckoutInput) => Promise<void>;
    stashDrop: (input: GitStashDropInput) => Promise<void>;
    stashInfo: (input: GitStashInfoInput) => Promise<GitStashInfoResult>;
    removeIndexLock: (input: GitRemoveIndexLockInput) => Promise<void>;
    init: (input: GitInitInput) => Promise<void>;
    stageFiles: (input: GitStageFilesInput) => Promise<GitStageFilesResult>;
    unstageFiles: (input: GitUnstageFilesInput) => Promise<GitUnstageFilesResult>;
    handoffThread: (input: GitHandoffThreadInput) => Promise<GitHandoffThreadResult>;
    resolvePullRequest: (input: GitPullRequestRefInput) => Promise<GitResolvePullRequestResult>;
    preparePullRequestThread: (
      input: GitPreparePullRequestThreadInput,
    ) => Promise<GitPreparePullRequestThreadResult>;
    // Stacked action API
    pull: (input: GitPullInput) => Promise<GitPullResult>;
    status: (input: GitStatusInput) => Promise<GitStatusResult>;
    readWorkingTreeDiff: (
      input: GitReadWorkingTreeDiffInput,
    ) => Promise<GitReadWorkingTreeDiffResult>;
    summarizeDiff: (input: GitSummarizeDiffInput) => Promise<GitSummarizeDiffResult>;
    runStackedAction: (input: GitRunStackedActionInput) => Promise<GitRunStackedActionResult>;
    onActionProgress: (callback: (event: GitActionProgressEvent) => void) => () => void;
  };
  contextMenu: {
    show: <T extends string>(
      items: readonly ContextMenuItem<T>[],
      position?: { x: number; y: number },
    ) => Promise<T | null>;
  };
  server: {
    getConfig: () => Promise<ServerConfig>;
    getEnvironment: () => Promise<ServerGetEnvironmentResult>;
    getSettings: () => Promise<ServerGetSettingsResult>;
    updateSettings: (input: ServerUpdateSettingsInput) => Promise<ServerUpdateSettingsResult>;
    getAuthSession: () => Promise<AuthSessionState>;
    bootstrapAuth: (input: AuthBootstrapInput) => Promise<AuthBootstrapResult>;
    bootstrapBearerAuth: (input: AuthBootstrapInput) => Promise<AuthBearerBootstrapResult>;
    issueAuthWebSocketToken: () => Promise<AuthWebSocketTokenResult>;
    createAuthPairingToken: (
      input?: AuthCreatePairingCredentialInput,
    ) => Promise<AuthPairingCredentialResult>;
    listAuthPairingLinks: () => Promise<ReadonlyArray<AuthPairingLink>>;
    revokeAuthPairingLink: (input: AuthRevokePairingLinkInput) => Promise<{ revoked: boolean }>;
    listAuthClients: () => Promise<ReadonlyArray<AuthClientSession>>;
    revokeAuthClient: (input: AuthRevokeClientSessionInput) => Promise<{ revoked: boolean }>;
    revokeOtherAuthClients: () => Promise<{ revokedCount: number }>;
    refreshProviders: () => Promise<ServerRefreshProvidersResult>;
    updateProvider: (input: ServerProviderUpdateInput) => Promise<ServerProviderUpdateResult>;
    listWorktrees: () => Promise<ServerListWorktreesResult>;
    listLocalServers: () => Promise<ServerListLocalServersResult>;
    stopLocalServer: (input: ServerStopLocalServerInput) => Promise<ServerStopLocalServerResult>;
    getProviderUsageSnapshot: (
      input: ServerGetProviderUsageSnapshotInput,
    ) => Promise<ServerGetProviderUsageSnapshotResult>;
    listProviderUsage: (
      input: ServerListProviderUsageInput,
    ) => Promise<ServerListProviderUsageResult>;
    getDiagnostics: () => Promise<ServerDiagnosticsResult>;
    generateThreadRecap: (
      input: ServerGenerateThreadRecapInput,
    ) => Promise<ServerGenerateThreadRecapResult>;
    generateAutomationIntent: (
      input: ServerGenerateAutomationIntentInput,
    ) => Promise<ServerGenerateAutomationIntentResult>;
    transcribeVoice: (
      input: ServerVoiceTranscriptionInput,
    ) => Promise<ServerVoiceTranscriptionResult>;
    upsertKeybinding: (input: ServerUpsertKeybindingInput) => Promise<ServerUpsertKeybindingResult>;
  };
  stats: {
    getProfileStats: (input: StatsGetProfileStatsInput) => Promise<StatsGetProfileStatsResult>;
    getProfileTokenStats: (
      input: StatsGetProfileTokenStatsInput,
    ) => Promise<StatsGetProfileTokenStatsResult>;
  };
  provider: {
    getComposerCapabilities: (
      input: ProviderGetComposerCapabilitiesInput,
    ) => Promise<ProviderComposerCapabilities>;
    compactThread: (input: ProviderCompactThreadInput) => Promise<void>;
    listCommands: (input: ProviderListCommandsInput) => Promise<ProviderListCommandsResult>;
    listSkills: (input: ProviderListSkillsInput) => Promise<ProviderListSkillsResult>;
    listSkillsCatalog: (input: ProviderSkillsCatalogInput) => Promise<ProviderSkillsCatalogResult>;
    listPlugins: (input: ProviderListPluginsInput) => Promise<ProviderListPluginsResult>;
    readPlugin: (input: ProviderReadPluginInput) => Promise<ProviderReadPluginResult>;
    listModels: (input: ProviderListModelsInput) => Promise<ProviderListModelsResult>;
    listAgents: (input: ProviderListAgentsInput) => Promise<ProviderListAgentsResult>;
  };
  orchestration: {
    getSnapshot: () => Promise<OrchestrationReadModel>;
    getShellSnapshot: () => Promise<OrchestrationShellSnapshot>;
    dispatchCommand: (command: ClientOrchestrationCommand) => Promise<{ sequence: number }>;
    importThread: (
      input: OrchestrationImportThreadInput,
    ) => Promise<OrchestrationImportThreadResult>;
    repairState: () => Promise<OrchestrationReadModel>;
    getTurnDiff: (input: OrchestrationGetTurnDiffInput) => Promise<OrchestrationGetTurnDiffResult>;
    getFullThreadDiff: (
      input: OrchestrationGetFullThreadDiffInput,
    ) => Promise<OrchestrationGetFullThreadDiffResult>;
    replayEvents: (fromSequenceExclusive: number) => Promise<OrchestrationEvent[]>;
    subscribeShell: () => Promise<void>;
    unsubscribeShell: () => Promise<void>;
    subscribeThread: (input: OrchestrationSubscribeThreadInput) => Promise<void>;
    unsubscribeThread: (input: OrchestrationSubscribeThreadInput) => Promise<void>;
    onDomainEvent: (callback: (event: OrchestrationEvent) => void) => () => void;
    onShellEvent: (callback: (event: OrchestrationShellStreamItem) => void) => () => void;
    onThreadEvent: (callback: (event: OrchestrationThreadStreamItem) => void) => () => void;
  };
  automation: {
    list: (input?: AutomationListInput) => Promise<AutomationListResult>;
    create: (input: AutomationCreateInput) => Promise<AutomationDefinition>;
    update: (input: AutomationUpdateInput) => Promise<AutomationDefinition>;
    delete: (input: AutomationDeleteInput) => Promise<void>;
    runNow: (input: AutomationRunNowInput) => Promise<AutomationRunNowResult>;
    cancelRun: (input: AutomationCancelRunInput) => Promise<AutomationCancelRunResult>;
    markRunRead: (input: AutomationMarkRunReadInput) => Promise<AutomationRunActionResult>;
    archiveRun: (input: AutomationArchiveRunInput) => Promise<AutomationRunActionResult>;
    onEvent: (callback: (event: AutomationStreamEvent) => void) => () => void;
  };
  browser: {
    open: (input: BrowserOpenInput) => Promise<ThreadBrowserState>;
    close: (input: BrowserThreadInput) => Promise<ThreadBrowserState>;
    hide: (input: BrowserThreadInput) => Promise<void>;
    getState: (input: BrowserThreadInput) => Promise<ThreadBrowserState>;
    setPanelBounds: (input: BrowserSetPanelBoundsInput) => Promise<void>;
    attachWebview: (input: BrowserAttachWebviewInput) => Promise<ThreadBrowserState>;
    detachWebview: (input: BrowserDetachWebviewInput) => Promise<void>;
    copyLink: (input: BrowserTabInput) => Promise<void>;
    copyScreenshotToClipboard: (input: BrowserTabInput) => Promise<void>;
    captureScreenshot: (input: BrowserTabInput) => Promise<BrowserCaptureScreenshotResult>;
    executeCdp: (input: BrowserExecuteCdpInput) => Promise<unknown>;
    navigate: (input: BrowserNavigateInput) => Promise<ThreadBrowserState>;
    reload: (input: BrowserTabInput) => Promise<ThreadBrowserState>;
    goBack: (input: BrowserTabInput) => Promise<ThreadBrowserState>;
    goForward: (input: BrowserTabInput) => Promise<ThreadBrowserState>;
    newTab: (input: BrowserNewTabInput) => Promise<ThreadBrowserState>;
    closeTab: (input: BrowserTabInput) => Promise<ThreadBrowserState>;
    selectTab: (input: BrowserTabInput) => Promise<ThreadBrowserState>;
    openDevTools: (input: BrowserTabInput) => Promise<void>;
    onState: (callback: (state: ThreadBrowserState) => void) => () => void;
    onCopyLink: (callback: (event: BrowserCopyLinkEvent) => void) => () => void;
  };
}

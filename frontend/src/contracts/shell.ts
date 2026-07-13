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

// ─── Tier 3 type imports (real shapes) ────────────────────────────────
// The symbols below were previously declared in this file as opaque stubs.
// T5b ported them to real shapes in the `./tier3/*` modules; import them
// here so the `NativeApi` / `DesktopBridge` interfaces below reference the
// real types AND the matching `export type { … } from "./tier3/…"`
// re-exports at the bottom of the re-export block keep the public barrel
// surface stable.
import type { IsoDateTime } from "./tier3/base";
import type { KeybindingRule } from "./tier3/keybindings";
import type {
  EditorId,
  ContextMenuItem,
  FilesystemBrowseResult,
} from "./tier3/misc";
import type {
  TerminalSessionSnapshot,
  TerminalEvent,
} from "./tier3/terminal";
import type {
  ProjectDiscoverScriptsResult,
  ProjectListDirectoriesResult,
  ProjectSearchEntriesResult,
  ProjectSearchLocalEntriesResult,
  ProjectReadFileResult,
  ProjectRunDevServerInput,
  ProjectDevServer,
  ProjectDevServerEvent,
} from "./tier3/project";
import type { ProjectCreateParams } from "../types/ProjectCreateParams";
import type { ProjectSummary } from "../types/ProjectSummary";
import type {
  GitListBranchesResult,
  GitStashInfoResult,
  GitResolvePullRequestResult,
  GitResolvedPullRequest,
  GitStatusResult,
  GitReadWorkingTreeDiffInput,
  GitRunStackedActionResult,
  GitActionProgressEvent,
} from "./tier3/git";
import type {
  ServerConfig,
  ServerGenerateAutomationIntentInput,
  ServerGenerateAutomationIntentResult,
  ServerGetProviderUsageSnapshotInput,
  ServerGetProviderUsageSnapshotResult,
  ServerListProviderUsageInput,
  ServerListProviderUsageResult,
  ServerLocalServerProcess,
  ServerProviderStatusesUpdatedPayload,
  ServerSettings,
  ServerSettingsPatch,
  ServerSettingsUpdatedPayload,
  ServerStopLocalServerInput,
} from "./tier3/server";
import type {
  AuthBootstrapInput,
  AuthBearerBootstrapResult,
  AuthWebSocketTokenResult,
  AuthSessionState,
  AuthCreatePairingCredentialInput,
  AuthPairingCredentialResult,
  AuthPairingLink,
  AuthRevokePairingLinkInput,
  AuthClientSession,
  AuthRevokeClientSessionInput,
} from "./tier3/auth";
import type {
  AutomationCreateInput,
  AutomationListResult,
  AutomationDefinition,
  AutomationRun,
  AutomationStreamEvent,
  AutomationUpdateInput,
} from "./tier3/automation";
// `ClientOrchestrationCommand` and `OrchestrationEvent` are referenced in
// the `NativeApi` interface below (dispatchCommand / replayEvents /
// onDomainEvent). They are also re-exported at the bottom of this module;
// the `import type` here brings them into local scope for the interface
// definitions (a bare `export type { X } from "…"` re-export does NOT).
import type {
  ClientOrchestrationCommand,
  OrchestrationEvent,
  OrchestrationReadModel,
  OrchestrationShellSnapshot,
  OrchestrationShellStreamItem,
  OrchestrationThreadStreamItem,
  ThreadEnvironmentMode,
} from "./tier3/orchestration";
import type {
  ProviderComposerCapabilities,
  ProviderListAgentsResult,
  ProviderListCommandsResult,
  ProviderListModelsResult,
  ProviderListPluginsResult,
  ProviderListSkillsResult,
  ProviderSkillsCatalogResult,
} from "./tier3/provider";
import type {
  StatsGetProfileStatsInput,
  StatsGetProfileStatsResult,
  StatsGetProfileTokenStatsInput,
  StatsGetProfileTokenStatsResult,
} from "./tier3/stats";

// ─── Self-contained supporting type aliases (deferred Tier 3 modules) ────
// These narrow the surface this file pulls in. Each is a placeholder for the
// matching MCode contracts module; method signatures use them only as opaque
// input/result shapes. Replace with `import type` when the bridge module lands.

/** Opaque stand-in for inputs/results routed over the JSON-RPC transport. */
export type OpaqueTransportInput = Readonly<Record<string, unknown>>;
/** Opaque stand-in for inputs/results routed over the JSON-RPC transport. */
export type OpaqueTransportResult = Readonly<Record<string, unknown>>;

// ─── Tier 3 re-exports (real shapes replace prior opaque stubs) ────────
// The symbols below were previously declared in this file as opaque
// `extends OpaqueTransportInput/Result {}` stubs. T5b ported them to real
// shapes in the tier3 sibling modules; re-export those shapes here so the
// NativeApi/DesktopBridge interfaces (which reference them) get the real
// types AND the symbol set exported from `./shell` stays consistent for
// existing importers.
export type {
  // editor
} from "./tier3/misc";
// `EditorId` is BOTH a type (literal union) AND a value (`Codec<EditorId>`
// factory used by `useLocalStorage`). A plain `export { … }` re-exports both
// namespaces; `export type` would drop the value and surface TS2693 at the
// `useLocalStorage(…, EditorId)` call sites in editorPreferences.ts.
export { EditorId } from "./tier3/misc";

export type {
  TerminalSessionSnapshot,
  TerminalEvent,
} from "./tier3/terminal";

export type {
  ProjectDiscoverScriptsResult,
  ProjectListDirectoriesResult,
  ProjectSearchEntriesResult,
  ProjectSearchLocalEntriesResult,
  ProjectReadFileResult,
  ProjectRunDevServerInput,
  ProjectDevServerEvent,
} from "./tier3/project";

export type {
  FilesystemBrowseResult,
  ContextMenuItem,
} from "./tier3/misc";

export type {
  GitStashInfoResult,
  GitResolvePullRequestResult,
  GitStatusResult,
  GitReadWorkingTreeDiffInput,
  GitRunStackedActionResult,
  GitActionProgressEvent,
} from "./tier3/git";

export type {
  ServerConfig,
  ServerGenerateAutomationIntentInput,
  ServerGenerateAutomationIntentResult,
  ServerGetProviderUsageSnapshotInput,
  ServerGetProviderUsageSnapshotResult,
  ServerListProviderUsageInput,
  ServerListProviderUsageResult,
  ServerStopLocalServerInput,
} from "./tier3/server";

export type {
  AuthBootstrapInput,
  AuthBearerBootstrapResult,
  AuthWebSocketTokenResult,
  AuthSessionState,
  AuthCreatePairingCredentialInput,
  AuthPairingCredentialResult,
  AuthPairingLink,
  AuthRevokePairingLinkInput,
  AuthClientSession,
  AuthRevokeClientSessionInput,
} from "./tier3/auth";

export type {
  AutomationListResult,
  AutomationDefinition,
  AutomationStreamEvent,
} from "./tier3/automation";

export type { ProviderComposerCapabilities } from "./tier3/provider";

export type {
  StatsGetProfileStatsInput,
  StatsGetProfileStatsResult,
  StatsGetProfileTokenStatsInput,
  StatsGetProfileTokenStatsResult,
} from "./tier3/stats";

// ─── Symbols still opaque (no Tier 3 model yet) ────────────────────────
// These remain as `extends OpaqueTransport*` stubs — the corresponding
// MCode modules were not in the T5b scope. T5c+ can model them.

/** MCode `terminal.ts` types (terminal PTY session surface). */
export interface TerminalOpenInput extends OpaqueTransportInput {}
export interface TerminalWriteInput extends OpaqueTransportInput {}
export interface TerminalAckOutputInput extends OpaqueTransportInput {}
export interface TerminalResizeInput extends OpaqueTransportInput {}
export interface TerminalClearInput extends OpaqueTransportInput {}
export interface TerminalRestartInput extends OpaqueTransportInput {}
export interface TerminalCloseInput extends OpaqueTransportInput {}

/** MCode `project.ts` types (project file/dev-server surface). */
export interface ProjectDiscoverScriptsInput extends OpaqueTransportInput {}
export interface ProjectListDirectoriesInput extends OpaqueTransportInput {}
export interface ProjectSearchEntriesInput extends OpaqueTransportInput {}
export interface ProjectSearchLocalEntriesInput extends OpaqueTransportInput {}
export interface ProjectReadFileInput extends OpaqueTransportInput {}
export interface ProjectWriteFileInput extends OpaqueTransportInput {}
export interface ProjectWriteFileResult {
  relativePath: string;
}
export interface ProjectRunDevServerResult {
  server: ProjectDevServer;
}
export interface ProjectStopDevServerInput extends OpaqueTransportInput {}
export interface ProjectStopDevServerResult extends OpaqueTransportResult {}
export interface ProjectListDevServersResult {
  servers: readonly ProjectDevServer[];
}

/** MCode `filesystem.ts` types (filesystem browser surface). */
export interface FilesystemBrowseInput extends OpaqueTransportInput {}

/** MCode `git.ts` types (git branch/worktree/stage/diff/PR surface). */
export interface GitHubRepositoryInput extends OpaqueTransportInput {}
export interface GitHubRepositoryResult {
  repository: {
    nameWithOwner: string;
    url: string;
  } | null;
}
export interface GitListBranchesInput extends OpaqueTransportInput {}
// Per MCode: GitListBranchesResult = { branches, isRepo, hasOriginRemote }.
// Vendored UI reads .branches; opaque stub collapsed it to unknown.
export type { GitListBranchesResult } from "./tier3/git";
export interface GitCreateWorktreeInput extends OpaqueTransportInput {}
export interface GitCreateWorktreeResult {
  worktree: {
    path: string;
    branch: string;
  };
}
export interface GitCreateDetachedWorktreeInput extends OpaqueTransportInput {}
export interface GitCreateDetachedWorktreeResult {
  worktree: {
    path: string;
    ref: string;
    branch: string | null;
  };
}
export interface GitRemoveWorktreeInput extends OpaqueTransportInput {}
export interface GitCreateBranchInput extends OpaqueTransportInput {}
export interface GitCheckoutInput extends OpaqueTransportInput {}
export interface GitStashAndCheckoutInput extends OpaqueTransportInput {}
export interface GitStashDropInput extends OpaqueTransportInput {}
export interface GitStashInfoInput extends OpaqueTransportInput {}
export interface GitRemoveIndexLockInput extends OpaqueTransportInput {}
export interface GitInitInput extends OpaqueTransportInput {}
export interface GitStageFilesInput extends OpaqueTransportInput {}
export interface GitStageFilesResult {
  ok: boolean;
}
export interface GitUnstageFilesInput extends OpaqueTransportInput {}
export interface GitUnstageFilesResult {
  ok: boolean;
}
export interface GitHandoffThreadInput extends OpaqueTransportInput {}
export interface GitHandoffThreadResult {
  targetMode: ThreadEnvironmentMode;
  branch: string | null;
  worktreePath: string | null;
  associatedWorktreePath: string | null;
  associatedWorktreeBranch: string | null;
  associatedWorktreeRef: string | null;
  changesTransferred: boolean;
  conflictsDetected: boolean;
  message: string | null;
}
export interface GitPullRequestRefInput extends OpaqueTransportInput {}
export interface GitPreparePullRequestThreadInput extends OpaqueTransportInput {}
export interface GitPreparePullRequestThreadResult {
  pullRequest: GitResolvedPullRequest;
  branch: string;
  worktreePath: string | null;
}
export interface GitPullInput extends OpaqueTransportInput {}
export interface GitPullResult extends OpaqueTransportResult {}
export interface GitStatusInput extends OpaqueTransportInput {}
export interface GitReadWorkingTreeDiffResult {
  patch: string;
}
export interface GitSummarizeDiffInput extends OpaqueTransportInput {}
export interface GitSummarizeDiffResult extends OpaqueTransportResult {}
export interface GitRunStackedActionInput extends OpaqueTransportInput {}

/** MCode `server.ts` types (server meta/settings/providers/diagnostics surface). */
export interface ServerGetEnvironmentResult extends OpaqueTransportResult {}
// Per MCode `packages/contracts/src/server.ts`: `ServerGetSettingsResult =
// ServerSettings`, `ServerUpdateSettingsInput = ServerSettingsPatch`,
// `ServerUpdateSettingsResult = ServerSettings`. The vendored UI relies on
// these being structurally identical (assignable both directions), so we alias
// them to the real Tier-3 shapes rather than the opaque transport stubs.
export type ServerGetSettingsResult = ServerSettings;
export type ServerUpdateSettingsInput = ServerSettingsPatch;
export type ServerUpdateSettingsResult = ServerSettings;
export interface ServerDiagnosticsResult extends OpaqueTransportResult {}
export interface ServerGenerateThreadRecapInput extends OpaqueTransportInput {}
export interface ServerGenerateThreadRecapResult {
  recap: string;
}
export interface ServerListLocalServersResult {
  generatedAt: IsoDateTime;
  servers: readonly ServerLocalServerProcess[];
}
export interface ServerManagedWorktree {
  path: string;
  workspaceRoot: string;
}
export interface ServerListWorktreesResult {
  worktrees: readonly ServerManagedWorktree[];
}
export interface ServerProviderUpdateInput extends OpaqueTransportInput {}
// Per MCode: `ServerProviderUpdateResult = ServerProviderStatusesUpdatedPayload`
// and `ServerRefreshProvidersResult = ServerProviderStatusesUpdatedPayload`.
export type ServerProviderUpdateResult = ServerProviderStatusesUpdatedPayload;
export type ServerRefreshProvidersResult = ServerProviderStatusesUpdatedPayload;
export type ServerSettingsUpdatedResult = ServerSettingsUpdatedPayload;
export interface ServerStopLocalServerResult extends OpaqueTransportResult {}
export type ServerUpsertKeybindingInput = KeybindingRule;
export interface ServerUpsertKeybindingResult extends OpaqueTransportResult {}
export interface ServerVoiceTranscriptionInput extends OpaqueTransportInput {}
export interface ServerVoiceTranscriptionResult {
  text: string;
}

/** MCode `auth.ts` types.
 *  NOTE: `AuthBootstrapResult` is intentionally NOT re-declared here — it is
 *  the ts-rs-generated concrete DTO in `../types/AuthBootstrapResult.ts`
 *  (Tier 1).
 */

/** MCode `automation.ts` types. */
export interface AutomationListInput extends OpaqueTransportInput {}
// Per MCode: `AutomationCreateInput = AutomationDefinitionConfig` and
// `AutomationUpdateInput` is the id-keyed partial. The vendored UI reads
// concrete fields off these (enabled/stopOnError/mode/…), so the opaque stub
// would surface `{}` field accesses. Alias to the real Tier-3 shapes.
export type { AutomationCreateInput, AutomationUpdateInput } from "./tier3/automation";
export interface AutomationDeleteInput extends OpaqueTransportInput {}
export interface AutomationRunNowInput extends OpaqueTransportInput {}
export interface AutomationRunNowResult {
  run: AutomationRun;
}
export interface AutomationCancelRunInput extends OpaqueTransportInput {}
export interface AutomationCancelRunResult {
  run: AutomationRun;
}
export interface AutomationMarkRunReadInput extends OpaqueTransportInput {}
export interface AutomationRunActionResult {
  run: AutomationRun;
}
export interface AutomationArchiveRunInput extends OpaqueTransportInput {}

/** MCode `providerDiscovery.ts` + `provider.ts` types. */
export interface ProviderGetComposerCapabilitiesInput extends OpaqueTransportInput {}
export interface ProviderCompactThreadInput extends OpaqueTransportInput {}
export interface ProviderListCommandsInput extends OpaqueTransportInput {}
// Per MCode `providerDiscovery.ts`: each list-result is a struct with an array
// field (models/skills/commands/agents/marketplaces) + optional source/cached.
// The vendored UI reads these arrays directly (e.g. `query.data?.models ?? []`),
// so the opaque stubs collapsed the array to `{}` and broke the `?? []`
// fallback. Alias to the real Tier-3 shapes.
export type { ProviderListCommandsResult } from "./tier3/provider";
export interface ProviderListSkillsInput extends OpaqueTransportInput {}
export type { ProviderListSkillsResult } from "./tier3/provider";
export interface ProviderSkillsCatalogInput extends OpaqueTransportInput {}
export type { ProviderSkillsCatalogResult } from "./tier3/provider";
export interface ProviderListPluginsInput extends OpaqueTransportInput {}
export type { ProviderListPluginsResult } from "./tier3/provider";
export interface ProviderReadPluginInput extends OpaqueTransportInput {}
export interface ProviderReadPluginResult extends OpaqueTransportResult {}
export interface ProviderListModelsInput extends OpaqueTransportInput {}
export type { ProviderListModelsResult } from "./tier3/provider";
export interface ProviderListAgentsInput extends OpaqueTransportInput {}
export type { ProviderListAgentsResult } from "./tier3/provider";

/** MCode `orchestration.ts` types (the aggregate stream surface). */
// Per MCode `packages/contracts/src/orchestration.ts`: `OrchestrationReadModel`
// and `OrchestrationShellSnapshot` are concrete aggregate snapshots (not opaque
// transport blobs). The vendored UI threads them through
// `SnapshotWithProjects<T>` (projectCreateRecovery), so the opaque stub would
// break structural assignability. Alias to the real Tier-3 shapes.
export type { OrchestrationReadModel, OrchestrationShellSnapshot } from "./tier3/orchestration";
// Re-exported from tier3/orchestration (real 28-variant discriminated union)
// so `Extract<ClientOrchestrationCommand, { type: "thread.create" }>` narrows
// to a real shape instead of collapsing to `never`.
export type { ClientOrchestrationCommand } from "./tier3/orchestration";
export interface OrchestrationImportThreadInput extends OpaqueTransportInput {}
export interface OrchestrationImportThreadResult extends OpaqueTransportResult {}
export interface OrchestrationGetTurnDiffInput extends OpaqueTransportInput {}
export interface OrchestrationGetTurnDiffResult {
  diff: string;
}
export interface OrchestrationGetFullThreadDiffInput extends OpaqueTransportInput {}
export interface OrchestrationGetFullThreadDiffResult {
  diff: string;
}
// Re-exported from tier3/orchestration (real 34-variant discriminated union)
// so `Extract<OrchestrationEvent, { type: "thread.message-sent" }>` narrows
// to a real shape instead of collapsing to `never`.
export type { OrchestrationEvent } from "./tier3/orchestration";
// Per MCode: shell/thread stream items are `snapshot | event` envelopes (real
// discriminated unions), not opaque blobs. The vendored UI reads
// `item.snapshot`/`item.thread`/`item.sequence`/`item.event` off them.
export type { OrchestrationShellStreamItem, OrchestrationThreadStreamItem } from "./tier3/orchestration";
export interface OrchestrationSubscribeThreadInput extends OpaqueTransportInput {}

// ─── Desktop-shell types (defined inline, verbatim from MCode ipc.ts) ────
// `ContextMenuItem` is re-exported from ./tier3/misc above (T5b); the inline
// declaration was removed to avoid a duplicate-export conflict.

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
    create: (input: ProjectCreateParams) => Promise<ProjectSummary>;
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

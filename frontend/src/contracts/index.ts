/**
 * @t3tools/contracts bridge barrel — drop-in re-export surface.
 *
 * This module is the public face of the path-identical `@t3tools/contracts`
 * shim (see CONTRACTS-BRIDGE-DESIGN.md §3.1). A cloned MCode `apps/web`
 * keeps its `import { ThreadId, type SessionView } from "@t3tools/contracts"`
 * verbatim — zero import-path edits — because `tsconfig`/`vite` alias
 * `@t3tools/contracts` → `./src/contracts` (here).
 *
 * Tier 0 (this file): re-exports ALL 26 ts-rs-generated types (the 16 from
 * `syncode-contracts/lib.rs` AND the 9 from `snapshots.rs` — the latter 9
 * were missing from the old `types/index.ts` barrel, a bug fixed here), plus
 * the hand-written branded IDs (`ids.ts`), runtime guards (`runtime.ts`),
 * and desktop-shell placeholders (`shell.ts`).
 *
 * Tier 1 (RPC registry + param types), Tier 2 (domain-event discriminated
 * union), and Tier 3 (deferred surfaces) land in sibling modules
 * (`rpc.ts`, `events.ts`, `stubs.ts`) in later tasks. Symbols they don't yet
 * define surface as ordinary TS errors (`Module has no exported member 'X'`),
 * which the compiler enumerates for free — that's the shim's whole value.
 */

// ─── Tier 0: 26 ts-rs-generated types (re-exported from ../types) ──────
// 16 from crates/syncode-contracts/src/lib.rs
export type { EntityId } from "../types/EntityId";
export type { Timestamp } from "../types/Timestamp";
export type { ProviderConfig } from "../types/ProviderConfig";
export type { ProviderCapabilities } from "../types/ProviderCapabilities";
export type { CreateSessionRequest } from "../types/CreateSessionRequest";
export type { SessionView } from "../types/SessionView";
export type { SessionStatus } from "../types/SessionStatus";
export type { MessageView } from "../types/MessageView";
export type { MessageRole } from "../types/MessageRole";
export type { GitFileStatusView } from "../types/GitFileStatusView";
export type { FileStatusKind } from "../types/FileStatusKind";
export type { GitStatusView } from "../types/GitStatusView";
export type { JsonRpcRequestView } from "../types/JsonRpcRequestView";
export type { JsonRpcResponseView } from "../types/JsonRpcResponseView";
export type { JsonRpcErrorView } from "../types/JsonRpcErrorView";
export type { PushEvent } from "../types/PushEvent";

// 9 from crates/syncode-contracts/src/snapshots.rs
// (these were MISSING from the old frontend/src/types/index.ts barrel —
//  the bug this file fixes; see CONTRACTS-BRIDGE-DESIGN.md §2.2 / §3.2)
export type { ProjectSummary } from "../types/ProjectSummary";
export type { ThreadSummary } from "../types/ThreadSummary";
export type { TurnSummary } from "../types/TurnSummary";
export type { MessageSummary } from "../types/MessageSummary";
export type { ActivitySummary } from "../types/ActivitySummary";
export type { SnapshotScope } from "../types/SnapshotScope";
export type { ShellSnapshot } from "../types/ShellSnapshot";
export type { ThreadDetailSnapshot } from "../types/ThreadDetailSnapshot";
export type { FullSnapshot } from "../types/FullSnapshot";

// ─── Tier 1: RPC served-method DTOs (from crates/syncode-contracts/src/rpc.rs) ─
// 23 concrete structs (type aliases like ProjectGetResult reuse the snapshot
// summary types above and have no dedicated .ts file). See CONTRACTS-BRIDGE-DESIGN.md §4.
export type { ListMethodsResult } from "../types/ListMethodsResult";
export type { PingResult } from "../types/PingResult";
export type { ProjectListResult } from "../types/ProjectListResult";
export type { ProjectGetParams } from "../types/ProjectGetParams";
export type { ProjectCreateParams } from "../types/ProjectCreateParams";
export type { ThreadListParams } from "../types/ThreadListParams";
export type { ThreadListResult } from "../types/ThreadListResult";
export type { ThreadGetParams } from "../types/ThreadGetParams";
export type { ThreadCreateParams } from "../types/ThreadCreateParams";
export type { ThreadLifecycleParams } from "../types/ThreadLifecycleParams";
export type { TurnListParams } from "../types/TurnListParams";
export type { TurnListResult } from "../types/TurnListResult";
export type { TurnGetParams } from "../types/TurnGetParams";
export type { TurnStartParams } from "../types/TurnStartParams";
export type { TurnCompleteParams } from "../types/TurnCompleteParams";
export type { AuthBootstrapParams } from "../types/AuthBootstrapParams";
export type { AuthBootstrapResult } from "../types/AuthBootstrapResult";
export type { AuthStatusResult } from "../types/AuthStatusResult";
export type { AuthLogoutResult } from "../types/AuthLogoutResult";
export type { PushSubscribeParams } from "../types/PushSubscribeParams";
export type { PushSubscribeResult } from "../types/PushSubscribeResult";
export type { PushUnsubscribeParams } from "../types/PushUnsubscribeParams";
export type { PushUnsubscribeResult } from "../types/PushUnsubscribeResult";

// ─── Tier 1: RPC method registry (the keystone) ────────────────────────
// Typed SERVED_RPC (21 served methods) + UNSERVED_RPC (~80 MCode methods
// returning MethodNotFound). Surfaces ServedRpcMethod/ServedRpcRequest/
// ServedRpcResult, UnservedRpcMethod, AnyRpcMethod, IsServed<M>.
export {
  SERVED_RPC,
  UNSERVED_RPC,
  type ServedRpcMethod,
  type ServedRpcRequest,
  type ServedRpcResult,
  type UnservedRpcMethod,
  type AnyRpcMethod,
  type IsServed,
} from "./rpc";

// ─── Tier 2: Domain-event discriminated union + typed push views ───────
// 44-variant tagged union (from crates/syncode-contracts/src/events.rs) +
// `DomainEventType`/`DomainEventPayload<E>` helpers, `EVENT_TYPES` const,
// `OrchestrationPushEnvelope`, and runtime guards. See
// CONTRACTS-BRIDGE-DESIGN.md §4 / §6.3 and `EVENT-MAP.md`.
export type {
  DomainEventDto,
  DomainEventType,
  DomainEventPayload,
  OrchestrationPushEnvelope,
  PushChannelViews,
} from "./events";
export {
  EVENT_TYPES,
  isDomainEventDto,
  isOrchestrationPushEnvelope,
} from "./events";

// ─── Hand-written bridge modules ───────────────────────────────────────
// Branded IDs (ThreadId, ProjectId, …) — replaces MCode baseSchemas.ts brand set.
export type {
  Branded,
  ThreadId,
  ProjectId,
  TurnId,
  MessageId,
  EventId,
  CommandId,
  SessionId,
  ProviderItemId,
  RuntimeSessionId,
  CheckpointRef,
  AutomationId,
  ApprovalRequestId,
} from "./ids";
export {
  asId,
  asThreadId,
  asProjectId,
  asTurnId,
  asMessageId,
  asEventId,
  asCommandId,
  asSessionId,
  asProviderItemId,
  asRuntimeSessionId,
  asCheckpointRef,
  asAutomationId,
  asApprovalRequestId,
} from "./ids";

// Minimal runtime guards — replaces Effect Schema.is / safe-decode usage.
export {
  isObject,
  hasKey,
  isString,
  isNumber,
  isBoolean,
  safeParse,
  decodeWithDefault,
} from "./runtime";

// Desktop-shell interfaces (NativeApi / DesktopBridge) + supporting types —
// full surfaces copied verbatim from MCode ipc.ts during the T6/B4 shell swap.
// These satisfy vendored UI imports of the desktop bridge types from
// `@t3tools/contracts`. See `shell.ts` for the source-of-truth header.
export type {
  NativeApi,
  DesktopBridge,
  ContextMenuItem,
  DesktopUpdateStatus,
  DesktopRuntimeArch,
  DesktopTheme,
  DesktopRuntimeInfo,
  DesktopUpdateState,
  DesktopUpdateActionResult,
  BrowserTabState,
  ThreadBrowserState,
  BrowserOpenInput,
  BrowserThreadInput,
  BrowserTabInput,
  BrowserNavigateInput,
  BrowserNewTabInput,
  BrowserPanelBounds,
  BrowserSetPanelBoundsInput,
  BrowserAttachWebviewInput,
  BrowserDetachWebviewInput,
  BrowserCaptureScreenshotResult,
  BrowserExecuteCdpInput,
  BrowserCopyLinkEvent,
  DesktopNotificationInput,
  DesktopWindowState,
  EditorId,
  // Supporting transport type aliases (terminal/git/project/server/auth/
  // automation/provider/stats/orchestration/filesystem). Self-contained in
  // shell.ts; replace with `import type` when the matching Tier 1/2/3 modules
  // land. Re-exported so vendored UI importing them from `@t3tools/contracts`
  // resolves, and so the Tauri NativeApi impl can import them.
  TerminalOpenInput,
  TerminalWriteInput,
  TerminalAckOutputInput,
  TerminalResizeInput,
  TerminalClearInput,
  TerminalRestartInput,
  TerminalCloseInput,
  TerminalSessionSnapshot,
  TerminalEvent,
  ProjectDiscoverScriptsInput,
  ProjectDiscoverScriptsResult,
  ProjectListDirectoriesInput,
  ProjectListDirectoriesResult,
  ProjectSearchEntriesInput,
  ProjectSearchEntriesResult,
  ProjectSearchLocalEntriesInput,
  ProjectSearchLocalEntriesResult,
  ProjectReadFileInput,
  ProjectReadFileResult,
  ProjectWriteFileInput,
  ProjectWriteFileResult,
  ProjectRunDevServerInput,
  ProjectRunDevServerResult,
  ProjectStopDevServerInput,
  ProjectStopDevServerResult,
  ProjectListDevServersResult,
  ProjectDevServerEvent,
  FilesystemBrowseInput,
  FilesystemBrowseResult,
  GitHubRepositoryInput,
  GitHubRepositoryResult,
  GitListBranchesInput,
  GitListBranchesResult,
  GitCreateWorktreeInput,
  GitCreateWorktreeResult,
  GitCreateDetachedWorktreeInput,
  GitCreateDetachedWorktreeResult,
  GitRemoveWorktreeInput,
  GitCreateBranchInput,
  GitCheckoutInput,
  GitStashAndCheckoutInput,
  GitStashDropInput,
  GitStashInfoInput,
  GitStashInfoResult,
  GitRemoveIndexLockInput,
  GitInitInput,
  GitStageFilesInput,
  GitStageFilesResult,
  GitUnstageFilesInput,
  GitUnstageFilesResult,
  GitHandoffThreadInput,
  GitHandoffThreadResult,
  GitPullRequestRefInput,
  GitResolvePullRequestResult,
  GitPreparePullRequestThreadInput,
  GitPreparePullRequestThreadResult,
  GitPullInput,
  GitPullResult,
  GitStatusInput,
  GitStatusResult,
  GitReadWorkingTreeDiffInput,
  GitReadWorkingTreeDiffResult,
  GitSummarizeDiffInput,
  GitSummarizeDiffResult,
  GitRunStackedActionInput,
  GitRunStackedActionResult,
  GitActionProgressEvent,
  ServerConfig,
  ServerGetEnvironmentResult,
  ServerGetSettingsResult,
  ServerUpdateSettingsInput,
  ServerUpdateSettingsResult,
  ServerDiagnosticsResult,
  ServerGenerateAutomationIntentInput,
  ServerGenerateAutomationIntentResult,
  ServerGenerateThreadRecapInput,
  ServerGenerateThreadRecapResult,
  ServerGetProviderUsageSnapshotInput,
  ServerGetProviderUsageSnapshotResult,
  ServerListProviderUsageInput,
  ServerListProviderUsageResult,
  ServerListLocalServersResult,
  ServerListWorktreesResult,
  ServerProviderUpdateInput,
  ServerProviderUpdateResult,
  ServerRefreshProvidersResult,
  ServerStopLocalServerInput,
  ServerStopLocalServerResult,
  ServerUpsertKeybindingInput,
  ServerUpsertKeybindingResult,
  ServerVoiceTranscriptionInput,
  ServerVoiceTranscriptionResult,
  AuthBootstrapInput,
  // AuthBootstrapResult comes from Tier 1 (../types/AuthBootstrapResult.ts) —
  // already re-exported above; not duplicated here.
  AuthBearerBootstrapResult,
  AuthWebSocketTokenResult,
  AuthSessionState,
  AuthCreatePairingCredentialInput,
  AuthPairingCredentialResult,
  AuthPairingLink,
  AuthRevokePairingLinkInput,
  AuthClientSession,
  AuthRevokeClientSessionInput,
  AutomationListInput,
  AutomationListResult,
  AutomationCreateInput,
  AutomationDefinition,
  AutomationUpdateInput,
  AutomationDeleteInput,
  AutomationRunNowInput,
  AutomationRunNowResult,
  AutomationCancelRunInput,
  AutomationCancelRunResult,
  AutomationMarkRunReadInput,
  AutomationRunActionResult,
  AutomationArchiveRunInput,
  AutomationStreamEvent,
  ProviderGetComposerCapabilitiesInput,
  ProviderComposerCapabilities,
  ProviderCompactThreadInput,
  ProviderListCommandsInput,
  ProviderListCommandsResult,
  ProviderListSkillsInput,
  ProviderListSkillsResult,
  ProviderSkillsCatalogInput,
  ProviderSkillsCatalogResult,
  ProviderListPluginsInput,
  ProviderListPluginsResult,
  ProviderReadPluginInput,
  ProviderReadPluginResult,
  ProviderListModelsInput,
  ProviderListModelsResult,
  ProviderListAgentsInput,
  ProviderListAgentsResult,
  StatsGetProfileStatsInput,
  StatsGetProfileStatsResult,
  StatsGetProfileTokenStatsInput,
  StatsGetProfileTokenStatsResult,
  OrchestrationReadModel,
  OrchestrationShellSnapshot,
  ClientOrchestrationCommand,
  OrchestrationImportThreadInput,
  OrchestrationImportThreadResult,
  OrchestrationGetTurnDiffInput,
  OrchestrationGetTurnDiffResult,
  OrchestrationGetFullThreadDiffInput,
  OrchestrationGetFullThreadDiffResult,
  OrchestrationEvent,
  OrchestrationShellStreamItem,
  OrchestrationThreadStreamItem,
  OrchestrationSubscribeThreadInput,
} from "./shell";

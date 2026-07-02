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

// ─── Tier 3: 139 deferred MCode-contracts symbols ─────────────────────
// Hand-ported from MCode `packages/contracts/src/*.ts` (Effect Schema →
// plain TS) so the vendored MCode `apps/web` UI's `import { … } from
// "@t3tools/contracts"` resolves. Real shapes for served-transport +
// core-UI domains (orchestration/provider/server/project/auth/automation/
// git/ws/terminal/stats/keybindings/model); minimal stubs (marked
// `STUB(T5c)`) for the rest. See `MISSING-SYMBOLS.md` and
// `tier3/TIER3-STATUS.md`.

// base primitives + extra branded IDs
export type {
  TrimmedString,
  TrimmedNonEmptyString,
  NonNegativeInt,
  PositiveInt,
  IsoDateTime,
  ProcessEnvRecord,
  ThreadMarkerId,
  AutomationRunId,
  EnvironmentId,
  AuthSessionId,
} from "./tier3/base";
export {
  asThreadMarkerId,
  asAutomationRunId,
  asEnvironmentId,
  asAuthSessionId,
} from "./tier3/base";

// orchestration + provider-kind + model-selection union + constants
export type {
  ProviderKind,
  ProviderWithDefaultModel,
  RuntimeMode,
  ProviderInteractionMode,
  ProviderApprovalPolicy,
  ProviderSandboxMode,
  ProviderRequestKind,
  ProviderApprovalDecision,
  AssistantDeliveryMode,
  TurnDispatchMode,
  ThreadEnvironmentMode,
  OrchestrationMessageSource,
  OrchestrationSessionStatus,
  CodexModelOptions,
  ClaudeModelOptions,
  CursorModelOptions,
  GeminiModelOptions,
  GrokModelOptions,
  OpenCodeModelOptions,
  KiloModelOptions,
  PiModelOptions,
  ProviderModelOptions,
  CodexModelSelection,
  ClaudeModelSelection,
  CursorModelSelection,
  GeminiModelSelection,
  GrokModelSelection,
  OpenCodeModelSelection,
  KiloModelSelection,
  PiModelSelection,
  ModelSelection,
  CodexReasoningEffort,
  ClaudeCodeEffort,
  GeminiThinkingLevel,
  GeminiThinkingBudget,
  PiThinkingLevel,
  GrokReasoningEffort,
  CodexProviderStartOptions,
  ClaudeProviderStartOptions,
  GeminiProviderStartOptions,
  CursorProviderStartOptions,
  GrokProviderStartOptions,
  OpenCodeProviderStartOptions,
  KiloProviderStartOptions,
  PiProviderStartOptions,
  ProviderStartOptions,
  ProviderUserInputAnswer,
  ProviderUserInputAnswers,
  ProviderMentionReference,
  ProviderSkillReference,
  EffortOption,
  ContextWindowOption,
  ModelCapabilities,
  ModelSlug,
  PinnedMessageLabel,
  PinnedMessage,
  ThreadMarkerStyle,
  ThreadMarkerColor,
  ThreadMarker,
  ThreadHandoffBootstrapStatus,
  ThreadHandoff,
  ThreadHandoffImportedMessage,
  OrchestrationMessageRole,
  OrchestrationMessage,
  OrchestrationThreadActivityTone,
  OrchestrationThreadActivity,
  OrchestrationSession,
  OrchestrationProposedPlanId,
  OrchestrationProposedPlan,
  OrchestrationLatestTurnState,
  OrchestrationLatestTurn,
  OrchestrationThreadPullRequest,
  ThreadTokenUsageSnapshot,
  ClientOrchestrationCommand as ClientOrchestrationCommandT3,
  OrchestrationThread,
  OrchestrationProjectShell,
  OrchestrationThreadShell,
  OrchestrationShellStreamEvent,
} from "./tier3/orchestration";
export {
  CODEX_REASONING_EFFORT_OPTIONS,
  CLAUDE_CODE_EFFORT_OPTIONS,
  GEMINI_THINKING_LEVEL_OPTIONS,
  GEMINI_THINKING_BUDGET_OPTIONS,
  PI_THINKING_LEVEL_OPTIONS,
  GROK_REASONING_EFFORT_OPTIONS,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
  PROVIDER_SEND_TURN_MAX_IMAGE_BYTES,
  PROVIDER_SEND_TURN_MAX_FILE_BYTES,
  MAX_PINNED_PROJECTS,
  CHAT_ASSISTANT_SELECTION_TEXT_MAX_CHARS,
  THREAD_NOTES_MAX_CHARS,
  PINNED_MESSAGES_MAX_COUNT,
  PINNED_MESSAGE_LABEL_MAX_CHARS,
  THREAD_MARKER_LABEL_MAX_CHARS,
  ORCHESTRATION_WS_METHODS,
  ORCHESTRATION_WS_CHANNELS,
  DEFAULT_RUNTIME_MODE,
  DEFAULT_TURN_DISPATCH_MODE,
} from "./tier3/orchestration";

// model catalog constants + provider display names
export {
  MODEL_OPTIONS_BY_PROVIDER,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_GIT_TEXT_GENERATION_MODEL,
  MODEL_SLUG_ALIASES_BY_PROVIDER,
  MODEL_CAPABILITIES_INDEX,
  PROVIDER_DISPLAY_NAMES,
} from "./tier3/model";

// provider discovery + agent-mention aliases + functions
export type {
  ProviderOptionChoice,
  SelectProviderOptionDescriptor,
  BooleanProviderOptionDescriptor,
  ProviderOptionDescriptor,
  ProviderOptionSelection,
  ProviderSkillInterface,
  ProviderSkillDescriptor,
  ProviderNativeCommandDescriptor,
  ProviderPluginMarketplaceInterface,
  ProviderPluginInstallPolicy,
  ProviderPluginAuthPolicy,
  ProviderPluginSource,
  ProviderPluginInterface,
  ProviderPluginDescriptor,
  ProviderPluginMarketplaceLoadError,
  ProviderPluginMarketplaceDescriptor,
  ProviderPluginAppSummary,
  ProviderPluginDetail,
  ProviderReasoningEffortDescriptor,
  ProviderContextWindowDescriptor,
  ProviderModelDescriptor,
  ProviderAgentDescriptor,
  CodexAgentAliasDefinition,
  ClaudeSubagentAliasDefinition,
  AgentAliasDefinition,
  ResolvedAgentAlias,
} from "./tier3/provider";
export {
  AGENT_MENTION_ALIASES_BY_PROVIDER,
  AGENT_MENTION_ALIASES,
  getAgentMentionAliases,
  getAgentMentionAutocompleteAliases,
  resolveAgentAlias,
  isValidAgentAlias,
  getAgentAliasNames,
} from "./tier3/provider";

// automation domain
export type {
  AutomationTimeOfDay,
  AutomationTimezone,
  AutomationCronExpression,
  AutomationSchedule,
  AutomationWorktreeMode,
  AutomationMode,
  AutomationTrigger,
  AutomationRunStatus,
  AutomationRunResultOutcome,
  AutomationRunResultCompletionEvaluation,
  AutomationRunResult,
  AutomationAllowedCapability,
  AutomationPermissionSnapshot,
  AutomationRetryPolicy,
  AutomationMisfirePolicy,
  AutomationCompletionPolicy,
  AutomationDefinition as AutomationDefinitionT3,
  AutomationRun,
  AutomationListResult as AutomationListResultT3,
  AutomationStreamEvent as AutomationStreamEventT3,
} from "./tier3/automation";
export {
  DEFAULT_AUTOMATION_RUNTIME_MODE,
  DEFAULT_AUTOMATION_MINIMUM_INTERVAL_SECONDS,
  DEFAULT_AUTOMATION_FAST_INTERVAL_MAX_ITERATIONS,
  DEFAULT_AUTOMATION_MAX_RUNTIME_SECONDS,
  DEFAULT_AUTOMATION_RETRY_POLICY,
  DEFAULT_AUTOMATION_MISFIRE_POLICY,
  DEFAULT_AUTOMATION_COMPLETION_POLICY,
  DEFAULT_AUTOMATION_STOP_CONFIDENCE_THRESHOLD,
} from "./tier3/automation";

// server domain
export type {
  ServerProviderStatusState,
  ServerProviderAuthStatus,
  ServerProviderVersionAdvisory,
  ServerProviderUpdateState,
  ServerProviderStatus,
  ServerProviderUsageLimit,
  ServerProviderUsageLine,
  ProviderUsageStatus,
  ServerProviderUsageSnapshot,
  ServerLocalServerAddress,
  ServerLocalServerProcess,
  ServerConfigIssue,
  ServerConfig as ServerConfigT3,
  ProviderSettingsBase,
  CodexServerProviderSettings,
  ClaudeServerProviderSettings,
  GeminiServerProviderSettings,
  GrokServerProviderSettings,
  CursorServerProviderSettings,
  OpenCodeServerProviderSettings,
  KiloServerProviderSettings,
  PiServerProviderSettings,
  SkillsServerSettings,
  ServerSettings,
  ServerSettingsPatch,
  ServerConfigUpdatedPayload,
  ServerProviderStatusesUpdatedPayload,
  ServerSettingsUpdatedPayload,
  ServerLifecycleWelcomePayload,
  ServerLifecycleStreamEvent,
  ServerConfigStreamEvent,
  ServerAutomationIntentMissingField,
  ServerGenerateAutomationIntentInput as ServerGenerateAutomationIntentInputT3,
  ServerGenerateAutomationIntentResult as ServerGenerateAutomationIntentResultT3,
  ServerGetProviderUsageSnapshotInput as ServerGetProviderUsageSnapshotInputT3,
  ServerGetProviderUsageSnapshotResult as ServerGetProviderUsageSnapshotResultT3,
  ServerListProviderUsageInput as ServerListProviderUsageInputT3,
  ServerListProviderUsageResult as ServerListProviderUsageResultT3,
  ServerStopLocalServerInput as ServerStopLocalServerInputT3,
} from "./tier3/server";
export { DEFAULT_SERVER_SETTINGS } from "./tier3/server";

// project domain
export type {
  ProjectKind,
  ProjectEntryKind,
  ProjectEntry,
  ProjectDirectoryEntry,
  ProjectFileSystemEntry,
  ProjectLocalSearchEntry,
  ProjectDiscoveredScript,
  ProjectDiscoveredScriptTarget,
  ProjectDiscoverScriptsResult as ProjectDiscoverScriptsResultT3,
  ProjectListDirectoriesResult as ProjectListDirectoriesResultT3,
  ProjectSearchEntriesResult as ProjectSearchEntriesResultT3,
  ProjectSearchLocalEntriesResult as ProjectSearchLocalEntriesResultT3,
  ProjectReadFileResult as ProjectReadFileResultT3,
  ProjectScriptIcon,
  ProjectScript,
  ProjectDevServerStatus,
  ProjectDevServer,
  ProjectDevServerRemovedReason,
  ProjectDevServerEvent as ProjectDevServerEventT3,
  ProjectRunDevServerInput as ProjectRunDevServerInputT3,
} from "./tier3/project";

// git domain
export type {
  GitStackedAction,
  GitActionProgressPhase,
  GitActionProgressKind,
  GitActionProgressStream,
  GitBranch,
  GitStatusPr,
  GitStatusFile,
  GitStatusResult as GitStatusResultT3,
  GitStashInfoResult as GitStashInfoResultT3,
  GitResolvedPullRequest,
  GitResolvePullRequestResult as GitResolvePullRequestResultT3,
  GitReadWorkingTreeDiffInput as GitReadWorkingTreeDiffInputT3,
  GitBranchStepStatus,
  GitCommitStepStatus,
  GitPushStepStatus,
  GitPrStepStatus,
  GitRunStackedActionResult as GitRunStackedActionResultT3,
  GitActionProgressEvent as GitActionProgressEventT3,
} from "./tier3/git";

// websocket / push channel domain
export type {
  WsWelcomePayload,
  WsPushPayloadByChannel,
  WsPushChannel,
  WsPushData,
  WsPushMessage,
  WsPush,
} from "./tier3/ws";
export { WS_METHODS, WS_CHANNELS, WsRpcGroup } from "./tier3/ws";

// terminal domain (real shapes; terminal Input DTOs remain in shell.ts)
export type {
  TerminalSessionStatus,
  TerminalSessionSnapshot as TerminalSessionSnapshotT3,
  TerminalEvent as TerminalEventT3,
} from "./tier3/terminal";
export {
  TERMINAL_MIN_COLS,
  TERMINAL_MAX_COLS,
  TERMINAL_MIN_ROWS,
  TERMINAL_MAX_ROWS,
  DEFAULT_TERMINAL_ID,
} from "./tier3/terminal";

// auth domain (real shapes; AuthBootstrapResult from Tier 1 ts-rs)
export type {
  ServerAuthPolicy,
  ServerAuthBootstrapMethod,
  ServerAuthSessionMethod,
  AuthSessionRole,
  ServerAuthDescriptor,
  AuthBootstrapInput as AuthBootstrapInputT3,
  AuthBearerBootstrapResult as AuthBearerBootstrapResultT3,
  AuthWebSocketTokenResult as AuthWebSocketTokenResultT3,
  AuthPairingCredentialResult as AuthPairingCredentialResultT3,
  AuthPairingLink as AuthPairingLinkT3,
  AuthClientMetadataDeviceType,
  AuthClientMetadata,
  AuthClientSession as AuthClientSessionT3,
  AuthRevokeClientSessionInput as AuthRevokeClientSessionInputT3,
  AuthCreatePairingCredentialInput as AuthCreatePairingCredentialInputT3,
  AuthRevokePairingLinkInput as AuthRevokePairingLinkInputT3,
  AuthSessionState as AuthSessionStateT3,
} from "./tier3/auth";

// stats domain
export type {
  StatsGetProfileStatsInput as StatsGetProfileStatsInputT3,
  StatsGetProfileTokenStatsInput as StatsGetProfileTokenStatsInputT3,
  ProfileHeatmapCell,
  ProfileProviderUsage,
  ProfileSkillUsage,
  ProfileMostWorkedProject,
  ProfileQuota,
  ProfileActivity,
  ProfileActiveHours,
  ProfileInsights,
  ProfileIdentity,
  ProfileTimezone,
  ProfileStats,
  ProfileTokenStats,
  StatsGetProfileStatsResult as StatsGetProfileStatsResultT3,
  StatsGetProfileTokenStatsResult as StatsGetProfileTokenStatsResultT3,
} from "./tier3/stats";

// keybindings domain
export type {
  ThreadJumpKeybindingCommand,
  KeybindingCommand,
  KeybindingRule,
  KeybindingShortcut,
  KeybindingWhenNode,
  ResolvedKeybindingRule,
  ResolvedKeybindingsConfig,
} from "./tier3/keybindings";
export {
  MAX_KEYBINDING_VALUE_LENGTH,
  MAX_SCRIPT_ID_LENGTH,
  THREAD_JUMP_KEYBINDING_COMMANDS,
  SCRIPT_RUN_COMMAND_PATTERN,
} from "./tier3/keybindings";

// misc / cross-cutting (editors, context menu, tool lifecycle, etc.)
export type {
  EditorLaunchStyle,
  EditorDefinition,
  EditorId as EditorIdT3,
  ContextMenuItem as ContextMenuItemT3,
  ToolLifecycleItemType,
  UserInputQuestion,
  UploadChatImageAttachment,
  UploadChatFileAttachment,
  UploadChatAssistantSelectionAttachment,
  UploadChatAttachment,
  FilesystemBrowseResult as FilesystemBrowseResultT3,
} from "./tier3/misc";
export {
  EDITORS,
  isToolLifecycleItemType,
  DEFAULT_PROVIDER_KIND,
} from "./tier3/misc";

// SCRIPT_RUN_COMMAND_PATTERN (also exported above from keybindings; ensure
// single export — the keybindings module is the canonical source).

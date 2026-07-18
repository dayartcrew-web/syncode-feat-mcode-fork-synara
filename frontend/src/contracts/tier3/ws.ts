/**
 * Tier 3 — WebSocket / push-channel domain.
 *
 * Hand-ported from MCode `packages/contracts/src/ws.ts` (Effect Schema →
 * plain TS types). Covers the WS method/channel constant maps and the typed
 * push envelope (`WsPush` union, `WsPushChannel`, `WsPushData<C>`,
 * `WsPushMessage<C>`, `WsWelcomePayload`) the transport layer + event
 * router import. The MCode contracts module derives these from a
 * `WsPushPayloadByChannel` interface keyed on `WS_CHANNELS`/`ORCHESTRATION_WS_CHANNELS`;
 * we mirror that derivation in plain TS here.
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/ws.ts
 */

import type { ProjectId, ThreadId } from "../ids";
import type { NonNegativeInt, TrimmedNonEmptyString } from "./base";
import type { ORCHESTRATION_WS_CHANNELS } from "./orchestration";
import type {
  ServerConfigUpdatedPayload,
  ServerLifecycleStreamEvent,
  ServerProviderStatusesUpdatedPayload,
  ServerSettingsUpdatedPayload,
} from "./server";
import type { AutomationStreamEvent } from "./automation";
import type { GitActionProgressEvent } from "./git";
import type { TerminalEvent } from "./terminal";
import type { ProjectDevServerEvent } from "./project";
import type { OrchestrationEvent, OrchestrationShellStreamItem, OrchestrationThreadStreamItem } from "../shell";
import type { OrchestrationPushEnvelope } from "../events";

// ─── WS method names (camelCase keys → dot method strings) ────────────

export const WS_METHODS = {
  // Project registry methods
  projectsList: "projects.list",
  projectsAdd: "projects.add",
  projectsRemove: "projects.remove",
  // `project/create` (syncode served form) — registers a folder as a backend
  // project entity so it shows in the sidebar. Used by the "Work in a project"
  // picker after a folder is selected. No MCode dot-string remap; the served
  // method is passed through directly (see wsTransport.ts passthrough).
  projectCreate: "project/create",
  projectsDiscoverScripts: "projects.discoverScripts",
  projectsListDirectories: "projects.listDirectories",
  projectsSearchEntries: "projects.searchEntries",
  projectsSearchLocalEntries: "projects.searchLocalEntries",
  projectsReadFile: "projects.readFile",
  projectsWriteFile: "projects.writeFile",
  projectsRunDevServer: "projects.runDevServer",
  projectsStopDevServer: "projects.stopDevServer",
  projectsListDevServers: "projects.listDevServers",
  subscribeProjectDevServerEvents: "projects.subscribeDevServerEvents",

  // Filesystem browse methods
  filesystemBrowse: "filesystem.browse",

  // Shell methods
  shellOpenInEditor: "shell.openInEditor",

  // Git methods
  gitPull: "git.pull",
  gitGithubRepository: "git.githubRepository",
  gitStatus: "git.status",
  gitReadWorkingTreeDiff: "git.readWorkingTreeDiff",
  gitSummarizeDiff: "git.summarizeDiff",
  gitRunStackedAction: "git.runStackedAction",
  gitListBranches: "git.listBranches",
  gitCreateWorktree: "git.createWorktree",
  gitCreateDetachedWorktree: "git.createDetachedWorktree",
  gitRemoveWorktree: "git.removeWorktree",
  gitCreateBranch: "git.createBranch",
  gitCheckout: "git.checkout",
  gitStashAndCheckout: "git.stashAndCheckout",
  gitStashDrop: "git.stashDrop",
  gitStashInfo: "git.stashInfo",
  gitRemoveIndexLock: "git.removeIndexLock",
  gitInit: "git.init",
  gitStageFiles: "git.stageFiles",
  gitUnstageFiles: "git.unstageFiles",
  gitHandoffThread: "git.handoffThread",
  gitResolvePullRequest: "git.resolvePullRequest",
  gitPreparePullRequestThread: "git.preparePullRequestThread",

  // Terminal methods
  terminalOpen: "terminal.open",
  terminalWrite: "terminal.write",
  terminalAckOutput: "terminal.ackOutput",
  terminalResize: "terminal.resize",
  terminalClear: "terminal.clear",
  terminalRestart: "terminal.restart",
  terminalClose: "terminal.close",

  // Server meta
  serverGetConfig: "server.getConfig",
  serverGetEnvironment: "server.getEnvironment",
  serverGetSettings: "server.getSettings",
  serverUpdateSettings: "server.updateSettings",
  serverRefreshProviders: "server.refreshProviders",
  serverUpdateProvider: "server.updateProvider",
  serverListWorktrees: "server.listWorktrees",
  serverListLocalServers: "server.listLocalServers",
  serverStopLocalServer: "server.stopLocalServer",
  serverGetProviderUsageSnapshot: "server.getProviderUsageSnapshot",
  serverListProviderUsage: "server.listProviderUsage",
  statsGetProfileStats: "stats.getProfileStats",
  statsGetProfileTokenStats: "stats.getProfileTokenStats",
  serverGetDiagnostics: "server.getDiagnostics",
  serverTranscribeVoice: "server.transcribeVoice",
  serverGenerateThreadRecap: "server.generateThreadRecap",
  serverGenerateAutomationIntent: "server.generateAutomationIntent",
  serverUpsertKeybinding: "server.upsertKeybinding",
  subscribeServerLifecycle: "server.subscribeLifecycle",
  subscribeServerConfig: "server.subscribeConfig",
  subscribeServerProviderStatuses: "server.subscribeProviderStatuses",
  subscribeServerSettings: "server.subscribeSettings",

  // Streaming subscriptions
  subscribeTerminalEvents: "terminal.subscribeEvents",
  subscribeOrchestrationDomainEvents: "orchestration.subscribeDomainEvents",
  subscribeGitActionProgress: "git.subscribeActionProgress",

  // Provider discovery
  providerGetComposerCapabilities: "provider.getComposerCapabilities",
  providerCompactThread: "provider.compactThread",
  providerListCommands: "provider.listCommands",
  providerListSkills: "provider.listSkills",
  providerListSkillsCatalog: "provider.listSkillsCatalog",
  providerListPlugins: "provider.listPlugins",
  providerReadPlugin: "provider.readPlugin",
  providerListModels: "provider.listModels",
  providerListAgents: "provider.listAgents",
  providerListMcpCatalog: "provider.listMcpCatalog",

  // MCP server management
  mcpCreate: "mcp.create",
  mcpUpdate: "mcp.update",
  mcpDelete: "mcp.delete",
  mcpTestConnection: "mcp.testConnection",

  // Automation methods
  automationList: "automation.list",
  automationCreate: "automation.create",
  automationUpdate: "automation.update",
  automationDelete: "automation.delete",
  automationRunNow: "automation.runNow",
  automationCancelRun: "automation.cancelRun",
  automationMarkRunRead: "automation.markRunRead",
  automationArchiveRun: "automation.archiveRun",
  subscribeAutomationEvents: "automation.subscribe",
} as const;

// ─── Push event channels ──────────────────────────────────────────────

export const WS_CHANNELS = {
  automationEvent: "automation.event",
  gitActionProgress: "git.actionProgress",
  terminalEvent: "terminal.event",
  projectDevServerEvent: "project.devServerEvent",
  serverWelcome: "server.welcome",
  serverMaintenanceUpdated: "server.maintenanceUpdated",
  serverConfigUpdated: "server.configUpdated",
  serverProviderStatusesUpdated: "server.providerStatusesUpdated",
  serverSettingsUpdated: "server.settingsUpdated",
} as const;

// ─── Welcome payload ──────────────────────────────────────────────────

export interface WsWelcomePayload {
  cwd: TrimmedNonEmptyString;
  homeDir?: TrimmedNonEmptyString;
  chatWorkspaceRoot?: TrimmedNonEmptyString;
  projectName: TrimmedNonEmptyString;
  bootstrapProjectId?: ProjectId;
  bootstrapThreadId?: ThreadId;
}

// ─── WsPushPayloadByChannel + channel-keyed push types ────────────────

export interface WsPushPayloadByChannel {
  readonly [WS_CHANNELS.serverWelcome]: WsWelcomePayload;
  readonly [WS_CHANNELS.serverMaintenanceUpdated]: ServerLifecycleStreamEvent;
  readonly [WS_CHANNELS.serverConfigUpdated]: ServerConfigUpdatedPayload;
  readonly [WS_CHANNELS.serverProviderStatusesUpdated]: ServerProviderStatusesUpdatedPayload;
  readonly [WS_CHANNELS.serverSettingsUpdated]: ServerSettingsUpdatedPayload;
  readonly [WS_CHANNELS.automationEvent]: AutomationStreamEvent;
  readonly [WS_CHANNELS.gitActionProgress]: GitActionProgressEvent;
  readonly [WS_CHANNELS.terminalEvent]: TerminalEvent;
  readonly [WS_CHANNELS.projectDevServerEvent]: ProjectDevServerEvent;
  /** Bare multiplexed orchestration channel (`push/orchestration`) — the raw
   *  envelope the backend actually emits (PascalCase `eventType`, double-
   *  nested `data`). The wsNativeApi demux adapts it to `OrchestrationEvent`. */
  readonly orchestration: OrchestrationPushEnvelope;
  readonly [ORCHESTRATION_WS_CHANNELS.domainEvent]: OrchestrationEvent;
  readonly [ORCHESTRATION_WS_CHANNELS.shellEvent]: OrchestrationShellStreamItem;
  readonly [ORCHESTRATION_WS_CHANNELS.threadEvent]: OrchestrationThreadStreamItem;
}

export type WsPushChannel = keyof WsPushPayloadByChannel;
export type WsPushData<C extends WsPushChannel> = WsPushPayloadByChannel[C];

interface WsPushEnvelopeBase {
  readonly type: "push";
  readonly sequence: NonNegativeInt;
  readonly channel: WsPushChannel;
}

export interface WsPushMessage<C extends WsPushChannel> extends WsPushEnvelopeBase {
  readonly channel: C;
  readonly data: WsPushData<C>;
}

export type WsPush = {
  [C in WsPushChannel]: WsPushMessage<C>;
}[WsPushChannel];

/** RPC grouping for the WS transport re-wire. Mirrors MCode's enum. */
export enum WsRpcGroup {
  System = "system",
  Project = "project",
  Filesystem = "filesystem",
  Git = "git",
  Terminal = "terminal",
  Server = "server",
  Provider = "provider",
  Automation = "automation",
  Orchestration = "orchestration",
}

/**
 * Tier 3 — Server domain (config / settings / providers / lifecycle / usage).
 *
 * Hand-ported from MCode `packages/contracts/src/server.ts` and
 * `settings.ts` (Effect Schema → plain TS types). Covers the served-transport
 * server-meta DTOs: ServerConfig, ServerSettings + patch, ServerProviderStatus
 * (+ auth status / advisory / update state), local-server process, lifecycle
 * stream event, automation-intent generation, provider-usage snapshot, and
 * the config/settings/provider-statuses push payloads.
 *
 * Source of truth:
 *   /home/vibe-dev/mcode/packages/contracts/src/server.ts
 *   /home/vibe-dev/mcode/packages/contracts/src/settings.ts
 */

import type { ProjectId, ThreadId } from "../ids";
import type { IsoDateTime, NonNegativeInt, PositiveInt, TrimmedNonEmptyString } from "./base";
import type {
  AutomationCompletionPolicy,
  AutomationMode,
  AutomationSchedule,
} from "./automation";
import type {
  ModelSelection,
  ProviderKind,
  ProviderStartOptions,
  ThreadEnvironmentMode,
} from "./orchestration";
import type { ResolvedKeybindingsConfig } from "./keybindings";
import type { EditorId } from "./misc";

// ─── Server provider status ───────────────────────────────────────────

export type ServerProviderStatusState = "ready" | "warning" | "error";
export type ServerProviderAuthStatus =
  | "authenticated"
  | "unauthenticated"
  | "unknown";

export interface ServerProviderVersionAdvisory {
  status: "unknown" | "current" | "behind_latest";
  currentVersion: TrimmedNonEmptyString | null;
  latestVersion: TrimmedNonEmptyString | null;
  updateCommand: TrimmedNonEmptyString | null;
  canUpdate: boolean;
  checkedAt: IsoDateTime | null;
  message: TrimmedNonEmptyString | null;
}

export interface ServerProviderUpdateState {
  status: "idle" | "queued" | "running" | "succeeded" | "failed" | "unchanged";
  startedAt: IsoDateTime | null;
  finishedAt: IsoDateTime | null;
  message: TrimmedNonEmptyString | null;
  output: string | null;
}

export interface ServerProviderStatus {
  provider: ProviderKind;
  status: ServerProviderStatusState;
  available: boolean;
  authStatus: ServerProviderAuthStatus;
  authType?: TrimmedNonEmptyString;
  authLabel?: TrimmedNonEmptyString;
  voiceTranscriptionAvailable?: boolean;
  version?: TrimmedNonEmptyString | null;
  checkedAt: IsoDateTime;
  message?: TrimmedNonEmptyString;
  versionAdvisory?: ServerProviderVersionAdvisory;
  updateState?: ServerProviderUpdateState;
}

export interface ServerProviderUsageLimit {
  window: TrimmedNonEmptyString;
  usedPercent?: number;
  resetsAt?: IsoDateTime;
  windowDurationMins?: NonNegativeInt;
}

export interface ServerProviderUsageLine {
  label: TrimmedNonEmptyString;
  value: TrimmedNonEmptyString;
  subtitle?: TrimmedNonEmptyString;
}

export type ProviderUsageStatus = "ok" | "needs-auth" | "unsupported" | "error";

export interface ServerProviderUsageSnapshot {
  provider: ProviderKind;
  updatedAt: IsoDateTime;
  limits: readonly ServerProviderUsageLimit[];
  usageLines: readonly ServerProviderUsageLine[];
  source: TrimmedNonEmptyString;
  status?: ProviderUsageStatus;
  planName?: TrimmedNonEmptyString;
  detail?: TrimmedNonEmptyString;
}

// ─── Local server process ─────────────────────────────────────────────

export interface ServerLocalServerAddress {
  host: TrimmedNonEmptyString;
  port: PositiveInt;
  family: "tcp4" | "tcp6" | "tcp";
  url: TrimmedNonEmptyString | null;
}

export interface ServerLocalServerProcess {
  id: TrimmedNonEmptyString;
  pid: PositiveInt;
  ppid?: PositiveInt;
  command: TrimmedNonEmptyString;
  displayName: TrimmedNonEmptyString;
  pageTitle?: TrimmedNonEmptyString;
  cwd?: TrimmedNonEmptyString;
  args: string;
  ports: readonly PositiveInt[];
  addresses: readonly ServerLocalServerAddress[];
  isStoppable: boolean;
  stopDisabledReason?: string;
}

// ─── Server config ────────────────────────────────────────────────────

export interface ServerConfigIssue {
  kind: "keybindings.malformed-config" | "keybindings.invalid-entry";
  message: TrimmedNonEmptyString;
  index?: number;
}

export interface ServerConfig {
  cwd: TrimmedNonEmptyString;
  homeDir?: TrimmedNonEmptyString;
  chatWorkspaceRoot?: TrimmedNonEmptyString;
  worktreesDir: TrimmedNonEmptyString;
  keybindingsConfigPath: TrimmedNonEmptyString;
  keybindings: ResolvedKeybindingsConfig;
  issues: readonly ServerConfigIssue[];
  providers: readonly ServerProviderStatus[];
  availableEditors: readonly EditorId[];
}

// ─── Server settings ──────────────────────────────────────────────────

export interface ProviderSettingsBase {
  enabled: boolean;
  binaryPath: string;
  customModels: readonly string[];
}

export interface CodexServerProviderSettings extends ProviderSettingsBase {
  homePath: string;
}
export interface ClaudeServerProviderSettings extends ProviderSettingsBase {
  launchArgs: string;
}
export interface GeminiServerProviderSettings extends ProviderSettingsBase {}
export interface GrokServerProviderSettings extends ProviderSettingsBase {}
export interface CursorServerProviderSettings extends ProviderSettingsBase {
  apiEndpoint: string;
}
export interface OpenCodeServerProviderSettings extends ProviderSettingsBase {
  serverUrl: string;
  serverPassword: string;
  experimentalWebSockets: boolean;
}
export interface KiloServerProviderSettings extends ProviderSettingsBase {
  serverUrl: string;
  serverPassword: string;
}
export interface PiServerProviderSettings extends ProviderSettingsBase {
  agentDir: string;
}

export interface SkillsServerSettings {
  disabled: readonly string[];
}

export interface McpServerSettings {
  disabled: readonly string[];
}

export interface ServerSettings {
  enableAssistantStreaming: boolean;
  defaultThreadEnvMode: ThreadEnvironmentMode;
  addProjectBaseDirectory: string;
  textGenerationModelSelection: ModelSelection;
  providers: {
    codex: CodexServerProviderSettings;
    claudeAgent: ClaudeServerProviderSettings;
    cursor: CursorServerProviderSettings;
    gemini: GeminiServerProviderSettings;
    grok: GrokServerProviderSettings;
    kilo: KiloServerProviderSettings;
    opencode: OpenCodeServerProviderSettings;
    pi: PiServerProviderSettings;
  };
  skills: SkillsServerSettings;
  mcp: McpServerSettings;
}

/** Patch for partial settings update. */
export interface ServerSettingsPatch {
  enableAssistantStreaming?: boolean;
  defaultThreadEnvMode?: ThreadEnvironmentMode;
  addProjectBaseDirectory?: string;
  textGenerationModelSelection?: {
    provider?: ProviderKind;
    model?: string;
    options?: unknown;
  };
  providers?: {
    codex?: Partial<CodexServerProviderSettings>;
    claudeAgent?: Partial<ClaudeServerProviderSettings>;
    cursor?: Partial<CursorServerProviderSettings>;
    gemini?: Partial<GeminiServerProviderSettings>;
    grok?: Partial<GrokServerProviderSettings>;
    kilo?: Partial<KiloServerProviderSettings>;
    opencode?: Partial<OpenCodeServerProviderSettings>;
    pi?: Partial<PiServerProviderSettings>;
  };
  skills?: { disabled?: readonly string[] };
  mcp?: { disabled?: readonly string[] };
}

// MCode derives DEFAULT_SERVER_SETTINGS via Schema.decodeSync at runtime. The
// vendored UI only references it as the default for state initialization, so
// a structural literal with the same field set is sufficient.
export const DEFAULT_SERVER_SETTINGS: ServerSettings = {
  enableAssistantStreaming: false,
  defaultThreadEnvMode: "local",
  addProjectBaseDirectory: "",
  textGenerationModelSelection: {
    provider: "codex",
    model: "gpt-5.4-mini",
  },
  providers: {
    codex: { enabled: true, binaryPath: "codex", customModels: [], homePath: "" },
    claudeAgent: { enabled: true, binaryPath: "claude", customModels: [], launchArgs: "" },
    cursor: { enabled: true, binaryPath: "cursor-agent", customModels: [], apiEndpoint: "" },
    gemini: { enabled: true, binaryPath: "gemini", customModels: [] },
    grok: { enabled: true, binaryPath: "grok", customModels: [] },
    kilo: { enabled: true, binaryPath: "kilo", customModels: [], serverUrl: "", serverPassword: "" },
    opencode: {
      enabled: true, binaryPath: "opencode", customModels: [],
      serverUrl: "", serverPassword: "", experimentalWebSockets: false,
    },
    pi: { enabled: true, binaryPath: "pi", customModels: [], agentDir: "" },
  },
  skills: { disabled: [] },
  mcp: { disabled: [] },
};

// ─── Push payloads + lifecycle ────────────────────────────────────────

export interface ServerConfigUpdatedPayload {
  issues: readonly ServerConfigIssue[];
  providers: readonly ServerProviderStatus[];
}

export interface ServerProviderStatusesUpdatedPayload {
  providers: readonly ServerProviderStatus[];
}

export interface ServerSettingsUpdatedPayload {
  settings: ServerSettings;
}

export interface ServerLifecycleWelcomePayload {
  cwd: TrimmedNonEmptyString;
  homeDir?: TrimmedNonEmptyString;
  chatWorkspaceRoot?: TrimmedNonEmptyString;
  projectName: TrimmedNonEmptyString;
  bootstrapProjectId?: ProjectId;
  bootstrapThreadId?: ThreadId;
}

export type ServerLifecycleStreamEvent =
  | { readonly type: "welcome"; readonly payload: ServerLifecycleWelcomePayload }
  | { readonly type: "ready"; readonly payload: { readonly at: IsoDateTime } }
  | {
      readonly type: "maintenance";
      readonly payload: {
        readonly task: "thread-retention";
        readonly state: "started" | "progress" | "completed" | "failed";
        readonly at: IsoDateTime;
        readonly deletedCount?: number;
        readonly totalCount?: number;
        readonly error?: string;
      };
    };

export type ServerConfigStreamEvent =
  | { readonly type: "snapshot"; readonly config: ServerConfig }
  | { readonly type: "configUpdated"; readonly payload: ServerConfigUpdatedPayload }
  | {
      readonly type: "providerStatuses";
      readonly payload: ServerProviderStatusesUpdatedPayload;
    }
  | { readonly type: "settingsUpdated"; readonly payload: ServerSettingsUpdatedPayload };

// ─── Automation-intent generation ─────────────────────────────────────

export type ServerAutomationIntentMissingField =
  | "schedule"
  | "taskPrompt"
  | "name"
  | "mode";

export interface ServerGenerateAutomationIntentInput {
  cwd: TrimmedNonEmptyString;
  message: TrimmedNonEmptyString;
  defaultMode?: AutomationMode;
  nowIso: IsoDateTime;
  codexHomePath?: TrimmedNonEmptyString;
  providerOptions?: ProviderStartOptions;
  textGenerationModel?: TrimmedNonEmptyString;
  textGenerationModelSelection?: ModelSelection;
}

export interface ServerGenerateAutomationIntentResult {
  isAutomation: boolean;
  confidence: number;
  language: TrimmedNonEmptyString | null;
  name: TrimmedNonEmptyString | null;
  taskPrompt: TrimmedNonEmptyString | null;
  schedule: AutomationSchedule | null;
  mode: AutomationMode | null;
  maxIterations?: PositiveInt | null;
  completionPolicy?: AutomationCompletionPolicy;
  missingFields: readonly ServerAutomationIntentMissingField[];
  needsConfirmation: boolean;
  reason: string | null;
}

// ─── Provider-usage snapshot ──────────────────────────────────────────

export interface ServerGetProviderUsageSnapshotInput {
  provider: ProviderKind;
  homePath?: TrimmedNonEmptyString;
}
export type ServerGetProviderUsageSnapshotResult = ServerProviderUsageSnapshot | null;

export interface ServerListProviderUsageInput {
  forceRefresh?: boolean;
}
export type ServerListProviderUsageResult = readonly ServerProviderUsageSnapshot[];

// ─── Stop-local-server ────────────────────────────────────────────────

export interface ServerStopLocalServerInput {
  pid: PositiveInt;
  port: PositiveInt;
}

// Re-export commonly-needed transit types referenced by callers.
export type { ProviderKind } from "./orchestration";

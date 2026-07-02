/**
 * Tier 3 — Orchestration + provider-kind / model-selection domain.
 *
 * Hand-ported from MCode `packages/contracts/src/orchestration.ts` (Effect
 * Schema → plain TS types). Covers the served-transport + core-UI hot path:
 * provider kind, runtime/interaction/approval modes, model-selection unions
 * (per-provider variants), thread-env mode, message source, delivery /
 * dispatch modes, approval decision, user-input answers, session status,
 * thread activity, latest turn, thread pull-request, proposed plan, marker,
 * pinned message, handoff.
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/orchestration.ts
 */

import type {
  ThreadId,
  TurnId,
  MessageId,
  EventId,
  ProjectId,
  CommandId,
  OrchestrationProposedPlanId,
  ApprovalRequestId,
  CheckpointRef,
} from "../ids";
import type {
  TrimmedNonEmptyString,
  NonNegativeInt,
  PositiveInt,
  IsoDateTime,
  ThreadMarkerId,
} from "./base";

// ─── Provider kind + interaction / runtime modes ──────────────────────

export type ProviderKind =
  | "codex"
  | "claudeAgent"
  | "cursor"
  | "gemini"
  | "grok"
  | "kilo"
  | "opencode"
  | "pi";

/** Provider with a default model (all except `pi`). */
export type ProviderWithDefaultModel = Exclude<ProviderKind, "pi">;

export type RuntimeMode = "approval-required" | "full-access";
export const DEFAULT_RUNTIME_MODE: RuntimeMode = "full-access";

export type ProviderInteractionMode = "default" | "plan";

export type ProviderApprovalPolicy =
  | "untrusted"
  | "on-failure"
  | "on-request"
  | "never";

export type ProviderSandboxMode =
  | "read-only"
  | "workspace-write"
  | "danger-full-access";

export type ProviderRequestKind = "command" | "file-read" | "file-change";

export type ProviderApprovalDecision =
  | "accept"
  | "acceptForSession"
  | "decline"
  | "cancel";

export type AssistantDeliveryMode = "buffered" | "streaming";
export type TurnDispatchMode = "queue" | "steer";
export const DEFAULT_TURN_DISPATCH_MODE: TurnDispatchMode = "queue";

export type ThreadEnvironmentMode = "local" | "worktree";

export type OrchestrationMessageSource =
  | "native"
  | "handoff-import"
  | "fork-import";

export type OrchestrationSessionStatus =
  | "idle"
  | "starting"
  | "running"
  | "ready"
  | "interrupted"
  | "stopped"
  | "error";

// ─── Per-provider ModelOptions + ModelSelection union ─────────────────

export interface CodexModelOptions {
  reasoningEffort?: TrimmedNonEmptyString;
  fastMode?: boolean;
}
export interface ClaudeModelOptions {
  thinking?: boolean;
  effort?: ClaudeCodeEffort;
  fastMode?: boolean;
  contextWindow?: string;
}
export interface CursorModelOptions {
  reasoningEffort?: TrimmedNonEmptyString;
  fastMode?: boolean;
  thinking?: boolean;
  contextWindow?: string;
}
export interface GeminiModelOptions {
  thinkingLevel?: GeminiThinkingLevel;
  thinkingBudget?: GeminiThinkingBudget;
}
export interface GrokModelOptions {
  reasoningEffort?: GrokReasoningEffort;
}
export interface OpenCodeModelOptions {
  variant?: TrimmedNonEmptyString;
  agent?: TrimmedNonEmptyString;
}
// Kilo reuses OpenCodeModelOptions.
export type KiloModelOptions = OpenCodeModelOptions;
export interface PiModelOptions {
  thinkingLevel?: PiThinkingLevel;
}

export interface ProviderModelOptions {
  codex?: CodexModelOptions;
  claudeAgent?: ClaudeModelOptions;
  cursor?: CursorModelOptions;
  gemini?: GeminiModelOptions;
  grok?: GrokModelOptions;
  kilo?: OpenCodeModelOptions;
  opencode?: OpenCodeModelOptions;
  pi?: PiModelOptions;
}

export interface CodexModelSelection {
  provider: "codex";
  model: TrimmedNonEmptyString;
  options?: CodexModelOptions;
}
export interface ClaudeModelSelection {
  provider: "claudeAgent";
  model: TrimmedNonEmptyString;
  options?: ClaudeModelOptions;
}
export interface CursorModelSelection {
  provider: "cursor";
  model: TrimmedNonEmptyString;
  options?: CursorModelOptions;
}
export interface GeminiModelSelection {
  provider: "gemini";
  model: TrimmedNonEmptyString;
  options?: GeminiModelOptions;
}
export interface GrokModelSelection {
  provider: "grok";
  model: TrimmedNonEmptyString;
  options?: GrokModelOptions;
}
export interface OpenCodeModelSelection {
  provider: "opencode";
  model: TrimmedNonEmptyString;
  options?: OpenCodeModelOptions;
}
export interface KiloModelSelection {
  provider: "kilo";
  model: TrimmedNonEmptyString;
  options?: OpenCodeModelOptions;
}
export interface PiModelSelection {
  provider: "pi";
  model: TrimmedNonEmptyString;
  options?: PiModelOptions;
}

export type ModelSelection =
  | CodexModelSelection
  | ClaudeModelSelection
  | CursorModelSelection
  | GeminiModelSelection
  | GrokModelSelection
  | KiloModelSelection
  | OpenCodeModelSelection
  | PiModelSelection;

// ─── Reasoning / thinking effort enums (provider model option sets) ───

export type CodexReasoningEffort = "low" | "medium" | "high" | "xhigh";
export const CODEX_REASONING_EFFORT_OPTIONS: readonly CodexReasoningEffort[] = [
  "low",
  "medium",
  "high",
  "xhigh",
];

export type ClaudeCodeEffort =
  | "low"
  | "medium"
  | "high"
  | "xhigh"
  | "max"
  | "ultrathink"
  | "ultracode";
export const CLAUDE_CODE_EFFORT_OPTIONS: readonly ClaudeCodeEffort[] = [
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
  "ultrathink",
  "ultracode",
];

export type GeminiThinkingLevel = "LOW" | "HIGH";
export const GEMINI_THINKING_LEVEL_OPTIONS: readonly GeminiThinkingLevel[] = [
  "LOW",
  "HIGH",
];

export type GeminiThinkingBudget = -1 | 512 | 0;
export const GEMINI_THINKING_BUDGET_OPTIONS: readonly GeminiThinkingBudget[] = [
  -1,
  512,
  0,
];

export type PiThinkingLevel =
  | "off"
  | "minimal"
  | "low"
  | "medium"
  | "high"
  | "xhigh";
export const PI_THINKING_LEVEL_OPTIONS: readonly PiThinkingLevel[] = [
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
];

export type GrokReasoningEffort = "none" | "low" | "medium" | "high";
export const GROK_REASONING_EFFORT_OPTIONS: readonly GrokReasoningEffort[] = [
  "none",
  "low",
  "medium",
  "high",
];

// ─── Provider start options ───────────────────────────────────────────

export interface CodexProviderStartOptions {
  binaryPath?: TrimmedNonEmptyString;
  homePath?: TrimmedNonEmptyString;
}
export interface ClaudeProviderStartOptions {
  binaryPath?: TrimmedNonEmptyString;
  permissionMode?: TrimmedNonEmptyString;
  maxThinkingTokens?: NonNegativeInt;
}
export interface GeminiProviderStartOptions {
  binaryPath?: TrimmedNonEmptyString;
}
export interface CursorProviderStartOptions {
  binaryPath?: TrimmedNonEmptyString;
  apiEndpoint?: TrimmedNonEmptyString;
}
export interface GrokProviderStartOptions {
  binaryPath?: TrimmedNonEmptyString;
}
export interface OpenCodeProviderStartOptions {
  binaryPath?: TrimmedNonEmptyString;
  serverUrl?: TrimmedNonEmptyString;
  serverPassword?: TrimmedNonEmptyString;
  experimentalWebSockets?: boolean;
}
export interface KiloProviderStartOptions {
  binaryPath?: TrimmedNonEmptyString;
  serverUrl?: TrimmedNonEmptyString;
  serverPassword?: TrimmedNonEmptyString;
}
export interface PiProviderStartOptions {
  binaryPath?: TrimmedNonEmptyString;
  agentDir?: TrimmedNonEmptyString;
}

export interface ProviderStartOptions {
  codex?: CodexProviderStartOptions;
  claudeAgent?: ClaudeProviderStartOptions;
  cursor?: CursorProviderStartOptions;
  gemini?: GeminiProviderStartOptions;
  grok?: GrokProviderStartOptions;
  kilo?: KiloProviderStartOptions;
  opencode?: OpenCodeProviderStartOptions;
  pi?: PiProviderStartOptions;
}

// ─── Provider user-input answers ──────────────────────────────────────

export type ProviderUserInputAnswer =
  | string
  | readonly string[]
  | null;
export type ProviderUserInputAnswers = Record<string, ProviderUserInputAnswer>;

// ─── Provider mention / skill references ──────────────────────────────

export interface ProviderMentionReference {
  name: TrimmedNonEmptyString;
  path: TrimmedNonEmptyString;
}
export interface ProviderSkillReference {
  name: TrimmedNonEmptyString;
  path: TrimmedNonEmptyString;
}

// ─── Constants (limits) ───────────────────────────────────────────────

export const PROVIDER_SEND_TURN_MAX_INPUT_CHARS = 120_000;
export const PROVIDER_SEND_TURN_MAX_ATTACHMENTS = 8;
export const PROVIDER_SEND_TURN_MAX_IMAGE_BYTES = 10 * 1024 * 1024;
export const PROVIDER_SEND_TURN_MAX_FILE_BYTES = 25 * 1024 * 1024;
export const MAX_PINNED_PROJECTS = 3;
export const CHAT_ASSISTANT_SELECTION_TEXT_MAX_CHARS = 4_000;
export const THREAD_NOTES_MAX_CHARS = 16_384;
export const PINNED_MESSAGES_MAX_COUNT = 100;
export const PINNED_MESSAGE_LABEL_MAX_CHARS = 60;
export const THREAD_MARKER_LABEL_MAX_CHARS = 60;

// ─── WS method / channel maps for orchestration ───────────────────────

export const ORCHESTRATION_WS_METHODS = {
  getSnapshot: "orchestration.getSnapshot",
  getShellSnapshot: "orchestration.getShellSnapshot",
  dispatchCommand: "orchestration.dispatchCommand",
  importThread: "orchestration.importThread",
  repairState: "orchestration.repairState",
  getTurnDiff: "orchestration.getTurnDiff",
  getFullThreadDiff: "orchestration.getFullThreadDiff",
  replayEvents: "orchestration.replayEvents",
  subscribeShell: "orchestration.subscribeShell",
  unsubscribeShell: "orchestration.unsubscribeShell",
  subscribeThread: "orchestration.subscribeThread",
  unsubscribeThread: "orchestration.unsubscribeThread",
} as const;

export const ORCHESTRATION_WS_CHANNELS = {
  domainEvent: "orchestration.domainEvent",
  shellEvent: "orchestration.shellEvent",
  threadEvent: "orchestration.threadEvent",
} as const;

// ─── Misc TS-domain types ─────────────────────────────────────────────

/** Effort menu option (model.ts). */
export interface EffortOption {
  readonly value: string;
  readonly label: string;
  readonly description?: string;
  readonly isDefault?: true;
}
export interface ContextWindowOption {
  readonly value: string;
  readonly label: string;
  readonly isDefault?: true;
}
export interface ModelCapabilities {
  readonly optionDescriptors?: readonly unknown[];
  readonly reasoningEffortLevels: readonly EffortOption[];
  readonly supportsFastMode: boolean;
  readonly supportsThinkingToggle: boolean;
  readonly promptInjectedEffortLevels: readonly string[];
  readonly contextWindowOptions: readonly ContextWindowOption[];
  readonly variantOptions?: readonly EffortOption[];
  readonly agentOptions?: readonly EffortOption[];
}

/** Model slug — built-in slug or arbitrary string. */
export type ModelSlug = string;

// ─── Pinned message ───────────────────────────────────────────────────

export type PinnedMessageLabel = TrimmedNonEmptyString;

export interface PinnedMessage {
  messageId: MessageId;
  label?: PinnedMessageLabel | null;
  done?: boolean;
  pinnedAt: IsoDateTime;
}

// ─── Thread marker ────────────────────────────────────────────────────

export type ThreadMarkerStyle = "highlight" | "underline";
export type ThreadMarkerColor = "yellow" | "blue" | "green" | "pink";

export interface ThreadMarker {
  id: ThreadMarkerId;
  messageId: MessageId;
  startOffset: NonNegativeInt;
  endOffset: NonNegativeInt;
  selectedText: TrimmedNonEmptyString;
  style: ThreadMarkerStyle;
  color: ThreadMarkerColor;
  label?: TrimmedNonEmptyString | null;
  done?: boolean;
  createdAt: IsoDateTime;
  updatedAt: IsoDateTime;
}

// ─── Thread handoff ───────────────────────────────────────────────────

export type ThreadHandoffBootstrapStatus = "pending" | "completed";

export interface ThreadHandoff {
  sourceThreadId: ThreadId;
  sourceProvider: ProviderKind;
  importedAt: IsoDateTime;
  bootstrapStatus: ThreadHandoffBootstrapStatus;
}

export interface ThreadHandoffImportedMessage {
  messageId: MessageId;
  role: "user" | "assistant";
  text: string;
  attachments?: readonly ChatAttachment[];
  createdAt: IsoDateTime;
  updatedAt: IsoDateTime;
}

// ─── Orchestration turn-diff checkpoint summary ───────────────────────
// Ported from MCode `packages/contracts/src/orchestration.ts`. The store
// reducer (`normalizeTurnDiffSummaries`) reads turnId/checkpointTurnCount/
// checkpointRef/status/files/assistantMessageId/completedAt off each entry;
// the prior `readonly unknown[]` typing collapsed every field to `unknown`.

export interface OrchestrationCheckpointFile {
  path: TrimmedNonEmptyString;
  kind: TrimmedNonEmptyString;
  additions: NonNegativeInt;
  deletions: NonNegativeInt;
}

export type OrchestrationCheckpointStatus = "ready" | "missing" | "error";

export interface OrchestrationCheckpointSummary {
  turnId: TurnId;
  checkpointTurnCount: NonNegativeInt;
  checkpointRef: CheckpointRef;
  status: OrchestrationCheckpointStatus;
  files: readonly OrchestrationCheckpointFile[];
  assistantMessageId: MessageId | null;
  completedAt: IsoDateTime;
}

// ─── Orchestration message + activity + session ───────────────────────

export type OrchestrationMessageRole = "user" | "assistant" | "system";

// Chat attachments (ported from MCode `packages/contracts/src/orchestration.ts`).
// The vendored UI's `normalizeChatAttachments` reducer reads `.type`/`.id`/
// `.name`/`.mimeType`/`.sizeBytes`/`.assistantMessageId`/`.text` off each
// attachment, so the prior `readonly unknown[]` typing collapsed every field
// access to `unknown` (TS18046). The shared `@t3tools/shared` `ChatAttachment`
// is structurally identical to this union.
export interface ChatImageAttachment {
  type: "image";
  id: string;
  name: string;
  mimeType: string;
  sizeBytes: NonNegativeInt;
}
export interface ChatFileAttachment {
  type: "file";
  id: string;
  name: string;
  mimeType: string;
  sizeBytes: NonNegativeInt;
}
export interface ChatAssistantSelectionAttachment {
  type: "assistant-selection";
  id: string;
  assistantMessageId: MessageId;
  text: string;
}
export type ChatAttachment =
  | ChatImageAttachment
  | ChatFileAttachment
  | ChatAssistantSelectionAttachment;

export interface OrchestrationMessage {
  id: MessageId;
  role: OrchestrationMessageRole;
  text: string;
  attachments?: readonly ChatAttachment[];
  skills?: readonly ProviderSkillReference[];
  mentions?: readonly ProviderMentionReference[];
  dispatchMode?: TurnDispatchMode;
  turnId: TurnId | null;
  streaming: boolean;
  source?: OrchestrationMessageSource;
  createdAt: IsoDateTime;
  updatedAt: IsoDateTime;
}

export type OrchestrationThreadActivityTone =
  | "info"
  | "tool"
  | "approval"
  | "error";

export interface OrchestrationThreadActivity {
  id: EventId;
  tone: OrchestrationThreadActivityTone;
  kind: TrimmedNonEmptyString;
  summary: TrimmedNonEmptyString;
  payload: unknown;
  turnId: TurnId | null;
  sequence?: NonNegativeInt;
  createdAt: IsoDateTime;
}

export interface OrchestrationSession {
  threadId: ThreadId;
  status: OrchestrationSessionStatus;
  providerName: TrimmedNonEmptyString | null;
  runtimeMode: RuntimeMode;
  activeTurnId: TurnId | null;
  lastError: TrimmedNonEmptyString | null;
  updatedAt: IsoDateTime;
}

// ─── Proposed plan ────────────────────────────────────────────────────

// Re-exported from ../ids (canonical branded-ID home) as a type+value pair
// exposing `.makeUnsafe` (12 vendored UI call sites). Was previously a
// plain `TrimmedNonEmptyString` alias here, which surfaced TS2693 at the
// `.makeUnsafe` call sites.
export { OrchestrationProposedPlanId } from "../ids";

export interface OrchestrationProposedPlan {
  id: OrchestrationProposedPlanId;
  turnId: TurnId | null;
  planMarkdown: TrimmedNonEmptyString;
  implementedAt: IsoDateTime | null;
  implementationThreadId: ThreadId | null;
  createdAt: IsoDateTime;
  updatedAt: IsoDateTime;
}

// ─── Latest turn + thread PR ──────────────────────────────────────────

export type OrchestrationLatestTurnState =
  | "running"
  | "interrupted"
  | "completed"
  | "error";

export interface OrchestrationLatestTurn {
  turnId: TurnId;
  state: OrchestrationLatestTurnState;
  requestedAt: IsoDateTime;
  startedAt: IsoDateTime | null;
  completedAt: IsoDateTime | null;
  assistantMessageId: MessageId | null;
  sourceProposedPlan?: {
    threadId: ThreadId;
    planId: OrchestrationProposedPlanId;
  };
}

export interface OrchestrationThreadPullRequest {
  number: PositiveInt;
  title: TrimmedNonEmptyString;
  url: string;
  baseBranch: TrimmedNonEmptyString;
  headBranch: TrimmedNonEmptyString;
  state: "open" | "closed" | "merged";
}

// ─── Token-usage snapshot for thread (used in profile / thread detail) ─
// Real shape ported from MCode `packages/contracts/src/providerRuntime.ts`
// lines 311-331. The vendored UI's `ContextWindowSnapshot` (in
// `lib/contextWindow.ts`) is a mapped type over this, so the field set
// here must mirror MCode exactly or `maxTokens`/`usedTokens`/… property
// accesses surface as TS2339.
export interface ThreadTokenUsageSnapshot {
  readonly usedTokens: number;
  readonly usedPercent?: number;
  readonly totalProcessedTokens?: number;
  readonly maxTokens?: number;
  readonly inputTokens?: number;
  readonly cachedInputTokens?: number;
  readonly outputTokens?: number;
  readonly reasoningOutputTokens?: number;
  readonly lastUsedTokens?: number;
  readonly lastInputTokens?: number;
  readonly lastCachedInputTokens?: number;
  readonly lastOutputTokens?: number;
  readonly lastReasoningOutputTokens?: number;
  readonly toolUses?: number;
  readonly durationMs?: number;
  readonly compactsAutomatically?: boolean;
}

// ─── ClientOrchestrationCommand (28-variant discriminated union) ──────
// Real per-variant modelling: literal `type` discriminator per variant +
// the common identifying fields the vendored UI narrows on (`commandId`,
// `threadId`, `projectId`, `createdAt`) + a permissive index signature so
// the deferred nested payload types (ModelSelection, RuntimeMode,
// ProviderStartOptions, ThreadHandoff, …) don't have to be ported in full
// here. Source of truth: MCode `packages/contracts/src/orchestration.ts`
// lines 767-1237. The union members mirror the 28 variants MCode's
// `ClientOrchestrationCommand` Schema.Union contains; this makes
// `Extract<ClientOrchestrationCommand, { type: "thread.create" }>` resolve
// to a real shape (was collapsing to `never` under the old `{type:string}`
// stub, breaking 24 call sites in threadCreatePromotion.ts).
export interface ClientOrchestrationCommandBase {
  readonly commandId: CommandId;
  readonly createdAt?: IsoDateTime;
  readonly [key: string]: unknown;
}
export type ClientOrchestrationCommand =
  | (ClientOrchestrationCommandBase & {
      readonly type: "project.create";
      readonly projectId?: ProjectId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "project.meta.update";
      readonly projectId: ProjectId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "project.delete";
      readonly projectId: ProjectId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.create";
      readonly threadId: ThreadId;
      readonly projectId: ProjectId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.handoff.create";
      readonly threadId: ThreadId;
      readonly projectId: ProjectId;
      readonly sourceThreadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.fork.create";
      readonly threadId: ThreadId;
      readonly projectId: ProjectId;
      readonly sourceThreadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.delete";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.archive";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.unarchive";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.meta.update";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.pinned-message.add";
      readonly threadId: ThreadId;
      readonly messageId: MessageId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.pinned-message.remove";
      readonly threadId: ThreadId;
      readonly messageId: MessageId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.pinned-message.done.set";
      readonly threadId: ThreadId;
      readonly messageId: MessageId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.pinned-message.label.set";
      readonly threadId: ThreadId;
      readonly messageId: MessageId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.marker.add";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.marker.remove";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.marker.done.set";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.marker.label.set";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.runtime-mode.set";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.interaction-mode.set";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.turn.start";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.turn.interrupt";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.approval.respond";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.user-input.respond";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.checkpoint.revert";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.message.edit-and-resend";
      readonly threadId: ThreadId;
      readonly messageId: MessageId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.activity.append";
      readonly threadId: ThreadId;
    })
  | (ClientOrchestrationCommandBase & {
      readonly type: "thread.session.stop";
      readonly threadId: ThreadId;
    });

// ─── Orchestration thread (full read-model thread) ────────────────────
// MCode's `OrchestrationThread` is the full per-thread projection the chat
// UI renders. It's a large struct; the field set below mirrors MCode's
// `OrchestrationThread` schema (orchestration.ts). A small number of fields
// use `unknown` where the nested shape is itself a deferred Tier 3 port
// (chat attachments, activity payloads). T5c can tighten those.

import type { ProjectKind, ProjectScript } from "./project";

export interface OrchestrationThread {
  id: ThreadId;
  projectId: ProjectId;
  title: TrimmedNonEmptyString;
  modelSelection: ModelSelection;
  runtimeMode: RuntimeMode;
  interactionMode: ProviderInteractionMode;
  envMode?: ThreadEnvironmentMode;
  branch: TrimmedNonEmptyString | null;
  worktreePath: TrimmedNonEmptyString | null;
  associatedWorktreePath?: TrimmedNonEmptyString | null;
  associatedWorktreeBranch?: TrimmedNonEmptyString | null;
  associatedWorktreeRef?: TrimmedNonEmptyString | null;
  createBranchFlowCompleted?: boolean;
  isPinned?: boolean;
  parentThreadId?: ThreadId | null;
  subagentAgentId?: TrimmedNonEmptyString | null;
  subagentNickname?: TrimmedNonEmptyString | null;
  subagentRole?: TrimmedNonEmptyString | null;
  forkSourceThreadId?: ThreadId | null;
  sidechatSourceThreadId?: ThreadId | null;
  lastKnownPr?: OrchestrationThreadPullRequest | null;
  latestTurn: OrchestrationLatestTurn | null;
  latestUserMessageAt?: IsoDateTime | null;
  hasPendingApprovals?: boolean;
  hasPendingUserInput?: boolean;
  hasActionableProposedPlan?: boolean;
  createdAt: IsoDateTime;
  updatedAt: IsoDateTime;
  archivedAt?: IsoDateTime | null;
  deletedAt: IsoDateTime | null;
  handoff?: ThreadHandoff | null;
  pinnedMessages?: readonly PinnedMessage[];
  threadMarkers?: readonly ThreadMarker[];
  notes?: string;
  messages: readonly OrchestrationMessage[];
  proposedPlans?: readonly OrchestrationProposedPlan[];
  activities: readonly OrchestrationThreadActivity[];
  checkpoints?: readonly OrchestrationCheckpointSummary[];
  session: OrchestrationSession | null;
}

// ─── Orchestration snapshots (read model + shell) ─────────────────────
// Ported from MCode `packages/contracts/src/orchestration.ts`. The full
// `OrchestrationReadModel` uses `OrchestrationProject`/`OrchestrationThread`
// (the rich projections); we don't yet port those, but the shell projections
// are structurally compatible supersets of the fields the UI consumes, so
// `OrchestrationReadModel` reuses the shell arrays. This satisfies the
// `SnapshotWithProjects<T>` structural constraint in `projectCreateRecovery.ts`
// (which only needs `projects: readonly T[]` where T matches
// `DuplicateProjectCreateRecoveryCandidate`: id/kind?/workspaceRoot/deletedAt?).

export interface OrchestrationShellSnapshot {
  snapshotSequence: NonNegativeInt;
  projects: readonly OrchestrationProjectShell[];
  threads: readonly OrchestrationThreadShell[];
  updatedAt: IsoDateTime;
}

export interface OrchestrationReadModel {
  snapshotSequence: NonNegativeInt;
  projects: readonly OrchestrationProjectShell[];
  threads: readonly OrchestrationThread[];
  updatedAt: IsoDateTime;
}

// ─── Orchestration shell snapshot thread (lighter projection) ──────────

export interface OrchestrationProjectShell {
  id: ProjectId;
  kind?: ProjectKind;
  title: TrimmedNonEmptyString;
  workspaceRoot: TrimmedNonEmptyString;
  defaultModelSelection: ModelSelection | null;
  scripts?: readonly ProjectScript[];
  isPinned?: boolean;
  createdAt: IsoDateTime;
  updatedAt: IsoDateTime;
}

export interface OrchestrationThreadShell {
  id: ThreadId;
  projectId: ProjectId;
  title: TrimmedNonEmptyString;
  modelSelection: ModelSelection;
  runtimeMode: RuntimeMode;
  interactionMode: ProviderInteractionMode;
  envMode?: ThreadEnvironmentMode;
  branch: TrimmedNonEmptyString | null;
  worktreePath: TrimmedNonEmptyString | null;
  associatedWorktreePath?: TrimmedNonEmptyString | null;
  associatedWorktreeBranch?: TrimmedNonEmptyString | null;
  associatedWorktreeRef?: TrimmedNonEmptyString | null;
  createBranchFlowCompleted?: boolean;
  isPinned?: boolean;
  parentThreadId?: ThreadId | null;
  subagentAgentId?: TrimmedNonEmptyString | null;
  subagentNickname?: TrimmedNonEmptyString | null;
  subagentRole?: TrimmedNonEmptyString | null;
  forkSourceThreadId?: ThreadId | null;
  sidechatSourceThreadId?: ThreadId | null;
  lastKnownPr?: OrchestrationThreadPullRequest | null;
  latestTurn: OrchestrationLatestTurn | null;
  latestUserMessageAt?: IsoDateTime | null;
  hasPendingApprovals?: boolean;
  hasPendingUserInput?: boolean;
  hasActionableProposedPlan?: boolean;
  createdAt: IsoDateTime;
  updatedAt: IsoDateTime;
  archivedAt?: IsoDateTime | null;
  handoff?: ThreadHandoff | null;
  session: OrchestrationSession | null;
}

// ─── Orchestration shell stream event ─────────────────────────────────
// Discriminated union pushed over the orchestration shell channel.

export type OrchestrationShellStreamEvent =
  | {
      readonly kind: "project-upserted";
      readonly sequence: NonNegativeInt;
      readonly project: OrchestrationProjectShell;
    }
  | {
      readonly kind: "project-removed";
      readonly sequence: NonNegativeInt;
      readonly projectId: ProjectId;
    }
  | {
      readonly kind: "thread-upserted";
      readonly sequence: NonNegativeInt;
      readonly thread: OrchestrationThreadShell;
    }
  | {
      readonly kind: "thread-removed";
      readonly sequence: NonNegativeInt;
      readonly threadId: ThreadId;
    };

// ─── Orchestration stream items (snapshot | event envelopes) ──────────
// Ported from MCode `packages/contracts/src/orchestration.ts`. The shell and
// thread channels multiplex an initial snapshot item with subsequent
// delta/event items, discriminated by `kind`. The vendored UI
// (`routes/__root.tsx` onShellEvent handler, `wsNativeApi.ts`) reads
// `item.snapshot` / `item.thread` / `item.sequence` / `item.event` off
// these; the opaque transport stubs collapsed those to `unknown`.

export interface OrchestrationThreadDetailSnapshot {
  snapshotSequence: NonNegativeInt;
  thread: OrchestrationThread;
}

export type OrchestrationShellStreamItem =
  | { readonly kind: "snapshot"; readonly snapshot: OrchestrationShellSnapshot }
  | OrchestrationShellStreamEvent;

export type OrchestrationThreadStreamItem =
  | { readonly kind: "snapshot"; readonly snapshot: OrchestrationThreadDetailSnapshot }
  | { readonly kind: "event"; readonly event: OrchestrationEvent };

// ─── OrchestrationEvent (34-variant discriminated union) ──────────────
// Real per-variant modelling: literal `type` discriminator per variant +
// the shared `EventBaseFields` (sequence, eventId, aggregateKind,
// aggregateId, occurredAt, commandId, …) + a permissive `payload` so the
// vendored UI's `Extract<OrchestrationEvent, { type: "thread.message-sent" }>`
// narrows to a real shape (was collapsing to `never` under the old
// `OpaqueTransportResult` stub in shell.ts, breaking 16 call sites in
// store.ts that access `event.payload.role` / `.activity.kind` /
// `.requestId` + `event.sequence`).
//
// Source of truth: MCode `packages/contracts/src/orchestration.ts`
// `OrchestrationEvent` Schema.Union (lines 1712-1884). Payload field sets
// are modeled permissively (`Record<string, unknown>`) because porting all
// 34 payload structs is out of T5c scope; the discriminator + base fields
// are what the vendored UI narrows on. T5d can tighten `payload` per arm.
export interface OrchestrationEventBase {
  readonly sequence: NonNegativeInt;
  readonly eventId: EventId;
  readonly aggregateKind: "project" | "thread";
  readonly aggregateId: ProjectId | ThreadId;
  readonly occurredAt: IsoDateTime;
  readonly commandId: CommandId | null;
  readonly causationEventId: EventId | null;
  readonly correlationId: CommandId | null;
  readonly metadata: Record<string, unknown>;
}
// Permissive payload with the common identifying/temporal fields the
// vendored UI accesses most often (threadId, projectId, messageId,
// createdAt, updatedAt, title, requestId, isPinned, …) typed explicitly so
// `event.payload.threadId` resolves to `ThreadId | undefined` instead of
// `unknown`. The index signature retains permissiveness for fields not yet
// ported from MCode's 34 payload structs. T5d can tighten per-variant.
export interface OrchestrationEventPayload {
  readonly threadId?: ThreadId;
  readonly projectId?: ProjectId;
  readonly messageId?: MessageId;
  readonly turnId?: TurnId;
  readonly createdAt?: IsoDateTime;
  readonly updatedAt?: IsoDateTime;
  readonly title?: string;
  readonly requestId?: ApprovalRequestId;
  readonly isPinned?: boolean;
  readonly role?: OrchestrationMessageRole;
  readonly text?: string;
  readonly streaming?: boolean;
  readonly dispatchMode?: TurnDispatchMode;
  readonly source?: OrchestrationMessageSource;
  readonly attachments?: readonly ChatAttachment[];
  readonly skills?: readonly ProviderSkillReference[];
  readonly mentions?: readonly ProviderMentionReference[];
  readonly activity?: OrchestrationThreadActivity;
  readonly proposedPlan?: OrchestrationProposedPlan;
  readonly session?: OrchestrationSession;
  readonly threadMarkers?: readonly unknown[];
  readonly subagentAgentId?: string | null;
  readonly subagentNickname?: string | null;
  readonly subagentRole?: string | null;
  readonly parentThreadId?: ThreadId | null;
  readonly modelSelection?: ModelSelection;
  readonly runtimeMode?: RuntimeMode;
  readonly interactionMode?: ProviderInteractionMode;
  readonly envMode?: string;
  readonly branch?: string | null;
  readonly worktreePath?: string | null;
  readonly [key: string]: unknown;
}
// ─── Per-variant event payloads (T5d) ────────────────────────────────
// Ported from MCode `packages/contracts/src/orchestration.ts`. Each variant
// re-declares the fields MCode's schema marks REQUIRED for that event
// (threadId / messageId / projectId / turnId / markerId / requestId / …) on
// top of the permissive {@link OrchestrationEventPayload} base. The base's
// index signature is retained so unported optional fields still resolve to
// `unknown` rather than failing compile — but the required IDs the
// reducer/UI threads through branded constructors (ThreadId.makeUnsafe etc.)
// are now precise, eliminating the `ThreadId | undefined`/`unknown` drift at
// the call sites.

/** Payload with a required `threadId`. */
interface ThreadIdPayload extends OrchestrationEventPayload {
  readonly threadId: ThreadId;
}
/** Payload with required `threadId` + `messageId`. */
interface ThreadMessagePayload extends ThreadIdPayload {
  readonly messageId: MessageId;
}
/** Payload for `thread.message-sent`: carries the full message envelope
 *  (MCode `ThreadMessageSentPayload` marks role/text/streaming/turnId/
 *  createdAt as required). Built via `Omit`+intersection because
 *  `exactOptionalPropertyTypes` forbids widening optional base fields
 *  (`role?: T`) to required ones (`role: T`) via interface `extends`. */
type ThreadMessageSentPayload = Omit<
  OrchestrationEventPayload,
  "threadId" | "messageId" | "role" | "text" | "streaming" | "turnId" | "createdAt" | "updatedAt"
> & {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly role: OrchestrationMessageRole;
  readonly text: string;
  readonly streaming: boolean;
  readonly turnId: TurnId | null;
  readonly createdAt: IsoDateTime;
  readonly updatedAt: IsoDateTime;
};
/** Payload with required `threadId` + `markerId`. */
interface ThreadMarkerPayload extends ThreadIdPayload {
  readonly markerId: ThreadMarkerId;
}
/** Payload with required `threadId` + `requestId`. */
interface ThreadRequestPayload extends ThreadIdPayload {
  readonly requestId: ApprovalRequestId;
}
/** Payload with a required `projectId`. */
interface ProjectIdPayload extends OrchestrationEventPayload {
  readonly projectId: ProjectId;
}
/** Payload for `project.created` (MCode `ProjectCreatedPayload`): kind/title/
 *  workspaceRoot/defaultModelSelection/scripts/createdAt/updatedAt required.
 *  Built via Omit+intersection for exactOptionalPropertyTypes compatibility. */
type ProjectCreatedPayload = Omit<
  OrchestrationEventPayload,
  | "projectId"
  | "title"
  | "createdAt"
  | "updatedAt"
  | "kind"
  | "workspaceRoot"
  | "defaultModelSelection"
  | "scripts"
> & {
  readonly projectId: ProjectId;
  readonly kind?: ProjectKind;
  readonly title: string;
  readonly workspaceRoot: string;
  readonly defaultModelSelection: unknown;
  readonly scripts?: readonly ProjectScript[];
  readonly createdAt: IsoDateTime;
  readonly updatedAt: IsoDateTime;
};

export type OrchestrationEvent =
  | (OrchestrationEventBase & {
      readonly type: "project.created";
      readonly payload: ProjectCreatedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "project.meta-updated";
      readonly payload: ProjectIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "project.deleted";
      readonly payload: ProjectIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.created";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.deleted";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.archived";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.unarchived";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.meta-updated";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.pinned-message-added";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.pinned-message-removed";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.pinned-message-done-set";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.pinned-message-label-set";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.marker-added";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.marker-removed";
      readonly payload: ThreadMarkerPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.marker-done-set";
      readonly payload: ThreadMarkerPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.marker-label-set";
      readonly payload: ThreadMarkerPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.runtime-mode-set";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.interaction-mode-set";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.session-set";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.activity-appended";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.message-sent";
      readonly payload: ThreadMessageSentPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.turn-start-requested";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.turn-queued";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.turn-interrupt-requested";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.turn-diff-completed";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.approval-response-requested";
      readonly payload: ThreadRequestPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.user-input-response-requested";
      readonly payload: ThreadRequestPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.checkpoint-revert-requested";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.reverted";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.conversation-rollback-requested";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.conversation-rolled-back";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.message-edit-resend-requested";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.session-stop-requested";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.proposed-plan-upserted";
      readonly payload: ThreadIdPayload;
    });

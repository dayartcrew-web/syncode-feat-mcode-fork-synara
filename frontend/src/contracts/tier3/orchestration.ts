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
  // T5e: optionals widened with `| undefined` so the vendored store reducer's
  // conditional-spread object construction type-checks under
  // `exactOptionalPropertyTypes: true` (TS2375).
  attachments?: readonly ChatAttachment[] | undefined;
  skills?: readonly ProviderSkillReference[] | undefined;
  mentions?: readonly ProviderMentionReference[] | undefined;
  dispatchMode?: TurnDispatchMode | undefined;
  turnId: TurnId | null;
  streaming: boolean;
  source?: OrchestrationMessageSource | undefined;
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
  tone?: OrchestrationThreadActivityTone | undefined;
  kind: TrimmedNonEmptyString;
  summary?: TrimmedNonEmptyString | undefined;
  payload: unknown;
  turnId: TurnId | null;
  sequence?: NonNegativeInt | undefined;
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
  projects: readonly OrchestrationProject[];
  threads: readonly OrchestrationThread[];
  updatedAt: IsoDateTime;
}

// ─── Orchestration shell snapshot thread (lighter projection) ──────────

// T5e: full `OrchestrationProject` projection (read-model). MCode's
// `OrchestrationReadModel.projects` uses the full `OrchestrationProject`
// (which carries `deletedAt`) — not the lighter `OrchestrationProjectShell`
// used by `OrchestrationShellSnapshot`. The vendored store filters on
// `.deletedAt` (`syncServerReadModel`) and the `project.created` event
// reducer builds a project shell with `deletedAt: null`, so the read-model
// project type must expose `deletedAt`. Source of truth: MCode
// `packages/contracts/src/orchestration.ts` lines 370-381.
export interface OrchestrationProject {
  id: ProjectId;
  kind?: ProjectKind | undefined;
  title: TrimmedNonEmptyString;
  workspaceRoot: TrimmedNonEmptyString;
  defaultModelSelection: ModelSelection | null;
  scripts: readonly ProjectScript[];
  isPinned?: boolean | undefined;
  createdAt: IsoDateTime;
  updatedAt: IsoDateTime;
  deletedAt: IsoDateTime | null;
}

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
  readonly threadMarkers?: readonly ThreadMarker[];
  readonly subagentAgentId?: string | null;
  readonly subagentNickname?: string | null;
  readonly subagentRole?: string | null;
  readonly parentThreadId?: ThreadId | null;
  readonly modelSelection?: ModelSelection;
  readonly runtimeMode?: RuntimeMode;
  readonly interactionMode?: ProviderInteractionMode;
  readonly envMode?: ThreadEnvironmentMode;
  readonly branch?: string | null;
  readonly worktreePath?: string | null;
  // T5e: fields the vendored store reducer reads off `event.payload.X` that
  // previously fell through to the index signature's `unknown`. Typed here so
  // `event.payload.kind` / `.scripts` / `.marker` / … resolve to real shapes
  // instead of `unknown` (TS2322/2345/18046). The index signature is retained
  // for fields not yet ported from MCode's 34 payload structs.
  readonly associatedWorktreePath?: string | null;
  readonly associatedWorktreeBranch?: string | null;
  readonly associatedWorktreeRef?: string | null;
  readonly createBranchFlowCompleted?: boolean;
  readonly notes?: string | undefined;
  readonly pinnedMessages?: readonly PinnedMessage[];
  readonly kind?: ProjectKind;
  readonly workspaceRoot?: string;
  readonly defaultModelSelection?: ModelSelection | null;
  readonly scripts?: readonly ProjectScript[];
  readonly archivedAt?: IsoDateTime | null;
  // Pinned-message + marker event payloads.
  readonly pin?: PinnedMessage;
  readonly label?: PinnedMessageLabel | null;
  readonly done?: boolean;
  readonly marker?: ThreadMarker;
  readonly markerId?: ThreadMarkerId;
  readonly decision?: ProviderApprovalDecision;
  // Turn-diff + revert + rollback payloads.
  readonly completedAt?: IsoDateTime;
  readonly status?: OrchestrationCheckpointStatus;
  readonly files?: readonly OrchestrationCheckpointFile[];
  readonly checkpointRef?: CheckpointRef;
  readonly assistantMessageId?: MessageId | null;
  readonly checkpointTurnCount?: NonNegativeInt;
  readonly turnCount?: number;
  readonly numTurns?: number;
  readonly removedTurnIds?: readonly TurnId[];
  readonly sourceProposedPlan?: OrchestrationLatestTurn["sourceProposedPlan"];
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
 *  (`role?: T`) to required ones (`role: T`) via interface `extends`.
 *
 *  T5e: the optional message fields (dispatchMode/source/attachments/skills/
 *  mentions) are also omitted+re-declared because TypeScript loses the
 *  explicit optional-property types when `Omit` is applied to an interface
 *  carrying a string index signature (`[key: string]: unknown`) — the index
 *  signature would otherwise swallow them and resolve reads to `unknown`. */
type ThreadMessageSentPayload = Omit<
  OrchestrationEventPayload,
  | "threadId"
  | "messageId"
  | "role"
  | "text"
  | "streaming"
  | "turnId"
  | "createdAt"
  | "updatedAt"
  | "dispatchMode"
  | "source"
  | "attachments"
  | "skills"
  | "mentions"
> & {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly role: OrchestrationMessageRole;
  readonly text: string;
  readonly streaming: boolean;
  readonly turnId: TurnId | null;
  readonly createdAt: IsoDateTime;
  readonly updatedAt: IsoDateTime;
  readonly dispatchMode?: TurnDispatchMode | undefined;
  readonly source?: OrchestrationMessageSource | undefined;
  readonly attachments?: readonly ChatAttachment[] | undefined;
  readonly skills?: readonly ProviderSkillReference[] | undefined;
  readonly mentions?: readonly ProviderMentionReference[] | undefined;
};
// ─── T5e per-variant payloads (required fields the store reducer reads) ──
// These re-declare the fields MCode marks REQUIRED for each event variant on
// top of the permissive {@link OrchestrationEventPayload} base. Built via
// `Omit`+intersection (same pattern as `ThreadMessageSentPayload`) for two
// reasons: (1) `exactOptionalPropertyTypes` forbids widening optional base
// fields (`session?: T`) to required ones (`session: T`) via `extends`;
// (2) `Omit` on an interface with a string index signature would otherwise
// swallow the explicit optional-property types of sibling fields and resolve
// reads to `unknown`. The fields the store reducer treats as required are
// omitted from the base then re-declared as required.
/** Payload for `thread.session-set`: required `session`. */
type ThreadSessionSetPayload = Omit<OrchestrationEventPayload, "session"> & {
  readonly threadId: ThreadId;
  readonly session: OrchestrationSession;
};
/** Payload for `thread.activity-appended`: required `activity`. */
type ThreadActivityAppendedPayload = Omit<OrchestrationEventPayload, "activity"> & {
  readonly threadId: ThreadId;
  readonly activity: OrchestrationThreadActivity;
};
/** Payload for `thread.proposed-plan-upserted`: required `proposedPlan`. */
type ThreadProposedPlanUpsertedPayload = Omit<OrchestrationEventPayload, "proposedPlan"> & {
  readonly threadId: ThreadId;
  readonly proposedPlan: OrchestrationProposedPlan;
};
/** Payload for `thread.reverted`: required `turnCount`. */
type ThreadRevertedPayload = Omit<OrchestrationEventPayload, "turnCount"> & {
  readonly threadId: ThreadId;
  readonly turnCount: number;
};
/** `project.meta-updated` (MCode `ProjectMetaUpdatedPayload`): `updatedAt`
 *  required; `kind`/`title`/`workspaceRoot`/`defaultModelSelection`/`scripts`/
 *  `isPinned` optional. Built via Omit+intersection so the optional fields
 *  keep their precise types (defeating the index-signature/Omit swallow). */
type ProjectMetaUpdatedPayload = Omit<
  OrchestrationEventPayload,
  | "projectId"
  | "updatedAt"
  | "kind"
  | "title"
  | "workspaceRoot"
  | "defaultModelSelection"
  | "scripts"
  | "isPinned"
> & {
  readonly projectId: ProjectId;
  readonly kind?: ProjectKind;
  readonly title?: string;
  readonly workspaceRoot?: string;
  readonly defaultModelSelection?: ModelSelection | null;
  readonly scripts?: readonly ProjectScript[];
  readonly isPinned?: boolean;
  readonly updatedAt: IsoDateTime;
};

// T5e: additional per-variant payloads whose required fields the store
// reducer reads (pin/marker/meta/turn/diff events). Each omits the
// fields it re-declares as required (to defeat the index-signature/Omit
// interaction and exactOptional widening). Source of truth: MCode
// `packages/contracts/src/orchestration.ts` payload structs (lines 1476-1689).
/** `thread.meta-updated`: `updatedAt` required; rest optional per MCode.
 *  T5e: re-declares ALL optional fields the store reducer reads because
 *  `Omit` on an interface with a string index signature swallows the
 *  sibling optional-property types (resolves reads to `unknown`). */
type ThreadMetaUpdatedPayload = Omit<
  OrchestrationEventPayload,
  | "threadId"
  | "updatedAt"
  | "title"
  | "modelSelection"
  | "envMode"
  | "branch"
  | "worktreePath"
  | "associatedWorktreePath"
  | "associatedWorktreeBranch"
  | "associatedWorktreeRef"
  | "createBranchFlowCompleted"
  | "isPinned"
  | "parentThreadId"
  | "subagentAgentId"
  | "subagentNickname"
  | "subagentRole"
  | "handoff"
  | "lastKnownPr"
  | "pinnedMessages"
  | "threadMarkers"
  | "notes"
> & {
  readonly threadId: ThreadId;
  readonly title?: string;
  readonly modelSelection?: ModelSelection;
  readonly envMode?: ThreadEnvironmentMode;
  readonly branch?: string | null;
  readonly worktreePath?: string | null;
  readonly associatedWorktreePath?: string | null;
  readonly associatedWorktreeBranch?: string | null;
  readonly associatedWorktreeRef?: string | null;
  readonly createBranchFlowCompleted?: boolean;
  readonly isPinned?: boolean;
  readonly parentThreadId?: ThreadId | null;
  readonly subagentAgentId?: string | null;
  readonly subagentNickname?: string | null;
  readonly subagentRole?: string | null;
  readonly handoff?: ThreadHandoff | null;
  readonly lastKnownPr?: OrchestrationThreadPullRequest | null;
  readonly pinnedMessages?: readonly PinnedMessage[];
  readonly threadMarkers?: readonly ThreadMarker[];
  readonly notes?: string;
  readonly updatedAt: IsoDateTime;
};
/** `thread.pinned-message-added`: `pin` + `updatedAt` required. */
type ThreadPinnedMessageAddedPayload = Omit<
  OrchestrationEventPayload,
  "pin" | "updatedAt"
> & {
  readonly threadId: ThreadId;
  readonly pin: PinnedMessage;
  readonly updatedAt: IsoDateTime;
};
/** `thread.pinned-message-removed`: `messageId` + `updatedAt` required. */
type ThreadPinnedMessageRemovedPayload = Omit<
  OrchestrationEventPayload,
  "messageId" | "updatedAt"
> & {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly updatedAt: IsoDateTime;
};
/** `thread.pinned-message-done-set`: `messageId`/`done`/`updatedAt` required. */
type ThreadPinnedMessageDoneSetPayload = Omit<
  OrchestrationEventPayload,
  "messageId" | "done" | "updatedAt"
> & {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly done: boolean;
  readonly updatedAt: IsoDateTime;
};
/** `thread.pinned-message-label-set`: `messageId`/`label`/`updatedAt` required. */
type ThreadPinnedMessageLabelSetPayload = Omit<
  OrchestrationEventPayload,
  "messageId" | "label" | "updatedAt"
> & {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly label: PinnedMessageLabel | null;
  readonly updatedAt: IsoDateTime;
};
/** `thread.marker-added`: `marker` + `updatedAt` required. */
type ThreadMarkerAddedPayload = Omit<
  OrchestrationEventPayload,
  "marker" | "updatedAt"
> & {
  readonly threadId: ThreadId;
  readonly marker: ThreadMarker;
  readonly updatedAt: IsoDateTime;
};
/** `thread.marker-done-set`: `markerId`/`done`/`updatedAt` required. */
type ThreadMarkerDoneSetPayload = Omit<
  OrchestrationEventPayload,
  "markerId" | "done" | "updatedAt"
> & {
  readonly threadId: ThreadId;
  readonly markerId: ThreadMarkerId;
  readonly done: boolean;
  readonly updatedAt: IsoDateTime;
};
/** `thread.marker-removed`: `markerId`/`updatedAt` required. */
type ThreadMarkerRemovedPayload = Omit<
  OrchestrationEventPayload,
  "markerId" | "updatedAt"
> & {
  readonly threadId: ThreadId;
  readonly markerId: ThreadMarkerId;
  readonly updatedAt: IsoDateTime;
};
/** `thread.marker-label-set`: `markerId`/`label`/`updatedAt` required. */
type ThreadMarkerLabelSetPayload = Omit<
  OrchestrationEventPayload,
  "markerId" | "label" | "updatedAt"
> & {
  readonly threadId: ThreadId;
  readonly markerId: ThreadMarkerId;
  readonly label: TrimmedNonEmptyString | null;
  readonly updatedAt: IsoDateTime;
};
/** `thread.turn-start-requested`: `createdAt`/`runtimeMode`/`interactionMode`
 *  required; `modelSelection`/`sourceProposedPlan` optional. T5e: the
 *  optional fields are omitted+re-declared so the index signature doesn't
 *  swallow them after `Omit`. */
type ThreadTurnStartRequestedPayload = Omit<
  OrchestrationEventPayload,
  | "createdAt"
  | "runtimeMode"
  | "interactionMode"
  | "modelSelection"
  | "sourceProposedPlan"
> & {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly modelSelection?: ModelSelection;
  readonly sourceProposedPlan?: OrchestrationLatestTurn["sourceProposedPlan"];
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly createdAt: IsoDateTime;
};
/** `thread.session-stop-requested`: `createdAt` required. */
type ThreadSessionStopRequestedPayload = Omit<
  OrchestrationEventPayload,
  "createdAt"
> & {
  readonly threadId: ThreadId;
  readonly createdAt: IsoDateTime;
};
/** `thread.turn-diff-completed`: all diff fields required. */
type ThreadTurnDiffCompletedPayload = Omit<
  OrchestrationEventPayload,
  | "turnId"
  | "completedAt"
  | "status"
  | "files"
  | "checkpointRef"
  | "assistantMessageId"
  | "checkpointTurnCount"
> & {
  readonly threadId: ThreadId;
  readonly turnId: TurnId;
  readonly completedAt: IsoDateTime;
  readonly status: OrchestrationCheckpointStatus;
  readonly files: readonly OrchestrationCheckpointFile[];
  readonly checkpointRef: CheckpointRef;
  readonly assistantMessageId: MessageId | null;
  readonly checkpointTurnCount: NonNegativeInt;
};
/** `thread.approval-response-requested`: `decision` + `createdAt` required. */
type ThreadApprovalResponseRequestedPayload = Omit<
  OrchestrationEventPayload,
  "decision" | "createdAt"
> & {
  readonly threadId: ThreadId;
  readonly requestId: ApprovalRequestId;
  readonly decision: ProviderApprovalDecision;
  readonly createdAt: IsoDateTime;
};
/** `thread.user-input-response-requested`: `createdAt` required. */
type ThreadUserInputResponseRequestedPayload = Omit<
  OrchestrationEventPayload,
  "createdAt"
> & {
  readonly threadId: ThreadId;
  readonly requestId: ApprovalRequestId;
  readonly createdAt: IsoDateTime;
};
/** `thread.conversation-rolled-back`: `messageId`/`numTurns` required. */
type ThreadConversationRolledBackPayload = Omit<
  OrchestrationEventPayload,
  "messageId" | "numTurns" | "removedTurnIds"
> & {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly numTurns: number;
  readonly removedTurnIds?: readonly TurnId[];
};
/** Payload with a required `projectId`. */
interface ProjectIdPayload extends OrchestrationEventPayload {
  readonly projectId: ProjectId;
}
/** Payload for `project.created` (MCode `ProjectCreatedPayload`): kind/title/
 *  workspaceRoot/defaultModelSelection/scripts/createdAt/updatedAt required.
 *  Built via Omit+intersection for exactOptionalPropertyTypes compatibility.
 *  T5e: `defaultModelSelection` typed as `ModelSelection | null` (was `unknown`)
 *  and `isPinned` omitted+re-declared so the index signature doesn't swallow
 *  them after `Omit` (same index-signature/Omit interaction as
 *  `ThreadMessageSentPayload`). */
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
  | "isPinned"
> & {
  readonly projectId: ProjectId;
  readonly kind?: ProjectKind;
  readonly title: string;
  readonly workspaceRoot: string;
  readonly defaultModelSelection: ModelSelection | null;
  readonly scripts?: readonly ProjectScript[];
  readonly isPinned?: boolean;
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
      readonly payload: ProjectMetaUpdatedPayload;
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
      readonly payload: ThreadMetaUpdatedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.pinned-message-added";
      readonly payload: ThreadPinnedMessageAddedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.pinned-message-removed";
      readonly payload: ThreadPinnedMessageRemovedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.pinned-message-done-set";
      readonly payload: ThreadPinnedMessageDoneSetPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.pinned-message-label-set";
      readonly payload: ThreadPinnedMessageLabelSetPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.marker-added";
      readonly payload: ThreadMarkerAddedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.marker-removed";
      readonly payload: ThreadMarkerRemovedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.marker-done-set";
      readonly payload: ThreadMarkerDoneSetPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.marker-label-set";
      readonly payload: ThreadMarkerLabelSetPayload;
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
      readonly payload: ThreadSessionSetPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.activity-appended";
      readonly payload: ThreadActivityAppendedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.message-sent";
      readonly payload: ThreadMessageSentPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.turn-start-requested";
      readonly payload: ThreadTurnStartRequestedPayload;
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
      readonly payload: ThreadTurnDiffCompletedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.approval-response-requested";
      readonly payload: ThreadApprovalResponseRequestedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.user-input-response-requested";
      readonly payload: ThreadUserInputResponseRequestedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.checkpoint-revert-requested";
      readonly payload: ThreadIdPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.reverted";
      readonly payload: ThreadRevertedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.conversation-rollback-requested";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.conversation-rolled-back";
      readonly payload: ThreadConversationRolledBackPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.message-edit-resend-requested";
      readonly payload: ThreadMessagePayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.session-stop-requested";
      readonly payload: ThreadSessionStopRequestedPayload;
    })
  | (OrchestrationEventBase & {
      readonly type: "thread.proposed-plan-upserted";
      readonly payload: ThreadProposedPlanUpsertedPayload;
    });

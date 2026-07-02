/**
 * Tier 3 — Automation domain.
 *
 * Hand-ported from MCode `packages/contracts/src/automation.ts` (Effect
 * Schema → plain TS types). Covers the automation CRUD/run contract:
 * schedule union (manual/once/interval/daily/weekdays/weekly/cron), mode,
 * worktree mode, completion policy, run + run-result, definition, and the
 * default constants the composer UI references.
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/automation.ts
 */

import type { ThreadId, ProjectId, AutomationId, CommandId, MessageId, TurnId } from "../ids";
import type { AutomationRunId, IsoDateTime, NonNegativeInt, PositiveInt, TrimmedNonEmptyString } from "./base";
import type {
  ModelSelection,
  ProviderInteractionMode,
  ProviderKind,
  ProviderStartOptions,
  RuntimeMode,
} from "./orchestration";

export const DEFAULT_AUTOMATION_RUNTIME_MODE: RuntimeMode = "approval-required";

export type AutomationTimeOfDay = string;
export type AutomationTimezone = string;
export type AutomationCronExpression = string;

export type AutomationSchedule =
  | { readonly type: "manual" }
  | { readonly type: "once"; readonly runAt: IsoDateTime }
  | { readonly type: "interval"; readonly everySeconds: PositiveInt }
  | {
      readonly type: "daily";
      readonly timeOfDay: AutomationTimeOfDay;
      readonly timezone?: AutomationTimezone;
    }
  | {
      readonly type: "weekdays";
      readonly timeOfDay: AutomationTimeOfDay;
      readonly timezone?: AutomationTimezone;
    }
  | {
      readonly type: "weekly";
      readonly dayOfWeek: NonNegativeInt;
      readonly timeOfDay: AutomationTimeOfDay;
      readonly timezone?: AutomationTimezone;
    }
  | {
      readonly type: "cron";
      readonly expression: AutomationCronExpression;
      readonly timezone: AutomationTimezone;
    };

export type AutomationWorktreeMode = "auto" | "local" | "worktree";
export type AutomationMode = "standalone" | "heartbeat";

export type AutomationTrigger =
  | { readonly type: "manual" }
  | { readonly type: "scheduled" };

export type AutomationRunStatus =
  | "pending"
  | "claimed"
  | "running"
  | "waiting-for-approval"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "interrupted"
  | "skipped";

export type AutomationRunResultOutcome =
  | "findings"
  | "no-findings"
  | "changed-files"
  | "needs-attention"
  | "unknown";

export interface AutomationRunResultCompletionEvaluation {
  stopMatched: boolean;
  confidence: number;
  reason: TrimmedNonEmptyString;
}

export interface AutomationRunResult {
  outcome: AutomationRunResultOutcome;
  summary: TrimmedNonEmptyString | null;
  severity?: "info" | "warning" | "error";
  unread: boolean;
  archivedAt: IsoDateTime | null;
  completionEvaluation?: AutomationRunResultCompletionEvaluation;
}

export type AutomationAllowedCapability =
  | "send-turn"
  | "create-worktree"
  | "full-access";

export interface AutomationPermissionSnapshot {
  provider: ProviderKind;
  modelSelection: ModelSelection;
  providerOptions?: ProviderStartOptions;
  completionPolicyVersion?: NonNegativeInt;
  runtimeMode: RuntimeMode;
  interactionMode: ProviderInteractionMode;
  worktreeMode: AutomationWorktreeMode;
  allowedCapabilities: readonly AutomationAllowedCapability[];
  createdAt: IsoDateTime;
}

export type AutomationRetryPolicy =
  | { readonly type: "none" }
  | {
      readonly type: "fixed";
      readonly maxAttempts: PositiveInt;
      readonly delaySeconds: PositiveInt;
    }
  | {
      readonly type: "exponential";
      readonly maxAttempts: PositiveInt;
      readonly initialDelaySeconds: PositiveInt;
      readonly maxDelaySeconds: PositiveInt;
    };

export type AutomationMisfirePolicy = "skip" | "coalesce" | "run-latest";

export const DEFAULT_AUTOMATION_MINIMUM_INTERVAL_SECONDS = 60;
export const DEFAULT_AUTOMATION_FAST_INTERVAL_MAX_ITERATIONS = 10;
export const DEFAULT_AUTOMATION_MAX_RUNTIME_SECONDS = 60 * 60;
export const DEFAULT_AUTOMATION_RETRY_POLICY: AutomationRetryPolicy = {
  type: "none",
};
export const DEFAULT_AUTOMATION_MISFIRE_POLICY: AutomationMisfirePolicy =
  "coalesce";
export const DEFAULT_AUTOMATION_COMPLETION_POLICY = { type: "none" } as const;
export const DEFAULT_AUTOMATION_STOP_CONFIDENCE_THRESHOLD = 0.8;

export type AutomationCompletionPolicy =
  | { readonly type: "none" }
  | {
      readonly type: "ai-evaluated";
      readonly stopWhen: TrimmedNonEmptyString;
      readonly confidenceThreshold: number;
    };

export interface AutomationDefinition {
  id: AutomationId;
  projectId: ProjectId;
  sourceThreadId: ThreadId | null;
  name: TrimmedNonEmptyString;
  prompt: TrimmedNonEmptyString;
  schedule: AutomationSchedule;
  enabled: boolean;
  nextRunAt: IsoDateTime | null;
  modelSelection: ModelSelection;
  providerOptions?: ProviderStartOptions;
  runtimeMode: RuntimeMode;
  interactionMode: ProviderInteractionMode;
  worktreeMode: AutomationWorktreeMode;
  mode: AutomationMode;
  targetThreadId: ThreadId | null;
  maxIterations: PositiveInt | null;
  stopOnError: boolean;
  completionPolicy?: AutomationCompletionPolicy;
  completionPolicyVersion?: NonNegativeInt;
  completionPolicyUpdatedAt?: IsoDateTime;
  minimumIntervalSeconds: PositiveInt;
  maxRuntimeSeconds: PositiveInt | null;
  retryPolicy: AutomationRetryPolicy;
  misfirePolicy: AutomationMisfirePolicy;
  acknowledgedRisks: readonly ("full-access" | "local-checkout" | "fast-interval")[];
  iterationCount: NonNegativeInt;
  createdAt: IsoDateTime;
  updatedAt: IsoDateTime;
  archivedAt: IsoDateTime | null;
}

export interface AutomationRun {
  id: AutomationRunId;
  automationId: AutomationId;
  projectId: ProjectId;
  threadId: ThreadId | null;
  turnId?: TurnId | null;
  trigger: AutomationTrigger;
  status: AutomationRunStatus;
  scheduledFor: IsoDateTime;
  claimedBy: TrimmedNonEmptyString | null;
  claimedAt: IsoDateTime | null;
  leaseExpiresAt: IsoDateTime | null;
  startedAt: IsoDateTime | null;
  finishedAt: IsoDateTime | null;
  threadCreateCommandId: CommandId | null;
  turnStartCommandId: CommandId | null;
  messageId: MessageId | null;
  error: string | null;
  result: AutomationRunResult | null;
  permissionSnapshot: AutomationPermissionSnapshot;
  createdAt: IsoDateTime;
  updatedAt: IsoDateTime;
}

export interface AutomationListResult {
  definitions: readonly AutomationDefinition[];
  runs: readonly AutomationRun[];
}

// Ported from MCode `packages/contracts/src/automation.ts`. The vendored UI
// constructs `AutomationDefinition`s from these create/update inputs (e.g.
// `createAutomationDefinitionFromCreateRequest` casts a WS body to
// `AutomationCreateInput` and reads `input.enabled`, `input.stopOnError`,
// etc.). Without real shapes those field accesses collapse to `{}` and break
// boolean/string assignments under `exactOptionalPropertyTypes`.

/** Subset of {@link AutomationDefinition} that the client supplies on create
 *  (omits server-managed runtime fields: id, nextRunAt, iterationCount,
 *  completionPolicyVersion/UpdatedAt, createdAt/updatedAt/archivedAt). */
export interface AutomationCreateInput {
  projectId: ProjectId;
  sourceThreadId?: ThreadId | null;
  name: TrimmedNonEmptyString;
  prompt: TrimmedNonEmptyString;
  schedule: AutomationSchedule;
  enabled?: boolean;
  modelSelection: ModelSelection;
  providerOptions?: ProviderStartOptions;
  runtimeMode?: RuntimeMode;
  interactionMode?: ProviderInteractionMode;
  worktreeMode?: AutomationWorktreeMode;
  mode?: AutomationMode;
  targetThreadId?: ThreadId | null;
  maxIterations?: PositiveInt | null;
  stopOnError?: boolean;
  completionPolicy?: AutomationCompletionPolicy;
  minimumIntervalSeconds?: PositiveInt;
  maxRuntimeSeconds?: PositiveInt | null;
  retryPolicy?: AutomationRetryPolicy;
  misfirePolicy?: AutomationMisfirePolicy;
  acknowledgedRisks?: readonly ("full-access" | "local-checkout" | "fast-interval")[];
}

/** Partial update input: id + any subset of the create fields. */
export interface AutomationUpdateInput {
  id: AutomationId;
  projectId?: ProjectId;
  sourceThreadId?: ThreadId | null;
  name?: TrimmedNonEmptyString;
  prompt?: TrimmedNonEmptyString;
  schedule?: AutomationSchedule;
  enabled?: boolean;
  modelSelection?: ModelSelection;
  providerOptions?: ProviderStartOptions;
  runtimeMode?: RuntimeMode;
  interactionMode?: ProviderInteractionMode;
  worktreeMode?: AutomationWorktreeMode;
  mode?: AutomationMode;
  targetThreadId?: ThreadId | null;
  maxIterations?: PositiveInt | null;
  stopOnError?: boolean;
  completionPolicy?: AutomationCompletionPolicy;
  minimumIntervalSeconds?: PositiveInt;
  maxRuntimeSeconds?: PositiveInt | null;
  retryPolicy?: AutomationRetryPolicy;
  misfirePolicy?: AutomationMisfirePolicy;
  acknowledgedRisks?: readonly ("full-access" | "local-checkout" | "fast-interval")[];
}

export type AutomationStreamEvent =
  | {
      readonly type: "snapshot";
      readonly definitions: readonly AutomationDefinition[];
      readonly runs: readonly AutomationRun[];
    }
  | { readonly type: "definition-upserted"; readonly definition: AutomationDefinition }
  | { readonly type: "definition-deleted"; readonly automationId: AutomationId }
  | { readonly type: "run-upserted"; readonly run: AutomationRun };

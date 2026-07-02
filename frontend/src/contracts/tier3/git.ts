/**
 * Tier 3 — Git domain (status / branch / diff / stacked action / progress).
 *
 * Hand-ported from MCode `packages/contracts/src/git.ts` (Effect Schema →
 * plain TS types). Covers the git RPC surface the vendored UI imports beyond
 * Tier 0 (which had the branch/worktree/diff Input/Result DTOs declared
 * opaque in shell.ts). Real shapes here unblock property-access on the
 * hot-path components (BranchToolbar, GitActionsControl, PullRequestDialog).
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/git.ts
 */

import type { NonNegativeInt, PositiveInt, TrimmedNonEmptyString } from "./base";

export type GitStackedAction =
  | "commit"
  | "push"
  | "create_pr"
  | "commit_push"
  | "commit_push_pr";

export type GitActionProgressPhase = "branch" | "commit" | "push" | "pr";
export type GitActionProgressKind =
  | "action_started"
  | "phase_started"
  | "hook_started"
  | "hook_output"
  | "hook_finished"
  | "action_finished"
  | "action_failed";
export type GitActionProgressStream = "stdout" | "stderr";

export interface GitBranch {
  name: TrimmedNonEmptyString;
  isRemote?: boolean;
  remoteName?: TrimmedNonEmptyString;
  current: boolean;
  isDefault: boolean;
  worktreePath: TrimmedNonEmptyString | null;
}

export interface GitStatusPr {
  number: PositiveInt;
  title: TrimmedNonEmptyString;
  url: string;
  baseBranch: TrimmedNonEmptyString;
  headBranch: TrimmedNonEmptyString;
  state: "open" | "closed" | "merged";
}

export interface GitStatusFile {
  path: TrimmedNonEmptyString;
  insertions: NonNegativeInt;
  deletions: NonNegativeInt;
}

export interface GitStatusResult {
  branch: TrimmedNonEmptyString | null;
  hasWorkingTreeChanges: boolean;
  workingTree: {
    files: readonly GitStatusFile[];
    insertions: NonNegativeInt;
    deletions: NonNegativeInt;
  };
  hasUpstream: boolean;
  upstreamBranch: TrimmedNonEmptyString | null;
  aheadCount: NonNegativeInt;
  behindCount: NonNegativeInt;
  pr: GitStatusPr | null;
}

export interface GitStashInfoResult {
  cwd: TrimmedNonEmptyString;
  branch: TrimmedNonEmptyString | null;
  stashRef: TrimmedNonEmptyString;
  message: TrimmedNonEmptyString;
  files: readonly TrimmedNonEmptyString[];
}

export interface GitResolvedPullRequest {
  number: PositiveInt;
  title: TrimmedNonEmptyString;
  url: string;
  baseBranch: TrimmedNonEmptyString;
  headBranch: TrimmedNonEmptyString;
  state: "open" | "closed" | "merged";
}

export interface GitResolvePullRequestResult {
  pullRequest: GitResolvedPullRequest;
}

export interface GitReadWorkingTreeDiffInput {
  cwd: TrimmedNonEmptyString;
  scope?: "workingTree" | "unstaged" | "staged" | "branch";
}

export type GitBranchStepStatus = "created" | "skipped_not_requested";
export type GitCommitStepStatus =
  | "created"
  | "skipped_no_changes"
  | "skipped_not_requested";
export type GitPushStepStatus =
  | "pushed"
  | "skipped_not_requested"
  | "skipped_up_to_date";
export type GitPrStepStatus =
  | "created"
  | "opened_existing"
  | "skipped_not_requested";

export interface GitRunStackedActionResult {
  action: GitStackedAction;
  branch: {
    status: GitBranchStepStatus;
    name?: TrimmedNonEmptyString;
  };
  commit: {
    status: GitCommitStepStatus;
    commitSha?: TrimmedNonEmptyString;
    subject?: TrimmedNonEmptyString;
  };
  push: {
    status: GitPushStepStatus;
    branch?: TrimmedNonEmptyString;
    upstreamBranch?: TrimmedNonEmptyString;
    setUpstream?: boolean;
  };
  pr: {
    status: GitPrStepStatus;
    url?: string;
    number?: PositiveInt;
    baseBranch?: TrimmedNonEmptyString;
    headBranch?: TrimmedNonEmptyString;
    title?: TrimmedNonEmptyString;
  };
}

export type GitActionProgressEvent =
  | {
      readonly actionId: TrimmedNonEmptyString;
      readonly cwd: TrimmedNonEmptyString;
      readonly action: GitStackedAction;
      readonly kind: "action_started";
      readonly phases: readonly GitActionProgressPhase[];
    }
  | {
      readonly actionId: TrimmedNonEmptyString;
      readonly cwd: TrimmedNonEmptyString;
      readonly action: GitStackedAction;
      readonly kind: "phase_started";
      readonly phase: GitActionProgressPhase;
      readonly label: TrimmedNonEmptyString;
    }
  | {
      readonly actionId: TrimmedNonEmptyString;
      readonly cwd: TrimmedNonEmptyString;
      readonly action: GitStackedAction;
      readonly kind: "hook_started";
      readonly hookName: TrimmedNonEmptyString;
    }
  | {
      readonly actionId: TrimmedNonEmptyString;
      readonly cwd: TrimmedNonEmptyString;
      readonly action: GitStackedAction;
      readonly kind: "hook_output";
      readonly hookName: TrimmedNonEmptyString | null;
      readonly stream: GitActionProgressStream;
      readonly text: TrimmedNonEmptyString;
    }
  | {
      readonly actionId: TrimmedNonEmptyString;
      readonly cwd: TrimmedNonEmptyString;
      readonly action: GitStackedAction;
      readonly kind: "hook_finished";
      readonly hookName: TrimmedNonEmptyString;
      readonly exitCode: number | null;
      readonly durationMs: NonNegativeInt | null;
    }
  | {
      readonly actionId: TrimmedNonEmptyString;
      readonly cwd: TrimmedNonEmptyString;
      readonly action: GitStackedAction;
      readonly kind: "action_finished";
      readonly result: GitRunStackedActionResult;
    }
  | {
      readonly actionId: TrimmedNonEmptyString;
      readonly cwd: TrimmedNonEmptyString;
      readonly action: GitStackedAction;
      readonly kind: "action_failed";
      readonly phase: GitActionProgressPhase | null;
      readonly message: TrimmedNonEmptyString;
    };

/**
 * Tier 3 — Profile stats / token stats / heatmap.
 *
 * Hand-ported from MCode `packages/contracts/src/stats.ts` (Effect Schema →
 * plain TS types). Real shapes for ProfileHeatmapCell, ProfileStats,
 * ProfileTokenStats (the projected profile-page aggregates) and the input
 * DTOs the vendored UI's Profile page imports.
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/stats.ts
 */

import type { IsoDateTime, NonNegativeInt, TrimmedNonEmptyString } from "./base";
import type { ProviderKind } from "./orchestration";

export interface StatsGetProfileStatsInput {
  utcOffsetMinutes: number;
}
export type StatsGetProfileTokenStatsInput = StatsGetProfileStatsInput;

export interface ProfileHeatmapCell {
  day: TrimmedNonEmptyString;
  count: NonNegativeInt;
  weekday: number;
  intensity: NonNegativeInt;
}

export interface ProfileProviderUsage {
  provider: ProviderKind | "unknown";
  model: TrimmedNonEmptyString;
  turnCount: NonNegativeInt;
  percent: number;
}

export interface ProfileSkillUsage {
  name: TrimmedNonEmptyString;
  displayName: TrimmedNonEmptyString;
  kind: "skill" | "agent";
  runCount: NonNegativeInt;
}

export interface ProfileMostWorkedProject {
  projectId: TrimmedNonEmptyString;
  title: TrimmedNonEmptyString;
  workspaceRoot: TrimmedNonEmptyString;
  promptCount: NonNegativeInt;
  threadCount: NonNegativeInt;
  activeDays: NonNegativeInt;
  lastWorkedAt: IsoDateTime;
}

export interface ProfileQuota {
  status: "available" | "unavailable";
  provider: ProviderKind | null;
  window: string | null;
  usedPercent: number | null;
  resetsAt: IsoDateTime | null;
  planName: string | null;
}

export interface ProfileActivity {
  currentStreakDays: NonNegativeInt;
  longestStreakDays: NonNegativeInt;
  totalPromptsSent: NonNegativeInt;
  totalThreads: NonNegativeInt;
  promptsToday: NonNegativeInt;
  heatmapMetric: "prompts";
  heatmap: readonly ProfileHeatmapCell[];
}

export interface ProfileActiveHours {
  startHour: number | null;
  endHour: number | null;
  turnCount: NonNegativeInt;
  label: string | null;
}

export interface ProfileInsights {
  topProvider: ProviderKind | null;
  topProviderPercent: number | null;
  topReasoning: string | null;
  topReasoningPercent: number | null;
  skillsExplored: NonNegativeInt;
  totalSkillsUsed: NonNegativeInt;
}

export interface ProfileIdentity {
  homeDirBasename: string;
  initials: string;
  defaultHandle: string;
}

export interface ProfileTimezone {
  utcOffsetMinutes: number;
  today: TrimmedNonEmptyString;
}

export interface ProfileStats {
  generatedAt: IsoDateTime;
  timezone: ProfileTimezone;
  identity: ProfileIdentity;
  activity: ProfileActivity;
  activeHours: ProfileActiveHours;
  insights: ProfileInsights;
  providerModels: readonly ProfileProviderUsage[];
  skills: readonly ProfileSkillUsage[];
  mostUsedSkill: ProfileSkillUsage | null;
  mostWorkedProject: ProfileMostWorkedProject | null;
  quota: ProfileQuota;
}

export interface ProfileTokenStats {
  available: boolean;
  lifetimeTotalTokens: NonNegativeInt | null;
  peakDayTokens: NonNegativeInt | null;
  peakDay: TrimmedNonEmptyString | null;
  providers: readonly ProviderKind[];
  unavailableProviders: readonly ProviderKind[];
  heatmapMetric: "tokens";
  heatmap: readonly ProfileHeatmapCell[];
}

export type StatsGetProfileStatsResult = ProfileStats;
export type StatsGetProfileTokenStatsResult = ProfileTokenStats;

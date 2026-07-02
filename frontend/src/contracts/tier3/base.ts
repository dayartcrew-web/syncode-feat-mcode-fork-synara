/**
 * Tier 3 — Base schema primitives + extra branded IDs.
 *
 * Re-declares the Effect-Schema primitives from MCode `packages/contracts/src/
 * baseSchemas.ts` as plain TS types so the vendored UI's `import {
 * TrimmedNonEmptyString, AutomationRunId, ThreadMarkerId, … } from
 * "@t3tools/contracts"` resolves. All vendored UI sites use these as opaque
 * string-valued fields; runtime validation lives in the WS layer.
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/baseSchemas.ts
 */

import type { Branded } from "../ids";

/** Trimmed string (effect `Schema.Trim`). */
export type TrimmedString = string;
/** Trimmed, non-empty string (effect `TrimmedNonEmptyString`). */
export type TrimmedNonEmptyString = string;

/** Non-negative integer. */
export type NonNegativeInt = number;
/** Positive integer (>= 1). */
export type PositiveInt = number;

/** ISO-8601 datetime string. */
export type IsoDateTime = string;

/** POSIX-style environment record. */
export type ProcessEnvRecord = Record<string, string>;

// ─── Extra branded IDs (not in Tier 0 ids.ts) ──────────────────────────
// These mirror MCode's baseSchemas.ts `makeEntityId` brands that the vendored
// UI references but Tier 0's ids.ts did not yet include.

export type ThreadMarkerId = Branded<"ThreadMarkerId">;
export type AutomationRunId = Branded<"AutomationRunId">;
export type EnvironmentId = Branded<"EnvironmentId">;
export type AuthSessionId = Branded<"AuthSessionId">;

// Cast helpers for the extra branded IDs.
export const asThreadMarkerId = (s: string): ThreadMarkerId => s as ThreadMarkerId;
export const asAutomationRunId = (s: string): AutomationRunId => s as AutomationRunId;
export const asEnvironmentId = (s: string): EnvironmentId => s as EnvironmentId;
export const asAuthSessionId = (s: string): AuthSessionId => s as AuthSessionId;

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

// ─── Extra branded IDs ─────────────────────────────────────────────────
// These branded IDs were originally declared here (type-only) because Tier
// 0's ids.ts did not yet include them. They have now been consolidated into
// `../ids` (the canonical branded-ID home) as type+value pairs exposing the
// `.makeUnsafe` runtime factory the vendored UI calls. Re-exported here so
// existing `import { AutomationRunId, … } from "@t3tools/contracts"` sites
// that resolve through tier3/base keep working, and so the value namespace
// (`.makeUnsafe`) flows through.
export {
  ThreadMarkerId,
  AutomationRunId,
  EnvironmentId,
  AuthSessionId,
} from "../ids";

// Cast helpers for the extra branded IDs (kept for backward-compat with
// the barrel re-export at `contracts/index.ts`).
export const asThreadMarkerId = (s: string): Branded<"ThreadMarkerId"> =>
  s as Branded<"ThreadMarkerId">;
export const asAutomationRunId = (s: string): Branded<"AutomationRunId"> =>
  s as Branded<"AutomationRunId">;
export const asEnvironmentId = (s: string): Branded<"EnvironmentId"> =>
  s as Branded<"EnvironmentId">;
export const asAuthSessionId = (s: string): Branded<"AuthSessionId"> =>
  s as Branded<"AuthSessionId">;

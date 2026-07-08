/**
 * Tier 2 — Domain-event typed push views.
 *
 * Layers typed helpers over the ts-rs-generated `DomainEventDto` discriminated
 * union (see `../types/DomainEventDto.ts`) so push payloads on the
 * `push/orchestration` channel are typed instead of `Record<string, unknown>`.
 * See `CONTRACTS-BRIDGE-DESIGN.md` §4 / §6.3.
 *
 * ## Wire-parity caveat (NOT a bug here — server-side, T5)
 *
 * The TYPE model in this file is **camelCase** (`eventType`, `aggregateId`,
 * camelCase field names), matching MCode's frontend expectations
 * (design §3.3). However, Syncode's WS push envelope
 * (`crates/syncode-ws/src/push.rs`) currently emits **snake_case** keys
 * (`event_type`, `aggregate_id`). Full wire parity depends on T5 updating the
 * server to emit camelCase; T4's scope is the TYPE model only. Until T5 lands,
 * a thin adapter on the client (or the server) will translate the wire keys to
 * the camelCase shape modeled here.
 */

import type { DomainEventDto } from "../types/DomainEventDto";

export type { DomainEventDto };

/**
 * Union of all 44 domain-event tag strings (the `eventType` discriminant).
 * Equivalent to MCode's `OrchestrationEventType` literal union — see
 * `EVENT-MAP.md` for the 1:1 vs divergent mapping.
 */
export type DomainEventType = DomainEventDto["eventType"];

/**
 * Extract the payload ("data") type for a given event tag. Use as
 * `DomainEventPayload<"turnCompleted">` → `{ id, assistantOutput, durationMs,
 * completedAt }`.
 */
export type DomainEventPayload<E extends DomainEventType = DomainEventType> =
  Extract<DomainEventDto, { eventType: E }>["data"];

/**
 * The full set of 44 `eventType` tag strings, as a readonly const tuple.
 * Useful for runtime membership checks (`EVENT_TYPES.includes(x)`) and for
 * exhaustiveness assertions. Order matches the source enum
 * (`crates/syncode-core/src/domain/events.rs`): project (3) → thread (18) →
 * pinned (4) → marker (4) → turn (7) → message (3) → plan/checkpoint (2) →
 * revert/rollback (3) → activity (1) = 44.
 */
export const EVENT_TYPES = [
  // Project (3)
  "projectCreated",
  "projectUpdated",
  "projectDeleted",
  // Thread (18)
  "threadCreated",
  "threadStatusChanged",
  "threadTitleSet",
  "threadCheckpointSet",
  "threadReverted",
  "threadArchived",
  "threadUnarchived",
  "threadDeleted",
  "threadMessagesImported",
  "threadSessionStopRequested",
  "threadRuntimeModeSet",
  "threadInteractionModeSet",
  "threadMetaUpdated",
  "threadApprovalResponded",
  "threadUserInputResponded",
  "threadMessageEditedAndResent",
  "threadSessionSet",
  "turnDispatchRequested",
  // Pinned message (4)
  "pinnedMessageAdded",
  "pinnedMessageRemoved",
  "pinnedMessageDoneSet",
  "pinnedMessageLabelSet",
  // Marker (4)
  "markerAdded",
  "markerRemoved",
  "markerDoneSet",
  "markerLabelSet",
  // Turn (7)
  "turnStarted",
  "turnCompleted",
  "turnFailed",
  "turnCancelled",
  "turnInterrupted",
  "turnFilesModified",
  "turnCheckpointSet",
  // Message (3)
  "messageAdded",
  "messageDeltaAppended",
  "messageStreamingFinalized",
  // Proposed plan / checkpoint (2)
  "proposedPlanUpserted",
  "turnDiffCompleted",
  // Revert / rollback (3)
  "threadRevertCompleted",
  "conversationRollbackRequested",
  "conversationRolledBack",
  // Activity (1)
  "activityLogged",
] as const satisfies readonly DomainEventType[];

/**
 * Syncode's single push channel carrying orchestration/domain events.
 * (MCode defines 12 typed channels; Syncode multiplexes onto one generic
 * `push/<channel>` envelope — design §6.3. This typed view narrows the
 * `data` payload using the Tier 2 event union.)
 *
 * NOTE: until T5 (server wire update), the live server emits snake_case
 * `event_type`/`aggregate_id` keys. The shape below models the TARGET
 * camelCase wire; a client-side adapter translates until the server lands.
 */
export interface OrchestrationPushEnvelope {
  eventType: DomainEventType;
  aggregateId: string;
  /**
   * Typed when `eventType` is a known Syncode variant; falls back to
   * `Record<string, unknown>` for forward-compat with unrecognized event
   * tags (e.g. server-emitted events not yet mirrored in `DomainEventDto`).
   */
  data: DomainEventPayload | Record<string, unknown>;
  /** Monotonic sequence within the aggregate's event stream (optional). */
  sequence?: number;
  /** ISO 8601 timestamp the event was emitted (optional). */
  timestamp?: string;
}

/**
 * Per-channel push view map. MCode keys typed channels by name
 * (`onThreadEvent`, `onTerminalEvent`, …); Syncode multiplexes everything
 * onto `push/orchestration`. This map is the bridge surface for
 * channel-narrowed subscriptions (T5 transport layers a dispatcher over it).
 * Only the orchestration channel is typed today; the others carry
 * `Record<string, unknown>` until their backing RPCs/SSE feeds exist.
 */
export interface PushChannelViews {
  /** The typed domain-event channel — narrowed via `DomainEventDto`. */
  "push/orchestration": OrchestrationPushEnvelope;
  /** Untyped channels (terminal, providerRuntime, server, …) — Tier 3. */
  [channel: string]: unknown;
}

/**
 * Runtime type guard: narrows `unknown` to `DomainEventDto` by checking the
 * `eventType` discriminant is one of the 44 known tags AND the envelope has a
 * `data` object. Replaces MCode's Effect `Schema.is` for push-decode call
 * sites. Does NOT validate field shapes inside `data` (deep validation is
 * deferred — see design §5: ~6 production sites, hand-written guards).
 *
 * @example
 *   if (isDomainEventDto(payload)) {
 *     // payload.data is typed per payload.eventType
 *   }
 */
export function isDomainEventDto(x: unknown): x is DomainEventDto {
  if (typeof x !== "object" || x === null) return false;
  const obj = x as Record<string, unknown>;
  if (typeof obj["eventType"] !== "string") return false;
  return EVENT_TYPES.includes(obj["eventType"] as DomainEventType);
}

/**
 * Runtime guard for the {@link OrchestrationPushEnvelope} wire shape. Checks
 * `eventType` is a known tag and `aggregateId` is a string. `data` is left
 * untyped-checked (forward-compat). Use to narrow incoming WS frames before
 * dispatching on `eventType`.
 */
export function isOrchestrationPushEnvelope(
  x: unknown,
): x is OrchestrationPushEnvelope {
  if (typeof x !== "object" || x === null) return false;
  const obj = x as Record<string, unknown>;
  if (typeof obj["eventType"] !== "string") return false;
  if (typeof obj["aggregateId"] !== "string") return false;
  return EVENT_TYPES.includes(obj["eventType"] as DomainEventType);
}

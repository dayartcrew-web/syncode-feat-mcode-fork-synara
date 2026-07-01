/**
 * Branded identifier types — drop-in replacement for MCode's
 * `@t3tools/contracts` `baseSchemas.ts` branded-ID set.
 *
 * MCode brands IDs via Effect Schema branding (`Schema.brand("ThreadId")`).
 * Syncode uses a single generic `EntityId = string` on the wire. To give the
 * cloned MCode UI the same nominal type-safety at compile time, we declare
 * branded aliases here as `string & { readonly __brand: "..." }`.
 *
 * Branded IDs are **structurally strings**, so they interoperate with the
 * ts-rs-generated `EntityId` (= `string`) at boundaries via the `asId` /
 * `as*Id` cast helpers. The brand is erased at runtime (it is purely a
 * compile-time phantom), so JSON serde round-trips are unaffected.
 *
 * Brand set mirrors `/home/vibe-dev/mcode/packages/contracts/src/baseSchemas.ts`
 * (12 IDs selected for the Syncode parity subset — the IDs the cloned UI's
 * import sites reference; see CONTRACTS-BRIDGE-DESIGN.md §6.1).
 */

/**
 * A branded string: structurally a `string` at runtime, but nominally tagged
 * with `__brand: B` at compile time so distinct ID kinds don't cross-assign.
 */
export type Branded<B extends string> = string & {
  readonly __brand: B;
};

// ─── Branded ID type aliases ───────────────────────────────────────────
// Each is a distinct nominal string type. Source of truth for the set is
// MCode's baseSchemas.ts.

export type ThreadId = Branded<"ThreadId">;
export type ProjectId = Branded<"ProjectId">;
export type TurnId = Branded<"TurnId">;
export type MessageId = Branded<"MessageId">;
export type EventId = Branded<"EventId">;
export type CommandId = Branded<"CommandId">;
export type SessionId = Branded<"SessionId">;
export type ProviderItemId = Branded<"ProviderItemId">;
export type RuntimeSessionId = Branded<"RuntimeSessionId">;
export type CheckpointRef = Branded<"CheckpointRef">;
export type AutomationId = Branded<"AutomationId">;
export type ApprovalRequestId = Branded<"ApprovalRequestId">;

// ─── Cast helpers ──────────────────────────────────────────────────────

/**
 * Generic brand cast: tags any `string` with brand `B`. Use when bridging
 * an unbranded string (e.g. an `EntityId` from the wire, a UUID from a URL
 * param) into a branded ID. The cast is zero-cost at runtime.
 */
export const asId = <B extends string>(s: string): Branded<B> => s as Branded<B>;

// Per-ID convenience casts — match the brand set above. Prefer these at
// call sites for readability (`asThreadId(id)` vs `asId<"ThreadId">(id)`).

export const asThreadId = (s: string): ThreadId => asId<"ThreadId">(s);
export const asProjectId = (s: string): ProjectId => asId<"ProjectId">(s);
export const asTurnId = (s: string): TurnId => asId<"TurnId">(s);
export const asMessageId = (s: string): MessageId => asId<"MessageId">(s);
export const asEventId = (s: string): EventId => asId<"EventId">(s);
export const asCommandId = (s: string): CommandId => asId<"CommandId">(s);
export const asSessionId = (s: string): SessionId => asId<"SessionId">(s);
export const asProviderItemId = (s: string): ProviderItemId =>
  asId<"ProviderItemId">(s);
export const asRuntimeSessionId = (s: string): RuntimeSessionId =>
  asId<"RuntimeSessionId">(s);
export const asCheckpointRef = (s: string): CheckpointRef =>
  asId<"CheckpointRef">(s);
export const asAutomationId = (s: string): AutomationId =>
  asId<"AutomationId">(s);
export const asApprovalRequestId = (s: string): ApprovalRequestId =>
  asId<"ApprovalRequestId">(s);

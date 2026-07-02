/**
 * Branded identifier types AND runtime values — drop-in replacement for
 * MCode's `@t3tools/contracts` `baseSchemas.ts` branded-ID set.
 *
 * MCode brands IDs via Effect Schema branding (`Schema.brand("ThreadId")`).
 * The resulting `Schema` value is ALSO an Effect-encoded factory with a
 * `.makeUnsafe(s)` method that brands a raw string WITHOUT validation.
 * Syncode's vendored MCode UI calls `XxxId.makeUnsafe(...)` at ~1047 sites
 * to construct branded IDs from raw strings (wire `EntityId`, URL params,
 * test fixtures, etc.).
 *
 * To give the cloned UI the same compile-time nominal safety AND the same
 * runtime construction API, we declare EACH branded ID as BOTH:
 *   - a **type** (`type ThreadId = string & { __brand }`), and
 *   - a **value** (`const ThreadId = { makeUnsafe(s) { ... } }`).
 *
 * TypeScript permits a type and a const to share a name (the type and value
 * namespaces are separate), so `import { ThreadId } from "@t3tools/contracts"`
 * resolves to BOTH the type annotation (`: ThreadId`) and the runtime factory
 * (`ThreadId.makeUnsafe(...)`).
 *
 * Branded IDs are **structurally strings** at runtime (the brand is a
 * compile-time phantom), so:
 *   - JSON serde round-trips are unaffected,
 *   - they interoperate with the ts-rs-generated `EntityId` (= `string`) at
 *     boundaries via the `asId` / `as*Id` cast helpers.
 *
 * Method set: only `.makeUnsafe` is actually CALLED on branded IDs in the
 * vendored UI (verified via `grep -rhoE "[A-Z][a-zA-Z]+Id\.[a-zA-Z]+"`).
 * The other string-instance methods (`.length`, `.trim`, ...) seen in
 * grep output are calls on `string`-typed variables whose NAMES happen to
 * end in `Id` — they work transparently because branded IDs are structurally
 * `string`. We therefore expose exactly `.makeUnsafe` (the Effect Schema
 * brand factory's no-validation constructor).
 *
 * Brand set mirrors `/home/vibe-dev/mcode/packages/contracts/src/baseSchemas.ts`
 * (the IDs the cloned UI's import sites reference — see
 * CONTRACTS-BRIDGE-DESIGN.md §6.1).
 */

/**
 * A branded string: structurally a `string` at runtime, but nominally tagged
 * with `__brand: B` at compile time so distinct ID kinds don't cross-assign.
 */
export type Branded<B extends string> = string & {
  readonly __brand: B;
};

/**
 * Factory shape shared by every branded ID value. Mirrors the subset of
 * MCode's Effect-Schema brand API that the vendored UI actually invokes.
 */
export interface BrandedIdFactory<T extends Branded<string>> {
  /**
   * Brand a raw string as `T` WITHOUT validation. Mirrors Effect Schema's
   * `Schema.brand(...)` `.makeUnsafe` — the cast is zero-cost at runtime.
   * Use when bridging an unbranded string (wire `EntityId`, a UUID from a
   * URL param, a test fixture) into a branded ID.
   */
  makeUnsafe(s: string): T;
}

// ─── Branded IDs: type + value (factory) ───────────────────────────────
// Each declaration pairs a nominal string type with a runtime factory that
// shares the name. `import { ThreadId }` resolves to both spaces.

export type ThreadId = Branded<"ThreadId">;
export const ThreadId: BrandedIdFactory<ThreadId> = {
  makeUnsafe(s: string): ThreadId {
    return s as ThreadId;
  },
};

export type ProjectId = Branded<"ProjectId">;
export const ProjectId: BrandedIdFactory<ProjectId> = {
  makeUnsafe(s: string): ProjectId {
    return s as ProjectId;
  },
};

export type TurnId = Branded<"TurnId">;
export const TurnId: BrandedIdFactory<TurnId> = {
  makeUnsafe(s: string): TurnId {
    return s as TurnId;
  },
};

export type MessageId = Branded<"MessageId">;
export const MessageId: BrandedIdFactory<MessageId> = {
  makeUnsafe(s: string): MessageId {
    return s as MessageId;
  },
};

export type EventId = Branded<"EventId">;
export const EventId: BrandedIdFactory<EventId> = {
  makeUnsafe(s: string): EventId {
    return s as EventId;
  },
};

export type CommandId = Branded<"CommandId">;
export const CommandId: BrandedIdFactory<CommandId> = {
  makeUnsafe(s: string): CommandId {
    return s as CommandId;
  },
};

export type SessionId = Branded<"SessionId">;
export const SessionId: BrandedIdFactory<SessionId> = {
  makeUnsafe(s: string): SessionId {
    return s as SessionId;
  },
};

export type ProviderItemId = Branded<"ProviderItemId">;
export const ProviderItemId: BrandedIdFactory<ProviderItemId> = {
  makeUnsafe(s: string): ProviderItemId {
    return s as ProviderItemId;
  },
};

export type RuntimeSessionId = Branded<"RuntimeSessionId">;
export const RuntimeSessionId: BrandedIdFactory<RuntimeSessionId> = {
  makeUnsafe(s: string): RuntimeSessionId {
    return s as RuntimeSessionId;
  },
};

export type RuntimeItemId = Branded<"RuntimeItemId">;
export const RuntimeItemId: BrandedIdFactory<RuntimeItemId> = {
  makeUnsafe(s: string): RuntimeItemId {
    return s as RuntimeItemId;
  },
};

export type RuntimeRequestId = Branded<"RuntimeRequestId">;
export const RuntimeRequestId: BrandedIdFactory<RuntimeRequestId> = {
  makeUnsafe(s: string): RuntimeRequestId {
    return s as RuntimeRequestId;
  },
};

export type RuntimeTaskId = Branded<"RuntimeTaskId">;
export const RuntimeTaskId: BrandedIdFactory<RuntimeTaskId> = {
  makeUnsafe(s: string): RuntimeTaskId {
    return s as RuntimeTaskId;
  },
};

export type CheckpointRef = Branded<"CheckpointRef">;
export const CheckpointRef: BrandedIdFactory<CheckpointRef> = {
  makeUnsafe(s: string): CheckpointRef {
    return s as CheckpointRef;
  },
};

export type AutomationId = Branded<"AutomationId">;
export const AutomationId: BrandedIdFactory<AutomationId> = {
  makeUnsafe(s: string): AutomationId {
    return s as AutomationId;
  },
};

export type AutomationRunId = Branded<"AutomationRunId">;
export const AutomationRunId: BrandedIdFactory<AutomationRunId> = {
  makeUnsafe(s: string): AutomationRunId {
    return s as AutomationRunId;
  },
};

export type ApprovalRequestId = Branded<"ApprovalRequestId">;
export const ApprovalRequestId: BrandedIdFactory<ApprovalRequestId> = {
  makeUnsafe(s: string): ApprovalRequestId {
    return s as ApprovalRequestId;
  },
};

export type ThreadMarkerId = Branded<"ThreadMarkerId">;
export const ThreadMarkerId: BrandedIdFactory<ThreadMarkerId> = {
  makeUnsafe(s: string): ThreadMarkerId {
    return s as ThreadMarkerId;
  },
};

// IDs referenced by the vendored UI's `makeUnsafe` call sites beyond
// baseSchemas.ts's primary set. NOTE: MCode defines `OrchestrationProposedPlanId`
// as `TrimmedNonEmptyString` (a plain trimmed string, NOT a branded ID — see
// orchestration.ts:424). It is BOTH a `string` type AND an Effect Schema value
// exposing `.makeUnsafe`. We model it here as a plain `string` type (so raw
// string assignments type-check, matching MCode) paired with a `.makeUnsafe`
// factory value (so the 12 vendored UI `OrchestrationProposedPlanId.makeUnsafe`
// call sites resolve). EnvironmentId / AuthSessionId ARE branded in MCode's
// baseSchemas.ts.
export type OrchestrationProposedPlanId = string;
export const OrchestrationProposedPlanId: {
  makeUnsafe(s: string): OrchestrationProposedPlanId;
} = {
  makeUnsafe(s: string): OrchestrationProposedPlanId {
    return s;
  },
};

export type EnvironmentId = Branded<"EnvironmentId">;
export const EnvironmentId: BrandedIdFactory<EnvironmentId> = {
  makeUnsafe(s: string): EnvironmentId {
    return s as EnvironmentId;
  },
};

export type AuthSessionId = Branded<"AuthSessionId">;
export const AuthSessionId: BrandedIdFactory<AuthSessionId> = {
  makeUnsafe(s: string): AuthSessionId {
    return s as AuthSessionId;
  },
};

// ─── Cast helpers ──────────────────────────────────────────────────────

/**
 * Generic brand cast: tags any `string` with brand `B`. Use when bridging
 * an unbranded string (e.g. an `EntityId` from the wire, a UUID from a URL
 * param) into a branded ID. The cast is zero-cost at runtime.
 *
 * Prefer the per-ID `XxxId.makeUnsafe(s)` factories above for new code
 * (they mirror MCode's Effect-Schema brand API). These `as*Id` helpers are
 * retained for backward-compat with T5b call sites.
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

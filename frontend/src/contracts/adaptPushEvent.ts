/**
 * Client-side push-event adapter — bridges the live WS push wire shape to the
 * `OrchestrationEvent` shape the store consumes.
 *
 * The syncode-ws backend publishes domain events on the `push/orchestration`
 * channel as a double-nested, PascalCase envelope:
 *   { eventType: "TurnCompleted", aggregateId, data: { event_type, data: {snake_payload} } }
 * The frontend store (`applyOrchestrationEvent`) switches on `event.type`
 * (dot-notation, e.g. "thread.session-set") and reads `event.payload`
 * (camelCase, branded IDs). Without this adapter, `event.type` is `undefined`
 * on every frame → `default` no-op → live updates silently dropped → chat
 * stuck in "working". (Documented as the "T5 transport adapter" gap in
 * `events.ts` §Wire-parity caveat + `EVENT-MAP.md`.)
 *
 * Responsibilities:
 *  - normalize the PascalCase wire tag → a dot `type` (per `EVENT-MAP.md`);
 *  - unwrap `data.data` (the snake_case payload) + snake→camelCase + null→undefined;
 *  - brand IDs (ThreadId/ProjectId/MessageId/TurnId/EventId);
 *  - resolve `threadId` for events whose payload lacks it (turn/message events
 *    key off the turn/message id) via per-connection `turns`/`messages` maps;
 *  - SYNTHESIZE `thread.session-set` + `thread.activity-appended` from
 *    `TurnCompleted`/`TurnFailed` (the backend emits no `ThreadSessionSet`
 *    during a turn, so the spinner would never clear without synthesis);
 *  - generate a per-connection monotonic `sequence` (the wire carries none).
 *
 * Returns an array: one backend frame may fold into 0, 1, or several store
 * events (synthesis / message-lifecycle fold).
 */
import { EventId, MessageId, ProjectId, ThreadId, TurnId } from "./ids";
import type { IsoDateTime, NonNegativeInt } from "./tier3/base";
import {
  DEFAULT_RUNTIME_MODE,
  type OrchestrationEvent,
  type OrchestrationSession,
  type OrchestrationSessionStatus,
  type OrchestrationThreadActivity,
  type OrchestrationThreadActivityTone,
  type RuntimeMode,
} from "./tier3/orchestration";

// ─── Raw wire envelope ────────────────────────────────────────────────
// The backend's actual shape (PascalCase tag, double-nested data). Distinct
// from the typed `OrchestrationPushEnvelope` (camelCase) in `events.ts`,
// which models the T5 *target* wire, not the current one.
export interface RawPushEnvelope {
  eventType: string; // PascalCase, e.g. "TurnCompleted"
  aggregateId: string | null;
  data: unknown;
  sequence?: number;
  timestamp?: string;
}

export interface PushAdaptContext {
  /** Per-connection monotonic counter (the wire carries no sequence). */
  nextSequence(): NonNegativeInt;
  /** turnId → threadId (seeded from TurnStarted). */
  readonly turns: Map<string, string>;
  /** messageId → turnId (seeded from MessageAdded/MessageDeltaAppended). */
  readonly messages: Map<string, string>;
  /** threadId → last-known session fields (seeded opportunistically). */
  readonly session: Map<string, { providerName: string | null; runtimeMode: RuntimeMode }>;
}

const LRU_CAP = 256;

function lruSet(map: Map<string, string>, k: string, v: string): void {
  map.set(k, v);
  if (map.size > LRU_CAP) {
    const oldest = map.keys().next().value;
    if (typeof oldest === "string") map.delete(oldest);
  }
}

export function createPushAdaptContext(): PushAdaptContext {
  let seq = 0;
  return {
    nextSequence: () => ++seq as NonNegativeInt,
    turns: new Map(),
    messages: new Map(),
    session: new Map(),
  };
}

// ─── Payload helpers ──────────────────────────────────────────────────

type RawObj = Record<string, unknown>;

/** Lowercase the first char: "TurnCompleted" → "turnCompleted". */
function toCamelTag(pascal: string): string {
  if (pascal.length === 0) return pascal;
  const first = pascal.charAt(0).toLowerCase();
  return first + pascal.slice(1);
}

/** snake_case → camelCase for a single key (leaves already-camel keys alone). */
function snakeToCamelKey(key: string): string {
  return key.replace(/_([a-z0-9])/g, (_, c) => c.toUpperCase());
}

/** Recursively convert snake_case keys → camelCase, and `null` → `undefined`. */
function normalizeValue(value: unknown): unknown {
  if (value === null) return undefined;
  if (Array.isArray(value)) return value.map(normalizeValue);
  if (typeof value === "object") {
    const out: RawObj = {};
    for (const [k, v] of Object.entries(value as RawObj)) {
      out[snakeToCamelKey(k)] = normalizeValue(v);
    }
    return out;
  }
  return value;
}

/**
 * Unwrap the inner payload: the wire nests it under `data.data` (the serde
 * `{event_type, data}` envelope). Defensive: if `data` is already the flat
 * payload (e.g. a snapshot), return it as-is.
 */
function unwrapPayload(env: RawPushEnvelope): RawObj | null {
  const outer = env.data;
  if (outer && typeof outer === "object" && "data" in outer && "event_type" in outer) {
    const inner = (outer as RawObj)["data"];
    return inner && typeof inner === "object" ? (inner as RawObj) : null;
  }
  return outer && typeof outer === "object" ? (outer as RawObj) : null;
}

/** Pick the first present snake_case timestamp from a raw payload. */
function pickTimestamp(raw: RawObj | null, ...keys: string[]): IsoDateTime {
  if (raw) {
    for (const k of keys) {
      const v = raw[k];
      if (typeof v === "string") return v;
    }
  }
  return new Date().toISOString() as IsoDateTime;
}

/** Resolve the owning threadId for an event whose payload may lack `thread_id`. */
function resolveThreadId(
  raw: RawObj | null,
  ctx: PushAdaptContext,
  fallbackAggregateId: string | null,
): string | null {
  if (raw) {
    const tid = raw["thread_id"];
    if (typeof tid === "string") return tid;
    const turnId = raw["turn_id"];
    if (typeof turnId === "string") {
      const t = ctx.turns.get(turnId);
      if (t) return t;
    }
    const msgId = raw["id"] ?? raw["message_id"];
    if (typeof msgId === "string") {
      // `id` may be a turn id (TurnCompleted/TurnFailed carry id=turnId) or a
      // message id (MessageStreamingFinalized). Try the turns map first, then
      // the messages→turns chain.
      const tFromTurn = ctx.turns.get(msgId);
      if (tFromTurn) return tFromTurn;
      const turn = ctx.messages.get(msgId);
      if (turn) {
        const t = ctx.turns.get(turn);
        if (t) return t;
      }
    }
  }
  return fallbackAggregateId;
}

// ─── Event construction ───────────────────────────────────────────────

interface BaseFields {
  sequence: NonNegativeInt;
  occurredAt: IsoDateTime;
  aggregateId: string;
  aggregateKind: "project" | "thread";
}

function makeEvent(
  type: OrchestrationEvent["type"],
  base: BaseFields,
  payload: Record<string, unknown>,
): OrchestrationEvent {
  const event = {
    sequence: base.sequence,
    eventId: EventId.makeUnsafe(`push-${base.sequence}`),
    aggregateKind: base.aggregateKind,
    aggregateId: base.aggregateId,
    occurredAt: base.occurredAt,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    type,
    payload,
  };
  return event as unknown as OrchestrationEvent;
}

function buildSession(
  threadId: string,
  status: OrchestrationSessionStatus,
  activeTurnId: string | null,
  lastError: string | null,
  updatedAt: IsoDateTime,
  ctx: PushAdaptContext,
): OrchestrationSession {
  const known = ctx.session.get(threadId);
  return {
    threadId: ThreadId.makeUnsafe(threadId),
    status,
    providerName: known?.providerName ?? null,
    runtimeMode: known?.runtimeMode ?? DEFAULT_RUNTIME_MODE,
    activeTurnId: activeTurnId ? TurnId.makeUnsafe(activeTurnId) : null,
    lastError,
    updatedAt,
  };
}

function buildActivity(
  activityId: string,
  kind: string,
  tone: OrchestrationThreadActivityTone,
  turnId: string | null,
  createdAt: IsoDateTime,
  payload: unknown,
): OrchestrationThreadActivity {
  return {
    id: EventId.makeUnsafe(activityId),
    tone,
    kind,
    summary: undefined,
    payload,
    turnId: turnId ? TurnId.makeUnsafe(turnId) : null,
    createdAt,
  };
}

// ─── The adapter ──────────────────────────────────────────────────────

export function adaptPushEnvelope(
  env: RawPushEnvelope,
  ctx: PushAdaptContext,
): OrchestrationEvent[] {
  const tag = toCamelTag(env.eventType ?? "");
  const raw = unwrapPayload(env);
  const out: OrchestrationEvent[] = [];

  const seq = ctx.nextSequence();
  const occurredAt = (env.timestamp as IsoDateTime | undefined) ??
    pickTimestamp(raw, "completed_at", "finalized_at", "created_at", "updated_at", "reverted_at", "archived_at", "unarchived_at", "deleted_at", "requested_at", "pinned_at");

  switch (tag) {
    // ── Turn lifecycle (synthesize session + activity) ────────────────
    case "turnStarted": {
      // Seed turnId → threadId for later resolution.
      if (raw) {
        const id = raw["id"];
        const tid = raw["thread_id"];
        if (typeof id === "string" && typeof tid === "string") lruSet(ctx.turns, id, tid);
      }
      // Emit session-set(running) so the UI's `phase` becomes "running" →
      // `serverAcknowledgedLocalDispatch` flips true → `isSendBusy` clears +
      // `localDispatch` resets (the composer stops thinking it's still sending).
      // Without this, the session stays null (backend emits no ThreadSessionSet
      // on the turn path) → serverAck stays false → isSendBusy → spinner stuck.
      const turnId = raw ? (raw["id"] as string | undefined) : undefined;
      const threadId = raw ? (raw["thread_id"] as string | undefined) : undefined;
      if (threadId) {
        const ts = pickTimestamp(raw, "created_at");
        out.push(
          makeEvent(
            "thread.session-set",
            { sequence: seq, occurredAt, aggregateId: threadId, aggregateKind: "thread" },
            {
              threadId: ThreadId.makeUnsafe(threadId),
              session: buildSession(threadId, "running", turnId ?? null, null, ts, ctx),
            },
          ),
        );
      }
      return out;
    }
    case "turnCompleted":
    case "turnFailed": {
      const turnId = raw ? (raw["id"] as string | undefined) : undefined;
      // No aggregateId fallback here — for turn events the aggregateId IS the
      // turn id, not the thread id, so falling back would address the wrong
      // thread. If the turns/messages maps can't resolve it (missed
      // TurnStarted/MessageDeltaAppended), drop and rely on snapshot/poll.
      const threadId = resolveThreadId(raw, ctx, null);
      if (!threadId) return out;
      const completed = tag === "turnCompleted";
      const updatedAt = pickTimestamp(raw, "completed_at", "created_at");
      const lastError = completed ? null : (extractError(raw) ?? "turn failed");
      out.push(
        makeEvent(
          "thread.session-set",
          { sequence: seq, occurredAt, aggregateId: threadId, aggregateKind: "thread" },
          {
            threadId: ThreadId.makeUnsafe(threadId),
            session: buildSession(threadId, completed ? "ready" : "error", null, lastError, updatedAt, ctx),
          },
        ),
      );
      out.push(
        makeEvent(
          "thread.activity-appended",
          { sequence: ctx.nextSequence(), occurredAt, aggregateId: threadId, aggregateKind: "thread" },
          {
            threadId: ThreadId.makeUnsafe(threadId),
            activity: buildActivity(
              `act-${seq}`,
              completed ? "turn.completed" : "turn.failed",
              completed ? "info" : "error",
              turnId ?? null,
              updatedAt,
              raw ? normalizeValue(raw) : {},
            ),
          },
        ),
      );
      // The backend emits no `MessageStreamingFinalized` on the turn path, so
      // the assistant message (streamed via MessageDeltaAppended with
      // id=turnId) would stay `streaming:true` → latestTurn.state stays
      // "running" → spinner never clears. Synthesize a finalize message-sent
      // (streaming:false) keyed on the same messageId so coalesce merges it
      // with the deltas and the store flips latestTurn.state → "completed".
      if (completed && turnId) {
        out.push(
          makeEvent(
            "thread.message-sent",
            { sequence: ctx.nextSequence(), occurredAt, aggregateId: threadId, aggregateKind: "thread" },
            {
              threadId: ThreadId.makeUnsafe(threadId),
              messageId: MessageId.makeUnsafe(turnId),
              role: "assistant",
              text: "",
              streaming: false,
              turnId: TurnId.makeUnsafe(turnId),
              createdAt: updatedAt,
              updatedAt,
            },
          ),
        );
      }
      return out;
    }

    // ── Message lifecycle (fold into thread.message-sent) ─────────────
    case "messageAdded":
    case "messageDeltaAppended":
    case "messageStreamingFinalized": {
      const messageId = raw ? (raw["id"] as string | undefined) : undefined;
      const turnId = raw ? (raw["turn_id"] as string | undefined) : undefined;
      if (messageId && turnId) lruSet(ctx.messages, messageId, turnId);
      const threadId = resolveThreadId(raw, ctx, env.aggregateId);
      if (!threadId || !messageId) return out;
      const role = (raw ? (raw["role"] as string | undefined) : undefined) ?? "assistant";
      const streaming =
        tag === "messageDeltaAppended" ? true : tag === "messageStreamingFinalized" ? false : Boolean(raw?.["streaming"]);
      const text = (raw ? (raw["text"] as string | undefined) : undefined) ?? (raw ? (raw["delta"] as string | undefined) : undefined) ?? "";
      const ts = pickTimestamp(raw, "created_at", "updated_at", "finalized_at");
      out.push(
        makeEvent(
          "thread.message-sent",
          { sequence: seq, occurredAt, aggregateId: threadId, aggregateKind: "thread" },
          {
            threadId: ThreadId.makeUnsafe(threadId),
            messageId: MessageId.makeUnsafe(messageId),
            role,
            text,
            streaming,
            turnId: turnId ? TurnId.makeUnsafe(turnId) : null,
            createdAt: ts,
            updatedAt: ts,
          },
        ),
      );
      return out;
    }

    // ── Activity (tool calls etc.) ────────────────────────────────────
    case "activityLogged": {
      const threadId = resolveThreadId(raw, ctx, env.aggregateId);
      if (!threadId) return out;
      const kind = (raw ? (raw["activity_type"] as string | undefined) : undefined) ?? (raw ? (raw["kind"] as string | undefined) : undefined) ?? "activity";
      const turnId = raw ? (raw["turn_id"] as string | undefined) : undefined;
      const ts = pickTimestamp(raw, "created_at", "updated_at");
      out.push(
        makeEvent(
          "thread.activity-appended",
          { sequence: seq, occurredAt, aggregateId: threadId, aggregateKind: "thread" },
          {
            threadId: ThreadId.makeUnsafe(threadId),
            activity: buildActivity(`act-${seq}`, kind, "tool", turnId ?? null, ts, raw ? normalizeValue(raw) : {}),
          },
        ),
      );
      return out;
    }

    // ── Thread lifecycle ──────────────────────────────────────────────
    case "threadCreated": {
      const threadId = (raw ? (raw["id"] as string | undefined) : undefined) ?? env.aggregateId;
      if (!threadId || !raw) return out;
      out.push(
        makeEvent(
          "thread.created",
          { sequence: seq, occurredAt, aggregateId: threadId, aggregateKind: "thread" },
          {
            threadId: ThreadId.makeUnsafe(threadId),
            projectId: brandOpt(raw["project_id"], ProjectId),
            providerId: typeof raw["provider_id"] === "string" ? raw["provider_id"] : undefined,
            model: typeof raw["model"] === "string" ? raw["model"] : undefined,
            createdAt: pickTimestamp(raw, "created_at"),
          },
        ),
      );
      return out;
    }
    case "threadMetaUpdated":
    case "threadTitleSet":
    case "threadStatusChanged":
    case "threadRuntimeModeSet":
    case "threadInteractionModeSet": {
      const threadId = resolveThreadId(raw, ctx, env.aggregateId);
      if (!threadId || !raw) return out;
      if (tag === "threadRuntimeModeSet" && typeof raw["runtime_mode"] === "string") {
        ctx.session.set(threadId, {
          providerName: ctx.session.get(threadId)?.providerName ?? null,
          runtimeMode: raw["runtime_mode"] as RuntimeMode,
        });
      }
      out.push(
        makeEvent(
          "thread.meta-updated",
          { sequence: seq, occurredAt, aggregateId: threadId, aggregateKind: "thread" },
          {
            threadId: ThreadId.makeUnsafe(threadId),
            updatedAt: pickTimestamp(raw, "updated_at", "created_at"),
            title: typeof raw["title"] === "string" ? raw["title"] : undefined,
          },
        ),
      );
      return out;
    }
    case "threadSessionSet": {
      const threadId = resolveThreadId(raw, ctx, env.aggregateId);
      if (!threadId || !raw) return out;
      // Seed session fields for future synthesis fallback.
      const sess = raw["session"];
      if (sess && typeof sess === "object") {
        const s = sess as RawObj;
        ctx.session.set(threadId, {
          providerName: typeof s["provider_name"] === "string" ? s["provider_name"] : null,
          runtimeMode: (typeof s["runtime_mode"] === "string" ? s["runtime_mode"] : DEFAULT_RUNTIME_MODE) as RuntimeMode,
        });
      }
      out.push(
        makeEvent(
          "thread.session-set",
          { sequence: seq, occurredAt, aggregateId: threadId, aggregateKind: "thread" },
          {
            threadId: ThreadId.makeUnsafe(threadId),
            session: buildSessionFromRaw(threadId, sess, occurredAt, ctx),
          },
        ),
      );
      return out;
    }

    // ── Project lifecycle ─────────────────────────────────────────────
    // NOTE: `projectCreated` is intentionally NOT emitted. The backend's
    // ProjectCreated payload ({id,name,root_path,created_at}) carries no
    // `default_model_selection`, and the store's project.created reducer
    // forwards `defaultModelSelection: undefined` into
    // `normalizeModelSelection(value, …)` which reads `value.model` → crash.
    // Passing `null` instead would clobber the richer shell-snapshot value.
    // The shell snapshot (getShellSnapshot) is the source of truth for
    // projects, so skipping the push is safe + avoids the crash.
    case "projectCreated":
      return out;
    case "projectUpdated": {
      const projectId = (raw ? (raw["id"] as string | undefined) : undefined) ?? env.aggregateId;
      if (!projectId || !raw) return out;
      out.push(
        makeEvent(
          "project.meta-updated",
          { sequence: seq, occurredAt, aggregateId: projectId, aggregateKind: "project" },
          {
            projectId: ProjectId.makeUnsafe(projectId),
            updatedAt: pickTimestamp(raw, "updated_at", "created_at"),
            title: typeof raw["name"] === "string" ? raw["name"] : undefined,
            workspaceRoot: typeof raw["root_path"] === "string" ? raw["root_path"] : undefined,
          },
        ),
      );
      return out;
    }
    case "projectDeleted": {
      const projectId = (raw ? (raw["id"] as string | undefined) : undefined) ?? env.aggregateId;
      if (!projectId) return out;
      out.push(
        makeEvent(
          "project.deleted",
          { sequence: seq, occurredAt, aggregateId: projectId, aggregateKind: "project" },
          {
            projectId: ProjectId.makeUnsafe(projectId),
            deletedAt: pickTimestamp(raw, "deleted_at", "updated_at"),
          },
        ),
      );
      return out;
    }

    // ── Turn dispatch request ─────────────────────────────────────────
    case "turnDispatchRequested": {
      // `id` here is the thread id.
      const threadId = (raw ? (raw["id"] as string | undefined) : undefined) ?? env.aggregateId;
      if (!threadId || !raw) return out;
      out.push(
        makeEvent(
          "thread.turn-start-requested",
          { sequence: seq, occurredAt, aggregateId: threadId, aggregateKind: "thread" },
          {
            threadId: ThreadId.makeUnsafe(threadId),
            messageId: MessageId.makeUnsafe(raw["message_id"] ? String(raw["message_id"]) : `msg-${seq}`),
            runtimeMode: (typeof raw["runtime_mode"] === "string" ? raw["runtime_mode"] : DEFAULT_RUNTIME_MODE) as RuntimeMode,
            interactionMode: (typeof raw["interaction_mode"] === "string" ? raw["interaction_mode"] : "default") as "default" | "plan",
            createdAt: pickTimestamp(raw, "requested_at", "created_at"),
          },
        ),
      );
      return out;
    }

    default:
      // Unrecognized / server-internal tag → no store case anyway (default
      // no-op). Dropping is safe; future store cases can add a mapping here.
      return out;
  }
}

// ─── Small helpers ────────────────────────────────────────────────────

function extractError(raw: RawObj | null): string | null {
  if (!raw) return null;
  const e = raw["error"];
  if (typeof e === "string") return e;
  if (e && typeof e === "object") {
    const m = (e as RawObj)["message"];
    if (typeof m === "string") return m;
  }
  return null;
}

function brandOpt<T extends { makeUnsafe(s: string): unknown }>(
  value: unknown,
  brand: T,
): ReturnType<T["makeUnsafe"]> | undefined {
  return typeof value === "string" ? (brand.makeUnsafe(value) as ReturnType<T["makeUnsafe"]>) : undefined;
}

function buildSessionFromRaw(
  threadId: string,
  sess: unknown,
  fallbackUpdatedAt: IsoDateTime,
  ctx: PushAdaptContext,
): OrchestrationSession {
  const s = sess && typeof sess === "object" ? (sess as RawObj) : {};
  const known = ctx.session.get(threadId);
  return {
    threadId: ThreadId.makeUnsafe(threadId),
    status: (typeof s["status"] === "string" ? s["status"] : "ready") as OrchestrationSessionStatus,
    providerName: typeof s["provider_name"] === "string" ? s["provider_name"] : (known?.providerName ?? null),
    runtimeMode: (typeof s["runtime_mode"] === "string" ? s["runtime_mode"] : known?.runtimeMode ?? DEFAULT_RUNTIME_MODE) as RuntimeMode,
    activeTurnId: typeof s["active_turn_id"] === "string" ? TurnId.makeUnsafe(s["active_turn_id"]) : null,
    lastError: typeof s["last_error"] === "string" ? s["last_error"] : null,
    updatedAt: typeof s["updated_at"] === "string" ? (s["updated_at"] as IsoDateTime) : fallbackUpdatedAt,
  };
}

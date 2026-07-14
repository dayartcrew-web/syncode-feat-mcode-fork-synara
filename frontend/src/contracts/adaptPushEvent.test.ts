import { describe, expect, it } from "vitest";
import {
  adaptPushEnvelope,
  createPushAdaptContext,
  type RawPushEnvelope,
} from "./adaptPushEvent";

// Raw frames mirror the live backend wire (PascalCase tag, double-nested
// snake_case data) — captured via e2e-capture-raw-push.cjs.
const threadId = "2f5670d3-0cf0-4aaa-0000-000000000001";
const turnId = "bfae1475-d222-4bbb-0000-000000000002";
const projectId = "20ab8a03-e090-4ccc-0000-000000000003";

function env(eventType: string, aggregateId: string | null, data: unknown): RawPushEnvelope {
  return { eventType, aggregateId, data: { event_type: eventType, data } };
}

describe("adaptPushEnvelope", () => {
  it("drops snapshots and unrecognized tags", () => {
    const ctx = createPushAdaptContext();
    expect(adaptPushEnvelope({ eventType: "snapshot", aggregateId: null, data: { x: 1 } }, ctx)).toEqual([]);
    expect(adaptPushEnvelope({ eventType: "SomeUnknownThing", aggregateId: null, data: {} }, ctx)).toEqual([]);
  });

  it("skips ProjectCreated (shell snapshot owns projects), maps Updated/Deleted", () => {
    const ctx = createPushAdaptContext();
    // projectCreated is intentionally dropped (see adapter NOTE).
    const created = adaptPushEnvelope(
      env("ProjectCreated", projectId, { id: projectId, name: "Home", root_path: "/tmp/x", created_at: "t1" }),
      ctx,
    );
    expect(created).toEqual([]);

    const updated = adaptPushEnvelope(
      env("ProjectUpdated", projectId, { id: projectId, updated_at: "t2" }),
      ctx,
    );
    expect(updated[0]?.type).toBe("project.meta-updated");

    const deleted = adaptPushEnvelope(env("ProjectDeleted", projectId, { id: projectId, deleted_at: "t3" }), ctx);
    expect(deleted[0]?.type).toBe("project.deleted");
  });

  it("seeds turns map on TurnStarted + emits session-set(running)", () => {
    const ctx = createPushAdaptContext();
    const out = adaptPushEnvelope(
      env("TurnStarted", turnId, { id: turnId, thread_id: threadId, sequence: 0, user_input: "hi", created_at: "t" }),
      ctx,
    );
    expect(ctx.turns.get(turnId)).toBe(threadId);
    // Emits session-set(running) so the UI's phase becomes "running" →
    // serverAcknowledgedLocalDispatch flips true → isSendBusy clears.
    expect(out).toHaveLength(1);
    expect(out[0]!.type).toBe("thread.session-set");
    const session = (out[0]!.payload as { session: { status: string; activeTurnId: string | null } }).session;
    expect(session.status).toBe("running");
    expect(session.activeTurnId).toBe(turnId);
  });

  it("maps MessageDeltaAppended → thread.message-sent (streaming) with threadId resolved via turns map", () => {
    const ctx = createPushAdaptContext();
    adaptPushEnvelope(env("TurnStarted", turnId, { id: turnId, thread_id: threadId, sequence: 0, user_input: "hi", created_at: "t" }), ctx);
    const out = adaptPushEnvelope(
      env("MessageDeltaAppended", turnId, { id: turnId, turn_id: turnId, delta: "PON", created_at: "t" }),
      ctx,
    );
    expect(out).toHaveLength(1);
    expect(out[0]!.type).toBe("thread.message-sent");
    const p = out[0]!.payload as { threadId: string; streaming: boolean; text: string; turnId: string };
    expect(p.threadId).toBe(threadId);
    expect(p.streaming).toBe(true);
    expect(p.text).toBe("PON");
    expect(p.turnId).toBe(turnId);
  });

  it("synthesizes session-set + activity-appended from TurnCompleted (clears spinner)", () => {
    const ctx = createPushAdaptContext();
    adaptPushEnvelope(env("TurnStarted", turnId, { id: turnId, thread_id: threadId, sequence: 0, user_input: "hi", created_at: "t" }), ctx);
    adaptPushEnvelope(env("MessageDeltaAppended", turnId, { id: turnId, turn_id: turnId, delta: "PONG", created_at: "t" }), ctx);

    const out = adaptPushEnvelope(
      env("TurnCompleted", turnId, {
        id: turnId,
        assistant_output: "PONG",
        completed_at: "tc",
        duration_ms: 1234,
        usage: { input_tokens: 10, output_tokens: 2, total_tokens: 12 },
      }),
      ctx,
    );
    expect(out).toHaveLength(3);
    const session = out.find((e) => e.type === "thread.session-set")!;
    const activity = out.find((e) => e.type === "thread.activity-appended")!;
    const finalize = out.find((e) => e.type === "thread.message-sent");
    expect(session).toBeTruthy();
    expect(activity).toBeTruthy();
    expect(finalize).toBeTruthy();
    expect((session.payload as { threadId: string }).threadId).toBe(threadId);
    expect((session.payload as { session: { status: string; activeTurnId: null } }).session.status).toBe("ready");
    expect((session.payload as { session: { activeTurnId: null } }).session.activeTurnId).toBeNull();
    expect((activity.payload as { activity: { kind: string } }).activity.kind).toBe("turn.completed");
    expect((activity.payload as { threadId: string }).threadId).toBe(threadId);
    // finalize message-sent (streaming:false) clears latestTurn.state → spinner.
    const fp = finalize!.payload as { streaming: boolean; role: string; messageId: string; text: string };
    expect(fp.streaming).toBe(false);
    expect(fp.role).toBe("assistant");
    expect(fp.messageId).toBe(turnId);
    // assistant_output is carried as text so synchronous adapters (Claude)
    // that emit no MessageDeltaAppended still produce a non-empty message.
    expect(fp.text).toBe("PONG");
  });

  it("synthesizes finalize carrying assistant_output as text when no deltas arrived", () => {
    const ctx = createPushAdaptContext();
    adaptPushEnvelope(env("TurnStarted", turnId, { id: turnId, thread_id: threadId, sequence: 0, user_input: "hi", created_at: "t" }), ctx);
    const out = adaptPushEnvelope(
      env("TurnCompleted", turnId, {
        id: turnId,
        assistant_output: "pong",
        completed_at: "tc",
        duration_ms: 800,
        usage: { input_tokens: 5, output_tokens: 1, total_tokens: 6 },
      }),
      ctx,
    );
    const finalize = out.find((e) => e.type === "thread.message-sent");
    expect(finalize).toBeTruthy();
    const fp = finalize!.payload as { text: string; streaming: boolean };
    expect(fp.text).toBe("pong");
    expect(fp.streaming).toBe(false);
  });

  it("synthesizes error session from TurnFailed", () => {
    const ctx = createPushAdaptContext();
    adaptPushEnvelope(env("TurnStarted", turnId, { id: turnId, thread_id: threadId, sequence: 0, user_input: "hi", created_at: "t" }), ctx);
    const out = adaptPushEnvelope(
      env("TurnFailed", turnId, { id: turnId, error: "boom", completed_at: "tc", duration_ms: 1 }),
      ctx,
    );
    const session = out.find((e) => e.type === "thread.session-set")!;
    expect((session.payload as { session: { status: string; lastError: string | null } }).session.status).toBe("error");
    expect((session.payload as { session: { lastError: string | null } }).session.lastError).toBe("boom");
    const activity = out.find((e) => e.type === "thread.activity-appended")!;
    expect((activity.payload as { activity: { kind: string } }).activity.kind).toBe("turn.failed");
  });

  it("drops TurnCompleted when threadId cannot be resolved (missed TurnStarted)", () => {
    const ctx = createPushAdaptContext();
    const out = adaptPushEnvelope(env("TurnCompleted", turnId, { id: turnId, assistant_output: "x", completed_at: "t" }), ctx);
    expect(out).toEqual([]);
  });

  it("produces strictly increasing sequences", () => {
    const ctx = createPushAdaptContext();
    adaptPushEnvelope(env("TurnStarted", turnId, { id: turnId, thread_id: threadId, sequence: 0, user_input: "hi", created_at: "t" }), ctx);
    const a = adaptPushEnvelope(env("MessageDeltaAppended", turnId, { id: turnId, turn_id: turnId, delta: "a", created_at: "t" }), ctx);
    const b = adaptPushEnvelope(env("MessageDeltaAppended", turnId, { id: turnId, turn_id: turnId, delta: "b", created_at: "t" }), ctx);
    const seqs = [...a, ...b].map((e) => e.sequence);
    expect(seqs).toEqual([...seqs].sort((x, y) => x - y));
    expect(new Set(seqs).size).toBe(seqs.length);
  });

  it("message-sent events share stable messageId+threadId (coalesce-compatible)", () => {
    const ctx = createPushAdaptContext();
    adaptPushEnvelope(env("TurnStarted", turnId, { id: turnId, thread_id: threadId, sequence: 0, user_input: "hi", created_at: "t" }), ctx);
    const d1 = adaptPushEnvelope(env("MessageDeltaAppended", turnId, { id: turnId, turn_id: turnId, delta: "PO", created_at: "t" }), ctx);
    const d2 = adaptPushEnvelope(env("MessageDeltaAppended", turnId, { id: turnId, turn_id: turnId, delta: "NG", created_at: "t" }), ctx);
    const p1 = d1[0]!.payload as { messageId: string; threadId: string };
    const p2 = d2[0]!.payload as { messageId: string; threadId: string };
    expect(p1.messageId).toBe(p2.messageId);
    expect(p1.threadId).toBe(p2.threadId);
  });
});

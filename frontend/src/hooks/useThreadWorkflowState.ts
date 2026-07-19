/**
 * Subscribe to workflow-context-bound events for a single chat thread and
 * return the latest snapshot (or `null` while no event has arrived yet).
 *
 * Backend (chat-workflow bridge C3): on every `StartTurn` /
 * `thread.turn.start`, the server emits
 * `{ eventType: "WorkflowContextBound", data: { threadId, workflowId, phase, currentTask, totalTasks, currentTaskIndex } }`
 * on the `orchestration` push channel. The adapter (`adaptPushEvent.ts`)
 * routes it as a `thread.workflow-context-bound` OrchestrationEvent. This
 * hook subscribes via `api.orchestration.onDomainEvent`, filters by threadId,
 * and stashes the latest snapshot in component state.
 *
 * v1 wire contract: `phase` is always `"EXECUTE"` (chat-driven workflows run
 * in execute mode by default — richer phase progression is future work).
 *
 * Usage:
 *   const workflow = useThreadWorkflowState(activeThreadId);
 *   if (workflow) <WorkflowBadge workflow={workflow} />
 */
import { useEffect, useState } from "react";
import type { ThreadId } from "../contracts/ids";
import type { ThreadWorkflowContextBoundPayload } from "../contracts/tier3/orchestration";
import { ensureNativeApi } from "../nativeApi";

export type ThreadWorkflowState = {
  readonly workflowId: string;
  readonly phase: string;
  readonly currentTask: string | null;
  readonly totalTasks: number | null;
  readonly currentTaskIndex: number | null;
  readonly updatedAt: string;
};

function fromPayload(payload: ThreadWorkflowContextBoundPayload): ThreadWorkflowState {
  // `payload.updatedAt` arrives via the OrchestrationEventPayload index
  // signature as `unknown`; narrow it explicitly to a string before use.
  const rawUpdatedAt = payload.updatedAt;
  return {
    workflowId: payload.workflowId,
    phase: payload.phase,
    currentTask: payload.currentTask,
    totalTasks: payload.totalTasks,
    currentTaskIndex: payload.currentTaskIndex,
    updatedAt:
      typeof rawUpdatedAt === "string" && rawUpdatedAt.length > 0
        ? rawUpdatedAt
        : new Date().toISOString(),
  };
}

export function useThreadWorkflowState(threadId: ThreadId): ThreadWorkflowState | null {
  const [state, setState] = useState<ThreadWorkflowState | null>(null);

  useEffect(() => {
    // Reset on thread switch so a stale snapshot from the previous thread
    // doesn't briefly render on the new one.
    setState(null);
    let cancelled = false;
    const unsubscribe = ensureNativeApi().orchestration.onDomainEvent((event) => {
      if (cancelled) return;
      if (event.type !== "thread.workflow-context-bound") return;
      if (event.payload.threadId !== threadId) return;
      setState(fromPayload(event.payload));
    });
    return () => {
      cancelled = true;
      unsubscribe();
    };
  }, [threadId]);

  return state;
}

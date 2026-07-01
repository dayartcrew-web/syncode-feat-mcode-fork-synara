/**
 * useProviderStream — subscribe to provider push events via WebSocket
 *
 * Hooks into the existing WebSocket push system to listen for
 * provider events (tokens, tool calls, completions, errors)
 * and provides a reactive stream interface for UI rendering.
 */

import { useState, useEffect, useCallback, useRef } from "react";
import type { PushEvent } from "./useWebSocket";

/** Provider event types that can arrive via WebSocket push */
export type ProviderStreamEvent =
  | { type: "token"; session_id: string; token: string }
  | { type: "tool_call"; session_id: string; name: string; args: unknown }
  | { type: "tool_result"; session_id: string; name: string; result: unknown }
  | { type: "completed"; session_id: string; output: string; usage: TokenUsage }
  | { type: "error"; session_id: string; error: string }
  | { type: "status_changed"; status: string };

export interface TokenUsage {
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
}

export interface StreamState {
  /** Whether a stream is currently active */
  streaming: boolean;
  /** Accumulated output tokens */
  output: string;
  /** Current token usage if available */
  usage: TokenUsage | null;
  /** Any error that occurred */
  error: string | null;
  /** Recent events for display (last N) */
  events: ProviderStreamEvent[];
}

interface UseProviderStreamOptions {
  /** The onPush callback from useWebSocket */
  onPush: (callback: (event: PushEvent) => void) => () => void;
  /** Session ID to filter events for (null = all) */
  sessionId?: string | null;
  /** Maximum events to keep in the buffer */
  maxEvents?: number;
}

const MAX_EVENTS_DEFAULT = 100;

export function useProviderStream({
  onPush,
  sessionId = null,
  maxEvents = MAX_EVENTS_DEFAULT,
}: UseProviderStreamOptions): StreamState & {
  /** Manually reset the stream state */
  reset: () => void;
} {
  const [streaming, setStreaming] = useState(false);
  const [output, setOutput] = useState("");
  const [usage, setUsage] = useState<TokenUsage | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [events, setEvents] = useState<ProviderStreamEvent[]>([]);
  const outputRef = useRef("");

  const reset = useCallback(() => {
    setStreaming(false);
    setOutput("");
    outputRef.current = "";
    setUsage(null);
    setError(null);
    setEvents([]);
  }, []);

  useEffect(() => {
    const unsub = onPush((pushEvent) => {
      // Filter by channel
      if (pushEvent.channel !== "provider") return;

      try {
        const data = pushEvent.data as Record<string, unknown>;
        const eventSessionId = (data.session_id as string) ?? "";

        // Filter by session if specified
        if (sessionId && eventSessionId !== sessionId) return;

        let streamEvent: ProviderStreamEvent | null = null;

        switch (pushEvent.eventType) {
          case "token": {
            const token = (data.token as string) ?? "";
            streamEvent = { type: "token", session_id: eventSessionId, token };
            outputRef.current += token;
            setOutput(outputRef.current);
            setStreaming(true);
            break;
          }
          case "tool_call": {
            streamEvent = {
              type: "tool_call",
              session_id: eventSessionId,
              name: (data.name as string) ?? "unknown",
              args: data.args,
            };
            break;
          }
          case "tool_result": {
            streamEvent = {
              type: "tool_result",
              session_id: eventSessionId,
              name: (data.name as string) ?? "unknown",
              result: data.result,
            };
            break;
          }
          case "completed": {
            const completedUsage = data.usage as TokenUsage | undefined;
            streamEvent = {
              type: "completed",
              session_id: eventSessionId,
              output: (data.output as string) ?? "",
              usage: completedUsage ?? { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
            };
            setStreaming(false);
            if (completedUsage) {
              setUsage(completedUsage);
            }
            break;
          }
          case "error": {
            streamEvent = {
              type: "error",
              session_id: eventSessionId,
              error: (data.error as string) ?? "Unknown error",
            };
            setStreaming(false);
            setError((data.error as string) ?? "Unknown error");
            break;
          }
          case "status_changed": {
            streamEvent = { type: "status_changed", status: (data.status as string) ?? "unknown" };
            break;
          }
        }

        if (streamEvent) {
          setEvents((prev) => {
            const next = [...prev, streamEvent!];
            return next.length > maxEvents ? next.slice(-maxEvents) : next;
          });
        }
      } catch (err) {
        console.warn("[useProviderStream] Failed to parse push event:", err);
      }
    });

    return unsub;
  }, [onPush, sessionId, maxEvents]);

  return { streaming, output, usage, error, events, reset };
}

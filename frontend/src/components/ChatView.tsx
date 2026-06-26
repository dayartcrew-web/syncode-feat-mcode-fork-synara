/**
 * ChatView — main content area showing messages for a selected thread
 *
 * Displays turns and messages in a chat bubble layout.
 * Includes an input area for sending new user messages.
 */

import { useEffect, useState, useCallback, useMemo } from "react";
import { useProviderStream } from "../hooks/useProviderStream";

export interface TurnItem {
  id: string;
  thread_id: string;
  sequence: number;
  user_input: string;
  assistant_output: string | null;
  status: string;
  duration_ms: number | null;
  files_modified: string[];
}

export interface MessageItem {
  id: string;
  turn_id: string;
  role: string;
  content: string;
  content_type: string;
  token_count: number | null;
  created_at: string;
}

interface ChatViewProps {
  rpc: <T = unknown>(method: string, params?: Record<string, unknown>) => Promise<T>;
  threadId: string | null;
  onRefresh: () => void;
  onPush: (callback: (params: { channel: string; event: string; data: unknown }) => void) => () => void;
}

export default function ChatView({ rpc, threadId, onRefresh, onPush }: ChatViewProps) {
  const [turns, setTurns] = useState<TurnItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);

  // Provider streaming state
  const stream = useProviderStream({
    onPush,
    sessionId: null, // Listen to all provider events
    maxEvents: 50,
  });

  // Check if any event shows tool activity
  const recentToolCalls = useMemo(() =>
    stream.events.filter((e) => e.type === "tool_call").slice(-5),
    [stream.events]
  );

  const fetchTurns = useCallback(async () => {
    if (!rpc || !threadId) return;
    setLoading(true);
    try {
      const result = await rpc<{ turns: TurnItem[] }>("turn/list", { threadId });
      setTurns(result.turns ?? []);
    } catch (err) {
      console.error("[ChatView] Failed to fetch turns:", err);
      setTurns([]);
    } finally {
      setLoading(false);
    }
  }, [rpc, threadId]);

  useEffect(() => {
    fetchTurns();
  }, [fetchTurns]);

  const handleSend = async () => {
    if (!input.trim() || !rpc || !threadId) return;
    setSending(true);
    try {
      // Start a new turn
      const seq = turns.length + 1;
      await rpc("turn/start", { threadId, sequence: seq, userInput: input.trim() });
      setInput("");
      onRefresh(); // Refresh thread list (turn count)
      // For now, complete immediately with a placeholder response
      // In production, this would wait for the provider to respond
      const result = await rpc<{ turns: TurnItem[] }>("turn/list", { threadId });
      const newTurn = (result.turns ?? []).find((t: TurnItem) => t.sequence === seq);
      if (newTurn) {
        await rpc("turn/complete", {
          id: newTurn.id,
          assistantOutput: "Processing... (provider integration in Phase 2)",
          durationMs: 100,
        });
      }
      fetchTurns(); // Refresh turn list
    } catch (err) {
      console.error("[ChatView] Failed to send:", err);
    } finally {
      setSending(false);
    }
  };

  if (!threadId) {
    return (
      <div style={{
        flex: 1,
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        color: "#555",
      }}>
        <div style={{ fontSize: 48, marginBottom: 16 }}>💬</div>
        <h2 style={{ fontSize: 20, fontWeight: 300, color: "#888", marginBottom: 8 }}>
          Select a thread to begin
        </h2>
        <p style={{ fontSize: 13, color: "#555" }}>
          Create a new thread or select an existing one from the sidebar
        </p>
      </div>
    );
  }

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column" }}>
      {/* Messages area */}
      <div style={{
        flex: 1,
        overflowY: "auto",
        padding: "16px 24px",
        display: "flex",
        flexDirection: "column",
        gap: 16,
      }}>
        {loading && turns.length === 0 && (
          <div style={{ fontSize: 13, color: "#666", textAlign: "center", padding: 32 }}>
            Loading conversation...
          </div>
        )}
        {turns.length === 0 && !loading && (
          <div style={{ fontSize: 13, color: "#555", textAlign: "center", padding: 32 }}>
            No messages in this thread yet. Send a message below.
          </div>
        )}
        {turns.map((turn) => (
          <div key={turn.id} style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {/* User message */}
            <div style={{ display: "flex", justifyContent: "flex-end" }}>
              <div style={{
                maxWidth: "70%",
                padding: "10px 14px",
                borderRadius: 12,
                borderBottomLeftRadius: 4,
                background: "#0f3460",
                color: "#eee",
                fontSize: 13,
                lineHeight: 1.5,
                whiteSpace: "pre-wrap",
              }}>
                {turn.user_input}
              </div>
            </div>

            {/* Streaming indicator when active */}
            {sending && stream.streaming && (
              <div style={{ display: "flex", justifyContent: "flex-start" }}>
                <div style={{
                  maxWidth: "70%",
                  padding: "10px 14px",
                  borderRadius: 12,
                  borderBottomRightRadius: 4,
                  background: "#1a1a2e",
                  border: "1px solid #2a2a3e",
                  color: "#ddd",
                  fontSize: 13,
                  lineHeight: 1.5,
                  whiteSpace: "pre-wrap",
                }}>
                  {stream.output || "⏳ Processing..."}
                  <span style={{ animation: "blink 1s infinite", color: "#e94560" }}>▌</span>
                </div>
              </div>
            )}

            {/* Tool calls in progress */}
            {recentToolCalls.length > 0 && sending && (
              <div style={{ fontSize: 11, color: "#888", padding: "2px 0" }}>
                🔧 Tools: {recentToolCalls.map((t) => t.name).join(", ")}
              </div>
            )}

            {/* Assistant response */}
            {turn.assistant_output && (
              <div style={{ display: "flex", justifyContent: "flex-start" }}>
                <div style={{
                  maxWidth: "70%",
                  padding: "10px 14px",
                  borderRadius: 12,
                  borderBottomRightRadius: 4,
                  background: "#1a1a2e",
                  border: "1px solid #2a2a3e",
                  color: "#ddd",
                  fontSize: 13,
                  lineHeight: 1.5,
                  whiteSpace: "pre-wrap",
                }}>
                  {turn.assistant_output}
                  {turn.files_modified.length > 0 && (
                    <div style={{ marginTop: 8, fontSize: 11, color: "#888" }}>
                      📁 Modified: {turn.files_modified.join(", ")}
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Turn metadata */}
            <div style={{ fontSize: 10, color: "#444", textAlign: "center" }}>
              Turn #{turn.sequence}
              {turn.duration_ms != null && ` · ${turn.duration_ms}ms`}
              {turn.status === "running" && " · ⏳ Processing..."}
            </div>
          </div>
        ))}
      </div>

      {/* Input area */}
      <div style={{
        borderTop: "1px solid #2a2a3e",
        padding: "12px 24px",
        display: "flex",
        gap: 8,
      }}>
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleSend()}
          placeholder="Type a message..."
          disabled={sending}
          style={{
            flex: 1,
            padding: "10px 14px",
            borderRadius: 8,
            border: "1px solid #2a2a3e",
            background: "#16213e",
            color: "#eee",
            fontSize: 13,
            outline: "none",
          }}
        />
        <button
          onClick={handleSend}
          disabled={sending || !input.trim()}
          style={{
            padding: "10px 20px",
            borderRadius: 8,
            border: "none",
            background: input.trim() ? "#e94560" : "#333",
            color: "#fff",
            cursor: input.trim() ? "pointer" : "not-allowed",
            fontSize: 13,
          }}>
          {sending ? "Sending..." : "Send"}
        </button>
      </div>
    </div>
  );
}

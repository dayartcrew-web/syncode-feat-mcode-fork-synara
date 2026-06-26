import { useState, useEffect, useRef, useCallback } from "react";

// Types matching Rust Tauri command results
interface TerminalSession {
  sessionId: string;
  pid: number;
  alive: boolean;
  createdAt: string;
  cols: number;
  rows: number;
}

interface OutputChunk {
  seq: number;
  data: string;
  timestamp: string;
}

interface TerminalOutputResult {
  sessionId: string;
  chunks: OutputChunk[];
  hasMore: boolean;
}

interface TerminalViewProps {
  rpc: ((method: string, params?: Record<string, unknown>) => Promise<unknown>) | null;
  visible: boolean;
}

export default function TerminalView({ rpc, visible }: TerminalViewProps) {
  const [sessions, setSessions] = useState<TerminalSession[]>([]);
  const [activeSessionId, setActiveSessionId] = useState<string | null>(null);
  const [output, setOutput] = useState<string>("");
  const [input, setInput] = useState("");
  const [lastSeq, setLastSeq] = useState<number>(0);
  const [loading, setLoading] = useState(false);
  const [shellCmd, setShellCmd] = useState("bash");
  const outputRef = useRef<HTMLPreElement>(null);
  const pollRef = useRef<number | null>(null);

  // Scroll to bottom on new output
  useEffect(() => {
    if (outputRef.current) {
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
    }
  }, [output]);

  // Poll for output when session is active
  useEffect(() => {
    if (!activeSessionId || !rpc || !visible) {
      if (pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
      return;
    }

    const poll = async () => {
      try {
        const result = (await rpc("terminal_read_output", {
          sessionId: activeSessionId,
          fromSeq: lastSeq,
        })) as TerminalOutputResult;
        if (result.chunks.length > 0) {
          const newOutput = result.chunks.map((c) => c.data).join("");
          const maxSeq = Math.max(...result.chunks.map((c) => c.seq));
          setOutput((prev) => prev + newOutput);
          setLastSeq(maxSeq + 1);
          // Ack the chunks
          await rpc("terminal_ack", {
            sessionId: activeSessionId,
            seq: maxSeq,
          });
        }
      } catch (err) {
        // Session might have ended, stop polling
        console.warn("[Terminal] Poll error:", err);
      }
    };

    pollRef.current = window.setInterval(poll, 200);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [activeSessionId, rpc, lastSeq, visible]);

  // List sessions
  const refreshSessions = useCallback(async () => {
    if (!rpc) return;
    try {
      const result = (await rpc("terminal_list_sessions")) as { sessions?: TerminalSession[] };
      const list = result.sessions ?? [];
      setSessions(Array.isArray(list) ? list : []);
    } catch (err) {
      console.warn("[Terminal] List sessions error:", err);
    }
  }, [rpc]);

  // Create new session
  const createSession = useCallback(async () => {
    if (!rpc) return;
    setLoading(true);
    try {
      const result = (await rpc("terminal_create_session", {
        command: shellCmd,
        args: [],
        cols: 80,
        rows: 24,
      })) as TerminalSession;
      setActiveSessionId(result.sessionId);
      setOutput("");
      setLastSeq(0);
      await refreshSessions();
    } catch (err) {
      console.error("[Terminal] Create session error:", err);
    } finally {
      setLoading(false);
    }
  }, [rpc, shellCmd, refreshSessions]);

  // Destroy session
  const destroySession = useCallback(
    async (sessionId: string) => {
      if (!rpc) return;
      try {
        await rpc("terminal_destroy_session", { sessionId });
        if (sessionId === activeSessionId) {
          setActiveSessionId(null);
          setOutput("");
        }
        await refreshSessions();
      } catch (err) {
        console.error("[Terminal] Destroy session error:", err);
      }
    },
    [rpc, activeSessionId, refreshSessions]
  );

  // Write input
  const handleInput = useCallback(
    async (data: string) => {
      if (!rpc || !activeSessionId) return;
      try {
        await rpc("terminal_write", {
          sessionId: activeSessionId,
          data,
        });
      } catch (err) {
        console.error("[Terminal] Write error:", err);
      }
    },
    [rpc, activeSessionId]
  );

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleInput(input + "\r");
      setInput("");
    }
  };

  if (!visible) return null;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        background: "#0d1117",
        color: "#c9d1d9",
        fontFamily: "'Cascadia Code', 'Fira Code', 'JetBrains Mono', Consolas, monospace",
        fontSize: 13,
      }}
    >
      {/* Toolbar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "6px 12px",
          background: "#161b22",
          borderBottom: "1px solid #30363d",
        }}
      >
        <span style={{ fontSize: 11, fontWeight: 600, color: "#8b949e" }}>
          TERMINAL
        </span>
        <select
          value={shellCmd}
          onChange={(e) => setShellCmd(e.target.value)}
          style={{
            background: "#0d1117",
            border: "1px solid #30363d",
            borderRadius: 4,
            color: "#c9d1d9",
            fontSize: 11,
            padding: "2px 6px",
          }}
        >
          <option value="bash">bash</option>
          <option value="sh">sh</option>
          <option value="powershell">powershell</option>
          <option value="cmd">cmd</option>
        </select>
        <button
          onClick={createSession}
          disabled={loading || !rpc}
          style={{
            padding: "3px 10px",
            borderRadius: 4,
            border: "1px solid #30363d",
            background: loading ? "#21262d" : "#238636",
            color: "#fff",
            cursor: loading ? "wait" : "pointer",
            fontSize: 11,
          }}
        >
          {loading ? "Starting..." : "+ New Session"}
        </button>

        {sessions.length > 0 && (
          <select
            value={activeSessionId ?? ""}
            onChange={(e) => {
              const sid = e.target.value;
              setActiveSessionId(sid || null);
              setOutput("");
              setLastSeq(0);
            }}
            style={{
              background: "#0d1117",
              border: "1px solid #30363d",
              borderRadius: 4,
              color: "#c9d1d9",
              fontSize: 11,
              padding: "2px 6px",
              marginLeft: "auto",
            }}
          >
            <option value="">Select session...</option>
            {sessions.map((s) => (
              <option key={s.sessionId} value={s.sessionId}>
                {s.sessionId.slice(0, 16)} (PID: {s.pid}) {s.alive ? "🟢" : "🔴"}
              </option>
            ))}
          </select>
        )}

        {activeSessionId && (
          <button
            onClick={() => destroySession(activeSessionId)}
            style={{
              padding: "3px 8px",
              borderRadius: 4,
              border: "1px solid #30363d",
              background: "#da3633",
              color: "#fff",
              cursor: "pointer",
              fontSize: 11,
            }}
          >
            ✕
          </button>
        )}
      </div>

      {/* Terminal output */}
      <div style={{ flex: 1, overflow: "hidden", position: "relative" }}>
        {activeSessionId ? (
          <pre
            ref={outputRef}
            style={{
              margin: 0,
              padding: "8px 12px",
              overflow: "auto",
              height: "100%",
              whiteSpace: "pre-wrap",
              wordBreak: "break-all",
              lineHeight: 1.5,
            }}
          >
            {output || <span style={{ color: "#484f58" }}>Waiting for output...</span>}
            <span style={{ animation: "blink 1s step-end infinite" }}>▋</span>
          </pre>
        ) : (
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              height: "100%",
              color: "#484f58",
              fontSize: 13,
            }}
          >
            No active terminal session. Click "+ New Session" to start.
          </div>
        )}
      </div>

      {/* Input area */}
      {activeSessionId && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            padding: "6px 12px",
            background: "#161b22",
            borderTop: "1px solid #30363d",
            gap: 8,
          }}
        >
          <span style={{ color: "#238636", fontSize: 12 }}>$</span>
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            rows={1}
            placeholder="Type command and press Enter..."
            style={{
              flex: 1,
              background: "transparent",
              border: "none",
              outline: "none",
              color: "#c9d1d9",
              fontFamily: "inherit",
              fontSize: 13,
              resize: "none",
            }}
          />
        </div>
      )}

      <style>{`
        @keyframes blink {
          0%, 100% { opacity: 1; }
          50% { opacity: 0; }
        }
      `}</style>
    </div>
  );
}

/**
 * ThreadList — sidebar component showing project threads
 *
 * Fetches threads via RPC and displays them in a list.
 * Highlights the selected thread.
 */

import { useEffect, useState, useCallback } from "react";

export interface ThreadItem {
  id: string;
  projectId: string;
  providerId: string;
  model: string;
  status: string;
  title: string | null;
  turnCount: number;
  createdAt: string;
}

interface ThreadListProps {
  rpc: <T = unknown>(method: string, params?: Record<string, unknown>) => Promise<T>;
  projectId: string | null;
  selectedThreadId: string | null;
  onSelectThread: (threadId: string) => void;
  refreshTrigger: number;
}

const STATUS_COLORS: Record<string, string> = {
  active: "#4caf50",
  paused: "#ff9800",
  completed: "#2196f3",
  error: "#f44336",
  cancelled: "#666",
};

export default function ThreadList({
  rpc,
  projectId,
  selectedThreadId,
  onSelectThread,
  refreshTrigger,
}: ThreadListProps) {
  const [threads, setThreads] = useState<ThreadItem[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchThreads = useCallback(async () => {
    if (!rpc) return;
    setLoading(true);
    try {
      const result = await rpc<{ threads: ThreadItem[] }>("thread/list", projectId ? { projectId } : {});
      setThreads(result.threads ?? []);
    } catch (err) {
      console.error("[ThreadList] Failed to fetch threads:", err);
      setThreads([]);
    } finally {
      setLoading(false);
    }
  }, [rpc, projectId]);

  useEffect(() => {
    fetchThreads();
  }, [fetchThreads, refreshTrigger]);

  const statusColor = (status: string) => STATUS_COLORS[status] ?? "#666";

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
      <div style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", color: "#aaa", padding: "4px 0" }}>
        Threads {threads.length > 0 && `(${threads.length})`}
      </div>
      {loading && threads.length === 0 && (
        <div style={{ fontSize: 12, color: "#666", padding: "8px 0" }}>Loading...</div>
      )}
      {threads.length === 0 && !loading && (
        <div style={{ fontSize: 12, color: "#555", padding: "8px 0" }}>No threads yet</div>
      )}
      {threads.map((t) => (
        <div
          key={t.id}
          onClick={() => onSelectThread(t.id)}
          style={{
            padding: "8px 10px",
            borderRadius: 6,
            cursor: "pointer",
            display: "flex",
            flexDirection: "column",
            gap: 2,
            background: t.id === selectedThreadId ? "#0f3460" : "transparent",
            borderLeft: t.id === selectedThreadId ? "2px solid #e94560" : "2px solid transparent",
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span style={{ fontSize: 13, color: "#ddd", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {t.title || `Thread ${t.id.slice(0, 8)}...`}
            </span>
            <span style={{ width: 7, height: 7, borderRadius: "50%", background: statusColor(t.status), flexShrink: 0 }} />
          </div>
          <div style={{ fontSize: 10, color: "#666", display: "flex", gap: 8 }}>
            <span>{t.model}</span>
            <span>{t.turnCount} turn{t.turnCount !== 1 ? "s" : ""}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

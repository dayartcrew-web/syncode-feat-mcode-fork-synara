/**
 * GitPanel — git status and branch information panel
 *
 * Displays current git branch, modified files, and quick actions
 * (commit, stage, checkout). Integrates with Tauri invoke or
 * WebSocket RPC for git operations.
 */

import { useState, useEffect, useCallback } from "react";

interface GitFileInfo {
  path: string;
  index_status: string;
  working_tree_status: string;
}

interface GitStatusData {
  branch: string | null;
  head_detached: boolean;
  files: GitFileInfo[];
  ahead: number;
  behind: number;
}

interface GitLogEntry {
  hash: string;
  short_hash: string;
  author: string;
  message: string;
  timestamp: string;
  refs?: string[];
}

interface GitPanelProps {
  /** Working directory to show git status for */
  workDir: string;
  /** RPC function (from useWebSocket) */
  rpc: <T = unknown>(method: string, params?: Record<string, unknown>) => Promise<T>;
  /** Callback when changes are committed */
  onCommit?: () => void;
  /** Whether to show the panel */
  visible?: boolean;
  /** Custom style */
  style?: React.CSSProperties;
}

const STATUS_COLORS: Record<string, string> = {
  M: "#f0ad4e",
  A: "#5cb85c",
  D: "#d9534f",
  R: "#5bc0de",
  "?": "#777",
  "!": "#555",
};

export default function GitPanel({
  workDir,
  rpc,
  onCommit,
  visible = true,
  style,
}: GitPanelProps) {
  const [status, setStatus] = useState<GitStatusData | null>(null);
  const [log, setLog] = useState<GitLogEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [commitMsg, setCommitMsg] = useState("");
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    if (!rpc || !visible) return;
    setLoading(true);
    try {
      const [statusResult, logResult] = await Promise.all([
        rpc<GitStatusData>("git/status", { path: workDir }),
        rpc<{ entries: GitLogEntry[] }>("git/log", { path: workDir, maxCount: 10 }),
      ]);
      setStatus(statusResult);
      setLog(logResult.entries ?? []);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Git status failed");
    } finally {
      setLoading(false);
    }
  }, [rpc, workDir, visible]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const handleCommit = async () => {
    if (!commitMsg.trim() || !rpc) return;
    try {
      await rpc("git/commit", { path: workDir, message: commitMsg.trim() });
      setCommitMsg("");
      onCommit?.();
      refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Commit failed");
    }
  };

  if (!visible) return null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8, ...style }}>
      {/* Branch info */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12 }}>
        <span style={{ color: "#e94560", fontWeight: 600 }}>⎇</span>
        <span style={{ color: "#eee" }}>
          {status?.branch ?? "—"}
        </span>
        {status && (status.ahead > 0 || status.behind > 0) && (
          <span style={{ fontSize: 10, color: "#888" }}>
            {status.ahead > 0 && `↑${status.ahead}`}
            {status.behind > 0 && ` ↓${status.behind}`}
          </span>
        )}
        {status?.head_detached && (
          <span style={{ fontSize: 10, color: "#d9534f" }}>DETACHED</span>
        )}
      </div>

      {/* Error */}
      {error && (
        <div style={{ fontSize: 11, color: "#d9534f", padding: "4px 8px", background: "#1a0000", borderRadius: 4 }}>
          {error}
        </div>
      )}

      {/* File list */}
      {loading && !status && (
        <div style={{ fontSize: 11, color: "#666" }}>Loading git status...</div>
      )}
      {status && status.files.length === 0 && (
        <div style={{ fontSize: 11, color: "#555" }}>Working tree clean ✓</div>
      )}
      {status?.files.map((file) => (
        <div
          key={file.path}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 6,
            fontSize: 11,
            padding: "2px 0",
          }}
        >
          <span
            style={{
              width: 14,
              height: 14,
              borderRadius: 3,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              fontSize: 9,
              fontWeight: 700,
              background: STATUS_COLORS[file.working_tree_status] || "#555",
              color: "#fff",
            }}
          >
            {file.working_tree_status === "?" ? "+" : file.working_tree_status || "·"}
          </span>
          <span style={{ color: "#ccc", fontFamily: "monospace", fontSize: 11 }}>
            {file.path}
          </span>
        </div>
      ))}

      {/* Commit input */}
      {status && status.files.length > 0 && (
        <div style={{ display: "flex", gap: 6 }}>
          <input
            type="text"
            value={commitMsg}
            onChange={(e) => setCommitMsg(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleCommit()}
            placeholder="Commit message..."
            style={{
              flex: 1,
              padding: "6px 10px",
              borderRadius: 6,
              border: "1px solid #0f3460",
              background: "#0d1b36",
              color: "#eee",
              fontSize: 11,
              outline: "none",
            }}
          />
          <button
            onClick={handleCommit}
            disabled={!commitMsg.trim()}
            style={{
              padding: "6px 12px",
              borderRadius: 6,
              border: "none",
              background: commitMsg.trim() ? "#5cb85c" : "#333",
              color: "#fff",
              cursor: commitMsg.trim() ? "pointer" : "not-allowed",
              fontSize: 11,
              fontWeight: 500,
            }}
          >
            Commit
          </button>
        </div>
      )}

      {/* Recent commits */}
      {log.length > 0 && (
        <div style={{ borderTop: "1px solid #0f3460", paddingTop: 6 }}>
          <div style={{ fontSize: 10, color: "#555", marginBottom: 4 }}>Recent commits</div>
          {log.slice(0, 5).map((entry) => (
            <div
              key={entry.short_hash}
              style={{ fontSize: 10, padding: "2px 0", display: "flex", gap: 6 }}
            >
              <span style={{ color: "#e94560", fontFamily: "monospace" }}>
                {entry.short_hash.slice(0, 7)}
              </span>
              <span style={{ color: "#aaa", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {entry.message}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

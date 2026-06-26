import { useState, useEffect, useCallback } from "react";
import { useWebSocket } from "./hooks/useWebSocket";
import ThreadList from "./components/ThreadList";
import ChatView from "./components/ChatView";
import ProviderSwitcher from "./components/ProviderSwitcher";
import TerminalView from "./components/TerminalView";

const WS_URL = "ws://127.0.0.1:8080/ws";

export default function App() {
  const { connected, rpc, onPush } = useWebSocket(WS_URL);
  const [selectedThreadId, setSelectedThreadId] = useState<string | null>(null);
  const [projectId, setProjectId] = useState<string | null>(null);
  const [refreshTrigger, setRefreshTrigger] = useState(0);
  const [, setShowNewThread] = useState(false);
  const [selectedProvider, setSelectedProvider] = useState("claude");
  const [selectedModel, setSelectedModel] = useState("claude-sonnet-4-20250514");
  const [showTerminal, setShowTerminal] = useState(false);

  // Create initial project if none exists
  useEffect(() => {
    if (!connected || !rpc) return;
    (async () => {
      try {
        const result = await rpc<{ projects: Array<{ id: string }> }>("project/list");
        const projects = result.projects ?? [];
        if (projects.length > 0 && projects[0]) {
          setProjectId(projects[0].id);
        } else {
          // Create a default project
          const created = await rpc<{ id: string }>("project/create", {
            name: "Default Project",
            rootPath: "/tmp/syncode",
          });
          setProjectId(created.id);
        }
      } catch (err) {
        console.warn("[App] Project init failed (expected if WS not running):", err);
      }
    })();
  }, [connected, rpc]);

  // Listen for push events to auto-refresh
  useEffect(() => {
    const unsub = onPush((params) => {
      console.log("[App] Push event:", params.channel, params.event);
      if (params.channel === "orchestration") {
        setRefreshTrigger((n) => n + 1);
      }
    });
    return unsub;
  }, [onPush]);

  const handleCreateThread = useCallback(async () => {
    if (!rpc || !projectId) return;
    try {
      const result = await rpc<{ id: string }>("thread/create", {
        projectId,
        providerId: selectedProvider,
        model: selectedModel,
      });
      setSelectedThreadId(result.id);
      setRefreshTrigger((n) => n + 1);
      setShowNewThread(false);
    } catch (err) {
      console.error("[App] Failed to create thread:", err);
    }
  }, [rpc, projectId, selectedProvider, selectedModel]);

  return (
    <div style={{ display: "flex", height: "100vh", background: "#1a1a2e", color: "#eee", fontFamily: "system-ui, -apple-system, sans-serif" }}>
      {/* Sidebar */}
      <aside style={{
        width: 280,
        background: "#16213e",
        borderRight: "1px solid #0f3460",
        padding: 16,
        display: "flex",
        flexDirection: "column",
        gap: 8,
      }}>
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
          <h1 style={{ fontSize: 18, fontWeight: 700, color: "#e94560", margin: 0 }}>
            ⚡ Syncode
          </h1>
          <span style={{
            marginLeft: "auto",
            width: 8, height: 8, borderRadius: "50%",
            background: connected ? "#4caf50" : "#f44336",
          }} />
          <span style={{ fontSize: 10, color: "#666" }}>
            {connected ? "connected" : "offline"}
          </span>
        </div>

        <div style={{ fontSize: 11, color: "#555" }}>
          v0.1.0 · Phase 1 Core Orchestration
        </div>

        <hr style={{ border: "none", borderTop: "1px solid #0f3460", margin: "8px 0" }} />

        {/* Provider switcher */}
        <ProviderSwitcher
          value={selectedProvider}
          model={selectedModel}
          onChange={(providerId, model) => {
            setSelectedProvider(providerId);
            setSelectedModel(model);
          }}
          disabled={!connected}
        />

        {/* Thread list */}
        <div style={{ flex: 1, overflowY: "auto" }}>
          <ThreadList
            rpc={rpc}
            projectId={projectId}
            selectedThreadId={selectedThreadId}
            onSelectThread={setSelectedThreadId}
            refreshTrigger={refreshTrigger}
          />
        </div>

        {/* New Thread button */}
        <button
          onClick={handleCreateThread}
          disabled={!connected}
          style={{
            padding: "10px 16px",
            borderRadius: 8,
            border: "none",
            background: connected ? "#e94560" : "#333",
            color: "#fff",
            cursor: connected ? "pointer" : "not-allowed",
            fontSize: 13,
            fontWeight: 500,
            marginTop: 8,
          }}
        >
          + New Thread
        </button>
      </aside>

      {/* Main content */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <ChatView
          rpc={rpc}
          threadId={selectedThreadId}
          onRefresh={() => setRefreshTrigger((n) => n + 1)}
          onPush={onPush}
        />
        {/* Terminal toggle */}
        <button
          onClick={() => setShowTerminal((v) => !v)}
          style={{
            padding: "4px 12px",
            border: "none",
            borderTop: "1px solid #0f3460",
            background: showTerminal ? "#0f3460" : "#16213e",
            color: showTerminal ? "#e94560" : "#666",
            cursor: "pointer",
            fontSize: 11,
            fontWeight: 600,
          }}
        >
          {showTerminal ? "▾ HIDE TERMINAL" : "▸ SHOW TERMINAL"}
        </button>
        {showTerminal && (
          <div style={{ flex: 1, minHeight: 200, maxHeight: 400 }}>
            <TerminalView rpc={rpc} visible={showTerminal} />
          </div>
        )}
      </div>
    </div>
  );
}

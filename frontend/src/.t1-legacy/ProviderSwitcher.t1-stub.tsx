/**
 * ProviderSwitcher — dropdown for selecting AI provider and model
 *
 * Displays a dropdown with available providers (Codex, Claude, etc.)
 * and their models. Used when creating threads to choose which
 * provider to route requests to.
 */

import { useState } from "react";

export interface ProviderInfo {
  id: string;
  name: string;
  available: boolean;
  configured: boolean;
  models: string[];
}

interface ProviderSwitcherProps {
  /** Currently selected provider ID */
  value: string;
  /** Currently selected model */
  model: string;
  /** Callback when provider/model changes */
  onChange: (providerId: string, model: string) => void;
  /** Whether the selector is disabled */
  disabled?: boolean;
  /** Custom style override */
  style?: React.CSSProperties;
}

/** Default provider list — in production this comes from provider/status RPC */
const DEFAULT_PROVIDERS: ProviderInfo[] = [
  {
    id: "claude",
    name: "Anthropic Claude",
    available: true,
    configured: false,
    models: [
      "claude-sonnet-4-20250514",
      "claude-3-5-sonnet-20241022",
      "claude-3-5-haiku-20241022",
      "claude-3-opus-20240229",
    ],
  },
  {
    id: "codex",
    name: "OpenAI Codex",
    available: true,
    configured: true,
    models: [
      "o4-mini",
      "o3",
      "o3-mini",
      "gpt-4.1",
      "gpt-4.1-mini",
      "gpt-4.1-nano",
    ],
  },
];

export default function ProviderSwitcher({
  value,
  model,
  onChange,
  disabled = false,
  style,
}: ProviderSwitcherProps) {
  const [providers] = useState<ProviderInfo[]>(DEFAULT_PROVIDERS);
  const [open, setOpen] = useState(false);

  const selectedProvider = providers.find((p) => p.id === value);
  const selectedModel = model || (selectedProvider?.models[0] ?? "");

  const handleProviderSelect = (providerId: string) => {
    const provider = providers.find((p) => p.id === providerId);
    if (provider) {
      onChange(providerId, provider.models[0] ?? "");
    }
    setOpen(false);
  };

  const handleModelSelect = (providerId: string, newModel: string) => {
    onChange(providerId, newModel);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6, ...style }}>
      {/* Provider selector */}
      <div style={{ position: "relative" }}>
        <button
          onClick={() => !disabled && setOpen(!open)}
          disabled={disabled}
          style={{
            width: "100%",
            padding: "8px 12px",
            borderRadius: 6,
            border: "1px solid #0f3460",
            background: disabled ? "#111" : "#16213e",
            color: "#eee",
            fontSize: 12,
            cursor: disabled ? "not-allowed" : "pointer",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            textAlign: "left",
          }}
        >
          <span>
            {selectedProvider ? (
              <span>
                <span style={{ color: "#e94560" }}>●</span>{" "}
                {selectedProvider.name}
                {!selectedProvider.configured && (
                  <span style={{ color: "#666", fontSize: 10, marginLeft: 4 }}>
                    (not configured)
                  </span>
                )}
              </span>
            ) : (
              <span style={{ color: "#666" }}>Select provider...</span>
            )}
          </span>
          <span style={{ fontSize: 10, color: "#555" }}>▾</span>
        </button>

        {open && !disabled && (
          <div
            style={{
              position: "absolute",
              top: "100%",
              left: 0,
              right: 0,
              zIndex: 100,
              background: "#16213e",
              border: "1px solid #0f3460",
              borderRadius: 6,
              marginTop: 4,
              overflow: "hidden",
              boxShadow: "0 4px 12px rgba(0,0,0,0.3)",
            }}
          >
            {providers.map((provider) => (
              <button
                key={provider.id}
                onClick={() => handleProviderSelect(provider.id)}
                style={{
                  width: "100%",
                  padding: "8px 12px",
                  border: "none",
                  background:
                    provider.id === value ? "#0f3460" : "transparent",
                  color: provider.available ? "#eee" : "#555",
                  fontSize: 12,
                  cursor: provider.available ? "pointer" : "not-allowed",
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                  textAlign: "left",
                }}
              >
                <span
                  style={{
                    color: provider.available
                      ? provider.id === value
                        ? "#e94560"
                        : "#4caf50"
                      : "#f44336",
                  }}
                >
                  ●
                </span>
                <span>{provider.name}</span>
                {!provider.configured && (
                  <span style={{ color: "#666", fontSize: 10 }}>⚡</span>
                )}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Model selector (shown when a provider is selected with multiple models) */}
      {selectedProvider && selectedProvider.models.length > 1 && (
        <select
          value={selectedModel}
          onChange={(e) =>
            handleModelSelect(selectedProvider.id, e.target.value)
          }
          disabled={disabled}
          style={{
            width: "100%",
            padding: "6px 10px",
            borderRadius: 6,
            border: "1px solid #0f3460",
            background: disabled ? "#111" : "#0d1b36",
            color: "#ccc",
            fontSize: 11,
            cursor: disabled ? "not-allowed" : "pointer",
            outline: "none",
          }}
        >
          {selectedProvider.models.map((m) => (
            <option key={m} value={m}>
              {m}
            </option>
          ))}
        </select>
      )}

      {/* Click-away close */}
      {open && (
        <div
          style={{
            position: "fixed",
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            zIndex: 99,
          }}
          onClick={() => setOpen(false)}
        />
      )}
    </div>
  );
}

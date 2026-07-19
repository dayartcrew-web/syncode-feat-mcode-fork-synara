// FILE: McpSettingsPanel.tsx
// Purpose: Settings → MCP Servers panel. Lists every MCP server aggregated
// from the four external discovery sources plus syncode's own store at
// ~/.syncode/mcp.json. Discovered entries are toggle-only; syncode-owned
// entries support full add/edit/delete + connection test.
// Layer: Web settings UI

import type { ServerSettings } from "@t3tools/contracts";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Button } from "~/components/ui/button";
import { Switch } from "~/components/ui/switch";
import { SettingsRow, SettingsSection } from "~/components/settings/SettingsPanelPrimitives";
import { McpServersIcon } from "~/lib/icons";
import { ensureNativeApi } from "~/nativeApi";
import {
  mcpCatalogQueryOptions,
  providerDiscoveryQueryKeys,
} from "~/lib/providerDiscoveryReactQuery";
import { serverQueryKeys, serverSettingsQueryOptions } from "~/lib/serverReactQuery";
import {
  buildMcpSections,
  mcpDisabledKey,
  mcpScopeLabel,
  patchForToggle,
} from "./mcpSettingsModel";
import type { McpServerDescriptor } from "@t3tools/contracts";
import { McpServerEditor } from "./McpServerEditor";

interface ServerRowProps {
  readonly server: McpServerDescriptor;
  readonly enabled: boolean;
  readonly onToggle: (nextEnabled: boolean) => void;
  readonly onEdit?: (() => void) | undefined;
  readonly onDelete?: (() => void) | undefined;
  readonly onTest?: (() => void) | undefined;
  readonly testing?: boolean;
  readonly testStatus?: "reachable" | "unreachable" | null;
  readonly testError?: string | null;
}

function ServerRow({
  server,
  enabled,
  onToggle,
  onEdit,
  onDelete,
  onTest,
  testing,
  testStatus,
  testError,
}: ServerRowProps) {
  const scopeLabel = mcpScopeLabel(server.scope);
  const transportLabel = server.transport.toUpperCase();
  const commandLine =
    server.transport === "stdio"
      ? [server.command, ...(server.args ?? [])].filter(Boolean).join(" ")
      : (server.url ?? "");
  const envNames = (server.env ?? []).map((entry) => entry.name).join(", ");
  return (
    <SettingsRow
      title={
        <span className="inline-flex min-w-0 items-center gap-1.5">
          <McpServersIcon
            aria-hidden="true"
            className="size-3.5 shrink-0 text-muted-foreground"
          />
          <span className="truncate">{server.name}</span>
        </span>
      }
      description={commandLine || "(no command/url configured)"}
      status={
        <span className="flex min-w-0 flex-col gap-1">
          <span className="flex min-w-0 items-center gap-1.5 text-[11px] text-muted-foreground">
            <span className="rounded bg-foreground/5 px-1.5 py-0.5">{scopeLabel}</span>
            <span className="rounded bg-foreground/5 px-1.5 py-0.5">{transportLabel}</span>
            {envNames ? <span className="truncate">env: {envNames}</span> : null}
          </span>
          <code className="truncate text-[11px] text-muted-foreground" title={server.sourcePath}>
            {server.sourcePath}
          </code>
          {testStatus ? (
            <span
              className={
                testStatus === "reachable"
                  ? "text-[11px] text-emerald-600 dark:text-emerald-400"
                  : "text-[11px] text-destructive"
              }
            >
              {testStatus === "reachable" ? "Reachable" : `Unreachable${testError ? `: ${testError}` : ""}`}
            </span>
          ) : null}
        </span>
      }
      control={
        <div className="flex items-center gap-2">
          {server.editable && onTest ? (
            <Button
              variant="ghost"
              size="sm"
              onClick={onTest}
              disabled={testing}
              aria-label={`Test connection for ${server.name}`}
            >
              {testing ? "Testing…" : "Test"}
            </Button>
          ) : null}
          {server.editable && onEdit ? (
            <Button variant="ghost" size="sm" onClick={onEdit} aria-label={`Edit ${server.name}`}>
              Edit
            </Button>
          ) : null}
          {server.editable && onDelete ? (
            <Button
              variant="ghost"
              size="sm"
              onClick={onDelete}
              aria-label={`Delete ${server.name}`}
            >
              Delete
            </Button>
          ) : null}
          <Switch
            checked={enabled}
            onCheckedChange={(checked) => onToggle(Boolean(checked))}
            aria-label={`Enable the ${server.name} MCP server`}
          />
        </div>
      }
    />
  );
}

export function McpSettingsPanel() {
  const queryClient = useQueryClient();
  const catalogQuery = useQuery(mcpCatalogQueryOptions());
  const serverSettingsQuery = useQuery(serverSettingsQueryOptions());

  const [editorOpen, setEditorOpen] = useState(false);
  const [editing, setEditing] = useState<McpServerDescriptor | null>(null);
  const [testingName, setTestingName] = useState<string | null>(null);
  const [testResult, setTestResult] = useState<{
    readonly name: string;
    readonly status: "reachable" | "unreachable";
    readonly error?: string;
  } | null>(null);

  const disabledList = serverSettingsQuery.data?.mcp?.disabled ?? [];
  const disabledSet = useMemo(() => new Set(disabledList.map(mcpDisabledKey)), [disabledList]);

  const sections = useMemo(
    () => buildMcpSections(catalogQuery.data?.servers ?? []),
    [catalogQuery.data?.servers],
  );

  const syncodeMcpPath = catalogQuery.data?.syncodeMcpPath;

  const setEnabled = (serverName: string, nextEnabled: boolean) => {
    const latestSettings = queryClient.getQueryData<ServerSettings>(serverQueryKeys.settings());
    const currentDisabled = latestSettings?.mcp?.disabled ?? [...disabledList];
    const patch = patchForToggle(currentDisabled, serverName, nextEnabled);
    if (latestSettings) {
      queryClient.setQueryData(serverQueryKeys.settings(), {
        ...latestSettings,
        mcp: { disabled: patch.mcp?.disabled ?? currentDisabled },
      });
    }
    void ensureNativeApi()
      .server.updateSettings(patch)
      .then((nextSettings) => {
        queryClient.setQueryData(serverQueryKeys.settings(), nextSettings);
        // Provider sessions need to see the new disabled list on next session/new.
        void queryClient.invalidateQueries({ queryKey: providerDiscoveryQueryKeys.all });
      })
      .catch(() => {
        void queryClient.invalidateQueries({ queryKey: serverQueryKeys.settings() });
      });
  };

  const deleteMutation = useMutation({
    mutationFn: async (name: string) => ensureNativeApi().mcp.delete({ name }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: providerDiscoveryQueryKeys.all });
    },
  });

  const testMutation = useMutation({
    mutationFn: async (server: McpServerDescriptor) => {
      setTestingName(server.name);
      setTestResult(null);
      try {
        const result = await ensureNativeApi().mcp.testConnection({ name: server.name });
        setTestResult({
          name: server.name,
          status: result.status,
          ...(result.error ? { error: result.error } : {}),
        });
      } finally {
        setTestingName(null);
      }
    },
  });

  const handleAddClick = () => {
    setEditing(null);
    setEditorOpen(true);
  };

  const handleEditClick = (server: McpServerDescriptor) => {
    setEditing(server);
    setEditorOpen(true);
  };

  const handleEditorSaved = () => {
    void queryClient.invalidateQueries({ queryKey: providerDiscoveryQueryKeys.all });
  };

  const totalServers = (catalogQuery.data?.servers ?? []).length;
  const enabledCount = (catalogQuery.data?.servers ?? []).filter(
    (server) => !disabledSet.has(mcpDisabledKey(server.name)),
  ).length;

  return (
    <div className="space-y-8">
      <SettingsSection title="Syncode store">
        <SettingsRow
          title="Syncode MCP store"
          description="MCP servers added here are stored in ~/.syncode/mcp.json and forwarded to all ACP-speaking providers (cursor / grok / gemini)."
          status={
            syncodeMcpPath ? (
              <code className="break-all text-[11px] text-muted-foreground">{syncodeMcpPath}</code>
            ) : null
          }
          control={
            <div className="flex items-center gap-2">
              <span className="text-xs font-medium text-muted-foreground">
                {catalogQuery.isLoading
                  ? "Scanning…"
                  : `${enabledCount} of ${totalServers} server${totalServers === 1 ? "" : "s"} enabled`}
              </span>
              <Button size="sm" onClick={handleAddClick}>
                Add server
              </Button>
            </div>
          }
        />
      </SettingsSection>

      {catalogQuery.isError ? (
        <SettingsSection title="MCP servers">
          <SettingsRow
            title="MCP discovery failed"
            description="Syncode could not scan the MCP config files. Retry after checking that the server is running."
          />
        </SettingsSection>
      ) : null}

      {!catalogQuery.isLoading && !catalogQuery.isError && totalServers === 0 ? (
        <SettingsSection title="MCP servers">
          <SettingsRow
            title="No MCP servers found"
            description="Add your first server with the button above, or seed ~/.claude.json, ~/.cursor/mcp.json, ~/.codex/config.toml, or a project-local .mcp.json."
          />
        </SettingsSection>
      ) : null}

      {sections.map((section) => {
        if (section.servers.length === 0) {
          return null;
        }
        return (
          <SettingsSection key={section.key} title={section.title}>
            {section.servers.map((server) => {
              const enabled = !disabledSet.has(mcpDisabledKey(server.name));
              const testState =
                testResult?.name === server.name ? testResult : null;
              return (
                <ServerRow
                  key={`${server.scope}:${server.name}:${server.sourcePath}`}
                  server={server}
                  enabled={enabled}
                  onToggle={(next) => setEnabled(server.name, next)}
                  onEdit={
                    server.editable ? () => handleEditClick(server) : undefined
                  }
                  onDelete={
                    server.editable
                      ? () => {
                          if (
                            typeof window !== "undefined" &&
                            window.confirm(`Delete the MCP server "${server.name}"?`)
                          ) {
                            void deleteMutation.mutateAsync(server.name);
                          }
                        }
                      : undefined
                  }
                  onTest={
                    server.editable ? () => testMutation.mutate(server) : undefined
                  }
                  testing={testingName === server.name}
                  testStatus={testState?.status ?? null}
                  testError={testState?.error ?? null}
                />
              );
            })}
          </SettingsSection>
        );
      })}

      <McpServerEditor
        open={editorOpen}
        onOpenChange={setEditorOpen}
        initial={editing}
        onSaved={handleEditorSaved}
      />
    </div>
  );
}

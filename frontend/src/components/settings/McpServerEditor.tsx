// FILE: McpServerEditor.tsx
// Purpose: Modal form for creating/editing a syncode-owned MCP server entry.
// Layer: Web settings UI
//
// Holds a draft locally; only commits on Save. The form is intentionally
// simple — name + transport picker + command/args/env (stdio) or url (http/sse).
// Env vars are edited as `NAME=value` text rows because the wire contract
// exposes only NAMES on read; values are persisted to ~/.syncode/mcp.json.

import { useEffect, useMemo, useState } from "react";
import {
  type McpServerDescriptor,
  type McpServerInput,
  type McpServerPatch,
  type McpTransport,
} from "@t3tools/contracts";
import { Dialog, DialogClose, DialogPopup, DialogTitle } from "~/components/ui/dialog";
import { Button } from "~/components/ui/button";
import { Label } from "~/components/ui/label";
import { Input } from "~/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import { ensureNativeApi } from "~/nativeApi";

interface McpServerEditorProps {
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
  /** When set, the dialog operates in "edit" mode; otherwise "create". */
  readonly initial?: McpServerDescriptor | null;
  /** Called after a successful create/update so the panel can refetch. */
  readonly onSaved: () => void;
}

interface DraftState {
  name: string;
  transport: McpTransport;
  command: string;
  argsText: string;
  envText: string;
  url: string;
}

const EMPTY_DRAFT: DraftState = {
  name: "",
  transport: "stdio",
  command: "",
  argsText: "",
  envText: "",
  url: "",
};

const TRANSPORTS: ReadonlyArray<{ readonly value: McpTransport; readonly label: string }> = [
  { value: "stdio", label: "Stdio (local command)" },
  { value: "http", label: "HTTP" },
  { value: "sse", label: "SSE" },
];

function parseArgs(text: string): string[] {
  // Simple whitespace splitter. shlex-style quoting is out of scope for v1;
  // users who need quotes can edit the JSON file directly.
  return text
    .split(/\s+/)
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function parseEnv(text: string): Array<readonly [string, string]> {
  const entries: Array<readonly [string, string]> = [];
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }
    const eq = line.indexOf("=");
    if (eq <= 0) {
      continue;
    }
    const name = line.slice(0, eq).trim();
    const value = line.slice(eq + 1).trim();
    entries.push([name, value] as const);
  }
  return entries;
}

function envToText(descriptor?: McpServerDescriptor | null): string {
  if (!descriptor) {
    return "";
  }
  // Existing values aren't on the wire (redacted); only NAMES are. So when
  // editing, we pre-populate the env text with `NAME=` rows so the user can
  // re-enter values without losing the names.
  return (descriptor.env ?? []).map((entry) => `${entry.name}=`).join("\n");
}

export function McpServerEditor({ open, onOpenChange, initial, onSaved }: McpServerEditorProps) {
  const isEdit = Boolean(initial);
  const [draft, setDraft] = useState<DraftState>(EMPTY_DRAFT);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Sync the draft when the dialog opens or the target row changes.
  useEffect(() => {
    if (!open) {
      return;
    }
    if (initial) {
      setDraft({
        name: initial.name,
        transport: initial.transport,
        command: initial.command ?? "",
        argsText: (initial.args ?? []).join(" "),
        envText: envToText(initial),
        url: initial.url ?? "",
      });
    } else {
      setDraft(EMPTY_DRAFT);
    }
    setError(null);
  }, [open, initial]);

  const trimmedName = draft.name.trim();
  const nameValid = trimmedName.length > 0 && /^[a-zA-Z0-9_-]+$/.test(trimmedName);
  const stdioValid = draft.transport === "stdio" && draft.command.trim().length > 0;
  const urlValid =
    (draft.transport === "http" || draft.transport === "sse") && draft.url.trim().length > 0;
  const canSubmit = nameValid && (stdioValid || urlValid) && !submitting;

  const handleSubmit = async () => {
    if (!canSubmit) {
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      const api = ensureNativeApi();
      if (isEdit && initial) {
        const patch: McpServerPatch = {
          name: trimmedName,
          transport: draft.transport,
          ...(draft.transport === "stdio"
            ? {
                command: draft.command.trim(),
                args: parseArgs(draft.argsText),
                env: parseEnv(draft.envText),
              }
            : {
                url: draft.url.trim(),
              }),
        };
        await api.mcp.update({ name: initial.name, patch });
      } else {
        const input: McpServerInput = {
          name: trimmedName,
          transport: draft.transport,
          ...(draft.transport === "stdio"
            ? {
                command: draft.command.trim(),
                args: parseArgs(draft.argsText),
                env: parseEnv(draft.envText),
              }
            : {
                url: draft.url.trim(),
              }),
        };
        await api.mcp.create(input);
      }
      onSaved();
      onOpenChange(false);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setSubmitting(false);
    }
  };

  const transportLabel = useMemo(() => {
    return TRANSPORTS.find((entry) => entry.value === draft.transport)?.label ?? draft.transport;
  }, [draft.transport]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogClose />
      <DialogPopup className="max-w-lg">
        <DialogTitle>{isEdit ? `Edit ${initial?.name}` : "Add MCP server"}</DialogTitle>

        <div className="space-y-4 pt-2">
          <div className="space-y-1.5">
            <Label htmlFor="mcp-name">Name</Label>
            <Input
              id="mcp-name"
              value={draft.name}
              onChange={(e) => setDraft((prev) => ({ ...prev, name: e.target.value }))}
              placeholder="filesystem"
              disabled={submitting}
            />
            {!nameValid && draft.name.length > 0 ? (
              <p className="text-xs text-destructive">
                Use letters, digits, underscores, or hyphens only.
              </p>
            ) : null}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="mcp-transport">Transport</Label>
            <Select
              value={draft.transport}
              onValueChange={(value: string | null) =>
                value
                  ? setDraft((prev) => ({ ...prev, transport: value as McpTransport }))
                  : undefined
              }
              disabled={submitting}
            >
              <SelectTrigger id="mcp-transport">
                <SelectValue>{transportLabel}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                {TRANSPORTS.map((entry) => (
                  <SelectItem key={entry.value} value={entry.value}>
                    {entry.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {draft.transport === "stdio" ? (
            <>
              <div className="space-y-1.5">
                <Label htmlFor="mcp-command">Command</Label>
                <Input
                  id="mcp-command"
                  value={draft.command}
                  onChange={(e) => setDraft((prev) => ({ ...prev, command: e.target.value }))}
                  placeholder="npx"
                  disabled={submitting}
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="mcp-args">Arguments (space-separated)</Label>
                <Input
                  id="mcp-args"
                  value={draft.argsText}
                  onChange={(e) => setDraft((prev) => ({ ...prev, argsText: e.target.value }))}
                  placeholder="-y @modelcontextprotocol/server-filesystem /tmp"
                  disabled={submitting}
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="mcp-env">Environment variables (NAME=value, one per line)</Label>
                <textarea
                  id="mcp-env"
                  className="flex min-h-[80px] w-full rounded-md border border-foreground/12 bg-transparent px-3 py-2 text-sm shadow-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                  value={draft.envText}
                  onChange={(e) => setDraft((prev) => ({ ...prev, envText: e.target.value }))}
                  placeholder={"GITHUB_TOKEN=ghp_xxx\nAPI_KEY=sk-xxx"}
                  disabled={submitting}
                />
                {isEdit ? (
                  <p className="text-[11px] text-muted-foreground">
                    Existing values are not shown for security. Re-enter any value you want to keep.
                  </p>
                ) : null}
              </div>
            </>
          ) : (
            <div className="space-y-1.5">
              <Label htmlFor="mcp-url">URL</Label>
              <Input
                id="mcp-url"
                value={draft.url}
                onChange={(e) => setDraft((prev) => ({ ...prev, url: e.target.value }))}
                placeholder={
                  draft.transport === "sse"
                    ? "https://example.com/events"
                    : "https://example.com/mcp"
                }
                disabled={submitting}
              />
            </div>
          )}

          {error ? <p className="text-sm text-destructive">{error}</p> : null}

          <div className="flex justify-end gap-2 pt-2">
            <Button
              variant="ghost"
              onClick={() => onOpenChange(false)}
              disabled={submitting}
            >
              Cancel
            </Button>
            <Button onClick={handleSubmit} disabled={!canSubmit}>
              {submitting ? "Saving…" : isEdit ? "Save changes" : "Add server"}
            </Button>
          </div>
        </div>
      </DialogPopup>
    </Dialog>
  );
}

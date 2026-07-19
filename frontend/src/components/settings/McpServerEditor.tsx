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
import {
  Dialog,
  DialogClose,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "~/components/ui/dialog";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Field, FieldDescription, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Separator } from "~/components/ui/separator";
import { Textarea } from "~/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import { TriangleAlertIcon } from "~/lib/icons";
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

interface TransportOption {
  readonly value: McpTransport;
  readonly label: string;
  readonly description: string;
}

const TRANSPORTS: ReadonlyArray<TransportOption> = [
  {
    value: "stdio",
    label: "Stdio (local command)",
    description: "Syncode spawns a local process and speaks JSON-RPC over stdin/stdout.",
  },
  {
    value: "http",
    label: "HTTP",
    description: "Remote server reachable at a single URL. Syncode POSTs JSON-RPC envelopes.",
  },
  {
    value: "sse",
    label: "SSE",
    description: "Remote server using Server-Sent Events for streaming responses.",
  },
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

  const transportOption = useMemo(
    () => TRANSPORTS.find((entry) => entry.value === draft.transport),
    [draft.transport],
  );

  const placeholderUrl =
    draft.transport === "sse" ? "https://example.com/events" : "https://example.com/mcp";

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogClose />
      <DialogPopup className="max-w-xl" surface="solid">
        <DialogHeader>
          <DialogTitle>{isEdit ? `Edit ${initial?.name}` : "Add MCP server"}</DialogTitle>
          <DialogDescription>
            Servers added here are persisted to <code className="font-chat-code">~/.syncode/mcp.json</code>{" "}
            and forwarded to every ACP-speaking provider (cursor / grok / gemini).
          </DialogDescription>
        </DialogHeader>

        <DialogPanel className="gap-5">
          {/* ── Identity ─────────────────────────────────────────── */}
          <section className="flex flex-col gap-4">
            <h4 className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
              Identity
            </h4>
            <Field>
              <FieldLabel htmlFor="mcp-name">Name</FieldLabel>
              <Input
                id="mcp-name"
                value={draft.name}
                onChange={(e) => setDraft((prev) => ({ ...prev, name: e.target.value }))}
                placeholder="filesystem"
                aria-invalid={!nameValid && draft.name.length > 0}
                disabled={submitting}
              />
              <FieldDescription>
                Use letters, digits, underscores, or hyphens. This is how the server appears in
                provider config files.
              </FieldDescription>
            </Field>

            <Field>
              <FieldLabel htmlFor="mcp-transport">Transport</FieldLabel>
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
                  <SelectValue>{transportOption?.label ?? draft.transport}</SelectValue>
                </SelectTrigger>
                <SelectContent>
                  {TRANSPORTS.map((entry) => (
                    <SelectItem key={entry.value} value={entry.value}>
                      {entry.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {transportOption ? (
                <FieldDescription>{transportOption.description}</FieldDescription>
              ) : null}
            </Field>
          </section>

          <Separator />

          {/* ── Connection ──────────────────────────────────────── */}
          <section className="flex flex-col gap-4">
            <h4 className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
              Connection
            </h4>

            {draft.transport === "stdio" ? (
              <>
                <Field>
                  <FieldLabel htmlFor="mcp-command">Command</FieldLabel>
                  <Input
                    id="mcp-command"
                    value={draft.command}
                    onChange={(e) =>
                      setDraft((prev) => ({ ...prev, command: e.target.value }))
                    }
                    placeholder="npx"
                    aria-required
                    disabled={submitting}
                  />
                  <FieldDescription>
                    The executable syncode launches (e.g. <code className="font-chat-code">npx</code>,{" "}
                    <code className="font-chat-code">node</code>,{" "}
                    <code className="font-chat-code">python</code>).
                  </FieldDescription>
                </Field>

                <Field>
                  <FieldLabel htmlFor="mcp-args">Arguments</FieldLabel>
                  <Input
                    id="mcp-args"
                    value={draft.argsText}
                    onChange={(e) =>
                      setDraft((prev) => ({ ...prev, argsText: e.target.value }))
                    }
                    placeholder="-y @modelcontextprotocol/server-filesystem /tmp"
                    disabled={submitting}
                  />
                  <FieldDescription>Whitespace-separated CLI arguments.</FieldDescription>
                </Field>

                <Field>
                  <FieldLabel htmlFor="mcp-env">Environment variables</FieldLabel>
                  <Textarea
                    id="mcp-env"
                    value={draft.envText}
                    onChange={(e) => setDraft((prev) => ({ ...prev, envText: e.target.value }))}
                    placeholder={"GITHUB_TOKEN=ghp_xxx\nAPI_KEY=sk-xxx"}
                    disabled={submitting}
                    rows={4}
                  />
                  <FieldDescription>
                    One <code className="font-chat-code">NAME=value</code> per line. Lines starting
                    with <code className="font-chat-code">#</code> are ignored.
                    {isEdit
                      ? " Existing values are redacted on read — re-enter any value you want to keep."
                      : " Values are stored locally only and never forwarded beyond the launched process."}
                  </FieldDescription>
                </Field>
              </>
            ) : (
              <Field>
                <FieldLabel htmlFor="mcp-url">URL</FieldLabel>
                <Input
                  id="mcp-url"
                  value={draft.url}
                  onChange={(e) => setDraft((prev) => ({ ...prev, url: e.target.value }))}
                  placeholder={placeholderUrl}
                  aria-required
                  disabled={submitting}
                />
                <FieldDescription>
                  {draft.transport === "sse"
                    ? "SSE endpoint — syncode opens the stream and POSTs commands to it."
                    : "HTTP endpoint accepting JSON-RPC 2.0 POST requests."}
                </FieldDescription>
              </Field>
            )}
          </section>

          {error ? (
            <Alert variant="error">
              <TriangleAlertIcon />
              <AlertTitle>Couldn&apos;t save the server</AlertTitle>
              <AlertDescription>
                <code className="break-all font-chat-code text-xs">{error}</code>
              </AlertDescription>
            </Alert>
          ) : null}
        </DialogPanel>

        <DialogFooter>
          <DialogClose
            render={
              <Button variant="ghost" type="button" disabled={submitting}>
                Cancel
              </Button>
            }
          />
          <Button onClick={handleSubmit} disabled={!canSubmit}>
            {submitting ? "Saving…" : isEdit ? "Save changes" : "Add server"}
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}

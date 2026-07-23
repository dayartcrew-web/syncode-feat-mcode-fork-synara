// FILE: UpdateChecker.tsx
// Purpose: "Check for updates" + "Update & restart" UI for the desktop app.
// Layer: Settings presentation (desktop-only — invokes the Tauri IPC
//        `check_for_updates` / `apply_update` wired in syncode-tauri's
//        desktop_commands, which delegate to tauri-plugin-updater).
//
// Browser/dev mode (no Tauri) shows a disabled hint instead of erroring.

import { useState } from "react";
import { invoke, isTauri } from "@tauri-apps/api/core";
import { Button } from "../ui/button";

interface CheckForUpdatesResult {
  available: boolean;
  status: string;
  version?: string | null;
  releaseNotes?: string | null;
  message?: string | null;
}

interface ApplyUpdateResult {
  installed: boolean;
  version?: string | null;
  reason?: string | null;
}

export function UpdateChecker() {
  const [checking, setChecking] = useState(false);
  const [available, setAvailable] = useState<boolean | null>(null);
  const [version, setVersion] = useState<string | null>(null);
  const [applying, setApplying] = useState(false);
  const [installed, setInstalled] = useState(false);
  const [note, setNote] = useState<string | null>(null);

  if (!isTauri()) {
    return (
      <p className="text-sm text-[var(--muted-fg)]">
        Update checks are available in the desktop app.
      </p>
    );
  }

  const onCheck = async () => {
    setChecking(true);
    setNote(null);
    try {
      const result = await invoke<CheckForUpdatesResult>("check_for_updates");
      setAvailable(result.available);
      setVersion(result.version ?? null);
      setNote(result.message ?? null);
    } catch (error) {
      setAvailable(false);
      setNote(error instanceof Error ? error.message : String(error));
    } finally {
      setChecking(false);
    }
  };

  const onApply = async () => {
    setApplying(true);
    setNote(null);
    try {
      const result = await invoke<ApplyUpdateResult>("apply_update");
      if (result.installed) {
        setInstalled(true);
        setNote("Update installed — restart Syncode to finish.");
      } else {
        setNote(result.reason ?? "Update did not apply.");
      }
    } catch (error) {
      setNote(error instanceof Error ? error.message : String(error));
    } finally {
      setApplying(false);
    }
  };

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-2">
        <Button onClick={onCheck} disabled={checking || applying}>
          {checking ? "Checking…" : "Check for updates"}
        </Button>
        {available ? (
          <Button onClick={onApply} disabled={applying}>
            {applying
              ? "Installing…"
              : installed
                ? "Installed — restart"
                : `Update to ${version ?? "latest"}`}
          </Button>
        ) : null}
      </div>
      {available === false && !checking && note === null ? (
        <span className="text-xs text-[var(--muted-fg)]">You're up to date.</span>
      ) : null}
      {note ? <span className="text-xs text-[var(--muted-fg)]">{note}</span> : null}
    </div>
  );
}

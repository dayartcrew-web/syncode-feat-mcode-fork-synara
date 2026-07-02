// FILE: ProjectPicker.tsx
// Purpose: Folder selector beneath the new-chat composer that groups active folders and home
//          folders while always creating chats as rows inside the shared Chats container.
// Layer: Chat / empty-state entrypoint

import { memo, useCallback, useDeferredValue, useEffect, useMemo, useState } from "react";
import { readNativeApi } from "../../nativeApi";
import { ChevronLeftIcon, FolderOpenIcon, PlusIcon, XIcon } from "~/lib/icons";
import { isLinuxPlatform, isMacPlatform, isWindowsPlatform } from "~/lib/utils";
import { toastManager } from "../ui/toast";
import { FolderClosed } from "../FolderClosed";
import { Input } from "../ui/input";
import { PickerTriggerButton } from "./PickerTriggerButton";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";
import { DirectoryTreeBrowser } from "./DirectoryTreeBrowser";
import { useWorkspaceStore } from "../../workspaceStore";

interface ProjectPickerProps {
  align?: "start" | "center" | "end";
  side?: "top" | "bottom";
  showResetToHome?: boolean;
  selectedWorkspaceRoot?: string | null;
  onSelectWorkspaceRoot?: ((workspaceRoot: string) => void) | undefined;
  onResetToHome?: (() => void) | undefined;
}

function basenameOfPath(value: string | null | undefined): string | null {
  if (!value) return null;
  const normalized = value.replace(/[\\/]+$/, "");
  const separatorIndex = Math.max(normalized.lastIndexOf("/"), normalized.lastIndexOf("\\"));
  const basename = separatorIndex === -1 ? normalized : normalized.slice(separatorIndex + 1);
  return basename.length > 0 ? basename : null;
}

export const ProjectPicker = memo(function ProjectPicker({
  align = "start",
  side = "bottom",
  showResetToHome = false,
  selectedWorkspaceRoot = null,
  onSelectWorkspaceRoot,
  onResetToHome,
}: ProjectPickerProps) {
  const homeDir = useWorkspaceStore((state) => state.homeDir);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const deferredQuery = useDeferredValue(query);
  const [isPicking, setIsPicking] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  // Platform-aware label for the home-folder group. Previously hardcoded as
  // "Folders on this Mac", which is wrong on Windows/Linux. Detect once.
  const homeFolderGroupLabel = useMemo(() => {
    if (typeof navigator === "undefined") return "Folders on this computer";
    const platform = navigator.platform;
    if (isMacPlatform(platform)) return "Folders on this Mac";
    if (isWindowsPlatform(platform)) return "Folders on this PC";
    if (isLinuxPlatform(platform)) return "Folders on this computer";
    return "Folders on this computer";
  }, []);

  // Tree root follows selection: initially the home dir, but after a folder is
  // selected the popup re-roots to that folder so its contents (subfolders) are
  // shown. Reset to home when the popup closes so the next open starts fresh.
  const [treeRootPath, setTreeRootPath] = useState<string | null>(homeDir);
  useEffect(() => {
    if (!open) {
      setTreeRootPath(homeDir);
      return;
    }
    setTreeRootPath((current) => current ?? homeDir);
  }, [homeDir, open]);

  const triggerLabel = selectedWorkspaceRoot ? (
    <span className="flex min-w-0 items-baseline gap-1.5">
      <span className="min-w-0 truncate">{basenameOfPath(selectedWorkspaceRoot) ?? selectedWorkspaceRoot}</span>
    </span>
  ) : (
    "Work in a project"
  );

  const handleOpenChange = useCallback((nextOpen: boolean) => {
    setOpen(nextOpen);
    if (!nextOpen) {
      setQuery("");
      setErrorMessage(null);
    }
  }, []);

  // Tree selection: persist the selection AND re-root the tree to the picked
  // folder so the popup shows that folder's contents (the "show tree folder
  // after select" behavior). The popup stays open; the footer's "Open folder"
  // becomes reachable because selectedWorkspaceRoot is now set.
  const handleTreeSelect = useCallback(
    (absolutePath: string) => {
      onSelectWorkspaceRoot?.(absolutePath);
      setTreeRootPath(absolutePath);
      setQuery("");
    },
    [onSelectWorkspaceRoot],
  );

  const handleAddNewProject = useCallback(async () => {
    if (isPicking) return;
    const api = readNativeApi();
    if (!api) {
      setErrorMessage("App is still connecting. Try again in a moment.");
      return;
    }

    setIsPicking(true);
    setErrorMessage(null);
    try {
      const pickedPath = await api.dialogs.pickFolder();
      setIsPicking(false);
      if (!pickedPath) {
        return;
      }
      onSelectWorkspaceRoot?.(pickedPath);
      setOpen(false);
    } catch (error) {
      setIsPicking(false);
      setErrorMessage(error instanceof Error ? error.message : "Unable to open the folder picker.");
    }
  }, [isPicking, onSelectWorkspaceRoot]);

  const handleOpenFolder = useCallback(async () => {
    if (!selectedWorkspaceRoot) return;
    const api = readNativeApi();
    if (!api) {
      setErrorMessage("App is still connecting. Try again in a moment.");
      return;
    }
    try {
      await api.shell.showInFolder(selectedWorkspaceRoot);
      setOpen(false);
    } catch (error) {
      toastManager.add({
        title: "Unable to open folder",
        description: error instanceof Error ? error.message : undefined,
        type: "error",
      });
    }
  }, [selectedWorkspaceRoot]);

  return (
    <Popover open={open} onOpenChange={handleOpenChange}>
      <PopoverTrigger
        render={
          <PickerTriggerButton icon={<FolderClosed className="size-3.5" />} label={triggerLabel} />
        }
      />
      <PopoverPopup align={align} side={side} className="p-0">
        {/* Outer column is bounded to the popover viewport's content box
            (--available-height minus the viewport's own py-4 = 2rem padding).
            This keeps the popup from overflowing the viewport, so the
            popover-viewport never scrolls — only the tree section below does.
            Search (top) and footer (bottom) are shrink-0 (locked); the tree
            (flex-1 overflow-y-auto) is the single scroller. */}
        <div
          className="flex w-72 flex-col"
          style={{ maxHeight: "calc(var(--available-height, 60vh) - 2rem)" }}
        >
          {/* Search header — LOCKED (shrink-0), consistent p-1 padding. */}
          <div className="shrink-0 border-b border-border bg-[var(--composer-surface)] p-1">
            <Input
              nativeInput
              size="sm"
              type="search"
              placeholder="Search folders"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              className="rounded-md border-border/60 bg-background shadow-none before:hidden has-focus-visible:border-neutral-500/15 has-focus-visible:ring-0 [&_input]:font-sans"
            />
          </div>
          {/* Tree section — the ONLY scroller (flex-1, fills remaining height). */}
          <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain py-1">
            {/* When rooted at a selected folder (not home), show a back-to-home row. */}
            {treeRootPath && homeDir && treeRootPath !== homeDir ? (
              <button
                type="button"
                className="flex w-full items-center gap-1 rounded-lg px-2 py-1 text-left text-xs text-muted-foreground transition-colors hover:bg-[var(--color-background-elevated-secondary)] hover:text-[var(--color-text-foreground)]"
                onClick={() => setTreeRootPath(homeDir)}
              >
                <ChevronLeftIcon className="size-3 shrink-0" />
                <span className="truncate">Back to {homeFolderGroupLabel}</span>
              </button>
            ) : null}
            <DirectoryTreeBrowser
              rootPath={treeRootPath}
              query={deferredQuery}
              emptyLabel="No folders found"
              unavailableLabel="Home directory unavailable."
              loadingLabel="Loading folders…"
              onSelectEntry={(absolutePath, entry) => {
                if (entry.kind === "directory") {
                  handleTreeSelect(absolutePath);
                }
              }}
            />
          </div>
          {/* Footer — LOCKED (shrink-0), always visible. */}
          <div className="shrink-0 border-t border-border p-1">
            <button
              type="button"
              className="flex w-full items-center gap-2 rounded-md px-2 py-1 text-left text-sm transition-colors hover:bg-[var(--color-background-elevated-secondary)] hover:text-[var(--color-text-foreground)] disabled:cursor-not-allowed disabled:opacity-60"
              onClick={() => void handleAddNewProject()}
              disabled={isPicking}
            >
              <PlusIcon className="size-3.5 shrink-0 text-muted-foreground/70" />
              <span className="truncate">
                {isPicking ? "Opening folder picker…" : "Add new project"}
              </span>
            </button>
            {selectedWorkspaceRoot ? (
              <button
                type="button"
                className="flex w-full items-center gap-2 rounded-md px-2 py-1 text-left text-sm transition-colors hover:bg-[var(--color-background-elevated-secondary)] hover:text-[var(--color-text-foreground)] disabled:cursor-not-allowed disabled:opacity-60"
                onClick={() => void handleOpenFolder()}
                title="Open the current project folder in your file manager"
              >
                <FolderOpenIcon className="size-3.5 shrink-0 text-muted-foreground/70" />
                <span className="truncate">Open folder</span>
              </button>
            ) : null}
            {showResetToHome ? (
              <button
                type="button"
                className="flex w-full items-center gap-2 rounded-md px-2 py-1 text-left text-sm transition-colors hover:bg-[var(--color-background-elevated-secondary)] hover:text-[var(--color-text-foreground)]"
                onClick={() => {
                  onResetToHome?.();
                  setOpen(false);
                }}
              >
                <XIcon className="size-3.5 shrink-0 text-muted-foreground/70" />
                <span className="truncate">Don&apos;t work in a project</span>
              </button>
            ) : null}
            {errorMessage ? (
              <div className="px-2 pb-1 text-destructive text-xs">{errorMessage}</div>
            ) : null}
          </div>
        </div>
      </PopoverPopup>
    </Popover>
  );
});

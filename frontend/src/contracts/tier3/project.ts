/**
 * Tier 3 — Project domain (file ops / dev servers / search / scripts).
 *
 * Hand-ported from MCode `packages/contracts/src/project.ts` (Effect Schema
 * → plain TS types). Covers the project file/dev-server/search surface the
 * vendored UI imports beyond what Tier 0/Tier 1 already exported. The
 * matching Input/Result DTOs the UI imports from `@t3tools/contracts` were
 * already declared opaque in shell.ts (B4/T6); this module adds the real
 * shapes for the entry / directory / dev-server / script descriptors so the
 * UI's property-access type-checks.
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/project.ts
 */

import type { ProjectId } from "../ids";
import type { PositiveInt, ProcessEnvRecord, TrimmedNonEmptyString } from "./base";

export type ProjectKind = "project" | "chat";

export type ProjectEntryKind = "file" | "directory";

export interface ProjectEntry {
  path: TrimmedNonEmptyString;
  kind: ProjectEntryKind;
  parentPath?: TrimmedNonEmptyString;
}

export interface ProjectDirectoryEntry {
  path: TrimmedNonEmptyString;
  name: TrimmedNonEmptyString;
  parentPath?: TrimmedNonEmptyString;
  hasChildren: boolean;
}

export interface ProjectFileSystemEntry {
  path: TrimmedNonEmptyString;
  name: TrimmedNonEmptyString;
  parentPath?: TrimmedNonEmptyString;
  kind: ProjectEntryKind;
  hasChildren?: boolean;
}

export interface ProjectLocalSearchEntry {
  path: TrimmedNonEmptyString;
  name: TrimmedNonEmptyString;
  parentPath?: TrimmedNonEmptyString;
  kind: ProjectEntryKind;
}

export interface ProjectDiscoveredScript {
  name: TrimmedNonEmptyString;
  command: TrimmedNonEmptyString;
}

export interface ProjectDiscoveredScriptTarget {
  cwd: TrimmedNonEmptyString;
  relativePath: string;
  packageJsonPath: TrimmedNonEmptyString;
  packageName?: TrimmedNonEmptyString;
  scripts: readonly ProjectDiscoveredScript[];
}

export interface ProjectDiscoverScriptsResult {
  targets: readonly ProjectDiscoveredScriptTarget[];
}

export interface ProjectListDirectoriesResult {
  entries: readonly ProjectFileSystemEntry[];
}

export interface ProjectSearchEntriesResult {
  entries: readonly ProjectEntry[];
  truncated: boolean;
}

export interface ProjectSearchLocalEntriesResult {
  entries: readonly ProjectLocalSearchEntry[];
  truncated: boolean;
}

export interface ProjectReadFileResult {
  relativePath: TrimmedNonEmptyString;
  contents: string;
  truncated: boolean;
}

export type ProjectScriptIcon =
  | "play"
  | "test"
  | "lint"
  | "configure"
  | "build"
  | "debug";

export interface ProjectScript {
  id: TrimmedNonEmptyString;
  name: TrimmedNonEmptyString;
  command: TrimmedNonEmptyString;
  icon: ProjectScriptIcon;
  runOnWorktreeCreate: boolean;
}

// ─── Dev server ───────────────────────────────────────────────────────

export type ProjectDevServerStatus = "starting" | "running";

export interface ProjectDevServer {
  projectId: ProjectId;
  command: TrimmedNonEmptyString;
  cwd: TrimmedNonEmptyString;
  pid: PositiveInt | null;
  startedAt: TrimmedNonEmptyString;
  status: ProjectDevServerStatus;
}

export type ProjectDevServerRemovedReason = "stopped" | "exited";

export type ProjectDevServerEvent =
  | { readonly type: "snapshot"; readonly servers: readonly ProjectDevServer[] }
  | { readonly type: "upserted"; readonly server: ProjectDevServer }
  | {
      readonly type: "removed";
      readonly projectId: ProjectId;
      readonly reason: ProjectDevServerRemovedReason;
    };

// Input DTOs the UI passes (kept opaque in shell.ts; re-declared with real
// shape here for typed call sites). Re-export-friendly aliases.
export interface ProjectRunDevServerInput {
  projectId: ProjectId;
  command: TrimmedNonEmptyString;
  cwd: TrimmedNonEmptyString;
  env?: ProcessEnvRecord;
}

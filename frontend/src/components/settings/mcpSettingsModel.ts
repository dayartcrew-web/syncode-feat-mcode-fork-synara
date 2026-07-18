// FILE: mcpSettingsModel.ts
// Purpose: Groups the MCP catalog for the Settings → MCP Servers panel.
// Layer: Settings UI logic
// Exports: pure helpers for section assignment, toggle patches, and disability.
//
// The backend (`crates/syncode-ws/src/mcp_catalog.rs`) already dedupes by
// name and emits one `McpServerDescriptor` per source. These helpers just
// bucket the catalog into the two UI sections (Discovered vs Configured by
// Syncode) and compute the settings patch needed to flip the disabled flag
// in `ServerSettings.mcp.disabled`.

import type { McpScope, McpServerDescriptor, ServerSettingsPatch } from "@t3tools/contracts";

export type McpSectionKey = "discovered" | "syncode";

export interface McpSection {
  readonly key: McpSectionKey;
  readonly title: string;
  readonly subtitle: string;
  readonly servers: ReadonlyArray<McpServerDescriptor>;
}

/** Scope → display label. */
export function mcpScopeLabel(scope: McpScope): string {
  switch (scope) {
    case "user":
      return "User";
    case "project":
      return "Project";
    case "syncode":
      return "Syncode";
  }
}

/** Section assignment: syncode-owned entries go to the CRUD section, all others to discovered. */
export function sectionForServer(server: McpServerDescriptor): McpSectionKey {
  return server.scope === "syncode" ? "syncode" : "discovered";
}

/**
 * Buckets the catalog into the two sections rendered by the panel. Sections
 * are always returned in display order (discovered first, syncode second)
 * even when empty — the panel hides empty sections, not the model.
 */
export function buildMcpSections(servers: ReadonlyArray<McpServerDescriptor>): readonly McpSection[] {
  const discovered: McpServerDescriptor[] = [];
  const syncode: McpServerDescriptor[] = [];
  for (const server of servers) {
    if (sectionForServer(server) === "syncode") {
      syncode.push(server);
    } else {
      discovered.push(server);
    }
  }
  discovered.sort(compareByThenName);
  syncode.sort(compareByThenName);
  return [
    {
      key: "discovered",
      title: "Discovered",
      subtitle: "Found in your existing MCP config files. Toggle only — edit the source file to change.",
      servers: discovered,
    },
    {
      key: "syncode",
      title: "Configured by Syncode",
      subtitle: "Stored in ~/.syncode/mcp.json. Full add / edit / delete + connection test.",
      servers: syncode,
    },
  ];
}

function compareByThenName(left: McpServerDescriptor, right: McpServerDescriptor): number {
  // Stable, predictable order: by scope (user → project → syncode) then by name.
  const scopeRank = scopeOrder(left.scope) - scopeOrder(right.scope);
  if (scopeRank !== 0) {
    return scopeRank;
  }
  return left.name.localeCompare(right.name);
}

function scopeOrder(scope: McpScope): number {
  switch (scope) {
    case "user":
      return 0;
    case "project":
      return 1;
    case "syncode":
      return 2;
  }
}

/** Returns the normalized key used by the disabled list (lowercase name). */
export function mcpDisabledKey(name: string): string {
  return name.trim().toLowerCase();
}

/**
 * Builds the ServerSettingsPatch needed to flip one server's enabled flag.
 *
 * The disabled list is treated as a set; immutable spread keeps callers
 * pure. When `enable === true` the key is removed; when `enable === false`
 * it's added (deduped).
 */
export function patchForToggle(
  currentDisabled: ReadonlyArray<string>,
  serverName: string,
  enable: boolean,
): Pick<ServerSettingsPatch, "mcp"> {
  const key = mcpDisabledKey(serverName);
  if (enable) {
    const next = currentDisabled.filter((existing) => existing !== key);
    return { mcp: { disabled: next } };
  }
  if (currentDisabled.some((existing) => existing === key)) {
    return { mcp: { disabled: [...currentDisabled] } };
  }
  return { mcp: { disabled: [...currentDisabled, key] } };
}

/** `true` if at least one row in the section is currently enabled. */
export function sectionHasEnabledServer(
  section: McpSection,
  disabled: ReadonlyArray<string>,
): boolean {
  return section.servers.some((server) => !disabled.includes(mcpDisabledKey(server.name)));
}

/** `true` if every row in the section is currently enabled. */
export function sectionAllEnabled(
  section: McpSection,
  disabled: ReadonlyArray<string>,
): boolean {
  return section.servers.length > 0 && section.servers.every((server) => !disabled.includes(mcpDisabledKey(server.name)));
}

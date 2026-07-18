// FILE: mcpSettingsModel.test.ts
// Purpose: Locks down Settings -> MCP Servers sectioning + toggle patch shape.
// Layer: Web settings logic tests

import type { McpServerDescriptor } from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import {
  buildMcpSections,
  mcpDisabledKey,
  mcpScopeLabel,
  patchForToggle,
  sectionAllEnabled,
  sectionForServer,
  sectionHasEnabledServer,
} from "./mcpSettingsModel";

function server(partial: Partial<McpServerDescriptor>): McpServerDescriptor {
  return {
    name: "example",
    transport: "stdio",
    command: null,
    args: [],
    env: [],
    url: null,
    scope: "user",
    sourcePath: "/tmp/example",
    editable: false,
    enabled: true,
    status: null,
    ...partial,
  };
}

describe("mcpScopeLabel", () => {
  it("maps each scope to its display label", () => {
    expect(mcpScopeLabel("user")).toBe("User");
    expect(mcpScopeLabel("project")).toBe("Project");
    expect(mcpScopeLabel("syncode")).toBe("Syncode");
  });
});

describe("sectionForServer", () => {
  it("routes syncode-owned entries to the CRUD section", () => {
    expect(sectionForServer(server({ scope: "syncode" }))).toBe("syncode");
  });

  it("routes discovered user entries to the discovered section", () => {
    expect(sectionForServer(server({ scope: "user" }))).toBe("discovered");
  });

  it("routes project-local entries to the discovered section", () => {
    expect(sectionForServer(server({ scope: "project" }))).toBe("discovered");
  });
});

describe("buildMcpSections", () => {
  it("always returns both sections in display order", () => {
    const sections = buildMcpSections([]);
    expect(sections.map((s) => s.key)).toEqual(["discovered", "syncode"]);
  });

  it("places discovered entries under Discovered and owned entries under Configured by Syncode", () => {
    const sections = buildMcpSections([
      server({ name: "claude-fs", scope: "user" }),
      server({ name: "my-custom", scope: "syncode" }),
    ]);
    const discovered = sections[0];
    const syncode = sections[1];
    expect(discovered?.servers.map((s) => s.name)).toEqual(["claude-fs"]);
    expect(syncode?.servers.map((s) => s.name)).toEqual(["my-custom"]);
  });

  it("sorts within a section by scope rank then by name", () => {
    const sections = buildMcpSections([
      server({ name: "zed", scope: "project" }),
      server({ name: "alpha", scope: "user" }),
      server({ name: "yankee", scope: "syncode" }),
      server({ name: "bravo", scope: "syncode" }),
    ]);
    expect(sections[0]?.servers.map((s) => s.name)).toEqual(["alpha", "zed"]);
    expect(sections[1]?.servers.map((s) => s.name)).toEqual(["bravo", "yankee"]);
  });
});

describe("mcpDisabledKey", () => {
  it("trims and lowercases the name", () => {
    expect(mcpDisabledKey("  Filesystem ")).toBe("filesystem");
  });
});

describe("patchForToggle", () => {
  it("adds the lowercased key when disabling a new entry", () => {
    const patch = patchForToggle([], "Filesystem", false);
    expect(patch.mcp?.disabled).toEqual(["filesystem"]);
  });

  it("does not duplicate the key if already disabled", () => {
    const patch = patchForToggle(["filesystem"], "Filesystem", false);
    expect(patch.mcp?.disabled).toEqual(["filesystem"]);
  });

  it("removes the key when enabling a previously disabled entry", () => {
    const patch = patchForToggle(["filesystem", "other"], "filesystem", true);
    expect(patch.mcp?.disabled).toEqual(["other"]);
  });

  it("returns a fresh array — never mutates the input list", () => {
    const original = ["filesystem"];
    const patch = patchForToggle(original, "filesystem", false);
    expect(original).toEqual(["filesystem"]);
    expect(patch.mcp?.disabled).not.toBe(original);
  });
});

describe("sectionHasEnabledServer / sectionAllEnabled", () => {
  const section = {
    key: "discovered" as const,
    title: "Discovered",
    subtitle: "",
    servers: [
      server({ name: "alpha" }),
      server({ name: "bravo" }),
      server({ name: "charlie" }),
    ],
  };

  it("reports has-enabled when any row is enabled", () => {
    expect(sectionHasEnabledServer(section, ["alpha"])).toBe(true);
    expect(sectionAllEnabled(section, ["alpha"])).toBe(false);
  });

  it("reports all-enabled only when no row is disabled", () => {
    expect(sectionAllEnabled(section, [])).toBe(true);
    expect(sectionAllEnabled(section, ["alpha"])).toBe(false);
  });

  it("returns false for all-enabled when the section is empty", () => {
    const empty = { ...section, servers: [] };
    expect(sectionAllEnabled(empty, [])).toBe(false);
  });
});

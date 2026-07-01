// FILE: chatProjects.test.ts
// Purpose: Verifies home chat-container project recognition across new and legacy roots.

import { describe, expect, it } from "vitest";

import { isHomeChatContainerProject } from "./chatProjects";

describe("isHomeChatContainerProject", () => {
  it("matches the managed Documents/MCode general-chat root used by older drafts", () => {
    expect(
      isHomeChatContainerProject(
        {
          cwd: "/Users/tester/Documents/MCode",
          kind: "chat",
          name: "Home",
          remoteName: "Home",
        },
        {
          homeDir: "/Users/tester",
          chatWorkspaceRoot: "/Users/tester/Documents/MCode",
        },
      ),
    ).toBe(true);
  });

  it("matches Codex-style date/slug chat workspaces under Documents/MCode", () => {
    expect(
      isHomeChatContainerProject(
        {
          cwd: "/Users/tester/Documents/MCode/2026-06-11/yes-it-takes-all-the-skills",
          kind: "chat",
          name: "Yes it takes",
          remoteName: "Yes it takes",
        },
        {
          homeDir: "/Users/tester",
          chatWorkspaceRoot: "/Users/tester/Documents/MCode",
        },
      ),
    ).toBe(true);
  });

  it("keeps recognizing the legacy home-directory chat container during migration", () => {
    expect(
      isHomeChatContainerProject(
        {
          cwd: "/Users/tester",
          kind: "chat",
          name: "Home",
          remoteName: "Home",
        },
        {
          homeDir: "/Users/tester",
          chatWorkspaceRoot: "/Users/tester/Documents/MCode",
        },
      ),
    ).toBe(true);
  });

  it("does not classify ordinary projects under Documents/MCode as home chat containers", () => {
    expect(
      isHomeChatContainerProject(
        {
          cwd: "/Users/tester/Documents/MCode",
          kind: "project",
          name: "MCode",
          remoteName: "MCode",
        },
        {
          homeDir: "/Users/tester",
          chatWorkspaceRoot: "/Users/tester/Documents/MCode",
        },
      ),
    ).toBe(false);
  });

  it("does not classify ordinary projects under date/slug chat folders", () => {
    expect(
      isHomeChatContainerProject(
        {
          cwd: "/Users/tester/Documents/MCode/2026-06-11/yes-it-takes-all-the-skills",
          kind: "project",
          name: "yes-it-takes-all-the-skills",
          remoteName: "yes-it-takes-all-the-skills",
        },
        {
          homeDir: "/Users/tester",
          chatWorkspaceRoot: "/Users/tester/Documents/MCode",
        },
      ),
    ).toBe(false);
  });
});

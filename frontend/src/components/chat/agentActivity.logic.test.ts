import { describe, expect, it } from "vitest";
import type { WorkLogEntry } from "../../session-logic";
import {
  deriveAgentActivityTimelineState,
  formatAgentActivityEntryPreview,
  isAgentActivityWorkEntry,
  isExploreEventEntry,
  isReasoningUpdateWorkEntry,
  isSkillDispatchEntry,
  isSubagentEventEntry,
} from "./agentActivity.logic";

function workEntry(overrides: Partial<WorkLogEntry> & Pick<WorkLogEntry, "id">): WorkLogEntry {
  return {
    createdAt: "2026-06-05T00:00:00.000Z",
    label: "Tool call",
    tone: "tool",
    ...overrides,
  };
}

describe("deriveAgentActivityTimelineState", () => {
  it("compacts consecutive reasoning updates while preserving detail entries", () => {
    const state = deriveAgentActivityTimelineState([
      workEntry({
        id: "reasoning-1",
        label: "Reasoning update",
        tone: "info",
        detail: "Running Check sidebar z-index",
      }),
      workEntry({
        id: "reasoning-2",
        label: "Reasoning update",
        tone: "info",
        detail: "Running Verify diffToggleControl uses valid props",
      }),
      workEntry({
        id: "tool-1",
        label: "Read",
        tone: "tool",
      }),
    ]);

    expect(state.timelineWorkEntries.map((entry) => entry.id)).toEqual([
      "agent-reasoning:reasoning-1",
      "tool-1",
    ]);
    expect(state.timelineWorkEntries[0]).toMatchObject({
      label: "Reasoning",
      toolTitle: "Reasoning",
      preview: "2 updates - Verify diffToggleControl uses valid props",
    });
    expect(state.detailById.get("agent-reasoning:reasoning-1")?.entries).toHaveLength(2);
  });

  it("cleans reasoning prefixes for single update previews", () => {
    const entry = workEntry({
      id: "reasoning-1",
      label: "Reasoning update",
      detail: "Reasoning update Running Complete analysis of the floating panel issue",
    });

    expect(formatAgentActivityEntryPreview(entry)).toBe(
      "Complete analysis of the floating panel issue",
    );
  });

  it("keeps generic agent task rows openable without compacting them away", () => {
    const state = deriveAgentActivityTimelineState([
      workEntry({
        id: "agent-task-1",
        label: "Find changelog implementation",
        itemType: "collab_agent_tool_call",
        toolTitle: "Find changelog implementation",
        subagentAction: {
          tool: "task",
          status: "completed",
          summaryText: "Agent activity",
          prompt: "Explore this codebase to find the changelog feature.",
        },
      }),
    ]);

    expect(state.timelineWorkEntries.map((entry) => entry.id)).toEqual(["agent-task-1"]);
    expect(isAgentActivityWorkEntry(state.timelineWorkEntries[0]!)).toBe(true);
    expect(state.detailById.get("agent-task-1")).toMatchObject({
      title: "Find changelog implementation",
      summary: "Explore this codebase to find the changelog feature.",
    });
  });

  it("uses the prompt as the detail summary when the agent result is long", () => {
    const state = deriveAgentActivityTimelineState([
      workEntry({
        id: "agent-task-1",
        label: "Find changelog implementation",
        itemType: "collab_agent_tool_call",
        toolTitle: "Find changelog implementation",
        detail: "Full changelog report\nwith many file references and implementation notes.",
        subagentAction: {
          tool: "task",
          status: "completed",
          summaryText: "Agent activity",
          prompt: "Explore this codebase to find the changelog feature.",
        },
      }),
    ]);

    expect(state.detailById.get("agent-task-1")).toMatchObject({
      summary: "Explore this codebase to find the changelog feature.",
    });
    expect(state.timelineWorkEntries[0]).toMatchObject({
      detail: "Full changelog report\nwith many file references and implementation notes.",
    });
  });

  it("classifies provider_reasoning via activityKind (not just text heading)", () => {
    const entry = workEntry({
      id: "r1",
      label: "any-text",
      activityKind: "provider_reasoning",
      detail: "Thinking about the problem",
    });
    expect(isReasoningUpdateWorkEntry(entry)).toBe(true);
    expect(formatAgentActivityEntryPreview(entry)).toBe("Thinking about the problem");
  });

  it("recognizes skill, subagent, and explore activityKind values", () => {
    expect(isSkillDispatchEntry(workEntry({ id: "s1", activityKind: "provider_skill_dispatched" }))).toBe(true);
    expect(isSubagentEventEntry(workEntry({ id: "s2", activityKind: "provider_subagent_started" }))).toBe(true);
    expect(isSubagentEventEntry(workEntry({ id: "s3", activityKind: "provider_subagent_completed" }))).toBe(true);
    expect(isExploreEventEntry(workEntry({ id: "e1", activityKind: "provider_explore_started" }))).toBe(true);
    expect(isExploreEventEntry(workEntry({ id: "e2", activityKind: "provider_explore_updated" }))).toBe(true);
  });

  it("compacts consecutive explore events into a single timeline row", () => {
    const state = deriveAgentActivityTimelineState([
      workEntry({
        id: "explore-1",
        activityKind: "provider_explore_started",
        label: "Exploring: how does the adapter work",
        detail: "Exploring: how does the adapter work",
      }),
      workEntry({
        id: "explore-2",
        activityKind: "provider_explore_updated",
        label: "Found 12 files",
        detail: "Found 12 files",
      }),
      workEntry({
        id: "explore-3",
        activityKind: "provider_explore_updated",
        label: "Found 4 more",
        detail: "Found 4 more",
      }),
      workEntry({
        id: "tool-after",
        label: "Read",
        tone: "tool",
      }),
    ]);

    expect(state.timelineWorkEntries.map((entry) => entry.id)).toEqual([
      "agent-explore:explore-1",
      "tool-after",
    ]);
    expect(state.timelineWorkEntries[0]).toMatchObject({
      label: "Exploring",
      toolTitle: "Exploring",
      tone: "info",
      preview: "3 finds - Found 4 more",
    });
    expect(state.detailById.get("agent-explore:explore-1")?.entries).toHaveLength(3);
  });

  it("renders a standalone skill dispatch row without collapsing", () => {
    const state = deriveAgentActivityTimelineState([
      workEntry({
        id: "skill-1",
        activityKind: "provider_skill_dispatched",
        label: "Skill dispatched",
        detail: "kmr-build",
      }),
    ]);

    expect(state.timelineWorkEntries.map((entry) => entry.id)).toEqual(["skill-1"]);
    expect(isAgentActivityWorkEntry(state.timelineWorkEntries[0]!)).toBe(true);
    expect(state.detailById.get("skill-1")?.title).toBe("Skill");
  });

  it("renders a subagent started+completed pair as two timeline rows (no collapse)", () => {
    const state = deriveAgentActivityTimelineState([
      workEntry({
        id: "subagent-1",
        activityKind: "provider_subagent_started",
        label: "Subagent started: code-reviewer",
        detail: "Subagent started: code-reviewer",
      }),
      workEntry({
        id: "subagent-2",
        activityKind: "provider_subagent_completed",
        label: "Subagent completed: code-reviewer",
        detail: "LGTM",
      }),
    ]);

    expect(state.timelineWorkEntries.map((entry) => entry.id)).toEqual([
      "subagent-1",
      "subagent-2",
    ]);
    expect(state.detailById.get("subagent-1")?.title).toBe("Subagent");
    expect(state.detailById.get("subagent-2")?.title).toBe("Subagent");
  });
});

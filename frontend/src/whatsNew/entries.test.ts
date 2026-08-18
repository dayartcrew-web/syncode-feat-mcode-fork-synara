// FILE: entries.test.ts
// Purpose: Guard the curated release history so it only contains this fork's
// releases. The UI was vendored from upstream MCode together with its entire
// changelog (0.0.29–0.3.0); those notes described features this fork never
// shipped and collided with fork versions 0.1.0–0.1.9. These tests fail the
// moment upstream history (or stray upstream branding) sneaks back in.
// Layer: unit tests over the static entries data.

import { describe, expect, it } from "vitest";

import { WHATS_NEW_ENTRIES } from "./entries";
import { compareVersions } from "./logic";

/** Releases this fork has actually tagged. v0.1.5 was never tagged (skipped). */
const FORK_VERSIONS = [
  "0.1.0", "0.1.1", "0.1.2", "0.1.3", "0.1.4", "0.1.6", "0.1.7", "0.1.8",
  "0.1.9", "0.1.10", "0.1.11", "0.1.12", "0.1.13", "0.1.14", "0.1.15",
  "0.1.16", "0.1.17", "0.1.18", "0.1.19", "0.1.20", "0.1.21", "0.1.22",
  "0.1.23", "0.1.24",
] as const;

describe("WHATS_NEW_ENTRIES release provenance", () => {
  it("contains exactly this fork's tagged versions, no upstream history", () => {
    const entryVersions = WHATS_NEW_ENTRIES.map((entry) => entry.version);
    expect(new Set(entryVersions)).toEqual(new Set(FORK_VERSIONS));
  });

  it("has no duplicate versions", () => {
    const versions = WHATS_NEW_ENTRIES.map((entry) => entry.version);
    expect(new Set(versions).size).toBe(versions.length);
  });

  it("is authored newest-first", () => {
    for (let index = 1; index < WHATS_NEW_ENTRIES.length; index += 1) {
      const previous = WHATS_NEW_ENTRIES[index - 1];
      const current = WHATS_NEW_ENTRIES[index];
      if (previous !== undefined && current !== undefined) {
        expect(
          compareVersions(current.version, previous.version),
          `${current.version} should not sort after ${previous.version}`,
        ).toBeLessThanOrEqual(0);
      }
    }
  });

  it("does not carry upstream branding in feature text", () => {
    const upstreamMarkers = /\bmcode\b|\bt3code\b|\bdpcode\b/i;
    for (const entry of WHATS_NEW_ENTRIES) {
      for (const feature of entry.features) {
        const text = `${feature.title} ${feature.description} ${feature.details ?? ""}`;
        expect(
          upstreamMarkers.test(text),
          `${entry.version} / ${feature.id}: upstream branding found in "${text.slice(0, 80)}"`,
        ).toBe(false);
      }
    }
  });

  it("gives every release at least one feature and a date", () => {
    for (const entry of WHATS_NEW_ENTRIES) {
      expect(entry.features.length, entry.version).toBeGreaterThan(0);
      expect(entry.date.trim(), entry.version).not.toBe("");
    }
  });
});

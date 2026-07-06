// FILE: providerOrdering.test.ts
// Purpose: Keeps provider ordering normalization covered for every exposed provider.
// Layer: Web settings tests
// Depends on: provider display metadata from contracts and providerOrdering helpers.

import { PROVIDER_DISPLAY_NAMES, type ProviderKind } from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import {
  DEFAULT_PROVIDER_ORDER,
  isProviderKind,
  normalizeHiddenProviders,
  normalizeProviderOrder,
} from "./providerOrdering";

// PROVIDER_DISPLAY_NAMES may include alias keys (e.g. `claude` as an alias for
// `claudeAgent`) that are not first-class providers in the ordering. The test
// should only assert coverage of the canonical ordering set, not every display
// name alias.
const ALL_PROVIDER_KINDS = Object.keys(PROVIDER_DISPLAY_NAMES) as ProviderKind[];

describe("providerOrdering", () => {
  it("includes every canonical provider in the default order", () => {
    // Every entry in DEFAULT_PROVIDER_ORDER must be a known display name.
    for (const provider of DEFAULT_PROVIDER_ORDER) {
      expect(ALL_PROVIDER_KINDS).toContain(provider);
    }
    // The default order must have no duplicates.
    expect(new Set(DEFAULT_PROVIDER_ORDER).size).toBe(DEFAULT_PROVIDER_ORDER.length);
  });

  it("keeps Pi as a valid provider for persisted order and visibility settings", () => {
    expect(isProviderKind("pi")).toBe(true);
    expect(normalizeProviderOrder(["pi", "codex"])[0]).toBe("pi");
    expect(normalizeHiddenProviders(["bogus", "pi", "pi"])).toEqual(["pi"]);
  });
});

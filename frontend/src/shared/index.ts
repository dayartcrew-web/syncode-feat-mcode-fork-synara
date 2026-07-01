/**
 * @t3tools/shared local shim — vendored source bridge.
 *
 * MCode's `apps/web` consumes `@t3tools/shared` via subpath imports
 * (`@t3tools/shared/model`, `@t3tools/shared/git`, ...) defined in
 * `packages/shared/package.json` `exports`. A source scan of the vendored
 * tree found 25 distinct subpaths in use.
 *
 * Rather than stub each subpath, this shim VENDORS the actual MCode
 * `packages/shared/src/*.ts` source (35 non-test modules) so that:
 *   1. Every `@t3tools/shared/<subpath>` import resolves to a real module
 *      (no `Cannot find module '@t3tools/shared/...'` errors).
 *   2. The shared modules' own `import { ... } from "@t3tools/contracts"`
 *      statements hit the contracts shim — surfacing the SAME missing-export
 *      type holes as apps/web. This is the intended hole-driving signal.
 *
 * The `tsconfig`/`vite` aliases map:
 *   `@t3tools/shared`      → `./src/shared`        (this barrel)
 *   `@t3tools/shared/*`    → `./src/shared/*`      (subpath modules)
 *
 * See docs/CONTRACTS-BRIDGE-DESIGN.md §3.1 for the shim strategy.
 *
 * Tier: vendor + flatten (T2). Type holes in contracts/effect are T3/T4.
 */

// Re-export every vendored module so the bare `@t3tools/shared` import also
// resolves. Subpath imports resolve directly to the sibling files via the
// `@t3tools/shared/*` wildcard alias.
export * from "./model";
export * from "./git";
export * from "./logging";
export * from "./errorMessages";
export * from "./shell";
export * from "./windowsProcess";
export * from "./codexConfig";
export * from "./desktopChrome";
export * from "./Net";
export * from "./DrainableWorker";
export * from "./chatThreads";
export * from "./localServers";
export * from "./browserShortcuts";
export * from "./browserSession";
export * from "./conversationEdit";
export * from "./schemaJson";
export * from "./Struct";
export * from "./serverSettings";
export * from "./agentMentions";
export * from "./composerSlashCommands";
export * from "./editorIcons";
export * from "./formatBytes";
export * from "./localPreviewFiles";
export * from "./path";
export * from "./pinnedMessages";
export * from "./providerUsage";
export * from "./subagents";
export * from "./terminalThreads";
export * from "./text";
export * from "./threadEnvironment";
export * from "./threadMarkers";
export * from "./threadSummary";
export * from "./threadWorkspace";
export * from "./toolOutputSummary";
export * from "./worktreeHandoff";

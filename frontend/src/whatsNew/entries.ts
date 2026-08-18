// FILE: whatsNew/entries.ts
// Purpose: Curated "What's new" changelog rendered in the post-update dialog
// and the settings Release history view.
// Layer: static data consumed by `useWhatsNew`, `WhatsNewDialog`, and
// `ChangelogAccordion`.
//
// Authoring guide
// ---------------
//   - Prepend new releases so the file reads newest-first (the UI sorts too,
//     but keeping the source tidy makes PRs easier to review).
//   - `version` must match the version in `crates/syncode-tauri/tauri.conf.json`
//     exactly (mirrored into `import.meta.env.APP_VERSION` by vite.config.ts).
//     The logic compares versions as semver and only opens the dialog when the
//     installed build has a curated entry here.
//   - Only add entries for releases this fork has actually tagged — never copy
//     upstream MCode history. `entries.test.ts` guards this.
//   - `date` is rendered verbatim — pick whatever format you want (e.g.
//     `"Aug 19"`, `"2026-08-19"`), just be consistent release-to-release.
//   - Each feature takes an `id` (stable, unique per release), a short
//     `title`, a marketing `description`, and optionally a `details` note for
//     the longer technical blurb.

import type { WhatsNewEntry } from "./logic";

export const WHATS_NEW_ENTRIES: readonly WhatsNewEntry[] = [
  {
    version: "0.1.24",
    date: "Aug 19",
    features: [
      {
        id: "activity-summary-text",
        title: "Work rows show real descriptions",
        description:
          "Activity summaries now carry their description text through to work rows, so the sidebar shows what actually happened instead of a blank label.",
        details:
          "The contracts layer forwards the activityLogged description as the summary field, and every real invoke call in the desktop native API is now covered by mockIPC tests.",
      },
    ],
  },
  {
    version: "0.1.23",
    date: "Aug 19",
    features: [
      {
        id: "release-version-sync",
        title: "Releases always report the right version",
        description:
          "The build pipeline now syncs the desktop app version from the release tag before building, so installers and update feeds can never disagree with the published release.",
        details:
          "Also includes an end-to-end test for live provider event push over a real WebSocket and a fix that keeps WebSocket static constants intact in the ws-probe e2e helper.",
      },
    ],
  },
  {
    version: "0.1.22",
    date: "Aug 14",
    features: [
      {
        id: "claude-reasoning-sidebar",
        title: "Claude reasoning lands in the sidebar",
        description:
          "Claude's live reasoning state and label now flow through the activity sidebar summary, matching what Codex and ACP providers already showed.",
        details:
          "Reasoning events emitted by the Claude adapter are wired into the shared activity summary used by the sidebar.",
      },
    ],
  },
  {
    version: "0.1.21",
    date: "Aug 13",
    features: [
      {
        id: "adapter-activity-variants",
        title: "More providers report what they're doing",
        description:
          "Codex, ACP, and OpenCode adapters now emit reasoning and explore activity variants, so the live activity view covers more of what agents actually do.",
        details:
          "Activities are scoped to their turn, Agent events route to the subagent view, and three previously missing activity variants are now emitted.",
      },
    ],
  },
  {
    version: "0.1.20",
    date: "Aug 12",
    features: [
      {
        id: "realtime-activity-chat",
        title: "See agents think, explore, and delegate in real time",
        description:
          "The chat surface now surfaces real-time reasoning, skill use, subagent work, and explore activity while a turn is running.",
        details:
          "Activity events stream into the chat view so long-running turns show live progress instead of silence.",
      },
    ],
  },
  {
    version: "0.1.19",
    date: "Aug 11",
    features: [
      {
        id: "dom-confirm-fallback",
        title: "Delete and archive work again on desktop",
        description:
          "Confirmation dialogs now fall back to a DOM prompt when the desktop dialog bridge is unavailable, so destructive actions never silently stop working.",
        details:
          "dialogs.confirm routes through a DOM fallback, restoring delete/archive flows inside the desktop shell.",
      },
    ],
  },
  {
    version: "0.1.18",
    date: "Aug 11",
    features: [
      {
        id: "lazy-provider-boot",
        title: "Faster startup",
        description:
          "Provider processes now spawn lazily and boot waits time out instead of hanging, so the app reaches a usable state noticeably sooner.",
        details:
          "Boot no longer blocks on eager provider spawn; a boot timeout keeps slow providers from stalling the whole window.",
      },
    ],
  },
  {
    version: "0.1.17",
    date: "Jul 23",
    features: [
      {
        id: "about-update-checker",
        title: "Check for updates from Settings",
        description:
          "The About section now knows the real installed version and includes an update checker, replacing placeholder values from the vendored UI.",
        details:
          "APP_VERSION is read from the desktop build config and the UpdateChecker control is wired into the About panel.",
      },
    ],
  },
  {
    version: "0.1.16",
    date: "Jul 23",
    features: [
      {
        id: "context-menu-fallback",
        title: "Right-click menus work on desktop",
        description:
          "Context menus now route through a fallback implementation, fixing menus that previously returned nothing on the desktop build.",
        details:
          "contextMenu.show is wired to the fallback path instead of returning null; start_turn tests were tightened to match the doubled event count.",
      },
    ],
  },
  {
    version: "0.1.15",
    date: "Jul 23",
    features: [
      {
        id: "persist-user-message",
        title: "Your messages survive a reopened thread",
        description:
          "User messages sent at the start of a turn are now persisted immediately, so reopening a thread no longer drops the prompt that kicked it off.",
        details:
          "start_turn persists the user message before the turn runs, preventing it from disappearing on thread reopen.",
      },
    ],
  },
  {
    version: "0.1.14",
    date: "Jul 23",
    features: [
      {
        id: "update-button-worktrees",
        title: "Update button and global worktrees setting",
        description:
          "A dedicated update button joins the desktop controls, and global worktrees can now be configured from settings.",
        details:
          "Adds the desktop update button affordance and a global worktrees settings surface.",
      },
    ],
  },
  {
    version: "0.1.13",
    date: "Jul 23",
    features: [
      {
        id: "stale-draft-cleanup",
        title: "Cleaner drafts after the database move",
        description:
          "Stale draft thread references left over from the database relocation are now cleared, so chats open on the right thread instead of a dangling draft.",
        details:
          "Migration v7 clears stale projectDraftThreadIdByProjectId entries created by the AppData move.",
      },
    ],
  },
  {
    version: "0.1.12",
    date: "Jul 23",
    features: [
      {
        id: "provider-probe-bin-resolver",
        title: "Provider status detects CLIs on release builds",
        description:
          "Provider status checks now resolve binaries the same way spawns do, fixing providers that looked missing in packaged builds with a narrower PATH.",
        details:
          "The status probe uses bin_resolver, so detection matches what actually runs outside of dev environments.",
      },
    ],
  },
  {
    version: "0.1.11",
    date: "Jul 23",
    features: [
      {
        id: "appdata-db-anchor",
        title: "Database anchored to AppData",
        description:
          "The local database now lives under AppData with a stable anchor, and the default provider setting drives the composer's armed provider.",
        details:
          "DB paths anchor to the OS AppData location and default_provider is wired through to provider arming.",
      },
    ],
  },
  {
    version: "0.1.10",
    date: "Jul 23",
    features: [
      {
        id: "global-cmd-window-hide",
        title: "No more flashing console windows",
        description:
          "All subprocess spawns across the app now share one chokepoint that hides Windows console windows — providers, git, and tooling all stay invisible.",
        details:
          "A shared chokepoint for every subprocess spawn hides cmd windows globally, verified by a WebdriverIO functional harness with an embedded WebDriver and console-window hide specs on shell spawns.",
      },
    ],
  },
  {
    version: "0.1.9",
    date: "Jul 22",
    features: [
      {
        id: "hide-provider-cmd-windows",
        title: "Provider CLIs run quietly",
        description:
          "Provider command-line tools no longer pop up console windows on Windows, and devtools are on by default for easier debugging.",
        details:
          "Hides provider CLI windows on Windows and makes devtools default-on in the desktop shell.",
      },
    ],
  },
  {
    version: "0.1.8",
    date: "Jul 22",
    features: [
      {
        id: "cache-cascade-normalize",
        title: "Squashed desktop-only cache bugs",
        description:
          "Fixed a cache cascade issue and normalized provider identity handling that only affected the packaged desktop app.",
        details:
          "Desktop-only cache cascade fix plus provider normalize corrections that dev builds never hit.",
      },
    ],
  },
  {
    version: "0.1.7",
    date: "Jul 22",
    features: [
      {
        id: "desktop-stubs-eliminated",
        title: "Desktop stubs became real implementations",
        description:
          "Settings, git, skills, MCP, terminal, devtools, and updater surfaces that were stubs in the vendored UI now work for real on desktop.",
        details:
          "Eliminates the desktop stub layer across settings/git/skills/mcp/terminal/devtools/updater so every panel is backed by a real implementation.",
      },
    ],
  },
  {
    version: "0.1.6",
    date: "Jul 22",
    features: [
      {
        id: "ws-transport-dispatcher",
        title: "Stable WebSocket transport",
        description:
          "The WebSocket transport no longer throws unsupported-operation errors under the desktop dispatcher, and duplicate IPC git operations were removed in favor of the WS path.",
        details:
          "Eliminates UnsupportedError paths in TransportDispatcher, routes git ops through WebSocket instead of duplicated IPC, and documents all noopUnsubscribe sites as platform-limited.",
      },
    ],
  },
  {
    version: "0.1.4",
    date: "Jul 21",
    features: [
      {
        id: "in-process-ws-csp",
        title: "Packaged app talks to its backend again",
        description:
          "Dropped the dev-only server URL and opened CSP for the in-process WebSocket backend, restoring connectivity in packaged builds.",
        details:
          "Removes devUrl and adjusts CSP so the bundled frontend can reach the in-process WS backend.",
      },
    ],
  },
  {
    version: "0.1.3",
    date: "Jul 21",
    features: [
      {
        id: "dispatch-crash-fixes",
        title: "Two startup crashes fixed",
        description:
          "Fixed crashes in the event dispatch path and the browser-use open-panel request that could take the desktop window down on launch.",
        details:
          "Patches the dispatch and onBrowserUseOpenPanelRequest crash paths found in early desktop testing.",
      },
    ],
  },
  {
    version: "0.1.2",
    date: "Jul 21",
    features: [
      {
        id: "welcome-push-file-logger",
        title: "Welcome screen and file logging",
        description:
          "The welcome flow now pushes through to the app shell on desktop, and a file logger captures backend output for easier bug reports.",
        details:
          "Wires the welcome push event and adds a file logger; changelog automation also got more resilient.",
      },
    ],
  },
  {
    version: "0.1.1",
    date: "Jul 21",
    features: [
      {
        id: "ws-port-panic-log",
        title: "New WebSocket port + panic log",
        description:
          "The backend WebSocket moved off its default port to avoid collisions with other local tooling, and panics are now logged to file.",
        details:
          "Moves the WS default port from 30101 to 33101 to avoid a masday collision and adds a panic log.",
      },
    ],
  },
  {
    version: "0.1.0",
    date: "Jul 21",
    features: [
      {
        id: "first-desktop-release",
        title: "First desktop release",
        description:
          "The initial packaged desktop build: bundled frontend, in-process Rust backend over WebSocket, and auto-generated release changelogs.",
        details:
          "Ships the Tauri packaging pipeline with CI-generated changelog and release notes; fixed the macOS icon error blocking the first build.",
      },
    ],
  },
];

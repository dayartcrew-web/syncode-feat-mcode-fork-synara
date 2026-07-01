# Frontend Comparison: MCode (`apps/web`) vs Syncode (`frontend/`)

> ⚠️ **STATUS (2026-07-02) — PLANNING DOC, SUPERSEDED.** Authored as a forward-looking gap analysis/roadmap for the (currently minimal) React+Vite frontend. The backend has since been substantially built — see [`ARCHITECTURE.md`](./ARCHITECTURE.md) and [`.masday/intel/`](../.masday/intel/README.md) for current state. Retained as the original frontend roadmap.
>
> ✅ **RECOMMENDED PATH (added 2026-07-02): clone + re-wire.** Rather than building the UI from scratch (the §12 roadmap, ~16-18 weeks), clone MCode's `apps/web` (752 files, production-grade) and **re-wire** its transport to Syncode's Rust endpoints — replace MCode's Effect-RPC-over-WS + Effect-Schema contracts (`@t3tools/contracts`) with Syncode's **plain JSON-RPC-over-WS + ts-rs-generated types**, and swap the Electron `nativeApi` shell for **Tauri v2 IPC**. The Effect runtime is confined to ~15 boundary files (not pervasive across the 752-file tree), so the dominant cost is **contract/type re-mapping**, not the UI — see §14.3. Body below is the original analysis, left intact.

> **Tanggal**: 2026-06-27
> **Scope**: Perbandingan detail frontend MCode (React + Vite + Electron) → Syncode (React + Vite + Tauri)
> **Tujuan**: Mengidentifikasi semua gap dan membuat roadmap implementasi frontend Syncode

---

## 1. Executive Summary

| Dimension | MCode (`apps/web`) | Syncode (`frontend/`) | Gap |
|-----------|-------------------|----------------------|-----|
| **Maturity** | Production-grade, 752 source files | MVP skeleton, 36 src files (8 handwritten + 27 ts-rs types) | 🔴 ~95% |
| **Components** | ~100+ organized components | 5 basic components | 🔴 ~95% |
| **State Management** | Zustand (15+ stores) + React Query | React useState/useEffect only | 🔴 100% |
| **Routing** | TanStack Router (file-based, 10+ routes) | None (single page) | 🔴 100% |
| **UI Library** | shadcn/ui v4 (48 primitives) + CVA + tailwind-merge | None (inline styles) | 🔴 100% |
| **CSS Framework** | Tailwind CSS v4 (no config file) | None (inline `style={{}}`) | 🔴 100% |
| **Rich Text** | Lexical (composer) | Plain `<input>` text | 🔴 100% |
| **Markdown** | react-markdown + Shiki + KaTeX + remark-gfm | None | 🔴 100% |
| **Terminal** | xterm.js + WebGL + 6 addons | Polling-based custom renderer | 🟡 70% |
| **Diff Viewer** | @pierre/diffs | None | 🔴 100% |
| **Drag & Drop** | @dnd-kit (kanban, sidebar, settings) | None | 🔴 100% |
| **Animations** | @formkit/auto-animate + tw-animate-css | None (CSS blink only) | 🔴 100% |
| **Testing** | Vitest + Playwright browser tests | None | 🔴 100% |
| **Icons** | Tabler Icons (600+ SVGs) + react-icons | Emoji only | 🔴 100% |
| **Theme System** | Full light/dark + custom packs + code theme | Hardcoded dark colors | 🔴 100% |
| **Desktop Shell** | Electron (nativeApi bridge) | Tauri v2 IPC | 🟢 Different but functional |
| **API Layer** | Effect RPC over WebSocket + typed contracts | Raw JSON-RPC with setTimeout | 🟡 60% |
| **Type Safety** | Effect Schema (contracts package) + auto-generated | ts-rs auto-generated (Rust→TS) | 🟢 Comparable |

**Overall Frontend Completeness: ~5% of MCode parity**

> 📌 **Reality check (2026-07-02):** the original "7 source files / 6 components" figures above are stale. `frontend/src` actually holds **36 files**: 8 handwritten (`App.tsx`, `main.tsx`, **5 components**, 2 hooks) + **27 auto-generated `ts-rs` type files** (`EntityId`, `SessionView`, `PushEvent`, `JsonRpcRequestView`, …). Runtime deps are **3** (`react`, `react-dom`, `@tauri-apps/api`). The auto-generated type surface is the one area where Syncode already **exceeds** hand-rolling — it is the natural keystone for the clone + re-wire contracts bridge (§14.3).

---

## 2. Technology Stack Comparison

### 2.1 Build & Runtime

| Aspect | MCode | Syncode | Status |
|--------|-------|---------|--------|
| **React** | 19.x | 19.x | ✅ Match |
| **Vite** | 8.x | 6.x | 🟡 Needs upgrade to v8 |
| **TypeScript** | ~5.7 | 5.7 | ✅ Match |
| **Module system** | ESM | ESM | ✅ Match |
| **Desktop shell** | Electron | Tauri v2 | 🟢 Different (Tauri is target) |
| **Dev server port** | 5733 | 5173 | 🟢 Config difference only |
| **React Compiler** | babel-plugin-react-compiler (active) | None | 🟡 Optional optimization |
| **Path alias** | `~/*` → `./src/*` | `@/*` → `./src/*` | 🟢 Convention difference |
| **Monorepo** | Turborepo | None (single Cargo workspace) | 🟢 N/A for frontend |

### 2.2 Dependencies Inventory

#### MCode Dependencies (50+ packages)
```
# Routing & Server State
@tanstack/react-router, @tanstack/react-query, @tanstack/react-virtual, @tanstack/react-pacer

# State Management
zustand

# UI Framework
shadcn (via components.json), class-variance-authority, tailwind-merge, clsx, cmdk

# Icons
@tabler/icons-react, react-icons

# Drag & Drop
@dnd-kit/core, @dnd-kit/sortable, @dnd-kit/modifiers

# Rich Text / Editor
lexical, @lexical/react

# Terminal
@xterm/xterm, @xterm/addon-clipboard, @xterm/addon-fit, @xterm/addon-image,
@xterm/addon-ligatures, @xterm/addon-search, @xterm/addon-unicode11,
@xterm/addon-webgl

# Markdown & Math
react-markdown, remark-gfm, remark-math, rehype-katex, katex

# Code Highlighting
shiki (via ChatMarkdown)

# PDF
pdfjs-dist

# Diffs
@pierre/diffs

# Animation
@formkit/auto-animate, tw-animate-css

# Typography
@fontsource-variable/inter, @fontsource-variable/jetbrains-mono

# Utility
html-to-image, react-colorful, @legendapp/list, effect

# Tauri
(none — uses Electron)
```

#### Syncode Dependencies (2 packages)
```
# Runtime
react, react-dom

# Desktop
@tauri-apps/api

# Dev Only
@tauri-apps/cli, @types/react, @types/react-dom, @vitejs/plugin-react, typescript, vite
```

### 2.3 Missing Dependencies in Syncode

| Priority | Package(s) | Purpose | Phase |
|----------|-----------|---------|-------|
| **P0** | `tailwindcss` v4, `@tailwindcss/vite` | CSS framework | Foundation |
| **P0** | `clsx`, `tailwind-merge`, `class-variance-authority` | `cn()` utility + CVA | Foundation |
| **P0** | `zustand` | Client state management | Foundation |
| **P0** | `@tanstack/react-query` | Server state caching | Foundation |
| **P0** | `@xterm/xterm` + addons | Proper terminal | Phase 4 |
| **P0** | `react-markdown`, `remark-gfm` | Markdown rendering | Phase 2 |
| **P1** | `@tanstack/react-router` | Client-side routing | Phase 3 |
| **P1** | `@tabler/icons-react` | Icon library | Foundation |
| **P1** | `lexical`, `@lexical/react` | Rich text composer | Phase 3 |
| **P1** | `@formkit/auto-animate` | Smooth animations | Phase 3 |
| **P1** | `cmdk` | Command palette | Phase 3 |
| **P2** | `remark-math`, `rehype-katex`, `katex` | Math rendering | Phase 4 |
| **P2** | `@dnd-kit/*` | Drag & drop (kanban, sidebar) | Phase 5 |
| **P2** | `@pierre/diffs` or `react-diff-viewer-continued` | Diff viewer | Phase 3 |
| **P2** | `pdfjs-dist` | PDF viewing | Phase 5 |
| **P2** | `shiki` | Code syntax highlighting | Phase 2 |
| **P2** | `@fontsource-variable/inter` | UI font | Phase 3 |
| **P2** | `@fontsource-variable/jetbrains-mono` | Mono font | Phase 3 |
| **P3** | `html-to-image`, `react-colorful` | Utilities | Future |
| **P3** | `@tanstack/react-virtual` | Virtual scrolling | Phase 4 |
| **P3** | `vitest`, `@vitest/browser-playwright`, `playwright` | Testing | Ongoing |

---

## 3. Architecture Comparison

### 3.1 MCode Architecture (Production)

```
┌─────────────────────────────────────────────────────────────────┐
│  index.html → main.tsx → router.ts (TanStack Router)           │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Root Route (__root.tsx)                                 │   │
│  │  ├── QueryClientProvider (TanStack React Query)          │   │
│  │  ├── StoreProvider (Zustand context)                     │   │
│  │  ├── ToastProvider                                       │   │
│  │  ├── EventRouter (WebSocket push → store + query)        │   │
│  │  ├── ShortcutsDialog                                     │   │
│  │  ├── WhatsNewDialog                                     │   │
│  │  └── DesktopWindowControls                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Layout Route (_chat.tsx)                                │   │
│  │  ├── Resizable Collapsible Sidebar                       │   │
│  │  │   ├── Project/Thread Tree                             │   │
│  │  │   └── Thread Retention Toast                         │   │
│  │  ├── Keyboard Shortcuts System                           │   │
│  │  └── <Outlet /> (child routes)                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Pages (10 routes)                                       │   │
│  │  ├── / (landing, restores last thread)                  │   │
│  │  ├── /$threadId (chat thread view)                       │   │
│  │  ├── /automations, /automations/$id                     │   │
│  │  ├── /kanban, /kanban/$projectId                        │   │
│  │  ├── /plugins                                           │   │
│  │  ├── /settings                                          │   │
│  │  ├── /workspace, /workspace/$id                         │   │
│  │  └── /worldcup (easter egg)                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  State: Zustand (15+ stores) + React Query (6+ query modules)    │
│  API: Effect RPC over WebSocket (typed contracts)                │
│  Theme: CSS custom properties (50+ variables), 3 modes          │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Syncode Architecture (Current MVP)

```
┌──────────────────────────────────────────────────┐
│  index.html → main.tsx → App.tsx                  │
│                                                   │
│  ┌───────────────────────────────────────────┐   │
│  │  App (single component, no router)         │   │
│  │  ├── useWebSocket (JSON-RPC hook)          │   │
│  │  ├── useState (all state inline)           │   │
│  │  ├── Sidebar                              │   │
│  │  │   ├── ProviderSwitcher                  │   │
│  │  │   ├── ThreadList                        │   │
│  │  │   └── New Thread Button                │   │
│  │  └── Main Area                            │   │
│  │       ├── ChatView (bubble layout)         │   │
│  │       ├── Terminal Toggle                  │   │
│  │       └── TerminalView (when visible)      │   │
│  └───────────────────────────────────────────┘   │
│                                                   │
│  State: React useState/useEffect (inline)          │
│  API: Raw JSON-RPC with 10s timeout              │
│  Theme: Hardcoded hex colors (#1a1a2e, #e94560)  │
└──────────────────────────────────────────────────┘
```

### 3.3 Architecture Gaps

| Concern | MCode Pattern | Syncode Current | Gap |
|---------|--------------|----------------|-----|
| **Routing** | TanStack Router (file-based, code-split) | None | Must add router |
| **State** | Zustand (normalized, persisted, 15+ stores) | useState only | Must add Zustand |
| **Server cache** | React Query (6+ query option modules) | None (manual fetch) | Must add React Query |
| **Event system** | EventRouter (buffer, coalesce, flush 100ms) | Simple `onPush` callback | Must rebuild |
| **Provider tree** | QueryClient + Store + Toast + Event | Single hook | Must add providers |
| **Layout system** | Resizable sidebar + content card | Fixed flex layout | Must add resize |
| **Error boundaries** | Full error boundary with retry | None | Must add |

---

## 4. Component-by-Component Gap Analysis

### 4.1 Chat System

| Component | MCode | Syncode | Gap |
|-----------|-------|---------|-----|
| **ChatTranscriptPane** | Virtualized message timeline with scroll anchoring | Simple scrollable div | 🔴 |
| **ChatMarkdown** | Full GFM, Shiki code highlight, KaTeX math, file comments, thread markers | Plain text rendering | 🔴 |
| **ComposerStackedPanel** | Lexical rich text editor | Plain `<input>` element | 🔴 |
| **ComposerSuggestions** | Slash commands (`/model`), @mentions (files, agents, skills) | None | 🔴 |
| **ComposerImageAttachmentChip** | Drag-drop image/file attachments | None | 🔴 |
| **ComposerVoiceButton** | Voice recording with transcription | None | 🔴 |
| **ComposerModelEffortPicker** | Inline model/effort selection | None | 🔴 |
| **ComposerPendingApprovalPanel** | Approval request UI (approve/deny tool use) | None | 🔴 |
| **ComposerPendingUserInputPanel** | User input prompts from AI | None | 🔴 |
| **ComposerPlanFollowUpBanner** | Proposed plan cards with accept/reject | None | 🔴 |
| **ProviderModelPicker** | Multi-provider model selector | ProviderSwitcher (hardcoded) | 🟡 |
| **ProviderHealthBanner** | Provider status/rate limit banners | None | 🔴 |
| **RateLimitBanner** | Rate limit warning UI | None | 🔴 |
| **MessagesTimeline** | Virtualized with @tanstack/react-virtual | Linear render | 🔴 |
| **ExpandedImagePreview** | Click-to-expand image viewer | None | 🔴 |
| **ToolCallDetailsDialog** | Expandable tool call details | Basic display | 🟡 |
| **AttachmentCard** | File attachment preview | None | 🔴 |
| **FileDiffView** | Inline diff within chat | None | 🔴 |
| **InlineAgentChip** | Agent mention badges in messages | None | 🔴 |
| **InlineSkillChip** | Skill mention badges | None | 🔴 |
| **ChatEmptyStateHero** | Rich empty/landing state | Simple emoji + text | 🟡 |
| **ThreadErrorBanner** | Error display with retry | None | 🔴 |
| **ChatScroll** | Smart scroll anchoring (preserve on new content) | Browser default scroll | 🔴 |

### 4.2 Terminal System

| Component | MCode | Syncode | Gap |
|-----------|-------|---------|-----|
| **TerminalViewportPane** | xterm.js with WebGL renderer, 6 addons | Polling-based div renderer | 🟡 |
| **TerminalChrome** | Tab management (create/switch/close) | Session switcher | 🟡 |
| **TerminalLayout** | Resizable terminal panel | Fixed height toggle | 🟡 |
| **TerminalActivityIndicator** | Visual activity indicator | None | 🔴 |
| **TerminalIdentityIcon** | Shell type icon | None | 🔴 |
| **TerminalRuntime** | Full PTY integration via WebSocket push | 200ms polling interval | 🟡 |

### 4.3 Git & Diff

| Component | MCode | Syncode | Gap |
|-----------|-------|---------|-----|
| **GitPanel** | Branch info, file statuses, commit, recent commits | GitPanel (exists but NOT wired in App.tsx) | 🟡 |
| **BranchToolbar** | Branch creation, checkout, worktree | None | 🔴 |
| **DiffPanel** | Full diff viewer with file list, patch viewport, toolbar | None | 🔴 |
| **DiffPanelFileList** | File-by-file diff navigation | None | 🔴 |
| **DiffPanelPatchViewport** | Unified diff rendering (@pierre/diffs) | None | 🔴 |
| **DiffStatLabel** | Changed/added/removed line counts | None | 🔴 |
| **DirectoryTreeBrowser** | Workspace file browser | None | 🔴 |
| **DirectoryTreePicker** | File/directory picker for context | None | 🔴 |
| **ReviewChangesButton** | Review AI changes button | None | 🔴 |
| **FileLineCommentBox** | Inline code comments | None | 🔴 |

### 4.4 Sidebar & Navigation

| Component | MCode | Syncode | Gap |
|-----------|-------|---------|-----|
| **Sidebar** | Resizable, collapsible, project/thread tree | Fixed 280px sidebar | 🟡 |
| **ThreadList** | Rich with status indicators, retention toast | Basic list with status dots | 🟡 |
| **ProjectList** | Workspace/project tree | None | 🔴 |
| **AppNavigationButtons** | Back/forward/thread switching | None | 🔴 |
| **SearchInput** | Global search | None | 🔴 |
| **CommandPalette** | cmdk-based command palette (Ctrl+K) | None | 🔴 |
| **ShortcutsDialog** | Keyboard shortcuts reference | None | 🔴 |
| **WhatsNewDialog** | Version-aware changelog | None | 🔴 |

### 4.5 Settings & Configuration

| Component | MCode | Syncode | Gap |
|-----------|-------|---------|-----|
| **Settings Page** | Full settings page (`/settings`) | None | 🔴 |
| **ProviderUsageSettingsPanel** | Token usage, rate limits | None | 🔴 |
| **SkillsSettingsPanel** | Skill management | None | 🔴 |
| **ProfileSettingsPanel** | Profile editing | None | 🔴 |
| **ThemePackEditor** | Full theme customization | None | 🔴 |
| **SettingControls** | Reusable setting input components | None | 🔴 |

### 4.6 Right Dock Panel

| Component | MCode | Syncode | Gap |
|-----------|-------|---------|-----|
| **RightDock** | Resizable dock (file preview, terminal) | None | 🔴 |
| **DockFilePane** | File preview with syntax highlighting | None | 🔴 |
| **DockTerminalPane** | Docked terminal | None | 🔴 |
| **DockPaneHeader** | Dock tab management | None | 🔴 |

### 4.7 Kanban Board

| Component | MCode | Syncode | Gap |
|-----------|-------|---------|-----|
| **KanbanView** | Full kanban board with columns | None | 🔴 |
| **KanbanColumn** | Draggable columns (@dnd-kit) | None | 🔴 |
| **KanbanCardView** | Task cards with extras | None | 🔴 |
| **KanbanNewTaskDialog** | Task creation dialog | None | 🔴 |

### 4.8 Environment Panel

| Component | MCode | Syncode | Gap |
|-----------|-------|---------|-----|
| **EnvironmentPanel** | Per-thread context panel | None | 🔴 |
| **EnvironmentAutomationsSection** | Automation display | None | 🔴 |
| **EnvironmentEditorSection** | Editor settings | None | 🔴 |
| **EnvironmentLocalServersSection** | Dev server status | None | 🔴 |
| **EnvironmentMarkersSection** | Highlighted text markers | None | 🔴 |
| **EnvironmentNotesSection** | Per-thread notes | None | 🔴 |
| **EnvironmentPinnedSection** | Pinned messages | None | 🔴 |
| **EnvironmentProjectInstructionsSection** | Project instructions | None | 🔴 |
| **EnvironmentUsageSection** | Usage/cost metrics | None | 🔴 |

### 4.9 Profile & Misc

| Component | MCode | Syncode | Gap |
|-----------|-------|---------|-----|
| **ProfileAvatar** | Avatar display | None | 🔴 |
| **EditProfileDialog** | Profile editing dialog | None | 🔴 |
| **ShareDialog** | Thread/profile sharing | None | 🔴 |
| **ActivityHeatmap** | GitHub-style activity chart | None | 🔴 |
| **PlanSidebar** | AI plan sidebar | None | 🔴 |
| **PluginLibrary** | Plugin management page | None | 🔴 |
| **BrowserPanel** | Browser preview | None | 🔴 |
| **EditorWorkspaceView** | Code editor view | None | 🔴 |
| **WorkspaceView** | Workspace management | None | 🔴 |
| **PdfPageView / PdfViewerToolbar** | PDF viewing | None | 🔴 |
| **SplashScreen** | Boot splash | None | 🟡 (Tauri handles native) |
| **DesktopWindowControls** | Frameless window traffic lights | None | 🟡 (Tauri handles native) |
| **Toast/Notification** | Full notification system | None | 🔴 |
| **WorldCup** | Easter egg | N/A (not needed) | ✅ |

---

## 5. State Management Gap Analysis

### 5.1 Zustand Stores in MCode

| Store | File | Purpose | Syncode Equivalent |
|-------|------|---------|-------------------|
| **Main Store** | `store.ts` (4447 lines) | Projects, threads, messages, activities, plans, diffs, turn states | ❌ useState |
| **terminalStateStore** | `terminalStateStore.ts` | Per-thread terminal state | ❌ useState |
| **composerDraftStore** | `composerDraftStore.ts` | Draft message persistence | ❌ None |
| **splitViewStore** | `splitViewStore.ts` | Split-view state | ❌ None |
| **rightDockStore** | `rightDockStore.ts` | Right dock panel state | ❌ None |
| **singleChatPanelStore** | `singleChatPanelStore.ts` | Single chat panel | ❌ None |
| **kanbanUiStore** | `kanbanUiStore.ts` | Kanban UI state | ❌ None |
| **latestProjectStore** | `latestProjectStore.ts` | Latest project memory | ❌ useState |
| **workspaceStore** | `workspaceStore.ts` | Workspace pages | ❌ None |
| **threadSelectionStore** | `threadSelectionStore.ts` | Multi-thread selection | ❌ None |
| **pinnedProjectsStore** | `pinnedProjectsStore.ts` | Pinned projects | ❌ None |
| **pinnedThreadsStore** | `pinnedThreadsStore.ts` | Pinned threads | ❌ None |
| **projectRunStore** | `projectRunStore.ts` | Dev server run targets | ❌ None |
| **browserStateStore** | `browserStateStore.ts` | Browser state persistence | ❌ None |
| **repoDiffScopeStore** | `repoDiffScopeStore.ts` | Repo diff scope | ❌ None |

### 5.2 Key State Patterns Missing

| Pattern | MCode Implementation | Syncode Need |
|---------|--------------------|--------------| 
| **Normalized state** | By-ID maps for referential stability | Must implement |
| **Shallow equality** | Prevents unnecessary re-renders | Must implement |
| **State persistence** | localStorage with 500ms debounce | Must implement |
| **Hot-path streaming merge** | Preserve assistant messages during snapshots | Must implement |
| **Thread cap limits** | 2000 messages, 500 activities | Must implement |
| **Rehydration** | Restore state from persistence on load | Must implement |
| **React Query** | 6 query option modules (server, provider, project, git, usage, discovery) | Must implement |

### 5.3 React Query Modules in MCode

| Module | Queries | Syncode Need |
|--------|---------|--------------|
| `serverReactQuery.ts` | Server config, settings | Phase 3 |
| `providerReactQuery.ts` | Provider status | Phase 2 |
| `providerDiscoveryReactQuery.ts` | Model/agent discovery | Phase 2 |
| `projectReactQuery.ts` | Project file queries | Phase 2 |
| `gitReactQuery.ts` | Git status queries | Phase 3 |
| `openUsageReactQuery.ts` | Rate limit/usage | Phase 2 |

---

## 6. API / Communication Layer Gap Analysis

### 6.1 Transport Comparison

| Aspect | MCode | Syncode | Gap |
|--------|-------|---------|-----|
| **Protocol** | JSON-RPC over WebSocket | JSON-RPC over WebSocket | ✅ Match |
| **Type safety** | Effect RPC (fully typed contracts) | Generic `rpc<T>()` | 🟡 |
| **Timeout** | Configurable per-method | Fixed 10s | 🟡 |
| **Reconnection** | Full state machine (connecting→open→reconnecting→closed) | Reconnect on mount only | 🔴 |
| **Request queue** | Queue during disconnect, flush on reconnect | None | 🔴 |
| **Push channels** | Channel-based with replayLatest | Single onPush callback | 🟡 |
| **Event buffering** | 100ms coalesce, immediate for first assistant msg | No buffering | 🔴 |
| **Desktop bridge** | `window.nativeApi` (Electron preload) | Tauri IPC commands | 🟢 Different |

### 6.2 RPC Method Coverage

| Category | MCode Methods (~80) | Syncode Methods Used | Gap |
|----------|---------------------|---------------------|-----|
| **Projects** | `list`, `add`, `remove`, `discoverScripts`, `searchEntries`, `readFile`, `writeFile`, `runDevServer`, `stopDevServer` | `project/list`, `project/create` | 🟡 |
| **Orchestration** | `getSnapshot`, `getShellSnapshot`, `dispatchCommand`, `subscribeThread`, `subscribeShell`, `importThread`, `repairState`, `getTurnDiff`, `getFullThreadDiff`, `replayEvents` | `thread/create` | 🟡 |
| **Threads/Turns** | `thread/list` (via subscriptions) | `turn/list`, `turn/start`, `turn/complete` | 🟡 |
| **Git** | `status`, `pull`, `listBranches`, `createBranch`, `checkout`, `createWorktree`, `removeWorktree`, `createDetachedWorktree`, `stageFiles`, `unstageFiles`, `readWorkingTreeDiff`, `summarizeDiff`, `runStackedAction`, `stashAndCheckout`, `stashDrop`, `stashInfo`, `removeIndexLock`, `init`, `githubRepository`, `handoffThread`, `preparePullRequestThread`, `resolvePullRequest` | None wired in UI | 🔴 |
| **Terminal** | `open`, `write`, `ackOutput`, `resize`, `clear`, `restart`, `close` | Implicit via polling | 🟡 |
| **Server** | `getConfig`, `getEnvironment`, `getSettings`, `updateSettings`, `refreshProviders`, `updateProvider`, `listWorktrees`, `listLocalServers`, `stopLocalServer`, `getProviderUsageSnapshot`, `listProviderUsage`, `getDiagnostics`, `transcribeVoice`, `generateThreadRecap`, `generateAutomationIntent`, `upsertKeybinding` | None | 🔴 |
| **Provider** | `getComposerCapabilities`, `compactThread`, `listCommands`, `listSkills`, `listSkillsCatalog`, `listPlugins`, `readPlugin`, `listModels`, `listAgents` | None (hardcoded list) | 🔴 |
| **Automation** | `list`, `create`, `update`, `delete`, `runNow`, `cancelRun`, `markRunRead`, `archiveRun` | None | 🔴 |
| **Filesystem** | `browse` | None | 🔴 |
| **Editor** | `openInEditor` | None | 🔴 |

### 6.3 Push Event Channels

| Channel | MCode | Syncode | Gap |
|---------|-------|---------|-----|
| `server.welcome` | ✅ Initial hydration | ❌ | 🔴 |
| `server.configUpdated` | ✅ Config changes | ❌ | 🔴 |
| `server.providerStatusesUpdated` | ✅ Provider status | ❌ | 🔴 |
| `server.settingsUpdated` | ✅ Settings changes | ❌ | 🔴 |
| `terminal.event` | ✅ PTY output | ❌ (polls instead) | 🟡 |
| `orchestration.domainEvent` | ✅ Domain state | ✅ (basic) | 🟡 |
| `automation.event` | ✅ Automation lifecycle | ❌ | 🔴 |
| `git.actionProgress` | ✅ Git progress | ❌ | 🔴 |
| `project.devServerEvent` | ✅ Dev server status | ❌ | 🔴 |

---

## 7. Styling & Theme Gap Analysis

### 7.1 CSS Framework

| Aspect | MCode | Syncode | Gap |
|--------|-------|---------|-----|
| **Framework** | Tailwind CSS v4 (inline config in CSS) | None | 🔴 |
| **Component variants** | class-variance-authority (CVA) | None | 🔴 |
| **Class merging** | tailwind-merge via `cn()` | None | 🔴 |
| **Class concatenation** | clsx | None | 🔴 |
| **CSS size** | ~2095 lines (index.css) + theme variables | 0 lines | 🔴 |
| **Animation** | tw-animate-css + @formkit/auto-animate | CSS `blink` keyframe only | 🔴 |

### 7.2 Theme System

| Feature | MCode | Syncode | Gap |
|---------|-------|---------|-----|
| **Light/Dark mode** | ✅ System/light/dark toggle | ❌ Dark only | 🔴 |
| **Custom theme packs** | ✅ Full color customization | ❌ Hardcoded | 🔴 |
| **CSS custom properties** | 50+ variables on `:root` | 0 | 🔴 |
| **Code theme** | ✅ Configurable syntax theme | ❌ None | 🔴 |
| **Font selection** | ✅ UI font + code font per variant | ❌ system-ui only | 🔴 |
| **Import/Export** | ✅ Shareable theme strings | ❌ None | 🔴 |
| **Frosted glass** | ✅ `backdrop-filter: blur()` | ❌ Solid colors | 🔴 |

### 7.3 shadcn/ui Components (48 primitives missing)

```
alert-dialog, alert, autocomplete, badge, button, card, checkbox,
collapsible, combobox, command, dialog, empty, field, fieldset, form,
group, icon-button, input-group, input, kbd, label, menu,
notificationSurface, popover, radio-group, scroll-area, search-input,
select, separator, sheet, shortcut-kbd, sidebar, skeleton, spinner,
switch, textarea, time-picker, toast, toggle-group, toggle, tooltip,
DisclosureChevron, DisclosureRegion
```

---

## 8. Routing Gap Analysis

### 8.1 MCode Routes (10)

| Route | URL | Purpose | Syncode |
|-------|-----|---------|---------|
| Landing | `/` | Restore last thread / create new | Part of App.tsx |
| Thread | `/$threadId` | Individual chat thread | Thread selection in App.tsx |
| Automations | `/automations` | Automation list | ❌ |
| Automation Detail | `/automations/$id` | Single automation | ❌ |
| Kanban | `/kanban` | Board overview | ❌ |
| Kanban Project | `/kanban/$projectId` | Project board | ❌ |
| Plugins | `/plugins` | Plugin library | ❌ |
| Settings | `/settings` | Full settings | ❌ |
| Workspace | `/workspace` | Workspace overview | ❌ |
| Workspace Detail | `/workspace/$id` | Single workspace | ❌ |

### 8.2 MCode Layout Patterns

| Pattern | Description | Syncode Need |
|---------|-------------|--------------|
| **Resizable sidebar** | Collapsible, width-persistent | Must add |
| **Chat content card** | Rounded seam edge, scroll management | Must add |
| **Right dock** | File preview + terminal dock | Must add |
| **Off-canvas sidebar** | Mobile-friendly offscreen panel | Nice-to-have |
| **Split view** | Side-by-side thread comparison | Phase 4 |

---

## 9. Desktop Integration Gap Analysis

| Feature | MCode (Electron) | Syncode (Tauri) | Gap |
|---------|-----------------|-----------------|-----|
| **Native bridge** | `window.nativeApi` (contextBridge) | Tauri IPC commands | 🟢 Different |
| **Auto-update** | GitHub releases, resumable | Tauri updater plugin | 🟡 |
| **System tray** | ✅ | Partial (tray.rs exists) | 🟡 |
| **Window management** | Frameless, traffic lights | Tauri window API | 🟡 |
| **Native menus** | Application menu, context menus | Tauri menu plugin | 🔴 |
| **Deep linking** | Protocol handler | Tauri deep-link | 🔴 |
| **File system access** | Full Node.js fs | Tauri fs plugin | 🟡 |
| **Notifications** | Native notifications | Tauri notification | 🔴 |
| **Media permissions** | Camera/microphone | Tauri permissions | 🔴 |

---

## 10. Testing Gap Analysis

| Aspect | MCode | Syncode | Gap |
|--------|-------|---------|-----|
| **Unit tests** | Vitest | None | 🔴 |
| **Browser tests** | Playwright + vitest-browser-react | None | 🔴 |
| **Test patterns** | `.browser.tsx` files for interactive components | None | 🔴 |
| **Mocking** | MSW (Mock Service Worker) | None | 🔴 |
| **E2E** | (likely Playwright E2E) | None | 🔴 |
| **CI integration** | Test in CI pipeline | CI has no frontend step | 🔴 |

---

## 11. Existing Syncode Code Quality Assessment

### 11.1 What Works

| Area | Assessment |
|------|-----------|
| **WebSocket hook** | Functional JSON-RPC implementation with push support |
| **Provider stream hook** | Good foundation for streaming event handling |
| **Auto-generated types** | ts-rs type bridge working (16 type files) |
| **Thread creation flow** | Basic end-to-end thread creation works |
| **Terminal view** | Functional PTY emulator with session management |
| **Provider switching** | Basic dropdown UI (hardcoded models) |
| **Connection indicator** | Visual connected/offline state |
| **GitPanel** | Component exists (not wired — needs integration) |

### 11.2 What Needs Immediate Fix

| Issue | Location | Fix |
|-------|----------|-----|
| **Hardcoded placeholder response** | ChatView.tsx:92 | Wire to real provider streaming |
| **Hardcoded provider list** | ProviderSwitcher.tsx | Fetch from backend via RPC |
| **GitPanel not imported** | App.tsx | Wire GitPanel into layout |
| ~~**No reconnection logic**~~ ~~useWebSocket.ts~~ | ~~Add reconnect with exponential backoff~~ | ✅ **DONE (ws-snapshot-reconnect)** — `min(500*2^n, 5000)` backoff + re-subscribe; reconnecting clients receive a snapshot via snapshot-then-stream |
| **No request queueing** | useWebSocket.ts | Queue requests during disconnect (low priority — MCode has neither; reconnect+resubscribe covers it) |
| **10s fixed timeout** | useWebSocket.ts | Configurable per-method timeout (low priority — MCode has neither) |
| **No error boundaries** | App.tsx | Add React error boundaries |
| **No loading states** | Multiple components | Add skeleton/spinner loading states |
| **Inline styles everywhere** | All components | Migrate to Tailwind CSS |
| **No responsive design** | All components | Add responsive breakpoints |

### 11.3 Code Metrics

| Metric | MCode | Syncode |
|--------|-------|---------|
| **Total source files** | ~750+ | 7 |
| **Component files** | ~100+ | 6 |
| **Hook files** | ~31 | 2 |
| **Store files** | ~15 | 0 |
| **Type files** | Contracts package | 16 (auto-generated) |
| **CSS files** | ~2095 lines (index.css) | 0 |
| **Lines of code (approx)** | ~50,000+ | ~700 |

---

## 12. Implementation Roadmap

> 📌 **Superseded by clone + re-wire (2026-07-02).** This from-scratch roadmap (Phases 0-6, ~16-18 weeks) remains valid as a *fallback* and as a checklist of what the cloned UI must ultimately cover. Under the clone strategy, Phase 0 (foundation/scaffolding) + Phase 1 (state & comms / transport re-wire) are the active work; Phases 2-5 (building chat/terminal/git/nav/kanban UI from nothing) are largely obviated because those components arrive intact from `apps/web`. See §13.2 for revised effort.

### Phase 0: Foundation (Week 1-2) — Frontend Scaffolding

| # | Task | Priority | Details |
|---|------|----------|---------|
| F0.1 | Install Tailwind CSS v4 + @tailwindcss/vite | P0 | Add to vite.config.ts, create index.css |
| F0.2 | Install and configure cn() utility | P0 | Add clsx, tailwind-merge, create lib/utils.ts |
| F0.3 | Install Zustand | P0 | Add with persist middleware |
| F0.4 | Install React Query | P0 | Set up QueryClientProvider |
| F0.5 | Install TanStack Router | P0 | File-based routing setup |
| F0.6 | Install shadcn/ui | P0 | Init with components.json, add base primitives |
| F0.7 | Migrate inline styles → Tailwind classes | P0 | Convert all 6 components |
| F0.8 | Add CSS custom properties for theming | P0 | Create base light/dark theme tokens |
| F0.9 | Upgrade Vite 6 → Vite 8 | P1 | Match MCode build tooling |
| F0.10 | Add ESLint + Prettier | P1 | Code quality baseline |

### Phase 1: Core State & Communication (Week 3-4)

| # | Task | Priority | Details |
|---|------|----------|---------|
| F1.1 | Create main Zustand store | P0 | Projects, threads, messages (normalized) |
| F1.2 | Add state persistence (localStorage) | P0 | 500ms debounce, rehydration |
| F1.3 | Create React Query modules | P0 | server, provider, project, git, usage |
| F1.4 | Upgrade useWebSocket hook | P0 | Reconnection, request queue, buffering |
| F1.5 | Create EventRouter component | P0 | Push events → store + query invalidation |
| F1.6 | Add error boundaries | P0 | Root + route-level error handling |
| F1.7 | Add toast notification system | P0 | Task completion, error toasts |
| F1.8 | Add loading skeletons | P0 | Spinner, skeleton components |
| F1.9 | Create thread subscription lifecycle | P0 | Subscribe/unsubscribe based on visible route |

### Phase 2: Chat System (Week 4-6)

| # | Task | Priority | Details |
|---|------|----------|---------|
| F2.1 | Install Lexical + @lexical/react | P0 | Rich text composer |
| F2.2 | Build ComposerInput component | P0 | Lexical-based, with submit handler |
| F2.3 | Install react-markdown + remark-gfm | P0 | Markdown rendering |
| F2.4 | Build ChatMarkdown component | P0 | GFM, code blocks, links |
| F2.5 | Install Shiki | P1 | Syntax highlighting in code blocks |
| F2.6 | Build MessagesTimeline | P0 | Virtualized message list (@tanstack/react-virtual) |
| F2.7 | Build ChatScroll | P0 | Smart scroll anchoring |
| F2.8 | Wire real provider streaming | P0 | Remove placeholder response, use actual provider output |
| F2.9 | Build ProviderModelPicker | P1 | Dynamic model selection from backend |
| F2.10 | Build ToolCallDetailsDialog | P1 | Expandable tool call display |
| F2.11 | Build ApprovalPanel | P0 | Approve/deny tool use requests |
| F2.12 | Build FileAttachmentChip | P1 | Drag-drop file attachments |
| F2.13 | Build ComposerCommandMenu | P1 | Slash commands (@mentions) |

### Phase 3: Navigation & Settings (Week 6-8)

| # | Task | Priority | Details |
|---|------|----------|---------|
| F3.1 | Build resizable sidebar | P1 | Drag-to-resize, collapsible, persistent width |
| F3.2 | Build project/thread tree | P1 | Hierarchical navigation |
| F3.3 | Build Settings page | P1 | Provider config, theme, keybindings |
| F3.4 | Build ThemePackEditor | P1 | Theme customization UI |
| F3.5 | Build CommandPalette | P1 | cmdk-based Ctrl+K palette |
| F3.6 | Build ShortcutsDialog | P2 | Keyboard shortcuts reference |
| F3.7 | Install @tabler/icons-react | P1 | Icon library |
| F3.8 | Build WhatsNewDialog | P2 | Version changelog |
| F3.9 | Build ProfileSettingsPanel | P2 | Avatar, sharing |
| F3.10 | Add split view support | P2 | Side-by-side thread comparison |

### Phase 4: Git, Diff & Terminal (Week 8-10)

| # | Task | Priority | Details |
|---|------|----------|---------|
| F4.1 | Install xterm.js + addons | P0 | WebGL, clipboard, fit, search, unicode |
| F4.2 | Rebuild TerminalView with xterm.js | P0 | Real PTY rendering via WebSocket push |
| F4.3 | Wire GitPanel into layout | P0 | Connect to git RPC methods |
| F4.4 | Build BranchToolbar | P1 | Branch creation, checkout, worktree |
| F4.5 | Install diff viewer library | P1 | @pierre/diffs or react-diff-viewer-continued |
| F4.6 | Build DiffPanel | P1 | File list, patch viewport, toolbar |
| F4.7 | Build DirectoryTreeBrowser | P1 | File system navigation |
| F4.8 | Build EnvironmentPanel | P1 | Per-thread context (git status, pinned, markers) |
| F4.9 | Build DockFilePane | P2 | File preview with syntax highlighting |
| F4.10 | Build DockTerminalPane | P2 | Docked terminal |

### Phase 5: Advanced Features (Week 10-14)

| # | Task | Priority | Details |
|---|------|----------|---------|
| F5.1 | Build Automation pages | P1 | List + editor + run history |
| F5.2 | Build Kanban board | P2 | @dnd-kit based task management |
| F5.3 | Build Plugin library page | P2 | Skill/plugin management |
| F5.4 | Build Workspace views | P2 | Workspace management |
| F5.5 | Install remark-math + rehype-katex | P3 | Math rendering |
| F5.6 | Build ProviderHealthBanner | P1 | Status/rate limit display |
| F5.7 | Build PlanSidebar | P2 | AI plan display with accept/reject |
| F5.8 | Build BrowserPanel | P3 | Browser preview |
| F5.9 | Install pdfjs-dist | P3 | PDF viewing |

### Phase 6: Polish & Testing (Week 14-16)

| # | Task | Priority | Details |
|---|------|----------|---------|
| F6.1 | Install Vitest + Playwright | P0 | Test infrastructure |
| F6.2 | Write component unit tests | P0 | Core components (ChatView, Composer, ThreadList) |
| F6.3 | Write browser tests | P1 | Interactive component tests (.browser.tsx) |
| F6.4 | Add MSW for API mocking | P1 | Test WebSocket RPC calls |
| F6.5 | Add frontend CI step | P1 | Build + typecheck + test in GitHub Actions |
| F6.6 | Performance audit | P2 | React DevTools profiler, bundle analysis |
| F6.7 | Accessibility audit | P2 | Keyboard navigation, screen reader support |
| F6.8 | Responsive design | P2 | Mobile/tablet layouts |

---

## 13. Summary Statistics

### 13.1 Gap by Category

| Category | MCode Count | Syncode Count | Gap % | Priority |
|----------|-------------|-------------|-------|----------|
| **Components** | ~100+ | 6 | 94% | P0 |
| **Zustand Stores** | 15 | 0 | 100% | P0 |
| **React Query Modules** | 6 | 0 | 100% | P0 |
| **Routes** | 10 | 0 (inline) | 100% | P1 |
| **UI Primitives** | 48 | 0 | 100% | P0 |
| **CSS** | 2095 lines | 0 | 100% | P0 |
| **Theme Variables** | 50+ | 0 | 100% | P0 |
| **Hooks** | 31 | 2 | 94% | P0 |
| **Dependencies** | 50+ | 2 | 96% | P0 |
| **RPC Methods Used** | ~80 | ~6 | 93% | P1 |
| **Push Channels** | 9 | 1 | 89% | P1 |
| **Test Files** | Many | 0 | 100% | P1 |

### 13.2 Estimated Effort

| Phase | Frontend Tasks | Est. Effort | Dependencies |
|-------|---------------|------------|--------------|
| Phase 0: Foundation | 10 tasks | 2 weeks | None |
| Phase 1: State & Comms | 9 tasks | 2 weeks | Phase 0 |
| Phase 2: Chat System | 13 tasks | 3 weeks | Phase 1 |
| Phase 3: Nav & Settings | 10 tasks | 2 weeks | Phase 1 |
| Phase 4: Git & Terminal | 10 tasks | 2-3 weeks | Phase 2-3 |
| Phase 5: Advanced | 9 tasks | 3-4 weeks | Phase 4 |
| Phase 6: Polish & Test | 8 tasks | 2 weeks | All |
| **Total** | **69 tasks** | **16-18 weeks** | — |

> 📌 **Revised under clone + re-wire (2026-07-02):** Phase 0 + Phase 1 + transport/contract re-wire ≈ **~3-6 weeks** to a working parity frontend; Phases 2-5 collapse into "integrate & adapt cloned components" (days-to-weeks each, not weeks-to-build). The 16-18 week total applies only to the from-scratch fallback. Dominant risk/cost is the contracts re-mapping (Effect Schema → ts-rs), not UI construction.

---

## 14. Recommendations

### 14.1 Immediate Actions (This Week)

1. **Install core dependencies**: Tailwind CSS v4, Zustand, React Query, clsx, tailwind-merge
2. **Set up shadcn/ui**: Initialize with `npx shadcn@latest init`, add button, input, dialog, toast, scroll-area, skeleton, tooltip
3. **Create lib/utils.ts**: `cn()` function for class merging
4. **Migrate App.tsx to Tailwind**: Replace all inline styles with utility classes
5. **Wire GitPanel**: Import and integrate the existing GitPanel component
6. **Upgrade useWebSocket**: Add reconnect logic, request queueing, configurable timeouts

### 14.2 Strategic Decisions

| Decision | Options | Recommendation |
|----------|---------|---------------|
| **UI Library** | shadcn/ui vs Headless UI vs Radix | ✅ shadcn/ui (matches MCode) |
| **State Management** | Zustand vs Jotai vs Redux | ✅ Zustand (matches MCode) |
| **Router** | TanStack Router vs React Router | ✅ TanStack Router (matches MCode) |
| **Terminal** | xterm.js vs custom canvas | ✅ xterm.js (proven in MCode) |
| **Diff Viewer** | @pierre/diffs vs react-diff-viewer | 🟡 Evaluate both |
| **Rich Text** | Lexical vs TipTap vs Slate | ✅ Lexical (matches MCode) |
| **Testing** | Vitest + Playwright (MCode) vs Jest + Cypress | ✅ Vitest + Playwright (match MCode) |
| **CSS** | Tailwind v4 (inline config) vs v3 (config file) | ✅ Tailwind v4 (match MCode) |

> 📌 Under the **clone + re-wire** strategy (§14.3), these decisions are largely *inherited* from MCode rather than re-litigated: the clone brings TanStack Router/Query, Zustand, shadcn/ui, xterm.js, and Lexical with it. The one genuinely new decision is the **transport/contract** choice — and it is forced: Syncode's backend speaks plain JSON-RPC + ts-rs types, so the Effect-RPC/Schema layer must go.

### 14.3 Potential Optimizations

> ✅ **Adopted as the recommended strategy (2026-07-02) — clone `apps/web` + re-wire.** See banner. Item 1 below is promoted from "optimization" to the primary plan; the from-scratch §12 roadmap becomes the fallback.

1. **Clone MCode's `apps/web` wholesale and re-wire (RECOMMENDED).** Both stacks share React 19 + (after adoption) Tailwind v4, so the production UI — chat, terminal (xterm.js + 6 addons), git/diff (@pierre/diffs), kanban (@dnd-kit), settings, markdown (react-markdown + KaTeX), composer (Lexical) — ports with the components intact. The re-wire is the real work:
   - **Transport/runtime (Effect RPC → JSON-RPC):** the `effect` runtime is confined to **~15 boundary files** (`wsTransport.ts`, RPC hooks, a handful of components) — *not* pervasive across the 752-file tree. Rewrite these to Syncode's plain JSON-RPC-over-WS and strip the Effect runtime.
   - **Contracts (Effect Schema → ts-rs):** **333 files** import `@t3tools/contracts`, but overwhelmingly for *type-level* `Schema.*` definitions (the DTO surface). Generate a `frontend/src/contracts/` from Syncode's ts-rs output that mirrors that export surface; runtime-Schema use is minimal (`decodeSync` ×5, `fromJsonString` ×6, `is` ×9), so most references are a type-import remap. **This is the dominant cost and the key risk** — Syncode's domain model diverges from MCode (Turn/Message/Activity are first-class aggregates here; MCode only has project + thread), so a reconciliation/adaptation layer is needed where shapes don't match 1:1.
   - **Shell (Electron → Tauri):** replace `nativeApi.ts` / `wsNativeApi.ts` (Electron bridge) with `@tauri-apps/api` `invoke` IPC.
   - **Monorepo flatten:** MCode is a Turborepo workspace (`apps/web` + `@t3tools/contracts` + `@t3tools/shared`); Syncode is a single `frontend/` folder — vendor/flatten and drop the workspace deps.
   - **Build config:** bring Vite 8, `@tailwindcss/vite`, TanStack router-plugin, `babel-plugin-react-compiler` (or drop).
   - **Risks:** schema-shape divergence (biggest — mitigate by maximizing contracts-bridge overlap); confirm MCode's LICENSE (same author lineage `synara → mcode → syncode`, dayartcrew-web, so internally fine, but verify).
   - **Effort:** roughly Phase 0 + Phase 1 of §12 plus the re-wire — **~3-6 weeks** for working parity, vs **16-18 weeks** from scratch. UI build (original Phases 2-5) is largely obviated.
2. **Share contracts (still applies):** the ts-rs auto-generated types are the contracts-bridge substrate above — ensure every RPC method + push channel generates a matching type so the 333-file remap stays mechanical.
3. **Start with Electron parity:** match MCode's feature set first via the clone, then optimize for Tauri-specific features (smaller bundle, native APIs) — unchanged.

---

*Document generated for frontend migration planning. Last updated: 2026-06-27*

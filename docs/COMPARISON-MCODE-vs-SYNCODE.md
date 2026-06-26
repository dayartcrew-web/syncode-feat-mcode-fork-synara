# MCode vs Syncode — Feature Comparison & Implementation Plan

> **Tanggal**: 2026-06-27
> **Scope**: Perbandingan fitur lengkap MCode (TypeScript monorepo) → Syncode (Rust DDD blueprint)
> **Tujuan**: High-to-low documentation untuk re-implementasi / porting dari MCode ke Syncode

---

## 1. HIGH-LEVEL: Executive Summary

### 1.1 Apa itu MCode?
MCode adalah **local-first desktop app** untuk coding dengan AI agents. Monorepo TypeScript/Bun dengan:
- **8 provider AI**: Codex, Claude, Cursor, Gemini, Grok, Kilo, OpenCode, Pi
- **Electron desktop** + **React web** + **Node.js WebSocket server**
- **CQRS/Event Sourcing** orchestration engine
- **Full Git integration** (branch, worktree, PR, diff, stash)
- **Terminal PTY**, **browser preview**, **automation scheduler**

### 1.2 Apa itu Syncode (Project Ini)?
Proyek ini **kosong** — hanya berisi legacy masday workflow state dari Juni 2026. Nama "syncode-rust-ddd-blueprint" mengindikasikan rencana untuk membangun **Rust-based DDD (Domain-Driven Design) blueprint** untuk sebuah IDE/agent workspace serupa MCode, tapi dengan arsitektur yang lebih robust.

### 1.3 Gap Summary

| Area | MCode (Current) | Syncode (Target) | Gap |
|------|----------------|-----------------|-----|
| Language | TypeScript/Bun | Rust (Axum) | 100% rewrite needed |
| Architecture | Event Sourcing + CQRS-lite | DDD + Event Sourcing (planned) | Re-architect |
| Providers | 8 providers, Codex-first | None | Build from scratch |
| UI | React + Vite | TBD (Yew? Leptos? Tauri?) | Build from scratch |
| Desktop | Electron | Tauri (likely) | Build from scratch |
| Server | Node.js WebSocket | Axum WebSocket | Port |
| Persistence | SQLite (better-sqlite3) | SQLite (rusqlite/sqlx) or PostgreSQL | Port |
| Testing | Vitest (1017+ tests) | cargo test | Build from scratch |

---

## 2. MEDIUM-LEVEL: Feature Inventory (MCode)

### 2.1 Server Architecture (`apps/server`)

#### Core Domain Modules
```
├── orchestration/     — CQRS Engine (decider, projector, command/event)
├── provider/          — Provider abstraction + adapters
├── checkpointing/     — Git-based workspace snapshots
├── git/               — Full Git operations
├── terminal/          — PTY process management
├── persistence/       — SQLite migrations + projections
├── project/           — Project registry
├── workspace/         — Workspace management
├── auth/              — Auth control plane
├── automation/        — Scheduled agent runs
├── environment/       — Per-thread env vars
├── telemetry/         — Usage tracking
├── providerUsage/     — Token/usage monitoring
└── stream/            — Stream processing utils
```

#### WebSocket RPC API (~80+ methods)
| Category | Methods | Description |
|----------|---------|-------------|
| **Projects** | `projects.list`, `add`, `remove`, `discoverScripts`, `searchEntries`, `readFile`, `writeFile`, `runDevServer`, `stopDevServer` | Project CRUD + dev server management |
| **Orchestration** | `orchestration.getSnapshot`, `getShellSnapshot`, `dispatchCommand`, `importThread`, `repairState`, `getTurnDiff`, `getFullThreadDiff`, `replayEvents`, `subscribeThread`, `subscribeShell` | CQRS read model + event replay |
| **Git** | `git.status`, `pull`, `listBranches`, `createBranch`, `checkout`, `createWorktree`, `removeWorktree`, `createDetachedWorktree`, `stageFiles`, `unstageFiles`, `readWorkingTreeDiff`, `summarizeDiff`, `runStackedAction`, `stashAndCheckout`, `stashDrop`, `stashInfo`, `removeIndexLock`, `init`, `githubRepository`, `handoffThread`, `preparePullRequestThread`, `resolvePullRequest` | Complete Git workflow |
| **Terminal** | `terminal.open`, `write`, `ackOutput`, `resize`, `clear`, `restart`, `close` | PTY lifecycle |
| **Server** | `server.getConfig`, `getEnvironment`, `getSettings`, `updateSettings`, `refreshProviders`, `updateProvider`, `listWorktrees`, `listLocalServers`, `stopLocalServer`, `getProviderUsageSnapshot`, `listProviderUsage`, `getDiagnostics`, `transcribeVoice`, `generateThreadRecap`, `generateAutomationIntent`, `upsertKeybinding` | Meta/settings |
| **Provider** | `provider.getComposerCapabilities`, `compactThread`, `listCommands`, `listSkills`, `listSkillsCatalog`, `listPlugins`, `readPlugin`, `listModels`, `listAgents` | Provider discovery |
| **Automation** | `automation.list`, `create`, `update`, `delete`, `runNow`, `cancelRun`, `markRunRead`, `archiveRun` | Scheduled agent tasks |
| **Filesystem** | `filesystem.browse` | File browsing |
| **Editor** | `shell.openInEditor` | External editor integration |

#### Push Event Channels (Real-time)
| Channel | Description |
|---------|-------------|
| `server.welcome` | Initial connection state hydration |
| `server.configUpdated` | Config change notifications |
| `server.providerStatusesUpdated` | Provider availability changes |
| `server.settingsUpdated` | Settings persistence |
| `terminal.event` | PTY output stream |
| `orchestration.domainEvent` | All domain state changes |
| `automation.event` | Automation lifecycle events |
| `git.actionProgress` | Git action progress (branch→commit→push→PR) |
| `project.devServerEvent` | Dev server status |

### 2.2 Provider Architecture

```
ProviderKind = "codex" | "claudeAgent" | "cursor" | "gemini" | "grok" | "kilo" | "opencode" | "pi"

Each provider has:
- ModelSelection (provider + model + options)
- ProviderAdapter (stdin/stdout JSON-RPC wrapper)
- ApprovalPolicy: untrusted | on-failure | on-request | never
- SandboxMode: read-only | workspace-write | danger-full-access
- RuntimeMode: approval-required | full-access
- InteractionMode: default | plan
- AssistantDeliveryMode: streaming | buffered
```

### 2.3 Orchestration Engine (CQRS/ES Pattern)

```
Commands → Decider → Events → Projector → Read Model
                ↓
            Reactors (side effects):
            - ProviderRuntimeIngestion (provider events → orchestration events)
            - ProviderCommandReactor (orchestration intent → provider calls)
            - CheckpointReactor (git checkpoint capture on turn boundaries)

All reactors use DrainableWorker (queue-backed) for deterministic ordering.
RuntimeReceiptBus emits typed receipts for test synchronization.
```

Key domain concepts:
- **Project**: Top-level workspace record
- **Thread**: Durable conversation unit (messages + activities + checkpoints)
- **Turn**: Single user→assistant work cycle
- **Checkpoint**: Git-based workspace snapshot for diff/restore
- **Activity**: Non-message log items (approvals, tool actions, failures)

### 2.4 Automation System

Rich scheduling engine:
- **Schedules**: manual, once, interval, daily, weekdays, weekly, cron
- **Modes**: standalone (fresh thread per run), heartbeat (continues existing thread)
- **Completion Policies**: none, ai-evaluated (NL stop condition + confidence threshold)
- **Retry Policies**: none, fixed, exponential
- **Misfire Policies**: skip, coalesce, run-latest
- **Worktree Modes**: auto, local, worktree
- **Capabilities**: send-turn, create-worktree, full-access

### 2.5 Desktop App (`apps/desktop`)

- **Electron** shell wrapping the web app + Node.js server
- **Backend process management** (spawn, readiness detection, lifecycle)
- **Auto-update** (GitHub releases feed, resumable downloads, update machine state machine)
- **Native menus** & keyboard shortcuts
- **Window management** (initial open, confirm dialogs)
- **Tray** (implied from Electron pattern)
- **Browser IPC bridge** (desktop ↔ web communication)
- **Media permissions**
- **Platform-specific builds**: macOS (DMG), Linux (AppImage), Windows (NSIS)

### 2.6 Web App (`apps/web`)

#### UI Components (100+)
| Category | Key Components |
|----------|---------------|
| **Chat** | ChatView, ChatMarkdown, ChatInput, ChatScroll, MessageRow, ToolRow, ApprovalRow, ActivityRow |
| **Terminal** | TerminalPanel, TerminalTabs, TerminalOutput |
| **Git** | BranchToolbar, GitStatus, DiffViewer, WorktreePanel |
| **Browser** | BrowserPanel (browser preview) |
| **Settings** | ProviderSettings, KeybindingSettings, GeneralSettings |
| **Profile** | ProfilePanel, StatsPanel, UsagePanel |
| **Automation** | AutomationList, AutomationEditor, RunHistory |
| **Navigation** | AppNavigationButtons, ThreadList, ProjectList |
| **Composer** | ComposerInput, SlashCommands, FileAttachments, Mentions |
| **Kanban** | KanbanBoard (task management) |
| **WorldCup** | WorldCupPanel (feature comparison?) |
| **Environment** | EnvironmentPanel, EnvVarEditor |

#### State Management
- Client-side WebSocket transport with typed push decode
- Connection state machine: connecting → open → reconnecting → closed → disposed
- Outbound request queue during disconnect, flush on reconnect
- Channel-based push caching with `replayLatest` support

### 2.7 Shared Contracts (`packages/contracts`)

Schema-only package (no runtime logic) using **Effect Schema**:
- `orchestration.ts` — Core domain types (Project, Thread, Turn, Message, Checkpoint, etc.)
- `provider.ts` — Provider session types, events, inputs
- `ws.ts` — WebSocket RPC method/channel definitions
- `git.ts` — Full Git operation types
- `terminal.ts` — Terminal session types
- `automation.ts` — Automation scheduling types
- `server.ts` — Server config, diagnostics, usage, settings
- `project.ts` — Project registry types
- `model.ts` — Model options per provider
- `auth.ts` — Auth types
- `environment.ts` — Per-thread environment types
- `filesystem.ts` — File browsing types
- `editor.ts` — External editor types
- `settings.ts` — Server settings types
- `keybindings.ts` — Keyboard shortcut types
- `stats.ts` — Profile statistics types
- `providerDiscovery.ts` — Provider capability discovery types
- `agentMentions.ts` — Agent mention/reference types
- `baseSchemas.ts` — Primitive type schemas (ID types, strings, dates)
- `ipc.ts` — Electron IPC types
- `rpc.ts` — RPC protocol types

### 2.8 Shared Utilities (`packages/shared`)

Runtime logic with explicit subpath exports:
- `DrainableWorker` — Queue-backed async worker with drain() for deterministic tests
- `git.ts` — Git utility functions
- `model.ts` — Model option resolution
- `codexConfig.ts` — Codex-specific config
- `chatThreads.ts` — Thread state utilities
- `conversationEdit.ts` — Message editing logic
- `threadSummary.ts` — Thread summary generation
- `threadMarkers.ts` — Thread bookmark markers
- `threadEnvironment.ts` — Thread environment variable management
- `threadWorkspace.ts` — Thread workspace utilities
- `terminalThreads.ts` — Terminal thread management
- `toolOutputSummary.ts` — Tool output summarization
- `pinnedMessages.ts` — Pinned message management
- `agentMentions.ts` — Agent mention parsing
- `subagents.ts` — Sub-agent management
- `composerSlashCommands.ts` — Composer slash command registry
- `worktreeHandoff.ts` — Worktree handoff logic
- `browserSession.ts` — Browser session management
- `browserShortcuts.ts` — Browser keyboard shortcuts
- `desktopChrome.ts` — Desktop chrome utilities
- `shell.ts` — Shell execution utilities
- `localServers.ts` — Local server discovery
- `localPreviewFiles.ts` — Local file preview
- `providerUsage.ts` — Provider usage calculation
- `errorMessages.ts` — Error message formatting
- `text.ts` — Text manipulation utilities
- `path.ts` — Path handling utilities
- `formatBytes.ts` — Byte formatting
- `logging.ts` — Logging utilities
- `schemaJson.ts` — Schema↔JSON conversion
- `serverSettings.ts` — Settings utilities
- `windowsProcess.ts` — Windows process utilities
- `editorIcons.ts` — Editor icon mappings
- `Net.ts` — Network utilities
- `Struct.ts` — Struct utilities

---

## 3. HIGH-LEVEL: Syncode Architecture Plan (Rust DDD)

### 3.1 Technology Stack Decision

```
┌─────────────────────────────────────────────────┐
│  Frontend (Tauri WebView)                       │
│  Option A: React + Vite (same as MCode)         │
│  Option B: Svelte + SvelteKit                   │
│  Option C: Yew (native Rust WASM)               │
└──────────┬──────────────────────────────────────┘
           │ Tauri IPC / WebSocket
┌──────────▼──────────────────────────────────────┐
│  Backend (Rust)                                 │
│  Framework: Axum (HTTP + WebSocket)              │
│  ORM: SQLx (compile-time SQL)                   │
│  Runtime: Tokio (async)                          │
│  Serialization: Serde + TS-RS (TypeScript bridge) │
│  Validation: Garde / Validator                   │
│  Logging: tracing                               │
│  CLI Provider: tokio::process (stdin/stdout)    │
└──────────┬──────────────────────────────────────┘
           │
┌──────────▼──────────────────────────────────────┐
│  Desktop (Tauri)                                 │
│  Auto-update, tray, native menus, window mgmt    │
│  Cross-platform: macOS, Linux, Windows           │
└─────────────────────────────────────────────────┘
```

### 3.2 Bounded Contexts (DDD)

```
syncode/
├── crates/
│   ├── syncode-core/              # Shared domain kernel
│   │   └── src/
│   │       ├── domain/           # Entities, Value Objects, Events
│   │       ├── application/      # Use cases, Commands, Queries
│   │       └── ports/            # Port interfaces (trait definitions)
│   │
│   ├── syncode-orchestration/     # CQRS/Event Sourcing engine
│   │   └── src/
│   │       ├── commands.rs       # Command definitions
│   │       ├── events.rs         # Domain events
│   │       ├── decider.rs        # Pure command→event logic
│   │       ├── projector.rs      # Event→read model projection
│   │       ├── read_model.rs     # Query projections
│   │       └── reactors/        # Side-effect reactors
│   │
│   ├── syncode-provider/          # Provider abstraction
│   │   └── src/
│   │       ├── trait_def.rs      # ProviderAdapter trait
│   │       ├── registry.rs       # Provider registry
│   │       ├── adapters/         # Per-provider implementations
│   │       │   ├── codex.rs
│   │       │   ├── claude.rs
│   │       │   ├── cursor.rs
│   │       │   ├── gemini.rs
│   │       │   ├── grok.rs
│   │       │   ├── kilo.rs
│   │       │   ├── opencode.rs
│   │       │   └── pi.rs
│   │       └── session.rs        # Session lifecycle
│   │
│   ├── syncode-git/               # Git integration
│   │   └── src/
│   │       ├── service.rs        # GitService trait + impl
│   │       ├── worktree.rs       # Worktree management
│   │       ├── checkpoint.rs    # Checkpoint store (git refs)
│   │       ├── diff.rs          # Diff computation
│   │       └── stacked_actions.rs # Commit→Push→PR pipeline
│   │
│   ├── syncode-terminal/          # Terminal PTY
│   │   └── src/
│   │       ├── pty.rs           # PTY process management
│   │       ├── session.rs       # Terminal sessions
│   │       └── output.rs       # Output buffering
│   │
│   ├── syncode-automation/        # Automation scheduler
│   │   └── src/
│   │       ├── scheduler.rs     # Cron/interval scheduler
│   │       ├── definition.rs    # Automation definition
│   │       ├── runner.rs        # Run lifecycle
│   │       └── policies.rs      # Retry, misfire, completion
│   │
│   ├── syncode-persistence/       # Persistence layer
│   │   └── src/
│   │       ├── event_store.rs    # Event store (SQLite)
│   │       ├── projections.rs    # Read model tables
│   │       ├── migrations/       # SQLx migrations
│   │       └── snapshot.rs       # Snapshot queries
│   │
│   ├── syncode-auth/              # Authentication
│   │   └── src/
│   │       ├── credential.rs    # Credential management
│   │       ├── policy.rs        # Auth policies
│   │       └── secret_store.rs  # Secret storage
│   │
│   ├── syncode-ws/                # WebSocket transport
│   │   └── src/
│   │       ├── server.rs        # WS server (axum)
│   │       ├── rpc.rs           # JSON-RPC handler
│   │       ├── push.rs          # Push bus (ordered)
│   │       ├── channels.rs      # Channel management
│   │       └── transport.rs     # Connection state machine
│   │
│   ├── syncode-http/              # HTTP API (if needed)
│   │   └── src/
│   │       ├── routes.rs
│   │       └── middleware.rs
│   │
│   └── syncode-tauri/             # Tauri desktop integration
│       └── src/
│           ├── main.rs          # Tauri entry
│           ├── commands.rs      # Tauri IPC commands
│           ├── updater.rs       # Auto-update
│           └── tray.rs          # System tray
│
├── frontend/                     # Web UI (React/Svelte)
├── contracts/                    # Shared type definitions
│   └── generated/                # Auto-generated from Rust types
│       ├── orchestration.ts
│       ├── provider.ts
│       ├── git.ts
│       ├── terminal.ts
│       └── ...
└── Cargo.toml                    # Workspace root
```

---

## 4. LOW-LEVEL: Implementation Phases

### Phase 0: Foundation (Week 1-2)
**Goal**: Project scaffolding + basic DDD skeleton

| Task | Description | Dependencies |
|------|-------------|-------------|
| 0.1 | Cargo workspace setup with all crate stubs | None |
| 0.2 | `syncode-core` domain primitives (EntityId, Timestamp, TrimmedString) | 0.1 |
| 0.3 | `syncode-persistence` SQLite setup with SQLx migrations | 0.1 |
| 0.4 | `syncode-ws` basic WebSocket server (axum) | 0.1 |
| 0.5 | `syncode-git` basic GitService trait (status, diff, branch) | 0.1 |
| 0.6 | Tauri app shell with WebView | 0.1 |
| 0.7 | Frontend scaffolding (React + Vite or Svelte) | 0.6 |
| 0.8 | Contract type generation pipeline (Rust→TypeScript via ts-rs) | 0.2 |

### Phase 1: Core Orchestration (Week 3-5)
**Goal**: Working CQRS/ES engine with basic thread management

| Task | Description | Dependencies |
|------|-------------|-------------|
| 1.1 | Domain model: Project, Thread, Turn, Message, Activity | 0.2 |
| 1.2 | Event store (append-only, replay, snapshot) | 0.3, 1.1 |
| 1.3 | Decider: pure command→event logic | 1.1 |
| 1.4 | Projector: event→read model | 1.2, 1.3 |
| 1.5 | Read model queries (getSnapshot, getThread, etc.) | 1.4 |
| 1.6 | WebSocket RPC for orchestration methods | 0.4, 1.5 |
| 1.7 | Push bus for domain events | 0.4, 1.4 |
| 1.8 | Frontend: basic thread list + chat view | 0.7, 1.6 |

### Phase 2: Provider System (Week 5-8)
**Goal**: Multi-provider session management

| Task | Description | Dependencies |
|------|-------------|-------------|
| 2.1 | ProviderAdapter trait (spawn process, send/receive JSON-RPC) | 0.2 |
| 2.2 | Provider registry (discover, configure, status) | 2.1 |
| 2.3 | Session lifecycle (start, resume, interrupt, stop) | 2.1, 1.3 |
| 2.4 | ProviderRuntimeIngestion reactor (provider events→domain events) | 2.3, 1.4 |
| 2.5 | ProviderCommandReactor (domain intent→provider calls) | 2.3, 1.3 |
| 2.6 | Codex adapter implementation (reference) | 2.1 |
| 2.7 | Claude adapter implementation | 2.1 |
| 2.8 | Frontend: provider switcher, model selector | 1.8, 2.2 |
| 2.9 | Frontend: streaming message display | 1.8, 2.4 |

### Phase 3: Git Integration (Week 8-10)
**Goal**: Complete Git workflow

| Task | Description | Dependencies |
|------|-------------|-------------|
| 3.1 | GitService full implementation (using git2 crate) | 0.5 |
| 3.2 | Worktree management (create, remove, detached) | 3.1 |
| 3.3 | CheckpointReactor (capture git refs on turn boundaries) | 3.1, 1.3 |
| 3.4 | Stacked actions (commit→push→PR pipeline) | 3.1 |
| 3.5 | Diff viewer (turn diff, full thread diff) | 3.1, 3.3 |
| 3.6 | WebSocket RPC for all git methods | 0.4, 3.1 |
| 3.7 | Git progress push events | 0.4, 3.4 |
| 3.8 | Frontend: git status panel, diff viewer, branch toolbar | 1.8, 3.6 |

### Phase 4: Terminal (Week 10-11)
**Goal**: Integrated terminal

| Task | Description | Dependencies |
|------|-------------|-------------|
| 4.1 | PTY management (spawn, resize, write, kill) | 0.2 |
| 4.2 | Terminal session lifecycle | 4.1 |
| 4.3 | Output buffering + ack protocol | 4.1 |
| 4.4 | WebSocket RPC for terminal methods | 0.4, 4.2 |
| 4.5 | Terminal push events | 0.4, 4.2 |
| 4.6 | Frontend: terminal panel with tabs | 1.8, 4.4 |

### Phase 5: Automation (Week 11-13)
**Goal**: Scheduled agent runs

| Task | Description | Dependencies |
|------|-------------|-------------|
| 5.1 | Scheduler engine (cron, interval, one-shot) | 0.2 |
| 5.2 | Automation CRUD + run lifecycle | 5.1, 1.3 |
| 5.3 | Retry/misfire/completion policies | 5.2 |
| 5.4 | Heartbeat mode (continuing existing threads) | 5.2 |
| 5.5 | AI-evaluated completion policy | 5.2, 2.4 |
| 5.6 | WebSocket RPC + push events for automation | 0.4, 5.2 |
| 5.7 | Frontend: automation editor, run history | 1.8, 5.6 |

### Phase 6: Desktop Polish (Week 13-15)
**Goal**: Production-quality desktop app

| Task | Description | Dependencies |
|------|-------------|-------------|
| 6.1 | Auto-update (Tauri updater + GitHub releases) | 0.6 |
| 6.2 | System tray integration | 0.6 |
| 6.3 | Native menu + keyboard shortcuts | 0.6 |
| 6.4 | Window management (tabs, split, maximize) | 0.6, 0.7 |
| 6.5 | Browser preview panel | 0.7 |
| 6.6 | Settings UI (provider config, keybindings, general) | 0.7 |
| 6.7 | Diagnostics & performance monitoring | 0.4 |

### Phase 7: Additional Providers (Week 15-18)
**Goal**: All 8 providers from MCode

| Task | Description | Dependencies |
|------|-------------|-------------|
| 7.1 | Cursor adapter | 2.1 |
| 7.2 | Gemini adapter | 2.1 |
| 7.3 | Grok adapter | 2.1 |
| 7.4 | Kilo adapter | 2.1 |
| 7.5 | OpenCode adapter | 2.1 |
| 7.6 | Pi adapter | 2.1 |
| 7.7 | Provider usage monitoring | 2.2 |
| 7.8 | Voice transcription | 2.2 |

### Phase 8: Testing & Quality (Ongoing)
**Goal**: 80%+ coverage

| Task | Description | Dependencies |
|------|-------------|-------------|
| 8.1 | Unit tests for domain logic (decider, invariants) | 1.3 |
| 8.2 | Integration tests for event store | 1.2 |
| 8.3 | Integration tests for provider adapters (mock processes) | 2.6 |
| 8.4 | E2E tests with Tauri WebDriver | 6.1 |
| 8.5 | Property-based testing for domain invariants | 1.1 |
| 8.6 | Load testing for WebSocket connections | 0.4 |
| 8.7 | CI pipeline (GitHub Actions) | All |

---

## 5. Feature Parity Matrix (Detailed)

| # | Feature | MCode | Syncode Priority | Phase |
|---|---------|-------|----------------|-------|
| 1 | Multi-provider AI (8 providers) | ✅ Complete | P0 | 2, 7 |
| 2 | WebSocket JSON-RPC API | ✅ ~80 methods | P0 | 1-6 |
| 3 | Real-time push events | ✅ 9 channels | P0 | 1 |
| 4 | CQRS/Event Sourcing | ✅ Decider+Projector | P0 | 1 |
| 5 | Thread/Project management | ✅ Full CRUD | P0 | 1 |
| 6 | Message streaming | ✅ Streaming + Buffered | P0 | 2 |
| 7 | Git status/diff | ✅ Full | P0 | 3 |
| 8 | Git worktree | ✅ Create/remove/detached | P0 | 3 |
| 9 | Git commit→push→PR | ✅ Stacked actions | P0 | 3 |
| 10 | Git stash | ✅ Stash/checkout/drop | P1 | 3 |
| 11 | Checkpoint system | ✅ Git ref snapshots | P0 | 3 |
| 12 | Turn diff | ✅ Per-turn file changes | P0 | 3 |
| 13 | Terminal PTY | ✅ Full lifecycle | P1 | 4 |
| 14 | Automation scheduler | ✅ Full (cron/interval/etc.) | P1 | 5 |
| 15 | Automation heartbeat | ✅ Self-resuming loops | P2 | 5 |
| 16 | AI completion policy | ✅ NL stop condition | P2 | 5 |
| 17 | Desktop (Tauri) | ✅ Electron | P0 | 6 |
| 18 | Auto-update | ✅ GitHub releases | P1 | 6 |
| 19 | System tray | ✅ (Electron) | P1 | 6 |
| 20 | Native menus | ✅ | P1 | 6 |
| 21 | Browser preview | ✅ | P2 | 6 |
| 22 | Dev server management | ✅ | P2 | 6 |
| 23 | File browsing | ✅ | P1 | 3 |
| 24 | External editor | ✅ | P2 | 6 |
| 25 | Thread import | ✅ | P2 | 1 |
| 26 | Event replay | ✅ | P1 | 1 |
| 27 | Thread handoff | ✅ Provider switch mid-thread | P1 | 2 |
| 28 | Provider usage monitoring | ✅ Token tracking | P1 | 7 |
| 29 | Voice transcription | ✅ | P3 | 7 |
| 30 | Thread recap | ✅ AI-generated summary | P2 | 2 |
| 31 | Slash commands | ✅ | P2 | 2 |
| 32 | Agent mentions | ✅ | P2 | 2 |
| 33 | Keybindings | ✅ Configurable | P2 | 6 |
| 34 | Profile/stats | ✅ | P2 | 7 |
| 35 | Kanban board | ✅ | P3 | Future |
| 36 | PDF support | ✅ | P3 | Future |
| 37 | Auth (bootstrap + session) | ✅ | P0 | Phase 0 |
| 38 | Server diagnostics | ✅ | P1 | 6 |
| 39 | Settings persistence | ✅ | P1 | 6 |
| 40 | Environment variables per thread | ✅ | P1 | 2 |
| 41 | Pinned messages | ✅ | P2 | 2 |
| 42 | Thread markers | ✅ | P2 | 2 |
| 43 | Error recovery | ✅ Reconnect/rehydrate | P0 | 1 |
| 44 | Local server discovery | ✅ | P2 | 6 |
| 45 | Marketing site | ✅ (separate app) | P3 | Future |

---

## 6. Architecture Decisions Log

### AD-001: Rust over TypeScript
- **Context**: MCode is TypeScript/Bun. Syncode targets Rust.
- **Decision**: Use Rust with Axum for the backend.
- **Rationale**: Performance, memory safety, better concurrency model (Tokio), stronger type system for DDD.
- **Trade-off**: Slower development velocity vs. runtime performance and safety.

### AD-002: Tauri over Electron
- **Context**: MCode uses Electron. Syncode should use Tauri.
- **Decision**: Tauri v2 for desktop shell.
- **Rationale**: ~10x smaller bundle size, lower memory, Rust backend shared with server.
- **Trade-off**: Smaller ecosystem than Electron, but growing rapidly.

### AD-003: SQLx over diesel
- **Context**: Need async SQLite/PostgreSQL access.
- **Decision**: SQLx for compile-time checked SQL.
- **Rationale**: Async-native, no ORM abstraction leak, direct SQL control.
- **Trade-off**: More verbose than diesel for simple CRUD, but better for complex queries.

### AD-004: Effect Schema → Serde + ts-rs
- **Context**: MCode uses Effect Schema for type-safe contracts. Rust needs equivalent.
- **Decision**: Serde for serialization + ts-rs for TypeScript bridge generation.
- **Rationale**: Standard Rust ecosystem, ts-rs auto-generates TypeScript types from Rust structs.
- **Trade-off**: Less runtime validation than Effect Schema; need garde/validator for input validation.

### AD-005: CQRS/ES in Rust
- **Context**: MCode implements CQRS/ES manually. Rust has event-sourcing crates.
- **Decision**: Manual implementation (like MCode) for maximum control.
- **Rationale**: MCode's pattern is proven; no need for heavy framework. Keep decider/projector pattern.
- **Trade-off**: More code to write, but full control over event store, snapshot strategy, etc.

---

## 7. Risk Register

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Provider protocol changes | High | Medium | Abstract behind trait; adapter pattern isolates changes |
| Tauri ecosystem gaps | Medium | Low | Fallback to custom implementations; Tauri v2 is mature |
| Frontend rewrite complexity | High | High | Consider reusing MCode's React components with Rust backend |
| Performance regression in Rust port | Low | Low | Rust should be faster; profile early |
| DDD over-engineering | Medium | Medium | Start pragmatic; add DDD patterns as complexity grows |
| SQLite limitations under load | Medium | Low | Support PostgreSQL as alternative via SQLx |

---

## 8. Quick Start (Skeleton)

```bash
# 1. Create Cargo workspace
mkdir syncode && cd syncode
cargo init --name syncode-core

# 2. Add workspace members in Cargo.toml
[workspace]
members = [
    "crates/syncode-core",
    "crates/syncode-orchestration",
    "crates/syncode-provider",
    "crates/syncode-git",
    "crates/syncode-terminal",
    "crates/syncode-automation",
    "crates/syncode-persistence",
    "crates/syncode-auth",
    "crates/syncode-ws",
    "crates/syncode-tauri",
]

# 3. Key dependencies per crate
[dependencies]
# syncode-ws
axum = { version = "0.8", features = ["ws"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
ts-rs = { version = "7", features = ["serde-compat"] }
tracing = "0.1"

# syncode-persistence
sqlx = { version = "0.8", features = ["runtime-tokio", "sqlite"] }

# syncode-git
git2 = "0.19"

# syncode-terminal
portable-pty = "0.8"

# syncode-tauri
tauri = { version = "2", features = ["..."] }
tauri-plugin-updater = "2"
```

---

## 9. Legacy Workflow State

The `.masday/state/workflows/` contains a stale workflow from June 2026:
- **Name**: "T3Code Feature Implementation for Syncode"
- **Status**: EXECUTE (stale)
- **Tasks**: 40 tasks, 6 DONE, 5 RUNNING, 29 PENDING
- **Key completed tasks**: AIProvider trait, CLI adapter, event sourcing, session lifecycle, GitService, Git routes
- **Key pending tasks**: Streaming pipeline, provider registry, session history, multi-project, frontend components, testing, security

This workflow state is **obsolete** and should be cleaned up. The plan in this document supersedes it.

---

*Document generated by masday-code-analyze skill. Last updated: 2026-06-27*

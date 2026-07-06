# PRD: Syncode Remaining Gaps — Feature Parity Roadmap

> ⛔ **STATUS: SUPERSEDED — SHIPPED (2026-07-06).** A parity audit on 2026-07-06 confirmed that **all 31 sub-tasks across P0–P5 are implemented and merged to `master`** via PRs #79, #85, #87, #90, #98, #99, #100, #102, #103, #108 (+ p0-2/p0-3 symbols). The gap analysis below describes **pre-implementation** state and is retained for historical/design context only — **do not treat the "What Syncode LACKS" tables as current.** 576 tests across the in-scope crates pass (0 fail). See [Appendix C: Audit Results](#appendix-c-audit-results-2026-07-06) for the verified parity matrix.
>
> **The only genuine remaining gap is P2-2's production harness** (the AI-completion *evaluator* is complete and tested, but the MCode-style bounded-queue + 2-worker + 30s-timeout host that drives it is absent). P2-8 has a minor caveat: the worktree lifecycle is real but the worktree path is not yet threaded into `DispatchRequest.working_dir`. Everything else is fully delivered.
>
> ---
>
> **Original status (historical):** Authored 2026-07-05. Covered the remaining feature gaps between Syncode and MCode after the STUB→REAL workflow (PRs #49–#78) closed all served-RPC gaps. This document defined **what to build next** to achieve full functional parity with MCode's production backend. Each section contains user stories, current-state analysis, target design (grounded in MCode's architecture), acceptance criteria, task breakdown, and risk register.

---

## Executive Summary

The STUB→REAL workflow closed all RPC-coverage gaps (every served method returns real data). However, comparing against MCode's production backend reveals depth gaps — features where "serving an RPC" ≠ "feature parity." This PRD covers 6 gap areas with ~31 tasks total.

### Priority Ranking

| Priority | Epic | Tasks | Complexity | Impact |
|---|---|---|---|---|
| **P0** | Provider Session Management | 7 | XL | 🔴 Critical — core AI interaction quality |
| **P1** | Agentic Workflow Orchestration | 6 | L | 🔴 Critical — supervised agent pipeline |
| **P2** | Automation Reactor + Completion | 8 | L | 🟡 Medium — advanced automation |
| **P3** | Persistent Memory | 4 | M | 🟡 Medium — context grounding |
| **P4** | Terminal Persistence + Workspace | 4 | M | 🟢 Low — quality of life |
| **P5** | importThread | 3 | L | 🟢 Low — niche feature |

### Dependency Graph

```
Phase A (parallel — no inter-deps):
  P0-1 (fix create_by_id)          ← unblocks all provider work
  P3-1 (memory crate skeleton)     ← unblocks agentic workflow
  P4-1 (terminal persistence)
  P2-1 (completion_policy field)

Phase B (depends on Phase A):
  P0-2 (token streaming)           ← depends P0-1
  P0-3 (steerTurn)                 ← depends P0-1
  P0-4 (resume cursors)            ← depends P0-1
  P3-2 (context retrieval)         ← depends P3-1
  P3-3 (persistence)               ← depends P3-1
  P1-1 (AgentState model)          ← depends P3-1
  P2-3 (run reactor)               ← depends P2-1

Phase C (depends on Phase B):
  P0-5 (ensureSession)             ← depends P0-3, P0-4
  P0-6 (idle auto-stop)            ← depends P0-5
  P0-7 (queued turn pipeline)      ← depends P0-5
  P1-2 (executeStep)               ← depends P1-1
  P1-3 (guardrails)                ← depends P1-1
  P3-4 (inject into sessions)      ← depends P0-5, P3-2
  P2-2 (AI completion eval)        ← depends P2-1

Phase D (depends on Phase C):
  P1-4 (executeWorkflow)           ← depends P1-2, P1-3, P3-2
  P1-5 (provider integration)      ← depends P1-4, P0-5
  P1-6 (memory integration)        ← depends P1-4, P3-4
  P2-4 (crash recovery)            ← depends P2-3
  P2-5 (wakeable scheduler)        ← depends P2-3
  P2-6..8 (caps/modes/worktree)    ← depends P2-1

Phase E (independent):
  P4-2,3,4 (workspace)             ← no deps
  P5-1,2,3 (importThread)          ← depends P0-4 (resume cursors)
```

---

## Section 1: Provider Session Management (P0, XL)

### User Stories
- **As a user**, I want AI responses to stream token-by-token so I see progress immediately, not wait for the full response.
- **As a user**, I want to steer a turn mid-generation ("wait, also do X") without starting a new turn.
- **As a user**, I want my conversation context to survive a server restart — the provider should resume where we left off.
- **As a user**, I want queued turns to wait for the prior turn to complete, not collide.

### Current State

**What Syncode HAS (from exploration):**
- `ProviderAdapter` trait (`trait_def.rs:269-325`) with `start_session`, `send_request`, `event_stream`, `interrupt`, `stop_session` — full session lifecycle.
- `SessionManager` (`session.rs:201-414`) — in-memory turn→session tracking, state machine (Pending→Processing→Completed/Interrupted/Errored).
- `ProviderCommandReactor` (`command.rs:34`) — reacts to StartTurn/Cancel/Fail commands, starts provider session, sends "chat" request **synchronously**.
- Streaming via `spawn_provider_stream_consumer` (`pipeline.rs:635-668`): consumes `event_stream`, ingests `ProviderEvent`s into domain events.
- `ProviderEvent` enum (`trait_def.rs:148-179`): Started, Token, ToolCall, ToolResult, Completed, Error.

**What Syncode LACKS vs MCode:**

| Feature | MCode | Syncode | Gap |
|---|---|---|---|
| Token streaming to client | `content.delta` events pushed live | `Token` events consumed silently (no domain event) | 🔴 |
| Turn steering | `steerTurn` redirects in-flight turn | `DispatchQueuedTurn` always sends new turn | 🔴 |
| Resume cursors | Durable provider thread ID stored, rehydrated on restart | Sessions are in-memory, lost on restart | 🔴 |
| `ensureSessionForThread` | Detects model/provider/runtime-mode changes → restarts with cursor | Sessions created once, no change detection | 🟡 |
| Idle session auto-stop | TTL-based auto-stop with generation-token guard | Sessions persist indefinitely | 🟡 |
| Queued turn pipeline | Turn waits for prior `turn.completed` before dispatching | Turns dispatched immediately (collision risk) | 🟡 |
| Production factory | All 8 providers armable | `create_by_id` only returns Some for cursor/grok/gemini — claude/codex CANNOT be armed | 🔴 |

### Target Design (from MCode Architecture)

MCode's architecture uses two parallel reactors communicating through the event-sourced `OrchestrationEngine`:

1. **ProviderCommandReactor** (outbound): reacts to "provider-intent" domain events (`turn-start-requested`, `approval-response-requested`, etc.). Calls `ensureSessionForThread` → `sendTurn`/`steerTurn`/`startReview`.
2. **ProviderRuntimeIngestion** (inbound): consumes `ProviderService.streamEvents`, translates each `ProviderRuntimeEvent` into orchestration commands (`thread.message.*`, `thread.activity.*`, `thread.session.set`).

Key design principles:
- **Sessions are resumable, not permanent** — each adapter maintains a `resumeCursor` (provider-native thread ID + resume point). `ensureSessionForThread` restarts lazily on changes.
- **Turns are queued** — the reactor decides when a queued turn becomes a real provider turn based on whether a turn is already active.
- **Capabilities are declarative** — `ProviderAdapterCapabilities` (`sessionModelSwitch`, `supportsTurnSteering`, etc.) queried at runtime.

### Acceptance Criteria
- [ ] Token events produce `MessageDelta` domain events → pushed to subscribed clients in real-time
- [ ] `steerTurn` available when provider supports it (capability-gated); redirects in-flight turn
- [ ] Server restart rehydrates provider sessions from persisted resume cursors
- [ ] `ensureSessionForThread` detects model/provider changes and restarts session with cursor
- [ ] Idle sessions auto-stop after configurable TTL (default 10 min)
- [ ] Queued turns wait for prior turn completion before dispatching
- [ ] `create_by_id` factory returns Some for all 10 providers
- [ ] ≥3 tests per task

### Task Breakdown

| ID | Title | Complexity | Deps | Agent |
|---|---|---|---|---|
| P0-1 | Fix `create_by_id` factory: support all 10 providers | M | — | masday-backend |
| P0-2 | Token streaming: emit `MessageDelta` domain events from Token ProviderEvents | L | P0-1 | masday-backend |
| P0-3 | `steerTurn`: add to ProviderCommandReactor (DispatchQueuedTurn → steer if active) | M | P0-1 | masday-backend |
| P0-4 | Resume cursor persistence: store provider thread ID on SessionState; rehydrate on restart | L | P0-1 | masday-backend |
| P0-5 | `ensureSessionForThread`: detect model/provider/runtime-mode changes → restart with cursor | L | P0-3, P0-4 | masday-backend |
| P0-6 | Idle session auto-stop: configurable TTL + generation-token guard | M | P0-5 | masday-backend |
| P0-7 | Queued turn pipeline: turn.queued → wait for prior turn.completed → dispatch | L | P0-5 | masday-backend |

### Risk Register
| Risk | Mitigation |
|---|---|
| Provider CLI doesn't support resume — cursor is useless | Probe provider capability; graceful fallback to fresh session |
| Token streaming overwhelms WS push channel | Batch tokens (100ms window) before pushing; sliding-window buffer |
| Session restart races with new turn dispatch | Generation-token guard (MCode pattern); await in-flight restart before dispatch |

---

## Section 2: Automation Reactor + Completion Policies (P2, L)

### User Stories
- **As a user**, I want automations to auto-stop when the AI detects the task is done ("stop when tests pass").
- **As a user**, I want automation run status to update automatically when the underlying turn succeeds/fails/pauses — not require manual polling.
- **As a user**, I want automations to recover gracefully after a server crash.

### Current State

**Where Syncode LEADS:**
- `RetryPolicy` with exponential backoff + fixed delay (MCode stubs retry — "not supported yet")
- `MisfirePolicy` (Skip/RunImmediately/RunNext) — functional, naming differs from MCode

**Where Syncode LACKS:**

| Feature | MCode | Syncode |
|---|---|---|
| AI-evaluated completion | Full: queue + 2 workers + 30s timeout + stale-check + versioning | `AiEvaluated` variant declared but inert (`exit_code == 0`) |
| AutomationRunReactor | Subscribes to orchestration events; reconciles run status | Absent — only manual `complete_run`/`fail_run` |
| Wakeable scheduler | Event-driven wakeup on definition-upserted/deleted | Bare `tick()` poll, host-driven |
| maxIterations + stopOnError | Cross-run caps + auto-disable | Absent (only per-run `timeout_secs`/`max_retries`) |
| maxRuntimeSeconds | Per-run timeout | Absent |
| worktreeMode | Per-run git worktree creation/cleanup | Absent |
| permissionSnapshot | Provider/model/mode frozen per run | Absent |

### Target Design

MCode's `AutomationRunReactor` (`automation/Services/AutomationRunReactor.ts`):
- Subscribes to `orchestrationEngine.streamDomainEvents`
- Filters to lifecycle events: `turn-diff-completed`, `approval-response-requested`, `user-input-response-requested`, `turn-interrupt-requested`, `thread.reverted`, `conversation-rolled-back`, `session-set`
- Uses `makeDrainableWorker` with per-thread dedupe (concurrent events for one thread coalesce)
- On startup: `recoverPendingRuns()` (crash recovery)
- `reconcileThread`: maps thread shell state → run status (waiting-for-approval / succeeded / failed / interrupted)

MCode's AI-evaluated completion (`AutomationService.ts:~600 lines`):
- Bounded queue (cap 100) + 2 worker fibers
- `evaluateCompletionPolicy()`: LLM call with `stopWhen` + run context + 30s timeout
- Stale-check guard: reloads definition after slow AI call; discards if policy version changed
- On match (`confidence >= 0.8`): disables automation, records result

### Task Breakdown

| ID | Title | Complexity | Deps | Agent |
|---|---|---|---|---|
| P2-1 | Add `completion_policy` field to `AutomationDef` + per-automation config | S | — | masday-backend |
| P2-2 | AI-evaluated completion: LLM call + confidence threshold + stale-check + versioning | XL | P2-1 | masday-backend |
| P2-3 | AutomationRunReactor: subscribe to domain events, reconcile run status | L | P2-1 | masday-backend |
| P2-4 | Crash recovery: `recoverPendingRuns` on startup | M | P2-3 | masday-backend |
| P2-5 | Wakeable scheduler loop (event-driven wakeup on definition changes) | M | P2-3 | masday-backend |
| P2-6 | maxIterations + stopOnError cross-run caps | M | P2-1 | masday-backend |
| P2-7 | maxRuntimeSeconds per-run timeout | S | P2-1 | masday-backend |
| P2-8 | worktreeMode (create/cleanup git worktree per standalone run) | L | P2-1 | masday-backend |

### Risk Register
| Risk | Mitigation |
|---|---|
| AI completion eval hangs | 30s timeout + bounded queue; stale-check discards late results |
| RunReactor event ordering issues | Drainable worker with per-thread dedupe (MCode pattern) |
| Crash recovery misses runs | `listRecoverableRuns` on startup + idempotent reconcile |

---

## Section 3: Terminal Persistence + Workspace (P4, M)

### User Stories
- **As a user**, I want terminal scrollback to survive session close and server restart.
- **As a user**, I want `filesystem.browse` to open a real native file picker.

### Current State

| Feature | MCode | Syncode |
|---|---|---|
| Terminal scrollback | Disk persistence (atomic rename, ANSI-safe cap, read-on-open, debounced writes) | In-memory ring only (1000 chunks × 4KB); no persistence |
| `resolveFileBySuffix` | Workspace-index lookup for bare names | Absent |
| Managed-worktree `.git` pointer | Parsed to resolve real CWD | Absent |
| `filesystem.browse` | Native dialog (`tauri-plugin-dialog`) | Platform-limited fallback (returns empty) |

### Task Breakdown

| ID | Title | Complexity | Deps | Agent |
|---|---|---|---|---|
| P4-1 | Terminal scrollback persistence (SQLite or file; atomic write; ANSI-safe cap; read-on-open) | L | — | masday-backend |
| P4-2 | `resolveFileBySuffix` in project_fs (bare-name → workspace-index lookup) | S | — | masday-backend |
| P4-3 | Managed-worktree `.git` pointer parsing | M | — | masday-backend |
| P4-4 | Real `filesystem.browse` (wire `tauri-plugin-dialog`) | M | — | masday-frontend |

---

## Section 4: importThread (P5, L)

### User Stories
- **As a user**, I want to import an existing Claude/Codex conversation into a Syncode thread, preserving context and enabling continuation.

### Current State

| Feature | MCode | Syncode |
|---|---|---|
| `orchestration.importThread` | Full: 4-provider import (Claude/Codex/Kilo/OpenCode) + `thread.messages.import` + resumeCursor | Completely absent |

### Task Breakdown

| ID | Title | Complexity | Deps | Agent |
|---|---|---|---|---|
| P5-1 | `thread.messages.import` orchestration command + event | M | — | masday-backend |
| P5-2 | Per-provider `readExternalThread` on ProviderAdapter trait | L | — | masday-backend |
| P5-3 | `orchestration.importThread` RPC handler (compose: resolve → read external → import → set session) | M | P5-1, P5-2, P0-4 | masday-backend |

---

## Section 5: Persistent Memory (P3, M)

### User Stories
- **As a user**, I want the AI to remember context from prior interactions in this project — codebase conventions, decisions, recurring patterns.
- **As a developer**, I want a `MemoryProvider` abstraction that retrieves and persists interaction context, injectable into any agent pipeline.

### Current State

No persistent memory layer exists in either MCode or Syncode. Every provider session starts with a hardcoded system prompt (`"You are a helpful AI coding assistant."` at `command.rs:342`). No conversation history, no project conventions, no decision log.

### Target Design

Based on the user-provided agentic workflow architecture:

```
┌─────────────────────────────────────────┐
│  MemoryProvider trait                    │
│  - retrieveContext(userId, query) → str  │
│  - persistInteraction(userId, q, a)      │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  SQLiteMemoryStore                       │
│  - interactions table:                   │
│    (user_id, project_id, prompt,         │
│     response, provider, tokens, ts)      │
│  - retrieveContext: SELECT last N        │
│    interactions ORDER BY ts DESC         │
│  - persistInteraction: INSERT row        │
└─────────────────────────────────────────┘
```

Context retrieval returns a formatted markdown string of the N most recent interactions:
```
## Prior Context

### Interaction 1 (2026-07-04, claude)
**User:** How do I add a new RPC handler?
**Assistant:** Add a dispatch arm in rpc.rs...

### Interaction 2 (2026-07-04, claude)
**User:** Fix the terminal output bug
**Assistant:** The issue is in output.rs...
```

This string is injected into the provider session's system prompt at startup.

### Task Breakdown

| ID | Title | Complexity | Deps | Agent |
|---|---|---|---|---|
| P3-1 | `syncode-memory` crate: `MemoryProvider` trait + SQLite-backed `SqliteMemoryStore` | M | — | masday-database-arch |
| P3-2 | Context retrieval: pull N most recent interactions per user/project, format as markdown | S | P3-1 | masday-backend |
| P3-3 | Persistence: store prompt-response pairs with metadata (provider, timestamp, tokens) | S | P3-1 | masday-backend |
| P3-4 | Integration: inject retrieved context into provider session startup as system prompt augmentation | M | P0-5, P3-2 | masday-integrator |

### Risk Register
| Risk | Mitigation |
|---|---|
| Context window overflow (too many interactions) | Cap at N=3 most recent; summarize older interactions |
| Privacy: storing conversation content | Per-project scoping; clear-on-delete; document retention |

---

## Section 6: Agentic Workflow Orchestration (P1, L)

### User Stories
- **As a user**, I want to define a multi-step agent workflow: "Plan → Execute → Validate → Persist" with guardrails at each step.
- **As a developer**, I want a supervised sequential pipeline where each step's output feeds the next, with automatic failure routing.
- **As a developer**, I want guardrails that validate output before committing state.

### Current State

No agentic workflow orchestration exists in Syncode. Provider turns are single-step (user sends prompt → provider responds → turn completes). There's no concept of a multi-step supervised pipeline with planning, execution, validation, and state persistence phases.

### Target Design

Based on the user-provided architecture, this is a **supervised sequential agent pipeline**:

```
[Initialization] → [Context Retrieval] → [Step 1: Planning] → [Step 2: Execution] → [Guardrail Validation] → [State Persistence]
                        │                        │                       │
                        └── (Fail) ──────────────┴─────── (Fail) ────────┘
                                                         │
                                                         ▼
                                              [Fallback Protocol / Abort]
```

#### Component Architecture

**`AgentState`** — the deterministic state frame:
```rust
pub struct AgentState {
    pub current_step: WorkflowStep,      // Initialization, Planning, Execution, Guardrails, Completed, Failed
    pub memory: AgentMemory,             // ephemeral + long_term_summary
    pub execution_logs: Vec<String>,     // tracking log
    pub is_completed: bool,
    pub workflow_id: String,
    pub user_id: String,
    pub initial_task: String,
}

pub struct AgentMemory {
    pub ephemeral: HashMap<String, String>,
    pub long_term_summary: String,
}
```

**`executeStep`** — the controlled step execution wrapper:
```rust
pub async fn execute_step<F, T>(
    step_name: &str,
    action: F,
    state: &mut AgentState,
) -> Result<StepResult<T>, WorkflowError>
where F: FnOnce() -> Result<T, WorkflowError>
{
    state.current_step = WorkflowStep::from_str(step_name);
    state.execution_logs.push(format!("[Harness] Starting step: {step_name}"));
    match action() {
        Ok(result) => {
            state.execution_logs.push(format!("[Harness] Step {step_name} completed"));
            Ok(StepResult::Success(result))
        }
        Err(e) => {
            handle_workflow_failure(state, &e).await;
            state.current_step = WorkflowStep::Failed;
            Err(e)
        }
    }
}
```

**`runOutputGuardrails`** — the validation gate:
```rust
pub fn run_output_guardrails(raw_output: &str, state: &mut AgentState) -> Result<GuardrailResult> {
    if raw_output.trim().is_empty() {
        let err = WorkflowError::GuardrailViolation(
            "Empty payload generated.".into()
        );
        handle_workflow_failure(state, &err);
        return Err(err);
    }
    Ok(GuardrailResult::Validated(raw_output.to_string()))
}
```

**`executeWorkflow`** — the orchestrator:
```rust
pub async fn execute_workflow(
    user_id: &str,
    workflow_id: &str,
    initial_task: &str,
    memory: &dyn MemoryProvider,
    adapter: &dyn ProviderAdapter,
) -> Result<AgentState, WorkflowError> {
    // 1. Initialization
    let mut state = AgentState::new(workflow_id, user_id, initial_task);

    // 2. Context Retrieval (Memory Grounding)
    let context = memory.retrieve_context(user_id, &initial_task);

    // 3. Planning
    let plan = execute_step("Planning", || {
        adapter.plan(&initial_task, &context)
    }, &mut state).await?;

    // 4. Execution
    let execution = execute_step("Execution", || {
        adapter.execute(&plan)
    }, &mut state).await?;

    // 5. Guardrail Validation
    let validated = run_output_guardrails(&execution, &mut state)?;

    // 6. State Persistence
    state.current_step = WorkflowStep::Completed;
    state.is_completed = true;
    state.execution_logs.push(
        "[Harness] Workflow completed successfully with zero drift.".into()
    );
    memory.persist_interaction(user_id, &initial_task, &validated);

    Ok(state)
}
```

#### Component Lifecycle Matrix

| Stage | Component | Primary Objective | Inputs | Output / Side Effect |
|---|---|---|---|---|
| **Initialization** | `AgentState::new` | Construct deterministic state frame | `workflow_id`, `user_id`, `initial_task` | Fresh `AgentState` |
| **Context** | `memory.retrieve_context` | Fact-grounding | `userId`, `query` | Context string (3 recent interactions or "No prior context") |
| **Planning** | `execute_step("Planning", ...)` | Generate plan | Context + task | Plan string |
| **Execution** | `execute_step("Execution", ...)` | Execute plan | Plan string | Output string |
| **Guardrails** | `run_output_guardrails` | Validate output | Raw output | Validated string or GuardrailViolation |
| **Persistence** | `memory.persist_interaction` | Save to memory | userId, prompt, response | Updated memory store |

### Task Breakdown

| ID | Title | Complexity | Deps | Agent |
|---|---|---|---|---|
| P1-1 | `AgentState` frame model + `WorkflowStep` enum + `AgentMemory` struct | M | P3-1 | masday-backend |
| P1-2 | `execute_step` wrapper: step name → state update → action → error capture → StepResult | M | P1-1 | masday-backend |
| P1-3 | `run_output_guardrails`: payload validation (null/empty/whitespace) + failure routing | S | P1-1 | masday-backend |
| P1-4 | `execute_workflow` orchestrator: sequential pipeline with failure routing at each step | L | P1-2, P1-3, P3-2 | masday-backend |
| P1-5 | Provider integration: plan/execute steps invoke provider via `ProviderAdapter` | L | P1-4, P0-5 | masday-backend |
| P1-6 | Memory integration: context retrieval before plan; persist after completion | M | P1-4, P3-4 | masday-integrator |

### Risk Register
| Risk | Mitigation |
|---|---|
| Planning step produces vague/unactionable plan | Guardrail validates plan structure (not just non-empty) |
| Execution diverges from plan | Optional re-planning loop (plan → execute → compare → re-plan if diverged) |
| Long-running workflows block the reactor | Run as async task with timeout; cancellation via state.current_step = Aborted |
| Provider unavailability mid-workflow | Failure routing captures error; workflow state = Failed with diagnostic logs |

---

## Appendix A: Complete Task List (31 tasks)

| ID | Epic | Title | Complexity | Priority |
|---|---|---|---|---|
| P0-1 | Provider | Fix `create_by_id` factory (all 10 providers) | M | P0 |
| P0-2 | Provider | Token streaming → `MessageDelta` domain events | L | P0 |
| P0-3 | Provider | `steerTurn` in ProviderCommandReactor | M | P0 |
| P0-4 | Provider | Resume cursor persistence | L | P0 |
| P0-5 | Provider | `ensureSessionForThread` | L | P0 |
| P0-6 | Provider | Idle session auto-stop | M | P0 |
| P0-7 | Provider | Queued turn pipeline | L | P0 |
| P1-1 | Agentic | `AgentState` frame model | M | P1 |
| P1-2 | Agentic | `execute_step` wrapper | M | P1 |
| P1-3 | Agentic | `run_output_guardrails` | S | P1 |
| P1-4 | Agentic | `execute_workflow` orchestrator | L | P1 |
| P1-5 | Agentic | Provider integration | L | P1 |
| P1-6 | Agentic | Memory integration | M | P1 |
| P2-1 | Automation | `completion_policy` field | S | P2 |
| P2-2 | Automation | AI-evaluated completion | XL | P2 |
| P2-3 | Automation | AutomationRunReactor | L | P2 |
| P2-4 | Automation | Crash recovery | M | P2 |
| P2-5 | Automation | Wakeable scheduler | M | P2 |
| P2-6 | Automation | maxIterations + stopOnError | M | P2 |
| P2-7 | Automation | maxRuntimeSeconds | S | P2 |
| P2-8 | Automation | worktreeMode | L | P2 |
| P3-1 | Memory | `syncode-memory` crate + SQLite store | M | P3 |
| P3-2 | Memory | Context retrieval | S | P3 |
| P3-3 | Memory | Persistence | S | P3 |
| P3-4 | Memory | Inject into provider sessions | M | P3 |
| P4-1 | Terminal | Scrollback persistence | L | P4 |
| P4-2 | Workspace | `resolveFileBySuffix` | S | P4 |
| P4-3 | Workspace | Managed-worktree `.git` parsing | M | P4 |
| P4-4 | Workspace | Real `filesystem.browse` | M | P4 |
| P5-1 | importThread | `thread.messages.import` command | M | P5 |
| P5-2 | importThread | Per-provider `readExternalThread` | L | P5 |
| P5-3 | importThread | `orchestration.importThread` RPC | M | P5 |

## Appendix B: Suggested Execution Order

```
Sprint 1 (Foundation):
  P0-1 (fix factory)  →  unblocks all provider work
  P3-1 (memory crate) →  unblocks agentic workflow
  P2-1 (completion field)
  P4-1 (terminal persistence)

Sprint 2 (Core provider):
  P0-2 (token streaming)
  P0-3 (steerTurn)
  P0-4 (resume cursors)
  P1-1 (AgentState)

Sprint 3 (Deep provider + agentic):
  P0-5 (ensureSession)
  P1-2 (executeStep)
  P1-3 (guardrails)
  P3-2, P3-3 (memory retrieval + persistence)

Sprint 4 (Pipeline assembly):
  P0-6 (idle auto-stop)
  P0-7 (queued turns)
  P1-4 (executeWorkflow)
  P2-3 (run reactor)

Sprint 5 (Integration + automation):
  P1-5 (provider integration)
  P1-6 (memory integration)
  P3-4 (inject into sessions)
  P2-2 (AI completion)
  P2-4, P2-5 (crash recovery + wakeable)

Sprint 6 (Polish + long tail):
  P2-6, P2-7, P2-8 (caps/modes/worktree)
  P4-2, P4-3, P4-4 (workspace)
  P5-1, P5-2, P5-3 (importThread)
```

---

*For the current status matrix see [`STATUS.md`](./STATUS.md); for the MCode architecture deep-dive see [`COMPARISON-MCODE-vs-SYNCODE.md`](./COMPARISON-MCODE-vs-SYNCODE.md).*

---

## Appendix C: Audit Results (2026-07-06)

A symbol+test parity audit confirmed every sub-task is implemented and merged to `master`. **576 tests across the in-scope crates pass (0 fail).** All 21 P1–P5 sub-tasks are **REAL** (functional + tested); P0 was verified separately (provider 303 tests).

| Sub-task | Status | Evidence (file) | Tests |
|---|---|---|---|
| P0-1..P0-7 | ✅ REAL | `registry.rs:222` (create_by_id, 10 providers), `pipeline.rs` (MessageDeltaAppended token streaming), `session.rs` (ResumeCursorStore, idle_stop), `reactors/command.rs` (ensure_session_for_thread, TurnQueue) — PRs #79/#85/#87/#90 | 303 |
| P1-1 AgentState | ✅ REAL | `core/agent/state.rs` — full struct + 6-variant enum + serde | 10 |
| P1-2 execute_step | ✅ REAL | `core/agent/harness.rs:79` — step advance + failure routing | 3 |
| P1-3 run_output_guardrails | ✅ REAL | `core/agent/harness.rs:118` — empty/whitespace reject | 4 |
| P1-4 execute_workflow | ✅ REAL | `orchestration/workflow.rs:108` — 6-step pipeline + 4 failure paths | 5 |
| P1-5 provider integration | ✅ REAL | `orchestration/workflow.rs:223` ProviderWorkflowExecutor — JSON-RPC plan/execute | 5 |
| P1-6 memory integration | ✅ REAL | `orchestration/workflow.rs:1006,1121` — SQLite retrieve→persist e2e (PR #98) | 2 |
| P2-1 completion_policy | ✅ REAL | `automation/definition.rs:85` + `policies.rs:83` AiEvaluated{stop_when,confidence_threshold} + version | ~6 |
| **P2-2 AI-evaluated completion** | 🟡 **PARTIAL** | `automation/completion_eval.rs:273` — evaluator COMPLETE (prompt builder, parse_confidence, stale-check, version guard). **MISSING: production harness** (bounded queue cap 100 + 2 worker fibers + 30s timeout) — no always-on host drives the evaluator | 16 (evaluator) |
| P2-3 AutomationRunReactor | ✅ REAL | `automation/run_reactor.rs:239` — subscribes DomainEventStream, 6 lifecycle events, per-thread drain+coalesce | 6 |
| P2-4 recover_pending_runs | ✅ REAL | `automation/run_reactor.rs:427` — startup sweep, ThreadLivenessProbe, RecoveryReport | 4 |
| P2-5 wakeable scheduler | ✅ REAL | `automation/scheduler.rs:75` — Arc<Notify> + wakeable sleep + notify on register/update | 3 |
| P2-6 maxIterations + stopOnError | ✅ REAL | `automation/definition.rs:104,112,119` + executor caps (PR #99) | ~6 |
| P2-7 maxRuntimeSeconds | ✅ REAL | `automation/definition.rs:128` + `executor.rs def_max_runtime()` (PR #99) | 1+ |
| **P2-8 worktreeMode** | ✅ REAL (caveat) | `automation/worktree.rs:82` WorktreeManager — real git worktree add/remove (PR #99). **Caveat:** worktree path not yet threaded into `DispatchRequest.working_dir` — runs isolated on create/cleanup but command not executed inside worktree | 12 |
| P3-1 MemoryProvider + SqliteMemoryStore | ✅ REAL | `memory/provider.rs:45` + `sqlite_store.rs:58` — interactions table, WAL, in-memory + file | 9 |
| P3-2 context retrieval | ✅ REAL | `memory/sqlite_store.rs:171` retrieve_context — ORDER BY ts DESC LIMIT N, markdown | (in 9) |
| P3-3 persistence | ✅ REAL | `memory/sqlite_store.rs:203` persist_interaction — metadata + RFC-3339 ts | (in 9) |
| P3-4 inject into session | ✅ REAL | `orchestration/reactors/command.rs:284,799` augment_ctx_with_memory — PR #98 | 3 |
| P4-1 scrollback persistence | ✅ REAL | `terminal/persistence.rs` ScrollbackStore — atomic tmp+fsync+rename, truncate_ansi_safe @256KiB, read-on-open/save-on-close | 17 |
| P4-2 resolveFileBySuffix | ✅ REAL | `ws/project_fs.rs:357` — basename/substring/path-suffix (PR #100) | ~4 |
| P4-3 parseGitWorktreePointer | ✅ REAL | `ws/project_fs.rs:444` — `gitdir:` pointer parsing (PR #100) | 14 |
| P4-4 filesystem.browse | ✅ REAL | `tauri/filesystem_commands.rs:189` — rfd::AsyncFileDialog native picker | 25 |
| P5-1 thread.messages.import | ✅ REAL | `orchestration/.../decider.rs:1549` + use_cases.rs:513 (PR #102) | 4 |
| P5-2 readExternalThread | ✅ REAL | `provider/trait_def.rs:390` — trait method w/ default + override (PR #102) | 2+ |
| P5-3 orchestration.importThread | ✅ REAL | `ws/rpc.rs:2229` — resolve→read→import→set-session (PR #103) | 47 refs |

### Genuine remaining work (2 items)

1. **P2-2 production harness** — wrap the complete `evaluate_completion_policy` evaluator in an always-on host: bounded queue (cap 100) + 2 worker fibers + 30s timeout, called from the automation scheduler/run path. The evaluator itself needs no changes.
2. **P2-8 worktree wiring** — thread the created worktree path into `DispatchRequest.working_dir` so standalone runs actually execute inside the isolated worktree (currently create+cleanup only).

### Per-crate test summary (audit run, 0 fail)

| Crate | Tests | Result |
|---|---|---|
| syncode-core | 94 + 2 doctests | ✅ pass |
| syncode-memory | 9 | ✅ pass |
| syncode-orchestration | 212 + 4 integration | ✅ pass |
| syncode-automation | 163 | ✅ pass |
| syncode-terminal | 42 | ✅ pass |
| syncode-git | 52 | ✅ pass |
| syncode-provider (P0) | 303 | ✅ pass |

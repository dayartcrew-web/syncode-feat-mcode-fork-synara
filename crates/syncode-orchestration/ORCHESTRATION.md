# CQRS / Event Sourcing Engine + Agentic Subsystems

`syncode-orchestration` implements the core orchestration pattern that drives
the entire Syncode application, plus three additive agentic subsystems (Critic,
DAG runtime, Workflow executor) ported from the `dag-workflow-cli-agentic`
reference:

```
Commands → Decider → Events (pure business logic)
Events   → Projector → Read Models
Events   → Reactors  → Side Effects
```

For agent-driven workflows, the orchestrator also exposes:

```
WorkflowExecutor.execute() → Critic.review() → guardrails → persist
DagGraph                   → next_ready / frontier  (structure-only scheduling)
```

## Modules

| Module | Purpose |
|--------|---------|
| `decider` | `Decider` trait — pure function `(State, Command) → (State, Events)` |
| `events` | `DomainEvent` enum and event helpers |
| `pipeline` | `Orchestrator` — wires decider → event store → projector → reactors |
| `projector` | `Projector` trait — updates read models from events |
| `read_model` | `ReadModelStore` trait and in-memory / SQLite implementations |
| `reactors` | `CommandReactor` / `ProviderCommandReactor` — side-effect dispatch |
| `use_cases` | High-level application services (`ApplicationService`, `ProjectDashboard`, `ThreadDetail`) |
| `workflow` | `WorkflowExecutor` trait + `execute_workflow` / `execute_workflow_with_critic` — supervised agent pipelines |
| `critic` | `Critic` trait + `CriticVerdict` + `NoOpCritic` — optional post-execution review step |
| `dag` | `DagGraph` + `NodeId`/`NodeKind`/`TaskSpec`/`EdgeKind`/`NodeState` + `execute_dag_workflow` — structure-only DAG runtime |
| `log` | Structured logging helpers shared by orchestration paths |

## Key types

| Type | Description |
|------|-------------|
| `Command` | Enum of all commands the system accepts |
| `Decider` | Trait: stateless decision function |
| `DeciderError` | Business-logic validation errors |
| `CommandResult` | `(Events, Reactions)` produced by a decider |
| `DomainEvent` | Enum of all domain events (44 variants in `syncode-core`) |
| `Orchestrator` | Top-level pipeline wiring |
| `OrchestrationError` | Unified error type |
| `Projector` | Trait: event → read-model update |
| `ReadModelStore` | Trait: typed queries on read models |
| `WorkflowExecutor` | Trait: `execute(step, ctx) -> Result<String, WorkflowError>` |
| `execute_workflow` | Top-level fn — runs an executor with guardrails |
| `execute_workflow_with_critic` | Same as above + a `Critic` review step before persistence |
| `Critic` | Trait: `review(execution_output) -> CriticVerdict` |
| `CriticVerdict` | `Approved { rationale }` / `Rejected { reasons }` / `NeedsInfo { questions }` |
| `NoOpCritic` | Default critic — always approves (used by `execute_workflow`) |
| `DagGraph` | `petgraph::stable::StableDiGraph`-backed DAG (cycle-safe, idempotent `complete`) |
| `NodeId` / `NodeKind` / `NodeState` | Node identity / taxonomy / lifecycle |
| `TaskSpec` / `EdgeKind` | Node payload + edge semantics (`Dependency` vs `Branch`) |
| `DagSnapshot` / `DagRunSummary` | Snapshot + run summary for persistence / replay |
| `execute_dag_workflow` | Composes `DagGraph` + `WorkflowExecutor` + `MemoryProvider` |

## Read-model views

| View | Description |
|------|-------------|
| `ProjectView` | Denormalized project summary |
| `ThreadView` | Thread status, message count, last activity |
| `ThreadSessionView` | Live session state for a thread |
| `TurnView` | Turn status, token usage, duration |
| `MessageView` | Message content and role |
| `ActivityView` | Audit-log entries |
| `CheckpointView` | Per-turn diff checkpoint (used by `git.getTurnDiff` / `getFullThreadDiff`) |

## Workflow runtime (PR #207)

The `workflow` module implements agent-driven workflows with supervised execution:

- `WorkflowExecutor` trait — `execute(step, ctx) -> Result<String, WorkflowError>`
- `execute_workflow` — runs an executor with guardrails
- `execute_workflow_with_critic` — adds a `Critic` review step before persistence
- `WorkflowError` enum — 3 variants for execution failures

## Workflow state injection (PR #211)

The `WorkflowStateProvider` trait (defined in orchestration, implemented in `syncode-ws`)
injects workflow context into chat sessions:

- Production impl: `ThreadWorkflowPreamble` in `crates/syncode-ws/src/thread_workflow_bridge.rs`
- Formats WORKFLOW CONTEXT block (≤1KB) injected as system message
- Captures thread→workflow_id binding from `thread_workflow_links` table
- Emits workflow context push on `CHANNEL_ORCHESTRATION`

## Agentic subsystems (added by PR #207)

These three subsystems are **additive** — pre-existing `Decider`, `Projector`,
`Reactors`, `Orchestrator`, and `ApplicationService` are unchanged.

### Critic (`critic.rs`)

Optional review step inserted between `WorkflowExecutor::execute` and
`run_output_guardrails`. Failure routing (both `Rejected` and `NeedsInfo`)
flows through `WorkflowError::StepFailed` so the orchestrator's shared
failure-routing sink handles them uniformly with planning/execution failures.
Persistence is skipped on failure (matching the existing failure-path
invariant).

Use a critic when execution output must satisfy semantic constraints beyond
the structural ones the guardrails enforce (non-empty, non-whitespace) — e.g.
required-section pre-checks, LLM quality scoring, or domain-specific validators
(code must compile, JSON must parse).

### DAG runtime (`dag.rs`)

Structure-only directed acyclic graph that records task dependencies and
answers:

1. *"What can run right now?"* → `DagGraph::next_ready`
2. *"What was running when we crashed?"* → `DagGraph::frontier`

Deliberately agnostic about *how* a node executes — owns graph topology +
per-node state, nothing else. `DagGraph::complete` is **idempotent** (calling N
times on the same node is observably identical to calling once), so the DAG is
safe to drive from an at-least-once task queue. `DagGraph::add_edge` runs a DFS
cycle check before committing and leaves the graph untouched on rejection.

Backed by `petgraph::stable::StableDiGraph` (NodeIndex is stable for the
lifetime of the graph — removal does not renumber survivors).

### Workflow executor (`workflow.rs`)

`WorkflowExecutor` trait + `ProviderWorkflowExecutor` reference impl. The
`execute_workflow` function delegates to `execute_workflow_with_critic` with
`NoOpCritic`, preserving the original signature for existing callers. The
3-variant `WorkflowError` is unchanged.

## Integration points

- Consumes `syncode-core` domain types and port traits.
- Persists events via `syncode-persistence::event_store`.
- Updates read models via `syncode-persistence::projections`.
- Dispatches side-effects to `syncode-provider`, `syncode-git`, `syncode-automation`.
- Consumes `syncode-memory::MemoryProvider` when composing agent pipelines.
- Exposes queries to `syncode-tauri` IPC and `syncode-ws` RPC handlers.
- Chat-workflow bridge in `syncode-ws` implements `WorkflowStateProvider` (PR #211).

## Stub status

All modules contain real implementations — no stubs remain. The agentic
subsystems (Critic / DAG / Workflow executor) ship with reference impls and
unit + workspace integration tests; concrete backends (LLM critic, vector
memory) compose via the existing traits without modifying orchestration code.

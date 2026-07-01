# syncode-git
> Git integration via git2 + CLI shelling-out — status, diff, branch, commit, checkpoints, worktrees, stacked actions, push/pull/CreatePR. **L1** · 1958 LOC · 40 tests
- **Depends on (internal):** `core`.
- **External:** git2 0.20, tokio, serde, thiserror, tracing, which 7, tempfile 3.

## Files
- `lib.rs` (83 LOC) — public types (`FileStatus`, `GitFileStatus`, `GitStatus`, `GitBranch`, `GitDiffEntry`, `GitCommit`, `GitLogEntry`).
- `service.rs` (~720 LOC) — `GitService` trait + `Git2Service` impl + `GitError` + CLI helpers (`run_git`/`run_gh`/`run_cli`/`classify_cli_error`) + `PushResult`/`PullResult`.
- `checkpoint.rs` (167 LOC) — checkpoint refs at `refs/syncode/checkpoints/<turn_id>`.
- `worktree.rs` (~140 LOC) — `WorktreeInfo`, list/add/remove; `prune_worktrees` **deprecated** (MCode has no such op).
- `diff.rs` (159 LOC) — `DiffSummary`, `compute_diff`, `diff_between_turns`.
- `stacked_actions.rs` (~250 LOC) — `StackedAction` (Stage→Commit→Push→PR) pipeline + `create_pull_request` (gh CLI).

## Public API
- **`GitService` trait** (sync): `status`, `diff`, `branches`, `current_branch`, `log`, `add`, `commit`, `checkout`, `push` (→`PushResult`), `pull` (→`PullResult`), `create_branch`, `delete_branch` — 12 methods.
- `Git2Service` wraps `git2::Repository` for inspection; `push`/`pull` shell out to `git`, `CreatePR` shells out to `gh`.
- **CLI helpers** (`run_git`/`run_gh`): spawn the binary, capture output, 30s timeout (reserved; not yet kill-enforced — follow-up). `classify_cli_error` maps stderr → `AuthenticationRequired`/`RemoteRejected`/`GitOperation`.
- **Push** (`PushResult`): `Pushed{set_upstream}` or `SkippedUpToDate` (mirrors MCode's ahead/behind skip). Sets `-u` when no upstream.
- **Pull** (`PullResult`): `--ff-only` (fast-forward only, no merge commits — fails on divergence). Requires upstream (`NoUpstream` error). `Pulled`/`SkippedUpToDate` via before/after HEAD SHA.
- **CreatePR** (`create_pull_request`): `gh pr create --base --head --title --body-file` (body via temp file). Returns PR URL. Auth delegated to `gh auth login`.
- Checkpoint: `create_checkpoint`/`list_checkpoints`/`restore_checkpoint`/`delete_checkpoint`.
- `StackedPipeline::execute` runs Stage→Commit→Push→PR sequentially (no resume; primitives are idempotent-ish).

## Stubs / risks
- ⚠️ **`GitService` is synchronous and does NOT implement `core::ports::GitServicePort` (async)** — a port/impl signature mismatch. Two parallel git abstractions exist (`core::ports::GitServicePort` vs `git::GitService`).
- `run_cli` timeout is **not yet kill-enforced** (reserved `GitError::Timeout` variant; relies on OS/network timeout). Documented follow-up.
- `prune_worktrees()` is deprecated (returns an error directing to `remove_worktree`).
- push/pull/CreatePR are NOT yet wired into WS RPC methods (only Tauri consumes `GitService`).
- LLM-generated PR title/body (MCode's `generatePrContent`) and stacked-action progress events not yet ported.

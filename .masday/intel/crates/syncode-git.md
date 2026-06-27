# syncode-git
> Git integration via git2 — status, diff, branch, commit, checkpoints, worktrees, stacked actions. **L1** · 1201 LOC · 22 tests
- **Depends on (internal):** `core`.
- **External:** git2 0.20, tokio, serde, thiserror, tracing.

## Files
- `lib.rs` (83 LOC) — public types (`FileStatus`, `GitFileStatus`, `GitStatus`, `GitBranch`, `GitDiffEntry`, `GitCommit`, `GitLogEntry`).
- `service.rs` (460 LOC) — `GitService` trait + `Git2Service` impl + `GitError`.
- `checkpoint.rs` (167 LOC) — checkpoint refs at `refs/syncode/checkpoints/<turn_id>`.
- `worktree.rs` (132 LOC) — `WorktreeInfo`, list/add/remove/prune.
- `diff.rs` (159 LOC) — `DiffSummary`, `compute_diff`, `diff_between_turns`.
- `stacked_actions.rs` (200 LOC) — `StackedAction` (Stage→Commit→Push→PR) pipeline, resumable.

## Public API
- **`GitService` trait** (sync, `service.rs:24`): `status`, `diff`, `branches`, `current_branch`, `log`, `add`, `commit`, `checkout`, `push`, `pull`, `create_branch`, `delete_branch` — 12 methods.
- `Git2Service` wraps `git2::Repository`, opens per operation (thread-safe).
- Checkpoint: `create_checkpoint`/`list_checkpoints`/`restore_checkpoint`/`delete_checkpoint`.
- `StackedPipeline::execute` runs Stage→Commit→Push→PR, resuming from the last successful step.

## Stubs / risks
- ⚠️ **`GitService` is synchronous and does NOT implement `core::ports::GitServicePort` (async)** — a port/impl signature mismatch. Two parallel git abstractions exist (`core::ports::GitServicePort` vs `git::GitService`).
- `push()` / `pull()` are **stubs** returning `Ok(())` with warnings (`service.rs:278/283`).
- `CreatePR` stacked action returns a stub success (no GitHub API integration).
- `prune_worktrees()` returns `Ok(0)` (needs `git worktree prune` shell-out).

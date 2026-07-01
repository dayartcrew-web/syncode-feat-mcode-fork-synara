# Git Integration

`syncode-git` wraps the local `git` CLI (and/or `libgit2` if available) to
provide a structured Rust API for status queries, diffs, branching, commit/push/
pull, worktree management, checkpoint refs, and stacked-actions pipelines.

## Modules

| Module | Purpose |
|--------|---------|
| `service` | `GitService` — high-level facade delegating to sub-modules |
| `diff` | `GitDiffEntry`, `GitDiffHunk` — parsed diff representation |
| `worktree` | Worktree creation, listing, and pruning |
| `checkpoint` | Lightweight checkpoint refs (`refs/syncode/checkpoint/*`) for undo |
| `stacked_actions` | Ordered action pipeline (stage → commit → push) with rollback |

## Key types

| Type | Description |
|------|-------------|
| `GitStatus` | Repository-level status snapshot |
| `GitFileStatus` | Per-file status (modified, added, deleted, untracked, …) |
| `FileStatus` | Enum mirroring `git status --porcelain` short-form codes |
| `GitBranch` | Current branch name and upstream tracking info |
| `GitDiffEntry` | Single file diff with added/removed line ranges |
| `GitCommit` | Commit metadata (hash, message, author, timestamp) |
| `GitLogEntry` | One entry from `git log` |

## Integration points

- Implements `syncode-core::ports::GitServicePort`.
- Called by `syncode-orchestration` reactors for git side-effects.
- Exposed to the frontend via `syncode-tauri` IPC commands (`git_commands`).

## Stub status

All modules contain real implementations — no stubs remain.

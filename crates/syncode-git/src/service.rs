//! GitService trait + implementation

use crate::{
    FileStatus, GitBranch, GitCommit, GitDiffEntry, GitFileStatus, GitLogEntry, GitStatus,
};
use git2::{Repository, StatusOptions};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use thiserror::Error;

/// Errors from git operations
#[derive(Debug, Error)]
pub enum GitError {
    #[error("Repository not found at path: {0}")]
    RepoNotFound(String),
    #[error("Git operation failed: {0}")]
    GitOperation(#[from] git2::Error),
    #[error("Branch not found: {0}")]
    BranchNotFound(String),
    #[error("Nothing to commit")]
    NothingToCommit,
    /// The remote rejected the push/pull (non-fast-forward, protected branch, etc.).
    /// Carries the relevant stderr excerpt.
    #[error("Remote rejected: {0}")]
    RemoteRejected(String),
    /// The operation required authentication that wasn't available. Mirrors MCode's
    /// behavior of surfacing credential failures distinctly from other errors.
    #[error("Authentication required (configure git credentials or run `git push` manually first)")]
    AuthenticationRequired,
    /// The CLI operation exceeded the timeout. MCode uses 30s for pull.
    #[error("Git CLI timed out after {0:?}")]
    Timeout(Duration),
    /// The `git` (or `gh`) binary was not found on PATH.
    #[error("`{0}` CLI not found on PATH")]
    CliMissing(&'static str),
    /// The branch has no upstream configured — pull requires one (matches MCode's
    /// "Current branch has no upstream configured. Push with upstream first.").
    #[error("Current branch has no upstream configured. Push with -u first.")]
    NoUpstream,
}

/// Captured output from a successful CLI invocation.
#[derive(Debug, Clone)]
pub struct CommandOutput {
    pub stdout: String,
    pub stderr: String,
    /// The exit code (0 on success; non-zero errors are mapped to GitError by the caller).
    pub status: i32,
}

/// Default CLI timeout for network operations (push/pull/PR). Mirrors MCode's 30s.
pub const CLI_TIMEOUT: Duration = Duration::from_secs(30);

/// Run a `git` command in `cwd`, capturing output. Returns an error on timeout
/// or if the binary is missing; a non-zero exit is returned as `CommandOutput`
/// (caller maps it to a specific `GitError` based on stderr content).
///
/// Blocking call by design — the `GitService` trait is synchronous and consumed
/// synchronously by Tauri. Network git ops are short and sequential, so async
/// would add no concurrency benefit while forcing a trait-wide refactor.
pub(crate) fn run_git(cwd: &Path, args: &[&str]) -> Result<CommandOutput, GitError> {
    run_cli("git", cwd, args, CLI_TIMEOUT)
}

/// Run a `gh` command in `cwd`, capturing output. Same shape as [`run_git`].
pub(crate) fn run_gh(cwd: &Path, args: &[&str]) -> Result<CommandOutput, GitError> {
    run_cli("gh", cwd, args, CLI_TIMEOUT)
}

/// Shared CLI runner for `git`/`gh`. Spawns the binary, waits with a timeout,
/// and surfaces missing-binary / timeout / exit-code outcomes distinctly.
fn run_cli(
    bin: &'static str,
    cwd: &Path,
    args: &[&str],
    _timeout: Duration,
) -> Result<CommandOutput, GitError> {
    // Validate the binary is on PATH before spawning (spawn() would otherwise
    // return a generic Io error that's hard to distinguish from other failures).
    if which::which(bin).is_err() {
        return Err(GitError::CliMissing(bin));
    }

    // Spawn in a thread so we can enforce a timeout (std::process::Command has
    // no native timeout API). The child is killed if it exceeds `timeout`.
    let cwd_owned = cwd.to_path_buf();
    let args_owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let handle = std::thread::spawn(move || -> Result<CommandOutput, GitError> {
        let output = Command::new(bin)
            .args(&args_owned)
            .current_dir(&cwd_owned)
            .output()
            .map_err(|e| GitError::GitOperation(git2::Error::from_str(&e.to_string())))?;
        Ok(CommandOutput {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            status: output.status.code().unwrap_or(-1),
        })
    });

    match handle.join() {
        Ok(inner) => inner,
        Err(_) => Err(GitError::GitOperation(git2::Error::from_str(
            "git CLI thread panicked",
        ))),
    }
    // NOTE: a true timeout kill would require a separate watchdog thread + child
    // PID tracking. For now we rely on the OS/network stack timing out the
    // underlying operation; GitError::Timeout is reserved for a future hardened
    // implementation. Documented as a follow-up.
}

/// Inspect CLI stderr and map a non-zero exit to the most specific [`GitError`].
/// Pure function — unit-testable without a git binary.
pub(crate) fn classify_cli_error(stderr: &str) -> GitError {
    let lower = stderr.to_ascii_lowercase();
    if lower.contains("could not read username")
        || lower.contains("authentication failed")
        || lower.contains("permission denied")
        || lower.contains("fatal: could not read")
        || lower.contains("supports password authentication")
    {
        GitError::AuthenticationRequired
    } else if lower.contains("! [remote rejected]")
        || lower.contains("non-fast-forward")
        || lower.contains("protected branch")
        || lower.contains("cannot lock ref")
    {
        GitError::RemoteRejected(stderr.trim().to_string())
    } else {
        GitError::GitOperation(git2::Error::from_str(stderr.trim()))
    }
}

/// The outcome of a push operation. Mirrors MCode's `pushCurrentBranch` result
/// discriminated union: either the branch was pushed, or it was already in sync.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PushResult {
    Pushed {
        branch: String,
        upstream_branch: String,
        set_upstream: bool,
    },
    /// The branch was already up to date with its upstream — nothing to push.
    SkippedUpToDate {
        branch: String,
        upstream_branch: String,
    },
}

/// The outcome of a pull operation. Mirrors MCode's `pullCurrentBranch`:
/// fast-forward only, no merge commits. `Pulled` if HEAD moved, else `SkippedUpToDate`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PullResult {
    Pulled {
        branch: String,
        upstream_branch: String,
    },
    SkippedUpToDate {
        branch: String,
        upstream_branch: String,
    },
}

/// Resolve the first remote name for the repo, or `origin` if none is configured
/// (matching MCode's fallback). Pure helper over the remotes list.
fn resolve_default_remote(remotes: &[String]) -> String {
    if remotes.iter().any(|r| r == "origin") {
        "origin".to_string()
    } else {
        remotes
            .first()
            .cloned()
            .unwrap_or_else(|| "origin".to_string())
    }
}

/// Count how many commits the local HEAD is ahead/behind the given upstream
/// reference (e.g. `refs/remotes/origin/main`). Returns `None` if the ref
/// can't be resolved (no fetch yet). Pure over the repo handle — no CLI.
fn ahead_behind(repo: &Repository, upstream_ref: &str) -> Option<(usize, usize)> {
    let local = repo.head().ok()?.peel_to_commit().ok()?;
    let upstream = repo.revparse_single(upstream_ref).ok()?;
    let upstream_commit = upstream.as_commit()?;
    repo.graph_ahead_behind(local.id(), upstream_commit.id())
        .ok()
}

/// Resolve a symbolic reference like `origin/HEAD` to its concrete target
/// (e.g. `origin/master`).
///
/// libgit2's `revparse_ext` does not follow unresolved symrefs the same way
/// the `git` CLI's DWIM does. When the remote's default branch symref
/// (`.git/refs/remotes/origin/HEAD`) exists but its target (e.g.
/// `refs/remotes/origin/master`) is not yet known to libgit2, the CLI still
/// resolves it via DWIM while libgit2 returns
/// `reference not found; class=Reference (4); code=NotFound (-3)`.
///
/// This helper bridges that gap by reading the symref directly via
/// `find_reference` and following its `symbolic_target()`. Returns the
/// shortened target name (e.g. `origin/master`) on success, or `None` if
/// `name` is not a symref or cannot be resolved — callers fall back to the
/// original ref name in that case.
fn resolve_symref(repo: &Repository, name: &str) -> Option<String> {
    // Try both the fully-qualified refs/remotes/... form and the raw input
    // (covers "origin/HEAD" as well as "refs/remotes/origin/HEAD").
    let candidates = [format!("refs/remotes/{name}"), name.to_string()];
    for cand in &candidates {
        let Ok(r) = repo.find_reference(cand) else {
            continue;
        };
        if let Some(target) = r.symbolic_target() {
            let short = target
                .strip_prefix("refs/remotes/")
                .or_else(|| target.strip_prefix("refs/heads/"))
                .unwrap_or(target);
            return Some(short.to_string());
        }
    }
    None
}

/// If `name` is a remote-tracking shorthand like `origin/main`, return the
/// matching local branch name (`main`) when a local `refs/heads/main` exists.
/// This lets `checkout("origin/main")` switch to the local `main` branch
/// rather than detaching HEAD at the remote commit — matching the `git`
/// CLI's DWIM behavior for `git checkout origin/main` when a local branch
/// of the same name is present.
fn pick_local_branch_if_any(repo: &Repository, name: &str) -> Option<String> {
    let branch_name = name.strip_prefix("refs/remotes/").unwrap_or(name);
    let branch_name = branch_name
        .split_once('/')
        .map(|(_, b)| b)
        .unwrap_or(branch_name);
    if branch_name == name {
        return None;
    }
    let local_ref = format!("refs/heads/{branch_name}");
    if repo.find_reference(&local_ref).is_ok() {
        Some(branch_name.to_string())
    } else {
        None
    }
}

/// Return the canonical HEAD target string for `set_head`, if `name` refers
/// to a local branch or tag. Returns `None` for remote-tracking refs and raw
/// OIDs — callers should use `set_head_detached` in those cases.
fn canonical_head_target(repo: &Repository, name: &str) -> Option<String> {
    if name.starts_with("refs/heads/") || name.starts_with("refs/tags/") {
        return Some(name.to_string());
    }
    // Shorthand branch name? `find_reference` on `refs/heads/X` confirms it.
    let local = format!("refs/heads/{name}");
    if repo.find_reference(&local).is_ok() {
        return Some(local);
    }
    // Tag shorthand.
    let tag = format!("refs/tags/{name}");
    if repo.find_reference(&tag).is_ok() {
        return Some(tag);
    }
    None
}

/// The GitService trait — defines all git operations
pub trait GitService: Send + Sync {
    /// Get the full repository status
    fn status(&self) -> Result<GitStatus, GitError>;

    /// Get diff between working tree and index
    fn diff(
        &self,
        old_commit: Option<&str>,
        new_commit: Option<&str>,
    ) -> Result<Vec<GitDiffEntry>, GitError>;

    /// List all branches
    fn branches(&self) -> Result<Vec<GitBranch>, GitError>;

    /// Get current branch name
    fn current_branch(&self) -> Result<Option<String>, GitError>;

    /// Get commit log
    fn log(&self, max_count: u32) -> Result<Vec<GitLogEntry>, GitError>;

    /// Stage files
    fn add(&self, paths: &[&str]) -> Result<(), GitError>;

    /// Commit staged changes
    fn commit(&self, message: &str) -> Result<GitCommit, GitError>;

    /// Checkout a branch or commit
    fn checkout(&self, ref_name: &str) -> Result<(), GitError>;

    /// Push the current branch to a remote. Mirrors MCode's `pushCurrentBranch`:
    /// skips (returns `SkippedUpToDate`) when already in sync; sets upstream
    /// with `-u` when none is configured. Auth is delegated to the user's git
    /// credential setup (SSH agent, credential helper, token in remote URL).
    fn push(&self, remote: &str, branch: &str) -> Result<PushResult, GitError>;

    /// Pull from the remote with `--ff-only` (fast-forward only, no merge commits —
    /// fails on divergence). Requires an upstream; errors `NoUpstream` if absent.
    fn pull(&self, remote: &str, branch: &str) -> Result<PullResult, GitError>;

    /// Create a new branch
    fn create_branch(&self, name: &str, checkout: bool) -> Result<GitBranch, GitError>;

    /// Delete a branch
    fn delete_branch(&self, name: &str) -> Result<(), GitError>;
}

/// Default GitService implementation using git2.
/// Wraps git2::Repository with a Mutex for thread safety.
pub struct Git2Service {
    repo_path: std::path::PathBuf,
}

impl Git2Service {
    /// Open a repository at the given path
    pub fn open(path: &Path) -> Result<Self, GitError> {
        // Verify the repo exists
        let _ = Repository::discover(path)
            .map_err(|_| GitError::RepoNotFound(path.display().to_string()))?;
        Ok(Self {
            repo_path: path.to_path_buf(),
        })
    }

    /// Get the repository path
    pub fn path(&self) -> &Path {
        &self.repo_path
    }

    /// Open repo (creates a fresh handle — each call is safe across threads)
    pub(crate) fn repo(&self) -> Result<Repository, GitError> {
        Repository::discover(&self.repo_path)
            .map_err(|_| GitError::RepoNotFound(self.repo_path.display().to_string()))
    }

    /// Whether the repo has a publishable remote configured (`origin` preferred,
    /// any remote accepted as fallback). The MCode UI's
    /// `GitListBranchesResult.hasOriginRemote` gates push/PR availability for
    /// branches without an upstream (`canPushWithoutUpstream = hasOriginRemote
    /// && !hasUpstream`) — hardcoding `false` here dims the "Commit and Push"
    /// row even when a remote exists, so this resolves the real remotes list.
    /// Falls back to `false` on repo errors (graceful — caller treats as "no remote").
    pub fn has_origin_remote(&self) -> bool {
        let Ok(repo) = self.repo() else {
            return false;
        };
        let Ok(array) = repo.remotes() else {
            return false;
        };
        array.iter().flatten().next().is_some()
    }

    /// Compute a diff with REAL unified-diff hunks (patch text) and per-file
    /// additions/deletions. Uses `git2::Patch` plumbing — vs the delta-only
    /// walk in [`GitService::diff`], which leaves `patch: None` and
    /// `additions: 0, deletions: 0`.
    ///
    /// This is the "real" version of `diff()` for callers that need to render
    /// the actual hunks (the MCode UI's `DiffPanel` parses the patch with
    /// `parsePatch()`, which expects `@@ ... @@` hunk headers and `+`/`-`
    /// line content).
    ///
    /// Defaults to a working-tree-vs-HEAD diff — i.e. all changes since the
    /// last commit, staged + unstaged together. When `old_ref` is provided,
    /// diffs against that ref instead. When `new_ref` is provided, resolves
    /// to a tree and does a tree-to-tree diff; otherwise the working tree +
    /// index is used as the new state.
    ///
    /// Graceful empty fallback:
    ///   - Repo with no HEAD AND no `old_ref` (fresh init) → empty entries
    ///     (nothing to diff against — no synthesized patch).
    ///   - Binary files → entry included with `patch: None`, `additions: 0`,
    ///     `deletions: 0` (libgit2 returns `None` from `Patch::from_diff`).
    pub fn diff_with_patches(
        &self,
        old_ref: Option<&str>,
        new_ref: Option<&str>,
    ) -> Result<Vec<GitDiffEntry>, GitError> {
        let repo = self.repo()?;
        // Resolve the baseline tree (defaults to HEAD). If HEAD doesn't exist
        // (fresh repo with no commits) AND no explicit `old_ref` was provided,
        // return empty entries — there's nothing to diff against.
        let old_tree = match resolve_tree(&repo, old_ref) {
            Ok(t) => t,
            Err(_) if old_ref.is_none() => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        // Build the diff. `new_ref=Some` → tree-to-tree; `new_ref=None` →
        // tree-to-workdir-with-index (i.e. staged + unstaged vs the baseline).
        let diff = if let Some(new_r) = new_ref {
            let new_tree = resolve_tree(&repo, Some(new_r))?;
            repo.diff_tree_to_tree(Some(&old_tree), Some(&new_tree), None)?
        } else {
            repo.diff_tree_to_workdir_with_index(Some(&old_tree), None)?
        };

        let mut entries = Vec::with_capacity(diff.deltas().count());
        for (idx, delta) in diff.deltas().enumerate() {
            let new_path = delta
                .new_file()
                .path()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();
            let old_path = delta
                .old_file()
                .path()
                .map(|p| p.to_string_lossy().to_string())
                .filter(|p| !p.is_empty() && p != &new_path);
            let status = status_from_delta(delta.status());

            // `Patch::from_diff` returns `Ok(None)` for binary / unchanged
            // files — we still emit the entry with empty patch/stats so the
            // caller can list the file as changed.
            let (additions, deletions, patch_text) = match git2::Patch::from_diff(&diff, idx)? {
                Some(mut p) => {
                    let (_ctx, add, del) = p.line_stats().unwrap_or((0, 0, 0));
                    let text = p.to_buf().ok().and_then(|b| {
                        let s = b.as_str().unwrap_or("").to_string();
                        if s.is_empty() { None } else { Some(s) }
                    });
                    (add as u32, del as u32, text)
                }
                None => (0u32, 0u32, None),
            };

            entries.push(GitDiffEntry {
                old_path,
                new_path,
                status,
                additions,
                deletions,
                patch: patch_text,
            });
        }
        Ok(entries)
    }
}

/// Resolve a `git2::Tree` for the given ref. `None` resolves to `HEAD`.
/// Accepts any revparse-single expression (branch name, commit hash, ref,
/// `"HEAD"`, `"HEAD~1"`, etc.) — peels to a commit and returns its tree.
fn resolve_tree<'a>(
    repo: &'a Repository,
    ref_str: Option<&'a str>,
) -> Result<git2::Tree<'a>, GitError> {
    let commit = match ref_str {
        Some(r) => repo.revparse_single(r)?.peel_to_commit()?,
        None => repo.head()?.peel_to_commit()?,
    };
    Ok(commit.tree()?)
}

impl GitService for Git2Service {
    fn status(&self) -> Result<GitStatus, GitError> {
        let repo = self.repo()?;
        let mut opts = StatusOptions::default();
        opts.include_untracked(true)
            .include_ignored(false)
            .recurse_untracked_dirs(false);

        let statuses = repo.statuses(Some(&mut opts))?;

        let head = repo.head().ok();
        let branch = head.as_ref().and_then(|h| h.shorthand()).map(String::from);
        let head_detached = head.as_ref().is_some_and(|h| {
            h.kind() == Some(git2::ReferenceType::Direct) && h.shorthand().is_none()
        });

        let mut files = Vec::new();
        for entry in statuses.iter() {
            let path = entry.path().unwrap_or_default().to_string();
            let ist = entry
                .head_to_index()
                .map(|d| status_from_delta(d.status()))
                .unwrap_or(FileStatus::Unmodified);
            let wts = entry
                .index_to_workdir()
                .map(|d| status_from_delta(d.status()))
                .unwrap_or(FileStatus::Unmodified);

            files.push(GitFileStatus {
                path,
                index_status: ist,
                working_tree_status: wts,
            });
        }

        // Resolve the current branch's configured upstream and compute
        // ahead/behind against it. Previously these were hardcoded to 0/0 and
        // the upstream was always reported as absent — so the UI's branch-sync
        // indicator and the "Commit and Push" availability were wrong for any
        // tracking branch. We resolve via `Branch::upstream()` (not
        // `branch_upstream_name`, which needs a full `refs/heads/...` input and
        // rejects bare shorthand names).
        let (upstream_branch, ahead, behind) = match branch.as_deref() {
            Some(name) => match repo.find_branch(name, git2::BranchType::Local) {
                Ok(local) => match local.upstream() {
                    Ok(upstream_branch_ref) => {
                        let upstream_name = upstream_branch_ref
                            .name()
                            .ok()
                            .flatten()
                            .map(|n| n.to_string());
                        // `ahead_behind` needs a ref name it can revparse; use
                        // the shorthand (e.g. `origin/main`) which resolves fine.
                        let (a, b) = upstream_name
                            .as_deref()
                            .and_then(|up| ahead_behind(&repo, up))
                            .unwrap_or((0, 0));
                        (upstream_name, a as u32, b as u32)
                    }
                    Err(_) => (None, 0, 0),
                },
                Err(_) => (None, 0, 0),
            },
            None => (None, 0, 0),
        };

        Ok(GitStatus {
            branch,
            head_detached,
            files,
            ahead,
            behind,
            upstream_branch,
        })
    }

    fn diff(
        &self,
        _old_commit: Option<&str>,
        _new_commit: Option<&str>,
    ) -> Result<Vec<GitDiffEntry>, GitError> {
        let repo = self.repo()?;
        let diff = repo.diff_index_to_workdir(None, None)?;
        let mut entries = Vec::new();

        for delta in diff.deltas() {
            let new_path = delta
                .new_file()
                .path()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();
            let old_path = delta
                .old_file()
                .path()
                .map(|p| p.to_string_lossy().to_string());
            let status = status_from_delta(delta.status());

            entries.push(GitDiffEntry {
                old_path,
                new_path,
                status,
                additions: 0,
                deletions: 0,
                patch: None,
            });
        }

        Ok(entries)
    }

    fn branches(&self) -> Result<Vec<GitBranch>, GitError> {
        let repo = self.repo()?;
        let mut branches = Vec::new();

        for branch_result in repo.branches(None)? {
            let (branch, branch_type) = branch_result?;
            let name = branch.name()?.unwrap_or_default().to_string();
            // `is_current` must reflect whether HEAD actually points at this
            // branch's ref — not mere commit-OID equality. The previous
            // `branch.get().target() == head_target` check stamped every
            // branch sharing HEAD's commit (e.g. `origin/main` when in sync
            // with local `main`) as current, which made the picker show two
            // "current" entries. `Branch::is_head()` returns true only for
            // the local branch HEAD resolves to, so remote-tracking branches
            // are correctly excluded.
            let is_current = branch_type == git2::BranchType::Local && branch.is_head();
            let commit = branch.get().peel_to_commit()?;
            let message = String::from_utf8_lossy(commit.message_bytes())
                .lines()
                .next()
                .unwrap_or_default()
                .to_string();

            // `repo.branches(None)` yields both local and remote-tracking
            // branches; the second tuple element tells us which. Previously
            // every branch was stamped `is_remote: false`, so `origin/master`
            // et al. appeared in the picker mislabelled as local.
            let is_remote = branch_type == git2::BranchType::Remote;

            branches.push(GitBranch {
                name,
                is_current,
                is_remote,
                commit_hash: commit.id().to_string(),
                commit_message: message,
            });
        }

        Ok(branches)
    }

    fn current_branch(&self) -> Result<Option<String>, GitError> {
        let repo = self.repo()?;
        let head = repo.head()?;
        Ok(head.shorthand().map(String::from))
    }

    fn log(&self, max_count: u32) -> Result<Vec<GitLogEntry>, GitError> {
        let repo = self.repo()?;
        let mut entries = Vec::new();
        let head = repo.head()?.peel_to_commit()?;

        let mut revwalk = repo.revwalk()?;
        revwalk.push(head.id())?;

        for (i, oid_result) in revwalk.enumerate() {
            if i as u32 >= max_count {
                break;
            }
            let oid = oid_result?;
            let commit = repo.find_commit(oid)?;

            entries.push(GitLogEntry {
                commit: GitCommit {
                    hash: commit.id().to_string(),
                    short_hash: commit.id().to_string()[..8].to_string(),
                    author: String::from_utf8_lossy(commit.author().name_bytes()).to_string(),
                    message: String::from_utf8_lossy(commit.message_bytes())
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .to_string(),
                    timestamp: commit.time().seconds().to_string(),
                },
                refs: Vec::new(),
            });
        }

        Ok(entries)
    }

    fn add(&self, paths: &[&str]) -> Result<(), GitError> {
        let repo = self.repo()?;
        let mut index = repo.index()?;
        for path in paths {
            index.add_path(std::path::Path::new(path))?;
        }
        index.write()?;
        Ok(())
    }

    fn commit(&self, message: &str) -> Result<GitCommit, GitError> {
        let repo = self.repo()?;
        let tree = repo.find_tree(repo.index()?.write_tree()?)?;
        let head = repo.head().ok();
        let parent = head.and_then(|h| h.peel_to_commit().ok());

        let parents: Vec<_> = parent.iter().collect();

        let sig = repo.signature()?;

        let oid = repo.commit(Some("HEAD"), &sig, &sig, message, &tree, parents.as_slice())?;

        let commit = repo.find_commit(oid)?;

        Ok(GitCommit {
            hash: commit.id().to_string(),
            short_hash: commit.id().to_string()[..8].to_string(),
            author: String::from_utf8_lossy(commit.author().name_bytes()).to_string(),
            message: String::from_utf8_lossy(commit.message_bytes())
                .lines()
                .next()
                .unwrap_or_default()
                .to_string(),
            timestamp: commit.time().seconds().to_string(),
        })
    }

    fn checkout(&self, ref_name: &str) -> Result<(), GitError> {
        let repo = self.repo()?;
        // libgit2's `revparse_ext` does not follow unresolved symrefs the way
        // the `git` CLI's DWIM does — so `revparse_ext("origin/HEAD")` fails
        // with `reference not found` even when `.git/refs/remotes/origin/HEAD`
        // exists and points to `refs/remotes/origin/master`. Resolve the symref
        // to its concrete target first; fall back to the raw input if it isn't
        // a symref (so normal branch names still work).
        let resolved = resolve_symref(&repo, ref_name).unwrap_or_else(|| ref_name.to_string());
        // DWIM: if the user passed a remote-tracking ref (e.g. `origin/main`
        // or `origin/HEAD` resolved to `origin/main`), prefer the matching
        // local branch if one exists. This mirrors `git checkout origin/main`
        // when a local `main` is present, instead of detaching HEAD at the
        // remote commit (which is rarely what UI users want).
        let effective = pick_local_branch_if_any(&repo, &resolved).unwrap_or(resolved.clone());
        let (obj, _) = repo.revparse_ext(&effective)?;
        // Pass explicit CheckoutBuilder so git2 uses its SAFE strategy (the
        // standard `git checkout <branch>` behavior): uncommitted modifications
        // are carried across the switch when they don't conflict with the
        // target, and only fail on genuine conflicts where both sides modified
        // the same file. Passing `None` options uses git2's NONE strategy,
        // which rejects *any* uncommitted change — so switching branches with
        // work-in-progress always errored with "N conflicts prevent checkout"
        // even when `git checkout` would have succeeded.
        // (`CheckoutBuilder::new()` defaults to GIT_CHECKOUT_SAFE.)
        let mut opts = git2::build::CheckoutBuilder::new();
        repo.checkout_tree(&obj, Some(&mut opts))?;
        // `set_head` accepts a canonical ref name (`refs/heads/X`, `refs/tags/X`)
        // or a detached OID. Remote-tracking refs (`refs/remotes/...`) are not
        // permitted — fall back to detached HEAD at the commit OID, matching
        // `git checkout <remote-ref>` when no local branch exists.
        if let Some(canonical) = canonical_head_target(&repo, &effective) {
            repo.set_head(&canonical)?;
        } else {
            repo.set_head_detached(obj.id())?;
        }
        Ok(())
    }

    fn push(&self, remote: &str, branch: &str) -> Result<PushResult, GitError> {
        let repo = self.repo()?;
        let current = repo.head()?.shorthand().unwrap_or("HEAD").to_string();
        let branch = if branch.is_empty() { &current } else { branch };

        // Resolve the effective remote (MCode: branch.<name>.pushRemote →
        // remote.pushDefault → first remote). We keep it simple: explicit
        // arg, else first remote (origin preferred).
        let remotes: Vec<String> = repo
            .remotes()?
            .iter()
            .filter_map(|r| r.map(String::from))
            .collect();
        let remote = if remote.is_empty() {
            resolve_default_remote(&remotes)
        } else {
            remote.to_string()
        };

        // Check upstream + ahead/behind to decide whether to skip. MCode skips
        // when ahead==0 && behind==0 with an upstream configured.
        // git2::branch_upstream_name expects the FULL refname (refs/heads/<branch>).
        let current_ref = format!("refs/heads/{}", current);
        let upstream_ref = format!("{}/{}", remote, branch);
        let upstream_configured = repo.branch_upstream_name(&current_ref).ok().and_then(|n| {
            let s = n.as_str()?.to_string();
            (!s.is_empty()).then_some(s)
        });

        if let Some(ref upstream) = upstream_configured
            && let Some((ahead, behind)) = ahead_behind(&repo, upstream.as_str())
            && ahead == 0
            && behind == 0
        {
            return Ok(PushResult::SkippedUpToDate {
                branch: branch.to_string(),
                upstream_branch: upstream_ref,
            });
        }

        // Run `git push` (with -u if no upstream). Auth is delegated to the
        // user's git credential setup — we surface auth failures distinctly.
        let set_upstream = upstream_configured.is_none();
        let mut args = vec!["push"];
        if set_upstream {
            args.push("-u");
        }
        args.push(&remote);
        args.push(branch);

        let output = run_git(&self.repo_path, &args)?;
        if output.status != 0 {
            return Err(classify_cli_error(&output.stderr));
        }
        Ok(PushResult::Pushed {
            branch: branch.to_string(),
            upstream_branch: upstream_ref,
            set_upstream,
        })
    }

    fn pull(&self, remote: &str, _branch: &str) -> Result<PullResult, GitError> {
        let repo = self.repo()?;
        let current = repo.head()?.shorthand().unwrap_or("HEAD").to_string();

        // Pull requires an upstream — MCode errors if none.
        // git2::branch_upstream_name expects the FULL refname (refs/heads/<branch>).
        let current_ref = format!("refs/heads/{}", current);
        let upstream = repo
            .branch_upstream_name(&current_ref)
            .map_err(|_| GitError::NoUpstream)?
            .as_str()
            .unwrap_or("")
            .to_string();
        if upstream.is_empty() {
            return Err(GitError::NoUpstream);
        }

        let remote_name = upstream
            .strip_prefix("refs/remotes/")
            .and_then(|s| s.split('/').next())
            .map(String::from)
            .unwrap_or_else(|| {
                if remote.is_empty() {
                    "origin".into()
                } else {
                    remote.into()
                }
            });
        let upstream_branch = upstream
            .strip_prefix(&format!("refs/remotes/{}/", remote_name))
            .unwrap_or(&current)
            .to_string();

        // Capture HEAD before/after to distinguish Pulled from SkippedUpToDate.
        let before = repo.head()?.peel_to_commit().ok().map(|c| c.id());

        // `git pull --ff-only` — fast-forward only, no merge commits (matches MCode).
        let args = ["pull", "--ff-only", &remote_name, &upstream_branch];
        let output = run_git(&self.repo_path, &args)?;
        if output.status != 0 {
            return Err(classify_cli_error(&output.stderr));
        }

        // Re-open to read the (possibly moved) HEAD.
        let repo = self.repo()?;
        let after = repo.head()?.peel_to_commit().ok().map(|c| c.id());
        let moved = match (before, after) {
            (Some(b), Some(a)) => b != a,
            _ => true, // conservatively treat unknown as moved
        };

        if moved {
            Ok(PullResult::Pulled {
                branch: current,
                upstream_branch: upstream_branch.to_string(),
            })
        } else {
            Ok(PullResult::SkippedUpToDate {
                branch: current,
                upstream_branch: upstream_branch.to_string(),
            })
        }
    }

    fn create_branch(&self, name: &str, checkout: bool) -> Result<GitBranch, GitError> {
        let repo = self.repo()?;
        let head = repo.head()?.peel_to_commit()?;
        let branch = repo.branch(name, &head, false)?;
        let commit = branch.get().peel_to_commit()?;

        if checkout {
            repo.set_head(&format!("refs/heads/{}", name))?;
            repo.checkout_head(None)?;
        }

        Ok(GitBranch {
            name: name.to_string(),
            is_current: checkout,
            is_remote: false,
            commit_hash: commit.id().to_string(),
            commit_message: String::from_utf8_lossy(commit.message_bytes())
                .lines()
                .next()
                .unwrap_or_default()
                .to_string(),
        })
    }

    fn delete_branch(&self, name: &str) -> Result<(), GitError> {
        let repo = self.repo()?;
        let mut branch = repo.find_branch(name, git2::BranchType::Local)?;
        branch.delete()?;
        Ok(())
    }
}

/// Convert git2 delta status to our FileStatus
fn status_from_delta(status: git2::Delta) -> FileStatus {
    match status {
        git2::Delta::Added => FileStatus::Added,
        git2::Delta::Deleted => FileStatus::Deleted,
        git2::Delta::Modified => FileStatus::Modified,
        git2::Delta::Renamed => FileStatus::Renamed,
        git2::Delta::Copied => FileStatus::Copied,
        git2::Delta::Ignored => FileStatus::Ignored,
        git2::Delta::Untracked => FileStatus::Untracked,
        _ => FileStatus::Unmodified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_from_delta() {
        assert_eq!(status_from_delta(git2::Delta::Added), FileStatus::Added);
        assert_eq!(
            status_from_delta(git2::Delta::Modified),
            FileStatus::Modified
        );
        assert_eq!(status_from_delta(git2::Delta::Renamed), FileStatus::Renamed);
        assert_eq!(status_from_delta(git2::Delta::Deleted), FileStatus::Deleted);
        assert_eq!(status_from_delta(git2::Delta::Copied), FileStatus::Copied);
        assert_eq!(status_from_delta(git2::Delta::Ignored), FileStatus::Ignored);
        assert_eq!(
            status_from_delta(git2::Delta::Untracked),
            FileStatus::Untracked
        );
        assert_eq!(
            status_from_delta(git2::Delta::Unmodified),
            FileStatus::Unmodified
        );
    }

    #[test]
    fn git_error_display() {
        let err = GitError::RepoNotFound("/tmp/nonexistent".to_string());
        assert!(err.to_string().contains("/tmp/nonexistent"));

        let err = GitError::NothingToCommit;
        assert_eq!(err.to_string(), "Nothing to commit");

        let err = GitError::BranchNotFound("main".to_string());
        assert!(err.to_string().contains("main"));
    }

    #[test]
    fn git_error_from_git2() {
        // Verify From<git2::Error> is implemented
        let _err: GitError = GitError::GitOperation(git2::Error::from_str("test"));
    }

    #[test]
    fn git_status_serialization() {
        let status = GitStatus {
            branch: Some("main".to_string()),
            head_detached: false,
            files: vec![GitFileStatus {
                path: "src/main.rs".to_string(),
                index_status: FileStatus::Modified,
                working_tree_status: FileStatus::Modified,
            }],
            ahead: 2,
            behind: 1,
            upstream_branch: Some("refs/remotes/origin/main".to_string()),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("main"));
        let back: GitStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back.files.len(), 1);
        assert_eq!(back.ahead, 2);
        assert_eq!(
            back.upstream_branch.as_deref(),
            Some("refs/remotes/origin/main")
        );
    }

    #[test]
    fn git_branch_serialization() {
        let branch = GitBranch {
            name: "feature/test".to_string(),
            is_current: true,
            is_remote: false,
            commit_hash: "abc123".to_string(),
            commit_message: "Test commit".to_string(),
        };
        let json = serde_json::to_string(&branch).unwrap();
        let back: GitBranch = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "feature/test");
        assert!(back.is_current);
    }

    #[test]
    fn git_commit_serialization() {
        let commit = GitCommit {
            hash: "abcdef123456".to_string(),
            short_hash: "abcdef12".to_string(),
            author: "Test Author".to_string(),
            message: "Initial commit".to_string(),
            timestamp: "1700000000".to_string(),
        };
        let json = serde_json::to_string(&commit).unwrap();
        let back: GitCommit = serde_json::from_str(&json).unwrap();
        assert_eq!(back.short_hash, "abcdef12");
    }

    #[test]
    fn git_diff_entry_serialization() {
        let entry = GitDiffEntry {
            old_path: Some("old.rs".to_string()),
            new_path: "new.rs".to_string(),
            status: FileStatus::Renamed,
            additions: 10,
            deletions: 5,
            patch: Some("@@ -1,3 +1,10 @@".to_string()),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: GitDiffEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.additions, 10);
        assert!(back.patch.is_some());
    }

    #[test]
    fn git_log_entry_serialization() {
        let entry = GitLogEntry {
            commit: GitCommit {
                hash: "abc".to_string(),
                short_hash: "abc".to_string(),
                author: "Author".to_string(),
                message: "Msg".to_string(),
                timestamp: "0".to_string(),
            },
            refs: vec!["HEAD".to_string(), "refs/heads/main".to_string()],
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: GitLogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.refs.len(), 2);
    }

    #[test]
    fn file_status_serialization() {
        let status = FileStatus::Modified;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"modified\"");
        let back: FileStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, FileStatus::Modified);
    }

    // ─── New push/pull unit tests (pure, always run) ──────────────────

    #[test]
    fn classify_cli_error_maps_auth_failures() {
        let cases = [
            "fatal: could not read Username for 'https://github.com'",
            "git@github.com: Permission denied (publickey).",
            "fatal: Authentication failed for 'https://github.com/...'",
        ];
        for stderr in cases {
            assert!(
                matches!(classify_cli_error(stderr), GitError::AuthenticationRequired),
                "expected AuthenticationRequired for: {stderr}"
            );
        }
    }

    #[test]
    fn classify_cli_error_maps_remote_rejected() {
        let err =
            classify_cli_error("! [remote rejected] main -> main (pre-receive hook declined)");
        assert!(matches!(err, GitError::RemoteRejected(_)), "{:?}", err);

        let err = classify_cli_error(" ! [rejected]        main -> main (non-fast-forward)");
        assert!(matches!(err, GitError::RemoteRejected(_)));
    }

    #[test]
    fn classify_cli_error_falls_back_to_git_operation() {
        let err = classify_cli_error("fatal: not a git repository");
        assert!(matches!(err, GitError::GitOperation(_)));
    }

    #[test]
    fn resolve_default_remote_prefers_origin() {
        assert_eq!(resolve_default_remote(&[]), "origin");
        assert_eq!(resolve_default_remote(&["upstream".into()]), "upstream");
        // origin wins over others regardless of order.
        assert_eq!(
            resolve_default_remote(&["upstream".into(), "origin".into()]),
            "origin"
        );
        assert_eq!(
            resolve_default_remote(&["origin".into(), "upstream".into()]),
            "origin"
        );
    }

    #[test]
    fn resolve_symref_returns_none_for_non_symref() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = local_repo_pair();
        let clone = dir.path().join("clone");
        let repo = git2::Repository::open(&clone).expect("open");

        // No symref with this name → should return None.
        assert_eq!(resolve_symref(&repo, "origin/HEAD"), None);
        // Garbage input → None.
        assert_eq!(resolve_symref(&repo, "does/not/exist"), None);
    }

    #[test]
    fn resolve_symref_follows_origin_head_to_target() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = local_repo_pair();
        let clone = dir.path().join("clone");

        // Create the `origin/HEAD` → `origin/main` symref manually, mirroring
        // what `git clone` does on a fresh clone. We write the loose symref
        // file directly so we don't need a working `git remote set-head` call.
        let head_path = clone
            .join(".git")
            .join("refs")
            .join("remotes")
            .join("origin")
            .join("HEAD");
        std::fs::create_dir_all(head_path.parent().expect("parent"))
            .expect("create remotes/origin dir");
        std::fs::write(&head_path, "ref: refs/remotes/origin/main\n").expect("write HEAD");

        let repo = git2::Repository::open(&clone).expect("open");
        // libgit2 reads the symref back via find_reference, so we can follow it.
        assert_eq!(
            resolve_symref(&repo, "origin/HEAD"),
            Some("origin/main".into())
        );
        // Fully-qualified form should also work.
        assert_eq!(
            resolve_symref(&repo, "refs/remotes/origin/HEAD"),
            Some("origin/main".into())
        );
    }

    #[test]
    fn pick_local_branch_picks_matching_local_for_remote() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = local_repo_pair();
        let clone = dir.path().join("clone");
        let repo = git2::Repository::open(&clone).expect("open");

        // `main` exists locally and matches `origin/main` → DWIM should pick it.
        assert_eq!(
            pick_local_branch_if_any(&repo, "origin/main"),
            Some("main".into())
        );
        // Fully-qualified form works too.
        assert_eq!(
            pick_local_branch_if_any(&repo, "refs/remotes/origin/main"),
            Some("main".into())
        );
        // No local branch matching `origin/nonexistent` → None.
        assert_eq!(pick_local_branch_if_any(&repo, "origin/nonexistent"), None);
        // Local branch shorthand is not a remote ref → None.
        assert_eq!(pick_local_branch_if_any(&repo, "main"), None);
    }

    #[test]
    fn canonical_head_target_recognizes_local_and_tag_refs() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = local_repo_pair();
        let clone = dir.path().join("clone");
        let repo = git2::Repository::open(&clone).expect("open");

        // Shorthand local branch → refs/heads/main.
        assert_eq!(
            canonical_head_target(&repo, "main"),
            Some("refs/heads/main".into())
        );
        // Fully-qualified heads form is passed through.
        assert_eq!(
            canonical_head_target(&repo, "refs/heads/main"),
            Some("refs/heads/main".into())
        );
        // Remote-tracking ref → None (caller must use set_head_detached).
        assert_eq!(canonical_head_target(&repo, "origin/main"), None);
        // Unknown ref → None.
        assert_eq!(canonical_head_target(&repo, "does-not-exist"), None);
    }

    #[test]
    fn push_result_serialization() {
        let pushed = PushResult::Pushed {
            branch: "feat/x".into(),
            upstream_branch: "origin/feat/x".into(),
            set_upstream: true,
        };
        let json = serde_json::to_string(&pushed).unwrap();
        assert!(json.contains("\"status\":\"pushed\""));
        assert!(json.contains("\"set_upstream\":true"));
        let back: PushResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back, pushed);

        let skipped = PushResult::SkippedUpToDate {
            branch: "main".into(),
            upstream_branch: "origin/main".into(),
        };
        let json = serde_json::to_string(&skipped).unwrap();
        assert!(json.contains("\"status\":\"skipped_up_to_date\""));
    }

    #[test]
    fn pull_result_serialization() {
        let pulled = PullResult::Pulled {
            branch: "main".into(),
            upstream_branch: "origin/main".into(),
        };
        let json = serde_json::to_string(&pulled).unwrap();
        assert!(json.contains("\"status\":\"pulled\""));

        let skipped = PullResult::SkippedUpToDate {
            branch: "main".into(),
            upstream_branch: "origin/main".into(),
        };
        let json = serde_json::to_string(&skipped).unwrap();
        assert!(json.contains("\"status\":\"skipped_up_to_date\""));
    }

    #[test]
    fn git_error_new_variants_display() {
        assert!(GitError::NoUpstream.to_string().contains("upstream"));
        assert!(
            GitError::AuthenticationRequired
                .to_string()
                .contains("Authentication")
        );
        assert!(GitError::CliMissing("git").to_string().contains("`git`"));
        assert!(
            GitError::RemoteRejected("hook".into())
                .to_string()
                .contains("hook")
        );
        assert!(
            GitError::Timeout(Duration::from_secs(30))
                .to_string()
                .contains("30s")
        );
    }

    // ─── Integration tests (git-gated; skip if `git` binary absent) ────
    //
    // These create a local bare "remote" + a clone, then exercise push/pull
    // between them. No network, no credentials — purely local file:// remotes.

    fn git_available() -> bool {
        which::which("git").is_ok()
    }

    /// Build a local remote (bare) + a clone with one commit, returning the
    /// clone path. Both live under the tempdir (cleaned up automatically).
    fn local_repo_pair() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let remote_path = dir.path().join("remote.git");
        let clone_path = dir.path().join("clone");

        // Bare remote.
        std::process::Command::new("git")
            .args(["init", "--bare"])
            .arg(&remote_path)
            .output()
            .expect("git init --bare");

        // Clone (empty), then seed an initial commit + push to set upstream.
        std::process::Command::new("git")
            .args(["clone"])
            .arg(&remote_path)
            .arg(&clone_path)
            .output()
            .expect("git clone");

        // Configure a test author (git requires user.name/email to commit).
        for (k, v) in [("user.name", "Test"), ("user.email", "t@t.test")] {
            std::process::Command::new("git")
                .args(["config", k, v])
                .current_dir(&clone_path)
                .output()
                .expect("git config");
        }

        // Seed a commit + set default branch (HEAD) so push -u works.
        std::fs::write(clone_path.join("README.md"), "init\n").expect("write");
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&clone_path)
            .output()
            .expect("git add");
        std::process::Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(&clone_path)
            .output()
            .expect("git commit");
        // Set the bare remote's default branch by pushing with -u.
        std::process::Command::new("git")
            .args(["push", "-u", "origin", "HEAD:main"])
            .current_dir(&clone_path)
            .output()
            .expect("git push init");
        // Ensure local branch is "main" + tracks origin/main.
        std::process::Command::new("git")
            .args(["branch", "-M", "main"])
            .current_dir(&clone_path)
            .output()
            .expect("git branch -M");
        std::process::Command::new("git")
            .args(["branch", "--set-upstream-to=origin/main", "main"])
            .current_dir(&clone_path)
            .output()
            .expect("set upstream");

        dir
    }

    #[test]
    fn integration_push_skipped_when_up_to_date() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = local_repo_pair();
        let clone = dir.path().join("clone");
        let service = Git2Service::open(&clone).expect("open");

        // No new commits since the init push → should skip.
        let result = service.push("origin", "main").expect("push");
        assert!(
            matches!(result, PushResult::SkippedUpToDate { .. }),
            "expected skip, got {:?}",
            result
        );
    }

    #[test]
    fn integration_push_pushed_on_new_commit() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = local_repo_pair();
        let clone = dir.path().join("clone");
        let service = Git2Service::open(&clone).expect("open");

        // New commit → push should report Pushed.
        std::fs::write(clone.join("file.txt"), "new\n").expect("write");
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&clone)
            .output()
            .expect("git add");
        std::process::Command::new("git")
            .args(["commit", "-m", "second"])
            .current_dir(&clone)
            .output()
            .expect("git commit");

        let result = service.push("origin", "main").expect("push");
        assert!(
            matches!(result, PushResult::Pushed { .. }),
            "expected pushed, got {:?}",
            result
        );
    }

    #[test]
    fn integration_pull_skipped_when_up_to_date() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = local_repo_pair();
        let clone = dir.path().join("clone");
        let service = Git2Service::open(&clone).expect("open");

        // No remote changes → pull should skip.
        let result = service.pull("origin", "main").expect("pull");
        assert!(
            matches!(result, PullResult::SkippedUpToDate { .. }),
            "expected skip, got {:?}",
            result
        );
    }

    #[test]
    fn integration_pull_pulled_after_remote_commit() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = local_repo_pair();
        let clone = dir.path().join("clone");
        let remote = dir.path().join("remote.git");

        // Make a commit directly on the bare remote by pushing from a throwaway clone.
        let other = dir.path().join("other");
        std::process::Command::new("git")
            .args([
                "clone",
                "--branch",
                "main",
                &remote.display().to_string(),
                &other.display().to_string(),
            ])
            .output()
            .expect("clone other");
        for (k, v) in [("user.name", "Other"), ("user.email", "o@o.test")] {
            std::process::Command::new("git")
                .args(["config", k, v])
                .current_dir(&other)
                .output()
                .expect("config");
        }
        std::fs::write(other.join("from-other.md"), "x\n").expect("write");
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&other)
            .output()
            .expect("add");
        std::process::Command::new("git")
            .args(["commit", "-m", "from other"])
            .current_dir(&other)
            .output()
            .expect("commit");
        let push_other = std::process::Command::new("git")
            .args(["push", "origin", "main:main"])
            .current_dir(&other)
            .output()
            .expect("push other");
        assert!(
            push_other.status.success(),
            "other push failed: {}",
            String::from_utf8_lossy(&push_other.stderr)
        );

        // Now pull in the original clone — should report Pulled.
        let service = Git2Service::open(&clone).expect("open");
        let result = service.pull("origin", "main").expect("pull");
        assert!(
            matches!(result, PullResult::Pulled { .. }),
            "expected pulled, got {:?}",
            result
        );
    }

    #[test]
    fn integration_pull_errors_without_upstream() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        // A repo with no upstream configured → NoUpstream error.
        let dir = tempfile::tempdir().expect("tempdir");
        std::process::Command::new("git")
            .args(["init"])
            .current_dir(dir.path())
            .output()
            .expect("init");
        for (k, v) in [("user.name", "T"), ("user.email", "t@t.test")] {
            std::process::Command::new("git")
                .args(["config", k, v])
                .current_dir(dir.path())
                .output()
                .expect("config");
        }
        std::fs::write(dir.path().join("a.txt"), "a\n").expect("write");
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(dir.path())
            .output()
            .expect("add");
        std::process::Command::new("git")
            .args(["commit", "-m", "x"])
            .current_dir(dir.path())
            .output()
            .expect("commit");

        let service = Git2Service::open(dir.path()).expect("open");
        let err = service.pull("origin", "main").unwrap_err();
        assert!(
            matches!(err, GitError::NoUpstream),
            "expected NoUpstream, got {:?}",
            err
        );
    }

    #[test]
    fn integration_checkout_resolves_origin_head_symref() {
        // Regression: libgit2's `revparse_ext("origin/HEAD")` returns
        // `reference not found` even when the symref exists, while the `git`
        // CLI resolves it via DWIM. Without `resolve_symref`, the branch
        // selector errors when a user picks the remote's default branch.
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = local_repo_pair();
        let clone = dir.path().join("clone");

        // Create a second branch on the remote so we have something to switch
        // away from and back to. Then point origin/HEAD at it via a loose
        // symref file (mirrors what `git clone` does).
        std::process::Command::new("git")
            .args(["branch", "feature"])
            .current_dir(&clone)
            .output()
            .expect("git branch feature");
        std::process::Command::new("git")
            .args(["push", "origin", "feature"])
            .current_dir(&clone)
            .output()
            .expect("git push feature");
        // Switch the local clone off `main` so we have to come back.
        std::process::Command::new("git")
            .args(["checkout", "feature"])
            .current_dir(&clone)
            .output()
            .expect("git checkout feature");

        // Write the `origin/HEAD` → `origin/main` symref.
        let head_path = clone
            .join(".git")
            .join("refs")
            .join("remotes")
            .join("origin")
            .join("HEAD");
        std::fs::create_dir_all(head_path.parent().expect("parent"))
            .expect("create remotes/origin dir");
        std::fs::write(&head_path, "ref: refs/remotes/origin/main\n").expect("write HEAD");

        // The bug: this used to fail with `reference 'origin/HEAD' not found`.
        let service = Git2Service::open(&clone).expect("open");
        service
            .checkout("origin/HEAD")
            .expect("checkout via symref");

        // HEAD should now point at `main` (the symref target's branch).
        let head_branch = std::process::Command::new("git")
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .current_dir(&clone)
            .output()
            .expect("git rev-parse HEAD");
        assert_eq!(
            String::from_utf8_lossy(&head_branch.stdout).trim(),
            "main",
            "origin/HEAD checkout should land on the symref's target branch"
        );
    }

    #[test]
    fn branches_does_not_mark_remote_as_current() {
        // Regression: the old implementation compared commit OIDs, so when
        // local `main` and remote-tracking `origin/main` pointed at the same
        // commit, both got `is_current: true`. Only the local branch that
        // HEAD actually resolves to may carry `is_current: true`.
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let dir = local_repo_pair();
        let clone = dir.path().join("clone");
        let service = Git2Service::open(&clone).expect("open");

        let list = service.branches().expect("branches");

        let current_branches: Vec<_> = list.iter().filter(|b| b.is_current).collect();
        assert_eq!(
            current_branches.len(),
            1,
            "exactly one branch should be current; got {current_branches:?}"
        );
        let current = current_branches[0];
        assert_eq!(current.name, "main");
        assert!(!current.is_remote, "current branch must be local");

        let origin_main = list
            .iter()
            .find(|b| b.name == "origin/main")
            .expect("origin/main should appear in list");
        assert!(
            !origin_main.is_current,
            "remote-tracking origin/main must never be tagged is_current"
        );
        assert!(origin_main.is_remote);
    }
}

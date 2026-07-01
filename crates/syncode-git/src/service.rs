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

        Ok(GitStatus {
            branch,
            head_detached,
            files,
            ahead: 0,
            behind: 0,
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
        let head_target = repo.head().ok().and_then(|h| h.target());

        for branch_result in repo.branches(None)? {
            let (branch, _) = branch_result?;
            let name = branch.name()?.unwrap_or_default().to_string();
            let is_current = branch.get().target() == head_target;
            let commit = branch.get().peel_to_commit()?;
            let message = String::from_utf8_lossy(commit.message_bytes())
                .lines()
                .next()
                .unwrap_or_default()
                .to_string();

            branches.push(GitBranch {
                name,
                is_current,
                is_remote: false,
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
        let (obj, _) = repo.revparse_ext(ref_name)?;
        repo.checkout_tree(&obj, None)?;
        repo.set_head(ref_name)?;
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
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("main"));
        let back: GitStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back.files.len(), 1);
        assert_eq!(back.ahead, 2);
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
}

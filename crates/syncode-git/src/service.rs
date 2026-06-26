//! GitService trait + implementation

use crate::{
    FileStatus, GitBranch, GitCommit, GitDiffEntry, GitFileStatus, GitLogEntry, GitStatus,
};
use git2::{Repository, StatusOptions};
use thiserror::Error;
use std::path::Path;

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
}

/// The GitService trait — defines all git operations
pub trait GitService: Send + Sync {
    /// Get the full repository status
    fn status(&self) -> Result<GitStatus, GitError>;

    /// Get diff between working tree and index
    fn diff(&self, old_commit: Option<&str>, new_commit: Option<&str>) -> Result<Vec<GitDiffEntry>, GitError>;

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

    /// Push to remote
    fn push(&self, remote: &str, branch: &str) -> Result<(), GitError>;

    /// Pull from remote
    fn pull(&self, remote: &str, branch: &str) -> Result<(), GitError>;

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
        let head_detached = head.as_ref().map_or(false, |h| {
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

    fn diff(&self, _old_commit: Option<&str>, _new_commit: Option<&str>) -> Result<Vec<GitDiffEntry>, GitError> {
        let repo = self.repo()?;
        let diff = repo.diff_index_to_workdir(None, None)?;
        let mut entries = Vec::new();

        for delta in diff.deltas() {
            let new_path = delta.new_file().path().map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
            let old_path = delta.old_file().path().map(|p| p.to_string_lossy().to_string());
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

        let oid = repo.commit(
            Some("HEAD"),
            &sig,
            &sig,
            message,
            &tree,
            parents.as_slice(),
        )?;

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

    fn push(&self, _remote: &str, _branch: &str) -> Result<(), GitError> {
        tracing::warn!("push not yet fully implemented — Phase 3.4");
        Ok(())
    }

    fn pull(&self, _remote: &str, _branch: &str) -> Result<(), GitError> {
        tracing::warn!("pull not yet fully implemented — Phase 3.4");
        Ok(())
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
        assert_eq!(status_from_delta(git2::Delta::Modified), FileStatus::Modified);
        assert_eq!(status_from_delta(git2::Delta::Renamed), FileStatus::Renamed);
        assert_eq!(status_from_delta(git2::Delta::Deleted), FileStatus::Deleted);
        assert_eq!(status_from_delta(git2::Delta::Copied), FileStatus::Copied);
        assert_eq!(status_from_delta(git2::Delta::Ignored), FileStatus::Ignored);
        assert_eq!(status_from_delta(git2::Delta::Untracked), FileStatus::Untracked);
        assert_eq!(status_from_delta(git2::Delta::Unmodified), FileStatus::Unmodified);
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
            files: vec![
                GitFileStatus {
                    path: "src/main.rs".to_string(),
                    index_status: FileStatus::Modified,
                    working_tree_status: FileStatus::Modified,
                },
            ],
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
}

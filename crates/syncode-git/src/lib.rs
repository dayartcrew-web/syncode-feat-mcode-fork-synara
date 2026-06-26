//! Syncode Git — Git Integration
//!
//! Full git workflow: status, diff, branch, commit, push, pull,
//! worktree management, checkpoint refs, and stacked actions pipeline.

pub mod checkpoint;
pub mod diff;
pub mod service;
pub mod stacked_actions;
pub mod worktree;

use serde::{Deserialize, Serialize};

/// Git file status (unstaged, staged, untracked, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileStatus {
    Unmodified,
    Modified,
    Added,
    Deleted,
    Renamed,
    Copied,
    Untracked,
    Ignored,
}

/// A single file's status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitFileStatus {
    pub path: String,
    pub index_status: FileStatus,
    pub working_tree_status: FileStatus,
}

/// Full repository status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitStatus {
    pub branch: Option<String>,
    pub head_detached: bool,
    pub files: Vec<GitFileStatus>,
    pub ahead: u32,
    pub behind: u32,
}

/// A git branch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitBranch {
    pub name: String,
    pub is_current: bool,
    pub is_remote: bool,
    pub commit_hash: String,
    pub commit_message: String,
}

/// A git diff entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitDiffEntry {
    pub old_path: Option<String>,
    pub new_path: String,
    pub status: FileStatus,
    pub additions: u32,
    pub deletions: u32,
    pub patch: Option<String>,
}

/// Git commit info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitCommit {
    pub hash: String,
    pub short_hash: String,
    pub author: String,
    pub message: String,
    pub timestamp: String,
}

/// Git log entry (commit + optional refs)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitLogEntry {
    #[serde(flatten)]
    pub commit: GitCommit,
    pub refs: Vec<String>,
}

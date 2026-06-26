//! Tauri IPC Commands — Git Operations
//!
//! Commands for git status, diff, commit, log, branches,
//! worktrees, checkpoints, and stacked actions.

use serde::{Deserialize, Serialize};
use syncode_git::service::GitService;

/// Git command results
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitStatusResult {
    pub branch: Option<String>,
    pub head_detached: bool,
    pub files: Vec<syncode_git::GitFileStatus>,
    pub ahead: u32,
    pub behind: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitLogResult {
    pub entries: Vec<syncode_git::GitLogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitBranchResult {
    pub branches: Vec<syncode_git::GitBranch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitDiffResult {
    pub entries: Vec<syncode_git::GitDiffEntry>,
    pub files_changed: usize,
    pub additions: u32,
    pub deletions: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitCommitResult {
    pub hash: String,
    pub short_hash: String,
    pub message: String,
}

/// Get git status of the working directory
#[tauri::command]
pub fn git_status(path: String) -> Result<GitStatusResult, String> {
    let service = syncode_git::service::Git2Service::open(std::path::Path::new(&path))
        .map_err(|e| e.to_string())?;
    let status = service.status().map_err(|e| e.to_string())?;
    Ok(GitStatusResult {
        branch: status.branch,
        head_detached: status.head_detached,
        files: status.files,
        ahead: status.ahead,
        behind: status.behind,
    })
}

/// Get git diff
#[tauri::command]
pub fn git_diff(path: String, old_ref: Option<String>, new_ref: Option<String>) -> Result<GitDiffResult, String> {
    let service = syncode_git::service::Git2Service::open(std::path::Path::new(&path))
        .map_err(|e| e.to_string())?;
    let entries = service.diff(
        old_ref.as_deref(),
        new_ref.as_deref(),
    ).map_err(|e| e.to_string())?;

    let additions: u32 = entries.iter().map(|e| e.additions).sum();
    let deletions: u32 = entries.iter().map(|e| e.deletions).sum();
    let files_changed = entries.len();

    Ok(GitDiffResult { entries, files_changed, additions, deletions })
}

/// Get commit log
#[tauri::command]
pub fn git_log(path: String, max_count: Option<u32>) -> Result<GitLogResult, String> {
    let service = syncode_git::service::Git2Service::open(std::path::Path::new(&path))
        .map_err(|e| e.to_string())?;
    let entries = service.log(max_count.unwrap_or(20)).map_err(|e| e.to_string())?;
    Ok(GitLogResult { entries })
}

/// List branches
#[tauri::command]
pub fn git_branches(path: String) -> Result<GitBranchResult, String> {
    let service = syncode_git::service::Git2Service::open(std::path::Path::new(&path))
        .map_err(|e| e.to_string())?;
    let branches = service.branches().map_err(|e| e.to_string())?;
    Ok(GitBranchResult { branches })
}

/// Stage files
#[tauri::command]
pub fn git_add(path: String, files: Vec<String>) -> Result<(), String> {
    let service = syncode_git::service::Git2Service::open(std::path::Path::new(&path))
        .map_err(|e| e.to_string())?;
    let refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();
    service.add(&refs).map_err(|e| e.to_string())
}

/// Commit staged changes
#[tauri::command]
pub fn git_commit(path: String, message: String) -> Result<GitCommitResult, String> {
    let service = syncode_git::service::Git2Service::open(std::path::Path::new(&path))
        .map_err(|e| e.to_string())?;
    let commit = service.commit(&message).map_err(|e| e.to_string())?;
    Ok(GitCommitResult {
        hash: commit.hash,
        short_hash: commit.short_hash,
        message: commit.message,
    })
}

/// Create a branch
#[tauri::command]
pub fn git_create_branch(path: String, name: String, checkout: bool) -> Result<syncode_git::GitBranch, String> {
    let service = syncode_git::service::Git2Service::open(std::path::Path::new(&path))
        .map_err(|e| e.to_string())?;
    service.create_branch(&name, checkout).map_err(|e| e.to_string())
}

/// Delete a branch
#[tauri::command]
pub fn git_delete_branch(path: String, name: String) -> Result<(), String> {
    let service = syncode_git::service::Git2Service::open(std::path::Path::new(&path))
        .map_err(|e| e.to_string())?;
    service.delete_branch(&name).map_err(|e| e.to_string())
}

/// Checkout a branch or commit
#[tauri::command]
pub fn git_checkout(path: String, ref_name: String) -> Result<(), String> {
    let service = syncode_git::service::Git2Service::open(std::path::Path::new(&path))
        .map_err(|e| e.to_string())?;
    service.checkout(&ref_name).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_result_types_serialization() {
        let result = GitStatusResult {
            branch: Some("main".to_string()),
            head_detached: false,
            files: vec![],
            ahead: 0,
            behind: 0,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("main"));
    }

    #[test]
    fn git_diff_result_serialization() {
        let result = GitDiffResult {
            entries: vec![],
            files_changed: 0,
            additions: 0,
            deletions: 0,
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: GitDiffResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.files_changed, 0);
    }
}

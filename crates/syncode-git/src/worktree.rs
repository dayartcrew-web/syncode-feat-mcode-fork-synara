//! Worktree management
//!
//! Git worktree support for parallel development workflows.
//! Each thread can optionally have its own worktree for isolated file changes.

use std::path::Path;

use crate::service::{Git2Service, GitError};

/// Information about a worktree
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorktreeInfo {
    /// Path to the worktree directory
    pub path: String,
    /// Branch the worktree is on
    pub branch: String,
    /// Whether this is the main worktree
    pub is_main: bool,
    /// Whether the worktree is locked
    pub is_locked: bool,
}

/// List all worktrees for the repository
pub fn list_worktrees(service: &Git2Service) -> Result<Vec<WorktreeInfo>, GitError> {
    let repo = service.repo()?;
    let wt_names = repo.worktrees()?;
    let main_path = repo.workdir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();

    let mut result = Vec::new();
    for wt_name_opt in &wt_names {
        let wt_name = match wt_name_opt {
            Some(name) => name,
            None => continue,
        };
        let wt = repo.find_worktree(wt_name)?;
        let path = wt.path().to_string_lossy().to_string();
        let is_locked = matches!(wt.is_locked()?, git2::WorktreeLockStatus::Locked { .. });

        let is_main = path == main_path;
        result.push(WorktreeInfo {
            path,
            branch: wt_name.to_string(),
            is_main,
            is_locked,
        });
    }

    Ok(result)
}

/// Add a new worktree for a branch
pub fn add_worktree(
    service: &Git2Service,
    _path: &Path,
    branch_name: &str,
    create_branch: bool,
) -> Result<WorktreeInfo, GitError> {
    let repo = service.repo()?;

    // Create the branch if requested
    if create_branch {
        let head = repo.head()?.peel_to_commit()?;
        repo.branch(branch_name, &head, false)?;
    }

    let branch_ref = format!("refs/heads/{}", branch_name);
    let branch_path = std::path::Path::new(&branch_ref);

    let wt = repo.worktree(
        branch_name,
        branch_path,
        None,
    )?;

    Ok(WorktreeInfo {
        path: wt.path().to_string_lossy().to_string(),
        branch: branch_name.to_string(),
        is_main: false,
        is_locked: false,
    })
}

/// Remove a worktree
pub fn remove_worktree(service: &Git2Service, branch_name: &str, _force: bool) -> Result<(), GitError> {
    let repo = service.repo()?;
    let wt = repo.find_worktree(branch_name)
        .map_err(|_| GitError::BranchNotFound(format!("Worktree '{}' not found", branch_name)))?;

    wt.prune(None)?;
    Ok(())
}

/// Prune stale worktree admin files
pub fn prune_worktrees(_service: &Git2Service) -> Result<u32, GitError> {
    // git2 doesn't expose prune_worktrees directly
    // In production, shell out: git worktree prune
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worktree_info_fields() {
        let info = WorktreeInfo {
            path: "/tmp/test".to_string(),
            branch: "feature/test".to_string(),
            is_main: false,
            is_locked: false,
        };
        assert_eq!(info.path, "/tmp/test");
        assert_eq!(info.branch, "feature/test");
        assert!(!info.is_main);
    }

    #[test]
    fn worktree_info_serialization() {
        let info = WorktreeInfo {
            path: "/tmp/test".to_string(),
            branch: "main".to_string(),
            is_main: true,
            is_locked: false,
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: WorktreeInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.branch, "main");
        assert!(back.is_main);
    }
}

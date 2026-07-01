//! Checkpoint store — git refs captured on turn boundaries
//!
//! When a turn completes, the current HEAD is saved as a checkpoint ref
//! (refs/syncode/checkpoints/<turn_id>). This enables:
//! - Reverting to any previous turn state
//! - Computing diffs between turns
//! - Undo/redo of agent actions

use crate::GitCommit;
use crate::service::{Git2Service, GitError};

/// A checkpoint representing the git state at a turn boundary
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    /// The turn ID this checkpoint is for
    pub turn_id: String,
    /// The commit hash at checkpoint time
    pub commit_hash: String,
    /// Short hash for display
    pub short_hash: String,
    /// Human-readable description
    pub description: String,
    /// Whether this checkpoint has been restored
    pub restored: bool,
}

/// Create a checkpoint for the current HEAD
pub fn create_checkpoint(
    service: &Git2Service,
    turn_id: &str,
    description: &str,
) -> Result<Checkpoint, GitError> {
    let repo = service.repo()?;
    let head = repo.head()?.peel_to_commit()?;
    let hash = head.id().to_string();
    let short_hash = hash[..8].to_string();

    // Create a ref at refs/syncode/checkpoints/<turn_id>
    let ref_name = format!("refs/syncode/checkpoints/{}", turn_id);
    repo.reference(
        &ref_name,
        head.id(),
        true, // force overwrite
        &format!("Syncode checkpoint for turn {}", turn_id),
    )?;

    Ok(Checkpoint {
        turn_id: turn_id.to_string(),
        commit_hash: hash.clone(),
        short_hash,
        description: description.to_string(),
        restored: false,
    })
}

/// List all checkpoints
pub fn list_checkpoints(service: &Git2Service) -> Result<Vec<Checkpoint>, GitError> {
    let repo = service.repo()?;
    let references = repo.references_glob("refs/syncode/checkpoints/*")?;

    let mut checkpoints = Vec::new();
    for reference_result in references {
        let reference = reference_result?;
        if let Some(name) = reference.shorthand() {
            // Extract turn_id from "syncode/checkpoints/<turn_id>"
            let turn_id = name
                .strip_prefix("syncode/checkpoints/")
                .unwrap_or(name)
                .to_string();

            if let Some(target) = reference.target() {
                let commit = repo.find_commit(target)?;
                let message = String::from_utf8_lossy(commit.message_bytes())
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .to_string();

                checkpoints.push(Checkpoint {
                    turn_id,
                    commit_hash: target.to_string(),
                    short_hash: target.to_string()[..8].to_string(),
                    description: message,
                    restored: false,
                });
            }
        }
    }

    Ok(checkpoints)
}

/// Restore a checkpoint by checking out the commit
pub fn restore_checkpoint(service: &Git2Service, turn_id: &str) -> Result<GitCommit, GitError> {
    let repo = service.repo()?;
    let ref_name = format!("refs/syncode/checkpoints/{}", turn_id);

    let reference = repo
        .find_reference(&ref_name)
        .map_err(|_| GitError::BranchNotFound(format!("Checkpoint '{}' not found", turn_id)))?;

    let target = reference.target().unwrap();
    let commit = repo.find_commit(target)?;

    // Create a detached HEAD at the checkpoint commit
    repo.set_head_detached(target)?;
    repo.checkout_head(None)?;

    Ok(GitCommit {
        hash: target.to_string(),
        short_hash: target.to_string()[..8].to_string(),
        author: String::from_utf8_lossy(commit.author().name_bytes()).to_string(),
        message: String::from_utf8_lossy(commit.message_bytes())
            .lines()
            .next()
            .unwrap_or_default()
            .to_string(),
        timestamp: commit.time().seconds().to_string(),
    })
}

/// Delete a checkpoint ref
pub fn delete_checkpoint(service: &Git2Service, turn_id: &str) -> Result<(), GitError> {
    let repo = service.repo()?;
    let ref_name = format!("refs/syncode/checkpoints/{}", turn_id);

    let mut reference = repo
        .find_reference(&ref_name)
        .map_err(|_| GitError::BranchNotFound(format!("Checkpoint '{}' not found", turn_id)))?;

    reference.delete()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_struct_fields() {
        let cp = Checkpoint {
            turn_id: "turn-123".to_string(),
            commit_hash: "abc123".to_string(),
            short_hash: "abc12345".to_string(),
            description: "After turn 1".to_string(),
            restored: false,
        };
        assert_eq!(cp.turn_id, "turn-123");
        assert!(!cp.restored);
    }

    #[test]
    fn checkpoint_serialization() {
        let cp = Checkpoint {
            turn_id: "turn-456".to_string(),
            commit_hash: "def456".to_string(),
            short_hash: "def45678".to_string(),
            description: "After turn 2".to_string(),
            restored: false,
        };
        let json = serde_json::to_string(&cp).unwrap();
        let back: Checkpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(back.turn_id, "turn-456");
        assert_eq!(back.short_hash, "def45678");
    }
}

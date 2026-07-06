//! Diff computation — turn diffs and full thread diffs
//!
//! Computes meaningful diffs between turn checkpoints,
//! generates patch text, and provides summary statistics.

use crate::service::{Git2Service, GitError, GitService};
use crate::{FileStatus, GitDiffEntry};

/// A summarized diff between two commits
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DiffSummary {
    /// Total files changed
    pub files_changed: usize,
    /// Total lines added
    pub additions: u32,
    /// Total lines deleted
    pub deletions: u32,
    /// Individual file diffs
    pub entries: Vec<GitDiffEntry>,
}

/// Compute diff between two commits (or working tree)
pub fn compute_diff(
    service: &Git2Service,
    old_ref: Option<&str>,
    new_ref: Option<&str>,
) -> Result<DiffSummary, GitError> {
    let entries = service.diff(old_ref, new_ref)?;

    let additions: u32 = entries.iter().map(|e| e.additions).sum();
    let deletions: u32 = entries.iter().map(|e| e.deletions).sum();
    let files_changed = entries.len();

    Ok(DiffSummary {
        files_changed,
        additions,
        deletions,
        entries,
    })
}

/// Like [`compute_diff`], but populates each entry's `patch` field with REAL
/// unified-diff hunks (`@@ ... @@` headers + `+`/`-` line content) and the
/// per-file `additions`/`deletions` counts. Uses `Git2Service::diff_with_patches`
/// (which goes through `git2::Patch`) instead of the delta-only
/// [`GitService::diff`](crate::service::GitService::diff) — the latter leaves
/// `patch: None` and `additions: 0, deletions: 0`.
///
/// Defaults to working-tree-vs-HEAD (staged + unstaged together). See
/// [`Git2Service::diff_with_patches`] for the resolution rules and graceful
/// fallbacks (no HEAD → empty; binary files → empty patch).
///
/// Used by the MCode UI's `DiffPanel` (via the `git.readWorkingTreeDiff` RPC),
/// which parses the per-file patch text with `parsePatch()` and renders the
/// actual `+`/`-` line chips with non-zero counts.
pub fn compute_diff_with_patches(
    service: &Git2Service,
    old_ref: Option<&str>,
    new_ref: Option<&str>,
) -> Result<DiffSummary, GitError> {
    let entries = service.diff_with_patches(old_ref, new_ref)?;

    let additions: u32 = entries.iter().map(|e| e.additions).sum();
    let deletions: u32 = entries.iter().map(|e| e.deletions).sum();
    let files_changed = entries.len();

    Ok(DiffSummary {
        files_changed,
        additions,
        deletions,
        entries,
    })
}

/// Compute diff between two turn checkpoints
pub fn diff_between_turns(
    service: &Git2Service,
    turn_a: &str,
    turn_b: &str,
) -> Result<DiffSummary, GitError> {
    let ref_a = format!("refs/syncode/checkpoints/{}", turn_a);
    let ref_b = format!("refs/syncode/checkpoints/{}", turn_b);
    compute_diff(service, Some(&ref_a), Some(&ref_b))
}

/// Filter diff entries by file extension
pub fn filter_by_extension(entries: &[GitDiffEntry], ext: &str) -> Vec<GitDiffEntry> {
    entries
        .iter()
        .filter(|e| e.new_path.ends_with(ext))
        .cloned()
        .collect()
}

/// Filter diff entries by status
pub fn filter_by_status(entries: &[GitDiffEntry], status: FileStatus) -> Vec<GitDiffEntry> {
    entries
        .iter()
        .filter(|e| e.status == status)
        .cloned()
        .collect()
}

/// Format a diff summary as a human-readable string
pub fn format_summary(summary: &DiffSummary) -> String {
    format!(
        "{} file(s) changed, {} insertion(s)(+), {} deletion(s)(-)",
        summary.files_changed, summary.additions, summary.deletions
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries() -> Vec<GitDiffEntry> {
        vec![
            GitDiffEntry {
                old_path: None,
                new_path: "src/main.rs".to_string(),
                status: FileStatus::Modified,
                additions: 10,
                deletions: 2,
                patch: None,
            },
            GitDiffEntry {
                old_path: None,
                new_path: "src/lib.rs".to_string(),
                status: FileStatus::Added,
                additions: 50,
                deletions: 0,
                patch: None,
            },
            GitDiffEntry {
                old_path: Some("old_file.rs".to_string()),
                new_path: "new_file.rs".to_string(),
                status: FileStatus::Renamed,
                additions: 5,
                deletions: 5,
                patch: None,
            },
        ]
    }

    #[test]
    fn filter_by_extension_works() {
        let entries = make_entries();
        let rust_files = filter_by_extension(&entries, ".rs");
        assert_eq!(rust_files.len(), 3);

        let ts_files = filter_by_extension(&entries, ".ts");
        assert_eq!(ts_files.len(), 0);
    }

    #[test]
    fn filter_by_status_works() {
        let entries = make_entries();
        let added = filter_by_status(&entries, FileStatus::Added);
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].new_path, "src/lib.rs");

        let modified = filter_by_status(&entries, FileStatus::Modified);
        assert_eq!(modified.len(), 1);
    }

    #[test]
    fn format_summary_works() {
        let summary = DiffSummary {
            files_changed: 3,
            additions: 65,
            deletions: 7,
            entries: make_entries(),
        };
        let text = format_summary(&summary);
        assert!(text.contains("3 file(s) changed"));
        assert!(text.contains("65 insertion(s)(+)"));
        assert!(text.contains("7 deletion(s)(-)"));
    }

    #[test]
    fn diff_summary_serialization() {
        let summary = DiffSummary {
            files_changed: 1,
            additions: 5,
            deletions: 3,
            entries: vec![],
        };
        let json = serde_json::to_string(&summary).unwrap();
        let back: DiffSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.files_changed, 1);
    }
}

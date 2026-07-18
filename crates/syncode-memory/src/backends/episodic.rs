//! Append-only JSONL episodic backend.
//!
//! Each `(scope, user_id)` pair gets its own JSONL file under
//! `<root>/episodic/<scope>/<user_id>.jsonl`. Appends are atomic (open in
//! append mode + write line + flush) and serialised per-file via a
//! [`tokio::sync::Mutex`], so concurrent writers for the same file are safe.
//!
//! Retrieve scans the file backwards (most recent first), parses each line
//! as a [`MemoryEntry`], skips malformed lines with a `tracing::warn!`, and
//! returns up to `k` records. Score is recency-ranked in `[0.5, 1.0]` — the
//! most recent entry gets `1.0`, older entries decay linearly toward `0.5`
//! so they still rank above other backends' zero-score contributions.
//!
//! No external dependencies. Suitable as the default non-vector backend
//! for installs that want history without Postgres.

use crate::hybrid::{MemoryBackend, MemoryEntry, MemoryRecord, Scope};
use crate::provider::{MemoryProviderError, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Mutex;

/// Soft cap above which we emit a `tracing::warn!` about file size. We do
/// NOT auto-rotate in v1 — rotation semantics (compaction? archival?) need
/// their own design. 100 MB is roughly 250k interactions of average size.
const ROTATION_WARN_BYTES: u64 = 100 * 1024 * 1024;

/// Append-only JSONL [`MemoryBackend`].
///
/// Construct with [`EpisodicBackend::new`] for the default root
/// (`~/.syncode/memory`), or [`EpisodicBackend::with_root`] for tests or
/// custom layouts. The backend is cheaply cloneable (it holds everything
/// behind `Arc`) and safe to share across tasks.
#[derive(Clone)]
pub struct EpisodicBackend {
    root: PathBuf,
    /// Per-file locks. The outer Mutex guards the map; each value is an
    /// Arc<Mutex> serialising writers to one JSONL file. Locks accumulate
    /// over the process lifetime but are bounded by `(scope × user_id)`
    /// cardinality, which is small for any single deployment.
    file_locks: Arc<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>>,
}

impl EpisodicBackend {
    /// Create a backend rooted at `~/.syncode/memory`. Files land under
    /// `<root>/episodic/<scope>/<user_id>.jsonl`. Returns an error if the
    /// home directory can't be resolved.
    pub fn new() -> Result<Self> {
        let home = dirs_or_err()?;
        Ok(Self::with_root(home.join(".syncode").join("memory")))
    }

    /// Create a backend with an explicit root. The caller is responsible
    /// for picking a stable, writable path. Used by tests and embeds that
    /// colocate the JSONL files with their own data dir.
    pub fn with_root(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            file_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Resolve `<root>/episodic/<scope>/<safe_user_id>.jsonl` for the pair.
    /// `safe_user_id` is the user_id with path separators / dots replaced,
    /// so a malicious user_id can't escape the scope directory.
    fn file_path(&self, scope: Scope, user_id: &str) -> PathBuf {
        self.root
            .join("episodic")
            .join(scope.as_str())
            .join(format!("{}.jsonl", sanitize_user_id(user_id)))
    }

    /// Get-or-insert a per-file lock. Lock acquisition is awaited inside
    /// `store` / `retrieve`, not here, so two callers racing for the same
    /// file both end up with the same `Arc<Mutex>`.
    async fn file_lock(&self, path: PathBuf) -> Arc<Mutex<()>> {
        let guard = self.file_locks.lock().await;
        if let Some(lock) = guard.get(&path) {
            return Arc::clone(lock);
        }
        drop(guard);

        // Re-acquire and insert. A second racer may have inserted between
        // the two locks; the entry() call resolves the race deterministically.
        let mut guard = self.file_locks.lock().await;
        Arc::clone(
            guard
                .entry(path)
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    /// Ensure the parent directory for `path` exists. Cheap when it already
    /// does (single stat). Failure surfaces as a Store error.
    async fn ensure_parent(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(io_to_store_err)?;
        }
        Ok(())
    }
}

impl Default for EpisodicBackend {
    fn default() -> Self {
        // Defaulting to `~/.syncode/memory` panics if home is unresolvable;
        // callers that care should use [`new`] for explicit error handling.
        // Tests use [`with_root`] and never hit this path.
        Self::new().expect("episodic backend default requires a resolvable HOME")
    }
}

#[async_trait]
impl MemoryBackend for EpisodicBackend {
    fn name(&self) -> &'static str {
        "episodic-jsonl"
    }

    async fn store(&self, entry: &MemoryEntry) -> Result<()> {
        let path = self.file_path(entry.scope, &entry.user_id);
        self.ensure_parent(&path).await?;

        // Serialize once outside the lock — no IO under contention.
        let mut line = serde_json::to_string(entry).map_err(|e| {
            MemoryProviderError::Store(sqlx::Error::Configuration(e.to_string().into()))
        })?;
        line.push('\n');

        let lock = self.file_lock(path.clone()).await;
        let _guard = lock.lock().await;

        // Append mode guarantees atomic appends on POSIX and Windows for
        // writes under the pipe buffer (4 KB minimum). Our lines are tiny
        // relative to that, so each store is one atomic syscall.
        use tokio::io::AsyncWriteExt;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(io_to_store_err)?;
        file.write_all(line.as_bytes())
            .await
            .map_err(io_to_store_err)?;
        file.flush().await.map_err(io_to_store_err)?;
        drop(file);

        // Post-append size check. Stat is cheap and lets us warn once per
        // crossing rather than on every write.
        if let Ok(meta) = fs::metadata(&path).await
            && meta.len() >= ROTATION_WARN_BYTES
        {
            tracing::warn!(
                path = %path.display(),
                size_bytes = meta.len(),
                "episodic memory file exceeds 100 MB; consider rotating or compacting"
            );
        }
        Ok(())
    }

    async fn retrieve(
        &self,
        user_id: &str,
        _query: &str,
        k: usize,
        scope: Scope,
    ) -> Result<Vec<MemoryRecord>> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let path = self.file_path(scope, user_id);
        let lock = self.file_lock(path.clone()).await;
        let _guard = lock.lock().await;

        let bytes = match fs::read_to_string(&path).await {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No file yet for this (scope, user) pair — empty result.
                return Ok(Vec::new());
            }
            Err(e) => return Err(io_to_store_err(e)),
        };

        // Parse all lines, skipping malformed ones. The file is append-only,
        // so the LAST k parseable entries are the most recent.
        let mut entries: Vec<MemoryEntry> = Vec::new();
        for (idx, line) in bytes.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<MemoryEntry>(line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    // Skip-but-warn: a partial line (e.g. crash mid-append)
                    // must never break retrieval of the rest of the file.
                    tracing::warn!(
                        path = %path.display(),
                        line = idx,
                        error = %e,
                        "episodic memory: skipping malformed JSONL line"
                    );
                }
            }
        }

        // Take the tail (most recent k), reverse to chronological for scoring.
        let start = entries.len().saturating_sub(k);
        let recent = &entries[start..];
        let n = recent.len();
        if n == 0 {
            return Ok(Vec::new());
        }

        // Score: most recent → 1.0, oldest in window → ~0.5, linear decay.
        // n==1 short-circuits to 1.0 (no peers to decay against).
        let records = recent
            .iter()
            .rev()
            .enumerate()
            .map(|(i, e)| {
                let score = if n == 1 {
                    1.0
                } else {
                    // i in [0, n-1]; map to [1.0, 0.5].
                    1.0 - 0.5 * (i as f64) / ((n - 1) as f64)
                };
                MemoryRecord {
                    prompt: e.prompt.clone(),
                    response: e.response.clone(),
                    provider: e.provider.clone(),
                    tokens: e.tokens,
                    score,
                }
            })
            .collect();
        Ok(records)
    }
}

/// Replace path separators and `..` segments so `user_id` can't escape the
/// scope directory. `.` and `/` become `_`. Empty result becomes `anon`.
fn sanitize_user_id(user_id: &str) -> String {
    let cleaned: String = user_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "anon".to_string()
    } else {
        cleaned
    }
}

/// Map a [`std::io::Error`] to the existing [`MemoryProviderError::Store`]
/// variant so we don't need to extend the error enum (additive-only change).
fn io_to_store_err(e: std::io::Error) -> MemoryProviderError {
    MemoryProviderError::Store(sqlx::Error::Io(e))
}

/// Resolve the user's home directory without pulling in a `dirs` crate.
/// Reads `$HOME` on Unix and `%USERPROFILE%` on Windows; falls back to
/// `$HOMEPATH` if `USERPROFILE` is missing. Errors if none are set.
fn dirs_or_err() -> Result<PathBuf> {
    if let Some(home) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(home));
    }
    if let Some(profile) = std::env::var_os("USERPROFILE") {
        return Ok(PathBuf::from(profile));
    }
    if let Some(path) = std::env::var_os("HOMEPATH") {
        let mut buf = PathBuf::new();
        if let Some(drive) = std::env::var_os("HOMEDRIVE") {
            buf.push(drive);
        }
        buf.push(path);
        return Ok(buf);
    }
    Err(MemoryProviderError::Store(sqlx::Error::Configuration(
        "HOME / USERPROFILE / HOMEPATH not set; cannot resolve episodic memory root".into(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn entry(user: &str, prompt: &str, scope: Scope) -> MemoryEntry {
        MemoryEntry {
            user_id: user.into(),
            prompt: prompt.into(),
            response: format!("resp:{prompt}"),
            provider: "test".into(),
            tokens: 1,
            scope,
        }
    }

    #[tokio::test]
    async fn roundtrip_persists_and_retrieves_entry() {
        let tmp = TempDir::new().unwrap();
        let backend = EpisodicBackend::with_root(tmp.path());
        backend
            .store(&entry("alice", "first", Scope::User))
            .await
            .unwrap();

        let records = backend.retrieve("alice", "", 5, Scope::User).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].prompt, "first");
        assert_eq!(records[0].response, "resp:first");
        assert!((records[0].score - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn most_recent_k_returned_in_descending_score_order() {
        let tmp = TempDir::new().unwrap();
        let backend = EpisodicBackend::with_root(tmp.path());
        for i in 0..5 {
            backend
                .store(&entry("u", &format!("p{i}"), Scope::Project))
                .await
                .unwrap();
        }

        let records = backend.retrieve("u", "", 3, Scope::Project).await.unwrap();
        assert_eq!(records.len(), 3);
        // Most recent first: p4 (score 1.0), p3, p2 (lowest of the window).
        assert_eq!(records[0].prompt, "p4");
        assert_eq!(records[1].prompt, "p3");
        assert_eq!(records[2].prompt, "p2");
        // Score monotonic non-increasing.
        assert!(records[0].score >= records[1].score);
        assert!(records[1].score >= records[2].score);
    }

    #[tokio::test]
    async fn scopes_isolate_files() {
        let tmp = TempDir::new().unwrap();
        let backend = EpisodicBackend::with_root(tmp.path());
        backend
            .store(&entry("u", "session-only", Scope::Session))
            .await
            .unwrap();
        backend
            .store(&entry("u", "project-only", Scope::Project))
            .await
            .unwrap();

        let session = backend.retrieve("u", "", 5, Scope::Session).await.unwrap();
        let project = backend.retrieve("u", "", 5, Scope::Project).await.unwrap();
        assert_eq!(session.len(), 1);
        assert_eq!(session[0].prompt, "session-only");
        assert_eq!(project.len(), 1);
        assert_eq!(project[0].prompt, "project-only");

        // Distinct files on disk.
        let session_path = tmp.path().join("episodic/session/u.jsonl");
        let project_path = tmp.path().join("episodic/project/u.jsonl");
        assert!(session_path.exists());
        assert!(project_path.exists());
        assert_ne!(session_path, project_path);
    }

    #[tokio::test]
    async fn users_isolate_within_scope() {
        let tmp = TempDir::new().unwrap();
        let backend = EpisodicBackend::with_root(tmp.path());
        backend
            .store(&entry("alice", "a-prompt", Scope::User))
            .await
            .unwrap();
        backend
            .store(&entry("bob", "b-prompt", Scope::User))
            .await
            .unwrap();

        let alice = backend.retrieve("alice", "", 5, Scope::User).await.unwrap();
        assert_eq!(alice.len(), 1);
        assert_eq!(alice[0].prompt, "a-prompt");
        let bob = backend.retrieve("bob", "", 5, Scope::User).await.unwrap();
        assert_eq!(bob.len(), 1);
        assert_eq!(bob[0].prompt, "b-prompt");
    }

    #[tokio::test]
    async fn malformed_line_is_skipped_not_fatal() {
        let tmp = TempDir::new().unwrap();
        let backend = EpisodicBackend::with_root(tmp.path());
        // Write a valid line, then garbage, then another valid line.
        // The retrieve must surface the two valid entries and skip the bad.
        backend
            .store(&entry("u", "good-1", Scope::User))
            .await
            .unwrap();
        let path = tmp.path().join("episodic/user/u.jsonl");
        {
            use tokio::io::AsyncWriteExt;
            let mut f = fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .await
                .unwrap();
            f.write_all(b"this is not json\n").await.unwrap();
        }
        backend
            .store(&entry("u", "good-2", Scope::User))
            .await
            .unwrap();

        let records = backend.retrieve("u", "", 5, Scope::User).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].prompt, "good-2");
        assert_eq!(records[1].prompt, "good-1");
    }

    #[tokio::test]
    async fn retrieve_returns_empty_for_unknown_pair() {
        let tmp = TempDir::new().unwrap();
        let backend = EpisodicBackend::with_root(tmp.path());
        let records = backend
            .retrieve("never-seen", "", 5, Scope::User)
            .await
            .unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn retrieve_with_k_zero_returns_empty_without_io() {
        let tmp = TempDir::new().unwrap();
        let backend = EpisodicBackend::with_root(tmp.path());
        backend
            .store(&entry("u", "ignored", Scope::User))
            .await
            .unwrap();
        let records = backend.retrieve("u", "", 0, Scope::User).await.unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn sanitize_user_id_replaces_path_separators() {
        assert_eq!(sanitize_user_id("alice"), "alice");
        assert_eq!(sanitize_user_id("../etc/passwd"), "___etc_passwd");
        assert_eq!(sanitize_user_id("a:b@c!d"), "a_b_c_d");
        assert_eq!(sanitize_user_id(""), "anon");
        assert_eq!(sanitize_user_id("%%%"), "___");
    }

    #[tokio::test]
    async fn concurrent_writers_to_same_file_do_not_interleave() {
        let tmp = TempDir::new().unwrap();
        let backend = EpisodicBackend::with_root(tmp.path());

        // Spawn 10 concurrent stores; the file must end up with exactly 10
        // well-formed JSONL lines (no torn writes).
        let mut handles = Vec::new();
        for i in 0..10 {
            let be = backend.clone();
            handles.push(tokio::spawn(async move {
                be.store(&entry("shared", &format!("p{i}"), Scope::User))
                    .await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }

        let records = backend
            .retrieve("shared", "", 100, Scope::User)
            .await
            .unwrap();
        assert_eq!(records.len(), 10, "expected 10 well-formed entries");
    }
}

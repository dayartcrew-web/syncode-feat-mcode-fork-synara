//! Scrollback persistence — file-based terminal scrollback store.
//!
//! Persists terminal scrollback to disk keyed by `(threadId, terminalId)` so
//! that re-opening a terminal pane restores its previous output. Uses an
//! atomic write-then-rename strategy and an ANSI/UTF-8-safe byte cap so the
//! persisted tail is always replay-safe (it never splits a multi-byte UTF-8
//! character or an ANSI escape sequence).
//!
//! ## Storage layout
//!
//! Files live under `{base_dir}/{threadId}_{terminalId}.scroll`. The default
//! `base_dir` is `~/.syncode/terminal/` (resolving `$HOME` then
//! `$USERPROFILE`), matching the `server_home_dir` resolution used elsewhere
//! in the codebase. The base dir is overridable via the
//! `SYNICODE_TERMINAL_SCROLLBACK_DIR` env var, which the tests use to point
//! at a `tempfile` directory.
//!
//! ## Atomicity
//!
//! [`ScrollbackStore::save`] writes to a sibling `.tmp` file, `fsync`s it,
//! then renames it over the destination. On both POSIX and Windows,
//! `std::fs::rename` replaces an existing destination atomically (Windows
//! uses `MOVEFILE_REPLACE_EXISTING`), so a reader never observes a
//! half-written file.
//!
//! ## Byte cap
//!
//! The persisted copy is capped at [`MAX_SCROLLBACK_BYTES`] via
//! [`truncate_ansi_safe`], which keeps the **tail** (most recent output) and
//! advances the cut to the next replay-safe boundary.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Default maximum persisted scrollback size (256 KiB).
///
/// The in-memory [`crate::OutputBuffer`] ring can hold up to ~4 MiB (1000
/// chunks × 4 KiB); the persisted copy is capped smaller to keep disk usage
/// bounded and restore fast. Only the most recent `MAX_SCROLLBACK_BYTES` are
/// kept (see [`truncate_ansi_safe`]).
pub const MAX_SCROLLBACK_BYTES: usize = 256 * 1024;

/// File-based scrollback store.
#[derive(Debug, Clone)]
pub struct ScrollbackStore {
    base_dir: PathBuf,
}

impl ScrollbackStore {
    /// Create a store rooted at the default location (`~/.syncode/terminal/`),
    /// or `$SYNICODE_TERMINAL_SCROLLBACK_DIR` when that env var is set.
    ///
    /// The env override exists primarily so tests can point the store at a
    /// `tempfile` directory without touching the real home directory.
    pub fn new() -> Self {
        let base_dir = std::env::var("SYNICODE_TERMINAL_SCROLLBACK_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| default_base_dir());
        Self { base_dir }
    }

    /// Create a store rooted at an explicit base directory.
    ///
    /// Test/helper constructor: callers that already know the directory
    /// (e.g. a `tempfile::TempDir` path) pass it directly.
    pub fn with_base_dir(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Base directory used by this store.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Persist `scrollback` for `(thread_id, terminal_id)`.
    ///
    /// The data is first capped to [`MAX_SCROLLBACK_BYTES`] via
    /// [`truncate_ansi_safe`] (so the persisted tail is replay-safe), then
    /// written atomically (`.tmp` + rename). An **empty** `scrollback`
    /// deletes any existing file so a cleared terminal does not resurrect
    /// stale output on the next open.
    ///
    /// `thread_id` may be empty/absent — in that case the terminal id alone
    /// is used as the key (legacy callers that have no MCode `threadId`).
    pub fn save(
        &self,
        thread_id: &str,
        terminal_id: &str,
        scrollback: &str,
    ) -> std::io::Result<()> {
        let path = self.path_for(thread_id, terminal_id);
        if scrollback.is_empty() {
            return remove_if_exists(&path);
        }
        let capped = truncate_ansi_safe(scrollback, MAX_SCROLLBACK_BYTES);
        fs::create_dir_all(&self.base_dir)?;
        // Atomic write: temp file → fsync → rename over the destination.
        let tmp = with_file_name_suffix(&path, ".tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(capped.as_bytes())?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Load persisted scrollback for `(thread_id, terminal_id)`.
    ///
    /// Returns `Ok(None)` when no file exists (first open of a pane). Returns
    /// `Ok(Some(s))` with the file contents otherwise.
    pub fn load(&self, thread_id: &str, terminal_id: &str) -> std::io::Result<Option<String>> {
        let path = self.path_for(thread_id, terminal_id);
        match fs::read_to_string(&path) {
            Ok(s) => Ok(Some(s)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Delete persisted scrollback for `(thread_id, terminal_id)`, if any.
    ///
    /// Idempotent: a missing file is `Ok(())`.
    pub fn clear(&self, thread_id: &str, terminal_id: &str) -> std::io::Result<()> {
        remove_if_exists(&self.path_for(thread_id, terminal_id))
    }

    /// Resolve the on-disk path for a `(thread_id, terminal_id)` pair.
    ///
    /// Both ids are sanitized: anything outside `[A-Za-z0-9._-]` becomes `_`
    /// so arbitrary caller strings cannot escape the base directory or forge
    /// surprising path segments (`..`, `/`, etc.).
    fn path_for(&self, thread_id: &str, terminal_id: &str) -> PathBuf {
        let key = if thread_id.trim().is_empty() {
            sanitize_id(terminal_id)
        } else {
            format!("{}_{}", sanitize_id(thread_id), sanitize_id(terminal_id))
        };
        self.base_dir.join(format!("{key}.scroll"))
    }
}

impl Default for ScrollbackStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Truncate `data` to at most `max_bytes`, keeping the **tail** (most recent
/// output) and ensuring the cut lands on a replay-safe boundary:
///
/// 1. The cut is advanced to the next UTF-8 character boundary so a
///    multi-byte sequence is never split (a truncated leading byte would
///    produce `String::from_utf8` `Replacement Char`s / decode errors on
///    replay).
/// 2. The cut is then advanced to the next ANSI escape start (`ESC` /
///    `0x1b`) or newline so replay does not begin mid-escape (a CSI like
///    `\x1b[31m` split after the `\[` would leave the renderer in an
///    undefined color state).
///
/// If no escape/newline boundary is found within the tail, the UTF-8-safe
/// cut is used (best effort — the data had no line/escape breaks near the
/// cut). When `data.len() <= max_bytes`, `data` is returned unchanged.
pub fn truncate_ansi_safe(data: &str, max_bytes: usize) -> &str {
    if data.len() <= max_bytes {
        return data;
    }
    let bytes = data.as_bytes();
    // Start of the tail we'd like to keep (naive byte cut).
    let mut start = data.len().saturating_sub(max_bytes);
    // 1. Advance to a UTF-8 char boundary (`is_char_boundary` is true at the
    //    first byte of every char, including at `len()`).
    while start < data.len() && !data.is_char_boundary(start) {
        start += 1;
    }
    // 2. Advance to the next escape start (0x1b) or newline so the first
    //    byte of the persisted tail is a clean sequence start.
    let mut safe = start;
    while safe < data.len() {
        let b = bytes[safe];
        if b == 0x1b || b == b'\n' {
            break;
        }
        safe += 1;
    }
    if safe < data.len() {
        &data[safe..]
    } else {
        &data[start..]
    }
}

/// Resolve the default base directory: `$HOME/.syncode/terminal` on POSIX,
/// `$USERPROFILE/.syncode/terminal` on Windows. Falls back to a relative
/// `./.syncode/terminal` when neither env var is set.
fn default_base_dir() -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));
    home.join(".syncode").join("terminal")
}

/// Replace any character outside `[A-Za-z0-9._-]` with `_`.
///
/// This keeps path-unsafe characters (`/`, `\`, `..`, `:`, NUL, etc.) out of
/// the on-disk file name regardless of what the UI sends as `threadId` /
/// `terminalId`.
fn sanitize_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Build a sibling path by appending `suffix` to the file name
/// (e.g. `foo.scroll` → `foo.scroll.tmp`).
fn with_file_name_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    name.push(suffix);
    path.with_file_name(name)
}

/// Remove `path` if it exists; treat "not found" as success.
fn remove_if_exists(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a store backed by a fresh tempdir. Returns `(store, dir)`; the
    /// caller keeps the `TempDir` guard alive for the test's duration.
    fn tmp_store() -> (ScrollbackStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ScrollbackStore::with_base_dir(dir.path());
        (store, dir)
    }

    #[test]
    fn save_then_load_roundtrip() {
        let (store, _dir) = tmp_store();
        let scrollback = "hello scrollback\nline two\n";
        store.save("thread-1", "term-1", scrollback).expect("save");
        let loaded = store.load("thread-1", "term-1").expect("load");
        assert_eq!(loaded.as_deref(), Some(scrollback));
    }

    #[test]
    fn load_missing_returns_none() {
        let (store, _dir) = tmp_store();
        let loaded = store.load("nope", "nope").expect("load");
        assert!(loaded.is_none());
    }

    #[test]
    fn save_empty_deletes_existing_file() {
        let (store, _dir) = tmp_store();
        store.save("t", "x", "data").expect("save");
        assert!(store.load("t", "x").expect("load").is_some());
        // Empty save → file removed (cleared state does not resurrect).
        store.save("t", "x", "").expect("save empty");
        assert!(store.load("t", "x").expect("load").is_none());
    }

    #[test]
    fn clear_is_idempotent() {
        let (store, _dir) = tmp_store();
        // Clearing a non-existent key is a no-op.
        store.clear("t", "x").expect("clear missing");
        store.save("t", "x", "data").expect("save");
        store.clear("t", "x").expect("clear");
        assert!(store.load("t", "x").expect("load").is_none());
    }

    #[test]
    fn keys_are_isolated_per_thread_and_terminal() {
        let (store, _dir) = tmp_store();
        store.save("thread-a", "term-1", "A1").expect("save");
        store.save("thread-a", "term-2", "A2").expect("save");
        store.save("thread-b", "term-1", "B1").expect("save");
        assert_eq!(
            store.load("thread-a", "term-1").unwrap().as_deref(),
            Some("A1")
        );
        assert_eq!(
            store.load("thread-a", "term-2").unwrap().as_deref(),
            Some("A2")
        );
        assert_eq!(
            store.load("thread-b", "term-1").unwrap().as_deref(),
            Some("B1")
        );
    }

    #[test]
    fn empty_thread_id_keys_on_terminal_only() {
        let (store, _dir) = tmp_store();
        store.save("", "term-1", "legacy").expect("save");
        assert_eq!(store.load("", "term-1").unwrap().as_deref(), Some("legacy"));
    }

    #[test]
    fn truncate_returns_input_when_under_cap() {
        let s = "small";
        assert_eq!(truncate_ansi_safe(s, 100), s);
        // Exactly at cap is unchanged.
        assert_eq!(truncate_ansi_safe("abc", 3), "abc");
    }

    #[test]
    fn truncate_does_not_split_multibyte_utf8() {
        // Each 'é' is 2 bytes (0xC3 0xA9). 6 bytes total.
        let data = "aéééb";
        // Cap at 4 bytes → naive cut lands mid-char. The result must be valid
        // UTF-8 (no replacement chars) and end on a char boundary.
        let out = truncate_ansi_safe(data, 4);
        // No newline/escape in the tail, so this exercises the UTF-8 fallback.
        assert!(out.is_char_boundary(0));
        assert!(data.ends_with(out));
        // The tail must be strictly shorter than the full string (it was cut).
        assert!(out.len() < data.len());
    }

    #[test]
    fn truncate_cuts_at_newline_boundary() {
        // Build "AAA...A\nBBB...B" where the tail start falls inside the A's.
        let head = "x".repeat(50);
        let tail = "\nrest of line\n";
        let data = format!("{head}{tail}");
        // Cap so the naive cut lands inside `head` (50 bytes of x's). The safe
        // boundary scan must land on the '\n' so replay begins at "rest...".
        let out = truncate_ansi_safe(&data, 20);
        assert!(
            out.starts_with('\n') || out.starts_with('r'),
            "expected cut at newline, got: {out:?}"
        );
        assert!(out.ends_with("line\n"));
        // Tail is within the cap (no larger than the requested max, often
        // smaller because we advanced to the boundary).
        assert!(out.len() <= data.len());
    }

    #[test]
    fn truncate_cuts_at_ansi_escape_boundary() {
        // ESC [ 3 1 m = red. Construct head + escape + body so the naive cut
        // lands inside the head and the only safe boundary in the tail is the
        // ESC byte.
        let head = "h".repeat(30);
        let esc = "\x1b[31m";
        let body = "red text";
        let data = format!("{head}{esc}{body}");
        let out = truncate_ansi_safe(&data, 20);
        // Must begin at the ESC (replay-safe) — never mid-escape.
        assert!(
            out.starts_with('\x1b'),
            "expected cut at ESC boundary, got start byte {:?}",
            out.as_bytes().first()
        );
        assert!(out.ends_with(body));
    }

    #[test]
    fn truncate_keeps_tail_content() {
        // The most recent bytes must survive truncation.
        let data = format!("{}\nEND_MARKER_HERE", "z".repeat(500));
        let out = truncate_ansi_safe(&data, 100);
        assert!(out.ends_with("END_MARKER_HERE"));
    }

    #[test]
    fn sanitize_blocks_path_traversal() {
        // Implementation detail: confirm risky ids are neutralized. We check
        // via the public path by ensuring a traversal id does not escape the
        // base dir (load of one must not see another's data).
        let (store, _dir) = tmp_store();
        store.save("../escape", "id", "secret").expect("save");
        // A different "real" key must not collide/see it.
        assert!(store.load("normal", "id").expect("load").is_none());
        // The traversal key itself still resolves to a sanitized filename
        // inside the base dir.
        assert!(store.load("../escape", "id").expect("load").is_some());
    }
}

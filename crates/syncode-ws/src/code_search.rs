//! Built-in content search backed by BurntSushi/ripgrep library crates.
//!
//! Exposes a single async entry point, [`search_code`], that the
//! `tool/search-code` JSON-RPC handler (`rpc.rs`) calls. Uses:
//!
//! - `grep_regex::RegexMatcher` — default regex-based `Matcher` impl
//! - `grep_searcher::Searcher` — file walker + matcher sink
//! - `ignore::WalkBuilder` — `.gitignore`/`.ignore`-aware directory traversal
//! - `globset::GlobSet` — optional `file_glob` filter
//!
//! CPU work runs on `tokio::task::spawn_blocking` so the async runtime is
//! never blocked. v1 is **syncode-side only** — not forwarded to ACP
//! providers (cursor/grok/gemini have their own grep).

use std::path::PathBuf;
use std::sync::Mutex;

use globset::{Glob, GlobSetBuilder};
use grep_matcher::Matcher;
use grep_regex::RegexMatcherBuilder;
use grep_searcher::{Searcher, Sink, SinkMatch};
use ignore::WalkBuilder;
use serde::Serialize;
use thiserror::Error;
use tracing::warn;

/// Hard ceiling on hits returned in one call regardless of caller-supplied
/// `limit`. Prevents pathological queries (e.g. single-char regex over a huge
/// monorepo) from blowing through memory budgets.
const DEFAULT_LIMIT: usize = 200;

/// Maximum hit count the API will accept from `limit`. Larger values are
/// clamped silently.
const MAX_LIMIT: usize = 1_000;

/// Error returned by [`search_code`].
#[derive(Debug, Error)]
pub enum CodeSearchError {
    /// Caller did not supply `cwd` (or supplied an empty string).
    #[error("cwd is required")]
    MissingCwd,
    /// Caller did not supply `query` (or supplied an empty string).
    #[error("query is required")]
    MissingQuery,
    /// The supplied pattern is not a valid regex (only when `regex: true`).
    #[error("invalid regex: {0}")]
    InvalidRegex(String),
    /// A `file_glob` pattern could not be compiled into a `Glob`.
    #[error("invalid file_glob: {0}")]
    InvalidFileGlob(String),
    /// The supplied `cwd` does not exist or is not a directory.
    #[error("invalid cwd: {0}")]
    InvalidCwd(String),
    /// An OS-level I/O error during traversal or file reads.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Internal matcher error surfaced by `grep-matcher`/`grep-regex`.
    #[error("matcher error: {0}")]
    Matcher(String),
}

type Result<T> = std::result::Result<T, CodeSearchError>;

/// A single match record returned by [`search_code`].
///
/// `matched_text` is the full line content (trimmed of the trailing newline),
/// not just the captured group — consumers render the whole line and use
/// `column` to highlight the start of the match.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SearchHit {
    /// Workspace-relative path using `/` separators (cross-platform stable).
    pub path: String,
    /// 1-based line number where the match starts.
    pub line: u32,
    /// 1-based column (byte offset within the line, in UTF-8 bytes) where the
    /// match starts.
    pub column: u32,
    /// Full text of the matched line (no trailing newline).
    pub matched_text: String,
}

/// Aggregated result of a [`search_code`] call.
#[derive(Debug, Clone, Serialize)]
pub struct SearchOutput {
    /// All matches collected, up to the effective limit.
    pub hits: Vec<SearchHit>,
    /// `true` when the search stopped early because the limit was reached.
    /// More matches may exist beyond the returned page.
    pub truncated: bool,
    /// Echo back the caller's query for client-side correlation.
    pub query: String,
}

/// Borrowed, validated inputs to [`search_code`].
#[derive(Debug, Clone, Copy)]
pub struct SearchInput<'a> {
    /// Absolute path to the project root to search within. Required.
    pub cwd: &'a str,
    /// Pattern to search for. Required.
    pub query: &'a str,
    /// Cap on the number of hits returned. `None` → `DEFAULT_LIMIT`. Clamped
    /// to `MAX_LIMIT` if larger.
    pub limit: Option<usize>,
    /// Optional glob (e.g. `"*.rs"`, `"src/**/*.ts"`) restricting which files
    /// are searched.
    pub file_glob: Option<&'a str>,
    /// Match-insensitive when `true`.
    pub case_insensitive: bool,
    /// Treat `query` as a regex when `true`; as a literal substring otherwise.
    pub regex: bool,
}

/// Run a content search.
///
/// CPU-bound work runs on `spawn_blocking` so callers can `.await` from an
/// async RPC handler without blocking the runtime. The matcher and glob
/// compilation happens on the calling task (cheap, fail-fast) so callers
/// receive [`CodeSearchError::InvalidRegex`] / [`CodeSearchError::InvalidFileGlob`]
/// before the blocking trip.
pub async fn search_code(input: SearchInput<'_>) -> Result<SearchOutput> {
    if input.cwd.is_empty() {
        return Err(CodeSearchError::MissingCwd);
    }
    if input.query.is_empty() {
        return Err(CodeSearchError::MissingQuery);
    }

    let cwd = PathBuf::from(input.cwd);
    if !cwd.is_dir() {
        return Err(CodeSearchError::InvalidCwd(input.cwd.to_string()));
    }

    let matcher = build_matcher(input)?;
    let glob_set = build_glob_set(input.file_glob)?;

    let limit = input.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);

    let query_string = input.query.to_string();
    let cwd_for_blocking = cwd.clone();
    // Drive the search from spawn_blocking: the ripgrep crates are
    // fundamentally synchronous (CPU + blocking I/O).
    let outcome = tokio::task::spawn_blocking(move || {
        run_search_blocking(&cwd_for_blocking, &matcher, glob_set.as_ref(), limit)
    })
    .await
    .map_err(|e| CodeSearchError::Matcher(format!("search task panicked: {e}")))??;

    Ok(SearchOutput {
        hits: outcome.hits,
        truncated: outcome.truncated,
        query: query_string,
    })
}

// ─── internals ──────────────────────────────────────────────────────────────

type RegexMatcher = grep_regex::RegexMatcher;

fn build_matcher(input: SearchInput<'_>) -> Result<RegexMatcher> {
    let mut builder = RegexMatcherBuilder::new();
    builder.case_insensitive(input.case_insensitive);
    builder.line_terminator(Some(b'\n'));

    let pattern = if input.regex {
        input.query.to_string()
    } else {
        // Escape regex metacharacters so a literal query is matched verbatim.
        regex_syntax::escape(input.query)
    };
    builder
        .build(&pattern)
        .map_err(|e| CodeSearchError::InvalidRegex(e.to_string()))
}

fn build_glob_set(file_glob: Option<&str>) -> Result<Option<globset::GlobSet>> {
    let Some(pattern) = file_glob else {
        return Ok(None);
    };
    if pattern.is_empty() {
        return Ok(None);
    }
    let glob = Glob::new(pattern).map_err(|e| CodeSearchError::InvalidFileGlob(e.to_string()))?;
    let set = GlobSetBuilder::new()
        .add(glob)
        .build()
        .map_err(|e| CodeSearchError::InvalidFileGlob(e.to_string()))?;
    Ok(Some(set))
}

/// Buffer holding hits collected by the [`CollectingSink`] plus a truncation
/// flag. Shared via `Arc<Mutex<…>>` between the sink and the post-search read.
struct HitBuffer {
    hits: Vec<SearchHit>,
    truncated: bool,
    limit: usize,
}

impl HitBuffer {
    fn new(limit: usize) -> std::sync::Arc<Mutex<Self>> {
        std::sync::Arc::new(Mutex::new(Self {
            hits: Vec::with_capacity(limit.min(64)),
            truncated: false,
            limit,
        }))
    }
}

struct CollectingSink<'a> {
    buffer: std::sync::Arc<Mutex<HitBuffer>>,
    relative_path: String,
    matcher: &'a RegexMatcher,
}

impl<'a> CollectingSink<'a> {
    fn new(
        buffer: std::sync::Arc<Mutex<HitBuffer>>,
        relative_path: String,
        matcher: &'a RegexMatcher,
    ) -> Self {
        Self {
            buffer,
            relative_path,
            matcher,
        }
    }
}

impl<'a> Sink for CollectingSink<'a> {
    type Error = std::io::Error;

    fn matched(
        &mut self,
        _searcher: &Searcher,
        mat: &SinkMatch<'_>,
    ) -> std::result::Result<bool, Self::Error> {
        let Some(line_number) = mat.line_number() else {
            return Ok(true);
        };
        if line_number == 0 {
            return Ok(true);
        }

        let mut buf = self.buffer.lock().expect("hit buffer poisoned");
        if buf.hits.len() >= buf.limit {
            buf.truncated = true;
            return Ok(false); // stop search
        }

        // Full line bytes (may include trailing newline).
        let line_bytes = mat.bytes();
        let matched_text_lossy = String::from_utf8_lossy(line_bytes);
        let matched_text = matched_text_lossy
            .strip_suffix('\n')
            .or_else(|| matched_text_lossy.strip_suffix('\r'))
            .unwrap_or(&matched_text_lossy)
            .to_string();

        // Find the actual match offset within the line. `SinkMatch::bytes()`
        // is the full line content, not just the match — use the matcher to
        // locate the first match within those bytes. Fall back to column 1 if
        // the matcher refuses (binary edge cases).
        let column = self
            .matcher
            .find(line_bytes.as_ref())
            .ok()
            .flatten()
            .map(|m| m.start() as u32 + 1)
            .unwrap_or(1);

        buf.hits.push(SearchHit {
            path: self.relative_path.clone(),
            line: line_number as u32,
            column,
            matched_text,
        });

        Ok(true)
    }
}

fn run_search_blocking(
    cwd: &PathBuf,
    matcher: &RegexMatcher,
    glob_set: Option<&globset::GlobSet>,
    limit: usize,
) -> Result<SearchOutcome> {
    let buffer = HitBuffer::new(limit);

    let walker = WalkBuilder::new(cwd)
        .hidden(true)
        .git_ignore(true)
        .git_exclude(true)
        .git_global(true)
        .build();

    for dent in walker {
        // Re-check the limit up-front so we exit the walk early once full.
        {
            let buf = buffer.lock().expect("hit buffer poisoned");
            if buf.hits.len() >= buf.limit {
                drop(buf);
                let mut buf = buffer.lock().expect("hit buffer poisoned");
                buf.truncated = true;
                break;
            }
        }

        let dent = match dent {
            Ok(d) => d,
            Err(e) => {
                warn!(error = %e, "code_search: skipping unreadable entry");
                continue;
            }
        };
        if !dent.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }

        let path = dent.path();
        let relative = match path.strip_prefix(cwd) {
            Ok(rel) => rel.to_string_lossy().replace('\\', "/"),
            Err(_) => path.to_string_lossy().replace('\\', "/"),
        };

        // Apply glob filter (matched against the workspace-relative path
        // with forward-slash separators, matching globset's convention).
        if let Some(set) = glob_set
            && !set.is_match(&relative)
            && !set.is_match(path)
        {
            continue;
        }

        let mut sink = CollectingSink::new(buffer.clone(), relative, matcher);
        let mut searcher = Searcher::new();
        if let Err(e) = searcher.search_path(matcher, path, &mut sink) {
            // Hard errors (binary files, broken symlinks) should not abort
            // the whole walk — log and continue.
            warn!(error = %e, path = ?path, "code_search: skipping file");
        }
    }

    let mut buf = buffer.lock().expect("hit buffer poisoned");
    let hits = std::mem::take(&mut buf.hits);
    let truncated = buf.truncated;
    drop(buf);

    Ok(SearchOutcome { hits, truncated })
}

struct SearchOutcome {
    hits: Vec<SearchHit>,
    truncated: bool,
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    fn tmp_dir(label: &str) -> PathBuf {
        let mut base = std::env::temp_dir();
        base.push(format!(
            "syncode-code-search-{}-{}",
            label,
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&base).expect("create tmp dir");
        base
    }

    fn write_file(root: &Path, rel: &str, content: &str) {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        fs::write(path, content).expect("write file");
    }

    #[tokio::test]
    async fn search_returns_hits_in_seeded_tree() {
        let dir = tmp_dir("hits");
        write_file(&dir, "src/lib.rs", "fn alpha() {}\nfn beta() {}\n");
        write_file(&dir, "src/main.rs", "mod lib;\nfn main() { alpha(); }\n");

        let out = search_code(SearchInput {
            cwd: dir.to_str().unwrap(),
            query: "alpha",
            limit: None,
            file_glob: None,
            case_insensitive: false,
            regex: false,
        })
        .await
        .expect("search ok");

        assert!(!out.truncated, "should not be truncated");
        // alpha appears in both files
        assert_eq!(out.hits.len(), 2, "expected 2 hits, got {:?}", out.hits);
        for hit in &out.hits {
            assert!(hit.matched_text.contains("alpha"));
            assert!(hit.path.starts_with("src/"));
            assert!(hit.line >= 1);
            assert!(hit.column >= 1);
        }
    }

    #[tokio::test]
    async fn search_respects_case_insensitive_flag() {
        let dir = tmp_dir("case");
        write_file(&dir, "a.txt", "Hello\nHELLO\nhello\n");

        let exact = search_code(SearchInput {
            cwd: dir.to_str().unwrap(),
            query: "Hello",
            limit: None,
            file_glob: None,
            case_insensitive: false,
            regex: false,
        })
        .await
        .expect("exact search ok");
        assert_eq!(exact.hits.len(), 1, "exact: {:?}", exact.hits);

        let ci = search_code(SearchInput {
            cwd: dir.to_str().unwrap(),
            query: "Hello",
            limit: None,
            file_glob: None,
            case_insensitive: true,
            regex: false,
        })
        .await
        .expect("ci search ok");
        assert_eq!(ci.hits.len(), 3, "ci: {:?}", ci.hits);
    }

    #[tokio::test]
    async fn search_supports_regex_pattern() {
        let dir = tmp_dir("regex");
        write_file(&dir, "r.txt", "foo123bar\nfoo456bar\nbaz000\n");

        let out = search_code(SearchInput {
            cwd: dir.to_str().unwrap(),
            query: r"foo\d+bar",
            limit: None,
            file_glob: None,
            case_insensitive: false,
            regex: true,
        })
        .await
        .expect("regex search ok");
        assert_eq!(out.hits.len(), 2, "regex hits: {:?}", out.hits);
    }

    #[tokio::test]
    async fn search_truncates_at_limit() {
        let dir = tmp_dir("limit");
        let mut content = String::new();
        for i in 0..50 {
            content.push_str(&format!("line {i} has match\n"));
        }
        write_file(&dir, "big.txt", &content);

        let out = search_code(SearchInput {
            cwd: dir.to_str().unwrap(),
            query: "match",
            limit: Some(5),
            file_glob: None,
            case_insensitive: false,
            regex: false,
        })
        .await
        .expect("limit search ok");
        assert!(out.truncated, "should be truncated");
        assert_eq!(out.hits.len(), 5, "got: {}", out.hits.len());
    }

    #[tokio::test]
    async fn search_returns_empty_for_no_match() {
        let dir = tmp_dir("empty");
        write_file(&dir, "a.txt", "alpha\nbeta\ngamma\n");

        let out = search_code(SearchInput {
            cwd: dir.to_str().unwrap(),
            query: "zzz-no-such-token",
            limit: None,
            file_glob: None,
            case_insensitive: false,
            regex: false,
        })
        .await
        .expect("empty search ok");
        assert!(out.hits.is_empty());
        assert!(!out.truncated);
    }

    #[tokio::test]
    async fn search_rejects_invalid_regex() {
        let dir = tmp_dir("badregex");
        write_file(&dir, "a.txt", "x\n");

        let err = search_code(SearchInput {
            cwd: dir.to_str().unwrap(),
            query: r"[\d",
            limit: None,
            file_glob: None,
            case_insensitive: false,
            regex: true,
        })
        .await
        .expect_err("expected error");
        assert!(matches!(err, CodeSearchError::InvalidRegex(_)));
    }

    #[tokio::test]
    async fn search_filters_by_file_glob() {
        let dir = tmp_dir("glob");
        write_file(&dir, "keep.rs", "match here\n");
        write_file(&dir, "skip.txt", "match here too\n");

        let out = search_code(SearchInput {
            cwd: dir.to_str().unwrap(),
            query: "match",
            limit: None,
            file_glob: Some("*.rs"),
            case_insensitive: false,
            regex: false,
        })
        .await
        .expect("glob search ok");
        assert_eq!(out.hits.len(), 1, "got: {:?}", out.hits);
        assert!(out.hits[0].path.ends_with(".rs"));
    }

    #[tokio::test]
    async fn search_rejects_missing_cwd() {
        let err = search_code(SearchInput {
            cwd: "",
            query: "x",
            limit: None,
            file_glob: None,
            case_insensitive: false,
            regex: false,
        })
        .await
        .expect_err("expected MissingCwd");
        assert!(matches!(err, CodeSearchError::MissingCwd));
    }

    #[tokio::test]
    async fn search_rejects_missing_query() {
        let dir = tmp_dir("noquery");
        let err = search_code(SearchInput {
            cwd: dir.to_str().unwrap(),
            query: "",
            limit: None,
            file_glob: None,
            case_insensitive: false,
            regex: false,
        })
        .await
        .expect_err("expected MissingQuery");
        assert!(matches!(err, CodeSearchError::MissingQuery));
    }

    #[tokio::test]
    async fn search_rejects_invalid_cwd() {
        let err = search_code(SearchInput {
            cwd: "/__syncode_definitely_does_not_exist__",
            query: "x",
            limit: None,
            file_glob: None,
            case_insensitive: false,
            regex: false,
        })
        .await
        .expect_err("expected InvalidCwd");
        assert!(matches!(err, CodeSearchError::InvalidCwd(_)));
    }

    #[tokio::test]
    async fn search_column_points_at_first_byte_of_match() {
        let dir = tmp_dir("col");
        write_file(&dir, "a.txt", "    alpha\n");

        let out = search_code(SearchInput {
            cwd: dir.to_str().unwrap(),
            query: "alpha",
            limit: None,
            file_glob: None,
            case_insensitive: false,
            regex: false,
        })
        .await
        .expect("col search ok");
        assert_eq!(out.hits.len(), 1);
        let hit = &out.hits[0];
        // 4 leading spaces → column 5.
        assert_eq!(hit.column, 5, "got column {}", hit.column);
    }
}

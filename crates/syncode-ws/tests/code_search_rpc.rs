//! Integration tests for the `tool/search-code` RPC pipeline.
//!
//! Unit tests inside `code_search.rs` cover the library API at the
//! matcher/sink level. These integration tests exercise the cross-module
//! boundary — the same path the RPC handler in `rpc.rs` invokes — against
//! realistic project trees (multi-file, nested, mixed extensions, .gitignore
//! interaction). They mirror the pattern in `tests/mcp_rpc.rs`: target the
//! library API directly, no server boot required.

use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use syncode_ws::code_search::{self, SearchInput};

fn tmp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "syncode-code-search-it-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_file(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, content).expect("write file");
}

#[tokio::test]
async fn integration_returns_hits_in_seeded_tree() {
    let dir = tmp_dir("hits");
    write_file(&dir, "src/lib.rs", "fn alpha() {}\nfn beta() {}\n");
    write_file(&dir, "src/main.rs", "mod lib;\nfn main() { alpha(); }\n");

    let out = code_search::search_code(SearchInput {
        cwd: dir.to_str().unwrap(),
        query: "alpha",
        limit: None,
        file_glob: None,
        case_insensitive: false,
        regex: false,
    })
    .await
    .expect("search ok");

    assert!(!out.truncated);
    assert_eq!(out.hits.len(), 2, "got: {:?}", out.hits);
    for hit in &out.hits {
        assert!(hit.matched_text.contains("alpha"));
        assert!(hit.path.starts_with("src/"));
    }
}

#[tokio::test]
async fn integration_respects_case_insensitive_flag() {
    let dir = tmp_dir("case");
    write_file(&dir, "a.txt", "Hello\nHELLO\nhello\n");

    let exact = code_search::search_code(SearchInput {
        cwd: dir.to_str().unwrap(),
        query: "Hello",
        limit: None,
        file_glob: None,
        case_insensitive: false,
        regex: false,
    })
    .await
    .expect("exact");
    assert_eq!(exact.hits.len(), 1);

    let ci = code_search::search_code(SearchInput {
        cwd: dir.to_str().unwrap(),
        query: "Hello",
        limit: None,
        file_glob: None,
        case_insensitive: true,
        regex: false,
    })
    .await
    .expect("ci");
    assert_eq!(ci.hits.len(), 3);
}

#[tokio::test]
async fn integration_supports_regex_pattern() {
    let dir = tmp_dir("regex");
    write_file(&dir, "r.txt", "foo123bar\nfoo456bar\nbaz000\n");

    let out = code_search::search_code(SearchInput {
        cwd: dir.to_str().unwrap(),
        query: r"foo\d+bar",
        limit: None,
        file_glob: None,
        case_insensitive: false,
        regex: true,
    })
    .await
    .expect("regex");
    assert_eq!(out.hits.len(), 2);
}

#[tokio::test]
async fn integration_truncates_at_limit() {
    let dir = tmp_dir("limit");
    let mut content = String::new();
    for i in 0..50 {
        content.push_str(&format!("line {i} has match\n"));
    }
    write_file(&dir, "big.txt", &content);

    let out = code_search::search_code(SearchInput {
        cwd: dir.to_str().unwrap(),
        query: "match",
        limit: Some(5),
        file_glob: None,
        case_insensitive: false,
        regex: false,
    })
    .await
    .expect("limit");
    assert!(out.truncated);
    assert_eq!(out.hits.len(), 5);
}

#[tokio::test]
async fn integration_returns_empty_for_no_match() {
    let dir = tmp_dir("empty");
    write_file(&dir, "a.txt", "alpha\nbeta\ngamma\n");

    let out = code_search::search_code(SearchInput {
        cwd: dir.to_str().unwrap(),
        query: "zzz-no-such-token",
        limit: None,
        file_glob: None,
        case_insensitive: false,
        regex: false,
    })
    .await
    .expect("empty");
    assert!(out.hits.is_empty());
    assert!(!out.truncated);
}

#[tokio::test]
async fn integration_rejects_invalid_regex() {
    let dir = tmp_dir("badregex");
    write_file(&dir, "a.txt", "x\n");

    let err = code_search::search_code(SearchInput {
        cwd: dir.to_str().unwrap(),
        query: r"[\d",
        limit: None,
        file_glob: None,
        case_insensitive: false,
        regex: true,
    })
    .await
    .expect_err("expected error");
    assert!(matches!(err, code_search::CodeSearchError::InvalidRegex(_)));
}

#[tokio::test]
async fn integration_filters_by_file_glob() {
    let dir = tmp_dir("glob");
    write_file(&dir, "keep.rs", "match here\n");
    write_file(&dir, "skip.txt", "match here too\n");
    write_file(&dir, "src/nested.rs", "match nested\n");

    let out = code_search::search_code(SearchInput {
        cwd: dir.to_str().unwrap(),
        query: "match",
        limit: None,
        file_glob: Some("**/*.rs"),
        case_insensitive: false,
        regex: false,
    })
    .await
    .expect("glob");
    let paths: Vec<&str> = out.hits.iter().map(|h| h.path.as_str()).collect();
    assert_eq!(out.hits.len(), 2, "got: {:?}", paths);
    assert!(paths.iter().all(|p| p.ends_with(".rs")));
}

#[tokio::test]
async fn integration_handles_nested_directories() {
    let dir = tmp_dir("nested");
    write_file(&dir, "top.txt", "alpha-top\n");
    write_file(&dir, "a/b/c/deep.txt", "alpha-deep\n");
    write_file(&dir, "x/y/z/deeper.txt", "alpha-deeper\n");

    let out = code_search::search_code(SearchInput {
        cwd: dir.to_str().unwrap(),
        query: "alpha",
        limit: None,
        file_glob: None,
        case_insensitive: false,
        regex: false,
    })
    .await
    .expect("nested");
    assert_eq!(out.hits.len(), 3, "got: {:?}", out.hits);
}

#[tokio::test]
async fn integration_serializes_to_expected_wire_shape() {
    let dir = tmp_dir("wire");
    write_file(&dir, "a.rs", "fn alpha() {}\n");

    let out = code_search::search_code(SearchInput {
        cwd: dir.to_str().unwrap(),
        query: "alpha",
        limit: None,
        file_glob: None,
        case_insensitive: false,
        regex: false,
    })
    .await
    .expect("wire");

    // Verify the wire shape serializes cleanly to JSON for RPC transport.
    let value = serde_json::to_value(&out).expect("serialize");
    let expected = json!({
        "truncated": false,
        "query": "alpha",
    });
    assert_eq!(value["truncated"], expected["truncated"]);
    assert_eq!(value["query"], expected["query"]);
    assert!(value["hits"].is_array());
    let hit = &value["hits"][0];
    assert!(hit["path"].is_string());
    assert!(hit["line"].is_u64());
    assert!(hit["column"].is_u64());
    assert!(hit["matched_text"].is_string());
}

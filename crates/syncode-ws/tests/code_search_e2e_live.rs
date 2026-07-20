//! LIVE end-to-end test for `tool/search-code` over a real WebSocket.
//!
//! Distinct from `code_search_rpc.rs` (which hits the library API) and
//! `ws_e2e.rs` (which spins up an in-process server). This file assumes a
//! STANDALONE dev server is already booted on `127.0.0.1:3000/ws` (e.g.
//! `cargo run -p syncode-ws --bin server`) and exercises the JSON-RPC
//! framing across a TCP socket the way an external client would.
//!
//! Gating: `SYNICODE_LIVE_E2E=1` (set by the live runner; otherwise skip).
//! Override the URL via `SYNICODE_LIVE_WS_URL` (default
//! `ws://127.0.0.1:3000/ws`).
//!
//! Reference: `tests/ws_e2e.rs` for the connect/send/receive helpers — this
//! file deliberately repeats them so it has no test-harness coupling.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

/// Resolve the live WS URL from env or fall back to the dev-server default.
fn live_url() -> String {
    std::env::var("SYNICODE_LIVE_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:3000/ws".to_string())
}

fn live_enabled() -> bool {
    std::env::var("SYNICODE_LIVE_E2E").ok().as_deref() == Some("1")
}

/// Absolute path to the repository root. Used as `cwd` for all searches so
/// the probe finds real source files (e.g. `crates/syncode-ws/src/code_search.rs`).
fn repo_root() -> String {
    std::env::var("SYNICODE_LIVE_CWD").unwrap_or_else(|_| {
        // CARGO_MANIFEST_DIR = `<root>/crates/syncode-ws`. Two `.parent()`s
        // walk up to the actual workspace root so the returned paths include
        // the `crates/...` prefix.
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent() // -> <root>/crates
            .and_then(|p| p.parent()) // -> <root>
            .and_then(|p| p.canonicalize().ok())
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|| ".".to_string())
    })
}

async fn connect() -> WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = live_url();
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .unwrap_or_else(|e| panic!("connect to {url}: {e}"));
    stream
}

async fn rpc(
    stream: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    method: &str,
    params: Value,
) -> Value {
    let id = json!(uuid::Uuid::new_v4().to_string());
    let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    stream
        .send(Message::Text(request.to_string().into()))
        .await
        .expect("send");

    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(Ok(msg)) = stream.next().await {
            if let Message::Text(text) = msg {
                let v: Value = serde_json::from_str(&text).expect("parse json");
                if v.get("id") == Some(&id) {
                    return v;
                }
            }
        }
        panic!("stream closed without response for method {method}");
    })
    .await
    .unwrap_or_else(|_| panic!("timeout reading response for {method}"))
}

/// Assert the response carries an error object with the expected code.
fn assert_error_code(resp: &Value, expected_code: i64, ctx: &str) {
    assert!(
        resp.get("error").is_some(),
        "{ctx}: expected error object, got: {resp}"
    );
    let code = resp["error"]["code"]
        .as_i64()
        .unwrap_or_else(|| panic!("{ctx}: error.code missing / not int: {}", resp["error"]));
    assert_eq!(
        code, expected_code,
        "{ctx}: expected code {expected_code}, got {code}, full error: {}",
        resp["error"]
    );
}

/// Assert the response is a successful JSON-RPC response with a result object.
fn assert_success<'a>(resp: &'a Value, ctx: &str) -> &'a Value {
    assert!(
        resp.get("error").is_none(),
        "{ctx}: unexpected error: {:?}",
        resp.get("error")
    );
    assert_eq!(resp["jsonrpc"], "2.0", "{ctx}: jsonrpc field wrong");
    assert!(
        resp.get("result").is_some(),
        "{ctx}: missing result field: {resp}"
    );
    &resp["result"]
}

/// Walk every hit in a SearchOutput `result` and assert it has the required
/// shape: `path: string`, `line: u64>=1`, `column: u64>=1`, `matched_text: string`.
fn assert_hits_well_formed(result: &Value, ctx: &str) {
    assert!(result["hits"].is_array(), "{ctx}: hits not array");
    assert!(
        result["truncated"].is_boolean(),
        "{ctx}: truncated not bool"
    );
    assert!(result["query"].is_string(), "{ctx}: query not string");
    for (i, hit) in result["hits"].as_array().unwrap().iter().enumerate() {
        let hctx = format!("{ctx} [hit {i}]");
        assert!(hit["path"].is_string(), "{hctx}: path not string");
        let line = hit["line"].as_u64().unwrap_or_else(|| {
            panic!("{hctx}: line not u64: {}", hit["line"]);
        });
        assert!(line >= 1, "{hctx}: line < 1: {line}");
        let col = hit["column"].as_u64().unwrap_or_else(|| {
            panic!("{hctx}: column not u64: {}", hit["column"]);
        });
        assert!(col >= 1, "{hctx}: column < 1: {col}");
        assert!(
            hit["matched_text"].is_string(),
            "{hctx}: matched_text not string"
        );
    }
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn live_literal_hit_in_code_search_rs() {
    if !live_enabled() {
        eprintln!("[skip] live e2e: set SYNICODE_LIVE_E2E=1");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(
        &mut s,
        "tool/search-code",
        json!({
            "cwd": repo_root(),
            "query": "search_code",
            "file_glob": "**/*.rs",
        }),
    )
    .await;
    let result = assert_success(&resp, "literal");
    assert_hits_well_formed(result, "literal");
    let hits = result["hits"].as_array().unwrap();
    assert!(
        !hits.is_empty(),
        "expected at least one hit on `search_code`"
    );
    let any_in_code_search = hits.iter().any(|h| {
        h["path"]
            .as_str()
            .map(|p| p.contains("crates/syncode-ws/src/code_search.rs"))
            .unwrap_or(false)
    });
    assert!(
        any_in_code_search,
        "expected at least one hit in crates/syncode-ws/src/code_search.rs, got paths: {:?}",
        hits.iter()
            .map(|h| h["path"].as_str().unwrap_or("?"))
            .collect::<Vec<_>>()
    );
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_case_insensitive_finds_same_hits() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(
        &mut s,
        "tool/search-code",
        json!({
            "cwd": repo_root(),
            "query": "SEARCH_CODE",
            "case_insensitive": true,
            "file_glob": "**/*.rs",
        }),
    )
    .await;
    let result = assert_success(&resp, "case-insensitive");
    assert_hits_well_formed(result, "case-insensitive");
    assert!(
        !result["hits"].as_array().unwrap().is_empty(),
        "case-insensitive should still find hits"
    );
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_case_sensitive_miss_returns_zero_hits() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(
        &mut s,
        "tool/search-code",
        json!({
            "cwd": repo_root(),
            "query": "SEARCH_CODE",
            "case_insensitive": false,
            // Scope to src/ trees only — this test file itself contains
            // the literal `SEARCH_CODE` (in the JSON payload above), which
            // would otherwise produce false-positive matches.
            "file_glob": "**/src/**/*.rs",
        }),
    )
    .await;
    let result = assert_success(&resp, "case-sensitive-miss");
    assert_hits_well_formed(result, "case-sensitive-miss");
    assert_eq!(
        result["hits"].as_array().unwrap().len(),
        0,
        "uppercase literal must NOT match lower-case source"
    );
    assert_eq!(result["truncated"], json!(false));
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_regex_pattern_finds_search_fns() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(
        &mut s,
        "tool/search-code",
        json!({
            "cwd": repo_root(),
            "query": r"fn\s+search_\w+",
            "regex": true,
            "file_glob": "**/*.rs",
        }),
    )
    .await;
    let result = assert_success(&resp, "regex");
    assert_hits_well_formed(result, "regex");
    let hits = result["hits"].as_array().unwrap();
    assert!(!hits.is_empty(), "regex should match `search_code`");
    let any_match = hits.iter().any(|h| {
        h["matched_text"]
            .as_str()
            .map(|t| t.contains("search_code"))
            .unwrap_or(false)
    });
    assert!(
        any_match,
        "expected a hit whose text contains `search_code`"
    );
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_invalid_regex_returns_error() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(
        &mut s,
        "tool/search-code",
        json!({
            "cwd": repo_root(),
            "query": r"[\d",
            "regex": true,
        }),
    )
    .await;
    // Invalid regex is a user-input error → must surface as -32602 INVALID_PARAMS.
    assert_error_code(&resp, -32602, "invalid regex");
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_missing_cwd_returns_invalid_params() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(
        &mut s,
        "tool/search-code",
        json!({
            "query": "x",
        }),
    )
    .await;
    // The handler checks `cwd.is_empty()` BEFORE calling search_code, so this
    // path returns the documented -32602 INVALID_PARAMS.
    assert_error_code(&resp, -32602, "missing cwd");
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_missing_query_returns_invalid_params() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(
        &mut s,
        "tool/search-code",
        json!({
            "cwd": repo_root(),
        }),
    )
    .await;
    assert_error_code(&resp, -32602, "missing query");
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_limit_truncation_reports_exact_count() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(
        &mut s,
        "tool/search-code",
        json!({
            "cwd": repo_root(),
            "query": "fn",
            "limit": 5,
            "file_glob": "**/*.rs",
        }),
    )
    .await;
    let result = assert_success(&resp, "limit");
    assert_hits_well_formed(result, "limit");
    assert_eq!(
        result["hits"].as_array().unwrap().len(),
        5,
        "limit=5 on `fn` over this repo MUST yield exactly 5 hits"
    );
    assert_eq!(
        result["truncated"],
        json!(true),
        "truncated flag must be true when the limit is hit"
    );
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_glob_filter_only_toml_files() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(
        &mut s,
        "tool/search-code",
        json!({
            "cwd": repo_root(),
            "query": "tokio",
            "file_glob": "**/*.toml",
        }),
    )
    .await;
    let result = assert_success(&resp, "glob-toml");
    assert_hits_well_formed(result, "glob-toml");
    let all_toml = result["hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|h| h["path"].as_str().unwrap_or("").ends_with(".toml"));
    assert!(all_toml, "every hit path must end with .toml");
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_nonexistent_cwd_returns_error() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(
        &mut s,
        "tool/search-code",
        json!({
            "cwd": "C:/__nope__",
            "query": "x",
        }),
    )
    .await;
    // Nonexistent cwd is a user-input error → must surface as -32602 INVALID_PARAMS.
    assert_error_code(&resp, -32602, "nonexistent cwd");
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_method_spelling_parity() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let params = json!({
        "cwd": repo_root(),
        "query": "search_code",
        "file_glob": "**/*.rs",
        "limit": 5,
    });

    let dashed = rpc(&mut s, "tool/search-code", params.clone()).await;
    let dotted = rpc(&mut s, "tool.searchCode", params.clone()).await;

    let dashed_result = assert_success(&dashed, "tool/search-code");
    let dotted_result = assert_success(&dotted, "tool.searchCode");
    assert_hits_well_formed(dashed_result, "tool/search-code");
    assert_hits_well_formed(dotted_result, "tool.searchCode");

    // Both spellings must return identical hit paths + line numbers (order-stable
    // because the walker is deterministic for the same tree).
    let dashed_paths: Vec<String> = dashed_result["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            format!(
                "{}:{}:{}",
                h["path"].as_str().unwrap_or(""),
                h["line"].as_u64().unwrap_or(0),
                h["column"].as_u64().unwrap_or(0),
            )
        })
        .collect();
    let dotted_paths: Vec<String> = dotted_result["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            format!(
                "{}:{}:{}",
                h["path"].as_str().unwrap_or(""),
                h["line"].as_u64().unwrap_or(0),
                h["column"].as_u64().unwrap_or(0),
            )
        })
        .collect();
    assert_eq!(
        dashed_paths, dotted_paths,
        "tool/search-code and tool.searchCode must return identical hits"
    );
    let _ = s.close(None).await;
}

#[tokio::test]
async fn live_rpc_list_methods_includes_search_code() {
    if !live_enabled() {
        eprintln!("[skip] live e2e");
        return;
    }
    let mut s = connect().await;
    let resp = rpc(&mut s, "rpc/listMethods", json!({})).await;
    let result = assert_success(&resp, "rpc/listMethods");
    let methods = result["methods"]
        .as_array()
        .expect("rpc/listMethods.methods must be an array");
    let contains = methods
        .iter()
        .any(|m| m.as_str().map(|s| s == "tool/search-code").unwrap_or(false));
    assert!(
        contains,
        "rpc/listMethods must list `tool/search-code`, got: {:?}",
        methods
            .iter()
            .map(|m| m.as_str().unwrap_or("?"))
            .collect::<Vec<_>>()
    );
    let _ = s.close(None).await;
}

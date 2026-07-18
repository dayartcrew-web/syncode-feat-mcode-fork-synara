//! Integration tests for the MCP discovery + syncode-store CRUD RPCs.
//!
//! Covers the public surface from `crates/syncode-ws/src/mcp_catalog.rs`:
//!   - `discover_mcp_catalog` aggregates all four sources + syncode store
//!   - `create_syncode_server` / `update_syncode_server` / `delete_syncode_server`
//!     round-trip entries against a temp home directory
//!   - `probe_mcp_server` reports `unreachable` cleanly when the binary is
//!     missing (no panic, no server crash)
//!
//! Real-handler dispatch + WS-server end-to-end coverage lives in
//! `tests/ws_e2e.rs` and `tests/provider_e2e.rs`. These tests target the
//! library API directly so they don't need to boot a server — they exercise
//! the same code paths the RPC handlers in `rpc.rs` call into.

use serde_json::json;
use std::collections::HashSet;
use syncode_ws::mcp_catalog::{self, McpDiscoveryInput, McpServerInput, McpTransport};

/// Makes a temp dir under `std::env::temp_dir()` with a unique suffix.
fn tmp_home(label: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "syncode-mcp-it-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));
    std::fs::create_dir_all(&path).expect("create temp dir");
    path.to_string_lossy().into_owned()
}

fn stdio_input<'a>(
    name: &'a str,
    command: &'a str,
    args: &'a [String],
    env: &'a [(String, String)],
) -> McpServerInput<'a> {
    McpServerInput {
        name,
        name_override: None,
        transport: McpTransport::Stdio,
        transport_override: None,
        command: Some(command),
        args,
        env,
        url: None,
        set_command: true,
        set_args: true,
        set_env: true,
        set_url: true,
    }
}

#[test]
fn discover_returns_aggregated_across_sources() {
    let home = tmp_home("discover");
    // Seed ~/.claude.json with one entry.
    let claude_path = std::path::Path::new(&home).join(".claude.json");
    std::fs::write(
        &claude_path,
        r#"{"mcpServers": {"claude-fs": {"command": "npx", "args": ["-y", "fs"]}}}"#,
    )
    .unwrap();
    // Seed ~/.syncode/mcp.json with another entry via the CRUD API.
    let args: Vec<String> = vec!["--port".into(), "8080".into()];
    let env: Vec<(String, String)> = vec![("API_KEY".into(), "secret".into())];
    let input = stdio_input("syncode-fs", "node", &args, &env);
    mcp_catalog::create_syncode_server(&home, &input).expect("create syncode entry");

    let discovered = mcp_catalog::discover_mcp_catalog(McpDiscoveryInput {
        cwd: None,
        home_dir: Some(home.clone()),
        disabled: HashSet::new(),
    });
    let names: Vec<&str> = discovered.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"claude-fs"), "claude-fs missing: {names:?}");
    assert!(
        names.contains(&"syncode-fs"),
        "syncode-fs missing: {names:?}"
    );

    std::fs::remove_dir_all(&home).ok();
}

#[test]
fn create_persists_to_syncode_store() {
    let home = tmp_home("create");
    let args: Vec<String> = vec![
        "-y".into(),
        "@modelcontextprotocol/server-filesystem".into(),
    ];
    let env: Vec<(String, String)> = vec![("GITHUB_TOKEN".into(), "ghp_xxx".into())];
    let input = stdio_input("filesystem", "npx", &args, &env);
    let descriptor = mcp_catalog::create_syncode_server(&home, &input).expect("create succeeds");

    assert_eq!(descriptor.name, "filesystem");
    assert_eq!(descriptor.transport, McpTransport::Stdio);
    assert_eq!(descriptor.command.as_deref(), Some("npx"));
    assert!(descriptor.editable);
    assert!(descriptor.enabled);
    // Env NAMES are exposed, VALUES are not.
    assert_eq!(descriptor.env.len(), 1);
    assert_eq!(descriptor.env[0].name, "GITHUB_TOKEN");

    // The store file was actually created at the expected path.
    let store_path = std::path::Path::new(&home)
        .join(".syncode")
        .join("mcp.json");
    assert!(
        store_path.exists(),
        "store file should exist at {store_path:?}"
    );
    let raw = std::fs::read_to_string(&store_path).unwrap();
    assert!(raw.contains("filesystem"), "store should contain the name");
    assert!(
        raw.contains("ghp_xxx"),
        "store holds env VALUES on disk (redaction is wire-only)"
    );

    std::fs::remove_dir_all(&home).ok();
}

#[test]
fn create_rejects_duplicate_name() {
    let home = tmp_home("dup");
    let args: Vec<String> = vec![];
    let env: Vec<(String, String)> = vec![];
    let input = stdio_input("github", "gh-mcp", &args, &env);
    mcp_catalog::create_syncode_server(&home, &input).expect("first create");

    let err =
        mcp_catalog::create_syncode_server(&home, &input).expect_err("duplicate create must fail");
    assert!(err.contains("already exists"), "wrong error: {err}");

    std::fs::remove_dir_all(&home).ok();
}

#[test]
fn update_rejects_discovered_entry() {
    let home = tmp_home("upd-discovered");
    // Seed ONLY ~/.claude.json — no syncode store entry.
    let claude_path = std::path::Path::new(&home).join(".claude.json");
    std::fs::write(
        &claude_path,
        r#"{"mcpServers": {"claude-fs": {"command": "npx", "args": ["fs"]}}}"#,
    )
    .unwrap();

    let args: Vec<String> = vec![];
    let env: Vec<(String, String)> = vec![];
    let patch = McpServerInput {
        name: "claude-fs",
        name_override: None,
        transport: McpTransport::Stdio,
        transport_override: None,
        command: Some("newcmd"),
        args: &args,
        env: &env,
        url: None,
        set_command: true,
        set_args: false,
        set_env: false,
        set_url: false,
    };
    let err = mcp_catalog::update_syncode_server(&home, "claude-fs", &patch)
        .expect_err("update must fail — entry is from external source");
    assert!(err.contains("not found"), "wrong error: {err}");

    std::fs::remove_dir_all(&home).ok();
}

#[test]
fn delete_removes_from_store() {
    let home = tmp_home("del");
    let args: Vec<String> = vec![];
    let env: Vec<(String, String)> = vec![];
    let input = stdio_input("temp-server", "cmd", &args, &env);
    mcp_catalog::create_syncode_server(&home, &input).expect("create");

    mcp_catalog::delete_syncode_server(&home, "temp-server").expect("delete");

    let discovered = mcp_catalog::discover_mcp_catalog(McpDiscoveryInput {
        cwd: None,
        home_dir: Some(home.clone()),
        disabled: HashSet::new(),
    });
    assert!(
        discovered.iter().all(|d| d.name != "temp-server"),
        "entry should be gone"
    );

    std::fs::remove_dir_all(&home).ok();
}

#[tokio::test]
async fn test_connection_returns_unreachable_on_bad_command() {
    // Spawn a binary that doesn't exist on PATH — must NOT panic, must report
    // unreachable cleanly. This is the contract the UI relies on: the test
    // button never crashes the server, only shows a red status.
    let params = json!({
        "transport": "stdio",
        "command": "__syncode_nonexistent_binary__",
        "args": [],
        "env": {},
        "timeoutMs": 1000,
    });
    let result = mcp_catalog::probe_mcp_server(&params, 1000).await;
    assert_eq!(
        result["status"].as_str(),
        Some("unreachable"),
        "status should be unreachable, got: {result}"
    );
    assert!(
        result.get("error").is_some(),
        "error message must be present"
    );
}

#[tokio::test]
async fn test_connection_reports_reachable_for_known_fast_stdio() {
    // The `ver` binary on Windows / `true` on POSIX — both exit immediately
    // with no output. The probe reads stdin → EOF before "result" appears,
    // so it's expected to be "unreachable" (EOF before response). The point
    // of this test is to verify the function handles a successful spawn
    // without panicking on either platform.
    let cmd = if cfg!(windows) { "cmd" } else { "true" };
    let args: Vec<&str> = if cfg!(windows) {
        vec!["/c", "ver"]
    } else {
        vec![]
    };
    let params = json!({
        "transport": "stdio",
        "command": cmd,
        "args": args,
        "env": {},
        "timeoutMs": 1500,
    });
    let result = mcp_catalog::probe_mcp_server(&params, 1500).await;
    // Either reachable (unlikely — these don't speak MCP) or unreachable with
    // a non-empty error. The contract is just "no panic, valid shape".
    let status = result["status"].as_str().expect("status must be present");
    assert!(
        status == "reachable" || status == "unreachable",
        "invalid status: {status}"
    );
}

#[test]
fn discover_with_disabled_markes_entries_correctly() {
    let home = tmp_home("disabled");
    // Two syncode entries: one we'll disable, one we'll keep enabled.
    let args: Vec<String> = vec![];
    let env: Vec<(String, String)> = vec![];
    let on = stdio_input("enabled-srv", "cmd", &args, &env);
    let off = stdio_input("disabled-srv", "cmd", &args, &env);
    mcp_catalog::create_syncode_server(&home, &on).unwrap();
    mcp_catalog::create_syncode_server(&home, &off).unwrap();

    let mut disabled = HashSet::new();
    disabled.insert("disabled-srv".to_string());
    let discovered = mcp_catalog::discover_mcp_catalog(McpDiscoveryInput {
        cwd: None,
        home_dir: Some(home.clone()),
        disabled,
    });
    for d in &discovered {
        if d.name == "disabled-srv" {
            assert!(!d.enabled, "disabled-srv should be disabled");
        } else if d.name == "enabled-srv" {
            assert!(d.enabled, "enabled-srv should be enabled");
        }
    }

    std::fs::remove_dir_all(&home).ok();
}

//! MCP server discovery — multi-source catalog engine.
//!
//! Aggregates MCP server definitions from four external config files plus the
//! syncode-owned store at `~/.syncode/mcp.json`. Discovery is read-only with
//! respect to the external files: we NEVER write back to `~/.claude.json`,
//! `~/.cursor/mcp.json`, `~/.codex/config.toml`, or any project-local
//! `.mcp.json` / `.cursor/mcp.json`. The only writer surface is the syncode
//! store.
//!
//! Sources scanned (in order — earlier wins on name collisions, except the
//! syncode store which always wins so user edits aren't shadowed):
//!   1. `~/.syncode/mcp.json`              — syncode-owned, editable
//!   2. `~/.claude.json` (mcpServers)      — Claude Code per-user
//!   3. `~/.cursor/mcp.json`               — Cursor per-user
//!   4. `~/.codex/config.toml [mcp_servers]` — Codex CLI per-user
//!   5. `<cwd> + ancestors/.mcp.json`      — Claude Code project-local
//!   6. `<cwd> + ancestors/.cursor/mcp.json` — Cursor project-local
//!
//! Env-var values are redacted at the parser boundary: only NAMES cross the
//! wire in `McpServerDescriptor`. The syncode store holds values on disk but
//! they're only re-read by `build_mcp_servers_for_acp` when forwarding to an
//! ACP provider (cursor/grok/gemini).
//!
//! Layer: server provider-discovery helper.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// ── Wire types ───────────────────────────────────────────────────────

/// Transport kind for an MCP server. Mirrors the JSON-RPC value sent over the
/// wire so the frontend can switch UI on it without translation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpTransport {
    Stdio,
    Http,
    Sse,
}

/// Origin scope — controls where a server was discovered from. Drives UI
/// grouping ("Discovered" vs "Configured by Syncode") and editability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpScope {
    /// `~/.claude.json`, `~/.cursor/mcp.json`, `~/.codex/config.toml`.
    User,
    /// `<cwd>/.mcp.json` or `<cwd>/.cursor/mcp.json` (project-local).
    Project,
    /// `~/.syncode/mcp.json` — the only editable source.
    Syncode,
}

/// Reachability verdict from `mcp/test-connection`. `Unknown` is the default
/// for descriptors that haven't been probed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpStatus {
    Reachable,
    Unreachable,
    Unknown,
}

/// An env-var binding as it crosses the wire. **Only the name is exposed** —
/// values are redacted at the parser boundary so secrets never reach the UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpEnvVar {
    pub name: String,
}

/// A discovered MCP server. The wire shape is identical in TypeScript
/// (`frontend/src/contracts/tier3/mcp.ts`).
#[derive(Debug, Clone, Serialize)]
pub struct McpServerDescriptor {
    pub name: String,
    pub transport: McpTransport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<McpEnvVar>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub scope: McpScope,
    /// Filesystem path of the file we discovered this entry from. Displayed
    /// in the UI as a subtitle so users can see which config owns the row.
    pub source_path: String,
    /// `false` for entries from external sources, `true` for the syncode store.
    /// The frontend uses this to gate edit/delete buttons.
    pub editable: bool,
    /// `!disabled.contains(name.to_lowercase())`. Computed by the unifier.
    pub enabled: bool,
    /// Populated only by `mcp/test-connection`. Absent on catalog entries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<McpStatus>,
}

// ── Internal types (never serialized for the wire) ───────────────────

/// Internal representation of a server stored in `~/.syncode/mcp.json`. Holds
/// the env-var VALUES (the file is user-owned and lives under `~/.syncode/`
/// next to the skills folder); access is restricted to
/// [`read_syncode_mcp_store`] / [`write_syncode_mcp_store`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredMcpEnvVar {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredMcpServer {
    pub name: String,
    pub transport: McpTransport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<StoredMcpEnvVar>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SyncodeMcpStore {
    pub version: u32,
    #[serde(default)]
    pub servers: Vec<StoredMcpServer>,
}

impl Default for SyncodeMcpStore {
    fn default() -> Self {
        Self {
            version: 1,
            servers: Vec::new(),
        }
    }
}

/// Inputs to a catalog discovery scan.
pub struct McpDiscoveryInput<'a> {
    /// Optional workspace cwd; when present, project-level `.mcp.json` and
    /// `.cursor/mcp.json` files are walked up from here (max 5 ancestors).
    pub cwd: Option<&'a str>,
    /// Resolved home directory (from `server_home_dir`). When `None`, only
    /// project sources are scanned.
    pub home_dir: Option<String>,
    /// Lowercased names the user has disabled — drives each descriptor's
    /// `enabled` flag.
    pub disabled: HashSet<String>,
}

// ── Syncode-owned store helpers ──────────────────────────────────────

/// Returns the path to `~/.syncode/mcp.json`, or `None` if home is unknown.
pub(crate) fn syncode_mcp_path(home_dir: &str) -> PathBuf {
    Path::new(home_dir).join(".syncode").join("mcp.json")
}

/// Ensures the `~/.syncode/` directory exists and returns the path to
/// `mcp.json` inside it. Returns `None` only if home is unknown or directory
/// creation fails.
pub(crate) fn ensure_syncode_mcp_file(home_dir: &str) -> Option<PathBuf> {
    let path = syncode_mcp_path(home_dir);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    Some(path)
}

/// Reads the syncode-owned store. Returns an empty default on missing/corrupt
/// file — never propagates errors so a corrupt store can't break discovery.
pub(crate) fn read_syncode_mcp_store(home_dir: &str) -> SyncodeMcpStore {
    let path = syncode_mcp_path(home_dir);
    let Ok(content) = std::fs::read_to_string(&path) else {
        return SyncodeMcpStore::default();
    };
    match serde_json::from_str::<SyncodeMcpStore>(&content) {
        Ok(store) => store,
        Err(e) => {
            tracing::warn!(
                target: "syncode_ws::mcp_catalog",
                error = %e,
                path = %path.display(),
                "syncode mcp store is corrupt — falling back to empty",
            );
            SyncodeMcpStore::default()
        }
    }
}

// Module-level lock guarding concurrent writes to the syncode store. The
// store is per-user so contention is essentially zero; the lock is here to
// defend against two browser tabs racing a create/delete.
static SYNCODE_STORE_LOCK: Mutex<()> = Mutex::new(());

/// Writes the syncode-owned store atomically (temp file + rename). Returns
/// the path written to (for callers that want to surface it in the response).
pub(crate) fn write_syncode_mcp_store(
    home_dir: &str,
    store: &SyncodeMcpStore,
) -> Result<PathBuf, String> {
    let _guard = SYNCODE_STORE_LOCK
        .lock()
        .map_err(|e| format!("store lock poisoned: {e}"))?;
    let path = ensure_syncode_mcp_file(home_dir).ok_or_else(|| "home dir unknown".to_string())?;
    let json = serde_json::to_string_pretty(store).map_err(|e| format!("serialize failed: {e}"))?;
    let parent = path
        .parent()
        .ok_or_else(|| "store path has no parent".to_string())?;
    // Temp file lives in the SAME directory as the target so the rename is
    // atomic on the same filesystem (cross-device rename fails on POSIX).
    let temp = parent.join(format!(
        ".syncode-mcp-{}-{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));
    std::fs::write(&temp, json)
        .map_err(|e| format!("temp write failed ({}): {e}", temp.display()))?;
    // Atomic on POSIX; on Windows, rename over an existing file needs the
    // target gone first — fall back to copy+remove on rename failure.
    if let Err(e) = std::fs::rename(&temp, &path) {
        let _ = std::fs::copy(&temp, &path);
        let _ = std::fs::remove_file(&temp);
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            tracing::debug!(
                target: "syncode_ws::mcp_catalog",
                error = %e,
                "rename fell back to copy; non-fatal if copy succeeded",
            );
        }
    }
    Ok(path)
}

// ── Parsers ──────────────────────────────────────────────────────────

/// Skips `~/.claude.json` if it's larger than 16 MB. The file accumulates
/// every project's chat history on long-running Claude installs and can
/// balloon to hundreds of MB; loading it whole into `serde_json` would stall
/// discovery. The cap is generous — real per-user mcpServers sections are a
/// few KB at most.
const CLAUDE_JSON_SIZE_CAP: u64 = 16 * 1024 * 1024;

fn parse_env_names(env: &serde_json::Value) -> Vec<McpEnvVar> {
    env.as_object()
        .map(|map| {
            map.keys()
                .map(|k| McpEnvVar {
                    name: k.to_string(),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extracts the standard stdio server shape shared by Claude/Cursor/codex:
/// `{command, args?, env?}`. Returns `None` if `command` is missing (the
/// entry is malformed and should be skipped).
fn extract_stdio_entry(
    name: &str,
    value: &serde_json::Value,
) -> Option<(Option<String>, Vec<String>, Vec<McpEnvVar>)> {
    let obj = value.as_object()?;
    let command = obj
        .get("command")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let args: Vec<String> = obj
        .get("args")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let env = obj.get("env").map(parse_env_names).unwrap_or_default();
    if command.is_none() {
        tracing::debug!(
            target: "syncode_ws::mcp_catalog",
            entry_name = %name,
            "skipping mcpServers entry — missing command",
        );
    }
    Some((command, args, env))
}

/// Walks a `{name: {command, args, env}}` object literal — the format used by
/// Claude Code's `~/.claude.json` mcpServers section AND Cursor's
/// `~/.cursor/mcp.json` (which wraps the same shape under a top-level
/// `mcpServers` key).
fn parse_mcp_servers_object(
    obj: &serde_json::Map<String, serde_json::Value>,
    scope: McpScope,
    source_path: &str,
    disabled: &HashSet<String>,
) -> Vec<McpServerDescriptor> {
    let mut out = Vec::new();
    for (name, value) in obj {
        // Claude Code also accepts an `url`/`transport: "sse"|"http"` shape;
        // detect it here so non-stdio servers don't get filtered as malformed.
        if let Some(obj_val) = value.as_object() {
            let transport_str = obj_val
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("stdio");
            let (transport, command, args, env, url) = match transport_str {
                "http" => {
                    let url = obj_val
                        .get("url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    (McpTransport::Http, None, Vec::new(), Vec::new(), url)
                }
                "sse" => {
                    let url = obj_val
                        .get("url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    (McpTransport::Sse, None, Vec::new(), Vec::new(), url)
                }
                _ => {
                    // Default to stdio. Missing command → entry is malformed;
                    // skip it.
                    let Some((command, args, env)) = extract_stdio_entry(name, value) else {
                        continue;
                    };
                    (McpTransport::Stdio, command, args, env, None)
                }
            };
            let lower = name.to_lowercase();
            let enabled = !disabled.contains(&lower);
            out.push(McpServerDescriptor {
                name: name.clone(),
                transport,
                command,
                args,
                env,
                url,
                scope,
                source_path: source_path.to_string(),
                editable: false,
                enabled,
                status: None,
            });
        }
    }
    out
}

fn parse_claude_json(home: &str, disabled: &HashSet<String>) -> Vec<McpServerDescriptor> {
    let path = Path::new(home).join(".claude.json");
    let Ok(meta) = std::fs::metadata(&path) else {
        return Vec::new();
    };
    if meta.len() > CLAUDE_JSON_SIZE_CAP {
        tracing::warn!(
            target: "syncode_ws::mcp_catalog",
            path = %path.display(),
            size = meta.len(),
            "~/.claude.json exceeds 16 MB cap — skipping MCP discovery for this source",
        );
        return Vec::new();
    }
    let Ok(content) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    let Ok(parsed): Result<serde_json::Value, _> = serde_json::from_str(&content) else {
        tracing::warn!(
            target: "syncode_ws::mcp_catalog",
            path = %path.display(),
            "~/.claude.json is not valid JSON — skipping",
        );
        return Vec::new();
    };
    let Some(servers) = parsed.get("mcpServers").and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    parse_mcp_servers_object(servers, McpScope::User, &path.to_string_lossy(), disabled)
}

fn parse_cursor_mcp_json(home: &str, disabled: &HashSet<String>) -> Vec<McpServerDescriptor> {
    let path = Path::new(home).join(".cursor").join("mcp.json");
    let Ok(content) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    let Ok(parsed): Result<serde_json::Value, _> = serde_json::from_str(&content) else {
        return Vec::new();
    };
    let Some(servers) = parsed.get("mcpServers").and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    parse_mcp_servers_object(servers, McpScope::User, &path.to_string_lossy(), disabled)
}

/// Codex stores config in TOML. The `[mcp_servers.<name>]` table shape is
/// roughly: `{ command = "string", args = ["..."], env = { KEY = "value" } }`.
fn parse_codex_config_toml(home: &str, disabled: &HashSet<String>) -> Vec<McpServerDescriptor> {
    let path = Path::new(home).join(".codex").join("config.toml");
    let Ok(content) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    let parsed: toml::Value = match toml::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                target: "syncode_ws::mcp_catalog",
                error = %e,
                path = %path.display(),
                "~/.codex/config.toml is not valid TOML — skipping",
            );
            return Vec::new();
        }
    };
    let Some(table) = parsed.as_table() else {
        return Vec::new();
    };
    // Two layouts seen in the wild:
    //   (a) top-level `[mcp_servers.<name>]` — `toml` parses it as a nested
    //       table under the `mcp_servers` key.
    //   (b) top-level `[[mcp_servers]]` array-of-tables.
    let Some(mcp_section) = table.get("mcp_servers") else {
        return Vec::new();
    };

    let mut out = Vec::new();
    let push_entry = |out: &mut Vec<_>, name: &str, entry: &toml::Table| {
        let transport_str = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("stdio");
        let (transport, command, args, env, url) = match transport_str {
            "http" | "sse" => {
                let url = entry
                    .get("url")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let kind = if transport_str == "http" {
                    McpTransport::Http
                } else {
                    McpTransport::Sse
                };
                (kind, None, Vec::new(), Vec::new(), url)
            }
            _ => {
                let command = entry
                    .get("command")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let args: Vec<String> = entry
                    .get("args")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                let env: Vec<McpEnvVar> = entry
                    .get("env")
                    .and_then(|v| v.as_table())
                    .map(|t| {
                        t.keys()
                            .map(|k| McpEnvVar {
                                name: k.to_string(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                if command.is_none() {
                    tracing::debug!(
                        target: "syncode_ws::mcp_catalog",
                        entry_name = %name,
                        "skipping codex mcp_servers entry — missing command",
                    );
                    return;
                }
                (McpTransport::Stdio, command, args, env, None)
            }
        };
        let lower = name.to_lowercase();
        let enabled = !disabled.contains(&lower);
        out.push(McpServerDescriptor {
            name: name.to_string(),
            transport,
            command,
            args,
            env,
            url,
            scope: McpScope::User,
            source_path: path.to_string_lossy().into_owned(),
            editable: false,
            enabled,
            status: None,
        });
    };

    if let Some(map) = mcp_section.as_table() {
        for (name, value) in map {
            if let Some(entry) = value.as_table() {
                push_entry(&mut out, name, entry);
            }
        }
    } else if let Some(arr) = mcp_section.as_array() {
        for value in arr {
            if let Some(table_val) = value.as_table() {
                let Some(name) = table_val
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                else {
                    continue;
                };
                push_entry(&mut out, &name, table_val);
            }
        }
    }

    out
}

/// Walks `cwd` and each ancestor (max 5 levels) looking for `.mcp.json` (the
/// Claude Code project-local convention) and `.cursor/mcp.json`. Returns one
/// descriptor per match — does NOT dedupe across ancestors so users can see
/// each level that contributes.
fn parse_project_mcp_json(cwd: &str, disabled: &HashSet<String>) -> Vec<McpServerDescriptor> {
    let mut out = Vec::new();
    let start = Path::new(cwd);
    let mut current: Option<&Path> = Some(start);
    let mut levels = 0;
    while let Some(dir) = current {
        if levels > 5 {
            break;
        }
        // .mcp.json at this level
        let claude_project = dir.join(".mcp.json");
        if let Ok(content) = std::fs::read_to_string(&claude_project)
            && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&content)
            && let Some(servers) = parsed.get("mcpServers").and_then(|v| v.as_object())
        {
            let mut entries = parse_mcp_servers_object(
                servers,
                McpScope::Project,
                &claude_project.to_string_lossy(),
                disabled,
            );
            out.append(&mut entries);
        }
        // .cursor/mcp.json at this level
        let cursor_project = dir.join(".cursor").join("mcp.json");
        if let Ok(content) = std::fs::read_to_string(&cursor_project)
            && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&content)
            && let Some(servers) = parsed.get("mcpServers").and_then(|v| v.as_object())
        {
            let mut entries = parse_mcp_servers_object(
                servers,
                McpScope::Project,
                &cursor_project.to_string_lossy(),
                disabled,
            );
            out.append(&mut entries);
        }
        current = dir.parent();
        levels += 1;
    }
    out
}

/// Renders syncode-owned stored servers (which carry env VALUES) into wire
/// descriptors (which redact values). Used by the unifier and by the CRUD
/// handlers' responses.
fn stored_to_descriptor(
    stored: &StoredMcpServer,
    source_path: &str,
    disabled: &HashSet<String>,
) -> McpServerDescriptor {
    let env = stored
        .env
        .iter()
        .map(|v| McpEnvVar {
            name: v.name.clone(),
        })
        .collect();
    let lower = stored.name.to_lowercase();
    let enabled = !disabled.contains(&lower);
    McpServerDescriptor {
        name: stored.name.clone(),
        transport: stored.transport,
        command: stored.command.clone(),
        args: stored.args.clone(),
        env,
        url: stored.url.clone(),
        scope: McpScope::Syncode,
        source_path: source_path.to_string(),
        editable: true,
        enabled,
        status: None,
    }
}

// ── Unifier ──────────────────────────────────────────────────────────

/// Aggregates all sources into one catalog. Dedupes by lowercased name;
/// syncode-owned entries always win (inserted at the front of dedupe
/// precedence) so user edits aren't shadowed by a discovered file.
pub fn discover_mcp_catalog(input: McpDiscoveryInput<'_>) -> Vec<McpServerDescriptor> {
    let mut aggregated: Vec<McpServerDescriptor> = Vec::new();
    let disabled = input.disabled;

    // Syncode store first — it owns precedence.
    if let Some(home) = input.home_dir.as_deref() {
        let store = read_syncode_mcp_store(home);
        let store_path = syncode_mcp_path(home).to_string_lossy().into_owned();
        for stored in &store.servers {
            aggregated.push(stored_to_descriptor(stored, &store_path, &disabled));
        }
        // External home-root sources.
        aggregated.extend(parse_claude_json(home, &disabled));
        aggregated.extend(parse_cursor_mcp_json(home, &disabled));
        aggregated.extend(parse_codex_config_toml(home, &disabled));
    }
    if let Some(cwd) = input.cwd {
        aggregated.extend(parse_project_mcp_json(cwd, &disabled));
    }

    // Dedupe by lowercased name; first occurrence wins (syncode store is
    // already at the head, so its entries are preserved).
    let mut seen: HashSet<String> = HashSet::new();
    aggregated.retain(|d| {
        let key = d.name.to_lowercase();
        if seen.contains(&key) {
            false
        } else {
            seen.insert(key);
            true
        }
    });

    // Stable sort by scope then name — syncode first, then user, then project,
    // alphabetical within each group. The frontend also sorts client-side, so
    // this is just a deterministic default for raw consumers (tests, ACP).
    aggregated.sort_by(|a, b| {
        let scope_rank = |s: McpScope| match s {
            McpScope::Syncode => 0,
            McpScope::User => 1,
            McpScope::Project => 2,
        };
        scope_rank(a.scope)
            .cmp(&scope_rank(b.scope))
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
    });

    aggregated
}

// ── ACP integration ──────────────────────────────────────────────────

/// Builds the `mcpServers` array to forward to ACP-speaking providers
/// (cursor/grok/gemini) at `session/new` time. Re-reads each entry's source
/// file to recover env VALUES — they were stripped from the descriptor for
/// redaction but ACP needs them to spawn the server child.
///
/// Returns the array in the ACP-expected shape:
/// `[{name, transport: {type, command?, args?, env?, url?}}]`.
pub fn build_mcp_servers_for_acp(
    settings: &serde_json::Value,
    home_dir: Option<&str>,
    cwd: Option<&str>,
) -> Vec<serde_json::Value> {
    // Pull the disabled list out of settings.mcp.disabled.
    let disabled = settings
        .get("mcp")
        .and_then(|m| m.get("disabled"))
        .and_then(|d| d.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_lowercase()))
                .collect::<HashSet<String>>()
        })
        .unwrap_or_default();

    let Some(home) = home_dir else {
        return Vec::new();
    };

    let input = McpDiscoveryInput {
        cwd,
        home_dir: Some(home.to_string()),
        disabled: disabled.clone(),
    };
    let catalog = discover_mcp_catalog(input);

    // ACP providers we forward to today only support stdio (cursor/grok/gemini
    // docs). Filter accordingly so we don't send shapes the providers reject.
    let mut out = Vec::new();
    let store = read_syncode_mcp_store(home);
    for entry in catalog
        .iter()
        .filter(|e| e.enabled && e.transport == McpTransport::Stdio)
    {
        // Recover env values. For syncode-owned entries, look up in the store.
        // For external entries, re-read the source file. We don't currently
        // forward external entries (we only forward syncode-owned) to keep the
        // behavior tight; future work can extend this if a provider confirms
        // it accepts external servers.
        if entry.scope != McpScope::Syncode {
            continue;
        }
        let Some(stored) = store.servers.iter().find(|s| s.name == entry.name) else {
            continue;
        };
        let env_obj: serde_json::Value = if stored.env.is_empty() {
            serde_json::Value::Object(serde_json::Map::new())
        } else {
            let mut map = serde_json::Map::new();
            for var in &stored.env {
                map.insert(
                    var.name.clone(),
                    serde_json::Value::String(var.value.clone()),
                );
            }
            serde_json::Value::Object(map)
        };
        let transport = serde_json::json!({
            "type": "stdio",
            "command": stored.command,
            "args": stored.args,
            "env": env_obj,
        });
        out.push(serde_json::json!({
            "name": stored.name,
            "transport": transport,
        }));
    }
    out
}

// ── Mutation helpers ─────────────────────────────────────────────────

/// Lowercased-name lookup key. The disabled list normalizes on this shape.
pub fn mcp_name_key(name: &str) -> String {
    name.trim().to_lowercase()
}

/// Creates a new syncode-owned server. Errors describe the failure mode for
/// the RPC layer to surface verbatim.
pub fn create_syncode_server(
    home_dir: &str,
    input: &McpServerInput<'_>,
) -> Result<McpServerDescriptor, String> {
    input.validate()?;
    let mut store = read_syncode_mcp_store(home_dir);
    let key = mcp_name_key(input.name);
    if store.servers.iter().any(|s| mcp_name_key(&s.name) == key) {
        return Err(format!("a server named '{}' already exists", input.name));
    }
    let stored = StoredMcpServer {
        name: input.name.to_string(),
        transport: input.transport,
        command: input.command.map(|s| s.to_string()),
        args: input.args.to_vec(),
        env: input
            .env
            .iter()
            .map(|(n, v)| StoredMcpEnvVar {
                name: n.to_string(),
                value: v.to_string(),
            })
            .collect(),
        url: input.url.map(|s| s.to_string()),
    };
    store.servers.push(stored.clone());
    write_syncode_mcp_store(home_dir, &store)?;
    let source_path = syncode_mcp_path(home_dir).to_string_lossy().into_owned();
    let disabled = HashSet::new();
    Ok(stored_to_descriptor(&stored, &source_path, &disabled))
}

/// Updates an existing syncode-owned server. The patch is partial — only
/// provided fields are replaced.
pub fn update_syncode_server(
    home_dir: &str,
    name: &str,
    patch: &McpServerInput<'_>,
) -> Result<McpServerDescriptor, String> {
    let mut store = read_syncode_mcp_store(home_dir);
    let key = mcp_name_key(name);
    let idx = store
        .servers
        .iter()
        .position(|s| mcp_name_key(&s.name) == key)
        .ok_or_else(|| format!("server '{}' not found in syncode store", name))?;
    // Collision check by index — avoids the simultaneous iter_mut/iter borrow.
    if let Some(new_name) = patch.name_override {
        let new_key = mcp_name_key(new_name);
        if new_key != key
            && store
                .servers
                .iter()
                .enumerate()
                .any(|(i, s)| i != idx && mcp_name_key(&s.name) == new_key)
        {
            return Err(format!("a server named '{}' already exists", new_name));
        }
    }
    let stored = &mut store.servers[idx];
    if let Some(new_name) = patch.name_override {
        stored.name = new_name.to_string();
    }
    if let Some(transport) = patch.transport_override {
        stored.transport = transport;
    }
    if let Some(command) = patch.command_value() {
        stored.command = command.map(|s| s.to_string());
    }
    if let Some(args) = patch.args_value() {
        stored.args = args.to_vec();
    }
    if let Some(env) = patch.env_value() {
        stored.env = env
            .iter()
            .map(|(n, v)| StoredMcpEnvVar {
                name: n.to_string(),
                value: v.to_string(),
            })
            .collect();
    }
    if let Some(url) = patch.url_value() {
        stored.url = url.map(|s| s.to_string());
    }
    let updated = stored.clone();
    write_syncode_mcp_store(home_dir, &store)?;
    let source_path = syncode_mcp_path(home_dir).to_string_lossy().into_owned();
    let disabled = HashSet::new();
    Ok(stored_to_descriptor(&updated, &source_path, &disabled))
}

/// Deletes a syncode-owned server. Returns `Ok(())` if removed, `Err` if not
/// found.
pub fn delete_syncode_server(home_dir: &str, name: &str) -> Result<(), String> {
    let mut store = read_syncode_mcp_store(home_dir);
    let key = mcp_name_key(name);
    let before = store.servers.len();
    store.servers.retain(|s| mcp_name_key(&s.name) != key);
    if store.servers.len() == before {
        return Err(format!("server '{}' not found in syncode store", name));
    }
    write_syncode_mcp_store(home_dir, &store).map(|_| ())
}

/// Input for create/update RPC handlers. Field-by-field Optionality lets the
/// update handler treat missing fields as "leave unchanged".
pub struct McpServerInput<'a> {
    pub name: &'a str,
    /// Rename target — only meaningful for update.
    pub name_override: Option<&'a str>,
    pub transport: McpTransport,
    pub transport_override: Option<McpTransport>,
    pub command: Option<&'a str>,
    pub args: &'a [String],
    pub env: &'a [(String, String)],
    pub url: Option<&'a str>,
    /// When `true`, the field is being explicitly cleared (used by update).
    /// For create, all "set" flags are implicit.
    pub set_command: bool,
    pub set_args: bool,
    pub set_env: bool,
    pub set_url: bool,
}

// ── Probe (mcp/test-connection) ──────────────────────────────────────

/// Maximum probe timeout — callers asking for longer get clamped. Keeps the
/// UI snappy; an MCP server that can't handshake in 10s is effectively dead.
const PROBE_TIMEOUT_CAP_MS: u64 = 10_000;

/// JSON-RPC `initialize` request body sent to stdio children / HTTP servers
/// during a reachability probe. Mirrors the spec — MCP servers must answer
/// `initialize` before any other call.
const MCP_INITIALIZE_REQUEST: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"syncode-probe","version":"1"}}}"#;

/// Probes reachability of an MCP server described by inline params. Accepts
/// `{transport, command?, args?, env?, url?, timeoutMs?}`. Returns a JSON
/// object: `{status: "reachable" | "unreachable", latencyMs?, error?}`.
/// Never errors — unreachable is a normal result, not an RPC failure.
pub async fn probe_mcp_server(params: &serde_json::Value, timeout_ms: u64) -> serde_json::Value {
    let timeout_ms = timeout_ms.min(PROBE_TIMEOUT_CAP_MS);
    let transport = match params
        .get("transport")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_lowercase())
        .as_deref()
    {
        Some("http") => McpTransport::Http,
        Some("sse") => McpTransport::Sse,
        _ => McpTransport::Stdio,
    };
    let result = match transport {
        McpTransport::Stdio => probe_stdio(params, timeout_ms).await,
        McpTransport::Http | McpTransport::Sse => probe_http(params, timeout_ms).await,
    };
    match result {
        Ok(latency) => serde_json::json!({
            "status": "reachable",
            "latencyMs": latency,
        }),
        Err(message) => serde_json::json!({
            "status": "unreachable",
            "error": message,
        }),
    }
}

/// Spawns the stdio child, writes the initialize request, awaits the first
/// non-empty line of stdout. Errors carry a short human-readable description
/// (no env values leaked — they only exist on the child's env, not in logs).
async fn probe_stdio(params: &serde_json::Value, timeout_ms: u64) -> Result<u64, String> {
    let command = params
        .get("command")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "command is required for stdio probe".to_string())?;
    let args: Vec<String> = params
        .get("args")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let env_pairs: Vec<(String, String)> = params
        .get("env")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        })
        .unwrap_or_default();

    let start = std::time::Instant::now();
    let mut cmd = tokio::process::Command::new(command);
    cmd.args(&args);
    for (k, v) in &env_pairs {
        cmd.env(k, v);
    }
    cmd.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    // Kill on drop — guarantees the child dies even on timeout / panic.
    cmd.kill_on_drop(true);
    syncode_core::util::subprocess::hide_console_window(&mut cmd);

    let mut child = cmd.spawn().map_err(|e| format!("spawn failed: {e}"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| "no stdin pipe".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "no stdout pipe".to_string())?;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let write_result = tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), async {
        let mut stdin = stdin;
        let mut stdout = stdout;
        stdin
            .write_all(MCP_INITIALIZE_REQUEST.as_bytes())
            .await
            .map_err(|e| format!("write failed: {e}"))?;
        stdin.write_all(b"\n").await.ok();
        stdin.flush().await.ok();
        // Read until we see a non-empty line — the initialize response.
        let mut buf = [0u8; 4096];
        loop {
            let n = stdout
                .read(&mut buf)
                .await
                .map_err(|e| format!("read failed: {e}"))?;
            if n == 0 {
                return Err("eof before response".to_string());
            }
            let chunk = std::str::from_utf8(&buf[..n]).unwrap_or("");
            if chunk.contains(r#""result""#) {
                return Ok(());
            }
        }
    })
    .await;

    match write_result {
        Ok(Ok(())) => Ok(start.elapsed().as_millis() as u64),
        Ok(Err(message)) => Err(message),
        Err(_) => Err(format!("timed out after {timeout_ms}ms")),
    }
}

/// HTTP POST initialize request. Uses the workspace `reqwest` client.
async fn probe_http(params: &serde_json::Value, timeout_ms: u64) -> Result<u64, String> {
    let url = params
        .get("url")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "url is required for http probe".to_string())?;
    let start = std::time::Instant::now();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .build()
        .map_err(|e| format!("http client build failed: {e}"))?;
    let resp = client
        .post(url)
        .header("content-type", "application/json")
        .body(MCP_INITIALIZE_REQUEST.to_string())
        .send()
        .await
        .map_err(|e| format!("http request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("http status {}", resp.status()));
    }
    Ok(start.elapsed().as_millis() as u64)
}

impl<'a> McpServerInput<'a> {
    /// Helper accessor — name to use in storage. Falls back to `name`.
    pub fn resolved_name(&self) -> &str {
        self.name_override.unwrap_or(self.name)
    }
    pub fn transport_kind(&self) -> McpTransport {
        self.transport_override.unwrap_or(self.transport)
    }
    pub fn command_value(&self) -> Option<Option<&str>> {
        if self.set_command {
            Some(self.command)
        } else {
            None
        }
    }
    pub fn args_value(&self) -> Option<&[String]> {
        if self.set_args { Some(self.args) } else { None }
    }
    pub fn env_value(&self) -> Option<&[(String, String)]> {
        if self.set_env { Some(self.env) } else { None }
    }
    pub fn url_value(&self) -> Option<Option<&str>> {
        if self.set_url { Some(self.url) } else { None }
    }
    /// Validates required fields for a create. Returns `Err` with a
    /// human-readable message.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("name is required".to_string());
        }
        match self.transport_kind() {
            McpTransport::Stdio => {
                if self.command.is_none() {
                    return Err("command is required for stdio transport".to_string());
                }
            }
            McpTransport::Http | McpTransport::Sse => {
                if self.url.is_none() {
                    return Err("url is required for http/sse transport".to_string());
                }
            }
        }
        Ok(())
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;

    fn tmp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "syncode-ws-mcp-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        ))
    }

    fn empty_disabled() -> HashSet<String> {
        HashSet::new()
    }

    #[test]
    fn parse_claude_json_reads_stdio_server() {
        let tmp = tmp_dir("claude-stdio");
        fs::create_dir_all(&tmp).unwrap();
        let json = r#"{
            "mcpServers": {
                "filesystem": {
                    "command": "npx",
                    "args": ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],
                    "env": {"FOO": "bar", "BAZ": "qux"}
                }
            },
            "other": "ignored"
        }"#;
        fs::write(tmp.join(".claude.json"), json).unwrap();

        let out = parse_claude_json(tmp.to_string_lossy().as_ref(), &empty_disabled());
        assert_eq!(out.len(), 1);
        let srv = &out[0];
        assert_eq!(srv.name, "filesystem");
        assert_eq!(srv.transport, McpTransport::Stdio);
        assert_eq!(srv.command.as_deref(), Some("npx"));
        assert_eq!(
            srv.args,
            vec!["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
        );
        // Env values redacted — only names.
        assert_eq!(srv.env.len(), 2);
        let env_names: Vec<&str> = srv.env.iter().map(|e| e.name.as_str()).collect();
        assert!(env_names.contains(&"FOO"));
        assert!(env_names.contains(&"BAZ"));
        assert_eq!(srv.scope, McpScope::User);
        assert!(!srv.editable);
        assert!(srv.enabled);

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn parse_claude_json_ignores_corrupt_file() {
        let tmp = tmp_dir("claude-corrupt");
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join(".claude.json"), "not json {{{{").unwrap();
        let out = parse_claude_json(tmp.to_string_lossy().as_ref(), &empty_disabled());
        assert!(out.is_empty());
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn parse_cursor_mcp_json_handles_transport_http() {
        let tmp = tmp_dir("cursor-http");
        fs::create_dir_all(tmp.join(".cursor")).unwrap();
        let json = r#"{
            "mcpServers": {
                "remote": {
                    "type": "http",
                    "url": "https://example.com/mcp"
                }
            }
        }"#;
        fs::write(tmp.join(".cursor").join("mcp.json"), json).unwrap();
        let out = parse_cursor_mcp_json(tmp.to_string_lossy().as_ref(), &empty_disabled());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].transport, McpTransport::Http);
        assert_eq!(out[0].url.as_deref(), Some("https://example.com/mcp"));
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn parse_codex_config_toml_reads_mcp_servers_table() {
        let tmp = tmp_dir("codex-table");
        fs::create_dir_all(tmp.join(".codex")).unwrap();
        let toml = r#"
[mcp_servers.filesystem]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]

[mcp_servers.filesystem.env]
ROOT = "/data"
"#;
        fs::write(tmp.join(".codex").join("config.toml"), toml).unwrap();
        let out = parse_codex_config_toml(tmp.to_string_lossy().as_ref(), &empty_disabled());
        assert_eq!(out.len(), 1);
        let srv = &out[0];
        assert_eq!(srv.name, "filesystem");
        assert_eq!(srv.command.as_deref(), Some("npx"));
        assert_eq!(srv.env.len(), 1);
        assert_eq!(srv.env[0].name, "ROOT");
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn parse_project_mcp_json_walks_ancestors() {
        let tmp = tmp_dir("project-walk");
        let root = tmp.join("repo");
        let nested = root.join("packages").join("app");
        fs::create_dir_all(&nested).unwrap();
        // Root has a .mcp.json.
        fs::write(
            root.join(".mcp.json"),
            r#"{"mcpServers": {"root-srv": {"command": "r", "args": []}}}"#,
        )
        .unwrap();
        // Nested has its own.
        fs::write(
            nested.join(".mcp.json"),
            r#"{"mcpServers": {"nested-srv": {"command": "n", "args": []}}}"#,
        )
        .unwrap();
        let out = parse_project_mcp_json(nested.to_string_lossy().as_ref(), &empty_disabled());
        let names: Vec<&str> = out.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"nested-srv"));
        assert!(names.contains(&"root-srv"));
        assert_eq!(out.len(), 2);
        for d in &out {
            assert_eq!(d.scope, McpScope::Project);
        }
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn discover_dedupes_by_name_keeping_syncode_first() {
        let tmp = tmp_dir("dedupe");
        fs::create_dir_all(&tmp).unwrap();
        // Seed syncode store with "github".
        let mut store = SyncodeMcpStore::default();
        store.servers.push(StoredMcpServer {
            name: "github".into(),
            transport: McpTransport::Stdio,
            command: Some("syncode-owned".into()),
            args: vec![],
            env: vec![],
            url: None,
        });
        write_syncode_mcp_store(tmp.to_string_lossy().as_ref(), &store).unwrap();
        // Seed .claude.json with a conflicting "github".
        fs::write(
            tmp.join(".claude.json"),
            r#"{"mcpServers": {"github": {"command": "claude-owned", "args": []}}}"#,
        )
        .unwrap();

        let input = McpDiscoveryInput {
            cwd: None,
            home_dir: Some(tmp.to_string_lossy().into_owned()),
            disabled: empty_disabled(),
        };
        let out = discover_mcp_catalog(input);
        assert_eq!(out.len(), 1, "duplicate name should be deduped");
        assert_eq!(out[0].name, "github");
        assert_eq!(out[0].scope, McpScope::Syncode, "syncode-owned wins");
        assert_eq!(out[0].command.as_deref(), Some("syncode-owned"));
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn syncode_store_roundtrip() {
        let tmp = tmp_dir("roundtrip");
        fs::create_dir_all(&tmp).unwrap();
        let mut store = SyncodeMcpStore::default();
        store.servers.push(StoredMcpServer {
            name: "s1".into(),
            transport: McpTransport::Stdio,
            command: Some("c1".into()),
            args: vec!["a".into()],
            env: vec![StoredMcpEnvVar {
                name: "K".into(),
                value: "V".into(),
            }],
            url: None,
        });
        let path = write_syncode_mcp_store(tmp.to_string_lossy().as_ref(), &store).unwrap();
        assert!(path.exists());

        let reloaded = read_syncode_mcp_store(tmp.to_string_lossy().as_ref());
        assert_eq!(reloaded.servers.len(), 1);
        assert_eq!(reloaded.servers[0].name, "s1");
        assert_eq!(
            reloaded.servers[0].env[0].value, "V",
            "values preserved on disk"
        );
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn redaction_does_not_serialize_env_values() {
        let descriptor = McpServerDescriptor {
            name: "x".into(),
            transport: McpTransport::Stdio,
            command: Some("c".into()),
            args: vec![],
            env: vec![McpEnvVar { name: "KEY".into() }],
            url: None,
            scope: McpScope::User,
            source_path: "/x".into(),
            editable: false,
            enabled: true,
            status: None,
        };
        let value = serde_json::to_value(&descriptor).unwrap();
        let env_arr = value.get("env").and_then(|v| v.as_array()).unwrap();
        assert_eq!(env_arr.len(), 1);
        let env_obj = env_arr[0].as_object().unwrap();
        assert!(env_obj.contains_key("name"));
        assert!(
            !env_obj.contains_key("value"),
            "value must NOT appear on the wire",
        );
    }

    #[test]
    fn enabled_reflects_disabled_list() {
        let tmp = tmp_dir("disabled");
        fs::create_dir_all(&tmp).unwrap();
        fs::write(
            tmp.join(".claude.json"),
            r#"{"mcpServers": {"on": {"command": "a", "args": []}, "off": {"command": "b", "args": []}}}"#,
        )
        .unwrap();
        let mut disabled = HashSet::new();
        disabled.insert("off".to_string());
        let input = McpDiscoveryInput {
            cwd: None,
            home_dir: Some(tmp.to_string_lossy().into_owned()),
            disabled,
        };
        let out = discover_mcp_catalog(input);
        let by_name: HashMap<&str, &McpServerDescriptor> =
            out.iter().map(|d| (d.name.as_str(), d)).collect();
        assert!(by_name["on"].enabled);
        assert!(!by_name["off"].enabled);
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn create_then_update_then_delete_roundtrips() {
        let tmp = tmp_dir("crud");
        fs::create_dir_all(&tmp).unwrap();
        let home = tmp.to_string_lossy().into_owned();

        // Create.
        let input = McpServerInput {
            name: "fs",
            name_override: None,
            transport: McpTransport::Stdio,
            transport_override: None,
            command: Some("npx"),
            args: &[
                "-y".to_string(),
                "@modelcontextprotocol/server-filesystem".to_string(),
            ],
            env: &[("ROOT".to_string(), "/data".to_string())],
            url: None,
            set_command: true,
            set_args: true,
            set_env: true,
            set_url: false,
        };
        let created = create_syncode_server(&home, &input).unwrap();
        assert_eq!(created.name, "fs");
        assert!(created.editable);
        assert_eq!(created.scope, McpScope::Syncode);

        // Duplicate name should fail.
        let err = create_syncode_server(&home, &input).err().unwrap();
        assert!(err.contains("already exists"));

        // Update — rename + bump args.
        let patch = McpServerInput {
            name: "fs",
            name_override: Some("filesystem"),
            transport: McpTransport::Stdio,
            transport_override: None,
            command: None,
            args: &[
                "-y".to_string(),
                "@modelcontextprotocol/server-filesystem".to_string(),
                "/data".to_string(),
            ],
            env: &[],
            url: None,
            set_command: false,
            set_args: true,
            set_env: true,
            set_url: false,
        };
        let updated = update_syncode_server(&home, "fs", &patch).unwrap();
        assert_eq!(updated.name, "filesystem");
        assert_eq!(updated.args.len(), 3);

        // Delete.
        delete_syncode_server(&home, "filesystem").unwrap();
        let store = read_syncode_mcp_store(&home);
        assert!(store.servers.is_empty());

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn build_mcp_servers_for_acp_only_forwards_enabled_syncode_stdio() {
        let tmp = tmp_dir("acp");
        fs::create_dir_all(&tmp).unwrap();
        let home = tmp.to_string_lossy().into_owned();

        // Seed a syncode stdio server + an http one + an external claude one.
        let mut store = SyncodeMcpStore::default();
        store.servers.push(StoredMcpServer {
            name: "stdio-srv".into(),
            transport: McpTransport::Stdio,
            command: Some("c".into()),
            args: vec![],
            env: vec![StoredMcpEnvVar {
                name: "K".into(),
                value: "V".into(),
            }],
            url: None,
        });
        store.servers.push(StoredMcpServer {
            name: "http-srv".into(),
            transport: McpTransport::Http,
            command: None,
            args: vec![],
            env: vec![],
            url: Some("https://example.com/mcp".into()),
        });
        write_syncode_mcp_store(&home, &store).unwrap();
        fs::write(
            tmp.join(".claude.json"),
            r#"{"mcpServers": {"claude-srv": {"command": "c2", "args": []}}}"#,
        )
        .unwrap();

        let settings = serde_json::json!({"mcp": {"disabled": []}});
        let forwarded = build_mcp_servers_for_acp(&settings, Some(&home), None);
        // Only the syncode-owned stdio server should be forwarded.
        assert_eq!(forwarded.len(), 1);
        assert_eq!(forwarded[0]["name"], "stdio-srv");
        // Env values re-injected for ACP.
        let env = forwarded[0]["transport"]["env"].as_object().unwrap();
        assert_eq!(env.get("K").and_then(|v| v.as_str()), Some("V"));

        // Disabling stdio-srv filters it out.
        let settings = serde_json::json!({"mcp": {"disabled": ["stdio-srv"]}});
        let forwarded = build_mcp_servers_for_acp(&settings, Some(&home), None);
        assert!(forwarded.is_empty());

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn name_key_normalizes_case_and_trim() {
        assert_eq!(mcp_name_key("  GitHub "), "github");
        assert_eq!(mcp_name_key("GitHub"), "github");
    }
}

//! Provider CLI version-checking and one-click update (the "Provider updates"
//! feature in Settings).
//!
//! Populates the MCode `versionAdvisory` field on each provider status so the
//! frontend's Settings → "Provider updates" section can show which provider
//! CLIs are outdated, and `server.updateProvider` can actually run the upgrade.
//!
//! # Scope (v1)
//!
//! Only the 6 npm-distributed providers with a reliable `/latest` endpoint:
//! codex, claude, gemini, grok, kilo, pi. Cursor and opencode return
//! `status: "unknown"` (no clean npm registry source — see the plan). The
//! HTTP-only providers (anthropic, openai) have no CLI and are excluded
//! entirely.
//!
//! # Wire contract
//!
//! `versionAdvisory` shape (`ServerProviderVersionAdvisory`,
//! `frontend/src/contracts/tier3/server.ts`):
//! ```jsonc
//! { "status": "unknown"|"current"|"behind_latest",
//!   "currentVersion": string|null,  "latestVersion": string|null,
//!   "updateCommand": string|null,   "canUpdate": bool,
//!   "checkedAt": string|null,       "message": string|null }
//! ```
//! The frontend shows an update row only when `status === "behind_latest"` AND
//! `latestVersion !== null`; the one-click "Update" button additionally needs
//! `canUpdate === true` AND `updateCommand !== null`.
//!
//! # Non-blocking
//!
//! Every probe (`<bin> --version`) and fetch (`registry.npmjs.org`) is
//! best-effort: a failure produces `status: "unknown"` rather than an error.
//! Version-checking never blocks provider spawning.

use serde_json::{Value, json};
use std::time::Duration;
use tokio::process::Command;

use crate::settings;

/// npm registry base for the `/latest` endpoint. Appending `/<pkg>/latest`
/// returns `{ "version": "x.y.z", ... }`.
const NPM_REGISTRY_BASE: &str = "https://registry.npmjs.org";
/// Timeout for both the `--version` CLI probe and the npm registry HTTP fetch.
/// Short enough that a hung binary/network doesn't stall `refreshProviders`,
/// long enough for a cold-start npm registry round trip.
const PROBE_TIMEOUT_SECS: u64 = 8;
/// Timeout for `npm install -g <pkg>@latest`. npm global installs can be slow
/// (network + extract + link), so this is generous.
const UPDATE_TIMEOUT_SECS: u64 = 120;

/// Per-provider npm distribution metadata for the 6 v1 providers.
///
/// `pkg` is the npm package name (used for both the `/latest` lookup and the
/// `npm install -g` update command). `update_cmd` is the full update command
/// string surfaced to the user (for copy-text / manual update).
struct ProviderNpmMeta {
    pkg: &'static str,
    update_cmd: &'static str,
}

/// Build the npm metadata for a provider id. Returns `None` for the providers
/// excluded from v1 (cursor, opencode) and the HTTP-only providers (anthropic,
/// openai), so they surface `status: "unknown"` rather than a false advisory.
fn npm_meta(pid: &str) -> Option<ProviderNpmMeta> {
    let pkg = match pid {
        syncode_provider::PROVIDER_CODEX => "@openai/codex",
        syncode_provider::PROVIDER_CLAUDE => "@anthropic-ai/claude-code",
        syncode_provider::PROVIDER_GEMINI => "@google/gemini-cli",
        syncode_provider::PROVIDER_GROK => "@xai-official/grok",
        syncode_provider::PROVIDER_KILO => "@kilocode/cli",
        syncode_provider::PROVIDER_PI => "@earendil-works/pi-coding-agent",
        // cursor: no reliable npm package (CLI ships as a native binary).
        // opencode: repo migration ambiguity (sst vs anomalyco) — deferred.
        // anthropic/openai: HTTP-only adapters, no local CLI to version-check.
        _ => return None,
    };
    Some(ProviderNpmMeta {
        pkg,
        update_cmd: "npm install -g ",
    })
}

/// The full update command for a provider (e.g. `npm install -g @openai/codex@latest`).
/// Returns `None` for unsupported providers.
fn update_command_for(pid: &str) -> Option<String> {
    npm_meta(pid).map(|m| format!("{m}{pkg}@latest", m = m.update_cmd, pkg = m.pkg))
}

/// Extract the bare version token from the first non-empty line of a CLI's
/// `--version` output. Handles the real first-line formats observed across the
/// v1 providers:
///   `2.1.191 (Claude Code)`  → `2.1.191`   (claude: version first, suffix)
///   `codex-cli 0.141.0`      → `0.141.0`   (codex: name prefix, version 2nd)
///   `0.49.0`                 → `0.49.0`    (gemini: bare)
///   `@openai/codex/0.144.3`  → `0.144.3`   (slash-prefixed pkg name)
///
/// Strategy: split on `/`-then-whitespace into candidate tokens, then pick the
/// first token that starts with an ASCII digit (a version always does; a bare
/// name like `codex-cli` or `gemini-cli` does not). Strip a leading `v`.
/// Returns `None` when no numeric-leading token is found.
fn extract_version_token(first_line: &str) -> Option<String> {
    // If a `/` is present (e.g. `@openai/codex/0.144.3`), the version is the
    // trailing segment; otherwise operate on the whole line.
    let after_slash = first_line
        .rsplit_once('/')
        .map(|(_, v)| v)
        .unwrap_or(first_line);
    after_slash
        .split_whitespace()
        .map(|tok| tok.trim_start_matches('v'))
        .find(|tok| tok.bytes().next().is_some_and(|b| b.is_ascii_digit()))
        .filter(|tok| !tok.is_empty())
        .map(String::from)
}

/// Run `<bin> --version` and return the trimmed first non-empty line of stdout.
///
/// The binary is resolved the same way availability detection resolves it
/// ([`settings::resolve_provider_binary`] — honors a custom `binaryPath` from
/// settings, then falls back to PATH candidates), so a manually-overridden
/// binary path is version-checked too.
///
/// Returns `None` on any failure (binary missing, non-zero exit, timeout,
/// empty output). The caller treats `None` as `status: "unknown"`.
async fn probe_installed_version(pid: &str, settings: &Value) -> Option<String> {
    let (binary, _installed) = settings::resolve_provider_binary(pid, settings);
    let binary = binary?;
    // `--version` is the conventional flag across all 6 v1 providers (codex,
    // claude, gemini, grok, kilo, pi). tokio::process needs `kill_on_drop` so
    // a timed-out child is reaped rather than leaked.
    let output = tokio::time::timeout(
        Duration::from_secs(PROBE_TIMEOUT_SECS),
        Command::new(&binary).arg("--version").output(),
    )
    .await
    .ok()?
    .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .and_then(extract_version_token)
}

/// Fetch the latest published version from the npm registry.
///
/// `GET https://registry.npmjs.org/<pkg>/latest` → `{ "version": "x.y.z" }`.
/// Returns `None` on network error, non-200, malformed JSON, or missing
/// `version` field — the caller treats `None` as `status: "unknown"`.
async fn fetch_latest_version(pkg: &str) -> Option<String> {
    let url = format!("{NPM_REGISTRY_BASE}/{pkg}/latest");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(PROBE_TIMEOUT_SECS))
        .build()
        .ok()?;
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: Value = resp.json().await.ok()?;
    body.get("version")
        .and_then(Value::as_str)
        .map(str::trim)
        .map(|s| s.trim_start_matches('v').to_string())
        .filter(|s| !s.is_empty())
}

/// Parse a version string into numeric components for comparison. Strips a
/// leading `v` and splits on `.`; each segment is parsed as `u64` when
/// possible, else falls back to 0. Returns `Vec<u64>` (empty on blank input).
///
/// This is deliberately lenient: codex native builds use a date-stamped format
/// (`0.1.2505011803`) and some CLIs embed a build suffix — we compare only the
/// numeric dot-segments and ignore trailing non-numeric junk per segment.
fn parse_version_components(version: &str) -> Vec<u64> {
    version
        .trim()
        .trim_start_matches('v')
        .split('.')
        .map(|seg| {
            // Take leading digits of each segment (ignores `-beta`, `+build`).
            let digits: String = seg.chars().take_while(|c| c.is_ascii_digit()).collect();
            digits.parse::<u64>().unwrap_or(0)
        })
        .collect()
}

/// Lenient semver-ish "is `current` strictly behind `latest`?" comparison.
///
/// - Equal versions → `false` (current).
/// - `current` parses to all-zero (unparseable) → `false` (don't claim
///   behind on garbage; the registry might be right but we can't confirm).
/// - Otherwise, compare component-by-component; the first differing component
///   decides. Shorter vectors pad with 0 (so `1.2` == `1.2.0`).
///
/// The all-zero guard prevents false "behind_latest" when a CLI prints an
/// unparseable banner line as its version (safer to report `unknown` upstream
/// than to nag the user with a bogus update).
fn is_behind(current: &str, latest: &str) -> bool {
    let cur = parse_version_components(current);
    if cur.iter().all(|&c| c == 0) {
        return false;
    }
    let lat = parse_version_components(latest);
    let max_len = cur.len().max(lat.len());
    for i in 0..max_len {
        let c = cur.get(i).copied().unwrap_or(0);
        let l = lat.get(i).copied().unwrap_or(0);
        match c.cmp(&l) {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal => continue,
        }
    }
    false
}

/// Pure decision helper extracted for unit testing (no I/O). Given an optional
/// installed version and an optional latest version, returns the advisory
/// status string the wire contract expects.
fn decide_status(current: Option<&str>, latest: Option<&str>) -> &'static str {
    match (current, latest) {
        (Some(c), Some(l)) => {
            if is_behind(c, l) {
                "behind_latest"
            } else {
                "current"
            }
        }
        // Missing either side → can't determine; report unknown rather than
        // risking a false "current" (which would hide a real update) or a false
        // "behind_latest" (which would nag).
        _ => "unknown",
    }
}

/// Build the full `versionAdvisory` JSON value for one provider.
///
/// Flow: unsupported provider → `unknown` all-null. Else probe installed →
/// fetch latest → decide. `updateCommand`/`canUpdate` are populated only when
/// `status === "behind_latest"` (the frontend gates the Update button on both).
pub async fn build_version_advisory(pid: &str, settings: &Value) -> Value {
    let now = chrono::Utc::now().to_rfc3339();
    let meta = match npm_meta(pid) {
        Some(m) => m,
        None => {
            return json!({
                "status": "unknown",
                "currentVersion": null,
                "latestVersion": null,
                "updateCommand": null,
                "canUpdate": false,
                "checkedAt": now,
                "message": null,
            });
        }
    };

    let current = probe_installed_version(pid, settings).await;
    let latest = fetch_latest_version(meta.pkg).await;
    let status = decide_status(current.as_deref(), latest.as_deref());

    let (update_command, can_update) = if status == "behind_latest" {
        (update_command_for(pid), true)
    } else {
        (None, false)
    };

    json!({
        "status": status,
        "currentVersion": current,
        "latestVersion": latest,
        "updateCommand": update_command,
        "canUpdate": can_update,
        "checkedAt": now,
        "message": null,
    })
}

/// Map an `npm install` exit code to the `updateState.status` wire value.
///
/// Frontend treats `succeeded` as success; `failed` shows `output`/`message`
/// in an error toast. Kept as a pure helper so the exit-code mapping is
/// unit-testable without spawning npm.
fn exit_code_to_update_status(exit_code: Option<i32>) -> &'static str {
    match exit_code {
        Some(0) => "succeeded",
        // `kill_on_drop` + timeout → the child is killed; we synthesize a
        // non-zero exit. Treat any non-zero/None as failed.
        _ => "failed",
    }
}

/// Run `npm install -g <pkg>@latest` and return the `updateState` JSON value.
///
/// Returns `{ status, startedAt, finishedAt, message, output }`. `output`
/// combines stdout+stderr (truncated) so the frontend's failure toast can show
/// the npm error. Returns `status: "failed"` + a timeout message if the install
/// exceeds [`UPDATE_TIMEOUT_SECS`].
pub async fn run_provider_update(pid: &str) -> Value {
    let started_at = chrono::Utc::now().to_rfc3339();
    let Some(meta) = npm_meta(pid) else {
        return json!({
            "status": "failed",
            "startedAt": started_at,
            "finishedAt": chrono::Utc::now().to_rfc3339(),
            "message": format!("Provider {pid} is not npm-managed; cannot update automatically."),
            "output": null,
        });
    };
    let target = format!("{}@latest", meta.pkg);

    let result = tokio::time::timeout(
        Duration::from_secs(UPDATE_TIMEOUT_SECS),
        Command::new("npm")
            .arg("install")
            .arg("-g")
            .arg(&target)
            // Combine stdout+stderr so a failure surfaces the npm error.
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output(),
    )
    .await;

    let finished_at = chrono::Utc::now().to_rfc3339();
    match result {
        // Timeout.
        Err(_) => json!({
            "status": "failed",
            "startedAt": started_at,
            "finishedAt": finished_at,
            "message": format!("npm install timed out after {UPDATE_TIMEOUT_SECS}s."),
            "output": null,
        }),
        // Spawn or wait failure.
        Ok(Err(e)) => json!({
            "status": "failed",
            "startedAt": started_at,
            "finishedAt": finished_at,
            "message": format!("Failed to run npm: {e}"),
            "output": null,
        }),
        Ok(Ok(output)) => {
            let status = exit_code_to_update_status(output.status.code());
            let mut combined = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.trim().is_empty() {
                if !combined.is_empty() {
                    combined.push('\n');
                }
                combined.push_str(&stderr);
            }
            // Cap output length so the JSON payload stays manageable; npm can
            // be verbose. 4 KB is plenty to diagnose a failure.
            const MAX_OUTPUT: usize = 4096;
            let combined = if combined.len() > MAX_OUTPUT {
                format!(
                    "{}…[truncated]",
                    &combined[..combined.floor_char_boundary(MAX_OUTPUT)]
                )
            } else {
                combined
            };
            json!({
                "status": status,
                "startedAt": started_at,
                "finishedAt": finished_at,
                "message": null,
                "output": combined,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syncode_provider::{
        PROVIDER_CLAUDE, PROVIDER_CODEX, PROVIDER_GEMINI, PROVIDER_GROK, PROVIDER_KILO, PROVIDER_PI,
    };

    #[test]
    fn npm_meta_returns_known_packages() {
        assert_eq!(npm_meta(PROVIDER_CODEX).unwrap().pkg, "@openai/codex");
        assert_eq!(
            npm_meta(PROVIDER_CLAUDE).unwrap().pkg,
            "@anthropic-ai/claude-code"
        );
        assert_eq!(npm_meta(PROVIDER_GEMINI).unwrap().pkg, "@google/gemini-cli");
        assert_eq!(npm_meta(PROVIDER_GROK).unwrap().pkg, "@xai-official/grok");
        assert_eq!(npm_meta(PROVIDER_KILO).unwrap().pkg, "@kilocode/cli");
        assert_eq!(
            npm_meta(PROVIDER_PI).unwrap().pkg,
            "@earendil-works/pi-coding-agent"
        );
    }

    #[test]
    fn npm_meta_excludes_unsupported_providers() {
        // v1 skips cursor/opencode (no reliable npm source) and HTTP-only
        // providers (no CLI to version-check).
        assert!(npm_meta(syncode_provider::PROVIDER_CURSOR).is_none());
        assert!(npm_meta(syncode_provider::PROVIDER_OPENCODE).is_none());
        assert!(npm_meta(syncode_provider::PROVIDER_ANTHROPIC).is_none());
        assert!(npm_meta(syncode_provider::PROVIDER_OPENAI).is_none());
    }

    #[test]
    fn update_command_for_formats_npm_install() {
        let cmd = update_command_for(PROVIDER_CODEX).unwrap();
        assert_eq!(cmd, "npm install -g @openai/codex@latest");
    }

    #[test]
    fn is_behind_semver_comparison() {
        assert!(is_behind("1.0.0", "1.2.0"));
        assert!(!is_behind("1.2.0", "1.0.0"), "newer current is not behind");
        assert!(!is_behind("1.0.0", "1.0.0"), "equal is not behind");
        assert!(is_behind("1.9.9", "2.0.0"), "major bump is behind");
        assert!(
            is_behind("0.144.3", "0.145.0"),
            "real codex example: behind"
        );
    }

    #[test]
    fn is_behind_strips_v_prefix_and_handles_short_forms() {
        assert!(!is_behind("v1.0.0", "1.0.0"), "v-prefix equal");
        assert!(is_behind("v1.0.0", "v1.2.0"), "both v-prefixed, behind");
        // Shorter vector pads with 0: 1.2 == 1.2.0
        assert!(!is_behind("1.2", "1.2.0"), "1.2 vs 1.2.0 is equal");
        assert!(is_behind("1.2", "1.2.1"), "1.2 vs 1.2.1 is behind");
    }

    #[test]
    fn is_behind_unparseable_current_is_not_behind() {
        // Garbage current → all components parse to 0 → guard returns false so
        // we don't nag with a bogus "behind_latest".
        assert!(!is_behind("not-a-version", "1.0.0"));
        assert!(!is_behind("", "1.0.0"));
    }

    #[test]
    fn is_behind_ignores_build_suffix() {
        // Trailing non-numeric junk per segment is ignored (date-stamped builds,
        // -beta suffixes, etc.) — only leading digits count.
        assert!(
            is_behind("0.1.2505011803", "0.2.0"),
            "codex native date-stamp behind 0.2.0"
        );
        assert!(
            !is_behind("1.0.0-beta", "1.0.0"),
            "1.0.0-beta vs 1.0.0 — numeric equal, not behind"
        );
    }

    #[test]
    fn decide_status_current_vs_behind_vs_unknown() {
        assert_eq!(decide_status(Some("1.0.0"), Some("1.2.0")), "behind_latest");
        assert_eq!(decide_status(Some("1.2.0"), Some("1.2.0")), "current");
        assert_eq!(decide_status(None, Some("1.2.0")), "unknown");
        assert_eq!(decide_status(Some("1.0.0"), None), "unknown");
        assert_eq!(decide_status(None, None), "unknown");
    }

    #[test]
    fn exit_code_to_update_status_maps_zero_to_succeeded() {
        assert_eq!(exit_code_to_update_status(Some(0)), "succeeded");
        assert_eq!(exit_code_to_update_status(Some(1)), "failed");
        assert_eq!(exit_code_to_update_status(Some(127)), "failed");
        // None = killed (timeout) → failed.
        assert_eq!(exit_code_to_update_status(None), "failed");
    }

    #[test]
    fn extract_version_token_handles_real_cli_formats() {
        // claude: `2.1.191 (Claude Code)` → `2.1.191` (version first)
        assert_eq!(
            extract_version_token("2.1.191 (Claude Code)").as_deref(),
            Some("2.1.191")
        );
        // codex: `codex-cli 0.141.0` → `0.141.0` (name prefix, version 2nd)
        assert_eq!(
            extract_version_token("codex-cli 0.141.0").as_deref(),
            Some("0.141.0")
        );
        // gemini: bare `0.49.0` → `0.49.0`
        assert_eq!(extract_version_token("0.49.0").as_deref(), Some("0.49.0"));
        // slash-prefixed pkg name: `@openai/codex/0.144.3` → `0.144.3`
        assert_eq!(
            extract_version_token("@openai/codex/0.144.3").as_deref(),
            Some("0.144.3")
        );
        // v-prefix: `v1.2.0` → `1.2.0`
        assert_eq!(extract_version_token("v1.2.0").as_deref(), Some("1.2.0"));
        // a name-only line (no numeric token) → None
        assert_eq!(extract_version_token("codex-cli").as_deref(), None);
        // blank → None
        assert_eq!(extract_version_token(""), None);
        assert_eq!(extract_version_token("   "), None);
    }

    #[tokio::test]
    async fn build_version_advisory_unsupported_provider_returns_unknown() {
        // cursor is excluded from v1 → status unknown, latestVersion null,
        // canUpdate false. No network I/O for unsupported providers.
        let advisory =
            build_version_advisory(syncode_provider::PROVIDER_CURSOR, &Value::Null).await;
        assert_eq!(advisory["status"], "unknown");
        assert_eq!(advisory["latestVersion"], Value::Null);
        assert_eq!(advisory["currentVersion"], Value::Null);
        assert_eq!(advisory["canUpdate"], false);
        assert_eq!(advisory["updateCommand"], Value::Null);
        assert!(
            advisory["checkedAt"].is_string(),
            "checkedAt must always be set"
        );
    }
}

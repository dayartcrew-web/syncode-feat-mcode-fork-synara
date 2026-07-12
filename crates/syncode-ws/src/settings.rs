//! In-memory server settings (T6c-18) — REAL persistence for the server
//! session, with optional on-disk write-through (SRV-1).
//!
//! The cloned MCode UI persists user edits via `server.setConfig` /
//! `updateSettings` / `patchSettings` / `updateProvider` /
//! `upsertKeybinding`. The in-memory store defined here makes those edits
//! durable for the lifetime of the WebSocket server: reads return the stored
//! value, writes merge into it, and push events fan out the new state to
//! subscribed connections.
//!
//! The store holds two top-level JSON documents:
//!   - `config`   — the MCode `ServerConfig` shape
//!     (`frontend/src/contracts/tier3/server.ts`).
//!   - `settings` — the MCode `ServerSettings` shape (same source).
//!
//! Both are initialized from the canonical default builders
//! (`build_default_server_config` / `build_default_server_settings`) at
//! `WsState` construction time. The `auth_mode` string is captured then so
//! the config's `authMode` field stays consistent across the session (the
//! UI doesn't read it today, but it's a cheap accurate signal).
//!
//! # On-disk persistence (SRV-1)
//!
//! When a SQLite pool is attached via [`ServerSettingsState::with_pool`] /
//! [`ServerSettingsState::attach_pool`], the constructor loads any previously
//! persisted `config`/`settings` documents from the `server_config` /
//! `server_settings` tables (falling back to defaults on a fresh DB), and
//! every mutation (`set_config` / `update_settings` / `patch_settings` /
//! `upsert_keybinding` / `update_provider`) write-throughs to disk so the
//! edits survive a server restart. Without a pool the store is purely
//! in-memory (backward-compatible with tests and `new_in_memory`).
//!
//! `merge_json` is a recursive JSON deep-merge used by `patchSettings` /
//! `updateSettings` to apply a partial patch: objects are merged key-by-key,
//! arrays and scalars are replaced wholesale (the MCode
//! `ServerSettingsPatch` semantics — e.g. `skills.disabled` is replaced, not
//! appended).

use serde_json::{Map, Value};
use std::collections::HashMap;
use syncode_persistence::SqlitePool;
use syncode_provider::PROVIDER_CLAUDE;
#[cfg(test)]
use syncode_provider::{
    PROVIDER_CODEX, PROVIDER_CURSOR, PROVIDER_GEMINI, PROVIDER_GROK, PROVIDER_KILO,
    PROVIDER_OPENCODE, PROVIDER_PI,
};

/// In-memory server settings — persists during the server session, with
/// optional on-disk write-through (SRV-1).
///
/// Stored as opaque `serde_json::Value` rather than typed structs because the
/// MCode `ServerConfig`/`ServerSettings` schemas are large and partially
/// optional; the handlers below touch a handful of fields each, and a Value
/// avoids drifting from the contracts layer when MCode evolves. The shapes
/// are validated structurally at the handler boundary (reject non-object
/// patches with `-32602`).
///
/// `pool` is `None` for in-memory/test deployments (the historical behavior —
/// edits don't survive a restart). When `Some`, mutations write-through to the
/// `server_config` / `server_settings` SQLite tables and the constructor loads
/// any prior document.
#[derive(Debug, Clone)]
pub struct ServerSettingsState {
    /// `ServerConfig` document. Initialized from `build_default_server_config`,
    /// or loaded from disk when a pool is attached.
    pub config: Value,
    /// `ServerSettings` document. Initialized from
    /// `build_default_server_settings`, or loaded from disk when a pool is
    /// attached.
    pub settings: Value,
    /// Optional SQLite pool for on-disk persistence. `None` for in-memory
    /// deployments (backward-compatible with `new_in_memory` tests). When
    /// `Some`, [`Self::persist_config`] / [`Self::persist_settings`] write
    /// the documents to the `server_config` / `server_settings` tables.
    pub pool: Option<SqlitePool>,
}

impl ServerSettingsState {
    /// Build the default in-memory state (no disk persistence). `auth_mode` is
    /// the syncode `WsAuthConfig` mode string (`unsafe-no-auth` |
    /// `remote-reachable` | …) surfaced in the config's `authMode` field. Kept
    /// here (rather than reading `WsState` at materialize time) so the store
    /// can be built before `WsState` is fully assembled.
    ///
    /// This is the backward-compatible constructor — no pool is attached, so
    /// mutations are in-memory only. Use [`Self::with_pool`] to enable disk
    /// persistence.
    pub fn new(auth_mode: String) -> Self {
        Self {
            config: build_default_server_config(&auth_mode),
            settings: build_default_server_settings(),
            pool: None,
        }
    }

    /// Build the state backed by a SQLite pool, loading any persisted
    /// `config`/`settings` documents from disk. Falls back to defaults when
    /// the tables are empty (fresh DB or pre-SRV-1 schema) — identical to the
    /// in-memory behavior.
    ///
    /// The `auth_mode` is used to seed the default config's `authMode` field
    /// **only when no document was previously persisted**. A persisted config
    /// wins (the stored `authMode` is restored verbatim).
    pub async fn with_pool(auth_mode: String, pool: SqlitePool) -> Self {
        let config = match syncode_persistence::settings_store::load_config(&pool).await {
            Ok(Some(stored)) => stored,
            Ok(None) => build_default_server_config(&auth_mode),
            // A load failure is non-fatal: fall back to defaults and keep the
            // pool attached so subsequent writes can still attempt to persist
            // (the schema is created by init_database, so this is rare — e.g.
            // a transient lock). Logged for diagnostics.
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to load persisted server_config — falling back to defaults"
                );
                build_default_server_config(&auth_mode)
            }
        };
        let settings = match syncode_persistence::settings_store::load_settings(&pool).await {
            Ok(Some(stored)) => stored,
            Ok(None) => build_default_server_settings(),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to load persisted server_settings — falling back to defaults"
                );
                build_default_server_settings()
            }
        };
        Self {
            config,
            settings,
            pool: Some(pool),
        }
    }

    /// Attach a SQLite pool to an existing in-memory store, loading any
    /// persisted documents from disk (overriding the in-memory values). Used
    /// when the pool is constructed after the store (e.g. the server binary
    /// builds `WsState` then attaches the pool). The in-memory documents are
    /// replaced by the on-disk values when present, else left as-is (defaults).
    pub async fn attach_pool(&mut self, pool: SqlitePool) {
        if let Ok(Some(stored)) = syncode_persistence::settings_store::load_config(&pool).await {
            self.config = stored;
        }
        if let Ok(Some(stored)) = syncode_persistence::settings_store::load_settings(&pool).await {
            self.settings = stored;
        }
        self.pool = Some(pool);
    }

    /// Write-through the `config` document to disk (no-op when no pool is
    /// attached). Best-effort: a persistence failure is logged at `WARN` but
    /// does **not** surface to the RPC caller — the in-memory mutation has
    /// already succeeded, and failing the RPC would roll back a valid edit
    /// for a disk-only issue. The next successful write retries the upsert.
    pub async fn persist_config(&self) {
        let Some(pool) = self.pool.as_ref() else {
            return;
        };
        if let Err(e) = syncode_persistence::settings_store::save_config(pool, &self.config).await {
            tracing::warn!(error = %e, "failed to persist server_config to disk");
        }
    }

    /// Write-through the `settings` document to disk (no-op when no pool is
    /// attached). Same best-effort semantics as [`Self::persist_config`].
    pub async fn persist_settings(&self) {
        let Some(pool) = self.pool.as_ref() else {
            return;
        };
        if let Err(e) =
            syncode_persistence::settings_store::save_settings(pool, &self.settings).await
        {
            tracing::warn!(error = %e, "failed to persist server_settings to disk");
        }
    }
}

/// Build the minimal valid `ServerConfig` shape (MCode
/// `frontend/src/contracts/tier3/server.ts`). Shared by the read-side
/// `server.getConfig` handler, the write-side `server.setConfig` handler, and
/// the in-memory store initialization (`ServerSettingsState::new`).
///
/// Top-level fields returned:
/// - `cwd`: process cwd (non-empty)
/// - `worktreesDir`: `<cwd>/.synara/worktrees` (non-empty)
/// - `keybindingsConfigPath`: `<home>/.synara/keybindings.json` (non-empty)
/// - `keybindings`: empty array (no resolved rules; UI tolerates empty)
/// - `issues`: empty array (no keybinding-config validation runs)
/// - `providers`: empty array (no provider-availability probe)
/// - `availableEditors`: empty array (no editor detection)
/// - `homeDir`: always populated — `$HOME`/`$USERPROFILE`/`$HOMEDRIVE$HOMEPATH`,
///   falling back to `cwd` when none are set (the MCode frontend requires a
///   non-empty `homeDir` for "New chat" / project-picker flows)
/// - `authMode`: syncode auth mode surfaced from `WsAuthConfig`
///   (`unsafe-no-auth` | `remote-reachable` | ...). Not part of the MCode
///   `ServerConfig` schema, but harmless as an extra field and useful for
///   the UI to display the active auth policy.
pub fn build_default_server_config(auth_mode: &str) -> Value {
    let cwd = server_cwd();
    let home = server_home_dir();
    let worktrees_dir = format!("{}/.synara/worktrees", cwd.trim_end_matches('/'));
    let keybindings_path = format!(
        "{}/.synara/keybindings.json",
        home.as_deref().unwrap_or(&cwd)
    );
    // Use the supplied mode verbatim — the UI doesn't read this field today,
    // but it's a cheap, accurate signal of the active policy. Fall back to the
    // no-auth default only if the caller passed an empty string.
    let auth_mode_str = if auth_mode.trim().is_empty() {
        "unsafe-no-auth".to_string()
    } else {
        auth_mode.to_string()
    };

    // Build provider status objects for all known providers so the frontend's
    // `subscribeConfig` snapshot (which includes a `providers` array) matches
    // the `subscribeProviderStatuses` snapshot.
    let now = chrono::Utc::now().to_rfc3339();
    let default_providers: Vec<Value> = syncode_provider::ALL_PROVIDERS
        .iter()
        .map(|&pid| {
            let mcode_kind = if pid == "claude" { "claudeAgent" } else { pid };
            // Probe the provider's CLI binary on PATH so the settings/provider
            // panel reflects REAL availability — previously every provider was
            // hardcoded `available:true / authenticated`, claiming CLIs that
            // aren't installed. The binary name matches the provider id
            // (codex, claude, cursor, gemini, grok, kilo, opencode, pi).
            let binary_path = which::which(pid).ok();
            let installed = binary_path.is_some();
            serde_json::json!({
                "provider": mcode_kind,
                "status": if installed { "ready" } else { "unavailable" },
                "available": installed,
                "authStatus": if installed { "authenticated" } else { "not_installed" },
                "binaryPath": binary_path.as_ref().map(|p| p.to_string_lossy().to_string()),
                "checkedAt": now,
            })
        })
        .collect();

    // Default keybindings: the MCode frontend ships a set of default
    // ResolvedKeybindingRule entries. We emit a minimal functional set
    // (command → shortcut) that the UI recognizes. The frontend merges
    // these with its own internal defaults.
    let default_keybindings: Vec<Value> = vec![
        serde_json::json!({"command": "sidebar.toggle", "shortcut": "meta+b"}),
        serde_json::json!({"command": "chat.send", "shortcut": "meta+enter"}),
        serde_json::json!({"command": "chat.new", "shortcut": "meta+l"}),
        serde_json::json!({"command": "search.open", "shortcut": "meta+k"}),
        serde_json::json!({"command": "terminal.toggle", "shortcut": "meta+`"}),
    ];

    // Available editors: probe common editors on the system. The frontend
    // uses this list to populate the "Open in editor" picker.
    let default_editors: Vec<Value> = {
        let mut editors = Vec::new();
        // Probe for common editors via which/where
        for cmd in &[
            "code", "cursor", "zed", "subl", "idea", "webstorm", "windsurf",
        ] {
            if which::which(cmd).is_ok() {
                editors.push(Value::String(cmd.to_string()));
            }
        }
        // Always include terminal as fallback
        if !editors.contains(&Value::String("terminal".to_string())) {
            editors.push(Value::String("terminal".to_string()));
        }
        editors
    };

    let mut cfg = serde_json::json!({
        "cwd": cwd,
        "worktreesDir": worktrees_dir,
        "keybindingsConfigPath": keybindings_path,
        "keybindings": default_keybindings,
        "issues": [],
        "providers": default_providers,
        "availableEditors": default_editors,
        "authMode": auth_mode_str,
    });
    // Always populate `homeDir`. When HOME/USERPROFILE are resolvable we use
    // the real value; otherwise we fall back to `cwd` so the field is never
    // absent. The MCode frontend treats `homeDir` as required for the
    // "New chat" / project-picker flows (`useHandleNewChat` errors with
    // "Home folder is not available yet" when it is null/empty), so omitting
    // it causes a blank splash screen. Falling back to `cwd` is safe: the
    // frontend uses `homeDir` only to anchor the project tree root, and the
    // process cwd is the most reasonable anchor when no home is set.
    if let Some(obj) = cfg.as_object_mut() {
        obj.insert(
            "homeDir".into(),
            Value::String(home.unwrap_or_else(|| cwd.clone())),
        );
    }
    cfg
}

/// Build the MCode `DEFAULT_SERVER_SETTINGS` literal. Shared by the read-side
/// `server.getSettings` handler, the write-side `server.updateSettings` /
/// `patchSettings` handlers, and the in-memory store initialization. The
/// vendored UI references this exact shape for state initialization (see
/// `frontend/src/contracts/tier3/server.ts` `DEFAULT_SERVER_SETTINGS`).
/// Each provider is enabled with its conventional binary name and empty
/// `customModels`; the text-generation model selection defaults to
/// `{ provider: "codex", model: "gpt-5.4-mini" }` (matches the literal).
pub fn build_default_server_settings() -> Value {
    serde_json::json!({
        "enableAssistantStreaming": false,
        "defaultThreadEnvMode": "local",
        "addProjectBaseDirectory": "",
        "textGenerationModelSelection": {
            "provider": "codex",
            "model": "gpt-5.4-mini",
        },
        "providers": {
            "codex": { "enabled": true, "binaryPath": "codex", "customModels": [], "homePath": "" },
            "claudeAgent": { "enabled": true, "binaryPath": "claude", "customModels": [], "launchArgs": "" },
            "cursor": { "enabled": true, "binaryPath": "cursor-agent", "customModels": [], "apiEndpoint": "" },
            "gemini": { "enabled": true, "binaryPath": "gemini", "customModels": [] },
            "grok": { "enabled": true, "binaryPath": "grok", "customModels": [] },
            "kilo": { "enabled": true, "binaryPath": "kilo", "customModels": [], "serverUrl": "", "serverPassword": "" },
            "opencode": {
                "enabled": true, "binaryPath": "opencode", "customModels": [],
                "serverUrl": "", "serverPassword": "", "experimentalWebSockets": false,
            },
            "pi": { "enabled": true, "binaryPath": "pi", "customModels": [], "agentDir": "" },
        },
        "skills": { "disabled": [] },
    })
}

/// Resolve the server's process cwd as a non-empty string. Falls back to
/// `"/"` (POSIX root) when `std::env::current_dir` fails or yields an empty
/// string — the MCode `ServerConfig.cwd` is `TrimmedNonEmptyString`, so we
/// must always return a non-empty value.
pub(crate) fn server_cwd() -> String {
    std::env::current_dir()
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "/".to_string())
}

/// Resolve the user's home directory from the environment, in priority order:
///   1. `$HOME`            — POSIX (and Windows shells like Git Bash / MSYS2).
///   2. `$USERPROFILE`     — Windows (e.g. `C:\Users\name`).
///   3. `$HOMEDRIVE$HOMEPATH` — Windows legacy combo (e.g. `C:` + `\Users\name`).
///
/// Returns `None` only when none of these yield a non-empty string. Callers
/// that must always have a value (e.g. `build_default_server_config`) fall back
/// to the process cwd on `None`.
pub(crate) fn server_home_dir() -> Option<String> {
    home_dir_from_env(
        std::env::var("HOME").ok(),
        std::env::var("USERPROFILE").ok(),
        std::env::var("HOMEDRIVE").ok(),
        std::env::var("HOMEPATH").ok(),
    )
}

/// Pure resolution logic extracted from [`server_home_dir`] so it can be tested
/// without mutating the process environment (which is `unsafe` on edition
/// 2024). Same priority order: HOME → USERPROFILE → HOMEDRIVE+HOMEPATH.
fn home_dir_from_env(
    home: Option<String>,
    userprofile: Option<String>,
    home_drive: Option<String>,
    home_path: Option<String>,
) -> Option<String> {
    if let Some(h) = home.filter(|s| !s.trim().is_empty()) {
        return Some(h);
    }
    if let Some(p) = userprofile.filter(|s| !s.trim().is_empty()) {
        return Some(p);
    }
    // Windows legacy: `HOMEDRIVE` (e.g. "C:") + `HOMEPATH` (e.g. "\Users\name").
    // Some launch contexts set these even when `USERPROFILE` is absent. Both
    // halves must be present and non-empty for a usable path.
    match (home_drive, home_path) {
        (Some(drive), Some(path)) if !drive.trim().is_empty() && !path.trim().is_empty() => {
            Some(format!("{drive}{path}"))
        }
        _ => None,
    }
}

/// Recursively deep-merge `patch` into `target` (in place).
///
/// Semantics (mirrors MCode `ServerSettingsPatch`):
///   - For each key in `patch`:
///     - If both `target[key]` and `patch[key]` are objects, recurse.
///     - Otherwise, replace `target[key]` with `patch[key]` (scalars, arrays,
///       null, and object-vs-non-object mismatches all overwrite).
///   - Keys absent from `patch` are left untouched in `target`.
///   - A non-object `target` or `patch` is a no-op (the caller validates
///     object shape before calling; defensive guard for safety).
///
/// Arrays are replaced wholesale (not concatenated) — the MCode patch
/// semantics treat `skills.disabled` and provider `customModels` as full
/// replacements, not append operations.
pub fn merge_json(target: &mut Value, patch: &Value) {
    // Deep-merge only applies when both sides are objects. Any other shape
    // (scalar/array/null on either side) falls through: the caller has
    // already validated the patch is an object, so a non-object target is
    // the only realistic path here, and replacing it with the patch is the
    // correct outcome (the field was previously missing or wrong-typed).
    let (Some(target_obj), Some(patch_obj)) = (target.as_object_mut(), patch.as_object()) else {
        // Replace wholesale — preserves patch semantics for scalar/array
        // patches against a non-object target.
        *target = patch.clone();
        return;
    };
    merge_objects(target_obj, patch_obj);
}

/// Object-level deep-merge: walks `patch` keys and recurses into matching
/// object-valued entries. Extracted so the top-level `merge_json` can take
/// `&mut Value` while the recursion works on `&mut Map`.
fn merge_objects(target: &mut Map<String, Value>, patch: &Map<String, Value>) {
    for (key, patch_value) in patch {
        match target.get_mut(key) {
            Some(existing) => {
                // Both object → recurse. Anything else → replace.
                if let (Some(existing_obj), Some(patch_obj)) =
                    (existing.as_object_mut(), patch_value.as_object())
                {
                    merge_objects(existing_obj, patch_obj);
                } else {
                    *existing = patch_value.clone();
                }
            }
            None => {
                target.insert(key.clone(), patch_value.clone());
            }
        }
    }
}

// ─── Provider selection helpers (SRV-1 follow-up) ──────────────────────
//
// Before SRV-1, the orchestrator was armed with `SYNCODE_DEFAULT_PROVIDER`
// (default "opencode") before persisted settings were loaded — so the
// Settings panel's provider picker was ignored at boot. The helpers below
// reverse that precedence: persisted `textGenerationModelSelection` is now
// the source of truth, with the env var kept as an operator override.

/// Default provider id when neither persisted settings nor the env var
/// specifies one. Matches the historical default ("opencode") so pre-SRV-1
/// deployments keep booting the same adapter. Declared `pub` so
/// `bin/server.rs` can reuse it instead of keeping a private copy that might
/// drift.
pub const DEFAULT_PROVIDER: &str = "opencode";

/// Fields copied verbatim from a provider's settings entry into
/// [`syncode_provider::ProviderConfig::extra`]. The provider adapters read
/// these to locate the CLI binary, optional MCP-server credentials, and
/// per-provider launch flags. Anything not in this allowlist is ignored —
/// the MCode schema has many more fields (`customModels`, `apiKey`, …) that
/// the adapter doesn't consume from `extra` and that may carry secrets we
/// don't want to leak into adapter logs.
const PROVIDER_EXTRA_FIELDS: &[&str] = &[
    "binaryPath",
    "homePath",
    "launchArgs",
    "serverUrl",
    "serverPassword",
    "apiEndpoint",
    "agentDir",
    "experimentalWebSockets",
];

/// Normalize an MCode frontend provider kind to the backend's provider id.
///
/// The frontend persists `claudeAgent` (the MCode `ProviderKind` literal) as
/// the provider map key, while the backend's `ProviderRegistry` and
/// `syncode_orchestration::Command` use `claude`. Other ids (codex, cursor,
/// gemini, grok, kilo, opencode, pi, …) pass through unchanged. This is the
/// canonical implementation — `rpc.rs` delegates here instead of keeping a
/// private copy.
pub fn normalize_provider_id(provider_id: &str) -> &str {
    if provider_id == "claudeAgent" {
        PROVIDER_CLAUDE
    } else {
        provider_id
    }
}

/// The inverse of [`normalize_provider_id`]: returns the JSON key the MCode
/// `ServerSettings.providers` map uses for a given backend provider id. Used
/// by [`extract_provider_extras`] to look up the right entry.
fn provider_settings_key(provider_id: &str) -> &str {
    if provider_id == PROVIDER_CLAUDE {
        "claudeAgent"
    } else {
        provider_id
    }
}

/// Resolve the default provider id at orchestrator arm time. Precedence:
///   1. `settings.textGenerationModelSelection.provider` — what the user
///      picks in the Settings panel (source of truth post-SRV-1).
///   2. `env_value` (`SYNCODE_DEFAULT_PROVIDER`) — operator override.
///   3. [`DEFAULT_PROVIDER`] — backwards-compatible default.
///
/// The result is normalized (so `claudeAgent` becomes `claude`) so the
/// caller can pass it straight to the provider registry.
pub fn resolve_default_provider(settings: &Value, env_value: Option<&str>) -> String {
    if let Some(p) = settings
        .get("textGenerationModelSelection")
        .and_then(|v| v.get("provider"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        return normalize_provider_id(p).to_string();
    }
    if let Some(env) = env_value.filter(|s| !s.is_empty()) {
        return normalize_provider_id(env).to_string();
    }
    DEFAULT_PROVIDER.to_string()
}

/// Resolve the default model id at orchestrator arm time. Precedence matches
/// [`resolve_default_provider`]: settings first, then env, then empty string
/// (the provider adapter falls back to its built-in default model).
pub fn resolve_default_model(settings: &Value, env_value: Option<&str>) -> String {
    if let Some(m) = settings
        .get("textGenerationModelSelection")
        .and_then(|v| v.get("model"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        return m.to_string();
    }
    env_value
        .filter(|s| !s.is_empty())
        .unwrap_or_default()
        .to_string()
}

/// Extract per-provider extras from the persisted `ServerSettings.providers`
/// map for use in a [`syncode_provider::ProviderConfig`]. Returns an empty
/// map when:
///   - the provider isn't in `providers` (unknown / never configured), or
///   - the entry is disabled (`enabled: false`), or
///   - the `providers` object is missing entirely.
///
/// Only the allowlisted fields in [`PROVIDER_EXTRA_FIELDS`] are copied.
pub fn extract_provider_extras(provider_id: &str, settings: &Value) -> HashMap<String, Value> {
    let key = provider_settings_key(provider_id);
    let Some(entry) = settings.get("providers").and_then(|v| v.get(key)) else {
        return HashMap::new();
    };
    // Disabled providers are skipped — the adapter won't spawn, so extras
    // would be misleading (the caller treats non-empty extras as "ready").
    if entry.get("enabled").and_then(Value::as_bool) == Some(false) {
        return HashMap::new();
    }
    let mut extras = HashMap::new();
    for field in PROVIDER_EXTRA_FIELDS {
        if let Some(v) = entry.get(*field) {
            // Skip explicit nulls — they mean "no value", not "default".
            if !v.is_null() {
                extras.insert((*field).to_string(), v.clone());
            }
        }
    }
    extras
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_state_has_config_and_settings() {
        let state = ServerSettingsState::new("unsafe-no-auth".into());
        assert!(state.config.is_object());
        assert!(state.settings.is_object());
        assert_eq!(state.config["authMode"], "unsafe-no-auth");
        assert_eq!(state.settings["defaultThreadEnvMode"], "local");
        // Provider map should be populated by the default.
        assert!(state.settings["providers"]["codex"].is_object());
    }

    #[test]
    fn default_config_non_empty_cwd() {
        let cfg = build_default_server_config("unsafe-no-auth");
        assert!(!cfg["cwd"].as_str().unwrap().is_empty());
        assert!(cfg["worktreesDir"].as_str().unwrap().contains(".synara"));
    }

    // ─── PR-4-1: homeDir must always be populated ───────────────────
    //
    // The MCode frontend's `useHandleNewChat` errors with "Home folder is
    // not available yet" when `homeDir` is null/empty, leaving the splash
    // screen stuck. `build_default_server_config` must therefore always
    // emit a non-empty `homeDir`, falling back to `cwd` when no home env
    // var is set.

    #[test]
    fn default_config_always_has_non_empty_homedir() {
        // Regardless of the host's env, the field must exist and be non-empty.
        let cfg = build_default_server_config("unsafe-no-auth");
        let home_dir = cfg
            .get("homeDir")
            .and_then(|v| v.as_str())
            .expect("homeDir must always be present in the config");
        assert!(
            !home_dir.trim().is_empty(),
            "homeDir must be non-empty: {home_dir:?}"
        );
    }

    #[test]
    fn home_dir_from_env_prefers_home_over_userprofile() {
        // POSIX HOME wins even when USERPROFILE is also set.
        let resolved = home_dir_from_env(
            Some("/posix/home/tester".into()),
            Some("C:\\win\\tester".into()),
            None,
            None,
        );
        assert_eq!(resolved.as_deref(), Some("/posix/home/tester"));
    }

    #[test]
    fn home_dir_from_env_falls_back_to_userprofile() {
        // No HOME → USERPROFILE is used (typical native Windows launch).
        let resolved = home_dir_from_env(
            None,
            Some("C:\\Users\\tester".into()),
            Some("C:".into()),
            Some("\\Users\\tester".into()),
        );
        assert_eq!(resolved.as_deref(), Some("C:\\Users\\tester"));
    }

    #[test]
    fn home_dir_from_env_uses_homedrive_homepath_combo() {
        // No HOME/USERPROFILE → legacy HOMEDRIVE+HOMEPATH combo is joined.
        // This is the Windows context that previously produced a missing
        // homeDir (PR-4-1 root cause).
        let resolved = home_dir_from_env(
            None,
            None,
            Some("C:".into()),
            Some("\\Users\\tester".into()),
        );
        assert_eq!(resolved.as_deref(), Some("C:\\Users\\tester"));
    }

    #[test]
    fn home_dir_from_env_returns_none_when_all_absent() {
        // No env vars at all → None (caller falls back to cwd).
        let resolved = home_dir_from_env(None, None, None, None);
        assert_eq!(resolved, None);
    }

    #[test]
    fn home_dir_from_env_ignores_blank_values() {
        // Whitespace-only / empty strings are treated as unset, so we don't
        // surface a bogus "   " homeDir to the frontend.
        let resolved =
            home_dir_from_env(Some("   ".into()), Some("".into()), Some(" ".into()), None);
        assert_eq!(resolved, None);
    }

    #[test]
    fn home_dir_from_env_requires_both_halves_of_drive_path_combo() {
        // Only HOMEDRIVE (no HOMEPATH) is not a usable path → None.
        assert_eq!(home_dir_from_env(None, None, Some("C:".into()), None), None);
        // Only HOMEPATH (no HOMEDRIVE) is not a usable path → None.
        assert_eq!(
            home_dir_from_env(None, None, None, Some("\\Users\\x".into())),
            None
        );
    }

    #[test]
    fn default_settings_has_all_providers() {
        let s = build_default_server_settings();
        let providers = s["providers"].as_object().unwrap();
        for key in [
            "codex",
            "claudeAgent",
            "cursor",
            "gemini",
            "grok",
            "kilo",
            "opencode",
            "pi",
        ] {
            assert!(providers.contains_key(key), "missing provider: {key}");
            assert_eq!(providers[key]["enabled"], true);
        }
    }

    #[test]
    fn merge_replaces_scalar() {
        let mut target = serde_json::json!({ "enableAssistantStreaming": false });
        let patch = serde_json::json!({ "enableAssistantStreaming": true });
        merge_json(&mut target, &patch);
        assert_eq!(target["enableAssistantStreaming"], true);
    }

    #[test]
    fn merge_recurses_into_objects() {
        let mut target = serde_json::json!({
            "textGenerationModelSelection": { "provider": "codex", "model": "gpt-5.4-mini" }
        });
        let patch = serde_json::json!({ "textGenerationModelSelection": { "model": "claude-4" } });
        merge_json(&mut target, &patch);
        // Untouched sibling key preserved.
        assert_eq!(target["textGenerationModelSelection"]["provider"], "codex");
        // Patched key replaced.
        assert_eq!(target["textGenerationModelSelection"]["model"], "claude-4");
    }

    #[test]
    fn merge_replaces_arrays_wholesale() {
        let mut target = serde_json::json!({ "skills": { "disabled": ["a"] } });
        let patch = serde_json::json!({ "skills": { "disabled": ["b", "c"] } });
        merge_json(&mut target, &patch);
        let disabled = target["skills"]["disabled"].as_array().unwrap();
        assert_eq!(disabled.len(), 2);
        assert_eq!(disabled[0], "b");
        assert_eq!(disabled[1], "c");
    }

    #[test]
    fn merge_adds_new_keys() {
        let mut target = serde_json::json!({ "keep": 1 });
        let patch = serde_json::json!({ "added": 2 });
        merge_json(&mut target, &patch);
        assert_eq!(target["keep"], 1);
        assert_eq!(target["added"], 2);
    }

    #[test]
    fn merge_object_into_scalar_replaces() {
        // If the target is a scalar but the patch is an object, the patch
        // replaces wholesale — there's no object to merge into.
        let mut target = serde_json::json!(42);
        let patch = serde_json::json!({ "a": 1 });
        merge_json(&mut target, &patch);
        assert_eq!(target["a"], 1);
    }

    #[test]
    fn merge_nested_provider_field() {
        let mut target = serde_json::json!({
            "providers": { "codex": { "enabled": true, "binaryPath": "codex", "customModels": [] } }
        });
        let patch = serde_json::json!({ "providers": { "codex": { "enabled": false } } });
        merge_json(&mut target, &patch);
        assert_eq!(target["providers"]["codex"]["enabled"], false);
        // Untouched sibling keys preserved.
        assert_eq!(target["providers"]["codex"]["binaryPath"], "codex");
    }

    // ─── SRV-1: on-disk persistence tests ──────────────────────────
    //
    // Four scenarios covering the acceptance criteria:
    //   1. load-default — fresh DB → defaults (backward-compat).
    //   2. write-read   — mutate + persist → reload returns the edit.
    //   3. patch-merge  — patch (deep-merge) + persist → reload reflects merge.
    //   4. restart-survives — simulated WsState reconstruction reads the
    //      persisted documents and the edits survive.

    /// Build an in-memory SQLite pool with the SRV-1 schema initialized.
    async fn setup_pool() -> SqlitePool {
        syncode_persistence::init_database(std::path::Path::new(""))
            .await
            .expect("init_database should succeed")
    }

    #[tokio::test]
    async fn srv1_load_default_on_fresh_db() {
        // AC: "empty/new DB → defaults (current behavior)".
        let pool = setup_pool().await;
        let state = ServerSettingsState::with_pool("unsafe-no-auth".into(), pool).await;
        // Defaults are loaded — no persisted document existed.
        assert_eq!(state.config["authMode"], "unsafe-no-auth");
        assert_eq!(state.settings["defaultThreadEnvMode"], "local");
        assert!(state.settings["providers"]["codex"].is_object());
    }

    #[tokio::test]
    async fn srv1_write_read_roundtrip() {
        // AC: "every mutation write-throughs" + "write-read".
        let pool = setup_pool().await;

        // First session: set config + update settings, then persist.
        let mut state = ServerSettingsState::with_pool("unsafe-no-auth".into(), pool.clone()).await;
        state.config = serde_json::json!({ "cwd": "/srv1", "authMode": "remote-reachable" });
        state.settings = serde_json::json!({
            "defaultThreadEnvMode": "container",
            "providers": { "codex": { "enabled": false } },
        });
        state.persist_config().await;
        state.persist_settings().await;

        // Second session on the same DB: with_pool loads the persisted docs.
        let reloaded = ServerSettingsState::with_pool("unsafe-no-auth".into(), pool).await;
        assert_eq!(reloaded.config["cwd"], "/srv1");
        assert_eq!(reloaded.config["authMode"], "remote-reachable");
        assert_eq!(reloaded.settings["defaultThreadEnvMode"], "container");
        assert_eq!(reloaded.settings["providers"]["codex"]["enabled"], false);
    }

    #[tokio::test]
    async fn srv1_patch_merge_persists() {
        // AC: "patch-merge" — a deep-merge patch is persisted and reloads
        // with the merged shape (untouched sibling keys preserved).
        let pool = setup_pool().await;
        let mut state = ServerSettingsState::with_pool("unsafe-no-auth".into(), pool.clone()).await;

        // Apply a partial patch via the same merge_json the RPC handler uses.
        let patch = serde_json::json!({
            "textGenerationModelSelection": { "model": "claude-4" }
        });
        merge_json(&mut state.settings, &patch);
        state.persist_settings().await;

        // Reload — the merge is reflected, untouched sibling key preserved.
        let reloaded = ServerSettingsState::with_pool("unsafe-no-auth".into(), pool).await;
        assert_eq!(
            reloaded.settings["textGenerationModelSelection"]["model"],
            "claude-4"
        );
        // Untouched sibling key from the default survives the merge.
        assert_eq!(
            reloaded.settings["textGenerationModelSelection"]["provider"],
            "codex"
        );
    }

    #[tokio::test]
    async fn srv1_restart_survives_wsstate_reconstruction() {
        // AC: "Settings survive WsState reconstruction in tests".
        //
        // Simulates: session 1 writes a config + a keybinding + a settings
        // edit; the process "restarts" (state dropped + reconstructed from
        // the same DB); session 2 reads back the full persisted state.
        let pool = setup_pool().await;

        // ── Session 1: writes ──
        let mut s1 = ServerSettingsState::with_pool("unsafe-no-auth".into(), pool.clone()).await;
        // setConfig (replace) — mirrors handle_server_set_config.
        s1.config = serde_json::json!({
            "cwd": "/restart-test",
            "keybindings": [{ "id": "kb1", "keys": "ctrl+s" }],
            "providers": [],
            "issues": [],
            "authMode": "unsafe-no-auth",
        });
        s1.persist_config().await;
        // updateSettings (deep-merge) — mirrors handle_server_update_settings.
        merge_json(
            &mut s1.settings,
            &serde_json::json!({ "enableAssistantStreaming": true }),
        );
        s1.persist_settings().await;

        // ── "Restart": drop the store, reconstruct from disk ──
        drop(s1);
        let s2 = ServerSettingsState::with_pool("unsafe-no-auth".into(), pool).await;

        // ── Session 2: reads ── everything survived.
        assert_eq!(s2.config["cwd"], "/restart-test");
        let keybindings = s2.config["keybindings"].as_array().unwrap();
        assert_eq!(keybindings.len(), 1);
        assert_eq!(keybindings[0]["id"], "kb1");
        assert_eq!(keybindings[0]["keys"], "ctrl+s");
        assert_eq!(s2.settings["enableAssistantStreaming"], true);
        // Untouched default key preserved through the merge + restart.
        assert_eq!(s2.settings["defaultThreadEnvMode"], "local");
    }

    #[tokio::test]
    async fn srv1_in_memory_state_has_no_pool() {
        // Backward-compat: the plain `new` constructor is purely in-memory.
        let state = ServerSettingsState::new("unsafe-no-auth".into());
        assert!(state.pool.is_none());
        // persist_* are no-ops (no panic, no write).
        state.persist_config().await;
        state.persist_settings().await;
    }

    // ─── Provider selection from persisted settings ─────────────────
    //
    // Bug: orchestrator armed with `SYNCODE_DEFAULT_PROVIDER` env var
    // (default "opencode") BEFORE persisted settings are loaded. The
    // fix reads `settings.textGenerationModelSelection.provider` first,
    // falling back to env, then DEFAULT_PROVIDER.

    #[test]
    fn normalize_provider_id_maps_claude_agent_to_claude() {
        // Frontend persists "claudeAgent" (matches the providers map key);
        // adapter registry expects "claude".
        assert_eq!(normalize_provider_id("claudeAgent"), PROVIDER_CLAUDE);
    }

    #[test]
    fn normalize_provider_id_passes_through_known_ids() {
        assert_eq!(normalize_provider_id("codex"), PROVIDER_CODEX);
        assert_eq!(normalize_provider_id("opencode"), PROVIDER_OPENCODE);
        assert_eq!(normalize_provider_id("cursor"), PROVIDER_CURSOR);
        assert_eq!(normalize_provider_id("gemini"), PROVIDER_GEMINI);
        assert_eq!(normalize_provider_id("grok"), PROVIDER_GROK);
        assert_eq!(normalize_provider_id("kilo"), PROVIDER_KILO);
        assert_eq!(normalize_provider_id("pi"), PROVIDER_PI);
    }

    #[test]
    fn resolve_default_provider_prefers_settings_over_env() {
        let settings = serde_json::json!({
            "textGenerationModelSelection": { "provider": "codex", "model": "gpt-5.4-mini" }
        });
        let resolved = resolve_default_provider(&settings, Some("opencode"));
        assert_eq!(resolved, PROVIDER_CODEX);
    }

    #[test]
    fn resolve_default_provider_falls_back_to_env_when_settings_missing() {
        let settings = serde_json::json!({});
        let resolved = resolve_default_provider(&settings, Some("claude"));
        assert_eq!(resolved, PROVIDER_CLAUDE);
    }

    #[test]
    fn resolve_default_provider_falls_back_to_default_when_both_empty() {
        let settings = serde_json::json!({});
        let resolved = resolve_default_provider(&settings, None);
        // DEFAULT_PROVIDER constant in server.rs = "opencode"
        assert_eq!(resolved, PROVIDER_OPENCODE);
    }

    #[test]
    fn resolve_default_provider_normalizes_claude_agent() {
        let settings = serde_json::json!({
            "textGenerationModelSelection": { "provider": "claudeAgent" }
        });
        let resolved = resolve_default_provider(&settings, None);
        assert_eq!(resolved, PROVIDER_CLAUDE);
    }

    #[test]
    fn resolve_default_model_prefers_settings_over_env() {
        let settings = serde_json::json!({
            "textGenerationModelSelection": { "provider": "codex", "model": "gpt-5.4-mini" }
        });
        let resolved = resolve_default_model(&settings, Some("env-model"));
        assert_eq!(resolved, "gpt-5.4-mini");
    }

    #[test]
    fn resolve_default_model_falls_back_to_env_when_settings_missing() {
        let settings = serde_json::json!({});
        let resolved = resolve_default_model(&settings, Some("env-model"));
        assert_eq!(resolved, "env-model");
    }

    #[test]
    fn resolve_default_model_returns_empty_when_both_empty() {
        let settings = serde_json::json!({});
        let resolved = resolve_default_model(&settings, None);
        assert_eq!(resolved, "");
    }

    #[test]
    fn extract_provider_extras_codex_returns_binary_path_and_home() {
        let settings = serde_json::json!({
            "providers": {
                "codex": {
                    "enabled": true,
                    "binaryPath": "/usr/local/bin/codex",
                    "customModels": [],
                    "homePath": "/home/user/.codex"
                }
            }
        });
        let extras = extract_provider_extras(PROVIDER_CODEX, &settings);
        assert_eq!(
            extras.get("binaryPath").and_then(|v| v.as_str()),
            Some("/usr/local/bin/codex")
        );
        assert_eq!(
            extras.get("homePath").and_then(|v| v.as_str()),
            Some("/home/user/.codex")
        );
    }

    #[test]
    fn extract_provider_extras_claude_uses_claude_agent_key() {
        let settings = serde_json::json!({
            "providers": {
                "claudeAgent": {
                    "enabled": true,
                    "binaryPath": "claude",
                    "customModels": [],
                    "launchArgs": "--debug"
                }
            }
        });
        let extras = extract_provider_extras(PROVIDER_CLAUDE, &settings);
        assert_eq!(
            extras.get("binaryPath").and_then(|v| v.as_str()),
            Some("claude")
        );
        assert_eq!(
            extras.get("launchArgs").and_then(|v| v.as_str()),
            Some("--debug")
        );
    }

    #[test]
    fn extract_provider_extras_opencode_includes_server_credentials() {
        let settings = serde_json::json!({
            "providers": {
                "opencode": {
                    "enabled": true,
                    "binaryPath": "opencode",
                    "customModels": [],
                    "serverUrl": "https://opencode.example.com",
                    "serverPassword": "secret",
                    "experimentalWebSockets": true
                }
            }
        });
        let extras = extract_provider_extras(PROVIDER_OPENCODE, &settings);
        assert_eq!(
            extras.get("binaryPath").and_then(|v| v.as_str()),
            Some("opencode")
        );
        assert_eq!(
            extras.get("serverUrl").and_then(|v| v.as_str()),
            Some("https://opencode.example.com")
        );
        assert_eq!(
            extras.get("serverPassword").and_then(|v| v.as_str()),
            Some("secret")
        );
        assert_eq!(
            extras
                .get("experimentalWebSockets")
                .and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn extract_provider_extras_cursor_includes_api_endpoint() {
        let settings = serde_json::json!({
            "providers": {
                "cursor": {
                    "enabled": true,
                    "binaryPath": "cursor-agent",
                    "customModels": [],
                    "apiEndpoint": "https://cursor.api"
                }
            }
        });
        let extras = extract_provider_extras(PROVIDER_CURSOR, &settings);
        assert_eq!(
            extras.get("binaryPath").and_then(|v| v.as_str()),
            Some("cursor-agent")
        );
        assert_eq!(
            extras.get("apiEndpoint").and_then(|v| v.as_str()),
            Some("https://cursor.api")
        );
    }

    #[test]
    fn extract_provider_extras_kilo_includes_server_credentials() {
        let settings = serde_json::json!({
            "providers": {
                "kilo": {
                    "enabled": true,
                    "binaryPath": "kilo",
                    "customModels": [],
                    "serverUrl": "https://kilo.example.com",
                    "serverPassword": "kilopw"
                }
            }
        });
        let extras = extract_provider_extras(PROVIDER_KILO, &settings);
        assert_eq!(
            extras.get("serverUrl").and_then(|v| v.as_str()),
            Some("https://kilo.example.com")
        );
        assert_eq!(
            extras.get("serverPassword").and_then(|v| v.as_str()),
            Some("kilopw")
        );
    }

    #[test]
    fn extract_provider_extras_pi_includes_agent_dir() {
        let settings = serde_json::json!({
            "providers": {
                "pi": {
                    "enabled": true,
                    "binaryPath": "pi",
                    "customModels": [],
                    "agentDir": "/home/user/.pi/agents"
                }
            }
        });
        let extras = extract_provider_extras(PROVIDER_PI, &settings);
        assert_eq!(
            extras.get("agentDir").and_then(|v| v.as_str()),
            Some("/home/user/.pi/agents")
        );
    }

    #[test]
    fn extract_provider_extras_unknown_provider_returns_empty() {
        let settings = serde_json::json!({ "providers": {} });
        let extras = extract_provider_extras("unknown", &settings);
        assert!(
            extras.is_empty(),
            "expected empty extras for unknown provider"
        );
    }

    #[test]
    fn extract_provider_extras_handles_missing_providers_key() {
        let settings = serde_json::json!({});
        let extras = extract_provider_extras(PROVIDER_CODEX, &settings);
        assert!(extras.is_empty());
    }

    #[test]
    fn extract_provider_extras_skips_disabled_provider() {
        // Disabled provider entry should yield empty extras (adapter won't spawn).
        let settings = serde_json::json!({
            "providers": {
                "codex": { "enabled": false, "binaryPath": "codex" }
            }
        });
        let extras = extract_provider_extras(PROVIDER_CODEX, &settings);
        assert!(
            extras.is_empty(),
            "disabled provider should return empty extras"
        );
    }
}

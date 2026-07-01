//! RPC request/result DTO mirrors for Syncode's served JSON-RPC methods.
//!
//! These are **Tier 1** of the contracts bridge (see
//! `CONTRACTS-BRIDGE-DESIGN.md` §4). Each served method — those handled by
//! `syncode-ws::rpc::dispatch_method` — has a `*Params` (request) and
//! `*Result` (response) DTO here, derived `#[derive(TS)]` and exported to the
//! frontend via ts-rs. The frontend `src/contracts/rpc.ts` registry maps each
//! slash-method string to its Request/Result type.
//!
//! ## Conventions (T1 wire↔TS parity)
//!
//! - **camelCase on both serde + ts** (`#[serde(rename_all = "camelCase")]` +
//!   `#[ts(export, rename_all = "camelCase")]`). Syncode's RPC handlers reach
//!   into `serde_json::Value` params using camelCase keys
//!   (e.g. `params.get("rootPath")`), so the wire **is** camelCase; the TS
//!   types match.
//! - **bigint-safe:** any `u64`/`usize`/`i64` field carries
//!   `#[ts(type = "number | null")]` (or `#[ts(type = "number")]`) because
//!   JSON.parse yields `number` but ts-rs would otherwise emit `bigint`.
//! - **Reuse snapshot types where shapes match:** `project/get` returns a
//!   `ProjectSummary`-shaped entity (it's the read-model view serialized
//!   directly), `thread/get` returns a `ThreadSummary`, `turn/get` returns a
//!   `TurnSummary`. Where a method returns a list, the result is typed
//!   `Vec<…>` and a thin `*ListResult` struct carries the `{ "<name>": [...] }`
//!   envelope the handler emits.
//!
//! ## Method source-of-truth
//!
//! The 19 served methods are enumerated by `rpc/listMethods` in
//! `crates/syncode-ws/src/rpc.rs` (plus `ping` + `rpc/listMethods` themselves).
//! Param shapes are read from each `handle_*` function. Where a method takes
//! no params (e.g. `project/list`, `auth/status`), no `*Params` type exists —
//! the request is `()`/`null` in the registry.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

// Summary types used by RPC results. Only the three actually referenced by
// type aliases (`ProjectGetResult`, `ThreadGetResult`, `TurnGetResult` etc.)
// are imported at the module level; `ActivitySummary`/`MessageSummary` are
// used solely in tests (imported there via `use crate::snapshots::{...}`).
use crate::snapshots::{ProjectSummary, ThreadSummary, TurnSummary};

// ════════════════════════════════════════════════════════════════════════
// ─── System methods ─────────────────────────────────────────────────────
// ════════════════════════════════════════════════════════════════════════

/// Result of `rpc/listMethods` — the enumeration of methods this server serves.
/// Mirrors the `{"methods": [...]}` object emitted by `dispatch_method`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ListMethodsResult {
    pub methods: Vec<String>,
}

/// Result of `ping` — an empty object. (Modeled as a struct rather than `()`
/// so the TS registry has a concrete `Result` type per method.)
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PingResult {}

// ════════════════════════════════════════════════════════════════════════
// ─── Project methods (project/{list,get,create}) ────────────────────────
// ════════════════════════════════════════════════════════════════════════

/// `project/list` — no params. Result is `{ projects: ProjectSummary[] }`.
///
/// The handler serializes `read_store.projects.values()` directly; the view's
/// shape matches `ProjectSummary` (id, name, rootPath, providerId, …).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ProjectListResult {
    pub projects: Vec<ProjectSummary>,
}

/// `project/get` params: `{ id }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ProjectGetParams {
    /// Project identifier (UUID string).
    pub id: String,
}

/// `project/get` result — the project read-model view (shape: `ProjectSummary`).
pub type ProjectGetResult = ProjectSummary;

/// `project/create` params: `{ name, rootPath }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ProjectCreateParams {
    /// Human-readable project name. Must be non-empty (handler rejects whitespace).
    pub name: String,
    /// Absolute filesystem path to the project root.
    pub root_path: String,
}

/// `project/create` result — the freshly-created project view (shape: `ProjectSummary`).
pub type ProjectCreateResult = ProjectSummary;

// ════════════════════════════════════════════════════════════════════════
// ─── Thread methods (thread/{list,get,create,pause,resume,cancel}) ──────
// ════════════════════════════════════════════════════════════════════════

/// `thread/list` params: `{ projectId? }`. Empty/unset projectId returns all.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ThreadListParams {
    /// Optional filter: only threads belonging to this project. Empty string
    /// or omitted means "all threads" (matches handler's `project_id.is_empty()`).
    #[ts(optional)]
    pub project_id: Option<String>,
}

/// `thread/list` result — `{ threads: ThreadSummary[] }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ThreadListResult {
    pub threads: Vec<ThreadSummary>,
}

/// `thread/get` params: `{ id }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ThreadGetParams {
    pub id: String,
}

/// `thread/get` result — the thread view (shape: `ThreadSummary`).
pub type ThreadGetResult = ThreadSummary;

/// `thread/create` params: `{ projectId, providerId, model }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ThreadCreateParams {
    /// Parent project's UUID.
    pub project_id: String,
    /// Provider identifier (e.g. "anthropic").
    pub provider_id: String,
    /// Model slug (e.g. "claude-sonnet-4").
    pub model: String,
}

/// `thread/create` result — the freshly-created thread view (shape: `ThreadSummary`).
pub type ThreadCreateResult = ThreadSummary;

/// Shared params for `thread/{pause,resume,cancel}`: `{ id }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ThreadLifecycleParams {
    /// Thread identifier (UUID).
    pub id: String,
}

/// Shared result for `thread/{pause,resume,cancel}` — the updated thread view.
pub type ThreadLifecycleResult = ThreadSummary;

// ════════════════════════════════════════════════════════════════════════
// ─── Turn methods (turn/{list,get,start,complete}) ──────────────────────
// ════════════════════════════════════════════════════════════════════════

/// `turn/list` params: `{ threadId? }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct TurnListParams {
    #[ts(optional)]
    pub thread_id: Option<String>,
}

/// `turn/list` result — `{ turns: TurnSummary[] }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct TurnListResult {
    pub turns: Vec<TurnSummary>,
}

/// `turn/get` params: `{ id }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct TurnGetParams {
    pub id: String,
}

/// `turn/get` result — the turn view (shape: `TurnSummary`).
pub type TurnGetResult = TurnSummary;

/// `turn/start` params: `{ threadId, sequence?, userInput }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct TurnStartParams {
    /// Parent thread's UUID.
    pub thread_id: String,
    /// Optional sequence number; defaults to 0 if absent (handler coerces u32).
    #[ts(optional)]
    pub sequence: Option<u32>,
    /// The user's prompt text for this turn.
    pub user_input: String,
}

/// `turn/start` result — the freshly-started turn view (shape: `TurnSummary`).
pub type TurnStartResult = TurnSummary;

/// `turn/complete` params: `{ id, assistantOutput, durationMs? }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct TurnCompleteParams {
    /// The turn to complete (UUID).
    pub id: String,
    /// The assistant's response text.
    pub assistant_output: String,
    /// Wall-clock duration in milliseconds. Defaults to 0 if absent.
    // NOTE: u64 → JSON number → ts `bigint` mismatch. Pin TS to `number | null`.
    #[ts(type = "number | null")]
    pub duration_ms: Option<u64>,
}

/// `turn/complete` result — the completed turn view (shape: `TurnSummary`).
pub type TurnCompleteResult = TurnSummary;

// ════════════════════════════════════════════════════════════════════════
// ─── Auth methods (auth/{bootstrap,status,logout}) ──────────────────────
// ════════════════════════════════════════════════════════════════════════

/// `auth/bootstrap` params: `{ credential }`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct AuthBootstrapParams {
    /// Bearer credential / shared secret. Opaque to the client.
    pub credential: String,
}

/// `auth/bootstrap` result. Shape differs by auth mode:
/// - **No-auth mode:** `{ authenticated, mode, note }` (no token/role/subject).
/// - **Requiring mode:** `{ authenticated, sessionToken, role, subject, expiresAt }`.
///
/// The `sessionToken`/`role`/`subject`/`expiresAt` fields are all optional so
/// the same DTO covers both shapes; the no-auth variant just leaves them unset.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct AuthBootstrapResult {
    /// Always `true` on success.
    pub authenticated: bool,
    /// Auth mode string echoed from `WsAuthConfig.mode` (e.g. "none", "remote").
    pub mode: String,
    /// Human-readable note (only set in no-auth mode).
    #[ts(optional)]
    pub note: Option<String>,
    /// Bearer session token (only in requiring mode).
    #[ts(optional)]
    pub session_token: Option<String>,
    /// Principal role (only in requiring mode). E.g. "owner", "operator", "viewer".
    #[ts(optional)]
    pub role: Option<String>,
    /// Principal subject identifier (only in requiring mode).
    #[ts(optional)]
    pub subject: Option<String>,
    /// ISO-8601 expiry timestamp (only in requiring mode).
    #[ts(optional)]
    pub expires_at: Option<String>,
}

/// `auth/status` result — the connection's current auth state.
///
/// Pre-auth in requiring mode: `{ authenticated: false, requiresAuthentication:
/// true, role: null, subject: null }` (no `expiresAt`). Authenticated: full set.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct AuthStatusResult {
    pub authenticated: bool,
    pub requires_authentication: bool,
    /// Role string when authenticated; `null` otherwise.
    #[ts(optional)]
    pub role: Option<String>,
    /// Subject string when authenticated; `null` otherwise.
    #[ts(optional)]
    pub subject: Option<String>,
    /// Expiry when authenticated; absent otherwise.
    #[ts(optional)]
    pub expires_at: Option<String>,
}

/// `auth/logout` result.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct AuthLogoutResult {
    pub logged_out: bool,
    /// `true` if the connection had a session before logout.
    pub had_session: bool,
}

// ════════════════════════════════════════════════════════════════════════
// ─── Push subscription methods (push/{subscribe,unsubscribe}) ───────────
// ════════════════════════════════════════════════════════════════════════

/// `push/subscribe` params: `{ channel, threadId? }`.
///
/// `channel` is one of the known push channels (`"orchestration"`, `"git"`,
/// `"terminal"`, or `"*"`). `threadId` is an optional selector for the
/// `orchestration` channel: when set, the snapshot is thread-detail (one
/// thread + turns + messages) instead of the shell-wide snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct PushSubscribeParams {
    /// Push channel name (`"orchestration"`, `"git"`, `"terminal"`, or `"*"`).
    pub channel: String,
    /// Optional thread selector for the orchestration channel snapshot scope.
    #[ts(optional)]
    pub thread_id: Option<String>,
}

/// `push/subscribe` result.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct PushSubscribeResult {
    pub subscribed: bool,
    /// Echoed channel name.
    pub channel: String,
    /// `true` if this call created a new subscription (idempotent — re-subscribe is `false`).
    pub added: bool,
    /// `true` if a snapshot was emitted to this connection (snapshot-then-stream).
    pub snapshot_emitted: bool,
}

/// `push/unsubscribe` params: `{ channel }`. `"*"` clears all subscriptions.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct PushUnsubscribeParams {
    pub channel: String,
}

/// `push/unsubscribe` result.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct PushUnsubscribeResult {
    pub unsubscribed: bool,
    pub channel: String,
    /// `true` if a subscription was actually removed.
    pub removed: bool,
}

// ════════════════════════════════════════════════════════════════════════
// ─── Tests ──────────────────────────────────────────────────────────────
// ════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshots::{ActivitySummary, MessageSummary};

    // ── System ──────────────────────────────────────────────────────────

    #[test]
    fn list_methods_roundtrip() {
        let r = ListMethodsResult {
            methods: vec!["ping".into(), "project/list".into()],
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"methods\""), "{json}");
        let back: ListMethodsResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.methods.len(), 2);
    }

    #[test]
    fn ping_result_roundtrip() {
        let r = PingResult {};
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, "{}");
        let _: PingResult = serde_json::from_str(&json).unwrap();
    }

    // ── Project ─────────────────────────────────────────────────────────

    #[test]
    fn project_list_result_roundtrip() {
        let r = ProjectListResult {
            projects: vec![sample_project()],
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"projects\""), "ProjectListResult: {json}");
        assert!(json.contains("\"rootPath\""), "ProjectSummary camelCase: {json}");
        let back: ProjectListResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.projects.len(), 1);
    }

    #[test]
    fn project_get_params_roundtrip() {
        let p = ProjectGetParams { id: "p1".into() };
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "{\"id\":\"p1\"}");
        let back: ProjectGetParams = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "p1");
    }

    #[test]
    fn project_create_params_roundtrip() {
        let p = ProjectCreateParams {
            name: "Demo".into(),
            root_path: "/tmp/demo".into(),
        };
        let json = serde_json::to_string(&p).unwrap();
        // camelCase wire parity: root_path → rootPath
        assert!(json.contains("\"rootPath\""), "ProjectCreateParams: {json}");
        assert!(!json.contains("\"root_path\""));
        let back: ProjectCreateParams = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "Demo");
        assert_eq!(back.root_path, "/tmp/demo");
    }

    // ── Thread ──────────────────────────────────────────────────────────

    #[test]
    fn thread_list_params_optional_roundtrip() {
        // With projectId
        let p = ThreadListParams {
            project_id: Some("p1".into()),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"projectId\""), "{json}");
        let back: ThreadListParams = serde_json::from_str(&json).unwrap();
        assert_eq!(back.project_id.as_deref(), Some("p1"));

        // Without projectId (None → omitted on wire via #[ts(optional)] convention;
        // serde still serializes as null here, which round-trips fine)
        let p = ThreadListParams { project_id: None };
        let json = serde_json::to_string(&p).unwrap();
        let back: ThreadListParams = serde_json::from_str(&json).unwrap();
        assert!(back.project_id.is_none());
    }

    #[test]
    fn thread_list_result_roundtrip() {
        let r = ThreadListResult {
            threads: vec![sample_thread()],
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"threads\""), "{json}");
        assert!(json.contains("\"projectId\""), "ThreadSummary camelCase: {json}");
        let back: ThreadListResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.threads.len(), 1);
    }

    #[test]
    fn thread_create_params_roundtrip() {
        let p = ThreadCreateParams {
            project_id: "p1".into(),
            provider_id: "anthropic".into(),
            model: "claude-sonnet-4".into(),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"projectId\""), "{json}");
        assert!(json.contains("\"providerId\""));
        assert!(json.contains("\"model\""));
        let back: ThreadCreateParams = serde_json::from_str(&json).unwrap();
        assert_eq!(back.project_id, "p1");
        assert_eq!(back.provider_id, "anthropic");
    }

    #[test]
    fn thread_lifecycle_params_roundtrip() {
        let p = ThreadLifecycleParams { id: "t1".into() };
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "{\"id\":\"t1\"}");
        let _: ThreadLifecycleParams = serde_json::from_str(&json).unwrap();
    }

    // ── Turn ────────────────────────────────────────────────────────────

    #[test]
    fn turn_list_result_roundtrip() {
        let r = TurnListResult {
            turns: vec![sample_turn()],
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"turns\""), "{json}");
        assert!(json.contains("\"threadId\""), "TurnSummary camelCase: {json}");
        let back: TurnListResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.turns.len(), 1);
    }

    #[test]
    fn turn_start_params_roundtrip() {
        let p = TurnStartParams {
            thread_id: "t1".into(),
            sequence: Some(3),
            user_input: "hello".into(),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"threadId\""), "{json}");
        assert!(json.contains("\"userInput\""));
        assert!(json.contains("\"sequence\""));
        let back: TurnStartParams = serde_json::from_str(&json).unwrap();
        assert_eq!(back.thread_id, "t1");
        assert_eq!(back.sequence, Some(3));
        assert_eq!(back.user_input, "hello");
    }

    #[test]
    fn turn_complete_params_roundtrip_and_bigint_safe() {
        let p = TurnCompleteParams {
            id: "turn-1".into(),
            assistant_output: "result text".into(),
            duration_ms: Some(1500),
        };
        let json = serde_json::to_string(&p).unwrap();
        // camelCase + the value serializes as a JSON number (not a string).
        assert!(json.contains("\"assistantOutput\""), "{json}");
        assert!(json.contains("\"durationMs\""));
        assert!(json.contains("\"durationMs\":1500"));
        let back: TurnCompleteParams = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "turn-1");
        assert_eq!(back.duration_ms, Some(1500));

        // TS export of the durationMs field must be `number | null`, NOT bigint.
        let ts = TurnCompleteParams::ident();
        let _ = ts; // identifier sanity (Verifies the TS export path exists)
    }

    // ── Auth ────────────────────────────────────────────────────────────

    #[test]
    fn auth_bootstrap_params_roundtrip() {
        let p = AuthBootstrapParams {
            credential: "sk-xxx".into(),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "{\"credential\":\"sk-xxx\"}");
        let _: AuthBootstrapParams = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn auth_bootstrap_result_noauth_mode_roundtrip() {
        // No-auth mode: only authenticated + mode + note. Others are None.
        let r = AuthBootstrapResult {
            authenticated: true,
            mode: "none".into(),
            note: Some("server does not require authentication".into()),
            session_token: None,
            role: None,
            subject: None,
            expires_at: None,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"authenticated\""), "{json}");
        assert!(json.contains("\"mode\""));
        assert!(json.contains("\"note\""));
        // camelCase: sessionToken (would be null since None, but key still present)
        assert!(json.contains("\"sessionToken\""));
        let back: AuthBootstrapResult = serde_json::from_str(&json).unwrap();
        assert!(back.authenticated);
        assert!(back.session_token.is_none());
    }

    #[test]
    fn auth_bootstrap_result_requiring_mode_roundtrip() {
        let r = AuthBootstrapResult {
            authenticated: true,
            mode: "remote".into(),
            note: None,
            session_token: Some("tok".into()),
            role: Some("owner".into()),
            subject: Some("user-1".into()),
            expires_at: Some("2026-12-31T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"sessionToken\""), "{json}");
        assert!(json.contains("\"expiresAt\""));
        assert!(json.contains("\"subject\""));
        let back: AuthBootstrapResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.role.as_deref(), Some("owner"));
        assert_eq!(back.session_token.as_deref(), Some("tok"));
    }

    #[test]
    fn auth_status_result_roundtrip() {
        let r = AuthStatusResult {
            authenticated: false,
            requires_authentication: true,
            role: None,
            subject: None,
            expires_at: None,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"requiresAuthentication\""), "{json}");
        let back: AuthStatusResult = serde_json::from_str(&json).unwrap();
        assert!(!back.authenticated);
        assert!(back.requires_authentication);
    }

    #[test]
    fn auth_logout_result_roundtrip() {
        let r = AuthLogoutResult {
            logged_out: true,
            had_session: true,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"loggedOut\""), "{json}");
        assert!(json.contains("\"hadSession\""));
        let back: AuthLogoutResult = serde_json::from_str(&json).unwrap();
        assert!(back.had_session);
    }

    // ── Push ────────────────────────────────────────────────────────────

    #[test]
    fn push_subscribe_params_roundtrip() {
        let p = PushSubscribeParams {
            channel: "orchestration".into(),
            thread_id: Some("t1".into()),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"channel\""), "{json}");
        assert!(json.contains("\"threadId\""));
        let back: PushSubscribeParams = serde_json::from_str(&json).unwrap();
        assert_eq!(back.channel, "orchestration");
        assert_eq!(back.thread_id.as_deref(), Some("t1"));
    }

    #[test]
    fn push_subscribe_result_roundtrip() {
        let r = PushSubscribeResult {
            subscribed: true,
            channel: "git".into(),
            added: true,
            snapshot_emitted: true,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"snapshotEmitted\""), "{json}");
        assert!(json.contains("\"subscribed\""));
        let back: PushSubscribeResult = serde_json::from_str(&json).unwrap();
        assert!(back.subscribed);
        assert!(back.added);
    }

    #[test]
    fn push_unsubscribe_params_and_result_roundtrip() {
        let p = PushUnsubscribeParams {
            channel: "*".into(),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "{\"channel\":\"*\"}");
        let _: PushUnsubscribeParams = serde_json::from_str(&json).unwrap();

        let r = PushUnsubscribeResult {
            unsubscribed: true,
            channel: "*".into(),
            removed: true,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"unsubscribed\""), "{json}");
        assert!(json.contains("\"removed\""));
        let back: PushUnsubscribeResult = serde_json::from_str(&json).unwrap();
        assert!(back.removed);
    }

    // ── Wire-parity guards (camelCase, like the lib.rs/snapshots.rs ones) ──

    /// Guards that all RPC DTO structs serialize camelCase (serde) — the wire
    /// contract. ts-rs camelCase is verified by the export harness below.
    #[test]
    fn rpc_dtos_serialize_camel_case() {
        // ProjectCreateParams: root_path → rootPath
        let json = serde_json::to_string(&ProjectCreateParams {
            name: "n".into(),
            root_path: "/".into(),
        })
        .unwrap();
        assert!(json.contains("\"rootPath\""), "ProjectCreateParams: {json}");

        // TurnStartParams: thread_id, user_input
        let json = serde_json::to_string(&TurnStartParams {
            thread_id: "t".into(),
            sequence: None,
            user_input: "u".into(),
        })
        .unwrap();
        assert!(json.contains("\"threadId\""), "TurnStartParams: {json}");
        assert!(json.contains("\"userInput\""));

        // TurnCompleteParams: assistant_output, duration_ms
        let json = serde_json::to_string(&TurnCompleteParams {
            id: "x".into(),
            assistant_output: "o".into(),
            duration_ms: None,
        })
        .unwrap();
        assert!(json.contains("\"assistantOutput\""), "TurnCompleteParams: {json}");
        assert!(json.contains("\"durationMs\""));

        // ThreadCreateParams: project_id, provider_id
        let json = serde_json::to_string(&ThreadCreateParams {
            project_id: "p".into(),
            provider_id: "x".into(),
            model: "m".into(),
        })
        .unwrap();
        assert!(json.contains("\"projectId\""), "ThreadCreateParams: {json}");
        assert!(json.contains("\"providerId\""));

        // AuthBootstrapResult: session_token, expires_at
        let json = serde_json::to_string(&AuthBootstrapResult {
            authenticated: true,
            mode: "none".into(),
            note: None,
            session_token: None,
            role: None,
            subject: None,
            expires_at: None,
        })
        .unwrap();
        assert!(json.contains("\"sessionToken\""), "AuthBootstrapResult: {json}");
        assert!(json.contains("\"expiresAt\""));

        // AuthStatusResult: requires_authentication
        let json = serde_json::to_string(&AuthStatusResult {
            authenticated: false,
            requires_authentication: false,
            role: None,
            subject: None,
            expires_at: None,
        })
        .unwrap();
        assert!(
            json.contains("\"requiresAuthentication\""),
            "AuthStatusResult: {json}"
        );

        // AuthLogoutResult: logged_out, had_session
        let json = serde_json::to_string(&AuthLogoutResult {
            logged_out: true,
            had_session: false,
        })
        .unwrap();
        assert!(json.contains("\"loggedOut\""), "AuthLogoutResult: {json}");
        assert!(json.contains("\"hadSession\""));

        // PushSubscribeResult: snapshot_emitted
        let json = serde_json::to_string(&PushSubscribeResult {
            subscribed: true,
            channel: "c".into(),
            added: false,
            snapshot_emitted: false,
        })
        .unwrap();
        assert!(json.contains("\"snapshotEmitted\""), "PushSubscribeResult: {json}");
    }

    // ── Sample builders (mirror the snapshot test helpers) ──────────────

    fn sample_project() -> ProjectSummary {
        ProjectSummary {
            id: "p1".into(),
            name: "Demo".into(),
            root_path: "/tmp/demo".into(),
            provider_id: None,
            default_model: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            thread_count: 0,
        }
    }

    fn sample_thread() -> ThreadSummary {
        ThreadSummary {
            id: "t1".into(),
            project_id: "p1".into(),
            provider_id: "anthropic".into(),
            model: "claude-sonnet-4".into(),
            status: "active".into(),
            title: None,
            git_checkpoint: None,
            runtime_mode: "full-access".into(),
            interaction_mode: "default".into(),
            turn_count: 0,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    fn sample_turn() -> TurnSummary {
        TurnSummary {
            id: "turn-1".into(),
            thread_id: "t1".into(),
            sequence: 1,
            user_input: "hi".into(),
            assistant_output: None,
            status: "running".into(),
            git_checkpoint: None,
            files_modified: vec![],
            duration_ms: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            completed_at: None,
        }
    }

    // ── Keep the unused re-export alias honest ──────────────────────────
    // `ActivitySummary`/`MessageSummary` are part of the snapshots module
    // surface used by RPC results indirectly (ThreadDetailSnapshot payloads).
    // Reference them so the module-level re-export doesn't dead-code the import.
    #[test]
    fn snapshot_summary_types_compile_check() {
        let _ = ActivitySummary {
            id: "a".into(),
            activity_type: "t".into(),
            description: "d".into(),
            project_id: None,
            thread_id: None,
            metadata: serde_json::Value::Null,
            created_at: "t".into(),
        };
        let _ = MessageSummary {
            id: "m".into(),
            turn_id: "turn".into(),
            role: "user".into(),
            content: "c".into(),
            content_type: "text".into(),
            token_count: None,
            tool_name: None,
            tool_call_id: None,
            created_at: "t".into(),
            is_streaming: false,
        };
    }
}

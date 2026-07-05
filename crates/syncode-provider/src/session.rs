//! Provider session state management
//!
//! Tracks the lifecycle of an individual provider session —
//! one session maps to one Syncode turn.
//!
//! [`SessionManager`] coordinates sessions across adapters:
//! start, resume, interrupt, stop — with state validation at each transition.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Utc};
use syncode_core::EntityId;
use tokio::sync::RwLock;

use crate::registry::SharedAdapter;
use crate::trait_def::*;

// ---------------------------------------------------------------------------
// Session state machine
// ---------------------------------------------------------------------------

/// Valid state transitions for a session.
///
/// Pending → Processing → Completed
/// Pending → Processing → Interrupted → Processing (resume)
/// Pending → Processing → Interrupted → Completed
/// Pending → Processing → Errored
/// Any active → Interrupted (user cancel)
static VALID_TRANSITIONS: &[(SessionStateStatus, &[SessionStateStatus])] = &[
    (
        SessionStateStatus::Pending,
        &[SessionStateStatus::Processing],
    ),
    (
        SessionStateStatus::Processing,
        &[
            SessionStateStatus::Completed,
            SessionStateStatus::Interrupted,
            SessionStateStatus::Errored,
        ],
    ),
    (
        SessionStateStatus::Interrupted,
        &[
            SessionStateStatus::Processing,
            SessionStateStatus::Completed,
        ],
    ),
];

/// State machine for a provider session
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SessionStateStatus {
    /// Session created, not yet processing
    Pending = 0,
    /// Provider is actively processing input
    Processing = 1,
    /// Provider returned a response
    Completed = 2,
    /// Session was interrupted by user
    Interrupted = 3,
    /// Session encountered an error
    Errored = 4,
}

/// The set of attributes that define "the same session" for a thread.
///
/// `ensure_session_for_thread` compares the requested identity against the
/// active session's recorded identity (see [`SessionState::identity`]): if any
/// field differs, the session is torn down and a new one started with the old
/// session's resume cursor. This mirrors mcode's `ensureSessionForThread`
/// change-detection (model/provider/runtime-mode changes → restart with
/// cursor).
///
/// The three tracked dimensions are:
/// - `provider_id` — switching providers (e.g. codex → claude) invalidates
///   the session.
/// - `model` — a different model selection invalidates the session. `None`
///   means "the caller has no model preference" and matches another `None`.
/// - `working_dir` — a different working directory invalidates the session
///   (the provider would be operating on a different project root).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIdentity {
    /// Provider id servicing the session (e.g. "codex", "claude").
    pub provider_id: String,
    /// Model selection. `None` when the caller has no model preference.
    pub model: Option<String>,
    /// Working directory the provider runs in.
    pub working_dir: String,
}

/// Internal state for a single provider session
pub struct SessionState {
    /// Unique session identifier (e.g., "codex-<uuid>")
    pub id: String,
    /// The thread this session belongs to
    pub thread_id: EntityId,
    /// The turn this session processes
    pub turn_id: EntityId,
    /// Working directory for the provider
    pub working_dir: String,
    /// Current session status
    pub status: std::sync::RwLock<SessionStateStatus>,
    /// Whether the session is still active
    pub active: AtomicBool,
    /// Session creation time
    pub created_at: DateTime<Utc>,
    /// Request count (total messages sent to provider)
    pub request_count: AtomicBool, // reuse AtomicBool as a simple flag
    /// Response tokens accumulated
    pub total_output_tokens: std::sync::atomic::AtomicU32,
    /// The provider/model/working-dir identity this session was started under.
    ///
    /// Recorded by `ensure_session_for_thread` when it starts (or restarts) a
    /// session, and compared on subsequent calls to decide reuse vs. restart.
    /// `None` for sessions started through other code paths — those have no
    /// recorded identity to compare against.
    identity: std::sync::RwLock<Option<SessionIdentity>>,
    /// Provider-side resume cursor (e.g. a thread id from the provider's API).
    ///
    /// When `Some`, this is the cursor the provider returned for the session —
    /// on a server restart [`SessionManager::rehydrate_sessions`] passes it to
    /// [`ProviderAdapter::resume_session`] so the provider can reattach to its
    /// in-flight conversation rather than starting fresh. Stored behind a
    /// lock so it can be updated after the adapter returns a cursor without
    /// taking a write lock on the whole `SessionState`.
    ///
    /// [`ProviderAdapter::resume_session`]: crate::trait_def::ProviderAdapter::resume_session
    resume_cursor: std::sync::RwLock<Option<String>>,
}

impl std::fmt::Debug for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionState")
            .field("id", &self.id)
            .field("thread_id", &self.thread_id.as_str())
            .field("turn_id", &self.turn_id.as_str())
            .field("working_dir", &self.working_dir)
            .field("created_at", &self.created_at)
            .field("resume_cursor", &self.resume_cursor())
            .field("identity", &self.identity())
            .finish()
    }
}

impl SessionState {
    /// Create a new session state
    pub fn new(id: String, thread_id: EntityId, turn_id: EntityId, working_dir: String) -> Self {
        Self {
            id,
            thread_id,
            turn_id,
            working_dir,
            status: std::sync::RwLock::new(SessionStateStatus::Pending),
            active: AtomicBool::new(true),
            created_at: Utc::now(),
            request_count: AtomicBool::new(false),
            total_output_tokens: std::sync::atomic::AtomicU32::new(0),
            resume_cursor: std::sync::RwLock::new(None),
            identity: std::sync::RwLock::new(None),
        }
    }

    /// Get the provider-side resume cursor, if one has been recorded.
    ///
    /// Returns a cloned `String` so callers can hand it to
    /// [`ProviderAdapter::resume_session`] without holding the session's lock.
    ///
    /// [`ProviderAdapter::resume_session`]: crate::trait_def::ProviderAdapter::resume_session
    pub fn resume_cursor(&self) -> Option<String> {
        self.resume_cursor.read().unwrap().clone()
    }

    /// Record (or clear) the provider-side resume cursor.
    ///
    /// Adapters call this once the provider returns a thread/conversation id
    /// so the cursor survives a server restart. Passing `None` clears any
    /// previously stored cursor.
    pub fn set_resume_cursor(&self, cursor: Option<String>) {
        *self.resume_cursor.write().unwrap() = cursor;
    }

    /// Get the provider/model/working-dir identity recorded for this session,
    /// if one was set (by `ensure_session_for_thread`).
    ///
    /// Returns a cloned [`SessionIdentity`] so callers can compare it against a
    /// requested identity without holding the session's lock.
    pub fn identity(&self) -> Option<SessionIdentity> {
        self.identity.read().unwrap().clone()
    }

    /// Record (or clear) the session's identity.
    ///
    /// `ensure_session_for_thread` calls this when it starts or restarts a
    /// session, so a subsequent call can decide reuse vs. restart by comparing
    /// the recorded identity against the newly requested one.
    pub fn set_identity(&self, identity: Option<SessionIdentity>) {
        *self.identity.write().unwrap() = identity;
    }

    /// Get the current session status
    pub fn status(&self) -> SessionStateStatus {
        *self.status.read().unwrap()
    }

    /// Transition to a new status
    pub fn set_status(&self, new_status: SessionStateStatus) {
        let mut status = self.status.write().unwrap();
        *status = new_status;
    }

    /// Check if the session is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Mark the session as inactive
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
    }

    /// Record additional output tokens
    pub fn add_tokens(&self, count: u32) {
        self.total_output_tokens.fetch_add(count, Ordering::Relaxed);
    }

    /// Get total output tokens
    pub fn total_tokens(&self) -> u32 {
        self.total_output_tokens.load(Ordering::Relaxed)
    }

    /// Validate that a status transition is legal
    pub fn can_transition_to(&self, target: SessionStateStatus) -> bool {
        let current = self.status();
        for (from, targets) in VALID_TRANSITIONS {
            if *from == current {
                return targets.contains(&target);
            }
        }
        false
    }

    /// Attempt a validated status transition. Returns Ok on success, Err with
    /// details if the transition is invalid.
    pub fn transition(&self, target: SessionStateStatus) -> Result<(), SessionTransitionError> {
        if self.can_transition_to(target) {
            self.set_status(target);
            Ok(())
        } else {
            Err(SessionTransitionError {
                session_id: self.id.clone(),
                from: self.status(),
                to: target,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Transition error
// ---------------------------------------------------------------------------

/// Error returned when an invalid session state transition is attempted
#[derive(Debug, thiserror::Error)]
#[error("invalid session transition for '{session_id}': {from:?} → {to:?}")]
pub struct SessionTransitionError {
    pub session_id: String,
    pub from: SessionStateStatus,
    pub to: SessionStateStatus,
}

// ---------------------------------------------------------------------------
// Session Manager — coordinates sessions across adapters
// ---------------------------------------------------------------------------

/// Manages all active provider sessions.
///
/// Responsibilities:
/// - Create sessions on the correct adapter
/// - Validate state transitions
/// - Track session → adapter mapping
/// - Handle interrupts and graceful shutdown
pub struct SessionManager {
    /// All active sessions keyed by session ID
    sessions: RwLock<HashMap<String, Arc<SessionState>>>,
    /// Map from turn_id → session_id (one turn = one session)
    turn_sessions: RwLock<HashMap<String, String>>,
    /// Map from thread_id → list of session_ids
    thread_sessions: RwLock<HashMap<String, Vec<String>>>,
}

impl SessionManager {
    /// Create a new empty session manager
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            turn_sessions: RwLock::new(HashMap::new()),
            thread_sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Start a new session on the given adapter
    pub async fn start_session(
        &self,
        adapter: &SharedAdapter,
        ctx: SessionContext,
    ) -> Result<Arc<SessionState>, ProviderAdapterError> {
        self.start_session_with_cursor(adapter, ctx, None).await
    }

    /// Like [`Self::start_session`] but seeds the new session with a resume
    /// cursor before returning.
    ///
    /// `ensure_session_for_thread` (in the command reactor) uses this on the
    /// restart path to carry the prior session's resume cursor onto the freshly
    /// started replacement session — so a model/provider/working-dir change
    /// doesn't discard the provider-side conversation position (the cursor
    /// survives rehydration per P0-4). When `resume_cursor` is `None` this is
    /// identical to [`Self::start_session`].
    pub async fn start_session_with_cursor(
        &self,
        adapter: &SharedAdapter,
        ctx: SessionContext,
        resume_cursor: Option<String>,
    ) -> Result<Arc<SessionState>, ProviderAdapterError> {
        let mut guard = adapter.write().await;
        let session_id = guard.start_session(ctx.clone()).await?;

        let session = Arc::new(SessionState::new(
            session_id.clone(),
            ctx.thread_id,
            ctx.turn_id,
            ctx.working_dir,
        ));
        // Seed the carried cursor (if any) before the session is observable.
        if let Some(cursor) = resume_cursor.as_ref() {
            session.set_resume_cursor(Some(cursor.clone()));
        }
        session
            .transition(SessionStateStatus::Processing)
            .map_err(|e| ProviderAdapterError::Internal(e.to_string()))?;

        // Track the session
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.clone(), session.clone());
        }
        {
            let mut turn_sessions = self.turn_sessions.write().await;
            turn_sessions.insert(ctx.turn_id.as_str(), session_id.clone());
        }
        {
            let mut thread_sessions = self.thread_sessions.write().await;
            thread_sessions
                .entry(ctx.thread_id.as_str())
                .or_default()
                .push(session_id.clone());
        }

        tracing::info!(
            session_id = %session_id,
            thread_id = %ctx.thread_id.as_str(),
            turn_id = %ctx.turn_id.as_str(),
            carried_cursor = resume_cursor.is_some(),
            "Session started and tracked"
        );

        Ok(session)
    }

    /// Resume an interrupted session
    pub async fn resume_session(
        &self,
        adapter: &SharedAdapter,
        session_id: &str,
    ) -> Result<(), ProviderAdapterError> {
        let session = self
            .get_session(session_id)
            .await
            .ok_or_else(|| ProviderAdapterError::SessionNotFound(session_id.to_string()))?;

        // Validate transition: Interrupted → Processing
        session
            .transition(SessionStateStatus::Processing)
            .map_err(|e| ProviderAdapterError::Internal(e.to_string()))?;

        let mut guard = adapter.write().await;
        guard.resume_session(session_id).await
    }

    /// Interrupt an active session (user cancel)
    pub async fn interrupt_session(
        &self,
        adapter: &SharedAdapter,
        session_id: &str,
    ) -> Result<(), ProviderAdapterError> {
        let session = self
            .get_session(session_id)
            .await
            .ok_or_else(|| ProviderAdapterError::SessionNotFound(session_id.to_string()))?;

        // Any active state → Interrupted
        let current = session.status();
        if current == SessionStateStatus::Pending
            || current == SessionStateStatus::Completed
            || current == SessionStateStatus::Errored
        {
            return Err(ProviderAdapterError::Internal(format!(
                "Cannot interrupt session in {:?} state",
                current
            )));
        }

        session
            .transition(SessionStateStatus::Interrupted)
            .map_err(|e| ProviderAdapterError::Internal(e.to_string()))?;

        let guard = adapter.read().await;
        guard.interrupt(session_id).await
    }

    /// Complete a session (normal completion)
    pub async fn complete_session(
        &self,
        adapter: &SharedAdapter,
        session_id: &str,
    ) -> Result<(), ProviderAdapterError> {
        let session = self
            .get_session(session_id)
            .await
            .ok_or_else(|| ProviderAdapterError::SessionNotFound(session_id.to_string()))?;

        session
            .transition(SessionStateStatus::Completed)
            .map_err(|e| ProviderAdapterError::Internal(e.to_string()))?;

        let mut guard = adapter.write().await;
        guard.stop_session(session_id).await
    }

    /// Force-stop a session (error or user cancel finalization)
    pub async fn stop_session(
        &self,
        adapter: &SharedAdapter,
        session_id: &str,
    ) -> Result<(), ProviderAdapterError> {
        let session = self
            .get_session(session_id)
            .await
            .ok_or_else(|| ProviderAdapterError::SessionNotFound(session_id.to_string()))?;

        session.deactivate();

        let mut guard = adapter.write().await;
        guard.stop_session(session_id).await
    }

    /// Get a session by ID
    pub async fn get_session(&self, session_id: &str) -> Option<Arc<SessionState>> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).cloned()
    }

    /// Get the session for a given turn
    pub async fn get_session_by_turn(&self, turn_id: &str) -> Option<Arc<SessionState>> {
        let turn_sessions = self.turn_sessions.read().await;
        let session_id = turn_sessions.get(turn_id).cloned()?;
        drop(turn_sessions);
        self.get_session(&session_id).await
    }

    /// Get all sessions for a thread
    pub async fn get_sessions_by_thread(&self, thread_id: &str) -> Vec<Arc<SessionState>> {
        let thread_sessions = self.thread_sessions.read().await;
        let session_ids = thread_sessions.get(thread_id).cloned().unwrap_or_default();
        drop(thread_sessions);

        let mut result = Vec::new();
        for sid in session_ids {
            if let Some(session) = self.get_session(&sid).await {
                result.push(session);
            }
        }
        result
    }

    /// List all active (non-completed, non-errored) session IDs
    pub async fn list_active_sessions(&self) -> Vec<String> {
        let sessions = self.sessions.read().await;
        sessions
            .iter()
            .filter(|(_, s)| {
                s.is_active()
                    && s.status() != SessionStateStatus::Completed
                    && s.status() != SessionStateStatus::Errored
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get total count of sessions
    pub async fn session_count(&self) -> usize {
        self.sessions.read().await.len()
    }

    /// Record the identity on a tracked session (best-effort).
    ///
    /// Returns `true` if the session was found and updated. Used by
    /// `ensure_session_for_thread` (in the command reactor) to stamp the
    /// provider/model/working-dir trio on a freshly started session so a
    /// subsequent call can decide reuse vs. restart.
    pub async fn set_session_identity(
        &self,
        session_id: &str,
        identity: SessionIdentity,
    ) -> bool {
        if let Some(session) = self.get_session(session_id).await {
            session.set_identity(Some(identity));
            true
        } else {
            false
        }
    }

    /// Interrupt all active sessions (e.g., thread cancellation)
    pub async fn interrupt_all(
        &self,
        adapter: &SharedAdapter,
    ) -> Vec<(String, Result<(), ProviderAdapterError>)> {
        let active = self.list_active_sessions().await;
        let mut results = Vec::new();
        for session_id in active {
            let result = self.interrupt_session(adapter, &session_id).await;
            results.push((session_id, result));
        }
        results
    }

    // -- Resume-cursor persistence -----------------------------------------
    //
    // On a server restart the in-memory `SessionManager` is lost — every
    // provider session that was in flight vanishes. The methods below pair
    // with [`ResumeCursorStore`] to persist enough of each session (its id +
    // provider-side resume cursor + thread/turn linkage + working dir) to
    // rebuild the manager after a restart and let the adapter reattach via
    // [`ProviderAdapter::resume_session`].
    //
    // [`ProviderAdapter::resume_session`]: crate::trait_def::ProviderAdapter::resume_session

    /// Snapshot every session that has a resume cursor into a vector of
    /// [`PersistedSessionCursor`] entries.
    ///
    /// Sessions without a cursor (`resume_cursor == None`) are skipped — the
    /// provider has nothing to reattach to, so there is no point persisting
    /// them. The returned vector is what [`Self::persist_sessions`] hands to
    /// a [`ResumeCursorStore`].
    pub async fn snapshot_cursorsors(&self) -> Vec<PersistedSessionCursor> {
        let sessions = self.sessions.read().await;
        sessions
            .values()
            .filter_map(|s| {
                s.resume_cursor().map(|cursor| PersistedSessionCursor {
                    session_id: s.id.clone(),
                    thread_id: s.thread_id,
                    turn_id: s.turn_id,
                    working_dir: s.working_dir.clone(),
                    resume_cursor: cursor,
                })
            })
            .collect()
    }

    /// Persist every cursor-bearing session through `store` (best-effort).
    ///
    /// Wraps [`Self::snapshot_cursorsors`] + [`ResumeCursorStore::save_all`].
    /// Returns the number of sessions persisted. Errors are logged at `WARN`
    /// and never propagated — persistence is best-effort, and a failure here
    /// must not crash the server (the sessions remain live in memory).
    pub async fn persist_sessions(&self, store: &dyn ResumeCursorStore) -> usize {
        let snapshot = self.snapshot_cursorsors().await;
        let count = snapshot.len();
        if let Err(e) = store.save_all(&snapshot).await {
            tracing::warn!(
                error = %e,
                persisted_count = count,
                "failed to persist session resume cursors — sessions remain live in memory",
            );
            return 0;
        }
        tracing::info!(persisted_count = count, "persisted session resume cursors");
        count
    }

    /// Rehydrate sessions from a [`ResumeCursorStore`] after a restart.
    ///
    /// For every persisted entry:
    /// 1. Register a fresh [`SessionState`] (keyed by the persisted
    ///    `session_id`) in the manager's three indices, mirroring
    ///    [`Self::start_session`]'s bookkeeping but **without** calling the
    ///    adapter's `start_session` (we already have a session id).
    /// 2. Seed it with the persisted `resume_cursor`.
    /// 3. Call `adapter.resume_session(session_id)` so the provider reattaches
    ///    to its in-flight conversation.
    ///
    /// Sessions whose adapter call fails are still tracked (their status
    /// reflects the failure via [`SessionStateStatus::Errored`]) so the
    /// operator can see them in `list_active_sessions`; the per-session result
    /// is returned for observability. The manager is always left in a
    /// consistent state regardless of how many adapter calls fail.
    pub async fn rehydrate_sessions(
        &self,
        store: &dyn ResumeCursorStore,
        adapter: &SharedAdapter,
    ) -> Vec<RehydratedSession> {
        let entries = match store.load_all().await {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to load persisted session cursors — starting with no rehydrated sessions",
                );
                return Vec::new();
            }
        };

        tracing::info!(
            persisted_count = entries.len(),
            "rehydrating sessions from persisted resume cursors"
        );
        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            let session = Arc::new(SessionState::new(
                entry.session_id.clone(),
                entry.thread_id,
                entry.turn_id,
                entry.working_dir.clone(),
            ));
            // Seed the cursor before the adapter call so it is observable even
            // if the provider reattach fails.
            session.set_resume_cursor(Some(entry.resume_cursor.clone()));
            // Rehydrated sessions start in Processing — that is the state they
            // were in before the restart (only Processing sessions are
            // reattachable; Interrupted sessions would resume explicitly).
            session
                .transition(SessionStateStatus::Processing)
                .map_err(|e| ProviderAdapterError::Internal(e.to_string()))
                .ok();

            // Track in all three indices (mirrors start_session).
            {
                let mut sessions = self.sessions.write().await;
                sessions.insert(entry.session_id.clone(), session.clone());
            }
            {
                let mut turn_sessions = self.turn_sessions.write().await;
                turn_sessions.insert(entry.turn_id.as_str(), entry.session_id.clone());
            }
            {
                let mut thread_sessions = self.thread_sessions.write().await;
                thread_sessions
                    .entry(entry.thread_id.as_str())
                    .or_default()
                    .push(entry.session_id.clone());
            }

            // Ask the provider to reattach. Best-effort: a failure marks the
            // session Errored but does not abort the rest of the rehydration.
            let mut guard = adapter.write().await;
            let outcome = match guard.resume_session(&entry.session_id).await {
                Ok(()) => RehydrationOutcome::Reattached,
                Err(e) => {
                    tracing::warn!(
                        session_id = %entry.session_id,
                        error = %e,
                        "provider failed to resume rehydrated session",
                    );
                    session.set_status(SessionStateStatus::Errored);
                    RehydrationOutcome::Failed(e.to_string())
                }
            };
            drop(guard);

            results.push(RehydratedSession {
                session_id: entry.session_id,
                outcome,
            });
        }
        results
    }
}

/// Outcome of rehydrating a single persisted session.
#[derive(Debug, Clone)]
pub struct RehydratedSession {
    /// The session id that was rehydrated.
    pub session_id: String,
    /// Whether the adapter's `resume_session` succeeded.
    pub outcome: RehydrationOutcome,
}

/// Per-session result of [`SessionManager::rehydrate_sessions`].
#[derive(Debug, Clone)]
pub enum RehydrationOutcome {
    /// The adapter reattached to the provider-side session.
    Reattached,
    /// The adapter's `resume_session` failed; the session is tracked but
    /// marked `Errored`. Carries the error message.
    Failed(String),
}

// ---------------------------------------------------------------------------
// Resume-cursor persistence — survives a server restart
// ---------------------------------------------------------------------------

/// A serialized snapshot of one session's resume cursor.
///
/// [`SessionManager::snapshot_cursorsors`] produces a `Vec<PersistedSessionCursor>`
/// which a [`ResumeCursorStore`] writes to disk; on the next start
/// [`SessionManager::rehydrate_sessions`] reads them back and re-registers
/// each session + asks the adapter to reattach.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PersistedSessionCursor {
    /// The session id (provider-assigned) to reattach.
    pub session_id: String,
    /// The thread this session belongs to.
    pub thread_id: EntityId,
    /// The turn this session processes.
    pub turn_id: EntityId,
    /// Working directory for the provider.
    pub working_dir: String,
    /// The provider-side resume cursor (e.g. a provider thread id).
    pub resume_cursor: String,
}

/// Errors that can occur while persisting/loading resume cursors.
#[derive(Debug, thiserror::Error)]
pub enum ResumeCursorStoreError {
    /// Filesystem I/O failure (read, write, create).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization/deserialization failure (corrupt file or a
    /// non-array stored shape).
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Pluggable persistence for session resume cursors.
///
/// Two reference implementations are provided:
/// - [`FileResumeCursorStore`] — JSON file at `~/.syncode/session_cursors.json`
///   (production; survives restarts).
/// - [`InMemoryResumeCursorStore`] — `Arc<Mutex<…>>` (tests).
///
/// The trait is async so a future SQLite-backed implementation can drop in
/// without touching call sites.
#[async_trait::async_trait]
pub trait ResumeCursorStore: Send + Sync {
    /// Replace the on-disk document with `entries` (full snapshot — not a
    /// merge). Called by [`SessionManager::persist_sessions`].
    async fn save_all(
        &self,
        entries: &[PersistedSessionCursor],
    ) -> Result<(), ResumeCursorStoreError>;

    /// Load every persisted cursor. Returns an empty vector when the store is
    /// empty or missing (fresh start). Called by
    /// [`SessionManager::rehydrate_sessions`].
    async fn load_all(&self) -> Result<Vec<PersistedSessionCursor>, ResumeCursorStoreError>;
}

/// File-backed `ResumeCursorStore` writing a single JSON document to
/// `{base_dir}/session_cursors.json`.
///
/// The default `base_dir` is `~/.syncode/` (resolving `$HOME` then
/// `$USERPROFILE`), matching the `server_home_dir` / `ScrollbackStore`
/// resolution used elsewhere in the codebase. Tests inject a `tempfile`
/// directory via [`FileResumeCursorStore::with_dir`].
///
/// Writes are atomic (write-to-`.tmp` + rename) so a reader never sees a
/// half-written document — mirrors the `ScrollbackStore` write strategy.
#[derive(Debug, Clone)]
pub struct FileResumeCursorStore {
    base_dir: std::path::PathBuf,
}

impl FileResumeCursorStore {
    /// Create a store rooted at the default location (`~/.syncode/`).
    ///
    /// Falls back to `./.syncode/` when neither `$HOME` nor `$USERPROFILE`
    /// is set, matching [`crate`] conventions.
    pub fn new() -> Self {
        Self {
            base_dir: default_cursor_dir(),
        }
    }

    /// Create a store rooted at `base_dir` (tests inject a `tempfile` dir).
    pub fn with_dir(base_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// The full path to the JSON document.
    fn file_path(&self) -> std::path::PathBuf {
        self.base_dir.join("session_cursors.json")
    }

    /// The full path to the sibling `.tmp` file used by atomic writes.
    fn tmp_path(&self) -> std::path::PathBuf {
        let mut name = self
            .file_path()
            .file_name()
            .map(|s| s.to_os_string())
            .unwrap_or_else(|| std::ffi::OsString::from("session_cursors.json"));
        name.push(".tmp");
        self.file_path().with_file_name(name)
    }
}

impl Default for FileResumeCursorStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ResumeCursorStore for FileResumeCursorStore {
    async fn save_all(
        &self,
        entries: &[PersistedSessionCursor],
    ) -> Result<(), ResumeCursorStoreError> {
        let json = serde_json::to_string(entries)
            .map_err(|e| ResumeCursorStoreError::Serialization(e.to_string()))?;
        let path = self.file_path();
        let tmp = self.tmp_path();

        // Ensure the base directory exists (best-effort; ignore "already
        // exists" — mirrors `ScrollbackStore` posture).
        if let Some(parent) = path.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
            && e.kind() != std::io::ErrorKind::AlreadyExists
        {
            return Err(ResumeCursorStoreError::Io(e));
        }

        // Write-to-tmp then rename — atomic on both POSIX and Windows.
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(json.as_bytes())?;
            file.sync_all().map_err(ResumeCursorStoreError::Io)?;
        }
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    async fn load_all(&self) -> Result<Vec<PersistedSessionCursor>, ResumeCursorStoreError> {
        let path = self.file_path();
        let json = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Fresh start — no persisted cursors.
                return Ok(Vec::new());
            }
            Err(e) => return Err(ResumeCursorStoreError::Io(e)),
        };
        if json.trim().is_empty() {
            return Ok(Vec::new());
        }
        serde_json::from_str::<Vec<PersistedSessionCursor>>(&json)
            .map_err(|e| ResumeCursorStoreError::Serialization(e.to_string()))
    }
}

/// Resolve the default base directory: `$HOME/.syncode` on POSIX,
/// `$USERPROFILE/.syncode` on Windows. Falls back to `./.syncode` when
/// neither env var is set (mirrors `ScrollbackStore::default_base_dir`).
fn default_cursor_dir() -> std::path::PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."));
    home.join(".syncode")
}

/// In-memory `ResumeCursorStore` for tests.
///
/// Holds a single snapshot behind an `async RwLock`; `save_all` replaces it,
/// `load_all` clones it. Drop-in replacement for [`FileResumeCursorStore`]
/// without touching the filesystem.
#[derive(Debug, Default)]
pub struct InMemoryResumeCursorStore {
    entries: tokio::sync::RwLock<Vec<PersistedSessionCursor>>,
}

impl InMemoryResumeCursorStore {
    /// Create an empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ResumeCursorStore for InMemoryResumeCursorStore {
    async fn save_all(
        &self,
        entries: &[PersistedSessionCursor],
    ) -> Result<(), ResumeCursorStoreError> {
        let mut guard = self.entries.write().await;
        *guard = entries.to_vec();
        Ok(())
    }

    async fn load_all(&self) -> Result<Vec<PersistedSessionCursor>, ResumeCursorStoreError> {
        Ok(self.entries.read().await.clone())
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_session() -> Arc<SessionState> {
        Arc::new(SessionState::new(
            "test-session".to_string(),
            EntityId::new(),
            EntityId::new(),
            "/tmp/project".to_string(),
        ))
    }

    #[test]
    fn session_new_defaults() {
        let session = make_session();
        assert_eq!(session.id, "test-session");
        assert_eq!(session.working_dir, "/tmp/project");
        assert!(session.is_active());
        assert_eq!(session.status(), SessionStateStatus::Pending);
        assert_eq!(session.total_tokens(), 0);
    }

    #[test]
    fn session_resume_cursor_defaults_none() {
        // A fresh session has no resume cursor — nothing to reattach to.
        let session = make_session();
        assert!(session.resume_cursor().is_none());
    }

    #[test]
    fn session_resume_cursor_round_trip() {
        // set_resume_cursor then resume_cursor returns a clone of the value.
        let session = make_session();
        session.set_resume_cursor(Some("provider-thread-123".to_string()));
        assert_eq!(
            session.resume_cursor().as_deref(),
            Some("provider-thread-123")
        );

        // Clearing works.
        session.set_resume_cursor(None);
        assert!(session.resume_cursor().is_none());
    }

    #[test]
    fn session_status_transitions() {
        let session = make_session();
        assert_eq!(session.status(), SessionStateStatus::Pending);

        session.set_status(SessionStateStatus::Processing);
        assert_eq!(session.status(), SessionStateStatus::Processing);

        session.set_status(SessionStateStatus::Completed);
        assert_eq!(session.status(), SessionStateStatus::Completed);
    }

    #[test]
    fn session_deactivate() {
        let session = make_session();
        assert!(session.is_active());
        session.deactivate();
        assert!(!session.is_active());
    }

    #[test]
    fn session_token_counting() {
        let session = make_session();
        session.add_tokens(100);
        session.add_tokens(50);
        assert_eq!(session.total_tokens(), 150);
    }

    #[test]
    fn session_status_equality() {
        assert_eq!(SessionStateStatus::Pending, SessionStateStatus::Pending);
        assert_ne!(SessionStateStatus::Pending, SessionStateStatus::Processing);
    }

    #[test]
    fn valid_transition_pending_to_processing() {
        let session = make_session();
        assert!(session.can_transition_to(SessionStateStatus::Processing));
        session.transition(SessionStateStatus::Processing).unwrap();
        assert_eq!(session.status(), SessionStateStatus::Processing);
    }

    #[test]
    fn valid_transition_processing_to_completed() {
        let session = make_session();
        session.set_status(SessionStateStatus::Processing);
        assert!(session.can_transition_to(SessionStateStatus::Completed));
        session.transition(SessionStateStatus::Completed).unwrap();
        assert_eq!(session.status(), SessionStateStatus::Completed);
    }

    #[test]
    fn valid_transition_processing_to_interrupted_to_processing() {
        let session = make_session();
        session.set_status(SessionStateStatus::Processing);
        session.transition(SessionStateStatus::Interrupted).unwrap();
        session.transition(SessionStateStatus::Processing).unwrap();
        assert_eq!(session.status(), SessionStateStatus::Processing);
    }

    #[test]
    fn invalid_transition_pending_to_completed() {
        let session = make_session();
        assert!(!session.can_transition_to(SessionStateStatus::Completed));
        let result = session.transition(SessionStateStatus::Completed);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.from, SessionStateStatus::Pending);
        assert_eq!(err.to, SessionStateStatus::Completed);
    }

    #[test]
    fn invalid_transition_completed_to_processing() {
        let session = make_session();
        session.set_status(SessionStateStatus::Processing);
        session.set_status(SessionStateStatus::Completed);
        assert!(!session.can_transition_to(SessionStateStatus::Processing));
    }

    #[test]
    fn transition_error_display() {
        let err = SessionTransitionError {
            session_id: "s-1".to_string(),
            from: SessionStateStatus::Pending,
            to: SessionStateStatus::Completed,
        };
        assert!(err.to_string().contains("s-1"));
        assert!(err.to_string().contains("Pending"));
        assert!(err.to_string().contains("Completed"));
    }

    // ---------------------------------------------------------------------------
    // SessionManager tests (using MockAdapter)
    // ---------------------------------------------------------------------------

    /// A minimal mock adapter for session manager tests
    struct MockSessionAdapter {
        spawned: std::sync::atomic::AtomicBool,
        sessions: std::sync::Mutex<Vec<String>>,
    }

    impl MockSessionAdapter {
        fn new() -> Self {
            Self {
                spawned: std::sync::atomic::AtomicBool::new(true), // pre-spawned
                sessions: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ProviderAdapter for MockSessionAdapter {
        fn provider_id(&self) -> &str {
            "mock-session"
        }
        fn capabilities(&self) -> Vec<ProviderCapability> {
            vec![]
        }
        fn status(&self) -> ProviderStatus {
            ProviderStatus::Idle
        }
        fn available_models(&self) -> Vec<String> {
            vec!["mock".to_string()]
        }

        async fn spawn(&mut self, _config: ProviderConfig) -> Result<(), ProviderAdapterError> {
            self.spawned
                .store(true, std::sync::atomic::Ordering::Release);
            Ok(())
        }

        async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
            self.spawned
                .store(false, std::sync::atomic::Ordering::Release);
            Ok(())
        }

        async fn interrupt(&self, _session_id: &str) -> Result<(), ProviderAdapterError> {
            Ok(())
        }

        async fn start_session(
            &mut self,
            _ctx: SessionContext,
        ) -> Result<String, ProviderAdapterError> {
            let sid = format!("mock-{}", uuid::Uuid::new_v4().hyphenated());
            self.sessions.lock().unwrap().push(sid.clone());
            Ok(sid)
        }

        async fn resume_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
            let sessions = self.sessions.lock().unwrap();
            if !sessions.contains(&session_id.to_string()) {
                return Err(ProviderAdapterError::SessionNotFound(
                    session_id.to_string(),
                ));
            }
            Ok(())
        }

        async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
            let mut sessions = self.sessions.lock().unwrap();
            if let Some(pos) = sessions.iter().position(|s| s == session_id) {
                sessions.remove(pos);
            } else {
                return Err(ProviderAdapterError::SessionNotFound(
                    session_id.to_string(),
                ));
            }
            Ok(())
        }

        async fn send_request(
            &self,
            _request: ProviderRequest,
        ) -> Result<ProviderResponse, ProviderAdapterError> {
            Ok(ProviderResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(1),
                result: Some(serde_json::json!({})),
                error: None,
            })
        }

        fn event_stream(&self, _session_id: &str) -> Result<ProviderStream, ProviderAdapterError> {
            Ok(Box::pin(tokio_stream::empty()))
        }

        async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
            Ok(true)
        }
    }

    fn make_shared_mock() -> SharedAdapter {
        Arc::new(RwLock::new(MockSessionAdapter::new()))
    }

    fn make_session_ctx() -> SessionContext {
        SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp/test".to_string(),
            system_prompt: None,
            user_input: "test input".to_string(),
            context_files: vec![],
        }
    }

    #[tokio::test]
    async fn session_manager_new() {
        let mgr = SessionManager::new();
        assert_eq!(mgr.session_count().await, 0);
        assert!(mgr.list_active_sessions().await.is_empty());
    }

    #[tokio::test]
    async fn session_manager_start_and_get() {
        let mgr = SessionManager::new();
        let adapter = make_shared_mock();
        let ctx = make_session_ctx();

        let session = mgr.start_session(&adapter, ctx).await.unwrap();
        assert_eq!(mgr.session_count().await, 1);
        assert!(session.is_active());
        assert_eq!(session.status(), SessionStateStatus::Processing);

        // Get by session ID
        let fetched = mgr.get_session(&session.id).await.unwrap();
        assert_eq!(fetched.id, session.id);
    }

    #[tokio::test]
    async fn session_manager_get_by_turn() {
        let mgr = SessionManager::new();
        let adapter = make_shared_mock();
        let ctx = make_session_ctx();
        let turn_id_str = ctx.turn_id.as_str();

        let session = mgr.start_session(&adapter, ctx).await.unwrap();

        let found = mgr.get_session_by_turn(&turn_id_str).await.unwrap();
        assert_eq!(found.id, session.id);
    }

    #[tokio::test]
    async fn session_manager_get_by_thread() {
        let mgr = SessionManager::new();
        let adapter = make_shared_mock();
        let thread_id = EntityId::new();
        let ctx1 = SessionContext {
            thread_id,
            turn_id: EntityId::new(),
            working_dir: "/tmp".to_string(),
            system_prompt: None,
            user_input: "input 1".to_string(),
            context_files: vec![],
        };
        let ctx2 = SessionContext {
            thread_id,
            turn_id: EntityId::new(),
            working_dir: "/tmp".to_string(),
            system_prompt: None,
            user_input: "input 2".to_string(),
            context_files: vec![],
        };

        mgr.start_session(&adapter, ctx1).await.unwrap();
        mgr.start_session(&adapter, ctx2).await.unwrap();

        let thread_sessions = mgr.get_sessions_by_thread(&thread_id.as_str()).await;
        assert_eq!(thread_sessions.len(), 2);
    }

    #[tokio::test]
    async fn session_manager_resume() {
        let mgr = SessionManager::new();
        let adapter = make_shared_mock();
        let ctx = make_session_ctx();

        let session = mgr.start_session(&adapter, ctx).await.unwrap();

        // Interrupt first
        mgr.interrupt_session(&adapter, &session.id).await.unwrap();
        assert_eq!(session.status(), SessionStateStatus::Interrupted);

        // Resume
        mgr.resume_session(&adapter, &session.id).await.unwrap();
        assert_eq!(session.status(), SessionStateStatus::Processing);
    }

    #[tokio::test]
    async fn session_manager_complete() {
        let mgr = SessionManager::new();
        let adapter = make_shared_mock();
        let ctx = make_session_ctx();

        let session = mgr.start_session(&adapter, ctx).await.unwrap();
        mgr.complete_session(&adapter, &session.id).await.unwrap();
        assert_eq!(session.status(), SessionStateStatus::Completed);
    }

    #[tokio::test]
    async fn session_manager_list_active() {
        let mgr = SessionManager::new();
        let adapter = make_shared_mock();

        let s1 = mgr
            .start_session(&adapter, make_session_ctx())
            .await
            .unwrap();
        let s2 = mgr
            .start_session(&adapter, make_session_ctx())
            .await
            .unwrap();

        assert_eq!(mgr.list_active_sessions().await.len(), 2);

        // Complete one
        mgr.complete_session(&adapter, &s1.id).await.unwrap();
        let active = mgr.list_active_sessions().await;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0], s2.id);
    }

    #[tokio::test]
    async fn session_manager_get_nonexistent() {
        let mgr = SessionManager::new();
        assert!(mgr.get_session("nope").await.is_none());
        assert!(mgr.get_session_by_turn("nope").await.is_none());
    }

    #[tokio::test]
    async fn session_manager_interrupt_nonexistent() {
        let mgr = SessionManager::new();
        let adapter = make_shared_mock();
        let result = mgr.interrupt_session(&adapter, "nope").await;
        assert!(result.is_err());
    }

    // ---------------------------------------------------------------------------
    // P0-4: Resume-cursor persistence + rehydration tests
    // ---------------------------------------------------------------------------

    /// A mock adapter whose `resume_session` succeeds only for session ids
    /// registered via [`Self::allow_resume`]. Used by the rehydration tests to
    /// exercise both the success and failure paths without touching a real
    /// provider.
    struct ResumableMockAdapter {
        /// Session ids that `resume_session` will accept.
        resumable: std::sync::Mutex<Vec<String>>,
    }

    impl ResumableMockAdapter {
        fn new() -> Self {
            Self {
                resumable: std::sync::Mutex::new(Vec::new()),
            }
        }

        /// Allow `resume_session` to succeed for this session id.
        fn allow_resume(&self, session_id: &str) {
            self.resumable
                .lock()
                .unwrap()
                .push(session_id.to_string());
        }
    }

    #[async_trait::async_trait]
    impl ProviderAdapter for ResumableMockAdapter {
        fn provider_id(&self) -> &str {
            "resumable-mock"
        }
        fn capabilities(&self) -> Vec<ProviderCapability> {
            vec![]
        }
        fn status(&self) -> ProviderStatus {
            ProviderStatus::Idle
        }
        fn available_models(&self) -> Vec<String> {
            vec!["mock".to_string()]
        }

        async fn spawn(&mut self, _config: ProviderConfig) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn interrupt(&self, _session_id: &str) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn start_session(
            &mut self,
            _ctx: SessionContext,
        ) -> Result<String, ProviderAdapterError> {
            Ok(format!("session-{}", uuid::Uuid::new_v4().hyphenated()))
        }

        async fn resume_session(
            &mut self,
            session_id: &str,
        ) -> Result<(), ProviderAdapterError> {
            let resumable = self.resumable.lock().unwrap();
            if resumable.contains(&session_id.to_string()) {
                Ok(())
            } else {
                Err(ProviderAdapterError::SessionNotFound(session_id.to_string()))
            }
        }

        async fn stop_session(
            &mut self,
            _session_id: &str,
        ) -> Result<(), ProviderAdapterError> {
            Ok(())
        }
        async fn send_request(
            &self,
            _request: ProviderRequest,
        ) -> Result<ProviderResponse, ProviderAdapterError> {
            Ok(ProviderResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(1),
                result: Some(serde_json::json!({})),
                error: None,
            })
        }
        fn event_stream(
            &self,
            _session_id: &str,
        ) -> Result<ProviderStream, ProviderAdapterError> {
            Ok(Box::pin(tokio_stream::empty()))
        }
        async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn in_memory_store_round_trip() {
        // save_all then load_all returns the same entries.
        let store = InMemoryResumeCursorStore::new();
        let entries = vec![PersistedSessionCursor {
            session_id: "sess-1".to_string(),
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp/x".to_string(),
            resume_cursor: "cursor-1".to_string(),
        }];
        store.save_all(&entries).await.unwrap();
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded, entries);
    }

    #[tokio::test]
    async fn in_memory_store_load_empty_returns_empty_vec() {
        // A fresh store has no entries — load_all returns an empty vec (not an
        // error), so rehydration is a no-op on first boot.
        let store = InMemoryResumeCursorStore::new();
        let loaded = store.load_all().await.unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn file_store_round_trip_under_tempdir() {
        // The file store persists to {dir}/session_cursors.json; a save then
        // load round-trips the entries exactly.
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileResumeCursorStore::with_dir(dir.path());
        let entries = vec![
            PersistedSessionCursor {
                session_id: "sess-a".to_string(),
                thread_id: EntityId::new(),
                turn_id: EntityId::new(),
                working_dir: "/tmp/a".to_string(),
                resume_cursor: "cursor-a".to_string(),
            },
            PersistedSessionCursor {
                session_id: "sess-b".to_string(),
                thread_id: EntityId::new(),
                turn_id: EntityId::new(),
                working_dir: "/tmp/b".to_string(),
                resume_cursor: "cursor-b".to_string(),
            },
        ];
        store.save_all(&entries).await.unwrap();
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded, entries);
        // The file actually exists on disk.
        assert!(dir.path().join("session_cursors.json").exists());
    }

    #[tokio::test]
    async fn file_store_load_missing_returns_empty_vec() {
        // A non-existent file is the first-boot case — load returns an empty
        // vec rather than an error.
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileResumeCursorStore::with_dir(dir.path());
        let loaded = store.load_all().await.unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn file_store_save_replaces_prior_snapshot() {
        // save_all is a full-snapshot replace — a second save with fewer
        // entries yields exactly those entries (no append, no leftovers).
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileResumeCursorStore::with_dir(dir.path());

        let first = vec![PersistedSessionCursor {
            session_id: "sess-1".to_string(),
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp".to_string(),
            resume_cursor: "c1".to_string(),
        }];
        store.save_all(&first).await.unwrap();

        let second = vec![PersistedSessionCursor {
            session_id: "sess-2".to_string(),
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp".to_string(),
            resume_cursor: "c2".to_string(),
        }];
        store.save_all(&second).await.unwrap();

        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].session_id, "sess-2");
    }

    #[tokio::test]
    async fn snapshot_skips_cursorless_sessions() {
        // Only sessions with a resume cursor are snapshotted — sessions with
        // no cursor are skipped (the provider has nothing to reattach to).
        let mgr = SessionManager::new();
        let adapter = make_shared_mock();

        // Start two sessions; neither has a cursor yet.
        let s1 = mgr
            .start_session(&adapter, make_session_ctx())
            .await
            .unwrap();
        let s2 = mgr
            .start_session(&adapter, make_session_ctx())
            .await
            .unwrap();

        // Give only s1 a cursor.
        s1.set_resume_cursor(Some("provider-thread-1".to_string()));

        let snapshot = mgr.snapshot_cursorsors().await;
        assert_eq!(snapshot.len(), 1, "only the cursor-bearing session appears");
        assert_eq!(snapshot[0].session_id, s1.id);
        assert_eq!(snapshot[0].resume_cursor, "provider-thread-1");
        // s2 is omitted.
        assert!(
            !snapshot.iter().any(|e| e.session_id == s2.id),
            "cursorless session must be skipped"
        );
    }

    #[tokio::test]
    async fn persist_then_rehydrate_round_trip() {
        // The full P0-4 lifecycle: start sessions, record cursors, persist to
        // an in-memory store, then on a "restart" (fresh manager) rehydrate —
        // the rehydrated sessions are tracked and the adapter's
        // `resume_session` is called for each.
        let adapter = make_shared_mock();

        // --- Pre-restart: build the original manager + cursors ---
        let mgr = SessionManager::new();
        let ctx1 = make_session_ctx();
        let ctx2 = make_session_ctx();

        let s1 = mgr.start_session(&adapter, ctx1).await.unwrap();
        let s2 = mgr.start_session(&adapter, ctx2).await.unwrap();
        s1.set_resume_cursor(Some("cursor-1".to_string()));
        s2.set_resume_cursor(Some("cursor-2".to_string()));

        // Persist via an in-memory store (simulates the JSON file on disk).
        let store = Arc::new(InMemoryResumeCursorStore::new());
        let persisted = mgr.persist_sessions(store.as_ref()).await;
        assert_eq!(persisted, 2, "both cursor-bearing sessions persisted");

        // --- Restart: fresh manager + fresh adapter state ---
        // A real restart re-creates both the SessionManager and the provider
        // adapter. Build a ResumableMockAdapter that pre-registers the two
        // persisted session ids so its `resume_session` succeeds — this
        // models "the provider still knows about these sessions".
        let new_mgr = SessionManager::new();
        let new_adapter: SharedAdapter = {
            let entries = store.load_all().await.unwrap();
            let mock = ResumableMockAdapter::new();
            for e in &entries {
                mock.allow_resume(&e.session_id);
            }
            Arc::new(RwLock::new(mock))
        };
        // The pre-restart adapter is dropped (out of scope after the restart).
        drop(adapter);

        // Rehydrate.
        let results = new_mgr.rehydrate_sessions(store.as_ref(), &new_adapter).await;
        assert_eq!(results.len(), 2, "both sessions rehydrated");

        // Both should have reattached.
        let mut reattached_ids: Vec<String> = results
            .iter()
            .map(|r| match &r.outcome {
                RehydrationOutcome::Reattached => r.session_id.clone(),
                RehydrationOutcome::Failed(msg) => panic!("unexpected failure: {msg}"),
            })
            .collect();
        reattached_ids.sort();
        assert_eq!(reattached_ids.len(), 2);

        // The new manager tracks both sessions, and each carries its cursor.
        assert_eq!(new_mgr.session_count().await, 2);
        for sid in &reattached_ids {
            let session = new_mgr.get_session(sid).await.expect("tracked");
            assert!(
                session.resume_cursor().is_some(),
                "rehydrated session must carry its cursor"
            );
        }

        // The persisted session ids match the originals.
        let mut original_ids = vec![s1.id.clone(), s2.id.clone()];
        original_ids.sort();
        assert_eq!(reattached_ids, original_ids);
    }

    #[tokio::test]
    async fn rehydrate_marks_failed_resume_as_errored() {
        // When the adapter's resume_session fails for a session, the manager
        // still tracks it but marks it Errored — so a single bad session
        // doesn't abort the whole rehydration.
        let store = InMemoryResumeCursorStore::new();
        let entry = PersistedSessionCursor {
            session_id: "doomed-session".to_string(),
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp".to_string(),
            resume_cursor: "cursor".to_string(),
        };
        store.save_all(&[entry]).await.unwrap();

        // The mock does not pre-register the session → resume_session errors.
        let adapter: SharedAdapter = Arc::new(RwLock::new(ResumableMockAdapter::new()));
        let mgr = SessionManager::new();

        let results = mgr.rehydrate_sessions(&store, &adapter).await;
        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0].outcome,
            RehydrationOutcome::Failed(_)
        ));

        // The session is tracked but Errored.
        let session = mgr.get_session("doomed-session").await.expect("tracked");
        assert_eq!(session.status(), SessionStateStatus::Errored);
        // Its cursor was seeded before the failure, so it is still observable.
        assert_eq!(session.resume_cursor().as_deref(), Some("cursor"));
    }

    #[tokio::test]
    async fn rehydrate_empty_store_is_no_op() {
        // First boot (no persisted file) → load returns empty → rehydrate is a
        // no-op, the manager stays empty, and the adapter is never called.
        let store = InMemoryResumeCursorStore::new();
        let adapter: SharedAdapter = Arc::new(RwLock::new(ResumableMockAdapter::new()));
        let mgr = SessionManager::new();

        let results = mgr.rehydrate_sessions(&store, &adapter).await;
        assert!(results.is_empty());
        assert_eq!(mgr.session_count().await, 0);
    }

    #[tokio::test]
    async fn persist_via_file_store_survives_new_manager() {
        // End-to-end through the file store: persist from one manager, load +
        // rehydrate into a second manager — proves the JSON file is the bridge
        // between two separate `SessionManager` instances (i.e. across a
        // restart).
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileResumeCursorStore::with_dir(dir.path());

        // First manager: start a session, set a cursor, persist.
        let adapter = make_shared_mock();
        let mgr_a = SessionManager::new();
        let session = mgr_a
            .start_session(&adapter, make_session_ctx())
            .await
            .unwrap();
        session.set_resume_cursor(Some("file-cursor".to_string()));
        let persisted = mgr_a.persist_sessions(&store).await;
        assert_eq!(persisted, 1);

        // Second manager: rehydrate from the same file store.
        let mgr_b = SessionManager::new();
        // Pre-register the session id so the mock's resume_session succeeds.
        let new_adapter: SharedAdapter = {
            let mock = ResumableMockAdapter::new();
            mock.allow_resume(&session.id);
            Arc::new(RwLock::new(mock))
        };
        let results = mgr_b.rehydrate_sessions(&store, &new_adapter).await;
        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0].outcome,
            RehydrationOutcome::Reattached
        ));

        // The session id from the first manager is now tracked in the second.
        let rehydrated = mgr_b.get_session(&session.id).await.expect("tracked");
        assert_eq!(rehydrated.resume_cursor().as_deref(), Some("file-cursor"));
    }
}

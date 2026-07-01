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
}

impl std::fmt::Debug for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionState")
            .field("id", &self.id)
            .field("thread_id", &self.thread_id.as_str())
            .field("turn_id", &self.turn_id.as_str())
            .field("working_dir", &self.working_dir)
            .field("created_at", &self.created_at)
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
        }
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
        let mut guard = adapter.write().await;
        let session_id = guard.start_session(ctx.clone()).await?;

        let session = Arc::new(SessionState::new(
            session_id.clone(),
            ctx.thread_id,
            ctx.turn_id,
            ctx.working_dir,
        ));
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
}

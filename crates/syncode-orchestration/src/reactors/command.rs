//! Provider Command Reactor
//!
//! Translates domain-level intents (from the Decider's Commands) into
//! actual provider adapter calls.
//!
//! This is the "write side" of the provider bridge:
//! - Command::StartTurn → adapter.start_session() + adapter.send_request()
//! - Command::FailTurn → adapter.interrupt()
//! - Command::CancelTurn → adapter.stop_session()
//! - Command::PauseThread → adapter.interrupt() all sessions
//! - Command::CancelThread → adapter.stop_session() all sessions

use syncode_core::EntityId;
use syncode_provider::{
    ProviderEvent, ProviderRequest,
    SessionContext, SessionManager, SessionStateStatus,
};

use crate::decider::Command;

/// Result of executing a command on a provider
#[derive(Debug, Clone)]
pub struct CommandReaction {
    /// Whether the command was handled
    pub handled: bool,
    /// Session ID if a session was started (for StartTurn)
    pub session_id: Option<String>,
    /// Provider events collected during execution (stub for now)
    pub events: Vec<ProviderEvent>,
}

/// The command reactor bridges domain commands to provider adapter calls.
///
/// It holds a reference to a `SessionManager` for session lifecycle.
pub struct ProviderCommandReactor {
    session_manager: SessionManager,
}

impl ProviderCommandReactor {
    /// Create a new command reactor
    pub fn new(session_manager: SessionManager) -> Self {
        Self { session_manager }
    }

    /// Get a reference to the session manager
    pub fn session_manager(&self) -> &SessionManager {
        &self.session_manager
    }

    /// React to a domain command by invoking the provider adapter.
    ///
    /// Returns a `CommandReaction` indicating what happened.
    /// This does NOT produce domain events — the ingestion reactor handles
    /// the reverse direction (provider events → domain events).
    pub async fn react(
        &self,
        command: &Command,
        adapter: &syncode_provider::registry::SharedAdapter,
        turn_id_hint: Option<EntityId>,
    ) -> Result<CommandReaction, CommandReactorError> {
        match command {
            Command::StartTurn {
                thread_id,
                sequence,
                user_input,
            } => {
                self.handle_start_turn(
                    turn_id_hint,
                    *thread_id,
                    *sequence,
                    user_input,
                    adapter,
                )
                .await
            }

            Command::FailTurn { id, error: _ } => {
                self.handle_fail_turn(*id, adapter).await
            }

            Command::CancelTurn { id } => {
                self.handle_cancel_turn(*id, adapter).await
            }

            Command::InterruptTurn { id } => {
                // Interrupt an in-progress turn: interrupt the provider session (if any).
                self.handle_interrupt_turn(*id, adapter).await
            }

            Command::PauseThread { id: _ } => {
                // Pause thread: interrupt all active sessions
                let results = self.session_manager.interrupt_all(adapter).await;
                Ok(CommandReaction {
                    handled: !results.is_empty(),
                    session_id: None,
                    events: vec![],
                })
            }

            Command::CancelThread { id: _ } => {
                // Cancel thread: stop all active sessions
                let active = self.session_manager.list_active_sessions().await;
                for sid in active {
                    let _ = self.session_manager.stop_session(adapter, &sid).await;
                }
                Ok(CommandReaction {
                    handled: true,
                    session_id: None,
                    events: vec![],
                })
            }

            Command::StopThreadSession { id } => {
                // Stop the active provider sessions backing THIS thread (not every
                // thread's sessions). Uses the SessionManager's thread→session index;
                // among a thread's sessions we stop the ones still active.
                let mut handled = false;
                let sessions = self.session_manager.get_sessions_by_thread(&id.as_str()).await;
                for session in &sessions {
                    if session.is_active() {
                        let _ = self.session_manager.stop_session(adapter, &session.id).await;
                        handled = true;
                    }
                }
                Ok(CommandReaction {
                    handled,
                    session_id: None,
                    events: vec![],
                })
            }

            // Provider-dispatch commands (T6 turn interactions). The Decider records
            // the client's response/edits via Requested-style events; these arms
            // dispatch the actual response to the provider session currently
            // Processing the thread. Faithful to mcode's approval/user-input
            // response-requested dispatch. If no session is Processing the thread
            // (nothing awaiting input), there is nothing to dispatch (handled = false).
            Command::RespondThreadApproval { id, request_id, decision } => {
                let payload = serde_json::json!({
                    "request_id": request_id,
                    "decision": decision,
                });
                let session_id = self
                    .dispatch_to_thread_session(*id, "approval/respond", payload, adapter)
                    .await?;
                Ok(CommandReaction {
                    handled: session_id.is_some(),
                    session_id,
                    events: vec![],
                })
            }
            Command::RespondThreadUserInput { id, request_id, answers } => {
                let payload = serde_json::json!({
                    "request_id": request_id,
                    "answers": answers,
                });
                let session_id = self
                    .dispatch_to_thread_session(*id, "user-input/respond", payload, adapter)
                    .await?;
                Ok(CommandReaction {
                    handled: session_id.is_some(),
                    session_id,
                    events: vec![],
                })
            }
            Command::EditAndResendThreadMessage { id, message_id, text } => {
                let payload = serde_json::json!({
                    "message_id": message_id.as_str(),
                    "text": text,
                });
                let session_id = self
                    .dispatch_to_thread_session(*id, "message/edit-and-resend", payload, adapter)
                    .await?;
                Ok(CommandReaction {
                    handled: session_id.is_some(),
                    session_id,
                    events: vec![],
                })
            }

            // Commands that don't need provider interaction
            Command::CreateProject { .. }
            | Command::UpdateProjectConfig { .. }
            | Command::DeleteProject { .. }
            | Command::CreateThread { .. }
            | Command::ResumeThread { .. }
            | Command::CompleteThread { .. }
            | Command::SetThreadTitle { .. }
            | Command::ArchiveThread { .. }
            | Command::UnarchiveThread { .. }
            | Command::DeleteThread { .. }
            | Command::SetThreadRuntimeMode { .. }
            | Command::SetThreadInteractionMode { .. }
            | Command::AppendThreadActivity { .. }
            | Command::AddPinnedMessage { .. }
            | Command::RemovePinnedMessage { .. }
            | Command::SetPinnedMessageDone { .. }
            | Command::SetPinnedMessageLabel { .. }
            | Command::AddMarker { .. }
            | Command::RemoveMarker { .. }
            | Command::SetMarkerDone { .. }
            | Command::SetMarkerLabel { .. }
            | Command::HandoffCreateThread { .. }
            | Command::ForkCreateThread { .. }
            | Command::RevertToCheckpoint { .. }
            | Command::CompleteTurn { .. }
            | Command::RecordTurnFiles { .. }
            | Command::SetTurnCheckpoint { .. }
            | Command::AddMessage { .. } => Ok(CommandReaction {
                handled: false,
                session_id: None,
                events: vec![],
            }),
        }
    }

    /// Find the id of the session currently Processing a thread, if any.
    ///
    /// Routes provider-dispatch commands (approval / user-input / edit-resend)
    /// to the one session actively Processing the thread. Uses the
    /// SessionManager's thread→session index; among a thread's sessions returns
    /// the most recent one in the Processing state (the session awaiting input).
    async fn active_session_id_for_thread(&self, thread_id: EntityId) -> Option<String> {
        self.session_manager
            .get_sessions_by_thread(&thread_id.as_str())
            .await
            .into_iter()
            .filter(|s| s.is_active() && s.status() == SessionStateStatus::Processing)
            .max_by_key(|s| s.created_at.timestamp_millis())
            .map(|s| s.id.clone())
    }

    /// Dispatch a JSON-RPC request to a thread's active Processing session.
    ///
    /// Returns the targeted session id on success, or `None` if no session is
    /// Processing the thread (nothing to dispatch to → `handled = false`). The
    /// `session_id` is injected into the request params so the provider adapter
    /// can correlate the response to its session (syncode's `send_request` is
    /// session-agnostic by design).
    async fn dispatch_to_thread_session(
        &self,
        thread_id: EntityId,
        method: &str,
        payload: serde_json::Value,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<Option<String>, CommandReactorError> {
        let Some(session_id) = self.active_session_id_for_thread(thread_id).await else {
            return Ok(None);
        };

        let mut params = match payload {
            serde_json::Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        params.insert("session_id".to_string(), serde_json::Value::String(session_id.clone()));

        let request = ProviderRequest::new(method, Some(serde_json::Value::Object(params)));
        let guard = adapter.read().await;
        guard
            .send_request(request)
            .await
            .map_err(|e| CommandReactorError::ProviderError(e.to_string()))?;

        Ok(Some(session_id))
    }

    /// Handle StartTurn: create a provider session and send the initial request
    async fn handle_start_turn(
        &self,
        turn_id: Option<EntityId>,
        thread_id: EntityId,
        sequence: u32,
        user_input: &str,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<CommandReaction, CommandReactorError> {
        let turn_id = turn_id.unwrap_or_else(EntityId::new);

        // Check if a session already exists for this turn
        if let Some(existing) = self.session_manager.get_session_by_turn(&turn_id.as_str()).await {
            return Ok(CommandReaction {
                handled: true,
                session_id: Some(existing.id.clone()),
                events: vec![],
            });
        }

        // Build session context
        let ctx = SessionContext {
            thread_id,
            turn_id,
            working_dir: "/tmp/syncode".to_string(),
            system_prompt: Some("You are a helpful AI coding assistant.".to_string()),
            user_input: user_input.to_string(),
            context_files: vec![],
        };

        // Start the session
        let session = self.session_manager.start_session(adapter, ctx).await?;
        let session_id = session.id.clone();

        // Send the initial request to the provider
        let request = ProviderRequest::new(
            "chat",
            Some(serde_json::json!({
                "input": user_input,
                "sequence": sequence,
            })),
        );

        let guard = adapter.read().await;
        let _resp = guard.send_request(request).await
            .map_err(|e| CommandReactorError::ProviderError(e.to_string()))?;

        Ok(CommandReaction {
            handled: true,
            session_id: Some(session_id),
            events: vec![],
        })
    }

    /// Handle FailTurn: interrupt the session
    async fn handle_fail_turn(
        &self,
        turn_id: EntityId,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<CommandReaction, CommandReactorError> {
        self.interrupt_session_for_turn(turn_id, adapter).await
    }

    /// Handle InterruptTurn: interrupt the session (same lifecycle as FailTurn).
    async fn handle_interrupt_turn(
        &self,
        turn_id: EntityId,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<CommandReaction, CommandReactorError> {
        self.interrupt_session_for_turn(turn_id, adapter).await
    }

    /// Interrupt the provider session backing a turn, if one exists.
    ///
    /// Shared by `FailTurn` and `InterruptTurn` — both interrupt an in-flight
    /// provider session, differing only in the domain event the Decider emits.
    async fn interrupt_session_for_turn(
        &self,
        turn_id: EntityId,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<CommandReaction, CommandReactorError> {
        let session = self.session_manager.get_session_by_turn(&turn_id.as_str()).await;
        if let Some(session) = session {
            let _ = self.session_manager.interrupt_session(adapter, &session.id).await;
            Ok(CommandReaction {
                handled: true,
                session_id: Some(session.id.clone()),
                events: vec![],
            })
        } else {
            Ok(CommandReaction {
                handled: false,
                session_id: None,
                events: vec![],
            })
        }
    }

    /// Handle CancelTurn: stop the session
    async fn handle_cancel_turn(
        &self,
        turn_id: EntityId,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<CommandReaction, CommandReactorError> {
        let session = self.session_manager.get_session_by_turn(&turn_id.as_str()).await;
        if let Some(session) = session {
            let _ = self.session_manager.stop_session(adapter, &session.id).await;
            Ok(CommandReaction {
                handled: true,
                session_id: Some(session.id.clone()),
                events: vec![],
            })
        } else {
            Ok(CommandReaction {
                handled: false,
                session_id: None,
                events: vec![],
            })
        }
    }
}

/// Errors during command reaction
#[derive(Debug, thiserror::Error)]
pub enum CommandReactorError {
    #[error("Provider error: {0}")]
    ProviderError(String),

    #[error("Session error: {0}")]
    SessionError(String),
}

impl From<syncode_provider::ProviderAdapterError> for CommandReactorError {
    fn from(e: syncode_provider::ProviderAdapterError) -> Self {
        CommandReactorError::ProviderError(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use syncode_provider::{ProviderAdapter, ProviderConfig, ProviderResponse, ProviderStatus};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// Mock adapter for command reactor tests
    struct CmdTestMock {
        started_sessions: std::sync::Mutex<Vec<String>>,
        interrupted: std::sync::Mutex<Vec<String>>,
        stopped: Arc<std::sync::Mutex<Vec<String>>>,
        /// (method, params) for every dispatched JSON-RPC request
        requests: Arc<std::sync::Mutex<Vec<(String, Option<serde_json::Value>)>>>,
    }

    impl CmdTestMock {
        fn new() -> Self {
            Self {
                started_sessions: std::sync::Mutex::new(Vec::new()),
                interrupted: std::sync::Mutex::new(Vec::new()),
                stopped: Arc::new(std::sync::Mutex::new(Vec::new())),
                requests: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        /// Construct with shared recording handles the test can inspect directly
        /// (the adapter is read back as a `dyn ProviderAdapter`, so its fields
        /// are not reachable through the trait object).
        fn new_with_handles() -> (
            Self,
            Arc<std::sync::Mutex<Vec<String>>>,
            Arc<std::sync::Mutex<Vec<(String, Option<serde_json::Value>)>>>,
        ) {
            let stopped = Arc::new(std::sync::Mutex::new(Vec::new()));
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let this = Self {
                started_sessions: std::sync::Mutex::new(Vec::new()),
                interrupted: std::sync::Mutex::new(Vec::new()),
                stopped: Arc::clone(&stopped),
                requests: Arc::clone(&requests),
            };
            (this, stopped, requests)
        }
    }

    #[async_trait::async_trait]
    impl ProviderAdapter for CmdTestMock {
        fn provider_id(&self) -> &str { "cmd-test-mock" }
        fn capabilities(&self) -> Vec<syncode_provider::ProviderCapability> { vec![] }
        fn status(&self) -> ProviderStatus { ProviderStatus::Idle }
        fn available_models(&self) -> Vec<String> { vec!["mock".to_string()] }

        async fn spawn(&mut self, _config: ProviderConfig) -> Result<(), syncode_provider::ProviderAdapterError> { Ok(()) }
        async fn shutdown(&mut self) -> Result<(), syncode_provider::ProviderAdapterError> { Ok(()) }

        async fn interrupt(&self, session_id: &str) -> Result<(), syncode_provider::ProviderAdapterError> {
            self.interrupted.lock().unwrap().push(session_id.to_string());
            Ok(())
        }

        async fn start_session(&mut self, _ctx: SessionContext) -> Result<String, syncode_provider::ProviderAdapterError> {
            let sid = format!("cmd-{}", uuid::Uuid::new_v4().hyphenated());
            self.started_sessions.lock().unwrap().push(sid.clone());
            Ok(sid)
        }

        async fn resume_session(&mut self, _session_id: &str) -> Result<(), syncode_provider::ProviderAdapterError> { Ok(()) }

        async fn stop_session(&mut self, session_id: &str) -> Result<(), syncode_provider::ProviderAdapterError> {
            self.stopped.lock().unwrap().push(session_id.to_string());
            Ok(())
        }

        async fn send_request(&self, request: ProviderRequest) -> Result<ProviderResponse, syncode_provider::ProviderAdapterError> {
            self.requests.lock().unwrap().push((request.method.clone(), request.params.clone()));
            Ok(ProviderResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(1),
                result: Some(serde_json::json!({"ok": true})),
                error: None,
            })
        }

        fn event_stream(&self, _session_id: &str) -> Result<syncode_provider::ProviderStream, syncode_provider::ProviderAdapterError> {
            Ok(Box::pin(tokio_stream::empty()))
        }

        async fn health_check(&self) -> Result<bool, syncode_provider::ProviderAdapterError> { Ok(true) }
    }

    fn make_shared_test_mock() -> syncode_provider::registry::SharedAdapter {
        Arc::new(RwLock::new(CmdTestMock::new()))
    }

    /// Like `make_shared_test_mock` but also returns shared handles for the
    /// recorded `stopped` session ids and dispatched `requests`.
    fn make_recorded_test_mock() -> (
        syncode_provider::registry::SharedAdapter,
        Arc<std::sync::Mutex<Vec<String>>>,
        Arc<std::sync::Mutex<Vec<(String, Option<serde_json::Value>)>>>,
    ) {
        let (mock, stopped, requests) = CmdTestMock::new_with_handles();
        (Arc::new(RwLock::new(mock)), stopped, requests)
    }

    #[tokio::test]
    async fn start_turn_creates_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let turn_id = EntityId::new();
        let thread_id = EntityId::new();
        let command = Command::StartTurn {
            thread_id,
            sequence: 1,
            user_input: "Fix the bug".to_string(),
        };

        let result = reactor.react(&command, &adapter, Some(turn_id)).await.unwrap();
        assert!(result.handled);
        assert!(result.session_id.is_some());
    }

    #[tokio::test]
    async fn fail_turn_interrupts_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let turn_id = EntityId::new();
        let thread_id = EntityId::new();

        // Start a turn first
        reactor.react(&Command::StartTurn {
            thread_id, sequence: 1, user_input: "test".to_string(),
        }, &adapter, Some(turn_id)).await.unwrap();

        // Now fail it
        let result = reactor.react(&Command::FailTurn {
            id: turn_id,
            error: "Something went wrong".to_string(),
        }, &adapter, None).await.unwrap();
        assert!(result.handled);
    }

    #[tokio::test]
    async fn cancel_turn_stops_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let turn_id = EntityId::new();
        let thread_id = EntityId::new();

        reactor.react(&Command::StartTurn {
            thread_id, sequence: 1, user_input: "test".to_string(),
        }, &adapter, Some(turn_id)).await.unwrap();

        let result = reactor.react(&Command::CancelTurn {
            id: turn_id,
        }, &adapter, None).await.unwrap();
        assert!(result.handled);
    }

    #[tokio::test]
    async fn non_provider_commands_not_handled() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let result = reactor.react(&Command::CreateProject {
            name: "Test".to_string(),
            root_path: "/tmp".to_string(),
        }, &adapter, None).await.unwrap();

        assert!(!result.handled);
        assert!(result.session_id.is_none());
    }

    #[tokio::test]
    async fn fail_turn_no_session_not_handled() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let result = reactor.react(&Command::FailTurn {
            id: EntityId::new(),
            error: "error".to_string(),
        }, &adapter, None).await.unwrap();

        assert!(!result.handled);
    }

    #[tokio::test]
    async fn cancel_turn_no_session_not_handled() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let result = reactor.react(&Command::CancelTurn {
            id: EntityId::new(),
        }, &adapter, None).await.unwrap();

        assert!(!result.handled);
    }

    #[tokio::test]
    async fn add_message_not_handled() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let result = reactor.react(&Command::AddMessage {
            turn_id: EntityId::new(),
            role: "user".to_string(),
            content: "hello".to_string(),
        }, &adapter, None).await.unwrap();

        assert!(!result.handled);
    }

    /// Helper: start a turn so a Processing session exists for the thread.
    async fn start_turn(reactor: &ProviderCommandReactor, adapter: &syncode_provider::registry::SharedAdapter, thread_id: EntityId) {
        reactor
            .react(
                &Command::StartTurn { thread_id, sequence: 1, user_input: "hi".to_string() },
                adapter,
                Some(EntityId::new()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn respond_approval_dispatches_to_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();
        start_turn(&reactor, &adapter, thread_id).await;

        let result = reactor
            .react(
                &Command::RespondThreadApproval {
                    id: thread_id,
                    request_id: "req-1".to_string(),
                    decision: "approved".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(result.handled);
        let session_id = result.session_id.clone().expect("session id");

        let reqs = requests.lock().unwrap().clone();
        // [0] = "chat" (StartTurn), [1] = approval/respond
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[1].0, "approval/respond");
        let params = reqs[1].1.as_ref().expect("params");
        assert_eq!(params["session_id"].as_str(), Some(session_id.as_str()));
        assert_eq!(params["request_id"].as_str(), Some("req-1"));
        assert_eq!(params["decision"].as_str(), Some("approved"));
    }

    #[tokio::test]
    async fn respond_user_input_dispatches_to_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();
        start_turn(&reactor, &adapter, thread_id).await;

        let result = reactor
            .react(
                &Command::RespondThreadUserInput {
                    id: thread_id,
                    request_id: "req-2".to_string(),
                    answers: "yes".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(result.handled);
        let reqs = requests.lock().unwrap().clone();
        assert_eq!(reqs.last().unwrap().0, "user-input/respond");
        let params = reqs.last().unwrap().1.as_ref().expect("params");
        assert_eq!(params["request_id"].as_str(), Some("req-2"));
        assert_eq!(params["answers"].as_str(), Some("yes"));
        assert!(params["session_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn edit_and_resend_dispatches_to_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();
        start_turn(&reactor, &adapter, thread_id).await;
        let message_id = EntityId::new();

        let result = reactor
            .react(
                &Command::EditAndResendThreadMessage {
                    id: thread_id,
                    message_id,
                    text: "edited".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(result.handled);
        let reqs = requests.lock().unwrap().clone();
        assert_eq!(reqs.last().unwrap().0, "message/edit-and-resend");
        let params = reqs.last().unwrap().1.as_ref().expect("params");
        assert_eq!(params["message_id"].as_str(), Some(message_id.as_str().as_str()));
        assert_eq!(params["text"].as_str(), Some("edited"));
    }

    #[tokio::test]
    async fn provider_dispatch_no_session_not_handled() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests) = make_recorded_test_mock();

        // No session was started for this thread → nothing to dispatch to.
        let result = reactor
            .react(
                &Command::RespondThreadApproval {
                    id: EntityId::new(),
                    request_id: "req-x".to_string(),
                    decision: "denied".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(!result.handled);
        assert!(result.session_id.is_none());
        assert!(requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stop_thread_session_stops_only_its_thread() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, stopped, _requests) = make_recorded_test_mock();
        let thread_a = EntityId::new();
        let thread_b = EntityId::new();

        let a = start_turn_capture(&reactor, &adapter, thread_a).await;
        let b = start_turn_capture(&reactor, &adapter, thread_b).await;

        reactor
            .react(&Command::StopThreadSession { id: thread_a }, &adapter, None)
            .await
            .unwrap();

        let stopped = stopped.lock().unwrap().clone();
        assert!(stopped.contains(&a), "thread A's session should be stopped");
        assert!(!stopped.contains(&b), "thread B's session should be left running");
    }

    /// Like start_turn but returns the created session id (for stop-scoping assertions).
    async fn start_turn_capture(
        reactor: &ProviderCommandReactor,
        adapter: &syncode_provider::registry::SharedAdapter,
        thread_id: EntityId,
    ) -> String {
        let r = reactor
            .react(
                &Command::StartTurn { thread_id, sequence: 1, user_input: "hi".to_string() },
                adapter,
                Some(EntityId::new()),
            )
            .await
            .unwrap();
        r.session_id.expect("session id")
    }
}

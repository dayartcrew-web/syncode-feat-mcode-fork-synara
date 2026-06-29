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
    SessionContext, SessionManager,
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

            Command::StopThreadSession { id: _ } => {
                // Stop the thread's active provider session. SessionManager has no
                // thread→session index yet, so stop all active sessions (same effect as
                // CancelThread). A thread-scoped stop needs a session-by-thread lookup.
                let active = self.session_manager.list_active_sessions().await;
                for sid in active {
                    let _ = self.session_manager.stop_session(adapter, &sid).await;
                }
                Ok(CommandReaction {
                    handled: !self.session_manager.list_active_sessions().await.is_empty(),
                    session_id: None,
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
        stopped: std::sync::Mutex<Vec<String>>,
    }

    impl CmdTestMock {
        fn new() -> Self {
            Self {
                started_sessions: std::sync::Mutex::new(Vec::new()),
                interrupted: std::sync::Mutex::new(Vec::new()),
                stopped: std::sync::Mutex::new(Vec::new()),
            }
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

        async fn send_request(&self, _request: ProviderRequest) -> Result<ProviderResponse, syncode_provider::ProviderAdapterError> {
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
}

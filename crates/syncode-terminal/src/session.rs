//! Terminal session — lifecycle management for PTY sessions
//!
//! Manages the lifecycle of terminal sessions: create, list, attach,
//! detach, and destroy. Each session has a PTY handle and output buffer.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::output::OutputBuffer;
use crate::pty::{PtyError, PtyHandle, PtyProcessInfo};

/// A terminal session with PTY and output buffer
pub struct TerminalSession {
    /// PTY handle
    pty: PtyHandle,
    /// Output buffer
    output: OutputBuffer,
    /// Session ID
    session_id: String,
    /// Creation timestamp
    created_at: String,
}

impl TerminalSession {
    /// Create a new terminal session
    pub fn new(
        session_id: String,
        command: &str,
        args: &[&str],
        working_dir: Option<&str>,
        cols: u16,
        rows: u16,
    ) -> Result<Self, PtyError> {
        let pty = PtyHandle::spawn(session_id.clone(), command, args, working_dir, cols, rows)?;
        let output = OutputBuffer::new(1000, 4096);

        Ok(Self {
            pty,
            output,
            session_id,
            created_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    /// Get session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Get PTY handle reference
    pub fn pty(&self) -> &PtyHandle {
        &self.pty
    }

    /// Get mutable output buffer reference
    pub fn output(&self) -> &OutputBuffer {
        &self.output
    }

    /// Get mutable output buffer
    pub fn output_mut(&mut self) -> &mut OutputBuffer {
        &mut self.output
    }

    /// Get creation timestamp
    pub fn created_at(&self) -> &str {
        &self.created_at
    }

    /// Whether the PTY process is still running
    pub fn is_alive(&self) -> bool {
        self.pty.is_running()
    }

    /// Get process info
    pub fn process_info(&self) -> PtyProcessInfo {
        self.pty.info()
    }

    /// Resize the terminal
    pub async fn resize(&self, cols: u16, rows: u16) -> Result<(), PtyError> {
        self.pty.resize(cols, rows).await
    }
}

/// Terminal session manager
pub struct SessionManager {
    sessions: RwLock<HashMap<String, Arc<RwLock<TerminalSession>>>>,
}

impl SessionManager {
    /// Create a new session manager
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new terminal session
    pub async fn create_session(
        &self,
        command: &str,
        args: &[&str],
        working_dir: Option<&str>,
        cols: u16,
        rows: u16,
    ) -> Result<String, PtyError> {
        let session_id = format!("term-{}", uuid::Uuid::new_v4().hyphenated());
        self.create_session_with_id(session_id, command, args, working_dir, cols, rows)
            .await
    }

    /// Create a new terminal session under a caller-provided session id.
    ///
    /// This is the WS-RPC entry point (T6c-5): the cloned MCode UI keys
    /// terminal sessions by `terminalId` (a stable string it generates), so
    /// the WS handler calls this with that id to keep the UI's session
    /// references stable across `open`/`write`/`resize`/`close`. If a session
    /// with the given id already exists it is overwritten (re-open semantics).
    pub async fn create_session_with_id(
        &self,
        session_id: String,
        command: &str,
        args: &[&str],
        working_dir: Option<&str>,
        cols: u16,
        rows: u16,
    ) -> Result<String, PtyError> {
        let session =
            TerminalSession::new(session_id.clone(), command, args, working_dir, cols, rows)?;
        self.sessions
            .write()
            .await
            .insert(session_id.clone(), Arc::new(RwLock::new(session)));
        Ok(session_id)
    }

    /// Get a session by ID
    pub async fn get_session(&self, session_id: &str) -> Option<Arc<RwLock<TerminalSession>>> {
        self.sessions.read().await.get(session_id).cloned()
    }

    /// List all sessions
    pub async fn list_sessions(&self) -> Vec<SessionInfo> {
        let sessions = self.sessions.read().await;
        let mut info = Vec::new();
        for (id, session) in sessions.iter() {
            let s = session.read().await;
            info.push(SessionInfo {
                session_id: id.clone(),
                pid: s.pty().pid(),
                alive: s.is_alive(),
                created_at: s.created_at().to_string(),
                cols: s.pty().size().0,
                rows: s.pty().size().1,
            });
        }
        info
    }

    /// Destroy a session
    pub async fn destroy_session(&self, session_id: &str) -> bool {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.remove(session_id) {
            let s = session.read().await;
            s.pty().mark_stopped();
            true
        } else {
            false
        }
    }

    /// Count active sessions
    pub async fn count(&self) -> usize {
        self.sessions.read().await.len()
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary info about a terminal session
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub session_id: String,
    pub pid: u32,
    pub alive: bool,
    pub created_at: String,
    pub cols: u16,
    pub rows: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_manager_new() {
        let mgr = SessionManager::new();
        assert_eq!(mgr.count().await, 0);
        let sessions = mgr.list_sessions().await;
        assert!(sessions.is_empty());
    }

    #[tokio::test]
    async fn session_manager_list_empty() {
        let mgr = SessionManager::new();
        let info = mgr.list_sessions().await;
        assert!(info.is_empty());
    }

    #[tokio::test]
    async fn session_manager_destroy_nonexistent() {
        let mgr = SessionManager::new();
        assert!(!mgr.destroy_session("nonexistent").await);
    }

    #[tokio::test]
    async fn session_info_serialization() {
        let info = SessionInfo {
            session_id: "term-123".to_string(),
            pid: 12345,
            alive: true,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            cols: 80,
            rows: 24,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("sessionId")); // camelCase
        assert!(!json.contains("session_id"));
        let back: SessionInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.pid, 12345);
        assert!(back.alive);
    }

    #[test]
    fn terminal_session_fields() {
        // Just test the types compile — can't spawn real PTYs in tests easily
        assert_eq!(PtyError::NotRunning.to_string(), "PTY not running");
    }
}

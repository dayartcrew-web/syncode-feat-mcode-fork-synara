//! Terminal Tauri Commands
//!
//! IPC commands for terminal PTY management: create sessions,
//! write input, read output, resize, and destroy sessions.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use syncode_terminal::SessionManager;
use tokio::sync::RwLock;

/// Shared session manager state
pub type SharedSessionManager = Arc<RwLock<SessionManager>>;

/// Result for creating a terminal session
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminalSessionResult {
    pub session_id: String,
    pub pid: u32,
    pub cols: u16,
    pub rows: u16,
}

/// Result for listing sessions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminalSessionInfo {
    pub session_id: String,
    pub pid: u32,
    pub alive: bool,
    pub created_at: String,
    pub cols: u16,
    pub rows: u16,
}

/// Result for reading terminal output
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminalOutputResult {
    pub session_id: String,
    pub chunks: Vec<TerminalOutputChunk>,
    pub has_more: bool,
}

/// Output chunk for frontend
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminalOutputChunk {
    pub seq: u64,
    pub data: String,
    pub timestamp: String,
}

/// Create a new terminal session
#[tauri::command]
pub async fn terminal_create_session(
    command: String,
    args: Vec<String>,
    working_dir: Option<String>,
    cols: Option<u16>,
    rows: Option<u16>,
    manager: tauri::State<'_, SharedSessionManager>,
) -> Result<TerminalSessionResult, String> {
    let mgr = manager.inner().clone();
    let cols = cols.unwrap_or(80);
    let rows = rows.unwrap_or(24);

    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let session_id = mgr
        .write()
        .await
        .create_session(&command, &arg_refs, working_dir.as_deref(), cols, rows)
        .await
        .map_err(|e| e.to_string())?;

    let session = mgr
        .read()
        .await
        .get_session(&session_id)
        .await
        .ok_or("Session not found after creation")?;
    let session = session.read().await;

    Ok(TerminalSessionResult {
        session_id: session_id.clone(),
        pid: session.pty().pid(),
        cols: session.pty().size().0,
        rows: session.pty().size().1,
    })
}

/// List all terminal sessions
#[tauri::command]
pub async fn terminal_list_sessions(
    manager: tauri::State<'_, SharedSessionManager>,
) -> Result<Vec<TerminalSessionInfo>, String> {
    let mgr = manager.inner().clone();
    let sessions = mgr.read().await.list_sessions().await;
    Ok(sessions
        .into_iter()
        .map(|s| TerminalSessionInfo {
            session_id: s.session_id,
            pid: s.pid,
            alive: s.alive,
            created_at: s.created_at,
            cols: s.cols,
            rows: s.rows,
        })
        .collect())
}

/// Destroy a terminal session
#[tauri::command]
pub async fn terminal_destroy_session(
    session_id: String,
    manager: tauri::State<'_, SharedSessionManager>,
) -> Result<bool, String> {
    let mgr = manager.inner().clone();
    Ok(mgr.write().await.destroy_session(&session_id).await)
}

/// Resize a terminal session
#[tauri::command]
pub async fn terminal_resize(
    session_id: String,
    cols: u16,
    rows: u16,
    manager: tauri::State<'_, SharedSessionManager>,
) -> Result<(), String> {
    let mgr = manager.inner().clone();
    let session = mgr
        .read()
        .await
        .get_session(&session_id)
        .await
        .ok_or("Session not found")?;
    session
        .write()
        .await
        .resize(cols, rows)
        .await
        .map_err(|e| e.to_string())
}

/// Write input to a terminal session
#[tauri::command]
pub async fn terminal_write(
    session_id: String,
    data: String,
    manager: tauri::State<'_, SharedSessionManager>,
) -> Result<(), String> {
    let mgr = manager.inner().clone();
    let session = mgr
        .read()
        .await
        .get_session(&session_id)
        .await
        .ok_or("Session not found")?;
    session
        .read()
        .await
        .pty()
        .write_str(&data)
        .await
        .map_err(|e| e.to_string())
}

/// Read output from a terminal session since a given sequence
#[tauri::command]
pub async fn terminal_read_output(
    session_id: String,
    from_seq: Option<u64>,
    manager: tauri::State<'_, SharedSessionManager>,
) -> Result<TerminalOutputResult, String> {
    let mgr = manager.inner().clone();
    let session = mgr
        .read()
        .await
        .get_session(&session_id)
        .await
        .ok_or("Session not found")?;
    let session = session.read().await;

    let from = from_seq.unwrap_or(0);
    let chunks = session.output().chunks_from(from);
    let buffered = session.output().buffered_bytes();

    Ok(TerminalOutputResult {
        session_id: session_id.clone(),
        chunks: chunks
            .into_iter()
            .map(|c| TerminalOutputChunk {
                seq: c.seq,
                data: c.data.clone(),
                timestamp: c.timestamp.clone(),
            })
            .collect(),
        has_more: buffered > 0,
    })
}

/// Acknowledge terminal output up to a sequence number
#[tauri::command]
pub async fn terminal_ack(
    session_id: String,
    seq: u64,
    manager: tauri::State<'_, SharedSessionManager>,
) -> Result<(), String> {
    let mgr = manager.inner().clone();
    let session = mgr
        .read()
        .await
        .get_session(&session_id)
        .await
        .ok_or("Session not found")?;
    session.write().await.output_mut().ack(seq);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_session_result_serialization() {
        let result = TerminalSessionResult {
            session_id: "term-123".to_string(),
            pid: 12345,
            cols: 80,
            rows: 24,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("sessionId"));
        assert!(json.contains("12345"));
        let back: TerminalSessionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.cols, 80);
    }

    #[test]
    fn terminal_output_chunk_serialization() {
        let chunk = TerminalOutputChunk {
            seq: 42,
            data: "hello".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&chunk).unwrap();
        assert!(json.contains("hello"));
        let back: TerminalOutputChunk = serde_json::from_str(&json).unwrap();
        assert_eq!(back.seq, 42);
    }
}

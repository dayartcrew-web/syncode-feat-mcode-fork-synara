//! PTY — pseudo-terminal process management
//!
//! Wraps `portable_pty` to spawn processes with PTY support,
//! handle resize, write input, and read output.

use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use thiserror::Error;

/// PTY errors
#[derive(Debug, Error)]
pub enum PtyError {
    #[error("Failed to spawn PTY: {0}")]
    SpawnFailed(String),
    #[error("PTY not running")]
    NotRunning,
    #[error("IO error: {0}")]
    Io(String),
    #[error("PTY system error: {0}")]
    PtySystem(String),
}

/// Information about a running PTY process
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyProcessInfo {
    pub pid: u32,
    pub cols: u16,
    pub rows: u16,
    pub working_dir: String,
    pub command: String,
}

/// A managed PTY session
pub struct PtyHandle {
    /// PTY master handle
    master: Arc<tokio::sync::Mutex<Box<dyn MasterPty + Send>>>,
    /// Read half of the PTY — a `std::sync::Mutex` because the PTY reader is
    /// inherently blocking I/O (it has no readiness notification; `read`
    /// blocks until bytes arrive). Wrapping it in `tokio::sync::Mutex` would
    /// block the async executor thread. The reader is consumed from a
    /// dedicated blocking reader thread (see `PtyHandle::read_output_blocking`)
    /// so a `std::sync::Mutex` — which never crosses an `.await` — is both
    /// correct and the lightest primitive.
    reader: Arc<Mutex<Box<dyn Read + Send>>>,
    /// Write half of the PTY — `tokio::sync::Mutex` because `write` is called
    /// from async contexts (the WS `terminal.write` handler) and should not
    /// block a reactor thread while another write is in flight.
    writer: Arc<tokio::sync::Mutex<Box<dyn Write + Send>>>,
    /// Process ID
    pid: u32,
    /// Whether the process is still running
    running: AtomicBool,
    /// Session ID
    session_id: String,
    /// Terminal dimensions
    cols: AtomicU64,
    rows: AtomicU64,
}

impl PtyHandle {
    /// Spawn a new PTY process
    pub fn spawn(
        session_id: String,
        command: &str,
        args: &[&str],
        working_dir: Option<&str>,
        cols: u16,
        rows: u16,
    ) -> Result<Self, PtyError> {
        let pty_system = native_pty_system();

        let pair = pty_system
            .openpty(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| PtyError::PtySystem(e.to_string()))?;

        let mut cmd = CommandBuilder::new(command);
        for arg in args {
            cmd.arg(arg);
        }
        if let Some(dir) = working_dir {
            cmd.cwd(dir);
        }
        cmd.env("TERM", "xterm-256color");

        let child = pair
            .slave
            .spawn_command(cmd)
            .map_err(|e| PtyError::SpawnFailed(e.to_string()))?;

        let pid = child.process_id();
        // Drop the child — the process continues running
        drop(child);

        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|e| PtyError::Io(e.to_string()))?;

        let writer = pair
            .master
            .take_writer()
            .map_err(|e| PtyError::Io(e.to_string()))?;

        let pid = pid.unwrap_or(0);

        Ok(Self {
            master: Arc::new(tokio::sync::Mutex::new(pair.master)),
            reader: Arc::new(Mutex::new(Box::new(reader))),
            writer: Arc::new(tokio::sync::Mutex::new(Box::new(writer))),
            pid,
            running: AtomicBool::new(true),
            session_id,
            cols: AtomicU64::new(cols as u64),
            rows: AtomicU64::new(rows as u64),
        })
    }

    /// Get session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Get process ID
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Check if the process is still running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Get terminal dimensions
    pub fn size(&self) -> (u16, u16) {
        (
            self.cols.load(Ordering::Acquire) as u16,
            self.rows.load(Ordering::Acquire) as u16,
        )
    }

    /// Resize the terminal
    pub async fn resize(&self, cols: u16, rows: u16) -> Result<(), PtyError> {
        let master = self.master.lock().await;
        master
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| PtyError::PtySystem(e.to_string()))?;
        self.cols.store(cols as u64, Ordering::Release);
        self.rows.store(rows as u64, Ordering::Release);
        Ok(())
    }

    /// Write input to the PTY
    pub async fn write(&self, data: &[u8]) -> Result<(), PtyError> {
        if !self.is_running() {
            return Err(PtyError::NotRunning);
        }
        let mut writer = self.writer.lock().await;
        writer
            .write_all(data)
            .map_err(|e| PtyError::Io(e.to_string()))?;
        writer.flush().map_err(|e| PtyError::Io(e.to_string()))?;
        Ok(())
    }

    /// Write a string to the PTY
    pub async fn write_str(&self, s: &str) -> Result<(), PtyError> {
        self.write(s.as_bytes()).await
    }

    /// Read available output from the PTY (blocking until bytes arrive).
    ///
    /// This is a **synchronous blocking** call — the underlying `portable_pty`
    /// reader exposes no readiness notification, so `read` blocks the calling
    /// thread until at least one byte is available (or the PTY closes, in which
    /// case it returns `Ok(0)`). Callers MUST NOT invoke this from an async
    /// task directly; use `tokio::task::spawn_blocking` (the WS push reader
    /// task does exactly this — see `syncode_ws::rpc::spawn_terminal_reader`).
    ///
    /// Returns the number of bytes written into `buf`; `0` signals EOF (the
    /// child process has exited and the PTY master is closed).
    pub fn read_output_blocking(&self, buf: &mut [u8]) -> Result<usize, PtyError> {
        let mut reader = self
            .reader
            .lock()
            .map_err(|e| PtyError::Io(format!("reader mutex poisoned: {e}")))?;
        let n = reader.read(buf).map_err(|e| PtyError::Io(e.to_string()))?;
        Ok(n)
    }

    /// Read available output from the PTY from an async context.
    ///
    /// This is a thin wrapper around [`PtyHandle::read_output_blocking`] that
    /// offloads the blocking `read` to a tokio blocking thread (the PTY reader
    /// is blocking std I/O with no readiness notification, so calling it
    /// directly on a reactor thread would stall the async runtime). Use this
    /// for on-demand reads (e.g. the `terminal_read_output` tauri command);
    /// for continuous live-push, the WS reader task (`spawn_terminal_reader`)
    /// calls `read_output_blocking` directly inside its own `spawn_blocking`
    /// loop to avoid the extra allocation per iteration.
    ///
    /// Returns the number of bytes written into `buf`; `0` signals EOF.
    pub async fn read_output(&self, buf: &mut [u8]) -> Result<usize, PtyError> {
        // The blocking closure must be `'static`, so it cannot borrow `buf`
        // directly. We allocate an owned buffer inside the closure, read into
        // it, and copy back into the caller's `buf` on the async side. The
        // allocation is bounded by `buf.len()` (typically 4 KiB) and only
        // paid on the on-demand read path — the live-push reader uses
        // `read_output_blocking` directly to avoid it.
        let cap = buf.len();
        let reader = self.reader.clone();
        let join_result = tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, usize), PtyError> {
            let mut local = vec![0u8; cap];
            let mut guard = reader
                .lock()
                .map_err(|e| PtyError::Io(format!("reader mutex poisoned: {e}")))?;
            let n = guard
                .read(&mut local)
                .map_err(|e| PtyError::Io(e.to_string()))?;
            local.truncate(n);
            Ok((local, n))
        })
        .await
        .map_err(|e| PtyError::Io(format!("blocking task failed: {e}")))?;
        let (bytes, n) = join_result?;
        buf[..n].copy_from_slice(&bytes);
        Ok(n)
    }

    /// Get process info
    pub fn info(&self) -> PtyProcessInfo {
        let (cols, rows) = self.size();
        PtyProcessInfo {
            pid: self.pid,
            cols,
            rows,
            working_dir: String::new(), // Not tracked after spawn
            command: String::new(),     // Not tracked after spawn
        }
    }

    /// Mark as not running
    pub fn mark_stopped(&self) {
        self.running.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pty_error_display() {
        let err = PtyError::NotRunning;
        assert_eq!(err.to_string(), "PTY not running");

        let err = PtyError::SpawnFailed("no such binary".to_string());
        assert!(err.to_string().contains("no such binary"));
    }

    #[test]
    fn pty_process_info_serialization() {
        let info = PtyProcessInfo {
            pid: 12345,
            cols: 80,
            rows: 24,
            working_dir: "/tmp".to_string(),
            command: "bash".to_string(),
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("12345"));
        let back: PtyProcessInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.cols, 80);
        assert_eq!(back.rows, 24);
    }

    #[test]
    fn pty_process_info_camel_case() {
        let info = PtyProcessInfo {
            pid: 1,
            cols: 120,
            rows: 40,
            working_dir: "/".to_string(),
            command: "sh".to_string(),
        };
        let json = serde_json::to_string(&info).unwrap();
        // camelCase serialization
        assert!(json.contains("workingDir"));
        assert!(!json.contains("working_dir"));
    }
}

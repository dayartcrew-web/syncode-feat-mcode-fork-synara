//! Syncode Terminal — PTY Management
//!
//! Terminal process management: PTY spawn, resize, write, kill,
//! session lifecycle, output buffering with ack protocol, and scrollback
//! persistence (file-based, keyed by `(threadId, terminalId)`).

pub mod output;
pub mod persistence;
pub mod pty;
pub mod session;

pub use output::{OutputBuffer, OutputChunk};
pub use persistence::{ScrollbackStore, MAX_SCROLLBACK_BYTES, truncate_ansi_safe};
pub use pty::{PtyError, PtyHandle, PtyProcessInfo};
pub use session::{SessionInfo, SessionManager, TerminalSession};

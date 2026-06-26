//! Syncode Terminal — PTY Management
//!
//! Terminal process management: PTY spawn, resize, write, kill,
//! session lifecycle, and output buffering with ack protocol.

pub mod output;
pub mod pty;
pub mod session;

pub use output::{OutputBuffer, OutputChunk};
pub use pty::{PtyError, PtyHandle, PtyProcessInfo};
pub use session::{SessionInfo, SessionManager, TerminalSession};

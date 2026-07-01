//! End-to-end test — real PTY process spawn via `portable-pty`.
//!
//! Spawns an actual OS child process (`echo`, `cat`, `sleep`) through the PTY
//! layer, writes input, reads output, and verifies lifecycle. Complements the
//! inline unit tests which only test type serialization and session-manager
//! bookkeeping (no real PTYs).
//!
//! Gating: `SYNICODE_TERMINAL_E2E=1` (always runnable on Linux/macOS — no
//! external binary beyond the OS shell — but kept gated so `cargo test` is
//! fast by default).

use std::time::Duration;

/// Gate: `SYNICODE_TERMINAL_E2E=1`.
fn e2e_enabled() -> bool {
    std::env::var("SYNICODE_TERMINAL_E2E").ok().as_deref() == Some("1")
}

// ── Tests ────────────────────────────────────────────────────────────────

#[test]
fn terminal_real_pty_spawn_echo() {
    if !e2e_enabled() {
        eprintln!("[skip] terminal e2e: set SYNICODE_TERMINAL_E2E=1");
        return;
    }

    let handle = syncode_terminal::PtyHandle::spawn(
        "test-echo".into(),
        "echo",
        &["hello from pty"],
        None,
        80,
        24,
    )
    .expect("spawn echo");

    assert!(handle.is_running());
    assert_eq!(handle.pid(), handle.pid());

    let info = handle.info();
    assert_eq!(info.cols, 80);
    assert_eq!(info.rows, 24);
}

#[tokio::test]
async fn terminal_real_pty_write_and_read() {
    if !e2e_enabled() {
        eprintln!("[skip] terminal e2e");
        return;
    }

    let handle = syncode_terminal::PtyHandle::spawn(
        "test-cat".into(),
        "cat",
        &[],
        None,
        80,
        24,
    )
    .expect("spawn cat");

    // Write input to cat (which echoes it back)
    handle.write_str("test input\n").await.expect("write");

    // Give the PTY time to echo
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Read output
    let mut buf = vec![0u8; 4096];
    let n = handle.read_output(&mut buf).await.expect("read_output");

    let output = String::from_utf8_lossy(&buf[..n]);
    assert!(
        output.contains("test input"),
        "expected 'test input' in output, got: {:?}",
        output
    );
}

#[tokio::test]
async fn terminal_real_pty_resize() {
    if !e2e_enabled() {
        eprintln!("[skip] terminal e2e");
        return;
    }

    let handle = syncode_terminal::PtyHandle::spawn(
        "test-resize".into(),
        "cat",
        &[],
        None,
        80,
        24,
    )
    .expect("spawn");

    // Resize
    handle.resize(120, 40).await.expect("resize");

    let (cols, rows) = handle.size();
    assert_eq!(cols, 120);
    assert_eq!(rows, 40);
}

#[tokio::test]
async fn terminal_real_session_manager_lifecycle() {
    if !e2e_enabled() {
        eprintln!("[skip] terminal e2e");
        return;
    }

    let mgr = syncode_terminal::SessionManager::new();
    assert_eq!(mgr.count().await, 0);

    // Spawn a session with `echo` (exits immediately)
    let session_id = mgr
        .create_session("echo", &["done"], None, 80, 24)
        .await
        .expect("create_session");

    assert!(!session_id.is_empty());
    assert_eq!(mgr.count().await, 1);

    // List sessions
    let sessions = mgr.list_sessions().await;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id, session_id);

    // Destroy
    let destroyed = mgr.destroy_session(&session_id).await;
    assert!(destroyed);
    assert_eq!(mgr.count().await, 0);
}

#[tokio::test]
async fn terminal_real_output_buffer() {
    if !e2e_enabled() {
        eprintln!("[skip] terminal e2e");
        return;
    }

    let mut buf = syncode_terminal::OutputBuffer::new(100, 10);

    // Write below chunk size
    let chunks = buf.write("hi");
    assert!(chunks.is_empty(), "below chunk size: no flush expected");

    // Write enough to trigger auto-flush
    let chunks = buf.write("0123456789X"); // 11 bytes, chunk size = 10
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].data, "0123456789");

    // Flush remaining
    let flushed = buf.flush().expect("flush");
    assert_eq!(flushed.data, "X");
    assert_eq!(flushed.seq, 1);

    // Ack
    buf.ack(1);
    let unacked = buf.unacked_chunks();
    assert!(unacked.is_empty());
}

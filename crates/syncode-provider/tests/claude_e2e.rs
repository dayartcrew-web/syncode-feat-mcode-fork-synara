//! Real-binary E2E for the Claude Code CLI streaming provider.
//!
//! Spawns the actual `claude` CLI (`claude -p <prompt> --output-format stream-json`)
//! per turn via the standard [`syncode_provider::adapters::claude::ClaudeAdapter`]:
//! spawn → `start_session` → `send_request`, asserting that streamed
//! [`ProviderEvent::Token`] chunks and a terminal [`ProviderEvent::Completed`]
//! arrive. This validates real-binary Claude interop — not just the `run_turn`
//! decoding path, which is covered by the in-process fake-reader tests in
//! `adapters/claude.rs`.
//!
//! The claude adapter's doc-comment flags this as the deferred validation for
//! its stream-json (path-1) wire model. This test surfaces any CLI-version flag
//! or output quirks.
//!
//! # Gating
//!
//! Off by default so `cargo test` is a no-op in environments without the CLI
//! (and without Anthropic credentials). Runs only when BOTH hold:
//! - env `SYNICODE_CLAUDE_E2E=1` is set, AND
//! - the `claude` binary is reachable on `PATH`.
//!
//! Run with:
//! ```text
//! SYNICODE_CLAUDE_E2E=1 cargo test -p syncode-provider --test claude_e2e -- --nocapture --test-threads=1
//! ```

use std::time::{Duration, Instant};

use syncode_core::EntityId;
use syncode_provider::adapters::claude::ClaudeAdapter;
use syncode_provider::{
    PROVIDER_CLAUDE, ProviderAdapter, ProviderConfig, ProviderEvent, ProviderRequest,
    SessionContext,
};
use tokio_stream::StreamExt;

/// Gate: only run when the operator opted in AND the `claude` CLI is on PATH.
///
/// We use [`syncode_provider::resolve_binary`] (which on Windows prefers the
/// native `claude.exe` from `~/.local/bin/` over the npm `claude.cmd` shim)
/// rather than `std::process::Command::new("claude")`, because the latter does
/// not resolve `.cmd` wrappers on Windows and would falsely skip the test.
///
/// `Command::status` succeeds (returns `Ok`) whenever the process *spawns*,
/// regardless of its exit code — so an unknown `--version` flag still confirms
/// the binary exists, while a missing binary yields `Err(NotFound)` → skip.
fn e2e_enabled() -> bool {
    if std::env::var("SYNICODE_CLAUDE_E2E").as_deref() != Ok("1") {
        return false;
    }
    let resolved = syncode_provider::resolve_binary("claude");
    std::process::Command::new(&resolved)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

#[tokio::test]
async fn claude_real_binary_one_turn_completes() {
    if !e2e_enabled() {
        eprintln!(
            "[skip] claude E2E: set SYNICODE_CLAUDE_E2E=1 and install the `claude` CLI to run"
        );
        return;
    }

    let mut provider = ClaudeAdapter::new();
    let cwd = std::env::temp_dir().to_string_lossy().to_string();
    provider
        .spawn(ProviderConfig {
            provider_id: PROVIDER_CLAUDE.to_string(),
            model: "sonnet".to_string(),
            api_key: None,
            base_url: None,
            max_tokens: Some(256),
            extra: std::collections::HashMap::new(),
        })
        .await
        .expect("claude spawn");

    let session_id = provider
        .start_session(SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: cwd.clone(),
            system_prompt: Some("You are a terse responder.".to_string()),
            user_input: "Reply with exactly the word: PONG".to_string(),
            context_files: vec![],
        })
        .await
        .expect("claude start_session");

    // Subscribe BEFORE send_request so streamed events are buffered for us
    // (broadcast only delivers to receivers that exist at send time).
    let stream = provider.event_stream(&session_id).expect("event_stream");
    tokio::pin!(stream);

    let req = ProviderRequest::new(
        "chat",
        Some(serde_json::json!({
            "input": "Reply with exactly the word: PONG",
            "session_id": session_id,
        })),
    );
    provider
        .send_request(req)
        .await
        .expect("claude turn completed");

    // Drain the event stream until Completed (or a generous deadline). A real
    // Claude turn should emit >=1 Token then a Completed; an Error event fails
    // the test loudly so CLI-version/flag quirks surface immediately.
    let mut tokens = 0u32;
    let mut completed = false;
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(Ok(ev))) => match ev {
                ProviderEvent::Token { content, .. } => {
                    tokens += 1;
                    eprintln!("[claude-e2e] token: {content}");
                }
                ProviderEvent::Completed { output, usage, .. } => {
                    eprintln!("[claude-e2e] completed: usage={usage:?} output={output}");
                    completed = true;
                    break;
                }
                ProviderEvent::Error { message, code, .. } => {
                    panic!("claude error event (code={code:?}): {message}");
                }
                other => eprintln!("[claude-e2e] event: {other:?}"),
            },
            _ => break, // stream closed or timed out
        }
    }

    assert!(
        completed,
        "expected a terminal Completed event from claude (tokens observed: {tokens})"
    );

    // Best-effort teardown — never fail the test on shutdown.
    let _ = provider.stop_session(&session_id).await;
    let _ = provider.shutdown().await;
}

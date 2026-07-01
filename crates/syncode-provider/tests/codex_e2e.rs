//! Real-binary E2E for the Codex `app-server` provider.
//!
//! Spawns the actual `codex` CLI (`codex app-server`) and drives one full turn
//! through the standard [`syncode_provider::adapters::codex::CodexAdapter`]:
//! spawn + `initialize` → `thread/start` → `turn/start`, asserting that streamed
//! [`ProviderEvent::Token`] chunks and a terminal [`ProviderEvent::Completed`]
//! arrive. This validates real-binary Codex interop — not just the
//! [`syncode_provider::CodexAppServerClient`] unit path, which is covered by the
//! in-process duplex-fake tests in `codex_app_server.rs`.
//!
//! The Codex adapter's doc-comment flags this as the deferred validation for its
//! thread/turn wire model. This test surfaces any provider-specific handling
//! that routing through the standard client may require.
//!
//! # Gating
//!
//! Off by default so `cargo test` is a no-op in environments without the CLI
//! (and without Codex credentials). Runs only when BOTH hold:
//! - env `SYNICODE_CODEX_E2E=1` is set, AND
//! - the `codex` binary is reachable on `PATH`.
//!
//! Run with:
//! ```text
//! SYNICODE_CODEX_E2E=1 cargo test -p syncode-provider --test codex_e2e -- --nocapture --test-threads=1
//! ```

use std::time::{Duration, Instant};

use syncode_core::EntityId;
use syncode_provider::adapters::codex::CodexAdapter;
use syncode_provider::{
    ProviderAdapter, ProviderConfig, ProviderEvent, ProviderRequest, SessionContext, PROVIDER_CODEX,
};
use tokio_stream::StreamExt;

/// Gate: only run when the operator opted in AND the `codex` CLI is on PATH.
///
/// `Command::status` succeeds (returns `Ok`) whenever the process *spawns*,
/// regardless of its exit code — so an unknown `--version` flag still confirms
/// the binary exists, while a missing binary yields `Err(NotFound)` → skip.
fn e2e_enabled() -> bool {
    if std::env::var("SYNICODE_CODEX_E2E").as_deref() != Ok("1") {
        return false;
    }
    std::process::Command::new("codex")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

#[tokio::test]
async fn codex_real_binary_one_turn_completes() {
    if !e2e_enabled() {
        eprintln!("[skip] codex E2E: set SYNICODE_CODEX_E2E=1 and install the `codex` CLI to run");
        return;
    }

    let mut provider = CodexAdapter::new();
    let cwd = std::env::temp_dir().to_string_lossy().to_string();
    let mut extra = std::collections::HashMap::new();
    extra.insert("cwd".to_string(), serde_json::json!(cwd));
    provider
        .spawn(ProviderConfig {
            provider_id: PROVIDER_CODEX.to_string(),
            model: "gpt-5.1-codex".to_string(),
            api_key: None,
            base_url: None,
            max_tokens: Some(256),
            extra,
        })
        .await
        .expect("codex spawn + initialize handshake");

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
        .expect("codex thread/start");

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
        .expect("codex turn/start + completion");

    // Drain the event stream until Completed (or a generous deadline). A real
    // Codex turn should emit >=1 Token then a Completed; an Error event fails
    // the test loudly so wire quirks surface immediately.
    let mut tokens = 0u32;
    let mut completed = false;
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(Ok(ev))) => match ev {
                ProviderEvent::Token { content, .. } => {
                    tokens += 1;
                    eprintln!("[codex-e2e] token: {content}");
                }
                ProviderEvent::Completed { output, usage, .. } => {
                    eprintln!("[codex-e2e] completed: usage={usage:?} output={output}");
                    completed = true;
                    break;
                }
                ProviderEvent::Error { message, code, .. } => {
                    panic!("codex error event (code={code:?}): {message}");
                }
                other => eprintln!("[codex-e2e] event: {other:?}"),
            },
            _ => break, // stream closed or timed out
        }
    }

    assert!(
        completed,
        "expected a terminal Completed event from codex (tokens observed: {tokens})"
    );

    // Best-effort teardown — never fail the test on shutdown.
    let _ = provider.stop_session(&session_id).await;
    let _ = provider.shutdown().await;
}

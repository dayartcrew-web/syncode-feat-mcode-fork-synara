//! Real-binary E2E for the Gemini ACP provider.
//!
//! Spawns the actual `gemini` CLI (`gemini --acp`) and drives one full turn
//! through the standard [`syncode_provider::AcpProvider`]: spawn → `initialize`
//! → `session/new` → `session/prompt`, asserting that streamed
//! [`ProviderEvent::Token`] chunks and a terminal [`ProviderEvent::Completed`]
//! arrive. This validates real-binary ACP interop — not just the `AcpClient`
//! unit path, which is covered by the in-process duplex-fake tests in
//! `acp.rs` / `acp_provider.rs`.
//!
//! The gemini adapter's doc-comment flags this as the deferred validation for
//! its "wire quirks" (mcode drives gemini with a bespoke manual JSON-RPC parse
//! rather than the shared ACP runtime). This test surfaces any provider-specific
//! handling that routing through the standard [`AcpClient`] may require.
//!
//! # Gating
//!
//! Off by default so `cargo test` is a no-op in environments without the CLI.
//! Runs only when BOTH hold:
//! - env `SYNICODE_ACP_E2E=1` is set, AND
//! - the `gemini` binary is reachable on `PATH`.
//!
//! Run with:
//! ```text
//! SYNICODE_ACP_E2E=1 cargo test -p syncode-provider --test gemini_e2e -- --nocapture --test-threads=1
//! ```

use std::time::{Duration, Instant};

use syncode_core::EntityId;
use syncode_provider::adapters::gemini;
use syncode_provider::{
    PROVIDER_GEMINI, ProviderAdapter, ProviderConfig, ProviderEvent, ProviderRequest,
    SessionContext,
};
use tokio_stream::StreamExt;

/// Gate: only run when the operator opted in AND the `gemini` CLI is on PATH.
///
/// `Command::status` succeeds (returns `Ok`) whenever the process *spawns*,
/// regardless of its exit code — so an unknown `--version` flag still confirms
/// the binary exists, while a missing binary yields `Err(NotFound)` → skip.
fn e2e_enabled() -> bool {
    if std::env::var("SYNICODE_ACP_E2E").as_deref() != Ok("1") {
        return false;
    }
    let resolved = syncode_provider::resolve_binary("gemini");
    std::process::Command::new(&resolved)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

#[tokio::test]
async fn gemini_real_binary_one_turn_completes() {
    if !e2e_enabled() {
        eprintln!("[skip] gemini E2E: set SYNICODE_ACP_E2E=1 and install the `gemini` CLI to run");
        return;
    }

    let mut provider = gemini::create();
    provider
        .spawn(ProviderConfig {
            provider_id: PROVIDER_GEMINI.to_string(),
            model: "gemini-2.5-flash".to_string(),
            api_key: None,
            base_url: None,
            max_tokens: Some(256),
            extra: std::collections::HashMap::new(),
        })
        .await
        .expect("gemini spawn + ACP initialize handshake");

    let working_dir = std::env::temp_dir().to_string_lossy().to_string();
    let session_id = provider
        .start_session(SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir,
            system_prompt: Some("You are a terse responder.".to_string()),
            user_input: "Reply with exactly the word: PONG".to_string(),
            context_files: vec![],
        })
        .await
        .expect("gemini session/new");

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
        .expect("gemini session/prompt response");

    // Drain the event stream until Completed (or a generous deadline). A real
    // Gemini turn should emit >=1 Token then a Completed; an Error event fails
    // the test loudly so wire quirks surface immediately.
    let mut tokens = 0u32;
    let mut completed = false;
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(Ok(ev))) => match ev {
                ProviderEvent::Token { content, .. } => {
                    tokens += 1;
                    eprintln!("[gemini-e2e] token: {content}");
                }
                ProviderEvent::Completed { output, usage, .. } => {
                    eprintln!("[gemini-e2e] completed: usage={usage:?} output={output}");
                    completed = true;
                    break;
                }
                ProviderEvent::Error { message, code, .. } => {
                    panic!("gemini error event (code={code:?}): {message}");
                }
                other => eprintln!("[gemini-e2e] event: {other:?}"),
            },
            _ => break, // stream closed or timed out
        }
    }

    assert!(
        completed,
        "expected a terminal Completed event from gemini (tokens observed: {tokens})"
    );

    // Best-effort teardown — never fail the test on shutdown.
    let _ = provider.stop_session(&session_id).await;
    let _ = provider.shutdown().await;
}

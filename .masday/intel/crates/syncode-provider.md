# syncode-provider
> Multi-LLM-provider abstraction — `ProviderAdapter` trait, 10 adapters, SessionManager, Registry. **L1** · 7210 LOC · 174 tests (largest crate)
- **Depends on (internal):** `core`.
- **External:** tokio, serde, reqwest (HTTP adapters), async-trait, thiserror, tracing, futures.

## Files
- `trait_def.rs` (18 KB) — `ProviderAdapter` trait + shared types (`ProviderRequest/Response`, `ProviderEvent`, `ProviderConfig`, `ProviderCapability`, `ProviderAdapterError`).
- `session.rs` (25 KB) — `SessionManager`, `SessionContext`, `SessionStateStatus`.
- `registry.rs` (15 KB) — provider registry + `ProviderStatusEntry`.
- `adapters/` — `anthropic.rs`, `openai.rs` (HTTP) + `claude/codex/cursor/gemini/grok/kilo/opencode/pi.rs` (subprocess).

## Public API
- **`ProviderAdapter` trait** (`trait_def.rs:237`): identity (`provider_id/capabilities/status/available_models`), lifecycle (`spawn/shutdown/interrupt`), sessions (`start_session/resume_session/stop_session`), comms (`send_request→ProviderResponse` JSON-RPC 2.0; `event_stream→ProviderStream`), `health_check`.
- **`SessionStateStatus`:** Pending→Processing→Completed|Interrupted|Errored (strict). 3 maps: `sessions`, `turn_sessions`, `thread_sessions`. Broadcast channel for event fanout.
- **Registry:** `SharedAdapter = Arc<RwLock<dyn ProviderAdapter>>`; `register/default_adapter/list_providers`.
- **Capabilities:** Streaming, ToolUse, Vision, CodeExecution, FileSystem, SystemPrompt.

## Adapters
- **HTTP (real):** `anthropic` (`AnthropicConfig`: api_key/base_url/model/max_tokens/api_version — Bedrock/Vertex/proxies), `openai` (`OpenAIConfig`: +organization_id — Azure/vLLM/Ollama).
- **Subprocess (ALL STUBS):** claude/codex/cursor/gemini/grok/kilo/opencode/pi — `spawn()` sets flags only, `send_request()` returns `{"stub":true}` echo, `interrupt()`/`health_check()` no-ops. Real stdin/stdout JSON-RPC unimplemented.

## Stubs / risks
- **8 subprocess adapters are non-functional stubs** — only anthropic+openai make real calls.
- No real subprocess/HTTP/streaming/concurrent-session integration tests (174 tests use `MockSessionAdapter`).
- No session timeout/cleanup — abandoned sessions linger in maps.
- No subprocess reaper / SIGCHLD handling; `ProcessExited` error defined but never thrown.
- **Trait change breaks all 10 adapters + orchestration reactors.**

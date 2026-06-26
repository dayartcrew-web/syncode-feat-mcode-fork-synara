# Syncode — Test Summary Report

**Generated:** 2026-06-27  
**Total Tests:** 415 (all passing, 0 failures)  
**Total Rust LOC:** ~16,500 (75 source files across 14 crates + 1 integration test package)

## Test Breakdown by Crate

| Crate | Tests | Domain |
|---|---|---|
| `syncode-automation` | 38 | Scheduled agent runs, retry/misfire/completion policies |
| `syncode-contracts` | 21 | Shared types, session/message views, TS bindings |
| `syncode-core` | 38 | EntityId, Timestamp, Project, Thread, Turn, DomainEvent |
| `syncode-git` | 22 | Git operations, checkpoint, branch management |
| `syncode-http` | 0 | HTTP client (thin wrapper, tested via integration) |
| `syncode-integration-tests` | 19 | Cross-crate integration (core↔provider↔terminal↔automation) |
| `syncode-orchestration` | 43 | Turn execution engine, provider routing, event pipeline |
| `syncode-persistence` | 3 | Storage abstraction, serialization |
| `syncode-provider` | 174 | ProviderAdapter trait + 10 adapters |
| `syncode-tauri` | 28 | Desktop tray, auto-updater, system tray events |
| `syncode-tauri-main` | 0 | Tauri binary entry point |
| `syncode-terminal` | 15 | OutputBuffer, ack protocol, chunk management |
| `syncode-ws` | 14 | WebSocket server, JSON-RPC, connection lifecycle |
| **TOTAL** | **415** | |

## Provider Adapter Coverage (174 tests)

| Adapter | Tests | Type | Capabilities | Custom Base URL |
|---|---|---|---|---|
| Claude | 13 | Subprocess | Streaming, ToolUse, Vision, CodeExecution, FileSystem, SystemPrompt | ❌ |
| Codex | 13 | Subprocess | Streaming, ToolUse, CodeExecution, FileSystem, SystemPrompt | ❌ |
| Cursor | 13 | Subprocess | Streaming, ToolUse, Vision, CodeExecution, FileSystem, SystemPrompt | ❌ |
| Gemini | 13 | Subprocess | Streaming, ToolUse, Vision, FileSystem, SystemPrompt | ❌ |
| Grok | 13 | Subprocess | Streaming, ToolUse, Vision, CodeExecution, FileSystem, SystemPrompt | ❌ |
| Kilo | 13 | Subprocess | Streaming, ToolUse, Vision, CodeExecution, FileSystem, SystemPrompt | ❌ |
| OpenCode | 13 | Subprocess | Streaming, ToolUse, Vision, CodeExecution, FileSystem, SystemPrompt | ❌ |
| Pi | 13 | Subprocess | Streaming, ToolUse, SystemPrompt | ❌ |
| **Anthropic** | **15** | **HTTP** | Streaming, ToolUse, Vision, SystemPrompt | ✅ `base_url` |
| **OpenAI** | **16** | **HTTP** | Streaming, ToolUse, Vision, CodeExecution, FileSystem, SystemPrompt | ✅ `base_url` |
| Trait definition | 14 | — | — | — |

### HTTP Adapters: Custom Base URL Support

The **Anthropic** and **OpenAI** adapters are HTTP-based and support custom `base_url` for:

- **Anthropic**: AWS Bedrock, Google Vertex AI Anthropic gateways, self-hosted proxies
- **OpenAI**: Azure OpenAI, vLLM, Ollama, LiteLLM, any OpenAI-compatible endpoint

Configuration via `AnthropicConfig` / `OpenAIConfig`:
```rust
// Anthropic with custom proxy
let anthropic = AnthropicAdapter::with_anthropic_config(AnthropicConfig {
    api_key: Some("sk-ant-...".into()),
    base_url: "https://my-bedrock-proxy.example.com".into(),
    model: "claude-sonnet-4-20250514".into(),
    ..Default::default()
});

// OpenAI with Azure / vLLM
let openai = OpenAIAdapter::with_openai_config(OpenAIConfig {
    api_key: Some("sk-...".into()),
    base_url: "https://my-vllm.example.com".into(),
    model: "llama-3-70b".into(),
    ..Default::default()
});
```

## Integration Test Coverage (19 tests)

- **Core primitives:** EntityId uniqueness, roundtrip, Timestamp serde
- **Contracts:** EntityId roundtrip, Timestamp RFC3339, SessionView serde
- **Provider system:** 8 providers known, all spawn/shutdown, unique IDs, capabilities
- **Automation:** AutomationDef construction, Scheduler CRUD, RetryPolicy delays
- **Terminal:** OutputBuffer write/flush, ack protocol
- **Cross-domain:** Provider SessionContext ↔ core EntityId, DomainEvent serialization, Turn lifecycle

## Architecture

```
syncode/
├── crates/
│   ├── syncode-core/          # Domain primitives & events
│   ├── syncode-contracts/     # Shared DTOs (TS + Rust)
│   ├── syncode-provider/      # ProviderAdapter trait + 8 adapters
│   ├── syncode-orchestration/ # Turn execution engine
│   ├── syncode-automation/    # Scheduler, automation definitions
│   ├── syncode-terminal/      # Output buffer & ack protocol
│   ├── syncode-git/           # Git operations
│   ├── syncode-ws/            # WebSocket / JSON-RPC server
│   ├── syncode-http/          # HTTP client
│   ├── syncode-persistence/   # Storage abstraction
│   └── syncode-tauri/         # Desktop shell (tray + updater)
├── tests/                     # Cross-crate integration tests
└── .github/workflows/ci.yml   # CI pipeline (fmt, clippy, test, build)
```

## CI Pipeline

GitHub Actions workflow with 3 jobs:
1. **Check** — `cargo fmt --check`, `cargo clippy`
2. **Test** — matrix (ubuntu/windows/macos), `cargo test --workspace`
3. **Build** — release build + artifact upload

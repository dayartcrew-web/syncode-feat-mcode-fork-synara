# Syncode — Test Summary Report

**Generated:** 2026-06-27  
**Total Tests:** 422 (all passing, 0 failures, 1 ignored doc-test)  
**Total Rust LOC:** ~19,600 (80 source files across 14 crates + 1 integration test package)

## Test Breakdown by Crate

| Crate | Tests | Domain |
|---|---|---|
| `syncode-automation` | 19 | Scheduled agent runs, retry/misfire/completion policies |
| `syncode-contracts` | 15 | Shared types, session/message views, TS bindings |
| `syncode-core` | 45 | EntityId, Timestamp, Project, Thread, Turn, DomainEvent, port traits |
| `syncode-git` | 22 | Git operations, checkpoint, branch management |
| `syncode-http` | 21 | HTTP client, request building, response parsing |
| `syncode-integration-tests` | — | Cross-crate integration (not run in workspace test) |
| `syncode-orchestration` | 57 | CQRS pipeline, Decider, Projector, Orchestrator, Reactors, Use Cases |
| `syncode-persistence` | 17 | SQLite event store, projections, snapshots, port adapters |
| `syncode-provider` | 174 | ProviderAdapter trait + 10 adapters |
| `syncode-tauri` | — | Desktop tray, auto-updater (pre-existing build issues, excluded) |
| `syncode-tauri-main` | — | Tauri binary entry point |
| `syncode-terminal` | 38 | OutputBuffer, ack protocol, chunk management, display |
| `syncode-ws` | 14 | WebSocket server, JSON-RPC, connection lifecycle, push bus |
| **TOTAL** | **422** | |

## CQRS / Event Sourcing Pipeline (New)

The orchestration pipeline implements a full CQRS/Event Sourcing architecture:

```
WebSocket RPC → ApplicationService → Orchestrator → Decider → Events
                                              → EventRepository (persist)
                                              → Projector → ReadModelStore
                                              → CommandReactor (side effects)
```

### Test Distribution within Orchestration (57 tests)

| Module | Tests | Covers |
|---|---|---|
| `decider` | 17 | All 16 Command variants, validation rules, error cases |
| `projector` | 12 | Event → read model projection for all entity types |
| `pipeline` | 5 | Full CQRS loop, persistence, concurrency, replay |
| `reactors::command` | 6 | Provider session lifecycle (start, complete, fail, cancel) |
| `reactors::ingestion` | 10 | Provider events → domain events, tool calls, truncation |
| `use_cases` | 9 | ApplicationService workflows, queries, aggregated views |

### Port Traits & Persistence Adapters (17 tests)

| Module | Tests | Covers |
|---|---|---|
| `event_store` | 5 | Append/replay, concurrency conflict, roundtrip |
| `snapshot` | 5 | Save/load/delete/overwrite snapshots |
| `projections` | 6 | Project, thread, turn, message, watermark tracking |
| `adapters` | 3 | SqliteEventRepository, SqliteReadModelRepository, snapshot roundtrip |

### Application Use Cases

The `ApplicationService` provides 24 methods:

**Command use cases (16):**
- `create_project`, `update_project_config`
- `create_thread`, `pause_thread`, `resume_thread`, `cancel_thread`, `complete_thread`, `set_thread_title`
- `start_turn`, `complete_turn`, `fail_turn`, `cancel_turn`, `record_turn_files`, `set_turn_checkpoint`
- `add_message`

**Query use cases (8):**
- `list_projects`, `get_project`, `list_threads(filter)`, `get_thread`
- `list_turns(filter)`, `get_turn`
- `get_project_dashboard` → aggregated (project + threads + recent turns)
- `get_thread_detail` → aggregated (thread + turns + messages + activities)

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

## Architecture

```
syncode/
├── crates/
│   ├── syncode-core/          # Domain primitives, events, port traits
│   ├── syncode-contracts/     # Shared DTOs (TS + Rust)
│   ├── syncode-provider/      # ProviderAdapter trait + 10 adapters
│   ├── syncode-orchestration/ # CQRS pipeline (Decider, Orchestrator, Projector, Reactors, Use Cases)
│   ├── syncode-persistence/   # SQLite event store, projections, port adapters
│   ├── syncode-automation/    # Scheduler, automation definitions
│   ├── syncode-terminal/      # Output buffer & ack protocol
│   ├── syncode-git/           # Git operations
│   ├── syncode-ws/            # WebSocket / JSON-RPC server
│   ├── syncode-http/          # HTTP client
│   ├── syncode-auth/          # Credentials, secret store, policy
│   └── syncode-tauri/         # Desktop shell (tray + updater)
├── tests/                     # Cross-crate integration tests
└── .github/workflows/ci.yml   # CI pipeline (fmt, clippy, test, build)
```

## CI Pipeline

GitHub Actions workflow with 3 jobs:
1. **Check** — `cargo fmt --check`, `cargo clippy`
2. **Test** — matrix (ubuntu/windows/macos), `cargo test --workspace`
3. **Build** — release build + artifact upload

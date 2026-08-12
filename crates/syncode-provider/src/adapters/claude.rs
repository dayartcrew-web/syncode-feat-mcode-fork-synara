//! Claude adapter — real Claude Code CLI streaming provider.
//!
//! Drives the `claude` (Claude Code) CLI in `stream-json` mode. Unlike the ACP
//! and codex providers (long-lived JSON-RPC subprocesses over [`JsonRpcTransport`]),
//! the Claude CLI is a **one-shot streaming producer**: each turn spawns
//! `claude -p <prompt> --output-format stream-json` and emits one JSON
//! `SDKMessage` per line on stdout until it terminates with a `result` message.
//! There is no request/response correlation, no inbound RPC, and no persistent
//! session on the wire — so this adapter does **not** use `JsonRpcTransport`;
//! it spawns a fresh [`tokio::process::Command`] per turn and decodes the NDJSON
//! stream directly. See `.masday/research/custom-providers.md` §2 (path 1).
//!
//! mcode ground truth: `ClaudeAdapter.ts` drives `@anthropic-ai/claude-agent-sdk`
//! `query()` (an `AsyncIterable<SDKMessage>`); the SDK spawns the same CLI. We
//! skip the SDK and drive the CLI directly.
//!
//! Lifecycle mapping (CLI one-shot turn model → trait):
//!
//! | trait method    | Claude operation                                       |
//! |-----------------|--------------------------------------------------------|
//! | `spawn`         | store config (bin, model, full-auto); no subprocess   |
//! | `start_session` | record (working_dir, system_prompt) for the session   |
//! | `send_request`  | spawn `claude -p <input> --output-format stream-json`  |
//! |                 | rooted at the session cwd; decode SDKMessage → events |
//! | `interrupt`     | `start_kill()` the in-flight turn subprocess          |
//! | `event_stream`  | subscribe to the broadcast event bus                  |
//! | `health_check`  | spawned flag                                           |
//! | `shutdown`      | kill any in-flight turn subprocesses                   |
//!
//! # Streaming bridge
//!
//! [`ClaudeAdapter::send_request`] runs the turn inline (mirroring the codex
//! adapter): it spawns the CLI, reads stdout line-by-line, decodes each
//! `SDKMessage` to a [`ProviderEvent`] pushed onto the shared broadcast bus, and
//! returns once the terminal `result` message arrives. An [`event_stream`]
//! subscriber created before `send_request` therefore observes the streamed
//! tokens and the terminal `Completed`/`Error` in real time.
//!
//! # Approval policy
//!
//! By default [`ClaudeConfig::full_auto`] passes `--dangerously-skip-permissions`
//! so a headless turn never deadlocks on the first permission prompt. Set
//! `full_auto = false` to omit the flag (the CLI then runs in its default
//! permission mode and may block on prompts it cannot answer headlessly —
//! intended for sandboxed/full-access environments only).
//!
//! [`JsonRpcTransport`]: crate::subprocess::JsonRpcTransport
//! [`event_stream`]: crate::trait_def::ProviderAdapter::event_stream

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, broadcast, oneshot};

use super::super::trait_def::*;
use crate::session::SessionState;

/// Per-session record: the shared session state plus the system prompt captured
/// at [`ClaudeAdapter::start_session`] (the CLI takes it per-turn via
/// `--append-system-prompt`, so it must be carried to `send_request`).
#[derive(Clone)]
struct ClaudeSession {
    state: Arc<SessionState>,
    system_prompt: Option<String>,
}

/// How a stream-json turn ended, decoded from the terminal `result` message.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TurnStatus {
    /// `result.subtype == "success"` and `is_error == false`.
    Completed,
    /// `result.subtype == "error_during_execution"` or `is_error == true`.
    Failed,
}

/// Outcome of a decoded stream-json turn.
#[derive(Debug, Clone)]
struct TurnOutcome {
    status: TurnStatus,
    output: String,
    usage: Option<UsageInfo>,
    raw: Value,
}

/// Claude-specific configuration.
#[derive(Debug, Clone)]
pub struct ClaudeConfig {
    /// Path to the `claude` CLI binary (default `"claude"`).
    pub bin_path: String,
    /// Extra args appended after the prompt + flags (default empty).
    pub extra_args: Vec<String>,
    /// Full-auto mode: pass `--dangerously-skip-permissions` so a headless turn
    /// never blocks on a permission prompt (default `true`).
    pub full_auto: bool,
    /// Default model passed via `--model` when `ProviderConfig.model` is empty.
    /// The CLI accepts aliases (`sonnet`/`opus`/`haiku`) or full model ids.
    pub model: String,
    /// Anthropic API key injected as `ANTHROPIC_API_KEY` when set; otherwise the
    /// CLI inherits the parent env (its usual auth path, incl. OAuth login).
    pub api_key: Option<String>,
}

impl Default for ClaudeConfig {
    fn default() -> Self {
        Self {
            bin_path: "claude".to_string(),
            extra_args: Vec::new(),
            full_auto: true,
            model: "sonnet".to_string(),
            api_key: std::env::var("ANTHROPIC_API_KEY").ok(),
        }
    }
}

impl ClaudeConfig {
    /// Build the `claude` argv for one streaming turn.
    ///
    /// `claude --print --output-format stream-json` requires `--verbose` (the
    /// CLI enforces this: stream-json under `--print` without `--verbose` exits
    /// with `Error: When using --print, --output-format=stream-json requires
    /// --verbose`). We add it unconditionally whenever we use stream-json.
    fn argv(&self, prompt: &str, model: Option<&str>, system_prompt: Option<&str>) -> Vec<String> {
        let mut argv = vec![self.bin_path.clone(), "-p".to_string(), prompt.to_string()];
        argv.push("--output-format".to_string());
        argv.push("stream-json".to_string());
        argv.push("--verbose".to_string());
        if let Some(m) = model.filter(|m| !m.is_empty()) {
            argv.push("--model".to_string());
            argv.push(m.to_string());
        }
        if let Some(sys) = system_prompt.filter(|s| !s.is_empty()) {
            argv.push("--append-system-prompt".to_string());
            argv.push(sys.to_string());
        }
        if self.full_auto {
            argv.push("--dangerously-skip-permissions".to_string());
        }
        argv.extend(self.extra_args.iter().cloned());
        argv
    }
}

/// What one decoded `SDKMessage` line produces.
enum SdkEmission {
    /// Zero or more live events to forward.
    Events(Vec<ProviderEvent>),
    /// The turn ended (terminal `result`).
    Terminal(TurnOutcome),
    /// Nothing to emit (`system` / `tool_progress` / `auth_status` / …).
    Ignore,
}

/// Semantic kind for an in-flight `tool_use`. Tracked by [`TurnMapper`] keyed
/// on the Anthropic `tool_use_id` so that the matching `tool_result` block
/// (which arrives later in a `user` message and carries only `tool_use_id`,
/// not the tool name) can be routed to the right PR #239 variant:
///
/// - `Subagent` tools → emit `SubagentCompleted` instead of generic `ToolResult`
/// - `Explore` tools → emit `ExploreUpdated` (count summary) before `ToolResult`
/// - `Skill`/`Plain` tools → emit `ToolResult` as before
#[derive(Clone, Debug, PartialEq)]
enum ToolKind {
    Plain { name: String },
    Explore { query: String },
    Subagent { agent: String },
    Skill { skill: String },
}

/// Classify a `tool_use` block by name + input into a [`ToolKind`]. Pure
/// helper — does not consult any state. Used both to seed the in-flight map
/// and to emit the initial `ProviderEvent` variant (ExploreStarted /
/// SubagentStarted / SkillDispatched / ToolCall).
fn classify_tool_use_kind(name: &str, input: &Value) -> ToolKind {
    if EXPLORE_TOOL_NAMES
        .iter()
        .any(|t| t.eq_ignore_ascii_case(name))
    {
        let query = input
            .get("pattern")
            .or_else(|| input.get("query"))
            .or_else(|| input.get("path"))
            .and_then(|v| v.as_str())
            .unwrap_or(name)
            .to_string();
        ToolKind::Explore { query }
    } else if SUBAGENT_TOOL_NAMES
        .iter()
        .any(|t| t.eq_ignore_ascii_case(name))
    {
        let agent = input
            .get("subagent_type")
            .or_else(|| input.get("agent"))
            .and_then(|v| v.as_str())
            .unwrap_or("general-purpose")
            .to_string();
        ToolKind::Subagent { agent }
    } else if SKILL_TOOL_NAMES
        .iter()
        .any(|t| t.eq_ignore_ascii_case(name))
    {
        let skill = input
            .get("skill")
            .or_else(|| input.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or(name)
            .to_string();
        ToolKind::Skill { skill }
    } else {
        ToolKind::Plain {
            name: name.to_string(),
        }
    }
}

/// Stateful per-turn mapper that tracks `tool_use_id → ToolKind` so tool_result
/// blocks can be routed to the right PR #239 variant. One instance lives for
/// the lifetime of a single `run_turn` call; the in-flight map is scoped to
/// the turn's tool calls and cleared implicitly when the turn ends.
#[derive(Default)]
struct TurnMapper {
    in_flight: HashMap<String, ToolKind>,
}

impl TurnMapper {
    /// Map one `SDKMessage` line. Updates `in_flight` as a side effect when
    /// tool_use blocks arrive; consults it when tool_result blocks arrive.
    fn map_sdk_message(&mut self, msg: &Value, session_id: &str) -> SdkEmission {
        let ty = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match ty {
            "stream_event" => {
                let event = msg.get("event").or_else(|| msg.get("payload"));
                SdkEmission::Events(
                    event
                        .map(|e| self.map_stream_event(e, session_id))
                        .unwrap_or_default(),
                )
            }
            "assistant" | "user" => SdkEmission::Events(self.map_message_blocks(msg, session_id)),
            "result" => SdkEmission::Terminal(decode_result(msg)),
            _ => SdkEmission::Ignore,
        }
    }

    /// Map an Anthropic stream event. Records tool_use starts into `in_flight`
    /// so the eventual tool_result can be routed correctly.
    fn map_stream_event(&mut self, event: &Value, session_id: &str) -> Vec<ProviderEvent> {
        let et = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match et {
            "content_block_delta" => map_delta(event, session_id),
            "content_block_start" => self.map_block_start(event, session_id),
            _ => Vec::new(),
        }
    }

    /// Map tool/thinking blocks of an assembled `assistant`/`user` message.
    /// Records tool_use blocks into `in_flight` (authoritative — full input
    /// available here, unlike the streaming path). Routes tool_result blocks
    /// via `in_flight` lookup.
    fn map_message_blocks(&mut self, msg: &Value, session_id: &str) -> Vec<ProviderEvent> {
        let mut out = Vec::new();
        let Some(blocks) = msg
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(|c| c.as_array())
        else {
            return out;
        };
        for block in blocks {
            let btype = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match btype {
                "thinking" => {
                    if let Some(text) = block.get("thinking").and_then(|v| v.as_str())
                        && !text.is_empty()
                    {
                        out.push(ProviderEvent::Reasoning {
                            session_id: session_id.to_string(),
                            text: text.to_string(),
                            is_delta: false,
                        });
                    }
                }
                "tool_use" => {
                    let name = block.get("name").and_then(|v| v.as_str()).unwrap_or("tool");
                    let input = block.get("input").cloned().unwrap_or(Value::Null);
                    // Record authoritative id → kind (full input available here).
                    if let Some(id) = block.get("id").and_then(|v| v.as_str()) {
                        self.in_flight
                            .insert(id.to_string(), classify_tool_use_kind(name, &input));
                    }
                    out.push(classify_tool_use(name, &input, session_id));
                }
                "tool_result" => out.extend(self.route_tool_result(block, session_id)),
                _ => {}
            }
        }
        out
    }

    /// `content_block_start` for a `tool_use` block: classify the tool by name
    /// (input is empty at this point — it streams in as `input_json_delta`),
    /// record `id → kind` so the eventual tool_result can be routed, and emit
    /// the initial ProviderEvent (ExploreStarted / SubagentStarted / etc.).
    fn map_block_start(&mut self, event: &Value, session_id: &str) -> Vec<ProviderEvent> {
        let block = match event.get("content_block") {
            Some(b) => b,
            None => return Vec::new(),
        };
        let name = match block.get("name").and_then(|v| v.as_str()) {
            Some(n) => n,
            None => return Vec::new(),
        };
        let input = block.get("input").cloned().unwrap_or(Value::Null);
        if let Some(id) = block.get("id").and_then(|v| v.as_str()) {
            // Streaming record — the assembled `assistant` message will overwrite
            // this with the full-input classification when it arrives.
            self.in_flight
                .entry(id.to_string())
                .or_insert_with(|| classify_tool_use_kind(name, &input));
        }
        vec![classify_tool_use(name, &input, session_id)]
    }

    /// Route a `tool_result` block to the right PR #239 variant by looking up
    /// its `tool_use_id` in `in_flight`. Emits `SubagentCompleted` for subagent
    /// tools, `ExploreUpdated` (count summary) + `ToolResult` for explore tools,
    /// and a plain `ToolResult` otherwise.
    fn route_tool_result(&self, block: &Value, session_id: &str) -> Vec<ProviderEvent> {
        let sid = session_id.to_string();
        let tool_use_id = block.get("tool_use_id").and_then(|v| v.as_str());
        let kind = tool_use_id.and_then(|id| self.in_flight.get(id));
        let result_text = extract_tool_result_text(block);
        match kind {
            Some(ToolKind::Subagent { agent }) => {
                vec![ProviderEvent::SubagentCompleted {
                    session_id: sid,
                    agent: agent.clone(),
                    result: result_text,
                }]
            }
            Some(ToolKind::Explore { .. }) => {
                let count = count_result_lines(&result_text);
                let message = format!("Found {count} result{}", if count == 1 { "" } else { "s" });
                vec![
                    ProviderEvent::ExploreUpdated {
                        session_id: sid.clone(),
                        message,
                    },
                    ProviderEvent::ToolResult {
                        session_id: sid,
                        tool_name: "explore".to_string(),
                        result: block
                            .get("content")
                            .cloned()
                            .unwrap_or_else(|| block.clone()),
                    },
                ]
            }
            Some(ToolKind::Skill { skill }) => {
                // Skill tool_result: synthesize a generic ToolResult. The
                // SkillDispatched event already fired on the tool_use side.
                vec![ProviderEvent::ToolResult {
                    session_id: sid,
                    tool_name: format!("Skill:{skill}"),
                    result: block
                        .get("content")
                        .cloned()
                        .unwrap_or_else(|| block.clone()),
                }]
            }
            Some(ToolKind::Plain { .. }) | None => {
                let tool_name = kind
                    .and_then(|k| match k {
                        ToolKind::Plain { name } => Some(name.clone()),
                        _ => None,
                    })
                    .or_else(|| {
                        block
                            .get("name")
                            .and_then(|v| v.as_str())
                            .map(str::to_owned)
                    })
                    .unwrap_or_else(|| tool_use_id.unwrap_or("tool").to_string());
                vec![ProviderEvent::ToolResult {
                    session_id: sid,
                    tool_name,
                    result: block
                        .get("content")
                        .cloned()
                        .unwrap_or_else(|| block.clone()),
                }]
            }
        }
    }
}

/// Extract a best-effort flat text representation of a tool_result `content`
/// field. Handles strings, arrays of `{type:"text", text:"..."}` blocks, and
/// falls back to a JSON rendering for other shapes. Used to populate
/// `SubagentCompleted.result` and to count explore matches.
fn extract_tool_result_text(block: &Value) -> String {
    let content = match block.get("content") {
        Some(c) => c,
        None => return block.to_string(),
    };
    match content {
        Value::String(s) => s.clone(),
        Value::Array(items) => {
            let mut buf = String::new();
            for item in items {
                if let Some(t) = item.get("text").and_then(|v| v.as_str()) {
                    buf.push_str(t);
                    buf.push('\n');
                } else if let Some(c) = item.get("content").and_then(|v| v.as_str()) {
                    buf.push_str(c);
                    buf.push('\n');
                }
            }
            buf.trim_end().to_string()
        }
        other => other.to_string(),
    }
}

/// Count non-empty lines in a tool_result text. Used to summarize explore
/// tool output volume ("Found N results") without dumping the full result.
fn count_result_lines(text: &str) -> usize {
    text.lines().filter(|l| !l.trim().is_empty()).count()
}

/// Detect a leading slash command in a turn prompt (`/skill-name [args]`) and
/// synthesize a `SkillDispatched` event for it. Claude has no built-in Skill
/// tool by default, so this is the only production path that surfaces the
/// `provider_skill_dispatched` activity type. Returns `None` for prompts that
/// don't start with `/` followed by an alphabetic character.
fn detect_slash_command_skill(prompt: &str, session_id: &str) -> Option<ProviderEvent> {
    let trimmed = prompt.trim_start();
    let rest = trimmed.strip_prefix('/')?;
    // First char must be alphabetic — avoids treating shell-style "//" or
    // filesystem paths as skill invocations.
    let head = rest.chars().next()?;
    if !head.is_ascii_alphabetic() {
        return None;
    }
    // Skill name = run of [a-zA-Z0-9-_]; everything after the next whitespace
    // is the argument payload.
    let mut name_end = 0;
    for (i, c) in rest.char_indices() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
            name_end = i + c.len_utf8();
        } else {
            break;
        }
    }
    if name_end == 0 {
        return None;
    }
    // Reject paths like `/path/to/file`: a skill invocation never has `/`
    // immediately after the name. Anything after the name is either end-of
    // string or whitespace-separated args.
    if rest[name_end..].starts_with('/') {
        return None;
    }
    let skill = rest[..name_end].to_string();
    let args_str = rest[name_end..].trim();
    let args = if args_str.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::Value::String(args_str.to_string())
    };
    Some(ProviderEvent::SkillDispatched {
        session_id: session_id.to_string(),
        skill,
        args,
    })
}

// --- stateless wrappers (kept for backwards-compat with existing tests) ---

/// Decode one `SDKMessage` JSON line into events or a terminal outcome.
/// Stateless wrapper around [`TurnMapper::map_sdk_message`] — constructs a
/// throwaway mapper per call, so tool_use_id tracking does NOT persist across
/// calls. Use `TurnMapper` directly (via `run_turn`) for stateful behavior.
#[cfg(test)]
fn map_sdk_message(msg: &Value, session_id: &str) -> SdkEmission {
    let mut mapper = TurnMapper::default();
    mapper.map_sdk_message(msg, session_id)
}

/// Stateless wrapper around [`TurnMapper::map_stream_event`].
#[cfg(test)]
fn map_stream_event(event: &Value, session_id: &str) -> Vec<ProviderEvent> {
    let mut mapper = TurnMapper::default();
    mapper.map_stream_event(event, session_id)
}

/// Stateless wrapper around [`TurnMapper::map_message_blocks`].
#[cfg(test)]
fn map_message_blocks(msg: &Value, session_id: &str) -> Vec<ProviderEvent> {
    let mut mapper = TurnMapper::default();
    mapper.map_message_blocks(msg, session_id)
}

/// Map a content_block_delta event. Pure — does not consult any state.
fn map_delta(event: &Value, session_id: &str) -> Vec<ProviderEvent> {
    // thinking deltas → Reasoning; text deltas → Token.
    if let Some(thinking) = event
        .get("delta")
        .and_then(|d| d.get("thinking"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        return vec![ProviderEvent::Reasoning {
            session_id: session_id.to_string(),
            text: thinking.to_string(),
            is_delta: true,
        }];
    }
    if let Some(text) = event
        .get("delta")
        .and_then(|d| d.get("text"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        vec![ProviderEvent::Token {
            session_id: session_id.to_string(),
            content: text.to_string(),
        }]
    } else {
        Vec::new()
    }
}

/// Tool names that represent codebase exploration. Mapped to `ExploreStarted`
/// so the timeline surfaces a single "🔍 Exploring: {query}" row instead of a
/// raw `ToolCall` row that the user has to mentally decode.
const EXPLORE_TOOL_NAMES: &[&str] = &["Glob", "Grep", "LS", "find", "Search", "rg"];

/// Tool names that delegate to a sub-agent. Mapped to `SubagentStarted`.
/// `Task` is the legacy Claude Code name; `Agent` is the current SDK name
/// (the harness shows real frames using `Agent`). `dispatch_agent` covers
/// Codex-style adapters that may share this code path in future.
const SUBAGENT_TOOL_NAMES: &[&str] = &["Task", "Agent", "dispatch_agent"];

/// Tool names that invoke a skill. Mapped to `SkillDispatched`.
const SKILL_TOOL_NAMES: &[&str] = &["Skill", "skill", "InvokeSkill"];

/// Classify a `tool_use` block into the appropriate `ProviderEvent` variant.
///
/// Most tools emit a plain `ToolCall` (Read, Write, Edit, Bash, …). A small
/// set of "navigation"/"delegation"/"slash-command" tools get a richer
/// variant so the frontend can render them as semantically distinct rows
/// (explore panel, subagent panel, skill chip) instead of generic tool rows.
///
/// Returns the single event to emit. Caller should push it in place of the
/// default `ToolCall`.
fn classify_tool_use(name: &str, input: &Value, session_id: &str) -> ProviderEvent {
    let sid = session_id.to_string();
    let tool_input = input.clone();
    if EXPLORE_TOOL_NAMES
        .iter()
        .any(|t| t.eq_ignore_ascii_case(name))
    {
        // Glob/Grep input carries `pattern`; LS carries `path`; Search carries `query`.
        let query = input
            .get("pattern")
            .or_else(|| input.get("query"))
            .or_else(|| input.get("path"))
            .and_then(|v| v.as_str())
            .unwrap_or(name)
            .to_string();
        ProviderEvent::ExploreStarted {
            session_id: sid,
            query,
        }
    } else if SUBAGENT_TOOL_NAMES
        .iter()
        .any(|t| t.eq_ignore_ascii_case(name))
    {
        // Task tool input: { description, prompt, subagent_type }
        let agent = input
            .get("subagent_type")
            .or_else(|| input.get("agent"))
            .and_then(|v| v.as_str())
            .unwrap_or("general-purpose")
            .to_string();
        let task = input
            .get("description")
            .or_else(|| input.get("prompt"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        ProviderEvent::SubagentStarted {
            session_id: sid,
            agent,
            task,
        }
    } else if SKILL_TOOL_NAMES
        .iter()
        .any(|t| t.eq_ignore_ascii_case(name))
    {
        // Skill tool input: { skill: "name", ...args }
        let skill = input
            .get("skill")
            .or_else(|| input.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or(name)
            .to_string();
        ProviderEvent::SkillDispatched {
            session_id: sid,
            skill,
            args: tool_input,
        }
    } else {
        ProviderEvent::ToolCall {
            session_id: sid,
            tool_name: name.to_string(),
            tool_input,
        }
    }
}

/// Decode the terminal `result` message (usage is filled by [`run_turn`] from
/// the last observed usage, which the `result` message also carries).
fn decode_result(msg: &Value) -> TurnOutcome {
    let subtype = msg.get("subtype").and_then(|v| v.as_str()).unwrap_or("");
    let is_error = msg
        .get("is_error")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let output = msg
        .get("result")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .unwrap_or_default();
    let status = if is_error || subtype == "error_during_execution" {
        TurnStatus::Failed
    } else {
        TurnStatus::Completed
    };
    TurnOutcome {
        status,
        output,
        usage: None,
        raw: msg.clone(),
    }
}

/// Best-effort usage extraction from any message shape that carries tokens.
/// Reads `usage` directly or nested under `message.usage` (the `assistant`
/// message shape). Falls back to `input + output` when `total_tokens` is absent.
fn extract_usage(msg: &Value) -> Option<UsageInfo> {
    let usage = msg
        .get("usage")
        .or_else(|| msg.get("message").and_then(|m| m.get("usage")))?;
    let input = usage
        .get("input_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let output = usage
        .get("output_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let total = usage
        .get("total_tokens")
        .and_then(|v| v.as_u64())
        .map(|n| n as u32)
        .unwrap_or(input + output);
    if input == 0 && output == 0 && total == 0 {
        return None;
    }
    Some(UsageInfo {
        input_tokens: input,
        output_tokens: output,
        total_tokens: total,
    })
}

/// Run one stream-json turn to completion over `reader`, decoding each NDJSON
/// `SDKMessage` line to a [`ProviderEvent`] on `event_tx` and returning the
/// terminal outcome. Split out from [`ClaudeAdapter::send_request`] as a free
/// function so it can be driven by a fake reader in tests (no real `claude`
/// binary) — the protocol logic is fully covered without spawning anything.
///
/// Returns [`ProviderAdapterError::ProcessExited`] if stdout closes before a
/// terminal `result` message arrives.
async fn run_turn<R>(
    reader: R,
    session_id: &str,
    event_tx: &broadcast::Sender<ProviderEvent>,
) -> Result<TurnOutcome, ProviderAdapterError>
where
    R: AsyncBufRead + Unpin,
{
    let mut reader = reader;
    let mut line = String::new();
    let mut last_usage: Option<UsageInfo> = None;
    // Stateful mapper: tracks tool_use_id → ToolKind so tool_result blocks can
    // be routed to the right PR #239 variant (SubagentCompleted / ExploreUpdated)
    // rather than always falling through to a generic ToolResult.
    let mut mapper = TurnMapper::default();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Err(ProviderAdapterError::ProcessExited(
                "claude stream closed before a terminal result message".to_string(),
            ));
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    provider = PROVIDER_CLAUDE,
                    line = %trimmed,
                    error = %e,
                    "skipping unparseable claude stream-json line"
                );
                continue;
            }
        };
        if let Some(usage) = extract_usage(&msg) {
            last_usage = Some(usage);
        }
        match mapper.map_sdk_message(&msg, session_id) {
            SdkEmission::Events(events) => {
                for ev in events {
                    let _ = event_tx.send(ev);
                }
            }
            SdkEmission::Terminal(mut outcome) => {
                if outcome.usage.is_none() {
                    outcome.usage = last_usage.clone();
                }
                return Ok(outcome);
            }
            SdkEmission::Ignore => {}
        }
    }
}

/// The Claude provider adapter.
pub struct ClaudeAdapter {
    config: Option<ProviderConfig>,
    claude_config: ClaudeConfig,
    status: AtomicU64,
    sessions: Mutex<HashMap<String, ClaudeSession>>,
    /// In-flight turn subprocesses keyed by session id, so `interrupt` can kill
    /// the turn's `claude` process. The stdout handle is taken out (the turn
    /// reader owns it); the entry holds the child for liveness/kill only.
    active_children: Mutex<HashMap<String, Child>>,
    /// Most recently opened session id (used to resolve a request without an
    /// explicit `session_id`, mirroring codex's `current_thread`).
    current_session: Mutex<Option<String>>,
    event_tx: broadcast::Sender<ProviderEvent>,
    spawned: AtomicBool,
}

impl Default for ClaudeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl ClaudeAdapter {
    /// Create a new Claude adapter with default settings.
    pub fn new() -> Self {
        Self::with_claude_config(ClaudeConfig::default())
    }

    /// Create a new Claude adapter with custom claude-specific config.
    pub fn with_claude_config(claude_config: ClaudeConfig) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config: None,
            claude_config,
            status: AtomicU64::new(ProviderStatus::Disconnected.into()),
            sessions: Mutex::new(HashMap::new()),
            active_children: Mutex::new(HashMap::new()),
            current_session: Mutex::new(None),
            event_tx,
            spawned: AtomicBool::new(false),
        }
    }

    fn set_status(&self, status: ProviderStatus) {
        self.status.store(status.into(), Ordering::Release);
    }

    /// Resolve the model for a turn: explicit `params.model` wins, else the
    /// spawn-time `ProviderConfig.model`, else the `ClaudeConfig.model` default.
    fn model_for(&self, params: &Option<Value>) -> Option<String> {
        let req_model = params
            .as_ref()
            .and_then(|p| p.get("model"))
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .map(|m| self.resolve_model_slug(&m));
        let cfg_model = self
            .config
            .as_ref()
            .map(|c| c.model.clone())
            .filter(|m| !m.is_empty())
            .map(|m| self.resolve_model_slug(&m));
        req_model.or(cfg_model).or_else(|| {
            (!self.claude_config.model.is_empty()).then(|| self.claude_config.model.clone())
        })
    }

    /// Map a model slug to one the `claude` CLI accepts.
    ///
    /// The frontend `ProviderKind.claudeAgent` model list uses internal slugs
    /// like `claude-sonnet-4-6`, `claude-opus-4-8` (future/hypothetical tiers)
    /// which are NOT real `claude` CLI model ids — passing them verbatim to
    /// `--model` makes the CLI reject with "Unknown Model" (400). Pass through
    /// anything already in [`available_models`] (aliases + real ids), and map
    /// other `claude-<tier>-*` slugs to the CLI alias for that tier.
    fn resolve_model_slug(&self, model: &str) -> String {
        if self.available_models().iter().any(|m| m == model) {
            return model.to_string();
        }
        let lower = model.to_lowercase();
        if lower.contains("sonnet") {
            "sonnet".to_string()
        } else if lower.contains("opus") {
            "opus".to_string()
        } else if lower.contains("haiku") {
            "haiku".to_string()
        } else if lower.contains("fable") {
            // "fable" isn't a CLI tier; fall back to sonnet (the default tier).
            "sonnet".to_string()
        } else {
            // Not a claude model — e.g. a codex/openai slug like "gpt-5.4-mini"
            // left over when the provider switched to claude but the model
            // selection didn't reset. Passing it through makes the CLI reject
            // with "Unknown Model". Fall back to the configured claude default.
            let fallback = self.claude_config.model.clone();
            if fallback.is_empty() {
                "sonnet".to_string()
            } else {
                fallback
            }
        }
    }

    /// Resolve the session id for a request: explicit `params.session_id`
    /// (injected by the command reactor's dispatcher) wins; otherwise the most
    /// recently opened session.
    async fn resolve_session(&self, params: &Option<Value>) -> Option<String> {
        if let Some(id) = params
            .as_ref()
            .and_then(|p| p.get("session_id"))
            .and_then(|v| v.as_str())
        {
            return Some(id.to_string());
        }
        self.current_session.lock().await.clone()
    }

    /// Extract the turn prompt text from request params (orchestrator `input`
    /// convention; falls back to a textual rendering of the params object so a
    /// turn is never silently empty).
    fn turn_prompt(params: &Option<Value>) -> String {
        params
            .as_ref()
            .and_then(|p| match p {
                Value::Null => None,
                other => p
                    .get("input")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .or_else(|| Some(other.to_string())),
            })
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl ProviderAdapter for ClaudeAdapter {
    // -- Identity ----------------------------------------------------------

    fn provider_id(&self) -> &str {
        PROVIDER_CLAUDE
    }

    fn capabilities(&self) -> Vec<ProviderCapability> {
        vec![
            ProviderCapability::Streaming,
            ProviderCapability::ToolUse,
            ProviderCapability::FileSystem,
            ProviderCapability::SystemPrompt,
            ProviderCapability::CodeExecution,
        ]
    }

    fn status(&self) -> ProviderStatus {
        self.status.load(Ordering::Acquire).into()
    }

    fn is_spawned(&self) -> bool {
        self.spawned.load(Ordering::Acquire)
    }

    fn available_models(&self) -> Vec<String> {
        // CLI aliases first, then a few well-known ids.
        vec![
            "sonnet".to_string(),
            "opus".to_string(),
            "haiku".to_string(),
            "claude-sonnet-4-5-20250929".to_string(),
            "claude-sonnet-4-20250514".to_string(),
            "claude-3-5-sonnet-20241022".to_string(),
            "claude-3-5-haiku-20241022".to_string(),
            "claude-3-opus-20240229".to_string(),
        ]
    }

    // -- Lifecycle ---------------------------------------------------------

    async fn spawn(&mut self, config: ProviderConfig) -> Result<(), ProviderAdapterError> {
        if self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::ConfigError(
                "Claude adapter already spawned".to_string(),
            ));
        }
        // Best-effort auth check: prefer an explicit key (config.api_key or
        // ClaudeConfig.api_key) or ANTHROPIC_API_KEY in the env. The CLI also
        // supports OAuth login, so absence is only a warning.
        let has_key = config.api_key.is_some()
            || self.claude_config.api_key.is_some()
            || std::env::var("ANTHROPIC_API_KEY").is_ok();
        if !has_key {
            tracing::warn!(
                provider = PROVIDER_CLAUDE,
                "No ANTHROPIC_API_KEY found — the claude CLI will rely on its own login/config."
            );
        }

        self.config = Some(config);
        self.spawned.store(true, Ordering::Release);
        self.set_status(ProviderStatus::Idle);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        });
        tracing::info!(
            provider = PROVIDER_CLAUDE,
            binary = %self.claude_config.bin_path,
            full_auto = self.claude_config.full_auto,
            "Claude adapter spawned (one process per turn)",
        );
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        self.set_status(ProviderStatus::ShuttingDown);

        // Kill any in-flight turns.
        let mut children = self.active_children.lock().await;
        for (_, mut child) in children.drain() {
            let _ = child.start_kill();
        }
        drop(children);

        self.sessions.lock().await.clear();
        *self.current_session.lock().await = None;
        self.spawned.store(false, Ordering::Release);
        self.set_status(ProviderStatus::Disconnected);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Disconnected,
        });
        tracing::info!(provider = PROVIDER_CLAUDE, "Claude adapter shut down");
        Ok(())
    }

    async fn interrupt(&self, session_id: &str) -> Result<(), ProviderAdapterError> {
        // Stream-only: killing the CLI is the only interrupt path (there is no
        // mid-turn RPC). If no child is running, the turn already finished —
        // succeed (best-effort) after a session check so unknown ids still error.
        let sessions = self.sessions.lock().await;
        if !sessions.contains_key(session_id) {
            return Err(ProviderAdapterError::SessionNotFound(
                session_id.to_string(),
            ));
        }
        drop(sessions);

        let mut children = self.active_children.lock().await;
        if let Some(mut child) = children.remove(session_id) {
            tracing::info!(
                provider = PROVIDER_CLAUDE,
                session_id,
                "Interrupting claude turn — killing subprocess"
            );
            let _ = child.start_kill();
        }
        Ok(())
    }

    // -- Session management -------------------------------------------------

    async fn start_session(&mut self, ctx: SessionContext) -> Result<String, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }

        let session_id = format!("claude-{}", uuid::Uuid::new_v4().hyphenated());
        let _ = self.event_tx.send(ProviderEvent::Started {
            session_id: session_id.clone(),
        });

        let state = Arc::new(SessionState::new(
            session_id.clone(),
            ctx.thread_id,
            ctx.turn_id,
            ctx.working_dir.clone(),
        ));
        self.sessions.lock().await.insert(
            session_id.clone(),
            ClaudeSession {
                state,
                system_prompt: ctx.system_prompt.clone(),
            },
        );
        *self.current_session.lock().await = Some(session_id.clone());
        self.set_status(ProviderStatus::Busy);

        tracing::info!(
            provider = PROVIDER_CLAUDE,
            session_id = %session_id,
            working_dir = %ctx.working_dir,
            "Claude session opened",
        );
        Ok(session_id)
    }

    async fn resume_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        // Stream-only CLI turns are stateless client-side; "resume" is a no-op —
        // the next send_request spawns a fresh `claude -p` rooted at the same cwd.
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        if !self.sessions.lock().await.contains_key(session_id) {
            return Err(ProviderAdapterError::SessionNotFound(
                session_id.to_string(),
            ));
        }
        Ok(())
    }

    async fn stop_session(&mut self, session_id: &str) -> Result<(), ProviderAdapterError> {
        let mut sessions = self.sessions.lock().await;
        if sessions.remove(session_id).is_none() {
            return Err(ProviderAdapterError::SessionNotFound(
                session_id.to_string(),
            ));
        }
        drop(sessions);

        // Best-effort: kill any in-flight child for this session.
        if let Some(mut child) = self.active_children.lock().await.remove(session_id) {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        if *self.current_session.lock().await == Some(session_id.to_string()) {
            *self.current_session.lock().await = None;
        }
        self.set_status(ProviderStatus::Idle);
        let _ = self.event_tx.send(ProviderEvent::StatusChanged {
            status: ProviderStatus::Idle,
        });
        tracing::info!(
            provider = PROVIDER_CLAUDE,
            session_id,
            "Claude session stopped"
        );
        Ok(())
    }

    // -- Communication -----------------------------------------------------

    async fn send_request(
        &self,
        request: ProviderRequest,
    ) -> Result<ProviderResponse, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Err(ProviderAdapterError::NotSpawned);
        }
        let session_id = self.resolve_session(&request.params).await.ok_or_else(|| {
            ProviderAdapterError::SessionNotFound(
                "send_request has no session_id — call start_session first".to_string(),
            )
        })?;
        let (working_dir, system_prompt) = {
            let sessions = self.sessions.lock().await;
            sessions
                .get(&session_id)
                .map(|s| (s.state.working_dir.clone(), s.system_prompt.clone()))
                .unwrap_or_else(|| {
                    (
                        std::env::current_dir()
                            .map(|p| p.to_string_lossy().into_owned())
                            .unwrap_or_else(|_| ".".to_string()),
                        None,
                    )
                })
        };
        let prompt = Self::turn_prompt(&request.params);
        let model = self.model_for(&request.params);

        // PR #239: detect slash-command prompts (`/skill-name [args]`) and emit
        // a SkillDispatched event before spawning the turn. Claude has no
        // built-in Skill tool by default, so without this synthesized event the
        // `provider_skill_dispatched` activity type would never fire in
        // production. The detector is conservative: it requires an alphabetic
        // first character after `/` to avoid false positives on shell-style
        // paths.
        if let Some(skill_event) = detect_slash_command_skill(&prompt, &session_id) {
            let _ = self.event_tx.send(skill_event);
        }

        let api_key = self
            .claude_config
            .api_key
            .clone()
            .or_else(|| self.config.as_ref().and_then(|c| c.api_key.clone()));

        let argv = self
            .claude_config
            .argv(&prompt, model.as_deref(), system_prompt.as_deref());
        let resolved_bin = crate::bin_resolver::resolve_binary(&argv[0]);
        let args = argv[1..].to_vec();

        // On Windows, .cmd wrappers need cmd /C for stdio pipes to work
        #[cfg(windows)]
        let mut cmd = if resolved_bin.ends_with(".cmd") || resolved_bin.ends_with(".bat") {
            let mut c = Command::new("cmd");
            c.arg("/C").arg(&resolved_bin);
            for a in &args {
                c.arg(a);
            }
            c
        } else {
            let mut c = Command::new(&resolved_bin);
            for a in &args {
                c.arg(a);
            }
            c
        };
        #[cfg(not(windows))]
        let mut cmd = {
            let mut c = Command::new(&resolved_bin);
            for a in &args {
                c.arg(a);
            }
            c
        };
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(&working_dir)
            .kill_on_drop(true);
        if let Some(key) = api_key {
            cmd.env("ANTHROPIC_API_KEY", key);
        }
        crate::subprocess::hide_console_window(&mut cmd);
        let mut child = cmd.spawn().map_err(|e| {
            ProviderAdapterError::ProcessExited(format!(
                "failed to spawn `claude` ({resolved_bin}): {e}"
            ))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            ProviderAdapterError::ProcessExited("claude subprocess stdout not captured".to_string())
        })?;

        // Drain stderr concurrently so the CLI never blocks on a full stderr
        // pipe; surface it only when the turn fails (diagnostics).
        let stderr_rx = child.stderr.take().map(|mut err| {
            let (tx, rx) = oneshot::channel::<String>();
            tokio::spawn(async move {
                let mut buf = String::new();
                let _ = err.read_to_string(&mut buf).await;
                let _ = tx.send(buf);
            });
            rx
        });

        self.active_children
            .lock()
            .await
            .insert(session_id.clone(), child);
        self.set_status(ProviderStatus::Busy);

        let turn = run_turn(BufReader::new(stdout), &session_id, &self.event_tx).await;

        // Reap the child (waits for exit → stderr drain task sees EOF).
        if let Some(mut child) = self.active_children.lock().await.remove(&session_id) {
            let _ = child.wait().await;
        }
        let stderr_buf = match stderr_rx {
            Some(rx) => rx.await.unwrap_or_default(),
            None => String::new(),
        };

        let turn = match turn {
            Ok(o) => o,
            Err(e) => {
                let mut diag = e.to_string();
                let trimmed = stderr_buf.trim();
                if !trimmed.is_empty() {
                    diag.push_str(" | stderr: ");
                    diag.push_str(trimmed);
                }
                let _ = self.event_tx.send(ProviderEvent::Error {
                    session_id: session_id.clone(),
                    message: diag.clone(),
                    code: None,
                });
                self.set_status(ProviderStatus::Idle);
                return Err(ProviderAdapterError::ProcessExited(diag));
            }
        };

        match turn.status {
            TurnStatus::Completed => {
                let _ = self.event_tx.send(ProviderEvent::Completed {
                    session_id: session_id.clone(),
                    output: turn.output.clone(),
                    usage: turn.usage.clone(),
                });
                self.set_status(ProviderStatus::Idle);
                Ok(ProviderResponse {
                    jsonrpc: "2.0".to_string(),
                    id: Some(request.id),
                    result: Some(json!({
                        "output": turn.output,
                        "usage": turn.usage,
                        "raw": turn.raw,
                    })),
                    error: None,
                })
            }
            TurnStatus::Failed => {
                let message = if turn.output.is_empty() {
                    "claude turn failed".to_string()
                } else {
                    turn.output.clone()
                };
                let _ = self.event_tx.send(ProviderEvent::Error {
                    session_id: session_id.clone(),
                    message: message.clone(),
                    code: None,
                });
                self.set_status(ProviderStatus::Idle);
                Ok(ProviderResponse {
                    jsonrpc: "2.0".to_string(),
                    id: Some(request.id),
                    result: None,
                    error: Some(ProviderError {
                        code: -32000,
                        message,
                        data: Some(turn.raw.clone()),
                    }),
                })
            }
        }
    }

    fn event_stream(&self, session_id: &str) -> Result<ProviderStream, ProviderAdapterError> {
        let rx = self.event_tx.subscribe();
        let sid = session_id.to_string();
        let stream = async_stream::stream! {
            let mut rx = rx;
            while let Ok(event) = rx.recv().await {
                let owned = match &event {
                    ProviderEvent::Started { session_id }
                    | ProviderEvent::Token { session_id, .. }
                    | ProviderEvent::ToolCall { session_id, .. }
                    | ProviderEvent::ToolResult { session_id, .. }
                    | ProviderEvent::Completed { session_id, .. }
                    | ProviderEvent::Error { session_id, .. }
                    | ProviderEvent::Reasoning { session_id, .. }
                    | ProviderEvent::SkillDispatched { session_id, .. }
                    | ProviderEvent::SubagentStarted { session_id, .. }
                    | ProviderEvent::SubagentCompleted { session_id, .. }
                    | ProviderEvent::ExploreStarted { session_id, .. }
                    | ProviderEvent::ExploreUpdated { session_id, .. } => session_id == &sid,
                    ProviderEvent::StatusChanged { .. } => true,
                };
                if owned {
                    yield Ok(event);
                }
            }
        };
        Ok(Box::pin(stream))
    }

    // -- Utility -----------------------------------------------------------

    async fn health_check(&self) -> Result<bool, ProviderAdapterError> {
        if !self.spawned.load(Ordering::Acquire) {
            return Ok(false);
        }
        Ok(self.status() != ProviderStatus::Disconnected && self.status() != ProviderStatus::Error)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use syncode_core::EntityId;

    fn make_ctx() -> SessionContext {
        SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: "/tmp/test-claude-project".to_string(),
            system_prompt: Some("Be helpful.".to_string()),
            user_input: "Fix the bug in main.rs".to_string(),
            context_files: vec![],
        }
    }

    // --- pure mapping tests (no transport) ---

    #[test]
    fn stream_event_text_delta_emits_token() {
        let events = map_stream_event(
            &json!({ "type": "content_block_delta",
                "delta": { "type": "text_delta", "text": "Hello " } }),
            "s1",
        );
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], ProviderEvent::Token { content, session_id } if content == "Hello " && session_id == "s1"),
            "{events:?}"
        );
    }

    #[test]
    fn stream_event_empty_text_emits_nothing() {
        assert!(
            map_stream_event(
                &json!({ "type": "content_block_delta", "delta": { "text": "" } }),
                "s1"
            )
            .is_empty()
        );
        // Non-text deltas (e.g. tool input streaming) are ignored here.
        assert!(
            map_stream_event(
                &json!({ "type": "content_block_delta",
                "delta": { "type": "input_json_delta", "partial_json": "{" } }),
                "s1"
            )
            .is_empty()
        );
    }

    #[test]
    fn stream_event_tool_use_start_emits_tool_call() {
        let events = map_stream_event(
            &json!({ "type": "content_block_start",
                "content_block": { "type": "tool_use", "name": "Read", "id": "t1" } }),
            "s1",
        );
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], ProviderEvent::ToolCall { tool_name, .. } if tool_name == "Read"),
            "{events:?}"
        );
    }

    #[test]
    fn classify_tool_use_routes_exploration_tools_to_explore_started() {
        // Glob with a pattern → ExploreStarted carrying the pattern as query.
        let ev = classify_tool_use("Glob", &json!({ "pattern": "**/*.rs" }), "s1");
        assert!(
            matches!(&ev, ProviderEvent::ExploreStarted { query, .. } if query == "**/*.rs"),
            "{ev:?}"
        );
        // Grep → ExploreStarted.
        let ev = classify_tool_use("Grep", &json!({ "pattern": "TODO" }), "s1");
        assert!(matches!(ev, ProviderEvent::ExploreStarted { .. }));
        // Case-insensitive match.
        let ev = classify_tool_use("grep", &json!({}), "s1");
        assert!(matches!(ev, ProviderEvent::ExploreStarted { .. }));
    }

    #[test]
    fn classify_tool_use_routes_task_to_subagent_started() {
        let ev = classify_tool_use(
            "Task",
            &json!({ "description": "review auth", "subagent_type": "code-reviewer" }),
            "s1",
        );
        assert!(
            matches!(&ev, ProviderEvent::SubagentStarted { agent, task, .. }
                if agent == "code-reviewer" && task == "review auth"),
            "{ev:?}"
        );
    }

    #[test]
    fn classify_tool_use_routes_agent_tool_to_subagent_started() {
        // The current Claude Code SDK spawns subagents via the `Agent` tool
        // (not the legacy `Task` name). Both must classify to SubagentStarted.
        let ev = classify_tool_use(
            "Agent",
            &json!({
                "description": "Read README first heading",
                "prompt": "read README.md"
            }),
            "s1",
        );
        assert!(
            matches!(&ev, ProviderEvent::SubagentStarted { task, .. }
                if task == "Read README first heading"),
            "{ev:?}"
        );
    }

    #[test]
    fn classify_tool_use_routes_skill_to_skill_dispatched() {
        let ev = classify_tool_use(
            "Skill",
            &json!({ "skill": "kmr-build", "target": "release" }),
            "s1",
        );
        assert!(
            matches!(&ev, ProviderEvent::SkillDispatched { skill, args, .. }
                if skill == "kmr-build" && args["target"] == "release"),
            "{ev:?}"
        );
    }

    #[test]
    fn classify_tool_use_passes_through_plain_tools() {
        let ev = classify_tool_use("Read", &json!({ "path": "/x" }), "s1");
        assert!(
            matches!(&ev, ProviderEvent::ToolCall { tool_name, .. } if tool_name == "Read"),
            "{ev:?}"
        );
        let ev = classify_tool_use("Bash", &json!({ "cmd": "ls" }), "s1");
        assert!(matches!(ev, ProviderEvent::ToolCall { .. }));
    }

    #[test]
    fn assistant_tool_use_block_emits_tool_call() {
        let events = map_message_blocks(
            &json!({ "message": { "content": [
                { "type": "text", "text": "hi" },
                { "type": "tool_use", "name": "Write", "input": { "path": "/x" } }
            ] } }),
            "s1",
        );
        assert_eq!(events.len(), 1, "text blocks must not surface: {events:?}");
        assert!(
            matches!(&events[0], ProviderEvent::ToolCall { tool_name, tool_input, .. }
                if tool_name == "Write" && tool_input["path"] == "/x"),
            "{events:?}"
        );
    }

    #[test]
    fn result_success_decodes_completed() {
        let outcome = decode_result(&json!({
            "type": "result", "subtype": "success", "is_error": false, "result": "done"
        }));
        assert_eq!(outcome.status, TurnStatus::Completed);
        assert_eq!(outcome.output, "done");
    }

    #[test]
    fn result_error_decodes_failed() {
        let outcome = decode_result(&json!({
            "type": "result", "subtype": "error_during_execution", "is_error": true, "result": "boom"
        }));
        assert_eq!(outcome.status, TurnStatus::Failed);
        assert_eq!(outcome.output, "boom");

        // is_error without an error subtype also counts as failed.
        let outcome = decode_result(&json!({
            "type": "result", "subtype": "success", "is_error": true, "result": "x"
        }));
        assert_eq!(outcome.status, TurnStatus::Failed);
    }

    #[test]
    fn map_sdk_message_routes_by_type() {
        assert!(matches!(
            map_sdk_message(&json!({ "type": "system", "subtype": "init" }), "s"),
            SdkEmission::Ignore
        ));
        assert!(matches!(
            map_sdk_message(
                &json!({ "type": "stream_event", "event": { "type": "content_block_delta",
                    "delta": { "text": "x" } } }),
                "s"
            ),
            SdkEmission::Events(_)
        ));
        assert!(matches!(
            map_sdk_message(&json!({ "type": "result", "subtype": "success" }), "s"),
            SdkEmission::Terminal(_)
        ));
    }

    #[test]
    fn extract_usage_from_result_and_message() {
        let u = extract_usage(&json!({
            "type": "result", "usage": { "input_tokens": 10, "output_tokens": 4 }
        }))
        .unwrap();
        assert_eq!(
            (u.input_tokens, u.output_tokens, u.total_tokens),
            (10, 4, 14)
        );

        let u = extract_usage(&json!({
            "message": { "usage": { "input_tokens": 7, "output_tokens": 3, "total_tokens": 99 } }
        }))
        .unwrap();
        assert_eq!(
            (u.input_tokens, u.output_tokens, u.total_tokens),
            (7, 3, 99)
        );

        assert!(
            extract_usage(&json!({ "usage": { "input_tokens": 0, "output_tokens": 0 } })).is_none()
        );
        assert!(extract_usage(&json!({ "type": "system" })).is_none());
    }

    // --- run_turn over a fake reader (no real binary) ---

    /// Drain Token events for `sid` from a broadcast receiver already subscribed
    /// BEFORE the turn ran (so broadcast buffered every emitted event). Times out
    /// once the bus goes quiet.
    async fn drain_tokens(mut rx: broadcast::Receiver<ProviderEvent>, sid: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        while let Ok(recv) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            match recv {
                Ok(ProviderEvent::Token {
                    content,
                    session_id,
                }) if session_id == sid => {
                    tokens.push(content);
                }
                _ => break,
            }
        }
        tokens
    }

    #[tokio::test]
    async fn run_turn_streams_tokens_and_completes() {
        let (event_tx, _rx) = broadcast::channel::<ProviderEvent>(256);
        let lines = concat!(
            r#"{"type":"system","subtype":"init","session_id":"abc"}"#,
            "\n",
            r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"Hello "}}}"#,
            "\n",
            r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"world"}}}"#,
            "\n",
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Hello world"}],"usage":{"input_tokens":5,"output_tokens":2}}}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false,"result":"Hello world","usage":{"input_tokens":5,"output_tokens":2}}"#,
            "\n",
        );

        // Subscribe BEFORE the turn so broadcast buffers every emitted event.
        let rx = event_tx.subscribe();

        let reader = BufReader::new(lines.as_bytes());
        let outcome = run_turn(reader, "s1", &event_tx).await.expect("run_turn");

        assert_eq!(outcome.status, TurnStatus::Completed);
        assert_eq!(outcome.output, "Hello world");
        let usage = outcome.usage.expect("usage");
        assert_eq!((usage.input_tokens, usage.output_tokens), (5, 2));

        let tokens = drain_tokens(rx, "s1").await;
        assert_eq!(tokens, vec!["Hello ", "world"]);
    }

    #[tokio::test]
    async fn run_turn_failed_result_is_failed() {
        let (event_tx, _rx) = broadcast::channel::<ProviderEvent>(64);
        let lines = r#"{"type":"result","subtype":"error_during_execution","is_error":true,"result":"boom"}"#;
        let outcome = run_turn(BufReader::new(lines.as_bytes()), "s2", &event_tx)
            .await
            .expect("run_turn");
        assert_eq!(outcome.status, TurnStatus::Failed);
        assert_eq!(outcome.output, "boom");
    }

    #[tokio::test]
    async fn run_turn_eof_before_result_errors() {
        let (event_tx, _rx) = broadcast::channel::<ProviderEvent>(64);
        let lines = r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"text":"partial"}}}"#;
        let err = run_turn(BufReader::new(lines.as_bytes()), "s3", &event_tx)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::ProcessExited(ref m) if m.contains("closed before")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn run_turn_skips_unparseable_lines() {
        let (event_tx, _rx) = broadcast::channel::<ProviderEvent>(64);
        // A garbage line is skipped; the turn still completes on the result.
        let lines = concat!(
            "this is not json\n",
            r#"{"type":"result","subtype":"success","is_error":false,"result":"ok"}"#,
            "\n",
        );
        let outcome = run_turn(BufReader::new(lines.as_bytes()), "s4", &event_tx)
            .await
            .expect("run_turn");
        assert_eq!(outcome.status, TurnStatus::Completed);
        assert_eq!(outcome.output, "ok");
    }

    // --- adapter lifecycle / glue (no real binary touched) ---

    #[tokio::test]
    async fn adapter_not_spawned_initially() {
        let adapter = ClaudeAdapter::new();
        assert_eq!(adapter.provider_id(), PROVIDER_CLAUDE);
        assert_eq!(adapter.status(), ProviderStatus::Disconnected);
        assert!(!adapter.spawned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn double_spawn_is_rejected_before_subprocess_launch() {
        // A spawned adapter must hit the guard and error WITHOUT spawning `claude`.
        let mut adapter = ClaudeAdapter::new();
        adapter
            .spawn(ProviderConfig::default())
            .await
            .expect("first spawn");
        let err = adapter.spawn(ProviderConfig::default()).await.unwrap_err();
        assert!(
            matches!(err, ProviderAdapterError::ConfigError(ref m) if m.contains("already spawned")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn operations_before_spawn_error() {
        let mut adapter = ClaudeAdapter::new();
        assert!(matches!(
            adapter.start_session(make_ctx()).await.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
        assert!(matches!(
            adapter.shutdown().await.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
    }

    #[tokio::test]
    async fn send_request_without_session_errors() {
        let adapter = ClaudeAdapter::new();
        // spawned=false → NotSpawned first.
        let req = ProviderRequest::new("chat", Some(json!({ "input": "hi" })));
        assert!(matches!(
            adapter.send_request(req).await.unwrap_err(),
            ProviderAdapterError::NotSpawned
        ));
    }

    #[tokio::test]
    async fn start_session_records_session_and_prompt() {
        let mut adapter = ClaudeAdapter::with_claude_config(ClaudeConfig {
            bin_path: "claude".to_string(),
            model: "sonnet".to_string(),
            full_auto: true,
            api_key: Some("sk-test".to_string()),
            extra_args: vec![],
        });
        adapter.spawn(ProviderConfig::default()).await.unwrap();

        let session_id = adapter.start_session(make_ctx()).await.unwrap();
        assert!(session_id.starts_with("claude-"));
        assert_eq!(adapter.status(), ProviderStatus::Busy);
        assert_eq!(
            adapter.current_session.lock().await.as_deref(),
            Some(session_id.as_str())
        );

        // The system prompt is carried into the session record.
        let sessions = adapter.sessions.lock().await;
        let rec = sessions.get(&session_id).expect("session recorded");
        assert_eq!(rec.system_prompt.as_deref(), Some("Be helpful."));
        assert_eq!(rec.state.working_dir, "/tmp/test-claude-project");
    }

    #[tokio::test]
    async fn resume_and_stop_session_unknown_errors() {
        let mut adapter = ClaudeAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();
        assert!(matches!(
            adapter.resume_session("nope").await.unwrap_err(),
            ProviderAdapterError::SessionNotFound(_)
        ));
        assert!(matches!(
            adapter.stop_session("nope").await.unwrap_err(),
            ProviderAdapterError::SessionNotFound(_)
        ));
    }

    #[tokio::test]
    async fn interrupt_unknown_session_errors() {
        let adapter = ClaudeAdapter::new();
        // Not spawned → no sessions → SessionNotFound.
        let err = adapter.interrupt("nope").await.unwrap_err();
        assert!(matches!(err, ProviderAdapterError::SessionNotFound(_)));
    }

    #[tokio::test]
    async fn health_check_reflects_spawned_state() {
        let adapter = ClaudeAdapter::new();
        assert!(!adapter.health_check().await.unwrap());

        let mut adapter = ClaudeAdapter::new();
        adapter.spawn(ProviderConfig::default()).await.unwrap();
        assert!(adapter.health_check().await.unwrap());
    }

    #[tokio::test]
    async fn capabilities_and_models() {
        let adapter = ClaudeAdapter::new();
        let caps = adapter.capabilities();
        assert!(caps.contains(&ProviderCapability::Streaming));
        assert!(caps.contains(&ProviderCapability::ToolUse));
        assert!(!adapter.available_models().is_empty());
    }

    // --- config helpers ---

    #[test]
    fn claude_config_defaults() {
        let config = ClaudeConfig::default();
        assert_eq!(config.bin_path, "claude");
        assert!(config.full_auto);
        assert_eq!(config.model, "sonnet");
    }

    #[test]
    fn argv_builds_print_streamjson_and_full_auto() {
        let config = ClaudeConfig::default();
        let argv = config.argv("hello", Some("sonnet"), Some("Be terse."));
        assert_eq!(argv[0], "claude");
        assert!(argv.iter().any(|a| a == "-p"));
        assert!(argv.windows(2).any(|w| w[0] == "-p" && w[1] == "hello"));
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--output-format" && w[1] == "stream-json")
        );
        // stream-json under --print REQUIRES --verbose (Claude CLI enforces it).
        assert!(argv.iter().any(|a| a == "--verbose"));
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--model" && w[1] == "sonnet")
        );
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "--append-system-prompt" && w[1] == "Be terse.")
        );
        assert!(argv.iter().any(|a| a == "--dangerously-skip-permissions"));
    }

    #[test]
    fn argv_omits_full_auto_flag_when_disabled() {
        let config = ClaudeConfig {
            full_auto: false,
            ..ClaudeConfig::default()
        };
        let argv = config.argv("hi", None, None);
        assert!(!argv.iter().any(|a| a == "--dangerously-skip-permissions"));
        // No model/system → those flags omitted too.
        assert!(!argv.iter().any(|a| a == "--model"));
        assert!(!argv.iter().any(|a| a == "--append-system-prompt"));
    }

    #[test]
    fn argv_appends_extra_args() {
        let config = ClaudeConfig {
            extra_args: vec!["--verbose".to_string(), "--foo".to_string()],
            ..ClaudeConfig::default()
        };
        let argv = config.argv("hi", None, None);
        assert!(argv.iter().any(|a| a == "--verbose"));
        assert!(argv.iter().any(|a| a == "--foo"));
    }

    #[test]
    fn turn_prompt_uses_input_field() {
        let prompt = ClaudeAdapter::turn_prompt(&Some(json!({ "input": "hi", "x": 1 })));
        assert_eq!(prompt, "hi");
    }

    #[test]
    fn turn_prompt_falls_back_to_params_rendering() {
        let prompt = ClaudeAdapter::turn_prompt(&Some(json!({ "foo": "bar" })));
        assert!(prompt.contains("foo"));
    }

    #[test]
    fn turn_prompt_empty_when_null() {
        assert_eq!(ClaudeAdapter::turn_prompt(&None), "");
    }

    #[test]
    fn model_resolution_prefers_request_then_config() {
        // Use real claude CLI models (aliases) so resolve_model_slug passes them
        // through unchanged — this test is about priority, not slug mapping.
        let mut adapter = ClaudeAdapter::with_claude_config(ClaudeConfig {
            model: "haiku".to_string(),
            ..ClaudeConfig::default()
        });
        adapter.config = Some(ProviderConfig {
            model: "opus".to_string(),
            ..ProviderConfig::default()
        });

        // No request model → spawn config model.
        let req = ProviderRequest::new("chat", Some(json!({ "input": "x" })));
        assert_eq!(adapter.model_for(&req.params).as_deref(), Some("opus"));

        // Request model wins.
        let req = ProviderRequest::new("chat", Some(json!({ "input": "x", "model": "sonnet" })));
        assert_eq!(adapter.model_for(&req.params).as_deref(), Some("sonnet"));

        // No config model → claude_config default.
        adapter.config = None;
        let req = ProviderRequest::new("chat", Some(json!({ "input": "x" })));
        assert_eq!(adapter.model_for(&req.params).as_deref(), Some("haiku"));
    }

    #[test]
    fn resolve_model_slug_maps_frontend_slugs_to_cli_aliases() {
        let adapter = ClaudeAdapter::with_claude_config(ClaudeConfig::default());
        // Frontend UI slugs (future/hypothetical tiers) map to CLI aliases.
        assert_eq!(adapter.resolve_model_slug("claude-sonnet-4-6"), "sonnet");
        assert_eq!(adapter.resolve_model_slug("claude-opus-4-8"), "opus");
        assert_eq!(adapter.resolve_model_slug("claude-haiku-4-5"), "haiku");
        assert_eq!(adapter.resolve_model_slug("claude-fable-5"), "sonnet");
        // CLI aliases and real ids pass through unchanged.
        assert_eq!(adapter.resolve_model_slug("sonnet"), "sonnet");
        assert_eq!(adapter.resolve_model_slug("opus"), "opus");
        assert_eq!(
            adapter.resolve_model_slug("claude-sonnet-4-5-20250929"),
            "claude-sonnet-4-5-20250929"
        );
        // Non-claude slugs (e.g. a codex/gpt slug like "gpt-5.4-mini" left over
        // when the provider switched to claude but the model didn't reset) fall
        // back to the claude default instead of being rejected by the CLI.
        assert_eq!(adapter.resolve_model_slug("gpt-5.4-mini"), "sonnet");
        assert_eq!(adapter.resolve_model_slug("some-unknown-model"), "sonnet");
    }

    #[test]
    fn model_for_maps_frontend_slug_to_alias() {
        let adapter = ClaudeAdapter::with_claude_config(ClaudeConfig::default());
        let req = ProviderRequest::new(
            "chat",
            Some(json!({ "input": "x", "model": "claude-sonnet-4-6" })),
        );
        assert_eq!(adapter.model_for(&req.params).as_deref(), Some("sonnet"));
    }

    // -----------------------------------------------------------------------
    // PR-1-1 trace proof — reproduces the EXACT SessionContext the command
    // reactor builds in `handle_start_turn` (command.rs:794-802) and drives it
    // through ClaudeAdapter::start_session, then reconstructs the argv
    // send_request would spawn. Asserts the two break points:
    //   (1) working_dir is the hardcoded "/tmp/syncode" (DEFAULT_WORKING_DIR),
    //       NOT the user's actual project cwd → `claude` runs in a phantom dir.
    //   (2) system_prompt is the hardcoded "You are a helpful AI coding
    //       assistant." string → project-specific instructions never reach the
    //       CLI.
    // No real `claude` binary is spawned: start_session only records state, and
    // the argv is built from the public ClaudeConfig::argv helper using the
    // recorded (working_dir, system_prompt) — exactly what send_request does
    // (claude.rs:684-708) before it calls Command::spawn.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn pr_1_1_trace_start_turn_propagates_hardcoded_working_dir_and_prompt() {
        // --- Arrange: a spawned Claude adapter (no binary needed for this path).
        let mut adapter = ClaudeAdapter::with_claude_config(ClaudeConfig {
            bin_path: "claude".to_string(),
            model: "sonnet".to_string(),
            full_auto: true,
            api_key: Some("sk-test".to_string()),
            extra_args: vec![],
        });
        adapter.spawn(ProviderConfig::default()).await.unwrap();

        // --- Reproduce the SessionContext that command.rs::handle_start_turn
        //     builds (command.rs:794-802). These are the two hardcoded values
        //     that the trace identifies as the break point:
        //       - working_dir: DEFAULT_WORKING_DIR = "/tmp/syncode"
        //       - system_prompt: "You are a helpful AI coding assistant."
        let reactor_built_ctx = SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            // BREAK POINT 1: hardcoded in command.rs:134 / 798.
            working_dir: "/tmp/syncode".to_string(),
            // BREAK POINT 2: hardcoded in command.rs:799.
            system_prompt: Some("You are a helpful AI coding assistant.".to_string()),
            user_input: "Fix the bug in main.rs".to_string(),
            context_files: vec![],
        };

        // --- Act: drive it through start_session (what ensure_session_for_thread
        //     → SessionManager::start_session → adapter.start_session does).
        let session_id = adapter.start_session(reactor_built_ctx).await.unwrap();
        assert!(session_id.starts_with("claude-"));

        // --- Assert (1): the session recorded the HARDCODED working_dir, not a
        //     real project cwd. send_request (claude.rs:716) does
        //     `current_dir(&working_dir)`, so the `claude` CLI would spawn in
        //     /tmp/syncode — a directory that does not exist on Windows and is
        //     almost never the user's project root.
        let recorded = {
            let sessions = adapter.sessions.lock().await;
            sessions.get(&session_id).expect("session recorded").clone()
        };
        assert_eq!(
            recorded.state.working_dir, "/tmp/syncode",
            "BREAK POINT 1: handle_start_turn hardcodes working_dir to DEFAULT_WORKING_DIR \
             (/tmp/syncode); the user's real project cwd never reaches the adapter"
        );

        // --- Assert (2): the session recorded the HARDCODED system prompt, not
        //     any project-specific instructions. send_request (claude.rs:708)
        //     passes this verbatim to argv → `--append-system-prompt`.
        assert_eq!(
            recorded.system_prompt.as_deref(),
            Some("You are a helpful AI coding assistant."),
            "BREAK POINT 2: handle_start_turn hardcodes the system prompt; project-specific \
             instructions are never injected"
        );

        // --- Assert (3): reconstruct the argv send_request WOULD spawn
        //     (claude.rs:706-708 builds it via ClaudeConfig::argv from the
        //     recorded prompt + system_prompt), and verify the working_dir and
        //     prompt land in the spawn. This is the exact argv that
        //     `Command::new(&bin).args(&args).current_dir(&working_dir)` uses.
        let prompt =
            ClaudeAdapter::turn_prompt(&Some(json!({ "input": "Fix the bug in main.rs" })));
        let argv =
            adapter
                .claude_config
                .argv(&prompt, Some("sonnet"), recorded.system_prompt.as_deref());

        // The user input flows through as the -p prompt.
        assert!(
            argv.windows(2)
                .any(|w| w[0] == "-p" && w[1] == "Fix the bug in main.rs"),
            "user input should reach the CLI as the -p prompt"
        );
        // The hardcoded system prompt flows through as --append-system-prompt.
        assert!(
            argv.windows(2).any(|w| w[0] == "--append-system-prompt"
                && w[1] == "You are a helpful AI coding assistant."),
            "the hardcoded system prompt should reach the CLI via --append-system-prompt"
        );
        // The working_dir is NOT the real project root — confirm it is the
        // sentinel value the trace flags as the break point.
        assert_ne!(
            recorded.state.working_dir,
            std::env::current_dir()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
            "the hardcoded working_dir differs from the actual process cwd — confirming the break"
        );
    }

    // -----------------------------------------------------------------------
    // PR #240 follow-up: stateful tool_result routing (SubagentCompleted,
    // ExploreUpdated) and slash-command SkillDispatched synthesis.
    // -----------------------------------------------------------------------

    #[test]
    fn turn_mapper_tracks_tool_use_id_and_routes_subagent_result() {
        // Drive a TurnMapper through the same sequence Claude emits for a Task
        // tool turn: assistant message carries the tool_use block, user message
        // carries the matching tool_result. The result must surface as
        // SubagentCompleted (not a generic ToolResult) because the originating
        // tool was Task.
        let mut mapper = TurnMapper::default();
        let assistant = json!({
            "type": "assistant",
            "message": { "content": [
                { "type": "tool_use", "id": "tu_1", "name": "Task",
                  "input": { "subagent_type": "code-reviewer", "description": "review auth" } }
            ] }
        });
        let user = json!({
            "type": "user",
            "message": { "content": [
                { "type": "tool_result", "tool_use_id": "tu_1",
                  "content": [{ "type": "text", "text": "approved" }] }
            ] }
        });

        let ev1 = mapper.map_sdk_message(&assistant, "s1");
        let ev2 = mapper.map_sdk_message(&user, "s1");

        use SdkEmission::*;
        let events1 = match ev1 {
            Events(v) => v,
            _ => Vec::new(),
        };
        let events2 = match ev2 {
            Events(v) => v,
            _ => Vec::new(),
        };

        // First message emits the SubagentStarted variant for the tool_use.
        assert!(
            events1.iter().any(|e| matches!(e,
                ProviderEvent::SubagentStarted { agent, task, .. }
                if agent == "code-reviewer" && task == "review auth")),
            "expected SubagentStarted from tool_use, got {events1:?}"
        );
        // Second message emits SubagentCompleted (not ToolResult).
        assert_eq!(events2.len(), 1, "expected exactly one event: {events2:?}");
        assert!(
            matches!(&events2[0], ProviderEvent::SubagentCompleted { agent, result, .. }
                if agent == "code-reviewer" && result == "approved"),
            "expected SubagentCompleted, got {:?}",
            events2[0]
        );
    }

    #[test]
    fn turn_mapper_emits_explore_updated_with_count_before_tool_result() {
        // Glob tool_use → tool_result. The result must emit ExploreUpdated with
        // a line count summary THEN a generic ToolResult.
        let mut mapper = TurnMapper::default();
        let assistant = json!({
            "type": "assistant",
            "message": { "content": [
                { "type": "tool_use", "id": "tu_2", "name": "Glob",
                  "input": { "pattern": "**/*.rs" } }
            ] }
        });
        let user = json!({
            "type": "user",
            "message": { "content": [
                { "type": "tool_result", "tool_use_id": "tu_2",
                  "content": "a.rs\nb.rs\nc.rs" }
            ] }
        });

        let _ = mapper.map_sdk_message(&assistant, "s1");
        let ev = mapper.map_sdk_message(&user, "s1");
        let events = match ev {
            SdkEmission::Events(v) => v,
            _ => Vec::new(),
        };

        assert_eq!(
            events.len(),
            2,
            "expected ExploreUpdated + ToolResult: {events:?}"
        );
        assert!(
            matches!(&events[0], ProviderEvent::ExploreUpdated { message, .. }
                if message.contains("Found 3")),
            "expected ExploreUpdated with count, got {:?}",
            events[0]
        );
        assert!(
            matches!(&events[1], ProviderEvent::ToolResult { tool_name, .. }
                if tool_name == "explore"),
            "expected ToolResult with tool_name=explore, got {:?}",
            events[1]
        );
    }

    #[test]
    fn turn_mapper_routes_unknown_tool_use_id_to_plain_tool_result() {
        // A tool_result whose tool_use_id wasn't recorded (e.g. arrived before
        // the assistant message in a replay) must fall back to a plain
        // ToolResult — no panic, no SubagentCompleted.
        let mut mapper = TurnMapper::default();
        let user = json!({
            "type": "user",
            "message": { "content": [
                { "type": "tool_result", "tool_use_id": "tu_unknown",
                  "content": "ok" }
            ] }
        });
        let ev = mapper.map_sdk_message(&user, "s1");
        let events = match ev {
            SdkEmission::Events(v) => v,
            _ => Vec::new(),
        };
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], ProviderEvent::ToolResult { .. }));
    }

    #[test]
    fn detect_slash_command_skill_recognizes_alpha_names() {
        // /kmr-build release → SkillDispatched { skill: "kmr-build", args: "release" }
        let ev = detect_slash_command_skill("/kmr-build release", "s1").expect("should detect");
        assert!(
            matches!(ev, ProviderEvent::SkillDispatched { ref skill, ref args, .. }
                if skill == "kmr-build" && args == "release"),
            "{ev:?}"
        );

        // No args.
        let ev = detect_slash_command_skill("/lint", "s1").expect("should detect");
        assert!(
            matches!(ev, ProviderEvent::SkillDispatched { ref skill, ref args, .. }
                if skill == "lint" && args == &serde_json::Value::Null),
            "{ev:?}"
        );

        // Underscores and digits allowed in name.
        let ev = detect_slash_command_skill("/skill_42", "s1").expect("should detect");
        assert!(
            matches!(ev, ProviderEvent::SkillDispatched { ref skill, .. } if skill == "skill_42"),
            "{ev:?}"
        );
    }

    #[test]
    fn detect_slash_command_skill_rejects_non_alpha_and_paths() {
        // Shell-style "//" and file paths must NOT be treated as skill invocations.
        assert!(detect_slash_command_skill("//comment", "s1").is_none());
        assert!(detect_slash_command_skill("/!bang", "s1").is_none());
        assert!(detect_slash_command_skill("/path/to/file", "s1").is_none());
        // Leading whitespace is tolerated; missing slash is not.
        assert!(detect_slash_command_skill("plain prompt", "s1").is_none());
        assert!(detect_slash_command_skill("   /kmr-build", "s1").is_some());
    }

    #[tokio::test]
    async fn run_turn_emits_subagent_completed_for_task_tool_chain() {
        // End-to-end: feed run_turn the same NDJSON sequence a real Claude CLI
        // emits for a Task-tool turn. Assert that both SubagentStarted (from
        // the tool_use block) and SubagentCompleted (from the tool_result)
        // reach the event bus.
        let (event_tx, _) = broadcast::channel::<ProviderEvent>(64);
        let mut rx = event_tx.subscribe();
        let lines = concat!(
            // Assistant emits a Task tool_use.
            r#"{"type":"assistant","message":{"role":"assistant","content":["#,
            r#"{"type":"tool_use","id":"tu_x","name":"Task","#,
            r#""input":{"subagent_type":"general-purpose","description":"read README"}}]}}"#,
            "\n",
            // User emits the matching tool_result.
            r#"{"type":"user","message":{"role":"user","content":["#,
            r#"{"type":"tool_result","tool_use_id":"tu_x","#,
            r#""content":[{"type":"text","text":"heading is SynCode"}]}]}}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false,"result":"done"}"#,
            "\n",
        );
        let outcome = run_turn(BufReader::new(lines.as_bytes()), "sx", &event_tx)
            .await
            .expect("run_turn");
        assert_eq!(outcome.status, TurnStatus::Completed);

        // Drain all events for our session id.
        let mut events = Vec::new();
        while let Ok(Ok(rx2)) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            if let ProviderEvent::SubagentStarted { .. } | ProviderEvent::SubagentCompleted { .. } =
                &rx2
            {
                events.push(rx2);
            }
        }
        let has_started = events.iter().any(
            |e| matches!(e, ProviderEvent::SubagentStarted { task, .. } if task == "read README"),
        );
        let has_completed = events.iter().any(|e| {
            matches!(e,
            ProviderEvent::SubagentCompleted { result, .. } if result == "heading is SynCode")
        });
        assert!(has_started, "SubagentStarted missing: {events:?}");
        assert!(has_completed, "SubagentCompleted missing: {events:?}");
    }

    #[tokio::test]
    async fn run_turn_emits_explore_updated_for_glob_result_chain() {
        let (event_tx, _) = broadcast::channel::<ProviderEvent>(64);
        let mut rx = event_tx.subscribe();
        let lines = concat!(
            r#"{"type":"assistant","message":{"role":"assistant","content":["#,
            r#"{"type":"tool_use","id":"tu_g","name":"Glob","input":{"pattern":"**/*.rs"}}]}}"#,
            "\n",
            r#"{"type":"user","message":{"role":"user","content":["#,
            r#"{"type":"tool_result","tool_use_id":"tu_g","#,
            r#""content":"a.rs\nb.rs"}]}}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false,"result":"ok"}"#,
            "\n",
        );
        let outcome = run_turn(BufReader::new(lines.as_bytes()), "sg", &event_tx)
            .await
            .expect("run_turn");
        assert_eq!(outcome.status, TurnStatus::Completed);

        let mut explore_updates = Vec::new();
        while let Ok(Ok(ev)) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            if let ProviderEvent::ExploreUpdated { message, .. } = ev {
                explore_updates.push(message);
            }
        }
        assert!(
            explore_updates.iter().any(|m| m.contains("Found 2")),
            "expected ExploreUpdated with Found 2, got {explore_updates:?}"
        );
    }
}

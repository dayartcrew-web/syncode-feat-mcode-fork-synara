# Syncode Production-Readiness Assessment

**Date:** 2026-07-17 (updated) · **Master:** `34557ec` · **Source of truth:** [`mcode`](https://github.com/...) at `/home/vibe-dev/mcode`
**Audited against current code** (post Bug #2 fixes #184–#186/#188, git panel #189–#192, production-readiness blocker fixes #194–#201, AND stub-gap cleanup #202–#204).

> **Headline: 13 of 15 subsystems fully production-ready; 2 at-parity (not blocking).**
> All 5 production blockers from the initial audit are **SHIPPED** (#194–#204). Stub/gap cleanup also done: 8 dead .t1-legacy files deleted (#202), git unstage implemented (#203), RenameProject command added (#204). The 2 non-✅ items are: **Memory** (🟡 SQLite interactions log — parity with mcode, not vector/graph by design) and **MCP/tools** (⚪ not-implemented — mcode also lacks it, parity). Neither is a regression or blocker.

## Status table

| # | Subsystem | Status | Risk | Evidence |
|---|-----------|--------|------|----------|
| 1 | Orchestration / CQRS pipeline | ✅ Prod | Low | 49 cmd / 45 event (> mcode), real decider/reactors/projector, optimistic concurrency, snapshotting (`syncode-orchestration/src/{pipeline,decider,projector,reactors/}`) |
| 2 | Persistence / Event store | ✅ Prod | Low | Real SQLite ES, WAL, snapshots, cold-start replay wired (`c5e429b`, `syncode-persistence/src/{event_store,snapshot}.rs`) |
| 3 | Providers — 7 CLI real | ✅ Prod | Low | codex, claude, pi (bespoke clients) + cursor/grok/gemini (shared ACP v0.11.3) + kilo (HTTP+SSE). Real wire protocols, 6 gated E2E |
| 3a | **opencode** | ✅ Prod (#198/#200) | Low | serve-primary (HTTP+SSE, kilo parity) + auth via `OPENCODE_SERVER_PASSWORD` env (#200) + defensive one-shot fallback (#198). E2E streamed P/ONG/PONG via SSE. |
| 3b | **anthropic, openai** | ✅ Prod (#197) | Low | HTTP non-streaming; **honest capabilities** (SystemPrompt only — no false Streaming/ToolUse/Vision, #197). Single-turn text completion. |
| 4 | Frontend chat cycle | ✅ Prod | Low | composer → turn → stream → render end-to-end; `contracts/adaptPushEvent.ts` (585 LOC) translator; 30+ event reducer (`store.ts`) |
| 5 | Sidebar / projects / threads / routing | ✅ Prod (**ahead**) | Low | shell snapshot subscribe typed; routing recovery better than mcode (PR #181–#183) |
| 6 | Settings / stats / provider picker | ✅ Prod | Low | server-backed `textGenerationProvider` (PR #177–#179); real 274-day activity heatmap + streaks + JSONL archive walk |
| 7 | Skills | ✅ Prod | None | 10-origin faithful port of mcode `skillsCatalog.ts` (`syncode-ws/src/skills_catalog.rs`) |
| 8 | Git ops | ✅ Prod | Low | git2 + CLI subprocess, 14 ops (incl unstage #203), real upstream detection (#189–#192) |
| 9 | Automations | ✅ Prod (**ahead**) | Low | real scheduler + `ProcessRunExecutor` + retry policies (mcode stubs retry) + LLM completion eval |
| 10 | MCP / tools | ⚪ N/A | None | zero handlers — mcode also has none (parity, not a regression) |
| 11 | **Desktop (Tauri)** | ✅ Prod (#199) | Low | bootable shell + 28 IPC handlers + **real WS push channel** (#199: nativeApi constructs WsTransport → embedded WS server, tauriNativeApi demux mirrors wsNativeApi). Browser path unchanged. |
| 12 | Auth | ✅ Prod | Low | shared-secret + pairing + `constant_time_eq` (local-first, matches mcode modes) |
| 13 | Terminals | ✅ Prod | None | real `portable_pty` + scrollback persistence |
| 14 | Local servers | ✅ Prod | None | real tokio subprocess lifecycle manager |
| 15 | Memory | 🟡 Partial | Low | SQLite interactions log (recent-N context) — **parity with mcode**, not vector/graph |

Legend: ✅ Production-ready · 🟡 Partial · 🔴/⚪ Stub/Not-implemented · "ahead" = syncode strictly better than mcode.

## Production blockers — ALL RESOLVED (#194–#200)

The initial audit (2026-07-17) identified 5 blockers. All are now **shipped**:

| # | Blocker | Fix | PR |
|---|---------|-----|----|
| 1 | opencode provider regression (HIGH) | serve-primary (kilo parity HTTP+SSE) + `OPENCODE_SERVER_PASSWORD` env auth + one-shot fallback | #198 + #200 |
| 2 | anthropic/openai false capabilities | honest `capabilities()` = `[SystemPrompt]` only | #197 |
| 3 | Tauri desktop push channel no-op | real WsTransport + push demux (mirror wsNativeApi) | #199 |
| 4 | Dead SQLite `view_*` projection layer (−1,571 LOC) | deleted (never used in production) | #196 |
| 5 | Stale "T6c-10 STUB" comments | comments corrected + 2 dead .t1-legacy files deleted | #194 |
| (bonus) | Token-dimension heatmap `[]` | filled with real 274-day data (UsageStore) | #195 |
| (cleanup) | 8 dead .t1-legacy stub files | deleted (zero references) | #202 |
| (cleanup) | git unstage deferred | implemented via `git restore --staged` | #203 |
| (cleanup) | project title "not yet supported" | full RenameProject command (event+decider+projector+rpc) | #204 |

## Already production-ready (confirmed, not a stub)

These subsystems are real, faithful, and end-to-end wired:

- **Orchestration / CQRS pipeline** — 49 cmd / 45 event (exceeds mcode), real decider / reactors / projector, optimistic concurrency, snapshotting.
- **Persistence / event store** — real SQLite ES, cold-start replay wired (`c5e429b`), snapshots tested.
- **7 CLI providers** — codex, claude, pi (bespoke clients) + cursor/grok/gemini (shared ACP) + kilo. Real wire protocols, 6 gated E2E.
- **Frontend chat cycle** — composer → turn → stream → render end-to-end; `adaptPushEvent.ts` 585 LOC translator; 30+ event reducer.
- **Sidebar / projects / threads / routing** — shell snapshot subscribe typed; routing recovery **ahead of mcode**.
- **Settings / stats / provider picker** — fully server-backed; real 274-day heatmap + streaks + JSONL archive reader.
- **Skills catalog** — 10-origin faithful port of mcode.
- **Git ops** — git2 + CLI, 13 ops, real upstream detection.
- **Automations** — full scheduler / executor / LLM-completion-eval; **ahead of mcode**.
- **Auth (local-first)** — shared-secret + pairing + constant-time compare.
- **Terminals** — real `portable_pty` + scrollback persistence.
- **Local servers** — real tokio subprocess lifecycle manager.

## Ahead of mcode

- **Sidebar / routing recovery** (PR #181–#183 removed mcode's buggy `!hasKnownServerThreads` gate that broke URL-deep-linked threads).
- **Automations retry policies** — mcode stubs retry; syncode honors `ExponentialBackoff` / `FixedDelay` / `None`.
- **Provider surface** — 8 CLI ACP (matches mcode) + 2 extra HTTP providers (anthropic, openai).

## Methodology

Subsystem-by-subsystem comparison vs `/home/vibe-dev/mcode` (TS monorepo: `apps/server`, `apps/web`, `packages/contracts`). For each: read current source, grep for stub markers (`unimplemented`, `TODO`, "not yet", empty/placeholder returns, synthetic data), verify wiring end-to-end, check test coverage. Prior gap-analysis memory (`syncode-mcode-gap-research-complete`, `syncode-stub-gap-final-workflow`, `syncode-frontend-wired-not-mock`, `syncode-docs-ground-truth`, `syncode-vs-mcode-porting-fidelity`, `syncode-impact-and-risk`) was cross-checked and re-verified — several old gaps are now closed (notably Bug #2 persistence/reload via #184–#186/#188).

**Tone:** 13/15 subsystems fully production-ready with concrete evidence; 2 at-parity (Memory narrow-scope, MCP not-implemented — both match mcode). All 5 initial blockers + stub/gap cleanup shipped (#194–#204). Syncode is a faithful, real, end-to-end Rust port of mcode — not a blueprint or mock.

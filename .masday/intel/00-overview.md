# 00 — Overview

## What is this?
`syncode-feat-mcode-fork-synara` is a **Rust DDD (Domain-Driven Design) blueprint reimplementation of MCode** — a local-first AI-coding-agent desktop app (multi-provider AI, CQRS/Event-Sourcing orchestration, Git integration, terminal, automation scheduler, Tauri desktop shell).

It is **not a feature-complete port**; it's a deliberately slim, well-architected reference skeleton (~19,600 LOC ≈ **20% of MCode's 96,870-LOC server**) focused on the core CQRS/ES engine.

## Lineage (confirmed via git remotes)
```
synara (upstream — github.com/Emanuele-web04/synara)
   └─ mcode (fork — dayartcrew-web, Bun/TypeScript/Effect-TS/turbo)   ← ground truth at /home/vibe-dev/mcode
        └─ syncode (Rust port — dayartcrew-web)                         ← THIS repo
```
- Repo remote: `git@github.com:dayartcrew-web/syncode-feat-mcode-fork-synara.git`, branch `master`, clean.
- The fork name "feat-mcode-fork-synara" encodes the whole lineage.

## Stack
- **Language:** Rust 2024 edition, MSRV 1.85.0, Cargo workspace (`resolver = 2`).
- **Async runtime:** Tokio ("full").
- **HTTP/WS:** Axum 0.8 (ws feature), tower / tower-http (cors, trace), tokio-tungstenite 0.26.
- **DB:** SQLx 0.8 (runtime-tokio, sqlite, migrate). SQLite is the only backend.
- **Git:** git2 0.20. **Terminal:** portable-pty 0.9. **Validation:** garde 0.22.
- **Serialization:** serde + serde_json. **Rust→TS bridge:** ts-rs 10 (generates `frontend/src/types/*.ts`).
- **IDs/time:** uuid v4, chrono. **Errors:** thiserror 2, anyhow 1. **Logging:** tracing.
- **Desktop:** Tauri v2.
- **Frontend:** React 19 + Vite 6 + TypeScript 5.7 (minimal: App + 5 components + 2 hooks + types).

## Build status
- 422 tests pass (per `TEST_SUMMARY.md`); `syncode-tauri` excluded from `cargo test --workspace` (pre-existing build issues).
- CI (`.github/workflows/ci.yml`): 3 jobs — Check (`cargo fmt --check` + `cargo clippy`), Test (ubuntu/windows/macos matrix, `cargo test --workspace`), Build (release + artifact upload).
- `docs/COMPARISON-MCODE-vs-SYNCODE.md` claims the project is "empty" — **stale**; implementation has progressed well past it.

## Design intent
Faithful *architectural* port of MCode's CQRS/ES pattern (decider → projector → reactors) with hexagonal ports, but a **slimmed domain surface** (16 of MCode's ~39 commands, 14 of 35 events) and a **modeling divergence** (Turn/Message/Activity are first-class aggregates here; in MCode only project+thread are aggregates). See [02 — MCode fidelity](crates/../#) and `syncode-vs-mcode-porting-fidelity` memory.

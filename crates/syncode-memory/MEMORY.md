# Persistent Interaction Context

`syncode-memory` implements PRD Section 5 ("Persistent Memory"). It exposes a
[`MemoryProvider`] abstraction that retrieves and persists interaction context,
plus a SQLite-backed default store with FTS5 retrieval and a hybrid backend
composition layer for multi-store (vector / graph / episodic) memory.

## Modules

| Module | Purpose |
|--------|---------|
| `provider` | `MemoryProvider` trait + `MemoryProviderError` + `NO_PRIOR_CONTEXT` sentinel |
| `sqlite_store` | `SqliteMemoryStore` — default SQLite-backed impl with FTS5 retrieval |
| `hybrid` | `MemoryBackend` trait + `HybridMemoryProvider` + `InMemoryBackend` reference impl |
| `backends::episodic` | `EpisodicBackend` — append-only JSONL (always built) |
| `backends::vector` | `VectorBackend` — pgvector + fastembed (feature `pgvector`) |
| `backends::graph` | `GraphBackend` — Apache AGE (feature `age`) |

## Key types

| Type | Description |
|------|-------------|
| `MemoryProvider` | Trait: `retrieve_context` + `persist_interaction` |
| `MemoryProviderError` | Error enum (DB / IO / not-found) |
| `SqliteMemoryStore` | SQLite-backed default implementation |
| `DEFAULT_CONTEXT_LIMIT` | Default N for recent-interaction retrieval (3) |
| `NO_PRIOR_CONTEXT` | Returned instead of `""` when no history exists |
| `DEFAULT_PROJECT_ID` | Fallback project scope when caller omits it |
| `MemoryBackend` | Trait describing a single-store backend (vector / graph / episodic) |
| `Scope` | Per-session / per-project / per-user / global scoping tag |
| `MemoryEntry` | Persist payload (user_id, prompt, response, provider, tokens, scope) |
| `MemoryRecord` | Retrieval result entry with backend-specific score |
| `HybridMemoryProvider` | Composes one or more `MemoryBackend`s; implements `MemoryProvider` |
| `InMemoryBackend` | Reference in-memory backend (test-friendly, no external deps) |
| `EpisodicBackend` | Append-only JSONL backend (always built) |
| `VectorBackend` | pgvector + fastembed backend (feature-gated) |
| `GraphBackend` | Apache AGE backend (feature-gated) |

## Architecture

```
MemoryProvider (trait)
  ├── SqliteMemoryStore   (default impl — SQLite + FTS5)
  └── HybridMemoryProvider (composes backends)
        └── Vec<Arc<dyn MemoryBackend>>
              ├── InMemoryBackend   (reference impl, always built)
              ├── EpisodicBackend   (JSONL append-only, always built)
              ├── VectorBackend     (feature `pgvector`)
              └── GraphBackend      (feature `age`)
```

The `HybridMemoryProvider` is **additive** — the existing `MemoryProvider`
trait, `SqliteMemoryStore` call sites, and trait shape are unchanged.
Adding a backend is `HybridMemoryProvider::new().with_backend(Arc::new(...))`.

## SQLite schema

The `interactions` table is created additively (`CREATE TABLE IF NOT EXISTS`)
by `SqliteMemoryStore::init_schema`, plus a `interactions_fts` FTS5 virtual
table linked via `content='interactions'` and an AFTER INSERT trigger that
keeps the FTS index in sync on every write. No UPDATE / DELETE triggers are
needed because the store is append-only.

### Retrieval paths (PR #210)

- **Empty query:** recent-N by `timestamp DESC` (matches the prior contract).
- **Non-empty query:** FTS5 `MATCH` ordered by `bm25() ASC` with `timestamp DESC`
  tiebreak — the most relevant matches surface first. If FTS5 finds no matches,
  the query falls back to recency-N (NOT `NO_PRIOR_CONTEXT`).

## Backend feature flags

| Feature | Backend | External deps | Default? |
|---------|---------|---------------|----------|
| — | `EpisodicBackend` | none | always built |
| `pgvector` | `VectorBackend` | `vector` PG extension, fastembed (~50 MB compiled + ~250 MB model cache) | opt-in |
| `pgvector-integration` | pgvector integration tests | live Postgres + `SYNCODE_TEST_PGVECTOR=1` + `DATABASE_URL` | opt-in |
| `age` | `GraphBackend` | `age` PG extension | opt-in |
| `age-integration` | AGE integration tests | live Postgres + `SYNCODE_TEST_AGE=1` + `DATABASE_URL` | opt-in |

Default builds (`cargo build -p syncode-memory`) include only the episodic +
in-memory backends; no Postgres, no fastembed, no model downloads.

## Migrations

Standalone SQL files under `crates/syncode-memory/migrations/`:

- `20260720_memory_vectors.sql` — `memory_vectors` table + HNSW cosine index
  + recency fallback index (companion to `VectorBackend`).
- `20260720_memory_graph.sql` — `memory_graph` Cypher graph + vertex/edge
  labels (companion to `GraphBackend`).

Apply ahead of deploying a feature-enabled build; ops don't need to build
the Rust crate to run them.

## Integration tests

`tests/` directory contains cross-module end-to-end coverage:

- `hybrid_composition.rs` — `HybridMemoryProvider` composing always-built
  backends (InMemory + Episodic): fan-out persist, merge ordering, k-truncate,
  default-scope propagation, trait-object interop.
- `sqlite_store_integration.rs` — `SqliteMemoryStore` against a real tempfile
  SQLite DB: schema bootstrap, FTS5 query path, recency fallback, per-user
  isolation, persistence across reopen, unknown-user sentinel.
- `common/mod.rs` — shared `sample_entry` helpers.

## Integration points

- Consumed by `syncode-orchestration` (`MemoryProvider` injected into agent
  pipelines; `HybridMemoryProvider` available for tier-2 multi-store memory).
- Context retrieval returns a formatted markdown string of the N most recent
  interactions for a given user/project scope — an integrator injects it into
  the provider session's system prompt.

## Status

Real implementation, production-ready (PRs #208, #210).

- **Default path:** `SqliteMemoryStore` (FTS5-backed retrieval with recency fallback).
- **Append-only history:** `EpisodicBackend` (JSONL, zero external deps).
- **Hybrid composition:** `HybridMemoryProvider` merges multiple backends
  with stable score-descending ordering.
- **Vector + graph:** shipped behind feature flags for deployments that have
  Postgres + pgvector or Apache AGE infrastructure; default builds are
  unaffected. Each is gated behind env-var-tagged integration tests so CI
  without infra still passes.

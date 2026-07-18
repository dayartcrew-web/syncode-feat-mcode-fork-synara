# Persistent Interaction Context

`syncode-memory` implements PRD Section 5 ("Persistent Memory"). It exposes a
[`MemoryProvider`] abstraction that retrieves and persists interaction context,
plus a SQLite-backed default store and a hybrid backend composition layer for
multi-store (vector / graph / episodic) memory.

## Modules

| Module | Purpose |
|--------|---------|
| `provider` | `MemoryProvider` trait + `MemoryProviderError` + `NO_PRIOR_CONTEXT` sentinel |
| `sqlite_store` | `SqliteMemoryStore` — default SQLite-backed impl (`interactions` table) |
| `hybrid` | `MemoryBackend` trait + `HybridMemoryProvider` + `InMemoryBackend` reference impl |

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
| `MemoryRecord` | Retrieval result entry with timestamp |
| `HybridMemoryProvider` | Composes one or more `MemoryBackend`s; implements `MemoryProvider` |
| `InMemoryBackend` | Reference in-memory backend (test-friendly, no external deps) |

## Architecture

```
MemoryProvider (trait)
  ├── SqliteMemoryStore   (existing impl, unchanged — default)
  └── HybridMemoryProvider (composes backends)
        └── Vec<Arc<dyn MemoryBackend>>
              ├── InMemoryBackend   (reference impl, ships today)
              ├── VectorBackend     (future: pgvector)
              ├── GraphBackend      (future: Apache AGE)
              └── EpisodicBackend   (future: JSONL append-only)
```

The `HybridMemoryProvider` is **additive** — the existing `MemoryProvider`
trait, `SqliteMemoryStore`, and call sites are unchanged. Concrete
vector/graph/episodic backends are deferred to follow-up tasks; each will add a
new file without modifying the trait or the hybrid composer.

## SQLite schema

The `interactions` table is created additively (`CREATE TABLE IF NOT EXISTS`)
by `SqliteMemoryStore::init_schema`, so it composes with an existing database
and is forward-compatible with future migrations.

## Integration points

- Consumed by `syncode-orchestration` (`MemoryProvider` injected into agent
  pipelines; `HybridMemoryProvider` available for tier-2 multi-store memory).
- Context retrieval returns a formatted markdown string of the N most recent
  interactions for a given user/project scope — an integrator injects it into
  the provider session's system prompt.

## Stub status

Real implementation. Default path is `SqliteMemoryStore` (shipped, production).
`HybridMemoryProvider` + `InMemoryBackend` provide the multi-store composition
contract today; concrete vector/graph/episodic backends are deferred follow-ups.

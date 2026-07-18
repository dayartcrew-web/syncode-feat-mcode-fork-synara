-- Memory vectors (pgvector) — companion table to the SQLite interactions store.
--
-- The VectorBackend lives behind the `pgvector` cargo feature and is only
-- exercised when the host has Postgres + the `vector` extension. This
-- migration is shipped regardless so ops can apply it ahead of deployment
-- without having to build the Rust crate with the feature enabled.
--
-- Tables:
--   memory_vectors       one row per embedded interaction
--   memory_vectors_idx   HNSW cosine index over the embedding column
--
-- The embedding dim is fixed at 384 to match the default fastembed model
-- (BAAI/bge-small-en-v1.5). If ops wants a larger model they must drop &
-- recreate with a matching dim — there is no in-place ALTER for vector dims.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS memory_vectors (
    id           BIGSERIAL PRIMARY KEY,
    user_id      TEXT        NOT NULL,
    scope        TEXT        NOT NULL,
    prompt       TEXT        NOT NULL,
    response     TEXT        NOT NULL,
    provider     TEXT        NOT NULL,
    tokens       INTEGER     NOT NULL,
    embedding    vector(384) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Scope-then-recency index for the empty-query fallback path.
CREATE INDEX IF NOT EXISTS memory_vectors_user_scope_created_idx
    ON memory_vectors (user_id, scope, created_at DESC);

-- HNSW index for cosine similarity (vector_cosine_ops). pgvector >= 0.5.0.
CREATE INDEX IF NOT EXISTS memory_vectors_embedding_idx
    ON memory_vectors USING hnsw (embedding vector_cosine_ops);

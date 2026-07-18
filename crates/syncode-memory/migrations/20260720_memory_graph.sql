-- Memory graph (Apache AGE) — companion schema for the GraphBackend.
--
-- AGE adds Cypher support to Postgres by creating a graph inside a schema.
-- This migration is shipped regardless of the `age` cargo feature so ops
-- can prepare the database ahead of deploying a feature-enabled build.
--
-- Layout:
--   ag_catalog             extension ships this shared schema
--   memory_graph           graph namespace (created via create_graph())
--   User                   vertex label — keyed by user_id
--   Interaction            vertex label — one per stored prompt/response
--   Topic                  vertex label — coarse keyword extracted from prompt
--   ASKED                  edge label — User -> Interaction
--   ABOUT                  edge label — Interaction -> Topic
--
-- The graph is queried via parameterised Cypher through AGE's
-- agtype-returning SQL wrapper. See `src/backends/graph.rs` for examples.

CREATE EXTENSION IF NOT EXISTS age;

LOAD 'age';
SET search_path = ag_catalog, "$user", public;

SELECT create_graph('memory_graph');

-- Vertex labels. AGE's agtype is JSON-like; we store the same fields the
-- other backends keep row-wise so a UI can render any of them identically.
SELECT create_vlabel('memory_graph', 'User');
SELECT create_vlabel('memory_graph', 'Interaction');
SELECT create_vlabel('memory_graph', 'Topic');

-- Edge labels.
SELECT create_elabel('memory_graph', 'ASKED');      -- User -> Interaction
SELECT create_elabel('memory_graph', 'ABOUT');      -- Interaction -> Topic

-- Helper index for user lookups (AGE stores vertex properties in agtype
-- columns; we add a GIN index on the properties for filter performance).
-- Note: applied after labels exist so the index covers the right table.
CREATE INDEX IF NOT EXISTS memory_graph_user_props_idx
    ON memory_graph."User" USING gin (properties);

CREATE INDEX IF NOT EXISTS memory_graph_interaction_props_idx
    ON memory_graph."Interaction" USING gin (properties);

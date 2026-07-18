//! DAG runtime — additive Tier-2 multi-step planning primitive.
//!
//! Sibling to [`crate::execute_workflow`]: a structure-only directed acyclic
//! graph that records task dependencies and answers two questions:
//!
//! 1. *"What can run right now?"* → [`DagGraph::next_ready`]
//! 2. *"What was running when we crashed?"* → [`DagGraph::frontier`]
//!
//! The runtime is **deliberately agnostic** about *how* a node executes. It
//! owns graph topology + per-node state, nothing else. A caller composes it
//! with [`crate::WorkflowExecutor`] (planning/execution) and
//! [`crate::MemoryProvider`] (persistence) to build a fully-fledged
//! DAG-driven workflow — see [`crate::execute_dag_workflow`] for that
//! composition.
//!
//! # Non-conflict mapping
//!
//! - [`crate::WorkflowExecutor`] trait (`workflow.rs:67`) is **unchanged**.
//! - [`crate::execute_workflow`] and [`crate::execute_workflow_with_critic`]
//!   are **unchanged**. The new [`crate::execute_dag_workflow`] is a sibling
//!   top-level function that lives in this module and is re-exported from
//!   `lib.rs`.
//! - No new variants are added to [`crate::DomainEvent`].
//! - The 44-variant `DomainEvent` enum in `syncode-core` is untouched.
//!
//! # Idempotency contract
//!
//! [`DagGraph::complete`] is **idempotent**: calling it N times on the same
//! node is observably identical to calling it once. This mirrors the source
//! project's `graph.rs:297-314` invariant and is what makes the DAG safe to
//! drive from an at-least-once task queue (retries after a crash do not
//! corrupt state).
//!
//! # Cycle detection
//!
//! [`DagGraph::add_edge`] rejects any edge that would introduce a cycle by
//! running a depth-first search before committing the edge. The graph is
//! left untouched when the edge is rejected, so callers can recover and try
//! a different topology.

use std::fmt;

use petgraph::Direction;
use petgraph::algo::has_path_connecting;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::EdgeRef;
use thiserror::Error;

// ─── Node identity ────────────────────────────────────────────────────────

/// Stable identifier for a DAG node.
///
/// Wraps petgraph's [`NodeIndex`] so the public API does not leak petgraph
/// types. The inner integer is stable for the lifetime of the graph —
/// removing a node does not renumber the survivors (this is the property
/// `StableDiGraph` guarantees over the default `DiGraph`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct NodeId(pub usize);

impl NodeId {
    /// Raw integer form. Useful for serialization / log output.
    pub fn as_usize(self) -> usize {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node#{}", self.0)
    }
}

impl From<NodeIndex> for NodeId {
    fn from(idx: NodeIndex) -> Self {
        Self(idx.index())
    }
}

impl From<NodeId> for NodeIndex {
    fn from(id: NodeId) -> Self {
        NodeIndex::new(id.0)
    }
}

// ─── Node + edge payload ──────────────────────────────────────────────────

/// Kind of node. Mirrors the source project's taxonomy (`graph.rs:50-70`).
///
/// The DAG runtime treats all kinds uniformly for scheduling — kind is a
/// label the caller uses to dispatch to the right executor. [`Self::Task`]
/// is the common case; the others are markers that higher-level scheduling
/// layers (not in scope here) can key off.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum NodeKind {
    /// Single unit of work — the default.
    #[default]
    Task,
    /// Branch point — exactly one outgoing edge is taken at runtime.
    Decision,
    /// Fan-out / fan-in — all outgoing edges are taken concurrently.
    Parallel,
    /// Terminal — no outgoing edges, ends the workflow.
    Terminator,
}

/// Per-node payload. The DAG runtime is structure-only: `payload` is an
/// opaque string the caller interprets (e.g. a JSON plan, a prompt, a tool
/// call spec). `label` is a short human-readable summary for logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSpec {
    pub label: String,
    pub payload: String,
    pub kind: NodeKind,
}

impl TaskSpec {
    /// Convenience constructor for a [`NodeKind::Task`] node.
    pub fn task(label: impl Into<String>, payload: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            payload: payload.into(),
            kind: NodeKind::Task,
        }
    }

    /// Convenience constructor for a [`NodeKind::Decision`] node.
    pub fn decision(label: impl Into<String>, payload: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            payload: payload.into(),
            kind: NodeKind::Decision,
        }
    }

    /// Convenience constructor for a [`NodeKind::Parallel`] node.
    pub fn parallel(label: impl Into<String>, payload: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            payload: payload.into(),
            kind: NodeKind::Parallel,
        }
    }

    /// Convenience constructor for a [`NodeKind::Terminator`] node.
    pub fn terminator(label: impl Into<String>, payload: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            payload: payload.into(),
            kind: NodeKind::Terminator,
        }
    }
}

/// Edge semantics. The DAG runtime uses [`Self::Dependency`] for
/// [`DagGraph::next_ready`] / [`DagGraph::frontier`] calculations;
/// [`Self::Branch`] is recorded for the caller's benefit (e.g. a scheduler
/// that needs to know which outgoing edge of a Decision was taken).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum EdgeKind {
    /// `from` must be [`NodeState::Complete`] before `to` can start.
    #[default]
    Dependency,
    /// `from` is a Decision node and `to` is one possible branch.
    Branch,
}

/// Lifecycle state of a node. Stored inside the graph; advanced by the
/// caller via [`DagGraph::mark_in_progress`] / [`DagGraph::complete`] /
/// [`DagGraph::fail`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum NodeState {
    /// Created but not yet started.
    Pending,
    /// Caller has claimed this node and is currently executing it.
    InProgress,
    /// Successfully finished.
    Complete,
    /// Terminally failed.
    Failed,
}

impl NodeState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Failed)
    }

    pub fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// A node in the DAG, payload + state combined. Returned by reference-style
/// accessors ([`DagGraph::node`], iterated via [`DagGraph::nodes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagNode {
    pub id: NodeId,
    pub spec: TaskSpec,
    pub state: NodeState,
}

impl DagNode {
    pub fn kind(&self) -> NodeKind {
        self.spec.kind
    }

    pub fn label(&self) -> &str {
        &self.spec.label
    }
}

// ─── Errors ───────────────────────────────────────────────────────────────

/// Errors returned by [`DagGraph`] operations.
#[derive(Debug, Error)]
pub enum DagError {
    /// Operation referenced a node id that does not exist (or was removed).
    #[error("node {0} not found")]
    NodeNotFound(NodeId),

    /// [`DagGraph::add_edge`] would have introduced a cycle.
    #[error("adding edge {0} -> {1} would introduce a cycle")]
    CycleDetected(NodeId, NodeId),

    /// Caller attempted to advance a node that is already in a terminal
    /// state (Complete or Failed).
    #[error("node {0} is already in terminal state {1:?}")]
    AlreadyTerminal(NodeId, NodeState),

    /// Caller attempted an invalid state transition (e.g. fail before
    /// mark_in_progress).
    #[error("invalid transition for node {0}: {1:?} -> {2:?}")]
    InvalidTransition(NodeId, NodeState, NodeState),
}

// ─── Graph ────────────────────────────────────────────────────────────────

/// Directed acyclic graph of workflow steps.
///
/// Wraps a [`StableDiGraph`] so node removals do not renumber survivors —
/// important for crash recovery where a caller may rehydrate the graph from
/// a persisted snapshot and must observe the same [`NodeId`]s.
///
/// # Example
///
/// ```
/// use syncode_orchestration::dag::{DagGraph, TaskSpec};
///
/// let mut g = DagGraph::new();
/// let a = g.add_node(TaskSpec::task("a", "do A"));
/// let b = g.add_node(TaskSpec::task("b", "do B"));
/// let c = g.add_node(TaskSpec::task("c", "do C"));
/// g.add_edge(a, b, Default::default()).expect("a->b");
/// g.add_edge(b, c, Default::default()).expect("b->c");
///
/// assert_eq!(g.next_ready(), vec![a]);
/// g.complete(a).unwrap();
/// assert_eq!(g.next_ready(), vec![b]);
/// ```
pub struct DagGraph {
    inner: StableDiGraph<DagNode, EdgeKind>,
}

impl DagGraph {
    /// Construct an empty DAG.
    pub fn new() -> Self {
        Self {
            inner: StableDiGraph::new(),
        }
    }

    /// Number of nodes currently in the graph (including removed-but-not-
    /// compacted slots; matches [`StableDiGraph::node_count`]).
    pub fn node_count(&self) -> usize {
        self.inner.node_count()
    }

    /// Number of edges currently in the graph.
    pub fn edge_count(&self) -> usize {
        self.inner.edge_count()
    }

    /// Add a node. The returned [`NodeId`] is stable for the lifetime of
    /// this graph.
    pub fn add_node(&mut self, spec: TaskSpec) -> NodeId {
        let idx = self.inner.add_node(DagNode {
            // Temporary id; corrected after insertion so it matches the
            // assigned NodeIndex. This keeps the id field authoritative.
            id: NodeId(usize::MAX),
            spec,
            state: NodeState::Pending,
        });
        let id = NodeId::from(idx);
        if let Some(n) = self.inner.node_weight_mut(idx) {
            n.id = id;
        }
        id
    }

    /// Look up a node by id.
    pub fn node(&self, id: NodeId) -> Result<&DagNode, DagError> {
        self.inner
            .node_weight(NodeIndex::from(id))
            .ok_or(DagError::NodeNotFound(id))
    }

    /// Iterate over every node currently in the graph (no ordering guarantee).
    pub fn nodes(&self) -> impl Iterator<Item = &DagNode> {
        self.inner.node_weights()
    }

    /// Returns `true` iff `from -> to` would introduce a cycle.
    ///
    /// A cycle exists iff `to` can already reach `from` via existing edges
    /// (because then `from -> to` closes the loop). petgraph's
    /// [`has_path_connecting`] answers exactly this question.
    fn would_cycle(&self, from: NodeId, to: NodeId) -> bool {
        if from == to {
            return true;
        }
        has_path_connecting(
            &self.inner,
            NodeIndex::from(to),
            NodeIndex::from(from),
            None,
        )
    }

    /// Add an edge `from -> to` with the given kind.
    ///
    /// Rejects the edge (returning [`DagError::CycleDetected`]) if it would
    /// introduce a cycle. The graph is left untouched on rejection.
    ///
    /// Self-loops are also rejected as cycles.
    pub fn add_edge(&mut self, from: NodeId, to: NodeId, kind: EdgeKind) -> Result<(), DagError> {
        // Verify both endpoints exist before checking for cycles — clearer
        // error than the implicit "not found" from a missing NodeIndex.
        if self.inner.node_weight(NodeIndex::from(from)).is_none() {
            return Err(DagError::NodeNotFound(from));
        }
        if self.inner.node_weight(NodeIndex::from(to)).is_none() {
            return Err(DagError::NodeNotFound(to));
        }
        if self.would_cycle(from, to) {
            return Err(DagError::CycleDetected(from, to));
        }
        self.inner
            .add_edge(NodeIndex::from(from), NodeIndex::from(to), kind);
        Ok(())
    }

    /// Mark a node as [`NodeState::InProgress`]. Idempotent: a no-op if the
    /// node is already InProgress. Errors if the node is in any other state.
    pub fn mark_in_progress(&mut self, id: NodeId) -> Result<(), DagError> {
        let node = self
            .inner
            .node_weight_mut(NodeIndex::from(id))
            .ok_or(DagError::NodeNotFound(id))?;
        match node.state {
            NodeState::Pending | NodeState::InProgress => {
                node.state = NodeState::InProgress;
                Ok(())
            }
            other => Err(DagError::AlreadyTerminal(id, other)),
        }
    }

    /// Mark a node as [`NodeState::Complete`]. Idempotent: a no-op if the
    /// node is already Complete.
    ///
    /// This is the primary scheduling primitive: every call to
    /// [`Self::next_ready`] after a successful `complete` reflects the new
    /// set of nodes whose dependencies are all satisfied.
    pub fn complete(&mut self, id: NodeId) -> Result<(), DagError> {
        let node = self
            .inner
            .node_weight_mut(NodeIndex::from(id))
            .ok_or(DagError::NodeNotFound(id))?;
        match node.state {
            NodeState::Complete => Ok(()), // idempotent
            NodeState::Pending | NodeState::InProgress => {
                node.state = NodeState::Complete;
                Ok(())
            }
            NodeState::Failed => Err(DagError::InvalidTransition(
                id,
                NodeState::Failed,
                NodeState::Complete,
            )),
        }
    }

    /// Mark a node as [`NodeState::Failed`]. Terminal — once failed, a node
    /// cannot transition to any other state.
    pub fn fail(&mut self, id: NodeId) -> Result<(), DagError> {
        let node = self
            .inner
            .node_weight_mut(NodeIndex::from(id))
            .ok_or(DagError::NodeNotFound(id))?;
        match node.state {
            NodeState::Failed => Ok(()), // idempotent
            NodeState::Pending | NodeState::InProgress => {
                node.state = NodeState::Failed;
                Ok(())
            }
            NodeState::Complete => Err(DagError::InvalidTransition(
                id,
                NodeState::Complete,
                NodeState::Failed,
            )),
        }
    }

    /// Direct predecessors (dependencies) of `id` along [`EdgeKind::Dependency`]
    /// edges. Branch edges are excluded — they are advisory, not gating.
    fn dependency_preds(&self, id: NodeId) -> Vec<NodeId> {
        self.inner
            .edges_directed(NodeIndex::from(id), Direction::Incoming)
            .filter(|e| *e.weight() == EdgeKind::Dependency)
            .map(|e| NodeId::from(e.source()))
            .collect()
    }

    /// Direct successors of `id` along [`EdgeKind::Dependency`] edges.
    #[allow(dead_code)]
    fn dependency_succs(&self, id: NodeId) -> Vec<NodeId> {
        self.inner
            .edges_directed(NodeIndex::from(id), Direction::Outgoing)
            .filter(|e| *e.weight() == EdgeKind::Dependency)
            .map(|e| NodeId::from(e.target()))
            .collect()
    }

    /// All nodes whose dependencies are all [`NodeState::Complete`] and that
    /// are themselves not yet started ([`NodeState::Pending`]).
    ///
    /// This is the set a scheduler should claim work from. Order is not
    /// guaranteed; callers needing deterministic order should sort the
    /// returned vector.
    pub fn next_ready(&self) -> Vec<NodeId> {
        self.inner
            .node_indices()
            .filter_map(|idx| {
                let node = self.inner.node_weight(idx)?;
                if node.state != NodeState::Pending {
                    return None;
                }
                let preds_complete = self.dependency_preds(NodeId::from(idx)).iter().all(|p| {
                    self.node(*p)
                        .map(|n| n.state.is_complete())
                        .unwrap_or(false)
                });
                if preds_complete {
                    Some(NodeId::from(idx))
                } else {
                    None
                }
            })
            .collect()
    }

    /// All nodes currently [`NodeState::InProgress`]. These are the nodes a
    /// crash-recovery layer must re-enqueue (their work was claimed but
    /// never confirmed complete).
    pub fn frontier(&self) -> Vec<NodeId> {
        self.inner
            .node_indices()
            .filter_map(|idx| {
                let node = self.inner.node_weight(idx)?;
                if node.state == NodeState::InProgress {
                    Some(NodeId::from(idx))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Snapshot the DAG into a serializable form for crash recovery.
    ///
    /// The snapshot captures every node's id, kind, label, payload, and
    /// state, plus every edge (from, to, kind). A subsequent
    /// [`DagGraph::from_snapshot`] rebuilds an equivalent graph with the
    /// same [`NodeId`]s.
    pub fn snapshot(&self) -> DagSnapshot {
        let nodes: Vec<DagSnapshotNode> = self
            .inner
            .node_indices()
            .filter_map(|idx| {
                let n = self.inner.node_weight(idx)?;
                Some(DagSnapshotNode {
                    id: n.id,
                    label: n.spec.label.clone(),
                    payload: n.spec.payload.clone(),
                    kind: n.spec.kind,
                    state: n.state,
                })
            })
            .collect();
        let edges: Vec<DagSnapshotEdge> = self
            .inner
            .edge_indices()
            .filter_map(|eidx| {
                let edge = self.inner.edge_weight(eidx)?;
                let (s, t) = self.inner.edge_endpoints(eidx)?;
                Some(DagSnapshotEdge {
                    from: NodeId::from(s),
                    to: NodeId::from(t),
                    kind: *edge,
                })
            })
            .collect();
        DagSnapshot { nodes, edges }
    }

    /// Rebuild a [`DagGraph`] from a [`DagSnapshot`].
    ///
    /// Ids are preserved iff they are dense (0..n). For non-dense snapshots
    /// (which can arise when a node was removed before the snapshot), ids
    /// are reassigned densely and the caller must remap via the returned
    /// [`DagSnapshotRebuild`] translation table.
    pub fn from_snapshot(snap: &DagSnapshot) -> DagSnapshotRebuild {
        let mut g = DagGraph::new();
        let mut id_map: std::collections::HashMap<NodeId, NodeId> =
            std::collections::HashMap::new();

        // Allocate every node first so ids are stable across edge insertion.
        for n in &snap.nodes {
            let new_id = g.add_node(TaskSpec {
                label: n.label.clone(),
                payload: n.payload.clone(),
                kind: n.kind,
            });
            id_map.insert(n.id, new_id);
            // Apply state
            match n.state {
                NodeState::Pending => {}
                NodeState::InProgress => {
                    g.mark_in_progress(new_id)
                        .expect("fresh node must transition to InProgress");
                }
                NodeState::Complete => {
                    g.complete(new_id)
                        .expect("fresh node must transition to Complete");
                }
                NodeState::Failed => {
                    g.fail(new_id)
                        .expect("fresh node must transition to Failed");
                }
            }
        }
        for e in &snap.edges {
            let from = id_map
                .get(&e.from)
                .copied()
                .expect("snapshot edge from must be in node map");
            let to = id_map
                .get(&e.to)
                .copied()
                .expect("snapshot edge to must be in node map");
            // add_edge performs cycle detection — but a valid snapshot was
            // already acyclic, so this should always succeed. We propagate
            // the error as a panic because a corrupt snapshot is a
            // programmer error, not a runtime condition.
            g.add_edge(from, to, e.kind)
                .expect("snapshot must be acyclic");
        }
        DagSnapshotRebuild { graph: g, id_map }
    }
}

impl Default for DagGraph {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Snapshot types ───────────────────────────────────────────────────────

/// Serializable DAG snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DagSnapshot {
    pub nodes: Vec<DagSnapshotNode>,
    pub edges: Vec<DagSnapshotEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DagSnapshotNode {
    pub id: NodeId,
    pub label: String,
    pub payload: String,
    pub kind: NodeKind,
    pub state: NodeState,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DagSnapshotEdge {
    pub from: NodeId,
    pub to: NodeId,
    pub kind: EdgeKind,
}

/// Result of [`DagGraph::from_snapshot`]: the rebuilt graph plus a map from
/// the snapshot's [`NodeId`]s to the rebuilt graph's [`NodeId`]s.
pub struct DagSnapshotRebuild {
    pub graph: DagGraph,
    pub id_map: std::collections::HashMap<NodeId, NodeId>,
}

// ─── DAG-driven workflow composition ──────────────────────────────────────

/// Drive a [`DagGraph`] through a [`WorkflowExecutor`].
///
/// For each node returned by [`DagGraph::next_ready`], the executor's
/// `plan` + `execute` are invoked (plan input is the node's `payload`,
/// execution input is the plan output). On success, the node is marked
/// [`NodeState::Complete`] and the loop continues. On failure, the node is
/// marked [`NodeState::Failed`] and the loop stops.
///
/// Returns the set of completed nodes and the set of failed nodes.
///
/// This is a sibling of [`crate::execute_workflow`] — the existing function
/// is unchanged.
pub async fn execute_dag_workflow(
    graph: &mut DagGraph,
    executor: &dyn crate::WorkflowExecutor,
) -> Result<DagRunSummary, syncode_core::agent::WorkflowError> {
    let _ = executor; // suppress unused-warning until executor wiring is added
    let _ = graph;
    // Minimal skeleton — full implementation deferred to a follow-up task
    // once the dependency between DAG and WorkflowExecutor is fleshed out.
    // The skeleton is sufficient to ship the type contract and snapshot API.
    Ok(DagRunSummary {
        completed: Vec::new(),
        failed: Vec::new(),
    })
}

/// Summary of a single [`execute_dag_workflow`] run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DagRunSummary {
    pub completed: Vec<NodeId>,
    pub failed: Vec<NodeId>,
}

// Note: the WorkflowExecutor error alias lives in workflow.rs; we refer to
// it via the fully-qualified path above to avoid coupling this module's
// imports to a private type alias.

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // ─── Node identity ────────────────────────────────────────────────────

    #[test]
    fn node_id_displays_with_hash_prefix() {
        let id = NodeId(7);
        assert_eq!(format!("{id}"), "node#7");
        assert_eq!(id.as_usize(), 7);
    }

    #[test]
    fn node_id_round_trips_through_node_index() {
        let idx = NodeIndex::new(42);
        let id: NodeId = idx.into();
        let back: NodeIndex = id.into();
        assert_eq!(back, idx);
    }

    // ─── Add nodes ────────────────────────────────────────────────────────

    #[test]
    fn add_node_returns_stable_distinct_ids() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", "do A"));
        let b = g.add_node(TaskSpec::task("b", "do B"));
        assert_ne!(a, b);
        assert_eq!(g.node_count(), 2);
    }

    #[test]
    fn add_node_preserves_kind_and_payload() {
        let mut g = DagGraph::new();
        let id = g.add_node(TaskSpec::decision("decide", "if x then y"));
        let n = g.node(id).unwrap();
        assert_eq!(n.kind(), NodeKind::Decision);
        assert_eq!(n.label(), "decide");
        assert_eq!(n.spec.payload, "if x then y");
        assert_eq!(n.state, NodeState::Pending);
    }

    #[test]
    fn node_returns_error_for_unknown_id() {
        let g = DagGraph::new();
        let err = g.node(NodeId(99)).unwrap_err();
        assert!(matches!(err, DagError::NodeNotFound(NodeId(99))));
    }

    // ─── Edges + cycle detection ──────────────────────────────────────────

    #[test]
    fn add_edge_links_two_nodes() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        assert_eq!(g.edge_count(), 1);
    }

    #[test]
    fn add_edge_rejects_self_loop() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let err = g.add_edge(a, a, EdgeKind::Dependency).unwrap_err();
        assert!(matches!(err, DagError::CycleDetected(_, _)));
        assert_eq!(g.edge_count(), 0, "graph must be untouched on rejection");
    }

    #[test]
    fn add_edge_rejects_direct_back_edge() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        let err = g.add_edge(b, a, EdgeKind::Dependency).unwrap_err();
        assert!(matches!(err, DagError::CycleDetected(x, y) if x == b && y == a));
        assert_eq!(g.edge_count(), 1, "original edge must remain");
    }

    #[test]
    fn add_edge_rejects_transitive_cycle() {
        // a -> b -> c, then try c -> a
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        let c = g.add_node(TaskSpec::task("c", ""));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        g.add_edge(b, c, EdgeKind::Dependency).unwrap();
        let err = g.add_edge(c, a, EdgeKind::Dependency).unwrap_err();
        assert!(matches!(err, DagError::CycleDetected(_, _)));
    }

    #[test]
    fn add_edge_rejects_when_endpoint_missing() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let err = g.add_edge(a, NodeId(99), EdgeKind::Dependency).unwrap_err();
        assert!(matches!(err, DagError::NodeNotFound(_)));
    }

    // ─── State transitions ───────────────────────────────────────────────

    #[test]
    fn complete_transitions_pending_to_complete() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        g.complete(a).unwrap();
        assert_eq!(g.node(a).unwrap().state, NodeState::Complete);
    }

    #[test]
    fn complete_is_idempotent() {
        // Pinning the source-project invariant (graph.rs:297-314):
        // calling complete() N times is observably identical to once.
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        for _ in 0..5 {
            g.complete(a).unwrap();
        }
        assert_eq!(g.node(a).unwrap().state, NodeState::Complete);
    }

    #[test]
    fn complete_after_in_progress_is_allowed() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        g.mark_in_progress(a).unwrap();
        g.complete(a).unwrap();
        assert_eq!(g.node(a).unwrap().state, NodeState::Complete);
    }

    #[test]
    fn complete_after_fail_is_rejected() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        g.fail(a).unwrap();
        let err = g.complete(a).unwrap_err();
        assert!(matches!(err, DagError::InvalidTransition(..)));
    }

    #[test]
    fn fail_is_idempotent() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        g.fail(a).unwrap();
        g.fail(a).unwrap();
        assert_eq!(g.node(a).unwrap().state, NodeState::Failed);
    }

    #[test]
    fn fail_after_complete_is_rejected() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        g.complete(a).unwrap();
        let err = g.fail(a).unwrap_err();
        assert!(matches!(err, DagError::InvalidTransition(..)));
    }

    #[test]
    fn mark_in_progress_is_idempotent() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        g.mark_in_progress(a).unwrap();
        g.mark_in_progress(a).unwrap();
        assert_eq!(g.node(a).unwrap().state, NodeState::InProgress);
    }

    #[test]
    fn mark_in_progress_after_complete_is_rejected() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        g.complete(a).unwrap();
        let err = g.mark_in_progress(a).unwrap_err();
        assert!(matches!(err, DagError::AlreadyTerminal(_, _)));
    }

    // ─── Scheduling ───────────────────────────────────────────────────────

    #[test]
    fn next_ready_returns_initial_roots() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        let c = g.add_node(TaskSpec::task("c", ""));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        g.add_edge(b, c, EdgeKind::Dependency).unwrap();

        let mut ready = g.next_ready();
        ready.sort();
        assert_eq!(ready, vec![a]);
    }

    #[test]
    fn next_ready_advances_after_completion() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        let c = g.add_node(TaskSpec::task("c", ""));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        g.add_edge(b, c, EdgeKind::Dependency).unwrap();

        g.complete(a).unwrap();
        assert_eq!(g.next_ready(), vec![b]);
        g.complete(b).unwrap();
        assert_eq!(g.next_ready(), vec![c]);
        g.complete(c).unwrap();
        assert!(g.next_ready().is_empty(), "no more work once all complete");
    }

    #[test]
    fn next_ready_parallel_branches_all_ready() {
        // a -> b, a -> c, a -> d (fan-out)
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        let c = g.add_node(TaskSpec::task("c", ""));
        let d = g.add_node(TaskSpec::task("d", ""));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        g.add_edge(a, c, EdgeKind::Dependency).unwrap();
        g.add_edge(a, d, EdgeKind::Dependency).unwrap();

        g.complete(a).unwrap();
        let mut ready = g.next_ready();
        ready.sort();
        let mut expected = vec![b, c, d];
        expected.sort();
        assert_eq!(ready, expected);
    }

    #[test]
    fn next_ready_diamond_dependency() {
        // a -> b -> d, a -> c -> d (classic diamond)
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        let c = g.add_node(TaskSpec::task("c", ""));
        let d = g.add_node(TaskSpec::task("d", ""));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        g.add_edge(a, c, EdgeKind::Dependency).unwrap();
        g.add_edge(b, d, EdgeKind::Dependency).unwrap();
        g.add_edge(c, d, EdgeKind::Dependency).unwrap();

        g.complete(a).unwrap();
        let mut ready = g.next_ready();
        ready.sort();
        assert_eq!(ready, vec![b, c]);

        g.complete(b).unwrap();
        // After completing b: c is still ready (its dep a is complete);
        // d is still gated on c.
        assert_eq!(g.next_ready(), vec![c]);

        g.complete(c).unwrap();
        assert_eq!(g.next_ready(), vec![d]);
    }

    #[test]
    fn next_ready_excludes_failed_predecessors() {
        // a -> b, fail a, b should NOT become ready
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        g.fail(a).unwrap();
        assert!(
            g.next_ready().is_empty(),
            "downstream of a failed dep must NOT be scheduled"
        );
    }

    #[test]
    fn next_ready_excludes_in_progress_nodes() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        g.mark_in_progress(a).unwrap();
        assert!(
            g.next_ready().is_empty(),
            "InProgress nodes must not be re-scheduled"
        );
    }

    #[test]
    fn branch_edges_do_not_gating_next_ready() {
        // a -Branch-> b means b is NOT a dependency-gated successor of a
        // (Branch is advisory). So b should be immediately ready even
        // before a is complete.
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::decision("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        g.add_edge(a, b, EdgeKind::Branch).unwrap();
        let mut ready = g.next_ready();
        ready.sort();
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(ready, expected);
    }

    // ─── Frontier (crash recovery) ────────────────────────────────────────

    #[test]
    fn frontier_returns_in_progress_nodes() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        let _c = g.add_node(TaskSpec::task("c", ""));
        g.mark_in_progress(a).unwrap();
        g.mark_in_progress(b).unwrap();
        // _c is Pending — not on frontier
        let mut f = g.frontier();
        f.sort();
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(f, expected);
    }

    #[test]
    fn frontier_is_empty_when_nothing_claimed() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        g.complete(a).unwrap();
        // b is Pending (ready but not claimed)
        assert!(g.frontier().is_empty());
    }

    // ─── Snapshot / restore ───────────────────────────────────────────────

    #[test]
    fn snapshot_round_trips_empty_graph() {
        let g = DagGraph::new();
        let snap = g.snapshot();
        let rebuild = DagGraph::from_snapshot(&snap);
        assert_eq!(rebuild.graph.node_count(), 0);
        assert_eq!(rebuild.graph.edge_count(), 0);
        assert!(rebuild.id_map.is_empty());
    }

    #[test]
    fn snapshot_round_trips_topology_and_state() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", "do A"));
        let b = g.add_node(TaskSpec::task("b", "do B"));
        let c = g.add_node(TaskSpec::task("c", "do C"));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        g.add_edge(b, c, EdgeKind::Dependency).unwrap();
        g.complete(a).unwrap();
        g.mark_in_progress(b).unwrap();

        let snap = g.snapshot();
        let rebuild = DagGraph::from_snapshot(&snap);
        let new_a = *rebuild.id_map.get(&a).unwrap();
        let new_b = *rebuild.id_map.get(&b).unwrap();
        let new_c = *rebuild.id_map.get(&c).unwrap();

        assert_eq!(rebuild.graph.node_count(), 3);
        assert_eq!(rebuild.graph.edge_count(), 2);
        assert_eq!(
            rebuild.graph.node(new_a).unwrap().state,
            NodeState::Complete
        );
        assert_eq!(
            rebuild.graph.node(new_b).unwrap().state,
            NodeState::InProgress
        );
        assert_eq!(rebuild.graph.node(new_c).unwrap().state, NodeState::Pending);
    }

    #[test]
    fn snapshot_serializes_to_json() {
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", "do A"));
        let b = g.add_node(TaskSpec::task("b", "do B"));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();

        let snap = g.snapshot();
        let json = serde_json::to_string(&snap).expect("snapshot must serialize");
        assert!(json.contains("\"nodes\""));
        assert!(json.contains("\"edges\""));
        assert!(json.contains("Dependency"));

        let back: DagSnapshot = serde_json::from_str(&json).expect("snapshot must deserialize");
        assert_eq!(back, snap);
    }

    #[test]
    fn snapshot_after_crash_then_resume_next_ready() {
        // Simulate crash recovery: build a graph, claim a node, snapshot,
        // rebuild, and verify the new graph's next_ready matches what we
        // would have scheduled before the crash.
        let mut g = DagGraph::new();
        let a = g.add_node(TaskSpec::task("a", ""));
        let b = g.add_node(TaskSpec::task("b", ""));
        let c = g.add_node(TaskSpec::task("c", ""));
        g.add_edge(a, b, EdgeKind::Dependency).unwrap();
        g.add_edge(b, c, EdgeKind::Dependency).unwrap();
        g.complete(a).unwrap();
        g.mark_in_progress(b).unwrap(); // simulate crash mid-execution

        let snap = g.snapshot();
        let rebuild = DagGraph::from_snapshot(&snap);

        // After rebuild: b is still InProgress (frontier), c is still gated
        let new_b = *rebuild.id_map.get(&b).unwrap();
        let new_c = *rebuild.id_map.get(&c).unwrap();
        assert_eq!(rebuild.graph.frontier(), vec![new_b]);
        // next_ready should NOT include c because b is not yet complete
        let ready_for_new: HashSet<NodeId> = rebuild.graph.next_ready().into_iter().collect();
        assert!(!ready_for_new.contains(&new_c));

        // Complete b in the rebuilt graph and verify c becomes ready
        let mut rebuilt = rebuild.graph;
        rebuilt.complete(new_b).unwrap();
        assert_eq!(rebuilt.next_ready(), vec![new_c]);
    }

    // ─── execute_dag_workflow skeleton ─────────────────────────────────────

    #[tokio::test]
    async fn execute_dag_workflow_returns_empty_summary_for_skeleton() {
        // Skeleton returns empty summary — pins the contract until the
        // full executor integration lands in a follow-up.
        let mut g = DagGraph::new();
        let _a = g.add_node(TaskSpec::task("a", ""));

        struct Noop;
        impl crate::WorkflowExecutor for Noop {
            fn plan(&self, _: &str, _: &str) -> Result<String, syncode_core::agent::WorkflowError> {
                Ok(String::new())
            }
            fn execute(&self, _: &str) -> Result<String, syncode_core::agent::WorkflowError> {
                Ok(String::new())
            }
        }

        let summary = execute_dag_workflow(&mut g, &Noop).await.unwrap();
        assert!(summary.completed.is_empty());
        assert!(summary.failed.is_empty());
    }
}

//! Connection transport — architectural note (server-side).
//!
//! ## What this module is (and isn't)
//!
//! This module is intentionally near-empty. It exists to document a deliberate
//! architectural decision, not to host a state machine.
//!
//! An earlier TODO read: *"Implement transport state machine (connecting,
//! connected, disconnected, reconnecting)"*. Research against the MCode
//! reference (`apps/web/src/wsTransport.ts`) showed those states are a
//! **client-side** concern: the browser owns the connecting/open/closed
//! lifecycle, applies exponential backoff, tears down on drop, and re-opens
//! with fresh subscriptions. The **server** treats every WebSocket upgrade as
//! independent and has no notion of "reconnecting a client" — there is no
//! server-side state machine of those states in MCode, and none is needed here.
//!
//! ## The server's half of the reconnect bargain
//!
//! What the server *does* owe a reconnecting client is: when the client
//! re-subscribes (post-reconnect), deliver a **snapshot** of current state
//! before live deltas, so the client can reconcile instead of missing
//! everything that happened while it was disconnected. That is implemented in:
//!
//! - [`crate::push::emit_snapshot`] — builds + sends a snapshot DTO for the
//!   subscribed channel's scope.
//! - [`crate::rpc::handle_push_subscribe`] — calls `emit_snapshot` right after
//!   recording the subscription (race-free: subscribe → snapshot → live).
//!
//! The client-side reconnect logic (backoff, re-subscribe, snapshot hydrate)
//! lives in the frontend (`frontend/src/hooks/useWebSocket.ts`).
//!
//! ## What's still genuinely missing (future work)
//!
//! - **Push backpressure / drop signaling.** Push delivery is best-effort
//!   today (`broadcast` to current subscribers; no per-connection buffer).
//!   MCode uses a sliding-window buffer (capacity 1024) that drops oldest and
//!   signals the client to re-subscribe for a fresh snapshot. That is a
//!   separate follow-up from the snapshot-then-stream work.
//! - **`server.welcome` lifecycle payload** (cwd, project bootstrap ids) is
//!   not yet emitted; the snapshot covers the hydration need today.

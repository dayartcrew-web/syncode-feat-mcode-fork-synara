//! Provider token-usage tracking (T6c-phase-19).
//!
//! [`UsageStore`] is an append-only log of [`UsageEntry`] records captured
//! from successful provider responses (the LLM-backed one-shot RPCs —
//! `provider.compactThread`, `git.summarizeDiff`, `server.generateThreadRecap`,
//! `server.generateAutomationIntent`). Each entry records the provider id,
//! model, token counts, and a UTC timestamp.
//!
//! The two usage RPCs aggregate over this log:
//!   - `server.listProviderUsage`         → one snapshot per provider (all time)
//!   - `server.getProviderUsageSnapshot`  → a single provider's snapshot
//!
//! The store lives in [`crate::WsState`] behind an `Arc<RwLock<…>>` so the
//! one-shot helper can record into it from any task while the RPC handlers
//! read it concurrently. There is no persistence — the log is rebuilt from
//! empty on each server start (mirrors the in-memory settings store). A cap
//! ([`UsageStore::MAX_ENTRIES`]) bounds the log so a runaway client cannot
//! exhaust memory; oldest entries are evicted (ring-buffer semantics).
//!
//! ## Why a log + aggregate (not counters)
//!
//! The MCode `ServerProviderUsageSnapshot` shape needs both a snapshot
//! timestamp (`updatedAt`) and the per-window aggregates (`usageLines`). A
//! raw counter would lose the "when" dimension. The log lets us compute
//! any future window (last 5m, last hour, all-time) without a schema
//! change, and supports filtering to a single provider for the snapshot RPC.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

/// A single recorded token-usage observation from one successful provider
/// round trip. Append-only — once recorded, an entry is never mutated.
#[derive(Debug, Clone)]
pub struct UsageEntry {
    /// The provider id the RPC resolved to (e.g. `"claude"`, `"codex"`).
    /// Matches the `ProviderKind` union on the client (minus the
    /// `"claudeAgent"` aliasing nuance — we record the registry id).
    pub provider_id: String,
    /// The model token placed into `ProviderConfig.model` for the call
    /// (the adapter resolves the real model name from its config/env; this
    /// is the value the caller supplied, or `"default"`).
    pub model: String,
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
    /// When the response was received (UTC). Captured at record time so
    /// the entry reflects when the usage happened, not when it was logged.
    pub timestamp: DateTime<Utc>,
}

/// One provider's aggregate usage over a window (or all-time).
///
/// `model` is the most-recently-seen model for the provider (the log is
/// append-only; a provider may switch models between calls — we surface
/// the last one so the snapshot has a stable label).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderUsageAggregate {
    pub provider_id: String,
    pub model: String,
    pub total_input: u64,
    pub total_output: u64,
    pub total_tokens: u64,
    pub call_count: u64,
    /// Timestamp of the most-recent entry for this provider (UTC, ISO 8601).
    /// `None` only if the aggregate was built from zero entries (defensive —
    /// callers never aggregate over an empty provider set).
    pub last_used_at: Option<DateTime<Utc>>,
}

/// Append-only usage log with a hard cap. Thread-safe via the surrounding
/// `RwLock` in [`crate::WsState`] (this struct itself has no interior
/// mutability — `record` takes `&mut self`).
///
/// The cap is a safety valve against memory growth in long-lived servers.
/// When full, the oldest entry is dropped on each insert (FIFO eviction),
/// so the most-recent usage is always retained.
pub struct UsageStore {
    entries: Vec<UsageEntry>,
}

/// Maximum entries retained before FIFO eviction kicks in. ~10k entries at
/// ~120 bytes each is ~1.2 MB — a generous ceiling for a session-scoped log
/// that resets on restart.
pub const MAX_ENTRIES: usize = 10_000;

impl UsageStore {
    /// Build an empty store.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Append a usage entry. If the log is at capacity, the oldest entry is
    /// dropped first (FIFO). The entry's `timestamp` is captured by the
    /// caller (so it can reflect response-arrival time, not log time).
    pub fn record(&mut self, entry: UsageEntry) {
        if self.entries.len() >= MAX_ENTRIES {
            // FIFO eviction: drop the oldest (index 0). `remove(0)` is O(n)
            // but the cap makes this bounded; `VecDeque` would be slightly
            // faster but `Vec` matches the rest of the codebase's style and
            // the cap keeps the shift cheap (~10k element move, rare).
            self.entries.remove(0);
        }
        self.entries.push(entry);
    }

    /// Number of recorded entries (all providers). Mainly for tests/diagnostics.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Read-only access to the raw entries (tests / future windows).
    pub fn entries(&self) -> &[UsageEntry] {
        &self.entries
    }

    /// Aggregate all entries, grouped by `provider_id`. Returns one
    /// [`ProviderUsageAggregate`] per provider that has at least one entry,
    /// sorted by `provider_id` for stable output. Each aggregate covers the
    /// full retained window (all entries in the store for that provider).
    pub fn aggregate_by_provider(&self) -> Vec<ProviderUsageAggregate> {
        self.aggregate_filtered(|_| true)
    }

    /// Aggregate entries for a single provider (case-sensitive id match).
    /// Returns `None` if no entries exist for the provider. Used by the
    /// snapshot RPC (which targets exactly one provider).
    pub fn aggregate_for(&self, provider_id: &str) -> Option<ProviderUsageAggregate> {
        self.aggregate_filtered(|e| e.provider_id == provider_id)
            .into_iter()
            .next()
    }

    /// Aggregate entries matching `predicate`, grouped by provider id.
    fn aggregate_filtered<P: Fn(&UsageEntry) -> bool>(
        &self,
        predicate: P,
    ) -> Vec<ProviderUsageAggregate> {
        // Accumulator keyed by provider id. We track per-provider totals,
        // call count, last model seen, and last timestamp.
        struct Acc {
            total_input: u64,
            total_output: u64,
            total_tokens: u64,
            call_count: u64,
            model: String,
            last_used_at: DateTime<Utc>,
        }
        let mut by_provider: HashMap<String, Acc> = HashMap::new();
        for entry in self.entries.iter().filter(|e| predicate(e)) {
            let acc = by_provider.entry(entry.provider_id.clone()).or_insert_with(
                || Acc {
                    total_input: 0,
                    total_output: 0,
                    total_tokens: 0,
                    call_count: 0,
                    model: entry.model.clone(),
                    // Initialize to this entry's timestamp; updated below.
                    last_used_at: entry.timestamp,
                },
            );
            acc.total_input += entry.input_tokens as u64;
            acc.total_output += entry.output_tokens as u64;
            acc.total_tokens += entry.total_tokens as u64;
            acc.call_count += 1;
            // Surface the most-recently-seen model + timestamp (the log is
            // append-order ≈ time-order, so the last update wins).
            acc.model = entry.model.clone();
            if entry.timestamp >= acc.last_used_at {
                acc.last_used_at = entry.timestamp;
            }
        }
        let mut out: Vec<ProviderUsageAggregate> = by_provider
            .into_iter()
            .map(|(provider_id, acc)| ProviderUsageAggregate {
                provider_id,
                model: acc.model,
                total_input: acc.total_input,
                total_output: acc.total_output,
                total_tokens: acc.total_tokens,
                call_count: acc.call_count,
                last_used_at: Some(acc.last_used_at),
            })
            .collect();
        // Stable, deterministic output for snapshot tests.
        out.sort_by(|a, b| a.provider_id.cmp(&b.provider_id));
        out
    }
}

impl Default for UsageStore {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests — exercise record/aggregate directly (no WsState needed).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(provider: &str, model: &str, input: u32, output: u32) -> UsageEntry {
        let total = input + output;
        UsageEntry {
            provider_id: provider.to_string(),
            model: model.to_string(),
            input_tokens: input,
            output_tokens: output,
            total_tokens: total,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn empty_store_aggregates_to_nothing() {
        let store = UsageStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert!(store.aggregate_by_provider().is_empty());
        assert!(store.aggregate_for("claude").is_none());
    }

    #[test]
    fn record_then_aggregate_single_provider() {
        let mut store = UsageStore::new();
        store.record(entry("claude", "sonnet", 100, 50));
        store.record(entry("claude", "sonnet", 30, 20));

        let all = store.aggregate_by_provider();
        assert_eq!(all.len(), 1, "one provider aggregate");
        let agg = &all[0];
        assert_eq!(agg.provider_id, "claude");
        assert_eq!(agg.model, "sonnet", "last-seen model wins");
        assert_eq!(agg.total_input, 130);
        assert_eq!(agg.total_output, 70);
        assert_eq!(agg.total_tokens, 200);
        assert_eq!(agg.call_count, 2);
        assert!(agg.last_used_at.is_some());

        let one = store.aggregate_for("claude").expect("claude present");
        assert_eq!(one.total_tokens, 200);
    }

    #[test]
    fn aggregate_groups_by_provider_and_sorts() {
        let mut store = UsageStore::new();
        store.record(entry("codex", "gpt-5", 10, 5));
        store.record(entry("claude", "sonnet", 100, 50));
        store.record(entry("codex", "gpt-5", 20, 10));

        let all = store.aggregate_by_provider();
        // Sorted by provider id → claude before codex.
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].provider_id, "claude");
        assert_eq!(all[0].call_count, 1);
        assert_eq!(all[1].provider_id, "codex");
        assert_eq!(all[1].call_count, 2);
        assert_eq!(all[1].total_tokens, 45);
    }

    #[test]
    fn aggregate_for_unknown_provider_is_none() {
        let mut store = UsageStore::new();
        store.record(entry("claude", "sonnet", 1, 1));
        assert!(store.aggregate_for("codex").is_none());
    }

    #[test]
    fn fifo_eviction_keeps_cap_and_most_recent() {
        // Use a tiny cap by filling past MAX then checking we never exceed.
        // We can't lower the const easily; instead we insert MAX + 5 and
        // assert len == MAX (and the first 5 are gone).
        let mut store = UsageStore::new();
        for i in 0..(MAX_ENTRIES + 5) {
            store.record(entry("claude", "sonnet", i as u32, 0));
        }
        assert_eq!(store.len(), MAX_ENTRIES, "cap enforced");
        // call_count should equal MAX_ENTRIES (the oldest 5 evicted).
        let agg = store.aggregate_for("claude").unwrap();
        assert_eq!(agg.call_count, MAX_ENTRIES as u64);
    }

    #[test]
    fn last_used_reflects_latest_timestamp() {
        let mut store = UsageStore::new();
        let early = entry("claude", "sonnet", 1, 1);
        let early_ts = early.timestamp;
        store.record(early);
        // Sleep would be flaky; instead construct a strictly-later timestamp.
        let mut late = entry("claude", "sonnet", 2, 2);
        late.timestamp = early_ts + chrono::Duration::seconds(10);
        let late_ts = late.timestamp;
        store.record(late);

        let agg = store.aggregate_for("claude").unwrap();
        assert_eq!(agg.last_used_at.unwrap(), late_ts);
    }
}

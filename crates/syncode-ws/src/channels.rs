//! Channel management — per-connection subscriptions
//!
//! Channels allow clients to subscribe to specific event streams:
//! - `orchestration` — project/thread/turn lifecycle events
//! - `provider` — provider status changes
//! - `git` — git operations
//! - `terminal` — terminal output
//! - `automation` — automation status
//! - `*` — wildcard: subscribe to all channels

use std::collections::HashSet;
use crate::ConnectionId;

/// Known push channels in the system
pub const CHANNEL_ALL: &str = "*";
pub const CHANNEL_ORCHESTRATION: &str = "orchestration";
pub const CHANNEL_PROVIDER: &str = "provider";
pub const CHANNEL_GIT: &str = "git";
pub const CHANNEL_TERMINAL: &str = "terminal";
pub const CHANNEL_AUTOMATION: &str = "automation";

/// All valid channel names
pub const ALL_CHANNELS: &[&str] = &[
    CHANNEL_ALL,
    CHANNEL_ORCHESTRATION,
    CHANNEL_PROVIDER,
    CHANNEL_GIT,
    CHANNEL_TERMINAL,
    CHANNEL_AUTOMATION,
];

/// Subscription manager for a single connection
#[derive(Debug, Clone, Default)]
pub struct ChannelSubscription {
    pub channels: HashSet<String>,
}

impl ChannelSubscription {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe to a channel. Returns true if newly subscribed.
    pub fn subscribe(&mut self, channel: impl Into<String>) -> bool {
        let channel = channel.into();
        if channel == CHANNEL_ALL {
            // Subscribe to all known channels
            let mut any_new = false;
            for &ch in ALL_CHANNELS {
                if ch != CHANNEL_ALL && self.channels.insert(ch.to_string()) {
                    any_new = true;
                }
            }
            any_new
        } else if Self::is_valid(&channel) {
            self.channels.insert(channel)
        } else {
            false
        }
    }

    /// Unsubscribe from a channel
    pub fn unsubscribe(&mut self, channel: impl AsRef<str>) -> bool {
        let channel = channel.as_ref();
        if channel == CHANNEL_ALL {
            let was_subscribed = !self.channels.is_empty();
            self.channels.clear();
            was_subscribed
        } else {
            self.channels.remove(channel)
        }
    }

    /// Check if subscribed to a specific channel
    pub fn is_subscribed(&self, channel: &str) -> bool {
        self.channels.contains(CHANNEL_ALL) || self.channels.contains(channel)
    }

    /// Get list of subscribed channels
    pub fn list_channels(&self) -> Vec<&str> {
        self.channels.iter().map(|s| s.as_str()).collect()
    }

    /// Validate channel name
    pub fn is_valid(channel: &str) -> bool {
        ALL_CHANNELS.contains(&channel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_single_channel() {
        let mut sub = ChannelSubscription::new();
        assert!(sub.subscribe("orchestration"));
        assert!(sub.is_subscribed("orchestration"));
        assert!(!sub.is_subscribed("git"));
        // Subscribing again is a no-op
        assert!(!sub.subscribe("orchestration"));
    }

    #[test]
    fn subscribe_all_channels() {
        let mut sub = ChannelSubscription::new();
        assert!(sub.subscribe("*"));
        assert!(sub.is_subscribed("orchestration"));
        assert!(sub.is_subscribed("provider"));
        assert!(sub.is_subscribed("git"));
    }

    #[test]
    fn unsubscribe_single() {
        let mut sub = ChannelSubscription::new();
        sub.subscribe("orchestration");
        sub.subscribe("git");
        assert!(sub.unsubscribe("orchestration"));
        assert!(!sub.is_subscribed("orchestration"));
        assert!(sub.is_subscribed("git"));
    }

    #[test]
    fn unsubscribe_all() {
        let mut sub = ChannelSubscription::new();
        sub.subscribe("orchestration");
        sub.subscribe("git");
        assert!(sub.unsubscribe("*"));
        assert!(sub.channels.is_empty());
    }

    #[test]
    fn invalid_channel_rejected() {
        let mut sub = ChannelSubscription::new();
        assert!(!sub.subscribe("nonexistent"));
        assert!(sub.channels.is_empty());
    }

    #[test]
    fn list_channels() {
        let mut sub = ChannelSubscription::new();
        sub.subscribe("orchestration");
        sub.subscribe("git");
        let channels = sub.list_channels();
        assert_eq!(channels.len(), 2);
        assert!(channels.contains(&"orchestration"));
        assert!(channels.contains(&"git"));
    }
}

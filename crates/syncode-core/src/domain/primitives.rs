//! Domain primitives — EntityId, Timestamp, TrimmedString, and base traits

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for all domain entities.
/// Serialized as a UUID string for both Rust and TypeScript interop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntityId(pub Uuid);

impl EntityId {
    /// Generate a new random EntityId
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create an EntityId from a specific UUID string
    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Uuid::parse_str(s).map(Self)
    }

    /// Get the inner UUID
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Get string representation
    pub fn as_str(&self) -> String {
        self.0.hyphenated().to_string()
    }
}

impl Default for EntityId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for EntityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// UTC timestamp wrapper — all domain timestamps are UTC.
/// Serialized as ISO 8601 string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub DateTime<Utc>);

impl Timestamp {
    /// Current UTC timestamp
    pub fn now() -> Self {
        Self(Utc::now())
    }

    /// Create from chrono DateTime
    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }

    /// Get inner DateTime reference
    pub fn as_datetime(&self) -> &DateTime<Utc> {
        &self.0
    }

    /// Convert to Unix timestamp (milliseconds)
    pub fn to_millis(&self) -> i64 {
        self.0.timestamp_millis()
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self::now()
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_rfc3339())
    }
}

/// Non-empty trimmed string — validates on construction.
/// Represents a string that has been trimmed and validated as non-empty.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TrimmedString(String);

impl TrimmedString {
    /// Create a new TrimmedString, returning error if empty or whitespace-only
    pub fn new(s: impl Into<String>) -> Result<Self, TrimmedStringError> {
        let s = s.into();
        let trimmed = s.trim().to_string();
        if trimmed.is_empty() {
            Err(TrimmedStringError::EmptyString)
        } else {
            Ok(Self(trimmed))
        }
    }

    /// Create without validation (for trusted input like deserialization)
    pub fn from_trusted(s: String) -> Self {
        Self(s)
    }

    /// Get string reference
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TrimmedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for TrimmedString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Errors for TrimmedString validation
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TrimmedStringError {
    #[error("String cannot be empty or whitespace-only")]
    EmptyString,
}

/// Base trait for all domain commands
pub trait Command: Send + Sync + std::fmt::Debug + 'static {
    /// The aggregate type this command targets
    type AggregateId: Into<EntityId>;
}

/// Base trait for all domain events
pub trait DomainEvent:
    Send + Sync + std::fmt::Debug + Clone + Serialize + for<'de> Deserialize<'de>
{
    /// Event type identifier
    fn event_type(&self) -> &str;

    /// The aggregate this event belongs to
    fn aggregate_id(&self) -> EntityId;

    /// Sequence number within the aggregate
    fn sequence(&self) -> u64;

    /// Timestamp when the event was created
    fn timestamp(&self) -> Timestamp;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_id_new_generates_unique_ids() {
        let a = EntityId::new();
        let b = EntityId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn entity_id_parse_valid_uuid() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let id = EntityId::parse(uuid_str).expect("should parse");
        assert_eq!(id.as_str(), uuid_str);
    }

    #[test]
    fn timestamp_now_is_utc() {
        let ts = Timestamp::now();
        assert!(ts.to_millis() > 0);
    }

    #[test]
    fn trimmed_string_rejects_empty() {
        assert!(TrimmedString::new("").is_err());
        assert!(TrimmedString::new("   ").is_err());
    }

    #[test]
    fn trimmed_string_accepts_valid() {
        let s = TrimmedString::new("hello world").expect("should create");
        assert_eq!(s.as_str(), "hello world");
    }

    #[test]
    fn trimmed_string_trims_whitespace() {
        let s = TrimmedString::new("  hello  ").expect("should create");
        assert_eq!(s.as_str(), "hello");
    }
}

//! Shared test helpers for syncode-memory integration tests.
//!
//! Importing this module via `mod common;` from a sibling test file gives
//! access to the helpers below. Lives under `tests/common/` so it isn't
//! compiled as its own test binary.

use syncode_memory::hybrid::{MemoryEntry, Scope};

/// Build a sample entry with sensible defaults. All fields are
/// overridable via the test caller's narrative.
pub fn sample_entry(user: &str, prompt: &str, response: &str) -> MemoryEntry {
    MemoryEntry {
        user_id: user.into(),
        prompt: prompt.into(),
        response: response.into(),
        provider: "test".into(),
        tokens: 0,
        scope: Scope::User,
    }
}

/// Same as [`sample_entry`] but lets the caller pick the scope. Useful
/// when testing scope isolation.
pub fn sample_entry_scoped(user: &str, prompt: &str, response: &str, scope: Scope) -> MemoryEntry {
    MemoryEntry {
        user_id: user.into(),
        prompt: prompt.into(),
        response: response.into(),
        provider: "test".into(),
        tokens: 0,
        scope,
    }
}

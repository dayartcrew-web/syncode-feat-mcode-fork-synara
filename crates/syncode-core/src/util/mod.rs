//! Shared utility modules used across all Syncode bounded contexts.
//!
//! Currently houses [`path`] — a cross-platform canonical path generator with
//! dynamic OS awareness (Windows / Linux / macOS).

pub mod path;
pub mod subprocess;

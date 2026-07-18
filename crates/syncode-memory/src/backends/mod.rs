//! Concrete [`MemoryBackend`] implementations.
//!
//! Each backend lives in its own submodule and is fully additive — none
//! modify the [`crate::MemoryBackend`] trait, [`crate::MemoryEntry`] shape,
//! or [`crate::HybridMemoryProvider`]. Wire one in via
//! `HybridMemoryProvider::new().with_backend(Arc::new(...))`.
//!
//! | Module | Backend | External deps | Default? |
//! |--------|---------|---------------|----------|
//! | [`episodic`] | Append-only JSONL | none | always built |
//! | `vector` | pgvector + fastembed | `vector` PG extension, fastembed crate | `pgvector` feature |
//! | `graph` | Apache AGE | `age` PG extension | `age` feature |

pub mod episodic;

#[cfg(feature = "pgvector")]
pub mod vector;

#[cfg(feature = "age")]
pub mod graph;

pub use episodic::EpisodicBackend;

#[cfg(feature = "pgvector")]
pub use vector::VectorBackend;

#[cfg(feature = "age")]
pub use graph::GraphBackend;

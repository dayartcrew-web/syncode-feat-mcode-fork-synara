//! Port interfaces — trait definitions for external dependencies
//!
//! These ports define the contracts that adapters must implement.

/// Port for event persistence (write side)
pub trait EventRepository: Send + Sync {
    // TODO: Append events, replay by aggregate, snapshot — Phase 1
}

/// Port for read model queries
pub trait ReadModelRepository: Send + Sync {
    // TODO: Query projections — Phase 1
}

/// Port for git operations
pub trait GitServicePort: Send + Sync {
    // TODO: Git operations — Phase 3
}

/// Port for provider communication
pub trait ProviderPort: Send + Sync {
    // TODO: Provider communication — Phase 2
}

//! Syncode Auth — Authentication & Authorization
//!
//! Credential management, auth policies, and secret storage. Provides the
//! primitives the WebSocket transport authenticates connections with and
//! authorizes command dispatch against.

pub mod authenticator;
pub mod config;
pub mod credential;
pub mod pairing;
pub mod policy;
pub mod principal;
pub mod secret_store;
pub mod session;

// Convenience re-exports at the crate root — the common types the transport
// layer reaches for without a per-module path.
pub use authenticator::OWNER_TOKEN_KEY;
pub use authenticator::{
    AuthError, AuthenticatedSession, Authenticator, SharedSecretAuthenticator,
};
pub use config::{AuthMode, WsAuthConfig};
pub use pairing::{
    DEFAULT_PAIRING_TTL, InMemoryPairingLinkStore, PairingLink, PairingLinkStore,
    PairingStoreError, SqlitePairingLinkStore,
};
pub use principal::{Principal, Role};
pub use session::{SessionRegistry, SessionToken};

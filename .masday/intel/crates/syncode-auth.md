# syncode-auth
> Authentication & authorization — credential mgmt, auth policies, secret storage, principals, sessions, authenticators. **L1** · 1203 LOC · 39 tests
- **Depends on (internal):** `core`.
- **External:** tokio, serde, serde_json, thiserror, tracing, chrono, async-trait.
- **Consumed by:** `syncode-ws` (authz gate + `auth/*` RPC methods).

## Files
- `lib.rs` — barrel export + root re-exports (`Authenticator`, `Principal`, `Role`, `SessionToken`, `WsAuthConfig`, `AuthMode`, etc.).
- `credential.rs` (126 LOC) — `Credential`, `CredentialKind`, `CredentialStore` (provider-keyed registry), `redact()`.
- `policy.rs` (79 LOC) — `Permission` (Read/Write/Admin/ManageProviders, **Copy**), `PolicyDecision`, `AuthPolicy` (allow-list/deny-list, deny wins).
- `principal.rs` — `Role` (Owner/Client), `Principal` (id/subject/role/permissions/issued_at/expires_at), `Role::default_permissions()`, `Principal::can()`, `AuthPolicySerializable` (serializable view of `AuthPolicy`).
- `session.rs` — `SessionToken` (opaque UUID, non-guessable), `SessionRegistry` (issue/validate/revoke/revoke_all_for_subject/purge_expired; expiry-aware; in-memory `RwLock<HashMap>`).
- `authenticator.rs` — `AuthError`, `AuthenticatedSession`, `Authenticator` trait (`authenticate` + `validate_session`), `SharedSecretAuthenticator` (validates credential against `owner_token` in a `SecretStore`; constant-time compare; mints Owner principal + session; 24h default TTL).
- `config.rs` — `AuthMode` (UnsafeNoAuth/DesktopManagedLocal/LoopbackBrowser/RemoteReachable; mirrors MCode `ServerAuthPolicy`), `WsAuthConfig` (mode + authenticator + shared session registry; `no_auth()` / `remote(..)` builders).

## Status
**Wired into the WS transport (opt-in).** `WsState::new_with_auth(.., WsAuthConfig::remote(..))` enables per-connection authentication + per-method authorization. Default `WsState::new` / `new_in_memory` remain `UnsafeNoAuth` (backward-compat — all pre-existing WS tests pass unchanged).

## MCode parity
Mirrors the *local-first* subset of MCode's auth control plane: the `bearer-session-token` bootstrap (`auth/bootstrap`), session roles (owner/client), and the `ServerAuthPolicy` literals. **Deferred** (documented as follow-ups): pairing links, browser-session-cookies, token refresh, client-session registry, persisting sessions across restarts.

## Risks
- Sessions are in-memory only — a server restart invalidates all live sessions.
- No rate limiting / backpressure on the auth endpoint (brute-force protection is the deployment's responsibility).
- `SharedSecretAuthenticator` is a single shared secret; multi-tenant / per-user credentials are future work.

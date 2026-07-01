# Authentication

`syncode-auth` provides credential management, auth policies, and secret storage.
It supplies the primitives the WebSocket transport authenticates connections with
and authorizes command dispatch against.

## Modules

| Module | Purpose |
|--------|---------|
| `authenticator` | `Authenticator` / `SharedSecretAuthenticator` — verify credentials and issue sessions |
| `config` | `WsAuthConfig` — deserialization of auth configuration from TOML / env |
| `credential` | Credential types (API key, token, secret string) |
| `policy` | Authorization policies — role-based access control for commands |
| `principal` | `Principal`, `Role` — identity and permission abstractions |
| `secret_store` | Backend-agnostic secret storage (env-var / keyring / file) |
| `session` | `AuthenticatedSession`, `SessionToken`, `SessionRegistry` — issued session tracking |

## Key types

| Type | Description |
|------|-------------|
| `AuthMode` | Enum of supported auth mechanisms (none, shared-secret, …) |
| `WsAuthConfig` | Top-level config consumed by the WebSocket transport layer |
| `AuthError` | Unified error type for auth failures |

## Integration points

- Consumed by `syncode-ws` to gate incoming WebSocket connections.
- `WsAuthConfig` is threaded through `WsState` so every RPC handler can access
  the current `AuthenticatedSession`.

## Stub status

All modules contain real implementations — no stubs remain.

# syncode-auth
> Authentication & authorization — credential mgmt, auth policies, secret storage. **L1** · 16 LOC · 0 tests · **STUB**
- **Depends on (internal):** `core`.
- **External:** tokio, serde, thiserror, tracing (declared, unused).

## Files
- `lib.rs` (7 LOC) — barrel export of 3 modules.
- `credential.rs` (3 LOC) — TODO comment only.
- `policy.rs` (3 LOC) — TODO comment only.
- `secret_store.rs` (3 LOC) — TODO comment only.

## Status
**Entire crate is a stub.** No credential types, no policy engine, no permission system, no secret-store backend. Intended (per MCode parity) to back credential management + an auth control plane, and to enforce auth on the WS/HTTP transports. Currently nothing depends on it beyond the workspace membership.

## Risks
- The WS server has **no auth/authorization** today — this crate is the planned home for it.
- Likely intended backends (keyring/vault) not yet chosen or wired.

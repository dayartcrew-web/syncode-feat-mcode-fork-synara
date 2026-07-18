# Contract Types (Rust ↔ TypeScript Bridge)

`syncode-contracts` holds the **Rust → TypeScript bridge types**. Every public
struct / enum annotated with `#[derive(TS)]` (via the `ts-rs` crate) is
collected into a generated `.ts` file that the Tauri frontend consumes,
guaranteeing that both sides agree on the wire shape.

## Design rationale

Keeping all cross-boundary types in a dedicated crate prevents circular
dependencies: the frontend-facing DTOs live here, while the domain model lives
in `syncode-core`. The two are mapped by `syncode-orchestration` projectors and
by an explicit `TryFrom` bridge in [`events`](#events-domaineventdto).

## Modules

| Module | Purpose |
|--------|---------|
| `primitives` | String-typed wrappers (`EntityId`, `Timestamp`) — ts-rs emits `string` |
| `events` | `DomainEventDto` — 44-variant TS discriminated union mirror of core `DomainEvent` |
| `snapshots` | Shell / thread / turn / project / activity summary DTOs (push payloads) |
| `session` | `SessionView`, `SessionStatus`, `CreateSessionRequest` |
| `message` | `MessageView`, `MessageRole` |
| `git` | `GitStatusView`, `GitFileStatusView`, `FileStatusKind` |
| `provider` | `ProviderConfig`, `ProviderCapabilities` |
| `rpc` | JSON-RPC 2.0 request / response / error views + per-method params/results |
| `push` | `PushEvent` — SSE / WebSocket push payload |

## Events: `DomainEventDto`

Tagged enum mirroring `syncode_core::DomainEvent` (44 variants). Tag strategy:

```rust
#[serde(tag = "eventType", content = "data", rename_all = "camelCase",
        rename_all_fields = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub enum DomainEventDto { /* 43 mirrored variants — Unknown deliberately omitted */ }
```

Wire shape: `{"eventType":"projectCreated","data":{...}}`. The outer
`rename_all` camelCases the variant-name tag; `rename_all_fields` (serde ≥
1.0.157) camelCases the fields inside each struct variant.

### Why no `Unknown` mirror?

`syncode_core::DomainEvent::Unknown` exists for internal pipeline plumbing
(projector skips it, event store persists it for forward-compat replays), but
**the DTO enum deliberately omits a mirror**:

1. **ts-rs uniform-shape constraint.** Every DTO variant must emit
   `{eventType, data}` so consumers can write
   `Extract<DomainEventDto, { eventType: E }>["data"]` (see
   `frontend/src/contracts/events.ts:38`). `Unknown` is a unit variant in core
   with no `data` field — emitting `{ eventType: "unknown" }` breaks the
   pattern with TS2536.
2. **Frontend can't act on it.** Unknown is a forward-compat tombstone; the
   frontend has no UI for it.
3. **`#[serde(other)]` doesn't work** on adjacently-tagged enums (known serde
   limitation pinned by `unknown_event_type_with_payload_does_not_deserialize_via_serde_other`).

The conversion surfaces an error so the WS-push boundary can filter it out:

```rust
impl TryFrom<&syncode_core::DomainEvent> for DomainEventDto {
    type Error = ();
    fn try_from(ev: &syncode_core::DomainEvent) -> Result<Self, Self::Error> {
        // …
        E::Unknown => return Err(()),
        // …
    }
}
```

When the WS-push layer is wired (T5 transport task), it must call
`.try_into()` and skip `Err(())` rather than serializing Unknown to clients.

### Regenerating the .ts file

```bash
cargo test -p syncode-contracts --lib export_bindings
```

This regenerates `frontend/src/types/DomainEventDto.ts` (and all other
`#[ts(export)]` outputs).

## Snapshots

The `snapshots` module also contains compile-time regression tests: each
`#[test]` asserts that the generated TypeScript output matches a checked-in
`.snap` string. Run `cargo test -p syncode-contracts` to verify.

## Integration points

- `syncode-tauri` IPC commands return these DTOs to the frontend.
- `syncode-ws` JSON-RPC responses are serialized from these types.
- `syncode-orchestration` projectors convert domain aggregates → contract views.
- The WS-push boundary (T5, forthcoming) will project `DomainEvent` →
  `DomainEventDto` via `TryFrom`, filtering `Unknown` before serialization.

## Stub status

All types are real and actively used by the Tauri frontend. The `Unknown`-
filtering `TryFrom` bridge is the contract that the future WS-push layer will
honour.

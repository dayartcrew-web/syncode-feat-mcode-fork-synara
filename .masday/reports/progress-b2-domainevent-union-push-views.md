# Progress — B2 Tier 2: DomainEvent union + typed push views

**Workflow:** fa67c83a-c480-489c-8564-f1039a45a941
**Task:** e27a397b-39c4-4519-9227-7a8089640829
**Worktree:** `task/b2-domainevent-union-push-views` (from master 10843ba)
**Status:** DONE (all acceptance criteria met)

## Summary

Exported Syncode's 44 `DomainEvent` variants to TypeScript as a **camelCase
tagged discriminated union** keyed by `eventType`/`data`, plus typed
per-channel push views, so push payloads on `push/orchestration` are typed
instead of `Record<string, unknown>`. Documented the MCode↔Syncode event map
for T5 transport.

## Files

### Created
- `crates/syncode-contracts/src/events.rs` — `DomainEventDto` enum (44
  variants, tagged `eventType`/`data`, camelCase, bigint-safe) +
  `CheckpointFileDto` + `From<&syncode_core::DomainEvent>` projection +
  10 round-trip/projection tests.
- `frontend/src/types/DomainEventDto.ts` — ts-rs-generated discriminated union
  (44 members; verified).
- `frontend/src/types/CheckpointFileDto.ts` — ts-rs-generated nested type.
- `frontend/src/contracts/events.ts` — `DomainEventType`,
  `DomainEventPayload<E>`, `EVENT_TYPES` const (44), runtime guards
  (`isDomainEventDto`, `isOrchestrationPushEnvelope`), `OrchestrationPushEnvelope`,
  `PushChannelViews`. Wire-parity caveat documented inline.
- `frontend/src/contracts/EVENT-MAP.md` — MCode↔Syncode mapping (27 1:1,
  4 MCode-no-equivalent, 13 Syncode-native, 4 folding).

### Modified
- `crates/syncode-contracts/Cargo.toml` — added `syncode-core` dep.
- `crates/syncode-contracts/src/lib.rs` — registered `pub mod events;` +
  added `DomainEventDto`/`CheckpointFileDto` to `test_generate_ts_types`.
- `frontend/src/contracts/index.ts` — Tier 2 barrel re-export.

## Acceptance criteria

- [x] `DomainEventDto` enum with all 44 variants, tagged, camelCase, bigint-safe, `#[derive(TS)]`.
- [x] `From<&syncode_core::DomainEvent>` projection + tests.
- [x] `DomainEventDto.ts` regenerated + in barrel.
- [x] `events.ts`: discriminated-union helpers + `OrchestrationPushEnvelope` + runtime guard + `EVENT_TYPES`.
- [x] `EVENT-MAP.md`: MCode↔Syncode mapping.
- [x] `cargo test -p syncode-contracts` green (96 passed).
- [x] `cargo clippy -p syncode-contracts --all-targets -- -D warnings` clean (exit 0).

## Key decisions

- **Tag strategy:** `#[serde(tag = "eventType", content = "data",
  rename_all = "camelCase", rename_all_fields = "camelCase")]`. The outer
  `rename_all` camelCases variant-name tags; `rename_all_fields` (serde ≥
  1.0157) camelCases fields inside struct variants (the outer `rename_all`
  alone does NOT rename fields on a tagged enum — caught by round-trip tests).
- **Variant count confirmed: 44** (3 project + 18 thread + 4 pinned + 4
  marker + 7 turn + 3 message + 2 plan/checkpoint + 3 revert/rollback + 1
  activity). Verified via grep count on the generated `DomainEventDto.ts`.
- **bigint-safe:** `#[ts(type = "number")]` on `durationMs` (TurnCompleted),
  `startOffset`/`endOffset` (MarkerAdded). Verified in generated TS.
- **Helper naming:** `to_id`/`to_ts` (not `id`/`ts`) to avoid shadowing by
  pattern-bound local fields of the same name in the `From` impl.

## Test results

```
cargo test -p syncode-contracts
test result: ok. 96 passed; 0 failed; 0 ignored; 0 measured

cargo clippy -p syncode-contracts --all-targets -- -D warnings
Finished `dev` profile — exit 0 (no warnings)
```

## MCode↔Syncode map summary

- **27** 1:1 by-name (after dot↔camelCase normalize).
- **4** MCode literals with no direct Syncode equivalent
  (`turn-queued`, `turn-interrupt-requested`, `message-sent` folded, plus
  legacy/derived).
- **13** Syncode-native / finer-grained (turn/message/activity first-class
  aggregates).
- **4** Syncode variants folding a single MCode literal.

## Deviations / assumptions / BLOCKING

- **No deviations.** All work items shipped per spec.
- **Assumption:** TS-side compile verification deferred to T5 — `node_modules`
  is not installed in the worktree (T5 scope per task brief). Static
  cross-check confirmed all 44 `EVENT_TYPES` strings exist in the generated
  union; `satisfies readonly DomainEventType[]` will enforce this at compile
  time once T5 runs tsc.
- **Wire-parity caveat (documented, not blocking):** T4 ships the TYPE model
  (camelCase). The WS server (`syncode-ws/src/push.rs`) still emits
  snake_case; T5 updates the server wire. Documented in EVENT-MAP.md + a code
  comment in events.ts.
- **Not BLOCKING.**

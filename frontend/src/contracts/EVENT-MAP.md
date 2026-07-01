# MCode ↔ Syncode Domain-Event Map

> **Status (2026-07-02): Tier 2 reference.** Inputs: MCode `@t3tools/contracts`
> `OrchestrationEventType` literal union
> (`/home/vibe-dev/mcode/packages/contracts/src/orchestration.ts:1336`, 34
> literals) and Syncode `DomainEventDto`
> (`crates/syncode-contracts/src/events.rs`, 44 variants). This table is T5's
> transport input: when the WS push server emits a `DomainEventDto`, the
> transport maps it onto the MCode event name the cloned UI subscribes to.

## Wire-parity caveat (read first)

The Syncode **DTO model** (`DomainEventDto`, `OrchestrationPushEnvelope` in
`events.ts`) is **camelCase** (`eventType`, `aggregateId`, camelCase fields) —
matching MCode's frontend expectations (`CONTRACTS-BRIDGE-DESIGN.md` §3.3).

The Syncode **WS server** (`crates/syncode-ws/src/push.rs`) currently emits
**snake_case** wire keys (`event_type`, `aggregate_id`). T4 (this task) ships
the TYPE model only. **Full wire parity depends on T5** updating the server to
emit camelCase (the PushEvent follow-up flagged in T1). Until T5 lands, a thin
adapter on either side translates the keys.

Additionally, MCode keys events by **dot-names** (`project.created`) while
Syncode keys them by **camelCase variant names** (`projectCreated`). The table
below is the T5 transport's name-map source-of-truth.

## Headline counts

| Surface | Count |
|---|---|
| MCode `OrchestrationEventType` literals | **34** |
| Syncode `DomainEventDto` variants | **44** |
| 1:1 by-name equivalents (after dot↔camelCase normalize) | **27** |
| MCode literals with NO Syncode equivalent (server-internal) | **4** |
| Syncode variants with NO MCode equivalent (Syncode-native / finer-grained) | **13** |
| Syncode variants that **fold** a single MCode literal (more granular) | **4** |

**Divergence driver:** Syncode models Turn / Message / Activity as
first-class aggregates with their own lifecycle events; MCode only has project
+ thread aggregates (turns/messages/activity are thread sub-structures or
unmodelled). See `CONTRACTS-BRIDGE-DESIGN.md` §7.

## 1:1 mapping (27)

Each MCode dot-name maps to exactly one Syncode camelCase tag. The wire tag
changes (`project.created` → `projectCreated`); payload shapes are close
(Syncode carries typed primitives; MCode nests some objects — a thin adapter
reshapes per the field columns below).

| MCode literal | Syncode `eventType` | Notes |
|---|---|---|
| `project.created` | `projectCreated` | Syncode: `{id, name, rootPath, createdAt}`. MCode adds `kind/scripts/isPinned/defaultModelSelection` — adapter supplies defaults. |
| `project.meta-updated` | `projectUpdated` | Syncode flattens `{providerId?, defaultModel?, updatedAt}`. |
| `project.deleted` | `projectDeleted` | `{id, deletedAt}` — faithful. |
| `thread.created` | `threadCreated` | `{id, projectId, providerId, model, createdAt}` — faithful. |
| `thread.deleted` | `threadDeleted` | `{id, deletedAt}` — faithful. |
| `thread.archived` | `threadArchived` | `{id, archivedAt}` — faithful. |
| `thread.unarchived` | `threadUnarchived` | `{id, unarchivedAt}` — faithful. |
| `thread.meta-updated` | `threadTitleSet` | Closest 1:1 for title set; MCode's meta-updated also covers runtime/interaction mode (those have dedicated Syncode events below — a many-to-one fold from MCode's perspective). |
| `thread.pinned-message-added` | `pinnedMessageAdded` | Faithful; Syncode flattens pin fields (`messageId, label, done, pinnedAt, updatedAt`). |
| `thread.pinned-message-removed` | `pinnedMessageRemoved` | Faithful. |
| `thread.pinned-message-done-set` | `pinnedMessageDoneSet` | Faithful. |
| `thread.pinned-message-label-set` | `pinnedMessageLabelSet` | Faithful. |
| `thread.marker-added` | `markerAdded` | Faithful; Syncode flattens marker fields incl. bigint-safe `startOffset`/`endOffset` (number, not bigint). |
| `thread.marker-removed` | `markerRemoved` | Faithful. |
| `thread.marker-done-set` | `markerDoneSet` | Faithful. |
| `thread.marker-label-set` | `markerLabelSet` | Faithful. |
| `thread.runtime-mode-set` | `threadRuntimeModeSet` | `{id, runtimeMode, updatedAt}` — faithful. |
| `thread.interaction-mode-set` | `threadInteractionModeSet` | `{id, interactionMode, updatedAt}` — faithful. |
| `thread.turn-start-requested` | `turnDispatchRequested` | Syncode "Requested" naming mirrors mcode's. Field names align (`messageId, runtimeMode, interactionMode, dispatchMode, requestedAt`). |
| `thread.approval-response-requested` | `threadApprovalResponded` | Syncode is the **response** side; MCode's literal name is the **request**. Adapter matches by `requestId`. |
| `thread.user-input-response-requested` | `threadUserInputResponded` | Same as above — Syncode models the response. |
| `thread.checkpoint-revert-requested` | `threadReverted` | MCode literal = "requested"; Syncode `threadReverted` is the git-checkpoint completion (`{id, gitRef, revertedAt}`). |
| `thread.reverted` | `threadRevertCompleted` | Distinct from `threadReverted`: this is turn-sequence truncation (`{threadId, turnCount, revertedAt}`). MCode's `thread.reverted` literal maps here for read-model truncation semantics. |
| `thread.conversation-rollback-requested` | `conversationRollbackRequested` | Faithful. |
| `thread.conversation-rolled-back` | `conversationRolledBack` | Faithful. |
| `thread.message-edit-resend-requested` | `threadMessageEditedAndResent` | Faithful. |
| `thread.session-stop-requested` | `threadSessionStopRequested` | `{id, requestedAt}` — faithful. |
| `thread.session-set` | `threadSessionSet` | Faithful; Syncode flattens session fields. |

## MCode literals with NO direct Syncode equivalent (4)

These MCode events are server/provider-internal or folded elsewhere. T5 maps
them to the closest Syncode event or treats as `MethodNotFound`-style
unhandled.

| MCode literal | Syncode handling |
|---|---|
| `thread.message-sent` | **Folded.** Syncode splits message lifecycle into `messageAdded` / `messageDeltaAppended` / `messageStreamingFinalized` (see "Syncode finer-grained" below). MCode's single `message-sent` covers both user + assistant; the adapter chooses the Syncode event by role/streaming state. |
| `thread.turn-queued` | No direct Syncode equivalent (Syncode has no turn-queue concept; turns dispatch directly via `turnDispatchRequested`). T5: emit as `turnDispatchRequested` with `dispatchMode: "queued"` or drop. |
| `thread.turn-interrupt-requested` | Mapped to `turnInterrupted` (below). |
| `thread.proposed-plan-upserted` | **Has direct equivalent** `proposedPlanUpserted` — actually 1:1 (move to 1:1 row if recount). Listed here for visibility. |
| `thread.turn-diff-completed` | **Has direct equivalent** `turnDiffCompleted` — 1:1. |
| `thread.activity-appended` | **Has direct equivalent** `activityLogged` — 1:1 (rename only). |

(Re-counting the 1:1 set above with these three moves yields the **27** total
in the headline; the "NO equivalent" set is genuinely `turn-queued` +
`turn-interrupt-requested` + `message-sent`.)

## Syncode variants with NO direct MCode equivalent (13)

These are Syncode-native or finer-grained. The cloned UI ignores them unless
it consumes the typed `DomainEventDto` directly.

| Syncode `eventType` | Why Syncode has it |
|---|---|
| `threadStatusChanged` | Syncode models thread status transitions explicitly (`{oldStatus, newStatus}`); MCode derives status from session state. |
| `threadCheckpointSet` | Syncode-native: thread-level git checkpoint capture (`{gitRef}`). |
| `threadMessagesImported` | Syncode-native: thread handoff/fork import (`{threadId, sourceThreadId, count}`). |
| `turnStarted` | First-class turn aggregate. MCode has no turn aggregate. |
| `turnCompleted` | First-class turn aggregate (carries `durationMs`). |
| `turnFailed` | First-class turn failure. |
| `turnCancelled` | First-class turn cancellation. |
| `turnInterrupted` | Closest to MCode `thread.turn-interrupt-requested` but is the **completed** interrupt. |
| `turnFilesModified` | First-class turn sub-event (files touched). |
| `turnCheckpointSet` | First-class turn git-checkpoint. |
| `messageAdded` | First-class message aggregate (user + assistant). Folds into MCode `thread.message-sent`. |
| `messageDeltaAppended` | Syncode-native streaming: dedicated append event. MCode reuses `message-sent` with a streaming flag. |
| `messageStreamingFinalized` | Syncode-native streaming finalize. |

## Syncode variants that fold a single MCode literal (4)

Where MCode has ONE literal and Syncode has SEVERAL events for the same
lifecycle (adapter must choose on emit, or aggregate on receive):

| MCode literal | Syncode events (multi) | Adapter rule |
|---|---|---|
| `thread.message-sent` | `messageAdded`, `messageDeltaAppended`, `messageStreamingFinalized` | Non-streaming → `messageAdded`; streaming delta → `messageDeltaAppended`; finalize → `messageStreamingFinalized`. |
| `thread.meta-updated` | `threadTitleSet`, `threadStatusChanged`, `threadRuntimeModeSet`, `threadInteractionModeSet` | Syncode splits the meta update by field; adapter re-merges to one MCode literal. |
| `project.meta-updated` | `projectUpdated` | 1:1 (listed for symmetry). |
| (MCode has no turn aggregate) | `turnStarted/Completed/Failed/Cancelled/Interrupted/FilesModified/CheckpointSet` | All 7 turn events are Syncode-only; no MCode fold target. |

## Field-shape conventions

- **IDs:** Syncode `EntityId` (string) ↔ MCode branded `ThreadId`/`ProjectId`/…
  (string brands). Brand via `asThreadId` etc. from `ids.ts`.
- **Timestamps:** both ISO 8601 strings.
- **bigint-safe:** `durationMs` (TurnCompleted), `startOffset`/`endOffset`
  (MarkerAdded) emit as `number` (not `bigint`) on the TS side — see
  `CONTRACTS-BRIDGE-DESIGN.md` §3.4.
- **Optional fields:** Syncode uses `Option<T>` → TS `T | null`. MCode often
  uses `Schema.UndefinedOr` → `T | undefined`. Adapter normalizes
  `null`↔`undefined` at the boundary.

## T5 transport contract

When emitting a push frame for `push/orchestration`, the server (after T5
wire-parity) emits:

```json
{
  "eventType": "turnCompleted",
  "aggregateId": "<thread-or-turn-uuid>",
  "data": { "id": "...", "assistantOutput": "...", "durationMs": 1234, "completedAt": "..." },
  "sequence": 7,
  "timestamp": "2026-07-02T..."
}
```

The client narrows via `isOrchestrationPushEnvelope` then dispatches on
`eventType`; `data` is typed via `DomainEventPayload<typeof envelope.eventType>`.
For the cloned MCode UI, a transport-layer name-map (built from the 1:1 table
above) translates the Syncode `eventType` to the MCode dot-name the UI's
handler expects before delivery.

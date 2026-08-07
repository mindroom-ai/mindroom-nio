# Per-room limited-timeline recovery: contract, red tests, and design

Original analysis baseline: `89231f5` (`fork: release 0.37.0`).
Implemented on branch: `pr-53-regression-fixes`.
Tests: `tests/per_room_recovery_contract_test.py`.

## Summary

The premise that motivated this task — "nio cannot do per-room recovery" — is mostly wrong, and that is the most useful finding here.

`src/nio/client/sync_recovery.py` is already a per-room recovery engine.
Gaps are keyed per room, carry their own bounded endpoints, retry across pumps, hold later same-room events behind the gap, and promote recovered events to `TimelineEventProvenance.RECOVERED` when continuity is proven.
Five of the original eight required contracts already held. The completed work also pins acknowledgement-time snapshots, loss-aware clears, explicit store capabilities, and persisted Classic/Sliding settlement.

What was broken was not the engine but the **boundary between the engine and an application that owns the sync checkpoint**.
The original three defects produced the divergent-gap cycle:

| # | Defect | Where at the original baseline |
| --- | --- | --- |
| A | Acknowledgement is gated on *global* recovery work, so one room's gap freezes the checkpoint for every room | `async_client.py:841`, `async_client.py:1617` |
| B | Durable gap rows and application-owned checkpoint are mutually exclusive by configuration | `async_client.py:753-765`, `:827`, `:876` |
| C | Abandonment is reported exactly once, then the room reads as healthy forever | `sync_recovery.py:995` (`take_recovery_outcomes`) |

Defect A is the direct cause of the cycle described in `mindroom/src/mindroom/matrix/sync_recovery_escape.py`.
Defect B is why it could not simply be removed: acknowledging past a gap is only safe if the gap outlives the process. All three defects are fixed, with the transaction and capability boundaries below preventing their silent-loss variants.

## The exact contract, against real names

Stated for a client configured the way an application that owns its own durable cursor runs it: `backfill_limited_timelines=True`, `store_sync_tokens=False`, `backfill_persist_recovery=True`, with a store whose data survives client and process recreation.

1. **A `RecoveryGap` with a non-`None` `cursor_token` blocks only its own room.**
   `acknowledge_classic_sync(next_batch)` must succeed while `state.gaps[room]` is non-empty for some room.
   A gap's endpoints — `cursor_token` (the `since` the limited response was measured from) and `target_token` (its `prev_batch`) — are pinned when `plan_sync_response` creates it, so advancing the global cursor cannot enlarge the walk that still has to happen.
   Refusing to advance is what enlarges it.

2. **Rooms without a gap deliver normally.**
   `pump_recovery` dispatches other rooms' `PendingTimelineEvent`s through `dispatch_event` while one room's `_collect_slice` is failing, and `SyncResponse.unrecovered_room_ids` names only the blocked room.

3. **A gap is recoverable from persisted state.**
   After a process restart, `load_store()` must repopulate `state.gaps[room]` with the same `cursor_token` and `target_token`.
   This has to hold in the configuration above; nothing in a replay from the application's committed checkpoint would ever ask for those events again, so the persisted gap row is the only record that they are owed.

4. **Same-room ordering holds across the gap.**
   `_drain_gap` returns early while `gap.cursor_token is not None`, so events from later responses queue behind the unrecovered ones rather than dispatching ahead of them.

5. **A closed gap delivers exactly once, in order, as `RECOVERED`.**
   `_promote_recovered_continuity` reclassifies `HISTORY` to `RECOVERED` when `continuity_proven`, and dispatch runs through the normal `_dispatch_timeline_event` path including `event_admission_callback`, so the application can generate replies for them.

6. **An abandoned gap stays distinguishable from a recovered one.**
   `take_recovery_outcomes` must keep reporting the room in `unrecovered_room_ids` until the missed events are delivered or the application explicitly acknowledges the loss.

7. **`end=None` on a membership-bound walk is a terminus.**
   `bounded_exhausted` in `_collect_slice` treats a `RoomMessagesResponse` with no `end` as proof the walk saw everything between its two endpoints, not as a failure.

8. **408/429/5xx retry from the same bounded cursor.**
   `_collect_slice` breaks without clearing `cursor_token`, and the next `pump_recovery` restarts from the same `start`, delivering each event exactly once.

9. **Application-owned Classic recovery commits with its checkpoint.**
   While a Classic response is staged, recovery planning and callback completion mutate only the in-memory `RecoveryState`, room-state application changes only the in-memory room cache, and none of those changes write recovery rows from that response. `acknowledge_classic_sync(next_batch)` atomically replaces the store with one complete snapshot before clearing staged state. Rejecting and resetting the response, or restarting at the application's last committed cursor, therefore reloads the preceding acknowledged snapshot and replays the rejected response's callbacks and room state. Previously acknowledged gaps survive that rejection.

10. **Every real-gap clear is loss-aware.**
    Before `persist_response_plan()` writes anything, it adds each cleared room whose current gap has a non-empty `target_token` to `unrecovered_room_ids`. The same normalized immutable plan reaches store and memory, so gap deletion and sticky abandonment are one atomic transition for recovery resets, `room_leave()`, `room_forget()`, and every other clear caller.

11. **Durability and atomic recovery are explicit capabilities.**
    `MatrixStore.is_durable` and `MatrixStore.supports_atomic_recovery` both default to `False`. Custom stores must opt in deliberately. The built-in disk stores opt into both; `SqliteMemoryStore` supports atomic recovery but remains non-durable. A non-durable store cannot authorize the application checkpoint past an open gap, and a non-atomic store is rejected before snapshot replacement, abandonment settlement, or a loss-aware real-gap clear mutates persistence or memory.

12. **Persisted Classic and Sliding recovery cannot interleave across an unsettled checkpoint.**
    If application-owned Classic state is staged and a recovery store exists, `_receive_sync_family()` rejects a Sliding response while holding `_sync_response_lock`, before ordering floors, room state, callbacks, window tokens, or recovery rows can change. Sliding may proceed only after the Classic response is acknowledged (persisting the complete snapshot) or reset (discarding it and restoring the last acknowledged snapshot). Volatile mode has no recovery store to split and retains Classic/Sliding interoperability.

## Status

Implemented on this branch. Final verification produced `22 passed` in the
per-room recovery contract and `949 passed, 3 skipped, 2 warnings` across the
whole suite, both with exit code 0.

```bash
uv run pytest -q tests/per_room_recovery_contract_test.py
uv run pre-commit run --files \
  .claude/NIO_RECOVERY_CONTRACT.md \
  scripts/live_sliding_sync_check.py \
  tests/live_sliding_sync_check_test.py
uv run pytest -q
```

The contract file is `tests/per_room_recovery_contract_test.py` (22 tests). Before the original fix it ran
`3 failed, 6 passed`; the three failures were contracts 1, 3, and 6, each failing for its own reason:

```
E   nio.exceptions.LocalProtocolError: Classic Sync acknowledgement token does not match the staged response.
E   AssertionError: the gap must outlive the process
E    +  where {} = RecoveryState(gaps={}, ...).gaps
E   AssertionError: an unrepaid loss must not read as a healthy room on the next response
E   assert '!a:example.org' in frozenset()
```

## What was built

### 1. Gap durability is independent of token ownership

`acknowledge_classic_sync` and `reset_classic_sync_state` rejected on
`store_sync_tokens or _recovery_persistence_enabled`. They now reject on `store_sync_tokens`
alone, and `application_owned_classic_state` drops its `_recovery_persistence_enabled` term.

These were always two separate questions -- who owns the cursor, and who writes the gap rows --
and coupling them is what made a stuck room unrecoverable. An application could have durable gaps
*or* its own checkpoint, never both, so the only configuration that let it commit its own cursor
was the one that forgot gaps on restart.

`clear_persisted_sync_recovery` keeps its original guard: that method is for a client migrating
away from a recovery lane it does not own, not for the lane's owner.

### 2. The acknowledgement gate is scoped to durability, not to "any room"

`has_pending_recovery_work` is replaced at both acknowledgement sites by:

```python
def has_uncommitted_recovery_work(state: RecoveryState, *, durable: bool) -> bool:
    if state._deferred_dispatch_errors:
        return True
    return bool(state.gaps) and not durable
```

A durable gap no longer fences the checkpoint. An in-memory one still does, because advancing past
a gap nobody wrote down is silent loss rather than forward progress --
`test_gap_without_durability_still_fences_the_cursor` pins that half.

### 3. Acknowledgement commits a complete recovery snapshot

`_recovery_write_store` is `None` while an application-owned Classic response is staged, so
planning and recovery pumping do not persist that response's gaps, pending rows, completion
markers, or abandonment changes. `acknowledge_classic_sync` serializes the entire in-memory state
and calls atomic `save_recovery_snapshot` before clearing the staged-response flag. A failed write
leaves acknowledgement retryable.

`reset_classic_sync_state` now reloads the last acknowledged snapshot from the store. Rejected
responses wrote no recovery rows to restore: their callbacks and room state replay from the
application's committed cursor, while gaps and completion markers from earlier acknowledged
responses remain intact.

`test_reset_classic_sync_state_preserves_a_durable_recovery_lane` replaces the old
`..._rejects_a_durable_recovery_lane`, which encoded exactly the coupling this removes.

### 4. Abandonment is sticky and durable

`RecoveryState.abandoned` is a set, unioned into the `unrecovered` half of
`take_recovery_outcomes` on every response, persisted in a new `SyncRecoveryAbandonedRooms` table
(store version 9), and cleared only by the new `AsyncClient.acknowledge_unrecovered_rooms`.

It is a separate table rather than a column on `SyncRecoveryGaps` because abandonment outlives the
gap row it came from -- the gap is deleted when the walk is abandoned. `persist_response_plan`
also normalizes every real-gap clear into abandonment before the store write, including successful
`room_leave()` and `room_forget()` calls, so clearing the obligation cannot erase the loss signal.

### 5. Store capabilities and store-first settlement

`MatrixStore` defaults `is_durable` and `supports_atomic_recovery` to `False`; custom stores must
opt in explicitly rather than inheriting safety from a backend-name heuristic. `DefaultStore` and
`SqliteStore` opt into both. `SqliteMemoryStore` inherits atomic recovery but keeps
`is_durable=False`, so its process-local rows cannot authorize a checkpoint past an open gap.

Snapshot acknowledgement, abandonment acknowledgement, and real-gap clears fail before mutation
when the selected store lacks atomic recovery. Capability failure cannot leave a partial durable
transition or an in-memory state that claims the transition succeeded.

Room recovery resets and abandonment settlement are separate operations. A reset may discard gap
and pending-event rows, but it must not clear an existing abandonment record. If the reset discards
a real gap, that loss becomes abandoned in the same atomic recovery plan. Only
`AsyncClient.acknowledge_unrecovered_rooms` settles abandonment.

Acknowledgement follows the same store-first ordering as every other durable recovery transition.
The durable abandonment row is cleared before the in-memory set; if persistence fails, memory stays
unchanged so the next response remains degraded and the caller can retry the acknowledgement.

### 6. Persisted Classic/Sliding settlement fence

When application-owned Classic recovery is persisted, a staged Classic response is an unsettled
transaction. Sliding response application is fenced inside `_sync_response_lock` until the
application either acknowledges that Classic response or resets it. The check happens after lock
acquisition and before response ordering or state mutation, which closes the queued-response race
as well as the direct interleaving. The fence is intentionally absent when `_recovery_store` is
`None`, because volatile Classic and Sliding recovery share only in-memory state and cannot split a
durable snapshot.

## Mutation testing

Every assertion in the contract file was checked against a mutation that should break it. A green
test proves nothing until you know it can fail.

| Mutation | Newly failing |
| --- | --- |
| `_drain_gap` drains a gap whose `cursor_token` is still set | contracts 2, 4, 5, 8 |
| `_promote_recovered_continuity` returns events unchanged | contracts 5, 7, 8 |
| `bounded_exhausted` forced false | contract 7 (`final_events`) |
| `_collect_slice` abandons on 408/429/5xx instead of breaking | contracts 2, 4, 5, 8 |
| `pump_recovery` returns early when any room has an open cursor | contracts 2, 5, 7, 8 |
| abandonment never recorded in memory | contract 6, restart durability |
| abandonment reported but not sticky | contract 6, restart durability |
| abandonment never persisted | contract 6, restart durability |
| acknowledgement not persisted | restart durability |
| durability ignored in the ack gate | the fencing safety test |
| reset does not reload from the store | the reset-preservation test |
| rejected Classic recovery writes reach the store before acknowledgement | reset/restart callback and room-state replay |
| real-gap clear omits sticky abandonment | low-level clear and leave/forget restart tests |
| unknown stores inherit durability or atomic recovery | volatile and queue-backed capability tests |
| Sliding applies while persisted Classic state is staged | direct and queued-lock settlement-fence tests |

Two tests were strengthened after surviving a mutation:

- Contract 7 originally used only an empty last page, which has nothing to reclassify, so it
  survived the `bounded_exhausted` mutation. It is now parametrized; the `final_events` case
  carries events on the page that omits `end`.
- `test_reset_classic_sync_state_preserves_a_durable_recovery_lane` initially passed for the wrong
  reason: `RoomMessagesError` was not imported, and `_collect_slice` swallowed the resulting
  `NameError` as a generic recovery failure. The import is now present, so the 429 path is the one
  actually exercised.

## LOC

Against the original `89231f5` analysis baseline, the final source diff is
**+316 / -19 in `src/`** across `sync_recovery.py`, `async_client.py`,
`store/database.py`, `store/models.py`, and `store/__init__.py`, as reported by
`git diff --numstat 89231f5 -- src` after final verification.

## What MindRoom can delete

Unchanged from the analysis that motivated this branch, including its caveat.

### Deletable outright

- `src/mindroom/matrix/sync_recovery_escape.py` (98 lines) and `tests/test_sync_recovery_escape.py`.
  Its docstring states its own premise: "certification fails closed, and the Classic cursor rewinds
  to the last certified checkpoint." That no longer happens for an unrecovered room, so there is no
  unchanging checkpoint to count failures against.

- The stall-debt trust branches in `src/mindroom/matrix/sync_checkpoint_trust.py`:
  `_recovery_stalls`, `_observe_recovery_stalls`, `_report_skipped_recovery_gaps`,
  `_record_skipped_history_debt`, and the `history_debt_provider` field.

- The `skipped_recovery_room_ids` plumbing in `src/mindroom/matrix/sync_certification.py` and the
  `sync_recovery_incomplete` arm of `_uncertain_reason`. The `admission_refused` and
  `missing_next_batch` arms must stay: those are genuine fail-closed conditions unrelated to
  recovery.

- `src/mindroom/event_journal/history_debt.py` (187 lines), the `room_history_debt` table, its
  journal registration, the three store methods, the `LEFT JOIN room_history_debt` in `reads.py`,
  and the repayment path in `conversation_hydration.py`.

### The caveat

The debt machinery is not purely redundant. It also covers the cases where **nio itself abandons**
a gap and never refetches: the `backfill_max_events` room cap, a hard non-retryable `/messages`
rejection, and an unverifiable page where `response.end == cursor`. MindRoom today turns those into
a debt and gets the messages back as context. After deletion it gets nothing for them.

That is still the right trade -- the events come back as *work* with `RECOVERED` provenance in
every case that is actually recoverable -- but it is not a no-loss deletion. `acknowledge_unrecovered_rooms`
and the sticky flag exist so the residue cannot become silent: MindRoom should surface a
persistently unrecovered room rather than treating it as healthy.

### Required MindRoom-side change

`matrix_sync.mode == "classic"` currently passes `persist_recovery=False`, which was the only
choice available while durability and checkpoint ownership were coupled. It must become `True`, or
the checkpoint keeps being fenced by design -- correctly, because without persistence there is
nothing to advance past safely.

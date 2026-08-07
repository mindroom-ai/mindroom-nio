# Per-room limited-timeline recovery: contract, red tests, and design

Written against `89231f5` (`fork: release 0.37.0`).
Branch: `per-room-recovery`.
Tests: `tests/per_room_recovery_contract_test.py`.

## Summary

The premise that motivated this task — "nio cannot do per-room recovery" — is mostly wrong, and that is the most useful finding here.

`src/nio/client/sync_recovery.py` is already a per-room recovery engine.
Gaps are keyed per room, carry their own bounded endpoints, retry across pumps, hold later same-room events behind the gap, and promote recovered events to `TimelineEventProvenance.RECOVERED` when continuity is proven.
Five of the eight required contracts already hold, and I have mutation-tested each one to confirm the tests observe real behaviour rather than passing by accident.

What is broken is not the engine but the **boundary between the engine and an application that owns the sync checkpoint**.
Three defects there, all narrow, produce the divergent-gap cycle:

| # | Defect | Where |
| --- | --- | --- |
| A | Acknowledgement is gated on *global* recovery work, so one room's gap freezes the checkpoint for every room | `async_client.py:841`, `async_client.py:1617` |
| B | Durable gap rows and application-owned checkpoint are mutually exclusive by configuration | `async_client.py:753-765`, `:827`, `:876` |
| C | Abandonment is reported exactly once, then the room reads as healthy forever | `sync_recovery.py:995` (`take_recovery_outcomes`) |

Defect A is the direct cause of the cycle described in `mindroom/src/mindroom/matrix/sync_recovery_escape.py`.
Defect B is why it cannot simply be removed: acknowledging past a gap is only safe if the gap outlives the process.

## The exact contract, against real names

Stated for a client configured the way an application that owns its own durable cursor runs it: `backfill_limited_timelines=True`, `store_sync_tokens=False`, `backfill_persist_recovery=False`.

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

## Test results

```bash
cd /Users/bas.nijholt/Work/dev/mindroom-nio
uv run pytest tests/per_room_recovery_contract_test.py -p no:randomly --no-header -q
```

`3 failed, 6 passed` (contract 7 is parametrized over two page shapes).
Full log: run the command above; exit code 1.

### RED — contracts 1, 3, 6

**Contract 1** — `test_unresolved_gap_does_not_block_the_global_cursor`

```
tests/per_room_recovery_contract_test.py:226: in test_unresolved_gap_does_not_block_the_global_cursor
    client.acknowledge_classic_sync("s1")
E   nio.exceptions.LocalProtocolError: Classic Sync acknowledgement token does not match the staged response.
```

The room's gap is open, so `has_pending_recovery_work(self._recovery)` is true and `async_client.py:841` refuses.
This is the cycle: the application cannot commit, so its next request repeats the same `since` against a live position that has moved.

**Contract 3** — `test_recovery_obligation_survives_a_restart`

```
tests/per_room_recovery_contract_test.py:297: in test_recovery_obligation_survives_a_restart
    assert ROOM_A in restarted._recovery.gaps, "the gap must outlive the process"
E   AssertionError: the gap must outlive the process
E   assert '!a:example.org' in {}
E    +  where {} = RecoveryState(gaps={}, events={}, completed={}, outcomes={}, room_offset=0, max_held_events=200).gaps
```

`_recovery_store` returns `None` because `_recovery_persistence_enabled` is false, so nothing was written.
Setting `backfill_persist_recovery=True` would persist the gap but makes `acknowledge_classic_sync` raise outright — there is no configuration in which contracts 1 and 3 both hold.

**Contract 6** — `test_abandoned_gap_stays_explicitly_degraded`

```
tests/per_room_recovery_contract_test.py:406: in test_abandoned_gap_stays_explicitly_degraded
    assert ROOM_A in second.unrecovered_room_ids, (
E   AssertionError: an unrepaid loss must not read as a healthy room on the next response
E   assert '!a:example.org' in frozenset()
------------------------------ Captured log call -------------------------------
ERROR    nio.client.sync_recovery:sync_recovery.py:1272 Abandoning failed gap in !a:example.org
```

`take_recovery_outcomes` drains `state.outcomes` on read, so a permanent loss is announced on exactly one response and never again.

### GREEN — contracts 2, 4, 5, 7, 8

These already hold. They are kept as regression tests, not as documentation of intent.

| Test | Contract |
| --- | --- |
| `test_other_rooms_keep_delivering_while_one_room_is_blocked` | 2 |
| `test_later_events_do_not_overtake_an_open_gap` | 4 |
| `test_recovered_events_arrive_once_in_order_with_provenance` | 5 |
| `test_page_without_end_closes_a_bounded_walk_as_exhausted[empty, final_events]` | 7 |
| `test_transient_failure_retries_without_loss_or_duplication` | 8 |

Each was mutation-tested against `sync_recovery.py`, because a green test proves nothing until you know it can fail.

| Mutation | Newly failing |
| --- | --- |
| `_drain_gap` drains a gap whose `cursor_token` is still set | contracts 2, 4, 5, 8 |
| `_promote_recovered_continuity` returns events unchanged | contracts 5, 7 (`final_events`), 8 |
| `bounded_exhausted` forced false | contract 7 (`final_events`) |
| `_collect_slice` abandons on 408/429/5xx instead of breaking | contracts 2, 4, 5, 8 |
| `pump_recovery` returns early when any room has an open cursor | contracts 2, 5, 7, 8 |

The first version of the contract-7 test used only an empty last page and survived the `bounded_exhausted` mutation, because an empty chunk has nothing to reclassify.
It is now parametrized: the `final_events` case carries events on the page that omits `end`, which is what makes the `RECOVERED` versus `HISTORY` decision observable.

## Smallest design

Four changes. Steps 1-3 are one indivisible unit; step 4 is separable.

**Step 2 is unsafe without steps 1 and 3.** Acknowledging past a gap is only correct if that gap outlives both a crash and a `reset_classic_sync_state()`. Shipping the gate removal alone converts a livelock into silent data loss.

### 1. Separate gap durability from token ownership

`_recovery_persistence_enabled` currently answers two questions at once, and `acknowledge_classic_sync` (`:827`), `reset_classic_sync_state` (`:876`), and `clear_persisted_sync_recovery` (`:789`) all reject on it.
Change those three guards to reject on `self.config.store_sync_tokens` alone.

The plumbing already exists: `persist_response_plan` is called with `token=(response.next_batch if self.config.store_sync_tokens else None)` (`async_client.py:1556`), so recovery rows are already written without a sync token.

~6 lines changed, 0 added.

### 2. Drop the global recovery gate from acknowledgement

Delete `has_pending_recovery_work(self._recovery)` from the two acknowledgement conditions (`:841` and `:1617`), leaving `self._recovery._active_dispatches`, which is the genuine constraint — an in-flight callback whose durable acceptance has not been marked.

**I probed this change against the existing suite.** With both terms removed, `tests/backfill_test.py`, `tests/sync_recovery_test.py`, and this new file give `2 failed, 342 passed` — the two remaining failures are contracts 3 and 6, and contract 1 turns green.
Not one existing test depends on the global gate.

~4 lines removed.

### 3. `reset_classic_sync_state` must preserve committed gaps

The inner `reset()` replaces `self._recovery` wholesale (`:927`).
Once a checkpoint can be acknowledged past an open gap, that discards obligations no replay will recreate.
Replace the construction with a reload from the store:

```python
self._recovery = RecoveryState(max_held_events=self.config.backfill_max_events)
store = self._recovery_store
if store:
    load_recovery_state(self._recovery, *store.load_sync_recovery())
```

Rows written by the rejected response are on disk too, but they are keyed by their own bounded tokens rather than by the rejected cursor, so replaying the same response re-plans the same rows and `apply_plan` deduplicates by `event_id`.
This is the only step whose correctness is not obvious from a guard change, and it needs its own test.

~8 lines added.

### 4. Sticky abandonment

Add `RecoveryState.abandoned: set[str]`, populated from `plan.unrecovered_room_ids` in `apply_plan`, unioned into the `unrecovered` half of `take_recovery_outcomes`, and cleared only by a new `acknowledge_unrecovered_rooms(room_ids)`.
Persist it as one boolean column on the existing recovery-gaps table rather than a new table.

~40 lines in `sync_recovery.py`, ~30 in `store/database.py`.

### What this deliberately does not build

No scheduler thread: `pump_recovery` is already driven from `_handle_joined_rooms` and the tail of `_handle_sync`.
No journal, no outbox, no admission ledger: the durable unit stays one `RecoveryGap` row plus its `PendingTimelineEvent` rows, which is what the store already holds.
No retry-count or backoff state: a gap that keeps failing keeps its bounded cursor and is retried by the next pump, which is what contract 8 already does.

### Estimated nio LOC

**~90 added, ~10 removed**, across `src/nio/client/sync_recovery.py`, `src/nio/client/async_client.py`, and `src/nio/store/database.py`, plus the schema migration for the abandoned-room column.
Steps 1-3 alone are ~18 added and ~10 removed.

## What MindRoom can delete

I traced every consumer. The answer is yes for the escape and yes with one caveat for the debt.

### Deletable outright

- **`src/mindroom/matrix/sync_recovery_escape.py` (98 lines)** and `tests/test_sync_recovery_escape.py`.
  Its docstring states its own premise exactly: "certification fails closed, and the Classic cursor rewinds to the last certified checkpoint."
  Under contract 1 the cursor does not rewind for an unrecovered room, so there is no unchanging checkpoint to count failures against.
  `_CLASSIC_SYNC_RECOVERY_STALL_LIMIT`, `SkippedRecoveryGap`, and `SyncRecoveryStallTracker` all lose their reason to exist.

- **The stall-debt trust branches** in `src/mindroom/matrix/sync_checkpoint_trust.py`: `_recovery_stalls`, `_observe_recovery_stalls`, `_report_skipped_recovery_gaps`, `_record_skipped_history_debt`, and the `history_debt_provider` field (`bot.py:434`).

- **The `skipped_recovery_room_ids` plumbing** in `src/mindroom/matrix/sync_certification.py`: the parameter on `certify_sync_response`, the field on `SyncCertificationDecision`, and the `reset_client_token=bool(skipped_recovery_room_ids)` branch.
  The `sync_recovery_incomplete` arm of `_uncertain_reason` goes with it — an unrecovered room no longer makes a response uncertain, because its gap is durable and independent of the cursor.
  The `admission_refused` and `missing_next_batch` arms must stay: those are genuine fail-closed conditions unrelated to recovery.

- **`src/mindroom/event_journal/history_debt.py` (187 lines)**, the `room_history_debt` table (`schema.py:149`), its `journal.py:131` registration, the `HistoryDebtOutcome` / `RoomHistoryDebt` / `HistoryDebtRecordView` exports, the three store methods (`store.py:348-378`, `_repay_history_debt` at `:727`), the `LEFT JOIN room_history_debt` in `reads.py:319`, and the repayment path in `conversation_hydration.py` (`_repay` at `:353` and the `:280` trigger).

### The caveat, stated plainly

The debt machinery is not purely redundant.
It also covers the cases where **nio itself abandons a gap** and never refetches: the `backfill_max_events` room cap, a hard non-retryable `/messages` rejection, and an unverifiable page where `response.end == cursor`.
Today MindRoom converts those into a debt and gets the messages back as context.
After deletion it gets nothing for them.

That is still the right trade — 187 lines of ledger replaced by a boolean, and the events come back as *work* rather than context in every case that is actually recoverable — but it is not a no-loss deletion, and the residual cases need a decision rather than silence.
The options are to raise `backfill_max_events`, or to surface the sticky flag from step 4 as an operator-visible degraded-room signal.
Step 4 exists precisely so that this residue cannot become silent.

### Sequencing

MindRoom cannot delete anything until nio ships steps 1-3 and the fork pin moves.
Step 4 and the MindRoom deletions can land together, since the sticky flag is what replaces the debt's "we know we lost something" role.

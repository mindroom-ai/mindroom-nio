# Per-room limited-timeline recovery contract

Original analysis baseline: `89231f5` (`fork: release 0.37.0`).
Implemented on branch: `pr-53-regression-fixes`.
Executable contract: `tests/per_room_recovery_contract_test.py`.

## Summary

`src/nio/client/sync_recovery.py` recovers limited timelines per room. Each
room keeps its own bounded gap, later events in that room wait behind it, and
other rooms continue independently. Continuity-proven history is dispatched as
`TimelineEventProvenance.RECOVERED` through the normal admission and event
callback path.

Classic Sync has one durable owner. Nio either owns both the Classic cursor and
recovery rows, or the application owns the cursor while nio keeps response
recovery state only in memory. Splitting those responsibilities is unsupported
because two independent stores cannot atomically commit one ingress position.

## Classic ownership matrix

`backfill_persist_recovery=None` follows `store_sync_tokens`. With
`backfill_limited_timelines=True`, the supported configurations are:

| `store_sync_tokens` | `backfill_persist_recovery` | Owner | Contract |
| --- | --- | --- | --- |
| `False` | `None` or `False` | Application | The application owns the Classic cursor. Nio stages one response in memory, writes no recovery rows, and allows acknowledgement only after recovery work finishes. |
| `True` | `None` or `True` | Nio | Nio atomically persists the Classic cursor and its recovery plan in one store operation. |
| `False` | `True` | Split | Unsupported. Rejected before a Classic request and before a supplied Classic response mutates state. |
| `True` | `False` | Split | Unsupported. Rejected at the same two boundaries. |

Pure Simplified Sliding Sync can persist its nio-owned room-window recovery
state. That does not make an application-owned Classic cursor compatible with
persisted nio recovery; a client that processes Classic responses must select
one of the two supported Classic rows above.

### Application-owned Classic

`acknowledge_classic_sync(next_batch)` performs no nio store I/O. It accepts
only the current staged token and refuses acknowledgement while a room gap, a
deferred recovery error, or active recovery dispatch still exists.

If the application rejects the response, `reset_classic_sync_state()` drains
started callback work and clears the response's in-memory rooms and recovery
state. The application then reinstalls its last committed cursor and replays
from there. Nio does not persist gaps, completion markers, abandonment, or
Sliding baselines while this Classic mode owns the client.

### Nio-owned Classic

`persist_response_plan(..., token=response.next_batch)` writes the Classic
cursor, gaps, pending timeline events, abandonment, and invalidated Sliding
baselines in one atomic store transaction before the equivalent in-memory
recovery transition. A replacement client restores the cursor and remaining
room obligations from that store.

## Room-local recovery contracts

1. **A gap blocks only its room.**
   A `RecoveryGap` pins the `cursor_token` and `target_token` from the limited
   response that created it. Other rooms continue to plan, recover, and
   dispatch while that bounded walk is incomplete.

2. **Same-room ordering survives the gap.**
   Later same-room timeline events queue until the open cursor closes. Once
   continuity is proven, recovered history is emitted exactly once and before
   the later queued events for that room.

3. **A membership boundary can prove the walk complete.**
   A membership-bound `/messages` page with `end=None` is a terminus. Events on
   that final page are retained and promoted to `RECOVERED` when continuity is
   proven.

4. **Transient fetch failures retry the same bounded cursor.**
   HTTP 408, 429, and 5xx responses leave `cursor_token` unchanged. A later
   recovery pump retries from that exact start and dispatches every recovered
   event once. Contract fakes fail on every unscripted fetch, so an accidental
   extra or reordered request cannot pass silently.

5. **Every real-gap clear records sticky loss.**
   Before `persist_response_plan()` writes or mutates memory, it normalizes
   every cleared room with a non-empty target-token gap into
   `unrecovered_room_ids`. This single boundary covers recovery resets,
   `room_leave()`, `room_forget()`, and other real-gap clears.

6. **Abandonment remains visible until the application settles it.**
   `RecoveryState.abandoned` is included in every later response's
   `unrecovered_room_ids`. Only `acknowledge_unrecovered_rooms()` removes the
   named rooms; failed store writes leave the in-memory marker intact so the
   acknowledgement can be retried.

7. **Sticky abandonment survives restart in store version 9.**
   `SyncRecoveryAbandonedRooms` is separate from `SyncRecoveryGaps` because
   loss outlives the gap row that was cleared. The v8-to-v9 migration creates
   the abandonment table without losing existing gap or pending-event rows.

## Atomic recovery-store contract

`MatrixStore.supports_atomic_recovery` defaults to `False`.
`MatrixStore.__init_subclass__()` resets it to `False` unless each subclass
declares the capability in its own class body. `DefaultStore`, `SqliteStore`,
and `SqliteMemoryStore` explicitly declare support. A subclass that changes
its backend must audit and redeclare the capability; the backend's name or
parent class is not treated as evidence.

Recovery-enabled sync response application rejects a non-atomic store before
response ordering, room state, cursor rows, or recovery rows mutate. Real-gap
clears, membership resets, and abandonment settlement likewise fail before
their store or in-memory transitions. `SqliteQueueDatabase` is intentionally
not opted in because its writer cannot provide the required multi-statement
transaction.

Atomic capability says that one recovery transition is indivisible. It does
not claim that an in-memory database survives process recreation.

## Explicit non-goals

- Durable application-owned Classic gaps are unsupported. Supporting them
  requires an application-controlled transaction boundary not provided here.
- Application-owned Sliding checkpoints and Classic/Sliding cursor conversion
  are unsupported.
- Arbitrary ordinary callback side effects are not made exactly-once by
  `acknowledge_classic_sync()`. Admission owners must remain idempotent by
  event ID, and ordinary callback failures retain their documented live versus
  recovered behavior.
- To-device processing is not part of the Classic cursor acknowledgement.
  Classic and Sliding Sync cannot both consume the to-device stream in one
  client generation because their cursor formats are incompatible.
- Room caches and crypto state are not rolled back as part of application-owned
  Classic acknowledgement.

## Verification

The completed release gate produced:

- `23 passed` in the focused per-room recovery contract;
- `945 passed, 3 skipped, 2 warnings` in the full repository suite;
- `29/29` wire-format and encryption live checks;
- `60/60` events dispatched with no missing or duplicate events in each of the
  Classic and Sliding client-restart probes; and
- `21/21` reduced recovery-slam checks, including 12 encrypted events decrypted
  and dispatched exactly once.

```bash
uv run pytest -q tests/per_room_recovery_contract_test.py
uv run pre-commit run --files \
  .claude/NIO_RECOVERY_CONTRACT.md \
  scripts/live_sliding_sync_check.py \
  tests/live_sliding_sync_check_test.py
git diff --check
uv run pytest -q
```

Against the original `89231f5` analysis baseline, the final production source
diff is `+194 / -24` across `async_client.py`, `sync_recovery.py`,
`store/__init__.py`, `store/database.py`, and `store/models.py`.

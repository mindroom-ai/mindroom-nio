# Durable Recovery Regression Fixes

## Scope

Fix the verified PR 53 correctness defects without inventing a transaction
protocol between nio's store and an application's checkpoint store:

- Classic Sync cursor and recovery state must have one durable owner;
- clearing a real room gap must persist sticky abandonment;
- a store used for nio-owned recovery must explicitly support atomic writes;
- the v9 migration and cleanup tests must exercise abandonment;
- recovery fetch tests must reject unscripted calls and cover transient retries;
- the live recovery recorder must exactly mirror the dispatch signature.

Prepared snapshots, checkpoint promotion, and application checkpoint blobs are
not part of this change.

## One-owner Classic boundary

Limited-timeline Classic Sync supports two configurations:

1. With `store_sync_tokens=True`, nio owns both the Classic cursor and recovery
   rows. `backfill_persist_recovery` must be `None` or `True`, and the token,
   gaps, pending events, abandonment, and Sliding baselines use nio's atomic
   store operations.
2. With `store_sync_tokens=False`, the application owns the Classic cursor and
   nio writes no recovery rows. `backfill_persist_recovery` must be `None` or
   `False`. The existing staged-response API remains memory-only, and
   `acknowledge_classic_sync()` refuses to advance while any gap or deferred
   recovery failure remains.

The two mixed configurations are rejected before a Classic HTTP request is
sent and again before a manually supplied Classic response can mutate state:

- nio-owned cursor with recovery persistence disabled;
- application-owned cursor with nio recovery persistence enabled.

There is no safe ordering for separate cursor and recovery writes. If the
application writes its cursor first, a crash can lose an unstored gap. If nio
writes recovery first, a crash can load completion markers from a response the
application will replay. Making that hybrid safe requires an application-owned
checkpoint backend or another explicit transaction design and belongs in a
separate change.

Pure Sliding Sync may continue to persist its nio-owned room-window recovery
state. A client configured for persisted Sliding recovery cannot process
application-owned Classic Sync without first choosing a valid Classic
ownership configuration.

## Gap clearing and abandonment

Before any `RecoveryPlan` is written, `persist_response_plan()` adds every
cleared room containing a real target-token gap to `unrecovered_room_ids`.
Store and memory receive the same normalized immutable plan, so deleting a gap
and recording sticky abandonment is one transition for recovery resets,
`room_leave()`, and `room_forget()`.

Abandonment remains in `RecoveryState.abandoned` and the v9
`SyncRecoveryAbandonedRooms` table until the application explicitly calls
`acknowledge_unrecovered_rooms()`.

## Atomic store boundary

`MatrixStore.supports_atomic_recovery` defaults to `False`, and every subclass
must redeclare the capability rather than inheriting an optimistic value.
`DefaultStore`, `SqliteStore`, and `SqliteMemoryStore` explicitly opt in.
Recovery operations fail before mutation when a custom backend does not opt in;
this excludes `SqliteQueueDatabase`, whose writer intentionally cannot provide
a multi-statement transaction.

This is a capability declaration, not backend-name inference. No separate
durability heuristic is needed once hybrid Classic ownership is rejected.

## Explicit non-goals

- Durable application-owned gaps are not supported by this PR.
- Application-owned Sliding checkpoints and Classic/Sliding cursor conversion
  are not supported.
- Arbitrary callback side effects and to-device processing are not made
  exactly-once by the Classic cursor acknowledgement API.
- Room caches and crypto state are not rollback snapshots.

## Verification

Every production correction is preceded by a regression that fails for the
intended reason. Final verification includes focused ownership, abandonment,
store, retry, and wrapper tests; the complete pytest suite and pre-commit; and
the disposable Tuwunel Classic/Sliding restart and recovery-slam probes.

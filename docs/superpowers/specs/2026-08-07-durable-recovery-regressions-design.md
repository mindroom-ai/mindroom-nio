# Durable Recovery Regression Fixes

## Scope

Fix every verified issue from the PR 53 re-review:

- rejected application-owned Classic Sync responses must replay after reset or restart;
- clearing a real room gap must persist sticky abandonment;
- only explicitly capable stores may authorize checkpoint advancement;
- non-atomic stores must fail before abandonment settlement mutates persistence;
- the v9 migration and cleanup tests must exercise the abandonment table;
- recovery fetch tests must fail on unscripted calls and cover 408, 429, and 5xx retries;
- the live recovery-slam wrapper must match the current dispatch signature.

## Recovery transaction boundary

Application-owned Classic Sync recovery changes are provisional until
`acknowledge_classic_sync()` succeeds. While a response is staged, nio mutates
only the in-memory `RecoveryState`; it does not incrementally mutate the last
acknowledged recovery snapshot in the store.

Acknowledgement writes a complete recovery snapshot atomically and store-first,
then clears the staged-response flag. If the write fails, memory stays staged
and acknowledgement can be retried. Reset and process restart therefore reload
the last acknowledged snapshot: previously committed gaps survive, while
completion markers created by a rejected response do not suppress replay.

Nio-owned Classic tokens and Sliding Sync retain their existing incremental
persistence because their token and recovery state already share one owner and
one transaction boundary.

## Gap clearing and abandonment

Before any `RecoveryPlan` is written, `persist_response_plan()` adds every
cleared room containing a real target-token gap to `unrecovered_room_ids`.
That normalization happens before the store write, so gap deletion and sticky
abandonment are atomic for all callers, including `room_leave()` and
`room_forget()`.

## Store capabilities

`MatrixStore` defaults to neither durable nor atomically recovery-capable.
The built-in disk stores opt into both capabilities; `SqliteMemoryStore` opts
into atomic recovery but remains non-durable. Custom stores must opt in
explicitly. Client acknowledgement and abandonment settlement fail before
mutation when atomic recovery is unavailable.

These are explicit capabilities, not heuristics based on backend class names.

## Verification

Each defect gets a regression test that fails on PR head before implementation.
After targeted tests pass, run the full pytest suite, pre-commit, and the
disposable Tuwunel live checks: wire/encryption checks, Classic and Sliding
restart probes, the multi-room recovery slam, and targeted reset/leave probes.

# Durable Recovery Regressions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make application-owned Classic recovery transactional at acknowledgement, preserve sticky loss across every room clear, and make the store and live-test contracts explicit.

**Architecture:** Stage application-owned Classic response recovery only in memory, then atomically replace the persisted recovery snapshot inside `acknowledge_classic_sync()`. Normalize real-gap clears before persistence, and use explicit store capability flags instead of backend heuristics.

**Tech Stack:** Python 3.10–3.14, asyncio, peewee/SQLite, pytest/pytest-asyncio, pre-commit, local Tuwunel live probes.

## Global Constraints

- No new database schema version: the snapshot uses existing recovery tables.
- Store writes happen before corresponding in-memory commit state changes.
- Rejected responses replay callbacks and room state after reset and process restart.
- Previously acknowledged gaps and completion markers survive later rejected responses.
- Durability and atomic recovery are explicit store capabilities; no concrete-backend heuristic.
- Every production correction is preceded by a red regression test.
- Preserve the dirty `pr-53-abandoned-field` worktree; all work stays in this isolated worktree.

---

### Task 1: Acknowledged recovery snapshots

**Files:**
- Modify: `src/nio/client/sync_recovery.py`
- Modify: `src/nio/client/async_client.py`
- Modify: `src/nio/store/database.py`
- Test: `tests/backfill_test.py`
- Test: `tests/store_test.py`

**Interfaces:**
- Produces: `snapshot_recovery_state(state) -> tuple[tuple[RecoveryGap, ...], tuple[PendingTimelineEvent, ...], frozenset[str]]`.
- Produces: `MatrixStore.save_recovery_snapshot(gaps, events, abandoned_rooms) -> None`.
- Produces: `AsyncClient._recovery_write_store`, which is `None` while an application-owned Classic response is staged.
- `acknowledge_classic_sync()` persists the snapshot before clearing `_classic_sync_state_staged`.

- [ ] **Step 1: Add failing reset and restart tests**

Add tests that configure `backfill_persist_recovery=True` and `store_sync_tokens=False`, dispatch a timeline `RoomNameEvent`, reject the response, then reset or recreate the client at the committed cursor. Assert the same event callback runs again and rebuilt room state has the name. Also assert an older acknowledged open gap remains after rejecting a later response.

- [ ] **Step 2: Run the red tests**

Run:

```bash
uv run pytest -q \
  tests/backfill_test.py::TestRoomLocalRecovery::test_durable_reset_replays_completed_events_from_rejected_response \
  tests/backfill_test.py::TestRoomLocalRecovery::test_durable_restart_replays_completed_events_from_uncommitted_response
```

Expected: both callback-count assertions fail because generation-0 markers suppress replay.

- [ ] **Step 3: Add a failing atomic snapshot store test**

Seed an old gap, pending row, completed marker, and abandonment row. Call `save_recovery_snapshot()` with a different complete state and assert only the replacement state remains. Inject a failure during the replacement and assert the original state remains intact.

- [ ] **Step 4: Implement snapshot serialization and replacement**

Flatten `RecoveryState.gaps`, `RecoveryState.events`, and `RecoveryState.completed`. Encode completed entries as generation-0 `PendingTimelineEvent` markers with empty payloads. Implement `save_recovery_snapshot()` under `@use_database_atomic`: delete the account's three recovery tables, insert gaps and abandonment rows in chunks, then reuse `_upsert_pending_events()` for pending rows and markers.

- [ ] **Step 5: Stage Classic recovery writes and commit on acknowledgement**

While `_classic_sync_state_staged` and `store_sync_tokens=False`, pass no store to Classic plan/pump persistence. In `acknowledge_classic_sync()`, call `save_recovery_snapshot()` before clearing staged state. Keep reset loading the last stored snapshot. Do not change Sliding Sync or nio-owned Classic token persistence.

- [ ] **Step 6: Verify and commit Task 1**

Run the new tests plus existing application-owned reset/restart tests and store recovery tests. Commit as `fix: commit recovery state with application checkpoint`.

### Task 2: Sticky abandonment for every real-gap clear

**Files:**
- Modify: `src/nio/client/sync_recovery.py`
- Test: `tests/per_room_recovery_contract_test.py`
- Test: `tests/backfill_test.py`

**Interfaces:**
- Produces: loss-aware normalization inside `persist_response_plan()` before `store.save_recovery()`.

- [ ] **Step 1: Add failing low-level and membership tests**

Persist a real target-token gap, then persist `RecoveryPlan(clear_rooms={room})`; assert the gap is removed and the room is in both memory and the store's abandonment set. Parameterize successful `room_leave()` and `room_forget()` to assert the same state survives a replacement client.

- [ ] **Step 2: Run the red tests**

Expected: the gap disappears, while `state.abandoned` and the persisted abandonment list are empty.

- [ ] **Step 3: Normalize the plan before persistence**

Before the store call, union into `plan.unrecovered_room_ids` every `clear_rooms` entry whose current state has a gap with a non-empty `target_token`. Use `dataclasses.replace()` so store and memory receive exactly the same normalized immutable plan.

- [ ] **Step 4: Verify and commit Task 2**

Run the new tests and all per-room recovery contract tests. Commit as `fix: persist loss when clearing real recovery gaps`.

### Task 3: Explicit store safety and migration coverage

**Files:**
- Modify: `src/nio/store/database.py`
- Modify: `src/nio/client/async_client.py`
- Test: `tests/store_test.py`
- Test: `tests/per_room_recovery_contract_test.py`

**Interfaces:**
- Produces: `MatrixStore.is_durable = False` and `MatrixStore.supports_atomic_recovery = False` defaults.
- Built-in disk stores set both capabilities to `True`; `SqliteMemoryStore` remains non-durable but inherits atomic recovery.
- Client recovery snapshot commit and abandonment acknowledgement reject stores without atomic recovery before mutation.

- [ ] **Step 1: Add failing custom volatile and non-atomic store tests**

Create a `MatrixStore` subclass backed by `SqliteDatabase(':memory:')` without capability overrides. Assert an open gap fences Classic acknowledgement. Create a queue-backed store, seed more than one deletion chunk of abandoned rooms, and assert client acknowledgement fails before deleting any row.

- [ ] **Step 2: Run the red tests**

Expected: the volatile store incorrectly reports durable and the queue-backed clear can partially mutate persistence.

- [ ] **Step 3: Implement explicit capabilities and fail-fast checks**

Set base defaults false, opt `DefaultStore` and `SqliteStore` into durability and atomic recovery, and leave `SqliteMemoryStore.is_durable=False`. Before snapshot commit or abandonment clear through `AsyncClient`, raise `LocalProtocolError` when `supports_atomic_recovery` is false.

- [ ] **Step 4: Strengthen v9 migration and cleanup tests**

Make the v8 fixture drop `SyncRecoveryAbandonedRooms` before reopening, then assert migration recreates it while preserving v8 gaps/events. Seed abandonment in `_clear_sync_recovery()` success and rollback tests and assert the third loaded value is cleared or rolled back with the rest.

- [ ] **Step 5: Make contract fetch tests strict**

Replace the swallowed bare assertion in `RecordingFetch` with `pytest.fail('unscripted recovery fetch')`, assert each scripted response list is empty after use, and parameterize eventual exact-once retry coverage over HTTP 408, 429, and 503.

- [ ] **Step 6: Verify and commit Task 3**

Run `tests/store_test.py`, `tests/per_room_recovery_contract_test.py`, and relevant backfill acknowledgement tests. Commit as `fix: require explicit atomic recovery stores`.

### Task 4: Live harness and end-to-end verification

**Files:**
- Modify: `scripts/live_sliding_sync_check.py`
- Modify: `tests/live_sliding_sync_check_test.py`
- Modify: `.claude/NIO_RECOVERY_CONTRACT.md`

**Interfaces:**
- `Recorder.observe_recovery()` mirrors all nine `_dispatch_timeline_event` arguments and records the `is_live` argument before forwarding unchanged.

- [ ] **Step 1: Add and run a red forwarding test**

Install the recorder on a fake client, call the wrapped dispatch with `(room_id, event, was_completed, kind, provenance, is_live, apply_room_state, admission_accepted, mark_admission_accepted)`, and assert all nine values reach the original callable and lane recording uses `is_live`. Expected before the fix: `TypeError` from the obsolete seven-argument wrapper.

- [ ] **Step 2: Repair the wrapper and run its unit tests**

Mirror the production signature exactly and forward every argument in order.

- [ ] **Step 3: Update contract documentation**

Describe acknowledgement-time snapshots, loss-aware clears, and explicit store capabilities. Replace stale test counts and diff metrics only after final commands produce the values.

- [ ] **Step 4: Run static and full-suite verification**

Run pre-commit on changed files and `uv run pytest -q`. Require zero failures.

- [ ] **Step 5: Run disposable live verification**

Start the cached Tuwunel image on loopback with a temporary database. Run the 29-check wire/encryption script, Classic and Sliding concurrent restart probes, the reduced multi-room recovery slam without monkeypatching, and the targeted rejected-response replay and room-leave persistence probes. Stop the container and remove only its validated temporary test directory.

- [ ] **Step 6: Commit final tests/docs**

Commit as `test: cover durable recovery failure boundaries`.

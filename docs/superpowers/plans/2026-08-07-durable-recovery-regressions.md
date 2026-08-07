# Durable Recovery Regressions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce one durable owner for Classic cursor and recovery state, preserve sticky room-loss reporting, and keep nio-owned recovery atomic.

**Architecture:** Reject mixed Classic ownership before network or response mutation. Application-owned Classic recovery remains entirely in memory and cannot acknowledge an open gap; nio-owned Classic persists its cursor and recovery together. Keep sticky abandonment and explicitly gate custom stores that cannot make recovery writes atomic.

**Tech Stack:** Python 3.10–3.14, asyncio, peewee/SQLite, pytest/pytest-asyncio, pre-commit, disposable Tuwunel live probes.

## Global Constraints

- Do not add a prepared-snapshot table, checkpoint blob, or schema version 10.
- Do not write recovery rows when the application owns the Classic cursor.
- Reject invalid Classic ownership before `_send()` and before response mutation.
- Store writes precede their corresponding in-memory recovery transitions.
- Every production correction gets a RED regression first.
- Preserve unrelated worktrees and Docker resources.

---

### Task 1: One-owner Classic configuration

**Files:**
- Modify: `src/nio/client/async_client.py`
- Modify: `src/nio/client/sync_recovery.py`
- Test: `tests/backfill_test.py`
- Test: `tests/per_room_recovery_contract_test.py`

**Interfaces:**
- Produces: `AsyncClient._require_valid_classic_recovery_ownership() -> None`.
- Application-owned acknowledgement uses `has_pending_recovery_work()` and performs no store I/O.
- Nio-owned Classic continues using `persist_response_plan(..., token=response.next_batch)`.

- [ ] **Step 1: Add request-side RED tests**

Parameterize the two mixed configurations:

```python
(
    {"store_sync_tokens": False, "backfill_persist_recovery": True},
    {"store_sync_tokens": True, "backfill_persist_recovery": False},
)
```

Patch `_send` with a function that calls `pytest.fail()`, invoke `sync()`, and
assert `LocalProtocolError` names the ownership conflict before `_send` runs.

- [ ] **Step 2: Add receive-side RED tests**

For the same configurations, pass a parsed `SyncResponse` to
`receive_response()`. Assert the exception occurs while `next_batch`, rooms,
recovery state, stored token, stored gaps, and callbacks remain unchanged.

- [ ] **Step 3: Add supported-mode regressions**

Pin both valid modes:

- application-owned, non-persisted Classic replays a rejected response after
  reset and refuses acknowledgement while an open gap remains;
- nio-owned Classic atomically persists the response token and real gap, and a
  replacement client restores the gap.

- [ ] **Step 4: Implement the minimal ownership gate**

Compute Classic recovery ownership only when limited-timeline recovery is on.
Require `store_sync_tokens == _recovery_persistence_enabled`. Call the helper
at the beginning of `sync()` and inside the serialized response path before
ordering floors or client state mutate.

Restore application-owned staging to the memory-only condition. Remove
`_recovery_write_store`, acknowledgement-time `save_recovery_snapshot()`, reset
reloads from nio's store, and durability-dependent acknowledgement logic.

- [ ] **Step 5: Run Task 1 tests**

Run the new nodes plus existing application-owned reset/acknowledgement and
nio-owned restart tests. Require all selected tests to pass.

### Task 2: Sticky abandonment for every real-gap clear

**Files:**
- Modify: `src/nio/client/sync_recovery.py`
- Test: `tests/per_room_recovery_contract_test.py`
- Test: `tests/backfill_test.py`

**Interfaces:**
- `persist_response_plan()` normalizes real-gap clears before store or memory mutation.

- [ ] **Step 1: Preserve the low-level RED/GREEN regression**

Persist `RecoveryGap(room, generation, target_token, cursor_token)`, apply a
plan with `clear_rooms={room}`, and assert both memory and store replace the gap
with sticky abandonment.

- [ ] **Step 2: Preserve membership restart regressions**

For successful `room_leave()` and `room_forget()` after a nio-owned real gap,
assert the replacement client loads no gap and does load abandonment.

- [ ] **Step 3: Keep one normalization path**

Use `dataclasses.replace()` to union current real-gap clear rooms into
`plan.unrecovered_room_ids` before calling the store. Do not add planner- or
membership-specific duplicates.

- [ ] **Step 4: Verify all per-room recovery contracts**

Run `tests/per_room_recovery_contract_test.py` and the direct membership nodes.

### Task 3: Explicit atomic recovery stores and storage coverage

**Files:**
- Modify: `src/nio/store/database.py`
- Modify: `src/nio/client/async_client.py`
- Test: `tests/store_test.py`
- Test: `tests/per_room_recovery_contract_test.py`

**Interfaces:**
- Produces: `MatrixStore.supports_atomic_recovery: ClassVar[bool] = False`.
- Every subclass must redeclare the capability; built-in SQLite stores declare `True`.

- [ ] **Step 1: Add capability RED tests**

Define a `SqliteStore` subclass that changes `_create_database()` to an
in-memory or `SqliteQueueDatabase` backend without declaring the capability.
Assert its class flag is `False` and recovery rejects it before any row or
in-memory mutation. Assert the built-in capability matrix explicitly.

- [ ] **Step 2: Implement non-inheriting opt-in**

Add `MatrixStore.__init_subclass__()` that resets
`supports_atomic_recovery` to `False` unless the subclass declares it in its
own class body. Explicitly opt `DefaultStore`, `SqliteStore`, and
`SqliteMemoryStore` into atomic recovery.

- [ ] **Step 3: Gate recovery operations before mutation**

Reject a non-atomic recovery store before sync response application,
loss-aware plan persistence, membership-reset network I/O, or abandonment
settlement. Do not infer capability from a database implementation name.

- [ ] **Step 4: Keep migration, rollback, and retry coverage**

Make the v8 fixture genuinely lack the abandonment table, verify v8→v9
creation preserves existing rows, include abandonment in clear success and
rollback tests, fail unscripted recovery fetches with `pytest.fail()`, and
cover exact-once retry after 408, 429, and 503.

- [ ] **Step 5: Run store and contract suites**

Run `tests/store_test.py` and `tests/per_room_recovery_contract_test.py`.

### Task 4: Live wrapper, documentation, and release gate

**Files:**
- Modify: `scripts/live_sliding_sync_check.py`
- Modify: `tests/live_sliding_sync_check_test.py`
- Modify: `.claude/NIO_RECOVERY_CONTRACT.md`

**Interfaces:**
- `Recorder.observe_recovery()` exactly mirrors and forwards the nine production parameters, including the `sync_origin` keyword.

- [ ] **Step 1: Finish the wrapper regression**

Invoke the wrapper once positionally and once with all production keyword
names. Use distinct sentinel objects for every boolean-position value. Assert
the original callable receives the exact tuple both times and lane recording
uses `sync_origin`.

- [ ] **Step 2: Update the recovery contract**

Replace acknowledgement-snapshot claims with the one-owner configuration
matrix. Retain sticky-abandonment, v9 migration, atomic-store, and retry
contracts. State the unsupported hybrid and callback/to-device non-goals.

- [ ] **Step 3: Run static and full verification**

Run pre-commit on every changed file, `git diff --check`, and `uv run pytest -q`.

- [ ] **Step 4: Run disposable live verification**

Run the 29-check wire/encryption harness, Classic and Sliding restart probes,
and the reduced recovery slam against a uniquely named loopback Tuwunel
container. Remove only that container and validated temporary directory.

- [ ] **Step 5: Independent review, commit, and push**

Obtain a fresh whole-diff review of the simplified tree, commit only intended
files, fast-forward `origin/per-room-recovery`, and verify PR #53's head SHA.

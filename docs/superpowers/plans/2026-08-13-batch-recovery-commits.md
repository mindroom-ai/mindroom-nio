# Batch Recovery Commits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in timeline batch-admission path that replaces per-event recovery acceptance and completion commits with one transaction per ordered batch.

**Architecture:** The existing single-event admission path stays unchanged.
The new path prepares one room-local timeline prefix, invokes one application batch admission callback, atomically marks the rows accepted, fans them out in order, and atomically records the completed prefix.

**Tech Stack:** Python 3.10+, asyncio, Peewee, SQLite, pytest, Ruff, Black, Mypy.

## Global Constraints

The optimization must remain opt-in because grouping completion commits widens the crash replay window for ordinary callbacks.

Do not relax SQLite journal or synchronous durability settings.

Do not add a transaction coordinator, timer, background worker, dependency, or production workload generator.

Keep the existing single-event API and event-by-event durability unchanged.

Use only the existing fuzz and stress scripts for live workload validation.

---

## File Map

`src/nio/client/timeline_admission.py` owns the public immutable batch entry, disposition enum, and callback type.

`src/nio/client/async_client.py` owns callback registration, timeline preparation, application admission, and ordinary fanout.

`src/nio/client/sync_recovery.py` owns ordered batch selection, in-memory recovery transitions, failure classification, and store calls.

`src/nio/store/database.py` owns atomic batch acceptance and completion SQL.

`src/nio/__init__.py` and `src/nio/client/__init__.py` export the public entry and disposition types.

`tests/store_test.py` protects transaction atomicity and stored completion semantics.

`tests/backfill_test.py` protects public callback behavior, ordering, retries, and restart behavior.

`tests/sync_recovery_test.py` protects recovery-layer partial completion and non-timeline fallback behavior.

### Task 1: Public Batch Admission Contract

**Files:**

- Create: `src/nio/client/timeline_admission.py`
- Modify: `src/nio/client/async_client.py`
- Modify: `src/nio/client/__init__.py`
- Modify: `src/nio/__init__.py`
- Test: `tests/backfill_test.py`

**Interfaces:**

- Produces: `TimelineAdmissionEntry(room: MatrixRoom, event: Event, provenance: TimelineEventProvenance)`.
- Produces: `TimelineAdmissionDisposition.FANOUT` and `TimelineAdmissionDisposition.NON_SEMANTIC`.
- Produces: `TimelineBatchAdmissionCallback` returning an ordered tuple of dispositions, synchronously or asynchronously.
- Produces: `AsyncClient.add_event_batch_admission_callback(callback, cb_filter=None) -> None`.

- [ ] **Step 1: Write failing registration tests**

Add tests that register each API form first and assert that registering the other form raises `LocalProtocolError`.

```python
async def test_batch_admission_rejects_simultaneous_admission_owners(self, client):
    def admit_batch(entries):
        return (TimelineAdmissionDisposition.FANOUT,) * len(entries)

    async def admit_event(_room, _event, _provenance):
        pass

    client.add_event_batch_admission_callback(admit_batch, RoomMessageText)
    with pytest.raises(LocalProtocolError, match="admission callback is already registered"):
        client.add_event_admission_callback(admit_event, RoomMessageText)
```

Add the inverse case on a fresh client.

- [ ] **Step 2: Run the focused test and confirm the expected failure**

Run: `uv run pytest tests/backfill_test.py -k simultaneous_admission_owners -vv`

Expected: FAIL because `add_event_batch_admission_callback` and the public types do not exist.

- [ ] **Step 3: Implement the public types and exclusive registration**

Create the focused type module.

```python
@dataclass(frozen=True)
class TimelineAdmissionEntry:
    room: MatrixRoom
    event: Event
    provenance: TimelineEventProvenance


class TimelineAdmissionDisposition(str, Enum):
    FANOUT = "fanout"
    NON_SEMANTIC = "non_semantic"


TimelineBatchAdmissionCallback = Callable[
    [tuple[TimelineAdmissionEntry, ...]],
    Awaitable[tuple[TimelineAdmissionDisposition, ...]]
    | tuple[TimelineAdmissionDisposition, ...],
]


@dataclass(frozen=True)
class _TimelineBatchAdmission:
    callback: TimelineBatchAdmissionCallback
    event_filter: type[Event] | tuple[type[Event], ...] | None
```

Store `_TimelineBatchAdmission` on `AsyncClient`, reject a second owner in both registration methods, and export only the public entry and disposition types.

- [ ] **Step 4: Run the focused test and public import smoke test**

Run: `uv run pytest tests/backfill_test.py -k simultaneous_admission_owners -vv`

Run: `uv run python -c 'from nio import TimelineAdmissionDisposition, TimelineAdmissionEntry'`

Expected: PASS.

- [ ] **Step 5: Commit the public contract**

```bash
git add src/nio/client/timeline_admission.py src/nio/client/async_client.py src/nio/client/__init__.py src/nio/__init__.py tests/backfill_test.py
git commit -m "feat: add timeline batch admission contract"
```

### Task 2: Atomic Batch Store Operations

**Files:**

- Modify: `src/nio/store/database.py`
- Test: `tests/store_test.py`

**Interfaces:**

- Consumes: `CompletedTimelineEvent` from `src/nio/client/sync_recovery.py`.
- Produces: `MatrixStore.accept_recovery_events(keys: Sequence[tuple[str, int, str]]) -> None`.
- Produces: `MatrixStore.finish_recovery_events(keys: Sequence[tuple[str, int, str]], completed: Sequence[CompletedTimelineEvent]) -> None`.

- [ ] **Step 1: Write failing atomic acceptance tests**

Seed three real pending rows and assert that one call marks all three accepted.

Add a second test containing one stale generation and one already-cleared key, and assert that every remaining real row becomes accepted without raising.

Add a third test with a temporary SQLite trigger that aborts the second update, and assert that the first row's acceptance is rolled back.

```python
sqlstore.accept_recovery_events(
    [
        (TEST_ROOM, 99, "$one"),
        (TEST_ROOM, 1, "$already-cleared"),
        (TEST_ROOM, 1, "$three"),
    ]
)

assert all(event.admission_accepted for event in sqlstore.load_sync_recovery()[1])
```

- [ ] **Step 2: Run the acceptance tests and confirm the expected failure**

Run: `uv run pytest tests/store_test.py -k accept_recovery_events -vv`

Expected: FAIL because `accept_recovery_events` does not exist.

- [ ] **Step 3: Implement atomic batch acceptance**

Use `@use_database_atomic`, resolve the account once, then update every row inside the same transaction.

Match `generation > 0` independently of the passed generation and warn without raising when a row is already gone, exactly as the current single-event method does.

Factor the exact update into a private helper shared with `accept_recovery_event` so both paths use the same predicate.

```python
def _accept_recovery_event(self, account, room_id: str, generation: int, event_id: str) -> int:
    return (
        PendingTimelineEvents.update(admission_accepted=True)
        .where(
            PendingTimelineEvents.account == account,
            PendingTimelineEvents.room_id == room_id,
            PendingTimelineEvents.generation == generation,
            PendingTimelineEvents.event_id == event_id,
        )
        .execute()
    )
```

- [ ] **Step 4: Run the acceptance tests and existing single-event acceptance tests**

Run: `uv run pytest tests/store_test.py -k 'accept_recovery_event' -vv`

Expected: PASS.

- [ ] **Step 5: Write failing batch completion tests**

Assert that one call replaces regular rows with generation-zero markers preserving literal encryption and provenance values.

Add cases for mismatched input lengths, a missing key becoming a conservative completion marker, a synthetic `~` row being deleted, and more than 512 completed rows pruning to the newest 512 markers.

Add a temporary SQLite trigger that aborts the second marker insert, and assert that the first pending row remains unchanged after rollback.

```python
sqlstore.finish_recovery_events(
    [(TEST_ROOM, 1, event_id) for event_id in ("$one", "$two", "$three")],
    [
        CompletedTimelineEvent(False, TimelineEventProvenance.LIVE),
        CompletedTimelineEvent(True, TimelineEventProvenance.RECOVERED),
        CompletedTimelineEvent(False, TimelineEventProvenance.HISTORY),
    ],
)
```

- [ ] **Step 6: Run the completion tests and confirm the expected failure**

Run: `uv run pytest tests/store_test.py -k finish_recovery_events -vv`

Expected: FAIL because `finish_recovery_events` does not exist.

- [ ] **Step 7: Implement atomic batch completion**

Validate equal lengths before mutation and resolve existing rows independently of the passed generation.

For each regular event, replace its row with the same generation-zero marker shape used by `finish_recovery`.

Delete synthetic events and prune generation-zero markers once per touched room after all replacements.

Keep all work under one `@use_database_atomic` transaction.

- [ ] **Step 8: Run focused and complete store tests**

Run: `uv run pytest tests/store_test.py -vv`

Expected: PASS.

- [ ] **Step 9: Commit the store operations**

```bash
git add src/nio/store/database.py tests/store_test.py
git commit -m "feat: batch recovery store transitions"
```

### Task 3: Recovery-Layer Batch State Machine

**Files:**

- Modify: `src/nio/client/sync_recovery.py`
- Test: `tests/sync_recovery_test.py`

**Interfaces:**

- Consumes: `MatrixStore.accept_recovery_events` and `MatrixStore.finish_recovery_events`.
- Produces: `DispatchEventBatch`, which receives pending rows, parsed events, and an acceptance closure.
- Produces: optional `dispatch_event_batch` on `pump_recovery`.

- [ ] **Step 1: Write a failing recovery-layer success test**

Use a real `RecoveryState` with three timeline rows and an `InlineStore` that records batch method calls.

Have the fake batch dispatcher call its acceptance closure and return three literal dispatch results.

Assert one ordered acceptance call, one ordered completion call, no remaining pending rows, and three completion markers.

The production mutation this test catches is falling back to the per-event store methods.

- [ ] **Step 2: Run the success test and confirm the expected failure**

Run: `uv run pytest tests/sync_recovery_test.py -k batch_dispatch_groups_store_transitions -vv`

Expected: FAIL because `pump_recovery` has no `dispatch_event_batch` path.

- [ ] **Step 3: Implement ordered batch state transitions**

Add helpers with these exact responsibilities.

```python
def _mark_batch_admission_accepted(
    state: RecoveryState,
    store: MatrixStore | None,
    gap: RecoveryGap,
    pending_events: Sequence[PendingTimelineEvent],
) -> None:
    unaccepted = tuple(
        pending for pending in pending_events if not pending.admission_accepted
    )
    if not unaccepted:
        return
    queued = state.events[(gap.room_id, gap.generation)]
    indexes = tuple(
        next(
            index
            for index, current in enumerate(queued)
            if current.event_id == pending.event_id and current.kind == pending.kind
        )
        for pending in unaccepted
    )
    if store:
        store.accept_recovery_events(
            [(gap.room_id, gap.generation, pending.event_id) for pending in unaccepted]
        )
    for index in indexes:
        queued[index] = replace(queued[index], admission_accepted=True)


def _finish_recovery_event_batch(
    state: RecoveryState,
    store: MatrixStore | None,
    gap: RecoveryGap,
    pending_events: Sequence[PendingTimelineEvent],
    delivered: Sequence[_DispatchResult],
) -> None:
    completed = tuple(
        CompletedTimelineEvent(
            was_encrypted=(
                isinstance(result, MegolmEvent)
                if result is not None
                else pending.was_encrypted
            ),
            provenance=pending.provenance,
        )
        for pending, result in zip(pending_events, delivered, strict=True)
    )
    if store:
        store.finish_recovery_events(
            [(gap.room_id, gap.generation, pending.event_id) for pending in pending_events],
            completed,
        )
    queued = state.events[(gap.room_id, gap.generation)]
    for pending, result in zip(pending_events, completed, strict=True):
        current = next(
            item
            for item in queued
            if item.event_id == pending.event_id and item.kind == pending.kind
        )
        queued.remove(current)
        if not pending.event_id.startswith("~"):
            record_completed_timeline_event(
                state,
                gap.room_id,
                pending.event_id,
                result.was_encrypted,
                result.provenance,
            )
```

Resolve every in-memory target before the store call, perform the atomic store call, and only then mutate in-memory state.

Build `CompletedTimelineEvent` values with the same encryption fallback and provenance rules as `_finish`.

- [ ] **Step 4: Run the success test and confirm it passes**

Run: `uv run pytest tests/sync_recovery_test.py -k batch_dispatch_groups_store_transitions -vv`

Expected: PASS.

- [ ] **Step 5: Write failing partial failure and fallback tests**

Add one live callback failure case that completes the delivered prefix plus the failed live event.

Add one recovered-history callback failure case that completes only the delivered prefix and leaves the failed row plus later rows pending.

Add one case proving that a leading account-data or ephemeral row and the later timeline rows remain ordered on the individual path rather than entering a mixed-kind batch.

The production mutations these tests catch are acknowledging failed history, losing acknowledged live work, and batching non-timeline rows.

- [ ] **Step 6: Run the new tests and confirm the expected failures**

Run: `uv run pytest tests/sync_recovery_test.py -k 'batch_dispatch and (failure or non_timeline)' -vv`

Expected: FAIL on missing partial-result and prefix-selection behavior.

- [ ] **Step 7: Implement batch dispatch, deadline reuse, and prefix selection**

Introduce `_BatchCallbackError` carrying ordered completed index/result pairs and whether the failing row was live.

Introduce `_run_batch_dispatch` and `_drain_timeline_batch` using one asyncio task aliased under every event dispatch key.

Make `_drain_gap` select only its leading contiguous timeline rows and fall back to the existing loop for everything else.

Clear every alias for a completed task so no stale event key retains a finished batch.

- [ ] **Step 8: Run recovery unit tests**

Run: `uv run pytest tests/sync_recovery_test.py -vv`

Expected: PASS.

- [ ] **Step 9: Commit the recovery state machine**

```bash
git add src/nio/client/sync_recovery.py tests/sync_recovery_test.py
git commit -m "feat: group recovery batch transitions"
```

### Task 4: AsyncClient Batch Admission And Fanout

**Files:**

- Modify: `src/nio/client/async_client.py`
- Test: `tests/backfill_test.py`

**Interfaces:**

- Consumes: `TimelineAdmissionEntry`, `_TimelineBatchAdmission`, `TimelineAdmissionDisposition`, `_BatchCallbackError`, and `DispatchEventBatch`.
- Produces: `AsyncClient._dispatch_timeline_batch` wired into `pump_recovery` only when the batch callback is registered.

- [ ] **Step 1: Write a failing commit-before-fanout integration test**

Register a batch callback that records both event IDs, waits on an event, and returns two literal `FANOUT` dispositions.

Register an ordinary callback that records each event ID.

Start `receive_response`, assert ordinary fanout has not started while batch admission is blocked, release admission, and assert the order is batch, committed marker, first event, second event.

The production mutation this test catches is any ordinary callback running before the batch acceptance closure succeeds.

- [ ] **Step 2: Run the integration test and confirm the expected failure**

Run: `uv run pytest tests/backfill_test.py -k batch_admission_commits_before_any_event_fanout -vv`

Expected: FAIL because AsyncClient does not supply the batch dispatcher.

- [ ] **Step 3: Split timeline processing into preparation and fanout**

Extract `_prepare_timeline_event` from `_dispatch_timeline_event` without changing the individual path.

Extract `_fanout_prepared_timeline_event` as the single call to `_on_event`.

Keep ephemeral and account-data behavior in `_dispatch_timeline_event`.

- [ ] **Step 4: Implement batch admission and ordered fanout**

Prepare every pending timeline event in order.

Build admission entries only for unaccepted, non-suppressed `Event` instances matching the registered filter.

Run the callback inside the existing callback scope, await it when necessary, and require one valid disposition per entry.

Call the acceptance closure only after callback success, then fan out every prepared event in order.

Convert rejection, acceptance failure, and ordinary callback failure into `_BatchCallbackError` with exact completed pairs.

- [ ] **Step 5: Run the commit-before-fanout test and confirm it passes**

Run: `uv run pytest tests/backfill_test.py -k batch_admission_commits_before_any_event_fanout -vv`

Expected: PASS.

- [ ] **Step 6: Write failing order, rejection, validation, and restart tests**

Add a recovered gap test asserting one batch in literal `gap-one`, `gap-two`, `held` order with literal provenance values.

Add a refusal test asserting no ordinary callback on the first attempt and the identical full batch on retry.

Add malformed result tests for wrong length and non-enum dispositions.

Add a restart test proving already accepted rows are absent from admission entries but still reach ordinary fanout before completion.

Add a state-event test proving `NON_SEMANTIC` does not suppress room-state updates or ordinary callbacks.

- [ ] **Step 7: Run the new tests and confirm the expected failures**

Run: `uv run pytest tests/backfill_test.py -k batch_admission -vv`

Expected: FAIL on each not-yet-implemented boundary.

- [ ] **Step 8: Complete error validation and restart behavior**

Raise `LocalProtocolError` through `_BatchCallbackError` for malformed callback results without marking the batch accepted.

Skip admission for rows whose durable marker is already accepted while keeping them in the prepared fanout sequence.

Preserve live and recovered-history failure acknowledgement rules when building completed pairs.

- [ ] **Step 9: Run all backfill tests**

Run: `uv run pytest tests/backfill_test.py -vv`

Expected: PASS.

- [ ] **Step 10: Commit the AsyncClient integration**

```bash
git add src/nio/client/async_client.py tests/backfill_test.py
git commit -m "feat: dispatch admitted timelines in batches"
```

### Task 5: Regression Verification And Live Profiling

**Files:**

- Modify only if checks find a defect in the files already listed.

**Interfaces:**

- Consumes: the complete opt-in batch API and the existing MindRoom fuzz and sustained-capacity scripts.
- Produces: verified test, commit-count, filesystem-sync, and 200-stream capacity evidence for the pull request.

- [ ] **Step 1: Run focused recovery suites together**

Run: `uv run pytest tests/store_test.py tests/sync_recovery_test.py tests/backfill_test.py -vv`

Expected: PASS.

- [ ] **Step 2: Run formatting, lint, and typing checks**

Run: `uv run pre-commit run --all-files`

Expected: PASS with no generated or unrelated file changes.

- [ ] **Step 3: Run the full nio test suite**

Run: `uv run pytest`

Expected: PASS.

- [ ] **Step 4: Integrate the opt-in callback in a temporary MindRoom test worktree only for profiling**

Use the existing durable journal batch operation and do not add a new workload script.

Keep the profiling integration separate from the nio PR unless the user separately requests the MindRoom consumer PR.

- [ ] **Step 5: Re-run the existing fuzz tests**

Run the existing fuzz script unchanged against the branch build.

Expected: no missing, duplicated, or unsettled events.

- [ ] **Step 6: Re-run the instrumented existing 100-stream workload**

Use the same monkeypatch counters as the baseline around the existing sustained-capacity script.

Record total elapsed time and exact calls and commits attributed to `accept_recovery_event`, `accept_recovery_events`, `finish_recovery`, and `finish_recovery_events`.

Expected: per-event acceptance and completion commits are replaced by materially fewer batch commits for batch-enabled clients.

- [ ] **Step 7: Run the existing 200-stream workload**

Record terminal settlement, full overlap, failure log, and process memory.

Expected: correctness passes and SQLite commit or filesystem-sync pressure is materially lower than the 0.40.0 baseline.

- [ ] **Step 8: Review the final diff and commit any verification-driven fixes**

Run: `git diff origin/main...HEAD --check`

Run: `git status --short`

Expected: only intended source, tests, and design or plan documentation are present.

- [ ] **Step 9: Push and open the pull request**

```bash
git push -u origin perf/group-recovery-commits
gh pr create --base main --head perf/group-recovery-commits --title "Batch durable timeline recovery commits" --body-file /tmp/nio-group-recovery-commits-pr.md
```

The PR body must include the 0.40.0 baseline, branch measurements, crash replay tradeoff, exact verification commands, and the fact that existing clients remain on the unchanged single-event path.

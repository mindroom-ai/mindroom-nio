# Durable Ingestion Cutover Hardening Plan

> For agentic workers: use baspowers:subagent-driven-development for production
> tasks. The controller owns final verification, design updates, and push.

**Goal:** Remove the demonstrated capacity stall, enforce logical ownership, and
reduce repeated production work without adding another ingestion state machine.
**Architecture:** Keep atomic prepared publication, one owned sync route, and
bounded exact-input reuse of validated values. Share policy through small helpers.
**Tech stack:** Python >=3.12, asyncio, SQLite/Peewee, pytest, uv, pre-commit.
**Spec:** [Durable ingestion contract](durable-ingestion-contract.md), including
its cutover hardening amendment.

## Global Constraints

- Preserve every required guarantee and deliberate limit in the contract.
- Preserve the existing MindRoom factory, batch, and settlement names/signatures.
- No new dependencies, schema tables, public configuration, general-purpose
  frameworks, deployed-store destruction, or unrelated refactoring.
- Use uv run for Python and tests. Do not commit docs/baspowers files.
- No Claude consultation. Never amend commits. Use the existing isolated branch.
- Privacy-scan proposed content and commit metadata before repository writes;
  stage explicit intended paths and inspect the staged diff before each commit.
- Use focused behavioral tests per task, repository hooks for changed files,
  and one full final verification matrix. Do not shrink tests to meet a quota.
- Keep architectural decisions in this tracked contract and plan. User authorized
  pushing the existing PR branch after verification and review; no live cutover.

## Task 1: Fence ordinary sync on the owned client

Files: src/nio/client/base_client.py, src/nio/client/async_client.py,
src/nio/ingest/coordinator.py; tests/public_sync_compatibility_test.py and
focused owned-client tests in tests/ingest/coordinator_test.py or a new adjacent
owned_client_mode_test.py using tests/ingestion_helpers.py fixtures.

Interfaces: existing `_ingestion_store_snapshot` marks attachment;
`_validate_owned_client_is_pristine` checks factory eligibility. Add one sticky
`_ordinary_sync_started` boolean and `_begin_ordinary_sync()` helper on Client.

- [ ] Add failing behavioral cases for owned sync, sync_forever, and direct
  SyncResponse handling before HTTP/callback/token/crypto effects. Exercise a
  custom send paused during ordinary sync while the factory tries to attach.
  Check bootstrap remains usable by a fresh client after rejection, and failed
  or cancelled ordinary sync likewise requires a fresh client for attachment.

```python
# Assertions applied to the real owned fixtures, with send/callback spies.
with pytest.raises(LocalProtocolError):
    await client.sync()
assert calls == []
assert client.next_batch == before_token
assert session._journal.load_source() == before_source
```

- [ ] Initialize the flag to False. The helper checks poison/attachment before
  setting it. Invoke it at entry to sync, sync_forever, and both implementations
  of _handle_sync, before side effects and before the first await.

```python
def _begin_ordinary_sync(self) -> None:
    self._assert_ingestion_not_poisoned()
    if self._ingestion_store_snapshot is not None:
        raise LocalProtocolError("ordinary sync is unavailable during owned ingestion")
    self._ordinary_sync_started = True
```

- [ ] Reject the flag in the existing pristine predicate. Preserve non-sync
  response handling, owned sends/history/hydration/cleanup, ordinary nested
  callbacks, healthy detach followed by ordinary sync, and poisoned rejection.
- [ ] Capture red/green evidence, run public compatibility and focused owned
  lifecycle tests, hooks, inspect diff, then commit with an explanatory body.

## Task 2: Admit whole frames within the hard output envelope

Files: src/nio/ingest/errors.py; src/nio/store/_sync_journal_values.py,
_sync_journal_plan.py, _sync_journal.py and directly used size helpers;
tests/ingest/materializer_test.py and focused owned capacity regressions.

Interfaces: remove the two private max_ready_work settings from MaterializerLimits;
keep the existing materialization and consumer APIs. Add JournalCapacityError
as a RuntimeError in ingest/errors.py, separate from JournalIntegrityError.

- [ ] Port the real owned 2,050-message failure into a regression using actual
  preparation and SQLite. Establish/hydrate/settle the baseline room first.
  Verify publication, restart with stable Work identity, acknowledgement, and
  eventual frame retirement. Add a frame above the former 16 MiB READY-byte
  limit but below total output limits, keeping each encoded row below 1 MiB.

```python
result = session._materialize_oldest_frame(limits=MaterializerLimits())
assert result.status is MaterializeStatus.MATERIALIZED
assert session._journal._execute(
    "SELECT COUNT(*) FROM NioIngestWork"
).fetchone()[0] == 2050
# Reopen the owned session, compare the first unacknowledged record identity,
# settle the durable records, and assert no retained frame remains.
```

- [ ] Remove READY-specific rejection and settings. Retain total, HELD, and
  per-record bounds plus existing held-loss behavior. Do not replace the old
  READY ceiling with a larger READY ceiling or discard deliverable messages.
- [ ] Reserve HELD-to-READY header growth using maximum SQLite integer revision
  and ordinal widths when testing per-row and total admission. Preserve exact
  stored-byte validation and no count growth during promotion. Test a boundary
  where a HELD row fits but its unreserved promoted representation would not.
- [ ] Raise JournalCapacityError for freshly planned hard resource excess,
  preserving the original bound-specific diagnostic and rollback. Do not wrap
  it as integrity failure. Keep corruption detection on persisted rows intact.
  Verify retained source input and poisoned-client reconstruction on rejection.
  Returning AT_CAPACITY after crypto mutation is forbidden.

```python
with pytest.raises(JournalCapacityError):
    session._materialize_oldest_frame(limits=small_total_limits)
assert session._journal.list_frames(1)[0].frame_id == accepted_frame_id
assert session._journal._execute(
    "SELECT COUNT(*) FROM NioIngestWork"
).fetchone()[0] == before_count
```

- [ ] Update direct planner tests to the new envelope and remove only assertions
  for discarded READY settings. Run materializer, hydration, owned crash/replay
  and focused source/settlement tests, hooks, inspect diff, then commit.

## Task 3: Avoid repeat Aggregate decoding and size-only hashing

Files: src/nio/store/_sync_journal.py, _sync_journal_rows.py,
_sync_journal_format.py, _sync_journal_plan.py; adjacent journal/codec tests.

Interfaces: journal-local single-entry cache of full stored Aggregate row,
authentication owner identity, and immutable RoomAggregateValue. Share envelope
prefix construction between _row persistence/authentication and size calculation.

- [ ] Add a regression counting Aggregate decodes across repeated real journal
  reads/settlements, asserting reuse only for identical stored input. Corrupt
  payload while leaving digest/revision unchanged and require integrity failure.
  Verify changed valid row, other room, other owner identity, and close behavior.

```python
first = journal._load_room_aggregate(owner, room_id)
second = journal._load_room_aggregate(owner, room_id)
assert second == first
assert decode_calls == 1
# Changing payload alone must bypass reuse and fail normal authentication.
```

- [ ] Fetch and validate row columns before consulting the single-entry cache;
  compare the complete row plus account/stream/transport authentication identity.
  Cache only successful immutable decode. Clear on close; do not cache projection
  execution, pending intents, or mutable client state.
- [ ] Factor a small canonical-envelope-prefix helper from _row. Compute full
  encoded row length without building/hash-digesting the full payload solely for
  size. Reuse exact unchanged sizes within one plan if still needed. Test length
  equality with persisted bytes for event/loss, READY/HELD, unicode identities,
  and large integer headers; test actual storage corruption still fails.

```python
assert _stored_work_size(owner, stored) == len(
    _row(owner, "NioIngestWork", stored.plaintext,
         header=_canonical_internal(stored.clear_values))[0]
)
```

- [ ] Run focused codec, corruption, delivery, hydration, and replay tests; record
  call-count and representative 1,000/10,000-member decode measurements before
  and after. Record workload/limits, not machine-independent timing thresholds.
- [ ] Run hooks, inspect production diff/size, then commit.

## Task 4: Share ordinary and owned projection policy

Files: src/nio/client/base_client.py, src/nio/client/async_client.py,
src/nio/ingest/coordinator.py; tests/ingest/preparation_test.py,
tests/public_sync_compatibility_test.py and existing transient tests.

Interfaces: small Client projection/control helpers consumed by ordinary handlers
and owned preparation/delivery. Keep route-specific transaction/callback timing.

- [ ] Share presence projection for known users in a Client helper used by base,
  async, and owned transient handling. Callers keep callback and timeout policy.
- [ ] Share OTK count and changed/left-device tracking policy between ordinary
  _handle_olm_events and owned frame preparation; pass existing concrete values,
  not a new carrier or strategy object. Preserve encryption-disabled behavior.
- [ ] Share the existing room-state membership invalidation/encryption logic
  only where its inputs/effects match; preserve invite/no-room gates and room
  summaries in their current owners. Do not merge the two orchestrators.

```python
# Existing parity fixture runs both routes with the same membership/key inputs.
assert owned.olm.uploaded_key_count == ordinary.olm.uploaded_key_count
assert owned.olm.users_for_key_query == ordinary.olm.users_for_key_query
assert set(owned.rooms[room_id].users) == set(ordinary.rooms[room_id].users)
```

- [ ] Use existing behavior/parity tests, adding a failing parity case only where
  a changed rule needs coverage. Run preparation, public compatibility, transient,
  and relevant crypto tests. Inspect net helper cost; omit abstractions that do
  not eliminate real duplicated policy and record that decision in results.
- [ ] Run hooks, inspect diff, then commit.

## Task 5: Bind the complete runtime model list directly

Files: src/nio/store/database.py; tests/store_test.py and focused adjacent store
binding tests using real ordinary and owned store fixtures.

Interfaces: keep both decorators and every lifecycle/transaction boundary; only
change the bind_ctx keyword arguments at their four ordinary/owned call sites.

- [ ] Verify MatrixStore/DefaultStore model lists include runtime foreign-key
  targets; SqliteStore/SqliteMemoryStore additionally include DeviceTrustState.
  Preserve existing session, forwarding-chain, device key, sidecar/SQL trust,
  room, request, and sync-token round trips. Migration-only models stay separate.
- [ ] Add real nested cross-store cases with distinct stored token/key values,
  plus read and atomic exception cases. Cover ordinary and owned decorator paths,
  rollback after a nested write, and complete binding restoration on every exit.

```python
# Inside a binding context for store_a, call decorated methods on store_b.
assert store_a.load_sync_token() == "token-a"
assert store_b.load_sync_token() == "token-b"
assert Accounts._meta.database is store_a.database
# After the outer context exits, each model has its original database binding.
```

- [ ] Apply the explicit binding arguments at exactly the four decorator sites.
  Preserve owner validation, read/write scopes, epoch fences, atomic/savepoint
  scopes, and queue database behavior. Do not alter migration/bootstrap bindings.

```python
with self.database.bind_ctx(
    self.models, bind_refs=False, bind_backrefs=False
):
    return fn(self, *args, **kwargs)
```

- [ ] Record serial binding/token/account microbenchmarks with original and new
  binding options on the same store/runtime. State runtime, workload, and timing
  boundary. No concurrent safety or whole-application throughput claim.
- [ ] Run store, encryption, ownership/recovery, migration, and new binding tests,
  hooks, inspect diff, then commit with the rationale and verification.

## Task 6: Verify, document results, review, and push

Controller task. Files: this plan and the contract where a final design decision
needs recording; no unreviewed production changes in this task.

- [ ] Confirm all task reviews and targeted tests pass. Run the complete suite
  on Python 3.12, 3.13, and 3.14; run repository pre-commit hooks and compare mypy
  diagnostics against the 8b529ed baseline (144 errors; baseline is not clean).
- [ ] Perform an independent whole-PR review against the contract and base
  5b6de3b, with a separate focus on this hardening delta from 8b529ed. Address
  valid findings, rerun affected checks, and record remaining limitations.
- [ ] Record source-size deltas and precise verification/performance results.
  Keep context recovery versus actionable replay explicit; any change in that
  product decision gets its own contract amendment before implementation.
- [ ] Commit tracked results, privacy-scan outgoing content/metadata, check remote
  drift, and push non-forcibly to the existing PR branch. Confirm remote HEAD.

## Design self-review

The five production tasks map directly to the amendment: logical ownership,
atomic capacity, exact-input reuse, shared policy, and explicit model binding.
Required crash/crypto and
consumer boundaries remain intact. The capacity promise explicitly stops at the
hard output envelope. History semantics stay separate until the product decision
is made. No schema migration or new public API is required. Task 2 may touch the
size helper before Task 3; Task 3 preserves its promotion-reservation semantics.
Focused tests precede commits; the complete matrix follows all production tasks.

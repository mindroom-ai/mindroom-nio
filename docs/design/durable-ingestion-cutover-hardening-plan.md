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

- [x] Add failing behavioral cases for owned sync, sync_forever, and direct
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

- [x] Initialize the flag to False. The helper checks poison/attachment before
  setting it. Invoke it at entry to sync, sync_forever, and both implementations
  of _handle_sync, before side effects and before the first await.

```python
def _begin_ordinary_sync(self) -> None:
    self._assert_ingestion_not_poisoned()
    if self._ingestion_store_snapshot is not None:
        raise LocalProtocolError("ordinary sync is unavailable during owned ingestion")
    self._ordinary_sync_started = True
```

- [x] Reject the flag in the existing pristine predicate. Preserve non-sync
  response handling, owned sends/history/hydration/cleanup, ordinary nested
  callbacks, healthy detach followed by ordinary sync, and poisoned rejection.
- [x] Capture red/green evidence, run public compatibility and focused owned
  lifecycle tests, hooks, inspect diff, then commit with an explanatory body.

## Task 2: Admit whole frames within the hard output envelope

Files: src/nio/ingest/errors.py; src/nio/store/_sync_journal_values.py,
_sync_journal_plan.py, _sync_journal.py and directly used size helpers;
tests/ingest/materializer_test.py and focused owned capacity regressions.

Interfaces: remove the two private max_ready_work settings from MaterializerLimits;
keep the existing materialization and consumer APIs. Add JournalCapacityError
as a RuntimeError in ingest/errors.py, separate from JournalIntegrityError.

- [x] Port the real owned 2,050-message failure into a regression using actual
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

- [x] Remove READY-specific rejection and settings. Retain total, HELD, and
  per-record bounds plus existing held-loss behavior. Do not replace the old
  READY ceiling with a larger READY ceiling or discard deliverable messages.
- [x] Reserve HELD-to-READY header growth using maximum SQLite integer revision
  and ordinal widths when testing per-row and total admission. Preserve exact
  stored-byte validation and no count growth during promotion. Test a boundary
  where a HELD row fits but its unreserved promoted representation would not.
- [x] Raise JournalCapacityError for freshly planned hard resource excess,
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

- [x] Update direct planner tests to the new envelope and remove only assertions
  for discarded READY settings. Run materializer, hydration, owned crash/replay
  and focused source/settlement tests, hooks, inspect diff, then commit.

## Task 3: Avoid repeat Aggregate decoding and size-only hashing

Files: src/nio/store/_sync_journal.py, _sync_journal_rows.py,
_sync_journal_format.py, _sync_journal_plan.py; adjacent journal/codec tests.

Interfaces: journal-local single-entry cache of full stored Aggregate row,
authentication owner identity, and immutable RoomAggregateValue. Share envelope
prefix construction between _row persistence/authentication and size calculation.

- [x] Add a regression counting Aggregate decodes across repeated real journal
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

- [x] Fetch and validate row columns before consulting the single-entry cache;
  compare the complete row plus account/stream/transport authentication identity.
  Cache only successful immutable decode. Clear on close; do not cache projection
  execution, pending intents, or mutable client state.
- [x] Factor a small canonical-envelope-prefix helper from _row. Compute full
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

- [x] Run focused codec, corruption, delivery, hydration, and replay tests; record
  call-count and representative 1,000/10,000-member decode measurements before
  and after. Record workload/limits, not machine-independent timing thresholds.
- [x] Run hooks, inspect production diff/size, then commit.

## Task 4: Share ordinary and owned projection policy

Files: src/nio/client/base_client.py, src/nio/client/async_client.py,
src/nio/ingest/coordinator.py; tests/ingest/preparation_test.py,
tests/public_sync_compatibility_test.py and existing transient tests.

Interfaces: small Client projection/control helpers consumed by ordinary handlers
and owned preparation/delivery. Keep route-specific transaction/callback timing.

- [x] Share presence projection for known users in a Client helper used by base,
  async, and owned transient handling. Callers keep callback and timeout policy.
- [x] Share OTK count and changed/left-device tracking policy between ordinary
  _handle_olm_events and owned frame preparation; pass existing concrete values,
  not a new carrier or strategy object. Preserve encryption-disabled behavior.
- [x] Share the existing room-state membership invalidation/encryption logic
  only where its inputs/effects match; preserve invite/no-room gates and room
  summaries in their current owners. Do not merge the two orchestrators.

```python
# Existing parity fixture runs both routes with the same membership/key inputs.
assert owned.olm.uploaded_key_count == ordinary.olm.uploaded_key_count
assert owned.olm.users_for_key_query == ordinary.olm.users_for_key_query
assert set(owned.rooms[room_id].users) == set(ordinary.rooms[room_id].users)
```

- [x] Use existing behavior/parity tests, adding a failing parity case only where
  a changed rule needs coverage. Run preparation, public compatibility, transient,
  and relevant crypto tests. Inspect net helper cost; omit abstractions that do
  not eliminate real duplicated policy and record that decision in results.
- [x] Run hooks, inspect diff, then commit.

## Task 5: Bind the complete runtime model list directly

Files: src/nio/store/database.py; tests/store_test.py and focused adjacent store
binding tests using real ordinary and owned store fixtures.

Interfaces: keep both decorators and every lifecycle/transaction boundary; only
change the bind_ctx keyword arguments at their four ordinary/owned call sites.

- [x] Verify MatrixStore/DefaultStore model lists include runtime foreign-key
  targets; SqliteStore/SqliteMemoryStore additionally include DeviceTrustState.
  Preserve existing session, forwarding-chain, device key, sidecar/SQL trust,
  room, request, and sync-token round trips. Migration-only models stay separate.
- [x] Add real nested cross-store cases with distinct stored token/key values,
  plus read and atomic exception cases. Cover ordinary and owned decorator paths,
  rollback after a nested write, and complete binding restoration on every exit.

```python
# Inside a binding context for store_a, call decorated methods on store_b.
assert store_a.load_sync_token() == "token-a"
assert store_b.load_sync_token() == "token-b"
assert Accounts._meta.database is store_a.database
# After the outer context exits, each model has its original database binding.
```

- [x] Apply the explicit binding arguments at exactly the four decorator sites.
  Preserve owner validation, read/write scopes, epoch fences, atomic/savepoint
  scopes, and queue database behavior. Do not alter migration/bootstrap bindings.

```python
with self.database.bind_ctx(
    self.models, bind_refs=False, bind_backrefs=False
):
    return fn(self, *args, **kwargs)
```

- [x] Record serial binding/token/account microbenchmarks with original and new
  binding options on the same store/runtime. State runtime, workload, and timing
  boundary. No concurrent safety or whole-application throughput claim.
- [x] Run store, encryption, ownership/recovery, migration, and new binding tests,
  hooks, inspect diff, then commit with the rationale and verification.

## Task 6: Verify, document results, review, and push

Controller task. Files: this plan and the contract where a final design decision
needs recording; no unreviewed production changes in this task.

- [x] Confirm all task reviews and targeted tests pass. Run the complete suite
  on Python 3.12, 3.13, and 3.14; run repository pre-commit hooks and compare mypy
  diagnostics against the 8b529ed baseline (144 errors; baseline is not clean).
- [x] Perform an independent whole-PR review against the contract and base
  5b6de3b, with a separate focus on this hardening delta from 8b529ed. Address
  valid findings, rerun affected checks, and record remaining limitations.
- [x] Record source-size deltas and precise verification/performance results.
  Keep context recovery versus actionable replay explicit; any change in that
  product decision gets its own contract amendment before implementation.
- [x] Commit tracked results, privacy-scan outgoing content/metadata, check remote
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

## Implementation and verification results

The five production tasks are implemented and their task reviews are clean.
Ownership uses one ordinary-sync flag plus the existing attachment marker;
capacity removes the conflicting READY limits; unchanged Aggregates reuse one
validated value; ordinary and owned routes share three concrete leaf policies;
store calls bind their complete runtime model list directly. No new persistent
representation, dependency, public configuration, or ingestion orchestrator was
added.

Production size counts tracked `src/**/*.py` physical lines, including comments
and blanks; tests and documentation are excluded.

| Revision | Production lines | Final change from revision |
| --- | ---: | ---: |
| PR base `5b6de3b` | 31,490 | +13,359 |
| Original reviewed PR `742806f` | 46,222 | -1,373 |
| Previous simplification `8b529ed` | 44,788 | +61 |
| Five production tasks `83cff56` | 44,831 | +18 |
| Final-review repairs `1ca6072` | 44,849 | 0 |

The hardening pass and final-review repairs add 61 production lines overall:
43 for the five tasks and 18 for the protocol/crypto repairs. Shared policy alone
removes 15 lines. The remaining large code is concentrated in
preparation/coordinator, journal validation/publication, and deployed-store
adoption. Those mechanisms carry the retained crash, crypto, ownership, and
admission guarantees; neither
line compression nor another state machine would improve this tradeoff.

`pytest --benchmark-disable` completed on Python 3.12.13, 3.13.14, and 3.14.7:
2,068 passed and 3 skipped on each interpreter after the behavioral repairs.
Existing fork tests emit three deprecation warnings on each; Python 3.14 also
reports two deprecated `asyncio.iscoroutinefunction` uses in tests. The matrix
began before an added cancellation assertion segment and the final local type
annotation correction; production runtime behavior was unchanged. The two
cancellation tests and 33 crypto/restart cases passed separately afterward.
All repository pre-commit hooks passed, including scoped checks after those last
changes. The full mypy command (`MYPYPATH=src uv run --no-sync mypy -p nio
--warn-redundant-casts --no-incremental`) still reports 144 errors in 23 files,
exactly matching the normalized baseline with no added or removed diagnostics.
This is existing debt, not a passing type check. The requested zero-error cleanup
follows completion and push of this pass; it is not waived by baseline parity.

### Performance boundaries

Repeated real Aggregate reads used in-memory SQLite, complete row lookup, and an
equality assertion against the expected immutable value. Seven samples per size;
medians include one cold read and repeated identical reads. The single-entry
cache benefits repeated reads of the same room, not alternating-room scans.

| Room members | Encoded Aggregate bytes | Before | After |
| --- | ---: | ---: | ---: |
| 1,000 | 119,861 | 6.830 ms | 0.152 ms |
| 10,000 | 1,199,861 | 71.233 ms | 1.497 ms |

The direct-binding measurement used CPython 3.14.7, Peewee 3.19.0, and one
`SqliteMemoryStore`. It invoked the actual decorated methods while selecting
original recursive or new direct-list binding options on the same database.
Seven serial batches used 2,000 binding iterations or 1,000 method calls per
batch; setup and correctness assertions were outside the timed section. These
measurements supersede the smaller synthetic single-binding token/account probe.

| Operation | Recursive binding | Direct-list binding |
| --- | ---: | ---: |
| Empty binding context | 150.308 us | 12.936 us |
| `load_sync_token()` | 494.097 us | 207.461 us |
| `load_account()` | 307.575 us | 163.766 us |

Neither measurement includes full Matrix traffic, application admission,
callbacks, disk durability, or concurrent throughput. Reprofile the paired
MindRoom/Nio workload before claiming application-wide savings.

### Remaining cutover limits

History behavior is unchanged: explicit loss and membership-state hydration in
Nio, later context repair in MindRoom. Actionable replay of missed requests and
removal of the remaining gap-planning representation require a separate decision.
Intrinsically oversized prepared output remains retained and needs intervention;
automatic spooling or chunking was not added. The companion integration still
needs conflict resolution and testing against the accepted Nio artifact before
deployment. Released fork admission APIs are intentionally replaced, requiring
coordinated consumer migration and release communication.

## Final-review repair addendum

Independent review at `a055bac` found three supported-workload blockers in the
broader PR. The targeted hardening changes had no additional blocker. Repair
these together before the final push; they enforce the existing contract.

- [x] R1: Accept valid noncanonical wire JSON in membership success and the two
  maintenance/membership error parsers. Keep bounded strict decoding, schemas,
  response identity, and internal canonical validation. Verify real owned
  join/leave progress and retry/terminal classification with ordinary JSON.
- [x] R2: Accept `m.room.encryption` state during hydration; correct the encrypted
  envelope rejection to `m.room.encrypted`. Preserve duplicate-key, membership,
  and intent checks. Verify a real encrypted-room hydration, delivery, settlement,
  and restart, including encryption projection and crypto tracking.
- [x] R3: Remove executor-only singleton assumptions from queued shares, waiting
  claims, dummy rerequests, and simultaneous waiting/wedged devices. Consume only
  the matching live message, preserve other messages, allow multiple valid
  follow-ups per device, and align per-session deduplication. Keep existing
  operation storage, exact retry bytes/IDs, atomic application, and poison rules.
  Cover ordinary Olm-generated multi-message workloads and restart around sends.
- [x] Run focused regressions and crash/replay checks, then the full interpreter
  matrix and hooks after the behavioral corrections. Compare normalized mypy
  diagnostics and update final production-size measurements.
- [x] Independently re-review the complete fix diff against R1–R3 and check it for
  new breakage before push. Record the verdict and remaining cutover limits.

### Related trust-order correction

The real R3 tests also exposed an unverified own-device request with no Olm
session. The current shared policy schedules a claim before checking trust;
after the claim, it returns callback Work that maintenance cannot publish. Move
the existing trust check in `Olm.share_with_ourselves` ahead of missing-session
claim scheduling. Existing preparation then publishes the collected callback.
This preserves the current Work/operation representation and deliberately makes
that ordinary/owned callback available earlier. Add real ordinary and owned
regressions proving no claim/share before verification, durable callback replay,
and successful explicit continuation after verification. No new maintenance
publication phase is needed.

### Repair verification and test intent

The three review findings were reproduced against the earlier implementation:
40 regression cases failed there. The corrected focused suite passed 447 tests
with 3 existing skips, followed by the complete interpreter matrix above. Tests
exercise actual SQLite ownership, Olm/Megolm encryption and recipient decryption,
callbacks, settlement, and reopening around preparation, application, and
uncertain sends. Fault cases verify atomic rollback, post-commit publication,
poisoned-client reconstruction, stable request bytes/IDs, and frame retirement.

Existing test changes were assessed against the supported behavior. The hydration
negative fixture now uses the encrypted envelope type; a separate positive test
proves ordinary encryption configuration survives encrypted-room delivery and
restart. HTTP fate tests retain their pending-state, request-identity, and
classification assertions, and use status 400 so fallback status handling cannot
hide ignored Matrix error codes. Cancellation still covers waiting-for-session
requests and unverified requests. A separate test covers the original combined
unverified/no-session input, cancellation and recollection, denied continuation
before verification, and successful real sharing after verification. Expected
security or recovery outcomes were not weakened to make the suite pass.

The shared trust-order change intentionally surfaces an unverified own-device
callback before a missing-session claim. Its cost is earlier ordinary callback
timing in this narrow case. Reversing that decision would require restoring the
old order and designing explicit callback Work publication during maintenance.

### Independent review outcome

The whole-PR review identified R1–R3 at `a055bac`. Scoped re-review of
`a055bac..1ca6072` accepted all three repairs and found no new Critical/Important
breakage. It independently checked the changed tests against protocol and
behavior, including their retained cancellation and crypto recovery assertions.
The broad review was risk-focused across the production architecture; this is
not a claim that every line of the large PR was exhaustively verified.

One separate existing limitation remains: a collected unverified key-request
callback can be replayed without restoring the Olm pending approval map. A real
store/Olm reconstruction probe confirmed that subsequent `continue_key_share`
rejects the request as unknown; reaching the same failure from owned callback
replay is inferred from its unchanged call chain, not a completed end-to-end
reproduction. The new owned tests prove durable callback replay and no premature
secret sharing; explicit successful continuation was tested on the ordinary
path only. Do not present those results as proof of continuation after restart.

This limitation is a cutover gap to reproduce and resolve separately before
relying on interactive key sharing across restart. It does not block publishing
the current repairs for review, and it is not waived as an excluded guarantee.
The failure blocks key availability for that request; it does not bypass device
trust or expose the key. Deferring it costs a failed continuation until separately
resolved. The companion integration and this path still need cutover verification.

### Follow-up work after this push

The user requested elimination of all 144 mypy errors, with truthful annotations
and meaningful tests rather than blanket suppression or test accommodation.
They also requested inspection of PR #57's performance ideas, with benchmarks on
this PR #55 before deciding whether any optimization merits adoption. Both are
separate follow-ups after the current repairs, verification, review, and push.
That historical hardening pass did not evaluate PR #57. The subsequent
[type and delivery-cost design](type-correctness-and-delivery-cost.md) records
the evaluation, bounded Work reuse, and the zero-error CI requirement that
supersedes this pass's historical mypy baseline comparison.

### Publication

The repaired source and final design/results document were pushed non-forcibly
to PR #55 in `aaa1671`; the remote branch was read back and matched that commit.
Local verification and independent review are recorded above. Remote CI was
running when this publication receipt was written. No merge or deployment was
performed.

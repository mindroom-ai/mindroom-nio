# Durable Sliding Ingestion Canary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a restart-safe, explicitly selected durable ingestion path for the pinned Synapse 1.148.0rc1-mindroom.2 `org.matrix.simplified_msc3575` compatibility dialect and qualify it with the mandatory real-server Sliding canary.

**Architecture:** Sliding list operations become authenticated `SyncFrame` evidence and committed cursor state. A fresh schema-v2 journal owns immutable diagnostic scope, semantic list observations, stable event receipts and occurrences, and source-rotation receipts; a dedicated Sliding diagnostic planner strips only exact control/state evidence and creates Work only for one external LIVE target event. MindRoom selects the configured transport, reuses its existing same-batch admission receipt, and the live gate proves `target → control → target`, admission-post crash replay, a positionless restart request, one application effect, and duplicate-free idles.

**Tech Stack:** Python 3.13/3.14, asyncio, raw SQLite through the existing ingestion owner, matrix-nio source adapters, MindRoom's SQLite event journal, pytest, Ruff 0.15.12, ty 0.0.32, Black 26.5.1, Docker, and pinned Synapse 1.148.0rc1-mindroom.2.

## Global Constraints

- The normative design is `docs/superpowers/specs/2026-08-12-durable-sliding-ingestion-canary-design.md`, SHA-256 `fb4805a03ce07085d2ed91ec1021461e42f1f8a663c4ead0e50b0f9e04e48357`.
- Start from nio push head `b32ebf8953215bcf946b75094cbc2529f20e640f`, nio code ancestor `b313ae284be5dcc7797a7f7de83c70ad80520beb`, and MindRoom head `925df7f3cbcc39d4904140efd9d16b93c77238ea`.
- Preserve Classic evidence file SHA-256 `5a1e2e03778832301c3cf7ede223b1744edb1a656711a79e4778bf8c5f7fb838` and payload SHA-256 `9ea59ceaffbe73b3b0a2eed0a116020dba7ee8b78adfafde3272752222073fdd` as historical Classic-only evidence.
- Call the wire behavior the pinned Synapse `org.matrix.simplified_msc3575` compatibility dialect; never claim current or stable MSC4186 conformance.
- Classic remains the default and its request, materialization, delivery, and runner behavior remain unchanged.
- The ingestion journal schema advances to 2 only for fresh stores; schema 1 requires fresh initialization and is never migrated in place.
- The cross-repository `SyncBatch` schema remains 1.
- Ingestion rows remain canonical plaintext BLOBs with SHA-256 and digest-bound clear headers; add no encryption or re-encryption path.
- Add no public YAML/configuration field. The canary control room is an internal environment-bound diagnostic input and the persisted scope is an internal Python value.
- Use one production Sliding source. No raw-response observer or second Sliding connection can govern PASS.
- Every accepted record ends in Work or an explicit durable context, control, self-control, duplicate, or acknowledged receipt fate.
- Unseen or conflicting history blocks except for the three exact event-fate cases in the design.
- Every negative materializer test invokes the same candidate twice and requires identical `BLOCKED`, identical Frame and graph, zero DML, and zero transition callback.
- Every transaction change gets all-old/all-new statement-failure coverage.
- RED tests must reach their named causal barrier; collection, fixture, warning, skip, or pre-barrier failures do not count.
- Add no `noqa`, `type: ignore`, skip, xfail, or formatter suppression.
- Use targeted `git add` paths only; never use `git add .` or `git add -A`.
- Never amend, rebase, force-push, delete unrelated files, or stage the existing untracked review and egg-info paths.
- Each independently reviewable GREEN checkpoint is committed and pushed non-force after code/evidence review.
- Routine checkpoints continue autonomously; stop only for a genuine technical or external-state blocker.
- Sliding evidence contains no raw position, connection ID, room ID, event ID, principal, access token, event body, configuration, environment, HTTP body, log, SQL, PID, or traceback.
- A qualified Sliding diagnostic PASS changes no default, performs no cutover, and authorizes no release.

---

### Task 0: Freeze the reviewed design and executable plan

**Files:**

- Add: `docs/superpowers/specs/2026-08-12-durable-sliding-ingestion-canary-design.md`
- Add: `docs/superpowers/plans/2026-08-12-durable-sliding-ingestion-canary.md`

**Interfaces:**

- Consumes: the final hash-bound design and the reviewed Classic commit/evidence boundary.
- Produces: a docs-only implementation authority whose parent is nio head `b32ebf8953215bcf946b75094cbc2529f20e640f`.

- [ ] **Step 1: Verify the frozen inputs and clean code indexes**

```bash
test "$(sha256sum docs/superpowers/specs/2026-08-12-durable-sliding-ingestion-canary-design.md | cut -d' ' -f1)" = fb4805a03ce07085d2ed91ec1021461e42f1f8a663c4ead0e50b0f9e04e48357
test "$(git rev-parse HEAD)" = b32ebf8953215bcf946b75094cbc2529f20e640f
test -z "$(git diff --cached --name-only)"
git diff --check
git -C /work/dev/mindroom diff --check
```

- [ ] **Step 2: Run document self-review gates**

Require no placeholder token, false MSC4186 qualification, plaintext/encryption contradiction, wildcard test selector, or stale schema-1-journal claim.

```bash
for token in 'T''BD' 'TO''DO' 'typed-row'' encryption'; do
  ! rg -n "$token" \
    docs/superpowers/specs/2026-08-12-durable-sliding-ingestion-canary-design.md \
    docs/superpowers/plans/2026-08-12-durable-sliding-ingestion-canary.md
done
! rg -ni '(qualified|passes|conforms to) (current|stable) MSC4186' \
  docs/superpowers/specs/2026-08-12-durable-sliding-ingestion-canary-design.md \
  docs/superpowers/plans/2026-08-12-durable-sliding-ingestion-canary.md
rg -n 'schema-v2|schema 2|SyncBatch.*1|org.matrix.simplified_msc3575' \
  docs/superpowers/specs/2026-08-12-durable-sliding-ingestion-canary-design.md \
  docs/superpowers/plans/2026-08-12-durable-sliding-ingestion-canary.md
```

- [ ] **Step 3: Obtain independent plan review**

The reviewer must map every design section to a task, verify every later interface is introduced earlier, and reject placeholders or invented existing symbols.

- [ ] **Step 4: Commit and push the docs-only checkpoint**

```bash
git add \
  docs/superpowers/specs/2026-08-12-durable-sliding-ingestion-canary-design.md \
  docs/superpowers/plans/2026-08-12-durable-sliding-ingestion-canary.md
test "$(git diff --cached --name-only)" = $'docs/superpowers/plans/2026-08-12-durable-sliding-ingestion-canary.md\ndocs/superpowers/specs/2026-08-12-durable-sliding-ingestion-canary-design.md'
git commit -m "docs: design durable sliding ingestion canary"
git push origin docs/durable-ingestion-rewrite-plan
```

Require remote and local branch heads to match after the non-force push.

---

### Task 1: Normalize and persist Sliding list evidence in source values

**Files:**

- Modify: `src/nio/ingest/source.py`
- Modify: `src/nio/ingest/sliding.py`
- Test: `tests/ingest/source_parity_test.py`
- Test: `tests/ingest/sliding_source_test.py`
- Test: `tests/ingest/source_journal_test.py`

**Interfaces:**

- Consumes: existing `SlidingCursor`, `SlidingSource.normalize()`, `SyncFrame`, frozen request replay, and canonical Frame envelopes.
- Produces:

```python
class SlidingListOperationKind(StrEnum):
    SYNC = "SYNC"
    INSERT = "INSERT"
    DELETE = "DELETE"
    INVALIDATE = "INVALIDATE"

@dataclass(frozen=True, slots=True)
class SlidingListOperation:
    kind: SlidingListOperationKind
    range_start: int
    range_end: int
    room_ids: tuple[str, ...]

@dataclass(frozen=True, slots=True)
class SlidingListResult:
    list_name: str
    count: int
    operations: tuple[SlidingListOperation, ...]
```

`SyncFrame.sliding_list_results` is `tuple[SlidingListResult, ...]`; Classic frames carry `()`.

`SyncFrame.sliding_request_config_sha256` is a 32-byte digest for Sliding and `None` for Classic.

`SlidingCursor.list_state_json` is canonical JSON mapping each requested list to its bounded ordered room/null slots.

```python
def canonical_sliding_request_config(config: SlidingSourceConfig) -> bytes: ...

def sliding_request_config_sha256(config: SlidingSourceConfig) -> bytes: ...
```

The canonical object describes effective static wire behavior, not raw authored JSON. It contains timeout, connection name, planned caller-list definitions, planned room subscriptions, internally normalized extensions, and reserved-list page size. It excludes connection instance, transaction ID, position, request ID, dynamically expanded reserved ranges, dynamic to-device `since`, and every response field.

The config-side helper must reuse the source's production list/subscription/extension planning rules with a canonical cold cursor, then erase only the enumerated connection-scoped fields. Authored inputs that produce byte-identical effective wire behavior intentionally produce the same digest; no claim is made that overwritten raw extension keys are recoverable.

The normalizer has a private effective-template extractor for each frozen `NetworkRequest`: it validates the request/body/cursor relationship, removes the same enumerated connection-scoped fields from the already planned wire shape, hashes the result, and stores the digest on `SyncFrame`. It never tries to invert raw `extensions_json`, and it never trusts a digest supplied by MindRoom or a live config during replay.

- [ ] **Step 1: Write literal source RED tests**

Add `test_sync_frame_carries_frozen_ordered_sliding_list_results` and `test_sliding_full_sync_lists_are_normalized_and_advance_cursor_state` with a hand-written pinned response containing full `SYNC` operations for `probe:[0,0]` and `__nio_all_rooms_v1:[0,0]`.

Assert exact operation values, resulting occupants, literal candidate cursor JSON, and a literal request-config digest; do not derive expected values with the production applier.

Add `test_sliding_list_operations_fail_closed` covering unknown op, wrong field set, invalid/inverted/out-of-request range, contradictory or overlapping operations/ranges, room count mismatch, duplicate or malformed Matrix room ID, duplicate/missing/extra list, INSERT/DELETE/INVALIDATE without known prior state, and noncanonical prior list state.

Add `test_sliding_frame_envelope_round_trips_list_evidence_exactly` to stage, close, reopen, and renormalize the same response byte-identically.

- [ ] **Step 2: Run the Task 1 RED nodes**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/source_parity_test.py::test_sync_frame_carries_frozen_ordered_sliding_list_results \
  tests/ingest/sliding_source_test.py::test_sliding_full_sync_lists_are_normalized_and_advance_cursor_state \
  tests/ingest/sliding_source_test.py::test_sliding_list_operations_fail_closed \
  tests/ingest/source_journal_test.py::test_sliding_frame_envelope_round_trips_list_evidence_exactly \
  -q --no-cov
```

Expected: causal failures because `SyncFrame` and `SlidingCursor` do not own list evidence; all fixtures must have reached a status-200 normalized response.

- [ ] **Step 3: Add the exact source values and cursor field**

Implement strict type/range validation in `source.py` and extend `SyncFrame.__post_init__()` so non-Sliding frames require `sliding_list_results == ()` and `sliding_request_config_sha256 is None`, while Sliding requires one exact 32-byte digest.

Extend `SlidingCursor`, `canonical_sliding_cursor()`, `_sliding_cursor_from_json()`, `initial_cursor()`, and `reset_sliding_connection()` with `list_state_json`, using `b"{}"` for a cold connection.

- [ ] **Step 4: Normalize and apply list operations**

In `sliding.py`, parse every requested result from the frozen request body, validate all result/operation fields, and apply operations to a copied bounded slot map.

Use these exact semantics:

```python
SYNC      # replace every slot in the inclusive range with room_ids
INSERT    # insert one room at the one-slot range and shift within known slots
DELETE    # delete the one known slot and shift within known slots
INVALIDATE  # replace the inclusive known range with null slots
```

Reject a non-SYNC operation that touches unknown state, any resulting duplicate or malformed Matrix room ID, any contradictory/overlapping operation coverage, or an operation outside the list's requested ranges.

Store list-name-sorted `SlidingListResult` values in the Frame and canonical resulting state in the candidate cursor.

Reconstruct and bind the exact effective request-config digest from the frozen request. Reject missing/extra caller lists, altered effective static list fields, planned subscription/extension drift, timeout/connection-name/page-size drift, or any request shape that cannot map to the canonical effective template.

- [ ] **Step 5: Prove canonical staged replay without duplicating derived state**

Keep `_frame_envelope()` limited to the frozen request, raw response, and source digest. On reopen, the existing re-normalization path must reproduce the exact `sliding_list_results` and candidate cursor byte-for-byte from those authenticated inputs.

Update the exact `SyncFrame` surface parity assertion; direct Classic/test constructors may rely only on the field's immutable `()` default.

- [ ] **Step 6: Run focused GREEN and source regressions**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/source_parity_test.py::test_sync_frame_carries_frozen_ordered_sliding_list_results \
  tests/ingest/sliding_source_test.py::test_sliding_full_sync_lists_are_normalized_and_advance_cursor_state \
  tests/ingest/sliding_source_test.py::test_sliding_list_operations_fail_closed \
  tests/ingest/source_journal_test.py::test_sliding_frame_envelope_round_trips_list_evidence_exactly \
  tests/ingest/source_journal_test.py::test_reopened_deserialized_frame_renormalizes_exactly_across_config_drift \
  -q --no-cov
```

- [ ] **Step 7: Static review, commit, and push**

```bash
uv run --frozen --group dev ruff check \
  src/nio/ingest/source.py src/nio/ingest/sliding.py \
  tests/ingest/source_parity_test.py tests/ingest/sliding_source_test.py \
  tests/ingest/source_journal_test.py
uv run --frozen --group dev black --check \
  src/nio/ingest/source.py src/nio/ingest/sliding.py \
  tests/ingest/source_parity_test.py tests/ingest/sliding_source_test.py \
  tests/ingest/source_journal_test.py
git diff --check
```

Review operation application, cursor canonicality, frozen replay, and Classic empty-list behavior.

Commit only the five named paths and push non-force.

---

### Task 2: Create the fresh schema-v2 diagnostic ownership topology

**Files:**

- Create: `src/nio/ingest/diagnostic.py`
- Create: `src/nio/store/_sync_journal_diagnostic_rows.py`
- Modify: `src/nio/ingest/coordinator.py`
- Modify: `src/nio/ingest/state.py`
- Modify: `src/nio/store/sync_journal_schema.py`
- Modify: `src/nio/store/_sync_journal_port.py`
- Modify: `src/nio/store/_sync_journal_preflight.py`
- Modify: `src/nio/store/_sync_journal.py`
- Modify: `src/nio/store/sync_journal.py`
- Modify: `src/nio/store/database.py`
- Test: `tests/ingest/source_journal_test.py`
- Test: `tests/ingest/materializer_test.py`
- Test: `tests/ingest/coordinator_test.py`
- Test: `tests/ingest/crash_recovery_test.py`

**Interfaces:**

- Consumes: canonical plaintext row helpers, `OwnerView`, preflight owner/source authentication, and fresh-store topology validation.
- Produces:

```python
@dataclass(frozen=True, slots=True)
class DiagnosticIngestionScope:
    account_id: str
    delivery_room_id: str
    control_room_id: str
    list_name: str
    range_start: int
    range_end: int
    request_config_sha256: bytes
```

`open_ingestion_store(..., diagnostic_scope: DiagnosticIngestionScope | None = None)` rejects a scope for Classic. A source-only Sliding store may omit it, but it can never activate diagnostic materialization; the production Sliding runner supplies one at fresh creation.

`SqliteIngestionJournal.load_diagnostic_scope() -> DiagnosticIngestionScope | None` authenticates the stored row.

`open_ingestion(..., diagnostic_scope: DiagnosticIngestionScope | None = None)` requires `None` for Classic. For Sliding it requires an exact persisted scope before session claim or HTTP work, and `room_id` must equal `scope.delivery_room_id`.

`DiagnosticJournalRows` owns strict load/snapshot helpers for the immutable scope and four growing diagnostic inventories.

Freeze these hard caps in the RED tests: 4,096 list observations, 4,096 receipts, 8,192 occurrences, and 64 source rotations. Readers scan at most `cap + 1`, authenticate the complete returned inventory, and block before any prospective overflow.

- [ ] **Step 1: Write schema/scope RED tests**

Add `test_v2_fresh_topology_and_scope_rows_are_exact`, `test_v1_ingestion_store_requires_fresh_without_mutation`, `test_sliding_scope_mismatch_reopen_is_byte_identical`, `test_classic_v2_store_has_no_diagnostic_scope`, and `test_sliding_session_requires_exact_persisted_scope_before_claim`. Update existing `test_fresh_schema_creation_is_atomic_at_every_statement` to the exact schema-2 topology and include scope insertion in its all-old/all-new matrix.

The exact topology must include:

```text
NioIngestDiagnosticScope
NioIngestListObservation
NioIngestEventReceipt
NioIngestEventOccurrence
NioIngestSourceRotation
```

Assert schema 2 in `NioIngestMeta`, while serialized `SyncBatch.schema_version` remains 1.

- [ ] **Step 2: Run the Task 2 RED nodes**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/source_journal_test.py::test_v2_fresh_topology_and_scope_rows_are_exact \
  tests/ingest/source_journal_test.py::test_v1_ingestion_store_requires_fresh_without_mutation \
  tests/ingest/source_journal_test.py::test_sliding_scope_mismatch_reopen_is_byte_identical \
  tests/ingest/materializer_test.py::test_classic_v2_store_has_no_diagnostic_scope \
  tests/ingest/coordinator_test.py::test_sliding_session_requires_exact_persisted_scope_before_claim \
  tests/ingest/crash_recovery_test.py::test_fresh_schema_creation_is_atomic_at_every_statement \
  -q --no-cov
```

Expected: causal schema/symbol failures; the v1 fixture must remain byte-identical.

- [ ] **Step 3: Define the exact schema-2 tables**

Bump only the ingestion journal schema constant, the row-envelope schema in `_row()`, `OwnerView` journal-schema validation, and the borrowed ingestion-store marker check/error text to 2.

Add strict clear columns, canonical payload BLOB, SHA-256, primary/unique keys, and bounded enum checks for:

- immutable one-row diagnostic scope;
- per-Frame/list semantic observations;
- stable room/event receipts with fate `context|control|self_control|ready|outstanding|acknowledged`;
- per-Frame/event occurrences with disposition `application|context|control|self_control|duplicate`; and
- source rotations with reason `reopen|unknown_pos`.

The rotation row also has nullable first-successor request, Frame, and source-digest bindings. The first committed successor response fills them atomically and proves that request zero omitted `pos` without storing the position or connection identifier.

Use foreign keys only where both lifetimes are compatible: every diagnostic row may reference the persistent account owner, and an occurrence may reference its persistent receipt. Never reference `NioIngestFrame` or `NioIngestWork` from a persistent diagnostic row because Frames and Work retire while observations, occurrences, receipts, and rotations survive; bind those identities through authenticated IDs and digests instead.

- [ ] **Step 4: Implement fresh insertion and existing-store authentication**

Insert a supplied scope in the fresh bootstrap transaction after source creation. Classic rejects it; a scope-less Sliding source-only store keeps the table empty and can never acquire a scope later.

On reopen, authenticate the complete topology, Owner, Source, Frames, Aggregates, Work, delivery state, scope, and all four growing diagnostic inventories before any writer/source mutation. Reject schema 1 with `FreshIngestionRequired` before writes.

Require exact scope equality and exact request-config digest; a changed target, control, list, range, or request configuration is not a reset. Validate the session's exact scope before `_claim_session()` so a mismatch causes no ownership mutation or HTTP work.

- [ ] **Step 5: Add row-value and inventory helpers**

`_sync_journal_diagnostic_rows.py` defines frozen internal values and strict canonical encoders/decoders. Every inventory loader uses the frozen cap, queries at most `cap + 1`, and authenticates clear headers before returning payload values.

Make `SqliteIngestionJournal` inherit the focused diagnostic-row helper rather than adding the decoding logic to `_sync_journal.py`.

- [ ] **Step 6: Run GREEN plus topology/corruption coverage**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/source_journal_test.py::test_v2_fresh_topology_and_scope_rows_are_exact \
  tests/ingest/source_journal_test.py::test_v1_ingestion_store_requires_fresh_without_mutation \
  tests/ingest/source_journal_test.py::test_sliding_scope_mismatch_reopen_is_byte_identical \
  tests/ingest/materializer_test.py::test_classic_v2_store_has_no_diagnostic_scope \
  tests/ingest/coordinator_test.py::test_sliding_session_requires_exact_persisted_scope_before_claim \
  tests/ingest/crash_recovery_test.py::test_fresh_schema_creation_is_atomic_at_every_statement \
  tests/ingest/serialization_test.py tests/ingest/delivery_test.py \
  -q --no-cov
```

The delivery/batch tests prove wire schema 1 did not change.

- [ ] **Step 7: Static review, commit, and push**

Run Ruff, Black, `git diff --check`, exact topology snapshots, and suppression-delta checks on the named paths.

Review fresh-only versioning, no migration, payload/header equality, row caps, and Classic scope absence.

Commit only the named source/test paths and push non-force.

---

### Task 3: Rotate Sliding source connections atomically on reopen

**Files:**

- Modify: `src/nio/ingest/sliding.py`
- Modify: `src/nio/store/_sync_journal.py`
- Modify: `src/nio/store/_sync_journal_preflight.py`
- Modify: `src/nio/store/_sync_journal_diagnostic_rows.py`
- Test: `tests/ingest/sliding_source_test.py`
- Test: `tests/ingest/source_journal_test.py`

**Interfaces:**

- Consumes: schema-v2 owner/source/scope inventories and cold list-state representation.
- Produces:

```python
def reset_sliding_source_state(
    state: SourceState,
    *,
    next_source_epoch: int,
    connection_instance: UUID,
    require_position: bool,
) -> SourceState: ...
```

The preflight bootstrap writes one authenticated `SourceRotationValue(reason="reopen", ...)` in the same all-old/all-new transaction.

- [ ] **Step 1: Write pure reset and reopen RED tests**

Add `test_sliding_source_reset_rotates_only_connection_scoped_state`, `test_sliding_reopen_rotates_connection_and_preserves_durable_graph`, `test_sliding_reopen_rotation_is_all_old_or_all_new`, `test_first_successor_frame_binds_rotation_receipt_without_pos`, `test_reopen_twice_before_http_binds_only_newest_rotation`, and `test_source_rotation_bind_is_all_old_or_all_new`.

Assert source epoch advances, request ID becomes zero, connection changes, `pos` and list state clear, range/page configuration and `to_device_since` survive, and the exact Frame/Aggregate/Work/receipt/occurrence/list/delivery inventories do not change.

- [ ] **Step 2: Run the Task 3 RED nodes**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/sliding_source_test.py::test_sliding_source_reset_rotates_only_connection_scoped_state \
  tests/ingest/source_journal_test.py::test_sliding_reopen_rotates_connection_and_preserves_durable_graph \
  tests/ingest/source_journal_test.py::test_sliding_reopen_rotation_is_all_old_or_all_new \
  tests/ingest/source_journal_test.py::test_first_successor_frame_binds_rotation_receipt_without_pos \
  tests/ingest/source_journal_test.py::test_reopen_twice_before_http_binds_only_newest_rotation \
  tests/ingest/source_journal_test.py::test_source_rotation_bind_is_all_old_or_all_new \
  -q --no-cov
```

- [ ] **Step 3: Implement the pure reset**

Validate exact types, Sliding transport, increasing epoch, a new UUID, and `require_position`.

Delegate cursor clearing to `reset_sliding_connection()`, which must preserve `to_device_since`, connection name, page size, and expanded reserved range while clearing `pos`, ack/coverage state, and `list_state_json`.

- [ ] **Step 4: Rotate within the existing bootstrap transaction**

Change `_inspect_existing()` to return the authenticated `OwnerView`, `SourceState`, and diagnostic inventories already read; do not open a second connection or decode twice.

For Classic, retain the writer-only update.

Thread the existing `transition_statement_hook` into preflight; do not add another public hook. For Sliding, in one `bootstrap_write()` transaction:

```text
source_rotation_meta_cas
source_rotation_source_update
source_rotation_receipt_insert
before_commit
commit
```

Advance revision and `next_source_epoch`, rotate writer and source epoch/connection, install request ID zero, insert one rotation row, and fence every old clear/digest value. Emit `commit` only after the transaction context exits successfully.

Key each rotation by account plus successor source epoch and request zero. Multiple prior rotations may remain durably unbound if a process reopens again before HTTP; this is valid crash history, not ambiguity.

When `stage_source_response()` later commits the first successful successor response, require current epoch/request zero and an exact frozen request without `pos`, then update only the unbound rotation whose successor epoch/request matches that current request with its request, Frame, and source digests in the same stage transaction under hook `source_rotation_bind`. Prior unbound rows remain historical; a missing current row, multiple current matches, stale binding, duplicate binding, or contradictory binding is terminal before commit.

- [ ] **Step 5: Run GREEN, failure matrix, and Classic reopen**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/sliding_source_test.py::test_sliding_source_reset_rotates_only_connection_scoped_state \
  tests/ingest/source_journal_test.py::test_sliding_reopen_rotates_connection_and_preserves_durable_graph \
  tests/ingest/source_journal_test.py::test_sliding_reopen_rotation_is_all_old_or_all_new \
  tests/ingest/source_journal_test.py::test_first_successor_frame_binds_rotation_receipt_without_pos \
  tests/ingest/source_journal_test.py::test_reopen_twice_before_http_binds_only_newest_rotation \
  tests/ingest/source_journal_test.py::test_source_rotation_bind_is_all_old_or_all_new \
  tests/ingest/source_journal_test.py::test_exact_same_owner_reopen_preserves_identity_and_topology \
  -q --no-cov
```

- [ ] **Step 6: Static review, commit, and push**

Review old/new crash outcomes, first-successor binding, exhaustion, exact preservation, source payload/header CAS, and Classic non-regression.

Run Ruff, Black, diff/suppression gates, commit named paths, and push non-force.

---

### Task 4: Recover positioned `M_UNKNOWN_POS` through the journal owner

**Files:**

- Modify: `src/nio/store/_sync_journal.py`
- Modify: `src/nio/store/_sync_journal_port.py`
- Modify: `src/nio/ingest/coordinator.py`
- Test: `tests/ingest/source_journal_test.py`
- Test: `tests/ingest/coordinator_test.py`

**Interfaces:**

- Consumes: `reset_sliding_source_state()` and rotation-row storage.
- Produces:

```python
def reset_sliding_source(
    self,
    request: NetworkRequest,
) -> SourceState: ...
```

The coordinator handles `SourceResultKind.RESET_REQUIRED` only through that method.

- [ ] **Step 1: Write journal and coordinator RED tests**

Add `test_positioned_unknown_pos_rotates_durably`, `test_sliding_reset_rejects_stale_or_positionless_request`, `test_unknown_pos_rotation_is_all_old_or_all_new`, and `test_positioned_unknown_pos_replans_from_committed_cold_state`; extend `test_source_only_journal_port_is_exact` with the exact method signature and retain existing `test_sliding_reset_required_is_terminal_without_state_change`.

At the successor-request barrier require epoch `+1`, request ID zero, no `pos` query, changed connection digest, preserved `to_device_since`/range/page/durable graph, and one reason `unknown_pos` rotation row.

- [ ] **Step 2: Run the Task 4 RED nodes**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/source_journal_test.py::test_positioned_unknown_pos_rotates_durably \
  tests/ingest/source_journal_test.py::test_sliding_reset_rejects_stale_or_positionless_request \
  tests/ingest/source_journal_test.py::test_unknown_pos_rotation_is_all_old_or_all_new \
  tests/ingest/source_journal_test.py::test_source_only_journal_port_is_exact \
  tests/ingest/coordinator_test.py::test_positioned_unknown_pos_replans_from_committed_cold_state \
  tests/ingest/coordinator_test.py::test_sliding_reset_required_is_terminal_without_state_change \
  -q --no-cov
```

- [ ] **Step 3: Implement the fenced reset transaction**

Authenticate the exact current owner/source/request relation. Reject Classic, cold, stale epoch/request/cursor, wrong stream, exhausted epoch/revision, or a repeated reset.

Within `journal_write()` CAS owner revision/next epoch without changing writer epoch, update Source through a full old-row clear/payload/digest fence, and insert the rotation row.

Emit exact hooks `source_rotation_meta_cas`, `source_rotation_source_update`, `source_rotation_receipt_insert`, `before_commit`, and `commit`, with `commit` only after successful transaction exit.

- [ ] **Step 4: Implement coordinator handling**

Add `reset_sliding_source(self, request: NetworkRequest) -> SourceState` to `IngestionJournal` and its exact surface test. When and only when normalized result kind is `RESET_REQUIRED`, call the journal method, reset retry count, and continue so the next loop reloads committed source state.

Do not mutate the prior cursor/request in memory and do not fall back to Classic.

- [ ] **Step 5: Run GREEN and broader coordinator coverage**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/coordinator_test.py tests/ingest/sliding_source_test.py \
  tests/ingest/source_journal_test.py -q --no-cov
```

- [ ] **Step 6: Static review, commit, and push**

Review stale-request fencing, no cold loop, response release/cancellation, exact rotation preservation, and all-old/all-new statement failures.

Run statics, commit named paths, and push non-force.

---

### Task 5: Plan Sliding diagnostic fates without the generic reducer planner

**Files:**

- Create: `src/nio/store/_sync_journal_sliding_diagnostic.py`
- Create: `tests/ingest/sliding_diagnostic_test.py`
- Modify: `src/nio/store/_sync_journal_values.py`
- Test: `tests/ingest/materializer_test.py`

**Interfaces:**

- Consumes: normalized `SyncFrame.sliding_list_results` and `sliding_request_config_sha256`, authenticated scope, room aggregates, Work inventory, list observations, receipts, occurrences, and source rotations.
- Produces:

```python
@dataclass(frozen=True, slots=True)
class SlidingDiagnosticPlan:
    room_values: tuple[RoomAggregateValue, ...]
    work_inserts: tuple[PlannedWork, ...]
    list_observations: tuple[ListObservationValue, ...]
    receipt_inserts: tuple[EventReceiptValue, ...]
    receipt_updates: tuple[EventReceiptUpdate, ...]
    occurrences: tuple[EventOccurrenceValue, ...]
    timeline_boundary: Literal["complete", "bootstrap_truncated", "rotation_truncated"]

def plan_sliding_diagnostic_frame(
    *,
    account_id: str,
    stream_id: UUID,
    scope: DiagnosticIngestionScope,
    frame: SyncFrame,
    aggregates: tuple[RoomAggregateValue, ...],
    work: tuple[AuthenticatedWork, ...],
    list_observations: tuple[ListObservationValue, ...],
    receipts: tuple[EventReceiptValue, ...],
    occurrences: tuple[EventOccurrenceValue, ...],
    rotations: tuple[SourceRotationValue, ...],
    revision: int,
    limits: MaterializerLimits,
) -> SlidingDiagnosticPlan | None: ...
```

`None` means stable `BLOCKED`; malformed authenticated inventory raises integrity error.

- [ ] **Step 1: Write the literal positive RED matrix**

Use complete literal Frames and independently hand-derived expected values for:

1. safe empty Sliding;
2. epoch-zero target JOIN anchor plus exact bot warm-up context;
3. initial-limited target bootstrap with `bootstrap_truncated`;
4. same-connection control `SYNC[C]` plus one bot fixture/control receipt;
5. uninterrupted `SYNC[C] → SYNC[T]` re-entry plus one external LIVE message/application Work;
6. exact receipt replay/duplicate with zero Work;
7. one authenticated LIVE self response/self-control;
8. first rotation-cold HISTORY self response/self-control; and
9. rotation-cold exact duplicate/control receipts with `rotation_truncated`; and
10. one exact mixed target/control Frame whose list transition and every descriptor independently satisfy their approved roles.

Each case asserts stable event SHA, stable UUIDv5 record/Work identity, JOIN anchor, observation transition, receipt fate, occurrence disposition, and aggregate sequence literally.

- [ ] **Step 2: Write the fail-closed RED matrix**

The unsupported-frame matrix covers other/third rooms; mixed target/control descriptors whose roles or list transition do not all validate; scope/config mismatch; list op mismatch; absent JOIN anchor; interrupted cursor/position/list chain; initial/limited/expanded/gap/loss after anchor; any membership delta; stripped/invite/knock/leave/ban; encryption state/event; global/room account data; presence/ephemeral/to-device/device-list/fallback; invalid OTK maps; arbitrary or second external target event; arbitrary/second self event; external C event; extra/malformed state; unseen HISTORY; cap exhaustion; and missing, unbound, stale, or wrong-binding rotation proof for a rotation-cold fate.

Invoke every unsupported Frame candidate twice and assert the same `None` decision with immutable inputs.

A separate authenticated-corruption matrix covers malformed stored inventories, duplicate identities or duplicate current-rotation proofs, and existing receipt/event IDs with conflicting stable digests. Invoke each twice and require the same `JournalIntegrityError`; these are not unsupported Frames and must never be converted to `None`/`BLOCKED`.

- [ ] **Step 3: Run the pure RED suite**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/sliding_diagnostic_test.py -q --no-cov
```

Expected: collection succeeds and positive nodes fail because the planner is absent; negative fixture validation must reach planner invocation.

- [ ] **Step 4: Implement strict event/state/list classification**

Parse canonical events once. The stable digest excludes `unsigned` and includes role-bound room, event ID, type, sender, origin timestamp, state key, redaction target, and canonical content.

Use `UUIDv5(stream_id, f"timeline:{room_id}:{event_id}")` for Sliding application identity.

Strip only exact own-JOIN/enumerated non-encryption state after recording explicit control/context facts. Never send proof state to Work.

Permit zero OTK only for a canonical object whose keys are a subset of `curve25519|signed_curve25519` and whose values have exact type `int` and value zero.

- [ ] **Step 5: Implement continuity, list, and history decisions**

Require exact `probe` observations and durable anchor/config/cursor chain. Persist `seed|enter|evict|reenter|replace` plus boundary kind.

Before any fate decision, require the Frame's re-derived `sliding_request_config_sha256` to equal the immutable scope digest. Persist that digest in each list observation and use it in the uninterrupted-configuration chain.

Apply exactly the three history fates and two bounded limited exceptions from the design. Every other history/gap/loss returns `None` without a proposed write.

For either rotation-cold fate, require exactly one authenticated rotation whose successor epoch/request is the Frame's epoch/zero, whose first-successor binding is nonnull, and whose frozen request, Frame, and source digests match this Frame and prove no `pos`. Missing, unbound, stale, or contradictory valid proof returns `None`; duplicate current proof or malformed rotation inventory raises `JournalIntegrityError`.

- [ ] **Step 6: Implement receipt/occurrence/Work decisions**

External target LIVE creates one application receipt/occurrence and one READY Work.

Target self sender creates the bounded self-control fate only under measured-receipt and uniqueness rules.

Control-room bot fixture creates one control fate. An exact mixed target/control Frame may combine those already enumerated fates only when every descriptor and the list transition validate independently. Existing exact receipts create duplicate occurrences only. Conflicting digest is integrity failure.

- [ ] **Step 7: Run pure GREEN and discriminator checks**

Run the pure suite. Its literal positive/negative rows must independently discriminate the stable Work UUID namespace, sender branch, limited-boundary branch, JOIN-chain branch, OTK bool check, and duplicate digest comparison; do not mutate production source merely to test the tests.

- [ ] **Step 8: Static review and commit the pure planner checkpoint**

Run Ruff, Black, `git diff --check`, no-suppression gates, and independent review focused on record fate, fail-closed negatives, boundedness, and Classic isolation.

Commit the new planner/tests/value changes and push non-force.

---

### Task 6: Commit Sliding plans atomically and bind delivery receipt transitions

**Files:**

- Modify: `src/nio/store/_sync_journal.py`
- Modify: `src/nio/store/_sync_journal_port.py`
- Modify: `src/nio/store/_sync_journal_rows.py`
- Modify: `src/nio/store/_sync_journal_diagnostic_rows.py`
- Test: `tests/ingest/materializer_test.py`
- Test: `tests/ingest/delivery_test.py`
- Test: `tests/ingest/crash_recovery_test.py`

**Interfaces:**

- Consumes: `SlidingDiagnosticPlan` and existing Classic `plan_frame_materialization()`.
- Produces: one materializer writer branch for Sliding diagnostics; receipt fate transitions `ready → outstanding → acknowledged` in the existing delivery claim/ack transactions.

- [ ] **Step 1: Write materializer writer RED tests**

Add `test_sliding_diagnostic_writer_commits_each_fate_atomically`, `test_sliding_diagnostic_inventory_corruption_raises_before_dml`, `test_sliding_diagnostic_statement_failures_are_all_old_or_all_new`, and a parameterized `test_sliding_diagnostic_negative_frames_block_twice_without_dml` for each positive, unsupported-frame, and corruption planner case, exact SQL transition order, complete inventory revalidation, caps, collision, and statement-failure rollback.

The negative writer test uses every pure negative fixture through the real journal entrypoint twice. Each attempt must return `MaterializeStatus.BLOCKED` (never `AT_CAPACITY`), preserve the byte-identical Frame and complete authenticated graph, execute zero business DML, and emit zero transition callbacks.

The corruption writer test invokes every authenticated-inventory/digest-corruption fixture twice and requires the same `JournalIntegrityError`, byte-identical graph, zero business DML, and zero transition callbacks.

Require the application transaction to commit meta, aggregates, list observation, receipt, occurrence, Work, and Frame delete together.

Require context/control/self-control/duplicate transactions to commit their rows and Frame delete with zero Work.

- [ ] **Step 2: Write claim/ack RED tests**

Add `test_sliding_claim_binds_receipt_to_exact_batch` and `test_sliding_ack_deletes_work_and_acknowledges_receipt_atomically`.

Assert replaying an outstanding batch is write-free and byte-identical. Classic Work without a receipt retains existing behavior.

Add `test_sliding_receipt_survives_process_death_boundaries` to the existing crash-recovery matrix, covering process death after each receipt/materializer/claim/ack DML and accepting only exact all-old or all-new graphs.

- [ ] **Step 3: Run Task 6 RED nodes**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/materializer_test.py::test_sliding_diagnostic_writer_commits_each_fate_atomically \
  tests/ingest/materializer_test.py::test_sliding_diagnostic_inventory_corruption_raises_before_dml \
  tests/ingest/materializer_test.py::test_sliding_diagnostic_statement_failures_are_all_old_or_all_new \
  tests/ingest/materializer_test.py::test_sliding_diagnostic_negative_frames_block_twice_without_dml \
  tests/ingest/delivery_test.py::test_sliding_claim_binds_receipt_to_exact_batch \
  tests/ingest/delivery_test.py::test_sliding_ack_deletes_work_and_acknowledges_receipt_atomically \
  tests/ingest/crash_recovery_test.py::test_sliding_receipt_survives_process_death_boundaries \
  -q --no-cov
```

- [ ] **Step 4: Branch only the diagnostic plan construction**

In `_materialize_oldest_frame()`, keep source/owner/Frame/Work/Aggregate authentication common.

Use `plan_sliding_diagnostic_frame()` only when diagnostic scope exists and transport is Sliding; keep the current Classic classifier and generic planner byte-for-byte in the other branch.

Do not modify the generic reducer to trust Sliding membership or history.

Extend Work authentication to allow a nonempty `EventRecord.event_id` only when its record ID is the exact diagnostic UUIDv5 identity and an exact authenticated diagnostic receipt owns it. Existing Classic Work retains the current null-event-ID invariant.

- [ ] **Step 5: Write diagnostic rows in one transaction**

Revalidate complete scope/list/receipt/occurrence/rotation inventories at the writer boundary. For a rotation-cold plan, re-prove the exact unique bound current-epoch rotation and its request/Frame/source digests before any DML.

Emit hooks after each business DML: `list_observation_insert`, `event_receipt_insert|update`, `event_occurrence_insert`, `work_insert`, `frame_delete`, plus existing meta/before_commit/commit hooks.

Any rowcount, collision, cap, digest, or snapshot mismatch rolls back the whole transaction.

- [ ] **Step 6: Bind claim and acknowledgement**

During `next_batch()`, if READY Work has a receipt, update it to `outstanding` with exact sequence/batch digest in the same transaction as delivery meta CAS.

During `acknowledge_batch()`, update the exact receipt to `acknowledged` with acknowledgement revision in the same transaction as Work delete and delivery meta CAS.

Fence old fate/work/sequence/digest/revision fields; mismatches are conflict/integrity failures, never retries.

- [ ] **Step 7: Run GREEN and full materializer/delivery regressions**

```bash
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/sliding_diagnostic_test.py \
  tests/ingest/materializer_test.py \
  tests/ingest/delivery_test.py \
  tests/ingest/crash_recovery_test.py \
  -q --no-cov
```

- [ ] **Step 8: Static review, commit, and push**

Review transaction order, writer revalidation, stable Work identity, receipt survival after Work deletion, Classic branch identity, and every crash boundary.

Run Ruff, Black, diff/suppression/numstat gates, commit named paths, and push non-force.

---

### Task 7: Select Sliding in the MindRoom durable runner

**Files:**

- Modify: `/work/dev/mindroom/src/mindroom/matrix/durable_ingestion.py`
- Test: `/work/dev/mindroom/tests/test_durable_ingestion_admission.py`

**Interfaces:**

- Consumes: `SlidingSourceConfig`, `DiagnosticIngestionScope`, schema-v2 store/session, stable Sliding LIVE Work, and existing admission receipt semantics.
- Produces:

```python
def validate_ingestion_batch(
    batch: ingest.SyncBatch,
    *,
    account_id: str,
    device_id: str,
    expected_transport: ingest.TransportKind,
) -> ej.IngestionBatchAdmission: ...
```

`consume_one_ingestion_batch()` receives and forwards the expected transport.

Keep Sliding-only imports lazy so this module and its historical Task-5 golden test still collect against the pinned `/tmp/nio-task5-install.yiFetw` Classic wheel. The candidate-focused commands explicitly put the current nio source first on `PYTHONPATH`.

- [ ] **Step 1: Write runner/validator RED tests**

Add exact tests for:

- Classic source/filter remains unchanged;
- Sliding requires `MINDROOM_INGESTION_CANARY_CONTROL_ROOM` to be one exact nonempty Matrix room ID distinct from the target;
- Sliding constructs `probe` `[0,0]`, recency sort, timeline limit one, exact `is_encrypted:false`/`is_invite:false`, own-member/encryption required state, empty explicit room subscriptions, page size one, and required extensions;
- scope/request-config digest passed to fresh store;
- configured expected transport accepted and a mismatch rejected;
- Sliding batch remains schema 1, one external LIVE `m.room.message`, and all existing identity/digest/membership/projection checks remain; and
- self/control/context occurrences never reach the batch validator.

The focused node names are `test_durable_runner_keeps_classic_source_unchanged`, `test_durable_runner_selects_exact_sliding_source_and_scope`, and `test_validate_ingestion_batch_accepts_configured_transport_only`.

- [ ] **Step 2: Run Task 7 RED nodes**

```bash
cd /work/dev/mindroom
PYTHONPATH=/work/dev/mindroom-nio/src:/work/dev/mindroom/src \
PYTHONNOUSERSITE=1 .venv/bin/python -m pytest \
  tests/test_durable_ingestion_admission.py::test_durable_runner_keeps_classic_source_unchanged \
  tests/test_durable_ingestion_admission.py::test_durable_runner_selects_exact_sliding_source_and_scope \
  tests/test_durable_ingestion_admission.py::test_validate_ingestion_batch_accepts_configured_transport_only \
  -n 0 -q
```

- [ ] **Step 3: Implement exact transport selection**

Keep the current Classic builder in its own branch.

For Sliding, read and validate `MINDROOM_INGESTION_CANARY_CONTROL_ROOM` before filesystem/HTTP work, construct the literal config, call nio's `sliding_request_config_sha256(source)`, and place that digest in `DiagnosticIngestionScope`. Pass the same source/scope to store/session; do not duplicate the canonical encoding in MindRoom.

Do not use the ordinary `sliding_sync_forever()` cache or a second source.

- [ ] **Step 4: Generalize validation only by expected transport**

Replace the hard-coded Classic origin check with exact equality to `expected_transport`; keep schema, payload, event, membership, batch, projection, and digest checks unchanged. Update the existing Task-5 golden call to pass `TransportKind.CLASSIC` explicitly without changing its exact bytes, origin assertion, or expected admission.

Pass the configured transport through the pump.

- [ ] **Step 5: Run GREEN and Classic runner regressions**

```bash
cd /work/dev/mindroom
PYTHONPATH=/work/dev/mindroom-nio/src:/work/dev/mindroom/src \
PYTHONNOUSERSITE=1 .venv/bin/python -m pytest \
  tests/test_durable_ingestion_admission.py \
  --deselect=tests/test_durable_ingestion_admission.py::test_validation_freezes_task5_golden_and_exact_conversion \
  -n 0 -q
PYTHONPATH=/tmp/nio-task5-install.yiFetw:/work/dev/mindroom/src \
PYTHONNOUSERSITE=1 .venv/bin/python -m pytest \
  tests/test_durable_ingestion_admission.py::test_validation_freezes_task5_golden_and_exact_conversion \
  -n 0 -q
```

- [ ] **Step 6: Run MindRoom statics and review**

```bash
cd /work/dev/mindroom
uv run --frozen --group dev ruff check \
  src/mindroom/matrix/durable_ingestion.py \
  tests/test_durable_ingestion_admission.py
uv run --frozen --group dev ty check src/mindroom/matrix/durable_ingestion.py
uv run --frozen --group dev black --check \
  src/mindroom/matrix/durable_ingestion.py \
  tests/test_durable_ingestion_admission.py
git diff --check
```

Review config exactness, no secret persistence, Classic identity, validator parity, and task cancellation/cleanup.

- [ ] **Step 7: Commit and push MindRoom**

Stage exactly the runner and test file, commit without amend, and push `wip/matrix-journal-ingress-cutover` non-force.

---

### Task 8: Prove cross-repository crash replay deterministically

**Files:**

- Modify: `/work/dev/mindroom/tests/test_durable_ingestion_admission.py`
- Modify: `tests/ingest/coordinator_test.py`

**Interfaces:**

- Consumes: the production nio session, production MindRoom admission owner, Sliding diagnostic scope, transition hooks, and response-release/cancellation seams.
- Produces: deterministic real-component coverage of `T → C → T`, admission-post crash state, reopen rotation, `DUPLICATE`, ACK, self-control echo, and occurrence-free idles.

- [ ] **Step 1: Write the integrated causal RED node**

Use a real temporary nio database and real MindRoom SQLite event journal. Patch only base `nio.AsyncClient.send` with complete pinned response objects; keep normalization, staging, materialization, delivery, admission, and acknowledgement real.

Use events/latches, never sleeps, for:

1. seed target materialization committed;
2. release C fixture response;
3. measured T re-entry and Work ready;
4. pause immediately after real MindRoom `ADMITTED` and before ACK;
5. close/kill-equivalent ownership boundary;
6. reopen first request observed without `pos`;
7. release exact duplicate/self-control cold response;
8. replay same batch returns `DUPLICATE`; and
9. two held-idle responses with no occurrence mutation.

- [ ] **Step 2: Assert exact graphs at each barrier**

Before crash: one outstanding Work/receipt, one admission receipt, no ACK.

After reopen: one rotation row, unchanged batch/Work/receipt, request ID zero/no position, fresh connection digest.

After replay: same batch ID/digest, `ADMITTED → DUPLICATE`, Work zero, receipt acknowledged once, frontier one.

After response echo: one outbox-correlated self-control receipt/occurrence, zero Work.

Across each idle: no new occurrence of any disposition and no application/delivery graph change.

- [ ] **Step 3: Run the isolated RED/GREEN node**

```bash
cd /work/dev/mindroom
PYTHONPATH=/work/dev/mindroom-nio/src:/work/dev/mindroom/src \
PYTHONNOUSERSITE=1 .venv/bin/python -m pytest \
  tests/test_durable_ingestion_admission.py::test_sliding_reentry_admission_post_crash_replays_once \
  -n 0 -q
```

The initial RED must reach the named missing production behavior, not fail in fake response shape or cleanup.

- [ ] **Step 4: Add coordinator ordering regression**

Add `test_sliding_reopen_drains_old_frame_before_positionless_request`. Prove old frozen Frames drain before new HTTP, old outstanding Work remains deliverable concurrently, and reset reloads committed state.

Run the two exact nodes in their owning repositories:

```bash
cd /work/dev/mindroom-nio
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/coordinator_test.py::test_sliding_reopen_drains_old_frame_before_positionless_request \
  -q --no-cov

cd /work/dev/mindroom
PYTHONPATH=/work/dev/mindroom-nio/src:/work/dev/mindroom/src \
PYTHONNOUSERSITE=1 .venv/bin/python -m pytest \
  tests/test_durable_ingestion_admission.py::test_sliding_reentry_admission_post_crash_replays_once \
  -n 0 -q
```

- [ ] **Step 5: Independent cross-repository review**

Review real-component ownership, latch positions, response release, cancellation, database cleanup, exact graph assertions, and absence of timing sleeps.

Commit test corrections with their owning repository checkpoints and push non-force.

---

### Task 9: Run complete behavior, static, and scope verification

**Files:**

- No new production file.
- Update only tests needed to repair a proven regression; any semantic change returns to its RED task.

**Interfaces:**

- Consumes: final nio and MindRoom code candidates.
- Produces: a frozen code/evidence set authorized for exact-wheel live testing.

- [ ] **Step 1: Run focused Sliding suites**

```bash
cd /work/dev/mindroom-nio
PYTHONNOUSERSITE=1 uv run --frozen --group dev python -m pytest \
  tests/ingest/sliding_source_test.py \
  tests/ingest/source_journal_test.py \
  tests/ingest/sliding_diagnostic_test.py \
  tests/ingest/materializer_test.py \
  tests/ingest/delivery_test.py \
  tests/ingest/coordinator_test.py \
  tests/ingest/crash_recovery_test.py \
  -q --no-cov
```

- [ ] **Step 2: Confirm exhaustive Classic non-regression coverage**

Task 9 Step 1 deliberately runs the complete nio source-journal, materializer, delivery, coordinator, and crash-recovery files, not Sliding-only selectors; those files are the exhaustive Classic gate and include the previously qualified Option-C nodes. Step 3 likewise runs the complete candidate MindRoom durable-admission file plus the exact historical golden node under its pinned wheel. Do not add a redundant partial selector, and do not remove or weaken any Classic test ID to make Sliding pass.

- [ ] **Step 3: Run MindRoom durable suites**

```bash
cd /work/dev/mindroom
PYTHONPATH=/work/dev/mindroom-nio/src:/work/dev/mindroom/src \
PYTHONNOUSERSITE=1 .venv/bin/python -m pytest \
  tests/test_durable_ingestion_admission.py \
  --deselect=tests/test_durable_ingestion_admission.py::test_validation_freezes_task5_golden_and_exact_conversion \
  tests/test_event_journal_crash_matrix.py \
  -n 0 -q
PYTHONPATH=/tmp/nio-task5-install.yiFetw:/work/dev/mindroom/src \
PYTHONNOUSERSITE=1 .venv/bin/python -m pytest \
  tests/test_durable_ingestion_admission.py::test_validation_freezes_task5_golden_and_exact_conversion \
  -n 0 -q
```

- [ ] **Step 4: Run statics**

Run Ruff and Black over every changed nio source/test file; run Ruff, ty, and Black over every changed MindRoom source/test file; run `git diff --check` in both repositories.

- [ ] **Step 5: Freeze scope and suppression inventories**

Record exact heads, parents, subjects, changed paths, blobs, numstats, full/production patch SHA-256 values, schema topology, zero new suppression/skip/xfail counts, and both empty indexes.

Reject any unrelated tracked change or staged untracked file.

- [ ] **Step 6: Obtain independent final code/evidence review**

Review must cover dialect honesty, schema/wire version split, normalized list evidence, rotation atomicity, record fates, receipt transitions, Classic non-regression, all-occurrence idle, cleanup, and release limits.

- [ ] **Step 7: Commit/push any final reviewed checkpoint**

Use targeted staging and non-force pushes. Recompute all frozen hashes after a commit; never reuse pre-commit hashes as post-commit evidence.

---

### Task 10: Build exact wheels and run the one-shot pinned-Synapse canary

**Files:**

- Create: `.superpowers/sdd/2026-08-10-durable-ingestion-scope-correction/task-8-sliding-brief.md`
- Create: `.superpowers/sdd/2026-08-10-durable-ingestion-scope-correction/checkpoint-8-sliding-canary.json`
- Modify: the governing design/report/progress artifacts in that SDD directory.
- Modify: `docs/superpowers/plans/2026-08-10-durable-ingestion-scope-correction.md`

**Interfaces:**

- Consumes: final reviewed nio/MindRoom commits, exact-wheel build procedure, pinned Synapse image, Classic operator ownership/cleanup framework, and the Task 8 cross-repository latch semantics.
- Produces: exactly one independently reviewed, canonical, secrecy-audited Sliding evidence object or a fixed-label STOP.

- [ ] **Step 1: Write and review the executable brief before live action**

Bind exact commits, parents, path sets, blob/patch hashes, schema versions, wheel expectations, Synapse version/image digest, dialect label, immutable scope, page size one, and all cleanup identities.

The operator must use fixed labels and emit no raw runtime diagnostic content.

- [ ] **Step 2: Add pure operator self-tests**

Cover exact list occupant sequence, JOIN-anchor continuity, limited boundary kinds, rotation request no-position, receipt/occurrence chain, admission result sequence, outbox self-control correlation, all-occurrence idle equality, fixed schemas, and forbidden-key/value scans.

The self-test uses synthetic allowlisted evidence only and performs no service/network action.

- [ ] **Step 3: Build fresh exact wheels in an owned lab**

Reconstruct both reviewed commits from archives, build/install exact wheels, prove Python/distribution/origin/lock closure, and record wheel sizes/SHA-256 values.

Do not reuse a prior Classic or failed Sliding lab.

- [ ] **Step 4: Run the real same-connection window sequence**

Create one disposable canary account and exactly two unencrypted joined rooms.

Send the bot-authored target warm-up, start production Sliding, and stop at the seed-materialization postcommit latch before another HTTP request can escape.

Send the bot-authored C fixture while stopped, resume, require committed `SYNC[C]`, then send the sole external measured T event and require committed `SYNC[T]` with exact LIVE provenance.

- [ ] **Step 5: Run the admission-post crash and restart**

Stop after real MindRoom `ADMITTED` and before nio ACK, kill the whole group, preserve both databases, restart, require the rotation receipt and first successor request without `pos`, then require the same batch `DUPLICATE` and one ACK/frontier advance.

- [ ] **Step 6: Settle the self response and durable idles**

Require exactly one visible response and one correlated self-control occurrence, then take two ten-second durable idle observations with no new event occurrence of any disposition and no application/delivery graph change. Source request count and opaque-position digest may advance.

- [ ] **Step 7: Emit and capture evidence losslessly**

Use both outer execution and inner PTY output budgets large enough for the complete final line. Require zero exit, no truncation metadata, exactly one complete newline-terminated `TASK8_SLIDING_EVIDENCE_INPUT=` line, and canonical compact JSON.

Missing, duplicate, partial, or truncated evidence is STOP and cannot be reconstructed.

- [ ] **Step 8: Audit and materialize the exact evidence**

Independently validate schemas, role-bound hashes, all phase/graph transitions, absence of forbidden keys/values, cleanup, and `outcome:"PASS"`.

Use `apply_patch` to materialize the exact JSON payload, record payload and one-terminal-LF file SHA-256 separately, and freeze governing docs for final review.

- [ ] **Step 9: Verify cleanup and hand off**

Require lab absence, exact service gone/inactive/dead/PIDs zero/empty cgroup, zero process references, zero canary registry rows, clean heads/indexes/locked paths, and no retained secret-bearing artifact.

Report the qualified result only as the pinned-Synapse diagnostic Sliding canary prerequisite. Keep default enablement, cutover, release, and general recovery blocked.

# Durable Ingestion Task 4.5 Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task with a fresh implementer and a fresh reviewer at each checkpoint.

**Goal:** Replace the oversized Task 4.5 carrier/effect platform and incomplete guarded-Peewee experiment with a source-only durable core that stores one canonical response, re-normalizes it after restart, and shares one ordinary Peewee connection with the exact `SqliteStore` type.

**Architecture:** A thin private `IngestionStoreOwner` owns the lifetime lock and one ordinary `peewee.SqliteDatabase`. Bootstrap, journal raw SQL, and Peewee E2EE operations all enter owner scopes; journal SQL uses `database.execute_sql()` and write scopes use Peewee `atomic("IMMEDIATE")`. The unreleased v1 ingestion topology contains only meta, source state, and staged frames. All room, lane, loss, batch, receipt, pending-decryption, effect, membership-delivery, and attachment persistence is removed until the task that owns both its producer and consumer.

**Tech Stack:** Python 3.13+, Peewee, SQLite/WAL, PyCryptodome AES-GCM, pytest, uv, Ruff, Black, pre-commit.

## Global Constraints

- The normative design is `docs/superpowers/specs/2026-08-09-durable-ingestion-simplification-design.md`.
- This plan supersedes `docs/superpowers/plans/2026-08-08-durable-matrix-ingestion-rewrite.md` Task 4.5 lines 1158-1437, Task 5's persistence assumptions at lines 1448-1519, and Task 6's prebuilt-lane/batch assumptions at lines 1591-1623. Those passages remain historical only.
- Preserve immutable transport behavior from commit `59642ef`, stored membership observations from `d4edd91`, and the canonical staging semantics from `61ea2cd`; re-port staging rather than mechanically retaining its carrier dependencies.
- Remove/defer the production introduced by `22606cd`, `39467c3`, `aa2ef44`, `c2a623f`, `c4cec82`, `da8df4e`, and `8bdd360`, except for cycle-neutral strict JSON helpers still required by canonical staging.
- Keep `SCHEMA_VERSION = 1`. This format is unreleased; add no migration.
- Only an absent or zero-length path may be initialized. Every nonempty no-marker, legacy, abandoned-v1, wrong-account, wrong-device, wrong-transport, or topology-mismatched database must raise `FreshIngestionRequired` before DDL, epoch takeover, E2EE construction, or writes.
- The exact ingestion topology is `NioIngestMeta`, `NioIngestSourceState`, `NioIngestFrame`, and `NioIngestFrame_drain`. E2EE tables are separately owned by `SqliteStore` and are not part of the ingestion topology comparison.
- The file-backed database is an ordinary `peewee.SqliteDatabase`; do not subclass or wrap it to guard `cursor`, `atomic`, `connect`, or other Peewee methods. The database is internal and architecture tests prevent new production reach-through.
- No HTTP or crypto computation may run inside a database transaction.
- The simplified checkpoint exposes no activation/attachment/coordinator path and therefore cannot enable Matrix HTTP.
- Production Python net is measured against fixed base `980e53b3a3815592c3bd0dab9e41e2b057e50724`. The final cross-repository net limit remains +6,000.
- After every task or 500 production additions, report actual additions/deletions, newly unlocked deletions, remaining forecast, SQL counts, and any estimate miss above 50 percent; continue automatically while the hard constraints pass.
- Preserve the unrelated untracked `src/mindroom_nio.egg-info/` tree.
- Every behavior change follows RED -> minimal GREEN -> focused regression -> review -> commit -> push.

---

## Task 1: Remove the Rejected Guard Experiment and Freeze a Clean Baseline

**Files:**

- Restore to `HEAD`: `src/nio/store/_sync_journal.py`
- Restore to `HEAD`: `src/nio/store/_sync_journal_effects.py`
- Restore to `HEAD`: `src/nio/store/_sync_journal_preflight.py`
- Restore to `HEAD`: `src/nio/store/_sync_journal_rows.py`
- Restore to `HEAD`: `src/nio/store/database.py`
- Restore to `HEAD`: `src/nio/store/sync_journal.py`
- Restore to `HEAD`: `tests/ingest/crash_recovery_test.py`
- Restore to `HEAD`: `tests/ingest/journal_test.py`
- Restore to `HEAD`: `tests/ingest/membership_operation_journal_test.py`
- Restore to `HEAD`: `tests/ingest/network_effect_journal_test.py`
- Delete: untracked `src/nio/store/_store_writer.py`
- Preserve: untracked `src/mindroom_nio.egg-info/`

### Step 1: Record the rejected experiment precisely

Run:

```bash
git status --short
git diff --stat -- \
  src/nio/store/_sync_journal.py \
  src/nio/store/_sync_journal_effects.py \
  src/nio/store/_sync_journal_preflight.py \
  src/nio/store/_sync_journal_rows.py \
  src/nio/store/database.py \
  src/nio/store/sync_journal.py \
  tests/ingest/crash_recovery_test.py \
  tests/ingest/journal_test.py \
  tests/ingest/membership_operation_journal_test.py \
  tests/ingest/network_effect_journal_test.py
wc -l src/nio/store/_store_writer.py
```

Expected: only the named failed experiment plus the unrelated egg-info is dirty; `_store_writer.py` is approximately 425 lines.

Proceed only if every dirty hunk in the ten tracked files and the untracked `_store_writer.py` exactly matches the recorded rejected experiment, and the only other change is the preserved egg-info tree. If any path or hunk differs, stop without restoring or deleting anything and report the mismatch.

### Step 2: Discard only the rejected experiment

Restore the ten tracked files from `HEAD` using explicit paths. Delete only `_store_writer.py`. Do not use a repository-wide reset or clean.

### Step 3: Verify the retained source boundary

Run:

```bash
uv run pytest -q \
  tests/ingest/classic_source_test.py \
  tests/ingest/sliding_source_test.py \
  tests/ingest/membership_test.py \
  tests/ingest/source_parity_test.py
git status --short
```

Expected: the source and observation suites pass; only `src/mindroom_nio.egg-info/` remains untracked. This cleanup is not committed because it merely removes uncommitted rejected work.

---

## Task 2: Specify the Source-Only Durable Contract

**Files:**

- Create: `tests/ingest/source_journal_test.py`
- Replace/reduce: `tests/ingest/journal_test.py`
- Replace/reduce: `tests/ingest/crash_recovery_test.py`
- Modify: `tests/ingest/model_test.py`
- Modify: `tests/ingest/serialization_test.py`
- Modify: `tests/ingest/membership_test.py`
- Delete later in Task 3: `tests/ingest/effects_test.py`
- Delete later in Task 3: `tests/ingest/network_effect_journal_test.py`
- Delete later in Task 3: `tests/ingest/membership_operation_journal_test.py`

### Step 1: Add an exact pure-value contract test

In `tests/ingest/source_journal_test.py`, add `test_source_only_state_surface_is_exact`. Pin these dataclass fields:

```python
assert tuple(field.name for field in fields(OwnerView)) == (
    "account_id",
    "device_id",
    "schema_version",
    "stream_id",
    "transport_kind",
    "revision",
    "writer_epoch",
    "next_source_epoch",
)
assert tuple(field.name for field in fields(SourceState)) == (
    "source_epoch",
    "transport_kind",
    "cursor_json",
    "next_request_id",
    "active",
)
assert tuple(field.name for field in fields(StagedFrame)) == (
    "frame_id",
    "response",
    "staged_revision",
)
assert tuple(field.name for field in fields(CommitResult)) == ("revision",)
```

Also assert that `nio.ingest.state` exports none of:

```python
RoomState, RoomLane, RoomAggregate, LaneRecord, ReadyRecord,
BatchMaterialization, JournalTransition, AckOutcome, ConsumerAttachStatus
```

### Step 2: Add an exact port contract test

Pin `IngestionJournal` to exactly:

```python
load_owner() -> OwnerView
load_source() -> SourceState
load_frame(frame_id: UUID) -> StagedFrame | None
list_frames(limit: int) -> tuple[StagedFrame, ...]
stage_source_response(
    *,
    expected_revision: int,
    writer_epoch: UUID,
    source: SourceState,
    frame: StagedFrame,
) -> CommitResult
```

Assert no room/lane/ready/batch/ack/effect/attachment method remains.

### Step 3: Add exact topology and fresh-path tests

Filter `sqlite_master` to ingestion-owned names (`NioIngestMeta`, `NioIngestSourceState`, `NioIngestFrame`, `NioIngestFrame_drain`, and every other name prefixed `NioIngest`). Require that filtered set and its normalized DDL to contain exactly:

```text
table NioIngestMeta
table NioIngestSourceState
table NioIngestFrame
index NioIngestFrame_drain
```

Do not treat borrowed E2EE tables or indexes as ingestion-topology members. For each of these pre-created nonempty paths, assert `FreshIngestionRequired`, no new tables, byte-for-byte file preservation as supplemental evidence, and no writer-epoch update:

- an arbitrary legacy table without `NioIngestMeta`;
- an E2EE-only legacy store;
- the abandoned current-v1 schema;
- a simplified-v1 schema with an extra ingestion table/index;
- a simplified-v1 schema with a missing column/check/index;
- exact topology owned by another account or device;
- exact topology with another transport.

Add positive tests for an absent path, a zero-length path, and an exact same-owner reopen.

For every rejected case, the statement observer must show no DDL/DML, no write-producing pragma, no epoch update, and no `SqliteStore` construction. The byte comparison is not a substitute for this zero-write trace.

### Step 4: Add source/frame transaction tests

Construct valid Classic and Sliding `StagedFrame` values from real adapter normalization. Assert:

- a successful stage atomically advances `SourceState`, inserts one frame, and increments revision exactly once;
- exact idempotent re-stage returns the stored revision without writes;
- same frame ID with changed request/body/digest/revision fails before meta/source mutation;
- wrong expected revision, writer epoch, stream, transport, source epoch, request ID, or cursor relation fails before business DML;
- `list_frames(limit)` accepts exact integers 1-256 and orders by `(staged_revision, source_epoch, request_id, frame_id)` using `NioIngestFrame_drain`;
- there is no delete/consume operation in this checkpoint.

The source/frame relation is:

```python
frame.response.request.stream_id == owner.stream_id
frame.response.request.transport is owner.transport_kind
frame.response.request.source_epoch == source.source_epoch
frame.response.request.request_id + 1 == source.next_request_id
source.transport_kind is owner.transport_kind
```

The current stored source state must equal `frame.response.request.request_cursor_json` and the request ID being completed before the proposed successor is accepted.

### Step 5: Add authenticated-header and canonical-envelope tests

Pin frame ciphertext to the exact canonical object:

```python
{
    "normalization_version": 1,
    "request": <exact frozen NetworkRequest object>,
    "response_body": <base64 canonical bytes>,
    "source_sha256": <base64 SHA-256 bytes>,
}
```

The clear frame header is the canonical byte encoding of `(source_epoch, request_id, staged_revision)` and is included in AES-GCM AAD. The source-state header is the canonical byte encoding of `(transport_kind, source_epoch, next_request_id, active)` and is included in AAD. Add corruption tests for every clear header field, ciphertext, digest, primary key, account, stream, normalization version, missing/extra envelope key, noncanonical envelope bytes, nested request field, body, and source digest. Each selected corruption fails as `JournalIntegrityError`.

Assert the canonical response body appears exactly once in decrypted durable payload and that no serialized `SyncFrame`, room segment, event list, or parsed normalized frame is stored.

### Step 6: Add crash and restart tests

The statement-hook kill matrix covers:

```text
meta revision/epoch CAS
source-state upsert
frame insert
frame collision probe
commit
```

After process interruption and reopen, assert exact old graph or exact new graph—never a new source cursor without its frame or a frame without its source cursor.

For both Classic and Sliding, load the frame from a reopened journal, construct a new adapter with drifted runtime configuration, call `renormalize_staged_frame`, and require the exact original `SyncFrame`, membership observations, digest, and frame ID. Use non-neutral own-membership fixtures.

### Step 7: Run RED

Run:

```bash
uv run pytest -q tests/ingest/source_journal_test.py
```

Expected: failures for the old state/port/topology and missing `stage_source_response`; source adapter/value tests that already describe retained behavior may pass.

Do not commit the RED-only state.

---

## Task 3: Collapse the Durable Core to Source State and Frames

**Files:**

- Modify: `src/nio/ingest/config.py`
- Modify: `src/nio/ingest/membership.py`
- Modify: `src/nio/ingest/model.py`
- Modify: `src/nio/ingest/serialization.py`
- Modify: `src/nio/ingest/state.py`
- Modify: `src/nio/ingest/ports.py`
- Modify: `src/nio/ingest/source.py`
- Modify: `src/nio/ingest/__init__.py`
- Delete: `src/nio/ingest/effects.py`
- Delete: `src/nio/ingest/recovery.py`
- Modify: `src/nio/store/sync_journal_schema.py`
- Modify: `src/nio/store/_sync_journal_codec.py`
- Modify: `src/nio/store/_sync_journal_preflight.py`
- Replace/reduce: `src/nio/store/_sync_journal_rows.py`
- Replace/reduce: `src/nio/store/_sync_journal.py`
- Replace/reduce: `src/nio/store/_sync_journal_port.py`
- Delete: `src/nio/store/_sync_journal_effects.py`
- Modify: `src/nio/store/sync_journal.py`
- Modify: `src/nio/store/__init__.py`
- Delete: `tests/ingest/effects_test.py`
- Delete: `tests/ingest/network_effect_journal_test.py`
- Delete: `tests/ingest/membership_operation_journal_test.py`
- Modify/reduce: `tests/ingest/model_test.py`
- Modify/reduce: `tests/ingest/serialization_test.py`
- Modify/reduce: `tests/ingest/membership_test.py`

### Step 1: Reduce pure durable values

Rewrite `state.py` to contain only `OwnerView`, `SourceState`, `StagedFrame`, and `CommitResult`. Preserve exact-type, nonnegative-counter, frozen/slotted, deep reconstruction, deterministic frame-ID, and source-digest validation.

Reduce `IngestionConfig` to the exact fields `(source, max_staged_frames, sqlite_busy_timeout_ms)`. Keep defaults `2` and `2_000`; exact-validate both integers, require `1 <= max_staged_frames <= 256`, and require a positive busy timeout. Remove consumer, room, batch, crypto, recovery, hydration, attachment, and acknowledgement bounds that have no immediate owner. Pin acceptance of staged-frame bounds 1 and 256 and rejection of 257.

Remove from `membership.py` the premature durable baseline/proof helpers:

```text
MembershipBaseline
MembershipProofKind
MembershipProof
prove_membership
membership_recovery_cursor
advance_membership_baseline
```

Keep `MembershipObservation` and shared parsed-event observation extraction unchanged.

Delete `effects.py` and the new ingestion-only `ingest/recovery.py`. Keep `_json.py` because canonical staged bodies depend on its strict UTF-8, duplicate-key, depth, finite-number, and canonical encoder behavior. Do not delete or refactor legacy `client/sync_recovery.py`, the legacy `client.sync_recovery.RecoveryGap`, or `MatrixStore` recovery compatibility surfaces in Task 4.5; their mapped bot-and-desktop observation gate has not passed.

Revert the premature `SystemOrigin` event producer expansion from `model.py` and `serialization.py`: remove `RecordKind.ROOM_READINESS`, `SystemOriginKind.ROOM_HYDRATION`, system-origin `EventRecord` acceptance, and its system-event identity helper. Retain any older cross-repository wire values still used by existing non-ingestion code, but persist none of them here.

### Step 2: Replace the schema

Define `META_TABLE_SQL` with exactly:

```text
account_id TEXT PRIMARY KEY
device_id TEXT NOT NULL
schema_version INTEGER NOT NULL CHECK (schema_version = 1)
stream_id TEXT NOT NULL
transport_kind TEXT NOT NULL CHECK classic/sliding
revision INTEGER NOT NULL CHECK revision >= 0
writer_epoch TEXT NOT NULL
next_source_epoch INTEGER NOT NULL CHECK next_source_epoch >= 1
created_at_ns INTEGER NOT NULL CHECK created_at_ns >= 0
```

Define `SCHEMA_SQL` with only:

```sql
CREATE TABLE NioIngestSourceState (
    account_id TEXT PRIMARY KEY REFERENCES NioIngestMeta(account_id),
    source_epoch INTEGER NOT NULL CHECK (source_epoch >= 0),
    cursor_ciphertext BLOB NOT NULL,
    cursor_sha256 BLOB NOT NULL,
    next_request_id INTEGER NOT NULL CHECK (next_request_id >= 0),
    active INTEGER NOT NULL CHECK (active IN (0, 1))
)

CREATE TABLE NioIngestFrame (
    account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
    frame_id TEXT NOT NULL,
    source_epoch INTEGER NOT NULL CHECK (source_epoch >= 0),
    request_id INTEGER NOT NULL CHECK (request_id >= 0),
    staged_revision INTEGER NOT NULL CHECK (staged_revision >= 1),
    payload_ciphertext BLOB NOT NULL,
    payload_sha256 BLOB NOT NULL,
    PRIMARY KEY (account_id, frame_id)
)

CREATE INDEX NioIngestFrame_drain ON NioIngestFrame(
    account_id, staged_revision, source_epoch, request_id, frame_id
)
```

Add `typeof`, nonempty text, UUID-shape-on-decode, and 32-byte BLOB checks where SQLite can enforce them without duplicating semantic authority.

### Step 3: Make row metadata authenticated

Extend `EncryptedRowCodec` with an optional exact `header: bytes = b""` parameter on `seal`, `encrypt`, and `decrypt`. Length-frame that header as the final AAD field. Existing callers are removed in this checkpoint, so source and frame rows are the only ingestion callers.

Use canonical internal JSON bytes for source/frame headers. Never trust a clear ordering or transition column unless decryption succeeds with that row's exact header.

### Step 4: Reduce preflight

Remove attachment/batch/effect metadata validation. Compile the expected ingestion topology on one transient `sqlite3.connect(":memory:")`; do not `ATTACH` it to the journal connection.

For a nonempty path, inspect through the sole file-backed Peewee connection before applying write-producing pragmas. Reject no-marker or mismatched topology/identity/transport before DDL or epoch takeover. For an absent/zero-length path, create meta and initial source state in one IMMEDIATE transaction.

The initial source state is derived only from the exact `SourceConfig`, has source epoch `0`, next request ID `0`, active `True`, and the canonical cold cursor for its transport. `next_source_epoch` starts at `1`.

### Step 5: Replace journal rows and port

Implement only source/frame codecs plus the five-method `IngestionJournal` protocol. Remove `_require_attached`, room/lane/effect/batch readers, loss/materialization helpers, acknowledgement, and frame deletion.

`list_frames` performs one bounded indexed query. `load_frame` and `list_frames` authenticate owner, header, payload, canonical re-encoding, normalization version, request owner/transport, and deterministic frame ID.

### Step 6: Implement `stage_source_response`

Inside one IMMEDIATE transaction:

1. load/authenticate owner and current source state;
2. validate exact expected revision and writer epoch;
3. deep-reconstruct the proposed source/frame carriers;
4. validate the request against current source and proposed successor;
5. classify an existing frame collision by fully decoding it;
6. for an exact idempotent existing frame plus exact already-committed successor, return its stored revision with no DML;
7. otherwise CAS meta revision/epoch from `R` to `R+1`;
8. write the encrypted successor source state with authenticated header;
9. insert the encrypted frame with `staged_revision=R+1` and authenticated header;
10. commit and return `CommitResult(R+1)`.

If any validation/collision/CAS fails, roll back without source/frame/revision change.

### Step 7: Remove obsolete tests and imports only after replacements exist

Delete the three effect-specific test files. Remove carrier/effect/attachment/batch/ack blocks from the shared model, serialization, membership, journal, and crash suites. Keep the replacement source-only tests from Task 2 and all transport/observation tests.

### Step 8: Verify symbol and topology removal

Run:

```bash
rg -n 'JournalTransition|RoomAggregate|RoomLane|LaneRecord|ReadyRecord|BatchMaterialization|ConsumerAttachStatus|PersistedNetworkEffect|MembershipOperation|MembershipBaseline' src/nio/ingest src/nio/store/_sync_journal.py src/nio/store/_sync_journal_rows.py src/nio/store/_sync_journal_port.py
rg -n 'from \.recovery import RecoveryGap|nio\.ingest\.recovery' src/nio/ingest src/nio/store
rg -n 'NioIngest(Room|Lane|Ready|Batch|Crypto|Pending|NetworkEffect)' src/nio/store
```

Expected: no production hits.

### Step 9: Run GREEN and focused regressions

Run:

```bash
uv run pytest -q \
  tests/ingest/source_journal_test.py \
  tests/ingest/crash_recovery_test.py \
  tests/ingest/model_test.py \
  tests/ingest/serialization_test.py \
  tests/ingest/membership_test.py \
  tests/ingest/classic_source_test.py \
  tests/ingest/sliding_source_test.py \
  tests/ingest/source_parity_test.py
```

Expected: all pass.

### Step 10: Measure, review, commit, and push

Run the production accounting command from Task 9 below. Obtain an independent review of the source-only boundary and schema. Then:

```bash
git add src/nio tests/ingest docs/superpowers/plans/2026-08-09-durable-ingestion-task-4.5-simplification.md
git commit -m "refactor(ingest): reduce journal to source staging"
git push origin docs/durable-ingestion-rewrite-plan
```

---

## Task 4: Re-Port Canonical Restart Staging End to End

**Files:**

- Modify: `src/nio/ingest/ports.py`
- Modify: `src/nio/ingest/source.py`
- Modify if required: `src/nio/ingest/classic.py`
- Modify if required: `src/nio/ingest/sliding.py`
- Modify: `src/nio/store/_sync_journal_rows.py`
- Modify: `tests/ingest/source_journal_test.py`
- Modify: `tests/ingest/classic_source_test.py`
- Modify: `tests/ingest/sliding_source_test.py`
- Modify: `tests/ingest/source_parity_test.py`

### Step 1: Add mutation-killing restart tests

For both transports, persist a non-neutral own-membership response, close, reopen, load the deserialized `StagedFrame`, and create an adapter with every allowed runtime configuration drift from the design. Require `renormalize_staged_frame` to reproduce the exact original `SyncFrame` and stored `MembershipObservation` values.

Mutate one frozen request field at a time through authenticated resealing and assert replay fails before adapter output is accepted:

- stream, transport, source epoch, request ID;
- Classic method/path/query/full-state/filter/cursor/timeout;
- Sliding connection name, reserved list/range, subscription/extensions, to-device since, E2EE flags, cursor/body;
- response body, source digest, frame ID, and normalization version.

### Step 2: Keep one canonical response copy

Inspect decrypted frame bytes and assert one body occurrence. Inspect `SyncFrame` fields and assert it contains only `source_sha256`, never `source_json` or the response body. Assert `EventRecord.source_json` is unchanged because it is a per-record payload with a different owner.

### Step 3: Run focused source and restart suites

Run:

```bash
uv run pytest -q \
  tests/ingest/source_journal_test.py \
  tests/ingest/classic_source_test.py \
  tests/ingest/sliding_source_test.py \
  tests/ingest/source_parity_test.py \
  tests/ingest/membership_test.py
```

### Step 4: Review, commit, and push

After independent source/restart review:

```bash
git add src/nio/ingest src/nio/store/_sync_journal_rows.py tests/ingest
git commit -m "feat(ingest): preserve canonical restart staging"
git push origin docs/durable-ingestion-rewrite-plan
```

If Task 3 already satisfies every test without production changes, make no empty production commit; include the additional tests in a `test(ingest): pin canonical restart staging` commit.

---

## Task 5: Specify the Ordinary Peewee Ownership Boundary

**Files:**

- Create: `tests/ingest/store_owner_test.py`
- Modify: `tests/ingest/journal_test.py`
- Modify: `tests/store_test.py`
- Modify: `tests/encryption_test.py`

### Step 1: Add the one-file-connection test

Instrument Peewee's SQLite connection factory by resolved filename. Across fresh bootstrap, topology creation, exact reopen, `SqliteStore` construction, journal reads/writes, and E2EE reads/writes, assert exactly one connection is opened to the journal path. Permit one separate transient `:memory:` connection used only to compile expected ingestion topology.

Assert the shared object is exact ordinary `peewee.SqliteDatabase`, with `thread_safe=False`, `autoconnect=False`, configured timeout, and `sqlite3.Row` row factory.

### Step 2: Add owner-entry fence tests

At every outer supported entrypoint, assert validation of:

```text
opening PID
opening thread
active/not revoked/not closing
lifetime lock FD and path device/inode
database path device/inode
persisted writer epoch
```

PID/thread/lock/inode/revoked/closed failures emit zero SQL. A stale external writer epoch may emit only the dedicated epoch fence query/CAS before `LocalProtocolError`; no business SQL follows.

### Step 3: Add shared-transaction tests

Inside one journal write scope, execute a raw journal update and a real nested Peewee model mutation. Inject an exception after the model DML and assert both roll back. On success, assert both commit. Assert one outer `BEGIN IMMEDIATE`, one owner epoch fence, and a nested Peewee savepoint rather than a second outer transaction.

Exercise at least:

- `save_account` / `load_account`;
- `save_session` / `load_sessions`;
- `save_device_keys` / `load_device_keys`;
- `verify_device` / `is_device_verified`.

### Step 4: Add exact store and lifecycle tests

Before filesystem or SQL side effects, reject `DefaultStore`, `SqliteMemoryStore`, queue-backed stores, and every subclass. Accept exact `SqliteStore` only.

Assert borrowed `SqliteStore` does not call `_create_database`, `connect`, or `close`. Every decorated E2EE method enters the private owner read/write scope before model binding or SQL.

Close order is:

```text
revoke borrowed store/session
stop/drain owner operations
close the one Peewee connection
release lifetime lock
```

Connection-close failure retains the lock and permits close retry only through `StoreBootstrap.close()`.

### Step 5: Add architecture inventory tests

Scan production AST/source and reject direct access to:

```text
.database
.cursor()
.connection()
.atomic()
.commit()
.rollback()
```

outside this exact allowlist:

```text
src/nio/store/_ingestion_store_owner.py
src/nio/store/_sync_journal_preflight.py
src/nio/store/_sync_journal.py
src/nio/store/_sync_journal_rows.py
src/nio/store/database.py  # decorators and borrowed initialization only
```

The test enforces an internal boundary; it does not claim the ordinary Peewee object is unforgeable.

For every decorated `SqliteStore` method reachable from the v2 bootstrap, maintain an explicit expected read/write classification and assert its decorator enters `owner.read` or `owner.e2ee_write` before model binding or SQL. The broad source scan catches new reach-through; it is not a substitute for this method inventory.

### Step 6: Run RED

Run:

```bash
uv run pytest -q tests/ingest/store_owner_test.py
```

Expected: missing owner and second-connection/transaction-boundary failures.

Do not commit the RED-only state.

---

## Task 6: Implement the Thin Peewee Store Owner

**Files:**

- Create: `src/nio/store/_ingestion_store_owner.py`
- Modify: `src/nio/store/_sync_journal_preflight.py`
- Modify: `src/nio/store/_sync_journal.py`
- Modify: `src/nio/store/_sync_journal_rows.py`
- Modify: `src/nio/store/sync_journal.py`
- Modify: `src/nio/store/database.py`
- Modify: `tests/ingest/store_owner_test.py`
- Modify: `tests/ingest/journal_test.py`

### Step 1: Implement the owner without a database façade

Create `IngestionStoreOwner` around an exact ordinary `SqliteDatabase`. It owns `StableFileLock`, resolved DB identity, opening PID/thread, lifecycle state, account/writer epoch after activation, and owner-scope depth.

Required private scopes:

```python
@contextmanager
def bootstrap_write(self) -> Iterator[None]: ...

@contextmanager
def read(self) -> Iterator[None]: ...

@contextmanager
def journal_write(self) -> Iterator[None]: ...

@contextmanager
def e2ee_write(self) -> Iterator[None]: ...
```

`bootstrap_write` is single-phase and becomes permanently invalid after `activate(account_id, writer_epoch)`. `read` validates ownership and executes one read-only persisted epoch fence at outermost depth. `journal_write` uses `database.atomic("IMMEDIATE")`, performs a read epoch fence, and leaves revision+epoch write CAS to `stage_source_response`. `e2ee_write` uses the same outer transaction and performs an exact no-op epoch CAS before E2EE DML. Nested scopes perform no second owner fence; nested Peewee `atomic()` remains a savepoint.

Keep this owner/integration at or below +200 net production lines relative to the deleted lock/connection logic. If it exceeds 300 net, stop that implementation attempt, simplify it, and rerun the line report before continuing.

### Step 2: Open one ordinary Peewee connection

After acquiring the lifetime lock and before opening Peewee, call `lstat` on the selected path and record a private immutable fresh-path witness: `ABSENT`, `ZERO_LENGTH`, or `NONEMPTY`, plus device/inode for an existing file. Creation eligibility is decided solely from this witness, never from post-connect file size or schema. Reject symlinks and non-regular targets under the existing path policy, and verify an existing file's identity did not change between witness capture and connection open.

Construct:

```python
SqliteDatabase(
    path,
    thread_safe=False,
    autoconnect=False,
    timeout=sqlite_busy_timeout_ms / 1000,
)
```

Open it once on the owner thread, set `sqlite3.Row`, install the optional statement observer, capture DB identity, and apply foreign keys/busy timeout/WAL/NORMAL only after the fresh/nonempty preflight rules allow writes.

No custom `SqliteDatabase` subclass, capability token, guarded view, or raw connection field is exported.

### Step 3: Route preflight and journal SQL through Peewee

Use `owner.database.execute_sql()` for all file-backed topology, metadata, source, and frame SQL. Use the transient `:memory:` compiler only for expected topology. Replace raw SQLite transaction helpers with owner Peewee scopes.

`SqliteIngestionJournal._execute` delegates to `database.execute_sql`; `_read` enters `owner.read`; `_transaction` enters `owner.journal_write`; `close` delegates to bootstrap/owner lifecycle.

### Step 4: Borrow the exact database in `SqliteStore`

`StoreBootstrap.open_matrix_store` requires `store_class is SqliteStore` before stat/mkdir/connect/SQL. `SqliteStore` receives the already-open `owner.database`; its ingestion initialization does not call legacy `_create_database`, `connect`, or `close`.

`use_database` enters an owner read scope. `use_database_atomic` enters an owner E2EE write scope, then uses the ordinary nested Peewee `atomic()` scope. Classify all E2EE methods by actual DML, including hidden upgrade writes; pure reads remain read-only.

### Step 5: Implement close and failure retry

Revoke the borrowed store first. Mark owner closing before DB close. If DB close succeeds, release lock. If DB close raises, keep the lock and closing state; a second bootstrap close retries only unfinished close steps. Successful repeated close is a no-op.

### Step 6: Run GREEN

Run:

```bash
uv run pytest -q \
  tests/ingest/store_owner_test.py \
  tests/ingest/journal_test.py \
  tests/store_test.py \
  tests/encryption_test.py
```

Expected: all pass.

### Step 7: Static boundary checks

Run:

```bash
rg -n '_WriterSqliteDatabase|WriterCapability|WriterView|_store_writer|immediate_transaction' src/nio
rg -n 'sqlite3\.connect\(' src/nio/store
```

Expected: no guarded-writer/capability/raw transaction helper; the sole permitted `sqlite3.connect` is the transient `:memory:` expected-topology compiler, with no file path.

### Step 8: Review, measure, commit, and push

Obtain an independent storage-boundary review, including the architecture scan and transaction traces. Then:

```bash
git add \
  src/nio/store/_ingestion_store_owner.py \
  src/nio/store/_sync_journal_preflight.py \
  src/nio/store/_sync_journal.py \
  src/nio/store/_sync_journal_rows.py \
  src/nio/store/sync_journal.py \
  src/nio/store/database.py \
  tests/ingest/store_owner_test.py \
  tests/ingest/journal_test.py \
  tests/store_test.py \
  tests/encryption_test.py
git commit -m "refactor(store): share ordinary peewee ingestion connection"
git push origin docs/durable-ingestion-rewrite-plan
```

---

## Task 7: Freeze Crash, Lock, and Capacity Behavior

**Files:**

- Modify: `src/nio/ingest/source.py`
- Modify: `tests/ingest/crash_recovery_test.py`
- Modify: `tests/ingest/source_journal_test.py`
- Modify: `tests/ingest/store_owner_test.py`
- Modify only if a test exposes a defect: source/journal/owner production files

### Step 1: Complete statement-hook crash coverage

Run each stage boundary in a subprocess, kill immediately after the labeled statement, reopen, and assert the exact old or new source/frame graph. Include the case where the nested Peewee E2EE write is rolled back with the raw source/frame write.

### Step 2: Freeze external SQLite lock behavior

Use a second test-only SQLite connection to hold an external write lock. With configured timeout `100 ms`, assert stage and E2EE write fail between 100 and 350 ms, leave zero partial business writes, and the next attempt succeeds after lock release. This is separate from the nonblocking lifetime file lock.

### Step 3: Freeze full staged-frame bound behavior

Task 4.5 exposes the smallest non-activating scheduling-admission seam needed to prove the bound in `source.py`:

```python
class SourceScheduleStatus(StrEnum):
    READY = "ready"
    AT_CAPACITY = "at_capacity"
    INACTIVE = "inactive"

@dataclass(frozen=True, slots=True)
class SourceScheduleDecision:
    status: SourceScheduleStatus
    request: NetworkRequest | None

def plan_source_poll(
    source: SyncSource,
    state: SourceState,
    request_id: int,
    staged_frames: tuple[StagedFrame, ...],
    max_staged_frames: int,
) -> SourceScheduleDecision: ...
```

It exact-validates all inputs, accepts a positive exact-int bound no greater than 256, returns `AT_CAPACITY` before calling `source.plan_request` when `len(staged_frames) >= max_staged_frames`, returns `INACTIVE` when the adapter returns no request, and otherwise returns `READY` with the frozen request. It never performs HTTP or SQL. The caller obtains the bounded tuple via `list_frames(max_staged_frames)`.

Use a fake sender that is invoked only for `READY`. At the bound, 10,000 admission polls must produce `AT_CAPACITY`, zero fake HTTP calls, and less than 8 MiB peak RSS growth. This gate is mandatory in Task 4.5 and is not deferred.

### Step 4: Run the complete Task 4.5 correctness gate

Run:

```bash
uv run pytest -q tests/ingest
uv run pytest -q tests/store_test.py tests/encryption_test.py
```

### Step 5: Review, commit, and push

After independent crash/lifecycle review, commit only real test or defect-fix changes:

```bash
git add src/nio tests/ingest tests/store_test.py tests/encryption_test.py
git commit -m "test(ingest): freeze source journal crash boundaries"
git push origin docs/durable-ingestion-rewrite-plan
```

---

## Task 8: Add and Run the Reproducible Staging Benchmark

**Files:**

- Create: `scripts/benchmark_ingest_staging.py`
- Reuse: `tests/data/ingest/classic_sync.json`
- Create: `tests/data/ingest/classic_steady_100_rooms_1000_events.json`
- Create: `tests/data/ingest/sliding_steady_100_rooms_1000_events.json`
- Create: `tests/data/ingest/canonical_1mib.json`
- Create: `tests/data/ingest/classic_continuation_1.json`
- Create: `tests/data/ingest/classic_continuation_2.json`
- Create: `tests/data/ingest/sliding_continuation_1.json`
- Create: `tests/data/ingest/sliding_continuation_2.json`
- Create: `tests/ingest/staging_benchmark_test.py`
- Create: `.superpowers/sdd/2026-08-08-durable-matrix-ingestion-rewrite/task-4.5-staging-benchmark.json` (ignored report)

### Step 1: Check in deterministic fixtures

Generate canonical UTF-8 JSON fixtures with stable room/event IDs and timestamps. Assert fixture SHA-256 values in `staging_benchmark_test.py` so accidental corpus drift fails review.

### Step 2: Implement two explicit benchmark adapters

The harness accepts:

```text
--mode control|candidate
--repo PATH
--fixture PATH
--iterations 30
--warmups 5
--busy-timeout-ms 100
--output PATH
```

`control` imports fixed commit `980e53b3a3815592c3bd0dab9e41e2b057e50724` from a detached worktree and executes its existing response normalization plus durable `SqliteStore.save_sync_recovery`/sync-token path for the same successful response. `candidate` imports the current worktree and executes adapter normalization plus `stage_source_response`. Both use on-disk temporary DBs, WAL, `synchronous=NORMAL`, foreign keys, the same Python/SQLite/Peewee environment, and no network.

Record raw wall time, process CPU time, peak RSS, SQL trace/count, WAL bytes, canonical response bytes, and success/failure for every iteration. Record CPU model/core count, available memory, filesystem, Python, Peewee, and SQLite versions.

### Step 3: Pin harness behavior

Tests use one warmup/two measured iterations and assert stable JSON schema, no per-event SQL, exactly one outer candidate transaction, at most six non-PRAGMA candidate statements, external-lock timeout/recovery, and full-bound no-HTTP behavior through the mandatory `plan_source_poll` admission seam.

### Step 4: Run the frozen corpus

Create a detached control worktree at the fixed base and run five warmups plus thirty measured iterations for each fixture/mode. Publish all raw samples and calculated medians/p95s in the ignored report.

The gate is:

```text
candidate p95 <= 2.0 * control p95 + 10 ms
candidate median CPU <= 2.0 * control median CPU + 5 ms
candidate peak RSS <= control + 32 MiB
candidate WAL <= 2.5 * canonical bytes + 128 KiB
external lock failure 100-350 ms, no partial writes, next attempt succeeds
```

### Step 5: Report measured results directly

Before committing, report directly to the approval owner the raw-sample artifact location, fixture hashes, median/p95, SQL trace and count, WAL delta, peak RSS, lock and capacity results, production additions/deletions/net, remaining forecast, and every estimate miss above 50 percent. The ignored JSON report is supporting evidence only; it never substitutes for the direct report. Under the standing execution delegation, continue after reporting when every hard limit passes.

### Step 6: Commit harness and fixtures, not generated samples

Run:

```bash
uv run pytest -q tests/ingest/staging_benchmark_test.py
git add scripts/benchmark_ingest_staging.py tests/data/ingest tests/ingest/staging_benchmark_test.py
git commit -m "perf(ingest): freeze source staging budgets"
git push origin docs/durable-ingestion-rewrite-plan
```

If a numeric limit fails, reduce SQL/copying/scope and rerun; do not relax a limit autonomously.

---

## Task 9: Verify Size, Rewrite the Master Plan, and Close Task 4.5

**Files:**

- Modify: `docs/superpowers/plans/2026-08-08-durable-matrix-ingestion-rewrite.md`
- Create: `.superpowers/sdd/2026-08-08-durable-matrix-ingestion-rewrite/task-4.5-simplification-report.md` (ignored)
- Modify: ignored task ledger in `.superpowers/sdd/2026-08-08-durable-matrix-ingestion-rewrite/`

### Step 1: Run production line accounting

Run exactly:

```bash
git diff --numstat 980e53b3a3815592c3bd0dab9e41e2b057e50724...HEAD -- \
  ':(glob)src/**/*.py' ':(glob)scripts/**/*.py' |
awk '{ additions += $1; deletions += $2 } END {
  printf "+%d -%d net %+d\n", additions, deletions, additions - deletions
}'
```

Also report uncommitted production diff and every untracked production file separately. Do not credit future deletions.

### Step 2: Verify the simplified envelope

Report:

- actual retained transport/observation/staging lines;
- owner/integration net;
- premature carrier/effect/attachment lines actually deleted;
- pre-Task4.5 dormant scaffold actually deleted;
- current cumulative net;
- named remaining work cap and conditional deletion dependencies;
- measured SQL, p95/median CPU, RSS, WAL, lock, and capacity results.

If the simplified checkpoint exceeds the approved +6,240 high side or the owner integration exceeds its +200 target by more than 50 percent, simplify/delete before starting the next subproject.

### Step 3: Rewrite Tasks 5 onward around the new ownership map

Replace the historical assumptions with separately planned checkpoints:

1. Task 5 pure reducer proposes typed room/recovery transitions and owns no SQL.
2. Task 6 adds the first minimal per-room aggregate, ready queue, batches, frame retirement, loss/gap fate, and bounded materialization.
3. Task 7 adds crypto receipt tables and same-Peewee-transaction Olm/Megolm commits.
4. Task 8 adds recovery/hydration execution and rescheduling.
5. Task 9 adds membership uncertain-delivery ownership.
6. MindRoom bot activation adds atomic admission, inventoried minimal room directory, and direct crypto reach-through removal.
7. Task 13 adds always-bounded resumable attachment before HTTP activation.
8. Desktop/pairing migration remains separately scoped and disabled for v2 until implemented.

Name every future deletion's exact current consumers, replacement, and observation gate. Do not treat the 5,544-line candidate inventory as firm.

### Step 4: Run final verification on the exact commit candidate

Run:

```bash
uv run pytest -q tests/ingest
uv run pytest -q tests/store_test.py tests/encryption_test.py
uv run pytest -q
uv run pre-commit run --all-files
python -m compileall -q src scripts
git diff --check
git status --short
```

Expected: all tests/hooks pass; only the preserved unrelated egg-info may remain untracked.

### Step 5: Independent final review

Request fresh read-only reviews for:

- source/staging correctness and crash fate;
- topology/fresh-path/cryptographic integrity;
- ordinary Peewee ownership and E2EE atomicity;
- benchmark reproducibility and numeric results;
- production line accounting and future owner map.

Resolve findings with RED -> GREEN and rerun the affected and broad gates.

### Step 6: Commit and push the rewritten plan/report references

```bash
git add docs/superpowers/plans/2026-08-08-durable-matrix-ingestion-rewrite.md
git commit -m "docs: rebase ingestion plan on source-only core"
git push origin docs/durable-ingestion-rewrite-plan
```

Then immediately write the Task 5 pure-reducer design/implementation plan and continue under the user's standing instruction; do not wait for another approval while every hard constraint remains satisfied.

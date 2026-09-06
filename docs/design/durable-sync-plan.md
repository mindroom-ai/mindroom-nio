# Durable Classic Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use baspowers:subagent-driven-development or baspowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Replace the large ingestion engine with shared upstream processing and
a small durable Classic sync adapter, including the coordinated MindRoom consumer.

**Architecture:** A synchronous iterator owns Matrix interpretation. SQLite owns
input, crypto, compact room projection and an output batch outbox. MindRoom
atomically admits each batch in its existing journal before acknowledging nio.

**Tech Stack:** Python >=3.12, existing aiohttp/Peewee/SQLite/vodozemac, stdlib JSON.

**Spec:** [durable-sync.md](durable-sync.md).

## Global constraints

- Ordinary upstream-style client public behavior is preserved.
- Durable transport is Classic `/sync` only; reject old prototype formats.
- No new dependency, writer thread, database queue, or generic retry framework.
- Retain process-crash atomicity, exact encrypted retries, ordered authorization,
  bounded missed-history recovery, and interactive key-share approval on restart.
- No callback/network await inside a SQLite preparation transaction.
- Production size and measured performance are acceptance criteria; tests may grow.
- Keep implementation and evidence in persistent isolated worktrees.
- Commit the self-reviewed design before implementation; never amend commits.
- Commit and push completed verified work to the existing PRs; do not merge/release.

## Task 1: Share synchronous sync interpretation

**Files:** `src/nio/client/base_client.py`, `src/nio/client/async_client.py`,
`tests/client_test.py`, `tests/async_client_test.py`, `tests/sync_iterator_test.py`.

**Consumes:** Existing parsed `SyncResponse`, normal room and crypto handlers.
**Produces:** Private `_SyncItem` dataclass and
`Client._iter_sync(response: SyncResponse, *, include_left: bool = False)`.
Items expose `route: str | None`, `event: object | None`,
`room: MatrixRoom | None`, and `section: str | None`.
Routes are `event`, `invite`, `ephemeral`, `room_account_data`,
`global_account_data`, `presence`, `to_device`, and `expired_verification`.
Section markers identify `invite`, `join`, or `leave`; non-callback state events
have `route=None`. Section markers precede room state. The iterator never changes
the cursor or invokes user callbacks. Ordinary wrappers retain their cursor logic.

- [ ] Add a callback-order characterization test using two room state changes
  and messages; callbacks must observe their original event-time room state.
  Add a callback exception test proving later events remain unprocessed.
- [ ] Run the focused tests before changing production code. Add a new iterator
  test that initially fails because `_iter_sync` does not exist:

  ```python
  items = list(client._iter_sync(response))
  assert [item.event.event_id for item in items if item.route == "event"] == [
      "$first", "$second"
  ]
  assert callback_events == []
  ```

- [ ] Extract shared synchronous section iterators. Dispatch ordinary callbacks
  at yields with synchronous/async wrappers; preserve subclass decryption hooks.
  Keep standalone section methods working. For joined state, emit observations
  immediately after each applied event. Include room identity in section markers.
- [ ] Add opt-in left-room processing; emit state/timeline observations before
  removing the room. Default ordinary behavior ignores left rooms.
- [ ] Preserve async versus sync response mutation timing where currently
  observable. Cover to-device decryption and collected/expired key callbacks.
- [ ] Run `uv run --locked pytest --benchmark-disable tests/client_test.py
  tests/async_client_test.py tests/sync_iterator_test.py`, plus relevant crypto
  tests and type checks. Self-review and commit the shared processing seam.

## Task 2: Define the batch codec and compact room persistence

**Files:** Create `src/nio/durable/model.py`, `codec.py`, `projection.py`,
`tests/durable/codec_test.py`, `tests/durable/projection_test.py`.

**Consumes:** Task 1 `_SyncItem`; ordinary nio event parsers and room objects.
**Produces:** Spec dataclasses and `RecordKind`,
`freeze_event(item: _SyncItem) -> SyncRecord`,
`restore_event(record: SyncRecord) -> object`, and compact room encode/restore
functions. Room metadata and member rows are separate. Record decoding occurs at
the disk boundary, not at every same-process call.

- [ ] Add failing encrypted-media, sanitized room-key, dummy, invitation, and
  verification-cancel round trips. Assert exact event class and literal crypto
  evidence after restoring; no second decrypt call may occur.
- [ ] Implement explicit small codec tags only where normal parsers cannot
  reconstruct sanitized events. Freeze envelope and clear payload before mutation
  can erase the envelope. Preserve provenance and event-time membership.
- [ ] Add room restart tests with changed power levels, membership departure,
  encrypted flag, and two same-name members. Check real room behavior after
  restore, including incomplete members; do not assert serialized implementation.
- [ ] Implement compact metadata plus member deltas. A message without state
  changes must not rewrite all member rows. Run focused tests, review and commit.

## Task 3: Implement the SQLite input and batch boundary

**Files:** Create `src/nio/durable/store.py`, `client.py`, `__init__.py`,
`tests/durable/store_test.py`, `tests/durable/client_test.py`; modify
`src/nio/store/database.py` and client ownership fences.

**Consumes:** Shared iterator, codec and projection. Existing `SqliteStore`.
**Produces:** `open_durable_sync(client, *, consumer_id: UUID, store_path: Path,
database_name: str | None = None, config: DurableSyncConfig | None = None,
source_store_class: type[MatrixStore] = SqliteStore) -> DurableSync`.
The supplied authenticated client has no loaded store or previous sync use.
The factory owns attaching the store and restoring crypto/rooms under its lease.
`DurableSync` exposes async `run()`, `next_batch() -> SyncBatch | None`,
`ack(batch: SyncBatch)`, `wait_for_work()`, `quiesce()`, and `close()`.
It exposes its `stream_id: UUID` and committed progress for the consumer.

- [ ] Add fresh/reopen/wrong-consumer/concurrent-owner tests with real SQLite.
  Opening a durable database through an ordinary store must fail safely.
- [ ] Implement one exclusive lifetime lease and a small versioned schema for
  retained input, metadata, batches, room metadata/members, and pending crypto.
  Use the same connection as `SqliteStore`. Preserve default-store effective trust
  on explicit adoption. Remove per-row path checking from this path.
- [ ] Add tests showing `next_batch` is read-only and repeated reads/reopens yield
  identical `(stream_id, sequence, index)` identities. Wrong/out-of-order ack
  must not delete another batch. Empty sync completion remains observable.
- [ ] Implement bounded capture, synchronous transactional preparation, and
  batched publication. Split authorization barriers per spec. Dispose the client
  after rollback; test this with a genuine transaction failure after mutation.
- [ ] Port the demonstrated main encrypted cursor crash into a passing adapter
  test. Kill subprocesses before input commit, after capture, after preparation,
  and after consumer receipt but before ack. Use real crypto and literal plaintext
  expectations. Healthy control must also pass.
- [ ] Run focused store/client/crypto tests, review and commit.

## Task 4: Persist crypto maintenance and chronological recovery

**Files:** Create `src/nio/durable/crypto.py`, `recovery.py`,
`tests/durable/crypto_test.py`, `tests/durable/recovery_test.py`; wire `client.py`.

**Consumes:** Session transaction and output boundary, existing Olm methods.
**Produces:** Exact pending crypto request persistence, response settlement,
bounded Classic recovery continuation, and serialized local membership intent.

- [ ] Test generated one-time keys surviving restart before upload response;
  identical encrypted retry body/transaction ID; dummy acknowledgement producing
  a retained follow-on; key query interleaved with newer sync invalidation.
- [ ] Reuse ordinary crypto methods inside the existing transaction. Persist
  waiting/untrusted interactive requests and test real `continue_key_share`
  after restart. Retain committed trust; document restarted active SAS exchange.
- [ ] Test a limited timeline with recovered membership, messages, and a retained
  tail, including a restart between pages. Assert chronological admission and
  that recovered membership never becomes a live grant.
- [ ] Retain one bounded raw response and continuation, fetching one page at a
  time. Keep crypto prologue and pending output ahead of further requests. Test
  oversize reduction, token cycles, unavailable history, and explicit fenced loss.
- [ ] Add and implement local membership intent/restart/stale-echo tests using
  expected membership and epoch. Run focused tests, review and commit.

## Task 5: Move MindRoom to batch admission

**Files:** In the companion repository, `src/mindroom/matrix/durable_ingestion.py`,
`matrix/_owned_session.py`, `matrix/client_session.py`, `matrix/sync_loop.py`,
`event_journal` admission methods, `config/matrix.py`, `orchestrator.py`, affected
doctor/configuration documentation and tests.

**Consumes:** `nio.durable` factory, dataclasses and session methods from Task 3.
**Produces:** One existing-journal transaction per admitted batch and coordinated
Classic-only consumer, preserving business authorization and projection barriers.

- [ ] Create a candidate worktree from the published PR head, preserving other
  user worktrees. Add failing batch rollback and redelivery tests before replacing
  the adapter. Record a batch receipt and admissions in one transaction.
- [ ] Replace per-record canonical proof validation with trusted batch conversion.
  Keep ordered lifecycle hooks, receipt replay semantics and existing projection
  wait. Test grant/message/departure ordering and no recovered grant.
- [ ] Extract the existing signed-device post-decrypt authentication into a pure
  helper and use it for fresh and restored to-device events. Test removed or
  changed devices failing closed; never deserialize app subtypes inside nio.
- [ ] Move safe self-authored pending/streaming edit filtering before expensive
  classification. Test original placeholders, terminal edits, foreign edits,
  redactions, encrypted content, and unknown statuses.
- [ ] Update session opening, lifecycle, completion, local membership and config;
  reject explicit Sliding configuration. Test quiesce and cleanup ordering.
- [ ] Point the candidate at the local built nio wheel for verification; final
  committed dependency uses the tested pushed commit. Run relevant tests, review
  and commit coordinated changes without touching the user's dirty checkout.

## Task 6: Remove the superseded engine and verify compatibility

**Files:** Delete `src/nio/ingest`, old sync-journal/owner/preflight/plan modules
under `src/nio/store`, and obsolete production preparation/settlement hooks.
Update exports, tests, release notes and historical document status.

- [ ] Identify required behavioral tests among old ingestion tests and retain
  them against the new API. Delete tests tied only to intentionally removed
  machinery. Do not preserve production shims to make those tests pass.
- [ ] Delete old source and imports once the replacement and consumer work.
  Keep upstream-compatible behavior and unrelated fork fixes.
- [ ] Run `uv run --locked pytest --benchmark-disable`, `uv run ruff check src
  tests`, `uv run black --check src tests`, `uv run pre-commit run --all-files`,
  and `MYPYPATH=src uv run --no-sync mypy -p nio --warn-redundant-casts
  --no-incremental`. Types must remain zero errors. Run companion full checks.
- [ ] Draft breaking-change release notes for the new API, removed old fork
  interfaces, Classic-only durable transport and transient receipts. Publication
  and version selection remain release work. Review and commit.

## Task 7: Measure, review, and push the completed replacement

**Files:** Update measured results here; retain detailed evidence in the persistent
capacity workspace and update both existing PR descriptions with relative paths.

- [ ] Run batched encrypted/plaintext workloads against the starting branch and
  replacement. Record CPU, JSON decode counts, commit counts, loop delay and
  member-row writes. Compare SQLite NORMAL and FULL before deciding durability.
- [ ] Repeat the existing integrated 200-request Synapse and Tuwunel controls.
  Report all accepted/replied counts, latency distributions, synthetic generation
  time, convergence fences and drain. Investigate regressions with actual profiles.
- [ ] Measure production additions/deletions against main and the starting head.
  Review whole-branch correctness, ownership, removed duplication and performance.
  Address demonstrated findings; excluded guarantees require a design amendment.
- [ ] Run final necessary checks, stage only intended files, inspect staged diff,
  commit, and push fast-forward to the existing PR branches. Update descriptions
  with verified results and wait for relevant CI/reviews; resolve valid findings.

## Measured results

- Starting nio head: `9df7aa92a626e6d422b0d8fb36c5197be312d4b8`.
- Starting main: `5b6de3bc6f4aa0c317185953f77c9c8fe3d806a3`.
- Fresh baseline: 2,181 passed, 3 skipped in 211.40 seconds.
- Main crash reproduction: healthy real-crypto control passes; crash regression
  fails with missing room key and `MegolmEvent` instead of `RoomMessageText`.
- Replacement implementation and performance results are not yet available.

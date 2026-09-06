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
`room: MatrixRoom | None`, `section: str | None`, and
`source: dict[str, Any] | None = None`. Preserve the original wire source for
encrypted to-device and timeline observations, before calling the decrypt hook.
Routes are `event`, `invite`, `ephemeral`, `room_account_data`,
`global_account_data`, `presence`, `to_device`, and `expired_verification`.
Section markers identify `invite`, `join`, or `leave`; non-callback state events
have `route=None`. Section markers precede room state. The iterator never changes
the cursor or invokes user callbacks. Ordinary wrappers retain their cursor logic.

- [x] Add a callback-order characterization test using two room state changes
  and messages; callbacks must observe their original event-time room state.
  Add a callback exception test proving later events remain unprocessed.
- [x] Run the focused tests before changing production code. Add a new iterator
  test that initially fails because `_iter_sync` does not exist:

  ```python
  items = list(client._iter_sync(response))
  assert [item.event.event_id for item in items if item.route == "event"] == [
      "$first", "$second"
  ]
  assert callback_events == []
  ```

- [x] Extract shared synchronous section iterators. Dispatch ordinary callbacks
  at yields with synchronous/async wrappers; preserve subclass decryption hooks.
  Keep standalone section methods working. For joined state, emit observations
  immediately after each applied event. Include room identity in section markers.
- [x] Add opt-in left-room processing; emit state/timeline observations before
  removing the room. Default ordinary behavior ignores left rooms.
- [x] Preserve async versus sync response mutation timing where currently
  observable. Cover to-device decryption and collected/expired key callbacks.
- [x] Run `uv run --locked pytest --benchmark-disable tests/client_test.py
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

- [x] Add failing encrypted-media, sanitized room-key, dummy, invitation, and
  verification-cancel round trips. Assert exact event class and literal crypto
  evidence after restoring; no second decrypt call may occur.
- [x] Implement explicit small codec tags only where normal parsers cannot
  reconstruct sanitized events. Freeze envelope and clear payload before mutation
  can erase the envelope. Preserve provenance and event-time membership.
- [x] Add room restart tests with changed power levels, membership departure,
  encrypted flag, and two same-name members. Check real room behavior after
  restore, including incomplete members; do not assert serialized implementation.
- [x] Implement compact metadata plus member deltas. A message without state
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

- [x] Add fresh/reopen/wrong-consumer/concurrent-owner tests with real SQLite.
  Opening a durable database through an ordinary store must fail safely.
- [x] Implement one exclusive lifetime lease and a small versioned schema for
  retained input, metadata, batches, room metadata/members, and pending crypto.
  Use the same connection as `SqliteStore`. Preserve default-store effective trust
  on explicit adoption. Remove per-row path checking from this path.
- [x] Add tests showing `next_batch` is read-only and repeated reads/reopens yield
  identical `(stream_id, sequence, index)` identities. Wrong/out-of-order ack
  must not delete another batch. Empty sync completion remains observable.
- [x] Implement bounded capture, synchronous transactional preparation, and
  batched publication. Split authorization barriers per spec. Dispose the client
  after rollback; test this with a genuine transaction failure after mutation.
- [x] Port the demonstrated main encrypted cursor crash into a passing adapter
  test. Kill subprocesses before input commit, after capture, after preparation,
  and after consumer receipt but before ack. Use real crypto and literal plaintext
  expectations. Healthy control must also pass.
- [x] Run focused store/client/crypto tests, review and commit.

## Task 4: Persist crypto maintenance and chronological recovery

**Files:** Create `src/nio/durable/crypto.py`, `recovery.py`,
`tests/durable/crypto_test.py`, `tests/durable/recovery_test.py`; wire `client.py`.

**Consumes:** Session transaction and output boundary, existing Olm methods.
**Produces:** Exact pending crypto request persistence, response settlement,
bounded Classic recovery continuation, and serialized local membership intent.

- [x] Test generated one-time keys surviving restart before upload response;
  identical encrypted retry body/transaction ID; dummy acknowledgement producing
  a retained follow-on; key query interleaved with newer sync invalidation.
- [x] Reuse ordinary crypto methods inside the existing transaction. Persist
  waiting/untrusted interactive requests and test real `continue_key_share`
  after restart. Retain committed trust; document restarted active SAS exchange.
- [x] Test a limited timeline with recovered membership, messages, and a retained
  tail, including a restart between pages. Assert chronological admission and
  that recovered membership never becomes a live grant.
- [x] Retain one bounded raw response and continuation, fetching one page at a
  time. Keep crypto prologue and pending output ahead of further requests. Test
  oversize reduction, token cycles, unavailable history, and explicit fenced loss.
- [x] Add and implement local membership intent/restart/stale-echo tests using
  expected membership and epoch. Run focused tests, review and commit.

## Task 5: Move MindRoom to batch admission

**Files:** In the companion repository, `src/mindroom/matrix/durable_ingestion.py`,
`matrix/_owned_session.py`, `matrix/client_session.py`, `matrix/sync_loop.py`,
`event_journal` admission methods, `config/matrix.py`, `orchestrator.py`, affected
doctor/configuration documentation and tests.

**Consumes:** `nio.durable` factory, dataclasses and session methods from Task 3.
**Produces:** One existing-journal transaction per admitted batch and coordinated
Classic-only consumer, preserving business authorization and projection barriers.

- [x] Create a candidate worktree from the published PR head, preserving other
  user worktrees. Add failing batch rollback and redelivery tests before replacing
  the adapter. Record a batch receipt and admissions in one transaction.
- [x] Replace per-record canonical proof validation with trusted batch conversion.
  Keep ordered lifecycle hooks, receipt replay semantics and existing projection
  wait. Test grant/message/departure ordering and no recovered grant.
- [x] Extract the existing signed-device post-decrypt authentication into a pure
  helper and use it for fresh and restored to-device events. Test removed or
  changed devices failing closed; never deserialize app subtypes inside nio.
- [x] Move safe self-authored pending/streaming edit filtering before expensive
  classification. Test original placeholders, terminal edits, foreign edits,
  redactions, encrypted content, and unknown statuses.
- [x] Update session opening, lifecycle, completion, local membership and config;
  reject explicit Sliding configuration. Test quiesce and cleanup ordering.
- [ ] Point the candidate at the local built nio wheel for verification; final
  committed dependency uses the tested pushed commit. Run relevant tests, review
  and commit coordinated changes without touching the user's dirty checkout.

## Task 6: Remove the superseded engine and verify compatibility

**Files:** Delete `src/nio/ingest`, old sync-journal/owner/preflight/plan modules
under `src/nio/store`, and obsolete production preparation/settlement hooks.
Update exports, tests, release notes and historical document status.

- [x] Identify required behavioral tests among old ingestion tests and retain
  them against the new API. Delete tests tied only to intentionally removed
  machinery. Do not preserve production shims to make those tests pass.
- [x] Delete old source and imports once the replacement and consumer work.
  Keep upstream-compatible behavior and unrelated fork fixes.
- [x] Run `uv run --locked pytest --benchmark-disable`, `uv run ruff check src
  tests`, `uv run black --check src tests`, `uv run pre-commit run --all-files`,
  and `MYPYPATH=src uv run --no-sync mypy -p nio --warn-redundant-casts
  --no-incremental`. Types must remain zero errors. Nio: 689 passed, 3 skipped;
  zero mypy errors and clean hooks.
- [x] Run companion full checks: 15,522 passed, 22 skipped; ty and hooks clean.
- [x] Draft breaking-change release notes for the new API, removed old fork
  interfaces, Classic-only durable transport and transient receipts. Publication
  and version selection remain release work. Review and commit.

## Task 7: Measure, review, and push the completed replacement

**Files:** Update measured results here; retain detailed evidence in the persistent
capacity workspace and update both existing PR descriptions with relative paths.

- [x] Run batched encrypted/plaintext workloads against the starting branch and
  replacement. Record CPU, JSON decode counts, commit counts, loop delay and
  member-row writes. Compare SQLite NORMAL and FULL before deciding durability.
- [x] Repeat the existing integrated 200-request Synapse and Tuwunel controls.
  Report all accepted/replied counts, latency distributions, synthetic generation
  time, convergence fences and drain. Investigate regressions with actual profiles.
- [x] Measure production additions/deletions against main and the starting head.
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

- Task 6 removal source measurement (2026-09-06): `src/nio` contains 28,537
  Python lines, including 2,785 in `nio.durable` and 122 in its shared SQLite
  lease module. Against `52c6c06`, production diff is +278/-20,813 (net -20,535).
  Against main `5b6de3bc`, it is +3,856/-6,809 (net -2,953). Task 6 alone removes
  20,613 net production lines from `91a513c`. These are source counts, not
  throughput measurements; integrated workload acceptance remains Task 7.

## Initial replacement measurements

At Nio `34699bf`, 689 tests passed with 3 skipped. The preliminary MindRoom
run at `72732c22b` passed 15,514 with 22 skipped, but automatic dependency sync
was not disabled for all subprocesses; it does not certify the combined artifact.
Final checks below supersede that limitation. Installed wheel files matched the
committed source for the initial performance controls. Independent review found
local-membership echo ownership defects in both layers, subsequently fixed.

Three sequential process runs per case compared capture, preparation, batch read
and acknowledgement against the published Nio implementation at `9df7aa9`.
Room setup, input generation, crypto maintenance and HTTP were outside timing.
Fixtures used 200 events/50 members and 1,000 events/500 members; encrypted cases
used real Megolm decryption. Counts were collected separately from timing.

| Events | Payload | Old NORMAL median | New NORMAL median | New FULL median |
| --- | --- | ---: | ---: | ---: |
| 200 | Plain | 196.7 ms | 17.1 ms | 25.1 ms |
| 200 | Encrypted | 262.7 ms | 52.9 ms | 61.5 ms |
| 1,000 | Plain | 1,736.7 ms | 76.7 ms | 87.8 ms |
| 1,000 | Encrypted | 2,107.8 ms | 270.4 ms | 280.9 ms |

For 200 plain events, SQL commits fell from 404 to 3, selects from 3,235 to 11,
and JSON decodes from 1,854 to 3. Encrypted decodes fell from 2,854 to 203. The new
adapter made no member-row writes for unchanged membership. These are
engine measurements, not end-to-end reply times. Select FULL for the durable
adapter; retain stdlib JSON and no new writer. PR57's broad idea of fewer commits
is already achieved by the batch boundary; its retired-engine patch is not used.

First uninstrumented 200-root controls used the candidate wheel and the unchanged
180-second reply deadline, 45-second overlap minimum, two-second health timeout,
all-principal reaction fence and drain checks. Tuwunel completed 200 replies with
median/p95 71.754/81.323 seconds, but failed overlap at 41.207 seconds. Synapse
completed 200 with 75.954/81.870 seconds and passed all criteria (50.008-second
overlap). Both had no loop-stall diagnostics and zero Nio input/batches or journal/
delivery backlog after shutdown. The synthetic stream itself lasts about 60
seconds. These initial NORMAL controls are not final acceptance evidence.

The final fix reconciles local echoes in the single producer intent, removes
typed consumer echo debt, refreshes known outbound recipients on device changes,
and discards unaccepted polls when local membership takes ownership. FULL is the
default. A diagnostic increase from eight to sixteen root preparations did not
materially improve reply latency or meet the Tuwunel overlap target; retain eight.
No writer, serializer, concurrency setting, or acceptance threshold was changed.


## Final implementation and engine measurements

Nio runtime `ed316ed` and MindRoom runtime `ed4a209d6` pass 698/3 and
15,522/22 tests respectively (passed/skipped). Nio mypy reports zero errors in
58 source files; MindRoom ty and affected repository hooks pass. The consumer
suite and its subprocesses use the candidate with automatic dependency sync
disabled, with modified producer file hashes checked before and after. Final
integrated controls use a built wheel whose complete Python source matches Git.

Production Python in `src/nio` totals 28,582 lines, including 2,823 in the durable
adapter and 122 in its shared lease. Against starting head `9df7aa9`, production
diff is +3,266/-20,908 (net -17,642); against main `5b6de3bc`, +3,901/-6,809
(net -2,908). The companion production diff against `79ae63c4b` is +441/-1,004
(net -563). Test deletion reflects removal of the superseded engine; retained
behavioral tests cover actual crash, replay, ownership and authorization seams.

The final engine repeat uses the same fixtures and three isolated runs per case.
Old results below are the already measured matched baseline, not a new run.

| Events | Payload | Old NORMAL median | Final NORMAL median | Final FULL median | Final FULL CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| 200 | Plain | 196.7 ms | 17.0 ms | 23.2 ms | 17.3 ms |
| 200 | Encrypted | 262.7 ms | 53.2 ms | 61.8 ms | 53.4 ms |
| 1,000 | Plain | 1,736.7 ms | 76.9 ms | 90.5 ms | 77.3 ms |
| 1,000 | Encrypted | 2,107.8 ms | 272.4 ms | 283.8 ms | 260.0 ms |

For 200 events, commits remain 404 versus 3; final selects are 12 instead of
3,235. JSON decodes remain 1,854 versus 3 for plain input, 2,854 versus 203 for
encrypted input. Unchanged members produce no member-row writes. This removes
repeated engine work without a new JSON dependency or background database owner.
The tight microbenchmark's median maximum loop gap is 23.2/61.9 milliseconds for
200 plain/encrypted events, and 90.6/283.9 milliseconds for 1,000; it runs the
capture/prepare/read/ack sequence without real network or consumer scheduling.
These figures measure that probe, not live health latency or a hard latency bound.

Detailed local evidence is retained under `capacity/durable-sync-kernel/`: initial
`micro-20260906T084930Z`, final `micro-final-20260906T091913Z`, and the explicitly
diagnostic `startup-cap16-20260906T090108Z`. These evidence directories are not
part of the library package or repository contract.


## Qualification correction: forward recovery completion

The first final FULL controls at Nio `ed316ed`/MindRoom `ed4a209d6` exposed a
server-dependent recovery defect. Tuwunel completed only 73 of 200 replies before
the shared deadline, despite empty queues and no application errors. Synapse
passed all criteria: 200 replies, median/p95 77.816/83.654 seconds, 48.356-second
overlap, all three fence principals settled, and zero pending input, batches,
journal work or outbox deliveries after shutdown. The failed Tuwunel result is
not a throughput measurement or a passing drain certificate for all requests.

A diagnostic trace reproduced the cause: a forward page requested from token
841 with `to=913` returned 14 events and `end=908`; the next page returned no
events and no end token. Tuwunel excludes the requested `to` event. Recovery
therefore could not see the retained-tail overlap and emitted false loss, making
later requests historical context. The full workload's clear provenance trace
confirmed recovered and live roots being followed by incorrectly historical
roots. Empty application queues do not prove that all requested work was handled.

Use the existing retained-tail overlap check without an HTTP `to` bound. Page,
byte, continuation and retry limits stay unchanged. Do not interpret events at or
after the first overlap during gap recovery; the captured tail owns those events.
Missing history without a proven boundary still emits loss. This removes one
request constraint and requires no new persisted mechanism or weaker durability.

- [x] Reproduce the bounded-end behavior with real HTTP/SQLite and restart; assert
  chronological recovered messages, one live tail, no false loss, and no early
  application of events beyond the retained tail.
- [x] Apply the omitted-`to` request change, preserve genuine loss cases, run
  focused/full checks, self-review and scoped review.
- [x] Rebuild the wheel and repeat unchanged live controls; record results here
  before pins, commits and publication are considered complete.

The trace is diagnostic evidence, not an uninstrumented latency result:
`recovery-trace-20260906T092704Z` in the retained capacity workspace. Its wrappers
record pagination metadata and classification, not event content or credentials.


The correction is committed at `aac2e32`: one removed HTTP argument and a short
comment, adding one net production line. The real HTTP/SQLite regression passes
both live and restart variants; all 29 recovery tests pass, including genuine
loss controls. Full nio checks pass 700 tests with 3 skipped, zero mypy errors
across 58 source files, and every repository hook. Final production totals are
28,583 Python lines, including 2,824 in the adapter; net reductions are 17,641
against the starting PR and 2,907 against main. The non-limited engine benchmark
above is unaffected by this history-request-only change; the final integrated results below qualify the rebuilt artifact within the
stated limitations.


## Final integrated results

The final runtime is Nio `aac2e32` with MindRoom `ed4a209d6`. All installed
Python files and loaded module paths match those commits before and after each
control. No production behavior, deadline, health timeout, overlap threshold,
workload, or shutdown check was patched. Each run uses FULL durability, 200 roots,
a 180-second shared reply deadline, a 45-second minimum full visible overlap,
and a two-second health timeout. Reply times include about 60 seconds of
synthetic generation; they are not LLM latency predictions.

| Control | Reply median | Reply p95 | Full overlap | Acceptance |
| --- | ---: | ---: | ---: | --- |
| Tuwunel | 73.927 s | 86.501 s | 37.567 s | Overlap below 45 s |
| Tuwunel repeat | 74.830 s | 86.533 s | 37.470 s | Overlap below 45 s |
| Synapse 5,000-event cap | 76.987 s | 82.719 s | 47.352 s | PASS |

Every run completed exactly 200 replies and all three post-terminal fence
principals, with zero input/batch rows, journal work or delivery-outbox debt after
clean shutdown. No event-loop stalls, degraded reads, or incomplete-drain
warnings were recorded. Synapse passes every predicate. Both Tuwunel runs fail
only the original overlap requirement; do not describe them as full capacity
passes. Their recovered-request failure is corrected, and the failed pre-fix
73/200 run remains recorded above.

Compared with the earlier published controls, Tuwunel's median is roughly
unchanged and p95 is about 2.6 seconds slower (86.5 versus 83.9). Synapse's median
and p95 improve from roughly 80.1/87.6 to 77.0/82.7 seconds, and its previous fence
and retained-input failures are absent. These few runs are workload evidence,
not a statistical guarantee of general speedup. The much lower engine cost does
not remove the application's shared startup persistence cost. In the final
Tuwunel runs, preparation starts span 23.5-23.9 seconds; the sixteen-slot trial
already failed to improve this class of delay materially. A separate application
persistence optimization needs its own profile and design. No writer redesign,
serializer dependency, durability reduction, or relaxed threshold is justified
as part of this adapter replacement.

Detailed final controls: `controls-20260906T093405Z` and
`controls-20260906T094006Z` in the retained capacity workspace. The full measured
source delta, test counts, required guarantees, and deliberate limits are above.

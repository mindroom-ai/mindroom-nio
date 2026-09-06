# Durable Classic Sync Implementation Plan

Current scope includes the completed Classic simplification and the subsequent
[Sliding restoration amendment](sliding-sync-restoration.md). Historical
Classic-only constraints and the compatibility audit below describe earlier
checkpoints; the amendment records their resolution and current qualification.

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
- [x] Point the candidate at the local built nio wheel for verification; final
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
- [x] Run final necessary checks, stage only intended files, inspect staged diff,
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


## Publication and artifact verification

The coordinated producer is pushed at `a686d43119a8c14b48d46a57858f70ee1579de55`;
its Python source is identical to the tested `aac2e32` wheel. MindRoom is pushed
at `556cacacb8822aae7b20bedfedadc60438a5871b`, with that immutable producer pin in
both its project configuration and lock file. The pinned-artifact consumer suite
passes 15,522 tests with 22 skipped and 16 warnings in 119.58 seconds. All Python
source hashes match before and after the suite, including subprocesses with
automatic dependency resync disabled. Complete consumer hooks, types and frontend
checks pass. The user's separate checkout is preserved.

Nio remote CI passes Python 3.12/3.13/3.14 tests, mypy, hooks and coverage. Both
existing PR descriptions contain the final API, ownership, source-size and
performance results, including the Tuwunel capacity limitation. The final source
and qualification fixes have independent scoped reviews with no open findings.


The companion remote suite passes 15,520 tests with 12 skipped. All four full/
minimal AMD64/ARM64 image builds, static/plugin checks, runtime image probes,
Kubernetes platform/instance smoke checks, and the Compose stack smoke pass on
`556cacacb`. Both PRs are mergeable. Automated review checks completed without
new findings. This completes the replacement and coordinated verification;
release publication and the separately measured Tuwunel startup-capacity limit
remain outside the completed implementation. Subsequent plan-only commits do
not change the pinned runtime source.

## Startup profiling follow-up

Correlated traces and alternating actual-source controls identify and resolve the consumer ledger's unnecessary serialization without changing Nio production code.
MindRoom `5502b1177` keeps conflicting identity mutations ordered while allowing unrelated writes to await persistence concurrently.
The ledger adds 47 net lines, with a total consumer production-file increase of 28 lines after shortening caller documentation.
Its full suite passes 15,542 tests with 22 skipped, including twenty new SQLite/Postgres cases; all repository hooks pass.

Two uninstrumented Tuwunel runs pass every original 200-root predicate, reducing initial reply spread from 21.69–21.81 seconds to 12.03–12.07 seconds and increasing full overlap from 39.71–40.34 seconds to 49.74–49.90 seconds.
Synapse also passes with 54.219 seconds of full overlap.
All fixed runs complete exactly 200 replies, settle three fence principals, retain zero producer/application/outbox debt and shut down cleanly with verified source hashes.
The producer design and the consumer's ledger ownership document retain the detailed evidence, unchanged guarantees and 1,000-reply qualification limit.
Release publication remains separate work; the previously unresolved local Tuwunel startup target is now met.
Final producer verification passes 700 tests with three skipped in 58.81 seconds, reports zero mypy errors across 58 source files, and passes every repository hook.
Producer source and tests remain unchanged; the final consumer documentation hooks also pass without changing the tested runtime source.

## Dependency tracing after the ledger fix

A follow-up on MindRoom `72a5f5d4f` and Nio `adddd44` correlates all 200 preparation tasks, their actual await chains, capacity releases and consumer writer operations.
Preparation tasks spend 97.9% of their aggregate occupied time awaiting pending-turn persistence.
Resuming the capacity waiter after a slot releases takes 0.022 ms median; the existing consumer database writer is occupied for 99.9% of the traced startup window, with only 0.971 seconds of worker CPU.
The next measured limit is consumer database service time, not another demonstrated preparation-scheduler defect.

Separate diagnostic changes to the Python thread switch interval and ordinary SQLite worker count do not improve initial reply spread.
Neither setting is adopted.
A final normal control passes every unchanged 200-root predicate with 12.083 seconds of initial reply spread, 49.817 seconds of full overlap, all three fence principals, zero retained producer/application/outbox work and clean shutdown.
A native-sampling run is retained only as diagnostic evidence: profiler overhead increases latency and its parent exits with a child-reaping error after application shutdown.

The consumer's `docs/dev/ledger-write-ownership.md` records timing denominators, native-symbol limitations, negative experiments and the next bounded SQL-level investigation.
No producer or consumer production code, dependency, durability policy or concurrency setting changes in this follow-up; 1,000 concurrent replies remain unqualified.

## Membership-query trial

A nine-line consumer candidate removed one duplicate membership-version SELECT per admitted event while preserving the existing transactions and locks.
Two alternating actual-source comparisons show no worthwhile startup improvement: baseline initial reply spreads are 12.090/12.153 seconds, versus 12.229/12.436 seconds for the candidate.
All four runs pass every unchanged 200-root acceptance predicate, including three-principal fences, zero retained debt, source verification and clean shutdown.
The production candidate is withdrawn; useful membership-transition coverage and a projection-progress test timing correction remain.
The consumer's `docs/dev/ledger-write-ownership.md` records the controls, rejected query optimization and remaining writer-time breakdown.
The largest combined category is delivery/outbox work (4.407 seconds of the 13.357-second traced startup); admission, ledger writes, hydration and settlement account for the rest.
This identifies which operations occupy the writer, while the exact low-level synchronization owners remain partly unresolved.
No Nio production change or weaker durability guarantee follows from this trial.

## Consumer SQL replay and GIL attribution

The next investigation uses MindRoom `e276982fdbd3f58b8d471376c417767654819da4` and Nio `ce18a1fed0a592b93b789de4480ac3e61e8c3145` with unchanged production source.
Two verified 200-root traces compare the same startup SQL operations live and in isolated FULL-durability replay: 11.439/11.130 seconds in the live worker versus 1.409/1.340 seconds for direct SQL replay.
Replay verifies fetched results and final logical database contents; it omits application computation and concurrent readers, so this ratio is not an achievable application speedup.
Native counters in the second trace record 98,212 GIL-acquisition condition waits totaling 7.693 seconds and 1,379 fsync calls totaling 2.298 seconds; result propagation through the event loop adds about three seconds.
Native timings include scheduling/CPU and slightly wider bookkeeping; they overlap thread CPU and must not be added as an exact partition.

A fresh normal control passes every original predicate with 11.983 seconds of initial reply spread, 49.873 seconds of full overlap and 75.561-second completion p95.
A diagnostic event-loop writer gives 11.768 seconds of initial spread but a worse 76.847-second completion p95; it also blocks a scheduled callback for 2.212 seconds under real external SQLite contention.
That policy is rejected; keep the existing consumer writer, FULL durability and eight preparations.
No producer/consumer production code or dependency changes in this investigation, and 1,000 concurrent replies remain unqualified.

MindRoom's `docs/dev/durable-ingestion-performance.md` is the evidence entry point, with a tracked JSON measurement summary, a transaction-boundary map in `docs/dev/ledger-write-ownership.md`, and a persistent checksummed reproduction package.
The package retains historical probe variants, helper scripts, native symbols and metadata for portable re-analysis; fresh SQL replay requires regenerating the synthetic workload because SQL parameters/results were intentionally not retained.
It also indexes the rejected query, pool-size, thread-switch and inline-writer experiments so they are not repeated without new evidence.


## Bounded consumer delivery-transaction trial

Two consumer candidates tested the next proposed local-write reductions: skip duplicate device binding for a proven fresh claim, then combine live delivery enqueue/claim.
They added two and 84 cumulative net production lines, passed 231/244 focused tests respectively, and each passed every unchanged 200-root Tuwunel acceptance predicate.
Initial reply spreads were 12.726 and 12.516 seconds, compared with earlier/fresh normal controls of 11.983 and 13.493 seconds.
The combined candidate's median initial latency improved in one run, but neither candidate beat the earlier control's p95; these exploratory results do not establish a repeatable startup-tail gain or statistical equivalence.
Both candidates are withdrawn rather than retaining extra code without a demonstrated benefit.
Consumer production source, tests and whitelist return to the pre-trial tree; the earlier ledger-concurrency improvement remains intact.
No Nio production change, dependency change or weaker guarantee follows from this trial.
The consumer's performance evidence index, tracked measurement JSON and ledger ownership document preserve the exact runs, candidate commits, decisions and reproduction addendum.
The existing 200-reply qualification stands; 1,000 replies remain unqualified.


## Compatibility audit: main-store adoption blocker (resolved)

Resolved by `98e0f78`: adoption rejects outstanding released recovery gaps,
unaccepted timeline records and unacknowledged loss markers before mutating the
store. Focused tests pass 33 cases; the previous release independently reopened
all ten rejected fixtures with keys, trust, token and obligations intact. Clean
stores remain adoptable. Sliding APIs and durable recovery are restored by the
linked amendment. The findings below remain as historical evidence of the defect.

At this audit checkpoint the PR removed the fork's Sliding Sync implementation entirely, including `sliding_sync()`, `sliding_sync_forever()`, response types and persisted window-token APIs.
It also replaces the released backfill configuration, admission callbacks and recovery/checkpoint APIs with `nio.durable`; the source reduction includes deliberate feature removal, not only implementation simplification.
Ordinary upstream-style APIs do not imply compatibility with all APIs on this fork's main branch.

A subsequent audit at `b86cb8f` reproduced an unsafe upgrade from main `5b6de3bc` using the actual store APIs in each checkout.
Main's `save_recovery()` persisted an advanced sync token, one recovery gap and one unaccepted pending timeline event.
`DurableStore` accepted that database and copied the advanced token, while `next_batch()` and retained input were both empty; the old gap and event rows remained in their legacy tables and were not consumed by the new engine.
This is a released main-store migration issue, distinct from deliberately rejecting unmerged ingestion databases.
Before merge, either transfer existing recovery obligations safely or reject adoption while outstanding obligations remain, with regression coverage against a database created by main.
Do not describe the current cutover as safe for an undrained main recovery store.
The local two-process evidence is retained under `capacity/compatibility-audit-main-to-durable`, with `main.json` and `adopted.json` recording the verified observations.
No production fix had been made at the audit checkpoint; its merge-ready recommendation was superseded by this blocker until the resolution above.


## Sliding restoration qualification

The [restoration amendment](sliding-sync-restoration.md) records completed ordinary
and durable Sliding support through the shared core. Both transports pass matched
200-reply controls on Tuwunel and Synapse; Sliding also passes actual history-gap
and application restart checks on both. Performance is similar in these controls,
with no demonstrated general speed advantage. Restoration adds 1,526 net producer
production lines, leaving the complete PR 1,381 lines smaller than main. Final
producer checks pass 844 tests with three skipped, zero mypy errors and all hooks.
The consumer evidence index retains failed attempts and reproduction metadata,
including one unexplained pre-fix health timeout. The 1,000-reply target and release
publication remain separate work.

## Review corrections and streaming-frequency experiment

This follow-up starts at `292ab7d`. Four reported failures are hypotheses until
reproduced with focused tests. Preserve the shared interpreter, SQLite owner,
bounded recovery and acknowledged batch interface; do not add another journal.

- [x] Inject a trust-write commit failure, then attempt subsequent key handling.
  Fix the existing mutation boundary so rolled-back trust cannot authorize work.
- [x] Reproduce an unseen leave/rejoin before a successful local leave's echo.
  Identify the actual authoritative observation instead of matching only a
  membership string; keep one retained local operation and existing epoch rules.
- [x] Reproduce a linked join-to-join profile update in a limited/reset Sliding
  timeline. Preserve proven continuity and recover intervening messages.
- [x] Reproduce power-state deletion followed by restart. Restore member levels
  from canonical room power, including its default, without rewriting all member
  rows or retaining a second authorization authority.
- [x] Run focused regressions, the full producer suite, clean typing and hooks;
  review the fixes, commit, rebuild and exactly pin the consumer wheel.
- [x] Run a bounded streaming-frequency experiment in the existing consumer
  harness. Keep 200 roots, 4,800 characters, 80 characters/second, the first
  40-character chunk, tool behavior, FULL durability and acceptance predicates.
  Compare subsequent 40-character chunks with 400-character chunks in ABBA order
  on Tuwunel Sliding. The diagnostic affects model chunk delivery only, and is
  not a proposed production change. Preserve source/helper hashes, all failures,
  per-reply timings and actual update counts. No concurrent test processes.
- [x] Record whether reducing chunk work improves startup latency consistently
  across both pairs. Confirm on Synapse only if the result warrants it. Retain
  the original capacity controls separately; do not claim a new production
  speedup, statistical equivalence or 1,000-reply qualification from this trial.
- [x] Save portable evidence and tracked conclusions, self-review, commit and
  push the completed work. No serializer, database or scheduling rewrite is
  implied by this experiment.

### Review correction results

All four reports at `292ab7d` reproduced independently. A failed verification
commit allowed an incoming key request to forward a key; the attached public
trust mutations now use the existing transaction/disposal boundary. Deleted room
power returned from a stale member snapshot after restart; member levels now
derive from canonical room power. Both changes together remove 23 production
lines, and their combined focused verification passes 62 tests.

The local-membership report exposed an unsupported exact-echo assumption: standard
join/leave responses do not identify a membership event. The normative contract
now describes one complete post-operation boundary, historical treatment of the
uncertain interval, and a single provable net transition. Tests injecting stale
pre-operation bodies directly after HTTP were replaced; actual runner tests
continue to verify rejection of completed pre-operation polls. Endpoint rejoin
clears room/member and Sliding checkpoint state before requesting a fresh
authorization baseline. Fresh own state and invitations settle pending commands;
absent evidence does not. Invited rooms cannot persist through the joined cache.

Linked Sliding profile updates now preserve continuity through an ordered chain
anchored in the previous proof. Genuine departures and unproven joins still
prevent recovery from claiming continuity. Scoped review reproduced and resolved
the related rejoin-baseline, optional-field and invitation-cache cases before
qualification. No new queue, journal, store or serializer was introduced.

Verification: 863 passed, three skipped in 75.38 seconds; mypy reports zero errors
across 60 source files; all repository hooks pass. The fixes change five
production files by +160/-59, or 101 net lines. Against main `5b6de3bc`, the full
PR changes production by +5,193/-6,473, or 1,280 fewer lines. Counts include
comments and blank lines and exclude tests, scripts and documentation.
Consumer `2fb726841` pins and installs the rebuilt producer wheel at `ba75e3a`.
All 200 affected consumer tests and repository hooks pass. Later documentation
commits do not change either qualified production tree or require another pin.

### Streaming-frequency results

All four sequential Tuwunel Sliding runs pass the unchanged 200-root acceptance
predicates, recovery/restart gap probe, clean shutdown and zero retained debt.
The runtime source, wheel, helpers and image identities match across the ABBA
series. The consumer's tracked performance document and measurement JSON retain
the complete run identifiers, timestamps, update counts and reproduction archive.

| Arm | Subsequent chunk characters | Initial reply p95 (s) | Completed reply p95 (s) | Matrix updates |
| --- | ---: | ---: | ---: | ---: |
| A1 | 40 | 12.257 | 78.657 | 5,084 |
| B1 | 400 | 11.674 | 74.538 | 3,032 |
| B2 | 400 | 11.353 | 74.021 | 3,031 |
| A2 | 40 | 12.231 | 76.001 | 5,027 |

The larger chunks reduce Matrix updates by about 40%, but initial-reply p95 only
improves 0.58 and 0.88 seconds in the two pairs; median startup is mixed. Completed
reply p95 improves 4.12 and 1.98 seconds. This changes provider wakeups, downstream
chunk work and Matrix edits together. Nominal generation time remains 60 seconds,
but coarser synthetic sleeps also change accumulated scheduling delay. The same
seed/tool policy produces different continuation counts from different assembled
histories. Four runs therefore do not isolate one bottleneck or prove a production
speedup. They show a modest tradeoff for less frequent streaming updates.

Decision: retain existing production streaming behavior. The small startup result
does not warrant Synapse confirmation or more implementation in this task. There
is no new performance code, database, serializer or weakened durability policy.
The retained correctness fixes add 101 net producer lines; the complete PR remains
1,280 production lines smaller than main. The 1,000-reply goal remains unqualified.

Portable evidence is retained in the capacity workspace as
`reproducibility/20260906-review-streaming.tar.gz`, SHA-256
`535f54ef93f610b865b5a31eeacf4c46d2b7c5d18399961fc1f3c8ad0f79a92b`.
All 25 manifested files verify, and standalone reanalysis reproduces the four
cohorts and ABBA comparison exactly. The archive README and the consumer's
`docs/dev/durable-ingestion-performance.md` describe fresh reproduction and the
frozen companion harness dependency; raw logs, keys and databases are excluded.

## Room lifecycle review corrections at `6143e8a`

All six new reports reproduce. Ten real-boundary room lifecycle cases fail for
plaintext sending after local join (including restart), stale Sliding proof,
reinvitation before/after restart and explicit Classic bans. Independent HTTP
tests reproduce the recipient-cache encryption rejection and truncated-body
retry omission. Correct the existing owners described in the contract; preserve
the shared interpreter, single transaction owner and deliberate membership limits.

- [x] In `durable/client.py` and `durable/recovery.py`, consolidate joined-room
  reset, retaining encryption and invalidating old members/recipients/group state.
  In `durable/sliding.py`, own checkpoint invalidation and reuse it for local HTTP
  outcomes and end-of-input reconciliation. Regression: local join followed by
  a limited or ordinary delta cannot recover/authorize from the removed snapshot.
  At `client/async_client.py`'s send boundary, reject plaintext before a new
  room's state is established; test that HTTP sees no message until fresh state.
- [x] In `durable/processor.py` and the existing room persistence boundary, make
  an invited projection replace its joined cache and member rows. Test both
  transports with and without a preceding local leave, then reopen the store.
- [x] In `durable/recovery.py`, preserve final explicit own membership before
  filtering captured timeline duplicates. Test bans in state and timeline after
  local join; persisted and emitted membership must both remain ban after restart.
- [x] In `client/base_client.py`, allow `encrypt()` to use a completed durable
  recipient cache. Test actual lookup/query/claim/share/send and peer decryption;
  preserve rejection for missing/in-flight cache and ordinary incomplete rooms.
- [x] In `durable/transport.py`, include `ClientPayloadError` in the existing
  retry tuple. Test a truncated real HTTP response followed by success; retain
  bounded exhaustion and cancellation behavior.
- [x] Run focused regressions, full producer suite, typing and hooks; review
  the combined lifecycle change and report actual production deltas. Rebuild
  and exactly pin the consumer wheel, run affected consumer checks, then commit
  and push. Existing capacity measurements retain their original source IDs;
  repeat live qualification only for a concrete new performance concern.

Verification commands: `uv run --locked pytest --benchmark-disable`,
`MYPYPATH=src uv run --no-sync mypy -p nio --warn-redundant-casts --no-incremental`,
and `uv run --no-sync pre-commit run --all-files`. Keep focused red/green logs in
the persistent `capacity/durable-sync-kernel/review-room-lifecycle-20260906`
evidence directory. No new benchmark campaign, serializer or database redesign.

The fixes share the existing lifecycle and projection owners. Seven production
files change by +79/-49, or 30 net lines; the full PR is now +5,221/-6,471 against
main `5b6de3bc`, or 1,250 fewer production lines. Sixteen added regression cases
exercise actual HTTP/crypto, restart, sparse Sliding state and both invitation
paths. The public encrypted-send test decrypts the delivered ciphertext at the
peer. The extra unknown-encryption case proves that a new local join cannot send
plaintext before receiving room state. Review found no further blocker in this
correction wave; its proposed cleanup removed a duplicate Classic member scan.
No persisted schema, public API, queue or new guarantee was added.

Producer verification after the final cleanup: 879 passed, three skipped in
71.72 seconds; zero mypy errors across 60 source files; all repository hooks
pass. Scoped re-review confirmed the cleanup preserves boundary ordering.
Consumer `pyproject.toml` and lock exactly pin producer
`cb7c55da1ffca2f1cdf0733968b208ffe082b397`. The rebuilt wheel matches all 60
producer Python files; 287 affected consumer tests pass in 35.45 seconds and
all consumer hooks pass. Consumer production source remains unchanged. Later
documentation commits do not require another wheel pin. Both repositories retain
the qualification record; no additional performance claim follows from these
correctness changes.

## Recipient and recovery invalidation review at `785c835`

Two reports concern existing invalidation owners. Recipient refresh must retire
keys shared with an earlier recipient set. A Sliding recovery loss must request
the fresh state needed to resume future authorization. Keep the existing crypto
lock, synchronous transaction, room metadata and source reset; no new schema,
queue, public API or per-event validation layer.

- [x] Reproduce recipient removal/addition through real HTTP and encryption.
  Compare completed recipient sets in `durable/outbound.py`, rotating for a
  changed or unknown set. Preserve the group for reordering/profile changes.
  Verify a removed recipient's old key cannot decrypt the next ciphertext.
- [x] Reproduce handled Sliding history failure followed by ordinary deltas.
  In `durable/recovery.py`, reuse the post-tail source reset when a joined room
  still lacks its baseline. Verify the next HTTP request asks for fresh initial
  state and later events become LIVE. Cover restart during retained recovery
  without changing pagination candidates before the tail.
- [x] Run focused regressions, full suite, mypy and repository hooks; review the
  combined diff. Record source size, commands and results. Rebuild and pin the
  consumer wheel, run affected checks, then commit and push both repositories.

Focused evidence lives in the persistent capacity workspace under
`durable-sync-kernel/review-invalidation-20260906`. Commands and final results
are recorded there and in this section. Existing live performance results
keep their original source revisions; these fixes make no new speed claim.

Producer verification: 885 passed, three skipped in 71.88 seconds; zero mypy
errors across 60 source files; all repository hooks pass. Six added regression
cases exercise HTTP and cryptography rather than only inspecting invalidation
calls. The recipient/outbound suites pass 31 tests, and Sliding/recovery suites
pass 55. Scoped review found no additional blocker in these corrections.

The fixes add five production lines across two existing files (+3 recipient
refresh, +2 Sliding recovery). The full PR against main `5b6de3bc` is
+5,226/-6,471, or 1,245 fewer production lines. Counts include comments and blank
lines and exclude tests, documentation and scripts. No schema, queue, public API
or performance-policy change was added. Set comparison runs only at completed
recipient lookup; Sliding resets occur only when a joined baseline is missing.

MindRoom pins the rebuilt producer wheel at
`76e4fa4bf441430227205671bb48448477d2a38c`; all 60 installed Python files match
that revision. Its broader integration check passes 337 tests with one skipped
in 7.78 seconds, and all consumer hooks pass. The initial parallel run exposed
a 20 ms watchdog fixture race unrelated to the Nio paths. Controlled poll ticks
and a module-local clock now test real watchdog progress handling deterministically;
disabling generation observation in a subprocess makes the test fail. Consumer
production code is unchanged. The original failure, isolated control, mutation
probe and final qualification logs are retained with the evidence. Subsequent
producer documentation commits do not require another dependency pin.

## Native whole-PR review loop at `10ef9aa`

Apply MindRoom's native review loop: the main thread owns all edits, validation,
commits and pushes; two fresh read-only reviewers inspect the entire PR after
each pushed correction. Findings are untrusted until reproduced. Stop only when
both approve the same pushed head. Reconsider the design after every third round
that still finds substantial issues or a new major bug class.
The review gate is a reproduced, realistic failure within the agreed contract.
Reject speculative safeguards, style-only changes and broader refactors without
demonstrated immediate value. Prefer correcting existing owners; report each
correction's production delta. Review must not expand the deliberate guarantees.

Round one reports four existing-boundary defects. Main-thread regressions
reproduce disposal bypass at public encryption/HTTP, malformed state preserving
live authority, completed-but-unaccepted polls escaping quiescence, and response
validation leaking payloads at WARNING and DEBUG. The initial focused run has
21 failures and three passing controls. An independent Python 3.12 full run
hangs at the truncated-response test's server cleanup: the successful connection
is never closed. Its request and retry assertions already succeeded.

- [x] Check disposal at shared login, store and HTTP boundaries; cover close,
  rollback and a send suspended before its network request.
- [x] Reject malformed state in the single durable collector, using existing
  transaction rollback/disposal. Preserve ordinary parsing and malformed messages.
- [x] Revoke poll ownership before cancelling during quiescence, using the same
  completed-poll rule as local membership operations.
- [x] Remove response bodies and validation exception messages from diagnostics.
  Keep response/error class and schema rule; callers still receive error details.
- [x] Close both test-server responses, then verify Python 3.12 and the normal
  environment, typing and repository hooks. This needs no production retry change.
- After each correction, commit/push and repeat fresh whole-PR reviews. Record
  final verdicts and companion-pin qualification with the retained loop evidence.

Persistent commands, logs and review dispositions live under the already indexed
capacity evidence directory `durable-sync-kernel/review-invalidation-20260906/native-loop`.
No new queue, schema, interpreter or performance guarantee is planned.

Round-one qualification: 910 passed and three skipped in Python 3.14.7 (85.66s)
and Python 3.12.13 (84.57s); zero mypy errors across 60 files; all repository
hooks pass. The corrections add 13 net production lines across five files
(+25/-12). The complete PR against `5b6de3bc` is +5,249/-6,481 across 42 production
files, or 1,232 fewer lines. These are correctness changes; existing live
performance measurements retain their original revision IDs.

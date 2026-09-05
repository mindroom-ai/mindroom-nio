# Classic gap recovery implementation plan

> For agentic workers: use executing-plans for the coupled recovery changes;
> independent performance and companion validation can use focused agents.

**Goal:** Recover skipped Classic requests and reduce measured ingestion waste.

**Architecture:** Freeze one bounded Classic sync response, checkpoint bounded
history pages through ordinary Frames, then commit its final tail. Reuse the
existing owner, crypto preparation, admission, and journal.

**Tech stack:** Python 3.12+, asyncio, SQLite, existing Matrix HTTP transport.

**Spec:** `docs/design/classic-gap-recovery-and-capacity.md`.

## Global constraints

- Preserve ownership, authenticated persisted data, event order, admission
  identities, and prepared-output bounds.
- Standard-library JSON; no runtime dependencies or second delivery queue.
- Cold history stays non-actionable. New recovery scope is Classic sync.
- Preserve the user's uncommitted companion dependency changes.
- Use persistent evidence, explicit staging, and fresh commits.
- Validate tests against the intended behavior; remove obsolete implementation
  assertions instead of restoring excluded guarantees.

## Task 1: Recover limited Classic intervals

Files: `src/nio/ingest/recovery.py`, `ports.py`, `source.py`, `coordinator.py`,
`reducer.py`, `src/nio/client/base_client.py`, and journal/schema modules.
Tests: owned recovery, phase, journal, completion, and existing source suites.

- [x] Reproduce missing-middle delivery in an owned session with a trusted
  baseline. Assert recovered then fresh order and recovered provenance.
- [x] Add authenticated recovery evidence and preserve the original response
  digest. Keep capture and replay in the existing owner and journal.
- [x] Replace whole-interval capture after its real capacity failure: retain
  the original response once, checkpoint crypto prologue, bounded pages,
  and final tail through independently identified Frames.
- [x] Reproduce and fix restart, encryption, cross-page state and membership,
  local departure ordering, multi-room phases, and final completion timing.
- [x] Reproduce and fix review findings: ordinary completion during pending
  recovery, eligibility behind queued Frames, invite/knock callback routing.
- [x] Pass a 19 MiB history regression with bounded queued Work, ordered
  delivery, unique record identities, and one final completion.
- [ ] Complete the final full-suite and real-server acceptance gates below.

## Task 2: Keep oversized responses recoverable

Files: `src/nio/ingest/recovery.py`, `classic.py`, `coordinator.py`.
Tests: owned recovery and existing Classic/coordinator suites.

- [x] Bound owned Classic windows, preserving smaller user limits. The initial
  ceiling was 500; Task 5 selects 100 after measuring retained-body latency.
- [x] Halve oversized successful responses at the same cursor down to one.
  Preserve other filter selections and recover limited history after narrowing.
- [x] Bound pages to 100 events and 2 MiB; narrow oversized pages at the same
  position. Retain the 16 MiB combined input and 24 MiB Frame limits.
- [x] Cover irreducible oversize, pagination progress/cycles, empty pages,
  adjacent overlap, bounded transient retries, and the 1,000-page watchdog.
- [x] Review canonical bounds: the strict parser rejects floats and compact
  UTF-8 encoding cannot expand an accepted unchanged root. Retain the defensive
  post-encoding bound; do not invent encoder behavior to test an unreachable
  expansion. Existing oversize/cursor-preservation cases pass.

## Task 3: Remove measured redundant validation

Files: source carriers, coordinator settlement, and journal settlement.

- [x] Benchmark before implementation with alternating real-SQLite workloads.
- [x] Remove internal frozen-source reconstruction and its unsupported
  mutation-escape-hatch test; retain network/disk corruption checks.
- [x] Remove repeated settlement event canonicalization; validate caller batch
  on acknowledged retries, retaining authenticated full equality otherwise.
- [x] Reject cache machinery without a convincing measured benefit.
- [x] Run focused ownership, retry, source, and settlement tests for both
  measured variants. Keep standard-library JSON.
- [x] Measure and implement one immutable decoded Frame entry, after the
  error-free live run exposed remaining catch-up cost. Preserve full-row,
  authentication, current-owner revision, mutable-outbound exclusion, and close
  behavior; verify changed disk data and normal delivery before rerunning live.

## Task 4: Companion and final acceptance

Companion files: recovered lifecycle admission and callback typing; tests for
admission, owned sessions, bot readiness, crypto, and desktop integration.

- [x] Accept recovered lifecycle provenance with positive coverage, preserving
  LIVE-only reply grants.
- [x] Check focused companion integration against local Nio without changing
  the user's dependency pins.
- [x] Reproduce pending delivery projection at the real admission boundary;
  preserve rollback and the outstanding batch while the pump awaits its
  existing outbox worker's next progress signal. Test success, no busy retry,
  cancellation, unrelated outbox debt, and propagation of unrelated admission
  errors.
- [ ] Run uninstrumented 200-conversation Tuwunel and Synapse 5,000-window
  controls. Require exact replies, zero duplicates, drained queues, and
  continued post-load sync, with the existing deadline unchanged.
- [x] Run final full Nio pytest, repository hooks, and zero-error mypy.
- [x] Recheck companion integration and zero-error typing against final source.
- [ ] Record actual production size, tests, performance, and capacity results.
- [ ] Review final changes, commit, push authorized branches, and verify remote
  checks. Report remaining limits without claiming unrelated gaps are closed.

## Task 5: Resolve remaining measured capacity limits

The subsequent unchanged controls distinguish corrected recovery from remaining
latency and workload-shape limits. Neither control passed every acceptance
predicate. Do not weaken the 180-second workload deadline, two-second health
read, 45-second full visible overlap, exact replies, fence, or drain checks.

- [x] Measure synchronous Nio processing against the actual large Synapse event
  shape; distinguish maximum event-loop blockage from total throughput before
  selecting a smaller source window or another performance change.
- [ ] Reduce the fixed Classic request ceiling to 100, preserving oversize
  halving, smaller user limits, and all recovery/cursor behavior; verify tests
  and repeat live controls.
- [x] Diagnose MindRoom's serial dispatch preparation with targeted timing.
  Preserve durable pending-turn recording and ordering. Fresh roots already
  skip thread-history reads and text debounce; do not delete imaginary work.
- [ ] Benchmark any selected change, test its real behavioral boundaries,
  rerun unchanged live acceptance, then update the final results and publish.

## Measured results

Baseline: Nio `1021de4`, 45,038 production Python lines. The earlier successful
capacity control used Synapse's 500-event window.

### Performance

Commit `44f7b91` removed 29 production lines and a 27-line unsupported mutation
test. Five paired 500-message runs with 4,800-character bodies measured median
total-delivery improvements of 2.64% for one Frame and 1.85% for 100 smaller
Frames. All pairs improved; 491 focused source tests passed.

The second patch removes 22 production lines. Five paired 1,000-message runs
with 4,800-character bodies measured median improvement of 4.67%
(all pairs 3.79–5.75%); control median was 1,202.8 ms and candidate median
1,137.1 ms. Short-message gains were noisy and are not claimed.
Each variant passed 101 focused settlement/ownership tests. No dependency
or cache was added. These are local delivery measurements, not application
throughput claims; percentages must not be summed.

The incremental-recovery benchmark used 1,000 recovered streaming edits and
100 fresh events. Keeping the same event shape while increasing the retained
sync response from 40,907 to 1,244,907 bytes raised median delivery time from
3.380 to 5.135 seconds. This isolates repeated retained-body work; it does
not attribute the real server's much longer delays entirely to Nio.

Three paired large-response runs measured removal of duplicate staging
payload encoding: median 2.63% improvement, all pairs 2.13–3.54%, with six
production lines removed. A normalized Frame cache measured 4.69%, but its
additional roughly 0.11-second saving here did not justify another cache.
The final encoding, size bounds, authentication, and transaction rollback
remain. Thirteen targeted atomic staging, crash, restage, and identity cases
passed with both the probe and actual production deletion.

The separate immutable decoded Frame cache adds 18 net production lines.
Three alternating recovery pairs reduced median delivery from 5.137 to 4.187
seconds, with a median paired improvement of 18.51% (17.13–19.28%). This
avoids repeated persisted Frame decoding, not original-response normalization.
A ten-Frame ordinary delivery control showed no speedup: medians were 1.909
and 1.919 seconds, with paired changes within approximately one percent.
These local measurements do not establish live application capacity.

The implementation keeps full actual-row equality and current-owner validation,
and excludes prepared state containing mutable outbound operations. Ten added
behavioral cases cover changed persisted columns, owner identity/revision,
rollback/restage/reopen, and mutable outbound context reload. All 314 focused
source/journal/recovery cases passed; mypy and changed-file hooks passed.

Review also found outbound readiness polled before checking the same Frame's
remaining Work, although outbound cannot run until that Work drains. Checking
authenticated Work first avoids this redundant probe while preserving pending
hydration progress. Three alternating pairs on top of decoded Frame reuse
measured a further 1.36% median reduction (0.69–3.97%; median elapsed 4.163
versus 4.089 seconds). The reorder adds two net source lines and no state.
This small local improvement remains separate from the live acceptance result.

### Capacity correction and regression verification

The first Tuwunel rerun completed 200 canonical replies and initial drain, then
failed the post-load reaction/sync fence after repeated whole-capture byte-limit
errors. This was a failed acceptance run. Incremental recovery replaces that
policy, draining bounded Work between pages.

The new 19 MiB backlog regression passes, along with staged/settled-page restart,
encrypted recovery, and membership transition coverage. Independent review
reproduced three additional issues and approved their fixes after six focused
tests plus a cancellation/reopen probe.

A pre-final full suite passed 2,132 tests and skipped three, with six failures
from schema/table inventory and internal dataclass-field assertions. Those
expectations were updated to retain meaningful schema crash checks and value
identity checks; all 151 model/crash tests then passed. Final full-suite and
real capacity results follow.

### Local checks and production cost

Before the final two optimizations, the Python 3.14.7 suite passed 2,147 tests,
skipped three, and reported
three existing fork-with-threads deprecation warnings in 213.06 seconds.
Full mypy reported zero issues in 71 source files. All repository hooks passed,
including explicit checks of the new, not-yet-tracked source and tests.

The pre-ceiling-change full run passed 2,155 tests, skipped three, and
failed two test synchronization cases in 213.59 seconds. Both cases waited
for the outbound probe that Work-first scheduling correctly skips. Their hook
now observes and delegates the actual progress wait. Both corrected cases pass,
preserving the original no-write, no-network, exact-retry, acknowledgement-wake,
and completion assertions. That correction changed tests only; the later
100-event ceiling has its own fresh full-suite result below.
This verifies 2,157 cases across the full run and corrected reruns; it is not
a claim that the first full command exited successfully.

Companion validation against this local Nio source passed all 694 selected
admission, journal ingress, bot readiness, client, and desktop tests. Its six
changed-file hooks passed, including full source/test typing and module-boundary
checks. The original checkout's dependency edits were preserved.

Production Python totals are 45,861 lines in 71 files: 823 net lines above
`1021de4`, and 361 below the original simplification baseline `742806f`.
The new continuation and journal integration account for the increase; the
three performance simplifications remove 57 production lines; decoded Frame
reuse and Work-first scheduling add 20. This is a real
correctness feature with maintenance cost, justified by the reproduced lost
requests and whole-backlog stall, not a source-size reduction.

The incremental Tuwunel rerun also failed the unchanged post-load fence.
It completed all 200 replies and initial drain, with no recovery byte error.
Three pending-delivery-projection exceptions escaped the companion pump and
triggered receive-loop restarts with 5-, 10-, and 20-second delays. Other
principals settled the fence reaction; the responder had not admitted it before
the deadline, and its final retained tail still contained that reaction.
This identifies a real admission retry defect and does not establish that
removing restart delay alone will satisfy capacity. Preserve the exact fence
and deadline for the next run. Synapse remains unrun.

Review reproduced a liveness defect in the first proposed wait: the relevant
edit was projected, but an unrelated room's failed send kept the complete
outbox task running and the pump parked. The corrected design wakes on each
recovery pass and rechecks the admission barrier, preserving the worker's
existing backoff and ownership.

The final companion projection-wait change passed 420 focused admission, bot,
and projection cases, including SQLite/PostgreSQL rollback and unrelated-debt
regressions. Four existing store-barrier cases also passed. Changed-file hooks,
full project typing, and module-boundary checks passed. Independent review ran
three additional probes for unrelated debt, a prior pass signal, and shutdown;
it approved the corrected wait. The next live controls remain the acceptance
gate.

The next Tuwunel run completed all 200 replies with zero sync failures,
projection errors, restarts, watchdog stalls, or degraded reads, but still
failed the unchanged post-load fence. Reply latency was 74.275 seconds median,
85.855 seconds p95, and 87.013 seconds from first input to last reply. All
402 outbox rows were acknowledged. Catch-up settled 16,744 companion events;
the fence had not been admitted by any of the three syncing principals by the
deadline. All three retained final tails contained the exact fence reaction.
The 16,747 total companion rows represented 5,586 distinct event IDs processed
by three principals, not runaway duplicate admission.
These numbers establish reply delivery, not passing capacity acceptance.

Removing restart delay was therefore necessary but insufficient. Re-examining
the recovery architecture identifies repeated frame decoding during per-event
delivery as the next measured target. Keep every recovered revision in order;
do not skip edits, weaken the fence, enlarge the deadline, or add a second
admission path to make this workload pass. Synapse remains unrun.

### Live results after decoded Frame reuse and Work-first scheduling

The next Tuwunel run completed all 200 replies, the exact post-load fence across
all three principals, subsequent sync advancement, zero-debt drain, and clean
shutdown. It reported no sync/projection errors or stalls. All 402 outgoing
initial/final deliveries were acknowledged and 17,016 companion rows settled.
Reply median was 74.007 seconds, p95 84.778 seconds, and the last reply completed
85.853 seconds after the first input.

The full run nevertheless failed its final overlap predicate: all 200 visible
streams overlapped for 43.297 seconds, below the unchanged 45-second minimum.
Roots were released in 0.171 seconds; initial Matrix replies spread over
20.402 seconds, while individual visible streams lasted 63.563–66.080 seconds.
The earlier slow Synapse control's longer streams also increased overlap, so
overlap alone is not a throughput metric. Preserve this failed qualification
alongside the passing recovery/fence result.

Current Tuwunel logs attribute the startup spread to one requester/coalescing
key: 200 serial flushes consumed 20.288 seconds with only 0.136 seconds of gaps.
Callback-to-enqueue averaged 1.9 milliseconds. The interval containing dispatch
preparation, planning, pending-turn persistence, and scheduling averaged
97.87 milliseconds; existing logs do not separate those costs. Visible Matrix
placeholder timestamps and the streaming-entry log do not establish actual
provider invocation times. No further production change follows from these
logs alone.

The first unchanged Synapse 5,000-window control failed a two-second health
HTTP read during terminal waiting, before reaching the fence. Its forced-stop
snapshot retained 133 acknowledged load replies, pending recovery for all
three principals, 7,692 settled companion rows, and 41 pending rows. There
were no sync/projection errors; the shutdown traceback came from forced
lifespan termination. This establishes a responsiveness failure but does not
identify which synchronous operation caused it. Partial-cohort reply times
are not compared with complete-cohort results. Both runs preserved source
files and the user's dependency edits byte-for-byte.

Matched local SQLite measurements reproduce the large-response latency risk.
Three owned sessions sharing one loop use the observed 8.25/5.06/5.06 MiB
response shapes and verify 3,000 ordered records and callbacks. A 100-event
window reduces recovery from 22.79 to 13.66 seconds and maximum heartbeat pause
from 2.236 to 0.723 seconds. Ordinary delivery changes from 9.77 to 10.01
seconds, with pause reduced from 1.399 to 0.392 seconds. Network results yield
but have no simulated latency; smaller windows need more requests. This
justifies changing the existing constant, without extra cache or batching
machinery, while keeping live attribution and acceptance separate.

The 100-event ceiling is implemented. Default and larger caller limits retry
100 then 50 on oversize; a smaller limit of 24 retries 24 then 12. Existing
cursor, filter, and recovered-delivery assertions remain. The fresh full suite
passes 2,159 tests, skips three, and reports three existing fork warnings in
205.75 seconds. All 488 focused tests, full mypy (71 files), repository hooks,
and explicit checks of new files pass. Live acceptance remains pending.

### Dispatch timing and proposed writer experiment

A diagnostic run timed the same 200-root Tuwunel workload without changing
production behavior. Its 200 serial dispatches consumed 21.314 seconds;
pending-turn persistence accounted for 20.337 seconds (95.4%). That includes
8.297 seconds waiting for the shared ledger lock and 11.836 seconds awaiting
the database: 10.259 before execution, 0.607 executing transactions, and 0.969
returning to the caller. Preparation and planning were small. The pre-execution
interval includes queueing and scheduling, so it is not all removable overhead.

This instrumented run completed all replies, the fence, and final drain, but
failed full overlap (41.433 seconds) and its wrapper's clean-shutdown check.
Application logs reached shutdown completion; the wrapper did not retain the
exit status needed to explain that discrepancy. It is diagnostic evidence,
not a passing capacity control.

Benchmark replacing the asynchronous SQLite writer queue with one serial
executor before selecting another production change. Preserve one FULL
transaction per operation, FIFO admission, rollback, cancellation settlement,
cross-loop callers, queued-write refusal on close, and drain before connection
teardown. Do not remove the shared ledger lock or either pending-turn write:
one freezes response context, the other binds the visible Matrix reply ID.
A prototype result alone does not authorize changing these semantics.

Two alternating-order pairs rejected this proposed backend change for the
current performance task. With 200 real pending-turn records, eight concurrent
write producers, and production JSON work on a retained response, mean elapsed
time fell only from 8.394 to 8.116 seconds (3.30%). Idle runs improved 4.16%.
Both variants committed 1,800 operations independently and passed rollback,
repeated-cancellation settlement, and queued-close refusal probes. Worker idle
time while writes were queued fell substantially, but execution wall time rose
under concurrent JSON processing; most cost moved rather than disappearing.
Cross-loop production support would require additional future/drain changes.
Keep the existing backend; these results do not justify that architecture change
or establish a fix for the live dispatch ramp.

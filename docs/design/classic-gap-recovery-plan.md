# Classic gap recovery implementation plan

> For agentic workers: use executing-plans for the coupled recovery changes;
> independent performance and companion validation can use focused agents.

**Goal:** Recover skipped Classic requests and reduce measured ingestion waste.

**Architecture:** Freeze one bounded Classic sync response, checkpoint bounded
history pages through ordinary Frames, then commit its final tail. Reuse the
existing owner, crypto preparation, admission, and journal.

**Tech stack:** Python 3.12+, asyncio, SQLite, existing Matrix HTTP transport.

**Spec:** `docs/design/classic-gap-recovery-and-capacity.md`.

**Current status:** The selected recovery, projection retry, and performance
corrections are committed and published in both repositories. Full local suites,
types, hooks, remote tests, image builds, and smoke stacks pass. Final Synapse
capacity acceptance passes; Tuwunel passes every functional check but remains
below the 45-second overlap requirement. Full qualification is still open.
See the final results and separate scheduling scope at the end of this plan.

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
- [x] Record actual production size, tests, performance, and capacity results.
- [x] Review final changes, commit, push authorized branches, and verify remote
  checks. Report remaining limits without claiming unrelated gaps are closed.

## Task 5: Resolve remaining measured capacity limits

The subsequent unchanged controls distinguish corrected recovery from remaining
latency and workload-shape limits. Neither control passed every acceptance
predicate. Do not weaken the 180-second workload deadline, two-second health
read, 45-second full visible overlap, exact replies, fence, or drain checks.

- [x] Measure synchronous Nio processing against the actual large Synapse event
  shape; distinguish maximum event-loop blockage from total throughput before
  selecting a smaller source window or another performance change.
- [x] Reduce the fixed Classic request ceiling to 100, preserving oversize
  halving, smaller user limits, and all recovery/cursor behavior; verify tests
  and repeat live controls.
- [x] Diagnose MindRoom's serial dispatch preparation with targeted timing.
  Preserve durable pending-turn recording and ordering. Fresh roots already
  skip thread-history reads and text debounce; do not delete imaginary work.
- [x] Benchmark any selected change, test its real behavioral boundaries,
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

### Latest unchanged capacity controls

The production 100-event-ceiling Tuwunel control completed 200 exact replies,
the fence across all three principals, subsequent sync progress, both drains,
and clean shutdown without application errors or stalls. It settled 17,061
companion events and acknowledged all 402 outgoing deliveries. Reply median was
74.558 seconds, p95 84.956 seconds, and first-input-to-last-reply 86.113 seconds.
Initial replies spread over 20.530 seconds. The sole failed predicate was full
visible overlap: 43.960 seconds against the unchanged 45-second minimum.
Tuwunel already capped responses at 100, so this run does not isolate an effect
from changing the client's ceiling.

The Synapse 5,000-window control with the same source completed all 200 exact
replies, acknowledged all 402 deliveries, remained responsive to the two-second
health read, and had no application errors or stalls. Reply median was 80.213
seconds, p95 87.112 seconds, and last reply 89.492 seconds after first input;
full overlap passed at 48.164 seconds. It failed the shared 180-second workload
deadline at the general principal's fence. The deadline includes reply waiting,
drain, fence, final audit, and shutdown; it is not 180 seconds reserved for
the fence. The final snapshot had 23,147 settled companion events and one
pending redaction. The other two principals had settled the fence.

The missing fence was durably captured at index 98 of the general principal's
last unprepared 100-event tail. That consumer was still settling events at the
deadline and during quiesce; the retained tail followed its progress in order.
There was no lost fence or exhausted recovery watchdog. This is insufficient
catch-up throughput within the unchanged deadline, not passing acceptance.
Exact projection-wait durations were not logged, so attribution remains bounded
by that evidence. Peak RSS was 608.30 MiB for Tuwunel and 646.20 MiB for Synapse.

### Reject the serial writer; select smaller Nio simplifications

One subsequent full-application diagnostic pair confirmed rejection of the
serial SQLite executor prototype. Both variants completed all 200 replies,
health, fence, sync, drain, and shutdown checks, but failed overlap. Serial
dispatch time increased from 20.198 to 25.066 seconds (24.1%); median reply time
increased from 74.968 to 79.635 seconds (6.2%), and p95 from 85.174 to 92.396.
Keep the existing companion writer. The original diagnostic parser rejected
two additional setup flushes in the candidate; a separate read-only audit
selected the exact 200 workload roots from each database and verified one
completed flush per root. These are diagnostic results, not production
qualification, and no acceptance predicate changed.

Select two smaller Nio changes described in the architecture amendment:

1. Return and reuse the blocking Frame's already authenticated decoded value.
   Three recovery pairs improved a median 1.55%; ordinary results were noise.
   This removes eight production lines without adding state.
2. Read delivery state once inside each existing claim/acknowledgement
   transaction and remove the second snapshot comparison. Three pairs improved
   ordinary delivery by a median 4.41% and recovery by 3.66%. Idle and retry
   operations retain their exact persisted graph and revision, with no DML or
   transition hooks; measured added cost was approximately zero to two
   microseconds per call. Claim and acknowledgement remain separate commits.

Review and remove only tests of coherent private writer injection between the
old read/write scopes. Preserve actual corruption, ownership, FIFO, byte/count
limits, rollback, and crash/replay coverage. Run the full suite and hooks, then
repeat both unchanged real capacity controls with frozen source and idle CPUs.
Do not add local percentage gains or promise an application result from them.
Publish the companion against the final pushed Nio revision from an isolated
checkout, preserving the user's original dependency edits byte-for-byte.

Both simplifications are implemented and independently reviewed. They remove
28 production lines and 97 test lines; the six removed parametrized cases
were the unsupported writer-entry injections described above. The fresh full
locked suite passes 2,153 tests, skips three, and reports three existing fork
warnings in 206.80 seconds. All 617 focused cases, full mypy across 71 files,
and all repository hooks pass. Production now totals 45,833 Python lines:
795 above the profiling baseline and 389 below the original simplification
baseline. The companion correction adds 24 net production lines. Real capacity
qualification still requires the next unchanged controls.

### Controls at `918a64a` and companion parser selection

Both unchanged controls still failed full qualification after these smaller
simplifications. Tuwunel completed all replies, the exact fence across three
principals, continued sync, drain, and clean shutdown. Its only failure was
43.257 seconds of full overlap. Reply median was 74.180 seconds, p95 85.034,
and last reply 86.181 seconds after first input; initial replies spread over
20.549 seconds. All 17,019 companion rows settled and all outbox debt cleared.
These results do not demonstrate an application latency improvement.

Synapse completed all 200 replies with median 81.664 seconds, p95 89.248,
and last reply at 91.304 seconds. Full overlap passed at 48.897 seconds.
The shared deadline expired during the observer's incremental sync while
polling the fence. The fence had settled for two principals but remained
in the general principal's retained 100-event tail; ten of that principal's
journal rows were pending, with no unacknowledged outbox rows. Final acceptance
for drain, subsequent sync, and shutdown was not reached. Both runs retained
unchanged source and dependency files and had no application error or stall
markers. Existing parser warnings are counted separately below.

The logs exposed a small companion bug with a measurable local cost:
`ingestion_timeline_views` tries every event through `RoomMessage` media parsing,
then parses non-media events again through `Event.parse_event`. This creates
false missing-`msgtype` warnings for valid reactions and redactions and ignores
the outer event type while probing media. The latest Tuwunel run emitted 606
annotation and 603 redaction warnings from this path; prior controls did too.

Use the existing encrypted-media predicate to select specialized media parsing
only for encrypted `m.room.message` attachments. Other sources go straight to
the existing generic event parser. Preserve the current fallback after failed
encrypted-media parsing, event-ID validation, provenance, and projection logic.
This keeps event type authoritative without another cache or delivery path.
Add a behavioral regression for reactions carrying image-shaped extension
fields, plus valid-event diagnostics and encrypted/malformed media boundaries.

Three alternating local pairs over 1,000 retained text edits measured median
parsing time falling from 107.01 to 67.95 milliseconds (36.55%). A mixed batch
with 80% retained edits and reactions, redactions, membership, clear images,
and encrypted images fell from 102.89 to 65.09 milliseconds (36.81%). All
22 ordinary, media, encrypted, and malformed fixture outcomes and parsed fields
matched. A separate warnings-enabled profile removed 120 false warnings and
41,120 formatted diagnostic bytes. These are parser timings, not a claim of
equivalent application improvement. Verify the companion correction against
the final pinned dependency before publishing and repeating capacity controls.

### Final published-source verification

MindRoom pins Nio `09d5a25` and publishes the companion corrections in
`b45966053`. Nio production is unchanged from `918a64a`; subsequent Nio commits
record design and results only. Both published source trees were compared with
the live workload sources; all production Python files matched, and the user's
original dependency edits remained byte-for-byte unchanged.

The companion parser regressions first produced six expected failures: four
reaction/redaction classification cases and two false-warning cases. The
correction preserves encrypted attachment replay and malformed-media rejection.
All 597 focused cases then passed. The full companion suite passes 15,838 tests,
skips 22, and reports 26 warnings; all repository hooks and full-project types
pass. Remote Python tests pass 15,836 cases with 12 skips. All four full/minimal
AMD64/ARM64 image builds and the complete smoke-stack workflow pass. Independent
reviews approved the projection wait and parser changes.

Nio's full local result remains 2,153 passed and three skipped, with zero mypy
errors across 71 files and all hooks passing. Remote tests pass on Python 3.12,
3.13, and 3.14, including coverage. Production remains 45,833 lines: 795 above
the profiling baseline and 389 below the original simplification baseline.
The companion changes add 25 net production lines, for a combined increase of
820 versus the profiling snapshot. The latest two Nio simplifications removed
28 lines; the companion parser correction adds one import and changes one call.

The final uninstrumented controls preserve all original acceptance predicates:

| Measurement | Tuwunel | Synapse, 5,000 server window |
| --- | ---: | ---: |
| Exact completed replies | 200 | 200 |
| Reply median | 74.638 s | 78.715 s |
| Reply p95 | 85.603 s | 85.750 s |
| First input to last reply | 86.313 s | 87.802 s |
| Initial visible reply spread | 20.465 s | 20.020 s |
| Full visible overlap | 43.390 s | 47.580 s |
| Fence principals settled | 3/3 | 3/3 |
| Pending journal / outbox rows | 0 / 0 | 0 / 0 |
| Peak process-tree RSS | 621.29 MiB | 658.78 MiB |

Both controls pass continued sync and clean shutdown with zero application
errors, stalls, degraded reads, or drain-failure markers. All false `msgtype`
warnings are gone; other warning lines remain (408 on Tuwunel, 407 on Synapse).
The scenario intentionally streams 4,800 characters at 80 characters per second;
these reply times include approximately 60 seconds of synthetic generation.

Synapse passes full acceptance, including 905 health checks, 200 peak active
streams, and the unchanged 180-second shared workload deadline. Its measured
phases total 175.946 seconds, including 82.067 seconds waiting for the fence and
5.509 seconds for shutdown. Tuwunel's sole failure remains full overlap below
45 seconds. Preserve that failure; do not label both controls fully passing.
One passing Synapse run does not establish a general latency bound or isolate
the parser correction as its cause. Local parser savings are approximately
38 milliseconds per 1,000 events, not a 37% application speedup.

### Separate scheduling scope

Read-only investigation locates the approximately 20-second startup spread in
MindRoom's shared new-root coalescing gate. Receipt-lane delivery enqueues
without awaiting preparation, and established explicit threads already have
independent drain tasks. New room roots share `(room, None, requester)`; that
gate awaits each batch's context preparation, planning, durable handoff, and
response-lock acquisition before preparing the next batch.

Same-sender speech readiness, attachment/caption grouping, and follow-ups within
one conversation need ordering. Independent closed root batches need not share
all preparation work, but parallelizing their handoff changes cleanup, retry,
shutdown, and durable-source ownership. It is not a lane-key or codec fix.
Substituting a root event ID for the semantic coalescing thread is specifically
unsafe: `turn_controller.py` uses that key to select context and reply targeting,
and room-level media promotion depends on the absent thread ID.

The subsequent authorized scheduling change is specified in MindRoom's
`docs/dev/root-preparation-scheduling.md`. Closed independent roots retain their
semantic key and may prepare concurrently, with eight tasks per gate owner;
explicit threads and single-conversation rooms remain serial. Tests block root
A while root B prepares and cover grouping, source ownership, failure and drain.
This remains separate from Nio's persistence and recovery implementation.

A frozen-source Tuwunel comparison found only a modest application benefit:
preparation start spread changed from 21.076 to 19.845 seconds, while median
reply time stayed approximately 74.6 seconds. Both runs completed 200 exact
replies, settled all three fence principals and drained, but failed the unchanged
45-second full-overlap requirement (42.491 and 43.247 seconds). The change fixes
independent-root blocking; it does not establish a general startup-throughput
improvement. The earlier serial dispatch diagnostic attributed 20.337 of 21.314
seconds to pending-turn persistence, including the shared ledger lock and writer
queue. This is prior diagnostic attribution, not a measured breakdown of the
concurrent candidate. Do not add a second writer or delete either pending-turn
write to force the synthetic overlap check to pass.

Interactive key-share restart is handled by the separately specified
[owned continuation change](key-share-restart-continuation.md). Its verification
and final integration results are recorded there and below. The outstanding
overlap result remains a cutover consideration; no release, deployment, or merge
is performed here.

### Completed interactive continuation and root preparation follow-ups

Nio runtime `398dae4` retains interactive approvals and approved handoffs across
restart through the existing SQLite owner and outbound maintenance. Its 22 new
owned cases exercise real crypto, receiving-device decryption, callback/ack
failure, changed trust, missing sessions and commit crashes. The full suite
passes 2,181 tests with three skipped; mypy reports zero errors in 72 files and
all repository hooks pass. Remote Python 3.12/3.13/3.14 and coverage jobs pass.

MindRoom runtime `55c37f9f3` bounds independent closed-root preparation at eight
tasks per gate while retaining semantic keys and lifecycle ownership. Its local
suite passes 15,850 tests with 22 skipped, and all hooks pass. Every installed
Nio source file matches the tested wheel and committed pin. Remote tests pass
(15,848 passed, 12 skipped), as do all four full/minimal AMD64/ARM64 image builds
and the complete smoke stack.

Production Python totals 46,224 Nio lines: this follow-up adds 391 net lines,
including the 340-line key-share carrier. The total is 1,186 lines above the
original profiling revision `1021de4` and two above the `742806f` simplification
baseline. MindRoom's root scheduling change adds 63 net production lines.

Final integrated controls retained the 200-root workload, 180-second shared
deadline, 45-second overlap requirement and two-second health read. All three
completed 200 exact replies with verified runtime source and no application
errors, degraded reads or event-loop stalls. These times include approximately
60 seconds of synthetic generation.

| Control | Reply median / p95 | Full overlap | Fence principals | Application pending / outbox |
| --- | ---: | ---: | ---: | ---: |
| Tuwunel | 74.607 / 83.888 s | 44.250 s | 3/3 | 0 / 0 |
| Synapse, 5,000-event cap | 80.235 / 86.141 s | 47.714 s | 2/3 | 1 / 0 |
| Same-source Synapse repeat | 80.058 / 87.582 s | 48.262 s | 1/3 | 0 / 0 |

Tuwunel passed fence, drain, continued sync and clean shutdown; its sole failure
was overlap below 45 seconds. Both Synapse runs failed fence qualification. The
repeat traceback locates the timeout in the shared workload deadline during
fence observation, not the two-second health read. The responder kept advancing
through old streaming edits across the deadline and cleanup, so there is no
evidence of a frozen owner. The first failed Synapse run had 7.7% more observer
journal traffic than the earlier passing control; it cannot establish a matched
per-event processing regression.

Zero application queue debt after cleanup does not certify full Nio drain: the
repeat retained authenticated Frames and Work. Preserve both failures and the
outstanding single-process catch-up limit. The scheduling change provides
independent-root isolation, with no large measured reply-median gain; these
results do not qualify the full 200-conversation capacity target. No deadline,
durability or ownership guarantee was relaxed to improve the result.

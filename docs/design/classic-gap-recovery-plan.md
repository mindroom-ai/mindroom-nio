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

- [x] Bound owned Classic windows to 500, preserving smaller user limits.
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
- [ ] Measure and implement one immutable decoded Frame entry, after the
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

The final Python 3.14.7 suite passed 2,147 tests, skipped three, and reported
three existing fork-with-threads deprecation warnings in 213.06 seconds.
Full mypy reported zero issues in 71 source files. All repository hooks passed,
including explicit checks of the new, not-yet-tracked source and tests.

Companion validation against this local Nio source passed all 694 selected
admission, journal ingress, bot readiness, client, and desktop tests. Its six
changed-file hooks passed, including full source/test typing and module-boundary
checks. The original checkout's dependency edits were preserved.

Production Python totals are 45,841 lines in 71 files: 803 net lines above
`1021de4`, and 381 below the original simplification baseline `742806f`.
The new continuation and journal integration account for the increase; the
three performance simplifications remove 57 production lines. This is a real
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
the fence had not reached any of the three syncing principals by the deadline.
These numbers establish reply delivery, not passing capacity acceptance.

Removing restart delay was therefore necessary but insufficient. Re-examining
the recovery architecture identifies repeated frame decoding during per-event
delivery as the next measured target. Keep every recovered revision in order;
do not skip edits, weaken the fence, enlarge the deadline, or add a second
admission path to make this workload pass. Synapse remains unrun.

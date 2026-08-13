# Durable ingestion lean replacement implementation plan

> Execute this plan in one PR. Do not pause for routine approval checkpoints. Stop only for a real safety, correctness, or scope blocker.

**Design:** `docs/superpowers/specs/2026-08-13-durable-ingestion-lean-replacement-design.md`

**Repos:** `/work/dev/mindroom-nio`, `/work/dev/mindroom`

**Starting code boundary:** Classic-qualified nio `b313ae284be5dcc7797a7f7de83c70ad80520beb`, MindRoom `925df7f3cbcc39d4904140efd9d16b93c77238ea`

**One-PR branch:** `docs/durable-ingestion-rewrite-plan`

## Invariants

- Preserve the current upstream-style sync methods, responses, callbacks, `AsyncClient.rooms`, and desktop behavior.
- Return ingestion schema to v1 and exactly five durable tables.
- Classic and Sliding share Frame normalization, reducer, materializer, Work, batch, and ack.
- Never use canary-specific production tables, scope, control room, `probe`, occurrence ledger, rotation ledger, or materializer.
- Response and cursor commit together.
- MindRoom application effects, semantic event identity, batch receipt, and frontier commit together.
- Legacy fork recovery stays until bot and desktop activation observations pass; then it is deleted in this PR.
- Preserve upstream `secure_delete = "fast"` and its regression from `origin/main`.
- Final net budgets versus `origin/main@6ed2b98`: runtime ≤ +4,500; tests ≤ +15,000; docs/scripts ≤ +1,000.

## Task 1: Freeze the lean authority and prune the canary architecture

**Files**

- Add this plan and its design.
- Revert, as normal commits and in reverse order, all commits after `b32ebf8` that implement or specify the diagnostic Sliding canary.
- Remove superseded historical plans/specs and benchmark-only scripts/fixtures after preserving any still-needed acceptance statements here.

**Required commit history**

Do not reset, rebase, amend, or force-push. Create ordinary revert commits for:

```text
56f5d0b fix: tighten schema-v2 diagnostic ownership
774b97b feat: add schema-v2 diagnostic topology
a6ed0c7 fix: validate sliding room ids strictly
8e1e6e1 feat: persist sliding list evidence
450387f docs: design durable sliding ingestion canary
```

Then integrate `origin/main` normally so `secure_delete = "fast"` and its test are present.

**Verification**

```bash
test "$(git diff --name-only --cached | wc -l)" -eq 0
git diff --check
rg -n 'SCHEMA_VERSION = 1' src/nio/store/sync_journal_schema.py
! rg -n 'DiagnosticIngestionScope|NioIngestListObservation|NioIngestEventReceipt|NioIngestEventOccurrence|NioIngestSourceRotation' src tests
! test -e src/nio/ingest/diagnostic.py
! test -e src/nio/store/_sync_journal_diagnostic_rows.py
rg -n '"secure_delete": "fast"' src/nio/store/database.py
```

Run the existing schema-v1/open/reopen, Classic materialization, and Classic compatibility slices. Record exact commands and results; do not claim full green from a subset.

**Commit:** `refactor: remove diagnostic canary architecture`

## Task 2: Prune documentation, benchmarks, and redundant tests

Delete branch-only artifacts that are not product verification:

- superseded implementation plans/specifications whose controlling requirements are preserved by the new design;
- in-repo staging benchmark runner, corpus harness, and generated benchmark-only fixtures;
- tests that assert only internal helper topology or duplicate a stronger behavior/crash matrix.

Keep:

- the Classic canary evidence and exact materialized evidence file;
- one concise design, one executable plan, and one final evidence report;
- behavior-parity fixtures, crash boundaries, public API/callback contracts, and external canary entrypoints.

Before deleting any test group, map every user-visible behavior it covers to a retained or replacement node. Add the replacement first if coverage would otherwise disappear.

Measure and record runtime/test/docs deltas against `origin/main`. Task 2 must reduce all three categories and must not introduce production code.

**Commit:** `test: consolidate durable ingestion assurance`

## Task 3: Implement the minimal Sliding source reset

Use test-driven development.

**Production files**

- `src/nio/store/_sync_journal_preflight.py`
- `src/nio/store/_sync_journal.py`
- `src/nio/ingest/coordinator.py`
- `src/nio/ingest/sliding.py` only if request construction needs a minimal cursor reload hook

**Test files**

- `tests/ingest/source_journal_test.py`
- `tests/ingest/coordinator_test.py`
- `tests/ingest/crash_recovery_test.py`

First write causal failures for:

1. Existing Sliding reopen atomically rotates writer/source epoch, creates a fresh connection UUID, sets request ID to zero, and clears `pos`.
2. It preserves to-device `since`, range/page/request configuration, Frames, aggregates, Work, outstanding batch, and ack frontier.
3. Every injected statement failure leaves all-old state; success leaves all-new state.
4. Classic reopen remains byte-for-byte behavior-compatible.
5. Exact current positioned `M_UNKNOWN_POS` performs the same fenced transition.
6. Stale, positionless, malformed, repeated-cold, or Classic errors are terminal and mutate nothing.
7. Old Frames drain before new HTTP; coordinator reloads Source state rather than editing a cursor in memory.

Implement one private journal transition using only existing Meta and Source rows. Do not add a table, receipt, diagnostic scope, or public option.

Run focused RED/GREEN, the full source-journal/coordinator/crash files, Ruff, type checks, Black, and `git diff --check`.

**Commit:** `fix: reset durable sliding source atomically`

## Task 4: Prove one shared reducer and materializer

**Production files**

- `src/nio/ingest/reducer.py`
- `src/nio/store/_sync_journal.py`
- `src/nio/store/_sync_journal_plan.py`
- transport adapters only where normalization lacks parity data

**Tests**

- `tests/ingest/reducer_test.py`
- `tests/ingest/materializer_test.py`
- shared Classic/Sliding response fixtures

Build a differential behavior matrix covering at least:

- empty sync;
- room hydration and ordinary timeline events;
- invite, join, knock, leave, and membership transitions;
- encryption state and encrypted events;
- to-device, device-list, fallback-key, account-data, presence, and ephemeral traffic;
- limited history, gaps, eviction, and re-entry;
- malformed/conflicting records and repeated BLOCKED behavior;
- callbacks and response-state projection.

For equivalent normalized input, both transports must produce the same record fates and durable graph. A transport adapter may normalize wire differences; it may not choose a different fate engine.

Remove any dedicated diagnostic entrypoint or transport-specific event whitelist that survives Task 1. Preserve every record-fate invariant: Work, aggregate/loss state, or retained Frame—never silent discard.

Run both full adapter suites plus the shared reducer/materializer suite and statics.

**Commit:** `refactor: share durable materialization across transports`

## Task 5: Add MindRoom semantic event idempotency and transport selection

Work in `/work/dev/mindroom` with test-driven development.

**Production files**

- `src/mindroom/event_journal/journal.py`
- `src/mindroom/matrix/durable_ingestion.py`
- the smallest existing model/view module needed to bind stable Matrix event identity

**Tests**

- `tests/test_durable_ingestion_admission.py`
- `tests/test_event_journal_crash_matrix.py`
- configuration/runner tests that already cover Matrix mode selection

Write causal failures for:

1. Exact batch sequence/digest replay remains `DUPLICATE`.
2. The same `(principal/account, room_id, event_id)` and canonical digest in a later Frame/batch advances the new batch receipt/frontier but creates no second application dispatch.
3. The same semantic key with a different digest rolls back the event, projection, receipt, and frontier transaction.
4. Concurrent attempts cannot dispatch twice.
5. Classic behavior is unchanged.
6. Existing `matrix_sync.mode` selects Classic or Sliding for the durable runner without a control room, `probe`, or one-message production scope.

Reuse the existing event journal and admission transaction. Do not add an event-identity database to nio or a canary-only receipt path.

Run focused RED/GREEN, full journal/admission/crash suites, statics, and diff checks.

**Commit:** `fix: deduplicate durable matrix events semantically`

## Task 6: Verify public API and callback compatibility

Create a retained compatibility matrix for:

- `sync()`, `sync_forever()`, `sliding_sync()`, and `sliding_sync_forever()` signatures and return behavior;
- event, response, ephemeral, and to-device callback registration, ordering, exceptions, and re-entry;
- `AsyncClient.rooms` and store projection timing;
- desktop's unchanged `sync_forever()` plus to-device callback path;
- ordinary sends and error responses.

Run the same fixtures with the durable opt-in off and on. Differences require either a bug fix or an explicit design amendment; do not bless them as internal changes.

Add counters around fork-only recovery entrypoints for observation. Counters must be operational telemetry only—no new persistence schema or user-visible semantics.

**Commit:** `test: bind durable sync compatibility`

## Task 7: Run cross-database crash and parity verification

Use real nio and MindRoom components. Cover one event through:

```text
HTTP response -> Frame -> shared materializer -> Work -> batch
-> MindRoom event/projection/receipt/frontier -> nio ack
```

Inject crashes before and after response staging, materialization, batch freeze, MindRoom admission, and nio ack. Include Sliding reopen and exact `M_UNKNOWN_POS` after admission-before-ack.

Assertions:

- response/cursor atomicity;
- deterministic batch replay;
- exactly one application dispatch;
- conflicting identity/digest is an integrity failure;
- no Work or Frame loss;
- frontier advances only with its owning transaction;
- public callbacks remain outside database transactions.

Run complete affected nio and MindRoom suites, Ruff, type checks, Black, compilation, diff checks, and suppression inventories.

No production line may exist solely to expose canary evidence.

## Task 8: Run external Classic and Sliding canaries

Build exact wheels from the candidate commits in a fresh isolated environment. Use an explicitly pinned Synapse and ordinary production configuration. The operator may observe request/response sequence and existing Source/Frame/Work/batch/event-journal/frontier/application outcomes; it may not require product diagnostic tables, a control room, or a `probe` witness.

Classic must remain green on schema v1. If any post-prune schema change appears, stop and rerun Classic before using the earlier PASS.

Sliding must demonstrate:

- normal sync and one visible application result;
- crash after MindRoom admission and before nio ack;
- restart with a fresh connection-scoped position;
- replay without a second application effect;
- exact current-position unknown-pos reset;
- two bounded idle observations without lost Work or duplicate dispatch.

Materialize a concise allowlisted evidence record and independently review it. A canary PASS authorizes activation observation, not release or legacy deletion.

## Task 9: Activate, observe, and only then delete fork recovery

Activation remains inside this PR and behind the existing opt-in.

1. Enable the MindRoom bot cohort.
2. Observe application duplicates/loss, backlog, resets, batch replay, callbacks, and fork-recovery counters for the agreed window.
3. Enable the desktop cohort through its unchanged `sync_forever()` and to-device callbacks.
4. Observe the same signals and prove zero fork-recovery calls.

Only after both observations pass, remove:

- `src/nio/client/sync_recovery.py`;
- `src/nio/client/sync_reset_fence.py`;
- `src/nio/client/sync_response_ordering.py`;
- `src/nio/client/sliding_membership.py`;
- `src/nio/recovery_abandonment.py`;
- `src/nio/sliding_sync_tokens.py` where no longer required;
- recovery/checkpoint code in `async_client.py`, `database.py`, `responses.py`, `base_client.py`, and `store/models.py`;
- implementation-detail tests for those internals.

Retain thin public sync/callback adapters and port every still-relevant parity test before deletion. Verify desktop and bot again after deletion.

**Commit:** `refactor: remove superseded sync recovery machinery`

## Task 10: Final consolidation and one-PR handoff

Run:

- the complete nio test suite;
- the complete affected MindRoom test suite;
- Classic and Sliding compatibility matrices;
- cross-database crash matrix;
- desktop smoke/soak;
- Ruff, type checking, Black, compilation, diff checks, and suppression scans;
- independent code/evidence review.

Measure net logical lines against `origin/main@6ed2b98`:

```text
runtime src      <= +4,500
tests            <= +15,000
docs and scripts <= +1,000
```

If any limit is exceeded, simplify or delete before handoff. Do not move the baseline or limit.

The final PR report must distinguish:

- upstream public compatibility retained;
- fork recovery implementation deleted;
- schema v1 and five-table topology;
- Classic and Sliding parity evidence;
- bot and desktop observation windows;
- exact commits, wheels, tests, canary evidence, and budgets;
- remaining limitations and rollback procedure.

Push only ordinary commits to the existing feature branch. No amend, rebase, force push, split PR, merge, release, or cutover claim is implied by completing this plan.

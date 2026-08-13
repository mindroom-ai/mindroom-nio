# Durable ingestion lean replacement implementation plan

> Execute this plan as one indivisible review decision. nio and MindRoom are separate Git repositories, so each has one linked PR; do not split either repository into phase PRs. Do not pause for routine approval checkpoints. Stop only for a real safety, correctness, or scope blocker.

**Design:** `docs/superpowers/specs/2026-08-13-durable-ingestion-lean-replacement-design.md`

**Repos:** `/work/dev/mindroom-nio`, `/work/dev/mindroom`

**Starting code boundary:** Classic-qualified nio `b313ae284be5dcc7797a7f7de83c70ad80520beb`; post-prune MindRoom `0acaea2baf05a4c41cce7497cfcdf4880afc6d04`

**Linked branches:** nio `docs/durable-ingestion-rewrite-plan`; MindRoom `wip/matrix-journal-ingress-cutover`

## Invariants

- Preserve the current upstream-style sync methods, responses, callbacks, `AsyncClient.rooms`, and desktop behavior.
- Return ingestion schema to v1 and exactly five durable tables.
- Classic and Sliding share Frame normalization, reducer, materializer, Work, batch, and ack.
- Never use canary-specific production tables, scope, control room, `probe`, occurrence ledger, rotation ledger, or materializer.
- Response and cursor commit together.
- MindRoom application effects, existing event-journal identity, batch receipt, and frontier commit together.
- Desktop remains on upstream-style `sync_forever()`; it is not connected to the durable batch engine.
- Legacy fork recovery stays while the public sync internals are restored upstream-style; deletion waits for both MindRoom and desktop zero-use observations, then desktop is regression-tested again.
- Preserve upstream `secure_delete = "fast"` and its regression from `origin/main`.
- Final nio net budgets versus `origin/main@6ed2b98`: runtime ≤ +4,500; tests ≤ +15,000; docs/scripts ≤ +1,000.
- Final MindRoom net budgets versus post-prune `0acaea2ba`: runtime ≤ +350; tests ≤ +1,000; the candidate is also net smaller than `925df7f3c`.

## Task 1: Freeze the lean authority and prune the canary architecture

**Files**

- Add this plan and its design.
- Revert, as normal commits and in reverse order, all commits after `b32ebf8` that implement or specify the diagnostic Sliding canary.
- In MindRoom, ordinarily revert `925df7f3c feat: run exact-wheel durable ingestion canary`; it contains latch/trace environment plumbing, exact-agent/one-room production constraints, and its canary-only tests. Retain the admission/conversion core at `0acaea2ba`.
- Remove nio's remaining schema-v1 canary branch: `_diagnostic_admits`, `materialize_oldest_diagnostic_frame`, the diagnostic journal-port method, and the `room_id` argument/state in `open_ingestion` and `IngestionSession`. The coordinator must call `materialize_oldest_frame()` and hydrate each candidate using `candidate.continuity.room_id`.
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

In MindRoom, do not reset, amend, rebase, or force-push. Revert `925df7f3c` as an ordinary commit and verify that the resulting production/test tree matches `0acaea2ba` before later lean changes.

**Verification**

```bash
test "$(git diff --name-only --cached | wc -l)" -eq 0
git diff --check
rg -n 'SCHEMA_VERSION = 1' src/nio/store/sync_journal_schema.py
! rg -n 'DiagnosticIngestionScope|NioIngestListObservation|NioIngestEventReceipt|NioIngestEventOccurrence|NioIngestSourceRotation' src tests
! test -e src/nio/ingest/diagnostic.py
! test -e src/nio/store/_sync_journal_diagnostic_rows.py
! rg -n '_diagnostic_admits|materialize_oldest_diagnostic_frame|diagnostic_room_id' src tests
rg -n '"secure_delete": "fast"' src/nio/store/database.py
git -C /work/dev/mindroom diff --exit-code 0acaea2ba HEAD -- \
  src/mindroom/bot.py src/mindroom/matrix/durable_ingestion.py \
  tests/test_bot_sync_lifecycle.py tests/test_durable_ingestion_admission.py
```

Run the existing schema-v1/open/reopen, Classic materialization, and Classic compatibility slices. Record exact commands and results; do not claim full green from a subset.

**Commits:** nio `refactor: remove diagnostic canary architecture`; MindRoom ordinary revert of `925df7f3c`

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

Measure and record runtime/test/docs deltas against pinned `6ed2b9817d2bc9de30dc72942f9cb867d829283b`. Task 2 must reduce all three categories and must not introduce production code.

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

Task 1 must already have removed every dedicated diagnostic entrypoint. Preserve every record-fate invariant: Work, aggregate/loss state, or retained Frame—never silent discard.

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
2. The same existing `(principal_id, event_id)` in a later Frame/batch, with matching retained room/thread/kind/sender/origin-timestamp headers, advances the new batch receipt/frontier but creates no second application dispatch.
3. The same event ID with a conflicting retained header rolls back the event, projection, receipt, and frontier transaction.
4. Concurrent attempts cannot dispatch twice.
5. Classic behavior is unchanged.
6. Existing `matrix_sync.mode` selects Classic or Sliding for the durable runner without a control room, `probe`, or one-message production scope.

Reuse the existing `journal_events` unique identity, immutable clear columns, ingestion-receipt foreign key, and admission transaction. Do not add a table, migration, event-identity database to nio, retain raw settled payload for a second digest, or add a canary-only receipt path.

Run focused RED/GREEN, full journal/admission/crash suites, statics, and diff checks.

**Commit:** `fix: deduplicate durable matrix events semantically`

## Task 6: Restore the upstream public sync path and verify compatibility

Before observation, remove every call from `sync()`, `sync_forever()`, and response handling into fork-only recovery/checkpoint machinery while leaving those modules present for the zero-use gate. Restore the smallest upstream matrix-nio implementation of request, response application, room projection, token persistence, and callback dispatch. Do not route desktop through the durable batch engine.

Use test-driven development against behavior, not deleted helper topology. The retained upstream public contract—not fork-specific recovered/unrecovered response fields or checkpoint outcomes—is authoritative.

Create a retained compatibility matrix for:

- `sync()`, `sync_forever()`, `sliding_sync()`, and `sliding_sync_forever()` signatures and return behavior;
- event, response, ephemeral, and to-device callback registration, ordering, exceptions, and re-entry;
- `AsyncClient.rooms` and store projection timing;
- desktop's unchanged `sync_forever()` plus to-device callback path;
- ordinary sends and error responses.

For the MindRoom runner, run the same behavior fixtures with its durable opt-in off and on. For desktop, compare the retained upstream-style public path before and after fork-recovery deletion; desktop never uses the durable opt-in. Differences require either a bug fix or an explicit design amendment; do not bless them as internal changes.

For observation, run the candidate under external Python coverage and require zero executed lines in the named fork-recovery modules and the recovery/checkpoint regions of `async_client.py` and `database.py`. Do not add counters, persistence, or evidence hooks to production.

**Commit:** `test: bind durable sync compatibility`

## Task 7: Run cross-database crash and parity verification

Use real nio and MindRoom components. Cover one event through:

```text
HTTP response -> Frame -> shared materializer -> Work -> batch
-> MindRoom event/projection/receipt/frontier -> nio ack
```

Inject deterministic crashes before and after response staging, materialization, batch freeze, MindRoom admission, and nio ack in this test harness. Include Sliding reopen and exact `M_UNKNOWN_POS` after admission-before-ack. This is the exact boundary proof; the live canary does not duplicate the hook.

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
- an ordinary process kill/restart while a durable batch is outstanding; the exact admission-before-ack boundary is already proved by Task 7 and no production latch is reintroduced;
- restart with a fresh connection-scoped position;
- replay without a second application effect;
- exact current-position unknown-pos reset;
- two bounded idle observations without lost Work or duplicate dispatch.

Materialize a concise allowlisted evidence record and independently review it. A canary PASS authorizes activation observation, not release or legacy deletion.

## Task 9: Activate, observe, and only then delete fork recovery

Activation remains inside the linked change set and behind the existing MindRoom opt-in. The following thresholds are fixed:

1. Enable the MindRoom bot cohort.
2. Observe for at least 24 continuous hours, 1,000 successful source polls, and three clean process restarts. Require zero duplicate visible effects, zero unexplained event loss, no permanently growing Frame/Work backlog, and external coverage showing zero fork-recovery execution.
3. Run desktop on the restored upstream-style `sync_forever()` for at least two hours and 100 successful responses, including one process restart and one real to-device callback. Require no callback/order/authentication failure and external coverage showing zero fork-recovery execution.
4. Preserve exact run start/end times, candidate hashes, counts, coverage data, and failure totals in the final report.

Only after both observations pass, remove:

- `src/nio/client/sync_recovery.py`;
- `src/nio/client/sync_reset_fence.py`;
- `src/nio/client/sync_response_ordering.py`;
- `src/nio/client/sliding_membership.py`;
- `src/nio/recovery_abandonment.py`;
- `src/nio/sliding_sync_tokens.py` where no longer required;
- recovery/checkpoint code in `async_client.py`, `database.py`, `responses.py`, `base_client.py`, and `store/models.py`;
- implementation-detail tests for those internals.

Retain the upstream public sync/callback implementation and port every still-relevant parity test before deletion. Verify desktop and bot again after deletion; desktop never changes ingestion engine.

**Commit:** `refactor: remove superseded sync recovery machinery`

## Task 10: Final consolidation and linked-PR handoff

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

Also measure MindRoom against `0acaea2baf05a4c41cce7497cfcdf4880afc6d04`: runtime ≤ +350 and tests ≤ +1,000, and require it to be net smaller than `925df7f3cbcc39d4904140efd9d16b93c77238ea`.

Use this fail-closed gate from clean tracked worktrees; unrelated untracked user files are reported but are not part of either diff:

```bash
set -euo pipefail
test "$(git rev-parse 6ed2b9817d2bc9de30dc72942f9cb867d829283b^{commit})" = \
  6ed2b9817d2bc9de30dc72942f9cb867d829283b
test -z "$(git status --porcelain --untracked-files=no)"
net_lines() {
  git diff --numstat 6ed2b9817d2bc9de30dc72942f9cb867d829283b -- "$@" |
    awk '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { n += $1 - $2 } END { print n + 0 }'
}
test "$(net_lines src)" -le 4500
test "$(net_lines tests)" -le 15000
test "$(net_lines docs scripts)" -le 1000

cd /work/dev/mindroom
test "$(git rev-parse 0acaea2baf05a4c41cce7497cfcdf4880afc6d04^{commit})" = \
  0acaea2baf05a4c41cce7497cfcdf4880afc6d04
test -z "$(git status --porcelain --untracked-files=no)"
mr_net() {
  git diff --numstat 0acaea2baf05a4c41cce7497cfcdf4880afc6d04 -- "$1" |
    awk '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { n += $1 - $2 } END { print n + 0 }'
}
test "$(mr_net src)" -le 350
test "$(mr_net tests)" -le 1000
test "$(git diff --numstat 925df7f3cbcc39d4904140efd9d16b93c77238ea -- src tests |
  awk '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { n += $1 - $2 } END { print n + 0 }')" -lt 0
```

The final PR report must distinguish:

- upstream public compatibility retained;
- fork recovery implementation deleted;
- schema v1 and five-table topology;
- Classic and Sliding parity evidence;
- bot and desktop observation windows;
- exact commits, wheels, tests, canary evidence, and budgets;
- remaining limitations and rollback procedure.

Push only ordinary commits to the existing feature branches. Create one linked PR per repository and present them as one accept/reject decision; do not split either into phase PRs. No amend, rebase, force push, merge, release, or cutover claim is implied by completing this plan.

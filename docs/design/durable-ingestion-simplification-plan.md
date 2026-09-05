# Durable Ingestion Simplification Implementation Plan

This completed plan records the first simplification checkpoint. Subsequent
[cutover hardening](durable-ingestion-cutover-hardening-plan.md) and
[type correctness and Work reuse](type-correctness-and-work-reuse-plan.md)
record later changes and current verification. The 144-error result below is
historical; the follow-up requires zero errors and enforces that in CI.

> **For agentic workers:** Use baspowers:subagent-driven-development task by task.
> Do not restore excluded guarantees during review. Read the contract first.

**Goal:** Simplify PR #55 production code while implementing its revised contract.
**Architecture:** One owned prepared runtime; transient typing/presence are
handled outside durable Work. Network/storage codecs validate at their boundary,
then immutable internal values flow through shared lifecycle and settlement code.
**Tech stack:** Python >=3.12, asyncio, SQLite/Peewee, pytest, uv, pre-commit.
**Spec:** [Durable ingestion contract](durable-ingestion-contract.md)

## Global constraints

- Preserve every required guarantee and deliberate limit in the contract.
- No Claude consultation, new dependencies, public configuration, schema table,
  deployed-store destruction, unrelated refactor, or production-code test shim.
- Use uv run for Python and tests. Do not commit docs/baspowers files.
- Tests need behavioral assertions and meaningful failure modes; no removed-symbol
  existence tests. The existing passing suite is the refactoring baseline.
- Work on refactor/simplify-durable-ingestion, based on 742806f.
- Keep changes local until verification and review finish; never amend commits.

## Task 1: Remove the unused plain runtime

Files: src/nio/ingest/coordinator.py, reducer.py, __init__.py;
src/nio/store/_sync_journal.py, _sync_journal_plan.py, sync_journal.py;
affected ingestion tests and exports.

- [x] Inventory callers and classify tests as shared behavior, owned behavior,
  or discarded plain-engine behavior. Capture this inventory in the work ledger.
- [x] Preserve owned factory, batch, settlement, source scheduling, and cleanup
  interfaces, including `_open_owned_ingestion`, `_OwnedIngestionSession`,
  `_FrameCompletion`, `_MarkedStoreRequiresSqlite`, `_settle_batch`, and the
  current `next_batch` and settlement signatures used by MindRoom. Exercise
  owned materialization, including frames with crypto.
- [x] Remove plan_frame_materialization, reduce_staged_frame, their exclusive
  carriers/helpers, journal's plain materialization entrypoints, and public
  open_ingestion/IngestionSession exports. Merge shared runner methods into the
  owned session rather than keeping a second instantiable runtime.
- [x] Remove bootstrap's plain-session/store-first paths where only the discarded
  API used them. Preserve owned construction, adoption, close, and transfer.
- [x] Port useful shared behavior coverage to prepared/owned fixtures; remove only
  tests specific to discarded behavior. Do not copy the old engine into tests.
- [x] Run collection, owned/crash/public compatibility suites, then commit.
  Expected: no production references to removed runtime; remaining engine works.

## Task 2: Make typing and presence transient

Files: src/nio/client/base_client.py; src/nio/ingest/coordinator.py, source.py,
classic.py, sliding.py, reducer.py; associated preparation/source/settlement tests.

- [x] First add failing cases: a valid durable message mixed with malformed
  typing/presence still produces and settles the message; valid transient updates
  update existing projections without creating Work; fresh callbacks failing or
  timing out do not prevent later durable processing; restart skips transient
  callbacks. Include Classic and Sliding and preserve read-receipt coverage.
- [x] Filter typing/presence out of preparation and prepared-source reconciliation.
  Keep raw accepted response capture and strict durable-section validation.
- [x] Handle fresh transient observations after source response/cursor commit.
  Parse and update known live projections, invoke callbacks outside transactions
  within the contract's cooperative budget, and isolate transient failures.
- [x] Delete now-unused transient Work reconstruction and validation branches.
  No compatibility machinery for intermediate unmerged Work formats.
- [x] Adapt existing parity tests to compare required durable behavior and assert
  documented transient exclusions. Keep ordinary public callback behavior.
- [x] Run source, preparation, settlement, crash, and public compatibility suites;
  commit after the new tests pass.

## Task 3: Consolidate validation and ownership

Files: src/nio/store/_sync_journal_rows.py, _sync_journal_plan.py,
_sync_journal_preflight.py, _ingestion_store_owner.py, sync_journal.py,
database.py; codec/store-owner/crash tests.

- [x] Introduce one work decoder returning validated immutable record, metadata,
  and canonical bytes. Preserve checksums, canonical shape, identities, and
  cross-row checks at the existing disk boundary.
- [x] Remove duplicate parsing through AuthenticatedWork construction and metadata
  extraction; adapt constructor/codec tests to the actual supported boundary.
- [x] Remove repeated ordinary-store authentication within the same unchanged
  BEGIN IMMEDIATE snapshot. Keep sidecar race checks and post-conversion checks.
- [x] Consolidate lifecycle state left after Task 1 and perform one lease identity
  check per actual SQLite execute, fetch, or iteration call, including every
  `fetchone` or `next`; remove duplicate wrapper checks without removing per-row
  checks.
- [x] Preserve closed/revoked/forked/cross-thread rejection and safe cleanup.
  Keep production imports usable without fcntl; unsupported durable filesystem
  ownership must fail clearly at use, without breaking ordinary client import.
- [x] Run corruption/adoption/ownership/crypto regression suites, then commit.

## Task 4: Simplify remaining settlement and verify

Files: coordinator.py, base_client.py and only directly related codec helpers/tests.

- [x] Consolidate repeated durable record parsing, room projection selection, and
  callback routing into small shared helpers where they remove real duplication.
  Preserve decrypted event reconstruction, verification annotations, no second
  crypto execution, outside-transaction callbacks, and ack-after-admission.
- [x] Keep class-specific validation where it establishes a distinct supported
  boundary. Do not turn all event handling into a generic framework.
- [x] Run the complete suite with uv run --locked pytest --benchmark-disable.
- [x] Run repository pre-commit hooks and applicable type checks.
- [x] Dispatch independent review of the full diff against the contract; fix valid
  findings and rerun affected checks. Review must distinguish excluded guarantees
  from regressions to required ones.
- [x] Update this tracked checklist and add measured source deltas and verification
  results. Commit only intended files. Report final branch/commit and any limits.

## Measured changes and verification

These are measurements at completion of this simplification pass. See the
[follow-up execution record](type-correctness-and-work-reuse-plan.md#execution-record)
for subsequent typing and delivery measurements. The later
[capacity execution record](classic-gap-recovery-plan.md#measured-results)
tracks resumable Classic recovery, current source totals, and real-server gates.

Raw Python source totals are 46,222 lines at `742806f` and 44,788 after
simplification. The change removes 1,434 net production lines; the earlier
2,000–3,000 estimate was optimistic. Shared runner/planner helpers remain needed
by the owned engine, and fresh transient handling replaces some removed durable
code. Further reductions must preserve the contract, not meet a line quota.

| Comparison | Production added/deleted | Production net | Tests added/deleted | Tests net |
| --- | --- | --- | --- | --- |
| Against `742806f` | +411 / -1,845 | -1,434 | +1,027 / -16,044 | -15,017 |
| Against PR base `5b6de3b` | +19,779 / -6,481 | +13,298 | +42,640 / -0 | +42,640 |

Counts use raw `src/**/*.py` lines and `git diff --numstat` for Python files;
tests are counted separately. Discarded plain-engine tests and tests requiring
unsupported mutation of private crypto objects account for the reduced suite.
Settlement reconstruction, real crypto crashes, persisted outbound decoding,
consumer admission, and ordinary client behavior remain covered.

These actual measurements supersede the August fixed size budgets. Large
behavioral tests are accepted for this change; their size creates no test-shrink
or historical-document deletion task.

The complete Python 3.12.13 and 3.13.14 suites each passed 2,012 tests, with 3
skipped and 3 existing warnings, in 210.59 and 209.29 seconds respectively. The
complete Python 3.14.7 suite passed 2,012 tests, with 3 skipped and 5 existing
warnings, in 184.69 seconds. All three runs exercised the same implementation
source immediately before this documentation-only update. The focused
settlement/materialization/preparation suite passed 485 tests. All six repository
pre-commit hooks passed.

The configured mypy command, with `src` on its module path and incremental
caching disabled, reports 144 errors in 23 files versus 150 at the original
baseline and 149 before settlement simplification. Task 4 adds no diagnostics
and removes five old room-variable assignment errors. Across the branch, one
missing-jsonschema-stubs diagnostic was added at a new import site and seven
original diagnostics disappeared.

All four task reviews and the whole-branch implementation review passed with no
Critical or Important findings. Scoped documentation review is complete; its
final wording clarification makes explicit that transient fanout uses the current
task. The companion MindRoom integration's former base conflict is resolved;
see its [current status](type-correctness-and-delivery-cost.md#companion-integration-status).
Before deployment or cutover, run its integration suite against an artifact
built from the accepted Nio revision; those integration steps remain outside
this local change.

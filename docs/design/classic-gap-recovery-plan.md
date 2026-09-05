# Classic gap recovery implementation plan

> For agentic workers: use executing-plans for the coupled recovery changes;
> independent performance and companion validation can use focused agents.

**Goal:** Recover skipped Classic requests and reduce measured ingestion waste.

**Architecture:** Capture bounded history before the source cursor commit;
persist the evidence in the existing Frame; prepare and deliver it through the
existing owner and journal. Reduce oversized requests at the same cursor.

**Tech stack:** Python 3.12+, asyncio, SQLite, existing Matrix HTTP transport.

**Spec:** `docs/design/classic-gap-recovery-and-capacity.md`.

## Global constraints

- Preserve crypto/store ownership, authenticated persisted data, ordering,
  admission identities, and prepared-output bounds.
- Standard-library JSON; no runtime dependencies or new recovery scheduler.
- Cold history stays non-actionable. New recovery scope is Classic sync.
- Preserve the user's uncommitted companion dependency changes.
- Use persistent evidence directories, explicit staging, and fresh commits.

## Task 1: Retain bounded history and recover the missing interval

Files: `src/nio/ingest/recovery.py`, `ports.py`, `source.py`, `coordinator.py`,
`reducer.py`, `src/nio/client/base_client.py`, and
`src/nio/store/_sync_journal_rows.py`, `_sync_journal.py`.
Tests: new `tests/ingest/owned_recovery_test.py`, existing source/journal suites.

- [ ] Add an owned-session regression with a trusted baseline, a limited fresh
  tail, and a missing middle page. Assert all requests appear in order and
  recovered events have `RECOVERED` provenance. Observe the failure first.

```python
assert [record.event_id for record in timeline] == ["$missed", "$fresh"]
assert timeline[0].provenance is TimelineEventProvenance.RECOVERED
assert session._journal.load_source().cursor_json == b'{"next_batch":"s2"}'
```

- [ ] Add optional canonical recovery evidence to StagedSourceResponse and its
  authenticated Frame envelope. Reconstruct the recovered normalized view at
  the journal boundary; keep the original sync body and digest unchanged.
- [ ] Fetch bounded forward pages before staging; verify token progress and
  completion. Feed recovered events through existing preparation/settlement.
- [ ] Cover multi-page and empty advancing pages, retained-page restart,
  duplicates, cold history, state/membership ordering, and incomplete recovery.
  Assert failures preserve the source cursor and no partial callbacks run.
- [ ] Run focused source, reducer, preparation, journal, and recovery tests;
  self-review source/consumer ownership and commit the complete change.

## Task 2: Keep oversized responses recoverable

Files: `src/nio/ingest/recovery.py`, `coordinator.py`.
Tests: `tests/ingest/owned_recovery_test.py`, `coordinator_test.py`.

- [ ] Reproduce an oversized response followed by a smaller valid response.
  Assert the second request retains `since` and other filter selections.

```python
assert first_query["since"] == second_query["since"] == "s1"
assert second_filter["room"]["timeline"]["limit"] < first_filter["room"]["timeline"]["limit"]
```

- [ ] Bound owned Classic windows to 500; halve on oversized successes down to
  one. Cover irreducible oversize, canonical expansion, cancellation, and
  history recovery after narrowing. Run focused tests and commit.

## Task 3: Remove measured redundant source validation

Files: `src/nio/ingest/ports.py`, `source.py`,
`src/nio/store/_sync_journal.py`, `tests/ingest/classic_source_test.py`.

- [x] Measure alternating real-SQLite workloads before implementation. Chosen
  change improves the two tested shapes by 2.64% and 1.85% median.
- [ ] Delete `_revalidated_staged_source_response` and its two internal calls.
  Keep network/disk constructors and identity checks. Remove only the test of
  the excluded frozen-object escape hatch; retain actual corruption tests.
- [ ] Run Classic/Sliding/source-journal tests, formatting, and type checks.
  Review the final diff and commit without adding cache machinery.

## Task 4: Companion acceptance and real capacity verification

Files: companion `src/mindroom/matrix/durable_ingestion.py` and
`tests/test_durable_ingestion_admission.py`; this plan and the contract.

- [ ] Accept recovered lifecycle provenance, replace its obsolete rejection
  case with positive coverage, and preserve LIVE-only reply grant semantics.
- [ ] Run focused companion admission tests against local Nio source.
- [ ] Run uninstrumented 200-conversation Tuwunel and Synapse 5,000-window
  controls. Require exact replies, zero duplicates, drained queues, and
  continued post-load sync. Investigate any failure at its actual boundary.
- [ ] Run full Nio pytest, ruff, black, pre-commit, and zero-error mypy checks.
- [ ] Self-review architecture and changed code; record actual measurements,
  limits, and source deltas here. Commit, push authorized branches, and verify
  remote checks. Report precisely what passed and what remains.

## Measured results

Baseline: Nio `1021de4`, 45,038 production Python lines. Earlier capacity
control passed 200 conversations only on Synapse's 500-event window. New
recovery and final application measurements are pending execution.

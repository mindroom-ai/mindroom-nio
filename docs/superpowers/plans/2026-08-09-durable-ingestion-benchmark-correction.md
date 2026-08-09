# Durable ingestion benchmark correction implementation plan

**Goal:** Produce truthful, reproducible fixed-base staging measurements while
retaining every approved candidate durability and performance limit.

**Design:** Follow
`docs/superpowers/specs/2026-08-09-durable-ingestion-benchmark-correction-design.md`.
The fixed base supplies a paired native-normalization floor for both transports
and a separate Classic cursor-durability reference. Candidate workers perform
adapter normalization plus durable staging. Fresh subprocesses isolate imports
and RSS.

## Task 1: Freeze canonical fixtures and report schema under RED

- Create the six named canonical response files plus exact 1 MiB fixture.
- Keep `classic_sync.json` unchanged and label it diagnostic-only.
- Add `tests/ingest/staging_benchmark_test.py` assertions for exact hashes,
  canonical round-trip, room/event counts, exact byte size, valid native
  parsing without `ErrorResponse` or bad-event fallback, survival of every
  expected event ID, exact transport-neutral Classic/Sliding steady projection,
  continuation/setup cursor/frame relationships, report schema, CLI
  validation, median/p95 calculation, warmup exclusion, all-30 success, and
  retained worker failures.
- Run the focused RED before adding the harness.

## Task 2: Implement isolated workers minimally

- Create `scripts/benchmark_ingest_staging.py` with orchestrator and internal
  worker modes.
- Support the approved CLI plus explicit repeatable `--setup-fixture` for
  direct runs; corpus metadata supplies setup paths in full runs.
- Implement `legacy_normalization_floor`,
  `classic_legacy_cursor_durability`, explicit Sliding `not_available`, and
  `candidate`.
- Verify import identity, fixed control SHA, no network, same environment,
  fresh process/sample, timing boundary, WAL/RSS/CPU/SQL collection, redacted
  traces, full close/reopen/re-normalization checks, Classic token reopen, and
  JSON schema.
- Run one warmup/two measured samples in tests.

## Task 3: Reduce candidate staging SQL from eight to six under RED

- Add a trace test proving one transaction, at most six significant
  statements, exactly three business DML statements, and identical statement
  count for one versus 1,000 events.
- Save the failing eight-statement evidence.
- Add the private joined owner/source snapshot and reuse decoded values in
  `stage_source_response`; do not alter public load contracts.
- Re-run collision, corruption, crash, owner, and source-journal suites.

## Task 4: Pin diagnostics and full corpus

- Exercise external-lock rollback/retry and full-capacity no-send through the
  Task 7 seams; store their raw diagnostics in the report.
- Create a detached clean worktree at fixed commit
  `980e53b3a3815592c3bd0dab9e41e2b057e50724`.
- Run five warmups and thirty measured samples for every paired case/mode and
  the Classic durability reference. Also run and publish the retained
  `legacy_noncanonical_cold` diagnostic in all three applicable modes; exclude
  only its paired ratio, not its absolute candidate gates.
- Write the ignored raw report to
  `.superpowers/sdd/2026-08-08-durable-matrix-ingestion-rewrite/task-4.5-staging-benchmark.json`.
- Fail, optimize, and rerun if any unchanged numeric gate fails. Never relax a
  limit autonomously.

## Task 5: Verify, review, report, and commit

- Run focused benchmark tests, full ingestion, store+encryption, full repo,
  changed-file hooks/static checks, and production line accounting.
- Obtain independent review of fixture semantics, control truthfulness,
  subprocess/import isolation, SQL/WAL measurements, raw statistics, and size.
- Report hashes, raw artifact path, medians/p95, CPU/RSS/WAL/SQL/write
  amplification/lock/capacity, additions/deletions/net by repository,
  cumulative production net, actual and newly unlocked deletions, low/high
  remaining-work and final-net forecasts, and every estimate miss above 50%
  directly in the task handoff.
- If benchmark/optimization production additions cross 500 lines, publish the
  intermediate size/forecast report before adding more production. Fail before
  commit if cumulative production exceeds +6,240; do not credit conditional
  future deletions.
- Commit harness, fixtures, tests, SQL optimization, and these correction docs;
  do not commit generated raw samples.

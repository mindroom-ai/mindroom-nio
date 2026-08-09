# Durable ingestion benchmark correction

## Status and authority

This amendment corrects only the benchmark-control and fixture semantics in
Task 8 of
`docs/superpowers/plans/2026-08-09-durable-ingestion-task-4.5-simplification.md`.
All numeric candidate gates remain unchanged. It is required because the fixed
control commit contains neither the named `save_sync_recovery` API nor a durable
Sliding `pos` path; reporting either would fabricate a baseline.

## Rejected approaches

1. **Pretend recovery/window-token persistence is a Sliding cursor.** Rejected:
   the fixed base never persists connection-scoped `pos`, and room recovery
   tokens are a different state.
2. **Drop paired performance comparison.** Rejected: absolute SQL/WAL gates do
   not show response-normalization regression.
3. **Use current code as its own control.** Rejected: it moves the fixed base
   and hides regressions.

## Corrected controls

Every measured fixture has a fixed-base `legacy_normalization_floor`:

- Classic: parse the canonical bytes and construct the fixed-base
  `SyncResponse`.
- Sliding: parse the canonical bytes and construct the fixed-base
  `SlidingSyncResponse`.

This is the common paired control for wall time, CPU, and RSS. It is explicitly
a conservative normalization floor, not durable persistence.

Classic additionally has `classic_legacy_cursor_durability`: native
normalization followed by `SqliteStore.save_sync_token(next_batch)` on a fresh
on-disk store with an account seeded outside the timed region. Sliding reports
the corresponding reference as `not_available` with the fixed reason
`fixed_base_has_no_durable_sliding_pos`.

Candidate timing remains response bytes through adapter normalization and
`stage_source_response` commit. Reopen verifies cursor, frame identity, digest,
and the canonical body. The paired gate remains:

```text
candidate p95 wall <= 2.0 * legacy_normalization_floor p95 + 10 ms
candidate median CPU <= 2.0 * legacy_normalization_floor median CPU + 5 ms
candidate peak RSS <= legacy_normalization_floor + 32 MiB
```

Candidate-only gates remain one outer transaction, at most six significant
non-PRAGMA/non-transaction statements, no per-event SQL, WAL no greater than
`2.5 * canonical bytes + 128 KiB`, 100–350 ms lock failure with exact rollback
and successful retry, and the existing full-capacity no-send/RSS gate.

## Fixture and setup semantics

All new fixtures are compact canonical UTF-8 JSON with deterministic IDs and
timestamps, complete event fields, stable SHA-256 values, and no trailing
newline. The existing `classic_sync.json` is retained byte-for-byte as a
separately labeled `legacy_noncanonical_cold` diagnostic with both raw and
normalized hashes; it is not used for the hard paired ratio because the fixed
parser produces bad-event fallbacks from its intentionally incomplete events.

Measured canonical cases are:

- Classic steady: 100 rooms, ten timeline events per room;
- Sliding steady: 100 rooms, ten timeline events per room;
- exact 1,048,576-byte canonical Classic response;
- Classic continuation 1 and 2;
- Sliding continuation 1 and 2.

The worker accepts explicit zero-or-more untimed setup fixtures. Optional
Sliding `txn_id` is omitted so fixtures do not encode a runtime connection
UUID. Every raw and setup file has an exact SHA-256 constant in the benchmark
test and report.

The frozen execution matrix is:

| Case | Ordered untimed setup | Floor setup action | Classic durability setup | Candidate setup | Expected cursor before → after | Candidate frames before → after | Timed state |
|---|---|---|---|---|---|---:|---|
| `legacy_noncanonical_cold` (`classic_sync.json`) | none | none | seed account only | none | cold → fixture `next_batch` | 0 → 1 | cold diagnostic; hard ratio excluded |
| `classic_continuation_1` | none | none | seed account only | none | cold → `classic-pos-1` | 0 → 1 | cold |
| `classic_continuation_2` | `classic_continuation_1.json` | parse setup | parse setup + `save_sync_token("classic-pos-1")` | normalize + stage setup | `classic-pos-1` → `classic-pos-2` | 1 → 2 | steady |
| `classic_steady_100_rooms_1000_events` | `classic_continuation_1.json` | parse setup | parse setup + `save_sync_token("classic-pos-1")` | normalize + stage setup | `classic-pos-1` → `classic-steady-2` | 1 → 2 | steady |
| `canonical_1mib` | none | none | seed account only | none | cold → `classic-1mib-1` | 0 → 1 | cold |
| `sliding_continuation_1` | none | none | not available | none | cold → `sliding-pos-1` | 0 → 1 | cold |
| `sliding_continuation_2` | `sliding_continuation_1.json` | parse setup | not available | normalize + stage setup | `sliding-pos-1` → `sliding-pos-2` | 1 → 2 | steady |
| `sliding_steady_100_rooms_1000_events` | `sliding_continuation_1.json` | parse setup | not available | normalize + stage setup | `sliding-pos-1` → `sliding-steady-2` | 1 → 2 | steady |

The Classic and Sliding 100-room cases use the same room IDs, own-membership
facts, 1,000 event IDs, senders, timestamps, types, and content. A test compares
that transport-neutral projection exactly. Their only differences are the
required transport envelope/cursor fields.

## Candidate SQL correction

The current candidate emits eight significant stage statements. Before any
full benchmark, a RED trace test pins six. The journal then adds one private
joined stage-snapshot query that reads and authenticates `NioIngestMeta` and
`NioIngestSourceState` together. It replaces two meta reads plus the separate
source read while preserving:

1. owner persisted-epoch fence;
2. joined owner/source snapshot;
3. bounded frame identity classification;
4. revision/writer-epoch CAS;
5. source-state UPSERT; and
6. frame INSERT.

Public load APIs remain unchanged. Event count must not change this SQL shape.

## Harness isolation and evidence

The orchestrator runs each sample in a fresh subprocess with the target
worktree's `src` first on `sys.path`, verifies `nio.__file__` and the fixed
control SHA, denies network, and uses the same interpreter, installed Peewee,
SQLite, filesystem, WAL/NORMAL/foreign-key pragmas, and 100 ms busy timeout.
Setup/schema/account/request planning and WAL truncation occur outside timing.

Each raw sample records wall and process CPU nanoseconds, process peak RSS, WAL
bytes while the connection is open, canonical bytes, success/failure, target
import path, structural/redacted SQL trace and count, transaction count, and
reopen verification. For 30 measured samples, median is the arithmetic mean of
sorted samples at zero-based indexes 14 and 15. Percentile p95 is nearest-rank
at `ceil(0.95 * sample_count) - 1` (index 28 for 30). Warmups are retained with
`phase="warmup"` but excluded from all statistics and gates.

Each sample is a fresh process. RSS is the absolute OS-maintained process high
water mark from `resource.getrusage(RUSAGE_SELF).ru_maxrss`, normalized to
bytes; it has no polling interval and is never claimed to reset after setup.
Mode-level peak RSS is the maximum absolute RSS across the 30 measured
subprocesses. The comparison therefore includes imports and untimed setup
conservatively on both sides.

After untimed setup, the worker executes `wal_checkpoint(TRUNCATE)` and records
the baseline `-wal` size. WAL growth is the post-commit/pre-close file size
minus that baseline; mode-level WAL is the maximum across measured samples.
All 30 measured samples in every required mode must succeed. Any failure is
retained verbatim in the report and fails the fixture and overall gate; failed
samples are never dropped to compute passing statistics. No raw expanded
ciphertext is written.

After WAL capture, candidate verification closes and reopens the journal,
loads the exact target frame and cursor, reruns `renormalize_staged_frame`, and
compares the complete normalized `SyncFrame`, frame identity, source digest,
canonical body, and expected before/after cursor/frame counts. The Classic
durability reference closes and reopens its store and requires the exact
`load_sync_token()` value.

The ignored report records environment details, fixture/setup hashes, every
raw sample including failures, per-mode statistics/gates, Classic durability
references, explicit Sliding unavailability, lock/capacity diagnostics, and
overall pass/fail.

The `legacy_noncanonical_cold` case is always executed and published for the
normalization floor, Classic durability reference, and candidate, but its
known parser-semantic mismatch keeps it outside the hard paired ratio. Its
candidate absolute SQL/WAL/reopen gates still apply.

## Size and stop conditions

The committed cumulative production net before this benchmark is +5,507
against the fixed base, leaving 733 lines to the approved +6,240 Task 4.5 high
side. Fixture generation belongs in tests. The production harness plus SQL
optimization must remain within that 733-line envelope; if it does not, stop
and simplify rather than relax the size or numeric gates.

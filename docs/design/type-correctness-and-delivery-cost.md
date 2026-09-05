# Type correctness and measured delivery cost

Accepted direction: 2026-09-05, after the hardening pass was pushed in
`bd320ab` and its CI passed. This follow-up implements the requested zero-error
mypy check and evaluates PR #57 against the current PR #55 production code.
The [durable ingestion contract](durable-ingestion-contract.md) remains binding.

## Type correctness

The canonical check is `MYPYPATH=src uv run --no-sync mypy -p nio
--warn-redundant-casts --no-incremental`. At `bd320ab`, mypy 2.3.0 reports
144 errors in 23 files while checking all 70 source files. Forty-five diagnostics
are in PR-new files; 99 are in pre-existing files, some modified by the PR. This
is provenance, not an allowed baseline. Completion requires zero diagnostics
under that command and an equivalent locked CI check.

Add published jsonschema, aiofiles, and Peewee typing packages to development
dependencies only. An isolated probe with these packages reports 136 errors in
20 files: missing imports disappear while thirteen dependency-facing diagnostics
become visible. Resolve those too. No runtime dependency or backend upgrade is
needed. Provide narrow local declarations for the optional legacy Olm migration
API and omitted queue-store interface. Verify omitted private/native APIs against
installed library code before declaring them. Preserve optional dependency loading
and existing legacy pickle adoption.

Fix annotations where current behavior is correct: JSON/query container shapes,
optional fields allowed by schemas, callback signatures, aliases, native return
values, and type flow after existing validation. Exact disk/network checks remain
exact; do not turn them into permissive subclass checks. Use distinct locals or
small typed values to expose invariants already enforced by the code. Local casts
may express an established invariant, not manufacture one. Intentionally dynamic
JSON and transparent third-party keyword forwarding may retain honest dynamic
types; do not replace precise types with `Any` to hide diagnostics. Do not remove
annotations, exclude source files, ignore whole modules, or suppress errors to
reach zero. This does not introduce a new strictness preset or require annotating
every previously untyped function.

Reproduce possible behavioral defects before changing them. In particular:

- Permission helpers return their declared error result when `whoami` fails;
  room upgrade wraps that failure in `RoomUpgradeError` before dependent writes.
  A v12 upgrade with no usable power-level state fails clearly rather than
  dereferencing `None` or silently inventing authorization state.
- A disk download directed to a directory needs a response filename. If absent,
  raise a clear `ValueError` before opening a destination file; callers may pass
  an explicit file path. Do not invent a filename or overwrite an unrelated file.
- Correct the HTTP/2 termination branch's wrong loop variable. Preserve its
  existing diagnostic-only behavior; no new reset or reconnect policy.
- jsonschema format callbacks accept arbitrary JSON values at their library
  boundary. Apply string-format rules to strings and leave non-string rejection
  to schema type validation, matching jsonschema's format contract.

Room-name sorting iterates known members whose names fall back to user IDs;
its optional lookup annotation does not establish an unnamed-member bug.
`Api.public_rooms` selects only GET or POST and returns in both cases; its
missing-return diagnostic is an exhaustiveness issue. HTTP/2 already passes
header lists to `add_response`; that mismatch is also annotation-only.
Tests must preserve these distinctions. Retain existing behavioral assertions;
add regressions for actual missing behavior, not implementation-shaped checks or
impossible mutations of private state.

## PR #57 assessment

[PR #57](https://github.com/mindroom-ai/mindroom-nio/pull/57), at `da081264`,
groups admission/completion commits in the former recovery engine. Its source
delta is 453 net lines across three files. PR #55 replaces that engine and API,
so the implementation is not directly reusable. Its old workload measurements do
not establish current PR #55 performance.

Current PR #55 still uses one Work record per delivered batch and two journal
transactions per record: claim and acknowledgement. Benchmark the real owned
session with file-backed SQLite WAL/NORMAL, real prepared Work, callback delivery,
and frame retirement. Exclude initial bootstrap, source preparation, network,
and application admission from the timed delivery interval. Assert every event
ID is delivered once in order and the frame retires. These measurements describe
one serial Nio store, not a 200-stream deployment or end-to-end application speed.

On CPython 3.14.7, five samples after warmup at each size gave:

| Messages | Median delivery | Commit count | Instrumented COMMIT time | Instrumented delivery |
| --- | ---: | ---: | ---: | ---: |
| 100 | 131.4 ms | 200 | 15.1 ms | 143.3 ms |
| 1,000 | 1,291.4 ms | 2,000 | 139.7 ms | 1,397.2 ms |

Commit timing wraps actual Peewee `commit()` calls; separate instrumentation
counts SQL and transaction time. COMMIT accounts for about 10% of instrumented
delivery time on this machine. Batching could also save repeated SQL/validation,
but would require group identity, per-record admission outcomes, replay/prefix
rules, and consumer changes. Do not port that design into this pass merely to
reduce commit count. Keep the current claim/acknowledgement and callback boundaries.

A separate profile found five full decodes of each unchanged Work record during
claim, settlement, and acknowledgement. A throwaway single-entry reuse probe kept
per-read column/revision validation and compared the complete raw row plus its
authentication identity before reuse. Five alternating-order paired runs of
1,000 messages gave 1,758.3 ms without reuse and 1,292.5 ms with it, about 26.5%
less time. Each optimized run had 1,000 decodes and 4,000 cache hits. Absolute
timings varied between runs; only the paired comparison supports that estimate.
The probe changed no production code or transaction semantics.

## Bounded Work reuse

Adopt one journal-local cached `AuthenticatedWork`, following the existing
Aggregate reuse policy. Every call first validates the actual row's columns,
including revision bounds. Reuse requires identical complete stored fields,
including payload and digest, plus account/stream/transport identity. A changed
row follows the normal authenticated decode; cache only successful immutable
results. Reads still fetch SQLite state, deleted rows remain absent, and HELD
promotion invalidates reuse by changing stored fields. Clear on close. At most
one Work value is retained; no global cache, effect cache, queue, or new persisted
state is introduced. Callback parsing/projection/admission are not cached.

Verify full-row mutation with unchanged digest/revision, different valid row or
authentication identity, lowered owner revision, HELD promotion, deletion/ack,
close, and reopen. Retain corruption detection and stable batch identities. Then
repeat the same real delivery benchmark against the production implementation,
report source-size cost, and retain only a measured improvement consistent with
the small design.

## Scope and release boundary

No storage format, delivery API, public ingestion configuration, batching mode,
or callback timing change is authorized here. Existing trust-first timing and
all deliberate guarantee limits remain as documented. The separately recorded
interactive key-share continuation gap after restart remains a cutover issue;
this type/performance work does not claim to resolve it. Companion integration,
release communication, and live deployment remain separate.

The work uses the existing isolated branch. Record decisions before implementation,
review each task, run focused tests and final Python 3.12/3.13/3.14 suites, require
zero mypy errors and passing repository hooks, then push the existing PR branch
without force. Production size and maintainability remain explicit review criteria.

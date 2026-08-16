# Durable Ingestion Lean Replacement Implementation Plan

> **For agentic workers:** Continue with `baspowers:executing-plans`, use
> `baspowers:test-driven-development` for every behavior change, and use
> `baspowers:verification-before-completion` before any completion claim.

**Status:** Tasks 1–8 complete and independently accepted. Task 9 has not
started. Task 10A is the documentation consolidation captured by this file;
the remaining Task 10 gates depend on Task 9.

**Goal:** Replace fork-specific sync recovery with one crash-safe,
transport-neutral durable ingestion path while preserving the pre-fork public
matrix-nio behavior used by Desktop.

**Semantic authority:**
`docs/superpowers/specs/2026-08-13-durable-ingestion-lean-replacement-design.md`.
This file is the execution/status authority and restart handoff. Git history is
the provenance for removed implementation diary entries.

**Scope:** one indivisible accept/reject decision represented by one linked PR
in matrix-nio and one in MindRoom.

## Immutable architecture and compatibility boundary

- The public compatibility boundary is matrix-nio commit
  `69aa99c51f354980bf5306a85b4c7b7d71ff3217`.
- Preserve `AsyncClient.sync()`, `sync_forever()`, `stop_sync_forever()`,
  `receive_response()`, `run_response_callbacks()`, response callback
  registration, all six event callback families, callback order/re-entry,
  `AsyncClient.rooms`, ordinary sends, response shapes, sync-token persistence,
  E2EE state, and Desktop/pairing behavior at that boundary.
- Fork-added public Sliding and recovery APIs are not compatibility promises.
  Durable Sliding is a private ingestion transport adapter, not an AsyncClient
  public API.
- Ingestion remains schema version 1 with exactly five tables:
  `NioIngestMeta`, `NioIngestSourceState`, `NioIngestFrame`,
  `NioIngestRoomAggregate`, and `NioIngestWork`. The authenticated nullable
  Frame completion-claim column does not change that topology.
- Classic and Sliding normalize into the same authenticated Frame,
  callback-free preparation, pure reducer, materializer, Work FIFO, batch,
  settlement, acknowledgement, and Frame-completion state machine.
- Response and cursor commit together. The retained Frame owns replay and
  outbound maintenance until all Work, maintenance, completion claim, and
  retirement finish.
- The same exclusive SQLite owner holds ordinary MatrixStore/E2EE state and the
  five ingestion tables. DefaultStore adoption converts trust state to the
  SqliteStore topology in one transaction while preserving legacy sidecars as
  rollback artifacts. A marked reopen authenticates the complete graph before
  rotating writer/source ownership.
- MindRoom owns application/event-journal identity, record receipt, semantic
  dispatch, projection, and frontier in one admission transaction. It passes
  separate `receipt_new` and `semantic_event_new` facts to nio settlement.
- Callbacks, parsing, compatibility projection, and application dispatch never
  run inside either database transaction. Callback failure leaves Work
  replayable; nio acknowledges only after admission and eligible callback
  fanout.
- Every `RecordKind` and `LossRecord` has one explicit fate. Known valid input
  cannot become an unacknowledgeable FIFO head; malformed or conflicting input
  fails closed and stays visible.
- Sliding reopen and exact positioned `M_UNKNOWN_POS` rotate connection/source
  state atomically while preserving durable Frames, Work, frontiers, and the
  non-connection-scoped cursor/configuration.
- MindRoom uses the durable runner as its sole candidate sync engine;
  `matrix_sync.mode` selects only Classic or Sliding transport. Desktop does
  not use the durable batch engine. It stays on the upstream-style public sync
  profile with token persistence, E2EE, callbacks, headers, and fork-only
  limited-timeline recovery disabled.
- No canary-specific production table, scope, control room, `probe`, occurrence
  ledger, rotation ledger, latch, environment switch, YAML field, or evidence
  hook may be introduced. External evidence may observe only ordinary product
  state and process behavior.
- Existing databases are never destructively downgraded and historical
  recovery tables are not dropped in place; restored ordinary store code
  ignores inert historical tables.

The shared lifecycle remains:

```text
HTTP -> Frame/cursor commit -> prepare/materialize -> Work FIFO
     -> MindRoom admission/receipt/frontier -> compatibility settlement -> ack
     -> outbound maintenance -> claim completion -> retire -> next HTTP
```

## Fixed baselines and immutable budgets

The baselines and limits never move:

| Repository | Baseline | Runtime | Tests | Docs/scripts | Additional gate |
| --- | --- | ---: | ---: | ---: | --- |
| matrix-nio | `6ed2b9817d2bc9de30dc72942f9cb867d829283b` | `src <= +4,500` | `tests <= +15,000` | `docs scripts <= +1,000` | preserve upstream compatibility |
| MindRoom | `0acaea2baf05a4c41cce7497cfcdf4880afc6d04` | `src <= +350` | `tests <= +1,000` | — | `src tests` net smaller than `925df7f3cbcc39d4904140efd9d16b93c77238ea` |

Fresh Task 10A entry measurements on 2026-08-16 were:

- matrix-nio entry: runtime `+19,930`, tests `+56,870`, docs/scripts `+5,260`;
  this 346-line consolidation reduces docs/scripts to `+639`;
- MindRoom: runtime `+201`, tests `+2,551`, and `src tests = -95`
  versus canary commit `925df7f3cbcc39d4904140efd9d16b93c77238ea`;
- the six whole legacy matrix-nio modules listed in Task 9 contain 2,433
  physical lines, so their deletion alone cannot satisfy the runtime gate.

The corrected consolidation estimate is binding planning evidence: safely
consolidating the 6,043-line journal plan/rows/values/preflight cluster can save
about 1,000–1,750 lines (about 2,300 only at an aggressive bound), not 5,000.
After known safe journal/coordinator/reducer/base-client consolidation and
observation-gated legacy deletion, projected final matrix-nio runtime remains
roughly `+10,600` to `+12,000`. Therefore the immutable `+4,500` runtime gate is
an honest unresolved blocker. Do not hide it, rebind it, or claim Task 10
complete unless the exact command passes.

## Completed implementation and review boundary

| Task | State | Authoritative final boundary |
| --- | --- | --- |
| 1 — freeze authority/prune canary architecture | Complete | matrix-nio `0b07c510`; reviewed history carried through `febbe9a5` |
| 2 — prune redundant docs/benchmarks/tests | Complete | matrix-nio `fe8a868`, `bdd5d7c`, `5709f34`; reviewed history through `febbe9a5` |
| 3 — minimal Sliding reset | Complete | matrix-nio `febbe9a5e850f7dc97e732819c0ac688c16ccce8` |
| Desktop no-recovery/query-only precursor | Complete | MindRoom `1fd376583`, `a5d3edc33` |
| 4 — shared preparation/reducer/materializer/settlement/completion | Complete | matrix-nio through `ec9fe0da38378e7a7d5d100737787fb7584d8662` |
| 5 — MindRoom receipt, pump, owned session, activation | Complete | matrix-nio `1ea9e4aae108c0f1fd2d1bec1ba81f188af408c4`; MindRoom `47180cc07e7d0177ff4981bd7ccf1f1ec65eb047` |
| 6 — restore public sync compatibility | Complete | matrix-nio `70d21bcb6b08a9528104d16a9c6c4537b4bf1a8a`; MindRoom `253c76245174f106162368993bf1393d452c6698` |
| 7 — cross-database crash/parity matrix | Complete | matrix-nio `6e4f14aa2b438ba431d8f7c7ab2ff176f4e11a94`; MindRoom `2cbeecb6f8e68f380a7fabb3cbe28cd88f3e1a2f` |
| 8 — external Classic/Sliding/crash evidence | Complete | product candidate matrix-nio `af8eab819a501518c860e6dbb8581d411eec15d9`; MindRoom `934bebfbbe1a4b174aef2c075d00e971a29697f6`; evidence commits `3b0b4d6`, `94502ea`, `10dd9f6`, `20a980d`; acceptance `e1a2408` |

Tasks 1–7 passed their scoped suites, static gates, and independent reviews.
Task 7 uses real nio and MindRoom components to cover:

```text
HTTP response -> Frame -> materializer -> Work -> batch
-> MindRoom event/projection/receipt/frontier -> nio ack
```

It proves response/cursor atomicity, deterministic replay, one application
dispatch, conflicting-identity rollback, no Work/Frame loss, frontier ownership,
callbacks outside transactions, Sliding reopen, and positioned
`M_UNKNOWN_POS` after admission-before-ack.

## Task 8 accepted evidence

Persistent bundle:
`~/.codex/worktrees/mindroom-task8-evidence/2026-08-15/`.

Exact candidate and infrastructure:

- matrix-nio commit `af8eab819a501518c860e6dbb8581d411eec15d9`;
- matrix-nio wheel SHA-256
  `cd11da3e96313bcd4e228c76799bf6fc7ebe5489276610ca47ac37bc0a8c1a0e`;
- MindRoom commit `934bebfbbe1a4b174aef2c075d00e971a29697f6`;
- MindRoom wheel SHA-256
  `a0028fc69809c774cf572c6428883b881204838486de11d0b685d5b47eee0eac`;
- Synapse `1.158.0`, image digest
  `sha256:39f47bcea65c544ef3a6d58848ee98e2ea98387e9991df30474d25b54477a0af`.

Accepted result hashes:

| Result | SHA-256 |
| --- | --- |
| `classic.json` | `b81418a02a9c66c109700c9f74b4e5c594f6a24f0eab54a63d6d6c6f43288489` |
| `sliding-restart.json` | `afba053c053e0bc93ab57d6490a3a6951987737d2abfcb9bd97dfbfd57c3a243` |
| `sliding-unknown-pos.json` | `8bce17e59626a603bee77422e794e44229ef58d9149fad8bf19af347654741e0` |
| `outstanding-batch.json` | `7d5db3432e6395edc150fb27e9b065236b117bef23dffb700f06fcc5aa7eb233` |

`ACCEPTED_SHA256SUMS` is the sole accepted-dependency manifest: 13 entries,
SHA-256
`a8881b46ed2169eb537288ee6dfeaf7cf7cecab3062335249cdcd1a40829a9f2`.
The broader provenance-only `SHA256SUMS` is not an acceptance allowlist.

The accepted outstanding-batch run causally binds the exact measured Matrix
event to its authenticated READY Work payload and durable MindRoom receipt.
It proves:

- the Work EventRecord ID and canonical source event ID both equal the measured
  Matrix event;
- Work and receipt record identity are equal;
- the same authenticated descriptor, receipt, event binding, database inode,
  and stopped disposable process group exist before stop, after confirmed
  `SIGSTOP`, immediately before `SIGKILL`, and after kill where applicable;
- the target application journal moves `pending -> settled` while measured
  responses move `0 -> 1` after ordinary restart;
- source epoch and connection identity rotate;
- the post-recovery outstanding descriptor is explicitly null;
- both owned databases remain at `NioIngestFrame=0` and `NioIngestWork=0`
  across the stable drain samples;
- the two independently accepted Sliding idle windows remain bound;
- the focused seven-test operator suite covers the exact measured EventRecord
  binding and a genuine canonical LossRecord authentication/skip path.

Independent re-review reproduced the focused tests/manifests and reported no
Critical, Important, or Minor finding. Task 8 PASS authorizes only the later
activation-observation decision; it is not release, deletion, or cutover.

## Current authorization boundary

No new security-sensitive work is authorized in this continuation. External
deployment, credentials, production process control, and the Task 9 observation
are notes-only until fresh authority is provided. Do not change authentication,
E2EE, secret handling, or deployment security to make a gate easier.

Task 9, observation, legacy deletion, final budgets, PR handoff, merge, release,
and cutover are not complete.

## Task 9: Activate, observe, then delete legacy recovery

Observation must precede deletion. Deployment selects the reviewed candidate
for the existing observation cohort; there is no product opt-in, fallback
switch, canary-agent variable, or new YAML setting.

### MindRoom observation gate

Run one uninterrupted 24-hour interval that includes:

- at least 1,000 successful source polls;
- three recorded clean process restarts;
- zero duplicate visible effects;
- zero unexplained event loss;
- no permanently growing Frame/Work backlog;
- external coverage showing zero execution of fork-recovery code.

### Desktop observation gate

Run the restored upstream-style `sync_forever()` profile for at least two
hours and 100 successful responses, including:

- one process restart;
- one real to-device callback;
- zero callback/order/authentication failure;
- external coverage showing zero execution of fork-recovery code.

### Required observation record

For both consumers preserve exact UTC start/end times, candidate repository
commits, exact wheel hashes, configuration identity, cohort identity, successful
poll/response totals, restart timestamps/counts, callback/result counts,
duplicate/loss totals, Frame/Work backlog samples, coverage file/module totals,
diagnostic/failure totals, and operator/version identity. Keep evidence outside
temporary directories and include its manifest hash in the final report.

If either observation fails, delete nothing. Fix the durable path or restore
Task 6's old-engine imports/calls within the linked branches, rerun every
affected verification gate, and repeat the complete observation interval.
There is no hidden runtime fallback.

### Observation-gated deletion

Only after both observations pass, remove:

- `src/nio/client/sync_recovery.py`;
- `src/nio/client/sync_reset_fence.py`;
- `src/nio/client/sync_response_ordering.py`;
- `src/nio/client/sliding_membership.py`;
- `src/nio/recovery_abandonment.py`;
- `src/nio/sliding_sync_tokens.py` where no longer required;
- recovery/checkpoint regions in `async_client.py`, `database.py`,
  `responses.py`, `base_client.py`, and `store/models.py`;
- implementation-detail tests coupled only to those internals.

Retain the upstream public sync/callback implementation. Before deleting any
test, map it to deleted internals or port its still-relevant parity assertion to
the retained public/durable matrices. Re-run both bot and Desktop verification
after deletion. Use one ordinary commit:
`refactor: remove superseded sync recovery machinery`.

## Task 10: Consolidate, verify, and prepare linked handoff

After Task 9's observation-gated deletion:

1. Consolidate runtime and tests without weakening the exhaustive behavior and
   crash oracle. Prefer safe journal/coordinator/reducer/base-client
   simplification and parameterized behavior matrices over an opaque generic
   persistence rewrite.
2. Run the complete matrix-nio suite and complete affected MindRoom suite.
3. Run Classic and Sliding compatibility matrices, the cross-database crash
   matrix, and post-deletion bot and Desktop smoke/soak checks.
4. Run Ruff, type checking, repository formatters, bytecode compilation,
   `git diff --check`, configured pre-commit hooks, and a suppression scan.
5. Run an independent code/evidence review and resolve every valid blocking
   finding.
6. Run the exact fixed-budget gates below from clean tracked worktrees. If any
   command fails, simplify/delete or report the blocker; never move a baseline
   or limit.

Matrix-nio gate, run from the matrix-nio root:

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
```

MindRoom gate, run from the MindRoom root:

```bash
set -euo pipefail
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

The final report must distinguish public compatibility retained, fork recovery
deleted, schema-v1/five-table topology, Classic/Sliding parity, crash evidence,
both observation windows, exact commits/wheels/evidence hashes, test/static
results, budgets, limitations, and rollback.

## Linked PR handoff

- matrix-nio already has draft PR #55 on
  `docs/durable-ingestion-rewrite-plan`. Do not create a duplicate. Only after
  every gate passes, update it and convert it to a normal non-draft PR.
- MindRoom's earlier PR #1800 is merged and its remote branch is gone. After all
  gates pass, push the ordinary history on
  `wip/matrix-journal-ingress-cutover` and open one new normal non-draft linked
  PR.
- Present the two PRs as one accept/reject decision. Do not split either into
  phase PRs.
- Use ordinary commits only. Do not amend, rebase, force-push, merge, release,
  or claim cutover as part of finishing this plan.
- Preserve unrelated untracked `src/mindroom_nio.egg-info/`; never stage it.

## Rollback and restart rule

- Before observation completion, legacy source remains the rollback material;
  the candidate has no runtime fallback toggle.
- If observation fails, restore Task 6's old-engine connections or fix the
  durable path in the linked branches and repeat the entire relevant interval.
- If deletion or consolidation regresses behavior, revert that ordinary change
  or restore only the mapped compatibility code/tests, then rerun all affected
  suites and observations required by the changed candidate.
- Never infer completion from prose. Confirm Git ancestry, clean tracked state,
  exact hashes, suite results, observation evidence, fixed budgets, and review
  before advancing a gate.

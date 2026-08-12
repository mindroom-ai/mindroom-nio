# Approved Durable Ingestion Scope Correction

> **Status:** APPROVED by the user on 2026-08-10. This document supersedes the
> durable ingestion rewrite plan wherever they conflict.

**Bound implementation:** `3cc3ca7fe80b1339e01377a42e317d65fa644a12`

**Fixed comparison base:** `980e53b`

**Fixed MindRoom comparison base:**
`ae04de40bd20d30741671a19bc97893b3ef052cb`

**Purpose:** Preserve the durable Matrix admission guarantee while removing or
deferring mechanisms that are not causal to it, close the overgrown Task 6
checkpoint, and put a real homeserver-to-MindRoom admission ahead of further
implementation breadth.

## Why a correction is required

The root defect remains real: advancing a Matrix source position must never
destroy the only replayable evidence before MindRoom has durably admitted the
corresponding work. The corrected design has three distinct frontiers:

- the **Matrix source cursor** advances only in the same nio transaction that
  makes the canonical raw Frame durable;
- the **materialization/delivery frontier** advances only while every Frame
  record has an exact durable nio owner; and
- the **acknowledgement/retirement frontier** advances only after MindRoom's
  matching admission transaction commits.

The current branch has established useful local ownership boundaries, but the
original delivery and size controls have not held:

- runtime `src/` is `+7,981/-53`, net `+7,928`, against the fixed base;
- runtime `src/` plus `scripts/` is net `+8,631`;
- Task 6 is exactly `+1,932/-56`, net `+1,876`, runtime lines from planning
  base `160c87d0c4df22655af341e1d757b7d7c3ec2d2a`; its checked-in
  `+1,000/+1,200` review alarms were crossed without an immediate rebaseline;
- the new materializer has no production network/coordinator caller;
- nio has no durable downstream acknowledgement API; and
- MindRoom has no consumer of the new batch contract.

The original `+6,000` architecture stop was later demoted to reporting, so it is
no longer an honest active control. It must be replaced, not silently invoked
as if it constrained the current branch.

## Outcome that remains non-negotiable

For every accepted Matrix response:

1. nio atomically commits the source successor and the canonical raw Frame;
2. every resulting record is retained in nio Work, admitted by MindRoom, or
   represented by an explicit terminal fact;
3. replay-sensitive E2EE mutation and its content-bound receipt commit in the
   same nio transaction before its result becomes deliverable;
4. nio exposes deterministic FIFO work identified by stream, sequence, and
   digest;
5. MindRoom admits the complete unit and its receipt in one transaction; and
6. nio advances its acknowledgement frontier only after that commit.

The cross-database crash outcomes are deliberately small:

- no MindRoom commit: nio redelivers;
- MindRoom committed but nio did not acknowledge: MindRoom deduplicates the
  identical replay;
- MindRoom committed and nio accepted the matching FIFO acknowledgement: nio
  may retire that work idempotently; and
- the same consumer/stream/sequence with different bytes fails before either
  side advances.

## Preserve the landed ownership graph

Do not re-platform the stabilized Frame/Aggregate/Work materializer before it
has a caller. Preserve:

- one lifetime store owner and ordinary SQLite connection;
- source cursor plus canonical raw Frame in one transaction;
- pure Classic and Sliding normalization;
- pure reduction/materialization planning;
- Aggregate-owned room continuity and pending hydration;
- deterministic READY/HELD Work ordering;
- the minimal ordered `LossRecord` concept; and
- crash-atomic Frame/Aggregate/Work transitions.

The implementation may be operationally simpler than main because it has one
ownership chain. It is not required to be smaller than a callback, but every
persistent concept must serve that chain.

## Corrected mechanism decisions

### 1. Task 6 closes at the bound implementation

Task 6 is complete for the currently reachable pre-crypto materializer routes.
The separate fresh-process RSS/time/WAL report is waived: it has no accepted
numeric threshold and duplicates deterministic SQL, envelope, lock, crash, and
reopen gates already in the suite.

Do not add more Task 6 route permutations before the end-to-end canary. Later
recovery/READY routes return only when a live Task 8 result produces their
durable predecessor.

### 2. Internalize global CAS

`stage_source_response()` and `materialize_oldest_frame()` must stop accepting
caller-supplied `expected_revision` and `writer_epoch`.

Retain:

- the lifetime sidecar lock;
- PID, thread, database-inode, and lock-inode ownership checks;
- one internally generated monotonic revision for ordering and diagnostics;
- exact read-to-writer entity snapshots; and
- entity-specific input/intent IDs for rejecting stale async results.

An internal open-generation fence may remain at transaction entry if it proves
a real supervisor-overlap case. It is not a caller capability and does not
replace exact entity predecessor checks.

### 3. Keep content binding only where replay crosses an owner boundary

Do not replace the already-tested live Frame/Work IDs merely to save tens of
lines. Treat them as private implementation details, not a public requirement.

New contracts use natural ordering plus content binding only where needed:

- crypto receipt: canonical crypto-input digest;
- nio delivery: stream plus monotonic sequence plus batch digest;
- MindRoom receipt: consumer generation plus stream plus sequence plus digest;
  and
- a duplicate sequence with a different digest is corruption, never replay.

Do not add another hierarchy of UUIDv5 identities for effects, lanes, batches,
or application records when an existing natural key plus digest is sufficient.

### 4. Use a minimal consumer binding

The two databases need one opaque downstream generation so a replaced MindRoom
journal cannot consume or acknowledge the wrong nio stream. They do not yet
need resumable baseline attachment, reset generations, or a general rebind
protocol.

The first vertical slice persists exactly:

- nio stream ID;
- MindRoom journal/consumer generation;
- next delivery sequence;
- acknowledged sequence/digest; and
- at most one frozen outstanding batch descriptor.

MindRoom first creates or loads an inactive durable journal/consumer generation
and passes it to `open_ingestion_store()`. Store bootstrap binds that generation
in the same transaction that creates the nio stream and source state, then
returns the stream ID. MindRoom binds that exact stream ID to its generation in
one idempotent transaction. A crash between the two transactions leaves an
inactive, retryable partial handshake; neither side may activate source HTTP
until reopening proves the same generation and stream ID in both databases.
There is no unbound active source and no in-place rebind in v1. A mismatch fails
before Matrix HTTP, MindRoom admission, or nio acknowledgement.

### 5. Reconstruct one outstanding batch from READY Work

Do not add Batch and BatchItem payload tables for the first delivery protocol.
Select the deterministic READY Work head and persist one compact outstanding
descriptor containing:

- consumer generation, stream, and delivery sequence;
- frozen batch creation revision;
- first and last included READY ordering keys;
- record count and canonical byte count; and
- canonical batch digest.

New READY Work may append after the frozen last key but cannot change the
outstanding membership. Included Work remains immutable and cannot be deleted
or moved until acknowledgement. Reopening selects exactly the frozen key range,
checks count/bytes, reconstructs the canonical batch using the frozen creation
revision, and verifies the persisted digest. A mismatch fails closed. This is a
BatchRef, not a second durable copy of every payload.

MindRoom admits that immutable batch in one event-journal transaction. nio
retires its Work only after the matching FIFO acknowledgement.

### 6. Preserve replay-triggered decryption replacement without a pending index

HEAD has only the `DECRYPTION_UPDATE` enum placeholder; it has no table,
producer, materializer route, or consumer. Remove the placeholder from the
active v1 wire until a live producer and consumer are designed together.

The unencrypted canary fails closed on any crypto-bearing Frame. Task 7 later
preserves main's actual narrower behavior without adding proactive key-arrival
repair:

- a missing Megolm session atomically produces one durable undecryptable fact
  plus a content-bound crypto receipt before the raw Frame is deleted;
- the receipt stores the stable crypto-input digest and original record ID, but
  no pending ciphertext/index row;
- key arrival alone performs no scan and emits no update;
- if the identical ciphertext is presented again after its key exists, Task 7
  decrypts it through the same receipt and emits exactly one deterministic
  replacement identified by the original record ID plus clear-content digest;
- the replacement preserves the original provenance and membership epoch; and
- duplicate failure, clear, or replacement replays remain idempotent.

MindRoom admits the original failure as a durable fact and later admits the
typed replacement as history repair. It decides actionability from the original
provenance/epoch rather than executing a stale command merely because a key
arrived. Proactive key-triggered repair remains a separate future product
feature and may add a bounded pending index only after a real crypto canary.

### 7. Store canonical plaintext with explicit corruption detection

AES-GCM for every ingestion row is not causal to downstream admission. The
current default empty `pickle_key` provides no meaningful confidentiality
against a code-aware same-user actor, but the existing codec does authenticate
payloads, clear headers, primary keys, table/account/stream context, and row
swaps.

The approved v1 threat model treats same-user malicious database rewriting as
out of scope. Filesystem permissions own confidentiality. Ingestion rows use
canonical plaintext plus explicit accidental-corruption detection:

- store canonical payload bytes directly;
- retain SHA-256 of canonical payload bytes;
- include routing/ownership fields in the canonical payload and compare every
  indexed clear column after decode;
- retain a bounded digest over the complete Frame drain header so corrupt clear
  state cannot hide an unowned Frame; and
- describe this honestly as accidental-corruption detection, not tamper proofing.

Forecast current production reduction: approximately `220..380` lines, with
`80..160` future lines avoided. Secret-backed authenticated ingestion-row
encryption is not part of v1; adding it later requires a separate threat-model,
key-lifecycle, migration, and operations proposal.

## The next milestone is a real vertical slice

The first production milestone deliberately proves the cross-repository
boundary before broad crypto, recovery, membership, or desktop work.

### Supported diagnostic envelope

- one disposable account and existing unencrypted joined room;
- Classic Sync for the first canary; Sliding receives its own live canary before
  release but is not needed to validate the cross-database boundary;
- one plaintext LIVE timeline event;
- no membership transition, recovery gap, to-device input, encrypted timeline,
  device-list change, or pending key;
- any unsupported route leaves the raw Frame durable and fails closed without
  advancing materialization, delivery, or acknowledgement. Its Matrix source
  cursor may already have advanced atomically with that retained Frame.

### Checkpoint order

1. **Scope-reduction checkpoint** (gross production additions `152..284`,
   deletions `406..655`, and a working net-reduction target `250..415` lines;
   the arithmetic outer net-reduction envelope is `122..503`)
   - internalize global CAS;
   - replace ingestion-row AEAD with canonical plaintext and the complete
     corruption-detection contract above;
   - remove dormant pending-decryption wire surface;
   - keep the Frame/Aggregate/Work graph and all relevant crash tests green.

2. **Pre-HTTP binding and crypto fence** (`+90..160` production lines across
   both repositories)
   - add the minimal MindRoom create/load operation for its inactive durable
     journal and consumer generation;
   - require that durable generation at fresh nio store creation and return the
     resulting nio stream ID;
   - bind that stream ID back to the MindRoom generation transactionally and
     make the two-step handshake exactly retryable after either commit;
   - reject an incomplete or mismatched binding while opening, before source
     activation or Matrix HTTP;
   - classify `m.room.encrypted` timeline input as crypto-bearing in addition to
     to-device, device-list, key-count, and fallback-key controls;
   - after staging, apply an explicit diagnostic allowlist before materializing:
     one Classic room, its initial same-membership hydration path, one plaintext
     LIVE timeline record, and no other room/global descriptor; multi-room,
     membership retirement, recovery/gap, loss-producing, global, presence, and
     every other route retain the raw Frame and stop later HTTP;
   - retain the raw Frame and stop source HTTP while any crypto-bearing Frame is
     awaiting Task 7; and
   - prove that the diagnostic path cannot materialize or delete such a Frame.

3. **Production `IngestionSession` checkpoint** (`+180..300` production lines)
   - add the real `open_ingestion()`/`IngestionSession` path under `src/nio`;
   - make it the sole owner of plan-request, HTTP-result staging, oldest-Frame
     materialization, and retry scheduling;
   - drain already-durable work before issuing new HTTP;
   - expose no application callback inside a nio transaction; and
   - test the production entry point rather than a benchmark or lab-only loop.

4. **Hydration-only Task 8 checkpoint** (`+180..300` production lines)
   - consume one canonically verified pending hydration intent;
   - commit the exact matching result or retain it for retry;
   - make a fresh room's existing HELD message eligible without adding recovery,
     membership, or generic effect dispatch.

5. **Minimal Task 10 delivery checkpoint** (`+220..380` production lines)
   - persist the minimal consumer generation;
   - freeze and reconstruct at most one outstanding batch from READY Work using
     the durable boundary described above;
   - expose identical replay after restart;
   - accept only the matching FIFO acknowledgement; and
   - keep Work until that acknowledgement commits.

6. **MindRoom one-record admission checkpoint** (`+180..300` production lines)
   - reuse the durable consumer generation created in checkpoint 2;
   - validate generation, stream, sequence, schema, and digest independently;
   - atomically admit the one LIVE timeline record plus receipt;
   - return `ADMITTED` or `DUPLICATE` only after commit; and
   - acknowledge nio only for those two outcomes.

7. **Exact-wheel lab canary** (`+30..90` production/tooling lines)
   - build and hash the candidate nio wheel;
   - install that exact artifact into a MindRoom test environment;
   - run the MindRoom bot's production sync lifecycle through
     `open_ingestion()`/`IngestionSession`, Frame, Work, batch, MindRoom
     admission, and nio acknowledgement against a disposable homeserver
     account; test-only glue does not satisfy this gate;
   - kill/restart before and after each database boundary;
   - prove one final MindRoom event, no skipped cursor, no duplicate application
     work, and an idle replay; and
   - publish the artifact hash, server version, transport, trace, and outcome.

The scope-reduction checkpoint has its own hard cap of 284 gross production
additions from bound implementation
`3cc3ca7fe80b1339e01377a42e317d65fa644a12`; its deletions do not buy headroom.
The approved scope-reduction commit then becomes the fixed canary-path baseline.
At `+500` gross production additions from that baseline, the production
`IngestionSession` must already run through the furthest implemented real
boundary; otherwise stop for architecture review. At `+1,530` gross additions,
the exact-wheel real canary must pass or implementation stops.

8. **Crypto checkpoint after the canary**
   - extend the pre-canary crypto-bearing classification fence into real crypto
     dispatch;
   - serialize crypto object ownership;
   - commit the minimal content-bound receipt with the E2EE mutation and Frame
     fate in one transaction;
   - validate against a real Olm and Megolm exchange before adding late repair.

## Deferred until after the canary

- general pending-decryption and `DECRYPTION_UPDATE` delivery;
- generic durable effect dispatch;
- recovery-gap breadth not produced by a live hydration result;
- resumable baseline attachment and consumer rebind/reset;
- generalized multi-retirement state machines beyond landed reachable routes;
- desktop compatibility/cutover;
- application room-directory projections beyond the one-record admission need;
- report-only materializer benchmarking without accepted thresholds; and
- new persistent tables whose producer and consumer are not both live-called.

Deferred means absent or fail-closed, not silently ignored.

The diagnostic canary is not release completion or cutover authorization. The
legacy production path remains active until the crypto checkpoint, the approved
missing-key behavior, deployment-required recovery/membership routes, a Sliding
live canary when Sliding is enabled, cross-repository crash tests, and legacy
consumer removal are all complete.

## Honest budgets and stop rules

The old global net ceiling is retired by this approved correction. Total
gross production additions across `mindroom-nio` and MindRoom from their fixed
bases are provisionally forecast at `+12,000..15,000` before legacy deletion.
Production accounting includes Python under `src/` and `scripts/` in both
repositories. HEAD is currently `+8,689` gross additions on that basis in
`mindroom-nio`; MindRoom remains at its fixed base. That implies roughly
`+3,311..6,311` further gross production additions, not another
`+12,000..15,000`. No final net forecast is credited until the replacement
caller is active and the exact legacy deletion inventory is proven.

Active controls become:

- every checkpoint has a checked-in forecast and measured add/delete numstat;
- stop before implementation when a checkpoint forecasts more than 30% over its
  approved range;
- stop immediately when the saved coherent implementation exceeds that range;
- no new table/type/module survives without a live producer and consumer in the
  same or immediately following checkpoint;
- tests and generated fixtures are reported separately from production code;
- no benchmark claim spans more of the pipeline than it actually measures;
- the scope-reduction `+284` cap is enforced from the bound implementation, and
  the `+500` executable-production-boundary and `+1,530` successful-canary gross
  gates are enforced from the approved scope-reduction commit; and
- legacy deletion is not credited until the replacement production caller is
  active and the old consumer is removed.

Forecast for the immediate correction, excluding internal-ID churn:

- current production scope: `152..284` additions and `406..655` deletions for
  CAS internalization, canonical plaintext storage, and dormant wire removal;
  the working net-reduction target is `250..415`, within the arithmetic
  `122..503` outer envelope;
- current tests removed/simplified: `820..1,280` lines;
- future replay-triggered decryption replacement: approximately `+170..320`
  production lines across both repositories, avoiding the larger proactive
  key-arrival scan/index design; and
- pre-HTTP binding/fence `+90..160`, production coordinator `+180..300`,
  hydration `+180..300`, delivery `+220..380`, MindRoom admission `+180..300`,
  and canary tooling `+30..90`, for `+880..1,530` gross canary-path additions.

Internal UUID identity churn is excluded because its likely `20..60` production
line saving does not justify invalidating the landed exact graph tests before a
caller exists.

## Approved decisions

The user's 2026-08-10 approval covers all of the following material scope
decisions:

- retiring the old `+6,000` ceiling in favor of the scope-reduction `+284` cap
  and the `+500`/`+1,530` canary-path gross-addition gates above;
- the provisional total `+12,000..15,000` gross two-repository envelope;
- closing Task 6 at the bound implementation and waiving its report-only
  fresh-process resource harness;
- Classic-first diagnostic validation, with a Sliding live canary still required
  before a Sliding-enabled release;
- no in-place consumer rebind in v1; and
- at most one outstanding batch descriptor in v1.

It also selects canonical plaintext plus accidental-corruption detection for
ingestion rows and replay-triggered decryption replacement without proactive
key-arrival scanning or a pending-decryption table.

## Checkpoint evidence

### Caller CAS internalization

Comparison base: `9d115d043c14560e5e649e31910b171de910f2d2`

The first scope-reduction sub-checkpoint removes caller-supplied revision and
writer-epoch tokens while retaining the internal Meta CAS, read-to-writer
revision/epoch checks, physical owner fences, and exact Frame/Work/Aggregate
snapshots. Source staging now rejects stale work by its exact source predecessor
and accepts a still-current result after an unrelated materialization revision.

The tests-only RED failed exactly five cases: two signature mismatches and three
missing-token calls; fifteen existing ownership/CAS controls remained green.
GREEN verification passed 280 source-journal tests, 290 materializer tests, and
the full repository at 2,011 passed / 3 skipped. Independent RED and GREEN
reviews approved the boundary. Production accounting is `+13/-40`, net `-27`;
tests are `+118/-111`, net `+7`. This consumes 13 of the scope-reduction gross
addition cap; deletions do not buy headroom.

### Canonical plaintext ingestion rows

Comparison base: `b0e560cf0efc2672f12fed77f8e04c87093c6b5b`

The second scope-reduction sub-checkpoint deletes the ingestion-row AES-GCM
codec and stores ordered canonical Source, Frame, Aggregate, and Work envelopes
with exact SHA-256 digests and clear-column equality checks. Frame discovery
uses a complete global `LIMIT 257` header scan and a digest over the complete
drain header. Final stored Frame and Work envelopes retain their exact 24 MiB
and 1 MiB limits, including HELD-to-READY growth. The public `pickle_key`
surface remains for E2EE.

Tests froze the new topology, canonical bytes, per-field corruption and row-swap
failures, encrypted-v1 rejection before mutation, exact-limit and one-byte-over
behavior, capacity accounting, and the existing transaction/crash/race fences.
Independent final review found that an account-filtered Frame scan could hide a
corrupt clear `account_id`; two focused RED cases reproduced that through the
public loader and materializer. The final global bounded scan rejects that row
through `load_frame`, `list_frames`, and materialization without adding a query
or changing the six-statement staging fast path.

Final verification passed 301 source-journal tests, 320 materializer tests, 22
staging-benchmark tests, and the full repository at 2,062 passed / 3 skipped.
Pre-commit, compileall, and `git diff --check` passed. Independent review found
no remaining critical, important, or minor issue.

Production accounting from the comparison base is `+271/-402`, net `-131`.
Cumulative scope-reduction accounting from bound implementation
`3cc3ca7fe80b1339e01377a42e317d65fa644a12` is `+284/-442`, net `-158`: it
exactly meets the hard `+284` addition cap and the arithmetic net-reduction envelope,
but not the `250..415` working net-reduction target. Tests and tooling are
reported separately at `+4,094/-2,060`, net `+2,034`; the forecast test-line
reduction did not materialize because the existing AEAD corruption, race, cap,
and crash matrices were retargeted to the plaintext representation rather than
removed.

### Pre-HTTP binding and diagnostic crypto fence

Comparison snapshots: nio
`6ee8b821cd476f609850952a5b6eb73d7e4d2d1b`; MindRoom
`ae04de40bd20d30741671a19bc97893b3ef052cb`.

MindRoom now owns one inactive per-principal consumer generation and binds the
exact nio stream with a predicate-owned, idempotent transaction. nio persists
that canonical generation with its stream/source bootstrap and rejects a
mismatch before writer-epoch mutation. Exact `m.room.encrypted` timeline input
is crypto-bearing for both transports and provenance positions. A separate
diagnostic materializer admits only the Classic one-room hydration shell or one
plaintext LIVE record; every unsupported route returns stable `BLOCKED` before
planning or writer entry while retaining the raw Frame.

Independent review found two integrity gaps: a zero-room rejection could mask
an unrelated corrupt Aggregate, and independent PostgreSQL principals racing
for one stream could leak a backend uniqueness exception. Focused REDs
reproduced both. The final implementation authenticates the complete Aggregate
inventory before diagnostic classification and translates only an exact
post-rollback unique-stream conflict after confirming the durable owner. A
first-seen JOIN+LIVE acceptance case and status-specific `BLOCKED` invariant
were also frozen.

Final verification passed all 1,072 nio ingestion tests and all 442 affected
MindRoom SQLite/PostgreSQL tests; the latter retained three pre-existing pytest
marking warnings. Black, Ruff, MindRoom `ty`, and `git diff --check` passed.
Scoped re-review found every issue addressed with no new critical or important
breakage.

Production accounting is nio `+105/-9` and MindRoom `+55/-1`, for exactly
`+160/-10` combined. Tests are `+624/-11`. This consumes 160 of the 500-line
executable-production-boundary gate from the approved scope-reduction baseline;
checkpoint 3's `+232..281` forecast reaches `+392..441`, where the real
`IngestionSession` must exist.

### Production `IngestionSession`

Comparison snapshot: `3ba52633f0ad130e74fbab5111f77afbd1bec48a`.

Top-level `nio.open_ingestion()` now validates the complete consumer
generation/stream pair, authenticated client identity, transport, and
single-use bootstrap before any source HTTP. `IngestionSession` owns one raw
public `AsyncClient.send()` attempt, bounded response reads, source retry
sleeps, exact cursor-plus-Frame staging, and diagnostic Frame-first draining.
It invokes no response/event callback and holds no nio transaction across an
await. `BLOCKED`, capacity, retained-IDLE, terminal/reset, malformed, and
oversized fates all stop later source HTTP while preserving their durable
owner.

Read-only audit caught retry/release gaps during response-body streaming and a
malformed reader overrun; focused REDs closed both before review. Independent
review then found three ownership edges: a session could claim a bootstrap
after a MatrixStore, custom headers could override the bearer token, and
concurrent/cancelled close could strand the lifetime lock. The final code
rejects store/session double claims, refuses case-insensitive custom
Authorization before bootstrap consumption, and makes every closer shield and
await one shared cleanup task.

Final verification passed all 1,119 ingestion tests. Black, Ruff, targeted
coordinator mypy, and `git diff --check` passed. Scoped re-review found every
issue addressed and no new critical or important breakage.

Production accounting is `+297/-0`; tests are `+1,282/-0`. Cumulative
canary-path production additions after checkpoints 2 and 3 are `+457`, so the
real executable `IngestionSession` exists before the active `+500` architecture
review gate as required.

### Hydration-only result handling

Reviewed comparison base: `b6b52374d79308597f947b96531e22bd545446d8`.
Approved snapshot: `14345602241973c47d1ff48f4955ca21884e682d`.

The production session now consumes one canonically verified pending JOIN
hydration intent, issues the bounded raw room-state GET without callbacks or a
database transaction across the await, and atomically installs the verified
membership baseline while promoting every matching HELD event in stable order.
Retryable, terminal, cancellation, stale-result, capacity, race, and process-
death fates preserve either the complete old graph or the complete new graph.

The first review rejected a stale-membership exception and tests that did not
discriminate the 64 MiB guard or Aggregate/Work snapshot comparisons. The fix
round added the complete stale-identity matrix, a real 65-row canonical exact/
+1 total boundary, and coherent authenticated Aggregate replacement, Work
insert, and Work mutation races. Temporary removal of each production guard
made its focused test fail before the guard was restored.

Independent verification passed 104 hydration/coordinator tests and all 1,176
ingestion tests. Black, Ruff, targeted mypy, and `git diff --check` passed;
scoped re-review approved every prior finding. Production accounting is
`+292/-11`, bringing cumulative canary-path gross additions through checkpoint
4 to `+749` and leaving `781` lines before the `+1,530` successful-canary gate.

### Typed-Meta one-record delivery

Reviewed comparison base: `e894a8e54311c908577d65671830500e69ac5ccc`.
Approved synthetic candidate: `c0a259d7297a88939948f4a843a43b3cc5117a47`.

nio now publishes at most one authenticated READY Work record through a
canonical `SyncBatch`, durably binds its full READY key and batch digest in six
typed fields on the existing singleton Meta row, replays byte-identically after
restart, and retires the Work only in the matching FIFO acknowledgement
transaction. There is no delivery table, copied payload, JSON/base64 delivery
state, checksum mini-codec, READY-specific query, or `AuthenticatedWork`
extension.

Two post-review amendments are controlling: the redundant delivery-state
checksum was removed under the explicit single-field-corruption threat model,
and each independent typed Meta column/CHECK may occupy one SQL line. The full
READY key, canonical batch SHA-256, strict typed decoder, global bounded Work
inventory, and predicate CAS over every delivery field remain.

Fresh independent verification passed all 1,244 ingestion tests, including
process-death, read-to-writer race, authenticated corruption, hydration-to-
delivery, session-lifetime, and later-READY replay cases. Black, Ruff, and
`git diff --check` passed. The required mypy command retains three documented
findings byte-for-byte present in the comparison base; no false green is
claimed.

Production/tooling accounting is `+374/-37`, exactly the reviewed working
target and four additions below the hard stop. Cumulative canary-path additions
through checkpoint 5 are `749 + 374 = 1,123`; reserving `+300` for MindRoom
admission and `+90` for the lab canary gives `1,513`, leaving 17 lines below the
`+1,530` gate. Full evidence is in
`.superpowers/sdd/2026-08-10-durable-ingestion-scope-correction/task-5-report.md`.

## Task 7 final Option-C local-commit boundary

Task 7 is frozen after exact compatibility 8P, Option-C 10P, Task-0 94P,
current Task-1 31P, all approved Ruff/ty/Black/diff/count/suppression gates,
and independent final code/evidence review. Historical Task-1 32P remains
preserved; the current count is 31 only because the obsolete blocked
`zero-room` case was replaced by the two exact Option-C positive cases.

The reviewed local nio commit is
`b313ae284be5dcc7797a7f7de83c70ad80520beb`, direct parent
`c0a259d7297a88939948f4a843a43b3cc5117a47`, with exact six-path scope and
`+1660/-118`. Its all-path and production-only binary-patch SHA-256 values are
`c9c3b236d941e5a697e4499abcd1dbe775ae646cf69b47343740264a78f8d7b8`
and `243a8a190c9250e0f24b812b7f20b8f72b99f52a64e3ae51694432265540b22b`.
The reviewed local MindRoom commit is
`925df7f3cbcc39d4904140efd9d16b93c77238ea`, direct parent
`0acaea2baf05a4c41cce7497cfcdf4880afc6d04`, with exact four-path scope and
`+2851/-4`. Its all-path and production-only binary-patch SHA-256 values are
`ae05c1a43584fc8cab63f3b2d3dbf4964208dffb22d0aec49f0be7e5c6510714`
and `338071988b3a0d4e706d525bfa38d4fd2174d5a97c1951c3a7784c957c467366`.
Production is exact combined 314 and cumulative 1720, inside the independently
approved 315/1721 hard gates. The production suppression inventory remains
exactly PLC0415 plus PLR0915; Option C added no suppression.

The executable Task-7 brief is the sole source for the next live boundary.
Its historical 779/a2812 reconstruction is non-rerunnable. Before any new
Step-5 lab, the active block must validate and remove only the two exact
orphaned roots `/tmp/nio-task7-deps.sWHOqWfj` and
`/tmp/nio-task7-dev.KE42WJO8`, prove absence, and reconstruct from the final
b313/925 commits with all ten exact blobs, both path/numstat sets, all four
patch digests, exact suppression scopes, clean indexes/locked paths, Python
3.13.14, combined 314/cumulative 1720, and required wheel version
`0.0.dev0+g925df7f3cbcc`. No cleanup, build, service, operator, network, push,
Classic PASS, or Sliding PASS is claimed by this documentation rebind.

## Task 7 Qft ACK-oracle superseding boundary

The final b313/925 boundary subsequently produced exact Qft7s6LI Step-5
wheels nio `8ff088a91a94851427dd0660a81aaaf9af72aeb1085259c7b56e6bd6e13554e5`
(324620 bytes) and MindRoom
`d5359971cab62e2b96d6c9cfea781f9fccfc34225944bc073c282cc099c3b98c`
(5052699 bytes), with Python 3.13.14, nio 0.39.0, MindRoom
`0.0.dev0+g925df7f3cbcc`, lock/sync150/precheck/final provenance, and AST PASS.
The sole Step 6 emitted no evidence/PASS and stopped/cleaned exact
`ack.pre:AssertionError:unclassified`; it was not rerun and no runtime content
was inspected.

The executable brief now makes only the minimal operator-oracle correction.
Non-ACK phases retain exact stable-graph equality. ACK-pre permits only
same-epoch Source request progress `N >= 0` with exact Meta revision progress
`2*N`, while requiring empty Frames, exact old Aggregate/Work/delivery and
MindRoom projections, exact remaining Meta fields, and writer rotation. Fixed
substages/labels and the pure N=0/1/2 plus negative projection self-test expose
no values. No generic classifier or traceback parser is added. Independent
review of the governing freeze is required before one fresh Step 5 and, only
after it passes, exactly one Step 6; no source/test/commit/service/network/
push/Classic/Sliding action is authorized by this entry.

## Task 7 CZX durable-idle oracle superseding boundary

After review of the Qft freeze, fresh CZXma3uj Step 5 passed against b313/925
with nio wheel
`37bd47ff0936c7ce43ebce4f45fb7ae19a9ee4db477045954128f83a76189fad`
(324620 bytes), MindRoom wheel
`d5359971cab62e2b96d6c9cfea781f9fccfc34225944bc073c282cc099c3b98c`
(5052699 bytes), MindRoom `0.0.dev0+g925df7f3cbcc`, Python 3.13.14,
lock/sync closure 150, final closure 152, and AST PASS. The sole roughly
203-second Step 6 progressed beyond ACK, emitted no evidence/PASS, and stopped
exact `operator-stop:durable-idle-1:AssertionError:unclassified`; no rerun or
runtime-content read followed. Cleanup proved the lab absent, unit
not-found/inactive/dead with zero PIDs/list rows and empty control group,
process references zero, and b313/925 HEADs/indexes unchanged.

The executable brief now uses one neutral empty-Classic-progress oracle at
ACK-pre, `idle.post`, and both durable-idle comparisons against `settled`.
It permits only same-epoch nonnegative integer Source request delta `N` plus
exact Meta revision delta `2*N`, while Frames remain empty and Aggregate,
Work, delivery, MindRoom, and all other Meta fields remain exact. Writer
rotation stays separate. Fixed labels/substages expose no values; idle evidence
reports only the integer request delta and `delivery_stable`, not blanket
stability. The renamed pure N=0/1/2 and negative self-test retains its exact
semantics. All other phase equalities remain unchanged. Independent review of
the five governing artifacts authorizes only one fresh Step 5 and, after PASS,
one Step 6; no source/test/commit/service/network/push/Classic/Sliding action
is authorized by this entry.

## Task 7 bLS legacy-application fixed-label boundary

Fresh bLS4F3oJ Step 5 passed b313/925 with nio wheel
`53082a4255d9b4787ddb2c3cba65848934a75e551693c78f8a53f3dc4e805895`/324620,
MindRoom wheel `d5359971cab62e2b96d6c9cfea781f9fccfc34225944bc073c282cc099c3b98c`/5052699,
g925, closure 150→152, both semantic fences bash-n PASS, and AST PASS. The sole
~4m Step 6 passed durable phases/ACK/idle, emitted no evidence/PASS, and stopped
exact `operator-stop:legacy-application:AssertionError:unclassified`; no rerun
or runtime-content read followed. Cleanup was exact green and b313/925
HEADs/indexes remained unchanged.

Fifteen fixed substages, eight allowlisted labels, and split response/model
checks before and after idle change diagnostics only. Static closure forbids
dynamic stages/labels and value emission while retaining exact graph equality,
all product semantics, combined314, and cumulative1720. Independent review of
the five artifacts gates one fresh Step 5 and then exactly one Step 6 only.

## Task 7 dEdr application model-oracle fixture boundary

The reviewed inputs were exact SHA-256 design
`affc08587a71fea0ac27524c40d83545635c751f0f9ff2802b2e0c7004a2904e`,
brief `2afbaeed2cf9c4a0a00af5428c375e305c470b38d875c84511fb0e6a05c3da0f`,
report `bd98f7fa420c8b57065b947fd01f452947aa40ef6346b3be7357b802d7602021`,
progress `fbf188f0b024c1fa8e64b4f7aa750b5dea5e0efe37d2a2df6a2edd88997d1c89`,
and this tracked plan
`b9469e75197f22a386ec23880fc669e158cd252e0fedab802dedf10a72d03389`.
Their prebuild gate and b313/925 reconstruction passed before fresh lab
`/tmp/mindroom-ingestion-canary.dEdrMnMW`. Step 5 produced exact nio wheel
`4350e5e5cd33018ec63045154d5434821d3bb6c92666dad454025c13f7dfd6ff`/324620,
MindRoom wheel `d5359971cab62e2b96d6c9cfea781f9fccfc34225944bc073c282cc099c3b98c`/5052699,
version `0.0.dev0+g925df7f3cbcc`, closure 150→152, and AST PASS. The sole
~4.5m Step 6 passed the settled sole direct-response checks, emitted no
evidence/PASS, and stopped exact `legacy-application.idle-model` /
`application-idle-model`; it was not rerun. Cleanup proved lab absent, unit
gone, PIDs0, refs0, registry0, b313/925 HEADs unchanged, and indexes0.

Static RCA establishes a stale aggregate-counter oracle, not a duplicate turn
or product defect. Default thread mode starts a thread, first automatic
summary threshold defaults to 1, and the post-response background summary
uses the default model. Its expected request increments the shared HTTP
counter after the direct reply is settled while the visible source-response
count stays one. Existing response-runner summary-scheduling and threshold
tests suffice; no new product TDD is required.

The active disposable config alone fixes
`defaults.thread_summary_first_threshold: 100`, preserving thread mode and
every exact-one foreground, fixed-label, receipt/frontier, and graph assertion.
Count 2 is neither accepted nor reset; any fresh vocabulary check returns on
`message_count < 100` before `_generate_summary`. The smallest external source check
binds the sole literal 100 and all three exact-one expressions without
expanding the classifier-secrecy AST. Independent five-hash review gates one
fresh Step 5 and, only after PASS, one Step 6; no source/test/commit/service/
network/push/Classic/Sliding action is otherwise authorized.

## Task 7 3n8rOwtD threshold-100 PASS-observed capture boundary

The verified inputs were exact SHA-256 design
`ba17289baf161848254e83086398d3f15607e7ec7cd470bb28581e4aef0c5755`,
brief `96680ffb43bac83a0a8729c6cfca8e9632ba4e8902bb8a76030a18ab67d3ec6e`,
report `162481604623e5d74790357260c17d59dbb335b4b7677a37d4c1fadf4c1451a5`,
progress `ae0adcf20c993da215239de9d06354ae94cf58f37266a16dd8702047f2e5e053`,
and this tracked plan
`433b6234312501ac10eebf38030e4b7ee8143f4f76dbe87696cb56e42d474f32`.
The corrected b313/925 reconstruction passed at exact combined production 314
and cumulative production 1720 before fresh Step-5 lab
`/tmp/mindroom-ingestion-canary.3n8rOwtD`. Step 5 recorded the Bun unit as
`success` and `collected: true`, exact closure 150 to 152, Python 3.13.14,
installed MindRoom `0.0.dev0+g925df7f3cbcc` and nio `0.39.0`, nio wheel
`0d5dc60408b2f26fa8a00278a6495cf637e372b6645ed9d1a4a6e48fbcbcc3ad`
at 324620 bytes, MindRoom wheel
`d5359971cab62e2b96d6c9cfea781f9fccfc34225944bc073c282cc099c3b98c`
at 5052699 bytes, and exact `TASK7_OPERATOR_LOG_AST_OK` PASS.

The sole Step 6 exited zero and exactly one PASS marker was observed. The run summary,
which is not a substitute for the lost exact JSON or reviewed evidence,
reports final nio
`b313ae284be5dcc7797a7f7de83c70ad80520beb` over base
`c0a259d7297a88939948f4a843a43b3cc5117a47` and final MindRoom
`925df7f3cbcc39d4904140efd9d16b93c77238ea` over base
`0acaea2baf05a4c41cce7497cfcdf4880afc6d04`, Classic transport, the sole
source-to-response path, 50 drained one-time keys, every declared phase,
`application_stable: true`, idle-latch `delivery_stable: true` with
`source_request_delta: 0`, and durable-idle ordinals 1 and 2 with
`delivery_stable: true`, `source_request_delta: 1`, and writer rotation true.
It additionally reports production 314/cumulative 1720, the exact wheels and
installed versions, Synapse `1.148.0rc1` plus its image digest, and cleanup
fields `clients`, `model`, `process_group`, `instance`, and `lab` all removed.
Final content-free checks proved lab absent; unit not-found/inactive/dead with
zero PIDs, empty control group, and zero list rows; process references zero;
registry entries zero; final b313/925 HEADs and both indexes unchanged/clean;
and locked paths clean. No rerun, edit, commit, push, Sliding action, or
forbidden inspection followed.

Terminal truncation discarded the exact captured `TASK7_EVIDENCE_INPUT` JSON,
and no `checkpoint-7-canary.json` exists. It must not be reconstructed from
the summary. The exact verdict is **Classic operator PASS observed /
evidence-materialization STOP**; qualified Classic diagnostic canary PASS
remains pending and is not whole-Task-7 PASS.

The minimal lossless-capture amendment changes no operator, fixture, product,
or assertion semantics. After independent five-hash review, one fresh Step 5
and then exactly one Step 6 may run. Step 6 must be a dedicated direct
execution whose outer `functions.exec` begins with exact pragma
`// @exec: {"yield_time_ms": 10000, "max_output_tokens": 100000}`. The nested
initial `exec_command` and every `write_stdin` poll must each use
`max_output_tokens: 100000`; retain every output chunk and concatenate them
once in arrival order. Require final exit zero, no truncation metadata or
`original_token_count` beyond returned content, and exactly one complete
newline-terminated `TASK7_EVIDENCE_INPUT=` line before `apply_patch`
materializes its verbatim JSON. Missing, partial, duplicate,
non-newline-terminated, or truncated evidence is STOP with no reconstruction
or unchanged-run retry. No capture file is authorized; if either 100000-token
layer is unsupported or direct losslessness cannot be guaranteed before
execution, STOP for separate review rather than capturing stdout or logs.

The sole designated next phase remains the prewritten Step 12 Sliding gate:
production MSC4186, one external event and one durable database, real window
entry/eviction/re-entry with membership proof, fresh connection-scoped `pos`
after restart, exactly-once source/Frame/Work/receipt/frontier correlation,
and duplicate-free idle observations. It remains inactive until exact Classic
evidence materialization and independent review pass, then still requires the
new separate checkbox brief and design review declared by Step 12 before it is
executable. Task 8, cutover, release,
unrelated breadth, push, and full-parity claims remain blocked.

This tracked plan amendment remains an unstaged docs-only worktree change
because this task authorizes no commit. Pushing existing commit b313 would not
include it. Before any push, obtain separate authorization and independent
review for a docs-only nio commit directly atop b313 whose sole tracked diff is
this plan and whose code/test tree is unchanged. Keep b313 as the code/wheel
SHA and record the new child separately as the nio push HEAD; never amend b313
or represent these docs as part of its wheel.

## Task 7 bvTOAy7w qualified Classic diagnostic canary PASS

The independently reviewed recovery inputs were exact SHA-256 design
`c7097f386e0024d42570651fc7052f9efa6b3791eccac04485deca0606a588b9`,
brief `f8e1c20899a3a787ad4936fd290e16e626a8c0d8cf0d9d447b78aadb0b669669`,
report `3822be4897774f824a55c5e8ff3c51460fa1f885e28ffefa9ca3268feeac5e36`,
progress `6a2e7dc7c66413a1ab56c73e5419b5f51619914baba4645dd0d471586bf5853c`,
and this tracked plan
`7a45ed2e302c39ba308d194c159a6a11293727bb302c3e78567a16ffcda57c40`.
Fresh b313/925 reconstruction passed at combined314/cumulative1720.
Separately captured Step-5/post-cleanup executor evidence binds lab
`/tmp/mindroom-ingestion-canary.bvTOAy7w`; it is not inferred from the JSON.
The JSON independently binds successful/collected Bun unit
`mindroom-task7-bun-bvTOAy7w.service` and `cleanup.lab: "removed"`; the final
state gate independently proves that exact lab absent.

Fresh Step 5 produced exact nio wheel
`b21755d7c16b67e12c1419765244080b280caacea21a470196c94492cd760b4b`
/324620 and MindRoom wheel
`d5359971cab62e2b96d6c9cfea781f9fccfc34225944bc073c282cc099c3b98c`
/5052699, closure150→152, Python3.13.14, installed nio0.39.0 and
MindRoom`0.0.dev0+g925df7f3cbcc`, and exact AST marker PASS. Dedicated Step 6
exited0 and the reviewed dual-budget path losslessly captured exactly one
complete newline-terminated marker.

The exact JSON payload is 24894 bytes/SHA-256
`9ea59ceaffbe73b3b0a2eed0a116020dba7ee8b78adfafde3272752222073fdd`.
After independent newline APPROVE, `apply_patch` materialized exactly payload
plus one LF. Ignored evidence file `checkpoint-7-canary.json` is 24895 bytes/
SHA-256
`5a1e2e03778832301c3cf7ede223b1744edb1a656711a79e4778bf8c5f7fb838`,
with outcome PASS and 11 phases.

Completed Step-7/8 audits bind true b313/925 ancestry over c0a/0aca, Classic
source/response, OTK50, all eleven ordered phase graphs through ACK, stable
ten-second application idle, idle-latch delivery stability/delta0, two stable
ten-second durable idles at delta1 with writer rotation, explicit-host Docker,
and Synapse1.148.0rc1/image digest
`sha256:1b32cacbc99b38a83d3370f9843c2211ac3b67eaaaf977ad8de03bb37f2d6cbf`.
Cleanup is exact JSON clients/model closed plus process-group/instance/lab
removed. Independent state is exact lab absent, unit not-found/inactive/dead,
PIDs0, empty group/list0, refs0, registry0, b313/925 HEADs and indexes
unchanged/clean, and locked paths clean.

This is qualified **Classic diagnostic canary PASS**, not whole-Task-7,
Sliding PASS, full parity, cutover, or release. Step 10 final independent
evidence/docs review still blocks staging, commit, and push. This tracked plan
remains unstaged. A later authorized docs-only nio commit must be a separately
reviewed direct child of b313 with no code/test changes; b313 remains the
code/wheel SHA and the child becomes the distinct push HEAD. Sliding remains
blocked and non-executable pending a new separate checkbox brief, design
review, and explicit user approval. No commit, push, or Sliding action is
authorized by this entry.

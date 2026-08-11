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

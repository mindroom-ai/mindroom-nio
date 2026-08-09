# Durable Ingestion Simplification Design

**Date:** 2026-08-09

**Status:** Approved for autonomous execution

**Scope:** Architecture and delivery reset for the durable Matrix ingestion rewrite

## Decision Summary

The current rewrite is a **NO-GO for Task 5** in its present form. The durability goal remains valid, but the implementation crossed the project size gate and put important invariants behind an unnecessarily complex raw-SQL/Peewee capability boundary.

The replacement design makes five decisions:

1. One ordinary Peewee `SqliteDatabase` owns the single journal-file connection. Journal code continues to use raw SQL, but executes it through `database.execute_sql()` inside Peewee-owned transactions.
2. The uncommitted guarded-database experiment is discarded. A small private lifetime/transaction owner retains thread, process, lock, inode, epoch, and close-order checks without pretending to secure every Peewee method.
3. Task 4.5 is reduced from a premature carrier/effect platform to a source-only durable core: immutable transport, stored membership observations, and one canonical staged response that is re-normalized after restart.
4. Recovery, losses, effects, membership delivery, hydration/lifecycle facts, and attachment are implemented beside their first real owner/consumer. Their guarantees are deferred, not removed.
5. The original **+6,000 final production-line net limit is hard**. Actual size, unlocked deletion, remaining forecast, and performance are reported directly to the human approval owner at every task or 500 added production lines, whichever comes first.

The initial activation scope is a bot-only v2 path with a minimal authoritative room directory. Desktop/pairing stays disabled for v2 accounts until its separately approved migration.

## Supersession and Plan Disposition

Upon approval of this written specification, it supersedes the existing plan's Task 4.5 implementation boundary and every Task 5/6 paragraph that assumes Task 4.5 has already persisted room lanes, losses, network effects, attachment state, or generalized carrier CRUD. The old plan remains historical evidence, not a second normative source.

Before production edits, the Task 4.5 simplification implementation plan must name the exact old plan sections it replaces. After that checkpoint, the master plan is rewritten around the delivery decomposition in this specification. No implementation step may select whichever of the old plan or this specification is more convenient.

## Why the Current Boundary Is Wrong

The current branch combines:

- a raw SQLite journal;
- a Peewee E2EE store;
- one physical connection requirement;
- a Python capability layer intended to make the raw connection inaccessible; and
- method-by-method guards around Peewee's public database object.

That guard cannot be complete. Peewee exposes `cursor()`, `commit()`, `rollback()`, internal connection state, and other transaction surfaces that bypass the guarded methods. Hardening every method would recreate a database API around Peewee without improving transaction correctness.

The correctness requirement is narrower: journal receipts and Olm/Megolm mutations must be able to commit in the same SQLite transaction. Peewee already provides this boundary. An outer `database.atomic("IMMEDIATE")` owns the transaction; journal `execute_sql()` and nested Peewee model writes share the same connection, and nested `atomic()` scopes become savepoints.

The database object is therefore an internal implementation detail, not an unforgeable security capability. The v2 public API never exposes it, architecture tests prevent new production reach-through, and the private owner validates every supported operation entrypoint.

## Measured Size Baseline

Production Python only; tests, generated files, locks, and design documents are excluded.

| Item | Measured net |
|---|---:|
| Tasks 1-4 and CI hardening | +5,578 |
| Committed Task 4.5 range | +4,312 |
| Committed branch before the uncommitted writer experiment | about +9,905 vs. `main` |
| Uncommitted guarded-writer experiment | about +364 |

The deletion forecast was also overstated:

- 3,393 production lines are conditional whole-file deletion candidates.
- 5,544 production lines are the broader measured, non-overlapping whole-file plus named-symbol candidate inventory.
- Zero of those lines is safely deletable before its replacement is active and the observation gate passes.

No future forecast may call the 5,544 candidate inventory “firm.” A deletion is credited to the current net only when the replacing task identifies its exact consumers and the final diff actually removes it. A remaining forecast may show named conditional deletions separately, but never blend them into measured actuals.

## Alternatives Considered

### A. Selective Task 4.5 rollback with Peewee ownership — chosen

Retain source-boundary behavior, remove premature reducer/effect persistence, and rebuild only the small staging seam. Route journal SQL through the existing Peewee transaction manager.

This preserves the valuable adapter and continuity work while removing roughly 3,850-3,920 lines of prematurely committed Task 4.5 implementation. It also removes approximately 235-300 lines from the uncommitted guarded-writer experiment relative to hardening that experiment.

### B. Keep Task 4.5 and patch every Peewee escape — rejected

This retains the 425-line guarded writer, still cannot honestly prevent private connection access, and leaves the projected final size well above the hard limit. It adds maintenance surface without adding atomicity.

### C. Port E2EE persistence from Peewee to raw SQL — rejected

This would duplicate at least 678 lines of existing schema and behavior before compatibility and migration work. It has no transaction advantage over approach A and would increase upstream-fork churn and crypto persistence risk.

## Task 4.5 Salvage Boundary

### Retain now

1. **Immutable transport at store creation**
   - Classic and Sliding cursors remain separate continuity domains.
   - Reopening with the wrong transport fails before epoch mutation or HTTP.

2. **Adapter-produced membership observations**
   - Classic and Sliding derive the same immutable observation convention.
   - Downstream code consumes stored observations and never reparses event bytes to rediscover them.

3. **Canonical staged source response behavior**
   - A successful response is canonicalized once.
   - The durable frame contains normalization version `1`, the frozen request, one canonical response body, its source digest, and the staged revision.
   - `SyncFrame` retains only the digest, not a second full response copy.
   - Restart loads the request/body, reruns the frozen transport adapter, and requires the same frame identity, digest, and normalized result.

The canonical successful response is the parsed JSON object re-encoded as UTF-8 with `ensure_ascii=False`, `allow_nan=False`, and separators `(",", ":")`; duplicate keys, non-finite values, invalid UTF-8, excessive nesting, and non-object roots are rejected. `source_sha256` is SHA-256 of those canonical bytes. The normalization version is authenticated and an unknown version fails stopped.

Restart authenticates the frozen request's safety constraints rather than reconstructing it from current configuration. Classic replay retains its canonical cold `full_state=true`, filter/query, cursor, stream, epoch, and request identity. Sliding replay retains its reserved all-room list/range, connection instance, to-device `since`, required E2EE/extensions, cursor, stream, epoch, and request identity. Current runtime drift is allowed only for the already enumerated adapter inputs: Classic filter/timeout configuration and Sliding caller lists/subscriptions/extensions, connection name, and page-size configuration. The durable request remains authoritative; any drift that would weaken its frozen safety overlay is rejected.

The existing staging commit is mechanically coupled to later carrier/effect code, so its behavior is re-ported as a focused frame slice rather than retained wholesale.

### Remove from the immediate foundation

- generalized recovery carrier codecs and lane persistence;
- SystemOrigin hydration/lifecycle expansion before a producer exists;
- lane-based loss ownership and materialization scaffolding;
- generic durable network-effect value and persistence layers;
- membership-operation delivery fencing before the membership owner exists; and
- resumable consumer attachment before provisioning consumes it.

The simplification also removes dormant future-owner scaffolding that predates Task 4.5. The immediate v1 ingestion schema contains only the owner/meta fields needed by this checkpoint, source state, and staged frames alongside the borrowed E2EE tables. Room/lane/ready/batch/receipt/pending-decryption/effect tables, their transition fields, and their CRUD ports return only with the task that has both their producer and consumer. Frozen cross-repository wire values may remain without pre-creating their persistence platform.

The v1 format is unreleased. `SCHEMA_VERSION` remains `1`. The simplified ingestion topology is exactly `NioIngestMeta`, `NioIngestSourceState`, `NioIngestFrame`, and the frame drain index, alongside the separately defined borrowed E2EE tables. A development database made by the abandoned v1 shape raises `FreshIngestionRequired` before DDL or epoch takeover. It is never migrated, partially reused, repaired, silently reset, or modified in place; the operator must select a genuinely fresh path.

The replacement map for removed persistence is exact:

| Removed immediate surface | Reintroduced by |
|---|---|
| `NioIngestRoomState`, `NioIngestRoomLane`, `NioIngestLaneRecord`, `NioIngestReadyRecord` and their codecs/CRUD | Task 6, from Task 5's approved pure transition types |
| `NioIngestBatch`, `NioIngestBatchItem`, acknowledgement frontier CRUD | Task 6 materializer/outbox |
| Recovery/hydration request values and `NioIngestNetworkEffect` | typed intent in Task 5, minimal durable row in Task 6, execution/rescheduling in Task 8 |
| `NioIngestCryptoReceipt`, `NioIngestPendingDecryption`, `NioIngestCryptoEffect` | Task 7 crypto replay-safety checkpoint |
| Membership request/delivery state and resolution APIs | Task 9 membership owner checkpoint |
| Consumer attach status, digest, ordinal, and chunk CRUD | Task 13 provisioning checkpoint |
| System-origin hydration/lifecycle event expansion | the Task 5/8 checkpoint that first produces the corresponding fact |

This is not a statement that those guarantees are optional. The following ownership map is normative.

## Invariant Ownership Map

| Invariant | Producer | Durable owner | Executor/consumer | First crash/restart gate |
|---|---|---|---|---|
| Transport never changes for a store | Task 4.5 source planner | Task 4.5 source state | source adapter | Task 4.5 reopen/wrong-kind gate |
| One canonical successful response survives restart without a parsed duplicate | Task 4.5 source adapter | Task 4.5 staged frame | Task 5 reducer | Task 4.5 stage/reopen/re-normalize gate |
| Membership evidence is not reparsed or inferred from current config | Task 4.5 source adapter | staged canonical response plus normalized frame value | Task 5 reducer | Task 4.5 config-drift replay gate |
| A real recovery gap and every loss have one durable owner | Task 5 pure reducer proposes the transition | Task 6 minimal per-room aggregate/materializer | Task 6 batch builder and Task 8 recovery executor | Task 6 frame-to-room-to-batch ownership kill matrix |
| One room cannot block unrelated rooms | Task 5 pure reducer identifies room-local work | Task 6 per-room aggregate and bounded ready queue | Task 6 scheduler/materializer | Task 6 stuck-room/unrelated-room restart gate |
| Recovery/hydration effects survive restart and are idempotent | Task 5 proposes typed recovery/hydration intent | Task 6 commits the minimal request row with its room transition | Task 8 coordinator executes and reschedules | Task 8 dispatch/result/restart gate |
| Olm/Megolm mutation and replay receipt commit together | Task 7 crypto worker prepares immutable mutations | Task 7 shared Peewee transaction | Task 7 crypto worker receives commit outcome | Task 7 duplicate-ciphertext commit/restart gate |
| Unknown membership HTTP outcome is never automatically replayed | Task 9 membership owner | Task 9 membership operation row | Task 9 coordinator/operator resolution | Task 9 dispatch-before-result kill gate |
| Hydration/lifecycle system facts have stable identities | Task 5/8 producer that first emits each fact | Task 6/8 owned row and outbox record | MindRoom admission | producer-specific commit/restart gate |
| Baseline attachment is bounded and resumable at every cardinality | Task 13 provisioning | Task 13 attach status, digest, and next ordinal | Task 13 attachment loop | Task 13 every-chunk/final-chunk kill gate |

No activation task may proceed while one of its required invariants is merely deferred. The simplified Task 4.5 checkpoint is deliberately not an activatable product.

Task 13 always attaches in hard-bounded, resumable chunks. Cardinality measurements may lower the configured chunk size but never select an unbounded path. Baseline losses are materialized in FIFO order ahead of source-derived records, and Matrix HTTP remains disabled until the final attachment chunk commits.

## Single-Connection Storage Boundary

### Connection ownership

A thin private owner first acquires and validates the lifetime file lock, then creates and opens exactly one ordinary `SqliteDatabase` for the journal file with:

- `thread_safe=False`;
- `autoconnect=False`;
- the configured busy timeout;
- `sqlite3.Row` row access for journal decoders; and
- WAL/foreign-key pragmas applied in the correct transaction-free phase.

The exact database object is passed to the exact `SqliteStore` type. `DefaultStore`, `SqliteMemoryStore`, queue-backed stores, and third-party subclasses remain invalid for v2. Journal and E2EE code never create another connection to that file.

A transient `:memory:` database used only to compile expected schema topology is allowed. It is not a connection to the journal file and must not read or write account data. This avoids ATTACH/DETACH machinery whose only purpose was satisfying an over-literal connection count.

### Bootstrap-only scope

Bootstrap cannot validate a persisted writer epoch before the meta row exists. It therefore has one private, single-use scope:

1. acquire and validate the lifetime lock;
2. open the sole file-backed Peewee connection;
3. inspect the marker and exact topology through that connection only;
4. atomically create the simplified topology or validate an existing exact match;
5. install/take over the writer epoch; and
6. activate normal owner scopes.

No normal journal or E2EE operation is legal before step 6. `database_shape()`, marker checks, pragmas, schema validation, and creation may not open a second file-backed SQLite connection. A test instruments the SQLite/Peewee connection factory and proves bootstrap, reopen, topology validation, and store construction use exactly one connection to the journal path.

Only a nonexistent or zero-length selected fresh path may take the create branch. Any nonempty database lacking the exact simplified v1 marker and topology—including legacy MatrixStore/E2EE tables, an abandoned v1 shape, or a mismatched account/device—raises `FreshIngestionRequired` before DDL, epoch takeover, E2EE construction, or any write. V2 never opens, migrates, repairs, resets, or adds tables to that database. Exact same-owner simplified-v1 reopen is the sole nonempty-file success path.

### Supported operation entrypoints

The private owner provides narrow scopes, not a generic SQL proxy:

- a read scope that validates ownership and performs a read-only writer-epoch fence;
- a journal write scope using `database.atomic("IMMEDIATE")` while preserving the journal's exact revision/epoch CAS;
- an E2EE write scope using the same outer transaction and a writer-epoch fence; and
- the future Task 7 combined crypto commit scope.

At each outer entry it validates:

- opening process;
- opening thread;
- active/revoked/closing state;
- lock file descriptor and path identity;
- database path/device/inode identity; and
- persisted writer epoch.

Owner depth is tracked separately from Peewee's transaction depth. An ambient internal `database.atomic()` is not treated as an owner-authorized transaction.

### Public boundary

The v2 public session, journal port, crypto port, and MindRoom consumer never expose the database, cursor, connection, file lock, or owner token. Internal code reaching into Peewee is unsupported rather than claimed to be cryptographically impossible. A focused architecture test rejects new direct database/connection access outside the storage implementation.

The borrowed `SqliteStore` receives an already-open database and never calls its legacy `_create_database()`, `connect()`, or `close()` path. Every supported E2EE method enters through the private read or write owner scope before binding models or issuing SQL. A borrowed-store initialization or close entrypoint rejects with `LocalProtocolError`; only the bootstrap/session owner may close the shared database. The ordinary Peewee object itself is deliberately not presented as a security boundary. Architecture coverage inventories every decorated E2EE read/write method and rejects production calls to `database`, `cursor`, `connection`, `atomic`, `commit`, or `rollback` outside the explicitly allowlisted owner/journal implementation.

### Close order

Close is idempotent and ordered:

1. revoke the borrowed store/session view;
2. stop new owner operations and drain the current operation;
3. close the one Peewee database connection;
4. release the lifetime file lock last.

If connection close fails, the lock stays held and only the owning bootstrap/session may retry close.

## Authenticated Row Boundary

Encryption remains required for sensitive staged responses, records, snapshots, requests, and crypto results. The simplification does not silently trust ordering or identity columns.

For each encrypted row:

- clear columns are limited to primary keys and fields needed for bounded lookup, ordering, capacity, or scheduling;
- every retained clear semantic field is included in the row's authenticated AAD/header;
- the ciphertext contains only fields not already represented in that authenticated header; and
- the AAD continues to bind schema version, table/domain, account, stream, primary key, and payload digest.

This removes some clear/envelope duplication without making FIFO order, room epoch, source ownership, lane ordinal, or scheduling state unauthenticated.

The immutable request and mutable state of a future network effect remain separate authentication domains. A state update must never rewrite the frozen request.

The threat model is explicit:

- selected-row corruption, ciphertext swaps, metadata mutation, stale revisions, wrong owners, and failed CAS operations fail stopped;
- process crashes and partial transactions recover to the exact old or new state;
- the design does **not** claim to detect malicious whole-row deletion, whole-database rollback, or an index mutation that hides every affected row without an authenticated manifest.

Adding such a manifest requires a separate design and budget approval.

Safe authenticated-header deduplication is expected to save only about 50-200 net production lines. It is correctness cleanup, not the main budget rescue.

## Data Flow

The immediate simplified flow is:

1. A frozen Classic or Sliding request is planned from durable source state.
2. Network I/O occurs outside SQLite transactions.
3. The successful response is canonicalized once and normalized by the matching adapter.
4. One short journal transaction advances source state and inserts the canonical staged response.
5. Restart reloads and re-normalizes the staged response before any downstream mutation.

The immediate journal API is correspondingly narrow: open/create/reopen the owner and source state, atomically stage one source response with its cursor transition, and load/list staged frames for restart. It does not expose a generic transition containing inactive room, lane, loss, batch, receipt, or effect mutations.

Later subprojects extend this flow in ownership order:

1. Task 5 reduces a staged frame into per-room aggregate transitions.
2. Task 6 materializes bounded FIFO outbox batches and retires the frame only after every input has one durable owner.
3. Task 7 prepares crypto work off-transaction, then commits E2EE mutations and replay receipts together on the owner thread.
4. Tasks 8-9 add the coordinator, network effects, lifecycle, and membership commands.
5. MindRoom atomically admits a batch before nio acknowledgement.

No future task may pre-build another task's generic persistence platform without an active producer and consumer in the same checkpoint.

## Activation Scope

### Required before initial bot activation

- the complete frame/reducer/batch/ack fate invariant;
- same-transaction crypto receipts;
- the coordinator and required network/lifecycle commands;
- MindRoom durable batch admission;
- a minimal immutable MindRoom room directory containing the fields required for authorization, delivery/encryption, room administration, and RTC dispatch; and
- fresh-device/store provisioning and crash/canary gates.

V2 does not populate `AsyncClient.rooms`, so Task 12A cannot be deferred wholesale. Cosmetic and tool-presentation fields may be deferred only when their absence produces a typed unavailable/degraded result and cannot affect sends, encryption, access, or authorization.

Before the room-directory schema is designed, MindRoom must check in an inventory of every `client.rooms` read and direct room-cache mutation. For each consumer it records the exact properties used, whether the value affects authorization/encryption/delivery, and the required behavior for `PENDING` and `UNAVAILABLE`. The minimal directory is the union of fields proven necessary by that inventory, not a guessed category list. No v2 consumer may fall back to `AsyncClient.rooms`; an omitted noncritical field returns a typed unavailable/degraded result.

### Explicitly outside initial activation

Task 12B desktop/pairing migration is a separate scope. For a bot-only v2 activation:

- cloud desktop tools, pairing, and response routing are disabled for the v2 account;
- a local desktop may continue on the legacy path only as a distinct Matrix device and store; and
- no code claims the old nio callback path is globally removable while that legacy device remains supported.

Task 12C's direct crypto/session reach-through removal remains required for the bot v2 path because the shared crypto owner cannot coexist with external mutation of its Olm/session state.

## Error and Recovery Semantics

- Ciphertext, digest, AAD, canonicalization, ownership, topology, or CAS failures fail stopped.
- A failed transaction leaves source cursor, frame, reducer state, outbox, receipts, and acknowledgement frontier at the exact prior committed state.
- No integrity error automatically skips a frame, loss, effect, or batch.
- Wrong process, thread, file identity, revoked owner, or closed owner fails before business SQL.
- A stale external writer epoch may execute only its dedicated fence query/CAS before failing.
- HTTP and crypto computation never execute while the journal write transaction is held.
- The simplified Task 4.5 core cannot start Matrix HTTP because it exposes no attachment or coordinator activation path.

## Testing Strategy

Tests follow the owned invariant rather than mirroring every implementation field.

### Simplified Task 4.5 gate

- immutable transport and cursor authority across reopen;
- refusal of every nonempty no-marker, legacy, abandoned-v1, or wrong-owner database before DDL/business SQL;
- exact one-journal-file Peewee connection identity shared by raw journal SQL and `SqliteStore`;
- raw SQL plus Peewee model mutation commit and roll back together;
- process/thread/lock/inode/epoch/revocation/close-order fences;
- canonical Classic and Sliding staging, one canonical response copy, restart re-normalization, and runtime-config drift;
- frame insertion collision and corruption before revision mutation;
- full existing source parity and membership-observation suites.

### Later invariant gates

- Task 5 owns recovery/loss state-machine and per-room isolation tests.
- Task 6 owns fate, ownership-transfer, bounded-batch, and acknowledgement crash tests.
- Task 7 owns duplicate Olm/Megolm replay and atomic E2EE receipt tests.
- Tasks 8-9 own effect idempotency and unknown membership-delivery tests.
- MindRoom owns atomic batch-admission and restart-dispatch tests.

Statement-hook crash tests remain for real atomic ownership transfers. Tests that exist only to prove duplicated metadata copies agree are removed when AAD/header authentication makes the duplicate disappear.

## Performance Contract

The rewrite is purchasing crash safety and replay safety. It is **not assumed to be faster than `main`**.

The reproducible control is `mindroom-nio` commit `980e53b3a3815592c3bd0dab9e41e2b057e50724` on the same host, filesystem, Python environment, and SQLite build as the candidate. Both runs use an on-disk temporary database with WAL, `synchronous=NORMAL`, foreign keys enabled, and the same 100 ms benchmark busy timeout. The harness records CPU model, core count, available memory, filesystem, Python, Peewee, and SQLite versions. It runs five warmups followed by 30 measured iterations and reports median and p95.

The frozen fixture corpus contains:

- the checked-in Classic cold-sync fixture;
- equivalent Classic and Sliding steady-state responses with 100 rooms and 1,000 timeline events total;
- one 1 MiB canonical successful response;
- a two-response continuation sequence for each transport; and
- an already-full staged-frame bound with repeated scheduling attempts.

Before Task 5, all of these numeric limits must pass:

- exactly one outer write transaction per newly staged response;
- at most six non-PRAGMA SQL statements for owner/revision fencing, source transition, collision classification, and frame insertion, with no per-event SQL;
- candidate p95 response-to-stage wall time no greater than `2.0 * control_p95 + 10 ms` for the matching response;
- candidate median CPU time no greater than `2.0 * control_median + 5 ms`;
- peak RSS no more than the control plus 32 MiB;
- WAL growth per response no more than `2.5 * canonical_response_bytes + 128 KiB`;
- failure under an external SQLite write lock (not the nonblocking lifetime file lock) between the configured 100 ms timeout and 350 ms, with zero partial business writes and a successful next attempt after release; and
- once the staged-frame limit is reached, zero further Matrix HTTP scheduling and less than 8 MiB RSS growth over 10,000 rejected scheduling polls.

Before Task 5 resumes, the checkpoint publishes every raw sample plus the median/p95, SQL trace, WAL delta, peak RSS, and external-lock/stalled-consumer result for every frozen fixture above.

After Task 6, a separately approved numeric gate adds frame-to-batch latency, batching efficiency, unrelated-room progress, and batch-to-ack latency. “Material regression” means any frozen numeric limit fails; changing a limit requires Bas Nijholt's explicit approval with the old and new measurements shown together.

## Line Budget and Human Gate

The final net production limit across `mindroom-nio` and MindRoom remains **+6,000 lines versus their fixed integration bases**:

- `mindroom-nio`: `980e53b3a3815592c3bd0dab9e41e2b057e50724`
- MindRoom: `ae04de40bd20d30741671a19bc97893b3ef052cb`

Changing either base requires a new measured baseline and explicit approval; it may not silently move the budget goalpost.

Production accounting includes Python under `src/` and `scripts/` and excludes tests, generated files, lockfiles, and documentation. Each repository uses the same command shape:

```bash
git diff --numstat <fixed-base>...HEAD -- \
  ':(glob)src/**/*.py' ':(glob)scripts/**/*.py' |
awk '{ additions += $1; deletions += $2 } END {
  printf "+%d -%d net %+d\n", additions, deletions, additions - deletions
}'
```

Uncommitted production changes are measured separately with `git diff --numstat` plus explicit `wc -l` for untracked production files. They never disappear from a report merely because they are not committed.

The simplified Task 4.5 target is:

- retain transport and membership-observation behavior: +242 measured;
- rebuild canonical staging as a focused slice: approximately +150 to +220;
- add the thin Peewee lifetime/transaction owner and integration with a hard ceiling of +200; and
- remove the six premature carrier/effect commits, the premature attachment commit, and the current guarded-writer experiment.

This yields a conservative simplified checkpoint envelope of +6,170 to +6,240 versus the fixed nio base before crediting either later replacement deletions or newly removed pre-Task-4.5 dormant scaffolding.

The entire remaining cross-repository program is capped at **+3,150 additional production lines**, including reducer/materializer, crypto receipts, coordinator/lifecycle, performance tooling, MindRoom admission, the minimal room directory, provisioning, contingency, and the later desktop migration. Desktop is outside initial bot activation but not outside the final line budget. If it cannot fit its reserved share, it must pair additions with immediate deletions or receive a new architecture approval.

Before any future deletion is credited, the provisional high side is **+9,390**: the +6,240 simplified checkpoint high side plus the +3,150 remaining program cap. The conditional scenario after all 3,393 mapped whole-file deletions actually land is +5,997. Those are reported as separate numbers until the deletion commit exists.

Task 5 remains blocked unless each deletion needed by the final budget is either already committed or assigned to an approved checkpoint with exact current consumers, its replacement, and its observation gate. The currently mapped whole-file dependencies are:

| Legacy production surface | Replacement checkpoint | Earliest deletion gate |
|---|---|---|
| nio sync recovery, response ordering, reset fence, abandonment, Sliding membership/token modules (2,433 lines) | source core plus reducer/materializer/coordinator, including desktop compatibility | bot and desktop observation complete |
| MindRoom continuity, checkpoint-trust, certification, recovery-escape, and token modules (960 lines) | MindRoom batch admission and authoritative directory | bot observation complete |

The named partial-symbol inventory remains contingency and is not required for the +5,997 conditional scenario.

Bas Nijholt is the human approval owner. On 2026-08-09 he delegated uninterrupted execution of this plan to Codex. At every task or every 500 added production lines, whichever occurs first, Codex reports directly, reforecasts, and continues without waiting while every hard constraint still passes:

1. production additions, deletions, and net by repository;
2. cumulative net versus the fixed bases;
3. exact legacy files/symbols newly made deletable;
4. actual deletions committed, which are the only deletions credited;
5. low/high remaining forecast and final-net forecast;
6. SQL/write-amplification and performance deltas where applicable; and
7. any task exceeding its estimate by more than 50 percent.

Work stops before the next task if:

- the conditional high-side final forecast exceeds +6,000 after subtracting only approved, checkpoint-assigned deletion dependencies;
- a task is more than 50 percent above its approved estimate;
- a required deletion has no named replacement and consumer inventory;
- a deferred invariant has no owner before activation; or
- performance exceeds its approved budget.

When a stop condition is reached, Codex must first reduce scope, delete superseded implementation, or redesign the next checkpoint to restore compliance, then continue. It requests another decision only when no compliant in-scope path remains.

The status may also be recorded in the ledger, but the ledger never substitutes for a direct report and human decision.

## Delivery Decomposition

This architecture is too large for one implementation plan. It is delivered as separately reviewed subprojects:

1. **Task 4.5 simplification:** remove the uncommitted guard experiment, perform the selective rollback, establish Peewee ownership, and re-port lean staging.
2. **Reducer and materializer:** Tasks 5-6, including the first minimal per-room aggregate and ownership transfer.
3. **Crypto replay safety:** Task 7 and the shared E2EE/receipt transaction.
4. **Coordinator and lifecycle:** Tasks 8-9 and their effects/commands.
5. **MindRoom bot activation:** admission, minimal directory, crypto reach-through removal, provisioning, and canary.
6. **Desktop migration:** separately approved after bot activation.
7. **Observation and deletion:** only after the relevant old path has passed its observation gate.

Each subproject receives its own design/plan and line envelope before implementation. Approval of this architecture authorizes writing the Task 4.5 simplification implementation plan only; it does not pre-approve all later code.

## Acceptance Criteria for This Replan

The replan succeeds when:

- the guard/capability database façade is absent;
- journal and E2EE use one ordinary Peewee database object and one journal-file connection;
- raw journal and Peewee model writes demonstrably share one transaction;
- the immediate durable core contains only transport, observations, and canonical staging;
- every removed correctness guarantee has the named future owner above;
- no deferred guarantee is needed by an enabled runtime path;
- the written and measured budget gate is enforced directly with the human approval owner; and
- Task 5 remains blocked until the simplified checkpoint's actual size, forecast, tests, and performance are approved.

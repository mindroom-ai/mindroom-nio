# Durable ingestion: supported guarantees and deliberate limits

Status: accepted for implementation, 2026-09-05.

This document controls the simplification of PR #55. It supersedes conflicting
requirements in the August durable-ingestion design and plan, especially the
new plain ingestion API, all-record durable callback parity, and repeated
internal validation. The user approved this contract after reviewing the
failure modes and maintenance costs. New guarantees require an explicit design
amendment explaining a realistic failure, user impact, implementation cost,
and verification. An excluded guarantee is not a review defect by itself.

## Scope and architecture

Keep one owned, prepared ingestion engine shared by Classic and Sliding.
Remove the second plain materializer and its new public session entrypoint.
Keep the existing owned factory and consumer batch/settlement interfaces used
by MindRoom. Desktop and other ordinary clients retain upstream-style public
sync, sync_forever, room projection, callbacks, and E2EE behavior.

The existing MindRoom integration surface comprises `_open_owned_ingestion`,
`_OwnedIngestionSession`, `_FrameCompletion`, `_MarkedStoreRequiresSqlite`, and
`_settle_batch`, including the current factory, `next_batch`, and settlement
signatures. Preserve these exact names and signatures for that consumer; a
leading underscore does not make them disposable. This consumer-specific
compatibility commitment does not create a public export or general third-party
compatibility promise. MindRoom accepts `EPHEMERAL` records and has no
`RecordKind.PRESENCE` branch, so durable receipts continue through the existing
`EPHEMERAL` route without defining new receipt semantics.

The discarded plain-session API existed only in this unmerged change; removing
that API does not remove a released interface. The full PR also replaces older
released fork-specific ingestion interfaces, including
`AsyncClient.add_event_admission_callback` and `CallbackNotAcceptedError`, with
the owned batch/admission contract. Those old fork interfaces are deliberately
removed, not covered by the upstream-style callback compatibility promise.
Consumers using them need the coordinated integration cutover; do not restore a
second admission path through compatibility shims. The release must communicate
these breaking changes. Version selection and release publication remain
separate work.

The data flow remains response/cursor commit, callback-free preparation with
crypto writes, prepared Work publication, consumer admission, acknowledgement,
outbound crypto maintenance, then frame retirement. Durable state stays in
the existing five ingestion tables; no new table, dependency, public option,
background worker, or general-purpose retry framework is needed.

## Required guarantees

1. Commit the accepted response and source cursor atomically. A process crash
   before commit advances neither; a crash after commit leaves replayable input.
   Do not acknowledge to-device delivery to the server before retaining its data.
2. Commit prepared durable output and its MatrixStore/crypto changes in the same
   transaction. Persist decrypted output so recovery does not decrypt it again.
   After an exception following in-memory crypto mutation, poison the session
   and reconstruct it from committed storage; never retry the mutated instance.
3. Replay unacknowledged durable Work under stable identities. MindRoom owns
   semantic-event admission and deduplication. Nio acknowledges only accepted
   work and does not invent another semantic-event ledger.
4. Preserve membership, departure fencing, authorization-related state, key
   processing, and ordered durable event semantics. State and account data are
   not generally disposable. Read receipts remain on their existing durable
   path to preserve observed projection and callback compatibility. This does
   not classify every receipt as business-critical or promise semantics beyond
   that observed compatibility. Discarding receipts requires a separate contract
   decision; this change relaxes only typing and presence.
5. Preserve history-gap detection, membership-state hydration, explicit losses,
   and fenced Sliding cursor resets. Nio emits loss for unresolved history;
   MindRoom owns later conversation-context repair. Replaying missed messages as
   actionable requests is a separate product decision, not an existing guarantee
   of this engine. Never pretend unknown history is complete or erase retained
   durable Work during a connection reset.
6. Keep a single supported owner of a durable crypto store, atomic adoption of
   deployed ordinary stores, account/device binding, and safe close/revocation.
   SQLite transactions, foreign keys, row identity/digest checks, and validation
   at network/disk boundaries remain required. Importing without `fcntl` remains
   supported. Filesystem ownership requires supported locking and must fail
   clearly when used otherwise.
7. Retain exact pending encrypted send bodies and transaction IDs across retry.
   Preserve atomic local application of maintenance responses. Retryable errors
   keep pending state and use bounded cancellable backoff.

These are application-process crash guarantees. SQLite WAL with
synchronous=NORMAL remains the selected performance/durability tradeoff.

## Best-effort typing and presence

Typing and presence are transient observations, not durable business records.
They must not produce Work, consumer receipts, hydration obligations, or retained
frame completion obligations. Valid fresh observations may update existing
in-memory room/user projections and invoke ordinary registered callbacks outside
transactions. There is no replay, completeness, exact ordering relative to
durable callbacks, or persistence guarantee for those observations. Updates for
unknown rooms/users may be dropped.

Only fresh successful source responses attempt transient delivery, after their
response/cursor commit. Reconstructing a staged frame after restart does not
replay transient callbacks. Identifiably malformed typing/presence content is
discarded with a bounded diagnostic; it must not poison the durable session or
prevent later messages, receipts, or keys from settling. The complete response
must still be valid bounded JSON; malformed durable sections and unidentifiable
mixed envelopes retain their normal validation.

Callback exceptions are isolated from durable progress. Within the owned
engine's transient path, `asyncio.timeout(1)` provides one cooperative budget
per fresh response for transient section extraction, event validation,
projection updates, and sequential callback awaits in the current task. The
already validated response body is decoded before this budget. It creates no
separate task for the fanout. A large transient burst can exhaust the budget
before all callbacks run; completeness remains deliberately unsupported. A timeout can
cancel a callback at an await after its projection update or earlier callback
side effects. Those effects are not rolled back, and the unfinished fanout is
discarded. Callbacks must cooperate with cancellation, clean up resources, and
not block the event loop. Keep the one-second bound so a stuck disposable
callback cannot stall durable progress, and propagate external cancellation.
Do not create a durable queue or unbounded background tasks for this. Ordinary
upstream sync callback behavior is unchanged. A malformed transient update or
failed transient callback is not an ingestion integrity failure. Diagnostics
must not include payloads, credentials, or keys.

## Deliberately unsupported guarantees

- Delivering every typing/presence change, preserving these values through
  restart, or giving them consumer receipts.
- Exactly-once arbitrary callback effects. Existing durable auxiliary callbacks
  can be lost after receipt and before fanout. Tool side effects require their
  own idempotency or reconciliation; Nio cannot promise exactly-once email,
  subprocess execution, or remote writes.
- Survival of every recent commit after power loss, an operating-system crash,
  lost storage, or an inconsistent backup. NORMAL can lose recent commits after
  machine failure. This is not authorization to weaken process-crash recovery.
- Protection against a malicious writer that can replace data and recompute
  hashes, or arbitrary mutation of already validated private Python objects.
  Checksums detect inconsistent storage; they are not a local security boundary.
- Rechecking identical canonical bytes or the same unchanged transaction snapshot
  at every internal function call. A validated immutable value may be trusted
  inside its owning operation. Crossing a disk, process, or transaction boundary
  still requires the appropriate check.
- Supporting two independent durable runtimes, or preserving public ingestion
  APIs introduced only during this unmerged PR.
- Format/API compatibility with intermediate unmerged PR checkpoints. Do not
  add migration machinery solely for their temporary ingestion records.
  Existing deployed MatrixStore identities, keys, sessions, and trust facts
  remain covered by the required adoption and upstream compatibility contracts.
- Accepting arbitrary concurrent raw SQLite writers, custom database factories,
  or live replacement of an owned database. Keep the supported lifetime lease
  and check identity once per actual SQLite execute, fetch, or iteration call,
  including every `fetchone` or `next` operation. This removes redundant wrapper
  checks while preserving per-row checks wherever iteration performs one
  operation per row.

## Failure policy

Malformed critical network or persisted data fails closed without advancing the
durable frontier. A transient parsing/callback failure is isolated and discarded.
An actual disk/identity mismatch must not be mislabeled as a transient error.
A programming error is not suppressed by a blanket exception around preparation,
materialization, or settlement. Exceptions are caught only inside the transient
handling boundary or an already established retry policy.

## Failure costs and maintenance tradeoffs

These are risk judgments, not measured incident frequencies. Each guarantee has
its stated scope; failed hardware, inconsistent backups, and arbitrary external
side effects require separate policies.

| Guarantee or limit | Realistic trigger | Worst case without the guarantee | Maintenance decision |
| --- | --- | --- | --- |
| Atomic response/cursor capture | Process kill or response arriving during shutdown | Cursor skips an unretained message or to-device key; later decryption can fail permanently | Keep one fundamental transaction boundary. |
| Atomic prepared output and crypto writes | Exception or crash during preparation | Crypto advances without deliverable output, or recovery uses inconsistent key state | Keep the transaction and poisoned-session recovery despite their complexity. |
| Stable Work replay and consumer admission | Consumer restarts after acceptance but before acknowledgement | Work is lost or processed twice; an external action can repeat | Keep stable identities and admission; consumers still own external-effect deduplication. |
| Membership and authorization ordering | Kick, leave, invite, join, or power-level change interleaved with messages | Consumer acts using stale membership or authorization | Keep ordered state and departure fencing because access decisions depend on them. |
| Gap detection, state hydration, and explicit loss | Limited timeline, reconnect, or incomplete state | Missing context is silently presented as complete history | Nio reports loss and hydrates state; MindRoom owns context repair. Actionable backfill requires a separate decision. |
| Single supported store and ingestion owner | Overlapping restart, accidental second client, or ordinary sync on an attached owned client | Competing crypto writers or sync routes overwrite state or progress | Keep the lifetime lease, actual I/O fences, and narrow ordinary-sync entry guard; exclude arbitrary local writers. |
| Atomic frames within output limits | A valid busy-room response exceeds the old READY threshold | Accepted input repeatedly fails after restart and the bot stops making progress | Remove the separate READY limits; retain explicit total/per-row bounds and precise resource errors. Intrinsically oversized output still needs intervention. |
| Deployed MatrixStore adoption | Existing deployment upgrades | Device identity, sessions, or trust facts are lost, disrupting encrypted history and verification | Keep careful adoption and boundary validation. |
| Exact encrypted outbound retry | Server accepts a request but its response is lost | One logical send repeats or retry uses a different body after crypto state changes | Keep persisted bodies and transaction IDs with atomic response application. |
| Durable receipts and account data | Restart while auxiliary records remain pending | Read/account settings become stale or callbacks are missed | Keep the shared durable route; callback effects are not exactly once. |
| Transient typing/presence durability is excluded | Restart, malformed update, callback error, or slow callback | Typing/online state is missing or stale until a later update | Drop replay obligations; bound fresh callbacks and isolate failures. |
| Recent commit survival after machine failure is excluded | Power loss or operating-system crash | Some recent commits are lost | Keep WAL NORMAL; stronger sync policy is a separate product decision. |
| Revalidation of immutable internal values is excluded | Programmer mutates private objects contrary to their invariants | Internal behavior becomes undefined | Validate network/disk boundaries, then trust unchanged values within the operation. |

Matrix allows a subsequent sync using the returned cursor to acknowledge
previously delivered to-device messages and permits their deletion. Its
transaction IDs also support request idempotency, motivating retained input
before cursor progress and exact outbound retry.
[Matrix to-device delivery](https://spec.matrix.org/latest/client-server-api/#send-to-device-messaging)

SQLite distinguishes application crashes from operating-system crashes or power
loss: WAL NORMAL preserves committed transactions across application crashes but
can lose recent commits after machine failure.
[SQLite synchronous modes](https://www.sqlite.org/pragma.html#pragma_synchronous)

## Review and maintenance rules

Every proposed added check or persistent state must identify its trust boundary
and a supported failure it catches. Tests that demonstrate corruption by writing
a valid but unsupported private object do not automatically justify production
defenses. Preserve independent crash, crypto, ownership, membership, and
admission tests; port useful behavior tests to the remaining engine. Remove
tests solely for the discarded engine instead of retaining production shims.

Consolidation must delete duplicated representations and branches, not compress
formatting, remove explanatory comments to meet a quota, or move production
implementations into tests. Production size is measured against 742806f and
the PR base 5b6de3b. The earlier 2,000–3,000-line estimate is not a correctness
gate; report actual changes and explain any difference.

Actual measurements supersede the August fixed size budgets. Large behavioral
tests are accepted for this change; their size does not create a test-shrink or
historical-document deletion task. Existing supersession statements remain
controlling.

## Cutover hardening amendment

Accepted direction, 2026-09-05: retain the atomic prepared engine and fix its
observed capacity and ownership gaps. This amendment does not authorize a live
cutover. The companion integration still needs its own verification.

### One ingestion route per client

An attached owned client rejects ordinary `sync`, `sync_forever`, and application
of an ordinary `SyncResponse` before network, callback, token, or crypto effects.
Its coordinator owns those effects. Non-sync response handling, sends, history
reads, hydration, and cleanup remain available. Ordinary clients keep their
existing callback behavior, including nested response callbacks.

The factory requires a client that has never attempted ordinary sync. Record
that attempt before its first await, including failed or cancelled attempts;
callers can create a fresh client instead. This closes the race where a custom
HTTP implementation has an outstanding sync but no visible HTTP session yet.
Use the existing attached-store marker and one ordinary-use flag, without a
second mode manager. A healthy owned close restores ordinary-client use; a
poisoned client still requires reconstruction.

### Atomic frame capacity

A real 322,948-byte Classic response containing 2,050 timeline messages was
accepted and then repeatedly failed preparation at the 2,048 READY-record limit,
including after reopening. This is a supported workload and must make progress.
The owned coordinator already drains the previous prepared frame before polling
again. Remove the separate READY count/byte limits and their unused settings.
Publish each prepared frame atomically within the remaining durable output
bounds; retain the existing crypto transaction and stable Work identities.

The supported durable output envelope remains 20,000 total Work records and
64 MiB of encoded Work, with 10,000 HELD records / 32 MiB of encoded HELD Work,
and 1 MiB per encoded Work row. Reserve the maximum header growth of HELD rows
when admitting them so promotion to READY cannot itself exhaust the per-row or
total byte allowance. Keep existing bounded loss handling for held-history
capacity; do not discard deliverable messages to fit a READY threshold.

The source-response byte limit is not a guarantee that its prepared output fits:
many tiny events, decrypted content, synthetic records, and storage metadata can
expand it. Exceeding a hard output bound is a distinct `JournalCapacityError`,
not evidence of corrupt storage and not an ordinary retryable queue pause.
The preparation transaction rolls back, the accepted source frame remains,
and a client whose crypto memory changed must be reconstructed. Reopening alone
cannot fix an intrinsically oversized frame. Such input requires operator
intervention or a separately designed larger-input representation; automatic
chunking, lossy skipping, or output spooling is outside this amendment. Persisted
rows that violate the supported schema still fail integrity validation.

A prepared-output spool or incremental crypto preparation would add durable
progress, completion fencing, ordering, and crash states. Neither removes the
need for a finite output bound. Those designs are not justified by the observed
READY-threshold bug and must not return as review-driven requirements without a
separate product decision.

### Reuse of unchanged validated data

Cache at most one decoded room Aggregate per journal instance. Every read still
fetches the row and checks its owner, identity, types, and revision. Reuse requires
an exact match of every stored field, including payload bytes and digest, plus
the stream/transport identity under which it was authenticated. A revision or
digest match alone is insufficient. A changed row takes the normal validating
decode path; clear the cache when closing. This extends internal-value reuse
across reads only when the complete authenticated input is unchanged. It does
not cache room projection effects or admit competing raw writers.

The measured follow-up also permits one cached decoded Work value per journal.
Validate the actual row's columns and revision bounds on every read before
reusing it; require identical complete stored fields plus account/stream/transport
identity. Cache only authenticated immutable results, clear on close, and retain
normal SQLite lookup/deletion behavior. Callback projection and admission are
not cached. The [type and delivery-cost design](type-correctness-and-delivery-cost.md)
records the benchmark and scope; claim/acknowledgement transactions stay separate.

Compute encoded Work length from the same canonical envelope prefix used for
persistence, without constructing and hashing the complete row just to count its
bytes. Reuse unchanged sizes inside one planning operation where that removes
repeat work; any changed status, revision, ordinal, identity, or plaintext needs
its own size. Keep one final digest for the bytes actually persisted. No global
cache, event batching framework, or new storage representation is introduced.

Share small projection and crypto-control helpers between ordinary sync and
owned preparation where they implement the same rules. Keep each route's
orchestration, callbacks, transaction timing, and durable admission separate.
Performance measurements must identify their workload and boundary; a faster
codec is not an end-to-end throughput claim.

### Explicit Peewee model binding

The store decorators bind their explicit `models` list with `bind_refs=False`
and `bind_backrefs=False`. MatrixStore/DefaultStore enumerate ten runtime models;
SqliteStore/SqliteMemoryStore add SQL device trust. Those lists include the
foreign-key targets and lazy relationships used by their operations. Adding an
operation or model requires keeping the appropriate list complete; no runtime
relationship-discovery framework is needed. DefaultStore keeps its sidecar trust
path, and historical migration-only models need not join the runtime list.

Keep all four ordinary/owned decorator binding contexts and their restoration,
ownership checks, read/write scopes, transactions, and queue-store handling.
Leave bootstrap, migration, and subset bindings unchanged. This avoids repeating
relationship traversal on every store call; it does not change Peewee's shared
class metadata or promise new concurrency safety. Verify distinct database
values, nested contexts, and exception rollback/restoration. Serial operation
speedups do not establish full application throughput improvements.

### History decision boundary

The Classic actionable-gap exclusion below is superseded by
[`classic-gap-recovery-and-capacity.md`](classic-gap-recovery-and-capacity.md)
following the measured missed-request failure and the user's instruction to
fix recovery and performance. That amendment defines bounded capture through
the existing journal owner. Its initial-history and Sliding limits are explicit.

The current prepared engine detects discontinuities and emits explicit loss; it
does not fetch missed messages through a Nio `/messages` backfill loop. Companion
MindRoom context hydration can fetch history later, but installing context is
not equivalent to admitting those messages as actionable requests. The required
guarantees above therefore do not promise to replay every missed request.
Changing that behavior or deleting the remaining gap-planning representation
requires a separate explicit decision. The capacity, ownership, and decoding
changes do not depend on that decision.

### Protocol and crypto-queue corrections from final review

These correct violations of the existing required guarantees; they do not add a
new persistence or delivery guarantee. Raw HTTP JSON need not use the internal
canonical serialization. Validate its bounded decoded value and response schema;
reserve canonical-byte equality for the internal/persisted representations that
actually require it. Valid join/leave responses and Matrix error codes must not
change meaning because of whitespace, key order, or JSON escapes.

Room-state hydration accepts ordinary `m.room.encryption` configuration state.
It rejects an `m.room.encrypted` envelope in the state snapshot, duplicate state
keys, and membership that contradicts its pending intent. Encryption projection
and tracking must survive publication and restart.

Crypto maintenance handles the normal lists already frozen by preparation:
multiple queued shares, several waiting key requests for one device, multiple
session rerequests, and a device that is both waiting and wedged. Consume the
matching operation while preserving other queued messages. Apply the same
per-session rerequest deduplication policy to live and frozen values. Persist
all generated follow-ups within the existing ordered operation list, with exact
bodies/transaction IDs and atomic crypto response application. No singleton-only
restriction, new schema, queue manager, or retry state machine is warranted.

For an own-device room-key request, the shared Olm policy checks existing device
trust before scheduling a missing-session key claim. An unverified device is
reported through the existing collected-request callback during preparation,
which can already publish durable callback Work. Verification and explicit
continuation still control sharing. This intentionally makes the callback
available earlier for the combined unverified/no-session case in ordinary and
owned sync, and avoids an unnecessary claim. It does not authorize sharing with
an unverified device or add callback Work publication during maintenance.

## Verification

- Real owned-engine Classic/Sliding processing, replay, acknowledgement, and
  source-progress tests; read receipts and account data remain covered.
- Malformed typing/presence mixed with a durable message cannot prevent delivery.
- Valid transient updates create no Work and are not replayed after restart.
- Callback failure/timeout remains isolated; cancellation and store cleanup work.
- Fault injection around real crypto, materialization, admission, and ack writes.
- Ordinary public sync/callback/store regression tests and safe store adoption.
- Codec corruption rejection with one authoritative decode per input boundary.
- Full repository tests, repository pre-commit hooks, and independent review
  against this document. No live deployment or cutover is part of this change.

Before deployment or cutover, run the companion MindRoom integration suite
against an artifact built from the accepted Nio revision. Its former base
conflict is resolved; see the [current integration status](type-correctness-and-delivery-cost.md#companion-integration-status)
for the verified companion revision and Nio pin. Artifact alignment and cutover
validation remain outside this local change.

### Known gap before relying on restart continuation

Replaying a collected unverified key-request callback currently does not restore
Olm's in-memory pending approval map. A real Olm/store reconstruction probe shows
that verification followed by `continue_key_share` rejects the request as
unknown; the same owned replay outcome is inferred from its unchanged callback
path and still needs an end-to-end reproduction. Existing tests establish
callback replay and absence of secret sharing before trust, not successful
interactive continuation after restart. Reproduce and resolve this before
relying on that flow at cutover; it is an implementation gap, not a newly excluded
guarantee. Publishing the current protocol/queue repairs does not deploy them.

## References

- [Matrix room encryption state](https://spec.matrix.org/latest/client-server-api/#mroomencryption)
- [Matrix to-device delivery](https://spec.matrix.org/latest/client-server-api/#send-to-device-messaging)
- [SQLite synchronous modes](https://www.sqlite.org/pragma.html#pragma_synchronous)
- [`asyncio.timeout`](https://docs.python.org/3/library/asyncio-task.html#asyncio.timeout)

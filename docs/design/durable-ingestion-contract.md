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

The discarded plain-session API existed only in this unmerged change, so no
released API is removed. Version and changelog publication remain release work.

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
5. Preserve bounded history-gap detection, repair/hydration, explicit unresolved
   losses, and fenced Sliding cursor resets. Never pretend unknown history is
   complete or erase retained durable Work during a connection reset.
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
engine's transient path, `asyncio.timeout(1)` encloses sequential callback
awaits; it does not create a task per callback or fanout item. A timeout can
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
| Bounded gap repair and explicit loss | Limited timeline, reconnect, or incomplete state | Missing context is silently presented as complete history | Keep bounded recovery and honest losses; do not promise unlimited repair. |
| Single supported store owner | Overlapping service restart or accidental second client | Competing crypto writers overwrite state or progress | Keep the lifetime lease and actual I/O fences; exclude arbitrary local writers. |
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

Before deployment or cutover, resolve the companion MindRoom integration's
current base conflict and run its integration suite against an artifact built
from the accepted Nio revision. Those integration steps remain outside this
local change.

## References

- [Matrix to-device delivery](https://spec.matrix.org/latest/client-server-api/#send-to-device-messaging)
- [SQLite synchronous modes](https://www.sqlite.org/pragma.html#pragma_synchronous)
- [`asyncio.timeout`](https://docs.python.org/3/library/asyncio-task.html#asyncio.timeout)

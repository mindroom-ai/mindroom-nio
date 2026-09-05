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
   path; this change relaxes only typing and presence.
5. Preserve bounded history-gap detection, repair/hydration, explicit unresolved
   losses, and fenced Sliding cursor resets. Never pretend unknown history is
   complete or erase retained durable Work during a connection reset.
6. Keep a single supported owner of a durable crypto store, atomic adoption of
   deployed ordinary stores, account/device binding, and safe close/revocation.
   SQLite transactions, foreign keys, row identity/digest checks, and validation
   at network/disk boundaries remain required.
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

Callback exceptions are isolated from durable progress. Use at most a one-second
cooperative callback budget per fresh response, discard unfinished transient
fanout on timeout, and propagate external cancellation. Do not create a durable
queue or unbounded background tasks for this. Callbacks remain responsible for
cooperating with asyncio cancellation and not blocking the event loop.
A malformed transient update or failed transient callback is not an ingestion
integrity failure. Diagnostics must not include payloads, credentials, or keys.

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
  and checks at actual I/O boundaries; redundant wrapper checks add no guarantee.

## Failure policy

Malformed critical network or persisted data fails closed without advancing the
durable frontier. A transient parsing/callback failure is isolated and discarded.
An actual disk/identity mismatch must not be mislabeled as a transient error.
A programming error is not suppressed by a blanket exception around preparation,
materialization, or settlement. Exceptions are caught only inside the transient
handling boundary or an already established retry policy.

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

## References

- [Matrix to-device delivery](https://spec.matrix.org/latest/client-server-api/#send-to-device-messaging)
- [SQLite synchronous modes](https://www.sqlite.org/pragma.html#pragma_synchronous)

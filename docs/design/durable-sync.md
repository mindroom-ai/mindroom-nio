# Durable Classic sync

Status: selected for implementation, 2026-09-06. This is the normative design
for the replacement of PR #55's ingestion engine. Implementation is tracked in
[the plan](durable-sync-plan.md); selection does not imply implementation is done.

This document supersedes conflicting requirements in
`durable-ingestion-contract.md`, including its cutover amendments and private
MindRoom API compatibility commitment. The replacement is coordinated with the
MindRoom consumer. Ordinary upstream-style clients retain their public behavior.
An explicitly excluded guarantee is not a review defect. Adding one requires a
realistic failure, user impact, implementation cost, and a test demonstrating it.

## Why change the design

The fork's main branch has a demonstrated crash gap. A real encrypted sync
containing a room key and message saves `next_batch` before applying its
to-device events. Killing the process at that boundary leaves a persisted
encrypted timeline record, no room key, and a restart request using the new
cursor. The healthy control decrypts the message. Recovery on main instead
delivers an undecrypted `MegolmEvent`. Retaining timeline events alone is not
enough to recover a sync response.

The existing PR fixes this class of bug, but adds a second event processor,
transport-neutral frames, a prepared-work validator, per-record delivery claims,
cross-database proof carriers, extensive schema preflight, and callback
settlement that partly duplicates application admission. At the starting head,
new ingestion and storage files total 19,100 lines. The PR's production delta
against main is +21,415/-6,681, or +14,734 net lines. Its 2,181 passing tests and
three skips establish a regression baseline, not evidence that this abstraction
is economical. The integrated 200-request runs still miss capacity targets.

Three approaches were considered:

1. Patch the old timeline-only recovery. Smallest initial change, but it still
   needs a transaction joining input, crypto mutations, and accepted output.
2. Keep simplifying the current engine. Preserves completed work, but leaves
   two event-processing implementations and an unnecessarily general protocol.
3. Add a small durable adapter around shared upstream sync processing. Selected:
   durability owns persistence and replay; normal nio code owns Matrix and crypto
   interpretation; MindRoom owns business admission and authorization.

The governing rule is **persist the minimum facts needed to resume useful work,
with one owner for each fact**. Size and performance are acceptance criteria.

## Ownership and data flow

The adapter requires nio's E2EE extra and an encryption-enabled SQLite client;
it handles plaintext rooms as well as encrypted rooms. This reuses the normal
account-bound store rather than introducing a second non-crypto storage backend.

Nio owns Matrix cursors, received input, crypto state, room projection, pending
crypto requests, and batches awaiting consumer acknowledgement. MindRoom owns
semantic deduplication, authorization, conversation scheduling, and application
effects. Their separate databases are appropriate; a second semantic ledger in
nio is not.

The durable adapter supports standard Classic `/sync` only. Ordinary client
interfaces remain available. The new durable API does not support Sliding Sync,
multiple consumers, concurrent store writers, arbitrary storage backends, or
custom reducers. Explicit Sliding configuration is rejected by the consumer.
Old unmerged ingestion databases are rejected clearly; their pending records or
tokens are never silently discarded or reinterpreted. Deployed ordinary SQLite
stores, including effective file-backed device trust, have an explicit adoption
path that preserves account identity and trust.

For each response:

1. Retain its bounded raw body before making any request acknowledging its
   `next_batch`. Until commit, the previous token remains authoritative.
2. In a synchronous SQLite transaction, run shared nio processing and freeze
   durable observations at their event-time yield points. Commit crypto writes,
   room changes, output batches, pending crypto work, and preparation progress
   together. No application callback or network await occurs in this transaction.
3. Expose the oldest committed batch with a read. Delivery does not claim, lease,
   hash, or rewrite the records. Acknowledgement deletes that batch transactionally.
4. MindRoom atomically inserts a batch receipt and its admission effects, applies
   ordered lifecycle hooks, then acknowledges nio. A crash between these steps
   redelivers the same batch; business admission is idempotent.

On any failure after in-memory crypto or room mutation but before confirmed
commit, discard the live client/store instance. SQLite rollback does not roll
back Python objects. Reopening restores the last committed state. Cancellation
obeys the same rule. Never retry preparation on the mutated instance.

## Shared event processing

`Client._iter_sync(response, *, include_left=False)` is synchronous and yields
`_SyncItem(route, event, room, section, source)` at current callback points. Its
optional `source` retains the original wire envelope when decryption replaces
the event. It also
yields state and room-section observations needed by durable consumers.
`Client` dispatches callbacks synchronously; `AsyncClient` awaits each callback
between yields. Existing callback ordering, room object identity, response
mutation, and exception propagation remain unchanged for ordinary clients.

The durable collector exhausts this iterator without callbacks. It freezes each
observation immediately, preserving event-time membership and verification.
After commit it delivers parsed committed events without running `_handle_sync`
or decrypting again. Leave-room processing is opt-in for the durable collector;
ordinary upstream behavior continues to ignore `rooms.leave`.

Room persistence contains compact live metadata and member deltas keyed by room
and user. It preserves own membership, encryption, power levels, room version,
creators and users, plus inexpensive display metadata. Avoid copying the entire
member list on a message. Restore derived indexes using normal room methods.
Do not claim a complete member list unless it was persisted as complete. Store
the live projection where upstream updates merge values; the latest raw state
event alone is not necessarily that projection.

Committed callback events retain their original event data and crypto evidence.
Room callbacks after restart see the restored current room projection; exact
historical snapshots of every room at every callback are not promised.

## Batch interface

`nio.durable` exposes `DurableSync`, `DurableSyncConfig`, `SyncBatch`,
`SyncRecord`, `RecordKind`, and `open_durable_sync`. Its records use:

```python
@dataclass(frozen=True, slots=True)
class SyncBatch:
    stream_id: UUID
    sequence: int
    records: tuple[SyncRecord, ...]
    completes_sync: bool = False

@dataclass(frozen=True, slots=True)
class SyncRecord:
    kind: RecordKind
    room_id: str | None
    source: dict[str, Any]
    clear: dict[str, Any] | None = None
    provenance: TimelineEventProvenance | None = None
    membership_epoch: int | None = None
    crypto: CryptoEvidence | None = None
    membership: OwnMembership | None = None
    route: str | None = None
```

`CryptoEvidence` stores `verified`, `sender_key`, and optional `session_id`.
`OwnMembership` stores previous/current membership and epoch, and whether the
change was local or reported. Record kinds are timeline, state, room account
data, global account data, to-device, room lifecycle, and loss. Loss contains a
reason and bounded recovery context; it never invents successful recovery.
Record identity is `(stream_id, sequence, index)`. Sequence and batch boundaries
are assigned during preparation, not at delivery. Account/device and consumer
generation are checked at session opening. No canonical digest, generated UUID
per record, room sequence, request identity, or transport origin crosses this
boundary. Decode network and persisted data defensively; trust unchanged values
inside their owning operation.

Membership epochs identify a joined tenure: the first join uses epoch zero,
departing from join increments it, and rejoining retains that incremented epoch.
This matches the application's authorization boundary. Rejoining discards stale
persisted members and treats its initial timeline as history.

Batches have bounded count and encoded size. Split before and after own
membership, loss, and membership events that change authorization. Preserve
their interleaving with messages. An earlier live grant must not run after a
later departure. `completes_sync` marks the final chunk, including an empty
response, only after required restoration and crypto maintenance.

The session offers `run()`, `next_batch()`, `ack(batch)`, `wait_for_work()`,
`quiesce()`, `close()`, durable progress, and serialized local membership changes.
Local changes retain an intent before HTTP, expected membership/epoch, and an
ordered result. They must not resurrect authorization through a stale echo.

`quiesce()` cancels an unaccepted long poll and stops new polls, then finishes
already captured input while the consumer keeps draining. It does not fetch an
extra final response. A completion batch remains replayable until acknowledged.
The consumer runs its completion hook before acknowledging that batch.

`dispatch(record, event=None)` invokes an observation's registered nio callback
outside storage transactions. It restores the committed event unless the
application supplies its authenticated extension event. The consumer decides
whether receipt/semantic novelty warrants dispatch and acknowledges separately.
Callback failure leaves the batch available; it does not poison committed state.
`progress_generation` exposes the highest committed batch sequence, including
acknowledged batches, and `cursor` exposes the last prepared Matrix position.

`change_membership(operation_id, room_id, previous_membership, previous_epoch,
current_membership)` accepts keyword arguments and returns success as a boolean.
Only join and leave are local targets. The application reads its current journal
position after `wait_for_membership_idle()` and supplies that expected position;
a stale expectation cannot authorize a newer transition.

## Crypto and history

Use the existing SQLite connection and crypto methods. Persist private one-time
keys with the exact pending upload before HTTP. Persist generated to-device
ciphertext and transaction ID with the ratchet mutation. Retry identical pending
sends. Apply responses, remove acknowledged work, and retain any generated
follow-on work atomically. Serialize crypto maintenance response application
with sync, so a key-query response cannot erase a newer dirty-user observation.

Persist waiting and untrusted interactive key-share requests needed by
`continue_key_share`, including state required after restart. Test the actual
restart and human-approval path. Committed device trust survives restart. An
in-progress SAS exchange may require restarting verification; persisting its
ephemeral handshake is excluded. No new cross-restart Megolm replay cache
guarantee is introduced beyond prepared-output replay and ordinary nio behavior.

The event codec reuses upstream parsers. Decrypted room events use
`Event.parse_decrypted_event()` and restore crypto evidence. Sanitized room-key
events require explicit reconstruction because their source omits key material.
Do not pickle arbitrary Python classes. MindRoom authenticates replayed custom
to-device events through its existing signed-device check, extracted into a pure
helper. It receives the original envelope and committed clear event; replay
must not invoke Olm decryption again.

Limited timelines recover the chronological missed interval from the previous
committed position before applying the retained live tail. Persist one bounded
continuation and fetch one page at a time, draining its output before fetching
more. Apply intervening membership/state in chronological order from the trusted
old projection. Context-only older history does not rewind the live projection
or acquire live authorization provenance. Initial history is not a live request.
Unknown authorization state requires hydration or an explicit fenced loss.
Token cycles, unavailable history, and oversized single events terminate as
explicit loss rather than silently advancing a supposedly complete history.

Defaults retain the established bounds: 16 MiB accepted sync body, 100 events
and 2 MiB per recovery page, 1,000 recovery pages, and bounded cancellable retry.
An oversized page reduces the window; one oversized event produces loss.

## Deliberate limits and durability

Typing, presence, and read receipts are best effort and are not replayed. Losing
them may leave temporary UI caches stale; a later observation refreshes them.
This explicitly relaxes the previous durable-receipt compatibility decision.
Account data and authorization state remain durable. Callback failures do not
roll back committed input. Auxiliary notification is at least once when retried;
exactly-once external effects remain the application's responsibility.

Use a lifetime exclusive filesystem lease for durable stores and ordinary-store
coordination at opening. Do not stat the database on every SQL execution or row.
Live file replacement, malicious database editing, custom SQLite triggers,
arbitrary schema variants, hostile same-process callers, and recovery of
unmerged prototype formats are unsupported. Normal schema version, identity,
foreign-key, and decoded-data validation remain. Import without `fcntl` works;
using ownership without supported locking fails clearly.

SQLite WAL with `synchronous=NORMAL` initially retains the established process
crash contract. Compare FULL under the batched workload before the final setting
is selected; document the measured tradeoff. Until then do not claim power-loss
durability. No new writer thread, database queue, serializer dependency, or
generic retry framework is introduced. Whole-response JSON is decoded once per
preparation attempt; prepared batches are encoded once and read without repeated
canonicalization. Diagnostics exclude message payloads, credentials, and keys.

HTTP retries keep the same operation and body for connection failures, timeouts,
408, 429, and server errors. Each request has at most five attempts with a
cancellable exponential delay capped at thirty seconds; bounded server retry
hints take precedence. Other HTTP errors return immediately.

## Consumer and acceptance

MindRoom adopts batch receipts in its existing application journal. Preserve its
projection barrier and ordered pre/post admission authorization hooks. Removing
that barrier would need a separate durable blocked-admission design. Discard this
bot's own pending/streaming replacement events early after decryption; preserve
placeholders, terminal edits, other users' edits, redactions, and unknown status.

Acceptance requires real process-crash and encrypted restart tests, idempotent
batch admission, membership ordering, bounded missed-history recovery, existing
interactive key-share approval after restart, ordinary client compatibility,
clean tests/types/hooks, and repeated 200-request integrated controls. Report
reply latency separately from synthetic generation time, history convergence,
event-loop delay, SQLite commits per admitted event, and production line delta.
Use the existing writer and stdlib JSON unless measurements justify a change.
The current engine is removed before completion; two permanent implementations
would defeat this design. Publish no release or merge as part of implementation.

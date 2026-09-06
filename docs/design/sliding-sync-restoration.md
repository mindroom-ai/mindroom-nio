# Sliding Sync through the shared durable core

Status: implementation plan accepted for execution, 2026-09-06.
This amendment supersedes the Classic-only scope in [durable-sync.md](durable-sync.md).
It preserves that document's transaction, consumer, crypto, size and loss contracts.

## Purpose and boundary

Restore the fork's useful Sliding Sync behavior without restoring its old
recovery engine. Faster Classic benchmarks do not replace room subscriptions,
bounded discovery windows, live/history classification or downtime recovery.
Both transports must use the same SQLite store, event interpreter, bounded
history walker, crypto owner and application batch acknowledgement protocol.

Restore `Api.sliding_sync`, `HttpClient.sliding_sync`,
`AsyncClient.sliding_sync`/`sliding_sync_forever`, and the public Sliding response
models. Ordinary clients retain normal callback semantics. Durable mode provides
restart and history guarantees through `nio.durable`; removed fork admission,
checkpoint and backfill APIs do not return. One durable session owns one transport
and one Sliding connection. Changing transport while reusing a durable store is
rejected explicitly; transport cursors must never be interchanged.

The wire boundary follows the deployed simplified MSC4186 endpoint, including
query-string position and timeout. Response positions, per-room `prev_batch`
tokens and the to-device extension cursor are distinct facts. A connection
starts without a position after reopening. `M_UNKNOWN_POS` resets only that
connection position, preserving accepted input, room baselines and crypto state.
Never pass a Sliding connection position to `/messages`.
The [MSC4186 proposal](https://github.com/matrix-org/matrix-spec-proposals/blob/main/proposals/4186-simplified-sliding-sync.md)
describes the protocol; deployed-server qualification is still required for
pagination and endpoint behavior.

## Shared code and ownership

* `api.py`, `responses.py` and `schemas.py` own wire requests and parsed models.
  Models contain protocol data, without a second recovery outcome hierarchy.
* A small `client/sliding_sync.py` module owns Sliding room normalization,
  initial snapshots, state deletion stubs, summaries and history/live decisions.
  Shared base-client event primitives continue to own decryption and state
  mutation. Historical context is decrypted and observed without rewinding the
  current projection. Both ordinary and durable clients use this path.
* `durable/sliding.py` owns only Sliding request configuration and persisted
  transport facts. `DurableSyncConfig.sliding` selects a `SlidingSyncConfig`
  (connection ID, lists, subscriptions, extensions); `None` retains Classic.
  Its request builder copies caller dictionaries and requests own membership
  alongside lazy membership. Required crypto extensions cannot be disabled in
  durable mode. A controlled subscription update replaces future request input,
  cancels an unaccepted poll, and retains accepted input and room progress.
* `durable/recovery.py` remains the only history walker. It consumes normalized
  room sections plus a per-room recovery start/target and provenance decision.
  Sliding adaptation supplies those facts; it does not add a second pagination
  or output-delivery loop. Retained input identifies its transport, so restart
  uses the same parser and continuation.
* Existing store transactions commit room baselines, to-device progress,
  projection, crypto mutations and output together. Room baseline facts contain
  the prior window token, exact own-membership event identity and bounded window
  event IDs needed to suppress overlap. No unbounded event ledger is introduced.

## Recovery and state semantics

An initial or limited window in a known room triggers bounded forward recovery
from its previous window token to the new window boundary when own membership
proves the same joined tenure. Exact membership identity, or a linked join-state
replacement, can prove continuity. An omitted member in an ordinary delta can
retain held proof; omission or deletion in an initial snapshot cannot. A new
live own join starts a new tenure. Leave/ban clears the old baseline.

Do not apply an overlapping previous window a second time or stop recovery just
because one duplicate event appears. Prove the requested interval complete at
the target boundary or bounded exhaustion; missing/cyclic tokens and exhausted
bounds produce explicit loss. Resume after crashes using the existing bounded
continuation and already committed batches.

`num_live` identifies the final N live entries, clamped to the actual window.
Without it, initial/expanded windows are history and ordinary deltas are live.
Both deployed expanded-timeline spellings are accepted. Proven downtime recovery
can make newly encountered window messages recovered; unproven snapshots remain
history and a known lost baseline emits loss. Previously observed history must
not become a new request on restart. First discovery cannot promise messages
from before subscription; no discovery-range widening may imply complete history.

State and membership changes remain ordered with actionable messages. Snapshot
state must not retroactively grant an earlier event or overwrite chronological
recovery. If the response cannot establish the necessary earlier state, retain
history provenance and report the missing baseline instead of guessing. Initial
snapshots rebuild stale room state and invalidate outbound membership caches;
account data, receipts and typing owned outside the snapshot survive. Contentless
state stubs clear represented state. Omitted heroes preserve the prior value;
an empty list clears it; departed heroes do not become joined members.

To-device processing precedes room decryption. Its delivery cursor advances only
with durable acceptance. Global and room account data remain durable, including
account data for a room outside the current window, coalesced by wire event type
until that room appears. Transient extensions retain the existing best-effort
budget and never become replay obligations.

### Expanded-window context boundary

Live qualification reproduced a subscription reset where Tuwunel expanded a
known room's window backwards beyond its previously observed window. Treating
every unseen event in that snapshot as recovered replayed an older invitation,
revoked the current joined tenure, and classified a newly sent request as history.
The regression reproduces that failure without a live server.

Retain the bounded window's observed identities separately from identities whose
interpretation completed. The first overlap with the previously observed window
separates older context from the recoverable suffix: unseen entries before it are
history and must not rewind state or trigger new requests. Successfully applied
overlap still skips duplicate interpretation. After successful recovery, a backward
expansion retains the prior pagination token and excludes the older prefix from
future overlap anchors. Missing boundaries or recovery loss invalidate that floor.
Otherwise a later window or forward page could move the recovery floor backwards.

Undecryptable actionable ciphertext also anchors the context boundary, while
remaining eligible for later promotion when a repeated window arrives with its
key. Historical ciphertext counts as observed history even without a key: automatic
repetition must not turn it into a new request. Applications can explicitly fetch
that history again after obtaining keys; durable sync does not promise automatic
historical re-decryption. This avoids a separate pending-history ledger. Both
identity sets belong to the same existing room checkpoint and share its byte
bound. No new queue, walker, database table or transaction owner is introduced.

## Main-store adoption

Reject adoption of a released store with outstanding recovery gaps, unaccepted
timeline records or unacknowledged recovery-loss markers. Explain that recovery
must be drained or explicitly settled using the prior release. Rejection must
leave the old account, token, obligations and schema usable by that release.
Clean stores and harmless completed-event markers remain adoptable, preserving
account keys and effective trust. Do not revive the old engine or silently erase
its obligations. Tests use the actual released schema rather than a production
compatibility shim.

## MindRoom requirements

Restore `matrix_sync.mode: sliding`, a stable per-agent connection ID, discovery
range `[0,99]`, and explicit subscriptions for configured resolved room IDs.
Preserve create/name/topic/avatar/encryption/lazy-member selectors and add own
membership for recovery. Enable to-device, E2EE and account data. Resolve/refresh
subscriptions after deferred joins and room configuration changes, including
rooms outside the discovery window. Keep existing consumer batch receipts,
authorization fences, first-sync readiness, membership hooks and watchdogs.
Pin and lock the tested producer commit before qualification.

## Execution and verification

1. Commit and self-review this amendment before implementation. The tightly
   coupled client/recovery work is implemented together; wire models and the
   adoption guard can be independently implemented with disjoint file ownership.
2. Add a failing adoption regression using released schema rows. Implement the
   guard before adoption mutations; test pending, gap, loss, completed-only and
   clean/trusted stores. Run the durable-store tests and hooks.
3. Restore wire models/builders with tests for request serialization, malformed
   room/extension data, state stubs, omitted versus empty fields, and both
   expanded-timeline names. Restore public names without old recovery fields.
4. Implement the shared Sliding interpreter and ordinary client methods. Port
   behavioral regressions for snapshot rebuilding, historical state, summaries,
   callback ordering, account data and cancellation. Remove the obsolete test
   that bans all Sliding API names; retain ordinary/durable ownership checks.
5. Add durable selection, connection reset, room checkpoints, provenance and
   shared pagination. Test encrypted response acceptance/restart; crash during a
   gap; duplicate overlap; equal/missing boundaries; expiry; live/history tails;
   membership transitions; malformed pages; subscription refresh; transport
   mismatch and local membership cancellation. Observe each behavioral test fail
   before its implementation. Keep tests aimed at user-visible guarantees.
6. Restore the MindRoom configuration and subscription refresh path, including
   configured rooms beyond the first 100. Test startup, runtime edits, readiness
   and durable event admission against the rebuilt producer.
7. Run Nio's complete suite, mypy and repository hooks; run the affected MindRoom
   suites, then its full suite/hooks. Review the integrated diff for duplicate
   implementations and unnecessary persistence. Fix verified findings.
8. Qualify Sliding behavior against Synapse and Tuwunel, including restart/gaps,
   encrypted delivery and a 200-reply run with the existing correctness/drain
   predicates. Retain a Classic control. Record source revisions, reproducible
   commands, actual results and production line deltas in the existing evidence
   index. A missing server capability is an explicit qualification limit, not a
   passing result. The existing 1,000-reply target remains unqualified.
9. Commit verified changes without amending, update the companion producer pin,
   and push both existing PR branches. No merge, release or deployment is implied.

The earlier 800–1,500-line restoration estimate is a planning estimate, not a
reason to omit a required behavior. Report actual production changes separately
from tests. No serializer, database writer, generic retry framework or old
recovery engine is added. Performance claims require measurements; restoring
Sliding does not by itself solve application SQLite contention.

## Implementation record

The design was committed before implementation as `21f58a7`.
The released-store adoption guard is `98e0f78`: 25 added production lines, 33
focused tests passed, and the prior release independently reopened all ten
rejected fixtures with their keys, token, trust and readable obligations intact.
Wire restoration is `9a64f98`; the shared interpreter and ordinary clients are
`0cfaad3`. Wire tests cover omitted fields, deletion stubs, both expanded-window
spellings and malformed extensions. Ordinary tests include real encrypted
late-key promotion, snapshot rotation and cancellation.

Durable tests cover process kills during capture and crypto processing, replay
after commit, bounded gap continuation across reopening, independent device
progress, expiry, missing membership/boundaries, subscription refresh, transient
isolation and persisted unknown-room account data. Failed actionable ciphertext remains
eligible for promotion if the server sends it again after keys arrive; there is
no new automatic replay queue for ciphertext the server never resends.
Independent review reproduced a stale recovery token after a missing-boundary
loss and an omitted persisted hero profile. Both received failing regressions
and focused fixes. A proposed subscription race was disproved by the existing
unaccepted-poll guard, so no additional concurrency mechanism was added.

MindRoom restoration is `7b05d07c4`; it refreshes subscriptions in the existing
`ensure_rooms` path after joins/leaves, including runtime room edits. Focused
consumer tests passed 352 cases with five skips. The tracked live harness now
accepts `--sync-mode sliding` (`0c59a7107`) for capacity and restart profiles.

Before live qualification, restoration adds 1,468 net Python production lines,
including the adoption guard. Against main `5b6de3bc`, the complete producer PR
is +4,998/-6,437, or 1,439 fewer production lines. These counts include comments
and blank lines and exclude tests, scripts and documentation. Qualification and
final consumer pinning remain pending; these implementation checks alone do not
establish deployed-server readiness or a performance gain.
Final producer unit verification at this checkpoint: 838 passed, three skipped;
mypy reports no issues in 60 source files. The complete consumer suite also
passed against the editable producer; locked-wheel qualification follows pinning.

The live expanded-window fix adds 58 net production lines across the shared
interpreter, Sliding checkpoint and existing recovery call site. Its 27 focused
tests include regression cases for old membership context, actionable late-key
promotion, historical ciphertext, forward pagination after reopening, and missing
boundaries/page loss. Review caught and reproduced the latter boundary cases
before the fix was committed. Restoration now adds 1,526 net production lines;
the complete PR is +5,056/-6,437, or 1,381 fewer production lines than main.
Final verification for this fix: 844 passed, three skipped; mypy is clean across
60 source files and all repository hooks pass. Live qualification below must use
a rebuilt, exactly pinned consumer wheel rather than the pre-fix package.

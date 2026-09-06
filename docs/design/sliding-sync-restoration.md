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

# Durable ingestion: lean replacement design

**Status:** controlling design, approved 2026-08-13

**Scope:** one indivisible review decision, implemented by one linked PR in each of the two Git repositories

**Supersedes:** the schema-v2 Sliding diagnostic-canary design and its implementation plan

## Goal

Replace the fork-specific sync recovery machinery with a smaller durable ingestion path that is safe under crashes and preserves the behavior already available on main. This is a replacement and simplification, not a new Matrix feature.

The mistake in the superseded design was allowing proof for one live canary to become production architecture: five diagnostic tables, one-room/control-room scope, list observations, event occurrences, rotation receipts, and a separate materializer. Those objects do not serve ordinary users and do not provide feature parity.

## Controlling decisions

1. Keep this as one indivisible change set so the complete replacement can be accepted or rejected as one unit. Because nio and MindRoom are separate Git repositories, each has one linked PR; neither is split into phase PRs or proposed independently.
2. Return the ingestion journal to schema version 1 and its existing five-table topology.
3. Remove all canary-specific production state and logic before implementing more behavior.
4. Keep one transport-neutral Frame/reducer/materializer/Work/batch/ack path for Classic and Sliding.
5. Preserve matrix-nio's public sync surface and callback behavior. Desktop continues to use the ordinary `sync_forever()` surface unchanged.
6. Do not preserve fork-only recovery/checkpoint internals after MindRoom is activated on the durable path and its observation window passes.
7. The sequence is prune, implement, activate, observe, then delete legacy recovery. The deletion gate cannot move earlier.
8. Live-canary evidence is external release assurance, never a reason to add production schema or semantics.

## Compatibility boundary

The following remain source- and behavior-compatible:

- `sync()`, `sync_forever()`, `sliding_sync()`, and `sliding_sync_forever()`;
- response, event, ephemeral, and to-device callback registration and ordering;
- response types, `AsyncClient.rooms`, ordinary sends, and desktop/pairing behavior;
- upstream matrix-nio behavior used by desktop, including room state, membership, encryption, account data, to-device traffic, responses, and callbacks.

MindRoom uses the durable engine. Desktop does not: it continues to use ordinary upstream-style `sync_forever()` plus to-device callbacks. Callers must not need new configuration or code. Callbacks never run inside a journal transaction.

Fork-added recovery outcomes, checkpoints, and Sliding helpers are deliberately outside that promise. The following are implementation details, not compatibility promises:

- `sync_recovery.py` and recovery-abandonment internals;
- reset-fence and response-ordering helpers;
- Sliding membership/checkpoint helpers;
- recovery persistence tables and private store APIs;
- tests coupled only to those implementations.

They remain temporarily while `sync()` and `sync_forever()` are restored to an upstream-style implementation that no longer calls them. MindRoom then runs the durable engine and desktop runs the restored public surface during separate observation windows. Only after both paths show zero calls into fork recovery are the unreachable internals removed in the same linked change set. Desktop is not migrated to the durable batch engine.

## Durable core

The ingestion schema remains version 1 with exactly these tables:

- `NioIngestMeta`: owner, stream binding, writer/source epochs, revision, outstanding-batch descriptor, and ack frontier;
- `NioIngestSourceState`: the canonical Classic or Sliding cursor and request configuration;
- `NioIngestFrame`: the canonical accepted response and candidate cursor;
- `NioIngestRoomAggregate`: room continuity, hydration, and recovery state;
- `NioIngestWork`: READY or HELD event/loss work.

There is no schema migration and no diagnostic topology. Remove:

- diagnostic scope, list-observation, event-receipt, event-occurrence, and source-rotation tables;
- `DiagnosticIngestionScope` and diagnostic row codecs;
- control-room and `probe` product configuration;
- canary event-fate types and diagnostic planner/materializer entrypoints;
- persisted list-operation evidence used only for the canary choreography.

The schema-v1 Classic canary branch is also scaffolding, not a keeper: remove `_diagnostic_admits`, `materialize_oldest_diagnostic_frame`, the one-room `room_id` coordinator contract, and its Classic-only/zero-OTK whitelist tests. The ordinary materializer and each pending hydration's own room ID replace that branch.

### Shared data flow

1. Classic or Sliding validates a response and normalizes it to the same `SyncFrame` contract.
2. One `IMMEDIATE` transaction authenticates Source state, inserts the canonical Frame, advances the cursor, and advances Meta revision. Both response and cursor commit, or neither does.
3. The oldest Frame is renormalized and passed to the same pure reducer for both transports.
4. The same materializer updates room aggregates, inserts READY/HELD Work or loss records, and retires the Frame only after every normalized record has a durable owner.
5. `next_batch()` retains the existing deterministic FIFO selection and compact outstanding descriptor.
6. MindRoom admits its batch receipt and application/event-journal effects in one transaction.
7. nio acknowledges only after MindRoom returns admitted or exact duplicate; ack advances the frontier and deletes the acknowledged Work atomically.
8. Compatibility adapters emit existing response values and callbacks with their existing ordering.

### Crash behavior

- Before response/cursor commit: neither advances.
- After response/cursor commit: the Frame is durable and replays.
- Before Frame materialization commit: the Frame remains.
- After materialization commit: Work is durable and the Frame is retired.
- Before MindRoom admission commit: the same batch replays.
- After MindRoom commit but before nio ack: the same batch receipt returns duplicate, then nio acks.
- A conflicting digest for the same durable identity is an integrity error and advances no frontier.

## Sliding restart and unknown positions

No rotation ledger is needed. The committed Source row and its frozen request are the authority.

On reopening an existing Sliding stream, one journal transaction:

- rotates writer and source epochs;
- starts request 0 with a fresh connection UUID and no `pos`;
- clears only connection-scoped position/coverage state;
- preserves to-device `since`, range/page configuration, Frames, aggregates, Work, outstanding batch, and ack frontier;
- advances the existing revision and next-source-epoch values atomically.

For a positioned request that receives the exact current `M_UNKNOWN_POS`, the journal performs the same fenced transition after authenticating stream, epoch, request ID, and cursor. The coordinator reloads committed Source state and replans. It does not mutate an in-memory cursor, silently fall back to Classic, or reset a stale/positionless request.

Old durable Frames and Work drain through the shared path. Normal Sliding responses do not enter a separate diagnostic planner.

## MindRoom semantic idempotency

The existing batch receipt remains authoritative for exact replay of `(consumer generation, stream, sequence, batch digest)`.

Sliding can legitimately redeliver the same Matrix event after a cold connection restart under a different nio batch. MindRoom therefore also applies the semantic identity already present in its event journal, in the same admission transaction:

- key: the existing unique `(principal_id, event_id)` journal row;
- comparison: the row's retained immutable clear headers—room, thread, kind, sender, and origin timestamp—must match the replay;
- absent key: admit and dispatch once;
- same key and matching headers: advance/record the new batch receipt but do not dispatch another application turn;
- same key and conflicting retained header: integrity error and full rollback.

The Matrix event ID remains the semantic content identity, as it already is on main; this design does not retain raw settled source merely to recompute a second digest. nio does not gain a second event-receipt or occurrence ledger, and MindRoom gains no new table or migration for this behavior.

## Activation and deletion sequence

1. **Prune now:** revert schema-v2 and all canary-specific production/test code in both repositories, including nio's schema-v1 diagnostic materializer/one-room coordinator and MindRoom's latch/env/one-room exact-wheel runner; remove historical implementation plans and in-repo benchmark/canary scaffolding that are not release artifacts.
2. **Implement:** complete the minimal Sliding reset, shared materialization, transport selection, and MindRoom semantic-idempotency kernels.
3. **Verify:** differential parity, crash matrices, public API/callback tests, and external Classic/Sliding canaries.
4. **Restore and observe both consumers:** opt MindRoom into the durable path and run desktop unchanged on the restored upstream-style `sync_forever()` path. Observe both with external coverage of fork-recovery modules.
5. **Delete legacy recovery:** only after MindRoom completes 24 continuous hours with at least 1,000 successful polls and three process restarts, and desktop completes a two-hour/100-response soak with a restart and a real to-device callback; both must show zero fork-recovery execution, zero duplicate visible effects, zero unexplained loss, and no permanently growing backlog. Keep the upstream public API and repeat desktop regression/smoke coverage after deletion.
6. **Final review:** run complete verification and report exact line budgets and remaining compatibility surface before the linked PRs are proposed for merge.

If observation fails, fix or retain the legacy implementation in this PR; do not delete on forecasted confidence.

## Assurance

Required tests:

- exact schema-v1 topology and explicit absence of all diagnostic tables/types/configuration;
- shared reducer/materializer parity fixtures for Classic and Sliding;
- atomic response/cursor and Frame/Work crash boundaries;
- Sliding reopen and positioned-unknown-pos preservation/fencing;
- exact batch replay and cross-batch same-event semantic idempotency using the existing journal identity;
- conflicting retained-identity rollback;
- public method signatures, response values, callback order, callback re-entry, and desktop to-device behavior against the upstream-style implementation;
- ordinary-room live canaries using released wheels and normal journal/frontier/application outcomes;
- the exact MindRoom and desktop observation thresholds above, with external coverage proving zero fork-recovery calls before deletion.

Test consolidation is part of the design. Preserve behavior matrices, not one test for every internal helper or historical implementation topology.

## Fixed final budget

Measured against `origin/main` at `6ed2b9817d2bc9de30dc72942f9cb867d829283b`:

- runtime `src/`: at most **+4,500** net lines;
- tests: at most **+15,000** net lines;
- docs and scripts: at most **+1,000** net lines.

These limits are fixed once. They are not rebound after work lands.

MindRoom is measured from the post-prune `0acaea2ba` boundary: runtime at most **+350** net lines and tests at most **+1,000** net lines. The final candidate must also be net smaller than canary commit `925df7f3c`, which added 271 runtime and 2,580 test lines over that boundary.

The post-prune baseline is approximately +8,807 runtime, +33,834 tests, and +4,867 docs/scripts. The measured fork-legacy production deletion pool is about 5,572 lines. Meeting the budget therefore requires real deletion and test consolidation, not credit for planned future work.

Any new persistent table, production module, public option, or transport-specific fate engine requires a parity-critical live producer and consumer plus an explicit design amendment. Canary evidence alone is not justification.

## Non-goals

- No schema v2 or v1-to-v2 migration.
- No diagnostic scope, control room, `probe` list, occurrence/receipt/rotation ledger, or canary-specific materializer.
- No new Matrix feature, protocol dialect, current-MSC4186 conformance claim, UI, YAML field, or default enablement.
- No removal or semantic change of the upstream-style sync and callback surface.
- No generalized nio event-dedup database.
- No deletion of fork recovery before MindRoom activation plus observation; no migration of desktop to the durable batch engine.
- No merge, release, or cutover claim from a canary alone.

## Completion criteria

The linked PR pair is complete only when:

- schema v1 and the five-table durable topology are the only ingestion schema;
- Classic and Sliding use one reducer/materializer and one Work/batch/ack ownership chain;
- Sliding reopen and positioned unknown-pos recovery are atomic and parity-tested;
- MindRoom dispatches each semantic Matrix event once across batch and connection replay;
- the upstream-style public sync/callback surface and desktop behavior are unchanged;
- MindRoom activation observation and desktop before/after compatibility checks pass;
- fork-specific recovery/checkpoint code and its implementation-detail tests are actually removed;
- all verification passes and all three fixed budgets are met;
- the entire result remains one indivisible review decision with one PR per repository.

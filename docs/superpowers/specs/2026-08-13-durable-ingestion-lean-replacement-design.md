# Durable ingestion: lean replacement design

**Status:** controlling design, approved 2026-08-13

**Scope:** one indivisible review decision, implemented by one linked PR in each of the two Git repositories

**Supersedes:** the schema-v2 Sliding diagnostic-canary design and its implementation plan

## Goal

Replace the fork-specific sync recovery machinery with a smaller durable ingestion path that is safe under crashes and preserves the behavior already available on main. This is a replacement and simplification, not a new Matrix feature.

The mistake in the superseded design was allowing proof for one live canary to become production architecture: five diagnostic tables, one-room/control-room scope, list observations, event occurrences, rotation receipts, and a separate materializer. Those objects do not serve ordinary users and do not provide feature parity.

## Controlling decisions

1. Keep this as one indivisible change set so the complete replacement can be accepted or rejected as one unit. Because nio and MindRoom are separate Git repositories, each has one linked PR; neither is split into phase PRs or proposed independently.
2. Return the ingestion journal to schema version 1 and exactly five tables. One load-bearing authenticated Frame completion column is allowed; it does not revive schema v2 or add a table, and Classic must be requalified against it.
3. Remove all canary-specific production state and logic before implementing more behavior.
4. Keep one transport-neutral Frame/reducer/materializer/Work/batch/ack path for Classic and Sliding.
5. Preserve matrix-nio's true pre-fork public sync surface and callback behavior at `69aa99c51f354980bf5306a85b4c7b7d71ff3217`. Desktop continues to use ordinary `sync()`/`sync_forever()` through one internal upstream-style profile that keeps sync-token persistence and E2EE but disables fork-only limited-timeline recovery.
6. Do not preserve fork-only recovery/checkpoint internals after MindRoom is activated on the durable path and its observation window passes.
7. The sequence is prune, implement, activate, observe, then delete legacy recovery. The deletion gate cannot move earlier.
8. Live-canary evidence is external release assurance, never a reason to add production schema or semantics.

## Compatibility boundary

The authoritative compatibility boundary is pre-fork matrix-nio commit `69aa99c51f354980bf5306a85b4c7b7d71ff3217` (`Support room version 12 (#546)`). The following remain source- and behavior-compatible:

- `AsyncClient.sync()`, `sync_forever()`, `stop_sync_forever()`, `receive_response()`, `run_response_callbacks()`, `add_response_callback()`, and `close()` with their boundary signatures and behavior;
- `add_event_callback()`, `add_ephemeral_callback()`, `add_global_account_data_callback()`, `add_room_account_data_callback()`, `add_to_device_callback()`, and `add_presence_callback()` with their boundary signatures and sequential ordering;
- the boundary `SyncResponse` shape, `AsyncClient.rooms`, ordinary sends, and desktop/pairing behavior;
- ordinary sync-token persistence and the upstream Matrix store/E2EE behavior retained by desktop;
- upstream matrix-nio behavior used by desktop, including room state, membership, encryption, account data, to-device traffic, responses, and callbacks.

MindRoom uses the durable engine. Desktop does not: it continues to use ordinary upstream-style `sync()`/`sync_forever()` plus to-device callbacks. Its internal client-construction profile sets limited-timeline recovery and its persistence off while keeping sync-token persistence, the exact `_MindRoomAsyncClient`, E2EE store, custom headers, key upload, room/client projection, callbacks, and permanent-authentication handling. Password login, SSO-token exchange and authenticated-client reconstruction, and restored sessions all select this profile. There is no public or user-visible option, and callers need no new configuration or code. Desktop's own durable command journal remains its command replay/idempotency owner. Callbacks never run inside a journal transaction.

Before Task 5C, Desktop controller-pin lookup reads the managed account
through a raw query-only SQLite connection and never constructs a MatrixStore;
therefore it cannot add `DeviceTrustState` or rewrite a pickle. Task 5C replaces
that fallback with the selected target bot's already-owned live client through
an internal orchestrator resolver before any bot acquires durable exclusive
ownership. Pairing thereafter performs no second database open; Task 5D only
activates the durable runner.

Fork-added recovery outcomes, checkpoints, and AsyncClient Sliding APIs are deliberately outside that promise. `AsyncClient.sliding_sync()`, `sliding_sync_forever()`, their HTTP/API helpers, and the `SlidingSyncResponse` family were added after the fork boundary and are removed from the restored public surface. Durable Sliding remains an ingestion transport adapter; it is not an AsyncClient compatibility API.

The following are implementation details, not compatibility promises:

- `sync_recovery.py` and recovery-abandonment internals;
- reset-fence and response-ordering helpers;
- Sliding membership/checkpoint helpers and fork-only Sliding response types;
- recovery persistence tables and private store APIs;
- tests coupled only to those implementations.

Their source modules remain temporarily while `sync()` and `sync_forever()` are restored to the boundary implementation and all normal imports, exports, registrations, and calls into those modules are removed. MindRoom then runs the durable engine and desktop runs the restored public surface during separate observation windows. Only after both paths show zero calls into fork recovery are the isolated source modules and implementation-detail tests removed in the same linked change set. Existing databases are never destructively downgraded or stripped of historical tables; the restored store ignores inert recovery tables. Desktop is not migrated to the durable batch engine.

## Durable core

The ingestion schema remains version 1 with exactly these tables:

- `NioIngestMeta`: owner, stream binding, writer/source epochs, revision, outstanding-batch descriptor, and ack frontier;
- `NioIngestSourceState`: the canonical Classic or Sliding cursor and request configuration;
- `NioIngestFrame`: the canonical accepted response and candidate cursor;
- `NioIngestRoomAggregate`: room continuity, hydration, and recovery state;
- `NioIngestWork`: READY or HELD event/loss work.

There is no ingestion schema-version bump, v1-to-v2 migration, or diagnostic topology. Activation does require one fail-closed v1 adoption transaction for the already-populated per-device MatrixStore. A private, non-exported configured opener receives the exact source class and authenticates account/device/store-v10 topology against it. A DefaultStore-authored database may lack `DeviceTrustState` or contain its exact ordinary topology and legacy rows because historical v2 upgrade and a pre-fix Desktop reader could create it while sidecars remained authoritative; a SqliteStore source requires the table. Malformed topology rejects, but absence, presence, emptiness, rows, or sidecar contents never infer historical class. A SqliteStore source remains SqliteStore. A supplied-Default source is explicitly converted to SqliteStore in the adoption transaction by creating the table when absent or replacing its inert legacy rows when present, then copying effective current sidecar facts for exact `DeviceKeys` identities with existing DefaultStore parsing and verified→blacklisted→ignored priority; stale unmatched entries remain inert, and the three legacy sidecars stay byte-for-byte unchanged as rollback artifacts. The returned `StoreBootstrap` carries only the immutable SqliteStore runtime binding; no ingestion table or persisted class marker is added. On every marked process reopen, the private caller supplies SqliteStore, and the opener reauthenticates ordinary and ingestion topology, identity, pickle, and foreign keys before writer/source rotation without rereading sidecars or adopting again. The public `open_ingestion_store()` signature remains unchanged and continues to reject a populated unmarked database without this private input. All non-trust MatrixStore/Olm/Megolm/key/session/token rows and inert historical recovery tables remain byte-identical, while trust conversion and the five v1 ingestion tables plus initial owner/source rows commit all-old or all-new. Durable runtime uses only SqliteStore, so later trust and device-key mutations share the compatibility-preparation SQLite transaction; it never performs legacy sidecar I/O. The unmerged schema-v1 definition may also add one nullable authenticated `NioIngestFrame.callbacks_claimed_revision` column so a completed/empty Frame can claim its private `_FrameCompletion` sink before fanout. Remove:

That conversion is scoped to MindRoom identities entering the durable engine.
Desktop remains on its ordinary upstream-style profile and keeps its existing
DefaultStore and sidecars; this change does not migrate Desktop storage.

- diagnostic scope, list-observation, event-receipt, event-occurrence, and source-rotation tables;
- `DiagnosticIngestionScope` and diagnostic row codecs;
- control-room and `probe` product configuration;
- canary event-fate types and diagnostic planner/materializer entrypoints;
- persisted list-operation evidence used only for the canary choreography.

The schema-v1 Classic canary branch is also scaffolding, not a keeper: remove `_diagnostic_admits`, `materialize_oldest_diagnostic_frame`, the one-room `room_id` coordinator contract, and its Classic-only/zero-OTK whitelist tests. The ordinary materializer and each pending hydration's own room ID replace that branch.

### Shared data flow

1. Classic or Sliding validates a response and normalizes it to the same `SyncFrame` contract.
2. One `IMMEDIATE` transaction authenticates Source state, inserts the canonical Frame, advances the cursor, and advances Meta revision. Both response and cursor commit, or neither does.
3. On restored login, MindRoom locates the exact existing per-device store before initializing Olm and supplies its exact source class to the private configured opener. The opener validates topology against that class; a DefaultStore source is atomically converted into the existing ordinary SqliteStore trust topology while preserving its sidecars, and every returned bootstrap binds SqliteStore runtime. On a true fresh login, a temporary login client obtains credentials but performs no sync, key upload, or store creation; the private owned factory then generates one canonical unshared Olm account and atomically creates exact SqliteStore v10 topology/account plus the five ingestion tables under one exclusive owner. A crash is all-absent or complete all-new, and reopen loads the committed account rather than generating another. Every exact built-in filesystem `DefaultStore` and `SqliteStore`, plus subclasses inheriting the built-in database factory unchanged, holds a shared lifetime sidecar/file-identity lease; ingestion adoption acquires and retains the exclusive form, and its privately borrowed exact SqliteStore reuses that owner. A subclass overriding `_create_database()` retains its public extension behavior but is outside this lease/adoption guarantee and must not open or retain a connection to a configured-ingestion path. The private opener accepts only exact `DefaultStore`/`SqliteStore` classes before filesystem or SQL access. Legacy DefaultStore sidecars are never read or written after successful adoption. Deployment overlaps neither an old binary nor an uncoordinated custom database factory for the same database. A non-exported `_open_owned_ingestion()` factory consumes the SqliteStore binding, attaches that exact store, and transfers it with the journal to `_OwnedIngestionSession`, preserving one close owner and the process/thread/file/writer fences. Existing public `open_ingestion()` remains session-first and signature-compatible, and public `open_ingestion_store()` remains signature-compatible and fail-closed on an unmarked populated database; direct public store-first continues to reject a later public session. A parallel fresh crypto database or second ingestion-owned/borrowed open is forbidden; participating ordinary shared same-file stores remain permitted. The five ingestion tables and ordinary E2EE rows therefore share one database owner and transaction boundary. The oldest Frame is then renormalized through one transport-neutral compatibility preparation phase preserving the exact pre-fork callback-free mutation order: token; to-device decrypt/mutation; invited/joined room state, timeline (including Megolm), ephemeral and account-data projection; presence/global projection; expired-verification mutation; device-list/key-count/fallback handling; then key-request collection. This allows a same-Frame room key to decrypt its later timeline and makes newly projected encrypted-room membership visible to same-response device-list handling. Callback fanout remains separate. Preparation stores decrypted `clear_json`, stable event IDs, `RoomSnapshot`, aggregate/Work changes, and the authenticated preparation revision without running callbacks. Device trust and rotated-key replacement are part of that same SQLite transaction, and no KeyStore sidecar writes are permitted. On rollback after in-memory Olm mutation, the session is poisoned and reconstructed from the rolled-back database; retrying the same Olm object is forbidden.
4. The prepared Frame is passed to the same pure reducer and materializer for both transports. Every `RecordKind`, including `TO_DEVICE`, and every loss becomes one-record Work with a stable `record_id`; device-list/OTK/fallback controls complete before preparation is marked. The same owner transaction persists an authenticated canonical outbound-maintenance plan inside the retained Frame envelope: exact ordered upload/query/claim/to-device request bodies, deterministic domain-separated to-device transaction IDs, exact ciphertext, canonical operation kind plus minimal typed local response-apply context, and per-operation pending/settled state. The corresponding Olm/session mutations and request bodies commit together; in-memory queues are not durable authority. Wire JSON alone is insufficient because dummy and room-key-request subtypes have distinct local settlement effects. This adds no table or column. A retained Frame is transient retry or frame-completion state, never a successful steady-state fate. The Frame remains until all of its Work is receipted/acknowledged, its persisted outbound maintenance succeeds, and its private `_FrameCompletion` claim is settled; empty Frames follow the same completion path.
5. `next_batch()` retains the existing deterministic FIFO selection and compact outstanding descriptor.
6. MindRoom admits every record receipt and its applicable event-journal, lifecycle, loss, projection, and frontier effects in one transaction. It returns separate `receipt_new` and `semantic_event_new` facts; a batch need not contain a Matrix event.
7. Outside both databases, the nio compatibility applier reapplies idempotent room/client state and publishes a record callback only when the receipt is new; semantic timeline/lifecycle dispatch is additionally gated by `semantic_event_new`. Those callbacks are best-effort/at-most-once across process death, matching the existing MindRoom wrappers, and never run inside a transaction.
8. nio acknowledges only after MindRoom accepts the receipt and callback fanout returns. Ack advances the frontier and deletes Work atomically. After Work drains, nio sends only the Frame's exact pending maintenance request. Each response is parsed callback-free, then its ordinary MatrixStore/Olm writes, the current operation's settled marker, and any causally generated follow-up operation commit in one owner transaction. Follow-ups monotonically append or replace authenticated Frame-plan entries with exact body/ciphertext and deterministic transaction ID; a fixed upstream-ordered loop runs to quiescence. This includes key-claim→dummy-to-device and successful dummy/to-device→room-key-rerequest chains. Rollback after in-memory mutation poisons the session before retry. To-device retries reuse identical ciphertext and transaction ID and therefore have server-side semantic deduplication. Upload/query reuse identical bodies. Matrix `/keys/claim` has no idempotency key, so response loss can consume another remote OTK; the guarantee is locally atomic application of one successful response, not remote at-most-once consumption. Retryable HTTP/Matrix errors preserve the same pending operation under bounded cancellable backoff; auth errors preserve it while entering the existing permanent-auth shutdown/health path. No error settles or fans out. When all maintenance succeeds, nio reconstructs an authenticated private `_FrameCompletion(frame_id, transport, source_epoch, request_id, staged_revision, claimed_revision)`, durably claims it, invokes the optional private owned-session sink outside transactions, and then retires the Frame. Sink raise/cancel/death after claim is at-most-once: restart retires without a second invocation. Durable Sliding never fabricates a public `SyncResponse` or `SlidingSyncResponse`; ordinary public response callbacks remain exclusively on Desktop's retained pre-fork sync path. `AsyncClient.rooms` is restored from persisted `RoomSnapshot` values before incremental delivery.

The coordinator expresses that lifecycle as one private progress state machine, not as a second fate engine:

```text
PREPARE -> HYDRATE_IF_REQUIRED -> WAIT_FOR_RECORD_ACK
        -> MAINTAIN_OUTBOUND_CRYPTO -> CLAIM_AND_FANOUT_COMPLETION
        -> RETIRE -> NEXT_SOURCE_HTTP
```

`WAIT_FOR_RECORD_ACK` yields to and is woken by the MindRoom pump; a retained prepared Frame is not `BLOCKED`. Hydration may issue its bounded room-state HTTP before record delivery, but the next source-sync HTTP cannot start until the prior Frame's Work, outbound maintenance, private completion claim, and retirement settle. Only malformed/authentication/conflict states are terminal `BLOCKED`. Close/cancellation wakes both sides. Before claim, durable Frame/Work remains replayable; after claim, the Frame remains durably retireable without invoking the private sink again.

### Crash behavior

- Before response/cursor commit: neither advances.
- After response/cursor commit: the Frame is durable and replays.
- Before Frame materialization commit: the Frame remains.
- After preparation/materialization commit: Work and room/crypto state are durable and the Frame remains as the completion owner.
- Before MindRoom admission commit: the same batch replays.
- After MindRoom commit but before nio ack: the same batch receipt returns duplicate, then nio acks.
- A conflicting digest or retained Matrix-event identity is an integrity error and advances no frontier.
- A crash during compatibility preparation rolls back its database writes, poisons the mutated client session, and reopens from durable state before replay.
- A crash before an outbound-maintenance plan commits exposes no request. After
  it commits, every retry uses the Frame's exact request body; to-device also
  reuses exact ciphertext and transaction ID. Maintenance response application
  and its settled marker commit together or the poisoned session reconstructs.
  A lost `/keys/claim` response may consume another remote OTK because Matrix
  provides no idempotency key; no stronger remote guarantee is claimed.
- A crash after a record receipt but before its best-effort callback can lose that auxiliary callback; claiming after fanout would instead duplicate arbitrary external side effects. Semantic MindRoom application dispatch remains exactly once through the event journal and record receipt.

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

The existing batch receipt remains authoritative for exact replay of `(consumer generation, stream, sequence, batch digest)`. The receipt schema in the unmerged MindRoom feature branch is corrected in this linked PR: replace its mandatory `event_id` foreign key with a nonempty `record_id`. Every one-record batch has that identity, while semantic Matrix identity remains independently owned by `journal_events`. This changes no deployed schema and adds no table or migration.

Sliding can legitimately redeliver the same Matrix event after a cold connection restart under a different nio batch. MindRoom therefore also applies the semantic identity already present in its event journal, in the same admission transaction:

- key: the existing unique `(principal_id, event_id)` journal row;
- comparison: the row's retained immutable clear headers—room, thread, kind, sender, and origin timestamp—must match the replay;
- absent key: admit and dispatch once;
- same key and matching headers: advance/record the new batch receipt but do not dispatch another application turn;
- same key and conflicting retained header: integrity error and full rollback.

The Matrix event ID remains the semantic content identity, as it already is on main; this design does not retain raw settled source merely to recompute a second digest. nio does not gain a second event-receipt or occurrence ledger, and MindRoom gains no new table or deployed migration for this behavior.

### Complete record fates

- decrypted LIVE/HISTORY `TIMELINE` records use the existing MindRoom event classifier; actionable events dispatch once and context events settle without a turn;
- `ROOM_LIFECYCLE` records use the existing membership/departure fencing transaction and own a batch receipt without fabricating a journal event;
- `LossRecord` values update the existing room-history-recovery obligation and own a batch receipt without fabricating an event;
- `STATE`, `EPHEMERAL`, `ROOM_ACCOUNT_DATA`, `GLOBAL_ACCOUNT_DATA`, `PRESENCE`, and `TO_DEVICE` records receive ordinary batch receipts, are applied by the compatibility owner to ordinary room/crypto state and corresponding registered callbacks, then settle without application dispatch;
- encrypted timeline inputs are decrypted before Work creation; an undecryptable event becomes the existing explicit decryption-failure application outcome rather than disappearing or retaining the Frame forever;
- malformed or unknown normalized input fails closed before Work publication while its Frame remains durable; no known `RecordKind` is allowed to become a permanently unacknowledgeable FIFO head.

## Activation and deletion sequence

1. **Prune now:** revert schema-v2 and all canary-specific production/test code in both repositories, including nio's schema-v1 diagnostic materializer/one-room coordinator and MindRoom's latch/env/one-room exact-wheel runner; remove historical implementation plans and in-repo benchmark/canary scaffolding that are not release artifacts.
2. **Implement:** complete the minimal Sliding reset, bind Desktop to its
   internal upstream-style no-recovery profile, then build the shared path in
   dependency order: in-place adoption, store/session lease transfer, MindRoom
   receipt/idempotency kernel, callback-free preparation, atomic all-kind
   materialization, receipt-gated compatibility apply/ack, MindRoom all-record
   pump, Frame completion state machine, durable login/session factory, and sole
   MindRoom engine activation. These are reviewable commits in the same linked
   PRs, not phase PRs.
3. **Verify:** differential parity, crash matrices, public API/callback tests, and external Classic/Sliding canaries.
4. **Restore and observe both consumers:** make the durable runner the candidate's sole MindRoom sync engine; the existing `matrix_sync.mode` selects only Classic or Sliding transport. Desktop pairing resolves a target controller fingerprint from that target bot's already-owned live client through an internal orchestrator resolver and never opens the exclusively owned database; missing or mismatched targets fail closed. Deploy that candidate to the observation cohort and run Desktop with unchanged user-visible behavior through its internal upstream-style no-recovery `sync_forever()` profile. Observe both with external coverage of fork-recovery modules. There is no canary-agent environment switch, new YAML field, or second sync-engine opt-in.
5. **Delete legacy recovery:** only after MindRoom completes one uninterrupted 24-hour observation interval spanning at least 1,000 successful polls and three recorded process restarts, and desktop completes a two-hour/100-response soak with a restart and a real to-device callback; both must show zero fork-recovery execution, zero duplicate visible effects, zero unexplained loss, and no permanently growing backlog. Keep the upstream public API and repeat desktop regression/smoke coverage after deletion.
6. **Final review:** run complete verification and report exact line budgets and remaining compatibility surface before the linked PRs are proposed for merge.

If observation fails, fix the durable path or restore Task 6's old-engine connections inside these linked branches before rerunning verification. The candidate has no hidden runtime fallback, and legacy source deletion is never based on forecasted confidence.

## Assurance

Required tests:

- exact schema-v1 topology and explicit absence of all diagnostic tables/types/configuration;
- shared reducer/materializer parity fixtures for Classic and Sliding;
- frame-scope compatibility parity against the pre-fork AsyncClient response applier, including replay after every MatrixStore write, encrypted timelines, to-device data, device lists, OTK/fallback state, and auxiliary callback ordering;
- atomic response/cursor and Frame/Work crash boundaries;
- Sliding reopen and positioned-unknown-pos preservation/fencing;
- exact batch replay and cross-batch same-event semantic idempotency using the existing journal identity;
- an explicit receipt/application disposition for every `RecordKind` and `LossRecord`, with no retained Frame or Work backlog after successful ordinary responses;
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

MindRoom is measured from the post-prune `0acaea2ba` boundary: runtime at most **+350** net lines and tests at most **+1,000** net lines. The final candidate must also be net smaller than canary commit `925df7f3c`, whose measured net delta over that boundary is +268 runtime and +2,579 tests.

The fixed post-prune baseline at `679e949` is +8,807 runtime, +32,098 tests, and +523 docs/scripts. The measured fork-legacy production deletion pool is about 5,572 lines. Meeting the budget therefore requires real deletion and test consolidation, not credit for planned future work.

Any new persistent table, production module, public option, or transport-specific fate engine requires a parity-critical live producer and consumer plus an explicit design amendment. Canary evidence alone is not justification.

## Non-goals

- No schema v2 or v1-to-v2 migration.
- No diagnostic scope, control room, `probe` list, occurrence/receipt/rotation ledger, or canary-specific materializer.
- No new Matrix feature, protocol dialect, current-MSC4186 conformance claim, UI, YAML field, or default enablement.
- No removal or semantic change of the true pre-fork sync and callback surface; fork-added AsyncClient Sliding and recovery APIs are not part of it.
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

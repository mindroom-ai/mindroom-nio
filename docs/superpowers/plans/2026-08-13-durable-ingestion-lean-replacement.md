# Durable ingestion lean replacement implementation plan

> Execute this plan as one indivisible review decision. nio and MindRoom are separate Git repositories, so each has one linked PR; do not split either repository into phase PRs. Do not pause for routine approval checkpoints. Stop only for a real safety, correctness, or scope blocker.

**Design:** `docs/superpowers/specs/2026-08-13-durable-ingestion-lean-replacement-design.md`

**Repos:** `/work/dev/mindroom-nio`, `/work/dev/mindroom`

**Starting code boundary:** Classic-qualified nio `b313ae284be5dcc7797a7f7de83c70ad80520beb`; post-prune MindRoom `0acaea2baf05a4c41cce7497cfcdf4880afc6d04`

**Linked branches:** nio `docs/durable-ingestion-rewrite-plan`; MindRoom `wip/matrix-journal-ingress-cutover`

## Invariants

- Preserve the exact pre-fork sync methods, responses, callbacks, `AsyncClient.rooms`, and desktop behavior at `69aa99c51f354980bf5306a85b4c7b7d71ff3217`.
- Return ingestion schema to v1 and exactly five durable tables.
- Classic and Sliding share Frame normalization, reducer, materializer, Work, batch, and ack.
- Never use canary-specific production tables, scope, control room, `probe`, occurrence ledger, rotation ledger, or materializer.
- Response and cursor commit together.
- MindRoom application effects, existing event-journal identity, batch receipt, and frontier commit together.
- Desktop remains on upstream-style `sync()`/`sync_forever()` through an internal profile that disables fork-only limited-timeline recovery while retaining tokens, E2EE, callbacks, and headers; it is not connected to the durable batch engine.
- Legacy fork recovery stays while the public sync internals are restored upstream-style; deletion waits for both MindRoom and desktop zero-use observations, then desktop is regression-tested again.
- Preserve upstream `secure_delete = "fast"` and its regression from `origin/main`.
- Final nio net budgets versus `origin/main@6ed2b98`: runtime ≤ +4,500; tests ≤ +15,000; docs/scripts ≤ +1,000.
- Final MindRoom net budgets versus post-prune `0acaea2ba`: runtime ≤ +350; tests ≤ +1,000; the candidate is also net smaller than `925df7f3c`.

## Task 1: Freeze the lean authority and prune the canary architecture

**Complete.** The diagnostic/schema-v2 chain was reverted through ordinary
commits, `origin/main@6ed2b98` was merged normally, MindRoom's canary commit was
ordinarily reverted, and nio's remaining schema-v1 diagnostic materializer was
removed at `0b07c510`. Current authority is schema v1/five ingestion tables,
`secure_delete="fast"`, generic materialization, and no canary production
types/configuration. Exact revert SHAs, paths, and verification are frozen in
the progress ledger and Task-1 report; do not replay or squash them.

## Task 2: Prune documentation, benchmarks, and redundant tests

**Complete.** `fe8a868`, `bdd5d7c`, and `5709f34` removed 2,435 net test lines
without production changes and retained stronger behavior/crash/public-API
coverage. Full verification was 2,056 passed/3 skipped. Exact mappings,
commands, deltas, and review approval are frozen in the ledger/report.

## Task 3: Implement the minimal Sliding source reset

**Complete.** `febbe9a5` implements one authenticated Meta+Source transaction
for Sliding reopen and exact positioned `M_UNKNOWN_POS`, preserving all other
durable state and Classic behavior. Full nio verification was 2,088 passed/3
skipped; exact causal/crash evidence and review are frozen in the ledger/report.

## Desktop compatibility slice: select the upstream sync profile

Implement this MindRoom-only slice before Task 4. It is part of the same linked
change set and adds no user-visible option.

Extend the internal `MatrixSyncStorage` construction policy with one exact
limited-timeline-recovery boolean. The default bot profile remains unchanged
until durable activation: recovery on, recovery persistence selected by the
existing field, and sync-token storage selected by the existing field. Define
one private Desktop profile with limited-timeline recovery off, recovery
persistence off, and sync-token storage on.

Pass that exact profile through every Desktop client-construction path:

- password login;
- SSO login-token exchange, including the temporary exchange client and the
  returned authenticated client;
- restored login.

Retain `_MindRoomAsyncClient`, custom headers, SSL, the existing DefaultStore
and pickle key, `replace_rotated_device_keys`, key upload, `sync()` and
`sync_forever()`, to-device and response callbacks, room/client projection,
presence handling, and permanent-authentication errors. Do not introduce a
Desktop sync loop, durable-ingestion adapter, public setting, or nio change.

Write causal tests first. Prove the Desktop profile yields
`backfill_limited_timelines=False`, `backfill_persist_recovery=False`, and
`store_sync_tokens=True`; default bot and application-owned profiles retain
their prior values. Prove password, SSO-token, and restore paths pass the exact
profile into the real internal client factory, and that Desktop startup sync,
pairing sync, bridge `sync_forever()`, to-device callbacks, key upload, headers,
and permanent-auth handling remain behavior-compatible. Run the focused
Matrix-client/Desktop session, pairing, CLI, identity, Cloudflare-access, and
to-device suites, then the full MindRoom suite and statics.

**Commit:** `refactor: isolate desktop from sync recovery`

Before Task 4A, also remove Desktop controller-identity lookup's mutating
`SqliteStore` construction. Read the expected account row through a raw
query-only SQLite connection, authenticate exact store version, user/device,
pickle bytes and shared flag, and decode only the Olm account needed for its
Ed25519 pin. It must not create a table, run a migration, or rewrite an old
pickle. Prove a real DefaultStore-authored database stays byte/schema/row
identical and never gains `DeviceTrustState`; malformed/cardinality/version
cases fail without mutation. This reader is the pre-activation compatibility
path. Task 5C replaces it with the already-owned live-bot resolver before the
exclusive durable session becomes the sole engine.

**Commit:** `fix: read desktop identity without store mutation`

## Task 4: Prove one shared reducer and materializer

Use test-driven development. First capture each causal failure below against the generic path; do not implement production preparation or callback claims before its failing behavior test exists.

Task 4 and Task 5 are one interleaved dependency chain. Execute and review the
following ordinary commits in this exact order inside the same linked PRs:

```text
4A in-place adoption
-> 4B store/session lease transfer
-> 5A MindRoom receipt/idempotency kernel
-> 4C callback-free preparation
-> 4D atomic all-kind materialization and snapshots
-> 4E receipt-gated compatibility apply/ack
-> 5B MindRoom all-record adapter and pump
-> 4F Frame completion state machine
-> 5C MindRoom durable login/session factory
-> 5D sole MindRoom engine activation
```

Do not collapse these into one implementation/review gate. They remain commits
in one PR per repository, not phase PRs.

### Task 4A: exclusive in-place v1 adoption

Adopt or reopen an exact populated store-v10 per-device database before Olm
initialization through a non-exported `_open_configured_ingestion_store()`
entrypoint.
It requires an explicit source class and owned runtime class. The only approved
pairings are existing `DefaultStore` source to `SqliteStore` owner, or existing
`SqliteStore` source to `SqliteStore` owner. A DefaultStore source must have the
ordinary MatrixStore tables and may legitimately have no `DeviceTrustState` or
the exact ordinary table with legacy rows: historical v2 upgrade and a pre-fix
Desktop reader could create it even while DefaultStore sidecars remained
authoritative. A SqliteStore source must have that table. Reject malformed table
topology, but the table's presence, absence, emptiness, or contents never infer
the source class. MindRoom explicitly supplies `DefaultStore`: in the same
adoption transaction, create the table when absent or replace its rows when
present, copying each current effective sidecar fact for an exact `DeviceKeys`
identity. Preserve DefaultStore's existing priority—verified, then blacklisted,
then ignored—and its parsing behavior; unmatched/stale entries remain inert.
Preserve all three
sidecar files byte-for-byte as rollback/legacy artifacts, and never use or
mutate them from the durable-owned session. Carry `SqliteStore` immutably as the
validated runtime class on the returned `StoreBootstrap` for Task 4B, without a
new ingestion table or persisted class marker. Keep the public
`open_ingestion_store()` signature unchanged; it remains fail-closed for a
populated unmarked database because it has no private configured-class input.
Retain schema v1, exactly five ingestion tables, and only the authenticated nullable Frame
`callbacks_claimed_revision` column. Preserve all MatrixStore, Olm/Megolm,
token, and inert recovery rows byte-for-byte; the sole ordinary-store mutation
is the explicit DefaultStore-to-SqliteStore trust conversion above. Inject every
bootstrap failure—including trust-table create, legacy-row delete,
post-delete/pre-first-insert, every trust insert, and ingestion schema/row
boundaries—and prove all-old/all-new recovery across both trust conversion and
ingestion bootstrap. Reject partial/mixed topology, wrong identity/version
or pickle key, a live legacy-store lease, and repeat adoption before DML.
Authenticate the exact account pickle without calling a loader that could
upgrade/rewrite it.

The same private entrypoint is the only configured-class restart path for an
already marked database. On every process reopen, the caller explicitly
supplies `SqliteStore` as the owned runtime class; reauthenticate that topology,
account/pickle, identity, foreign keys, and all ingestion state before Task-3
writer/source rotation DML, then return a newly bound `StoreBootstrap` without
re-running adoption or rereading sidecars. A second live or concurrent adoption
rejects; a clean marked reopen does not. Test both source-class adoption paths
and the SqliteStore marked restart.

Every exact built-in filesystem `DefaultStore` and `SqliteStore`, plus a
subclass inheriting the built-in `_create_database()` unchanged, must hold an
internal shared lifetime sidecar/file-identity lease. Ingestion adoption takes
the exclusive form and holds it through session close. Multiple participating
ordinary same-file stores therefore retain upstream behavior; exclusive
adoption rejects any live participating built-in-factory store in any process.
The privately borrowed ingestion store reuses the exclusive owner and opens no
second connection. Release follows the existing database-connection/finalizer
path for ordinary stores and the one ingestion close owner for adopted stores.
A subclass overriding `_create_database()` retains its pre-candidate extension
behavior and public API but is an uncoordinated legacy extension: it is
ineligible for `_open_configured_ingestion_store()` and must not open, or have a
live connection to, any path selected for configured ingestion. The private
opener accepts only exact `DefaultStore`/`SqliteStore` classes before lock, SQL,
sidecar access, or filesystem mutation. This is an explicit deployment
precondition, like stopping a pre-candidate binary before adoption.
Every ordinary `DefaultStore` trust-sidecar read or write additionally enters
one shared ownership guard for the whole semantic method—even multi-file trust
mutations and `load_device_keys`—with per-I/O assertions, including after its
physical database connection closes. Exclusive adoption therefore cannot
snapshot a transient partial trust update.
The durable-owned store is always `SqliteStore` and has no sidecar operations.
Deployment must stop the old binary before candidate adoption; a dormant
pre-candidate connection cannot retroactively participate in the new sidecar
protocol. Cover two ordinary same-file stores, shared/exclusive cross-process
rejection, process/thread close, cancellation, and stale-file identity in
`store_owner_test.py`, `journal_test.py`, and `crash_recovery_test.py`.

**Commit:** `feat: adopt populated matrix stores durably`

### Task 4B: exact store-to-session lease transfer

Preserve the existing public state machine and exact `open_ingestion()`
signature: public session-first remains successful, and a prior direct public
`StoreBootstrap.open_matrix_store()` continues to make public session claim
fail without consuming the bootstrap. Add a non-exported
`_open_owned_ingestion()` factory for MindRoom. It privately creates the one
borrowed `SqliteStore` that Task 4A validated and bound immutably on the
bootstrap from the bootstrap's exclusive owner, attaches that exact object
to the client before Olm initialization, and atomically transfers journal/store
ownership to the session once. Reject a foreign store, second store/session,
use after transfer, wrong-thread close, and double close. Cancellation and close
revoke the client and store before closing the shared connection and finally
releasing the exclusive lease. Add export and `inspect.signature` tests proving
the public factory/state machine is unchanged and the owned factory/protocol is
not exported from `nio` or `nio.ingest`.

Task 4B also owns the true new-device branch needed after password or SSO login
returns credentials but before any sync, key upload, or ordinary store open.
Under the same exclusive owner, generate one canonical unshared `OlmAccount`
and atomically create exact `SqliteStore` v10 topology, its one
StoreVersion/account row, and the five ingestion tables/owner/source rows. The
borrowed store loads that committed account rather than generating a second
one. Every injected boundary is all-absent or complete all-new; a committed
crash reopens through the configured marked path. Cover the fresh SqliteStore
branch, no-account/no-second-account cases, and prove the temporary login client
performs no sync, key upload, or store creation.

**Commit:** `refactor: transfer matrix store ownership to ingestion`

### Task 5A: MindRoom record receipt and semantic-idempotency kernel

This MindRoom slice executes before Task 4C. Replace the undeployed receipt
table's mandatory `event_id` foreign key with nonempty `record_id`; add no table
or migration. Every record gets a receipt. Return separate `receipt_new` and
`semantic_event_new` facts for fresh semantic events, later-batch matching event
identity, and exact receipt replay. Compare retained room/thread/kind/sender and
origin timestamp; conflict rolls back event, projection, receipt, and frontier.
Define literal transaction dispositions for event, lifecycle, history loss, and
compatibility-only records, and prove concurrent admission dispatches once.

**Commit:** `fix: deduplicate durable matrix events semantically`

### Task 4C: callback-free shared compatibility preparation

Parse Classic and private durable Sliding Frames into one private preparation
contract. Preserve the pre-fork callback-free mutation order exactly: token;
to-device decrypt/mutation; invited/joined room state, timeline (including
Megolm), ephemeral, and account-data projection; presence/global projection;
expired-verification mutation; device-list/OTK/fallback handling; then key
request collection. Callback fanout is a later, separate phase. A same-Frame
room key must decrypt its later timeline event, and a same-response newly
projected encrypted room/member plus device-list change must queue that member
for key query. Produce stable event
IDs, decrypted `clear_json` or explicit failure, `RoomSnapshot` values, and a
fully authenticated retained-Frame inputs sufficient for Task 4F to reconstruct
completion after it has a claimed revision. Do not create `_FrameCompletion` in
this commit. Run no callbacks and expose no public Sliding response API.
Ordinary public response callbacks remain solely on Desktop's pre-fork sync
path.

Create `tests/ingest/preparation_test.py` and cover equivalent Classic/Sliding
output, both causal crypto-order cases above, explicit failure, and zero
callbacks.

**Commit:** `refactor: prepare durable frames without callbacks`

### Task 4D: atomic all-kind materialization and snapshots

In one owner transaction persist crypto writes, clear events, stable IDs,
aggregates, room snapshots, every `RecordKind` including `TO_DEVICE`, every
`LossRecord`, Work, and preparation revision. In that same transaction, make
the retained Frame envelope the authenticated durable owner of one canonical,
ordered outbound-maintenance plan: exact key-upload/query/claim and queued
to-device request bodies, deterministic domain-separated per-message to-device
transaction IDs, canonical operation kind plus minimal typed local response-
apply context, and an explicit pending/settled state for every operation.
Persist the exact encrypted to-device bodies and the Olm/session mutations that
produced them together; do not reconstruct ciphertext from an in-memory queue.
Wire JSON alone is insufficient because dummy and room-key-request subtypes
have distinct local settlement effects after restart.
This is canonical Frame payload, not a new table or column. Retain the Frame as
completion owner. Any failure after in-memory Olm mutation rolls back SQLite,
poisons that session object, and requires clean reconstruction before replay.
Prove literal fates for all record kinds, canonical plan/header authentication,
identical body/transaction-ID replay, all statement crash boundaries,
empty/nonempty Frame retention, and equal Classic/Sliding durable graphs.

**Commit:** `refactor: materialize all durable record kinds atomically`

### Task 4E: snapshot restore and receipt-gated apply/ack

Restore `AsyncClient.rooms` before incremental delivery. Reapply client/room
state idempotently; publish auxiliary callbacks only when `receipt_new` and
semantic event callbacks only when `semantic_event_new`. Duplicate receipts run
no callback and proceed to ack. Callback error/cancellation leaves Work
replayable; after the committed receipt, replay suppresses callback and acks.
No callback runs in either database transaction. Add only the underscored
`session._settle_batch(batch, receipt_new=..., semantic_event_new=...)` method
to the non-exported owned-session protocol; do not add a non-underscored method
to the public `IngestionSession` contract. MindRoom obtains that protocol only
from `_open_owned_ingestion()`.

**Commit:** `feat: settle durable records through compatibility callbacks`

### Task 5B: MindRoom all-record adapter and pump

Accept both origins and map every record/loss to Task 5A's disposition. Timeline
uses the existing classifier; history/context settles without a turn; lifecycle
uses membership/departure fences; loss uses room-history recovery; state,
ephemeral, account data, presence, and to-device are compatibility receipts.
Call `session._settle_batch()` with the two facts. Keep `max_records=1`; malformed
or unknown input never acks and no known record can block the FIFO forever.

**Commit:** `feat: consume every durable ingestion record`

### Task 4F: outbound maintenance, Frame completion, and coordinator progress

Only after Tasks 5A, 4E, and 5B exist, implement
`PREPARE -> HYDRATE_IF_REQUIRED -> WAIT_FOR_RECORD_ACK ->
MAINTAIN_OUTBOUND_CRYPTO -> CLAIM_AND_FANOUT_COMPLETION -> RETIRE ->
NEXT_SOURCE_HTTP`. After all Frame Work is acked but before claim/retirement,
run the callback-free upstream key upload, key query, key claim, and queued
to-device send maintenance in its exact persisted order. Replay only the exact
authenticated pending request body owned by the Frame. Parse each response
without callbacks, then apply its ordinary MatrixStore/Olm effects and mark that
operation settled in one owner transaction. That transaction also appends or
replaces any causally generated follow-up operation in the same authenticated
Frame plan before commit. Run a fixed loop to quiescence in upstream order;
notably key-claim may create a dummy to-device send, and successful
dummy/to-device settlement may create room-key re-requests. Every follow-up
stores exact body/ciphertext and a deterministic transaction ID before it can be
sent. Each also stores the authenticated semantic subtype/fields needed to
reproduce local dummy or room-key-request settlement after restart. Any
exception or rollback after an
in-memory crypto mutation poisons the owned session and requires reconstruction
before replay. Retryable network failure leaves the Frame retained. A queued
to-device resend uses the identical transaction ID and body, so the server
deduplicates the semantic send. Key upload/query replay the identical body.
`/keys/claim` has no Matrix idempotency key: response loss may consume another
remote one-time key on an identical retry, so do not claim remote at-most-once;
claim only locally atomic application of one successful response and no
duplicate downstream event/callback. Auth errors such as `M_UNKNOWN_TOKEN` or
`M_FORBIDDEN` retain the pending Frame/operation and enter the existing
permanent-auth shutdown/health path. Retryable HTTP/Matrix errors retain the
same body with bounded cancellable backoff. No error response settles an
operation or fans out silently. No next source poll can pass this state.

Define a private non-exported `_FrameCompletion` with authenticated
`frame_id`, transport, source epoch, request ID, staged revision, and claimed
revision, reconstructed only from a fully authenticated retained Frame/owner.
The non-exported `_OwnedIngestionSession` holds one
`Callable[[_FrameCompletion], Awaitable[None]] | None` sink installed only by
MindRoom's private 5C factory; no registration method is added to public
`IngestionSession` or `open_ingestion()`. Claim completion only after no Work
references the Frame and outbound maintenance succeeds. Persist the claim
before invoking the sink outside transactions. Sink raise, cancellation, or
process death leaves the Frame durably claimed; restart retires it without a
second invocation. Empty Frames use the same path. Hydration may run while the
next source sync is fenced. Ack and close wake the runner without lost wakeups.
Only malformed, authentication, or conflict states are terminal BLOCKED.

Add causal tests for exact key upload/query/claim/to-device ordering; canonical
Frame-owned bodies and deterministic to-device transaction IDs; response loss
and restart at every operation; byte-identical to-device ciphertext/ID replay;
the explicit `/keys/claim` retry limit above; statement failure/rollback while
applying each maintenance response; maintenance-before-claim; authenticated
completion reconstruction; claim→dummy and dummy-success→key-rerequest chains
with rollback/poison/reopen at every append/replace boundary; sink
success/raise/cancel/crash after claim; per-operation retryable/auth error
responses and close/cancel during backoff; claimed
restart retirement; and absence of `_FrameCompletion`,
`_OwnedIngestionSession`, and the owned factory from public exports/signatures.

**Commit:** `feat: complete durable frames after record settlement`

### Tasks 5C and 5D: session factory, then sole MindRoom engine

5C wires fresh and restored login through the non-exported
`_open_owned_ingestion()` factory, exact per-device adoption/lease, client
attachment using the authenticated SqliteStore runtime binding, session claim,
and private completion sink before Olm initialization; poison rebuilds a new
client/Olm object. In the same 5C commit, replace Desktop pairing's raw
read-only fallback with the internal live-bot controller-identity resolver
before any bot can acquire the exclusive lease. 5D makes
`Bot.sync_forever()` use the durable
runner/pump as MindRoom's only candidate engine, while existing
`matrix_sync.mode` chooses only Classic or Sliding transport. Preserve health,
first-sync, invites/membership, presence, E2EE/to-device pairing, RTC,
permanent-auth error, cancellation, and close behavior. Add no environment
latch, control room, `probe`, YAML engine toggle, or public nio API. The 5C
resolver is injected by the command/orchestrator boundary. It
must resolve the selected target entity through the live bot registry and read
the exact user ID, device ID, and Ed25519 key from that bot's already-owned
client/Olm account; it never opens the store. Missing/not-running/mismatched
targets fail closed. The pre-5C raw read-only compatibility path is removed or
made unreachable in 5C before exclusive ownership exists. Cover setup and
confirm when the serving bot is
or is not the target, target restart/replacement, and absence of any second
MatrixStore/SQLite open under the exclusive lease.

For password/SSO credentials, prove the temporary no-store/no-sync client's
HTTP session closes on success, login error, owned-factory failure, and
cancellation before exclusive construction. The appservice direct-credential
path creates no temporary MatrixStore or client before the owned factory.
Bot owns the ingestion session separately from its existing journal handle.
Shutdown/config reload joins runner and pump, revokes client/store, closes the
owned session to release the exclusive lease, then closes HTTP; every cleanup
lane runs despite another's error, and replacement registration/start waits for
confirmed lease release. Cover startup failure and bot replacement/reload.

**Commits:** `feat: open durable matrix sessions in place`; then
`feat: activate durable matrix ingestion`

**Production files**

- `src/nio/client/async_client.py` and `base_client.py` for a reusable no-recovery response-preparation boundary
- `src/nio/responses.py` for private durable Sliding parsing without a public AsyncClient Sliding response API
- `src/nio/ingest/reducer.py`
- `src/nio/ingest/model.py` and `serialization.py`
- `src/nio/ingest/coordinator.py`
- `src/nio/store/sync_journal.py` and `_ingestion_store_owner.py`
- `src/nio/store/_sync_journal_preflight.py`
- `src/nio/store/sync_journal_schema.py` and `_sync_journal_rows.py`
- `src/nio/store/_sync_journal.py`
- `src/nio/store/_sync_journal_port.py`
- `src/nio/store/_sync_journal_plan.py`
- transport adapters only where normalization lacks parity data

**Tests**

- `tests/ingest/reducer_test.py`
- `tests/ingest/materializer_test.py`
- shared Classic/Sliding response fixtures

Build a differential behavior matrix covering at least:

- empty sync;
- room hydration and ordinary timeline events;
- invite, join, knock, leave, and membership transitions;
- encryption state and encrypted events;
- to-device, device-list, fallback-key, account-data, presence, and ephemeral traffic;
- limited history, gaps, eviction, and re-entry;
- malformed/conflicting records and repeated BLOCKED behavior;
- callbacks and response-state projection.
- same-Frame Olm room-key arrival followed by Megolm timeline decryption;
- rollback after each crypto/store/Work/marker statement, session poisoning, and clean reopen;
- Frame completion and one private `_FrameCompletion` hook for empty and nonempty Frames;
- `AsyncClient.rooms` restoration from persisted snapshots before incremental response delivery.

For equivalent normalized input, both transports must produce the same record fates and durable graph. A transport adapter may normalize wire differences; it may not choose a different fate engine.

Add one frame-scope compatibility preparation phase before Work becomes application-visible. The MindRoom AsyncClient must adopt the ingestion-owned MatrixStore so ordinary E2EE rows and the five ingestion tables share one SQLite owner/transaction. Preserve the exact pre-fork callback-free mutation order: token; to-device; invited/joined room state, timeline, ephemeral, and account data; presence/global state; expired verifications; device-list/key-count/fallback controls; then key-request collection. This ordering must causally prove both same-Frame room-key decryption and same-response encrypted-room membership before device-list handling; callback fanout remains separate. Persist stable event IDs, decrypted `clear_json`, `RoomSnapshot`, every `RecordKind` including `TO_DEVICE`, aggregates/Work, and the preparation revision atomically. On rollback after Olm memory mutation, poison/close the session and reconstruct it from the rolled-back database; never retry the same object.

Implement ownership through the non-exported owned-session factory, not two independent opens and not a public state-machine reversal. The factory privately creates the borrowed `SqliteStore` from the bootstrap's exclusive owner, consumes the immutable runtime-class binding established by the private configured opener, accepts only that object and authenticated topology, transfers journal/store to `_OwnedIngestionSession`, and revokes/closes them once. Existing public `open_ingestion()` remains session-first; direct public store-first continues to reject a later public session. Add causal lifecycle and public signature/export tests for successful transfer, cancellation, preparation rollback/poison, close ordering, and clean reopen. Prove `replace_rotated_device_keys=True` rolls back `DeviceKeys` and `DeviceTrustState` together, and prove durable preparation/maintenance performs no legacy trust-sidecar I/O. Do not weaken the single-process/thread/file-identity/writer-epoch fences. For a true new device, the same factory takes credentials from a temporary no-store/no-sync login client and, under one exclusive owner, atomically creates exact `SqliteStore` v10 topology, one canonical unshared Olm account row, and the five ingestion tables/initial rows before constructing the borrowed store. Crash boundaries are all-absent or complete all-new, and the committed account is loaded rather than regenerated.

Before that lease exists, add a non-exported exclusive configured opener for the populated MindRoom per-device database. Its caller must supply the exact source class, and it authenticates exact account/device/store-v10 ownership plus topology against that class. A supplied-Default database may lack `DeviceTrustState` or contain its exact ordinary topology and legacy rows because historical v2 upgrade and a pre-fix Desktop identity reader could create it while sidecars remained authoritative; malformed topology rejects, and presence/content never infers source class. A SqliteStore source requires the table and is adopted without class conversion. A DefaultStore source is converted in that same transaction by creating the table when absent or deleting/replacing its inert rows when present, then copying effective current sidecar facts for exact `DeviceKeys` identities using existing DefaultStore parsing and verified→blacklisted→ignored priority; unmatched stale entries stay inert, and all three source sidecars remain byte-for-byte unchanged. Return immutable `SqliteStore` runtime binding on `StoreBootstrap` for private Task-4B use, but persist no ingestion class marker and leave the public `open_ingestion_store()` signature and populated-unmarked rejection unchanged. The same transaction adds the five v1 ingestion tables/initial rows and otherwise leaves every existing Olm account pickle, Megolm/session/key row, sync token, and inert historical recovery table byte-identical. Every injected statement failure reopens with the exact prior ordinary shape and no ingestion marker; success reopens as complete SqliteStore plus exactly one marker/source. On later marked process reopen, require `SqliteStore`, reauthenticate the entire ordinary and ingestion state before writer/source DML, and return a new binding without adoption or sidecar reads. Reject mixed/partial topology, source-class/topology mismatch, wrong identity/pickle, unsupported store version, a simultaneously open client/store, or a concurrent/re-entrant adoption without mutation. Do not create a parallel ingestion database for the same device.

Keep the Frame after materialization as the completion owner. Add only one authenticated nullable `callbacks_claimed_revision` column to `NioIngestFrame` while retaining schema version 1 and exactly five tables. One-record receipts suppress record callback replay; when the Frame has no Work left, finish ordered outbound crypto maintenance, claim its private transport-neutral `_FrameCompletion` sink before best-effort fanout, then retire it. Empty Frames use the same path. Persist/restore the existing `RoomSnapshot` carrier before incremental delivery. No callback runs in either database transaction.

Replace the current “any remaining Frame is blocked” coordinator branch with the explicit private progress machine bound in Task 4F, including `MAINTAIN_OUTBOUND_CRYPTO`. A prepared retained Frame yields/wakes the concurrent MindRoom pump instead of raising; first-room HELD Work can complete its hydration HTTP; the next source-sync HTTP remains fenced until every record receipt/ack, outbound maintenance, private completion claim, and retirement settles. Only malformed/auth/conflict states are `BLOCKED`. Add causal concurrent runner+pump tests for empty and nonempty Classic/Sliding Frames and first-room hydration, proving the second source poll occurs only after completion and leaves zero Frame/Work; add crash, cancellation, close, and lost-wakeup coverage. Do not add a progress table.

A retained Frame is retry state, not a successful terminal fate. For both transports, successful encrypted, to-device, device-list, OTK, fallback-key, state, account-data, presence, ephemeral, lifecycle, and loss responses must end with zero permanently retained Frames and zero unacknowledgeable Work. Each known input must resolve to exactly one explicit owner: MindRoom event/lifecycle/loss Work or compatibility-owned MatrixStore/callback state. Malformed input remains durably blocked and visible.

Task 1 must already have removed every dedicated diagnostic entrypoint. Preserve every record-fate invariant: successful input has Work or a committed compatibility owner; only malformed, failed, or in-progress input may retain its Frame—never silent discard.

Run both full adapter suites plus the shared reducer/materializer suite and statics.

The preceding cross-cutting matrix governs Tasks 4C through 4F. Each subtask runs its
focused tests and statics before commit; after 4F, run both full adapter suites,
the shared reducer/materializer/coordinator/delivery/crash suites, and all nio
statics.

## Task 5: Complete the interleaved MindRoom slices

Work in `/work/dev/mindroom` with test-driven development.

Task 5A executes after Task 4B; Task 5B after Task 4E; Tasks 5C and 5D after
Task 4F, as specified above. The following files and acceptance matrix apply
across all four MindRoom slices; they are not authority to reorder them.

**Production files**

- `src/mindroom/event_journal/journal.py`
- `src/mindroom/event_journal/schema.py`
- `src/mindroom/event_journal/models.py`
- `src/mindroom/event_journal/store.py`
- `src/mindroom/event_journal/views.py`
- `src/mindroom/bot.py`
- `src/mindroom/matrix/durable_ingestion.py`
- `src/mindroom/matrix/sync_loop.py`
- `src/mindroom/matrix/journal_ingress.py`
- `src/mindroom/matrix/client_session.py`
- `src/mindroom/matrix/users.py`
- `src/mindroom/commands/handler.py`
- `src/mindroom/commands/desktop_commands.py`
- `src/mindroom/desktop/identity.py`
- `src/mindroom/orchestrator.py` only for the internal live controller-identity resolver
- the appservice login helper if it independently constructs/restores agent clients
- the existing sync-certification/checkpoint modules only to remove old-engine dependencies

**Tests**

- `tests/test_durable_ingestion_admission.py`
- `tests/test_event_journal_crash_matrix.py`
- configuration/runner tests that already cover Matrix mode selection

Write causal failures for:

1. Exact batch sequence/digest replay remains `DUPLICATE`.
2. The same existing `(principal_id, event_id)` in a later Frame/batch, with matching retained room/thread/kind/sender/origin-timestamp headers, advances the new batch receipt/frontier but creates no second application dispatch.
3. The same event ID with a conflicting retained header rolls back the event, projection, receipt, and frontier transaction.
4. Concurrent attempts cannot dispatch twice.
5. Classic behavior is unchanged.
6. Existing `matrix_sync.mode` selects Classic or Sliding for the durable runner without a control room, `probe`, or one-message production scope.
7. `Bot.sync_forever()` uses the durable runner as the candidate's only MindRoom sync engine; `matrix_sync.mode` selects transport, not engine. Activation is deployment of the reviewed candidate cohort, not a canary environment variable or a new YAML option.
8. Every nio `RecordKind` and `LossRecord` has one literal disposition: semantic timeline admission, room-lifecycle fencing, room-history loss obligation, or compatibility-owned state/callback settlement. No known record can occupy the FIFO without an acknowledgement path.
9. Invite/membership, E2EE/to-device, RTC/pairing, response health, first-sync, presence, permanent-auth-error, cancellation, and close behavior remain available through the durable runner and compatibility owner.
10. Restored-login and fresh-login paths locate/adopt the existing per-device MatrixStore before Olm initialization; its account pickle, inbound/outbound sessions, device keys, and sync-token rows survive adoption and restart byte-for-byte.

Keep `max_records=1`. Reuse the existing `journal_events` unique identity, immutable clear columns, and admission transaction. Correct the unmerged `matrix_ingestion_receipts` definition by replacing its mandatory `event_id` foreign key with nonempty `record_id`. Every EventRecord/LossRecord batch receives a receipt; semantic Matrix events independently bind `journal_events`. Extend `IngestionBatchAdmission` with `record_id`, optional membership/event/projection fields, and return separate `receipt_new`/`semantic_event_new` facts so callbacks and application dispatch are gated correctly. This is an in-PR schema-definition correction, not a deployed migration. Do not add a table, event-identity database to nio, retain raw settled payload for a second digest, or add a canary-only receipt path.

Run focused RED/GREEN, full journal/admission/crash suites, statics, and diff checks.

Use the four commit subjects bound in Tasks 5A through 5D above. Run each focused
RED/GREEN gate before its commit, then the full MindRoom suite and statics after
5D.

## Task 6: Restore the pre-fork public sync path and verify compatibility

Use `69aa99c51f354980bf5306a85b4c7b7d71ff3217` as the exact local upstream boundary. Before observation, remove every normal import, export, registration, and call from `sync()`, `sync_forever()`, response handling, and store opening into fork-only recovery/checkpoint machinery while leaving those source modules present for the zero-use gate. Restore the smallest boundary implementation of request, response application, room projection, token persistence, and callback dispatch. Do not route desktop through the durable batch engine and do not replace unrelated later E2EE/cross-signing/security work with a whole-file checkout.

Use test-driven development against behavior, not deleted helper topology. The retained upstream public contract—not fork-specific recovered/unrecovered response fields or checkpoint outcomes—is authoritative.

Create a retained compatibility matrix for:

- `sync()`, `sync_forever()`, `stop_sync_forever()`, `receive_response()`, `run_response_callbacks()`, `add_response_callback()`, and `close()` signatures and return behavior;
- `add_event_callback()`, `add_ephemeral_callback()`, `add_global_account_data_callback()`, `add_room_account_data_callback()`, `add_to_device_callback()`, and `add_presence_callback()` signatures, ordering, exceptions, and re-entry;
- `AsyncClient.rooms` and store projection timing;
- `synced` set/clear behavior, first-sync-before-auxiliary ordering, callback exception propagation, cancellation, and stop-flag reset;
- desktop's unchanged `sync_forever()` plus to-device callback path;
- ordinary sends and error responses.

Explicitly remove fork-added `AsyncClient.sliding_sync()`, `sliding_sync_forever()`, `SlidingSyncResponse` types, recovery/checkpoint/admission methods, recovery response fields, and their top-level exports from the public compatibility contract. Durable Sliding remains under `nio.ingest`, not `AsyncClient`.

Before removing those nio symbols, require a fail-closed MindRoom source scan proving no production reference remains to `add_event_admission_callback`, `SlidingSyncResponse`, `sliding_sync_forever`, Classic checkpoint acknowledgement/reset methods, recovered/unrecovered response fields, or recovery/token exports. This gate covers `bot.py`, `matrix/sync_loop.py`, `matrix/journal_ingress.py`, `matrix/client_session.py`, and sync certification/checkpoint helpers.

For MindRoom, run the same application-behavior fixtures through the durable runner with Classic and Sliding transport. There is no old-engine/durable-engine product toggle in the candidate. For desktop, compare the retained pre-fork public path before and after fork-recovery source deletion. Differences require either a bug fix or an explicit design amendment; do not bless them as internal changes.

Do not destructively downgrade an existing Matrix store or drop historical recovery tables. Fresh stores create only ordinary active tables; existing higher-version databases may retain inert historical tables, but ordinary open/sync/token/callback operations must neither read nor write recovery state.

For observation, run the candidate under external Python coverage and require zero executed lines in the named fork-recovery modules and the recovery/checkpoint regions of `async_client.py` and `database.py`. Do not add counters, persistence, or evidence hooks to production.

**Commit:** `test: bind durable sync compatibility`

## Task 7: Run cross-database crash and parity verification

Use real nio and MindRoom components. Cover one event through:

```text
HTTP response -> Frame -> shared materializer -> Work -> batch
-> MindRoom event/projection/receipt/frontier -> nio ack
```

Inject deterministic crashes before and after response staging, materialization, batch freeze, MindRoom admission, and nio ack in this test harness. Include Sliding reopen and exact `M_UNKNOWN_POS` after admission-before-ack. This is the exact boundary proof; the live canary does not duplicate the hook.

Assertions:

- response/cursor atomicity;
- deterministic batch replay;
- exactly one application dispatch;
- conflicting identity/digest is an integrity failure;
- no Work or Frame loss;
- frontier advances only with its owning transaction;
- public callbacks remain outside database transactions.

Run complete affected nio and MindRoom suites, Ruff, type checks, Black, compilation, diff checks, and suppression inventories.

No production line may exist solely to expose canary evidence.

## Task 8: Run external Classic and Sliding canaries

Build exact wheels from the candidate commits in a fresh isolated environment. Use an explicitly pinned Synapse and ordinary production configuration. The operator may observe request/response sequence and existing Source/Frame/Work/batch/event-journal/frontier/application outcomes; it may not require product diagnostic tables, a control room, or a `probe` witness.

Classic must remain green on schema v1. If any post-prune schema change appears, stop and rerun Classic before using the earlier PASS.

Sliding must demonstrate:

- normal sync and one visible application result;
- an ordinary process kill/restart while a durable batch is outstanding; the exact admission-before-ack boundary is already proved by Task 7 and no production latch is reintroduced;
- restart with a fresh connection-scoped position;
- replay without a second application effect;
- exact current-position unknown-pos reset;
- two bounded idle observations without lost Work or duplicate dispatch.

Materialize a concise allowlisted evidence record and independently review it. A canary PASS authorizes activation observation, not release or legacy deletion.

Because Task 4 changes the schema-v1 Frame layout and E2EE ownership after the earlier Classic qualification, rebuild and rerun the full Classic canary against this exact candidate before accepting any Sliding result. The old Classic PASS cannot be rebound to the new column or store ownership.

## Task 9: Activate, observe, and only then delete fork recovery

Activation remains inside the linked change set. The candidate's `Bot.sync_forever()` already owns the durable runner, and deployment selects the observation cohort; no product opt-in or canary-agent environment variable is introduced. The following thresholds are fixed:

1. Deploy the reviewed candidate to the MindRoom observation cohort.
2. Observe for one uninterrupted 24-hour interval spanning 1,000 successful source polls and three recorded clean process restarts. Require zero duplicate visible effects, zero unexplained event loss, no permanently growing Frame/Work backlog, and external coverage showing zero fork-recovery execution.
3. Run desktop on the restored upstream-style `sync_forever()` for at least two hours and 100 successful responses, including one process restart and one real to-device callback. Require no callback/order/authentication failure and external coverage showing zero fork-recovery execution.
4. Preserve exact run start/end times, candidate hashes, counts, coverage data, and failure totals in the final report.

If either observation fails, do not delete legacy source. Fix the durable path or restore Task 6's old-engine imports/calls inside the linked branches, then rerun all affected verification and the full observation interval; there is no hidden runtime fallback in the candidate.

Only after both observations pass, remove:

- `src/nio/client/sync_recovery.py`;
- `src/nio/client/sync_reset_fence.py`;
- `src/nio/client/sync_response_ordering.py`;
- `src/nio/client/sliding_membership.py`;
- `src/nio/recovery_abandonment.py`;
- `src/nio/sliding_sync_tokens.py` where no longer required;
- recovery/checkpoint code in `async_client.py`, `database.py`, `responses.py`, `base_client.py`, and `store/models.py`;
- implementation-detail tests for those internals.

Retain the upstream public sync/callback implementation and port every still-relevant parity test before deletion. Verify desktop and bot again after deletion; desktop never changes ingestion engine.

**Commit:** `refactor: remove superseded sync recovery machinery`

## Task 10: Final consolidation and linked-PR handoff

Run:

- the complete nio test suite;
- the complete affected MindRoom test suite;
- Classic and Sliding compatibility matrices;
- cross-database crash matrix;
- desktop smoke/soak;
- Ruff, type checking, Black, compilation, diff checks, and suppression scans;
- independent code/evidence review.

Measure net logical lines against `origin/main@6ed2b98`:

```text
runtime src      <= +4,500
tests            <= +15,000
docs and scripts <= +1,000
```

If any limit is exceeded, simplify or delete before handoff. Do not move the baseline or limit.

Also measure MindRoom against `0acaea2baf05a4c41cce7497cfcdf4880afc6d04`: runtime ≤ +350 and tests ≤ +1,000, and require it to be net smaller than `925df7f3cbcc39d4904140efd9d16b93c77238ea`.

Use this fail-closed gate from clean tracked worktrees; unrelated untracked user files are reported but are not part of either diff:

```bash
set -euo pipefail
test "$(git rev-parse 6ed2b9817d2bc9de30dc72942f9cb867d829283b^{commit})" = \
  6ed2b9817d2bc9de30dc72942f9cb867d829283b
test -z "$(git status --porcelain --untracked-files=no)"
net_lines() {
  git diff --numstat 6ed2b9817d2bc9de30dc72942f9cb867d829283b -- "$@" |
    awk '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { n += $1 - $2 } END { print n + 0 }'
}
test "$(net_lines src)" -le 4500
test "$(net_lines tests)" -le 15000
test "$(net_lines docs scripts)" -le 1000

cd /work/dev/mindroom
test "$(git rev-parse 0acaea2baf05a4c41cce7497cfcdf4880afc6d04^{commit})" = \
  0acaea2baf05a4c41cce7497cfcdf4880afc6d04
test -z "$(git status --porcelain --untracked-files=no)"
mr_net() {
  git diff --numstat 0acaea2baf05a4c41cce7497cfcdf4880afc6d04 -- "$1" |
    awk '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { n += $1 - $2 } END { print n + 0 }'
}
test "$(mr_net src)" -le 350
test "$(mr_net tests)" -le 1000
test "$(git diff --numstat 925df7f3cbcc39d4904140efd9d16b93c77238ea -- src tests |
  awk '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { n += $1 - $2 } END { print n + 0 }')" -lt 0
```

The final PR report must distinguish:

- upstream public compatibility retained;
- fork recovery implementation deleted;
- schema v1 and five-table topology;
- Classic and Sliding parity evidence;
- bot and desktop observation windows;
- exact commits, wheels, tests, canary evidence, and budgets;
- remaining limitations and rollback procedure.

Push only ordinary commits to the existing feature branches. Create one linked PR per repository and present them as one accept/reject decision; do not split either into phase PRs. No amend, rebase, force push, merge, release, or cutover claim is implied by completing this plan.

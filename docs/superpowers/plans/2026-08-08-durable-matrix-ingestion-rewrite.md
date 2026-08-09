# Durable Matrix Ingestion Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task by task, `superpowers:test-driven-development` for every behavior change, and `superpowers:verification-before-completion` before each commit or release gate.

**Goal:** Replace the current callback-, lock-, and recovery-patch-based sync subsystem with a durable ingestion stream in which every Matrix timeline event is either durably admitted to MindRoom, retained by nio for retry, or accompanied by an explicit durable loss record.

**Architecture:** A new `nio.ingest` package owns Classic Sync and Sliding Sync through one queue-driven coordinator, transport-specific pure adapters, a pure per-room reducer, a fresh version-1 ingestion journal, and one serialized replay-safe crypto worker. nio stages immutable frames and publishes immutable FIFO batches. MindRoom admits a whole batch in one event-journal transaction and only then acknowledges it to nio. The two databases never share a transaction; idempotent replay is the boundary.

The plan calls this replacement implementation “v2” when contrasting it with the callback architecture; its new wire and journal schemas both start at version 1.

**Tech Stack:** Python 3.12+, `asyncio.TaskGroup`, frozen/slotted dataclasses, `enum.StrEnum`, SQLite/Peewee for the nio-local journal and E2EE state, AES-GCM for staged payloads, MindRoom's existing SQLite/PostgreSQL event-journal abstraction, pytest, Hypothesis where state-machine coverage is valuable, Ruff, pre-commit, and uv.

## Global Constraints

- Both Classic Sync and Sliding Sync are first-class. Neither may be implemented as a compatibility wrapper around the other.
- There is one active ingestion source per Matrix account/device. Switching sources is an explicit epoch transition.
- The coordinator is the sole owner of mutable ingestion state. One serialized crypto worker separately owns all Olm/Megolm state. Network workers, the crypto worker, callers, and MindRoom exchange only immutable epoch-tagged commands and results.
- Allow one in-flight primary sync request per account/device. Recovery requests may run concurrently across rooms, one per room, because they return immutable epoch-tagged results.
- No application callback runs in the coordinator, under a nio lock, or inside a nio database transaction.
- A state transition becomes visible in memory, publishes a batch, or schedules a follow-up effect only after its journal transaction commits.
- A transport cursor may advance only when the corresponding immutable raw frame is durable in the nio journal.
- A frame may be retired only when every record is durably represented in a batch/loss, a generic ready row, or its room's durable recovery lane.
- A batch may be retired only after MindRoom's admission transaction commits and nio accepts a matching FIFO acknowledgement.
- No NIO ingestion persistence path may commit once per Matrix event. Its transaction count must scale with durable raw frames, recovery pages, bounded crypto groups, materialized batches, and acknowledgements—not with the number of records inside those units. MindRoom and desktop admission each use one downstream transaction per delivered batch; later application work is outside this ingestion invariant.
- When ready work is backlogged, the materializer takes deterministic oldest eligible heads across already-durable frames and room lanes until none remains or the next head would exceed configured record/byte capacity, then publishes that batch. When no backlog exists, it publishes available work immediately; it never waits for a future record, and v1 adds no batching timer or artificial latency window.
- Recovery is per room. A stuck room must not stop unrelated rooms, global account data, or another transport frame from making progress.
- A terminal loss record is ordered before any held live records it releases.
- Olm/Megolm mutations and their replay receipt commit in the same SQLite transaction. A separate receipt write is not sufficient.
- Batch IDs and SHA-256 digests are computed from canonical bytes by nio, recomputed when read, and recomputed independently by MindRoom. No caller-supplied fingerprint is trusted.
- Unknown batch schemas, record kinds, or loss reasons fail closed before MindRoom writes a receipt or nio receives an acknowledgement.
- The replacement never parses or migrates old sync/recovery rows. A one-time offline initializer deliberately abandons that state, preserves only the current E2EE tables, and cold-starts a new stream.
- No backwards-compatibility shim, dual reader, legacy-row importer, or runtime fallback survives activation. Dormant old sync tables may remain physically present until a later schema cleanup, but v2 never reads them.
- Keep nio's per-device E2EE database separate from MindRoom's event journal. Do not introduce a distributed transaction.
- Bind each nio stream to the MindRoom journal's durable generation UUID. A replaced/restored consumer journal must fail closed rather than resume from nio's advanced cursor.
- Raise `mindroom-nio`'s Python floor to 3.12 before using `StrEnum` or `TaskGroup`; MindRoom already requires 3.12.
- Planning envelope across both repositories: 6,000–9,000 new and 3,500–4,500 deleted production lines, or roughly +1,500 to +5,500 net. Record actual additions/deletions after Tasks 2, 7, 10, and 12; review any task more than 50% above its estimate immediately, and stop for an architecture review if projected net growth exceeds +6,000. Do not wait until deletion to discover the design is too large.
- Timed micro-batching, compression, Rust extensions, and speculative SQLite tuning are out of scope until Task 10 measurements identify a concrete bottleneck.
- Do not commit this plan to the implementation branch.

## Why This Is a Rewrite

The old subsystem has several independent owners of the same logical transition: the HTTP request path, `AsyncClient` response application, recovery callbacks, retained callback tasks, application-owned Classic acknowledgement, reset fences, room gates, and persistence capability branches. The recent sequence of fixes was not random bad luck; it was the cost of maintaining invariants across those owners.

The rewrite removes that topology instead of translating it:

```text
Matrix HTTP
    │ immutable result tagged with source/request epoch
    ▼
transport adapter ──► durable raw frame
                         │
                         ▼
                 crypto worker receipt
                         │
                         ▼
source row + affected room lanes ──► immutable FIFO SyncBatch
                                             │
                                             ▼
                              MindRoom one-write admission
                                             │ commit
                                             ▼
                                      nio FIFO ack
```

The acknowledgement boundary is durable journal admission, not model completion, tool execution, delivery, or an outgoing Matrix response. Holding nio work until semantic completion would create head-of-line blocking across model and network latency without improving recovery correctness.

## Corrections Incorporated From External Review

The reviews identified eight real gaps in the earlier drafts. This plan resolves them as follows:

1. Python is raised to 3.12 in Task 1, before any `StrEnum` or `TaskGroup` use.
2. The replacement does not attempt to reinterpret legacy pending events, gaps, tokens, or sticky losses. Task 13 makes the destructive sync-state reset explicit, preserves E2EE state, and records `BASELINE_LOST` for rooms already authoritative in MindRoom before new HTTP begins.
3. `BatchRef` identity is computed and validated from canonical bytes; `dataclasses.replace()` with a stale digest must fail.
4. Task 7 implements same-transaction crypto receipts, including Olm to-device replay. The plan no longer assumes replay safety.
5. The new stream remains test-only until crypto ordering and every durable/best-effort record class are implemented. MindRoom activation happens later.
6. MindRoom owns a local loss enum with exhaustive mapping. A future unknown nio value fails closed.
7. Membership transitions now have one owner and an explicit epoch order: unresolved `E` loss, then lifecycle `E+1`, with stale `E` recovery ignored and genuine `E+1` failure retained.
8. Persistence cardinality is normative and measured: frames, pages, bounded crypto groups, materialized batches, and acknowledgements create transactions; individual records do not. Backlog coalescing is immediate and limit-driven, never timer-driven.

One review premise is deliberately rejected: this is not a `MatrixStore` v10-to-v11 migration that drops five tables. The new journal has its own version-1 schema. Supporting arbitrary old sync state would preserve the wrong boundary; the only supported in-place data preservation is the exact current E2EE schema used by this fork at the cutover commit.

## Frozen Public Contract

Create these public values in `src/nio/ingest/model.py`. They are frozen and slotted. Persist enum strings, never enum ordinals.

```python
class TransportKind(StrEnum):
    CLASSIC = "classic"
    SLIDING = "sliding"


class RecordKind(StrEnum):
    TIMELINE = "timeline"
    STATE = "state"
    EPHEMERAL = "ephemeral"
    ROOM_ACCOUNT_DATA = "room_account_data"
    GLOBAL_ACCOUNT_DATA = "global_account_data"
    PRESENCE = "presence"
    TO_DEVICE = "to_device"
    ROOM_LIFECYCLE = "room_lifecycle"
    DECRYPTION_UPDATE = "decryption_update"


class SystemOriginKind(StrEnum):
    FRESH_START = "fresh_start"
    CONSUMER_RESET = "consumer_reset"
    MEMBERSHIP_CHANGE = "membership_change"
    SOURCE_REBIND = "source_rebind"
    STORE_VALIDATION = "store_validation"


class LossReason(StrEnum):
    EVENT_LIMIT = "event_limit"
    FETCH_FAILED = "fetch_failed"
    BASELINE_LOST = "baseline_lost"
    UNVERIFIABLE = "unverifiable"
    CORRUPT_STORED_RECORD = "corrupt_stored_record"
    OVERSIZED_EVENT = "oversized_event"


class TimelineEventProvenance(StrEnum):
    LIVE = "live"
    RECOVERED = "recovered"
    HISTORY = "history"


class RoomHydrationStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class ConsumerBinding:
    journal_generation: UUID
    consumer_generation: UUID


@dataclass(frozen=True, slots=True)
class ConsumerBootstrap:
    binding_operation_id: UUID
    binding: ConsumerBinding
    first_sequence: int
    baseline_room_ids: tuple[str, ...]
    baseline_sha256: bytes


@dataclass(frozen=True, slots=True)
class RecordOrigin:
    transport: TransportKind
    source_epoch: int
    request_id: int
    frame_index: int


@dataclass(frozen=True, slots=True)
class SystemOrigin:
    kind: SystemOriginKind
    operation_id: UUID


@dataclass(frozen=True, slots=True)
class EventRecord:
    record_id: str
    kind: RecordKind
    origin: RecordOrigin
    room_id: str | None
    membership_epoch: int | None
    room_sequence: int | None
    event_id: str | None
    provenance: TimelineEventProvenance | None
    source_json: bytes
    clear_json: bytes | None


@dataclass(frozen=True, slots=True)
class RoomMemberSnapshot:
    user_id: str
    membership: str
    display_name: str | None
    avatar_url: str | None
    power_level: int


@dataclass(frozen=True, slots=True)
class RoomSnapshot:
    room_id: str
    membership_epoch: int
    own_membership: str | None
    encrypted: bool
    name: str | None
    canonical_alias: str | None
    topic: str | None
    avatar_url: str | None
    join_rule: str | None
    room_version: str | None
    guest_access: str | None
    power_levels_json: bytes | None
    members: tuple[RoomMemberSnapshot, ...]


@dataclass(frozen=True, slots=True)
class LossBoundary:
    prior_event_id: str | None
    prior_origin_server_ts: int | None
    start_token: str | None
    target_token: str | None


@dataclass(frozen=True, slots=True)
class LossRecord:
    loss_id: str
    origin: RecordOrigin | SystemOrigin
    room_id: str
    membership_epoch: int
    reason: LossReason
    boundary: LossBoundary
    detail_json: bytes


@dataclass(frozen=True, slots=True)
class BatchRef:
    stream_id: UUID
    sequence: int
    batch_id: UUID
    sha256: bytes


@dataclass(frozen=True, slots=True)
class SyncBatch:
    schema_version: int
    account_id: str
    device_id: str
    consumer: ConsumerBinding
    ref: BatchRef
    created_revision: int
    records: tuple[EventRecord | LossRecord, ...]
```

Canonical batch payload bytes exclude `batch_id` and `sha256` and include `schema_version`, Matrix account/device identity, both consumer-binding generations, `stream_id`, `sequence`, `created_revision`, and records. Compute:

```text
sha256 = SHA256(canonical_payload)
batch_id = UUIDv5(stream_id, f"{sequence}:{sha256.hex()}")
```

`SyncBatch.__post_init__` recomputes both values and rejects mismatches. Database decode and MindRoom admission recompute them again. `SyncBatch` is created through an internal `batch_from_records()` factory; the constructor remains defensive because Python cannot make a dataclass constructor a security boundary.

`RoomSnapshot` is a read model, not part of batch identity. Derived properties such as `display_name`, `member_count`, and `is_group` are pure calculations over frozen fields. The coordinator owns a private dictionary and exposes only `room_snapshot(room_id)`, `room_hydration_status(room_id)`, `wait_room_ready(room_id)`, `room_ids()`, and an explicitly requested immutable `room_directory_snapshot()`; it never exposes a live mapping that can change during iteration. A commit replaces/removes only affected keys and preserves unchanged `RoomSnapshot` identities. Full-map copying happens only when a caller explicitly requests a directory snapshot, never on a source commit. A baseline room is `PENDING` until authoritative current state commits, then `READY`; a confirmed not-joined/forbidden room is `UNAVAILABLE`. Outbound encryption requires `READY`. The old mutable `AsyncClient.rooms` cache is not an authority and is removed at final cutover.

Frame, record, and loss identities are deterministic too:

```text
frame_id  = UUIDv5(stream_id, source_epoch:request_id:frame_sha256)
record_id = UUIDv5(stream_id, kind:room_id:event_id)             # Matrix event ID exists
record_id = UUIDv5(stream_id, kind:crypto_input_sha256)          # app to-device/crypto result
record_id = UUIDv5(frame_id, frame_index:kind:source_sha256)     # truly frame-local signal
loss_id   = UUIDv5(stream_id, room_id:membership_epoch:origin_id:reason:boundary_sha256)
```

Transport/recovery losses retain the exact `RecordOrigin` that detected them. A loss created without a source frame uses a deterministic `SystemOrigin`: initial consumer attach uses `FRESH_START`, destructive existing-v1 reset uses `CONSUMER_RESET`, confirmed leave/forget uses `MEMBERSHIP_CHANGE`, offline source reset uses `SOURCE_REBIND`, and a room-keyed stored-row failure uses `STORE_VALIDATION`. `EventRecord` never accepts `SystemOrigin`. Membership epoch `0` is reserved for a fresh-start/reset placeholder loss before authoritative membership is hydrated; the first committed membership observation advances that room to epoch `1`.

An empty normalized frame advances only the nio-local staged cursor and is retired locally after reduction; it does not create an empty cross-database batch. Every public `SyncBatch` has at least one event or loss record.

Public entrypoint:

```python
bootstrap = nio.open_ingestion_store(
    store_path,
    account_id=account_id,
    device_id=device_id,
)
consumer = await principal_store.bind_sync_consumer(
    account_id,
    device_id,
    bootstrap.stream_id,
    binding_operation_id=bootstrap.binding_operation_id,
    first_sequence=bootstrap.next_batch_sequence,
)
await bootstrap.attach_consumer(consumer)
client = nio.AsyncClient(..., store_bootstrap=bootstrap)
ingestion = await nio.open_ingestion(
    client,
    bootstrap,
    replace(config, consumer=consumer.binding),
)
await ingestion.start()

batch = await ingestion.next_batch()
outcome = await ingestion.acknowledge(batch.ref)

await ingestion.close()
```

The transport kind is fixed for one running session. v1 has no live `switch_source()` API because Classic `next_batch`, Sliding `pos`, and Sliding to-device `since` are different continuity domains. Changing modes requires closing the session and running the explicit offline source-rebind operation described under source semantics.

`open_ingestion_store()` is mandatory and runs before `MatrixStore` construction. It acquires the v2 lifetime lock and opens an existing v1 journal or atomically creates one in a genuinely new database. A database containing E2EE/legacy data but no v1 marker raises `FreshIngestionRequired` without DDL; the operator must first run Task 13's explicit offline `initialize_fresh_ingestion_store(..., discard_sync_state=True)`. That initializer validates the one supported E2EE schema but never selects from an old sync table. The returned bootstrap retains the lock and file identity until transferred to `IngestionSession`; it is single-use.

A newly initialized/reset meta row has a fresh `binding_operation_id`, is unbound, and cannot perform Matrix HTTP. `bind_sync_consumer()` atomically binds that operation/stream/first-sequence tuple in MindRoom, then returns a `ConsumerBootstrap` containing the current journal/consumer generations plus MindRoom's durable active-room IDs and their canonical digest. `attach_consumer()` recomputes that digest, validates the operation and sequence, idempotently fills the nio binding, and creates a priority, hard-bounded `BASELINE_LOST` release plan for those rooms before enabling the owner. On ordinary restart the existing operation, binding, sequence, and baseline digest must match exactly. The guarantee horizon begins at that attach transaction; abandoned rows that existed only in the old nio sync tables are deliberately out of scope.

The frozen configuration is explicit and bounded:

```python
@dataclass(frozen=True, slots=True)
class ClassicSourceConfig:
    timeout_ms: int
    filter_json: bytes
    full_state_on_cold_start: bool = True


@dataclass(frozen=True, slots=True)
class SlidingSourceConfig:
    timeout_ms: int
    connection_name: str
    lists_json: bytes
    room_subscriptions_json: bytes
    extensions_json: bytes


@dataclass(frozen=True, slots=True)
class IngestionConfig:
    source: ClassicSourceConfig | SlidingSourceConfig
    consumer: ConsumerBinding
    max_staged_frames: int = 2
    max_unacknowledged_batches: int = 8
    max_records_per_batch: int = 256
    max_bytes_per_batch: int = 2 * 1024 * 1024
    max_record_bytes: int = 1 * 1024 * 1024
    max_crypto_inputs_per_commit: int = 32
    max_crypto_input_bytes_per_commit: int = 1 * 1024 * 1024
    sqlite_busy_timeout_ms: int = 2_000
    sqlite_write_retry_limit: int = 2
    max_concurrent_recovery_rooms: int = 8
    max_concurrent_room_hydrations: int = 8
    sliding_bootstrap_range_size: int = 100
    max_recovery_events_per_room: int = 10_000
    max_held_events_per_room: int = 10_000
    max_held_bytes_per_room: int = 32 * 1024 * 1024
```

These values control backpressure and resource use, not correctness. Tests exercise each value at 1 and at its default. Validate them against immutable implementation ceilings of 256 records, 2 MiB of canonical batch payload, and 1 MiB per canonical record; configuration may lower but never raise those ceilings. No batch-materialization or MindRoom admission transaction may exceed either batch ceiling. A room event above the record ceiling becomes a small `OVERSIZED_EVENT` loss containing only identity, size, digest, and boundary evidence; an over-limit account-wide or crypto-control record has no truthful room loss representation and fail-stops while retaining its raw frame.

`next_batch()` returns only the oldest unacknowledged batch. Cancellation has no state effect. `acknowledge()` is FIFO and idempotent for the current acknowledgement frontier: retrying the most recently acknowledged matching reference returns `AckOutcome.ALREADY_ACKNOWLEDGED`; an older stale reference or a wrong sequence, ID, or digest fails closed. Concurrent acknowledgement calls are rejected, so one persisted frontier is sufficient and acknowledgement metadata remains bounded.

A terminal coordinator/network/store/crypto failure is stored on the session. Existing durable batches remain drainable; once none remains, `next_batch()` and `wait_closed()` raise the typed terminal error. Transient HTTP errors follow the configured retry policy and do not masquerade as an empty stream. This replaces response-error callbacks.

`open_ingestion()` composes the authenticated `AsyncClient` created under that bootstrap with narrow network and crypto ports. It does not add ingestion locks, cursors, callbacks, or room lanes back to `AsyncClient`.

## Internal Interfaces

```python
class NetworkPort(Protocol):
    async def execute(self, request: NetworkRequest) -> NetworkResult: ...


class CryptoPort(Protocol):
    async def process(self, work: CryptoWorkUnit) -> CryptoResult: ...


class SyncSource(Protocol):
    def plan_request(
        self,
        state: SourceState,
        request_id: int,
    ) -> NetworkRequest | None: ...

    def normalize(
        self,
        request: NetworkRequest,
        result: NetworkResult,
    ) -> SourceResult: ...


class IngestionJournal(Protocol):
    def load_owner(self) -> OwnerView: ...
    def load_rooms(self, room_ids: frozenset[str]) -> dict[str, RoomAggregate]: ...
    def load_ready_heads(self, limit: int) -> tuple[ReadyHead, ...]: ...
    def commit(
        self,
        *,
        expected_revision: int,
        writer_epoch: UUID,
        transition: JournalTransition,
    ) -> CommitResult: ...
    def oldest_unacknowledged(self) -> SyncBatch | None: ...
    def acknowledge(self, ref: BatchRef) -> AckOutcome: ...
```

`RoomAggregate` is an immutable current `RoomState` plus its active lane and any retiring predecessor lanes. The reducer receives one source row plus only the affected room aggregates:

```python
def reduce_source_result(
    source: SourceState,
    rooms: Mapping[str, RoomAggregate],
    result: SourceResult,
) -> Reduction: ...
```

`Reduction` contains a replacement source row, changed room-state/lane rows, ready/frame/batch/loss inserts, and deterministic effects. It contains no coroutine, callback, `MatrixRoom`, mutable event object, lock, future, or database handle.

The coordinator's order is fixed:

1. Validate an immutable command's effect ID, source epoch, and membership epoch.
2. Load only the referenced source/room aggregate rows.
3. Compute a pure reduction.
4. Commit using `writer_epoch` and `expected_revision` compare-and-swap.
5. Replace committed in-memory values.
6. Publish a batch notification and schedule returned effects.

Any database, encryption, digest, or CAS failure is fail-stop. No cursor advances in memory and no follow-up effect is scheduled.

## nio Journal Schema

The ingestion schema is versioned independently from `MatrixStore.store_version`. It lives in the same per-device SQLite file as E2EE state so crypto receipts can share a transaction with Olm/Megolm mutations, but it uses new table names and a singleton `schema_version = 1`.

```text
NioIngestMeta
    account_id PK
    device_id
    schema_version
    stream_id
    binding_operation_id
    journal_generation NULL until consumer attach
    consumer_generation NULL until consumer attach
    baseline_rooms_sha256 NULL until consumer attach
    consumer_attached_revision NULL until consumer attach
    revision
    writer_epoch
    next_source_epoch
    next_ready_order
    next_batch_sequence
    last_acked_sequence
    last_acked_batch_id
    last_acked_sha256
    created_at_ns

NioIngestSourceState
    account_id PK/FK
    source_epoch
    transport_kind
    cursor_ciphertext
    cursor_sha256
    next_request_id
    active

NioIngestRoomState
    account_id
    room_id
    current_membership_epoch
    next_room_sequence
    hydration_status CHECK (hydration_status IN ('pending', 'ready', 'unavailable'))
    state_ciphertext
    state_sha256
    updated_revision
    PK(account_id, room_id)

NioIngestRoomLane
    account_id
    room_id
    membership_epoch
    lane_status CHECK (lane_status IN ('active', 'retiring'))
    held_record_count
    held_canonical_bytes
    release_phase CHECK (release_phase IN ('idle', 'recovering', 'releasing_recovered', 'releasing_terminal'))
    release_loss_id NULL unless releasing_terminal
    ready_order NULL while the lane has no eligible release head
    next_held_ordinal
    next_recovery_page
    successor_membership_epoch NULL unless retiring
    pending_lifecycle_ciphertext NULL unless retiring
    pending_lifecycle_sha256 NULL unless retiring
    updated_revision
    PK(account_id, room_id, membership_epoch)
    INDEX(account_id, ready_order)

NioIngestReadyRecord
    account_id
    ready_order
    record_id
    source_frame_id
    room_id NULL for account-wide records
    membership_epoch NULL for account-wide records
    room_sequence NULL for account-wide records
    payload_ciphertext
    payload_sha256
    canonical_bytes
    created_revision
    PK(account_id, record_id)
    UNIQUE(account_id, ready_order)

NioIngestLaneRecord
    account_id
    room_id
    membership_epoch
    section CHECK (section IN ('recovered', 'held'))
    page_ordinal (0 for held rows)
    record_ordinal
    record_id
    payload_ciphertext
    payload_sha256
    canonical_bytes
    created_revision
    PK(account_id, room_id, membership_epoch, section, page_ordinal, record_ordinal)
    UNIQUE(account_id, record_id)

NioIngestFrame
    account_id
    frame_id
    source_epoch
    request_id
    payload_ciphertext
    payload_sha256
    staged_revision
    PK(account_id, frame_id)

NioIngestCryptoReceipt
    account_id
    input_id
    input_sha256
    result_ciphertext
    result_sha256
    retention CHECK (retention IN ('until_frame_consumed', 'permanent_olm'))
    committed_at_ns
    PK(account_id, input_id)

NioIngestPendingDecryption
    account_id
    original_record_id
    room_id
    membership_epoch
    sender_key
    session_id
    source_ciphertext
    source_sha256
    created_revision
    PK(account_id, original_record_id)
    INDEX(account_id, room_id, sender_key, session_id)

NioIngestNetworkEffect
    account_id
    effect_id
    effect_kind
    source_epoch
    membership_epoch NULL for account-wide effects
    request_ciphertext
    request_sha256
    created_revision
    PK(account_id, effect_id)

NioIngestCryptoEffect
    account_id
    effect_id
    effect_kind
    source_epoch
    membership_epoch NULL for account-wide effects
    transaction_id NULL when the Matrix endpoint has no transaction ID
    request_ciphertext
    request_sha256
    created_at_ns
    PK(account_id, effect_id)

NioIngestBatch
    account_id
    sequence
    batch_id
    payload_ciphertext
    payload_sha256
    created_revision
    acknowledged_revision NULL until ack
    PK(account_id, sequence)
    UNIQUE(account_id, batch_id)

NioIngestLoss
    account_id
    loss_id
    room_id
    membership_epoch
    reason
    origin_ciphertext
    origin_sha256
    boundary_ciphertext
    boundary_sha256
    detail_ciphertext
    detail_sha256
    loss_sha256
    detected_revision
    PK(account_id, loss_id)

NioIngestConsumerResetRoom
    account_id
    binding_operation_id
    room_id
    evidence_ciphertext
    evidence_sha256
    PK(account_id, binding_operation_id, room_id)

```

`NioIngestRoomState` is the current authoritative room snapshot/epoch head. `NioIngestRoomLane` is epoch-keyed so one room may have an active `E+1` lane while bounded output from retiring `E` drains. Normal reduced records, including account-wide records, move into individually encrypted `NioIngestReadyRecord` rows; gap-blocked/recovered payloads use `NioIngestLaneRecord`. This durable rowization lets raw frames retire and lets one batch coalesce work across frames without copying a whole lane.

Recovery pages receive monotonic page ordinals as they are fetched toward older history. Any generic ready records already assigned to epoch `E` retain their earlier ready order. Within a terminal lane release, the loss precedes the held records it releases; all remaining `E` ready/recovery-lane records precede its pending successor lifecycle record. Only then may ready records from the successor epoch materialize. Other rooms may interleave at every bounded chunk. Materializing one batch inserts its canonical payload, deletes only ready/lane records now owned by that batch, and advances small cursors/barriers in one transaction; it never rewrites the whole lane.

Batch selection never depends on Python mapping or SQLite row order. `NioIngestMeta.next_ready_order` allocates contiguous values in the same transition that makes generic records or a recovery-lane head eligible. The materializer compares persisted `ready_order`, then a frozen cross-kind rank and stable primary key; records behind one room head remain ineligible. A partially drained lane receives a new tail `ready_order` in the batch transaction so bounded chunks round-robin with other ready work. This exact head-order policy makes restart or a different query plan produce byte-identical batches without letting one large lane monopolize the writer.

The batch table contains unacknowledged outbox rows plus at most the latest acknowledged row. Acknowledgement atomically advances the matching frontier, marks the current row acknowledged, and deletes the previously acknowledged row. Retrying the latest ack can therefore reread canonical payload bytes and recompute its identity, while storage remains bounded; older stale acknowledgements are rejected. Loss rows are append-only and a terminal lane references its `loss_id` until the loss is materialized into a batch. A frame is deleted when each record is in a batch/loss, a generic ready row, or an epoch-keyed recovery lane. Content-addressed receipts for non-idempotent Olm inputs remain across frame retirement, restart, and source rebind so repeated ciphertext cannot reratchet; they have no automatic correctness-risking TTL. Receipts for idempotent Megolm/derived work are deleted when the frame's durable output commits.

Network/recovery/lifecycle effects and crypto effects deliberately use separate tables and writers. The coordinator alone writes `NioIngestNetworkEffect`. The crypto worker alone writes `NioIngestCryptoEffect`, including key query/claim/upload and protocol to-device sends. The coordinator may execute the frozen HTTP request but treats its result as opaque and posts it back to the owning actor; only that actor validates the effect/epochs and deletes or advances its row in the same transaction as its owned state. Restart asks each actor to reschedule only its table. No actor mutates the other's effect row.

Use the existing recovery-payload key material and AES-GCM primitives, but define new contextual AAD including table, account, stream, primary key, schema version, and payload digest. Reject `SqliteQueueDatabase`: its statements do not form a real atomic transaction. The coordinator and dedicated crypto thread use separate direct SQLite connections to the same WAL database with an explicit busy timeout and short transactions; the crypto transaction never waits on HTTP. Tests inject writer contention and prove bounded retry/fail-stop behavior. Crash tests use an on-disk temporary database, not an in-memory substitute.

## Source and Recovery Semantics

Classic and Sliding normalize into the same `SyncFrame`:

```python
@dataclass(frozen=True, slots=True)
class SyncFrame:
    frame_id: UUID
    origin: RecordOrigin
    candidate_cursor_json: bytes
    to_device_json: tuple[bytes, ...]
    device_list_delta_json: bytes
    one_time_key_counts_json: bytes
    unused_fallback_key_types_json: bytes
    room_segments: tuple[RoomSegment, ...]
    ephemeral_json: tuple[bytes, ...]
    global_account_data_json: tuple[bytes, ...]
    presence_json: tuple[bytes, ...]
```

- `ClassicCursor(next_batch)` and `SlidingCursor(pos, to_device_since, connection_instance)` are distinct frozen state types; no function accepts their union without matching on `TransportKind`.
- Classic stores `next_batch` with the raw-frame transaction.
- Sliding `pos` is valid only for the live connection. A process restart or `M_UNKNOWN_POS` creates a new `connection_instance` and discards only `pos`; the independent to-device `since` cursor remains durable. Durable room lanes and event/content receipts suppress duplicates and determine whether a room has a provable baseline.
- Consumer attach creates `PENDING` room state plus an epoch-0 active lane for every baseline room. Classic's cold request uses full state and reconciles that entire set. Sliding v1 always adds reserved list `__nio_all_rooms_v1`, expands its ranges by `sliding_bootstrap_range_size` until the server-reported count is covered, and keeps that coverage active; caller lists/subscriptions are additional and cannot use that name or weaken primary-ingestion coverage.
- The internal Sliding list requests every state type/member field required by `RoomSnapshot` and the crypto recipient projection. Any baseline ID not observed after Classic's initial full-state frame or Sliding's full list coverage receives a bounded authenticated room-state hydration request. A committed authoritative snapshot changes it to `READY`; a confirmed not-joined/forbidden response changes it to `UNAVAILABLE`; retryable failures remain `PENDING`.
- `wait_baseline_hydrated()` resolves only when all attached baseline rooms are `READY` or `UNAVAILABLE`. Normal inbound batching can proceed while hydration runs, but `room_snapshot()` and outbound encryption never fabricate state for a pending room; `wait_room_ready()` either returns the committed snapshot or raises typed unavailability.
- Classic `device_lists`/key counts and Sliding's E2EE extension normalize into the same crypto-control fields; neither transport may drop device invalidation, one-time-key counts, or fallback-key state.
- An offline `rebind_ingestion_source()` requires a closed owner and no staged frames/effects. It increments `source_epoch`, discards the incompatible source cursor, preserves per-room lanes only as candidate overlap evidence, and cold-starts the new transport. It refuses unresolved real gaps by default; an explicit loss-recording policy emits `BASELINE_LOST`. Every room whose first new-source window cannot prove overlap emits a loss rather than inheriting cross-mode continuity.
- Recovery pages may run concurrently across rooms, with at most one outstanding page per room lane. One completed page is validated and reduced with one journal transition using bulk row operations; the implementation must not commit each recovered event separately.
- Reducing a frame atomically moves eligible records into encrypted generic ready rows and records blocked by a room gap into that epoch's encrypted held lane, then retires the frame. A gap or predecessor-epoch barrier in one room therefore cannot consume the global staged-frame bound. Per-room held-event/byte caps terminate with `EVENT_LIMIT` before the lane can grow without bound.
- Timeout, 408, 429, and retryable 5xx retain the lane without a loss.
- Terminal fetch rejection becomes `FETCH_FAILED`; cursor cycles/impossible exhaustion become `UNVERIFIABLE`; budget exhaustion becomes `EVENT_LIMIT`; membership or source reset with a real unresolved gap becomes `BASELINE_LOST`. An unreadable room-keyed lane/pending-decryption row becomes `CORRUPT_STORED_RECORD` for that known room. An unreadable whole raw frame has no trustworthy room inventory and therefore fails stop with `JournalCorruptionError`; nio retains the row and cursor and does not invent a room loss or continue past it.
- `UNKNOWN` is intentionally absent from the new producer enum. The producer must name a cause; discarded pre-v2 sync rows are never reinterpreted as new facts.
- Recovered records precede held live records. A terminal loss precedes held records it releases.
- Resolving a lane atomically creates a durable ordered release plan; it does not require one giant batch. Successful recovery materializes bounded FIFO chunks of recovered records and only then bounded chunks of held live records. Terminal recovery materializes the loss as the first record of the first bounded chunk and held live records only in that or later chunks. One `SyncBatch` may coalesce ready records from multiple already-durable frames and room lanes while preserving every lane's order. Global FIFO acknowledgement preserves the boundary even when other rooms are interleaved between chunks.
- Record-count and canonical-byte ceilings are absolute for every batch-materialization and MindRoom admission transaction. A 10,000-event/32-MiB lane is released through multiple bounded transactions and cannot monopolize the shared journal writer.
- Config defaults bound raw staged frames, unacknowledged batches, records per batch, and bytes per batch. Reaching a bound pauses new sync requests; it never discards a durable frame.

Each room has a monotonic membership epoch. Incoming own-member invite/leave/ban/rejoin and confirmed coordinator-owned join/rejoin/leave/forget success cross epochs. One semantic own-membership transition crosses exactly once: a later sync echo of an already-finalized local join/leave reconciles state inside the current epoch instead of incrementing it again. Crossing an epoch invalidates old recovery effects and Sliding baselines. A real unresolved gap becomes durable `BASELINE_LOST` in the same transaction as the reset. A rejected membership request changes nothing. Once an HTTP membership request may have reached the server, caller cancellation detaches the caller but not owner finalization.

Epoch transition order is a hard invariant. In one journal transition, ending membership epoch `E` marks its lane retiring, creates `LossRecord(membership_epoch=E)` when its gap is unresolved, stores the pending `ROOM_LIFECYCLE(E+1)` barrier, creates the active `E+1` lane, and advances `NioIngestRoomState`. The active lane may retain new `E+1` work immediately, but no `E+1` record is eligible for batch materialization until retiring `E` has emitted its loss and all retained `E` records, followed by the lifecycle barrier. Large retirements therefore remain bounded without orphaning old records or publishing a new epoch early; multiple rapid transitions form the same ordered epoch chain.

Recovery results/effect IDs tagged `E` are stale as soon as that transition commits and are ignored: they cannot mutate either lane, be reclassified under `E+1`, or create another loss. A recovery effect genuinely created under `E+1` has a distinct epoch-bound identity, and its later failure remains retained/actionable even when an `E` loss already exists for the room.

## Crypto Replay Contract

The crypto worker is a second actor with a deliberately disjoint ownership domain: it alone owns Olm/Megolm state and the minimal room encryption/membership projection needed to choose recipients; the coordinator alone owns transport, lane, frame, batch, and loss state. A dedicated single worker thread constructs and accesses all crypto objects and its SQLite connection; an asyncio mailbox serializes commands and returns frozen `CryptoResult` values tagged with work ID, source epoch, and relevant membership epochs. Do not use the shared `asyncio.to_thread()` pool, which would move mutable ratchets between threads. Neither actor shares a mutable object, and ingestion never mutates `AsyncClient.rooms` or passes a mutable `MatrixRoom` across the boundary.

Crypto receipt IDs are content-addressed per input, not frame-addressed. For an Olm to-device ciphertext, the ID/digest includes canonical sender, sender key, event type, session/ciphertext content, and algorithm. For a Megolm room event it includes room ID, event ID, sender key, session ID, ciphertext, and origin timestamp. Redelivery in another frame, after restart, or after source rebind therefore finds the same receipt rather than reratcheting.

The worker orders a frame's to-device inputs before room decryptions, but persists them in bounded groups. Before acquiring SQLite's write lock it derives content-addressed input IDs, reads receipts, returns matching cached results without ratcheting, and fails closed on an ID/digest collision. For missing inputs it may derive one group's private crypto mutations because no other actor can observe or mutate that object graph; it must then commit or terminally discard the entire worker before touching the next group. A group contains source-ordered to-device inputs or deterministic room/frame-ordered decryptions, never both sides of that ordering boundary.

For each derived group, one short direct-SQLite transaction must:

1. Revalidate the expected receipt absence/digests.
2. Persist every modified account/session/message-index value.
3. Persist encrypted immutable result receipts plus pending-decryption/effect rows.
4. Commit before returning any new result to the coordinator.

If mutation succeeds in memory but the database transaction fails, the crypto worker enters a terminal failed state and closes the ingestion session. It must not process another input against divergent in-memory ratchets; restart reconstructs state from the rolled-back database. Group input-count/byte ceilings and the SQLite busy timeout bound writer occupancy and coordinator wait; decryption/model work itself never runs while a SQLite write transaction is open.

Any key upload/query/claim or protocol to-device send produced by crypto processing is inserted into `NioIngestCryptoEffect` in that same transaction. The coordinator executes the frozen request only after commit and posts the immutable response back without touching the row. The crypto worker alone applies the result after its effect ID and epochs match the durable row, deleting or advancing it in the same transaction as resulting crypto state. Requests carry deterministic Matrix transaction IDs where the endpoint supports them; safe endpoint-specific replay behavior is covered by tests.

When a room event cannot decrypt because its inbound group session is missing, the worker returns a stable decryption-failure record and inserts `NioIngestPendingDecryption`. A later matching room key queries that index, decrypts each pending input through the same receipt discipline, and posts `DECRYPTION_UPDATE` records referencing `original_record_id`; the update ID is `UUIDv5(original_record_id, clear_sha256)`. The coordinator emits updates idempotently and removes the pending row only in the transaction that makes the update durable. Membership changes do not erase the pending cryptographic fact; MindRoom decides whether the later clear content is actionable under the original provenance and membership epoch.

Outbound encrypted sends enter the same serialized crypto-worker queue. The worker returns immutable encrypted content to the caller, and HTTP transmission remains outside the worker. A read-only `IngestionSession.room_snapshot(room_id)` exposes committed membership/state needed by callers without sharing owner objects. User code never executes in either actor.

The worker exposes typed async commands and frozen snapshots, never the mutable `Olm` object, account, session store, device store, or group-session objects:

```python
identity = await ingestion.crypto.identity_snapshot()
devices = await ingestion.crypto.device_snapshots(user_ids)
encrypted = await ingestion.crypto.encrypt_to_devices(targets, event_type, content_json)
encrypted_room = await ingestion.crypto.encrypt_room(room_id, content_json)
await ingestion.crypto.set_device_trust(device_id, trust)
await ingestion.crypto.import_keys(path, passphrase)
await ingestion.crypto.export_keys(path, passphrase)
```

Verification/SAS and key-request operations receive the same command treatment. Read snapshots contain values only and cannot be used to reach a session. Once the v2 owner starts, direct `client.olm` access is a protocol error; all nio and MindRoom call sites are migrated before old-code deletion.

## MindRoom Admission Boundary

The durable handoff is:

```text
nio batch transaction
    -> MindRoom event-journal transaction
        -> nio acknowledgement transaction
```

Add these MindRoom tables:

```text
matrix_sync_batches
    principal_id
    matrix_account_id
    matrix_device_id
    journal_generation
    consumer_generation
    stream_id
    sequence
    batch_id
    schema_version
    sha256
    admitted_at_ns
    PK(principal_id, stream_id, sequence)
    UNIQUE(principal_id, batch_id)

matrix_sync_consumers
    principal_id PK
    matrix_account_id
    matrix_device_id
    journal_generation
    consumer_generation
    stream_id
    binding_operation_id
    next_sequence
    baseline_rooms_sha256
    bound_at_ns

matrix_sync_consumer_baseline_rooms
    principal_id
    consumer_generation
    room_id
    PK(principal_id, consumer_generation, room_id)

matrix_sync_recovery_losses
    principal_id
    loss_id
    stream_id
    batch_sequence
    loss_order
    room_id
    membership_epoch
    reason
    boundary_json
    detail_json
    PK(principal_id, loss_id)
    UNIQUE(principal_id, stream_id, batch_sequence, loss_order)

matrix_sync_to_device_inputs
    principal_id
    input_id
    stream_id
    batch_sequence
    record_order
    event_type
    source_json
    pending
    admitted_at_ns
    PK(principal_id, input_id)
    UNIQUE(principal_id, stream_id, batch_sequence, record_order)
```

Before nio starts HTTP, `PrincipalStore.bind_sync_consumer(account_id, device_id, stream_id, binding_operation_id=..., first_sequence=...)` creates or validates `matrix_sync_consumers` against the event journal's own generation and initializes `next_sequence` to the advertised nio frontier. On first bind the same transaction snapshots the durable active-room projection into `matrix_sync_consumer_baseline_rooms`, stores its canonical digest, and returns `ConsumerBootstrap`; retries return the identical operation/sequence/snapshot. There is no renewable lease.

`PrincipalStore.admit_sync_batch()` performs exactly one `Backend.write()`. Inside that transaction it:

1. Loads `matrix_sync_consumers` and verifies Matrix account/device, journal generation, consumer generation, and active stream ID. No caller-side check substitutes for this predicate.
2. Recomputes canonical bytes, digest, and batch ID.
3. When `sequence < next_sequence`, validates the existing receipt and returns duplicate; when `sequence > next_sequence`, fails out of order.
4. For `sequence == next_sequence`, inserts the non-empty batch receipt, events, projections, app-relevant to-device inputs, losses, and history debt in record order.
5. Advances `matrix_sync_consumers.next_sequence` in that same transaction.
6. Returns which records were newly admitted and which best-effort notifications may now be dispatched.

A duplicate batch with the same identity and digest is a no-op. The same stream/sequence or batch ID with different bytes raises corruption. Empty batches are invalid. Any record failure rolls back the receipt, event rows, projections, loss rows, and history-debt changes together.

After commit, MindRoom may dispatch typing and presence notifications best-effort. They never gate nio acknowledgement and are explicitly excluded from the durable event-fate guarantee. Matrix receipts are not best-effort: they update their durable projection in the batch transaction. App-relevant to-device outputs must either update a durable projection in that transaction or be classified in a checked-in audit as safely replayable/best-effort before cutover.

MindRoom defines `InboundRecoveryLossReason(StrEnum)` with one member per released nio `LossReason`. Translation uses an exhaustive `match`; the default raises before `Backend.write()`. A contract test compares the two serialized value sets so upgrading nio cannot silently create enum drift.

PR 1800's event-journal generation UUID is one half of the binding; `matrix_sync_consumers.consumer_generation` is the other. MindRoom passes both to `open_ingestion()`, nio persists them in `NioIngestMeta`, and every batch carries them. A mismatch raises `ConsumerBindingMismatch` before HTTP or nio ack, and the admission transaction independently enforces it.

v1 deliberately has no in-place consumer rebind that preserves sync position. Rotating `stream_id` would invalidate encrypted-row AAD. If the MindRoom journal is replaced with a different generation, either restore the matching nio database as a consistent pair or run Task 13's destructive existing-v1 reset. Before deletion, that reset successfully decodes all affected v1 rows and durably inventories every room represented by a frame, generic ready row, room state/epoch lane record, retained batch (acknowledged or not), or pending decryption. It refuses unreadable evidence or any unresolved network/crypto/lifecycle effect. It then makes nio unbound, preserves `stream_id`, `next_batch_sequence`, E2EE state, permanent Olm receipts, append-only loss evidence, and the reset-room inventory, but discards the remaining consumer/sync/transient state. MindRoom explicitly replaces its consumer row at that advertised sequence; attach unions its active-room snapshot with the reset inventory and cold-starts every room in that union with `BASELINE_LOST`. Crashes between those steps leave nio unbound, so no two-database prepare/finalize protocol or row re-encryption is needed. Events previously acknowledged to a now-lost MindRoom journal remain that journal backup's responsibility; nio cannot reconstruct data it correctly handed off and compacted.

A batch that MindRoom cannot admit also remains fail-stopped and retained in nio. The supported remedy is to fix or roll forward the exactly pinned consumer and replay the identical batch—not consumer reset and not automatic quarantine. The destructive reset refuses the same journal generation and therefore cannot be used to skip a poison batch. This is intentional: a projection bug or unknown schema is not evidence that Matrix history was lost, and converting it into a loss would destroy recoverable data. Startup/version gates prevent knowingly running a consumer that does not support the batch schema.

Crash behavior:

- Before the MindRoom transaction commits: no ack; nio redelivers the batch.
- After MindRoom commit but before nio ack: the batch receipt and event IDs deduplicate the replay; MindRoom retries the ack.
- During nio ack: retry is safe because matching acknowledgements are idempotent.
- During semantic work, model calls, tool calls, outbox delivery, or Matrix sending: irrelevant to sync ack; the MindRoom journal already owns replay.

## Target File Map

### mindroom-nio: create

```text
src/nio/ingest/__init__.py
src/nio/ingest/config.py
src/nio/ingest/errors.py
src/nio/ingest/model.py
src/nio/ingest/serialization.py
src/nio/ingest/ports.py
src/nio/ingest/network.py
src/nio/ingest/source.py
src/nio/ingest/classic.py
src/nio/ingest/sliding.py
src/nio/ingest/membership.py
src/nio/ingest/recovery.py
src/nio/ingest/state.py
src/nio/ingest/reducer.py
src/nio/ingest/crypto.py
src/nio/ingest/metrics.py
src/nio/ingest/coordinator.py
src/nio/store/sync_journal_schema.py
src/nio/store/sync_journal.py
src/nio/store/crypto_receipts.py
tests/ingest/model_test.py
tests/ingest/serialization_test.py
tests/ingest/journal_test.py
tests/ingest/classic_source_test.py
tests/ingest/sliding_source_test.py
tests/ingest/membership_test.py
tests/ingest/recovery_reducer_test.py
tests/ingest/frame_staging_test.py
tests/ingest/crypto_worker_test.py
tests/ingest/metrics_test.py
tests/ingest/coordinator_test.py
tests/ingest/consumer_protocol_test.py
tests/ingest/crash_recovery_test.py
tests/ingest/source_parity_test.py
tests/ingest/fresh_start_test.py
tests/ingest/performance_test.py
scripts/initialize_ingestion_store.py
scripts/live_ingest_check.py
```

### mindroom-nio: modify

```text
pyproject.toml
uv.lock
.github/workflows/tests.yml
src/nio/__init__.py
src/nio/client/base_client.py
src/nio/client/async_client.py
src/nio/client/__init__.py
src/nio/crypto/olm_machine.py
src/nio/store/database.py
src/nio/store/models.py
src/nio/store/__init__.py
tests/store_test.py
tests/encryption_test.py
tests/async_client_test.py
tests/backfill_test.py
```

### MindRoom: create

```text
src/mindroom/event_journal/sync_batch_admission.py
src/mindroom/matrix/sync_batch_ingress.py
src/mindroom/matrix/room_directory.py
src/mindroom/matrix/fresh_ingestion.py
src/mindroom/desktop/ingestion.py
tests/test_event_journal_sync_batches.py
tests/test_matrix_sync_batch_stream.py
tests/test_matrix_room_directory.py
tests/test_matrix_fresh_ingestion.py
tests/test_desktop_ingestion.py
```

### MindRoom: modify

```text
pyproject.toml
uv.lock
src/mindroom/event_journal/models.py
src/mindroom/event_journal/schema.py
src/mindroom/event_journal/store.py
src/mindroom/event_journal/views.py
src/mindroom/event_journal/history_debt.py
src/mindroom/event_journal/__init__.py
src/mindroom/matrix/journal_ingress.py
src/mindroom/matrix/sync_loop.py
src/mindroom/matrix/large_messages.py
src/mindroom/matrix/rooms.py
src/mindroom/matrix/presence.py
src/mindroom/matrix/client_room_admin.py
src/mindroom/matrix/client_delivery.py
src/mindroom/matrix_rtc/call_manager.py
src/mindroom/custom_tools/matrix_room.py
src/mindroom/custom_tools/subagents.py
src/mindroom/custom_tools/attachments.py
src/mindroom/custom_tools/dynamic_workflow.py
src/mindroom/response_runner.py
src/mindroom/desktop/pairing_receiver.py
src/mindroom/desktop/pairing_client.py
src/mindroom/desktop/client.py
src/mindroom/desktop/command_journal.py
src/mindroom/cli/desktop.py
src/mindroom/desktop/session.py
src/mindroom/matrix/olm_to_device.py
src/mindroom/matrix/client_session.py
src/mindroom/matrix/cross_signing.py
src/mindroom/matrix/conversation_hydration.py
src/mindroom/commands/encryption_commands.py
src/mindroom/matrix_rtc/key_transport.py
src/mindroom/journal_dispatch.py
src/mindroom/bot.py
tests/test_event_journal_store.py
tests/test_event_journal_crash_matrix.py
tests/test_journal_ingress.py
tests/test_matrix_sync_continuity.py
tests/test_room_history_debt.py
tests/test_room_member_hooks.py
```

### Delete after production cutover

```text
src/nio/client/sync_recovery.py
src/nio/client/sliding_membership.py
src/nio/client/sync_reset_fence.py
src/nio/client/sync_response_ordering.py
src/nio/recovery_abandonment.py
src/nio/sliding_sync_tokens.py
tests/sync_recovery_test.py
tests/sliding_membership_test.py
tests/sync_response_ordering_test.py
tests/per_room_recovery_contract_test.py

/work/dev/mindroom/src/mindroom/matrix/sync_continuity.py
/work/dev/mindroom/src/mindroom/matrix/sync_checkpoint_trust.py
/work/dev/mindroom/src/mindroom/matrix/sync_certification.py
/work/dev/mindroom/src/mindroom/matrix/sync_recovery_escape.py
/work/dev/mindroom/src/mindroom/matrix/sync_token_values.py
/work/dev/mindroom/tests/test_matrix_sync_continuity.py
/work/dev/mindroom/tests/test_sync_checkpoint_trust.py
/work/dev/mindroom/tests/test_sync_certification.py
/work/dev/mindroom/tests/test_sync_recovery_escape.py
```

Port semantic cases from deleted tests into `tests/ingest/`; do not delete a test until its invariant is named and covered in the new suite. Delete tests that assert lock topology, callback re-entry, application-owned checkpoint staging, optional persistence capability flags, or mutable response outcomes.

---

## Production-Line Forecast

Tests, fixtures, generated locks, and this document are excluded. These are planning ranges, not targets to fill:

| Work | Expected production lines added |
|---|---:|
| Tasks 1–2: model, serialization, journal, bootstrap | 900–1,300 |
| Tasks 3–4: Classic and Sliding adapters | 400–700 |
| Tasks 5–6: reducer, lanes, staging, batching | 1,000–1,500 |
| Task 7: crypto worker, receipts, commands | 900–1,400 |
| Tasks 8–9: coordinator, network, lifecycle | 700–1,100 |
| Tasks 11–12: MindRoom admission and stream | 700–1,100 |
| Tasks 12A–12C: room, desktop, crypto consumers | 600–1,000 |
| Task 13 plus activation/reset glue | 250–500 |
| **Gross addition** | **5,450–8,600** |
| Task 16 old architecture deletion | **−3,500 to −4,500** |
| **Expected net** | **+1,500 to +5,000** |

At each checkpoint, replace estimates for completed work with actual `git diff --stat`/language-aware production counts and reforecast the remaining rows. Crossing +6,000 projected net or a 50% row overrun triggers review before more implementation.

---

## Delivery and Branch Strategy

- Implement nio in an isolated `feat/durable-ingestion-v2` worktree based on the then-current `mindroom-nio/main`; implement MindRoom in a companion branch based on the exact PR 1800 integration head after that work is finalized. Record both base SHAs in the first implementation checkpoint.
- Keep both pull requests draft through Task 14. Publish rc1/rc2/rc3 only from the nio feature branch; the MindRoom feature branch pins exact artifacts and never imports an unpublished workspace checkout.
- Rebase/merge current main into both feature branches at Tasks 2, 10, and 12 before recording performance/line checkpoints. Unrelated production work continues on main and must not depend on unfinished v2 APIs.
- Merge no half of a cross-repository contract. The final nio artifact is built first; MindRoom pins and verifies that exact artifact, then the coordinated maintenance window performs Task 15. Because this fork has one consumer, no compatibility release or parallel runtime is maintained.
- Preserve task-sized commits and review checkpoints rather than one rewrite commit. Task 16 is the only deletion commit and is not squashed into earlier implementation work.

---

## Task 1: Raise the Floor and Freeze the Wire Model

**Files:**

- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `.github/workflows/tests.yml`
- Create: `src/nio/ingest/__init__.py`
- Create: `src/nio/ingest/errors.py`
- Create: `src/nio/ingest/model.py`
- Create: `src/nio/ingest/serialization.py`
- Modify: `src/nio/event_provenance.py`
- Create: `tests/ingest/model_test.py`
- Create: `tests/ingest/serialization_test.py`

**Interfaces:** Define the public enums/dataclasses above, reuse and re-export the existing `TimelineEventProvenance` name, add `canonical_batch_payload(batch) -> bytes`, and internal `batch_from_records(...) -> SyncBatch`.

- [ ] Change `requires-python` to `>=3.12`, remove Python 3.10/3.11 classifiers and CI jobs, set Ruff to `py312`, and regenerate `uv.lock` before importing `StrEnum`.
- [ ] Write failing tests for every serialized enum value, frozen/slotted behavior, tuple-only records, canonical dict-key ordering, UTF-8 encoding, and schema-version rejection.
- [ ] Round-trip both transport and system loss origins; reject `SystemOrigin` on an `EventRecord`, missing membership epoch on a room loss, and a system origin whose kind/operation ID is inconsistent with its deterministic loss ID.
- [ ] Write failing tests that room/member snapshots contain no mutable mapping/list and that their derived display/member properties match the current `MatrixRoom` cases MindRoom uses.
- [ ] Write a failing test where `dataclasses.replace(valid_batch, ref=replace(valid_batch.ref, sha256=b""))` raises `BatchIntegrityError`.
- [ ] Write a failing test where changing one record byte changes both digest and batch ID.
- [ ] Write a failing test that constructing or decoding a zero-record `SyncBatch` is rejected; empty source frames are a local journal concern, not a public batch.
- [ ] Implement canonical JSON with explicit field ordering and no platform-dependent repr, float, datetime, or enum ordinal.
- [ ] Pin golden canonical bytes and SHA-256 values as fixtures.
- [ ] Export only stable public types from `nio.ingest`; do not export reducer or store internals.

Run red:

```bash
uv run pytest -q tests/ingest/model_test.py tests/ingest/serialization_test.py
```

Run green:

```bash
uv run pytest -q tests/ingest/model_test.py tests/ingest/serialization_test.py
uv run pre-commit run --files pyproject.toml uv.lock .github/workflows/tests.yml src/nio/event_provenance.py src/nio/ingest/__init__.py src/nio/ingest/errors.py src/nio/ingest/model.py src/nio/ingest/serialization.py tests/ingest/model_test.py tests/ingest/serialization_test.py
```

Commit: `feat(ingest): freeze durable batch wire model`

---

## Task 2: Build the Fresh Version-1 Ingestion Journal

**Files:**

- Create: `src/nio/ingest/config.py`
- Create: `src/nio/ingest/state.py`
- Create: `src/nio/store/sync_journal_schema.py`
- Create: `src/nio/store/sync_journal.py`
- Modify: `src/nio/store/__init__.py`
- Modify: `src/nio/store/database.py`
- Create: `tests/ingest/journal_test.py`
- Create: `tests/ingest/crash_recovery_test.py`

**Interfaces:** Implement `IngestionConfig`, `open_ingestion_store()`, single-use `StoreBootstrap`, `StoreBootstrap.attach_consumer()`, `IngestionJournal`, `SqliteIngestionJournal.open(...)`, `commit(...)`, `oldest_unacknowledged()`, `acknowledge(...)`, and deterministic encrypted row codecs.

- [ ] Start with failing tests for fresh schema creation, second-writer refusal, wrong account/device/consumer-generation refusal, and independent `schema_version == 1`.
- [ ] Seed a nonempty E2EE/legacy database without a v1 marker, spy on every SQL statement, and prove normal open raises `FreshIngestionRequired` before `MatrixStore.__post_init__`, migration, repair, table creation, or any write. Seed a genuinely new path and prove the v1 marker is the first atomic schema mutation.
- [ ] Make `StoreBootstrap` the only way the v2 `AsyncClient` opens storage. Its store initialization validates the retained lock/marker and opens only E2EE plus v1 ingestion tables; it must not create, migrate, or repair legacy sync/recovery tables. Direct legacy store construction must refuse a file already owned by a v1 bootstrap.
- [ ] Require the v2 lifetime lock before every v1/E2EE open and test two v2 owners cannot coexist. Running an old binary after explicit initialization is an unsupported operator error, not a compatibility mode.
- [ ] Test that opening the journal neither reads nor alters `SyncTokens`, `SyncRecoveryGaps`, `SyncRecoveryAbandonedRooms`, `PendingTimelineEvents`, or `SlidingWindowTokens`.
- [ ] Test unbound meta refuses client HTTP/batch delivery; `attach_consumer()` validates the canonical baseline digest and atomically installs the binding plus priority bounded-loss plan, and exact retries are idempotent.
- [ ] Test CAS rollback injection after every SQL statement in a transition: revision, cursor, room heads, epoch lanes, ready rows, frames, batches, and losses must all remain at the prior state.
- [ ] Round-trip one active lane plus multiple retiring predecessor epochs for the same room. `NioIngestRoomState.current_membership_epoch` must identify exactly one active lane, and each predecessor's successor/lifecycle barrier must form one gap-free ordered chain.
- [ ] Test that only named affected room state/lanes are read and written; instrument SQL and assert no full room-state or lane scan during a one-room transition.
- [ ] Test AES-GCM round-trip and AAD failure after swapping account, table, primary key, stream, or digest.
- [ ] Round-trip a delayed loss after its source frame has been compacted and prove origin, membership epoch, reason, boundary, detail, all three ciphertext digests, and the canonical whole-loss digest reconstruct the exact frozen `LossRecord`.
- [ ] Test FIFO acknowledgement, retry of the latest acknowledgement with payload revalidation, stale older acknowledgement, future/out-of-order acknowledgement, wrong digest, and atomic deletion of the previously acknowledged row.
- [ ] Reject `SqliteQueueDatabase` with `LocalProtocolError` before creating any ingestion table.
- [ ] Hold an exclusive OS/process writer lock for the session lifetime and a persisted `writer_epoch` CAS for every write.
- [ ] Use on-disk temporary databases for restart and kill-point tests.
- [ ] Establish the first performance checkpoint: one-room CAS commit latency, hard-ceiling batch write/read latency, SQLite write-lock wait, and bytes allocated. Record hardware and commands so later tasks compare like-for-like.
- [ ] Record actual production additions/deletions and revise the project line forecast before Task 3.

Run red, then green:

```bash
uv run pytest -q tests/ingest/journal_test.py tests/ingest/crash_recovery_test.py
uv run pre-commit run --files src/nio/ingest/config.py src/nio/ingest/state.py src/nio/store/sync_journal_schema.py src/nio/store/sync_journal.py src/nio/store/__init__.py src/nio/store/database.py tests/ingest/journal_test.py tests/ingest/crash_recovery_test.py
```

Commit: `feat(ingest): add transactional ingestion journal`

---

## Task 3: Implement the Pure Classic Source Adapter

**Files:**

- Create: `src/nio/ingest/ports.py`
- Create: `src/nio/ingest/source.py`
- Create: `src/nio/ingest/classic.py`
- Create: `tests/ingest/classic_source_test.py`

**Interfaces:** Implement frozen `NetworkRequest`, `NetworkResult`, `SyncFrame`, `RoomSegment`, `SourceResult`, and `ClassicSource.plan_request()/normalize()`.

- [ ] Write fixture-driven failing tests for initial sync, continuation, empty response, invite/join/leave sections, state, account data, presence, ephemeral events, to-device events, device-list deltas, key counts, fallback-key types, limited timelines, and HTTP error classification.
- [ ] Prove `next_batch` appears only as a candidate cursor on the normalized frame; the adapter never persists or mutates it.
- [ ] Prove normalization retains canonical source bytes required for later typed parsing and fingerprinting.
- [ ] Prove two normalizations of the same response produce byte-identical frames and deterministic record order.
- [ ] Keep the module pure: no `AsyncClient`, store, callback, lock, task, or coroutine creation.

Run red, then green:

```bash
uv run pytest -q tests/ingest/classic_source_test.py
uv run pre-commit run --files src/nio/ingest/ports.py src/nio/ingest/source.py src/nio/ingest/classic.py tests/ingest/classic_source_test.py
```

Commit: `feat(ingest): normalize classic sync frames`

---

## Task 4: Implement Sliding as an Equal Source Adapter

**Files:**

- Create: `src/nio/ingest/sliding.py`
- Create: `src/nio/ingest/membership.py`
- Create: `tests/ingest/sliding_source_test.py`
- Create: `tests/ingest/membership_test.py`
- Create: `tests/ingest/source_parity_test.py`

**Interfaces:** Implement `SlidingSource.plan_request()/normalize()` and pure membership/baseline proof functions.

- [ ] Port all eight semantic cases from `tests/sliding_membership_test.py`: initial rooms, `num_live`, membership-bound baselines, invites, expansions, linked joins, departures, and unsafe baselines.
- [ ] At the pure adapter/state level, test `M_UNKNOWN_POS` creates a new connection instance without the old `pos`, carries the supplied independent to-device `since`, and marks uncertain room history for shared reducer classification. Durable frame draining belongs to Task 6 and crypto replay belongs to Task 7.
- [ ] Test equal normalized ordering and record kinds for equivalent Classic and Sliding fixtures.
- [ ] Pure-test the reserved all-rooms list planner: bounded range expansion to the reported count, stable coverage across reordering/reconnect, required state sufficient for `RoomSnapshot`/crypto membership, and caller list JSON unable to remove or collide with the reserved list.
- [ ] Test Sliding E2EE extension output is byte-equivalent to Classic device-list/key-count control input for the crypto worker.
- [ ] Test that the connection-specific `pos` never becomes a cross-restart continuity proof.
- [ ] Keep transport-specific membership extraction in this adapter and shared membership-epoch rules in `membership.py`.

Run red, then green:

```bash
uv run pytest -q tests/ingest/sliding_source_test.py tests/ingest/membership_test.py tests/ingest/source_parity_test.py
uv run pre-commit run --files src/nio/ingest/sliding.py src/nio/ingest/membership.py tests/ingest/sliding_source_test.py tests/ingest/membership_test.py tests/ingest/source_parity_test.py
```

Commit: `feat(ingest): normalize sliding sync with membership proof`

---

## Task 5: Port Recovery Into a Pure Per-Room Reducer

**Files:**

- Create: `src/nio/ingest/recovery.py`
- Create: `src/nio/ingest/reducer.py`
- Create: `tests/ingest/recovery_reducer_test.py`

**Interfaces:** Implement `reduce_source_result()`, `reduce_recovery_result()`, `RoomState`, epoch-keyed `RoomLane`, `RoomAggregate`, `RecoveryGap`, deterministic effect IDs, lifecycle barriers, and loss creation.

- [ ] Port chronology, duplicate suppression, held-live ordering, own-join boundaries, bounded exhaustion, stalled cursor, cursor cycle, cap, corrupt stored record, fetch rejection, and retryable failure cases from `sync_recovery_test.py` and `per_room_recovery_contract_test.py`.
- [ ] Mutation-test every terminal branch: changing a loss reason, dropping the loss, releasing held records before the loss, or treating a retryable response as terminal must make a focused test fail.
- [ ] Test two rooms where one repeatedly returns 429 and the other recovers and batches normally.
- [ ] Test stale recovery results after membership/source epoch changes are ignored without altering a lane.
- [ ] Test an unresolved real gap crossing membership emits `BASELINE_LOST` in the same reduction that retires lane `E`, creates active lane `E+1`, and installs the pending lifecycle barrier.
- [ ] State-machine test the reducer half of the epoch race exactly: start recovery effect `R(E)`; commit rejoin with retiring `loss(E)` and a pending lifecycle barrier to active `E+1`; deliver stale `R(E)` and prove it is ignored without a duplicate or epoch reclassification; start/fail `R(E+1)` and prove its distinct current-epoch loss is retained behind the predecessor barrier rather than suppressed by `loss(E)`. Task 6 adds bounded materialization plus journal/ack/restart persistence around this sequence.
- [ ] Test room state and membership records produce a replacement frozen `RoomSnapshot` only after the reduction commits; consumers can never mutate the authoritative lane through a snapshot.
- [ ] Test no reduction produces `UNKNOWN`; missing cause construction raises `ReducerInvariantError`.
- [ ] Corrupt a room-keyed held/pending row and require a truthful room `CORRUPT_STORED_RECORD`; corrupt a whole frame and require fail-stop retention with no fabricated loss and no later HTTP request.
- [ ] Keep reducer tests synchronous and free of `AsyncClient`, callbacks, tasks, SQLite, locks, and clocks.

Run red, then green:

```bash
uv run pytest -q tests/ingest/recovery_reducer_test.py
uv run pre-commit run --files src/nio/ingest/recovery.py src/nio/ingest/reducer.py tests/ingest/recovery_reducer_test.py
```

Commit: `feat(ingest): add pure per-room recovery reducer`

---

## Task 6: Stage Raw Frames and Build Bounded FIFO Batches

**Files:**

- Create: `tests/ingest/frame_staging_test.py`
- Modify: `src/nio/store/sync_journal.py`
- Modify: `src/nio/ingest/reducer.py`
- Modify: `src/nio/ingest/config.py`
- Modify: `tests/ingest/journal_test.py`
- Modify: `tests/ingest/crash_recovery_test.py`

**Interfaces:** Add `stage_frame()`, frame-consumption transitions, durable per-room release plans, deterministic bounded batch splitting, and backlog admission decisions.

- [ ] Test kill points before frame commit, after frame commit, after crypto result, after batch commit, and after frame compaction.
- [ ] Assert a cursor never advances without the exact durable raw frame that justified it.
- [ ] Assert a frame is never compacted before every derived record is durable in a batch/loss, generic ready row, or epoch-keyed recovery lane.
- [ ] Test record-count, byte-count, staged-frame, and unacknowledged-batch bounds pause network planning rather than discard data.
- [ ] Test a terminal recovery atomically creates an ordered release plan, then splits it into hard-bounded FIFO batches with the loss first and every held event after it.
- [ ] Retire an epoch with a held lane larger than one batch while new successor-epoch records arrive. Assert bounded output is `loss(E)`, all retained `E` records, `ROOM_LIFECYCLE(E+1)`, then `E+1` records; the successor work is durable before the barrier clears but never materializes early.
- [ ] Commit `E -> E+1 -> E+2` before `E` finishes draining and restart between every bounded chunk. Assert the epoch-keyed lane chain reconstructs exactly and materializes both lifecycle barriers in order without losing, duplicating, or prematurely exposing either successor's records.
- [ ] Test successful recovery splits recovered history into bounded chronological batches and emits held live events only after the final recovered chunk.
- [ ] Reduce a recovery page containing many events with exactly one journal transition and bulk row insertion. Instrument transaction calls and mutation-kill an implementation that commits once per recovered event.
- [ ] Reduce one or more eligible staged raw frames in source order with one journal transition, grouping inserts/deletes by affected table and room rather than issuing a persistence operation per record. An idle transition may contain one frame; a ready backlog may contain several. Mutation-test both the `commits <= eligible frames` bound and grouped-SQL path.
- [ ] Materialize a 10,000-event/32-MiB room lane and prove every nio and MindRoom-facing batch stays at or below both hard ceilings, unrelated rooms can interleave, and no database write contains the whole lane.
- [ ] Test a room event above `max_record_bytes` becomes one bounded `OVERSIZED_EVENT` loss with digest/boundary evidence; an oversized non-room record retains its frame and fail-stops.
- [ ] Test unrelated room segments can enter earlier batches while one lane waits for recovery.
- [ ] Let ready records accumulate across multiple durable frames and room lanes; under backlog, assert one batch greedily consumes oldest heads until a ceiling is reached or the next head would not fit, without violating per-room order. With only one ready unit, assert publication is immediate and does not wait for a timer.
- [ ] Randomize database insertion/query order and restart before materialization; the persisted ready/lane-head ordering must produce byte-identical record order, digest, and batch ID, including the same batch boundary when the next head does not fit.
- [ ] Feed more than `max_staged_frames` frames while one room remains gapped; assert its records move into the durable held lane, frames retire, and unrelated rooms continue until consumer-level batch backpressure rather than gap-level backpressure.
- [ ] Count journal transactions for frozen workloads and assert growth is `O(raw frames + recovery pages + materialized batches + acknowledgements)`, not `O(records)`. A mutation that replaces any bulk transition with a per-record commit loop must fail this test.
- [ ] Persist the full epoch race through the journal: `R(E)` in flight; durable `loss(E) -> ROOM_LIFECYCLE(E+1)`; stale `R(E)`; failed acknowledgement; process restart, identical batch redelivery, and successful acknowledgement; then a genuine failing `R(E+1)`. Prove there is exactly one durable `loss(E)` identity (despite idempotent redelivery), it is never relabelled as `E+1`, and the distinct `E+1` loss remains retained until acknowledged.
- [ ] Recompute frame and batch hashes on every database read before decoding.
- [ ] Benchmark hard-ceiling batch materialization and a 10,000-record lane drain; record transaction duration, writer wait, peak memory, and unrelated-room latency before adding crypto contention.

Run red, then green:

```bash
uv run pytest -q tests/ingest/frame_staging_test.py tests/ingest/journal_test.py tests/ingest/crash_recovery_test.py
uv run pre-commit run --files src/nio/store/sync_journal.py src/nio/ingest/reducer.py src/nio/ingest/config.py tests/ingest/frame_staging_test.py tests/ingest/journal_test.py tests/ingest/crash_recovery_test.py
```

Commit: `feat(ingest): stage frames before advancing transport`

---

## Task 7: Make Crypto Replay-Safe

**Files:**

- Create: `src/nio/ingest/crypto.py`
- Create: `src/nio/store/crypto_receipts.py`
- Create: `tests/ingest/crypto_worker_test.py`
- Modify: `src/nio/ingest/ports.py`
- Modify: `src/nio/crypto/olm_machine.py`
- Modify: `src/nio/client/base_client.py`
- Modify: `src/nio/client/async_client.py`
- Modify: `src/nio/store/database.py`
- Modify: `src/nio/store/models.py`
- Modify: `tests/encryption_test.py`
- Modify: `tests/async_client_test.py`
- Modify: `tests/backfill_test.py`

**Interfaces:** Implement serialized `CryptoWorker`, `CryptoWorkUnit`, `CryptoResult`, frozen identity/device/trust snapshots, typed crypto commands, `CryptoStoreTransaction`, and content-addressed receipt lookup/write in the same transaction as E2EE mutations.

- [ ] Start with an Olm to-device replay test: process once, crash after crypto commit but before coordinator commit, process the same work again, and assert the cached clear result is returned without advancing the ratchet twice.
- [ ] Add equivalent Megolm room-event, room-key-before-room-event, device-list invalidation, one-time/fallback-key maintenance, duplicate message-index, and mixed to-device/timeline tests.
- [ ] Kill after crypto state/effect commit but before key upload/query/claim/to-device HTTP, then reopen and prove the identical durable effect is rescheduled with the same transaction identity.
- [ ] Enforce table ownership: the crypto worker is the only writer of `NioIngestCryptoEffect`; the coordinator can execute its frozen request but must post the result back without deleting or updating that row.
- [ ] Store a failed Megolm event, deliver its room key in a later frame, and assert one `DECRYPTION_UPDATE` references the original record; repeat with crashes before/after key receipt, restart, duplicate key delivery, and a changed membership epoch.
- [ ] Inject a same-work-ID/different-digest collision and require a terminal integrity error.
- [ ] Redeliver identical Olm ciphertext in a different frame, after restart, and under synthetic changed source epochs; assert the same per-input receipt returns the same result without reratcheting. Task 9 owns real Classic/Sliding rebind integration.
- [ ] Prove only non-idempotent Olm receipts survive frame consumption; transient Megolm receipts are removed and replay remains safe under its event-ID/message-index rule.
- [ ] Inject transaction failure after in-memory mutation and assert the worker rejects all further work until closed/reopened.
- [ ] Route immutable outbound-encryption commands through the same worker; prove serial execution with inbound to-device work without a lock exposed to callers.
- [ ] Assert all crypto mutation and receipt transactions execute on one dedicated thread while an intentionally slow crypto command does not stall an event-loop heartbeat or batch acknowledgements for already-produced work.
- [ ] Store encrypted immutable result bytes in the receipt; a receipt containing only a high-water mark is insufficient to replay the output.
- [ ] Confirm no user callback executes in the worker and no network request is awaited inside its transaction.
- [ ] Keep crypto mutations outside SQLite write-lock hold time in the private worker, then commit one bounded input group of mutated E2EE rows, receipts, and effects in a short transaction. On commit failure discard the in-memory object graph and fail stop before another input.
- [ ] Feed many batchable crypto inputs and assert commit count follows bounded crypto groups, with grouped row writes, rather than input-record count; mutation-kill a one-transaction-per-input implementation. Ordering boundaries may split groups but may not silently force every homogeneous input into its own transaction.
- [ ] With a barrier immediately before and inside the crypto write transaction, prove coordinator commits proceed before `BEGIN`, and once a crypto writer owns SQLite they complete or raise typed contention within the configured busy-timeout/retry bound—never wait indefinitely.
- [ ] Inventory every `BaseClient`/`AsyncClient` access to mutable `olm`, account, device/session stores, group sessions, verification state, key import/export, and trust mutation. Add a worker command or immutable snapshot for each v2 use; no mutable crypto object may cross the port.
- [ ] When v2 starts, reconstruct crypto objects inside the dedicated worker thread from the database and detach the main-thread object graph. Test that direct `client.olm` access then fails rather than creating a second owner.
- [ ] Record actual production additions/deletions and rerun journal/crypto contention benchmarks before Task 8.

Run red, then green:

```bash
uv run pytest -q tests/ingest/crypto_worker_test.py tests/encryption_test.py -k 'olm or megolm or replay or receipt'
uv run pre-commit run --files src/nio/ingest/crypto.py src/nio/ingest/ports.py src/nio/store/crypto_receipts.py src/nio/crypto/olm_machine.py src/nio/client/base_client.py src/nio/client/async_client.py src/nio/store/database.py src/nio/store/models.py tests/ingest/crypto_worker_test.py tests/encryption_test.py tests/async_client_test.py tests/backfill_test.py
```

Commit: `feat(ingest): make crypto work replay safe`

---

## Task 8: Add the Single-Writer Coordinator and Consumer API

**Files:**

- Create: `src/nio/ingest/coordinator.py`
- Create: `src/nio/ingest/network.py`
- Create: `src/nio/ingest/metrics.py`
- Create: `tests/ingest/coordinator_test.py`
- Create: `tests/ingest/consumer_protocol_test.py`
- Create: `tests/ingest/metrics_test.py`
- Modify: `src/nio/ingest/__init__.py`
- Modify: `src/nio/__init__.py`
- Modify: `src/nio/client/async_client.py`
- Modify: `src/nio/client/__init__.py`

**Interfaces:** Implement `open_ingestion()`, `IngestionSession.start()/wait_ready()/wait_baseline_hydrated()/next_batch()/acknowledge()/room_snapshot()/room_hydration_status()/wait_room_ready()/room_ids()/room_directory_snapshot()/metrics_snapshot()/wait_closed()/close()`, `AsyncClientNetworkPort`, immutable command types, process-local `IngestionMetricsSnapshot`, and `TaskGroup` lifecycle.

- [ ] Test CAS-before-publish and CAS-before-effect ordering with recording fakes.
- [ ] Test stale network, crypto, recovery, and membership results are rejected by effect/source/membership epochs.
- [ ] Enforce table ownership: coordinator transitions are the only writers of `NioIngestNetworkEffect`, and no coordinator transaction writes a crypto-owned table.
- [ ] Test `next_batch()` cancellation leaves the oldest batch unchanged.
- [ ] Test acknowledgement cancellation at every await point is safely retryable.
- [ ] Test backpressure pauses request planning and resumes only after matching ack.
- [ ] Test graceful close cancels pure HTTP work, drains commands already posted, persists all committed transitions, closes owned resources, and never waits for application code.
- [ ] Enforce close order: stop scheduling; cancel/drain HTTP results; persist posted owner commands; stop crypto worker and close its SQLite connection; close journal and `MatrixStore.database`; close HTTP; release the retained store lock last. Assert no connection/file descriptor survives lock release.
- [ ] Test forced close followed by reopen reconstructs only journal state, not old mutable objects.
- [ ] Test a changed consumer/journal generation fails before HTTP, batch delivery, or ack. There is no position-preserving rebind API; only a matching database-pair restore or Task 13's explicit destructive reset against a different journal generation can recover.
- [ ] Test terminal auth/protocol/store/crypto failures drain already-durable batches and then raise through `next_batch()`/`wait_closed()`; transient failures retry without invoking a response callback.
- [ ] Test `wait_ready()` completes only after the first source frame is durably staged and reduced, including a genuinely empty frame, and raises the terminal session failure instead of hanging.
- [ ] Test `wait_baseline_hydrated()` independently: Classic full-state, paged Sliding all-room coverage, fallback room-state requests, retryable pending state, and terminal unavailable state. No outbound crypto request for a pending/unavailable room reaches the worker.
- [ ] Expose immutable, non-authoritative observability for records and canonical bytes per batch; record/byte fill ratios; raw-frame, recovery-page, reducer, materialization, crypto-group, and acknowledgement commit counts; staged-frame/ready-lane/unacknowledged-batch depths; oldest unacknowledged-batch age; and commit-latency p50/p95/max by transaction class. Metrics are updated only after the measured transition resolves, never execute a callback, and never add a correctness transaction.
- [ ] With a fake monotonic clock and recording journal/crypto fakes, prove counters increment once per committed unit rather than once per record, gauges fall after drain, oldest-batch age survives ordinary polling, and failed/rolled-back transitions are labeled separately rather than counted as commits.
- [ ] Instrument a one-room commit in a 10,000-room synthetic directory and assert only that key is loaded, allocated, and replaced; all unrelated snapshot identities remain unchanged. Iterating an explicit directory snapshot while later commits occur must remain stable, while no commit copies all rooms.
- [ ] Add a low-level raw transport method that performs authenticated HTTP but never calls `_receive_sync_family()`, `_handle_sync()`, a response callback, or a room mutator; `AsyncClientNetworkPort` is the sole ingestion caller.
- [ ] Add an architecture test that rejects `asyncio.Lock`, `ContextVar`, callback invocation, or `MatrixRoom` in `src/nio/ingest/`.
- [ ] Keep the entrypoint opt-in and test-only; do not switch `sync_forever()` or MindRoom yet.
- [ ] Record actual production additions/deletions and revise the project line forecast before Task 9.

Run red, then green:

```bash
uv run pytest -q tests/ingest/coordinator_test.py tests/ingest/consumer_protocol_test.py tests/ingest/metrics_test.py tests/ingest/crash_recovery_test.py
uv run pre-commit run --files src/nio/ingest/coordinator.py src/nio/ingest/network.py src/nio/ingest/metrics.py src/nio/ingest/__init__.py src/nio/__init__.py src/nio/client/async_client.py src/nio/client/__init__.py tests/ingest/coordinator_test.py tests/ingest/consumer_protocol_test.py tests/ingest/metrics_test.py tests/ingest/crash_recovery_test.py
```

Commit: `feat(ingest): expose single-owner batch stream`

---

## Task 9: Route Membership and Lifecycle Commands Through the Owner

**Files:**

- Modify: `src/nio/ingest/coordinator.py`
- Modify: `src/nio/ingest/config.py`
- Modify: `src/nio/ingest/membership.py`
- Modify: `src/nio/ingest/ports.py`
- Modify: `src/nio/store/sync_journal.py`
- Modify: `src/nio/client/async_client.py`
- Modify: `tests/ingest/membership_test.py`
- Modify: `tests/ingest/coordinator_test.py`
- Modify: `tests/ingest/source_parity_test.py`
- Modify: `tests/async_client_test.py`

**Interfaces:** Add `IngestionSession.join_room()`, `leave_room()`, `forget_room()`, lifecycle commands/results, offline `rebind_ingestion_source()`, and crypto-worker-backed outbound encryption.

- [ ] Route local join/rejoin through the coordinator owner and test that successful HTTP finalization establishes the next membership epoch; incoming own-join observations use the same reducer transition rather than an `AsyncClient` side path.
- [ ] Test successful leave/forget crosses the epoch and records `BASELINE_LOST` when a real gap exists.
- [ ] With a real gap and `R(E)` in flight, test successful join/rejoin commits `LossRecord(membership_epoch=E)` before `ROOM_LIFECYCLE(E+1)` and makes the late result stale.
- [ ] Test rejected join/rejoin/leave/forget changes neither lane nor loss set.
- [ ] After a locally finalized join or leave, deliver its later sync echo and prove the same semantic transition does not increment the membership epoch or emit a second lifecycle/loss record.
- [ ] Test caller cancellation after the server may have accepted the operation detaches the caller but owner finalization still commits.
- [ ] Test callback failure is impossible because lifecycle finalization contains no callback.
- [ ] Test source rebind is refused while the owner is open, while frames/effects remain, or while a real gap lacks an explicit loss policy.
- [ ] Test Classic-to-Sliding and Sliding-to-Classic cold rebind with limited initial rooms, cross-frame duplicate events, duplicated Olm ciphertext, and no inherited cursor claim.
- [ ] Remove gap-aware send gates from the opt-in path; outbound sends use committed membership state and the crypto worker, not recovery locks.
- [ ] `open_ingestion()` installs one narrow `OutboundCryptoPort` on the authenticated client; `room_send()` delegates only encryption/session mutation to it and performs HTTP outside it. Refuse a second active ingestion/crypto owner and detach the port deterministically on close.
- [ ] Test room state/membership changes become immutable `room_snapshot()` values and update the worker's recipient/encryption projection before a later outbound encryption command.
- [ ] Seed a baseline room outside the caller's Sliding windows and prove the reserved coverage/hydration path commits its snapshot and crypto projection before `room_send()` succeeds; pending and unavailable counterexamples must fail with typed errors.

Run red, then green:

```bash
uv run pytest -q tests/ingest/membership_test.py tests/ingest/coordinator_test.py tests/ingest/source_parity_test.py tests/async_client_test.py -k 'leave or forget or membership or rebind or ingestion'
uv run pre-commit run --files src/nio/ingest/coordinator.py src/nio/ingest/config.py src/nio/ingest/membership.py src/nio/ingest/ports.py src/nio/store/sync_journal.py src/nio/client/async_client.py tests/ingest/membership_test.py tests/ingest/coordinator_test.py tests/ingest/source_parity_test.py tests/async_client_test.py
```

Commit: `feat(ingest): serialize membership lifecycle transitions`

---

## Task 10: Prove nio End-to-End and Publish an Integration Release

**Files:**

- Create: `scripts/live_ingest_check.py`
- Create: `tests/ingest/performance_test.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `CHANGELOG.md`
- Modify: all `tests/ingest/*.py` as required by parity findings

**Interfaces:** No new contract. Freeze batch schema version 1 and publish `mindroom-nio==0.40.0rc1` before MindRoom imports the new API.

- [ ] Run the same semantic contract parameterized over Classic and Sliding: initial sync, continuation, limited timeline, recovery, restart, offline source rebind, loss, membership reset, encryption, and backpressure.
- [ ] Include pre-existing baseline rooms outside user-supplied Sliding ranges; both transports must converge to the same ready/unavailable directory and crypto projection before the parity gate passes.
- [ ] Run crash injection at every durable boundary and a state-machine test over frame/recovery/ack/restart commands.
- [ ] Run the live checker against disposable accounts in Classic, Sliding, slam, and reconnect modes; run offline source-rebind tests against a copied store rather than pretending it is a live switch.
- [ ] Build frozen-fixture, fake-consumer NIO-native benchmarks for: a large limited timeline with hundreds and thousands of events; multiple recovery pages; many independent rooms; one stuck room while unrelated rooms continue; a burst of small durable frames that naturally coalesce under backlog; a slow consumer/backpressure; and crash/restart after batch creation but before acknowledgement.
- [ ] For every fixture, record records/canonical bytes per batch, fill ratio, transaction and grouped-SQL counts by class, queue/lane depths, oldest-batch age, commit p50/p95/max, CPU time, peak RSS/allocations, bytes copied, frame-arrival-to-batch-publication latency, batch-publication-to-fake-ack latency, and event throughput. Assert transaction growth is `O(raw frames + recovery pages + bounded crypto groups + materialized batches + acknowledgements)`, not `O(records)`, and reject `O(total rooms)` work for a one-room response.
- [ ] Mutation-test the transaction-count gate by replacing bulk recovery reduction, frame reduction, batch materialization, or acknowledgement with per-record commits; every mutant must exceed the fixture's structural count bound before latency is considered.
- [ ] Establish explicit empirical performance/resource budgets from these reproducible NIO-native results for Tasks 14 and 17. Do not promise an arbitrary speedup percentage before measurement, and do not add timed micro-batching, compression, Rust, or database tuning without evidence from this gate.
- [ ] Measure p50/p95/max coordinator commit wait during bounded crypto writes and Classic/Sliding throughput under identical fixtures.
- [ ] Record actual production additions/deletions through rc1 and stop here for architecture review if projected final net growth exceeds +6,000 lines.
- [ ] Build wheel/sdist, install them into a clean Python 3.12 environment, and rerun the public consumer test.
- [ ] Update version and changelog only after the gates pass; publish/tag the release candidate.

Run:

```bash
uv run pytest -q tests/ingest
uv run pytest -q tests/ingest/performance_test.py
uv run pytest -q
uv run pre-commit run --all-files
uv build
uv run python scripts/live_ingest_check.py --source classic
uv run python scripts/live_ingest_check.py --source sliding
uv run python scripts/live_ingest_check.py --source classic --reconnect
uv run python scripts/live_ingest_check.py --source sliding --reconnect
```

Commit: `release: prepare durable ingestion rc1`

Gate: Do not start cross-repository implementation until the artifact is installable and MindRoom pins its exact version.

---

## Task 11: Add Atomic MindRoom Batch Admission

**Files (MindRoom repository):**

- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Create: `src/mindroom/event_journal/sync_batch_admission.py`
- Modify: `src/mindroom/event_journal/models.py`
- Modify: `src/mindroom/event_journal/schema.py`
- Modify: `src/mindroom/event_journal/store.py`
- Modify: `src/mindroom/event_journal/views.py`
- Modify: `src/mindroom/event_journal/history_debt.py`
- Modify: `src/mindroom/event_journal/__init__.py`
- Create: `tests/test_event_journal_sync_batches.py`
- Modify: `tests/test_event_journal_store.py`
- Modify: `tests/test_event_journal_crash_matrix.py`
- Modify: `tests/test_room_history_debt.py`

**Interfaces:** Pin `mindroom-nio[e2e]==0.40.0rc1`; add `InboundRecoveryLossReason`, `ConsumerBootstrap`, `BatchAdmissionOutcome`, `PrincipalStore.bind_sync_consumer()`, `PrincipalStore.admit_sync_batch()`, and narrow `SyncBatchAdmissionView`.

- [ ] Write schema tests for SQLite and PostgreSQL backends, including receipt/loss uniqueness.
- [ ] Write mixed event/loss/to-device, loss-only, and state-only tests, plus an empty-batch rejection test.
- [ ] Write duplicate replay, same-ID/different-digest, same-sequence/different-ID, concurrent duplicate, unknown schema, unknown record kind, and unknown loss reason tests.
- [ ] Pass a valid batch from another Matrix account/device or event-journal generation to the principal-bound store and require rejection before `Backend.write()`.
- [ ] Race two stream IDs and stale/current consumer generations for one principal; only the bound stream/current generation may insert, and sequence validation/advance must occur inside the batch transaction.
- [ ] On first bind, install the advertised first sequence and snapshot durable active-room IDs/their canonical digest in the same transaction as `matrix_sync_consumers`; exact retries return identical `ConsumerBootstrap` bytes even if later room projections change. Explicit replacement requires a different journal generation plus the destructive-reset operation identity.
- [ ] Before `Backend.write()`, reject a batch above the hard canonical record-count, whole-batch byte, or single-record byte ceiling. Add a contract test that nio's serializer and MindRoom's validator agree at each boundary.
- [ ] Compare serialized local/nio loss-reason sets and require exhaustive translation.
- [ ] Inject failure after every record and assert receipt, events, projections, losses, and history debt all roll back.
- [ ] Assert every injected admission rollback also leaves `matrix_sync_consumers.next_sequence` unchanged.
- [ ] Store nio's exact pre-gap boundary in history debt; never reconstruct it from the newest projected event.
- [ ] Admit a batch containing `loss(E)`, lifecycle `E+1`, and later current-epoch records; assert storage/projections preserve that order atomically and history debt remains keyed to the loss's measured boundary/epoch rather than the room's newest projection.
- [ ] Persist app-relevant to-device inputs as pending journal work keyed by their content-addressed nio record ID; duplicate batches cannot create duplicate desktop/pairing work.
- [ ] Ensure one `admit_sync_batch()` call performs exactly one `Backend.write()`.
- [ ] Feed a syntactically valid batch whose projection raises, and an unsupported schema/kind/reason batch. Assert nio retains the same oldest batch, later sequences cannot pass it, deploying a fixed/exactly pinned consumer admits the identical bytes, and no automatic loss/quarantine/rebind path exists.

Run red, then green from `/work/dev/mindroom`:

```bash
uv run pytest -q tests/test_event_journal_sync_batches.py tests/test_event_journal_store.py tests/test_event_journal_crash_matrix.py tests/test_room_history_debt.py
uv run pre-commit run --files pyproject.toml uv.lock src/mindroom/event_journal/sync_batch_admission.py src/mindroom/event_journal/models.py src/mindroom/event_journal/schema.py src/mindroom/event_journal/store.py src/mindroom/event_journal/views.py src/mindroom/event_journal/history_debt.py src/mindroom/event_journal/__init__.py tests/test_event_journal_sync_batches.py tests/test_event_journal_store.py tests/test_event_journal_crash_matrix.py tests/test_room_history_debt.py
```

Commit: `feat(matrix): admit sync batches atomically`

---

## Task 12: Replace MindRoom Callbacks With the Batch Stream

**Files (MindRoom repository):**

- Create: `src/mindroom/matrix/sync_batch_ingress.py`
- Modify: `src/mindroom/matrix/journal_ingress.py`
- Modify: `src/mindroom/matrix/sync_loop.py`
- Modify: `src/mindroom/matrix/client_session.py`
- Modify: `src/mindroom/journal_dispatch.py`
- Modify: `src/mindroom/bot.py`
- Create: `tests/test_matrix_sync_batch_stream.py`
- Modify: `tests/test_journal_ingress.py`
- Modify: `tests/test_room_member_hooks.py`
- Modify: `tests/test_matrix_sync_continuity.py`
- Modify: `tests/test_matrix_client_session.py`

**Interfaces:** Implement one loop for both sources:

```python
while True:
    batch = await ingestion.next_batch()
    admitted = await journal_ingress.admit_batch(batch)
    journal_dispatcher.wake()  # non-blocking hint; restart scanning is authoritative
    best_effort_signals.offer(admitted.ephemeral_signals)  # non-blocking, drops on pressure
    await ingestion.acknowledge(batch.ref)
```

- [ ] First inventory every current event, response, room-member, to-device, ephemeral, receipt, presence, account-data, and RTC callback registered in `bot.py` and desktop code.
- [ ] Bind the nio stream to `matrix_sync_consumers` before `open_ingestion()` and pass the exact returned journal/consumer generations; a second competing stream must fail before Matrix HTTP.
- [ ] Run `open_ingestion_store()` before constructing a stored `AsyncClient`; propagate `FreshIngestionRequired` without letting client/store startup mutate a non-v1 database, and refuse HTTP until `ConsumerBootstrap` is attached.
- [ ] Check in a classification table naming each as durable projection, nio-internal crypto work, or post-commit best-effort signal. No current callback may be unclassified.
- [ ] Parse/validate the entire batch before entering the journal transaction.
- [ ] Ensure timeline, state, receipts, account data, room lifecycle, decryption updates, and required app to-device facts become durable in admission.
- [ ] Dispatch typing and presence only after commit. Document and test their best-effort semantics.
- [ ] Test `next_batch -> MindRoom commit -> nio ack` order for both Classic and Sliding.
- [ ] Test no ack on parse, storage, enum, integrity, projection, or history-debt failure.
- [ ] Test crash after MindRoom commit/before nio ack, ack failure, duplicate replay, and restart.
- [ ] Replace sync-error response callbacks with typed session failure propagation and preserve the orchestrator's bounded restart/permanent-error policy.
- [ ] Replace first-sync/ready response callbacks with `wait_ready()`; an empty first response must still release startup without creating an empty batch.
- [ ] Test `MindRoom commit -> nio ack -> crash before semantic dispatch`; startup must scan pending journal rows and run the work without nio payload. The `admitted` return is only a wake-up optimization, never the owner of replay.
- [ ] Explicitly test and document that typing/presence may be lost in the commit/dispatch crash window; they are the only accepted best-effort signals.
- [ ] Keep the new loop behind one temporary cutover flag and ensure enabling it disables all old sync/admission callbacks. Never run both paths for one account.
- [ ] Record actual production additions/deletions across both repositories and revise the final line forecast before cutover work.

Run red, then green from `/work/dev/mindroom`:

```bash
uv run pytest -q tests/test_matrix_sync_batch_stream.py tests/test_journal_ingress.py tests/test_room_member_hooks.py tests/test_matrix_sync_continuity.py tests/test_matrix_client_session.py
uv run pre-commit run --files src/mindroom/matrix/sync_batch_ingress.py src/mindroom/matrix/journal_ingress.py src/mindroom/matrix/sync_loop.py src/mindroom/matrix/client_session.py src/mindroom/journal_dispatch.py src/mindroom/bot.py tests/test_matrix_sync_batch_stream.py tests/test_journal_ingress.py tests/test_room_member_hooks.py tests/test_matrix_sync_continuity.py tests/test_matrix_client_session.py
```

Commit: `feat(matrix): consume durable nio batches`

Gate: Do not activate the new path in production until the callback classification has no omissions and both source modes pass the same suite.

---

## Task 12A: Replace MindRoom's Mutable nio Room Cache

**Files (MindRoom repository):**

- Create: `src/mindroom/matrix/room_directory.py`
- Create: `tests/test_matrix_room_directory.py`
- Modify: `src/mindroom/matrix/large_messages.py`
- Modify: `src/mindroom/matrix/rooms.py`
- Modify: `src/mindroom/matrix/presence.py`
- Modify: `src/mindroom/matrix/client_room_admin.py`
- Modify: `src/mindroom/matrix/client_delivery.py`
- Modify: `src/mindroom/matrix_rtc/call_manager.py`
- Modify: `src/mindroom/custom_tools/matrix_room.py`
- Modify: `src/mindroom/custom_tools/subagents.py`
- Modify: `src/mindroom/custom_tools/attachments.py`
- Modify: `src/mindroom/custom_tools/dynamic_workflow.py`
- Modify: `src/mindroom/response_runner.py`
- Modify: `src/mindroom/bot.py`

**Interfaces:** Define a narrow read-only `RoomDirectory` protocol over `nio.RoomSnapshot`/`RoomHydrationStatus`. The production v2 binding delegates to `room_snapshot()`, status/wait methods, `room_ids()`, and explicit immutable directory snapshots; it never wraps a live coordinator mapping. A temporary old-path adapter converts current `MatrixRoom` values to frozen snapshots and is deleted in Task 16.

- [ ] Inventory every `client.rooms` access and the exact properties it consumes. Add missing frozen snapshot fields only when the inventory proves they are needed.
- [ ] Write contract tests for lookup, iteration, member lookup, display name, power levels, encryption, group/member counts, aliases, and lifecycle removal.
- [ ] Test pending/ready/unavailable baseline rooms. Callers that need authoritative state await readiness or surface a typed unavailable result; none falls back to stale MindRoom or `client.rooms` data. Include a Sliding baseline room outside every caller-supplied window.
- [ ] Replace authoritative `client.rooms` reads in tools, delivery, presence, room administration, response display names, bot startup, and RTC call management with the injected directory.
- [ ] Remove every direct cache mutation, including setting `cached_room.encrypted`. A confirmed local state write either posts an owner command or waits for/refetches its durable state observation.
- [ ] Prove a caller retaining an old snapshot cannot mutate or influence a later owner transition.
- [ ] Keep the old adapter isolated to `room_directory.py`; no other production module may mention `MatrixRoom` as a room-state authority.

Run red, then green from `/work/dev/mindroom`:

```bash
uv run pytest -q tests/test_matrix_room_directory.py tests/test_matrix_room_access.py tests/test_matrix_delivery.py tests/test_presence.py tests/test_matrix_rtc_call_manager.py tests/test_multi_agent_bot.py
uv run pre-commit run --files src/mindroom/matrix/room_directory.py src/mindroom/matrix/large_messages.py src/mindroom/matrix/rooms.py src/mindroom/matrix/presence.py src/mindroom/matrix/client_room_admin.py src/mindroom/matrix/client_delivery.py src/mindroom/matrix_rtc/call_manager.py src/mindroom/custom_tools/matrix_room.py src/mindroom/custom_tools/subagents.py src/mindroom/custom_tools/attachments.py src/mindroom/custom_tools/dynamic_workflow.py src/mindroom/response_runner.py src/mindroom/bot.py tests/test_matrix_room_directory.py
```

Commit: `refactor(matrix): use frozen room directory`

---

## Task 12B: Migrate Desktop and Pairing Off Sync Callbacks

**Files (MindRoom repository):**

- Create: `src/mindroom/desktop/ingestion.py`
- Create: `tests/test_desktop_ingestion.py`
- Modify: `src/mindroom/desktop/pairing_receiver.py`
- Modify: `src/mindroom/desktop/pairing_client.py`
- Modify: `src/mindroom/desktop/client.py`
- Modify: `src/mindroom/desktop/command_journal.py`
- Modify: `src/mindroom/cli/desktop.py`
- Modify: `tests/test_desktop_pairing.py`
- Modify: `tests/test_desktop_client.py`
- Modify: `tests/test_desktop_cli.py`

**Interfaces:** Implement a narrow `DesktopBatchConsumer` over the same ingestion stream. It accepts app-relevant to-device records after crypto processing and bulk-admits the batch/input identities plus every record classification before acknowledgement; it does not resurrect nio callbacks or `sync_forever()`.

- [ ] Cloud-side bot pairing/desktop records are admitted as pending MindRoom journal work in Task 11. Their handlers run from the journal dispatcher and settle only after their existing durable pairing/command boundary.
- [ ] Local desktop admission performs one command-journal transaction for the batch receipt and all input/classification rows, using grouped writes rather than one commit per record. Only after that commit may it acknowledge nio. The restart scanner, not the nio payload, then owns pending work.
- [ ] After batch admission/ack, each semantic command may call `DesktopCommandJournal.remember_started()` before its own local side effect and retain the existing completion/replay behavior. These downstream command-lifecycle writes are not ingestion-admission transactions and do not hold or gate nio batches.
- [ ] Cloud response routing may resolve an in-memory waiter after batch admission. If the process dies, the requester also dies and reports an unknown outcome; the durable batch/input receipt still prevents reratcheting or duplicate side effects.
- [ ] Extend the desktop journal with a batch receipt plus all input identities/classifications so crash after local journal commit/before nio ack is deduplicated as one replayed batch.
- [ ] Persist a desktop-local consumer generation and bind its ingestion stream to it; replacing the command journal fails closed and requires an explicitly new desktop stream/cold start rather than cursor reuse.
- [ ] Give non-command records an explicit classification and receipt outcome; never silently leave an unclassified batch record while advancing ack.
- [ ] Test Classic and Sliding delivery, multi-command bulk admission, duplicate Olm ciphertext across frames, crash before/after command journal write, crash before ack, pairing persistence, response-router timeout, and restart. Instrument writes and mutation-kill a per-input admission transaction loop.
- [ ] Remove all `add_to_device_callback`, `add_response_callback`, and `sync_forever()` use from the four desktop/CLI production files before Task 16 may delete the old nio path.

Run red, then green from `/work/dev/mindroom`:

```bash
uv run pytest -q tests/test_desktop_ingestion.py tests/test_desktop_pairing.py tests/test_desktop_client.py tests/test_desktop_cli.py
uv run pre-commit run --files src/mindroom/desktop/ingestion.py src/mindroom/desktop/pairing_receiver.py src/mindroom/desktop/pairing_client.py src/mindroom/desktop/client.py src/mindroom/desktop/command_journal.py src/mindroom/cli/desktop.py tests/test_desktop_ingestion.py tests/test_desktop_pairing.py tests/test_desktop_client.py tests/test_desktop_cli.py
```

Commit: `refactor(desktop): consume durable to-device batches`

---

## Task 12C: Remove MindRoom's Direct Olm/Session Access

**Files (MindRoom repository):**

- Modify: `src/mindroom/matrix/olm_to_device.py`
- Modify: `src/mindroom/matrix/client_session.py`
- Modify: `src/mindroom/matrix/cross_signing.py`
- Modify: `src/mindroom/matrix/conversation_hydration.py`
- Modify: `src/mindroom/matrix/client_delivery.py`
- Modify: `src/mindroom/commands/encryption_commands.py`
- Modify: `src/mindroom/matrix_rtc/key_transport.py`
- Modify: `src/mindroom/desktop/session.py`
- Modify: `src/mindroom/desktop/pairing_receiver.py`
- Modify: `tests/test_matrix_rtc_key_transport.py`
- Modify: `tests/test_matrix_client_session.py`
- Modify: `tests/test_desktop_pairing.py`
- Modify: `tests/test_desktop_client.py`

**Interfaces:** Replace every `client.olm`/session/device-store reach-through with the frozen query and typed command surface on `IngestionSession.crypto`.

- [ ] Port arbitrary authenticated to-device encryption to `encrypt_to_devices()`; no MindRoom code selects or mutates an Olm session directly.
- [ ] Port RTC key transport, desktop identity fingerprints, pairing sender authentication, device lookup, key-count updates, and session establishment to worker commands/snapshots.
- [ ] Preserve exact pinned-device and Ed25519/Curve25519 authentication checks using immutable device snapshots.
- [ ] Test concurrent RTC key send, desktop to-device receive, inbound room key, and room-message encryption; the worker's command order must be deterministic and all callers receive immutable values.
- [ ] Add an architecture test requiring `rg -n '\b(client|self)\.olm\b|\.session_store\b|\._olm_encrypt\b|\.device_store\b|\.uploaded_key_count\b|\.users_for_key_query\b' src/mindroom` to return no production matches.

Run red, then green from `/work/dev/mindroom`:

```bash
uv run pytest -q tests/test_matrix_rtc_key_transport.py tests/test_matrix_client_session.py tests/test_desktop_pairing.py tests/test_desktop_client.py
uv run pre-commit run --files src/mindroom/matrix/olm_to_device.py src/mindroom/matrix/client_session.py src/mindroom/matrix/cross_signing.py src/mindroom/matrix/conversation_hydration.py src/mindroom/matrix/client_delivery.py src/mindroom/commands/encryption_commands.py src/mindroom/matrix_rtc/key_transport.py src/mindroom/desktop/session.py src/mindroom/desktop/pairing_receiver.py tests/test_matrix_rtc_key_transport.py tests/test_matrix_client_session.py tests/test_desktop_pairing.py tests/test_desktop_client.py
```

Commit: `refactor(matrix): route crypto through the worker`

---

## Task 13: Perform a Destructive Fresh-Sync Cutover

**Files:**

- Create: `scripts/initialize_ingestion_store.py`
- Create: `tests/ingest/fresh_start_test.py`
- Modify: `src/nio/ingest/__init__.py`
- Modify: `src/nio/store/sync_journal.py`
- Modify: `src/nio/store/database.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `CHANGELOG.md`
- Create: `/work/dev/mindroom/src/mindroom/matrix/fresh_ingestion.py`
- Create: `/work/dev/mindroom/tests/test_matrix_fresh_ingestion.py`
- Modify: `/work/dev/mindroom/src/mindroom/bot.py`
- Modify: `/work/dev/mindroom/pyproject.toml`
- Modify: `/work/dev/mindroom/uv.lock`

**Interfaces:** Add explicit offline `initialize_fresh_ingestion_store(..., discard_sync_state=True)`, `reset_ingestion_for_replaced_consumer(..., expected_stream_id, expected_old_journal_generation, new_journal_generation, discard_sync_state=True)`, and the MindRoom operator command that binds/attaches the resulting stream. There is no legacy exporter, importer, manifest, token reuse, dual reader, or clean-cutover path.

If the operator is willing to provision a new Matrix device, use a genuinely new database and skip this initializer entirely; that is the absolute greenfield path. The default below keeps the existing device only to preserve its E2EE identity/sessions, not to preserve any sync compatibility.

- [ ] Require the old process stopped and acquire one exclusive v2 store lock. The command refuses unless `discard_sync_state=True` is spelled explicitly; normal application startup never performs this transition.
- [ ] Validate account/device identity and the exact E2EE schema frozen at the cutover commit. Hash representative Olm/Megolm/device/trust rows before and after and prove the initializer neither selects, updates, migrates, nor deletes E2EE payloads.
- [ ] Never select from `SyncTokens`, `SyncRecoveryGaps`, `SyncRecoveryAbandonedRooms`, `PendingTimelineEvents`, or `SlidingWindowTokens`. Their contents are deliberately abandoned, and the new guarantee begins at consumer attach.
- [ ] In one nio transaction create the fresh v1 schema, a new unbound `stream_id`, binding-operation ID, writer epoch, and cold Classic/Sliding source state with no inherited token, `pos`, window token, gap, pending event, or abandonment. Exact retry returns the same unbound operation/stream; wrong account/device fails, and the fresh initializer refuses an existing v1 stream.
- [ ] For a genuinely different MindRoom journal generation, test the separate existing-v1 reset: require the exact old stream/generation; refuse unresolved network/crypto/lifecycle effects; decode and inventory the union of room IDs/evidence in frames, generic ready rows, room state/epoch lanes and lane records, retained acknowledged/unacknowledged batches, and pending decryptions before mutation. Any unreadable or internally inconsistent row refuses the reset.
- [ ] In that reset transaction create a new binding-operation ID, rotate the writer epoch, advance the never-reused source epoch, keep `stream_id` and monotonically increasing `next_batch_sequence`, preserve E2EE/permanent Olm receipts/append-only losses byte-for-byte, persist `NioIngestConsumerResetRoom`, discard the remaining consumer/sync/transient state, and leave meta unbound. Refuse the same journal generation so this operation cannot bypass a poison batch.
- [ ] In one MindRoom transaction create or explicitly replace the consumer at that advertised first sequence, snapshot current durable active-room IDs into `matrix_sync_consumer_baseline_rooms`, and return `ConsumerBootstrap`. In one subsequent nio transaction attach it, union those rooms with matching `NioIngestConsumerResetRoom` rows, create `PENDING` room state plus epoch-0 active lanes, and create a priority `BASELINE_LOST` release plan. Use deterministic `SystemOrigin(FRESH_START, binding_operation_id)` on first initialization and `SystemOrigin(CONSUMER_RESET, binding_operation_id)` on existing-v1 reset; remove reset-inventory rows only in the transaction that durably creates every corresponding loss.
- [ ] Chunk thousands of baseline losses through the ordinary hard-bounded batch materializer. They remain ahead of every new source-derived batch, but no special `PENDING_LOSS_ACK` runtime state exists; normal FIFO/backpressure is sufficient.
- [ ] Classic starts with `next_batch=None`; Sliding starts with a new connection instance and `pos=None`. Content-addressed crypto receipts/E2EE state suppress replayed to-device ciphertext without importing an old to-device cursor.
- [ ] Test crashes after v1 initialization/reset, after MindRoom bind/replace, after nio attach, and before first HTTP. Each step is idempotently resumable; unbound nio state cannot perform HTTP, and attached loss rows cannot be bypassed by later batches.
- [ ] Reset against an empty/older replacement MindRoom journal with distinct room IDs present only in a v1 frame, generic ready row, room state, epoch lane/record, batch, and pending-decryption row. Every ID must receive a durable reset loss; deletion of any one inventory source must make this test fail.
- [ ] Test 10,000 baseline rooms produce only hard-bounded batches and MindRoom advances `next_sequence` transactionally for each. A room first observed later with an unprovable limited baseline gets the ordinary reducer `BASELINE_LOST`; no legacy lookup fills the gap.
- [ ] In both Classic and Sliding cutover fixtures, wait until every baseline room is ready/unavailable before enabling MindRoom outbound room operations. For Sliding, require multiple reserved range expansions plus fallback hydration of a baseline room absent from the server list; caller-supplied partial windows cannot strand it.
- [ ] Keep old sync tables physically dormant. Product code neither drops nor reads them; optional disk reclamation is an operator maintenance action after the observation window, not a migration feature.
- [ ] Before the maintenance window, make an operator-owned SQLite online backup and rehearse restoring/running the old pair on isolated copies. That is a runbook gate, not product code. After any v2 Matrix HTTP or crypto mutation, rollback is unsupported because it would restore stale ratchets; recovery is fix-forward.

First run and publish nio:

```bash
cd /work/dev/mindroom-nio
uv run pytest -q tests/ingest/fresh_start_test.py tests/ingest/consumer_protocol_test.py
uv run pre-commit run --files pyproject.toml uv.lock CHANGELOG.md scripts/initialize_ingestion_store.py src/nio/ingest/__init__.py src/nio/store/sync_journal.py src/nio/store/database.py tests/ingest/fresh_start_test.py
uv build
```

Commit nio: `feat(ingest): add explicit fresh-sync initialization`

Publish/tag `mindroom-nio==0.40.0rc2`. Only after that artifact exists, pin MindRoom exactly to rc2 and regenerate its lock:

```bash
cd /work/dev/mindroom
uv add --exact 'mindroom-nio[e2e]==0.40.0rc2'
uv sync --locked
uv run pytest -q tests/test_matrix_fresh_ingestion.py tests/test_matrix_sync_batch_stream.py
uv run pre-commit run --files pyproject.toml uv.lock src/mindroom/matrix/fresh_ingestion.py src/mindroom/bot.py tests/test_matrix_fresh_ingestion.py
```

Commit MindRoom: `build: pin mindroom-nio rc2` then `feat(matrix): add fresh-sync cutover command`

---

## Task 14: Pass the Full Pre-Deletion Crash and Canary Gate

**Files:**

- Modify: `scripts/live_ingest_check.py`
- Modify: `tests/ingest/crash_recovery_test.py`
- Modify: `tests/ingest/source_parity_test.py`
- Modify: `/work/dev/mindroom/tests/test_event_journal_crash_matrix.py`
- Modify: `/work/dev/mindroom/tests/test_matrix_sync_batch_stream.py`

- [ ] Run a process-kill matrix at every boundary: fresh-store initialization, consumer bind/attach, HTTP result, frame staging, crypto mutation, crypto receipt, reducer commit, batch publish, MindRoom receipt, each record projection, MindRoom commit, nio ack, batch compaction, membership HTTP success, and offline source rebind.
- [ ] Run property/state-machine tests generating sync/recovery/membership/restart/ack interleavings for both sources.
- [ ] Include the epoch-retirement race in the cross-repository crash matrix: in-flight `R(E)`; committed `loss(E) -> lifecycle(E+1)`; stale `R(E)` arrival; MindRoom admission or nio acknowledgement failure; restart and identical replay; then a genuinely failing `R(E+1)`. Prove the `E` loss is neither lost, duplicated, nor applied to `E+1`, and the `E+1` loss remains distinct and retained.
- [ ] Assert the fate invariant after every simulated crash: each generated timeline event is in MindRoom, in a durable nio raw frame, generic ready row, epoch recovery lane, or batch, or is named by a durable loss.
- [ ] Explicitly include unbound/attached fresh-start crashes, priority baseline-loss chunking, ack-before-dispatch restart scan, cross-frame Olm replay, later-frame Megolm keys, held-lane frame retirement, desktop command admission, and source-rebind cases.
- [ ] Run destructive fresh initialization against production-like copies; prove E2EE table digests are unchanged, no old sync row is read, and baseline losses derive only from MindRoom's current durable room projection.
- [ ] Verify the installed artifact is exactly `mindroom-nio==0.40.0rc2` and rerun the cross-repository tests from that artifact before any canary. If this task changes product code, publish/pin rc3 and repeat the artifact gate.
- [ ] Activate disposable canary accounts in Classic and Sliding while the old implementation remains compiled but disabled for those stores. Require successful MindRoom batch receipts and nio acks.
- [ ] Soak both canaries for at least one hour with reconnects, 429s, homeserver restarts, encrypted rooms, limited timelines, leave/rejoin, and consumer stalls.
- [ ] Verify one blocked room does not reduce throughput for unrelated rooms beyond SQLite writer contention.
- [ ] Compare CPU time, peak RSS/allocations, SQLite transaction/grouped-statement counts, coordinator commit wait, frame-to-batch latency, batch-to-ack latency, and throughput against the empirical budgets frozen in Task 10. No unexplained regression or unbounded contention reaches canary; changing a budget requires an explicit architecture/performance review, not a late waiver.
- [ ] As a downstream handoff—not a NIO benchmark specification—the consuming MindRoom repository must run its established sustained-stream and durable-admission benchmarks against the exact installed release candidate before activation. Keep application workload/oracle details in MindRoom's own integration plan.
- [ ] Do not delete old code in this task. A canary failure must be diagnosable against the old implementation and fixtures before the deletion diff obscures it.

Run:

```bash
cd /work/dev/mindroom-nio
uv run pytest -q tests/ingest --hypothesis-show-statistics
uv run pytest -q
uv run pre-commit run --all-files
uv build
uv run python scripts/live_ingest_check.py --source classic --slam --reconnect --duration 3600
uv run python scripts/live_ingest_check.py --source sliding --slam --reconnect --duration 3600

cd /work/dev/mindroom
uv sync --locked
uv run pytest -q tests/test_event_journal_crash_matrix.py tests/test_matrix_sync_batch_stream.py tests/test_desktop_ingestion.py tests/test_matrix_fresh_ingestion.py
uv run pytest -q
uv run pre-commit run --all-files
```

Commit nio: `test(ingest): close pre-deletion crash matrix`

Commit MindRoom: `test(matrix): close batch admission crash matrix`


---

## Task 15: Activate Production While Keeping Old Code for Comparison

**Files:**

- Modify: `/work/dev/mindroom/src/mindroom/bot.py`
- Modify: `/work/dev/mindroom/src/mindroom/config/matrix.py`
- Modify: `/work/dev/mindroom/tests/test_bot_sync_lifecycle.py`
- Modify: `/work/dev/mindroom/tests/test_matrix_sync_batch_stream.py`

- [ ] Stop the old process, take the rehearsed operator backup, run explicit fresh initialization, attach the bound consumer, and verify its priority baseline-loss plan is the oldest durable work before Matrix HTTP begins.
- [ ] Select the v2 path at process startup and forbid live toggling. Once a store has a v1 ingestion marker, the old path in the shipped binary must refuse it; after Matrix HTTP/crypto begins, recovery is fail-stop/fix-forward rather than stale-database rollback.
- [ ] Keep the old code physically present for one production observation window, but make v2 the only legal path for the cut-over store.
- [ ] Require `wait_baseline_hydrated()` before enabling outbound MindRoom room operations; inspect and resolve every `UNAVAILABLE` baseline room explicitly in both source modes.
- [ ] Observe Classic and Sliding production accounts through reconnect, encrypted sends, room recovery, and at least one membership change.
- [ ] Require zero unacknowledged batches outside consumer backpressure, zero unexplained loss records, and a fully drained initial baseline plan before authorizing deletion.

Run from `/work/dev/mindroom`:

```bash
uv run pytest -q tests/test_bot_sync_lifecycle.py tests/test_matrix_sync_batch_stream.py
uv run pre-commit run --files src/mindroom/bot.py src/mindroom/config/matrix.py tests/test_bot_sync_lifecycle.py tests/test_matrix_sync_batch_stream.py
```

Commit: `feat(matrix): activate durable ingestion v2`

---

## Task 16: Delete the Old Architecture

**Files:** Delete/modify every file listed in “Delete after production cutover” and remove old symbols from `AsyncClient`, response models, store models, config, exports, MindRoom bot startup, desktop code, and tests.

- [ ] Make the new path unconditional and remove the temporary feature flag.
- [ ] Remove `_SyncGeneration`, sync response/classic/to-device request locks, room recovery gates, reset fences, ingestion `ContextVar`s, retained callback task machinery, `_receive_sync_family`, `_handle_sync`, `_handle_sliding_sync`, and old recovery pumps.
- [ ] Remove `event_admission_callback`, per-event `admission_accepted` writes, `acknowledge_classic_sync`, `reset_classic_sync_state`, `clear_persisted_recovery`, `backfill_*`, `store_sync_tokens`, atomic-recovery capability branching, mutable recovery outcomes on responses, and gap-aware send bypasses.
- [ ] Remove legacy sync/recovery models and migration code after the observation window. Leave the five physical legacy tables dormant; v2 never reads them, and optional disk reclamation belongs to an operator runbook rather than startup/product code.
- [ ] Remove MindRoom continuity/checkpoint/certification/escape/token modules and callback registration.
- [ ] Delete the old `MatrixRoom` directory adapter and temporary path-selection flag. Keep `FreshIngestionRequired`, explicit initialization, the v1 marker check, and the frozen v2 `RoomDirectory` binding.
- [ ] Port every retained semantic test, then delete topology/compatibility tests.
- [ ] Measure production lines added/deleted across both repos. Stop if the net line budget in Global Constraints is exceeded.
- [ ] Run architecture searches and require zero old symbol references.

Architecture checks:

```bash
cd /work/dev/mindroom-nio
rg -n 'event_admission_callback|acknowledge_classic_sync|reset_classic_sync_state|clear_persisted_recovery|PendingTimelineEvents|SyncRecoveryGaps|SyncRecoveryAbandonedRooms|SlidingWindowTokens|_sync_response_lock|ContextVar' src tests
rg -n 'asyncio\.Lock|ContextVar|ClientCallback|MatrixRoom' src/nio/ingest
rg -n '\b(self|client)\.olm\b' src/nio/client

cd /work/dev/mindroom
rg -n 'sync_continuity|sync_checkpoint_trust|sync_certification|sync_recovery_escape|sync_token_values|event_admission_callback|add_(event|to_device|response)_callback|client\.(sliding_)?sync_forever' src tests
rg -n 'client\.rooms' src
rg -n '\b(client|self)\.olm\b|\.session_store\b|\._olm_encrypt\b|\.device_store\b|\.uploaded_key_count\b|\.users_for_key_query\b' src/mindroom
```

Expected result: no production reference; only fresh-start fixtures may name legacy table strings.

Run full gates:

```bash
cd /work/dev/mindroom-nio
uv run pytest -q
uv run pre-commit run --all-files
uv build

cd /work/dev/mindroom
uv run pytest -q
uv run pre-commit run --all-files
```

Commit nio: `refactor(ingest)!: remove callback sync architecture`

Commit MindRoom: `refactor(matrix)!: remove legacy sync continuity layer`

---

## Task 17: Repeat Final Verification and Release

- [ ] Repeat the entire Task 14 crash matrix against the deletion commit; deletion must not change any semantic result or failure boundary.
- [ ] Repeat Task 10's frozen NIO-native performance suite and require every empirically frozen transaction, latency, memory, and backpressure budget to pass; rerun the consuming repository's separately owned acceptance benchmark against the exact final artifact.
- [ ] Repeat one-hour Classic and Sliding soaks from clean, cut-over stores.
- [ ] Publish the final nio release, update MindRoom from the RC pin to the exact final pin, regenerate `uv.lock`, and rerun both full suites from clean checkouts.

Final commands:

```bash
cd /work/dev/mindroom-nio
uv run pytest -q
uv run pytest -q tests/ingest --hypothesis-show-statistics
uv run pre-commit run --all-files
uv build
uv run python scripts/live_ingest_check.py --source classic --slam --reconnect --duration 3600
uv run python scripts/live_ingest_check.py --source sliding --slam --reconnect --duration 3600

cd /work/dev/mindroom
uv sync --locked
uv run pytest -q
uv run pre-commit run --all-files
```

Commit nio: `release: finalize durable ingestion`

Commit MindRoom: `release: pin durable nio ingestion`

## Completion Criteria

The rewrite is complete only when all of these are true:

- Both source modes pass one parameterized semantic and crash suite.
- MindRoom acknowledges only after one atomic journal admission transaction.
- Olm to-device replay after a crash returns a receipt without reratcheting.
- No application callback or semantic task executes inside nio ingestion ownership.
- No legacy sync lock, `ContextVar`, recovery capability flag, application-owned checkpoint, or callback acceptance write remains.
- Fresh initialization preserves the exact supported E2EE state, never reads old sync rows, and places bounded `BASELINE_LOST` records for MindRoom's pre-existing active rooms ahead of new source records.
- A consumer/journal mismatch and a poison batch both fail stopped with original nio payload retained; neither is silently skipped or mislabeled as Matrix history loss.
- Unknown wire values fail before admission and ack.
- A one-room response performs O(affected rooms) work, not O(all rooms).
- Full test, lint, build, live-soak, line-budget, and crash-matrix gates pass from clean worktrees.

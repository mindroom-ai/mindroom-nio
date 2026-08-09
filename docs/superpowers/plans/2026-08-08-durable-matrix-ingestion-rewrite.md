# Durable Matrix Ingestion Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (preferred when available) or `superpowers:executing-plans` to implement this plan task by task, `superpowers:test-driven-development` for every behavior change, and `superpowers:verification-before-completion` before each commit or release gate.

**Goal:** Replace the current callback-, lock-, and recovery-patch-based sync subsystem with a durable ingestion stream in which every Matrix timeline event is either durably admitted to MindRoom, retained by nio for retry, or accompanied by an explicit durable loss record.

**Architecture:** A new `nio.ingest` package owns Classic Sync and Sliding Sync through one queue-driven coordinator, transport-specific pure adapters, a pure per-room reducer, a fresh version-1 ingestion journal, and one serialized replay-safe crypto worker. nio stages immutable frames and publishes immutable FIFO batches. MindRoom admits a whole batch in one event-journal transaction and only then acknowledges it to nio. The two databases never share a transaction; idempotent replay is the boundary.

The plan calls this replacement implementation “v2” when contrasting it with the callback architecture; its new wire and journal schemas both start at version 1.

**Tech Stack:** Python 3.12+, `asyncio.TaskGroup`, frozen/slotted dataclasses, `enum.StrEnum`, SQLite/Peewee for the nio-local journal and E2EE state, AES-GCM for staged payloads, MindRoom's existing SQLite/PostgreSQL event-journal abstraction, pytest, Hypothesis where state-machine coverage is valuable, Ruff, pre-commit, and uv.

## Global Constraints

- Both Classic Sync and Sliding Sync are first-class. Neither may be implemented as a compatibility wrapper around the other.
- There is one active ingestion source per Matrix account/device/store and one immutable stream binding per MindRoom principal consumer. Classic and Sliding are both first-class choices when that previously unbound consumer and v1 store are created; `NioIngestMeta.transport_kind` is immutable thereafter. v1 never translates continuity or rebinds an existing principal. Exercising the other transport later requires a genuinely new consumer identity/journal (normally a new test principal/account), not merely another Matrix device.
- The coordinator is the sole logical owner of mutable ingestion state. One serialized crypto worker separately owns all Olm/Megolm objects and performs expensive crypto computation outside a write transaction. One thread-affine physical ingestion-store writer owns the sole SQLite connection used by the journal and v1 E2EE storage. Task 4.5 establishes its synchronous typed transaction seam; Task 7 adds an immutable owner-thread receipt-lookup → worker-prepare → owner-thread commit handshake. Network workers, the crypto worker, callers, and MindRoom exchange only immutable epoch-tagged commands and results.
- Allow one in-flight primary sync request per account/device. Recovery requests may run concurrently across rooms, one per room, because they return immutable epoch-tagged results.
- No application callback runs in the coordinator, under a nio lock, or inside a nio database transaction.
- A state transition becomes visible in memory, publishes a batch, or schedules a follow-up effect only after its journal transaction commits.
- A transport cursor may advance only when the corresponding immutable raw frame is durable in the nio journal.
- A frame may be retired only when every record is durably represented in a batch, a generic ready row, or its room's durable recovery lane. A loss is an ordinary ready/lane record, never a second side ledger.
- A batch may be retired only after MindRoom's admission transaction commits and nio accepts a matching FIFO acknowledgement.
- No NIO ingestion persistence path may commit once per Matrix event. Its transaction count must scale with durable raw frames, recovery pages, bounded crypto groups, materialized batches, and acknowledgements—not with the number of records inside those units. MindRoom and desktop admission each use one downstream transaction per delivered batch; later application work is outside this ingestion invariant.
- When ready work is backlogged, the materializer takes deterministic oldest eligible heads across already-durable frames and room lanes until none remains or the next head would exceed configured record/byte capacity, then publishes that batch. When no backlog exists, it publishes available work immediately; it never waits for a future record, and v1 adds no batching timer or artificial latency window.
- Recovery is per room. A stuck room must not stop unrelated rooms, global account data, or another transport frame from making progress.
- A terminal loss record is ordered before any held live records it releases.
- Olm/Megolm mutations and their replay receipt commit in the same SQLite transaction. A separate receipt write is not sufficient.
- Batch IDs and SHA-256 digests are computed from canonical bytes by nio, recomputed when read, and recomputed independently by MindRoom. No caller-supplied fingerprint is trusted.
- Unknown batch schemas, record kinds, or loss reasons fail closed before MindRoom writes a receipt or nio receives an acknowledgement.
- The replacement never parses or migrates an old store. Activation provisions a fresh Matrix device and genuinely new SQLite database, then cold-starts a new stream. The old database remains an operator-owned rollback artifact and v2 never opens it.
- No backwards-compatibility shim, dual reader, legacy-row importer, in-place initializer, or runtime fallback exists. The old store is never opened by v2.
- Keep nio's per-device E2EE database separate from MindRoom's event journal. Do not introduce a distributed transaction.
- Bind each nio stream to the MindRoom journal's durable generation UUID. A replaced/restored consumer journal must fail closed rather than resume from nio's advanced cursor.
- Raise `mindroom-nio`'s Python floor to 3.12 before using `StrEnum` or `TaskGroup`; MindRoom already requires 3.12.
- Actual production growth through Tasks 1–4 is +5,578 net (`src/nio` plus scripts) against the original +1,300–2,000 estimate. Task 5 is blocked until Task 4.5 consolidates the durable core. With the now-explicit writer/effect work and measured deletion inventory below, the revised final envelope is about +2,700 to +6,600 net. Reforecast after Tasks 4.5, 6, 7, 9, 10, 12, and 13; stop before the next task if the high-side projection exceeds +6,000 or a task is more than 50% above its current estimate. The current high side therefore requires a second architecture/size review at the Task 4.5 gate before Task 5.
- Timed micro-batching, compression, Rust extensions, and speculative SQLite tuning are out of scope until Task 10 measurements identify a concrete bottleneck.

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
2. The replacement does not attempt to reinterpret legacy pending events, gaps, tokens, sticky losses, or E2EE rows. Task 13 provisions a fresh Matrix device/database and records `BASELINE_LOST` for rooms already authoritative in MindRoom before new HTTP begins. An existing v1 stream is never destructively rebound or reset.
3. `BatchRef` identity is computed and validated from canonical bytes; `dataclasses.replace()` with a stale digest must fail.
4. Task 7 implements same-transaction crypto receipts, including Olm to-device replay. The plan no longer assumes replay safety.
5. The new stream remains test-only until crypto ordering and every durable/best-effort record class are implemented. MindRoom activation happens later.
6. MindRoom owns a local loss enum with exhaustive mapping. A future unknown nio value fails closed.
7. Membership transitions now have one owner and an explicit epoch order: unresolved `E` loss, then lifecycle `E+1`, with stale `E` recovery ignored and genuine `E+1` failure retained.
8. Persistence cardinality is normative and measured: frames, pages, bounded crypto groups, materialized batches, and acknowledgements create transactions; individual records do not. Backlog coalescing is immediate and limit-driven, never timer-driven.

One review premise is deliberately rejected: this is not a `MatrixStore` v10-to-v11 migration that drops five tables. The new journal has its own version-1 schema in a fresh device database. Supporting or modifying an old store would preserve the wrong boundary, so v2 refuses it entirely.

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
    ROOM_READINESS = "room_readiness"
    DECRYPTION_UPDATE = "decryption_update"


class SystemOriginKind(StrEnum):
    FRESH_START = "fresh_start"
    MEMBERSHIP_CHANGE = "membership_change"
    ROOM_HYDRATION = "room_hydration"
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
    origin: RecordOrigin | SystemOrigin
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

`RoomSnapshot` is a reducer/crypto read model, not part of batch identity and not a second application room directory. Derived properties such as `display_name`, `member_count`, and `is_group` are pure calculations over frozen fields. The coordinator owns a private affected-room cache and exposes only single-room `room_snapshot(room_id)`, `room_hydration_status(room_id)`, and `wait_room_ready(room_id)` queries needed for outbound Matrix operations. A commit replaces/removes only affected keys and preserves unchanged `RoomSnapshot` identities. MindRoom owns its application-wide room directory projection from atomically admitted lifecycle/state records; nio exposes no live mapping, `room_ids()`, or whole-directory snapshot API. A baseline room is `PENDING` until authoritative current state commits, then `READY`; a confirmed not-joined/forbidden room is `UNAVAILABLE`. Outbound encryption requires `READY`. The old mutable `AsyncClient.rooms` cache is not an authority and is removed at final cutover.

Frame, record, and loss identities are deterministic too:

```text
frame_id  = UUIDv5(stream_id, source_epoch:request_id:source_sha256)
record_id = UUIDv5(stream_id, kind:room_id:event_id)             # Matrix event ID exists
record_id = UUIDv5(stream_id, kind:crypto_input_sha256)          # app to-device/crypto result
record_id = UUIDv5(frame_id, frame_index:kind:source_sha256)     # truly frame-local signal
record_id = UUIDv5(stream_id, kind:room_id:membership_epoch:system_kind:operation_id:source_sha256)  # system fact
loss_id   = UUIDv5(stream_id, room_id:membership_epoch:origin_id:reason:boundary_sha256)
```

Transport/recovery losses retain the exact `RecordOrigin` that detected them. A loss created without a source frame uses a deterministic `SystemOrigin`: initial consumer attach uses `FRESH_START`, confirmed local membership change uses `MEMBERSHIP_CHANGE`, and a room-keyed stored-row failure uses `STORE_VALIDATION`. `EventRecord` accepts `SystemOrigin` only for `ROOM_LIFECYCLE` with `MEMBERSHIP_CHANGE`, or for hydration-derived `STATE`/`ROOM_READINESS` with `ROOM_HYDRATION`; sync-observed facts retain their exact `RecordOrigin`. Hydration emits the authenticated Matrix state events plus a canonical `{status: "ready"}` fact, or only `{status: "unavailable"}` when the endpoint proves that state; all share the deterministic effect-bound origin and commit together. Membership epoch `0` is reserved for a fresh-start placeholder loss before authoritative membership is hydrated; the first committed membership observation advances that room to epoch `1`.

An empty normalized frame advances only the nio-local staged cursor and is retired locally after reduction; it does not create an empty cross-database batch. Every public `SyncBatch` has at least one event or loss record.

Public entrypoint:

```python
bootstrap = nio.open_ingestion_store(
    store_path,
    account_id=account_id,
    device_id=device_id,
    source=source_config,
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

The source type passed to `open_ingestion_store()` selects and durably freezes the transport kind in the store-creation transaction. Classic `next_batch`, Sliding `pos`, and Sliding to-device `since` are different continuity domains, so there is no live or offline source-rebind API. Reopening requires a source config of the same type, while non-continuity request settings may change because frozen outstanding requests authenticate their own safety overlays. An already-bound MindRoom principal cannot change modes; tests and deployments choose the transport at first binding, and cross-mode evaluation uses a separate unbound consumer identity.

`open_ingestion_store()` is mandatory and runs before `MatrixStore` construction. It acquires the v2 lifetime lock and opens an existing v1 journal or atomically creates one in a genuinely new database. Any non-v1 database raises `FreshIngestionRequired` without DDL; Task 13 provisions a fresh Matrix device/database instead of mutating it. v2 supports only the SQLite-backed `SqliteStore` trust/E2EE schema—never `DefaultStore`, `SqliteMemoryStore`, or a third-party store—so trust, E2EE state, receipts, effects, and journal rows can share the sole physical connection. The returned bootstrap retains the lock and writer lifetime until transferred to `IngestionSession`; it is single-use.

A newly initialized meta row has a fresh `binding_operation_id`, is unbound, and cannot perform Matrix HTTP. `bind_sync_consumer()` atomically binds that operation/stream/first-sequence tuple in MindRoom, then returns a `ConsumerBootstrap` containing the current journal/consumer generations plus MindRoom's durable active-room IDs and their canonical sorted tuple/digest. `attach_consumer()` recomputes that digest, validates the operation and immutable first sequence, and enters `ATTACHING`. It processes at most 256 rooms per transaction, advancing a durable next-room ordinal after each chunk and yielding to the event loop between chunks; exact retries resume from that ordinal. Only the final chunk marks `ATTACHED` and enables HTTP/batch delivery. On ordinary restart the existing operation, binding, first sequence, baseline digest, and any resumed tuple must match exactly. The guarantee horizon begins at completed attachment; the old store is deliberately out of scope.

The frozen configuration is explicit and bounded:

```python
@dataclass(frozen=True, slots=True)
class ClassicSourceConfig:
    timeout_ms: int
    filter_json: bytes


@dataclass(frozen=True, slots=True)
class SlidingSourceConfig:
    timeout_ms: int
    connection_name: str
    lists_json: bytes
    room_subscriptions_json: bytes
    extensions_json: bytes
    all_rooms_page_size: int = 100


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
    max_recovery_events_per_room: int = 10_000
    max_recovery_pages_per_room: int = 200
    recovery_page_size: int = 50
    recovery_timeout_ms: int = 30_000
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

`Reduction` contains replacement source/room/lane values, ready and lane-record candidates, frame-consumption IDs, and deterministic network-effect inserts/deletes. It contains no revision or ready-order allocation, SQL row, coroutine, callback, `MatrixRoom`, mutable event object, lock, clock, future, or database handle. Task 6 alone translates it into one compare-and-swap journal transition.

The coordinator's order is fixed:

1. Validate only the command's relevant immutable fences: source epoch for a primary-sync result; effect/stream/transport/gap/membership identity for recovery or hydration; durable work/receipt/membership identity for crypto; and operation/membership identity for a local lifecycle command.
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
    transport_kind CHECK (transport_kind IN ('classic', 'sliding'))
    binding_operation_id
    consumer_attach_status CHECK (consumer_attach_status IN ('unbound', 'attaching', 'attached'))
    consumer_first_sequence NULL until consumer attach starts
    consumer_attach_next_room_ordinal
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
    state_ciphertext (authenticated RoomState payload: snapshot + membership baseline)
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
    ready_order NULL while the lane has no eligible release head
    next_held_ordinal
    successor_membership_epoch NULL unless retiring
    lane_state_ciphertext (authenticated RecoveryGap + pending lifecycle payload)
    lane_state_sha256
    updated_revision
    PK(account_id, room_id, membership_epoch)
    INDEX(account_id, ready_order)

NioIngestReadyRecord
    account_id
    ready_order
    item_id (event.record_id or loss.loss_id)
    item_kind CHECK (item_kind IN ('event', 'loss'))
    source_frame_id
    room_id NULL for account-wide records
    membership_epoch NULL for account-wide records
    room_sequence NULL for account-wide records
    payload_ciphertext
    payload_sha256
    canonical_bytes
    created_revision
    PK(account_id, item_id)
    UNIQUE(account_id, ready_order)

NioIngestLaneRecord
    account_id
    room_id
    membership_epoch
    section CHECK (section IN ('loss', 'recovered', 'held'))
    page_ordinal (0 for held rows)
    record_ordinal
    item_id (event.record_id or loss.loss_id)
    item_kind CHECK (item_kind IN ('event', 'loss'))
    source_frame_id NULL for effect/system-derived records
    source_effect_id NULL for frame/system-derived records
    payload_ciphertext
    payload_sha256
    canonical_bytes
    created_revision
    PK(account_id, room_id, membership_epoch, section, page_ordinal, record_ordinal)
    UNIQUE(account_id, item_id)

NioIngestFrame
    account_id
    frame_id
    source_epoch
    request_id
    payload_ciphertext (one frozen NetworkRequest + one canonical raw success body)
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
    room_id NULL for account-wide effects
    source_epoch NULL for recovery, hydration, and membership effects
    membership_epoch NULL for account-wide effects
    attempt_ordinal NOT NULL DEFAULT 0
    membership_delivery_state NULL unless effect_kind = 'membership'
    prior_delivery_uncertain NULL unless effect_kind = 'membership'
    request_ciphertext
    request_sha256
    created_revision
    PK(account_id, effect_id)
    UNIQUE(account_id, room_id) WHERE effect_kind = 'membership'

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
    PK(account_id, sequence)
    UNIQUE(account_id, batch_id)

```

`NioIngestMeta` never persists the sidecar lock file's device/inode. The lifetime owner still holds `flock`, checks that its path has not been replaced before each operation, retains database-file identity while open, and rotates `writer_epoch` through compare-and-swap. A consistent online-backup copy may therefore open under its own newly created sidecar, while two owners of one path and stale/replaced handles still fail closed. Only one restored clone may be active operationally.

`NioIngestRoomState` is the current authoritative room snapshot/epoch head; its authenticated payload includes the epoch-scoped `MembershipBaseline`. `NioIngestRoomLane` is epoch-keyed, and its authenticated payload includes `RecoveryGap | None` plus `pending_lifecycle | None`, so restart does not reconstruct continuity from transient adapter output. One room may have an active `E+1` lane while bounded output from retiring `E` drains. Normal reduced records, including account-wide records and immediately eligible losses, move into individually encrypted `NioIngestReadyRecord` rows; gap-blocked losses/recovered/held payloads use `NioIngestLaneRecord`. A `LossRecord` has exactly one nio-internal durable owner at a time—ready row, lane row, or unacknowledged batch. MindRoom becomes the authoritative audit owner in its admission transaction; nio retains no acknowledged payload or parallel loss ledger.

Recovery pages receive monotonic page ordinals as they walk forward (`dir=f`) from the old membership baseline toward the current live-window token. Any generic ready records already assigned to epoch `E` retain their earlier ready order. Within a terminal lane release, every loss precedes the held records it releases; all remaining `E` ready/recovery-lane records precede its pending successor lifecycle record. Only then may ready records from the successor epoch materialize. Other rooms may interleave at every bounded chunk. Materializing one batch inserts its canonical payload, deletes only ready/lane records now owned by that batch, and advances small cursors/barriers in one transaction; it never rewrites the whole lane.

Batch selection never depends on Python mapping or SQLite row order. `NioIngestMeta.next_ready_order` allocates contiguous values in the same transition that makes generic records or a recovery-lane head eligible. The materializer compares persisted `ready_order`, then a frozen cross-kind rank and stable primary key; records behind one room head remain ineligible. A partially drained lane receives a new tail `ready_order` in the batch transaction so bounded chunks round-robin with other ready work. This exact head-order policy makes restart or a different query plan produce byte-identical batches without letting one large lane monopolize the writer.

The batch table contains only unacknowledged outbox rows. Acknowledgement rereads/revalidates that payload, atomically advances `last_acked_sequence`/ID/digest in meta, and deletes the acknowledged row. An exact retry matches only those stored metadata; it does not require or retain the payload. Older or same-sequence/different-identity acknowledgements are rejected. A frame is deleted when each derived record is in a batch, a generic ready row, or an epoch-keyed recovery lane. Content-addressed receipts for non-idempotent Olm inputs remain across frame retirement and restart so repeated ciphertext cannot reratchet; they have no automatic correctness-risking TTL. Receipts for idempotent Megolm/derived work are deleted when the frame's durable output commits.

Network/recovery/lifecycle effects and crypto effects deliberately use separate typed tables and logical owners. The coordinator alone proposes mutations for `NioIngestNetworkEffect`. The crypto worker alone proposes mutations for `NioIngestCryptoEffect`, including key query/claim/upload and protocol to-device sends. Both persist through one thread-affine `IngestionStoreWriter` and its literal single SQLite connection. Task 4.5 keeps journal and SQLite-backed E2EE compatibility calls synchronous on the opening thread. Task 7's async caller posts immutable crypto input to the worker thread, receives a frozen prepared transaction, resumes on the opening event-loop thread to commit it, then returns a typed commit-success/failure message to the worker before that worker accepts another input. No second connection or arbitrary callback crosses the boundary. The coordinator may execute a frozen HTTP request but treats its result as opaque and posts it back to the logical owner; only that owner validates the effect/epochs and proposes deleting or advancing its row in the same transaction as its owned state. Restart reschedules only effects eligible under their durable state: recovery/hydration requests and membership `READY`; it preserves membership `DISPATCHED_UNCONFIRMED` without sending it. No actor mutates the other's effect row.

Use the existing recovery-payload key material and AES-GCM primitives, but define new contextual AAD including table, account, stream, primary key, schema version, and payload digest. Reject `SqliteQueueDatabase`: its statements do not form a real atomic transaction. The dedicated store writer owns the only v2 write-capable SQLite connection. Crypto computation runs on its own actor/thread without a write transaction, then submits a bounded mutation group; neither HTTP nor crypto computation executes inside the store writer. Tests prove serialized write ownership, bounded transaction duration, and fail-stop behavior. Add another physical writer only if Task 10 measures a concrete bottleneck. Crash tests use an on-disk temporary database, not an in-memory substitute.

## Source and Recovery Semantics

Classic and Sliding normalize into the same `SyncFrame`:

```python
@dataclass(frozen=True, slots=True)
class SyncFrame:
    frame_id: UUID
    origin: RecordOrigin
    request_cursor_json: bytes
    candidate_cursor_json: bytes
    source_sha256: bytes
    to_device_json: tuple[bytes, ...]
    device_list_delta_json: bytes
    one_time_key_counts_json: bytes
    unused_fallback_key_types_json: bytes
    room_segments: tuple[RoomSegment, ...]
    ephemeral_json: tuple[bytes, ...]
    global_account_data_json: tuple[bytes, ...]
    presence_json: tuple[bytes, ...]
```

`SyncFrame` is a pure normalized view and never carries the complete raw response. A successful `SourceResult` carries the frozen `NetworkRequest` plus the one canonical raw response body. The frame transaction encrypts that pair once as `NioIngestFrame`; on restart nio re-runs the transport adapter over those durable bytes and requires the same `source_sha256`/`frame_id`. Parsed event arrays and room segments are never persisted as a second copy of the raw response.

Every `RoomSegment` carries the adapter-produced `MembershipObservation` alongside its event arrays. Sliding derives it once from top-level membership plus exact own-member state/live-suffix evidence; Classic receives `own_user_id` too and produces the same transport-neutral value. Task 5 consumes this field directly and never reparses transport JSON to rediscover membership evidence.

- `ClassicCursor(next_batch)` and the resolved persisted `SlidingCursor(pos, to_device_since, connection_instance, connection_name, all_rooms_range_end, all_rooms_page_size, acknowledgement_mode, all_rooms_coverage_complete)` are distinct frozen state types; no function accepts their union without matching on `TransportKind`.
- Classic stores `next_batch` with the raw-frame transaction.
- Sliding `pos` is valid only for the live connection. A process restart or `M_UNKNOWN_POS` creates a new `connection_instance` and discards only `pos`; the independent to-device `since` cursor remains durable. Durable room lanes and event/content receipts suppress duplicates and determine whether a room has a provable baseline.
- `source_epoch` fences same-transport request generations, not transport selection. Restart first replays any frozen durable request under its original epoch; a committed Sliding `M_UNKNOWN_POS` reset then advances the epoch/connection instance and makes older results stale. It never authorizes Classic↔Sliding translation.
- A primary `RESET_REQUIRED` result cannot commit while any staged frame from its current/older source epoch remains. The coordinator stops source HTTP, drains/reduces those immutable frames under their captured request cursor in order, then commits the reset and resumes. If the process dies first, no reset cursor was committed; restart drains the same frames and retries the still-current request. A reset never discards a staged frame and an older frame never regresses the new cursor.
- Consumer attach creates MindRoom's initial `PENDING` directory projections in its binding transaction, then creates nio `PENDING` room state plus an epoch-0 active lane for every baseline room in resumable bounded chunks. Classic's cold request uses full state and reconciles that entire set. Sliding v1 freezes the positive `SlidingSourceConfig.all_rooms_page_size` into its initial cursor, always adds reserved list `__nio_all_rooms_v1`, expands its ranges by that durable size until the server-reported count is covered, and keeps that coverage active; caller lists/subscriptions are additional and cannot use that name or weaken primary-ingestion coverage.
- The internal Sliding list requests every state type/member field required by `RoomSnapshot` and the crypto recipient projection. Any baseline ID not observed after Classic's initial full-state frame or Sliding's full list coverage receives a bounded authenticated room-state hydration request. A committed authoritative snapshot changes it to `READY`; a confirmed not-joined/forbidden response changes it to `UNAVAILABLE`; retryable failures remain `PENDING`. Every committed `READY`/`UNAVAILABLE` transition emits an ordered `ROOM_READINESS` fact with its frame origin or deterministic effect-bound `SystemOrigin(ROOM_HYDRATION, effect_id)`, so MindRoom projects the same state without reading nio's private cache.
- `wait_baseline_hydrated()` resolves only when all attached baseline rooms are `READY` or `UNAVAILABLE`. Normal inbound batching can proceed while hydration runs, but `room_snapshot()` and outbound encryption never fabricate state for a pending room; `wait_room_ready()` either returns the committed snapshot or raises typed unavailability.
- Classic `device_lists`/key counts and Sliding's E2EE extension normalize into the same crypto-control fields; neither transport may drop device invalidation, one-time-key counts, or fallback-key state.
- v1 exposes no source-rebind transition. Tests exercise both startup choices on separate previously unbound consumer identities rather than pretending a cross-mode token translation or principal rebind exists.
- Recovery pages may run concurrently across rooms, with at most one outstanding page per room lane. One completed page is validated and reduced with one journal transition using bulk row operations; the implementation must not commit each recovered event separately.
- Reducing a frame atomically moves eligible records into encrypted generic ready rows and records blocked by a room gap into that epoch's encrypted held lane, then retires the frame. A gap or predecessor-epoch barrier in one room therefore cannot consume the global staged-frame bound. Per-room held-event/byte caps terminate with `EVENT_LIMIT` before the lane can grow without bound.
- Timeout, 408, 429, and retryable 5xx retain the lane without a loss.
- Terminal fetch rejection becomes `FETCH_FAILED`; cursor cycles/impossible exhaustion become `UNVERIFIABLE`; budget exhaustion becomes `EVENT_LIMIT`; a membership transition with a real unresolved gap becomes `BASELINE_LOST`. An unreadable room-keyed lane/pending-decryption row becomes `CORRUPT_STORED_RECORD` for that known room. An unreadable whole raw frame has no trustworthy room inventory and therefore fails stop with `JournalCorruptionError`; nio retains the row and cursor and does not invent a room loss or continue past it.
- `UNKNOWN` is intentionally absent from the new producer enum. The producer must name a cause; discarded pre-v2 sync rows are never reinterpreted as new facts.
- Recovered records precede held live records. A terminal loss precedes held records it releases.
- Resolving a lane atomically creates a durable ordered release plan; it does not require one giant batch. Successful recovery materializes bounded FIFO chunks of recovered records and only then bounded chunks of held live records. Terminal recovery materializes the loss as the first record of the first bounded chunk and held live records only in that or later chunks. One `SyncBatch` may coalesce ready records from multiple already-durable frames and room lanes while preserving every lane's order. Global FIFO acknowledgement preserves the boundary even when other rooms are interleaved between chunks.
- Record-count and canonical-byte ceilings are absolute for every batch-materialization and MindRoom admission transaction. A 10,000-event/32-MiB lane is released through multiple bounded transactions and cannot monopolize the shared journal writer.
- Config defaults bound raw staged frames, unacknowledged batches, records per batch, and bytes per batch. Reaching a bound pauses new sync requests; it never discards a durable frame.

Each room has a monotonic membership epoch. Incoming own-member invite/leave/ban/rejoin and confirmed coordinator-owned join/rejoin/leave/forget success cross epochs. One semantic own-membership transition crosses exactly once: a later sync echo of an already-finalized local join/leave reconciles state inside the current epoch instead of incrementing it again. Crossing an epoch invalidates old recovery effects and Sliding baselines. It also atomically resolves the room's sole pending membership operation: an exact join/leave postcondition finalizes it, while an unrelated transition supersedes it; neither case permits a late old-epoch result to mutate the successor. A real unresolved gap becomes durable `BASELINE_LOST` in the same transaction as the reset. A rejected membership request changes nothing. Once an HTTP membership request may have reached the server, caller cancellation detaches the caller but not owner finalization.

Epoch transition order is a hard invariant. In one journal transition, ending membership epoch `E` marks its lane retiring, creates `LossRecord(membership_epoch=E)` when its gap is unresolved, stores the pending `ROOM_LIFECYCLE(E+1)` barrier, creates the active `E+1` lane, and advances `NioIngestRoomState`. The active lane may retain new `E+1` work immediately, but no `E+1` record is eligible for batch materialization until retiring `E` has emitted its loss and all retained `E` records, followed by the lifecycle barrier. Large retirements therefore remain bounded without orphaning old records or publishing a new epoch early; multiple rapid transitions form the same ordered epoch chain.

Recovery results/effect IDs tagged `E` are stale as soon as that transition commits and are ignored: they cannot mutate either lane, be reclassified under `E+1`, or create another loss. A recovery effect genuinely created under `E+1` has a distinct epoch-bound identity, and its later failure remains retained/actionable even when an `E` loss already exists for the room.

## Crypto Replay Contract

The crypto worker is a second logical actor with a deliberately disjoint ownership domain: it alone owns Olm/Megolm objects and the minimal room encryption/membership projection needed to choose recipients; the coordinator alone owns transport, lane, frame, batch, and loss state. A dedicated single worker thread constructs and accesses all crypto objects; an asyncio mailbox serializes commands and returns frozen `CryptoResult` values tagged with work ID, source epoch, and relevant membership epochs. It submits bounded immutable persistence commands to the shared physical ingestion-store writer rather than opening a second write-capable SQLite connection. Do not use the shared `asyncio.to_thread()` pool, which would move mutable ratchets between threads. Neither actor shares a mutable object, and ingestion never mutates `AsyncClient.rooms` or passes a mutable `MatrixRoom` across the boundary.

Crypto receipt IDs are content-addressed per input, not frame-addressed. For an Olm to-device ciphertext, the ID/digest includes canonical sender, sender key, event type, session/ciphertext content, and algorithm. For a Megolm room event it includes room ID, event ID, sender key, session ID, ciphertext, and origin timestamp. Redelivery in another frame or after restart therefore finds the same receipt rather than reratcheting.

The worker orders a frame's to-device inputs before room decryptions, but persists them in bounded groups. Before acquiring SQLite's write lock it derives content-addressed input IDs, reads receipts, returns matching cached results without ratcheting, and fails closed on an ID/digest collision. For missing inputs it may derive one group's private crypto mutations because no other actor can observe or mutate that object graph; it must then commit or terminally discard the entire worker before touching the next group. A group contains source-ordered to-device inputs or deterministic room/frame-ordered decryptions, never both sides of that ordering boundary.

For each derived group, one short store-writer transaction must:

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

v1 deliberately has no in-place consumer rebind or destructive reset. Rotating `stream_id` would invalidate encrypted-row AAD, and deleting retained frames/batches would violate the fate invariant. If either database is replaced, restore the matching nio/MindRoom pair from one consistent backup or fix forward. A new Matrix device/store cannot bind to an already-bound principal; a genuinely new guarantee horizon requires a separate unbound MindRoom consumer identity/journal. An existing v1 store with a mismatched consumer generation remains fail-closed; product code never inventories and discards its retained evidence to make the mismatch disappear.

A batch that MindRoom cannot admit also remains fail-stopped and retained in nio. The supported remedy is to fix or roll forward the exactly pinned consumer and replay the identical batch—not reset, rebind, or automatic quarantine. This is intentional: a projection bug or unknown schema is not evidence that Matrix history was lost, and converting it into a loss would destroy recoverable data. Startup/version gates prevent knowingly running a consumer that does not support the batch schema.

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
src/nio/store/_ingestion_store_writer.py
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

Tests, fixtures, generated locks, and this document are excluded. Completed work is measured; remaining rows are revised planning ranges, not targets to fill:

| Work | Production net |
|---|---:|
| Tasks 1–4 plus CI hardening (actual) | **+5,578** |
| Task 4.5 durable-core consolidation | +400 to +1,000 |
| Tasks 5–6 reducer/staging/materialization | +1,000 to +1,400 |
| Task 7 crypto worker/receipts/commands | +900 to +1,200 |
| Tasks 8–9 coordinator/network/lifecycle | +500 to +800 |
| Task 10 metrics/live benchmark tooling | +200 to +400 |
| Tasks 11–12 MindRoom admission/stream | +600 to +900 |
| Tasks 12A–12C downstream consumers | +400 to +600 |
| Task 13 fresh-device activation glue | +100 to +200 |
| Task 16 measured old-architecture deletion | **−5,500 to −7,000** |
| **Projected final net** | **about +2,700 to +6,600** |

At each checkpoint, replace estimates for completed work with actual `git diff --stat`/language-aware production counts and reforecast the remaining rows. The deletion range is backed by 5,427 directly identified old production lines before ancillary fields/models/callback code. The present +6,600 high side requires another architecture/size review after Task 4.5 actuals; no Task 5 work starts until that review either lowers/accepts the forecast or cuts scope.

The Task 4 checkpoint is **NO-GO for Task 5 without consolidation**. Tasks 1–4 plus CI hardening are +5,578 production lines, and the original forecast materially understated the durable journal and dialect-proof contracts. The pure Classic/Sliding adapter seams remain sound, and two proposed “simplifications” are rejected: replay validation must authenticate the frozen request's safety invariants rather than reconstruct it from today's possibly changed configuration, and Synapse/Tuwunel Sliding acknowledgement negotiation remains required by verified deployed dialects. The verified defects are narrower: persisted lock-file inode blocks a legitimate restored copy; each loss has two durable nio owners; raw response bytes coexist with a second normalized durable copy; Sliding computes and discards membership evidence; and Task 5 has no durable baseline/gap/effect carrier. Task 4.5 fixes those boundaries and selects one physical SQLite writer before reducer work resumes.

---

## Delivery and Branch Strategy

- Continue nio on `docs/durable-ingestion-rewrite-plan` and draft PR #55, as explicitly selected for implementation; commit/push every reviewed task-sized checkpoint. Implement MindRoom later in a companion branch based on the exact PR 1800 integration head and record that base SHA before Task 11.
- Keep PR #55 and the later MindRoom pull request draft through Task 14. Publish rc1/rc2/rc3 only from the nio feature branch; the MindRoom feature branch pins exact artifacts and never imports an unpublished workspace checkout.
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
- [ ] Round-trip both transport and system loss origins; accept `SystemOrigin` on `ROOM_LIFECYCLE` only with `MEMBERSHIP_CHANGE` and on hydration-derived `STATE`/`ROOM_READINESS` only with `ROOM_HYDRATION`, reject it on every other `EventRecord`, reject a missing membership epoch on a room loss, and reject a system origin whose kind/operation ID is inconsistent with its deterministic identity.
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

**Interfaces:** Implement `IngestionConfig`, `open_ingestion_store(..., source: ClassicSourceConfig | SlidingSourceConfig)`, single-use `StoreBootstrap`, resumable `StoreBootstrap.attach_consumer()`, `IngestionJournal`, `SqliteIngestionJournal.open(...)`, `commit(...)`, `oldest_unacknowledged()`, `acknowledge(...)`, and deterministic encrypted row codecs.

- [ ] Start with failing tests for fresh schema creation, second-writer refusal, wrong account/device/consumer-generation refusal, and independent `schema_version == 1`.
- [ ] Seed a nonempty E2EE/legacy database without a v1 marker, spy on every SQL statement, and prove normal open raises `FreshIngestionRequired` before `MatrixStore.__post_init__`, migration, repair, table creation, or any write. Seed a genuinely new path and prove the v1 marker is the first atomic schema mutation.
- [ ] Make `StoreBootstrap` the only way the v2 `AsyncClient` opens storage. Its store initialization validates the retained lock/marker and opens only E2EE plus v1 ingestion tables; it must not create, migrate, or repair legacy sync/recovery tables. Direct legacy store construction must refuse a file already owned by a v1 bootstrap.
- [ ] Require the v2 lifetime lock before every v1/E2EE open and test two v2 owners cannot coexist. Running an old binary after explicit initialization is an unsupported operator error, not a compatibility mode.
- [ ] Test that opening the journal neither reads nor alters `SyncTokens`, `SyncRecoveryGaps`, `SyncRecoveryAbandonedRooms`, `PendingTimelineEvents`, or `SlidingWindowTokens`.
- [ ] Test unbound/attaching meta refuses client HTTP/batch delivery; `attach_consumer()` validates the canonical baseline tuple/digest and immutable first sequence, installs the binding plus priority bounded-loss plan through resumable hard-bounded chunks, and exact retries are idempotent.
- [ ] Test CAS rollback injection after every SQL statement in a transition: revision, cursor, room heads, epoch lanes, ready/lane records, frames, and batches must all remain at the prior state.
- [ ] Round-trip one active lane plus multiple retiring predecessor epochs for the same room. `NioIngestRoomState.current_membership_epoch` must identify exactly one active lane, and each predecessor's successor/lifecycle barrier must form one gap-free ordered chain.
- [ ] Test that only named affected room state/lanes are read and written; instrument SQL and assert no full room-state or lane scan during a one-room transition.
- [ ] Test AES-GCM round-trip and AAD failure after swapping account, table, primary key, stream, or digest.
- [ ] Round-trip a delayed `LossRecord` from its sole ready/lane owner after its source frame has been compacted and prove origin, membership epoch, reason, boundary, and detail reconstruct exactly.
- [ ] Test FIFO acknowledgement, exact retry using last-ack metadata after payload deletion, stale older acknowledgement, future/out-of-order acknowledgement, wrong digest, and atomic deletion of the acknowledged row.
- [ ] Reject `SqliteQueueDatabase` with `LocalProtocolError` before creating any ingestion table.
- [ ] Hold an exclusive OS/process writer lock for the session lifetime and a persisted `writer_epoch` CAS for every write. Filesystem lock identity remains an in-memory lifetime check rather than persisted database state, so a consistent restored copy can create its own sidecar.
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
- [ ] Prove normalization returns the one canonical successful source body in `SourceResult`, while `SyncFrame` retains only its SHA-256 and normalized immutable values.
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
- [ ] Keep transport-specific membership extraction in this adapter and shared membership-epoch rules in `membership.py`; persist the resulting frozen `MembershipObservation` on every room segment so downstream code never reparses it.

Resolved implementation contract at `4f1b850`:

- The exact persisted `SlidingCursor` contains independent `pos` and to-device
  `since`, a coordinator-seeded connection UUID, its logical connection name, the
  reserved range end and immutable page size, negotiated range-ack mode, and the
  range-complete flag. A reset retains to-device/range configuration, clears `pos`
  and completion, returns ack mode to unknown, and explicitly marks room history
  uncertain.
- Requests always carry deterministic MSC3575 `txn_id` evidence. A new connection
  negotiates `TXN_ECHO` when the server echoes it and `RESPONSE_BOUND` when native
  Synapse omits it. Once echo mode is observed, omission fails closed; stale valid
  echoes may map payload/`pos`/to-device evidence but cannot expand or newly complete
  all-room coverage.
- Reserved range completion overscans one index: completion requires
  `range_end >= reported_count`, which is harmless for inclusive servers and avoids
  archived conduwuit's exclusive-end bug. Configured room extensions are scoped to
  the reserved all-room list, exact wildcard input is normalized to that list, and
  to-device uses an authenticated positive internal limit.
- Missing top-level room membership is derived from the latest exact own-member
  evidence in state plus only the `num_live` timeline suffix. Explicit and derived
  sections must agree. Initial/expanded rooms without exact proof remain unsafe.
- `all_rooms_coverage_complete` proves only that the reserved room-ID range was
  acknowledged. It never proves authoritative room state or crypto readiness.
  Task 8 must keep any initial/expanded Sliding room with incomplete required-state
  proof (including quiet initial Tuwunel responses and archived conduwuit member
  wildcard failure) `PENDING` until authenticated full-state hydration succeeds.

Run red, then green:

```bash
uv run pytest -q tests/ingest/sliding_source_test.py tests/ingest/membership_test.py tests/ingest/source_parity_test.py
uv run pre-commit run --files src/nio/ingest/sliding.py src/nio/ingest/membership.py tests/ingest/sliding_source_test.py tests/ingest/membership_test.py tests/ingest/source_parity_test.py
```

Commit: `feat(ingest): normalize sliding sync with membership proof`

---

## Task 4.5: Consolidate the Durable Core Before Recovery

Task 5 is blocked until this task passes its independent review. The version-1 schema is unreleased, so amend it in place: do not add a migration or bump `schema_version`.

**Files:**

- Modify: `src/nio/ingest/model.py`
- Modify: `src/nio/ingest/config.py`
- Modify: `src/nio/ingest/source.py`
- Modify: `src/nio/ingest/classic.py`
- Modify: `src/nio/ingest/sliding.py`
- Modify: `src/nio/ingest/membership.py`
- Create: `src/nio/ingest/recovery.py` (durable values only; Task 5 adds reducer behavior)
- Modify: `src/nio/ingest/state.py`
- Create: `src/nio/store/_ingestion_store_writer.py`
- Modify: `src/nio/store/sync_journal.py`
- Modify: `src/nio/store/sync_journal_schema.py`
- Modify: `src/nio/store/_sync_journal.py`
- Modify: `src/nio/store/_sync_journal_port.py`
- Modify: `src/nio/store/_sync_journal_preflight.py`
- Modify: `src/nio/store/_sync_journal_rows.py`
- Modify: `src/nio/store/database.py`
- Modify: `tests/ingest/model_test.py`
- Modify: `tests/ingest/classic_source_test.py`
- Modify: `tests/ingest/sliding_source_test.py`
- Modify: `tests/ingest/source_parity_test.py`
- Modify: `tests/ingest/membership_test.py`
- Modify: `tests/ingest/journal_test.py`
- Modify: `tests/ingest/crash_recovery_test.py`
- Modify: `tests/store_test.py`
- Modify: `tests/encryption_test.py`

**Interfaces:** Freeze the following internal values. They are frozen/slotted and validate exact built-in/enumerated types just like the existing v1 values.

```python
@dataclass(frozen=True, slots=True)
class StagedSourceResponse:
    request: NetworkRequest
    response_body: bytes       # canonical successful Matrix JSON
    source_sha256: bytes       # SHA256(response_body)


@dataclass(frozen=True, slots=True)
class MembershipBaseline:
    room_id: str
    source_epoch: int
    membership_epoch: int
    prev_batch: str
    membership_event_id: str


@dataclass(frozen=True, slots=True)
class RecoveryGap:
    gap_id: UUID
    room_id: str
    opening_source_epoch: int
    membership_epoch: int
    origin: RecordOrigin
    membership_event_id: str
    start_token: str
    target_token: str
    cursor_token: str
    seen_cursor_tokens: tuple[str, ...]
    pages_committed: int
    recovered_record_count: int
    in_flight_effect_id: UUID | None


@dataclass(frozen=True, slots=True)
class RecoveryRequest:
    effect_id: UUID
    stream_id: UUID
    transport: TransportKind
    room_id: str
    membership_epoch: int
    gap_id: UUID
    page_ordinal: int
    from_token: str
    to_token: str
    limit: int
    timeout_ms: int


@dataclass(frozen=True, slots=True)
class RoomHydrationRequest:
    effect_id: UUID
    stream_id: UUID
    transport: TransportKind
    room_id: str
    membership_epoch: int
    timeout_ms: int


class MembershipAction(StrEnum):
    JOIN = "join"
    LEAVE = "leave"
    FORGET = "forget"


class MembershipDeliveryState(StrEnum):
    READY = "ready"
    DISPATCHED_UNCONFIRMED = "dispatched_unconfirmed"


@dataclass(frozen=True, slots=True)
class MembershipRequest:
    effect_id: UUID             # also the durable operation_id
    stream_id: UUID
    transport: TransportKind
    room_id: str                # v1 membership commands require a canonical room ID
    membership_epoch: int       # expected current epoch before finalization
    action: MembershipAction
    request_body: bytes         # canonical JSON, including reason/via where applicable
    timeout_ms: int


NetworkEffectRequest = RecoveryRequest | RoomHydrationRequest | MembershipRequest


@dataclass(frozen=True, slots=True)
class PersistedNetworkEffect:
    request: NetworkEffectRequest
    attempt_ordinal: int
    membership_delivery_state: MembershipDeliveryState | None
    prior_delivery_uncertain: bool | None


@dataclass(frozen=True, slots=True)
class MembershipOperationRef:
    effect_id: UUID
    room_id: str
    membership_epoch: int
    attempt_ordinal: int
    request_sha256: bytes


@dataclass(frozen=True, slots=True)
class MembershipOperationStatus:
    ref: MembershipOperationRef
    action: MembershipAction
    delivery_state: MembershipDeliveryState
    prior_delivery_uncertain: bool


class MembershipOperationResolution(StrEnum):
    RETRY = "retry"
    SUPERSEDE = "supersede"


class MembershipOperationResolutionOutcome(StrEnum):
    READY = "ready"
    SUPERSEDED = "superseded"
    ABSENT = "absent"


class NetworkEffectKind(StrEnum):
    RECOVERY = "recovery"
    ROOM_HYDRATION = "room_hydration"
    MEMBERSHIP = "membership"


@dataclass(frozen=True, slots=True)
class NetworkEffectResult:
    effect_id: UUID
    kind: NetworkEffectKind
    stream_id: UUID
    transport: TransportKind
    attempt_ordinal: int | None
    status_code: int | None
    body: bytes
    failure: NetworkFailureKind | None


class LaneRecordSection(StrEnum):
    LOSS = "loss"
    RECOVERED = "recovered"
    HELD = "held"


@dataclass(frozen=True, slots=True)
class LaneRecordKey:
    room_id: str
    membership_epoch: int
    section: LaneRecordSection
    page_ordinal: int
    record_ordinal: int


@dataclass(frozen=True, slots=True)
class LaneRecord:
    key: LaneRecordKey
    record: EventRecord | LossRecord
    source_frame_id: UUID | None
    source_effect_id: UUID | None
    canonical_bytes: int


class StoreWriterOwner(StrEnum):
    BOOTSTRAP = "bootstrap"
    JOURNAL = "journal"
    E2EE = "e2ee"


class IngestionStoreWriter:
    database: SqliteDatabase

    def assert_owner(self) -> None: ...
    def read(self, owner: StoreWriterOwner) -> ContextManager[sqlite3.Connection]: ...
    def transaction(
        self,
        owner: StoreWriterOwner,
        *,
        fence_writer_epoch: bool = True,
    ) -> ContextManager[sqlite3.Connection]: ...
    def close(self) -> None: ...
```

`RecoveryGap` requires non-empty tokens, `seen_cursor_tokens[0] == start_token`, `cursor_token == seen_cursor_tokens[-1]`, nonnegative counters, exact room/membership agreement with its containing lane, and `origin.source_epoch == opening_source_epoch`. The historical opening epoch need not equal the current `SourceState.source_epoch`; the gap survives a primary-source reset. Its ID is stable while a later same-membership discontinuity extends `target_token`:

```text
gap_id = UUIDv5(
    stream_id,
    room_id:opening_source_epoch:membership_epoch:opening_origin_id:baseline_sha256,
)
```

`target_token` is deliberately absent from the `gap_id` inputs. `opening_origin_id` is the immutable canonical identity of the frame/room segment that opened the gap, so reopening from the same baseline in a later frame cannot collide.

`RoomState` gains `membership_baseline: MembershipBaseline | None`. `RoomLane` gains `recovery_gap: RecoveryGap | None`, retains `pending_lifecycle: EventRecord | None`, removes `release_loss_id`, and persists the baseline/gap/lifecycle inside authenticated state/lane envelopes. `JournalTransition.losses` and `IngestionJournal.load_loss()` disappear. `NioIngestLaneRecord` becomes a real codec-covered journal value, not schema-only scaffolding. Ready/lane schema uses union-safe `item_id`/`item_kind`, not a column that calls every loss identity `record_id`.

`RecoveryRequest`, `RoomHydrationRequest`, and `MembershipRequest` are the restart-complete tagged union payloads of `NioIngestNetworkEffect`; the row decodes as `PersistedNetworkEffect`. Recovery/hydration rows require ordinal `0` and no membership-delivery/uncertainty state. A membership row starts at ordinal `0`/`READY`/`prior_delivery_uncertain=False`. `JournalTransition` gains typed network-effect inserts, exact state updates, and deletes; the journal port gains exact load/list operations and authenticated union codecs. The HTTP executor returns `NetworkEffectResult` tagged with exact kind/effect/stream/transport and, for membership, attempt ordinal, never an unbound generic result. Room effects are membership-bound but deliberately not primary-source-generation-bound. A Sliding `M_UNKNOWN_POS` reset fences stale primary-sync results while committed `/messages`, room-hydration, and membership effects remain valid/resumable.

At most one unresolved `MembershipRequest` may exist for a room. The partial unique index is the durable backstop and the coordinator rejects a second command with `MembershipOperationPending` before any second HTTP request can start; it never queues contradictory same-epoch operations behind one another. Immediately before membership HTTP, the owner atomically increments the attempt ordinal and changes `READY` to `DISPATCHED_UNCONFIRMED`; only then may the executor send. A crash while still `READY` is safely schedulable. A crash or transport ambiguity after `DISPATCHED_UNCONFIRMED` is **not** automatically replayed: the exact row remains visible through `uncertain_membership_operations()`, but source ingestion, recovery, hydration, and unrelated rooms continue. The live operation waiter receives `MembershipOperationUncertain`; restart does not turn this room-scoped condition into a terminal session failure. This conservative false-positive window (a crash after the state change but before the first byte) is accepted because these endpoints have no transaction-ID slot.

An exact current-attempt success finalizes the lifecycle and deletes the effect in one transition, even after an operator-authorized retry, because that response proves the requested postcondition. With `prior_delivery_uncertain=False`, a documented definitive rejection from the exact current attempt deletes the effect and changes no local membership, and current-attempt 429 may atomically return it to `READY`; retry timing belongs to the ordinary in-process scheduler and is not part of the correctness state. With `prior_delivery_uncertain=True`, any non-success response remains ambiguous. Timeout, connection loss, 408, 5xx, a mismatched attempt ordinal, or any non-success response after an unobserved possibly-sent attempt leaves it `DISPATCHED_UNCONFIRMED` and cannot fabricate rejection. Caller cancellation detaches only the waiter and never rewinds this durable state.

The operator escape hatch is narrow and fenced. `uncertain_membership_operations()` returns frozen `MembershipOperationStatus` values without exposing request bodies or mutable rows. `resolve_membership_operation(ref, RETRY)` requires an exact effect/room/epoch/attempt/request-digest match and an unchanged current room epoch, then changes only `DISPATCHED_UNCONFIRMED -> READY` and sets `prior_delivery_uncertain=True`; it explicitly accepts the risk of repeating the same semantic POST. Repeating that exact resolution before the next dispatch claim is a no-op; once a claim increments the ordinal, the old ref is stale. `SUPERSEDE` under the same fences deletes the effect without claiming that it succeeded or changing membership state. A repeat after deletion can only return `ABSENT`—there is deliberately no tombstone and therefore no claim that the supplied ref was ever live or that the server did anything. All paths reject a mismatched live row/result and make no HTTP call themselves. This gives an unconfirmed `FORGET` an implementable fix-forward path without weakening automatic replay safety.

Every independent membership-epoch transition also resolves any pending membership effect for that room in the same CAS. An own-join observation may independently prove `JOIN`, and an exact own-leave observation may independently prove `LEAVE`; the transition then finalizes that operation exactly once and deletes its effect. `FORGET` is never inferred from sync. Any conflicting or unrelated epoch transition deletes the old-epoch effect as superseded, completes a live waiter with `MembershipOperationSuperseded`, and relies on the ordered lifecycle record as the durable explanation; a late HTTP result finds no matching effect and is rejected before mutation. This rule applies whether the request was `READY` or `DISPATCHED_UNCONFIRMED`, so an old command can never be replayed against `E+1`. Membership-epoch retirement separately deletes old recovery/hydration effects atomically, and an incomplete successor receives a new deterministic hydration effect.

`RecoveryRequest.effect_id = UUIDv5(gap_id, f"page:{page_ordinal}")`. `RoomHydrationRequest.effect_id = UUIDv5(stream_id, f"hydrate:{room_id}:{membership_epoch}")`. Exact retry therefore reproduces the row, while membership retirement necessarily changes hydration identity and cannot confuse an old result with successor state.

`MembershipRequest.effect_id` is a coordinator-supplied operation UUID persisted before HTTP and reused as the operation identity for its whole lifetime. Join/leave/forget POSTs have no Matrix transaction-ID slot; the delivery state above, rather than blind replay, is the source of truth after restart. `forget` therefore neither depends on a later sync echo nor claims success when the server's outcome is unknowable.

`SyncFrame.source_json` becomes `source_sha256: bytes`, and each `RoomSegment` gains `membership_observation: MembershipObservation`. A frame-producing `SourceResult` retains the one canonical successful response body long enough to construct `StagedSourceResponse`; only that staged response is encrypted in `NioIngestFrame`. `source_sha256` authenticates the canonical Matrix response bytes, while `NioIngestFrame.payload_sha256` authenticates the serialized request/response envelope; they are distinct hashes and both are revalidated. Restart decodes the frozen request/raw response, reruns the matching adapter, and requires the same frame ID/digest.

`IngestionStoreWriter` creates one Peewee `SqliteDatabase(thread_safe=False, autoconnect=False)`, connects it once on the opening thread, and owns its exact underlying `sqlite3.Connection`. The journal borrows that raw connection and the v2 `SqliteStore` view borrows the same Peewee object without connecting or closing independently. It records opening PID/thread, lifetime lock identity, database inode, and writer epoch, and validates them before SQL. Its private capability-based `read(owner)` and `transaction(owner, fence_writer_epoch=True)` context managers yield the one connection; transactions use Peewee `atomic("IMMEDIATE")` so Peewee and raw journal transaction depth cannot diverge. Journal/bootstrap and actual synchronous E2EE calls use this seam in Task 4.5. Task 7 adds typed receipt lookup plus the prepared-transaction handshake that returns persistence to this opening thread. No generic callback, arbitrary SQL command, HTTP, crypto computation, second writer handle, or actor mailbox is introduced in Task 4.5.

Direct `MatrixStore.database.connect()`/`close()` is invalid for a borrowed v2 store; bootstrap/session close revokes the store view, closes the sole connection, and releases `flock` last. v2 rejects `DefaultStore`, `SqliteMemoryStore`, and third-party store classes. This is intentional greenfield scope: MindRoom currently defaults to `DefaultStore`, so Task 12C switches the new ingestion path to `SqliteStore`; the old path remains untouched until deletion.

- [ ] Write a failing online-backup test: initialize/close a journal, copy it with SQLite's backup API into a distinct path without copying the sidecar, and open the copy under its new sidecar. Keep counterexamples proving a concurrent owner of the same path is refused, replacing a live lock path invalidates the old owner before mutation, a forked/stale handle is rejected, and database-file replacement is rejected.
- [ ] Remove `lock_device`/`lock_inode` from schema, creation, open/CAS queries, E2EE bootstrap checks, and strict topology fixtures. Keep lifetime `flock`, in-memory lock-path/file-descriptor identity, database-file identity, PID/lease fencing, and persisted `writer_epoch` CAS. Commit this independently as `fix(ingest): make journal ownership restore safe`.
- [ ] Write failing fresh/open tests for both transports. Creation accepts an exact Classic or Sliding source config, stores immutable `NioIngestMeta.transport_kind`, and inserts exactly one matching cold source state. Sliding freezes `all_rooms_page_size` into its cursor before attach, including value `1` and default `100`. Reopen with the same source type succeeds despite non-continuity config drift because the durable cursor retains that size; cross-kind reopen and every direct transition attempting to replace the transport fail before write.
- [ ] Write failing attachment tests that bind 10,000 canonically sorted baseline rooms using bounded chunks and durable `UNBOUND -> ATTACHING -> ATTACHED` state/next ordinal. Kill between every chunk, retry with the exact tuple, and prove no duplicate state/lane/loss, no batch delivery/HTTP before `ATTACHED`, immutable `consumer_first_sequence` after later batches/acks, and no O(all rooms) transaction.
- [ ] Write failing schema/codec tests that round-trip a `RoomState` baseline, a lane gap plus pending lifecycle, and LOSS/RECOVERED/HELD lane records after restart. Tamper each encrypted payload/digest and require fail-stop before revision mutation.
- [ ] Remove unused `SystemOriginKind.CONSUMER_RESET`/`SOURCE_REBIND`, add `ROOM_HYDRATION`, and enforce the exact `ROOM_LIFECYCLE`/`MEMBERSHIP_CHANGE` plus hydration-derived `STATE`/`ROOM_READINESS` with `ROOM_HYDRATION`. Matrix state events retain their Matrix-event-ID identity; system facts without an event ID use kind/room/membership/system-kind/operation/content digest and reject same-operation/different-content collisions. Keep every other event source-bound and update canonical serialization/golden tests.
- [ ] Write failing ownership tests proving a fresh attach loss, terminal lane loss, and batched loss each have exactly one nio durable owner. Mutation-kill retaining `NioIngestLoss`, `JournalTransition.losses`, `load_loss()`, or `release_loss_id`.
- [ ] Delete `NioIngestLoss` and `NioIngestConsumerResetRoom`; remove all loss-ledger codecs/queries. Materialization moves one loss payload ready/lane → unacknowledged batch transactionally. Acknowledgement stores only exact last-ack metadata and deletes the payload, so MindRoom is then the sole audit owner. Commit carriers and single ownership as `refactor(ingest): make losses ordinary lane records`.
- [ ] Write failing effect tests that round-trip/list all three request variants and atomically insert/update/delete them with matching room/gap/lifecycle transitions. Recovery/hydration restart byte-identically reschedules their request; membership restart schedules only `READY`, while `DISPATCHED_UNCONFIRMED` remains durable and appears in `uncertain_membership_operations()` without failing the session. Swap two `NetworkEffectResult` values and reject kind/action/effect/gap/room/membership/stream/transport/page/attempt mismatches. Resetting only the primary Sliding source generation preserves every effect kind. Retiring membership deletes old recovery/hydration effects, a late result cannot revive them, and an incomplete successor gets a distinct hydration effect. Membership success/rejection deletes only its exact operation row.
- [ ] Mutation-kill the membership-operation interlock: two concurrent commands for one room must produce one durable row and at most one HTTP attempt; a second command raises `MembershipOperationPending` before network. Prove `READY -> DISPATCHED_UNCONFIRMED` commits before send, a crash before that transition is schedulable, a crash after it is never auto-replayed, and ambiguous/mismatched outcomes retain the row. With no prior uncertain delivery, current-attempt success/rejection is exact and 429 can return to `READY`; after explicit uncertain retry, success may finalize but every non-success remains uncertain.
- [ ] Cross the expected epoch while a membership effect is both `READY` and `DISPATCHED_UNCONFIRMED`. An exact own-join/own-leave observation finalizes the matching action; a conflicting transition supersedes it; `FORGET` is never sync-proven. In every case delete the effect in the epoch transition, reject its late result before mutation, and prove restart neither recreates nor applies it to the successor.
- [ ] Round-trip the frozen membership operation ref/status, including `prior_delivery_uncertain`, and test explicit resolution. First `RETRY` and `SUPERSEDE` require exact effect/room/epoch/attempt/request digest plus `DISPATCHED_UNCONFIRMED`; stale refs, current-epoch drift, an unrelated `READY`, and a swapped room fail before write. Retry only returns the same semantic effect to `READY` and sets prior uncertainty; its exact pre-claim repeat is a no-op. Supersede only deletes it; a repeat reports `ABSENT` without claiming the ref was valid or the server succeeded. Kill before/after each transition and assert the resulting state/status, without inventing a durable-resolution-receipt guarantee.
- [ ] Write failing Classic/Sliding tests asserting every room segment carries byte-identical transport-neutral membership evidence for equivalent fixtures. Add same-ID live join, linked join, invite/knock/leave/ban, account-data-only, initial/expanded-incomplete, and malformed own-member cases. Classic receives and validates `own_user_id`; neither reducer nor restart code reparses member events.
- [ ] Write failing staging tests showing the canonical response body occurs once in a staged row, `SyncFrame` contains only its digest, and restart re-normalization produces the same digest/frame/observations. Mutations that persist both raw and normalized frame bytes, accept a changed raw digest, or trust a previously parsed frame must fail.
- [ ] Preserve durable-request replay across configuration changes: adapters authenticate the frozen request's stream/effect/cursor and mandatory safety overlays, not equality with the process's current config. Classic cold requests always require `full_state=true`; continuation filter/timeout and Sliding caller-list changes cannot invalidate a legitimately frozen older request. Keep mutation tests for removal/change of cold full-state, reserved list, to-device, E2EE, cursor, connection, stream, and request identity.
- [ ] Add one-writer instrumentation proving journal creation/attach/commit/ack and real SQLite-backed E2EE account/session/device-trust save/load all execute through the identical `sqlite3.Connection` on the opening thread. Assert `thread_safe=False`, `autoconnect=False`, no second writable connection, wrong owner/thread/PID/stale path/inode/post-close rejection before SQL, epoch fencing on every E2EE write, atomic rollback, external lock timeout/fail-stop, and retryable close order. Direct borrowed-store connect/close and unsupported store classes fail explicitly. Commit this seam as `refactor(ingest): serialize durable writes through one owner`.
- [ ] Run the full Task 1–4 ingestion suite, strict schema/topology checks, store compatibility tests, full repository suite, and all changed-file hooks. Record actual production addition/deletion counts, update this forecast, and obtain independent spec/quality approval before Task 5.

Run red/green slices and the final gate:

```bash
uv run pytest -q tests/ingest/journal_test.py tests/ingest/crash_recovery_test.py
uv run pytest -q tests/ingest/classic_source_test.py tests/ingest/sliding_source_test.py tests/ingest/membership_test.py tests/ingest/source_parity_test.py
uv run pytest -q tests/ingest
uv run pytest -q tests/store_test.py tests/encryption_test.py
uv run pytest -q
uv run pre-commit run --all-files
```

Gate: exactly one canonical staged response, one nio-internal loss owner, one literal v2 SQLite connection shared by journal and SQLite E2EE, immutable store transport, bounded resumable attachment, and restart-complete membership baseline/gap/effect state. Recompute the forecast from actual Task 4.5 code; because the current high side exceeds +6,000, obtain another architecture/size approval before Task 5 even if every correctness test is green.

---

## Task 5: Port Recovery Into a Pure Per-Room Reducer

**Files:**

- Modify: `src/nio/ingest/recovery.py`
- Create: `src/nio/ingest/reducer.py`
- Modify: `src/nio/ingest/config.py`
- Modify: `src/nio/ingest/state.py`
- Create: `tests/ingest/recovery_reducer_test.py`

**Interfaces:** Implement these frozen/slotted pure-reducer values and functions. Task 4.5 already owns their persistence codecs; Task 5 adds no SQL.

```python
@dataclass(frozen=True, slots=True)
class ReducerPolicy:
    own_user_id: str
    max_held_events_per_room: int
    max_held_bytes_per_room: int
    max_recovery_events_per_room: int
    max_recovery_pages_per_room: int
    recovery_page_size: int
    recovery_timeout_ms: int


class RecoveryResultKind(StrEnum):
    PAGE = "page"
    RETRYABLE_ERROR = "retryable_error"
    TERMINAL_ERROR = "terminal_error"


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    request: RecoveryRequest
    kind: RecoveryResultKind
    start_token: str | None
    end_token: str | None
    chunk_json: tuple[bytes, ...]
    status_code: int | None
    network_failure: NetworkFailureKind | None
    retry_after_ms: int | None
    response_body: bytes
    detail: str | None


@dataclass(frozen=True, slots=True)
class ReadyRecordCandidate:
    record: EventRecord | LossRecord
    source_frame_id: UUID | None
    canonical_bytes: int


@dataclass(frozen=True, slots=True)
class Reduction:
    source_state: SourceState | None = None
    room_states: tuple[RoomState, ...] = ()
    room_lanes: tuple[RoomLane, ...] = ()
    ready_records: tuple[ReadyRecordCandidate, ...] = ()
    lane_record_inserts: tuple[LaneRecord, ...] = ()
    lane_record_deletes: tuple[LaneRecordKey, ...] = ()
    network_effect_inserts: tuple[PersistedNetworkEffect, ...] = ()
    network_effect_updates: tuple[PersistedNetworkEffect, ...] = ()
    network_effect_deletes: tuple[UUID, ...] = ()
    consumed_frame_ids: tuple[UUID, ...] = ()


def recovery_gap_id(
    stream_id: UUID,
    baseline: MembershipBaseline,
    origin: RecordOrigin,
) -> UUID: ...


def recovery_effect_id(gap: RecoveryGap, page_ordinal: int) -> UUID: ...


def plan_recovery_request(
    policy: ReducerPolicy,
    stream_id: UUID,
    transport: TransportKind,
    room: RoomAggregate,
) -> RecoveryRequest | None: ...


def normalize_recovery_result(
    request: RecoveryRequest,
    result: NetworkEffectResult,
) -> RecoveryResult: ...


def plan_room_hydration_request(
    policy: ReducerPolicy,
    stream_id: UUID,
    transport: TransportKind,
    room: RoomAggregate,
) -> RoomHydrationRequest | None: ...


def reduce_room_hydration_result(
    policy: ReducerPolicy,
    room: RoomAggregate,
    request: RoomHydrationRequest,
    result: NetworkEffectResult,
) -> Reduction: ...


def reduce_source_result(
    policy: ReducerPolicy,
    source: SourceState,
    rooms: Mapping[str, RoomAggregate],
    result: SourceResult,
) -> Reduction: ...


def reduce_recovery_result(
    policy: ReducerPolicy,
    room: RoomAggregate,
    result: RecoveryResult,
) -> Reduction: ...
```

`RecoveryResult` embeds the complete Task-4.5 request, so effect, stream, immutable transport, room, membership, gap, and page identity are inseparable. Hydration reduction likewise validates the complete durable request/result tag before interpreting a response. Primary-source generation is intentionally absent from both: committed room effects survive a same-transport Sliding `M_UNKNOWN_POS` reset. A valid full-state response produces the authoritative snapshot, hydration-origin `STATE` records, and `ROOM_READINESS(READY)` in one reduction; confirmed 403/not-joined produces `ROOM_READINESS(UNAVAILABLE)`; retryable transport/408/429/5xx retains the effect unchanged; malformed or contradictory success fails stopped with the effect retained. `Reduction` contains proposed immutable values only; Task 6 allocates revisions/ready order and commits them. Identical typed inputs produce identical IDs/bytes.

- [ ] Port chronology, duplicate suppression, held-live ordering, own-join boundaries, bounded exhaustion, stalled cursor, cursor cycle, cap, fetch rejection, and retryable failure cases from `sync_recovery_test.py` and `per_room_recovery_contract_test.py`.
- [ ] Open a same-epoch proven discontinuity with `from=old MembershipBaseline.prev_batch`, `to=current RoomSegment.timeline_prev_batch`, and Matrix `/messages?dir=f`. Preserve page/chunk chronology; never reverse it. A later discontinuity may extend `target_token` without changing `gap_id`.
- [ ] Validate page semantics exactly: `end == target` succeeds; `end is None` on a membership-bounded walk succeeds by exhaustion; empty chunk plus a new non-null end continues; `end == from` or any previously seen cursor emits `UNVERIFIABLE`; retryable transport/408/429/5xx retains the exact gap/effect; page/event budget emits `EVENT_LIMIT`; terminal HTTP rejection emits `FETCH_FAILED`.
- [ ] Enforce `max_recovery_events_per_room`, `max_recovery_pages_per_room`, `recovery_page_size`, `recovery_timeout_ms`, held-event count, and held canonical bytes from `ReducerPolicy`. Reducers never read configuration, globals, stores, or clocks.
- [ ] Mutation-test every terminal branch: changing a loss reason, dropping the loss, releasing held records before the loss, or treating a retryable response as terminal must make a focused test fail.
- [ ] Test two rooms where one repeatedly returns 429 and the other recovers and batches normally.
- [ ] Test restart-rescheduled hydration for an observed-but-incomplete room and an attached baseline room absent from source coverage. A successful full-state response emits one READY fact; confirmed unavailable emits one UNAVAILABLE fact; retryable and malformed results cannot silently mark readiness. Membership retirement deletes the old effect, and a late result cannot update the successor epoch.
- [ ] Test stale recovery results after a membership transition are ignored without altering the successor lane. A same-transport Sliding `M_UNKNOWN_POS` reset fences primary sync results but preserves and reschedules the independently gap/membership-bound recovery effect; it never rekeys committed recovery pages. Transport is immutable and no Classic↔Sliding rebind exists.
- [ ] Test an unresolved real gap crossing membership emits `BASELINE_LOST` in the same reduction that retires lane `E`, creates active lane `E+1`, and installs the pending lifecycle barrier.
- [ ] State-machine test the reducer half of the epoch race exactly: start recovery effect `R(E)`; commit rejoin with retiring `loss(E)` and a pending lifecycle barrier to active `E+1`; deliver stale `R(E)` and prove it is ignored without a duplicate or epoch reclassification; start/fail `R(E+1)` and prove its distinct current-epoch loss is retained behind the predecessor barrier rather than suppressed by `loss(E)`. Task 6 adds bounded materialization plus journal/ack/restart persistence around this sequence.
- [ ] If one transition proves multiple terminal facts, emit one `LossRecord` per distinct reason in `LossReason` declaration order as LOSS lane rows before held records. Never collapse causes behind a precedence rank or suppress a current-epoch loss because an older epoch already emitted one.
- [ ] Test state/membership records propose a replacement frozen private `RoomSnapshot`; it is not externally visible from the pure reducer. Task 8 replaces the affected cache entry only after Task 6's CAS succeeds.
- [ ] Validate `EventRecord` origin: `ROOM_LIFECYCLE` may use `SystemOrigin` only with `MEMBERSHIP_CHANGE`, and hydration-derived `STATE`/`ROOM_READINESS` only with `ROOM_HYDRATION`; sync-observed facts keep their triggering `RecordOrigin`, while confirmed local effects use their deterministic effect ID.
- [ ] Test no reduction produces `UNKNOWN`; missing cause construction raises `ReducerInvariantError`.
- [ ] Keep reducer tests synchronous and free of `AsyncClient`, callbacks, tasks, SQLite, locks, and clocks.
- [ ] Record Task 5 production additions/deletions, replace its estimate with actuals, and reforecast before Task 6. A >50% task overrun or projected high side above the approved gate pauses execution for review.

Run red, then green:

```bash
uv run pytest -q tests/ingest/recovery_reducer_test.py
uv run pre-commit run --files src/nio/ingest/recovery.py src/nio/ingest/reducer.py src/nio/ingest/config.py src/nio/ingest/state.py tests/ingest/recovery_reducer_test.py
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
- [ ] Assert a frame is never compacted before every derived record is durable in a batch, generic ready row, or epoch-keyed recovery lane. Loss is a payload in those owners, not a separate location.
- [ ] Test record-count, byte-count, staged-frame, and unacknowledged-batch bounds pause network planning rather than discard data.
- [ ] Test a terminal recovery atomically inserts one or more LOSS lane records at the release head, then splits the lane into hard-bounded FIFO batches with every loss before every held event.
- [ ] Retire an epoch with a held lane larger than one batch while new successor-epoch records arrive. Assert bounded output is `loss(E)`, all retained `E` records, `ROOM_LIFECYCLE(E+1)`, then `E+1` records; the successor work is durable before the barrier clears but never materializes early.
- [ ] Commit `E -> E+1 -> E+2` before `E` finishes draining and restart between every bounded chunk. Assert the epoch-keyed lane chain reconstructs exactly and materializes both lifecycle barriers in order without losing, duplicating, or prematurely exposing either successor's records.
- [ ] Test successful recovery splits recovered history into bounded chronological batches and emits held live events only after the final recovered chunk.
- [ ] Reduce a recovery page containing many events with exactly one journal transition and bulk row insertion. Instrument transaction calls and mutation-kill an implementation that commits once per recovered event.
- [ ] Reduce one large room-hydration state response with exactly one journal transition and grouped state/readiness insertion. Mutation-kill a per-state-event transaction loop.
- [ ] Reduce one or more eligible staged raw frames in source order with one journal transition, grouping inserts/deletes by affected table and room rather than issuing a persistence operation per record. An idle transition may contain one frame; a ready backlog may contain several. Mutation-test both the `commits <= eligible frames` bound and grouped-SQL path.
- [ ] Materialize a 10,000-event/32-MiB room lane and prove every nio and MindRoom-facing batch stays at or below both hard ceilings, unrelated rooms can interleave, and no database write contains the whole lane.
- [ ] Test a room event above `max_record_bytes` becomes one bounded `OVERSIZED_EVENT` loss with digest/boundary evidence; an oversized non-room record retains its frame and fail-stops.
- [ ] Test unrelated room segments can enter earlier batches while one lane waits for recovery.
- [ ] Let ready records accumulate across multiple durable frames and room lanes; under backlog, assert one batch greedily consumes oldest heads until a ceiling is reached or the next head would not fit, without violating per-room order. With only one ready unit, assert publication is immediate and does not wait for a timer.
- [ ] Randomize database insertion/query order and restart before materialization; the persisted ready/lane-head ordering must produce byte-identical record order, digest, and batch ID, including the same batch boundary when the next head does not fit.
- [ ] Feed more than `max_staged_frames` frames while one room remains gapped; assert its records move into the durable held lane, frames retire, and unrelated rooms continue until consumer-level batch backpressure rather than gap-level backpressure.
- [ ] Stage two Sliding frames, then receive `M_UNKNOWN_POS`. Assert source HTTP pauses, both old-epoch frames reduce in order under their captured request cursors, crash/restart at each boundary repeats no record, and only then may the reset advance source epoch/connection. Mutation-kill discarding an old frame or letting it regress the reset cursor.
- [ ] Count journal transactions for frozen workloads and assert growth is `O(raw frames + recovery pages + materialized batches + acknowledgements)`, not `O(records)`. A mutation that replaces any bulk transition with a per-record commit loop must fail this test.
- [ ] Persist the full epoch race through the journal: `R(E)` in flight; durable `loss(E) -> ROOM_LIFECYCLE(E+1)`; stale `R(E)`; failed acknowledgement; process restart, identical batch redelivery, and successful acknowledgement; then a genuine failing `R(E+1)`. Prove the `loss(E)` payload moves lane → batch exactly once (despite idempotent redelivery), is never relabelled as `E+1`, and the distinct `E+1` loss remains retained until acknowledged.
- [ ] Recompute frame and batch hashes on every database read before decoding.
- [ ] Corrupt a room-keyed held/pending-decryption row and require a truthful room `CORRUPT_STORED_RECORD` ready/lane payload; corrupt a whole staged response and require fail-stop retention with no fabricated room loss and no later HTTP request.
- [ ] Benchmark hard-ceiling batch materialization and a 10,000-record lane drain; record transaction duration, writer wait, peak memory, and unrelated-room latency before adding crypto contention.
- [ ] Record Task 6 production additions/deletions, replace its estimate with actuals, and reforecast/obtain any required size approval before Task 7.

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

**Interfaces:** Implement serialized `CryptoWorker`, immutable `CryptoStoreBootstrapSnapshot`, `CryptoWorkUnit`, `CryptoReceiptLookup`, `CryptoPreparedResult`, `CryptoResult`, frozen identity/device/trust snapshots, typed crypto commands, immutable `CryptoStoreTransaction`, and a narrow async `CryptoPersistencePort` with `load_receipts(inputs) -> CryptoReceiptLookup` plus `commit(transaction) -> CryptoCommitOutcome`. Both methods invoke the shared `IngestionStoreWriter` only from its opening thread; content-addressed receipt lookup happens before worker mutation, and receipt write/revalidation commits atomically with E2EE mutations.

`CryptoWorker.process()` is an async three-phase handshake: the opening/event-loop thread loads content-addressed receipts for the immutable inputs and returns cached outputs without posting hits to the worker; the dedicated worker thread mutates its private crypto graph only for misses and returns a frozen prepared result plus `CryptoStoreTransaction`; the awaiting coroutine resumes on the opening thread and commits through `IngestionStoreWriter`, revalidating the expected hit/miss receipt set inside that transaction. Only after a typed commit-success message may the worker expose new results or accept the next unit. Lookup/commit/cancellation failure discards the prepared worker graph and fail-stops. This is the narrow owner-thread bridge required by Task 7; it is not a generic callback/SQL proxy.

- [ ] Start with an Olm to-device replay test: process once, crash after crypto commit but before coordinator commit, process the same work again, and assert the cached clear result is returned without advancing the ratchet twice.
- [ ] Prove receipt lookup executes on the opening thread before any worker mutation, cache hits never reach the worker, and commit revalidates expected hits/misses atomically. Inject a receipt between lookup and commit and require the prepared crypto graph to be discarded rather than double-ratcheted or overwrite the stored result.
- [ ] Add equivalent Megolm room-event, room-key-before-room-event, device-list invalidation, one-time/fallback-key maintenance, duplicate message-index, and mixed to-device/timeline tests.
- [ ] Kill after crypto state/effect commit but before key upload/query/claim/to-device HTTP, then reopen and prove the identical durable effect is rescheduled with the same transaction identity.
- [ ] Enforce logical table ownership: only crypto-authorized store commands may mutate `NioIngestCryptoEffect`; the coordinator can execute its frozen request but must post the result back without requesting deletion/update of that row. Every physical write traverses the shared writer.
- [ ] Store a failed Megolm event, deliver its room key in a later frame, and assert one `DECRYPTION_UPDATE` references the original record; repeat with crashes before/after key receipt, restart, duplicate key delivery, and a changed membership epoch.
- [ ] Inject a same-work-ID/different-digest collision and require a terminal integrity error.
- [ ] Redeliver identical Olm ciphertext in a different frame and after restart/rescheduling; assert the same per-input receipt returns the same result without reratcheting. No source-rebind path exists.
- [ ] Prove only non-idempotent Olm receipts survive frame consumption; transient Megolm receipts are removed and replay remains safe under its event-ID/message-index rule.
- [ ] Inject transaction failure after in-memory mutation and assert the worker rejects all further work until closed/reopened.
- [ ] Route immutable outbound-encryption commands through the same worker; prove serial execution with inbound to-device work without a lock exposed to callers.
- [ ] Assert all crypto computation/object mutation executes on the dedicated crypto thread, while every resulting persistence transaction executes on the one store-writer owner. An intentionally slow crypto computation must not stall an event-loop heartbeat or batch acknowledgements for already-produced work.
- [ ] Block the owner thread before/after prepared-result receipt, cancel the awaiting caller, race close, and crash before/after commit. Prove exactly one typed transaction reaches the writer, cancellation cannot expose an uncommitted result or let the worker accept another unit, and no worker-thread SQLite/Peewee call occurs.
- [ ] Store encrypted immutable result bytes in the receipt; a receipt containing only a high-water mark is insufficient to replay the output.
- [ ] Confirm no user callback executes in the worker and no network request is awaited inside its transaction.
- [ ] Keep crypto mutations outside SQLite write-lock hold time in the private worker, then commit one bounded input group of mutated E2EE rows, receipts, and effects in a short transaction. On commit failure discard the in-memory object graph and fail stop before another input.
- [ ] Feed many batchable crypto inputs and assert commit count follows bounded crypto groups, with grouped row writes, rather than input-record count; mutation-kill a one-transaction-per-input implementation. Ordering boundaries may split groups but may not silently force every homogeneous input into its own transaction.
- [ ] With barriers before crypto submission and inside the shared writer transaction, prove expensive crypto work does not hold the writer; once submitted, bounded crypto and coordinator commands serialize fairly. An external SQLite lock must complete or raise typed timeout/fail-stop within the configured bound—never wait indefinitely.
- [ ] Inventory every `BaseClient`/`AsyncClient` access to mutable `olm`, account, device/session stores, group sessions, verification state, key import/export, and trust mutation. Add a worker command or immutable snapshot for each v2 use; no mutable crypto object may cross the port.
- [ ] When v2 starts, read and serialize one immutable `CryptoStoreBootstrapSnapshot` through the opening-thread writer, pass only those bytes/values to the dedicated worker, and construct the private crypto objects there. No worker-thread database call or mutable crypto object crosses the boundary. Detach the main-thread object graph and test that direct `client.olm` access then fails rather than creating a second owner.
- [ ] Record Task 7 production additions/deletions, replace its estimate, rerun journal/crypto contention benchmarks, and reforecast/obtain any required size approval before Task 8.

Run red, then green:

```bash
uv run pytest -q tests/ingest/crypto_worker_test.py tests/encryption_test.py -k 'olm or megolm or replay or receipt'
uv run pre-commit run --files src/nio/ingest/crypto.py src/nio/ingest/ports.py src/nio/store/crypto_receipts.py src/nio/crypto/olm_machine.py src/nio/client/base_client.py src/nio/client/async_client.py src/nio/store/database.py src/nio/store/models.py tests/ingest/crypto_worker_test.py tests/encryption_test.py tests/async_client_test.py tests/backfill_test.py
```

Commit: `feat(ingest): make crypto work replay safe`

---

## Task 8: Add the Single-Owner Coordinator and Consumer API

**Files:**

- Create: `src/nio/ingest/coordinator.py`
- Create: `src/nio/ingest/network.py`
- Create: `tests/ingest/coordinator_test.py`
- Create: `tests/ingest/consumer_protocol_test.py`
- Modify: `src/nio/ingest/__init__.py`
- Modify: `src/nio/__init__.py`
- Modify: `src/nio/client/async_client.py`
- Modify: `src/nio/client/__init__.py`

**Interfaces:** Implement `open_ingestion()`, `IngestionSession.start()/wait_ready()/wait_baseline_hydrated()/next_batch()/acknowledge()/room_snapshot()/room_hydration_status()/wait_room_ready()/wait_closed()/close()`, `AsyncClientNetworkPort`, immutable command types, and `TaskGroup` lifecycle. Whole-directory and public metrics APIs are deliberately absent.

- [ ] Test CAS-before-publish and CAS-before-effect ordering with recording fakes.
- [ ] Test primary-source results are fenced by source epoch; recovery/hydration results are paired to their exact effect plus stream/transport/gap/membership identity; crypto and membership results are fenced by their typed effect/epoch identity. A Sliding primary-source reset must not strand an independently valid recovery effect.
- [ ] Hold a `RESET_REQUIRED` primary result while older staged frames drain; schedule no source HTTP until the reset commits. Crash before/after each drain/reset boundary and prove restart derives the same order without an in-memory-only correctness dependency.
- [ ] Execute durable `RecoveryRequest` values as `/messages?dir=f` and `RoomHydrationRequest` values against the authenticated room-state endpoint; return exact-tagged `NetworkEffectResult`, swap results between rooms/effect kinds, and fail closed before reducer or journal mutation.
- [ ] Enforce table ownership: coordinator transitions are the only writers of `NioIngestNetworkEffect`, and no coordinator transaction writes a crypto-owned table.
- [ ] Test `next_batch()` cancellation leaves the oldest batch unchanged.
- [ ] Test acknowledgement cancellation at every await point is safely retryable.
- [ ] Test backpressure pauses request planning and resumes only after matching ack.
- [ ] Test graceful close cancels pure HTTP work, drains commands already posted, persists all committed transitions, closes owned resources, and never waits for application code.
- [ ] Enforce close order: stop scheduling; cancel/drain HTTP results; persist posted owner commands; stop the crypto compute actor; close the shared store writer/journal and `MatrixStore` view; close HTTP; release the retained store lock last. Assert no connection/file descriptor survives lock release.
- [ ] Test forced close followed by reopen reconstructs only journal state, not old mutable objects.
- [ ] Test a changed consumer/journal generation fails before HTTP, batch delivery, or ack. That principal requires a matching database-pair restore or fix-forward; only a separate unbound consumer identity may start another stream. No reset/rebind API exists.
- [ ] Test terminal auth/protocol/store/crypto failures drain already-durable batches and then raise through `next_batch()`/`wait_closed()`; transient failures retry without invoking a response callback.
- [ ] Test `wait_ready()` completes only after the first source frame is durably staged and reduced, including a genuinely empty frame, and raises the terminal session failure instead of hanging.
- [ ] Test `wait_baseline_hydrated()` independently: Classic full-state, paged Sliding all-room coverage, fallback room-state requests, retryable pending state, and terminal unavailable state. Sliding `all_rooms_coverage_complete` is only room-ID coverage: an initial/expanded room with incomplete required-state proof remains `PENDING` and must complete authenticated full-state hydration before becoming `READY`, including quiet initial Tuwunel responses and archived conduwuit wildcard failure. No outbound crypto request for a pending/unavailable room reaches the worker.
- [ ] Instrument a one-room commit in 10,000 persisted room states and assert only that key is loaded, allocated, and replaced in nio's private affected-room cache; no commit scans/copies all rooms, and no whole-directory API exists.
- [ ] Add a low-level raw transport method that performs authenticated HTTP but never calls `_receive_sync_family()`, `_handle_sync()`, a response callback, or a room mutator; `AsyncClientNetworkPort` is the sole ingestion caller.
- [ ] Add an architecture test that rejects `asyncio.Lock`, `ContextVar`, callback invocation, or `MatrixRoom` in `src/nio/ingest/`.
- [ ] Keep the entrypoint opt-in and test-only; do not switch `sync_forever()` or MindRoom yet.
- [ ] Record actual production additions/deletions and revise the project line forecast before Task 9.

Run red, then green:

```bash
uv run pytest -q tests/ingest/coordinator_test.py tests/ingest/consumer_protocol_test.py tests/ingest/crash_recovery_test.py
uv run pre-commit run --files src/nio/ingest/coordinator.py src/nio/ingest/network.py src/nio/ingest/__init__.py src/nio/__init__.py src/nio/client/async_client.py src/nio/client/__init__.py tests/ingest/coordinator_test.py tests/ingest/consumer_protocol_test.py tests/ingest/crash_recovery_test.py
```

Commit: `feat(ingest): expose single-owner batch stream`

---

## Task 9: Route Membership and Lifecycle Commands Through the Owner

**Files:**

- Modify: `src/nio/ingest/coordinator.py`
- Modify: `src/nio/ingest/config.py`
- Modify: `src/nio/ingest/errors.py`
- Modify: `src/nio/ingest/membership.py`
- Modify: `src/nio/ingest/model.py`
- Modify: `src/nio/ingest/ports.py`
- Modify: `src/nio/ingest/__init__.py`
- Modify: `src/nio/store/sync_journal.py`
- Modify: `src/nio/client/async_client.py`
- Modify: `tests/ingest/membership_test.py`
- Modify: `tests/ingest/coordinator_test.py`
- Modify: `tests/ingest/source_parity_test.py`
- Modify: `tests/async_client_test.py`

**Interfaces:** Add `IngestionSession.join_room()`, `leave_room()`, `forget_room()`, `uncertain_membership_operations()`, `resolve_membership_operation(ref, resolution)`, lifecycle commands/results, and crypto-worker-backed outbound encryption. Transport choice remains immutable for the stream. The uncertainty APIs are operation-scoped; they never stop the ingestion session.

- [ ] Persist a frozen `MembershipRequest` before join/leave/forget HTTP. Durably claim one attempt (`READY -> DISPATCHED_UNCONFIRMED`) before sending, and return exact-tagged `NetworkEffectResult`. Restart schedules only a never-dispatched/known-rejected `READY` operation; it never blindly replays an unconfirmed attempt. Success, rejection, supersession, and final uncertainty follow the Task-4.5 classification without relying on an in-memory waiter or later sync echo. v1 accepts canonical room IDs, not unresolved aliases.
- [ ] Enforce at most one unresolved membership operation per room in both the journal and coordinator. Concurrent or restarted competing commands fail with `MembershipOperationPending` before a second HTTP request; no FIFO of contradictory membership commands exists.
- [ ] Expose frozen, request-body-free status for `DISPATCHED_UNCONFIRMED` operations. Test operator-authorized `RETRY` and `SUPERSEDE` with exact ref/digest/epoch/attempt fencing, crash idempotency, no implicit success, and continued sync/recovery for that and unrelated rooms while an operation remains uncertain.
- [ ] Route local join/rejoin through the coordinator owner and test that successful HTTP finalization establishes the next membership epoch; incoming own-join observations use the same reducer transition rather than an `AsyncClient` side path.
- [ ] Test successful leave/forget crosses the epoch and records `BASELINE_LOST` when a real gap exists.
- [ ] With a real gap and `R(E)` in flight, test successful join/rejoin commits `LossRecord(membership_epoch=E)` before `ROOM_LIFECYCLE(E+1)` and makes the late result stale.
- [ ] Test a documented exact current-attempt rejection of join/rejoin/leave/forget with `prior_delivery_uncertain=False` changes neither lane nor loss set and deletes only that effect. After an unconfirmed older attempt, only exact success may finalize; every rejection-shaped response remains ambiguous and retains the row.
- [ ] After a locally finalized join or leave, deliver its later sync echo and prove the same semantic transition does not increment the membership epoch or emit a second lifecycle/loss record.
- [ ] Test caller cancellation after the server may have accepted the operation detaches the caller but owner finalization still commits.
- [ ] While join/leave/forget is pending, commit both (a) an independently observed exact join/leave postcondition and (b) a conflicting remote membership transition. The exact observation finalizes join/leave once; the conflict supersedes any action; sync never proves forget. Test both delivery states, restart, and late network results. The old effect must be absent, `MembershipOperationSuperseded` must reach a live waiter for the conflict, and no old action may cross or mutate the successor epoch.
- [ ] Kill the process before the durable dispatch claim, after the claim but before HTTP, after the server applies each of join/leave/forget but before local commit, and after finalization. Before the claim, restart may send the exact frozen request. After the claim, restart retains and exposes the unconfirmed effect without automatic replay, a fabricated lifecycle transition, or a terminal session failure; the live pre-crash waiter is gone, status inspection returns the exact ref, and a competing ordinary command raises `MembershipOperationPending`. A later exact join/leave observation may independently finalize once; forget remains unresolved until explicit retry/supersede. Separately prove current-attempt success, first-attempt definitive rejection, first-attempt 429 returning to `READY`, explicit uncertain retry accepting only success as proof, and post-finalization restart, each deleting only the matching effect when justified.
- [ ] Test callback failure is impossible because lifecycle finalization contains no callback.
- [ ] Remove gap-aware send gates from the opt-in path; outbound sends use committed membership state and the crypto worker, not recovery locks.
- [ ] `open_ingestion()` installs one narrow `OutboundCryptoPort` on the authenticated client; `room_send()` delegates only encryption/session mutation to it and performs HTTP outside it. Refuse a second active ingestion/crypto owner and detach the port deterministically on close.
- [ ] Test room state/membership changes emit deterministic batch records, replace only the affected private snapshot, and update the worker's recipient/encryption projection before a later outbound encryption command.
- [ ] Seed a baseline room outside the caller's Sliding windows and prove the reserved coverage/hydration path commits its snapshot and crypto projection before `room_send()` succeeds; pending and unavailable counterexamples must fail with typed errors.
- [ ] Record Task 9 production additions/deletions, replace its estimate with actuals, and reforecast/obtain any required size approval before Task 10.

Run red, then green:

```bash
uv run pytest -q tests/ingest/membership_test.py tests/ingest/coordinator_test.py tests/ingest/source_parity_test.py tests/async_client_test.py -k 'leave or forget or membership or ingestion'
uv run pre-commit run --files src/nio/ingest/coordinator.py src/nio/ingest/config.py src/nio/ingest/errors.py src/nio/ingest/membership.py src/nio/ingest/model.py src/nio/ingest/ports.py src/nio/ingest/__init__.py src/nio/store/sync_journal.py src/nio/client/async_client.py tests/ingest/membership_test.py tests/ingest/coordinator_test.py tests/ingest/source_parity_test.py tests/async_client_test.py
```

Commit: `feat(ingest): serialize membership lifecycle transitions`

---

## Task 10: Prove nio End-to-End and Publish an Integration Release

**Files:**

- Create: `scripts/live_ingest_check.py`
- Create: `tests/ingest/performance_test.py`
- Create: `src/nio/ingest/metrics.py`
- Create: `tests/ingest/metrics_test.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `CHANGELOG.md`
- Modify: `tests/ingest/source_parity_test.py`
- Modify: `tests/ingest/recovery_reducer_test.py`
- Modify: `tests/ingest/frame_staging_test.py`
- Modify: `tests/ingest/crypto_worker_test.py`
- Modify: `tests/ingest/coordinator_test.py`
- Modify: `tests/ingest/consumer_protocol_test.py`
- Modify: `tests/ingest/crash_recovery_test.py`

**Interfaces:** Freeze batch schema version 1 and publish `mindroom-nio==0.40.0rc1` before MindRoom imports the new API. After the benchmark fixtures prove which fields are useful and cheap, freeze process-local `IngestionMetricsSnapshot` plus `metrics_snapshot()`; metrics remain observability only and add no correctness transaction.

- [ ] Run the same semantic contract parameterized over separately initialized Classic and Sliding streams: initial sync, continuation, limited timeline, recovery, restart, loss, membership transition, encryption, and backpressure. There is no cross-mode rebind case.
- [ ] Include pre-existing baseline rooms outside user-supplied Sliding ranges; both transports must emit equivalent state/lifecycle records and converge to equivalent private crypto readiness before the parity gate passes. MindRoom directory parity is a downstream integration gate.
- [ ] Run crash injection at every durable boundary and a state-machine test over frame/recovery/ack/restart commands.
- [ ] Run the live checker against independently provisioned disposable accounts in Classic, Sliding, slam, and reconnect modes.
- [ ] Build frozen-fixture, fake-consumer NIO-native benchmarks for: a large limited timeline with hundreds and thousands of events; multiple recovery pages; many independent rooms; one stuck room while unrelated rooms continue; a burst of small durable frames that naturally coalesce under backlog; a slow consumer/backpressure; and crash/restart after batch creation but before acknowledgement.
- [ ] For every fixture, record records/canonical bytes per batch, fill ratio, transaction and grouped-SQL counts by class, queue/lane depths, oldest-batch age, commit p50/p95/max, CPU time, peak RSS/allocations, bytes copied, frame-arrival-to-batch-publication latency, batch-publication-to-fake-ack latency, and event throughput. Assert transaction growth is `O(raw frames + recovery pages + bounded crypto groups + materialized batches + acknowledgements)`, not `O(records)`, and reject `O(total rooms)` work for a one-room response.
- [ ] With a fake monotonic clock and recording store/crypto fakes, prove the selected metric counters increment once per committed unit rather than once per record, gauges fall after drain, oldest-batch age survives ordinary polling, and failed/rolled-back transitions are labeled separately rather than counted as commits.
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
uv run pytest -q tests/ingest/metrics_test.py
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
- [ ] On first bind, install the advertised first sequence, snapshot durable active-room IDs/their canonical digest, and create their initial `PENDING` room-directory projections in the same transaction as `matrix_sync_consumers`; exact retries return identical `ConsumerBootstrap` bytes even if later room projections change. Any non-identical existing binding fails closed; that principal requires matched-pair restore/fix-forward, while a new stream requires a separate unbound consumer identity.
- [ ] Before `Backend.write()`, reject a batch above the hard canonical record-count, whole-batch byte, or single-record byte ceiling. Add a contract test that nio's serializer and MindRoom's validator agree at each boundary.
- [ ] Compare serialized local/nio loss-reason sets and require exhaustive translation.
- [ ] Inject failure after every record and assert receipt, events, projections, losses, and history debt all roll back.
- [ ] Assert every injected admission rollback also leaves `matrix_sync_consumers.next_sequence` unchanged.
- [ ] Store nio's exact pre-gap boundary in history debt; never reconstruct it from the newest projected event.
- [ ] Admit a batch containing `loss(E)`, lifecycle `E+1`, and later current-epoch records; assert storage/projections preserve that order atomically and history debt remains keyed to the loss's measured boundary/epoch rather than the room's newest projection.
- [ ] Admit `ROOM_READINESS` atomically with room-directory projections. A valid fact advances only its named baseline/current room to `READY` or `UNAVAILABLE`; duplicate replay is idempotent, a rollback exposes neither the fact nor status, and unknown/status/origin/membership combinations fail closed before `Backend.write()`.
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
- [ ] Configure the v2 client with `SqliteStore` and the bootstrap-borrowed writer/database; prove the default `DefaultStore` path is never instantiated for v2. Keep the old callback path's configuration untouched while the cutover flag is disabled.
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

**Interfaces:** Define a MindRoom-owned immutable `RoomDirectory` over room/state/membership projections written by `PrincipalStore.admit_sync_batch()`. It never delegates to nio's private snapshot cache or a live coordinator mapping. A temporary old-path adapter converts current `MatrixRoom` values only until Task 16.

- [ ] Inventory every `client.rooms` access and the exact properties it consumes. Add missing frozen snapshot fields only when the inventory proves they are needed.
- [ ] Write contract tests for lookup, iteration, member lookup, display name, power levels, encryption, group/member counts, aliases, and lifecycle removal.
- [ ] Test projected pending/ready/unavailable baseline rooms. Callers that need authoritative state await MindRoom projection reconciliation or surface a typed unavailable result; none falls back to stale `client.rooms` data. Include a Sliding baseline room outside every caller-supplied window.
- [ ] Prove one admitted batch transaction updates event rows, membership/state projections, and the immutable directory generation atomically; a rollback exposes neither events nor a partially updated directory.
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
- [ ] Configure the new ingestion path explicitly with nio `SqliteStore`; reject `DefaultStore`, `SqliteMemoryStore`, third-party stores, and any direct borrowed-database connect/close. The still-disabled old path keeps its existing store until Task 16 deletes it.
- [ ] Preserve exact pinned-device and Ed25519/Curve25519 authentication checks using immutable device snapshots.
- [ ] Test concurrent RTC key send, desktop to-device receive, inbound room key, and room-message encryption; the worker's command order must be deterministic, all callers receive immutable values, and trust/account/session/receipt persistence uses the shared v2 writer connection.
- [ ] Add an architecture test requiring `rg -n '\b(client|self)\.olm\b|\.session_store\b|\._olm_encrypt\b|\.device_store\b|\.uploaded_key_count\b|\.users_for_key_query\b' src/mindroom` to return no production matches.
- [ ] Record actual production additions/deletions for Tasks 12, 12A, 12B, and 12C across both repositories; replace the aggregate estimate and reforecast/obtain any required size approval before Task 13.

Run red, then green from `/work/dev/mindroom`:

```bash
uv run pytest -q tests/test_matrix_rtc_key_transport.py tests/test_matrix_client_session.py tests/test_desktop_pairing.py tests/test_desktop_client.py
uv run pre-commit run --files src/mindroom/matrix/olm_to_device.py src/mindroom/matrix/client_session.py src/mindroom/matrix/cross_signing.py src/mindroom/matrix/conversation_hydration.py src/mindroom/matrix/client_delivery.py src/mindroom/commands/encryption_commands.py src/mindroom/matrix_rtc/key_transport.py src/mindroom/desktop/session.py src/mindroom/desktop/pairing_receiver.py tests/test_matrix_rtc_key_transport.py tests/test_matrix_client_session.py tests/test_desktop_pairing.py tests/test_desktop_client.py
```

Commit: `refactor(matrix): route crypto through the worker`

---

## Task 13: Provision and Activate a Fresh v1 Device/Store

**Files:**

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

**Interfaces:** Add the MindRoom operator path that logs in/provisions a fresh Matrix device, chooses Classic or Sliding, opens a genuinely new v1 SQLite store, binds its consumer, and completes bounded attachment before activation. There is no existing-store initializer, existing-v1 reset, legacy exporter/importer, manifest, token reuse, dual reader, or clean-cutover path.

- [ ] Require the old process stopped for the account, provision a new Matrix device/access token, and choose a new empty store path. Any existing file, old device/store reuse, non-`SqliteStore` class, or pre-existing v1 marker is refused by this provisioning path.
- [ ] In one nio transaction create the fresh v1 schema, a new unbound `stream_id`, immutable selected transport, binding-operation ID, writer epoch, and matching cold Classic/Sliding source state with no inherited token, `pos`, gap, pending event, or abandonment. Exact reopen uses normal v1 open; provisioning itself is never a reset/rebind operation.
- [ ] In one MindRoom transaction create the consumer at the advertised first sequence, create initial `PENDING` room-directory projections, snapshot canonical sorted durable active-room IDs into `matrix_sync_consumer_baseline_rooms`, and return `ConsumerBootstrap`. Then resume nio attachment in hard-bounded room chunks, creating `PENDING` room state, epoch-0 active lanes, and priority `BASELINE_LOST` ready/lane records with deterministic `SystemOrigin(FRESH_START, binding_operation_id)` until the final chunk atomically marks `ATTACHED`.
- [ ] Chunk thousands of baseline losses through the ordinary hard-bounded batch materializer. They remain ahead of every new source-derived batch, but no special `PENDING_LOSS_ACK` runtime state exists; normal FIFO/backpressure is sufficient.
- [ ] Classic starts with `next_batch=None`; Sliding starts with a new connection instance and `pos=None`. The new device begins with fresh E2EE state and never imports an old to-device cursor, ratchet, trust file, or receipt.
- [ ] Test crashes after device provisioning, v1 initialization, MindRoom bind, every nio attach chunk, and before first HTTP. Each durable step is idempotently resumable; unbound/attaching nio state cannot perform HTTP or deliver batches, and attached loss records cannot be bypassed by later source work.
- [ ] Seed an existing bound v1 marker with a mismatched consumer generation or requested transport and prove provisioning/open performs no inventory/deletion/rebind. That principal permits only matching-pair restore/fix-forward; another source requires a separate unbound consumer identity and store.
- [ ] Test 10,000 baseline rooms attach and materialize through bounded transactions/batches while MindRoom advances `next_sequence` transactionally for each. A room first observed later with an unprovable limited baseline gets the ordinary reducer `BASELINE_LOST`; no legacy lookup fills the gap.
- [ ] In both Classic and Sliding cutover fixtures, wait until every baseline room is ready/unavailable before enabling MindRoom outbound room operations. For Sliding, require multiple reserved range expansions plus fallback hydration of a baseline room absent from the server list; caller-supplied partial windows cannot strand it.
- [ ] Preserve the old device/database untouched as an operator rollback artifact. Before v2 sends Matrix HTTP, rollback means stop v2 and resume the old device/process; after either device has continued independently, never copy ratchets between them. Retire/log out the old device only after the observation window.
- [ ] Record Task 13 production additions/deletions in both repositories, replace its estimate, and reforecast/obtain any required size approval before Task 14.

First run and publish nio:

```bash
cd /work/dev/mindroom-nio
uv run pytest -q tests/ingest/fresh_start_test.py tests/ingest/consumer_protocol_test.py
uv run pre-commit run --files pyproject.toml uv.lock CHANGELOG.md src/nio/ingest/__init__.py src/nio/store/sync_journal.py src/nio/store/database.py tests/ingest/fresh_start_test.py
uv build
```

Commit nio: `feat(ingest): finalize fresh-device initialization`

Publish/tag `mindroom-nio==0.40.0rc2`. Only after that artifact exists, pin MindRoom exactly to rc2 and regenerate its lock:

```bash
cd /work/dev/mindroom
uv add --exact 'mindroom-nio[e2e]==0.40.0rc2'
uv sync --locked
uv run pytest -q tests/test_matrix_fresh_ingestion.py tests/test_matrix_sync_batch_stream.py
uv run pre-commit run --files pyproject.toml uv.lock src/mindroom/matrix/fresh_ingestion.py src/mindroom/bot.py tests/test_matrix_fresh_ingestion.py
```

Commit MindRoom: `build: pin mindroom-nio rc2` then `feat(matrix): provision fresh ingestion device`

---

## Task 14: Pass the Full Pre-Deletion Crash and Canary Gate

**Files:**

- Modify: `scripts/live_ingest_check.py`
- Modify: `tests/ingest/crash_recovery_test.py`
- Modify: `tests/ingest/source_parity_test.py`
- Modify: `/work/dev/mindroom/tests/test_event_journal_crash_matrix.py`
- Modify: `/work/dev/mindroom/tests/test_matrix_sync_batch_stream.py`

- [ ] Run a process-kill matrix at every boundary: fresh-store initialization, consumer bind/attach, HTTP result, frame staging, crypto mutation, crypto receipt, reducer commit, batch publish, MindRoom receipt, each record projection, MindRoom commit, nio ack, batch compaction, and membership dispatch/success. A crash after membership dispatch but before durable success must retain `DISPATCHED_UNCONFIRMED` and must not invent or automatically replay the operation.
- [ ] Run property/state-machine tests generating sync/recovery/membership/restart/ack interleavings for both sources.
- [ ] Include the epoch-retirement race in the cross-repository crash matrix: in-flight `R(E)`; committed `loss(E) -> lifecycle(E+1)`; stale `R(E)` arrival; MindRoom admission or nio acknowledgement failure; restart and identical replay; then a genuinely failing `R(E+1)`. Prove the `E` loss is neither lost, duplicated, nor applied to `E+1`, and the `E+1` loss remains distinct and retained.
- [ ] Assert the fate invariant after every simulated crash: each generated timeline event is in MindRoom, in a durable nio raw frame, generic ready row, epoch recovery lane, or batch, or is named by a `LossRecord` payload in ready/lane/batch or MindRoom's admitted loss projection.
- [ ] Explicitly include unbound/attached fresh-start crashes, priority baseline-loss chunking, ack-before-dispatch restart scan, cross-frame Olm replay, later-frame Megolm keys, held-lane frame retirement, and desktop command admission.
- [ ] Provision production-like fresh devices/stores for both sources; prove the old database is never opened, the new store uses the one-connection SQLite-backed E2EE schema, and baseline losses derive only from MindRoom's current durable room projection.
- [ ] Verify the installed artifact is exactly `mindroom-nio==0.40.0rc2` and rerun the cross-repository tests from that artifact before any canary. If this task changes product code, publish/pin rc3 and repeat the artifact gate.
- [ ] Activate disposable canary accounts in Classic and Sliding while the old implementation remains compiled but disabled for those stores. Require successful MindRoom batch receipts and nio acks.
- [ ] Soak both canaries for at least one hour with reconnects, 429s, homeserver restarts, encrypted rooms, limited timelines, leave/rejoin, and consumer stalls.
- [ ] Verify one blocked room does not reduce throughput for unrelated rooms beyond bounded shared-writer queue occupancy.
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

- [ ] Stop the old process, preserve its device/database untouched, provision the selected fresh device/store, attach the bound consumer, and verify its priority baseline-loss plan is the oldest durable work before Matrix HTTP begins.
- [ ] Select the v2 path at process startup and forbid live toggling. Once a store has a v1 ingestion marker, the old path in the shipped binary must refuse it; after Matrix HTTP/crypto begins, recovery is fail-stop/fix-forward rather than stale-database rollback.
- [ ] Keep the old code physically present for one production observation window, but make v2 the only legal path for the cut-over store.
- [ ] Require both nio's private `wait_baseline_hydrated()` crypto-readiness gate and MindRoom's admitted room-directory projection reconciliation before enabling outbound room operations; inspect and resolve every unavailable baseline room explicitly in both source modes.
- [ ] Observe Classic and Sliding production accounts through reconnect, encrypted sends, room recovery, and at least one membership change.
- [ ] Require zero unacknowledged batches outside consumer backpressure, zero unexplained MindRoom-admitted or nio-ready/lane/batch loss payloads, and a fully drained initial baseline plan before authorizing deletion.

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
- [ ] Remove legacy sync/recovery models and migration code after the observation window. The old database remains outside v2 and may be archived or deleted by the operator; product code never opens or reclaims it.
- [ ] Remove MindRoom continuity/checkpoint/certification/escape/token modules and callback registration.
- [ ] Delete the old `MatrixRoom` directory adapter and temporary path-selection flag. Keep `FreshIngestionRequired`, fresh-device/store provisioning, the v1 marker check, and MindRoom's own immutable `RoomDirectory` projection.
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
- One physical SQLite writer executes all v1 journal/E2EE persistence; coordinator and crypto remain separate logical owners with immutable commands/results.
- Each nio loss payload has exactly one durable owner at a time (ready row, lane row, or batch), then MindRoom owns the admitted audit projection after acknowledgement.
- No legacy sync lock, `ContextVar`, recovery capability flag, application-owned checkpoint, or callback acceptance write remains.
- Fresh-device initialization never opens an old store, uses the single-connection SQLite-backed E2EE schema, and places bounded `BASELINE_LOST` records for MindRoom's pre-existing active rooms ahead of new source records.
- A stream's transport and consumer binding are immutable after v1 creation. A consumer/journal mismatch and a poison batch both fail stopped with original nio payload retained; neither is silently skipped or mislabeled as Matrix history loss.
- MindRoom owns the application room-directory projection; nio retains only private affected-room state/readiness required for reduction and crypto.
- Unknown wire values fail before admission and ack.
- A one-room response performs O(affected rooms) work, not O(all rooms).
- Full test, lint, build, live-soak, line-budget, and crash-matrix gates pass from clean worktrees.

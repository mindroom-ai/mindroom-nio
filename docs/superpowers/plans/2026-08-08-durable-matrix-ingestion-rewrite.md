# Durable Matrix Ingestion Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (preferred when available) or `superpowers:executing-plans` to implement this plan task by task, `superpowers:test-driven-development` for every behavior change, and `superpowers:verification-before-completion` before each commit or release gate.

**Goal:** Replace the current callback-, lock-, and recovery-patch-based sync subsystem with a durable ingestion stream in which every Matrix timeline event is either durably admitted to MindRoom, retained by nio for retry, or accompanied by an explicit durable loss record.

**Architecture:** A new `nio.ingest` package owns Classic Sync and Sliding Sync through one queue-driven coordinator, transport-specific pure adapters, a pure per-room reducer, a fresh version-1 ingestion journal, and one serialized replay-safe crypto worker. nio stages immutable frames and publishes immutable FIFO batches. MindRoom admits a whole batch in one event-journal transaction and only then acknowledges it to nio. The two databases never share a transaction; idempotent replay is the boundary.

The plan calls this replacement implementation “v2” when contrasting it with the callback architecture; its new wire and journal schemas both start at version 1.

**Tech Stack:** Python 3.12+, `asyncio.TaskGroup`, frozen/slotted dataclasses, `enum.StrEnum`, SQLite/Peewee for the nio-local journal and E2EE state, AES-GCM for staged payloads, MindRoom's existing SQLite/PostgreSQL event-journal abstraction, pytest, Hypothesis where state-machine coverage is valuable, Ruff, pre-commit, and uv.

> **Task 4.5 supersession:** Every pre-Task 1 interface or schema section
> whose heading is marked **Archived** (including **Archived Frozen Public
> Contract**) is historical evidence only and is non-normative: it defines no
> current API, schema, owner, acceptance criterion, or implementation task.
> The authoritative current boundary is the Task 4.5 Closure and Tasks 5-17
> inserted after it, read with the approved simplification design and plan. In
> particular, no current task may infer that Task 4.5 persisted room lanes,
> ready/batch rows, recovery/hydration effects, membership delivery, crypto
> receipts, or attachment state.

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
- The current Task 4.5 closure is +5,537 runtime source in nio alone and
  +6,239 under the formal nio src-plus-scripts accounting rule. The checkpoint
  is one line below +6,240; the final cross-repository cap remains +6,000 after
  actual observed deletions. Reforecast after every task or 500 production
  additions, keep conditional deletions separate from actuals, and stop before
  the next task if no compliant scope reduction, deletion, or redesign remains.
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

## Archived Frozen Public Contract

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

## Archived Internal Interfaces

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

## Archived nio Journal Schema

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

## Archived Source and Recovery Semantics

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

## Archived Crypto Replay Contract

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

## Archived MindRoom Admission Boundary

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

## Archived Target File Map

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

## Archived Production-Line Forecast

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

## Archived Delivery and Branch Strategy

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

## Task 4.5 Closure: Source-Only Durable Core

**Authority:** This closure replaces the Task 4.5 implementation boundary and
all later-task assumptions that the checkpoint already persists room lanes,
losses, batches, recovery/hydration effects, membership delivery state, or
attachment state. The approved source is
docs/superpowers/specs/2026-08-09-durable-ingestion-simplification-design.md,
implemented through
docs/superpowers/plans/2026-08-09-durable-ingestion-task-4.5-simplification.md.
The archived sections elsewhere in this file are retained only as historical
evidence.

The closed checkpoint owns exactly one ordinary Peewee SQLite connection,
immutable transport selection, stored membership observations, and one
canonical staged source response that is re-normalized on restart. Raw journal
SQL and borrowed SqliteStore E2EE model writes share the same outer Peewee
transaction. The immediate topology contains only NioIngestMeta,
NioIngestSourceState, NioIngestFrame, and its drain index alongside the
borrowed E2EE schema.

It deliberately does **not** own room aggregates, lanes, ready rows, batches,
loss ownership, recovery/hydration effects, crypto receipts, membership
operations, consumer attachment, or a desktop v2 path. Each returns only with
the task that has both its producer and its durable consumer.

### Closure evidence and accounting

The fixed nio base is 980e53b3a3815592c3bd0dab9e41e2b057e50724. The final
Task 8 benchmark candidate is commit
81e86789dcdfa0288dd6d99ddb88a23862fde4ea.

| Accounting view | Additions | Deletions | Net |
|---|---:|---:|---:|
| Runtime source under src/ | 5,590 | 53 | +5,537 |
| Scripts/benchmark tooling | 707 | 5 | +702 |
| Formal source-plus-scripts accounting | 6,297 | 58 | +6,239 |

Runtime source is the primary complexity measure. Formal reporting still
includes scripts because the approved line-accounting rule includes both src
and scripts. Task 8 changed runtime source by only +62/-26, net +36; its
reproducible harness is 696 lines of tooling, for a Task 8 formal contribution
of +758/-26, net +732. The formal checkpoint is one line below the approved
+6,240 high side. The measured 58 deletions are already in the table; no future
deletion is credited.

### Provenance, rollback, and rework accounting

The following commits identify where retained behavior and removed premature
scope came from. They are provenance, not additional terms to add to the
current +6,239 formal subtotal:

| Provenance | Commit(s) | Additions | Deletions | Net |
|---|---|---:|---:|---:|
| Retained transport behavior | `59642ef` | 196 | 42 | +154 |
| Retained observation behavior | `d4edd91` | 198 | 110 | +88 |
| Retained staging behavior | `61ea2cd` | 532 | 124 | +408 |
| Selective rollback | `5ad75bf..e3b63a5` | 766 | 5,411 | -4,645 |
| Owner integration | `c566348..0bb296b` | 607 | 408 | +199 |
| Task 7 planner | Task 7 | 48 | 0 | +48 |
| Task 8 formal contribution | Task 8 | 758 | 26 | +732 |

The original retained-behavior provenance is +650 (+154 + +88 + +408). It
documents behavior re-ported through the simplification; it is not an additive
current subtotal. The 5,411 deleted lines in the selective rollback are
attributed exactly as 3,978 from premature commits
`22606cd/39467c3/aa2ef44/c2a623f/c4cec82/da8df4e/8bdd360`, 1,131 from
pre-Task-4.5 dormant or future-owner scaffold origins, and 302 from the
retained transport/observation/staging commits re-ported rather than retained
verbatim. Thus 3,978 + 1,131 + 302 = 5,411. Task 8's runtime source portion is
only +62/-26, net +36; the remaining +696 formal additions are its benchmark
harness tooling.

At the benchmark closure candidate, uncommitted and untracked production Python
was zero. Any subsequently uncommitted closure documentation is non-production
and does not change that closure fact.

The approved benchmark ran 735 of 735 required samples. Its raw artifact SHA-256
is f8410859c0642f4f47e5a5e70e6a4cd52cf8cd283e9f9e1f0f6947fd5bf16bcc and
its report SHA-256 is
db11dd504b114ac5c56a96ff4cbda0ad008dae541e3a7f33c51d61d50bfe40cf.
Every candidate sample met the six-significant-statement, one-outer-transaction,
three-DML constraint. The external-write-lock diagnostic was 100.518 ms with
rollback and retry; the 10,000-poll capacity diagnostic made zero plan calls,
zero sends, zero SQL statements, and zero RSS growth. The worst paired wall
gate consumed 89.72 percent of its allowed budget. The final repository gate
was 1,665 passed and 3 skipped.

### Conditional deletion dependencies and hard forecast

The final cross-repository limit remains +6,000 production lines. The remaining
program is capped at +3,150. Therefore the formal pre-deletion forecast is
+6,239 + +3,150 = +9,389. The following 3,393 lines are conditional
whole-file deletions, not present credit. The consumer column is an
import/symbol scan of live production Python (type-only consumers are labelled);
tests are not counted as production consumers.

| Conditional whole-file candidate | Exact live production consumers | Replacement checkpoint (hard blocker while any named consumer remains) | Observation gate |
|---|---|---|---|
| nio `src/nio/client/sync_recovery.py` (1,780) | `src/nio/client/async_client.py` imports and invokes the recovery plan/pump/persist API; `src/nio/client/sliding_membership.py` imports `is_own_join`; `src/nio/store/database.py` imports recovery values type-only and runtime-imports/constructs `PendingTimelineEvent` in `MatrixStore.load_sync_recovery`; `/work/dev/mindroom/src/mindroom/bot.py` calls `clear_persisted_sync_recovery`, `has_uncommitted_classic_sync_state`, `acknowledge_classic_sync`, and `reset_classic_sync_state`, and consumes `unrecovered_room_ids` in its sync handlers. | Tasks 5-8 supply reducer, materializer, crypto handoff, and recovery execution; Task 10 must remove the `AsyncClient` recovery API/response dependency; Tasks 11-12 replace the named bot callback/checkpoint calls with admitted-batch ownership. | Task 15 bot observation **and** separately scoped Task 12B desktop/pairing observation; Task 16 may delete only after every named import/call is gone. |
| nio `src/nio/client/sliding_membership.py` (262) | `src/nio/client/async_client.py` imports `plan_sliding_prev_batches`, `sliding_live_event_count`, `sliding_membership_proof_mismatch`, `sliding_recovery_cursor`, `sliding_recovery_membership`, `sliding_room_is_invite`, and `sliding_unrecoverable_discontinuity_room_ids`. | Task 5's pure reducer replaces membership decisions; Task 10 must make `AsyncClient` cease importing these helpers before Task 16. | Task 15 bot observation and Task 12B desktop/pairing observation; the `async_client.py` import is a deletion blocker. |
| nio `src/nio/client/sync_reset_fence.py` (123) | `src/nio/client/async_client.py` imports `SyncRequestId`, `SyncResetFence`, `issue_sync_request`, `finish_sync_request`, `mark_room_reset`, and `commit_one_time_key_counts`; `src/nio/client/sync_response_ordering.py` imports the fence and acceptance helpers. | Task 10 must replace legacy request/reset ordering with durable source-stage sequencing and remove both named import sites. | Task 15 bot observation and Task 12B desktop/pairing observation; either named importer blocks deletion. |
| nio `src/nio/client/sync_response_ordering.py` (162) | `src/nio/client/async_client.py` imports `OneTimeKeyCountCommit`, `account_data_kind`, and `ordered_response_view`. | Task 10's integrated source/crypto path must replace response filtering and remove this `async_client.py` import. | Task 15 bot observation and Task 12B desktop/pairing observation; the named importer blocks deletion. |
| nio `src/nio/recovery_abandonment.py` (83) | `src/nio/client/async_client.py` and `src/nio/client/sync_recovery.py` import `RecoveryAbandonment`; `src/nio/responses.py` imports it and normalizes `abandoned_rooms`; `src/nio/store/models.py` uses it for `SyncRecoveryAbandonedRooms.reason`; `src/nio/store/database.py` uses it in recovery write/load methods; `src/nio/__init__.py` re-exports it. | Task 6's single durable `LossRecord` owner and Task 10's frozen batch-loss contract must replace the response/store/public-export contract; Task 16 must remove or replace each named consumer, not merely delete this module. | Task 15 bot observation and Task 12B desktop/pairing observation; `responses.py`, `store/models.py`, `store/database.py`, or `nio/__init__.py` remaining is a deletion blocker. |
| nio `src/nio/sliding_sync_tokens.py` (23) | `src/nio/client/async_client.py` imports/holds `SlidingWindowToken`; `src/nio/client/sliding_membership.py` imports it; `src/nio/client/sync_recovery.py` imports it type-only; `src/nio/store/database.py` loads/saves it; `src/nio/store/__init__.py` and `src/nio/__init__.py` re-export it. | Tasks 5 and 10 must use the staged source/reducer transport state; Task 13's fresh-store attach must replace legacy token persistence; Task 16 must remove every named store and public re-export consumer. | Task 15 bot observation and Task 12B desktop/pairing observation; any named import/re-export blocks deletion. |
| MindRoom `src/mindroom/matrix/sync_continuity.py` (246) | `src/mindroom/bot.py` constructs `SyncContinuityStore`, wires it into `BotRoomLifecycleDeps`, and publishes `SyncContinuityRecord`; `src/mindroom/bot_room_lifecycle.py` type-imports both values and calls `load`, `update_join_fences`, and `apply_continuity_record` from `observe_trusted_sync_rooms` and `restore_pending_join_decrypt_fences`; `sync_checkpoint_trust.py` type-imports the same values. | Tasks 11-12 atomic admission/batch stream and Task 12A RoomDirectory must replace the bot construction, lifecycle dependency, and exact calls. | Task 15 bot observation; any listed bot/lifecycle/type import must be removed before Task 16. |
| MindRoom `src/mindroom/matrix/sync_checkpoint_trust.py` (413) | `src/mindroom/bot.py` constructs `SyncCheckpointTrust` and calls `prepare_startup`, `apply_response`, `retry_token`, `observed_recovery`, `plan_response`, rejection/failure methods, and `persist_current`. | Tasks 11-12 admitted-batch cursor/dispatch ownership plus Task 13 attachment replace the named bot checkpoint calls. | Task 15 bot observation; the `bot.py` construction or any named call blocks deletion. |
| MindRoom `src/mindroom/matrix/sync_certification.py` (175) | `src/mindroom/bot.py` imports `SyncCertificationDecision`, `SyncRecoveryOutcome`, and `SyncTrustState`; `sync_checkpoint_trust.py` imports and uses `SyncRecoveryOutcome`, `certify_sync_response`, `handle_unknown_pos`, and `sync_recovery_diagnostics`. | Task 8's durable recovery outcome and Tasks 11-12 admitted-batch state must replace both bot and checkpoint-trust consumers. | Task 15 bot observation; either named consumer blocks deletion. |
| MindRoom `src/mindroom/matrix/sync_recovery_escape.py` (100) | `src/mindroom/matrix/sync_checkpoint_trust.py` imports `SkippedRecoveryGap` and `SyncRecoveryStallTracker` and calls them from `_observe_recovery_stalls` and skipped-gap reporting. | Task 8's durable recovery/hydration terminal result and Task 11's admitted loss path must replace the named trust-owner logic. | Task 15 bot observation; the `sync_checkpoint_trust.py` import/calls block deletion. |
| MindRoom `src/mindroom/matrix/sync_token_values.py` (26) | `sync_continuity.py` imports `SyncCheckpoint` and `normalize_sync_token`; `sync_checkpoint_trust.py` imports `SyncCheckpoint`; `sync_certification.py` imports both values. | Task 10's canonical source state, Task 13 attachment, and Tasks 11-12 admitted consumer progression must remove all three named consumers. | Task 15 bot observation; any named consumer blocks deletion. |

The six nio candidates total 2,433 whole-file lines; the five MindRoom
candidates total 960; 2,433 + 960 = 3,393. Their source paths are the measured
whole-file list, not a claim that nio's +5,537 runtime-source figure is
cross-repository.

Only after the exact replacement and observation gate may a deletion commit
change the actual count. If all 3,393 conditional lines are actually deleted,
the conditional final forecast is +5,996. The broader 5,544-line
whole-file-plus-symbol inventory remains contingency only.

The only quantified greater-than-50-percent estimate miss in the simplified
checkpoint is the joined stage snapshot/validation slice: +62 actual against
an approved +10 to +25 estimate, 148 percent above the high estimate. Its 26
deletions are reported separately. The +36 runtime source change and 696-line
harness had no separately approved sub-estimates; they are reported as measured
scope, not retroactive estimate compliance.

## Task 5: Define the Pure Reducer

**Scope:** Define frozen, deterministic room/recovery transition values and
pure reduction functions. Task 5 performs no SQL, opens no store, schedules no
HTTP, and does not introduce lanes, batches, effects, or attachment rows.

- [ ] Consume the durable StagedFrame identity and its restart-renormalized
  SyncFrame, not a live SourceResult or mutable SourceState. Task 4.5 has
  already advanced source state when it staged the canonical response.
- [ ] Give every restart-renormalized frame and stored membership observation a
  deterministic room/recovery transition result.
- [ ] Represent recovery/hydration intent as typed proposed values only; Task 6
  is the first durable owner.
- [ ] Prove with synchronous state-machine tests that gap/loss classification,
  membership-epoch retirement, ordering, and retry/terminal distinctions are
  deterministic and room-local.
- [ ] Publish the Task 5 line envelope, RED/GREEN evidence, and independent
  review before Task 6.

Gate: no deferred persistence guarantee becomes reachable; the reducer is
independent of SQLite, callbacks, clocks, and AsyncClient.

---

## Task 6: Add the First Durable Aggregate and Materializer

**Scope:** Add only the minimal per-room aggregate, ready queue, batch/outbox,
frame retirement, and loss/gap ownership required to consume Task 5 proposals.
This is the first task allowed to reintroduce room/lane/ready/batch persistence.

- [ ] Commit room transition and its minimal recovery/hydration request row
  atomically. A crypto-free frame may retire only after all of its derived room
  work has a durable owner.
- [ ] Retain every frame containing to-device, device-list, one-time-key-count,
  or fallback-key crypto input. Task 6 neither interprets nor clears those
  inputs and does not claim final retirement for that frame.
- [ ] Materialize bounded FIFO batches without per-record transactions; retain
  one durable owner for every loss and frame-derived record.
- [ ] Prove frame-to-room-to-batch crash fate, recovery/loss ordering,
  acknowledgement replay, limits, and unrelated-room progress.
- [ ] Record SQL, writer wait, memory, and actual line changes before Task 7.

Gate: a crypto-free frame never retires before all of its derived work has a
durable owner; a crypto-bearing frame remains staged until Task 7 completes its
receipt-safe handoff. One blocked room cannot block unrelated eligible rooms
beyond bounded shared-writer occupancy.

---

## Task 7: Make Crypto Replay-Safe

**Scope:** Add crypto receipt tables and the owner-thread shared Peewee
transaction that commits Olm/Megolm mutations with replay receipts. Crypto
preparation remains outside the write-lock hold.

- [ ] Define immutable lookup, prepared transaction, and commit outcome values.
- [ ] Revalidate receipt hits/misses inside the one owner transaction and
  fail-stop/discard prepared state on commit failure or cancellation.
- [ ] Claim the retained crypto-bearing staged-frame inputs, commit their
  receipt-safe E2EE outcome, and authorize final frame retirement only after
  the Task 6 room-derived work and Task 7 crypto-derived work both have durable
  owners.
- [ ] Prove duplicate Olm/Megolm inputs, crash/restart, and receipt races never
  reratchet or expose an uncommitted result.

Gate: no worker-thread SQLite/Peewee access, no second journal-file connection,
and no crypto result before its durable outcome.

---

## Task 8: Execute Recovery and Hydration Effects

**Scope:** Execute the typed recovery/hydration requests made durable by Task 6
and reschedule them after retryable result or restart. It owns execution, not
the task-5 proposal grammar or task-6 durable state.

- [ ] Dispatch only frozen durable requests and return tagged results to the
  owner.
- [ ] Commit result handling/idempotency with the owning aggregate and prove
  dispatch/result/restart crash behavior.
- [ ] Keep primary-source reset fencing separate from independently
  membership-bound recovery work.

Gate: recovery and hydration survive restart exactly once at their durable
owner, without enabling a broad generic effect platform.

---

## Task 9: Own Uncertain Membership Delivery

**Scope:** Add the membership-operation row and operator-visible resolution
path only after a coordinator can own dispatch.

- [ ] Persist READY to DISPATCHED_UNCONFIRMED before membership HTTP.
- [ ] Never automatically replay an unknown outcome; exact observation or
  explicitly fenced RETRY/SUPERSEDE is required to resolve it.
- [ ] Prove dispatch-before-result kills, concurrent command rejection, and
  unrelated ingestion progress.

Gate: a membership ambiguity remains visible and bounded, never fabricated as
success or terminal session failure.

---

## Task 10: Integrate nio and Freeze End-to-End Budgets

**Scope:** Publish the first integration candidate and measure the now-complete
nio path. Add observability only after it cannot alter correctness ownership.

- [ ] Freeze batch schema/API and record end-to-end transaction, latency,
  memory, backpressure, and one-room-versus-all-room work budgets.
- [ ] Reforecast actual production changes and deletion dependencies before any
  MindRoom activation work.

Gate: all Task 4.5 benchmark limits still pass and new integration budgets are
measured rather than inferred.

---

## Tasks 11-12C: MindRoom Bot Activation Prerequisites

**Scope:** In the companion repository, add atomic batch admission, a minimal
authoritative RoomDirectory, batch-stream dispatch, and remove direct
MindRoom-to-nio crypto/session reach-through. These are bot activation
prerequisites, not work implicitly owned by nio Task 4.5.

- [ ] Task 11 makes each batch admission atomic and restart-deduplicated.
- [ ] Task 12 replaces callbacks with the durable batch stream.
- [ ] Task 12A makes MindRoom's immutable RoomDirectory authoritative.
- [ ] Task 12B is a separately scoped desktop/pairing migration; desktop stays
  disabled for v2 until its written plan, budget, and review pass.
- [ ] Task 12C removes direct Olm/session access before an activated path could
  depend on it.

Gate: MindRoom consumes only admitted batches and its own room projection; no
callback or mutable nio room cache is a v2 correctness dependency.

---

## Task 13: Provision a Fresh Store and Attach a Bounded Baseline

**Scope:** Provision a fresh v1 device/store, bind the consumer, and attach
baseline facts in always-bounded, resumable chunks before source HTTP begins.

- [ ] Reject mismatched existing binding/transport without opening, rebinding,
  or mutating it.
- [ ] Persist attach digest/ordinal and restart safely at every chunk/final
  boundary.
- [ ] Materialize FIFO baseline loss ahead of source-derived work.

Gate: Matrix HTTP and normal delivery stay disabled until the final attachment
chunk commits.

---

## Task 14: Run the Pre-Deletion Crash and Canary Gate

**Scope:** Run the complete cross-repository crash matrix and canary against
the exact release candidate while legacy code remains available for comparison.

- [ ] Cover source staging, crypto, reducer/materializer, admission,
  acknowledgement, attachment, and membership ambiguity crash boundaries.
- [ ] Verify frozen performance budgets and the consuming repository's
  acceptance benchmark.
- [ ] Do not delete legacy code in this task.

Gate: every enabled invariant has its named durable owner and all production
canary evidence passes without a late waiver.

---

## Task 15: Activate the Bot-Only v2 Path

**Scope:** Activate a fresh, bound bot account while retaining the old path for
the observation window. Desktop/pairing remains disabled for v2.

- [ ] Require baseline completion, crypto/readiness gates, admitted
  RoomDirectory reconciliation, and explicit resolution of unavailable rooms.
- [ ] Observe Classic and Sliding through reconnect, encrypted traffic,
  recovery, and membership change.
- [ ] Require no unexplained durable loss, unacknowledged batch outside
  backpressure, or hidden legacy fallback before deletion authorization.

Gate: bot observation is complete; this is not desktop activation.

---

## Task 16: Delete Only Observed Legacy Paths

**Scope:** Delete the conditional files listed in the Task 4.5 closure table
only after their exact replacement checkpoint and observation gate pass.

- [ ] Port each retained invariant into the new tests before removing its old
  test or implementation.
- [ ] Delete the nio 2,433-line group only after bot and desktop observation.
- [ ] Delete the MindRoom 960-line group only after bot observation.
- [ ] Recompute actual source and source-plus-script accounting from the fixed
  bases; do not substitute a forecast for a deletion diff.

Gate: the final measured cross-repository net is at or below +6,000 and all
legacy search invariants are absent from production.

---

## Task 17: Repeat Verification and Release

- [ ] Repeat Task 14 crash, benchmark, and integration gates after the deletion
  commits.
- [ ] Run Classic and Sliding release soaks from fresh cut-over stores.
- [ ] Publish the final nio artifact, pin it in MindRoom, and rerun both clean
  suites.

## Completion Criteria

The rewrite is complete only when the source-only core has grown through the
named owners above, every enabled bot invariant is covered by the pre-deletion
gate, desktop has its separate approved migration, actual—not conditional—line
accounting is at or below +6,000, and the old paths have passed their
observation gates before deletion.

SCHEMA_VERSION = 2

META_TABLE_SQL = """CREATE TABLE NioIngestMeta (
    account_id TEXT PRIMARY KEY CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    device_id TEXT NOT NULL CHECK (
        typeof(device_id) = 'text' AND length(device_id) > 0
    ),
    schema_version INTEGER NOT NULL CHECK (
        typeof(schema_version) = 'integer' AND schema_version = 2
    ),
    stream_id TEXT NOT NULL CHECK (
        typeof(stream_id) = 'text' AND length(stream_id) > 0
    ),
    consumer_generation TEXT NOT NULL CHECK (
        typeof(consumer_generation) = 'text' AND length(consumer_generation) > 0
    ),
    transport_kind TEXT NOT NULL CHECK (
        typeof(transport_kind) = 'text'
        AND length(transport_kind) > 0
        AND transport_kind IN ('classic', 'sliding')
    ),
    revision INTEGER NOT NULL CHECK (
        typeof(revision) = 'integer' AND revision >= 0
    ),
    writer_epoch TEXT NOT NULL CHECK (
        typeof(writer_epoch) = 'text' AND length(writer_epoch) > 0
    ),
    next_source_epoch INTEGER NOT NULL CHECK (
        typeof(next_source_epoch) = 'integer' AND next_source_epoch >= 1
    ),
    created_at_ns INTEGER NOT NULL CHECK (
        typeof(created_at_ns) = 'integer' AND created_at_ns >= 0
    ),
    delivery_next_sequence INTEGER NOT NULL CHECK (typeof(delivery_next_sequence) = 'integer' AND delivery_next_sequence BETWEEN 0 AND 9223372036854775807),
    delivery_acknowledged_sha256 BLOB NULL CHECK (delivery_acknowledged_sha256 IS NULL OR (typeof(delivery_acknowledged_sha256) = 'blob' AND length(delivery_acknowledged_sha256) = 32)),
    delivery_outstanding_work_id TEXT NULL CHECK (delivery_outstanding_work_id IS NULL OR (typeof(delivery_outstanding_work_id) = 'text' AND length(delivery_outstanding_work_id) > 0)),
    delivery_outstanding_ready_revision INTEGER NULL CHECK (delivery_outstanding_ready_revision IS NULL OR (typeof(delivery_outstanding_ready_revision) = 'integer' AND delivery_outstanding_ready_revision >= 1)),
    delivery_outstanding_ready_ordinal INTEGER NULL CHECK (delivery_outstanding_ready_ordinal IS NULL OR (typeof(delivery_outstanding_ready_ordinal) = 'integer' AND delivery_outstanding_ready_ordinal >= 0)),
    delivery_outstanding_batch_sha256 BLOB NULL CHECK (delivery_outstanding_batch_sha256 IS NULL OR (typeof(delivery_outstanding_batch_sha256) = 'blob' AND length(delivery_outstanding_batch_sha256) = 32)),
    CHECK (
        (delivery_outstanding_work_id IS NULL
         AND delivery_outstanding_ready_revision IS NULL
         AND delivery_outstanding_ready_ordinal IS NULL
         AND delivery_outstanding_batch_sha256 IS NULL)
     OR (delivery_outstanding_work_id IS NOT NULL
         AND delivery_outstanding_ready_revision IS NOT NULL
         AND delivery_outstanding_ready_revision <= revision
         AND delivery_outstanding_ready_ordinal IS NOT NULL
         AND delivery_outstanding_batch_sha256 IS NOT NULL)
    ),
    CHECK (
        (delivery_next_sequence = 0
         AND delivery_acknowledged_sha256 IS NULL
         AND delivery_outstanding_work_id IS NULL)
     OR (delivery_next_sequence = 1
         AND delivery_acknowledged_sha256 IS NULL
         AND delivery_outstanding_work_id IS NOT NULL)
     OR (delivery_next_sequence >= 1
         AND delivery_acknowledged_sha256 IS NOT NULL
         AND delivery_outstanding_work_id IS NULL)
     OR (delivery_next_sequence >= 2
         AND delivery_acknowledged_sha256 IS NOT NULL
         AND delivery_outstanding_work_id IS NOT NULL)
    ))"""

SCHEMA_SQL = (
    """CREATE TABLE NioIngestSourceState (
    account_id TEXT PRIMARY KEY REFERENCES NioIngestMeta(account_id) CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    source_epoch INTEGER NOT NULL CHECK (
        typeof(source_epoch) = 'integer' AND source_epoch >= 0
    ),
    payload BLOB NOT NULL CHECK ( typeof(payload) = 'blob' AND length(payload) > 0 ), payload_sha256 BLOB NOT NULL CHECK ( typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32 ),
    next_request_id INTEGER NOT NULL CHECK (
        typeof(next_request_id) = 'integer' AND next_request_id >= 0
    ),
    active INTEGER NOT NULL CHECK (
        typeof(active) = 'integer' AND active IN (0, 1)
    ))""",
    """CREATE TABLE NioIngestFrame (
    account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    frame_id TEXT NOT NULL CHECK (
        typeof(frame_id) = 'text' AND length(frame_id) > 0
    ),
    source_epoch INTEGER NOT NULL CHECK (
        typeof(source_epoch) = 'integer' AND source_epoch >= 0
    ),
    request_id INTEGER NOT NULL CHECK (
        typeof(request_id) = 'integer' AND request_id >= 0
    ),
    staged_revision INTEGER NOT NULL CHECK (
        typeof(staged_revision) = 'integer' AND staged_revision >= 1
    ),
    payload BLOB NOT NULL CHECK ( typeof(payload) = 'blob' AND length(payload) > 0 AND length(payload) <= 24 * 1024 * 1024 ),
    payload_sha256 BLOB NOT NULL CHECK (
        typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
    ),
    room_materialized_revision INTEGER NULL CHECK (
        room_materialized_revision IS NULL OR
        (typeof(room_materialized_revision) = 'integer'
         AND room_materialized_revision >= 1)
    ),
    drain_header_sha256 BLOB NOT NULL CHECK ( typeof(drain_header_sha256) = 'blob' AND length(drain_header_sha256) = 32 ),
    PRIMARY KEY (account_id, frame_id))""",
    """CREATE INDEX NioIngestFrame_drain ON NioIngestFrame(
    account_id, staged_revision, source_epoch, request_id, frame_id)""",
    """CREATE TABLE NioIngestRoomAggregate (
    account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    room_id TEXT NOT NULL CHECK (
        typeof(room_id) = 'text' AND length(room_id) > 0
    ),
    updated_revision INTEGER NOT NULL CHECK (
        typeof(updated_revision) = 'integer' AND updated_revision >= 1
    ),
    intent_kind TEXT NULL CHECK (
        intent_kind IS NULL OR
        (typeof(intent_kind) = 'text'
         AND intent_kind IN ('recovery','hydration'))
    ),
    payload BLOB NOT NULL CHECK ( typeof(payload) = 'blob' AND length(payload) > 0 ),
    payload_sha256 BLOB NOT NULL CHECK (
        typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
    ),
    PRIMARY KEY (account_id, room_id))""",
    """CREATE INDEX NioIngestRoomAggregate_intent
    ON NioIngestRoomAggregate(account_id, intent_kind, room_id)""",
    """CREATE TABLE NioIngestWork (
    account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    work_id TEXT NOT NULL CHECK (
        typeof(work_id) = 'text' AND length(work_id) > 0
    ),
    kind TEXT NOT NULL CHECK (
        typeof(kind) = 'text' AND kind IN ('event','loss')
    ),
    status TEXT NOT NULL CHECK (typeof(status) = 'text' AND (
        (kind = 'event' AND status IN ('ready','held')) OR
        (kind = 'loss' AND status = 'ready')
    )),
    frame_id TEXT NOT NULL CHECK (
        typeof(frame_id) = 'text' AND length(frame_id) > 0
    ),
    room_id TEXT NULL CHECK (room_id IS NULL OR
        (typeof(room_id) = 'text' AND length(room_id) > 0)
    ),
    membership_epoch INTEGER NULL CHECK (membership_epoch IS NULL OR
        (typeof(membership_epoch) = 'integer' AND membership_epoch >= 0)
    ),
    room_sequence INTEGER NULL CHECK (room_sequence IS NULL OR
        (typeof(room_sequence) = 'integer' AND room_sequence >= 0)
    ),
    ready_revision INTEGER NULL CHECK (ready_revision IS NULL OR
        (typeof(ready_revision) = 'integer' AND ready_revision >= 1)
    ),
    ready_ordinal INTEGER NULL CHECK (ready_ordinal IS NULL OR
        (typeof(ready_ordinal) = 'integer' AND ready_ordinal >= 0)
    ),
    created_revision INTEGER NOT NULL CHECK (
        typeof(created_revision) = 'integer' AND created_revision >= 1
    ),
    payload BLOB NOT NULL CHECK ( typeof(payload) = 'blob' AND length(payload) > 0 AND length(payload) <= 1024 * 1024 ),
    payload_sha256 BLOB NOT NULL CHECK (
        typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
    ),
    PRIMARY KEY (account_id, work_id),
    UNIQUE (account_id, ready_revision, ready_ordinal),
    CHECK (
        (status = 'ready' AND ready_revision IS NOT NULL
                          AND ready_ordinal IS NOT NULL)
        OR
        (status <> 'ready' AND ready_revision IS NULL
                           AND ready_ordinal IS NULL)
    ),
    CHECK (
        (kind = 'event' AND (
            (status = 'held' AND room_id IS NOT NULL
                              AND membership_epoch IS NOT NULL
                              AND room_sequence IS NOT NULL)
            OR
            (status = 'ready' AND (
                (room_id IS NULL AND membership_epoch IS NULL
                                 AND room_sequence IS NULL)
                OR
                (room_id IS NOT NULL AND membership_epoch IS NOT NULL
                                     AND room_sequence IS NOT NULL)
            ))
        ))
        OR
        (kind = 'loss' AND room_id IS NOT NULL
                            AND membership_epoch IS NOT NULL
                            AND room_sequence IS NULL)
    ))""",
    """CREATE INDEX NioIngestWork_ready ON NioIngestWork(
    account_id, status, ready_revision, ready_ordinal, work_id)""",
    """CREATE INDEX NioIngestWork_held_release ON NioIngestWork(
    account_id, room_id, membership_epoch, status, room_sequence, work_id)""",
    """CREATE INDEX NioIngestWork_frame_kind ON NioIngestWork(
    account_id, frame_id, kind)""",
    """CREATE TABLE NioIngestDiagnosticScope (
    account_id TEXT PRIMARY KEY REFERENCES NioIngestMeta(account_id) CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    delivery_room_id TEXT NOT NULL CHECK (
        typeof(delivery_room_id) = 'text' AND length(delivery_room_id) > 0
    ),
    control_room_id TEXT NOT NULL CHECK (
        typeof(control_room_id) = 'text' AND length(control_room_id) > 0
    ),
    list_name TEXT NOT NULL CHECK (
        typeof(list_name) = 'text' AND length(list_name) > 0
    ),
    range_start INTEGER NOT NULL CHECK (
        typeof(range_start) = 'integer' AND range_start >= 0
    ),
    range_end INTEGER NOT NULL CHECK (
        typeof(range_end) = 'integer' AND range_end >= range_start
    ),
    request_config_sha256 BLOB NOT NULL CHECK (
        typeof(request_config_sha256) = 'blob'
        AND length(request_config_sha256) = 32
    ),
    created_revision INTEGER NOT NULL CHECK (
        typeof(created_revision) = 'integer' AND created_revision = 0
    ),
    payload BLOB NOT NULL CHECK (
        typeof(payload) = 'blob' AND length(payload) > 0
    ),
    payload_sha256 BLOB NOT NULL CHECK (
        typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
    ))""",
    """CREATE TABLE NioIngestListObservation (
    account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    observation_id TEXT NOT NULL CHECK (
        typeof(observation_id) = 'text' AND length(observation_id) > 0
    ),
    frame_id TEXT NOT NULL CHECK (
        typeof(frame_id) = 'text' AND length(frame_id) > 0
    ),
    source_epoch INTEGER NOT NULL CHECK (
        typeof(source_epoch) = 'integer' AND source_epoch >= 0
    ),
    request_id INTEGER NOT NULL CHECK (
        typeof(request_id) = 'integer' AND request_id >= 0
    ),
    list_name TEXT NOT NULL CHECK (
        typeof(list_name) = 'text' AND length(list_name) > 0
    ),
    transition TEXT NOT NULL CHECK (
        typeof(transition) = 'text'
        AND transition IN ('seed','enter','evict','reenter','replace')
    ),
    target_present INTEGER NOT NULL CHECK (
        typeof(target_present) = 'integer' AND target_present IN (0, 1)
    ),
    control_present INTEGER NOT NULL CHECK (
        typeof(control_present) = 'integer' AND control_present IN (0, 1)
    ),
    created_revision INTEGER NOT NULL CHECK (
        typeof(created_revision) = 'integer' AND created_revision >= 1
    ),
    payload BLOB NOT NULL CHECK (
        typeof(payload) = 'blob' AND length(payload) > 0
    ),
    payload_sha256 BLOB NOT NULL CHECK (
        typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
    ),
    PRIMARY KEY (account_id, observation_id),
    UNIQUE (account_id, frame_id, list_name))""",
    """CREATE TABLE NioIngestEventReceipt (
    account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    receipt_id TEXT NOT NULL CHECK (
        typeof(receipt_id) = 'text' AND length(receipt_id) > 0
    ),
    room_id TEXT NOT NULL CHECK (
        typeof(room_id) = 'text' AND length(room_id) > 0
    ),
    event_id TEXT NOT NULL CHECK (
        typeof(event_id) = 'text' AND length(event_id) > 0
    ),
    stable_event_sha256 BLOB NOT NULL CHECK (
        typeof(stable_event_sha256) = 'blob'
        AND length(stable_event_sha256) = 32
    ),
    first_frame_id TEXT NOT NULL CHECK (
        typeof(first_frame_id) = 'text' AND length(first_frame_id) > 0
    ),
    first_source_sha256 BLOB NOT NULL CHECK (
        typeof(first_source_sha256) = 'blob'
        AND length(first_source_sha256) = 32
    ),
    work_id TEXT NULL CHECK (
        work_id IS NULL OR (typeof(work_id) = 'text' AND length(work_id) > 0)
    ),
    fate TEXT NOT NULL CHECK (
        typeof(fate) = 'text'
        AND fate IN ('context','control','self_control','ready','outstanding','acknowledged')
    ),
    delivery_sequence INTEGER NULL CHECK (
        delivery_sequence IS NULL OR
        (typeof(delivery_sequence) = 'integer' AND delivery_sequence >= 0)
    ),
    delivery_batch_sha256 BLOB NULL CHECK (
        delivery_batch_sha256 IS NULL OR
        (typeof(delivery_batch_sha256) = 'blob'
         AND length(delivery_batch_sha256) = 32)
    ),
    acknowledged_revision INTEGER NULL CHECK (
        acknowledged_revision IS NULL OR
        (typeof(acknowledged_revision) = 'integer'
         AND acknowledged_revision >= 1)
    ),
    created_revision INTEGER NOT NULL CHECK (
        typeof(created_revision) = 'integer' AND created_revision >= 1
    ),
    updated_revision INTEGER NOT NULL CHECK (
        typeof(updated_revision) = 'integer'
        AND updated_revision >= created_revision
    ),
    payload BLOB NOT NULL CHECK (
        typeof(payload) = 'blob' AND length(payload) > 0
    ),
    payload_sha256 BLOB NOT NULL CHECK (
        typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
    ),
    PRIMARY KEY (account_id, receipt_id),
    UNIQUE (account_id, room_id, event_id),
    UNIQUE (account_id, work_id),
    CHECK (
        (fate IN ('context','control','self_control','ready')
         AND delivery_sequence IS NULL
         AND delivery_batch_sha256 IS NULL
         AND acknowledged_revision IS NULL)
     OR (fate = 'outstanding'
         AND delivery_sequence IS NOT NULL
         AND delivery_batch_sha256 IS NOT NULL
         AND acknowledged_revision IS NULL)
     OR (fate = 'acknowledged'
         AND delivery_sequence IS NOT NULL
         AND delivery_batch_sha256 IS NOT NULL
         AND acknowledged_revision IS NOT NULL)
    ))""",
    """CREATE TABLE NioIngestEventOccurrence (
    account_id TEXT NOT NULL CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    occurrence_id TEXT NOT NULL CHECK (
        typeof(occurrence_id) = 'text' AND length(occurrence_id) > 0
    ),
    receipt_id TEXT NOT NULL CHECK (
        typeof(receipt_id) = 'text' AND length(receipt_id) > 0
    ),
    frame_id TEXT NOT NULL CHECK (
        typeof(frame_id) = 'text' AND length(frame_id) > 0
    ),
    source_epoch INTEGER NOT NULL CHECK (
        typeof(source_epoch) = 'integer' AND source_epoch >= 0
    ),
    request_id INTEGER NOT NULL CHECK (
        typeof(request_id) = 'integer' AND request_id >= 0
    ),
    source_sha256 BLOB NOT NULL CHECK (
        typeof(source_sha256) = 'blob' AND length(source_sha256) = 32
    ),
    provenance TEXT NOT NULL CHECK (
        typeof(provenance) = 'text' AND provenance IN ('live','history')
    ),
    disposition TEXT NOT NULL CHECK (
        typeof(disposition) = 'text'
        AND disposition IN ('application','context','control','self_control','duplicate')
    ),
    created_revision INTEGER NOT NULL CHECK (
        typeof(created_revision) = 'integer' AND created_revision >= 1
    ),
    payload BLOB NOT NULL CHECK (
        typeof(payload) = 'blob' AND length(payload) > 0
    ),
    payload_sha256 BLOB NOT NULL CHECK (
        typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
    ),
    PRIMARY KEY (account_id, occurrence_id),
    UNIQUE (account_id, frame_id, receipt_id),
    FOREIGN KEY (account_id, receipt_id)
        REFERENCES NioIngestEventReceipt(account_id, receipt_id))""",
    """CREATE TABLE NioIngestSourceRotation (
    account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    successor_source_epoch INTEGER NOT NULL CHECK (
        typeof(successor_source_epoch) = 'integer'
        AND successor_source_epoch >= 1
    ),
    successor_request_id INTEGER NOT NULL CHECK (
        typeof(successor_request_id) = 'integer'
        AND successor_request_id = 0
    ),
    reason TEXT NOT NULL CHECK (
        typeof(reason) = 'text' AND reason IN ('reopen','unknown_pos')
    ),
    predecessor_source_epoch INTEGER NOT NULL CHECK (
        typeof(predecessor_source_epoch) = 'integer'
        AND predecessor_source_epoch >= 0
        AND predecessor_source_epoch < successor_source_epoch
    ),
    predecessor_request_id INTEGER NOT NULL CHECK (
        typeof(predecessor_request_id) = 'integer'
        AND predecessor_request_id >= 0
    ),
    predecessor_cursor_sha256 BLOB NOT NULL CHECK (
        typeof(predecessor_cursor_sha256) = 'blob'
        AND length(predecessor_cursor_sha256) = 32
    ),
    predecessor_connection_sha256 BLOB NOT NULL CHECK (
        typeof(predecessor_connection_sha256) = 'blob'
        AND length(predecessor_connection_sha256) = 32
    ),
    predecessor_pos_present INTEGER NOT NULL CHECK (
        typeof(predecessor_pos_present) = 'integer'
        AND predecessor_pos_present IN (0, 1)
    ),
    successor_cursor_sha256 BLOB NOT NULL CHECK (
        typeof(successor_cursor_sha256) = 'blob'
        AND length(successor_cursor_sha256) = 32
    ),
    successor_connection_sha256 BLOB NOT NULL CHECK (
        typeof(successor_connection_sha256) = 'blob'
        AND length(successor_connection_sha256) = 32
    ),
    successor_pos_present INTEGER NOT NULL CHECK (
        typeof(successor_pos_present) = 'integer'
        AND successor_pos_present = 0
    ),
    first_successor_request_sha256 BLOB NULL CHECK (
        first_successor_request_sha256 IS NULL OR
        (typeof(first_successor_request_sha256) = 'blob'
         AND length(first_successor_request_sha256) = 32)
    ),
    first_successor_frame_id TEXT NULL CHECK (
        first_successor_frame_id IS NULL OR
        (typeof(first_successor_frame_id) = 'text'
         AND length(first_successor_frame_id) > 0)
    ),
    first_successor_source_sha256 BLOB NULL CHECK (
        first_successor_source_sha256 IS NULL OR
        (typeof(first_successor_source_sha256) = 'blob'
         AND length(first_successor_source_sha256) = 32)
    ),
    created_revision INTEGER NOT NULL CHECK (
        typeof(created_revision) = 'integer' AND created_revision >= 1
    ),
    payload BLOB NOT NULL CHECK (
        typeof(payload) = 'blob' AND length(payload) > 0
    ),
    payload_sha256 BLOB NOT NULL CHECK (
        typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
    ),
    PRIMARY KEY (account_id, successor_source_epoch, successor_request_id),
    CHECK (
        (first_successor_request_sha256 IS NULL
         AND first_successor_frame_id IS NULL
         AND first_successor_source_sha256 IS NULL)
     OR (first_successor_request_sha256 IS NOT NULL
         AND first_successor_frame_id IS NOT NULL
         AND first_successor_source_sha256 IS NOT NULL)
    ))""",
)

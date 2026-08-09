SCHEMA_VERSION = 1

META_TABLE_SQL = """CREATE TABLE NioIngestMeta (
    account_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    stream_id TEXT NOT NULL,
    binding_operation_id TEXT NOT NULL,
    journal_generation TEXT,
    consumer_generation TEXT,
    consumer_first_sequence INTEGER,
    baseline_rooms_sha256 BLOB,
    consumer_attached_revision INTEGER,
    revision INTEGER NOT NULL,
    writer_epoch TEXT NOT NULL,
    lock_device INTEGER NOT NULL,
    lock_inode INTEGER NOT NULL,
    next_source_epoch INTEGER NOT NULL,
    next_ready_order INTEGER NOT NULL,
    next_batch_sequence INTEGER NOT NULL,
    last_acked_sequence INTEGER NOT NULL,
    last_acked_batch_id TEXT,
    last_acked_sha256 BLOB,
    created_at_ns INTEGER NOT NULL
)""".strip()

SCHEMA_SQL = (
    """CREATE TABLE NioIngestSourceState (
        account_id TEXT PRIMARY KEY REFERENCES NioIngestMeta(account_id),
        source_epoch INTEGER NOT NULL,
        transport_kind TEXT NOT NULL CHECK (transport_kind IN ('classic', 'sliding')),
        cursor_ciphertext BLOB NOT NULL,
        cursor_sha256 BLOB NOT NULL,
        next_request_id INTEGER NOT NULL,
        active INTEGER NOT NULL CHECK (active IN (0, 1))
    )""",
    """CREATE TABLE NioIngestRoomState (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        room_id TEXT NOT NULL,
        current_membership_epoch INTEGER NOT NULL,
        next_room_sequence INTEGER NOT NULL,
        hydration_status TEXT NOT NULL
            CHECK (hydration_status IN ('pending', 'ready', 'unavailable')),
        state_ciphertext BLOB,
        state_sha256 BLOB,
        updated_revision INTEGER NOT NULL,
        PRIMARY KEY (account_id, room_id)
    )""",
    """CREATE TABLE NioIngestRoomLane (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        room_id TEXT NOT NULL,
        membership_epoch INTEGER NOT NULL,
        lane_status TEXT NOT NULL CHECK (lane_status IN ('active', 'retiring')),
        held_record_count INTEGER NOT NULL,
        held_canonical_bytes INTEGER NOT NULL,
        release_phase TEXT NOT NULL CHECK (release_phase IN (
            'idle', 'recovering', 'releasing_recovered', 'releasing_terminal'
        )),
        release_loss_id TEXT,
        ready_order INTEGER,
        next_held_ordinal INTEGER NOT NULL,
        next_recovery_page INTEGER NOT NULL,
        successor_membership_epoch INTEGER,
        pending_lifecycle_ciphertext BLOB,
        pending_lifecycle_sha256 BLOB,
        updated_revision INTEGER NOT NULL,
        PRIMARY KEY (account_id, room_id, membership_epoch)
    )""",
    "CREATE INDEX NioIngestRoomLane_ready ON NioIngestRoomLane(account_id, ready_order)",
    """CREATE TABLE NioIngestReadyRecord (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        ready_order INTEGER NOT NULL,
        record_id TEXT NOT NULL,
        source_frame_id TEXT,
        room_id TEXT,
        membership_epoch INTEGER,
        room_sequence INTEGER,
        payload_ciphertext BLOB NOT NULL,
        payload_sha256 BLOB NOT NULL,
        canonical_bytes INTEGER NOT NULL,
        created_revision INTEGER NOT NULL,
        PRIMARY KEY (account_id, record_id),
        UNIQUE (account_id, ready_order)
    )""",
    """CREATE TABLE NioIngestLaneRecord (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        room_id TEXT NOT NULL,
        membership_epoch INTEGER NOT NULL,
        section TEXT NOT NULL CHECK (section IN ('recovered', 'held')),
        page_ordinal INTEGER NOT NULL,
        record_ordinal INTEGER NOT NULL,
        record_id TEXT NOT NULL,
        payload_ciphertext BLOB NOT NULL,
        payload_sha256 BLOB NOT NULL,
        canonical_bytes INTEGER NOT NULL,
        created_revision INTEGER NOT NULL,
        PRIMARY KEY (account_id, room_id, membership_epoch,
                     section, page_ordinal, record_ordinal),
        UNIQUE (account_id, record_id)
    )""",
    """CREATE TABLE NioIngestFrame (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        frame_id TEXT NOT NULL,
        source_epoch INTEGER NOT NULL,
        request_id INTEGER NOT NULL,
        payload_ciphertext BLOB NOT NULL,
        payload_sha256 BLOB NOT NULL,
        staged_revision INTEGER NOT NULL,
        PRIMARY KEY (account_id, frame_id)
    )""",
    """CREATE TABLE NioIngestCryptoReceipt (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        input_id TEXT NOT NULL,
        input_sha256 BLOB NOT NULL,
        result_ciphertext BLOB NOT NULL,
        result_sha256 BLOB NOT NULL,
        retention TEXT NOT NULL
            CHECK (retention IN ('until_frame_consumed', 'permanent_olm')),
        committed_at_ns INTEGER NOT NULL,
        PRIMARY KEY (account_id, input_id)
    )""",
    """CREATE TABLE NioIngestPendingDecryption (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        original_record_id TEXT NOT NULL,
        room_id TEXT NOT NULL,
        membership_epoch INTEGER NOT NULL,
        sender_key TEXT NOT NULL,
        session_id TEXT NOT NULL,
        source_ciphertext BLOB NOT NULL,
        source_sha256 BLOB NOT NULL,
        created_revision INTEGER NOT NULL,
        PRIMARY KEY (account_id, original_record_id)
    )""",
    """CREATE INDEX NioIngestPendingDecryption_room_session ON
        NioIngestPendingDecryption(account_id, room_id, sender_key, session_id)""",
    """CREATE TABLE NioIngestNetworkEffect (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        effect_id TEXT NOT NULL,
        effect_kind TEXT NOT NULL,
        source_epoch INTEGER NOT NULL,
        membership_epoch INTEGER,
        request_ciphertext BLOB NOT NULL,
        request_sha256 BLOB NOT NULL,
        created_revision INTEGER NOT NULL,
        PRIMARY KEY (account_id, effect_id)
    )""",
    """CREATE TABLE NioIngestCryptoEffect (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        effect_id TEXT NOT NULL,
        effect_kind TEXT NOT NULL,
        source_epoch INTEGER NOT NULL,
        membership_epoch INTEGER,
        transaction_id TEXT,
        request_ciphertext BLOB NOT NULL,
        request_sha256 BLOB NOT NULL,
        created_at_ns INTEGER NOT NULL,
        PRIMARY KEY (account_id, effect_id)
    )""",
    """CREATE TABLE NioIngestBatch (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        sequence INTEGER NOT NULL,
        batch_id TEXT NOT NULL,
        payload_ciphertext BLOB NOT NULL,
        payload_sha256 BLOB NOT NULL,
        created_revision INTEGER NOT NULL,
        acknowledged_revision INTEGER,
        PRIMARY KEY (account_id, sequence),
        UNIQUE (account_id, batch_id)
    )""",
    """CREATE TABLE NioIngestLoss (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        loss_id TEXT NOT NULL,
        room_id TEXT NOT NULL,
        membership_epoch INTEGER NOT NULL,
        reason TEXT NOT NULL,
        origin_ciphertext BLOB NOT NULL,
        origin_sha256 BLOB NOT NULL,
        boundary_ciphertext BLOB NOT NULL,
        boundary_sha256 BLOB NOT NULL,
        detail_ciphertext BLOB NOT NULL,
        detail_sha256 BLOB NOT NULL,
        loss_sha256 BLOB NOT NULL,
        detected_revision INTEGER NOT NULL,
        PRIMARY KEY (account_id, loss_id)
    )""",
    """CREATE TABLE NioIngestConsumerResetRoom (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        binding_operation_id TEXT NOT NULL,
        room_id TEXT NOT NULL,
        evidence_ciphertext BLOB NOT NULL,
        evidence_sha256 BLOB NOT NULL,
        PRIMARY KEY (account_id, binding_operation_id, room_id)
    )""",
)

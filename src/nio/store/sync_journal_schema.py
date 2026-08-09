SCHEMA_VERSION = 1

META_TABLE_SQL = """CREATE TABLE NioIngestMeta (
    account_id TEXT PRIMARY KEY, device_id TEXT NOT NULL, schema_version INTEGER NOT NULL,
    stream_id TEXT NOT NULL,
    transport_kind TEXT NOT NULL CHECK (transport_kind IN ('classic', 'sliding')),
    binding_operation_id TEXT NOT NULL,
    consumer_attach_status TEXT NOT NULL CHECK (consumer_attach_status IN ('unbound', 'attaching', 'attached')),
    consumer_attach_next_room_ordinal INTEGER NOT NULL CHECK (consumer_attach_next_room_ordinal >= 0),
    journal_generation TEXT,
    consumer_generation TEXT, consumer_first_sequence INTEGER, baseline_rooms_sha256 BLOB,
    consumer_attached_revision INTEGER, revision INTEGER NOT NULL, writer_epoch TEXT NOT NULL,
    next_source_epoch INTEGER NOT NULL,
    next_ready_order INTEGER NOT NULL, next_batch_sequence INTEGER NOT NULL,
    last_acked_sequence INTEGER NOT NULL, last_acked_batch_id TEXT,
    last_acked_sha256 BLOB, created_at_ns INTEGER NOT NULL,
    CHECK (
        (consumer_attach_status = 'unbound'
            AND consumer_attach_next_room_ordinal = 0
            AND revision = 0
            AND journal_generation IS NULL
            AND consumer_generation IS NULL
            AND consumer_first_sequence IS NULL
            AND baseline_rooms_sha256 IS NULL
            AND consumer_attached_revision IS NULL
            AND next_ready_order = 0
            AND next_batch_sequence = 1
            AND typeof(last_acked_sequence) = 'integer'
            AND last_acked_sequence = 0
            AND last_acked_batch_id IS NULL
            AND last_acked_sha256 IS NULL)
        OR
        (consumer_attach_status = 'attaching'
            AND journal_generation IS NOT NULL
            AND consumer_generation IS NOT NULL
            AND consumer_first_sequence IS NOT NULL
            AND consumer_first_sequence > 0
            AND baseline_rooms_sha256 IS NOT NULL
            AND typeof(baseline_rooms_sha256) = 'blob'
            AND length(baseline_rooms_sha256) = 32
            AND consumer_attached_revision IS NULL
            AND revision >= 1
            AND consumer_attach_next_room_ordinal > 0
            AND consumer_attach_next_room_ordinal = next_ready_order
            AND next_batch_sequence = consumer_first_sequence
            AND typeof(last_acked_sequence) = 'integer'
            AND last_acked_sequence = consumer_first_sequence - 1
            AND last_acked_batch_id IS NULL
            AND last_acked_sha256 IS NULL)
        OR
        (consumer_attach_status = 'attached'
            AND journal_generation IS NOT NULL
            AND consumer_generation IS NOT NULL
            AND consumer_first_sequence IS NOT NULL
            AND consumer_first_sequence > 0
            AND baseline_rooms_sha256 IS NOT NULL
            AND typeof(baseline_rooms_sha256) = 'blob'
            AND length(baseline_rooms_sha256) = 32
            AND consumer_attached_revision IS NOT NULL
            AND consumer_attached_revision >= 1
            AND consumer_attached_revision <= revision
            AND consumer_attach_next_room_ordinal <= next_ready_order
            AND next_batch_sequence >= consumer_first_sequence
            AND typeof(last_acked_sequence) = 'integer'
            AND last_acked_sequence >= consumer_first_sequence - 1
            AND last_acked_sequence < next_batch_sequence
            AND (
                (last_acked_sequence = consumer_first_sequence - 1
                    AND last_acked_batch_id IS NULL
                    AND last_acked_sha256 IS NULL)
                OR
                (last_acked_sequence >= consumer_first_sequence
                    AND typeof(last_acked_batch_id) = 'text'
                    AND typeof(last_acked_sha256) = 'blob'
                    AND length(last_acked_sha256) = 32)))
    )
)""".strip()

SCHEMA_SQL = (
    """CREATE TABLE NioIngestSourceState (
        account_id TEXT PRIMARY KEY REFERENCES NioIngestMeta(account_id), source_epoch INTEGER NOT NULL,
        cursor_ciphertext BLOB NOT NULL, cursor_sha256 BLOB NOT NULL,
        next_request_id INTEGER NOT NULL, active INTEGER NOT NULL CHECK (active IN (0, 1))
    )""",
    """CREATE TABLE NioIngestRoomState (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id), room_id TEXT NOT NULL,
        current_membership_epoch INTEGER NOT NULL, next_room_sequence INTEGER NOT NULL,
        hydration_status TEXT NOT NULL CHECK (hydration_status IN ('pending', 'ready', 'unavailable')),
        state_ciphertext BLOB NOT NULL, state_sha256 BLOB NOT NULL,
        updated_revision INTEGER NOT NULL,
        PRIMARY KEY (account_id, room_id)
    )""",
    """CREATE TABLE NioIngestRoomLane (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        room_id TEXT NOT NULL, membership_epoch INTEGER NOT NULL,
        lane_status TEXT NOT NULL CHECK (lane_status IN ('active', 'retiring')),
        held_record_count INTEGER NOT NULL, held_canonical_bytes INTEGER NOT NULL,
        release_phase TEXT NOT NULL CHECK (release_phase IN (
            'idle', 'recovering', 'releasing_recovered', 'releasing_terminal'
        )),
        ready_order INTEGER, next_held_ordinal INTEGER NOT NULL,
        successor_membership_epoch INTEGER,
        lane_state_ciphertext BLOB NOT NULL, lane_state_sha256 BLOB NOT NULL,
        updated_revision INTEGER NOT NULL,
        CHECK (held_record_count >= 0 AND held_canonical_bytes >= 0),
        CHECK (next_held_ordinal >= held_record_count),
        CHECK ((held_record_count = 0) = (held_canonical_bytes = 0)),
        CHECK ((release_phase IN (
            'releasing_recovered', 'releasing_terminal'
        )) = (ready_order IS NOT NULL)),
        CHECK (ready_order IS NULL OR ready_order >= 0),
        CHECK ((lane_status = 'retiring') = (
            successor_membership_epoch IS NOT NULL
        )),
        PRIMARY KEY (account_id, room_id, membership_epoch)
    )""",
    "CREATE INDEX NioIngestRoomLane_ready ON NioIngestRoomLane(account_id, ready_order)",
    """CREATE TABLE NioIngestReadyRecord (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        ready_order INTEGER NOT NULL, item_id TEXT NOT NULL,
        item_kind TEXT NOT NULL CHECK (item_kind IN ('event', 'loss')),
        source_frame_id TEXT, room_id TEXT, membership_epoch INTEGER,
        room_sequence INTEGER, payload_ciphertext BLOB NOT NULL,
        payload_sha256 BLOB NOT NULL, canonical_bytes INTEGER NOT NULL,
        created_revision INTEGER NOT NULL, PRIMARY KEY (account_id, item_id),
        CHECK (length(item_id) > 0), CHECK (canonical_bytes > 0),
        UNIQUE (account_id, ready_order)
    )""",
    """CREATE TABLE NioIngestLaneRecord (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        room_id TEXT NOT NULL, membership_epoch INTEGER NOT NULL,
        section TEXT NOT NULL CHECK (section IN ('loss', 'recovered', 'held')),
        page_ordinal INTEGER NOT NULL, record_ordinal INTEGER NOT NULL,
        item_id TEXT NOT NULL,
        item_kind TEXT NOT NULL CHECK (item_kind IN ('event', 'loss')),
        source_frame_id TEXT, source_effect_id TEXT,
        payload_ciphertext BLOB NOT NULL,
        payload_sha256 BLOB NOT NULL, canonical_bytes INTEGER NOT NULL,
        created_revision INTEGER NOT NULL,
        CHECK ((section = 'loss') = (item_kind = 'loss')),
        CHECK (length(item_id) > 0),
        CHECK (canonical_bytes > 0),
        CHECK (section != 'held' OR page_ordinal = 0),
        CHECK (source_frame_id IS NULL OR source_effect_id IS NULL),
        CHECK (section != 'held' OR (
            source_frame_id IS NOT NULL AND source_effect_id IS NULL
        )),
        CHECK (section != 'recovered' OR (
            source_effect_id IS NOT NULL AND source_frame_id IS NULL
        )),
        PRIMARY KEY (account_id, room_id, membership_epoch, section, page_ordinal, record_ordinal),
        UNIQUE (account_id, item_id)
    )""",
    """CREATE TABLE NioIngestFrame (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id), frame_id TEXT NOT NULL,
        source_epoch INTEGER NOT NULL, request_id INTEGER NOT NULL,
        payload_ciphertext BLOB NOT NULL, payload_sha256 BLOB NOT NULL,
        staged_revision INTEGER NOT NULL, PRIMARY KEY (account_id, frame_id)
    )""",
    """CREATE INDEX NioIngestFrame_drain ON NioIngestFrame(
        account_id, staged_revision, source_epoch, request_id, frame_id
    )""",
    """CREATE TABLE NioIngestCryptoReceipt (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id), input_id TEXT NOT NULL,
        input_sha256 BLOB NOT NULL, result_ciphertext BLOB NOT NULL, result_sha256 BLOB NOT NULL,
        retention TEXT NOT NULL CHECK (retention IN ('until_frame_consumed', 'permanent_olm')),
        committed_at_ns INTEGER NOT NULL, PRIMARY KEY (account_id, input_id)
    )""",
    """CREATE TABLE NioIngestPendingDecryption (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id), original_record_id TEXT NOT NULL,
        room_id TEXT NOT NULL, membership_epoch INTEGER NOT NULL, sender_key TEXT NOT NULL,
        session_id TEXT NOT NULL, source_ciphertext BLOB NOT NULL, source_sha256 BLOB NOT NULL,
        created_revision INTEGER NOT NULL, PRIMARY KEY (account_id, original_record_id)
    )""",
    """CREATE INDEX NioIngestPendingDecryption_room_session ON
        NioIngestPendingDecryption(account_id, room_id, sender_key, session_id)""",
    """CREATE TABLE NioIngestNetworkEffect (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        effect_id TEXT NOT NULL CHECK (
            typeof(effect_id) = 'text' AND length(effect_id) > 0
        ),
        effect_kind TEXT NOT NULL CHECK (
            effect_kind IN ('recovery', 'room_hydration', 'membership')
        ),
        room_id TEXT NOT NULL CHECK (
            typeof(room_id) = 'text' AND length(room_id) > 0
        ),
        membership_epoch INTEGER NOT NULL CHECK (
            typeof(membership_epoch) = 'integer' AND membership_epoch >= 0
        ),
        attempt_ordinal INTEGER NOT NULL CHECK (
            typeof(attempt_ordinal) = 'integer' AND attempt_ordinal >= 0
        ),
        membership_delivery_state TEXT CHECK (
            membership_delivery_state IN ('ready', 'dispatched_unconfirmed')
        ),
        prior_delivery_uncertain INTEGER CHECK (
            prior_delivery_uncertain IS NULL OR (
                typeof(prior_delivery_uncertain) = 'integer'
                AND prior_delivery_uncertain IN (0, 1)
            )
        ),
        request_ciphertext BLOB NOT NULL CHECK (typeof(request_ciphertext) = 'blob'),
        request_sha256 BLOB NOT NULL CHECK (
            typeof(request_sha256) = 'blob' AND length(request_sha256) = 32
        ),
        state_ciphertext BLOB NOT NULL CHECK (typeof(state_ciphertext) = 'blob'),
        state_sha256 BLOB NOT NULL CHECK (
            typeof(state_sha256) = 'blob' AND length(state_sha256) = 32
        ),
        created_revision INTEGER NOT NULL CHECK (
            typeof(created_revision) = 'integer' AND created_revision >= 0
        ),
        updated_revision INTEGER NOT NULL CHECK (
            typeof(updated_revision) = 'integer' AND updated_revision >= created_revision
        ),
        CHECK (
            (effect_kind = 'membership')
            = (membership_delivery_state IS NOT NULL)
        ),
        CHECK (
            (effect_kind = 'membership')
            = (prior_delivery_uncertain IS NOT NULL)
        ),
        CHECK (effect_kind = 'membership' OR attempt_ordinal = 0),
        CHECK (
            effect_kind != 'membership'
            OR membership_delivery_state = 'ready'
            OR (
                membership_delivery_state = 'dispatched_unconfirmed'
                AND attempt_ordinal >= 1
                AND (prior_delivery_uncertain = 0 OR attempt_ordinal >= 2)
            )
        ),
        CHECK (
            effect_kind != 'membership'
            OR attempt_ordinal != 0
            OR prior_delivery_uncertain = 0
        ),
        PRIMARY KEY (account_id, effect_id)
    )""",
    """CREATE UNIQUE INDEX NioIngestNetworkEffect_membership_room ON
        NioIngestNetworkEffect(account_id, room_id)
        WHERE effect_kind = 'membership'""",
    """CREATE INDEX NioIngestNetworkEffect_room ON
        NioIngestNetworkEffect(account_id, room_id)""",
    """CREATE INDEX NioIngestNetworkEffect_order ON
        NioIngestNetworkEffect(account_id, created_revision, effect_id)""",
    """CREATE INDEX NioIngestNetworkEffect_schedulable ON
        NioIngestNetworkEffect(account_id, created_revision, effect_id)
        WHERE effect_kind != 'membership'
        OR membership_delivery_state = 'ready'""",
    """CREATE INDEX NioIngestNetworkEffect_uncertain ON
        NioIngestNetworkEffect(account_id, effect_id)
        WHERE effect_kind = 'membership'
        AND membership_delivery_state = 'dispatched_unconfirmed'""",
    """CREATE TABLE NioIngestCryptoEffect (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        effect_id TEXT NOT NULL, effect_kind TEXT NOT NULL,
        source_epoch INTEGER NOT NULL, membership_epoch INTEGER,
        transaction_id TEXT, request_ciphertext BLOB NOT NULL,
        request_sha256 BLOB NOT NULL, created_at_ns INTEGER NOT NULL,
        PRIMARY KEY (account_id, effect_id)
    )""",
    """CREATE TABLE NioIngestBatch (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id),
        sequence INTEGER NOT NULL, batch_id TEXT NOT NULL,
        payload_ciphertext BLOB NOT NULL, payload_sha256 BLOB NOT NULL,
        created_revision INTEGER NOT NULL,
        PRIMARY KEY (account_id, sequence),
        UNIQUE (account_id, batch_id)
    )""",
    """CREATE TABLE NioIngestBatchItem (
        account_id TEXT NOT NULL, item_id TEXT NOT NULL,
        item_kind TEXT NOT NULL CHECK (item_kind IN ('event', 'loss')),
        sequence INTEGER NOT NULL, record_ordinal INTEGER NOT NULL,
        CHECK (length(item_id) > 0), CHECK (record_ordinal >= 0),
        PRIMARY KEY (account_id, item_id),
        UNIQUE (account_id, sequence, record_ordinal),
        FOREIGN KEY (account_id, sequence)
            REFERENCES NioIngestBatch(account_id, sequence) ON DELETE CASCADE
    )""",
)

SCHEMA_VERSION = 1

META_TABLE_SQL = """CREATE TABLE NioIngestMeta (
    account_id TEXT PRIMARY KEY CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    device_id TEXT NOT NULL CHECK (
        typeof(device_id) = 'text' AND length(device_id) > 0
    ),
    schema_version INTEGER NOT NULL CHECK (
        typeof(schema_version) = 'integer' AND schema_version = 1
    ),
    stream_id TEXT NOT NULL CHECK (
        typeof(stream_id) = 'text' AND length(stream_id) > 0
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
    ))"""

SCHEMA_SQL = (
    """CREATE TABLE NioIngestSourceState (
    account_id TEXT PRIMARY KEY REFERENCES NioIngestMeta(account_id) CHECK (
        typeof(account_id) = 'text' AND length(account_id) > 0
    ),
    source_epoch INTEGER NOT NULL CHECK (
        typeof(source_epoch) = 'integer' AND source_epoch >= 0
    ),
    cursor_ciphertext BLOB NOT NULL CHECK (
        typeof(cursor_ciphertext) = 'blob' AND length(cursor_ciphertext) >= 29
    ),
    cursor_sha256 BLOB NOT NULL CHECK (
        typeof(cursor_sha256) = 'blob' AND length(cursor_sha256) = 32
    ),
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
    payload_ciphertext BLOB NOT NULL CHECK (
        typeof(payload_ciphertext) = 'blob' AND length(payload_ciphertext) >= 29
    ),
    payload_sha256 BLOB NOT NULL CHECK (
        typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
    ),
    PRIMARY KEY (account_id, frame_id))""",
    """CREATE INDEX NioIngestFrame_drain ON NioIngestFrame(
    account_id, staged_revision, source_epoch, request_id, frame_id)""",
)

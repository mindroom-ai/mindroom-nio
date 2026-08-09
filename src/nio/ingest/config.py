from dataclasses import dataclass, fields

from .model import ConsumerBinding

MAX_RECORDS_PER_BATCH = 256
MAX_BYTES_PER_BATCH = 2 * 1024 * 1024
MAX_RECORD_BYTES = 1024 * 1024


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


@dataclass(frozen=True, slots=True)
class ClassicSourceConfig:
    timeout_ms: int
    filter_json: bytes
    full_state_on_cold_start: bool = True

    def __post_init__(self) -> None:
        _require_exact(self.timeout_ms, int, "timeout_ms")
        _require_exact(self.filter_json, bytes, "filter_json")
        _require_exact(
            self.full_state_on_cold_start,
            bool,
            "full_state_on_cold_start",
        )
        if self.timeout_ms < 0:
            raise ValueError("timeout_ms must be nonnegative")


@dataclass(frozen=True, slots=True)
class SlidingSourceConfig:
    timeout_ms: int
    connection_name: str
    lists_json: bytes
    room_subscriptions_json: bytes
    extensions_json: bytes

    def __post_init__(self) -> None:
        _require_exact(self.timeout_ms, int, "timeout_ms")
        _require_exact(self.connection_name, str, "connection_name")
        _require_exact(self.lists_json, bytes, "lists_json")
        _require_exact(
            self.room_subscriptions_json,
            bytes,
            "room_subscriptions_json",
        )
        _require_exact(self.extensions_json, bytes, "extensions_json")
        if self.timeout_ms < 0:
            raise ValueError("timeout_ms must be nonnegative")
        if not self.connection_name:
            raise ValueError("connection_name must not be empty")


@dataclass(frozen=True, slots=True)
class IngestionConfig:
    source: ClassicSourceConfig | SlidingSourceConfig
    consumer: ConsumerBinding
    max_staged_frames: int = 2
    max_unacknowledged_batches: int = 8
    max_records_per_batch: int = MAX_RECORDS_PER_BATCH
    max_bytes_per_batch: int = MAX_BYTES_PER_BATCH
    max_record_bytes: int = MAX_RECORD_BYTES
    max_crypto_inputs_per_commit: int = 32
    max_crypto_input_bytes_per_commit: int = 1024 * 1024
    sqlite_busy_timeout_ms: int = 2_000
    sqlite_write_retry_limit: int = 2
    max_concurrent_recovery_rooms: int = 8
    max_concurrent_room_hydrations: int = 8
    sliding_bootstrap_range_size: int = 100
    max_recovery_events_per_room: int = 10_000
    max_held_events_per_room: int = 10_000
    max_held_bytes_per_room: int = 32 * 1024 * 1024

    def __post_init__(self) -> None:
        if type(self.source) not in (ClassicSourceConfig, SlidingSourceConfig):
            raise TypeError("source must be ClassicSourceConfig or SlidingSourceConfig")
        _require_exact(self.consumer, ConsumerBinding, "consumer")

        for config_field in fields(self):
            if not config_field.name.startswith("max_") and config_field.name not in {
                "sqlite_busy_timeout_ms",
                "sqlite_write_retry_limit",
                "sliding_bootstrap_range_size",
            }:
                continue
            value = getattr(self, config_field.name)
            _require_exact(value, int, config_field.name)
            if value <= 0:
                raise ValueError(f"{config_field.name} must be positive")

        ceilings = {
            "max_records_per_batch": MAX_RECORDS_PER_BATCH,
            "max_bytes_per_batch": MAX_BYTES_PER_BATCH,
            "max_record_bytes": MAX_RECORD_BYTES,
        }
        for field_name, ceiling in ceilings.items():
            if getattr(self, field_name) > ceiling:
                raise ValueError(f"{field_name} exceeds immutable ceiling {ceiling}")

from dataclasses import dataclass

from .model import TransportKind


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


@dataclass(frozen=True, slots=True)
class ClassicSourceConfig:
    timeout_ms: int
    filter_json: bytes

    def __post_init__(self) -> None:
        _require_exact(self.timeout_ms, int, "timeout_ms")
        _require_exact(self.filter_json, bytes, "filter_json")
        if self.timeout_ms < 0:
            raise ValueError("timeout_ms must be nonnegative")


@dataclass(frozen=True, slots=True)
class SlidingSourceConfig:
    timeout_ms: int
    connection_name: str
    lists_json: bytes
    room_subscriptions_json: bytes
    extensions_json: bytes
    all_rooms_page_size: int = 100

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
        _require_exact(
            self.all_rooms_page_size,
            int,
            "all_rooms_page_size",
        )
        if self.timeout_ms < 0:
            raise ValueError("timeout_ms must be nonnegative")
        if not self.connection_name:
            raise ValueError("connection_name must not be empty")
        if self.all_rooms_page_size <= 0:
            raise ValueError("all_rooms_page_size must be positive")


SourceConfig = ClassicSourceConfig | SlidingSourceConfig


def source_transport(source: object) -> TransportKind:
    if type(source) is ClassicSourceConfig:
        return TransportKind.CLASSIC
    if type(source) is SlidingSourceConfig:
        return TransportKind.SLIDING
    raise TypeError("source must be ClassicSourceConfig or SlidingSourceConfig")


@dataclass(frozen=True, slots=True)
class IngestionConfig:
    source: SourceConfig
    max_staged_frames: int = 2
    sqlite_busy_timeout_ms: int = 2_000

    def __post_init__(self) -> None:
        source_transport(self.source)
        _require_exact(self.max_staged_frames, int, "max_staged_frames")
        _require_exact(
            self.sqlite_busy_timeout_ms,
            int,
            "sqlite_busy_timeout_ms",
        )
        if not 1 <= self.max_staged_frames <= 256:
            raise ValueError("max_staged_frames must be between 1 and 256")
        if self.sqlite_busy_timeout_ms <= 0:
            raise ValueError("sqlite_busy_timeout_ms must be positive")

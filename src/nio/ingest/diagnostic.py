from dataclasses import dataclass

from .source import _require_matrix_room_id


def _require_nonempty_text(value: object, field_name: str) -> None:
    if type(value) is not str or not value:
        raise TypeError(f"{field_name} must be a nonempty str")


@dataclass(frozen=True, slots=True)
class DiagnosticIngestionScope:
    account_id: str
    delivery_room_id: str
    control_room_id: str
    list_name: str
    range_start: int
    range_end: int
    request_config_sha256: bytes

    def __post_init__(self) -> None:
        _require_nonempty_text(self.account_id, "account_id")
        _require_matrix_room_id(self.delivery_room_id, "delivery_room_id")
        _require_matrix_room_id(self.control_room_id, "control_room_id")
        if self.delivery_room_id == self.control_room_id:
            raise ValueError("delivery_room_id and control_room_id must be distinct")
        if self.list_name != "probe":
            raise ValueError("list_name must be probe")
        if type(self.range_start) is not int or self.range_start != 0:
            raise ValueError("range_start must be exactly 0")
        if type(self.range_end) is not int or self.range_end != 0:
            raise ValueError("range_end must be exactly 0")
        if (
            type(self.request_config_sha256) is not bytes
            or len(self.request_config_sha256) != 32
        ):
            raise TypeError("request_config_sha256 must be exactly 32 bytes")

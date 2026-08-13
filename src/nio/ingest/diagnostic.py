from dataclasses import dataclass


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
        _require_nonempty_text(self.delivery_room_id, "delivery_room_id")
        _require_nonempty_text(self.control_room_id, "control_room_id")
        _require_nonempty_text(self.list_name, "list_name")
        if type(self.range_start) is not int or self.range_start < 0:
            raise TypeError("range_start must be a nonnegative int")
        if type(self.range_end) is not int or self.range_end < self.range_start:
            raise TypeError("range_end must be an int not less than range_start")
        if (
            type(self.request_config_sha256) is not bytes
            or len(self.request_config_sha256) != 32
        ):
            raise TypeError("request_config_sha256 must be exactly 32 bytes")

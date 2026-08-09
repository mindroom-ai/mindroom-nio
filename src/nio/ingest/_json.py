"""Cycle-neutral strict Matrix JSON loading and canonical encoding."""

import json
from collections.abc import Callable
from typing import Any

MATRIX_CANONICAL_INTEGER_MAX = (1 << 53) - 1
_MAX_JSON_CONTAINER_DEPTH = 257


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _reject_json_float(value: str) -> None:
    raise ValueError(f"JSON floats are not canonical: {value}")


def _parse_json_integer(value: str) -> int:
    parsed = int(value)
    if not -MATRIX_CANONICAL_INTEGER_MAX <= parsed <= MATRIX_CANONICAL_INTEGER_MAX:
        raise ValueError("JSON integer exceeds the Matrix canonical range")
    return parsed


def _parse_internal_json_integer(value: str) -> int:
    return int(value)


def _object_from_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _validate_json_nesting(text: str, field_name: str) -> None:
    depth = 0
    in_string = False
    escaped = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > _MAX_JSON_CONTAINER_DEPTH:
                raise ValueError(f"{field_name} exceeds the JSON nesting limit")
        elif character in "]}":
            depth -= 1


def _load_json(
    data: bytes,
    field_name: str,
    *,
    parse_integer: Callable[[str], int],
) -> Any:
    if type(data) is not bytes:
        raise TypeError(f"{field_name} must be bytes")
    try:
        text = data.decode("utf-8")
        _validate_json_nesting(text, field_name)
        return json.loads(
            text,
            parse_constant=_reject_json_constant,
            parse_float=_reject_json_float,
            parse_int=parse_integer,
            object_pairs_hook=_object_from_pairs,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError(f"{field_name} must contain valid UTF-8 JSON") from error


def load_json(data: bytes, field_name: str) -> Any:
    return _load_json(
        data,
        field_name,
        parse_integer=_parse_json_integer,
    )


def load_internal_json(data: bytes, field_name: str) -> Any:
    """Load canonical internal JSON without Matrix's wire-integer ceiling."""

    return _load_json(
        data,
        field_name,
        parse_integer=_parse_internal_json_integer,
    )


def canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except RecursionError as error:
        raise ValueError("JSON value exceeds the canonical nesting limit") from error

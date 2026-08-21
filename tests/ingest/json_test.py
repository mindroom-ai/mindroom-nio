import sys

import pytest

from nio.ingest import _json


def test_large_flat_string_does_not_trigger_bytewise_depth_validation() -> None:
    payload = b'{"content":"' + (b"x" * (1024 * 1024)) + b'"}'
    validator_lines = 0

    def trace(frame, event, arg):
        nonlocal validator_lines
        if frame.f_code is _json._validate_json_nesting.__code__ and event == "line":
            validator_lines += 1
        return trace

    previous_trace = sys.gettrace()
    sys.settrace(trace)
    try:
        assert _json.load_json(payload, "payload") == {"content": "x" * (1024 * 1024)}
    finally:
        sys.settrace(previous_trace)

    assert validator_lines <= 20


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        pytest.param(b"null", None, id="null"),
        pytest.param(b'"scalar"', "scalar", id="scalar"),
        pytest.param(b"[1]", [1], id="list"),
        pytest.param(b'{"value":1}', {"value": 1}, id="dict"),
    ],
)
def test_load_json_preserves_top_level_json_values(
    payload: bytes, expected: object
) -> None:
    assert _json.load_json(payload, "payload") == expected


def test_load_json_ignores_delimiters_and_escapes_inside_strings() -> None:
    assert _json.load_json(rb'{"content":"[{}]\""}', "payload") == {"content": '[{}]"'}


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b'{"duplicate":1,"duplicate":2}', id="duplicate-key"),
        pytest.param(b"1.0", id="float"),
        pytest.param(b"NaN", id="nan"),
        pytest.param(b"Infinity", id="infinity"),
        pytest.param(b"-Infinity", id="negative-infinity"),
        pytest.param(b"\xff", id="invalid-utf8"),
        pytest.param(b'{"missing":', id="invalid-json"),
        pytest.param(b"9007199254740992", id="out-of-range-integer"),
    ],
)
def test_load_json_rejects_noncanonical_or_invalid_values(payload: bytes) -> None:
    with pytest.raises(ValueError):
        _json.load_json(payload, "payload")


def test_load_internal_json_retains_wider_integer_behavior() -> None:
    assert _json.load_internal_json(b"9007199254740992", "payload") == 9007199254740992


def test_load_json_fails_closed_for_extremely_deep_input() -> None:
    with pytest.raises(ValueError):
        _json.load_json((b"[" * 50_000) + (b"]" * 50_000), "payload")

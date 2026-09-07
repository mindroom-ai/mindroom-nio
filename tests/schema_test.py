import pytest
from jsonschema import ValidationError

from nio.schemas import Checker, validate_json


def test_string_format_ignores_non_string_values_until_type_validation():
    assert Checker.check(42, "user_id") is None

    with pytest.raises(ValidationError):
        validate_json(42, {"type": "string", "format": "user_id"})

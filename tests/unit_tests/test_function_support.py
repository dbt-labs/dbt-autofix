import json
import pytest
from unittest.mock import patch, MagicMock
from dbt_autofix.function_support import (
    load_function_support,
    get_unsupported_functions,
    BUNDLED_DATA_PATH,
)


SAMPLE_DATA = {
    "_meta": {"platform": "snowflake", "count": 3},
    "functions": [
        {"name": "CONCAT", "fusion_typecheck": True},
        {"name": "AGG", "fusion_typecheck": False},
        {"name": "ANY_VALUE", "fusion_typecheck": True},
    ],
}


def test_load_function_support_uses_bundled_when_offline():
    with patch("httpx.get", side_effect=Exception("network error")):
        data = load_function_support(adapter="snowflake")
    assert "_meta" in data
    assert "functions" in data


def test_load_function_support_uses_remote_when_online():
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_DATA
    mock_response.raise_for_status = MagicMock()
    with patch("httpx.get", return_value=mock_response) as mock_get:
        data = load_function_support(adapter="snowflake")
    assert data == SAMPLE_DATA
    mock_get.assert_called_once()


def test_get_unsupported_functions_returns_uppercase_set():
    unsupported = get_unsupported_functions(SAMPLE_DATA)
    assert unsupported == {"AGG"}


def test_get_unsupported_functions_empty_when_all_supported():
    all_supported = {
        "_meta": {},
        "functions": [{"name": "CONCAT", "fusion_typecheck": True}],
    }
    assert get_unsupported_functions(all_supported) == set()


def test_bundled_data_path_exists():
    assert BUNDLED_DATA_PATH.exists(), f"Bundled data file missing: {BUNDLED_DATA_PATH}"


def test_bundled_data_is_valid_json():
    with open(BUNDLED_DATA_PATH) as f:
        data = json.load(f)
    assert "functions" in data
    assert len(data["functions"]) > 0

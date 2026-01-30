"""Tests for dict config subkey validation - ensuring +prefixed subkeys are moved to +meta"""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from dbt_autofix.refactors.changesets.dbt_project_yml import rec_check_yaml_path
from dbt_autofix.retrieve_schemas import SchemaSpecs


@pytest.fixture(scope="module")
def real_schema():
    """Provides REAL dbt Fusion schema specs."""
    return SchemaSpecs()


@pytest.fixture
def models_node_fields(real_schema):
    """Provides the real node fields for models from dbt Fusion schema."""
    return real_schema.dbtproject_specs_per_node_type["models"]


@pytest.fixture
def temp_path():
    """Provides a temp directory for path validation"""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def test_persist_docs_with_plus_prefixed_subkeys(models_node_fields, temp_path, real_schema):
    """Test case from example.yml: +persist_docs with incorrectly +prefixed subkeys"""
    input_dict = {
        "+persist_docs": {
            "+columns": True,  # ❌ Wrong: subkey has + prefix
            "+relation": True,  # ❌ Wrong: subkey has + prefix
        }
    }

    expected_output = {
        "+persist_docs": {},  # Empty after moving invalid subkeys
        "+meta": {
            "+columns": True,  # Moved here with + prefix preserved
            "+relation": True,  # Moved here with + prefix preserved
        },
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields, None, real_schema, "models")

    assert result == expected_output
    assert len(logs) == 2
    assert any("+columns" in log and "meta" in log for log in logs)
    assert any("+relation" in log and "meta" in log for log in logs)


def test_persist_docs_with_correct_subkeys(models_node_fields, temp_path, real_schema):
    """Test case: +persist_docs with correctly named subkeys (no + prefix)"""
    input_dict = {
        "+persist_docs": {
            "columns": True,  # ✅ Correct: no + prefix
            "relation": True,  # ✅ Correct: no + prefix
        }
    }

    expected_output = {
        "+persist_docs": {
            "columns": True,  # Kept as-is
            "relation": True,  # Kept as-is
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields, None, real_schema, "models")

    assert result == expected_output
    assert len(logs) == 0  # No changes needed


def test_persist_docs_with_mixed_subkeys(models_node_fields, temp_path, real_schema):
    """Test case: +persist_docs with mix of correct and incorrect subkeys"""
    input_dict = {
        "+persist_docs": {
            "columns": True,  # ✅ Correct: no + prefix - keep
            "+relation": True,  # ❌ Wrong: has + prefix - move to meta
            "+invalid": False,  # ❌ Wrong: has + prefix AND not a valid property - move to meta
        }
    }

    expected_output = {
        "+persist_docs": {
            "columns": True  # Kept as-is
        },
        "+meta": {
            "+relation": True,  # Moved here
            "+invalid": False,  # Moved here
        },
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields, None, real_schema, "models")

    assert result == expected_output
    assert len(logs) == 2
    assert any("+relation" in log and "meta" in log for log in logs)
    assert any("+invalid" in log and "meta" in log for log in logs)


def test_labels_with_plus_prefixed_subkeys(models_node_fields, temp_path, real_schema):
    """Test case: +labels (accepts any key-value pairs) should keep all subkeys"""
    input_dict = {
        "+labels": {
            "+env": "prod",  # Even with + prefix, keep as-is
            "team": "analytics",  # Keep as-is
            "+custom_label": "value",  # Even with + prefix, keep as-is
        }
    }

    expected_output = {
        "+labels": {
            "+env": "prod",  # Kept in labels (accepts any key-value)
            "team": "analytics",  # Kept in labels
            "+custom_label": "value",  # Kept in labels
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields, None, real_schema, "models")

    assert result == expected_output
    assert len(logs) == 0  # No changes - labels accepts any key-value pairs


def test_grants_with_plus_prefixed_subkeys(models_node_fields, temp_path, real_schema):
    """Test case: +grants (BTreeMap<String, StringOrArrayOfStrings>) accepts any key-value pairs"""
    input_dict = {
        "+grants": {
            "+select": ["role1", "role2"],  # ✅ Keep (grants accepts any key)
            "usage": ["role3"],  # ✅ Keep
            "+custom_grant": ["role4"],  # ✅ Keep (grants accepts any key)
        }
    }

    expected_output = {
        "+grants": {
            "+select": ["role1", "role2"],  # Kept as-is
            "usage": ["role3"],  # Kept as-is
            "+custom_grant": ["role4"],  # Kept as-is
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields, None, real_schema, "models")

    assert result == expected_output
    assert len(logs) == 0  # No changes - grants accepts any key-value pairs


def test_multiple_dict_configs_with_various_subkeys(models_node_fields, temp_path, real_schema):
    """Test case: Multiple dict configs with different behaviors"""
    input_dict = {
        "+persist_docs": {
            "+columns": True,  # ❌ Move to meta (wrong prefix)
            "relation": False,  # ✅ Keep (correct)
        },
        "+labels": {
            "+env": "prod",  # ✅ Keep (labels accepts any key-value)
            "team": "data",  # ✅ Keep
        },
        "+grants": {
            "+select": ["role1"],  # ✅ Keep (grants accepts any key-value)
            "usage": ["role2"],  # ✅ Keep
        },
    }

    expected_output = {
        "+persist_docs": {
            "relation": False  # Only this stays
        },
        "+labels": {
            "+env": "prod",  # Everything stays in labels
            "team": "data",
        },
        "+grants": {
            "+select": ["role1"],  # Everything stays in grants
            "usage": ["role2"],
        },
        "+meta": {
            "+columns": True  # Only from persist_docs
        },
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields, None, real_schema, "models")

    assert result == expected_output
    assert len(logs) == 1  # Only one move to meta
    assert any("+columns" in log and "meta" in log for log in logs)


def test_nested_logical_grouping_with_dict_configs(models_node_fields, temp_path, real_schema):
    """Test case: Dict configs inside logical groupings"""
    input_dict = {
        "my_models": {
            "+persist_docs": {
                "+columns": True,  # ❌ Move to meta at this level
                "relation": True,  # ✅ Keep
            },
            "+materialized": "table",
        }
    }

    expected_output = {
        "my_models": {
            "+persist_docs": {
                "relation": True  # Only this stays
            },
            "+materialized": "table",
            "+meta": {
                "+columns": True  # Moved to meta at the my_models level
            },
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields, None, real_schema, "models")

    assert result == expected_output
    assert len(logs) == 1  # Only one for moving +columns to meta
    assert any("+columns" in log and "meta" in log for log in logs)


def test_empty_dict_after_moving_all_subkeys(models_node_fields, temp_path, real_schema):
    """Test case: What happens when all subkeys are moved out"""
    input_dict = {
        "+persist_docs": {
            "+columns": True,  # ❌ Move to meta
            "+relation": True,  # ❌ Move to meta
        }
    }

    expected_output = {
        "+persist_docs": {},  # Empty dict remains
        "+meta": {"+columns": True, "+relation": True},
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields, None, real_schema, "models")

    assert result == expected_output
    assert len(logs) == 2


def test_dict_config_with_invalid_non_prefixed_keys(models_node_fields, temp_path, real_schema):
    """Test case: Dict config with keys that don't match schema (without + prefix)"""
    input_dict = {
        "+persist_docs": {
            "columns": True,  # ✅ Valid key
            "invalid_key": True,  # ❌ Not in schema for persist_docs
        }
    }

    expected_output = {
        "+persist_docs": {
            "columns": True  # Keep valid key
        },
        "+meta": {
            "invalid_key": True  # Move invalid key
        },
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields, None, real_schema, "models")

    assert result == expected_output
    assert len(logs) == 1
    assert "invalid_key" in logs[0] and "meta" in logs[0]


def test_meta_with_any_subkeys(models_node_fields, temp_path, real_schema):
    """Test case: +meta accepts any key-value pairs (including +prefixed)"""
    input_dict = {"+meta": {"+custom_key": "value1", "another_key": "value2", "+nested": {"key": "value"}}}

    expected_output = {
        "+meta": {
            "+custom_key": "value1",  # Kept as-is
            "another_key": "value2",  # Kept as-is
            "+nested": {"key": "value"},  # Kept as-is
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields, None, real_schema, "models")

    assert result == expected_output
    assert len(logs) == 0  # No changes - meta accepts anything

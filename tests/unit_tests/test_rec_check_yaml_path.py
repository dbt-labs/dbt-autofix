"""Simple input/output tests for rec_check_yaml_path function.

Each test shows:
- Input: what goes into the function
- Expected Output: what we expect to get back
- Why: explanation of the transformation

🎯 KEY FEATURE: These tests use the REAL dbt Fusion schema, not mocks!

This means:
✅ Tests are always accurate to the actual Fusion schema
✅ Tests will catch if Fusion schema changes
✅ No need to keep mocks in sync with reality
✅ You can trust that these tests reflect real-world behavior

The schema is fetched once per test run (module scope fixture) for efficiency.

🔧 LOGIC: Simple validation - no spelling correction!
- If config is IN schema → keep it
- If config is NOT in schema → move to +meta
- No attempts to "fix" typos or rename configs
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from dbt_autofix.refactors.changesets.dbt_project_yml import rec_check_yaml_path
from dbt_autofix.retrieve_schemas import SchemaSpecs


@pytest.fixture(scope="module")
def real_schema():
    """Provides REAL dbt Fusion schema specs.
    This fetches the actual schema from dbt Fusion, so tests are accurate!
    """
    return SchemaSpecs()


@pytest.fixture
def models_node_fields(real_schema):
    """Provides the real node fields for models from dbt Fusion schema.
    This tells us what config keys are actually valid for models in dbt_project.yml.
    """
    return real_schema.dbtproject_specs_per_node_type["models"]


@pytest.fixture
def temp_path():
    """Provides a temp directory for path validation"""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# =============================================================================
# SCENARIO 1: Correct syntax - no changes needed
# =============================================================================


def test_correct_hook_syntax_unchanged(models_node_fields, temp_path):
    """INPUT:  +post-hook with hyphen (correct for dbt_project.yml)
    OUTPUT: Same, no changes
    WHY:    This is the correct syntax according to the REAL Fusion schema
    """
    input_dict = {"+post-hook": ["select 1", "select 2"], "+pre-hook": "select 0"}

    expected_output = {"+post-hook": ["select 1", "select 2"], "+pre-hook": "select 0"}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0  # No changes = no logs


def test_correct_configs_unchanged(models_node_fields, temp_path):
    """INPUT:  Valid configs with + prefix
    OUTPUT: Same, no changes
    WHY:    All configs are recognized and properly formatted
    """
    input_dict = {"+materialized": "table", "+tags": ["tag1", "tag2"], "+enabled": True}

    expected_output = {"+materialized": "table", "+tags": ["tag1", "tag2"], "+enabled": True}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


# =============================================================================
# SCENARIO 2: Schema does not support the key - moved to meta
# =============================================================================


def test_unsupported_hook_moved_to_meta(models_node_fields, temp_path):
    """INPUT:  +post_hook with underscore (NOT in schema)
    OUTPUT: Moved to +meta.post_hook
    WHY:    Schema doesn't recognize post_hook, so it's treated as custom config
    """
    input_dict = {"+post_hook": "grant select", "+pre_hook": "begin"}

    expected_output = {
        "+meta": {
            "post_hook": "grant select",  # Moved to meta
            "pre_hook": "begin",  # Moved to meta
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 2
    assert "post_hook" in logs[0] and "meta" in logs[0]
    assert "pre_hook" in logs[1] and "meta" in logs[1]


def test_mixed_valid_and_invalid_configs(models_node_fields, temp_path):
    """INPUT:  Mix of valid (+post-hook) and invalid (+post_hook) configs
    OUTPUT: Valid kept, invalid moved to meta
    WHY:    Only schema-supported configs stay at top level
    """
    input_dict = {
        "+post-hook": "select 1",  # Valid, keep
        "+post_hook": "select 2",  # Invalid (underscore), move to meta
        "+materialized": "table",  # Valid, keep
    }

    expected_output = {
        "+post-hook": "select 1",
        "+materialized": "table",
        "+meta": {
            "post_hook": "select 2"  # Moved
        },
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 1  # Only one move needed
    assert "post_hook" in logs[0] and "meta" in logs[0]


# =============================================================================
# SCENARIO 3: Missing + prefix
# =============================================================================


def test_valid_config_missing_plus(models_node_fields, temp_path):
    """INPUT:  materialized without + prefix
    OUTPUT: +materialized with + prefix added
    WHY:    In dbt_project.yml, config keys need + prefix
    """
    input_dict = {"materialized": "table", "tags": ["tag1"]}

    expected_output = {"+materialized": "table", "+tags": ["tag1"]}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 2
    assert any("materialized" in log for log in logs)
    assert any("tags" in log for log in logs)


def test_hook_missing_plus(models_node_fields, temp_path):
    """INPUT:  post-hook without + prefix (hyphen is correct, just missing +)
    OUTPUT: +post-hook with + prefix added
    WHY:    Config keys in dbt_project.yml need + prefix
    """
    input_dict = {"post-hook": "select 1"}

    expected_output = {"+post-hook": "select 1"}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 1
    assert "post-hook" in logs[0]


# =============================================================================
# SCENARIO 4: Unknown/custom configs
# =============================================================================


def test_unknown_config_with_plus_moved_to_meta(models_node_fields, temp_path):
    """INPUT:  +custom_config (not in schema)
    OUTPUT: Moved to +meta.custom_config
    WHY:    Unknown configs must go under meta to avoid errors
    """
    input_dict = {"+custom_config": "my_value", "+materialized": "table"}

    expected_output = {"+materialized": "table", "+meta": {"custom_config": "my_value"}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 1
    assert "custom_config" in logs[0]
    assert "meta" in logs[0]


def test_unknown_config_without_plus_moved_to_meta(models_node_fields, temp_path):
    """INPUT:  custom_field without + (not in schema)
    OUTPUT: Moved to +meta.custom_field
    WHY:    Unknown configs go to meta, even if they don't have +
    """
    input_dict = {"custom_field": "value", "materialized": "table"}

    expected_output = {"+materialized": "table", "+meta": {"custom_field": "value"}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 2  # One for adding +, one for moving to meta


def test_multiple_unknowns_merged_into_meta(models_node_fields, temp_path):
    """INPUT:  Multiple unknown configs
    OUTPUT: All merged into single +meta block
    WHY:    Keep all custom configs organized under meta
    """
    input_dict = {"+unknown1": "val1", "+unknown2": "val2", "+unknown3": "val3", "+materialized": "table"}

    expected_output = {"+materialized": "table", "+meta": {"unknown1": "val1", "unknown2": "val2", "unknown3": "val3"}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 3
    assert all("meta" in log for log in logs)


# =============================================================================
# SCENARIO 5: Complex real-world examples
# =============================================================================


def test_real_world_example_user_config(models_node_fields, temp_path):
    """INPUT:  Real user config with +post_hook (underscore - not in schema!)
    OUTPUT: Moved to +meta.post_hook
    WHY:    post_hook is not in schema, so treated as custom config

    NOTE: Using REAL schema - post_hook with underscore is NOT recognized
    """
    input_dict = {
        "+my_custom_unknown_config": "value",
        "+post_hook": [
            "{{ grant_select(this, 'data-analytics') }}",
            "{{ grant_select(this, 'airflow_service_user_role') }}",
        ],
        "+materialized": "table",
    }

    expected_output = {
        "+materialized": "table",
        "+meta": {
            "my_custom_unknown_config": "value",
            "post_hook": [  # Moved to meta (not in schema)
                "{{ grant_select(this, 'data-analytics') }}",
                "{{ grant_select(this, 'airflow_service_user_role') }}",
            ],
        },
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 2
    # Both moved to meta
    assert any("post_hook" in log and "meta" in log for log in logs)
    assert any("my_custom_unknown_config" in log and "meta" in log for log in logs)


def test_all_scenarios_combined(models_node_fields, temp_path):
    """INPUT:  Mix of all scenarios
    OUTPUT: Everything properly handled
    WHY:    Real projects have all these issues at once
    """
    input_dict = {
        "+post-hook": "select 1",  # Valid, keep
        "+pre_hook": "select 0",  # Invalid (underscore), move to meta
        "materialized": "table",  # Missing +, add it
        "+enabled": True,  # Valid, keep
        "+custom_unknown": "val",  # Invalid, move to meta
        "another_custom": "val2",  # Invalid without +, move to meta
    }

    expected_output = {
        "+post-hook": "select 1",
        "+materialized": "table",  # + added!
        "+enabled": True,
        "+meta": {  # All invalids here
            "pre_hook": "select 0",
            "custom_unknown": "val",
            "another_custom": "val2",
        },
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 4
    # Should have logs for: materialized +, and 3 meta moves


# =============================================================================
# SCENARIO 6: Edge cases
# =============================================================================


def test_empty_dict(models_node_fields, temp_path):
    """INPUT:  Empty dict
    OUTPUT: Empty dict
    WHY:    Nothing to process
    """
    input_dict = {}
    expected_output = {}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


def test_preserves_complex_values(models_node_fields, temp_path):
    """INPUT:  Configs with complex values (lists, dicts)
    OUTPUT: Values preserved exactly
    WHY:    We only change keys, never values
    """
    input_dict = {
        "+post-hook": ["{{ macro1() }}", "select * from {{ ref('model') }}"],
        "+persist_docs": {"relation": True, "columns": True},
        "+tags": ["tag1", "tag2", "tag3"],
    }

    expected_output = input_dict.copy()  # Should be unchanged

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


def test_jinja_in_hook_values_preserved(models_node_fields, temp_path):
    """INPUT:  Invalid config (+post_hook) with complex Jinja
    OUTPUT: Moved to meta with Jinja preserved exactly
    WHY:    We only move keys, never touch the values/Jinja content
    """
    input_dict = {
        "+post_hook": [
            "{%- if target.name.startswith('sf')-%}{{ grant_select(this, 'role') }}{%- endif -%}",
            "CALL UTILS.GOVERNANCE.REAPPLY_JIRA_RESOURCE_GRANTS('{{ this | upper }}')",
        ]
    }

    expected_output = {
        "+meta": {
            "post_hook": [  # Moved to meta, values preserved exactly
                "{%- if target.name.startswith('sf')-%}{{ grant_select(this, 'role') }}{%- endif -%}",
                "CALL UTILS.GOVERNANCE.REAPPLY_JIRA_RESOURCE_GRANTS('{{ this | upper }}')",
            ]
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 1
    assert "post_hook" in logs[0] and "meta" in logs[0]


# =============================================================================
# SCENARIO 7: Nested configs under logical groupings (non-existent directories)
# =============================================================================


def test_nested_config_under_logical_grouping(models_node_fields, temp_path):
    """INPUT:  Logical grouping 'example' (doesn't exist as directory) with nested configs
    OUTPUT: Grouping preserved, nested configs get + prefix
    WHY:    Logical groupings in YAML should be recursed into, not moved to meta

    This is the bug from the user's question - materialized wasn't getting +
    """
    input_dict = {"example": {"materialized": "view"}}

    expected_output = {"example": {"+materialized": "view"}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 1
    assert "materialized" in logs[0]


def test_multiple_nested_configs_under_logical_grouping(models_node_fields, temp_path):
    """INPUT:  Logical grouping with multiple nested configs
    OUTPUT: All nested configs get + prefix
    WHY:    All valid configs in a logical grouping need + prefix
    """
    input_dict = {"example": {"materialized": "view", "schema": "analytics", "enabled": True}}

    expected_output = {"example": {"+materialized": "view", "+schema": "analytics", "+enabled": True}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 3


def test_deeply_nested_logical_groupings(models_node_fields, temp_path):
    """INPUT:  Multiple levels of logical groupings
    OUTPUT: Configs at all levels get + prefix
    WHY:    Recursion should work at any depth
    """
    input_dict = {"external_views": {"example": {"materialized": "view", "schema": "analytics"}}}

    expected_output = {"external_views": {"example": {"+materialized": "view", "+schema": "analytics"}}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 2


def test_users_exact_scenario_from_question(models_node_fields, temp_path):
    """INPUT:  The exact structure from the user's question
    OUTPUT: materialized gets + prefix
    WHY:    This is the bug that was reported

    User's YAML:
    models:
      external_views:
        example:
          materialized: view
        schema: external_analytics
    """
    input_dict = {"external_views": {"example": {"materialized": "view"}, "schema": "external_analytics"}}

    expected_output = {"external_views": {"example": {"+materialized": "view"}, "+schema": "external_analytics"}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 2


def test_nested_custom_configs_in_logical_grouping(models_node_fields, temp_path):
    """INPUT:  Logical grouping with custom (invalid) configs
    OUTPUT: Custom configs moved to +meta at the right level
    WHY:    Custom configs should still move to meta, even in nested groupings
    """
    input_dict = {"example": {"materialized": "view", "custom_unknown": "value"}}

    expected_output = {"example": {"+materialized": "view", "+meta": {"custom_unknown": "value"}}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 2  # One for materialized +, one for moving custom to meta


def test_mixed_logical_groupings_and_configs(models_node_fields, temp_path):
    """INPUT:  Top-level configs mixed with logical groupings
    OUTPUT: Both handled correctly
    WHY:    Real projects have configs at multiple levels
    """
    input_dict = {
        "materialized": "table",  # Top-level config
        "example": {  # Logical grouping
            "materialized": "view",
            "enabled": False,
        },
    }

    expected_output = {"+materialized": "table", "example": {"+materialized": "view", "+enabled": False}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 3


def test_logical_grouping_with_already_prefixed_configs(models_node_fields, temp_path):
    """INPUT:  Logical grouping where some configs already have +
    OUTPUT: Already prefixed configs unchanged, others get +
    WHY:    Partial migrations should work correctly
    """
    input_dict = {
        "example": {
            "+materialized": "view",  # Already has +
            "schema": "analytics",  # Missing +
        }
    }

    expected_output = {"example": {"+materialized": "view", "+schema": "analytics"}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 1  # Only schema needs fixing


def test_empty_logical_grouping(models_node_fields, temp_path):
    """INPUT:  Logical grouping with empty dict
    OUTPUT: Empty dict preserved
    WHY:    Edge case - empty groupings should work
    """
    input_dict = {"example": {}}

    expected_output = {"example": {}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


# =============================================================================
# SCENARIO 8: Config values with non-dict types (Bug Fix)
# =============================================================================


def test_none_value_returned_as_is(models_node_fields, temp_path):
    """INPUT:  None (non-dict value passed to function)
    OUTPUT: None, empty logs
    WHY:    Type guard should handle None gracefully
    """
    result, logs = rec_check_yaml_path(None, temp_path, models_node_fields)

    assert result is None
    assert len(logs) == 0


def test_integer_value_returned_as_is(models_node_fields, temp_path):
    """INPUT:  Integer value (e.g., 5)
    OUTPUT: Same integer, empty logs
    WHY:    Type guard should preserve non-dict scalar values
    """
    result, logs = rec_check_yaml_path(5, temp_path, models_node_fields)

    assert result == 5
    assert len(logs) == 0


def test_string_value_returned_as_is(models_node_fields, temp_path):
    """INPUT:  String value (e.g., "table")
    OUTPUT: Same string, empty logs
    WHY:    Type guard should preserve string values
    """
    result, logs = rec_check_yaml_path("table", temp_path, models_node_fields)

    assert result == "table"
    assert len(logs) == 0


def test_boolean_value_returned_as_is(models_node_fields, temp_path):
    """INPUT:  Boolean values (True/False)
    OUTPUT: Same boolean, empty logs
    WHY:    Type guard should preserve boolean values
    """
    result_true, logs_true = rec_check_yaml_path(True, temp_path, models_node_fields)
    result_false, logs_false = rec_check_yaml_path(False, temp_path, models_node_fields)

    assert result_true is True
    assert result_false is False
    assert len(logs_true) == 0
    assert len(logs_false) == 0


def test_list_value_returned_as_is(models_node_fields, temp_path):
    """INPUT:  List value (e.g., ["a", "b"])
    OUTPUT: Same list, empty logs
    WHY:    Type guard should preserve list values
    """
    input_list = ["a", "b", "c"]
    result, logs = rec_check_yaml_path(input_list, temp_path, models_node_fields)

    assert result == input_list
    assert len(logs) == 0


def test_partition_by_with_nested_dict_preserved(models_node_fields, temp_path):
    """INPUT:  Config with partition_by containing nested dict with range
    OUTPUT: Config preserved exactly (this is the user's exact scenario)
    WHY:    partition_by value is a complex dict that should be preserved as-is

    This is the exact scenario from the user's bug report:
    partition_by={
        "field": "timezone_offset_amt",
        "data_type": "int64",
        "range": {"start": -11, "end": 12, "interval": 1}
    }
    """
    input_dict = {
        "+partition_by": {
            "field": "timezone_offset_amt",
            "data_type": "int64",
            "range": {"start": -11, "end": 12, "interval": 1},
        }
    }

    expected_output = {
        "+partition_by": {
            "field": "timezone_offset_amt",
            "data_type": "int64",
            "range": {"start": -11, "end": 12, "interval": 1},
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


def test_cluster_by_list_preserved(models_node_fields, temp_path):
    """INPUT:  Config with cluster_by as list
    OUTPUT: List value preserved exactly
    WHY:    cluster_by is a valid config that accepts list values
    """
    input_dict = {"+cluster_by": ["timezone_nm", "zip5_cd", "timezone_offset_adj_amt"]}

    expected_output = {"+cluster_by": ["timezone_nm", "zip5_cd", "timezone_offset_adj_amt"]}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


def test_persist_docs_dict_preserved(models_node_fields, temp_path):
    """INPUT:  Config with persist_docs as dict
    OUTPUT: Dict value preserved exactly
    WHY:    persist_docs is a valid config that accepts dict values
    """
    input_dict = {"+persist_docs": {"relation": True, "columns": True}}

    expected_output = {"+persist_docs": {"relation": True, "columns": True}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


def test_tags_list_with_special_values(models_node_fields, temp_path):
    """INPUT:  Config with tags list including special values
    OUTPUT: List preserved exactly
    WHY:    tags is a valid config accepting list values
    """
    input_dict = {"+tags": ["exclude_hourly_build", "tag2", "special-tag"]}

    expected_output = {"+tags": ["exclude_hourly_build", "tag2", "special-tag"]}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


def test_user_exact_scenario_from_traceback(models_node_fields, temp_path):
    """INPUT:  Exact scenario from user's traceback
    OUTPUT: All configs get + prefix, complex values preserved
    WHY:    This is the real-world bug that was reported

    User had a logical grouping with partition_by (nested dict),
    cluster_by (list), and tags (list). All should be handled correctly.
    """
    input_dict = {
        "example": {
            "partition_by": {
                "field": "timezone_offset_amt",
                "data_type": "int64",
                "range": {"start": -11, "end": 12, "interval": 1},
            },
            "cluster_by": ["timezone_nm", "zip5_cd", "timezone_offset_adj_amt"],
            "tags": ["exclude_hourly_build"],
        }
    }

    expected_output = {
        "example": {
            "+partition_by": {
                "field": "timezone_offset_amt",
                "data_type": "int64",
                "range": {"start": -11, "end": 12, "interval": 1},
            },
            "+cluster_by": ["timezone_nm", "zip5_cd", "timezone_offset_adj_amt"],
            "+tags": ["exclude_hourly_build"],
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 3  # Three configs got + prefix
    assert any("partition_by" in log for log in logs)
    assert any("cluster_by" in log for log in logs)
    assert any("tags" in log for log in logs)


def test_mixed_config_types_in_logical_grouping(models_node_fields, temp_path):
    """INPUT:  Logical grouping with string, dict, list, and boolean configs
    OUTPUT: Valid configs get + prefix, invalid moved to meta, values preserved
    WHY:    Real projects have diverse config value types

    Note: 'threads' is not a valid config in dbt_project.yml for models,
    so it gets moved to +meta (correct behavior)
    """
    input_dict = {
        "my_models": {
            "materialized": "table",  # string - valid config
            "partition_by": {"field": "date", "data_type": "date"},  # dict - valid config
            "cluster_by": ["col1", "col2"],  # list - valid config
            "enabled": True,  # boolean - valid config
            "threads": 4,  # integer - NOT valid in dbt_project.yml, should move to meta
        }
    }

    expected_output = {
        "my_models": {
            "+materialized": "table",
            "+partition_by": {"field": "date", "data_type": "date"},
            "+cluster_by": ["col1", "col2"],
            "+enabled": True,
            "+meta": {
                "threads": 4  # Moved to meta (not a valid dbt_project.yml config)
            },
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 5  # 4 configs got + prefix, 1 moved to meta


def test_deeply_nested_with_complex_config_values(models_node_fields, temp_path):
    """INPUT:  Multi-level logical groupings with complex values at leaf level
    OUTPUT: Complex values preserved at all nesting levels
    WHY:    Deep nesting should work correctly with complex values
    """
    input_dict = {
        "external_views": {
            "example": {
                "partition_by": {"field": "date", "data_type": "date", "granularity": "day"},
                "materialized": "view",
            }
        }
    }

    expected_output = {
        "external_views": {
            "example": {
                "+partition_by": {"field": "date", "data_type": "date", "granularity": "day"},
                "+materialized": "view",
            }
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 2


def test_config_with_empty_dict_value(models_node_fields, temp_path):
    """INPUT:  Config with empty dict as value
    OUTPUT: Empty dict preserved
    WHY:    Edge case - empty dict values should be preserved
    """
    input_dict = {"+persist_docs": {}}

    expected_output = {"+persist_docs": {}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


def test_config_with_empty_list_value(models_node_fields, temp_path):
    """INPUT:  Config with empty list as value
    OUTPUT: Empty list preserved
    WHY:    Edge case - empty list values should be preserved
    """
    input_dict = {"+tags": []}

    expected_output = {"+tags": []}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


def test_config_with_nested_empty_structures(models_node_fields, temp_path):
    """INPUT:  Config with nested empty dict
    OUTPUT: Nested structure preserved
    WHY:    Complex nested structures with empty values should work
    """
    input_dict = {"+partition_by": {"field": "date", "range": {}}}

    expected_output = {"+partition_by": {"field": "date", "range": {}}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0


def test_all_scalar_types_in_one_config(models_node_fields, temp_path):
    """INPUT:  Multiple valid configs with different value types
    OUTPUT: All get + prefix, all value types preserved
    WHY:    Ensure type guard works for all common value types together

    This test focuses on configs with different VALUE types (string, bool, list, dict)
    to ensure the type guard preserves all value types correctly.
    """
    input_dict = {
        "my_models": {
            "materialized": "table",  # string value
            "enabled": True,  # boolean value
            "tags": ["tag1", "tag2"],  # list value
            "persist_docs": {"relation": True, "columns": False},  # dict value
        }
    }

    expected_output = {
        "my_models": {
            "+materialized": "table",
            "+enabled": True,
            "+tags": ["tag1", "tag2"],
            "+persist_docs": {"relation": True, "columns": False},
        }
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 4  # All 4 configs got + prefix


def test_persist_docs_dict_value_not_recursed(models_node_fields, temp_path):
    """INPUT:  Config with +persist_docs that has dict value
    OUTPUT: Dict value preserved as-is, NOT recursed into
    WHY:    Bug fix - persist_docs value should not be treated as nested configs

    This was a real bug where the function would recurse into the dict value
    of +persist_docs and treat 'relation' and 'columns' as config keys,
    moving them to +meta incorrectly.
    """
    input_dict = {"+persist_docs": {"relation": True, "columns": True}}

    expected_output = {"+persist_docs": {"relation": True, "columns": True}}

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0  # No changes, value preserved as-is


def test_labels_dict_value_not_recursed(models_node_fields, temp_path):
    """INPUT:  Config with +labels that has dict value
    OUTPUT: Dict value preserved as-is, NOT recursed into
    WHY:    Bug fix - labels value should not be treated as nested configs

    Real-world scenario: labels config with key-value pairs should not
    have those pairs moved to +meta.
    """
    input_dict = {
        "+labels": {"application": "data_analytics", "environment": "{{ target.name }}", "billing": "analytics"}
    }

    expected_output = {
        "+labels": {"application": "data_analytics", "environment": "{{ target.name }}", "billing": "analytics"}
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 0  # No changes, value preserved as-is


def test_mixed_valid_configs_with_dict_values(models_node_fields, temp_path):
    """INPUT:  Multiple configs, some with dict values, some without + prefix
    OUTPUT: Missing + added, dict values preserved without recursion
    WHY:    Comprehensive test for real dbt_project.yml structure

    This simulates a real seeds: section from dbt_project.yml
    """
    input_dict = {
        "project": "my_project",  # string config, needs +
        "dataset": "seeds",  # string config, needs +
        "persist_docs": {  # dict config, needs +, value preserved
            "relation": True,
            "columns": True,
        },
        "labels": {  # dict config, needs +, value preserved
            "application": "analytics",
            "environment": "prod",
        },
    }

    expected_output = {
        "+project": "my_project",
        "+dataset": "seeds",
        "+persist_docs": {"relation": True, "columns": True},
        "+labels": {"application": "analytics", "environment": "prod"},
    }

    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)

    assert result == expected_output
    assert len(logs) == 4  # All 4 configs got + prefix
    # Verify no unwanted nesting under +meta
    assert "+meta" not in result["+persist_docs"]
    assert "+meta" not in result["+labels"]


# =============================================================================
# Quick reference table (as docstring)
# =============================================================================

"""
QUICK REFERENCE - What rec_check_yaml_path does:

Input Key                  | Output Key           | Why
---------------------------|----------------------|----------------------------------
+post-hook                 | +post-hook          | ✅ In schema (hyphen) - kept
+post_hook                 | +meta.post_hook     | ❌ Not in schema - moved to meta
post-hook                  | +post-hook          | 🔧 Added + prefix (in schema)
+materialized              | +materialized       | ✅ In schema - kept
materialized               | +materialized       | 🔧 Added + prefix (in schema)
+file_format               | +file_format        | ✅ In schema - kept (real schema!)
+custom_config             | +meta.custom_config | ❌ Not in schema - moved to meta
custom_config              | +meta.custom_config | ❌ Not in schema - moved to meta

Key insights:
1. Only checks if key is IN SCHEMA - no spelling correction
2. dbt_project.yml schema expects: +pre-hook, +post-hook (hyphens)
3. All configs need + prefix in dbt_project.yml
4. Any config NOT in schema → moved to +meta
5. 🎯 Tests use REAL Fusion schema - accurate validation!
"""

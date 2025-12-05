"""Test cases for the improved config.get/require to meta_get/meta_require autofix."""

import pytest
from dbt_autofix.refactors.changesets.dbt_sql_improved import (
    move_custom_config_access_to_meta_sql_improved,
)
from dbt_autofix.retrieve_schemas import SchemaSpecs
from dbt_autofix.fields_properties_configs import models_allowed_config


class MockSchemaSpecs:
    """Mock schema specs for testing."""
    def __init__(self):
        self.yaml_specs_per_node_type = {
            "models": models_allowed_config,
        }


def test_basic_config_get_refactor():
    """Test basic config.get() refactoring."""
    sql_content = """
{{ config(
    materialized='table',
    custom_key='custom_value'
) }}

SELECT
    '{{ config.get('custom_key') }}' as custom,
    '{{ config.get('materialized') }}' as mat
"""

    result = move_custom_config_access_to_meta_sql_improved(
        sql_content, MockSchemaSpecs(), "models"
    )

    assert result.refactored
    assert "config.meta_get('custom_key')" in result.refactored_content
    assert "config.get('materialized')" in result.refactored_content  # Should not change
    assert len(result.deprecation_refactors) == 1


def test_config_get_with_default():
    """Test config.get() with default value."""
    sql_content = """
SELECT
    '{{ config.get('custom_key', 'default_value') }}' as custom,
    '{{ config.get('another_key', var('my_var')) }}' as another
"""

    result = move_custom_config_access_to_meta_sql_improved(
        sql_content, MockSchemaSpecs(), "models"
    )

    assert result.refactored
    assert "config.meta_get('custom_key', 'default_value')" in result.refactored_content
    assert "config.meta_get('another_key', var('my_var'))" in result.refactored_content


def test_config_require_refactor():
    """Test config.require() refactoring."""
    sql_content = """
{% set required_val = config.require('custom_required') %}
{% set mat = config.require('materialized') %}
"""

    result = move_custom_config_access_to_meta_sql_improved(
        sql_content, MockSchemaSpecs(), "models"
    )

    assert result.refactored
    assert "config.meta_require('custom_required')" in result.refactored_content
    assert "config.require('materialized')" in result.refactored_content  # Should not change


def test_config_with_validator():
    """Test that config with validators are now properly refactored."""
    sql_content = """
{%- set file_format = config.get('custom_format', validator=validation.any[basestring]) -%}
"""

    result = move_custom_config_access_to_meta_sql_improved(
        sql_content, MockSchemaSpecs(), "models"
    )

    assert result.refactored  # Should refactor since validators are now supported
    assert "config.meta_get('custom_format', validator=validation.any[basestring])" in result.refactored_content
    assert len(result.refactor_warnings) == 0  # No warnings since validators work


def test_variable_shadowing_detection():
    """Test that variable shadowing is detected and skipped."""
    sql_content = """
{% set config = my_custom_config %}
{{ config.get('some_key') }}
"""

    result = move_custom_config_access_to_meta_sql_improved(
        sql_content, MockSchemaSpecs(), "models"
    )

    assert not result.refactored
    assert len(result.refactor_warnings) == 1
    assert "shadowing" in result.refactor_warnings[0]


def test_chained_access_warning():
    """Test that chained access patterns generate warnings."""
    sql_content = """
{% set dict_val = config.get('custom_dict').subkey %}
{% set another = config.get('custom_dict').get('key', 'default') %}
"""

    result = move_custom_config_access_to_meta_sql_improved(
        sql_content, MockSchemaSpecs(), "models"
    )

    assert result.refactored
    assert "config.meta_get('custom_dict').subkey" in result.refactored_content
    assert len(result.refactor_warnings) == 2  # Two chained access warnings


def test_mixed_quotes():
    """Test handling of mixed quote styles - preserves original quotes."""
    sql_content = """
{{ config.get("custom_key1") }}
{{ config.get('custom_key2') }}
{{ config.get(  "custom_key3"  ) }}
"""

    result = move_custom_config_access_to_meta_sql_improved(
        sql_content, MockSchemaSpecs(), "models"
    )

    assert result.refactored
    # Now preserves original quote style
    assert 'config.meta_get("custom_key1")' in result.refactored_content
    assert "config.meta_get('custom_key2')" in result.refactored_content
    assert 'config.meta_get(  "custom_key3"  )' in result.refactored_content  # Preserves spacing too


def test_complex_defaults():
    """Test handling of complex default values."""
    sql_content = """
{{ config.get('custom_list', []) }}
{{ config.get('custom_dict', {}) }}
{{ config.get('custom_none', none) }}
"""

    result = move_custom_config_access_to_meta_sql_improved(
        sql_content, MockSchemaSpecs(), "models"
    )

    assert result.refactored
    assert "config.meta_get('custom_list', [])" in result.refactored_content
    assert "config.meta_get('custom_dict', {})" in result.refactored_content
    assert "config.meta_get('custom_none', none)" in result.refactored_content


def test_no_refactor_for_dbt_configs():
    """Test that dbt-native configs are not refactored."""
    sql_content = """
{{ config.get('materialized') }}
{{ config.get('unique_key') }}
{{ config.get('cluster_by') }}
{{ config.get('grants') }}
"""

    result = move_custom_config_access_to_meta_sql_improved(
        sql_content, MockSchemaSpecs(), "models"
    )

    assert not result.refactored
    assert result.refactored_content == sql_content


def test_multiline_config_calls():
    """Test handling of multiline config calls - preserves formatting."""
    sql_content = """
{{ config.get(
    'custom_key',
    'default_value'
) }}
"""

    result = move_custom_config_access_to_meta_sql_improved(
        sql_content, MockSchemaSpecs(), "models"
    )

    assert result.refactored
    # Should preserve the exact multiline formatting
    assert "config.meta_get(\n    'custom_key',\n    'default_value'\n)" in result.refactored_content
"""Test cases for the improved config.get/require to meta_get/meta_require autofix."""

from pathlib import Path

from dbt_autofix.fields_properties_configs import models_allowed_config
from dbt_autofix.refactors.changesets.dbt_sql_improved import (
    move_custom_config_access_to_meta_sql_improved,
)
from dbt_autofix.refactors.results import SQLContent, SQLRefactorConfig


class MockSchemaSpecs:
    """Mock schema specs for testing."""

    def __init__(self):
        self.yaml_specs_per_node_type = {
            "models": models_allowed_config,
        }


def _sql(sql_str: str) -> SQLContent:
    return SQLContent(original_str=sql_str, current_str=sql_str, current_file_path=Path("model.sql"))


def _sql_cfg(schema_specs=None, node_type: str = "models") -> SQLRefactorConfig:
    return SQLRefactorConfig(schema_specs=schema_specs or MockSchemaSpecs(), node_type=node_type)


def test_basic_config_get_refactor():
    """Test basic config.get() refactoring."""
    input_sql = """
{{ config(
    materialized='table',
    custom_key='custom_value'
) }}

SELECT
    '{{ config.get('custom_key') }}' as custom,
    '{{ config.get('materialized') }}' as mat
"""

    expected_sql = """
{{ config(
    materialized='table',
    custom_key='custom_value'
) }}

SELECT
    '{{ config.meta_get('custom_key') }}' as custom,
    '{{ config.get('materialized') }}' as mat
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 1


def test_config_get_with_default():
    """Test config.get() with default value."""
    input_sql = """
SELECT
    '{{ config.get('custom_key', 'default_value') }}' as custom,
    '{{ config.get('another_key', var('my_var')) }}' as another
"""

    expected_sql = """
SELECT
    '{{ config.meta_get('custom_key', 'default_value') }}' as custom,
    '{{ config.meta_get('another_key', var('my_var')) }}' as another
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 2


def test_config_require_refactor():
    """Test config.require() refactoring."""
    input_sql = """
{% set required_val = config.require('custom_required') %}
{% set mat = config.require('materialized') %}
"""

    expected_sql = """
{% set required_val = config.meta_require('custom_required') %}
{% set mat = config.require('materialized') %}
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 1


def test_config_with_validator():
    """Test that config with validators are now properly refactored."""
    input_sql = """
{%- set file_format = config.get('custom_format', validator=validation.any[basestring]) -%}
"""

    expected_sql = """
{%- set file_format = config.meta_get('custom_format', validator=validation.any[basestring]) -%}
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.refactor_warnings) == 0  # No warnings since validators work
    assert len(result.deprecation_refactors) == 1


def test_variable_shadowing_detection():
    """Test that variable shadowing is detected and skipped."""
    input_sql = """
{% set config = my_custom_config %}
{{ config.get('some_key') }}
"""

    # Expected to remain unchanged due to shadowing
    expected_sql = input_sql

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert not result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.refactor_warnings) == 1
    assert "shadowing" in result.refactor_warnings[0]


def test_chained_access_warning():
    """Test that chained access patterns generate warnings."""
    input_sql = """
{% set dict_val = config.get('custom_dict').subkey %}
{% set another = config.get('custom_dict').get('key', 'default') %}
"""

    expected_sql = """
{% set dict_val = config.meta_get('custom_dict').subkey %}
{% set another = config.meta_get('custom_dict').get('key', 'default') %}
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.refactor_warnings) == 2  # Two chained access warnings
    assert len(result.deprecation_refactors) == 2


def test_mixed_quotes():
    """Test handling of mixed quote styles - preserves original quotes."""
    input_sql = """
{{ config.get("custom_key1") }}
{{ config.get('custom_key2') }}
{{ config.get(  "custom_key3"  ) }}
"""

    expected_sql = """
{{ config.meta_get("custom_key1") }}
{{ config.meta_get('custom_key2') }}
{{ config.meta_get(  "custom_key3"  ) }}
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 3


def test_complex_defaults():
    """Test handling of complex default values."""
    input_sql = """
{{ config.get('custom_list', []) }}
{{ config.get('custom_dict', {}) }}
{{ config.get('custom_none', none) }}
"""

    expected_sql = """
{{ config.meta_get('custom_list', []) }}
{{ config.meta_get('custom_dict', {}) }}
{{ config.meta_get('custom_none', none) }}
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 3


def test_no_refactor_for_dbt_configs():
    """Test that dbt-native configs are not refactored."""
    input_sql = """
{{ config.get('materialized') }}
{{ config.get('unique_key') }}
{{ config.get('cluster_by') }}
{{ config.get('grants') }}
"""

    # Expected to remain unchanged as these are dbt-native configs
    expected_sql = input_sql

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert not result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 0


def test_multiline_config_calls():
    """Test handling of multiline config calls - preserves formatting."""
    input_sql = """
{{ config.get(
    'custom_key',
    'default_value'
) }}
"""

    expected_sql = """
{{ config.meta_get(
    'custom_key',
    'default_value'
) }}
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 1


def test_config_get_with_named_default_parameter():
    """Test config.get() with default= named parameter syntax and complex default values."""
    input_sql = """
{{ config.get('custom_config', default='default_value') }}

{{ config.get('custom_config', default=var.get('my_var')) }}

{{ config.get('custom_config', default=dest_columns | map(attribute="quoted") | list) }}
"""

    expected_sql = """
{{ config.meta_get('custom_config', default='default_value') }}

{{ config.meta_get('custom_config', default=var.get('my_var')) }}

{{ config.meta_get('custom_config', default=dest_columns | map(attribute="quoted") | list) }}
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 3


def test_model_config_get_refactor():
    """Test basic model.config.get() refactoring."""
    input_sql = """
{{ config(
    materialized='table',
    custom_key='custom_value'
) }}

SELECT
    '{{ model.config.get('custom_key') }}' as custom,
    '{{ model.config.get('materialized') }}' as mat
"""

    expected_sql = """
{{ config(
    materialized='table',
    custom_key='custom_value'
) }}

SELECT
    '{{ config.meta_get('custom_key') }}' as custom,
    '{{ model.config.get('materialized') }}' as mat
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 1


def test_prefixed_model_receiver_is_not_mangled():
    """A receiver ending in `model` must not have its prefix orphaned.

    Regression test: the optional `model.` prefix used to match mid-identifier, so
    `test_model.config.get(...)` matched starting at `model.`. Dropping that match
    left the orphaned `test_` fused onto the replacement, producing
    `test_config.meta_get(...)` -- an undefined Jinja variable that fails at render
    time. See dbt-labs/fs#12687.
    """
    input_sql = """
SELECT
    '{{ test_model.config.get('custom_key') }}' as custom
"""

    expected_sql = """
SELECT
    '{{ test_model.config.meta_get('custom_key') }}' as custom
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert "test_config" not in result.refactored_content
    assert len(result.deprecation_refactors) == 1


def test_non_model_receiver_is_preserved():
    """An arbitrary receiver sits outside the match and must survive untouched."""
    input_sql = """
SELECT
    '{{ node.config.get('custom_key', 'false') }}' as custom
"""

    expected_sql = """
SELECT
    '{{ node.config.meta_get('custom_key', 'false') }}' as custom
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 1


def test_prefixed_model_receiver_with_require():
    """The same boundary rule applies to `config.require(...)`."""
    input_sql = "SELECT '{{ this_model.config.require('custom_key') }}' as custom"
    expected_sql = "SELECT '{{ this_model.config.meta_require('custom_key') }}' as custom"

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert "this_config" not in result.refactored_content


def test_dbt_constraints_style_receivers():
    """Real-world shape from the dbt_constraints package that triggered dbt-labs/fs#12687.

    Exercises all three receiver forms in one template: a `test_model.` prefix, a bare
    `node.` receiver, and a plain `model.` prefix that is still stripped as before.
    """
    input_sql = """
{%- if test_model.config.get("dbt_constraints_enabled", "true")|string|lower == "true"
    or node.config.get("always_create_constraint", "false")|string|lower == "true"
    or model.config.get("custom_key", "false")|string|lower == "true" -%}
    {{ return(true) }}
{%- endif -%}
"""

    expected_sql = """
{%- if test_model.config.meta_get("dbt_constraints_enabled", "true")|string|lower == "true"
    or node.config.meta_get("always_create_constraint", "false")|string|lower == "true"
    or config.meta_get("custom_key", "false")|string|lower == "true" -%}
    {{ return(true) }}
{%- endif -%}
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert result.refactored
    assert result.refactored_content == expected_sql
    assert len(result.deprecation_refactors) == 3


def test_identifier_ending_in_config_is_not_mangled():
    """A plain dict variable named `..._config` must not be treated as a receiver.

    Regression test: the bare `config.` alternative had no lookbehind, so it could
    match mid-identifier at the `config.get(...)` suffix of `model_config.get(...)`,
    an ordinary dict -- not dbt's `config` object. That rewrote valid code into
    `model_config.meta_get(...)`, an undefined method that breaks at render time.
    See dbt-labs/fs#14000.
    """
    input_sql = """
SELECT
    '{{ model_config.get('source_config', {}) }}' as custom,
    '{{ test_config.get('other_key', 'x') }}' as other
"""

    result = move_custom_config_access_to_meta_sql_improved(_sql(input_sql), _sql_cfg())

    assert not result.refactored
    assert result.refactored_content == input_sql
    assert "meta_get" not in result.refactored_content

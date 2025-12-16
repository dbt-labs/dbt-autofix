import pytest

from dbt_autofix.refactors.changesets.dbt_sql import _serialize_config_macro_call


@pytest.mark.parametrize(
    "config_dict,config_source_map,expected_output",
    [
        # Simple string values - should be double quoted
        ({"materialized": "table"}, {}, '\n    materialized="table"'),
        # Multiple configs
        ({"materialized": "table", "schema": "my_schema"}, {}, '\n    materialized="table", \n    schema="my_schema"'),
        # Boolean values
        ({"enabled": True, "transient": False}, {}, "\n    enabled=True, \n    transient=False"),
        # Integer values
        ({"partition_by": 5, "threads": 10}, {}, "\n    partition_by=5, \n    threads=10"),
        # List values
        ({"tags": ["tag1", "tag2"]}, {}, "\n    tags=['tag1', 'tag2']"),
        # Simple meta block with string
        (
            {"materialized": "table", "meta": {"custom_key": "custom_value"}},
            {},
            "\n    materialized=\"table\", \n    meta={'custom_key': 'custom_value'}",
        ),
        # Meta block with multiple keys
        ({"meta": {"key1": "value1", "key2": "value2"}}, {}, "\n    meta={'key1': 'value1', 'key2': 'value2'}"),
        # Source map with single-quoted value (should convert to double quotes)
        ({"materialized": "table"}, {"materialized": "'table'"}, '\n    materialized="table"'),
        # Source map with double-quoted value (should preserve)
        ({"materialized": "table"}, {"materialized": '"table"'}, '\n    materialized="table"'),
        # Source map with Jinja expression (should preserve)
        ({"materialized": "env_var('MAT')"}, {"materialized": "env_var('MAT')"}, "\n    materialized=env_var('MAT')"),
        # Source map with var() function
        ({"schema": "var('my_schema')"}, {"schema": "var('my_schema')"}, "\n    schema=var('my_schema')"),
        # Source map with complex Jinja expression
        (
            {"schema": "target.schema + '_suffix'"},
            {"schema": "target.schema + '_suffix'"},
            "\n    schema=target.schema + '_suffix'",
        ),
        # Mixed: regular config and config with Jinja
        (
            {"materialized": "table", "schema": "var('schema')"},
            {"schema": "var('schema')"},
            "\n    materialized=\"table\", \n    schema=var('schema')",
        ),
        # Meta with Jinja preserved from source map
        (
            {"materialized": "table", "meta": {"custom_config": "var('my_var')"}},
            {"custom_config": "var('my_var')"},
            "\n    materialized=\"table\", \n    meta={'custom_config': var('my_var')}",
        ),
        # Meta with multiple configs from source map
        (
            {"meta": {"config1": "value1", "config2": "var('x')"}},
            {"config1": "'value1'", "config2": "var('x')"},
            "\n    meta={'config1': 'value1', 'config2': var('x')}",
        ),
        # String with spaces (single quotes in source map)
        ({"description": "my description"}, {"description": "'my description'"}, '\n    description="my description"'),
        # String with spaces (double quotes in source map)
        ({"description": "my description"}, {"description": '"my description"'}, '\n    description="my description"'),
        # Complex expression with double quotes
        (
            {"path": 'target.schema + "/" + var("folder")'},
            {"path": 'target.schema + "/" + var("folder")'},
            '\n    path=target.schema + "/" + var("folder")',
        ),
        # env_var with double quotes
        (
            {"materialized": 'env_var("DBT_MAT")'},
            {"materialized": 'env_var("DBT_MAT")'},
            '\n    materialized=env_var("DBT_MAT")',
        ),
        # Multiple meta values with different quote styles
        (
            {"meta": {"key1": "val1", "key2": "val2", "key3": "val3"}},
            {"key1": "'val1'", "key2": '"val2"', "key3": "var('x')"},
            "\n    meta={'key1': 'val1', 'key2': \"val2\", 'key3': var('x')}",
        ),
        # List in meta
        ({"meta": {"tags": ["a", "b"]}}, {}, "\n    meta={'tags': ['a', 'b']}"),
        # Dict in meta
        ({"meta": {"nested": {"key": "value"}}}, {}, "\n    meta={'nested': {'key': 'value'}}"),
        # Config with hyphenated keys (should return dict str representation)
        ({"pre-hook": "select 1", "post-hook": "select 2"}, {}, "{'pre-hook': 'select 1', 'post-hook': 'select 2'}"),
        # String with escaped quotes (single quotes in source)
        ({"query": "select 'value'"}, {"query": "\"select 'value'\""}, "\n    query=\"select 'value'\""),
        # String with escaped quotes (double quotes containing single)
        ({"description": "it's a test"}, {"description": '"it\'s a test"'}, '\n    description="it\'s a test"'),
        # Conditional Jinja expression
        (
            {"severity": "'error' if var('strict') else 'warn'"},
            {"severity": "'error' if var('strict') else 'warn'"},
            "\n    severity='error' if var('strict') else 'warn'",
        ),
        # Arithmetic expression
        (
            {"threshold": "var('base', 100) + 50"},
            {"threshold": "var('base', 100) + 50"},
            "\n    threshold=var('base', 100) + 50",
        ),
        # Empty meta dict
        ({"materialized": "table", "meta": {}}, {}, '\n    materialized="table", \n    meta={}'),
        # Meta with integer value
        ({"meta": {"count": 42}}, {}, "\n    meta={'count': 42}"),
        # Meta with boolean value
        ({"meta": {"enabled": True}}, {}, "\n    meta={'enabled': True}"),
        # Source map preserves function call with nested quotes
        (
            {"hook": 'log("Starting process")'},
            {"hook": 'log("Starting process")'},
            '\n    hook=log("Starting process")',
        ),
        # Multiple values with mixed quoting from dict literal
        (
            {"materialized": "table", "meta": {"hook1": "select 1", "hook2": "select 2"}},
            {"hook1": '"select 1"', "hook2": '"select 2"'},
            '\n    materialized="table", \n    meta={\'hook1\': "select 1", \'hook2\': "select 2"}',
        ),
    ],
)
def test_serialize_config_macro_call(config_dict, config_source_map, expected_output):
    """Test _serialize_config_macro_call with various configs and quote styles."""
    result = _serialize_config_macro_call(config_dict, config_source_map)
    assert result == expected_output


def test_serialize_with_none_source_map():
    """Test serialization when source map is None (default parameter)."""
    config_dict = {"materialized": "table", "schema": "my_schema"}
    result = _serialize_config_macro_call(config_dict)
    expected = '\n    materialized="table", \n    schema="my_schema"'
    assert result == expected


def test_serialize_empty_config():
    """Test serialization of empty config dict."""
    result = _serialize_config_macro_call({})
    assert result == ""


def test_serialize_preserves_jinja_in_meta_from_source_map():
    """Test that Jinja expressions in meta values are preserved from source map."""
    config_dict = {
        "materialized": "table",
        "meta": {"custom1": "var('x')", "custom2": "env_var('Y')", "custom3": "static"},
    }
    config_source_map = {"custom1": "var('x')", "custom2": "env_var('Y')", "custom3": "'static'"}
    result = _serialize_config_macro_call(config_dict, config_source_map)

    # Check that Jinja expressions are preserved
    assert "var('x')" in result
    assert "env_var('Y')" in result
    # Check that simple string preserves quotes from source map (single quotes in this case)
    assert "'static'" in result


def test_serialize_complex_nested_structure():
    """Test serialization of complex nested structure in meta."""
    config_dict = {
        "materialized": "table",
        "meta": {"nested_dict": {"key1": "val1", "key2": "val2"}, "nested_list": ["item1", "item2"], "simple": "value"},
    }
    result = _serialize_config_macro_call(config_dict)

    assert "materialized=" in result
    assert "meta=" in result
    assert "nested_dict" in result
    assert "nested_list" in result
    assert "simple" in result


def test_serialize_quote_conversion():
    """Test that single-quoted simple strings are converted to double quotes."""
    config_dict = {"key1": "value1", "key2": "value2"}
    config_source_map = {
        "key1": "'value1'",  # Single quotes
        "key2": '"value2"',  # Double quotes
    }
    result = _serialize_config_macro_call(config_dict, config_source_map)

    # Both should be converted to double quotes
    assert 'key1="value1"' in result
    assert 'key2="value2"' in result


def test_serialize_does_not_convert_jinja_quotes():
    """Test that quotes in Jinja expressions are NOT converted."""
    config_dict = {"schema": "var('my_schema')", "path": 'target.name + "_" + var("suffix")'}
    config_source_map = {"schema": "var('my_schema')", "path": 'target.name + "_" + var("suffix")'}
    result = _serialize_config_macro_call(config_dict, config_source_map)

    # Original quotes should be preserved in Jinja
    assert "var('my_schema')" in result
    assert 'var("suffix")' in result
    assert 'target.name + "_"' in result

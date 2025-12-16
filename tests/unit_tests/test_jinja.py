import jinja2
import pytest
from dbt_common.clients.jinja import get_environment

from dbt_autofix.jinja import (
    _SourceCodeExtractor,
    construct_static_kwarg_value,
    statically_parse_unrendered_config,
)


@pytest.mark.parametrize(
    "input_string,expected_output",
    [
        # No config call returns None
        ("select 1 as id", None),
        # Empty config call
        ("{{ config() }}", None),
        # Simple string config
        ("{{ config(materialized='table') }}", {"materialized": "'table'"}),
        # Config with env_var() Jinja function
        ("{{ config(materialized=env_var('DBT_MATERIALIZED')) }}", {"materialized": "env_var('DBT_MATERIALIZED')"}),
        # Config with var() Jinja function
        ("{{ config(schema=var('my_schema')) }}", {"schema": "var('my_schema')"}),
        # Config with multiple keyword arguments
        (
            "{{ config(materialized='table', schema='my_schema', enabled=true) }}",
            {"materialized": "'table'", "schema": "'my_schema'", "enabled": "true"},
        ),
        # Config with complex Jinja expression
        (
            "{{ config(severity='error' if var('strict', false) else 'warn') }}",
            {"severity": "'error' if var('strict', false) else 'warn'"},
        ),
        # Config with dictionary literal argument - simple values
        (
            "{{ config({'pre-hook': 'select 1', 'post-hook': 'select 2'}) }}",
            {"pre-hook": "'select 1'", "post-hook": "'select 2'"},
        ),
        # Config with dictionary literal containing Jinja
        ("{{ config({'myconf': run_started_at - 1}) }}", {"myconf": "run_started_at - 1"}),
        # Config with list value
        ("{{ config(tags=['tag1', 'tag2']) }}", {"tags": "['tag1', 'tag2']"}),
        # Config with dict value
        ("{{ config(meta={'key': 'value'}) }}", {"meta": "{'key': 'value'}"}),
        # Config with integer value
        ("{{ config(partition_by=5) }}", {"partition_by": "5"}),
        # Config with boolean value
        ("{{ config(enabled=false) }}", {"enabled": "false"}),
        # Config call spanning multiple lines
        (
            """{{ config(
            materialized='table',
            schema='my_schema'
        ) }}""",
            {"materialized": "'table'", "schema": "'my_schema'"},
        ),
        # Config with nested function calls
        (
            "{{ config(schema=target.schema + '_' + env_var('ENV')) }}",
            {"schema": "target.schema + '_' + env_var('ENV')"},
        ),
        # Config with SQL after it
        ("{{ config(materialized='table') }}\nselect 1 as id", {"materialized": "'table'"}),
        # Config with other Jinja expressions before it
        ("{% set x = 1 %}\n{{ config(materialized='table') }}", {"materialized": "'table'"}),
        # Custom config with var() - will be evaluated by dbt at runtime
        ("{{ config(custom_config=var('my_variable')) }}", {"custom_config": "var('my_variable')"}),
        # Custom config with env_var() - will be evaluated by dbt at runtime
        ("{{ config(custom_env=env_var('MY_ENV_VAR')) }}", {"custom_env": "env_var('MY_ENV_VAR')"}),
        # Custom config with complex Jinja expression
        (
            "{{ config(custom_threshold=var('base_threshold', 100) + 50) }}",
            {"custom_threshold": "var('base_threshold', 100) + 50"},
        ),
        # Custom config with target object access
        ("{{ config(custom_schema=target.schema + '_suffix') }}", {"custom_schema": "target.schema + '_suffix'"}),
        # Double-quoted string values
        ('{{ config(materialized="table") }}', {"materialized": '"table"'}),
        # Multiple configs with double quotes
        ('{{ config(schema="my_schema", enabled=true) }}', {"schema": '"my_schema"', "enabled": "true"}),
        # Double quotes with spaces
        ('{{ config(custom_config="value with spaces") }}', {"custom_config": '"value with spaces"'}),
        # Mixed quotes: double quotes containing single quotes
        (
            "{{ config(description=\"This is a 'test' description\") }}",
            {"description": "\"This is a 'test' description\""},
        ),
        # Double quotes in SQL query
        ('{{ config(query="select * from table") }}', {"query": '"select * from table"'}),
        # Dict literal with double-quoted keys and values
        (
            '{{ config({"pre-hook": "select 1", "post-hook": "select 2"}) }}',
            {"pre-hook": '"select 1"', "post-hook": '"select 2"'},
        ),
        # Complex Jinja with double quotes
        (
            '{{ config(custom_path=target.schema + "/" + var("folder")) }}',
            {"custom_path": 'target.schema + "/" + var("folder")'},
        ),
    ],
)
def test_statically_parse_unrendered_config(input_string, expected_output):
    """Test statically_parse_unrendered_config with various inputs.

    Note: Jinja functions like var(), env_var(), and target.* are preserved
    as-is in the parsed output. When these configs are moved to the meta block,
    dbt will still evaluate them at runtime because the entire config() block
    is a Jinja expression.
    """
    result = statically_parse_unrendered_config(input_string)
    assert result == expected_output


@pytest.mark.parametrize(
    "source,start_pos,delimiters,expected_output",
    [
        # Extract simple value
        ("config(key='value')", 11, (",", ")"), "'value'"),
        # Extract with nested parentheses
        ("config(key=func(arg1, arg2), other='val')", 11, (",", ")"), "func(arg1, arg2)"),
        # Extract with nested brackets
        ("config(tags=['tag1', 'tag2'], materialized='table')", 12, (",", ")"), "['tag1', 'tag2']"),
        # Extract with nested braces
        ("config(meta={'key': 'value'}, materialized='table')", 12, (",", ")"), "{'key': 'value'}"),
        # Delimiters inside strings are ignored
        ("config(query='select a, b', materialized='table')", 13, (",", ")"), "'select a, b'"),
        # Extraction with double-quoted strings
        ('config(key="value, with comma")', 11, (",", ")"), '"value, with comma"'),
        # Extract complex expression with multiple nesting levels
        (
            "config(value=func(nested([a, b]), {'key': 'val'}), next='item')",
            13,
            (",", ")"),
            "func(nested([a, b]), {'key': 'val'})",
        ),
        # Extraction stops at closing paren
        ("config(key='value')", 11, (",", ")"), "'value'"),
        # Extraction stops at comma
        ("config(key1='value1', key2='value2')", 12, (",", ")"), "'value1'"),
        # Trailing commas are stripped
        ("config(key='value',)", 11, (",", ")"), "'value'"),
        # Double-quoted value
        ('config(key="value")', 11, (",", ")"), '"value"'),
        # Double quotes with spaces
        ('config(name="my table")', 12, (",", ")"), '"my table"'),
        # Double quotes containing single quotes
        ('config(desc="it\'s a test")', 12, (",", ")"), '"it\'s a test"'),
        # Mixed quotes in SQL
        ("config(sql=\"select 'x' as col\")", 11, (",", ")"), "\"select 'x' as col\""),
    ],
)
def test_source_code_extractor(source, start_pos, delimiters, expected_output):
    """Test _SourceCodeExtractor.extract_until_delimiter with various inputs."""
    extractor = _SourceCodeExtractor(source)
    result = extractor.extract_until_delimiter(start_pos, delimiters=delimiters)
    assert result == expected_output


def test_source_code_extractor_multiline():
    """Test extraction across multiple lines."""
    source = """config(
        key='value',
        other='test'
    )"""
    extractor = _SourceCodeExtractor(source)
    # Start after "key="
    start = source.index("key='") + 4
    result = extractor.extract_until_delimiter(start, delimiters=(",", ")"))
    assert "'value'" in result.strip()


def _extract_first_kwarg(source: str):
    """Helper to extract first kwarg from a config source string."""
    env = get_environment(None, capture_macros=True)
    parsed = env.parse(source)
    func_calls = list(parsed.find_all(jinja2.nodes.Call))
    config_call = func_calls[0]
    return config_call.kwargs[0]


@pytest.mark.parametrize(
    "source,expected_output",
    [
        # Simple string keyword argument
        ("{{ config(materialized='table') }}", "'table'"),
        # env_var() call
        ("{{ config(materialized=env_var('MY_VAR')) }}", "env_var('MY_VAR')"),
        # Complex Jinja expression
        ("{{ config(schema=target.schema + '_' + var('suffix')) }}", "target.schema + '_' + var('suffix')"),
        # List value
        ("{{ config(tags=['tag1', 'tag2']) }}", "['tag1', 'tag2']"),
        # Dict value
        ("{{ config(meta={'key': 'value', 'other': 123}) }}", "{'key': 'value', 'other': 123}"),
        # Double-quoted string value
        ('{{ config(materialized="table") }}', '"table"'),
        # Double quotes with var()
        ('{{ config(schema=var("my_schema")) }}', 'var("my_schema")'),
        # Double quotes in complex expression
        ('{{ config(path=target.schema + "/" + "suffix") }}', 'target.schema + "/" + "suffix"'),
    ],
)
def test_construct_static_kwarg_value(source, expected_output):
    """Test construct_static_kwarg_value with various keyword argument types."""
    kwarg = _extract_first_kwarg(source)
    result = construct_static_kwarg_value(kwarg, source)
    assert result == expected_output


def test_construct_static_kwarg_value_multiline():
    """Test extracting value from multiline config."""
    source = """{{ config(
        materialized='table',
        schema='my_schema'
    ) }}"""
    kwarg = _extract_first_kwarg(source)
    result = construct_static_kwarg_value(kwarg, source)
    assert "'table'" in result


def test_construct_static_kwarg_value_multiple_kwargs():
    """Test extracting when multiple kwargs are present."""
    source = "{{ config(materialized='table', schema='my_schema', enabled=true) }}"

    expected_values = {"materialized": "'table'", "schema": "'my_schema'", "enabled": "true"}

    env = get_environment(None, capture_macros=True)
    parsed = env.parse(source)
    func_calls = list(parsed.find_all(jinja2.nodes.Call))
    config_call = func_calls[0]

    for kwarg in config_call.kwargs:
        result = construct_static_kwarg_value(kwarg, source)
        assert result == expected_values[kwarg.key]


def test_construct_static_kwarg_value_fallback():
    """Test that function falls back to str(kwarg) on error."""
    source = "{{ config(materialized='table') }}"
    kwarg = _extract_first_kwarg(source)

    # Call with empty source - should fall back
    result = construct_static_kwarg_value(kwarg, "")
    assert result is not None
    assert len(result) > 0


@pytest.mark.parametrize(
    "config_str,expected_keys,expected_values",
    [
        # Simple config with multiple values
        (
            "{{ config(materialized='table', schema='my_schema') }}",
            {"materialized", "schema"},
            {"materialized": "'table'", "schema": "'my_schema'"},
        ),
        # Config with Jinja expressions
        (
            "{{ config(materialized=env_var('MAT'), on_error=var('error_policy')) }}",
            {"materialized", "on_error"},
            {"materialized": "env_var('MAT')", "on_error": "var('error_policy')"},
        ),
        # Dict literal with Jinja
        (
            "{{ config({'pre-hook': 'select 1', 'myconf': run_started_at - 1}) }}",
            {"pre-hook", "myconf"},
            {"pre-hook": "'select 1'", "myconf": "run_started_at - 1"},
        ),
        # Complex nested structures
        (
            """{{ config(
            materialized='table',
            tags=['tag1', 'tag2'],
            meta={'key': 'value'},
            pre_hook=["{{ log('Starting') }}", "select 1"]
        ) }}""",
            {"materialized", "tags", "meta", "pre_hook"},
            {
                "materialized": "'table'",
                "tags": "['tag1', 'tag2']",
                "meta": "{'key': 'value'}",
                "pre_hook": """["{{ log('Starting') }}", "select 1"]""",
            },
        ),
        # Double quotes in simple config
        (
            '{{ config(materialized="table", schema="my_schema") }}',
            {"materialized", "schema"},
            {"materialized": '"table"', "schema": '"my_schema"'},
        ),
        # Mixed quotes with Jinja
        (
            '{{ config(custom_config=var("my_var"), other="static") }}',
            {"custom_config", "other"},
            {"custom_config": 'var("my_var")', "other": '"static"'},
        ),
    ],
)
def test_integration_full_workflow(config_str, expected_keys, expected_values):
    """Integration tests for the full workflow from parsing to extraction."""
    result = statically_parse_unrendered_config(config_str)

    assert result is not None
    assert set(result.keys()) == expected_keys

    for key, expected_value in expected_values.items():
        assert key in result
        assert result[key] == expected_value


def test_construct_static_kwarg_value_very_long_value():
    """Test that very long config values (>1000 chars) are extracted and serialized properly.

    This test ensures that the 1000 character limit has been removed from
    construct_static_kwarg_value() so that long config values (like multi-line SQL
    in post_hook) are properly extracted and can be serialized back without
    writing AST object representations.

    Without the fix, extraction would fail for values >1000 chars, falling back to
    str(kwarg) which returns an AST representation like "Keyword(key='post_hook', ...)".
    This AST string would then be written to the file, corrupting it.
    """
    from dbt_autofix.refactors.changesets.dbt_sql import _serialize_config_macro_call

    # Create a long SQL string (over 1000 chars)
    long_sql = "SELECT " + ", ".join([f"column_{i}" for i in range(200)])
    config_str = f"{{{{ config(post_hook='{long_sql}') }}}}"

    # Step 1: Extract the config
    result = statically_parse_unrendered_config(config_str)

    assert result is not None
    assert "post_hook" in result

    # Step 2: Try to serialize it back - this is where the bug would manifest
    # Without the fix, this would try to serialize an AST object string
    serialized = _serialize_config_macro_call(result, result)

    # The serialized output should contain the actual SQL, not AST representations
    assert "post_hook=" in serialized
    assert long_sql in serialized
    # Most importantly: should NOT contain AST object markers
    assert "Keyword(" not in serialized
    assert "Const(" not in serialized
    assert "List(" not in serialized

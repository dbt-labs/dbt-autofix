"""Test cases for Python model config.get to meta_get autofix."""

from dbt_autofix.fields_properties_configs import models_allowed_config
from dbt_autofix.refactors.changesets.dbt_python import (
    move_custom_config_access_to_meta_python,
)


class MockSchemaSpecs:
    """Mock schema specs for testing."""

    def __init__(self):
        self.yaml_specs_per_node_type = {
            "models": models_allowed_config,
        }


def test_basic_python_config_get_refactor():
    """Test basic dbt.config.get() refactoring in Python models."""
    input_python = """def model(dbt, session):
    dbt.config(
        materialized='table',
        meta={'custom_key': 'custom_value'}
    )

    custom = dbt.config.get('custom_key')
    mat = dbt.config.get('materialized')

    return session.sql("SELECT 1")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    assert result.refactored
    assert "dbt.meta_get('custom_key')" in result.refactored_content
    assert "dbt.config.get('materialized')" in result.refactored_content
    assert len(result.deprecation_refactors) == 1


def test_python_config_get_with_default():
    """Test dbt.config.get() with default value in Python models."""
    input_python = """def model(dbt, session):
    custom = dbt.config.get('custom_key', 'default_value')
    another = dbt.config.get('another_key', None)
    return session.sql("SELECT 1")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    assert result.refactored
    assert "dbt.meta_get('custom_key'" in result.refactored_content
    assert "dbt.meta_get('another_key'" in result.refactored_content
    assert len(result.deprecation_refactors) == 2


def test_python_no_refactor_for_dbt_configs():
    """Test that dbt-native configs are not refactored in Python models."""
    input_python = """def model(dbt, session):
    mat = dbt.config.get('materialized')
    uk = dbt.config.get('unique_key')
    cb = dbt.config.get('cluster_by')
    return session.sql("SELECT 1")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    assert not result.refactored
    assert result.refactored_content == input_python
    assert len(result.deprecation_refactors) == 0


def test_python_variable_shadowing_detection():
    """Test that config variable shadowing is detected and skipped."""
    input_python = """def model(dbt, session):
    config = {'my': 'dict'}
    val = dbt.config.get('some_key')
    return session.sql("SELECT 1")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    assert not result.refactored
    assert len(result.refactor_warnings) == 1
    assert "variable assignment" in result.refactor_warnings[0].lower()


def test_python_chained_access_warning():
    """Test that chained access patterns generate warnings."""
    input_python = """def model(dbt, session):
    dict_val = dbt.config.get('custom_dict')
    subkey = dict_val.get('key', 'default')
    return session.sql("SELECT 1")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    assert result.refactored
    assert "dbt.meta_get('custom_dict')" in result.refactored_content
    # No chained access in this case (it's on a separate variable)
    assert len(result.deprecation_refactors) == 1


def test_python_inline_chained_access_warning():
    """Test that inline chained access patterns generate warnings."""
    input_python = """def model(dbt, session):
    val = dbt.config.get('custom_dict').get('key', 'default')
    return session.sql("SELECT 1")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    assert result.refactored
    assert "dbt.meta_get('custom_dict')" in result.refactored_content
    assert len(result.refactor_warnings) >= 1
    assert any("chained access" in w.lower() for w in result.refactor_warnings)


def test_python_mixed_quotes():
    """Test handling of mixed quote styles in Python models."""
    input_python = """def model(dbt, session):
    val1 = dbt.config.get("custom_key1")
    val2 = dbt.config.get('custom_key2')
    return session.sql("SELECT 1")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    assert result.refactored
    # AST unparsing will normalize quotes, but the transformation should happen
    assert "meta_get" in result.refactored_content
    assert len(result.deprecation_refactors) == 2


def test_python_complex_defaults():
    """Test handling of complex default values in Python models."""
    input_python = """def model(dbt, session):
    list_val = dbt.config.get('custom_list', [])
    dict_val = dbt.config.get('custom_dict', {})
    none_val = dbt.config.get('custom_none', None)
    return session.sql("SELECT 1")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    assert result.refactored
    assert "dbt.meta_get('custom_list'" in result.refactored_content
    assert "dbt.meta_get('custom_dict'" in result.refactored_content
    assert "dbt.meta_get('custom_none'" in result.refactored_content
    assert len(result.deprecation_refactors) == 3


def test_python_multiline_model():
    """Test handling of realistic multiline Python models."""
    input_python = """def model(dbt, session):
    \"\"\"My model with custom config.\"\"\"
    dbt.config(
        materialized='table',
        schema='analytics',
        meta={
            'owner': 'data-team',
            'priority': 'high',
            'custom_setting': 'value'
        }
    )

    # Get custom configs
    owner = dbt.config.get('owner', 'default-owner')
    priority = dbt.config.get('priority')
    setting = dbt.config.get('custom_setting', 'default')

    # Get dbt config
    mat = dbt.config.get('materialized')
    schema = dbt.config.get('schema')

    # Build query
    return session.sql(f\"\"\"
        SELECT
            '{owner}' as owner,
            '{priority}' as priority,
            '{setting}' as setting
    \"\"\")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    assert result.refactored

    # Custom configs should be transformed
    assert "dbt.meta_get('owner'" in result.refactored_content
    assert "dbt.meta_get('priority')" in result.refactored_content
    assert "dbt.meta_get('custom_setting'" in result.refactored_content

    # dbt-native configs should NOT be transformed
    assert "dbt.config.get('materialized')" in result.refactored_content
    assert "dbt.config.get('schema')" in result.refactored_content

    assert len(result.deprecation_refactors) == 3


def test_python_syntax_error_handling():
    """Test that syntax errors are handled gracefully."""
    input_python = """def model(dbt, session):
    this is not valid python syntax!!!
    val = dbt.config.get('custom_key')
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    assert not result.refactored
    assert len(result.refactor_warnings) == 1
    assert "parse" in result.refactor_warnings[0].lower()


def test_python_dynamic_key_skipped():
    """Test that dynamic keys are skipped."""
    input_python = """def model(dbt, session):
    key_name = 'my_key'
    val = dbt.config.get(key_name)
    return session.sql("SELECT 1")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    # Should not be refactored since key is dynamic
    assert not result.refactored
    assert "dbt.config.get(key_name)" in result.refactored_content


def test_python_no_args_skipped():
    """Test that config.get() with no arguments is skipped."""
    input_python = """def model(dbt, session):
    # This is invalid but should be handled gracefully
    val = dbt.config.get()
    return session.sql("SELECT 1")
"""

    result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

    # Should not crash, just skip transformation
    assert not result.refactored

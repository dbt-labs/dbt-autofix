"""Unit tests for Python model refactoring functions."""

from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.fields_properties_configs import models_allowed_config
from dbt_autofix.refactors.changesets.dbt_python import (
    move_custom_config_access_to_meta_python,
    refactor_custom_configs_to_meta_python,
)


class MockSchemaSpecs:
    """Mock schema specs for testing."""

    def __init__(self):
        self.yaml_specs_per_node_type = {
            "models": models_allowed_config,
        }


class TestRefactorCustomConfigsToMetaPython:
    """Tests for refactor_custom_configs_to_meta_python function."""

    def test_single_custom_config_moved_to_meta(self):
        """A single custom config should be moved to meta dict."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", random_config="AR")
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        # Check that the config was moved to meta (quote style may vary due to ast.unparse)
        assert "meta={" in result.refactored_content
        assert '"random_config"' in result.refactored_content
        assert "random_config=" not in result.refactored_content
        assert len(result.deprecation_refactors) == 1
        assert result.deprecation_refactors[0].deprecation == DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION
        assert "random_config" in result.deprecation_refactors[0].log

    def test_multiple_custom_configs_moved_to_meta(self):
        """Multiple custom configs should all be moved to meta dict."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", custom_a="A", custom_b="B")
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        assert "meta={" in result.refactored_content
        assert '"custom_a"' in result.refactored_content
        assert '"custom_b"' in result.refactored_content
        assert "custom_a=" not in result.refactored_content
        assert "custom_b=" not in result.refactored_content

    def test_native_configs_preserved(self):
        """Native configs like materialized should not be moved to meta."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", schema="my_schema", custom_key="value")
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        assert "materialized=" in result.refactored_content
        assert "schema=" in result.refactored_content
        assert '"custom_key"' in result.refactored_content
        # custom_key should be inside meta, not a top-level kwarg
        assert "custom_key=" not in result.refactored_content

    def test_no_custom_configs_unchanged(self):
        """File should not change when there are no custom configs."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", schema="my_schema")
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert not result.refactored
        assert result.refactored_content == input_python
        assert len(result.deprecation_refactors) == 0

    def test_no_config_call_unchanged(self):
        """File without dbt.config() call should not change."""
        input_python = """def model(dbt, session):
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert not result.refactored
        assert result.refactored_content == input_python
        assert len(result.deprecation_refactors) == 0

    def test_integer_config_value(self):
        """Custom config with integer value should be preserved correctly."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", custom_count=42)
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        assert '"custom_count": 42' in result.refactored_content

    def test_boolean_config_value(self):
        """Custom config with boolean value should be preserved correctly."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", custom_flag=True)
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        assert '"custom_flag": True' in result.refactored_content


class TestMoveCustomConfigAccessToMetaPython:
    """Tests for move_custom_config_access_to_meta_python function."""

    def test_basic_config_get_refactored(self):
        """Basic dbt.config.get() should be refactored to access meta."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", meta={"random_config": "AR"})
    random_config = dbt.config.get("random_config")
    return session.sql(f"SELECT '{random_config}'")
"""
        expected_python = """def model(dbt, session):
    dbt.config(materialized="table", meta={"random_config": "AR"})
    random_config = dbt.config.get("meta").get("random_config")
    return session.sql(f"SELECT '{random_config}'")
"""
        result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python
        assert len(result.deprecation_refactors) == 1
        assert result.deprecation_refactors[0].deprecation == DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION

    def test_config_get_with_default_value(self):
        """dbt.config.get() with default value should preserve the default."""
        input_python = """def model(dbt, session):
    custom_val = dbt.config.get("custom_key", "default_value")
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    custom_val = dbt.config.get("meta").get("custom_key", "default_value")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python

    def test_multiple_config_get_calls(self):
        """Multiple dbt.config.get() calls should all be refactored."""
        input_python = """def model(dbt, session):
    val_a = dbt.config.get("custom_a")
    val_b = dbt.config.get("custom_b", "default")
    return session.sql(f"SELECT '{val_a}', '{val_b}'")
"""
        result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        assert 'dbt.config.get("meta").get("custom_a")' in result.refactored_content
        assert 'dbt.config.get("meta").get("custom_b", "default")' in result.refactored_content
        assert len(result.deprecation_refactors) == 2

    def test_native_config_access_unchanged(self):
        """Access to native configs like materialized should not change."""
        input_python = """def model(dbt, session):
    mat = dbt.config.get("materialized")
    schema = dbt.config.get("schema")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert not result.refactored
        assert result.refactored_content == input_python
        assert len(result.deprecation_refactors) == 0

    def test_no_config_get_calls_unchanged(self):
        """File without dbt.config.get() calls should not change."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert not result.refactored
        assert result.refactored_content == input_python

    def test_mixed_native_and_custom_config_access(self):
        """Only custom config access should be refactored, native should remain."""
        input_python = """def model(dbt, session):
    mat = dbt.config.get("materialized")
    custom = dbt.config.get("custom_key")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        assert 'dbt.config.get("materialized")' in result.refactored_content
        assert 'dbt.config.get("meta").get("custom_key")' in result.refactored_content
        assert len(result.deprecation_refactors) == 1

    def test_single_quotes_preserved(self):
        """Single quotes in original should work correctly."""
        input_python = """def model(dbt, session):
    custom = dbt.config.get('custom_key')
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        assert 'dbt.config.get("meta").get("custom_key")' in result.refactored_content

    def test_yaml_defined_custom_config_access(self):
        """Custom config defined only in YAML should still have its access refactored.

        This tests the edge case where a Python model only has dbt.config.get()
        without a corresponding dbt.config() call - the config may be defined
        in the schema.yml file instead.
        """
        input_python = """def model(dbt, session):
    # Config is defined in schema.yml, not inline
    custom_val = dbt.config.get("yaml_defined_custom")
    return session.sql(f"SELECT '{custom_val}'")
"""
        expected_python = """def model(dbt, session):
    # Config is defined in schema.yml, not inline
    custom_val = dbt.config.get("meta").get("yaml_defined_custom")
    return session.sql(f"SELECT '{custom_val}'")
"""
        result = move_custom_config_access_to_meta_python(input_python, MockSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python


class TestIntegration:
    """Integration tests combining both refactor functions."""

    def test_full_transformation_pipeline(self):
        """Test the full transformation: move configs to meta, then update access."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", random_config="AR")
    random_config = dbt.config.get("random_config")
    return session.sql(f"SELECT '{random_config}' as config_value")
"""

        # First pass: move configs to meta
        result1 = refactor_custom_configs_to_meta_python(input_python, MockSchemaSpecs(), "models")
        assert result1.refactored
        assert 'meta={"random_config"' in result1.refactored_content

        # Second pass: update config access
        result2 = move_custom_config_access_to_meta_python(result1.refactored_content, MockSchemaSpecs(), "models")
        assert result2.refactored

        # Verify both transformations happened
        assert 'dbt.config.get("meta").get("random_config")' in result2.refactored_content
        assert 'meta={"random_config"' in result2.refactored_content

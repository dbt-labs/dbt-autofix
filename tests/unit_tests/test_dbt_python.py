"""Unit tests for Python model refactoring functions."""

import tempfile
from pathlib import Path

from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.fields_properties_configs import models_allowed_config
from dbt_autofix.refactor import process_python_files
from dbt_autofix.refactors.changesets.dbt_python import (
    move_custom_config_access_to_meta_python,
    refactor_custom_configs_to_meta_python,
)
from dbt_autofix.retrieve_schemas import SchemaSpecs


class FakeSchemaSpecs(SchemaSpecs):
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
        expected_python = """def model(dbt, session):
    dbt.config(materialized="table", meta={"random_config": "AR"})
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python
        assert len(result.deprecation_refactors) == 1
        assert result.deprecation_refactors[0].deprecation == DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION
        assert "random_config" in result.deprecation_refactors[0].log

    def test_multiple_custom_configs_moved_to_meta(self):
        """Multiple custom configs should all be moved to meta dict."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", custom_a="A", custom_b="B")
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    dbt.config(materialized="table", meta={"custom_a": "A", "custom_b": "B"})
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python

    def test_native_configs_preserved(self):
        """Native configs like materialized should not be moved to meta."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", schema="my_schema", custom_key="value")
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    dbt.config(materialized="table", schema="my_schema", meta={"custom_key": "value"})
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python

    def test_no_custom_configs_unchanged(self):
        """File should not change when there are no custom configs."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", schema="my_schema")
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert not result.refactored
        assert result.refactored_content == input_python
        assert len(result.deprecation_refactors) == 0

    def test_no_config_call_unchanged(self):
        """File without dbt.config() call should not change."""
        input_python = """def model(dbt, session):
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert not result.refactored
        assert result.refactored_content == input_python
        assert len(result.deprecation_refactors) == 0

    def test_integer_config_value(self):
        """Custom config with integer value should be preserved correctly."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", custom_count=42)
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    dbt.config(materialized="table", meta={"custom_count": 42})
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python

    def test_boolean_config_value(self):
        """Custom config with boolean value should be preserved correctly."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", custom_flag=True)
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    dbt.config(materialized="table", meta={"custom_flag": True})
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python


class TestMoveCustomConfigAccessToMetaPython:
    """Tests for move_custom_config_access_to_meta_python function."""

    def test_basic_config_get_refactored(self):
        """Basic dbt.config.get() should be refactored to access meta."""
        input_python = """def model(dbt, session):
    random_config = dbt.config.get("random_config")
    return session.sql(f"SELECT '{random_config}'")
"""
        expected_python = """def model(dbt, session):
    random_config = dbt.config.get("meta").get("random_config")
    return session.sql(f"SELECT '{random_config}'")
"""
        result = move_custom_config_access_to_meta_python(input_python, FakeSchemaSpecs(), "models")

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
        result = move_custom_config_access_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python

    def test_multiple_config_get_calls(self):
        """Multiple dbt.config.get() calls should all be refactored."""
        input_python = """def model(dbt, session):
    val_a = dbt.config.get("custom_a")
    val_b = dbt.config.get("custom_b", "default")
    return session.sql(f"SELECT '{val_a}', '{val_b}'")
"""
        expected_python = """def model(dbt, session):
    val_a = dbt.config.get("meta").get("custom_a")
    val_b = dbt.config.get("meta").get("custom_b", "default")
    return session.sql(f"SELECT '{val_a}', '{val_b}'")
"""
        result = move_custom_config_access_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python
        assert len(result.deprecation_refactors) == 2

    def test_native_config_access_unchanged(self):
        """Access to native configs like materialized should not change."""
        input_python = """def model(dbt, session):
    mat = dbt.config.get("materialized")
    schema = dbt.config.get("schema")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert not result.refactored
        assert result.refactored_content == input_python
        assert len(result.deprecation_refactors) == 0

    def test_no_config_get_calls_unchanged(self):
        """File without dbt.config.get() calls should not change."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert not result.refactored
        assert result.refactored_content == input_python

    def test_mixed_native_and_custom_config_access(self):
        """Only custom config access should be refactored, native should remain."""
        input_python = """def model(dbt, session):
    mat = dbt.config.get("materialized")
    custom = dbt.config.get("custom_key")
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    mat = dbt.config.get("materialized")
    custom = dbt.config.get("meta").get("custom_key")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python
        assert len(result.deprecation_refactors) == 1

    def test_single_quotes_converted_to_double(self):
        """Single quotes in original should work correctly."""
        input_python = """def model(dbt, session):
    custom = dbt.config.get('custom_key')
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    custom = dbt.config.get("meta").get("custom_key")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(input_python, FakeSchemaSpecs(), "models")

        assert result.refactored
        assert result.refactored_content == expected_python


class TestIntegration:
    """Integration tests using process_python_files with actual files."""

    def test_multiple_files_transformed(self):
        """Test that multiple Python files are all processed."""
        input_with_config = """def model(dbt, session):
    dbt.config(custom_key="value")
    return session.sql("SELECT 1")
"""
        expected_with_config = """def model(dbt, session):
    dbt.config(meta={"custom_key": "value"})
    return session.sql("SELECT 1")
"""
        input_with_access = """def model(dbt, session):
    val = dbt.config.get("custom_key")
    return session.sql("SELECT 1")
"""
        expected_with_access = """def model(dbt, session):
    val = dbt.config.get("meta").get("custom_key")
    return session.sql("SELECT 1")
"""

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            models_path = project_path / "models"
            models_path.mkdir()

            (models_path / "model_a.py").write_text(input_with_config)
            (models_path / "model_b.py").write_text(input_with_access)

            results = process_python_files(
                path=project_path,
                python_paths_to_node_type={"models": "models"},
                schema_specs=FakeSchemaSpecs(),
            )

            assert len(results) == 2
            results_by_name = {r.file_path.name: r for r in results}

            assert results_by_name["model_a.py"].refactored
            assert results_by_name["model_a.py"].refactored_content == expected_with_config

            assert results_by_name["model_b.py"].refactored
            assert results_by_name["model_b.py"].refactored_content == expected_with_access

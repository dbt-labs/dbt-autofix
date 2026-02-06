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
        result = refactor_custom_configs_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

        assert result.refactored
        assert result.refactored_content == expected_python
        assert len(result.deprecation_refactors) == 1
        assert (
            result.deprecation_refactors[0].deprecation
            == DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION
        )
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
        result = refactor_custom_configs_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

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
        result = refactor_custom_configs_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

        assert result.refactored
        assert result.refactored_content == expected_python

    def test_no_custom_configs_unchanged(self):
        """File should not change when there are no custom configs."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table", schema="my_schema")
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

        assert not result.refactored
        assert result.refactored_content == input_python
        assert len(result.deprecation_refactors) == 0

    def test_no_config_call_unchanged(self):
        """File without dbt.config() call should not change."""
        input_python = """def model(dbt, session):
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

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
        result = refactor_custom_configs_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

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
        result = refactor_custom_configs_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

        assert result.refactored
        assert result.refactored_content == expected_python

    def test_complex_config_with_existing_meta(self):
        """Complex case: native configs, existing meta keys, and custom keys to move."""
        input_python = """def model(dbt, session):
    dbt.config(
        materialized="table",
        schema="my_schema",
        tags=["daily"],
        meta={"already_meta_a": "A", "already_meta_b": "B"},
        custom_x="X",
        custom_y="Y",
    )
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    dbt.config(materialized="table", schema="my_schema", tags=['daily'], meta={"already_meta_a": "A", "already_meta_b": "B", "custom_x": "X", "custom_y": "Y"})
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

        assert result.refactored
        assert result.refactored_content == expected_python
        assert len(result.deprecation_refactors) == 1
        assert "custom_x" in result.deprecation_refactors[0].log
        assert "custom_y" in result.deprecation_refactors[0].log

    def test_quote_style_normalized_to_double(self):
        """Config values are normalized to double quotes since AST reconstructs the call."""
        input_python = """def model(dbt, session):
    dbt.config(materialized='table', custom_key='single_quoted')
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    dbt.config(materialized="table", meta={"custom_key": "single_quoted"})
    return session.sql("SELECT 1")
"""
        result = refactor_custom_configs_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

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
    random_config = dbt.config.meta_get("random_config")
    return session.sql(f"SELECT '{random_config}'")
"""
        result = move_custom_config_access_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

        assert result.refactored
        assert result.refactored_content == expected_python
        assert len(result.deprecation_refactors) == 1
        assert (
            result.deprecation_refactors[0].deprecation
            == DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION
        )

    def test_config_get_with_default_value(self):
        """dbt.config.get() with default value should preserve the default."""
        input_python = """def model(dbt, session):
    custom_val = dbt.config.get("custom_key", "default_value")
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    custom_val = dbt.config.meta_get("custom_key", "default_value")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

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
    val_a = dbt.config.meta_get("custom_a")
    val_b = dbt.config.meta_get("custom_b", "default")
    return session.sql(f"SELECT '{val_a}', '{val_b}'")
"""
        result = move_custom_config_access_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

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
        result = move_custom_config_access_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

        assert not result.refactored
        assert result.refactored_content == input_python
        assert len(result.deprecation_refactors) == 0

    def test_no_config_get_calls_unchanged(self):
        """File without dbt.config.get() calls should not change."""
        input_python = """def model(dbt, session):
    dbt.config(materialized="table")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

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
    custom = dbt.config.meta_get("custom_key")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

        assert result.refactored
        assert result.refactored_content == expected_python
        assert len(result.deprecation_refactors) == 1

    def test_quote_style_preserved(self):
        """Both single and double quote styles should be preserved in output."""
        input_python = """def model(dbt, session):
    val_a = dbt.config.get("double_quoted")
    val_b = dbt.config.get('single_quoted')
    val_c = dbt.config.get("with_default", "fallback")
    val_d = dbt.config.get('with_default_single', 'fallback')
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    val_a = dbt.config.meta_get("double_quoted")
    val_b = dbt.config.meta_get('single_quoted')
    val_c = dbt.config.meta_get("with_default", "fallback")
    val_d = dbt.config.meta_get('with_default_single', 'fallback')
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

        assert result.refactored
        assert result.refactored_content == expected_python
        assert len(result.deprecation_refactors) == 4

    def test_complex_access_with_native_and_existing_meta_access(self):
        """Complex case: native config access, existing meta access, and custom keys to refactor."""
        input_python = """def model(dbt, session):
    mat = dbt.config.get("materialized")
    schema = dbt.config.get("schema")
    tags = dbt.config.get("tags")
    already_meta_a = dbt.config.meta_get("already_meta_a")
    already_meta_b = dbt.config.meta_get("already_meta_b")
    custom_x = dbt.config.get("custom_x")
    custom_y = dbt.config.get("custom_y", "default")
    return session.sql("SELECT 1")
"""
        expected_python = """def model(dbt, session):
    mat = dbt.config.get("materialized")
    schema = dbt.config.get("schema")
    tags = dbt.config.get("tags")
    already_meta_a = dbt.config.meta_get("already_meta_a")
    already_meta_b = dbt.config.meta_get("already_meta_b")
    custom_x = dbt.config.meta_get("custom_x")
    custom_y = dbt.config.meta_get("custom_y", "default")
    return session.sql("SELECT 1")
"""
        result = move_custom_config_access_to_meta_python(
            input_python, FakeSchemaSpecs(), "models"
        )

        assert result.refactored
        assert result.refactored_content == expected_python
        assert len(result.deprecation_refactors) == 2


class TestIntegration:
    """Integration tests using process_python_files with actual files.

    These run both refactors (config-to-meta and config.get-to-meta_get)
    through the full pipeline, verifying that surrounding code structure
    (comments, docstrings, imports, blank lines) is preserved.
    """

    def test_full_pipeline_preserves_structure(self):
        """Both refactors applied through process_python_files preserve surrounding code."""
        input_python = """import pandas as pd

# Constants
REPORT_NAME = "metrics"


def model(dbt, session):
    \"\"\"Calculates customer metrics.

    This model aggregates data from multiple sources
    and applies custom classification logic.
    \"\"\"
    # Configure the model
    dbt.config(materialized="table", refresh_frequency="daily")

    # Get custom classification
    classification = dbt.config.get("data_classification")

    # Build the query
    query = f"SELECT '{classification}' as data_class, current_date as report_date"

    return session.sql(query)
"""
        expected_python = """import pandas as pd

# Constants
REPORT_NAME = "metrics"


def model(dbt, session):
    \"\"\"Calculates customer metrics.

    This model aggregates data from multiple sources
    and applies custom classification logic.
    \"\"\"
    # Configure the model
    dbt.config(materialized="table", meta={"refresh_frequency": "daily"})

    # Get custom classification
    classification = dbt.config.meta_get("data_classification")

    # Build the query
    query = f"SELECT '{classification}' as data_class, current_date as report_date"

    return session.sql(query)
"""

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            models_path = project_path / "models"
            models_path.mkdir()

            (models_path / "customer_metrics.py").write_text(input_python)

            results = process_python_files(
                path=project_path,
                python_paths_to_node_type={"models": "models"},
                schema_specs=FakeSchemaSpecs(),
            )

            assert len(results) == 1
            result = results[0]
            assert result.refactored
            assert result.refactored_content == expected_python

    def test_multiple_files_transformed(self):
        """Multiple Python files are all processed through the full pipeline."""
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
    val = dbt.config.meta_get("custom_key")
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
            assert (
                results_by_name["model_a.py"].refactored_content == expected_with_config
            )

            assert results_by_name["model_b.py"].refactored
            assert (
                results_by_name["model_b.py"].refactored_content == expected_with_access
            )

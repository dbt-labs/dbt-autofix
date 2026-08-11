"""Tests for skipping individual rules via skip_rules / --skip-rule."""

from pathlib import Path

import pytest

from dbt_autofix.refactor import process_dbt_project_yml
from dbt_autofix.retrieve_schemas import SchemaSpecs

DBT_PROJECT_YML = """\
name: "demo"
version: "1.0.0"
profile: "demo"
models:
  demo:
    +materialized: external
    +options:
      partition_by: "country"
"""


@pytest.fixture(scope="module")
def schema_specs():
    return SchemaSpecs()


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    (tmp_path / "dbt_project.yml").write_text(DBT_PROJECT_YML)
    (tmp_path / "models").mkdir()
    return tmp_path


def _rule_names(result) -> set[str]:
    return {r.rule_name for r in result.refactors if r.refactored}


def test_rule_applied_by_default(project_dir, schema_specs):
    """Without skip_rules, prefix_plus_for_config runs and moves +options to +meta."""
    result = process_dbt_project_yml(project_dir, schema_specs, dry_run=True)

    assert "prefix_plus_for_config" in _rule_names(result)
    assert "+meta" in result.refactored_yaml


def test_skipped_rule_does_not_run(project_dir, schema_specs):
    """With the rule skipped, +options is left alone."""
    result = process_dbt_project_yml(project_dir, schema_specs, dry_run=True, skip_rules={"prefix_plus_for_config"})

    assert "prefix_plus_for_config" not in _rule_names(result)
    assert "+options" in result.refactored_yaml
    assert "+meta" not in result.refactored_yaml


def test_skipping_one_rule_leaves_others_running(project_dir, schema_specs):
    """Skipping a rule must not disable the rest of the run."""
    result = process_dbt_project_yml(project_dir, schema_specs, dry_run=True, skip_rules={"prefix_plus_for_config"})

    assert _rule_names(result), "expected other rules to still apply"


def test_unknown_rule_name_is_a_no_op(project_dir, schema_specs):
    """An unrecognized rule name should not change behaviour."""
    baseline = process_dbt_project_yml(project_dir, schema_specs, dry_run=True)
    result = process_dbt_project_yml(project_dir, schema_specs, dry_run=True, skip_rules={"not_a_real_rule"})

    assert _rule_names(result) == _rule_names(baseline)

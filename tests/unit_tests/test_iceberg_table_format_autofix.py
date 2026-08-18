from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from typing import cast

import pytest
from ruamel.yaml.comments import CommentedMap

from dbt_autofix.refactors.changesets.dbt_project_yml import (
    changeset_iceberg_table_format_project_yml,
    project_has_unsafe_table_format,
)
from dbt_autofix.refactors.changesets.dbt_schema_yml import changeset_iceberg_table_format_yml
from dbt_autofix.refactors.changesets.dbt_sql import refactor_iceberg_table_format_sql
from dbt_autofix.refactors.results import (
    DbtProjectYMLRefactorConfig,
    SQLContent,
    SQLRefactorConfig,
    YMLContent,
    YMLRefactorConfig,
    YMLRefactorResult,
)
from dbt_autofix.refactors.yml import load_yaml
from dbt_autofix.retrieve_schemas import SchemaSpecs

SQL_CONFIG = SQLRefactorConfig(cast(SchemaSpecs, None), "model")
YML_CONFIG = YMLRefactorConfig(cast(SchemaSpecs, None))
ICEBERG_SETTINGS = ("external_volume", "base_location_root", "base_location_subpath")


def run_sql_rule(content, config=SQL_CONFIG):
    return refactor_iceberg_table_format_sql(SQLContent(content, content, Path("model.sql")), config)


def run_schema_rule(content, config=YML_CONFIG):
    return changeset_iceberg_table_format_yml(YMLContent(content, cast(CommentedMap, None), content), config)


@pytest.mark.parametrize("setting", ICEBERG_SETTINGS)
def test_sql_adds_iceberg_and_is_idempotent(setting):
    content = f"{{{{ config({setting}='value') }}}}\nselect 1"
    result = run_sql_rule(content)
    assert "table_format='iceberg'" in result.refactored_content
    rerun = run_sql_rule(result.refactored_content)
    assert not rerun.refactored


def test_sql_adds_iceberg_for_all_settings():
    content = "{{ config(external_volume='vol', base_location_root='root', base_location_subpath='sub') }}"
    result = run_sql_rule(content)
    assert "table_format='iceberg'" in result.refactored_content


def test_sql_preserves_iceberg_and_default_conflict():
    for table_format, changed, warning in [("iceberg", False, False), ("default", False, True)]:
        content = f"{{{{ config(table_format='{table_format}', external_volume='vol') }}}}"
        result = run_sql_rule(content)
        assert result.refactored is changed
        if warning:
            assert "Cannot safely" in result.refactor_warnings[0]
        else:
            assert result.refactor_warnings == []


def test_sql_does_not_warn_for_unrelated_config_shapes():
    for content in (
        "{{ config({'sql_header': 'select 2 ** 3'}) }}\nselect 1",
        "{{ config(sql_header='select 2 ** 3') }}\nselect 1",
    ):
        result = run_sql_rule(content)
        assert result.refactor_warnings == []


def test_model_level_autofix_respects_project_default():
    content = "{{ config(external_volume='vol') }}\nselect 1"
    sql_result = refactor_iceberg_table_format_sql(
        SQLContent(content, content, Path("model.sql")),
        SQLRefactorConfig(cast(SchemaSpecs, None), "model", project_has_unsafe_table_format=True),
    )
    assert not sql_result.refactored
    assert sql_result.refactor_warnings

    yaml_content = """models:
  - name: model
    config:
      external_volume: vol
"""
    yaml_result = changeset_iceberg_table_format_yml(
        YMLContent(yaml_content, cast(CommentedMap, None), yaml_content),
        YMLRefactorConfig(cast(SchemaSpecs, None), project_has_unsafe_table_format=True),
    )
    assert not yaml_result.refactored
    assert yaml_result.refactor_warnings


def test_project_dynamic_table_format_blocks_model_autofix():
    project_yml = load_yaml("models:\n  project:\n    +table_format: \"{{ env_var('TABLE_FORMAT') }}\"\n")
    assert project_has_unsafe_table_format(project_yml)


def test_sql_dynamic_kwargs_detection_ignores_string_contents():
    content = "{{ config(external_volume='vol', sql_header='select 2 ** 3') }}\nselect 1"
    result = run_sql_rule(content, SQLRefactorConfig(cast(SchemaSpecs, None), "model"))
    assert result.refactored
    assert result.refactor_warnings == []


def test_sql_dynamic_kwargs_are_not_autofixed():
    content = "{{ config(external_volume='vol', **model_config) }}\nselect 1"
    result = run_sql_rule(content)
    assert not result.refactored
    assert "dynamic keyword" in result.refactor_warnings[0]


def test_malformed_project_yaml_raises_so_the_caller_can_fail_closed():
    # project_has_unsafe_table_format takes an already-parsed dbt_project.yml; a
    # malformed file fails during that parse, and refactor.py treats the failure
    # as unsafe (fail closed) rather than silently allowing the autofix.
    with pytest.raises(Exception):
        load_yaml("models: [unclosed")


def test_snapshot_yaml_adds_iceberg():
    content = """snapshots:
  - name: snapshot
    config:
      external_volume: vol
"""
    result = run_schema_rule(content)
    assert "table_format: iceberg" in result.refactored_yaml


def test_empty_project_config_does_not_block_model_autofix():
    assert not project_has_unsafe_table_format(load_yaml(""))


def test_yaml_warning_is_reported_without_marking_file_changed():
    content = """models:
  - name: explicit
    config:
      table_format: default
      external_volume: vol
"""
    result = YMLRefactorResult(
        dry_run=True,
        file_path=Path("schema.yml"),
        original_parsed=cast(CommentedMap, None),
        refactored_yaml=content,
        original_yaml=content,
        refactors=[],
    )
    result.apply_changeset(changeset_iceberg_table_format_yml, YML_CONFIG)
    output = StringIO()
    with redirect_stdout(output):
        result.print_to_console(json_output=True)
    assert not result.refactored
    assert '"refactors": []' in output.getvalue()
    assert "Cannot safely" in output.getvalue()


@pytest.mark.parametrize("setting", ICEBERG_SETTINGS)
@pytest.mark.parametrize("table_format", [None, "iceberg"])
def test_schema_yaml_adds_iceberg_in_config(setting, table_format):
    content = f"""models:
  - name: implicit
    config:
      {setting}: value
      {"table_format: " + table_format if table_format else ""}
"""
    result = run_schema_rule(content)
    assert result.refactored is (table_format is None)
    if table_format is None:
        assert "table_format: iceberg" in result.refactored_yaml
        rerun = run_schema_rule(result.refactored_yaml)
        assert not rerun.refactored


def test_schema_yaml_preserves_default():
    content = """models:
  - name: implicit
    config:
      external_volume: vol
  - name: explicit
    config:
      table_format: default
      external_volume: vol
"""
    result = run_schema_rule(content)
    assert "table_format: iceberg" in result.refactored_yaml
    assert "table_format: default" in result.refactored_yaml
    assert "Cannot safely" in result.refactor_warnings[0]


def test_schema_yaml_handles_all_iceberg_settings_at_legacy_model_level():
    content = """models:
  - name: model
    external_volume: vol
    base_location_root: root
    base_location_subpath: sub
"""
    result = run_schema_rule(content)
    assert "config:\n      table_format: iceberg" in result.refactored_yaml


@pytest.mark.parametrize("setting", ICEBERG_SETTINGS)
@pytest.mark.parametrize("table_format", [None, "iceberg"])
def test_project_yaml_adds_iceberg_in_deprecated_config_path(setting, table_format):
    content = f"""models:
  my_project:
    +{setting}: value
    {f"+table_format: {table_format}" if table_format else ""}
"""
    result = changeset_iceberg_table_format_project_yml(
        YMLContent(content, cast(CommentedMap, None), content),
        DbtProjectYMLRefactorConfig(cast(SchemaSpecs, None), Path(".")),
    )
    assert result.refactored is (table_format is None)
    if table_format is None:
        assert "+table_format: iceberg" in result.refactored_yaml
        rerun = changeset_iceberg_table_format_project_yml(
            YMLContent(result.refactored_yaml, cast(CommentedMap, None), result.refactored_yaml),
            DbtProjectYMLRefactorConfig(cast(SchemaSpecs, None), Path(".")),
        )
        assert not rerun.refactored


def test_project_yaml_adds_iceberg_for_all_settings():
    content = """models:
  my_project:
    +external_volume: vol
    +base_location_root: root
    +base_location_subpath: sub
"""
    result = changeset_iceberg_table_format_project_yml(
        YMLContent(content, cast(CommentedMap, None), content),
        DbtProjectYMLRefactorConfig(cast(SchemaSpecs, None), Path(".")),
    )
    assert "+table_format: iceberg" in result.refactored_yaml


def test_project_yaml_respects_parent_default_and_skips_meta_values():
    content = """models:
  my_project:
    +table_format: default
    staging:
      +external_volume: vol
      +meta:
        external_volume: note
"""
    result = changeset_iceberg_table_format_project_yml(
        YMLContent(content, cast(CommentedMap, None), content),
        DbtProjectYMLRefactorConfig(cast(SchemaSpecs, None), Path(".")),
    )
    assert not result.refactored
    assert result.refactor_warnings
    assert result.refactored_yaml == content

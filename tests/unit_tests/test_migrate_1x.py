"""Unit tests for the deterministic dbt 1.x migration rules and the migrate-1x driver."""

from pathlib import Path

from ruamel.yaml.comments import CommentedMap

from dbt_autofix.migrate_1x import _active_rules, _version_tuple, migrate_1x_all_files
from dbt_autofix.refactors.changesets.dbt_1x import (
    changeset_1x_clean_targets,
    changeset_1x_rename_predicates_project,
    changeset_1x_rename_predicates_yml,
    changeset_1x_tests_to_data_tests_project,
    changeset_1x_tests_to_data_tests_yml,
    refactor_1x_current_timestamp,
    refactor_1x_rename_predicates_sql,
)
from dbt_autofix.refactors.results import (
    DbtProjectYMLRefactorConfig,
    SQLContent,
    SQLRefactorConfig,
    YMLContent,
    YMLRefactorConfig,
)


def _yml(content: str) -> YMLContent:
    return YMLContent(original_str=content, original_parsed=CommentedMap(), current_str=content)


def _sql(content: str) -> SQLContent:
    return SQLContent(original_str=content, current_str=content, current_file_path=Path("model.sql"))


# ---------------------------------------------------------------------------
# current_timestamp
# ---------------------------------------------------------------------------


def test_current_timestamp_renamed():
    result = refactor_1x_current_timestamp(_sql("select {{ dbt_utils.current_timestamp() }}"), SQLRefactorConfig())
    assert result.refactored
    assert result.refactored_content == "select {{ dbt.current_timestamp() }}"


def test_current_timestamp_in_utc_left_alone():
    src = "select {{ dbt_utils.current_timestamp_in_utc() }}"
    result = refactor_1x_current_timestamp(_sql(src), SQLRefactorConfig())
    assert not result.refactored
    assert result.refactored_content == src


def test_current_timestamp_noop_when_absent():
    result = refactor_1x_current_timestamp(_sql("select 1"), SQLRefactorConfig())
    assert not result.refactored


# ---------------------------------------------------------------------------
# predicates -> incremental_predicates
# ---------------------------------------------------------------------------


def test_predicates_sql_in_config():
    src = "{{ config(materialized='incremental', predicates=['1=1']) }}\nselect 1"
    result = refactor_1x_rename_predicates_sql(_sql(src), SQLRefactorConfig())
    assert result.refactored
    assert "incremental_predicates=['1=1']" in result.refactored_content
    assert "predicates=['1=1']" not in result.refactored_content.replace("incremental_predicates", "X")


def test_predicates_sql_already_renamed_is_noop():
    src = "{{ config(incremental_predicates=['1=1']) }}\nselect 1"
    result = refactor_1x_rename_predicates_sql(_sql(src), SQLRefactorConfig())
    assert not result.refactored


def test_predicates_sql_outside_config_untouched():
    # `predicates` appearing outside a config() call must not be renamed.
    src = "select predicates=1 as x"
    result = refactor_1x_rename_predicates_sql(_sql(src), SQLRefactorConfig())
    assert not result.refactored


def test_predicates_yaml_config_block():
    src = "models:\n  - name: m\n    config:\n      predicates: ['x = y']\n"
    result = changeset_1x_rename_predicates_yml(_yml(src), YMLRefactorConfig())
    assert result.refactored
    assert "incremental_predicates:" in result.refactored_yaml
    assert "predicates:" not in result.refactored_yaml.replace("incremental_predicates:", "X")


def test_predicates_project_plus_prefixed():
    src = "models:\n  my_project:\n    incr:\n      +predicates: ['a = b']\n"
    result = changeset_1x_rename_predicates_project(_yml(src), DbtProjectYMLRefactorConfig(root_path=Path(".")))
    assert result.refactored
    assert "+incremental_predicates:" in result.refactored_yaml


# ---------------------------------------------------------------------------
# clean-targets
# ---------------------------------------------------------------------------


def test_clean_targets_removes_source_and_outside_paths():
    src = "model-paths: ['models']\nclean-targets:\n  - target\n  - dbt_packages\n  - models\n  - ../outside\n"
    result = changeset_1x_clean_targets(_yml(src), DbtProjectYMLRefactorConfig(root_path=Path(".")))
    assert result.refactored
    assert "target" in result.refactored_yaml
    assert "dbt_packages" in result.refactored_yaml
    assert "- models" not in result.refactored_yaml
    assert "../outside" not in result.refactored_yaml


def test_clean_targets_noop_when_all_safe():
    src = "clean-targets:\n  - target\n  - dbt_packages\n"
    result = changeset_1x_clean_targets(_yml(src), DbtProjectYMLRefactorConfig(root_path=Path(".")))
    assert not result.refactored


def test_clean_targets_uses_default_source_paths():
    # No model-paths configured -> default "models" must still be protected/removed.
    src = "clean-targets:\n  - target\n  - models\n"
    result = changeset_1x_clean_targets(_yml(src), DbtProjectYMLRefactorConfig(root_path=Path(".")))
    assert result.refactored
    assert "- models" not in result.refactored_yaml


def test_clean_targets_keeps_absolute_path_inside_project(tmp_path):
    # dbt resolves clean-targets against the project dir, so an absolute path that lives inside
    # the project is valid and must NOT be removed just for being absolute.
    inside = tmp_path / "target"
    src = f"clean-targets:\n  - {inside}\n"
    result = changeset_1x_clean_targets(_yml(src), DbtProjectYMLRefactorConfig(root_path=tmp_path))
    assert not result.refactored


def test_clean_targets_removes_absolute_path_outside_project(tmp_path):
    outside = tmp_path.parent / "somewhere_else"
    src = f"clean-targets:\n  - target\n  - {outside}\n"
    result = changeset_1x_clean_targets(_yml(src), DbtProjectYMLRefactorConfig(root_path=tmp_path))
    assert result.refactored
    assert str(outside) not in result.refactored_yaml
    assert "target" in result.refactored_yaml


def test_clean_targets_removes_absolute_source_path(tmp_path):
    # An absolute path that resolves to a configured source path is still a source path.
    src = f"model-paths: ['models']\nclean-targets:\n  - {tmp_path / 'models'}\n"
    result = changeset_1x_clean_targets(_yml(src), DbtProjectYMLRefactorConfig(root_path=tmp_path))
    assert result.refactored
    assert "is a configured source path" in result.deprecation_refactors[0].log


# ---------------------------------------------------------------------------
# require_explicit_package_overrides — deliberately NOT automated
# ---------------------------------------------------------------------------


def test_require_explicit_overrides_flag_is_never_written(tmp_path):
    """The 1.8 package-override flag must be left alone.

    Writing `false` only makes sense if the project actually depends on a package overriding a
    built-in materialization, which we cannot establish from project files alone. Guards against
    the rule being reintroduced into the deterministic set.
    """
    (tmp_path / "dbt_project.yml").write_text("name: p\nprofile: p\nmodel-paths: ['models']\n")
    (tmp_path / "models").mkdir()

    yaml_results, _ = migrate_1x_all_files(tmp_path, dry_run=True)

    for result in yaml_results:
        assert "require_explicit_package_overrides_for_builtin_materializations" not in result.refactored_yaml


# ---------------------------------------------------------------------------
# tests -> data_tests
# ---------------------------------------------------------------------------


def test_tests_to_data_tests_yaml_nested():
    src = (
        "models:\n  - name: m\n    tests:\n      - unique\n"
        "    columns:\n      - name: id\n        tests:\n          - not_null\n"
    )
    result = changeset_1x_tests_to_data_tests_yml(_yml(src), YMLRefactorConfig())
    assert result.refactored
    assert "data_tests:" in result.refactored_yaml
    assert "\n    tests:" not in result.refactored_yaml


def test_tests_to_data_tests_skips_when_data_tests_present():
    src = "models:\n  - name: m\n    tests:\n      - unique\n    data_tests:\n      - not_null\n"
    result = changeset_1x_tests_to_data_tests_yml(_yml(src), YMLRefactorConfig())
    # tests + data_tests both present at same level -> do not clobber
    assert not result.refactored


def test_tests_to_data_tests_project_top_level():
    src = "tests:\n  +store_failures: true\n"
    result = changeset_1x_tests_to_data_tests_project(_yml(src), DbtProjectYMLRefactorConfig())
    assert result.refactored
    assert result.refactored_yaml.startswith("data_tests:")


# ---------------------------------------------------------------------------
# version registry / filtering
# ---------------------------------------------------------------------------


def test_version_tuple():
    assert _version_tuple("1.8") == (1, 8)
    assert _version_tuple(" 1.3 ") == (1, 3)


def test_active_rules_full_range():
    active = _active_rules("1.3", "1.8")
    assert len(active) == 7


def test_active_rules_to_1_6_only_1_4_rules():
    active = _active_rules("1.3", "1.6")
    assert {rule.introduced for rule in active} == {"1.4"}


def test_active_rules_to_1_8_includes_all_tiers():
    active = _active_rules("1.3", "1.8")
    assert {rule.introduced for rule in active} == {"1.4", "1.7", "1.8"}

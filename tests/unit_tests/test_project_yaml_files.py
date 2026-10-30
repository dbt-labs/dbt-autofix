"""Tests for one-pass YAML discovery and semantic definition collection."""

from collections import Counter
from pathlib import Path
from textwrap import dedent
from unittest.mock import MagicMock, patch

import pytest

from dbt_autofix import semantic_definitions
from dbt_autofix.jinja import statically_parse_ref
from dbt_autofix.refactor import process_yaml_files_except_dbt_project
from dbt_autofix.refactors.results import YMLRefactorResult
from dbt_autofix.refactors.yml import iter_project_yaml_files
from dbt_autofix.semantic_definitions import SemanticDefinitions


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(content).lstrip(), encoding="utf-8")
    return path


class TestIterProjectYamlFiles:
    def test_finds_yml_and_yaml_sorted(self, tmp_path: Path):
        b = _write(tmp_path / "models" / "b.yaml", "b: 1\n")
        a = _write(tmp_path / "models" / "sub" / "a.yml", "a: 1\n")
        _write(tmp_path / "models" / "model.sql", "select 1\n")
        assert iter_project_yaml_files(tmp_path, ["models"]) == sorted([a.resolve(), b.resolve()])

    def test_nested_and_duplicate_paths_are_walked_once(self, tmp_path: Path):
        a = _write(tmp_path / "models" / "a.yml", "a: 1\n")
        b = _write(tmp_path / "models" / "semantic" / "b.yml", "b: 1\n")
        with patch.object(Path, "glob", autospec=True, side_effect=Path.glob) as glob:
            files = iter_project_yaml_files(tmp_path, ["models/semantic", "models", "models"])
        assert files == [a.resolve(), b.resolve()]
        assert {call.args[0] for call in glob.call_args_list} == {(tmp_path / "models").resolve()}

    def test_sibling_paths_with_common_prefix_are_both_walked(self, tmp_path: Path):
        a = _write(tmp_path / "models" / "a.yml", "a: 1\n")
        b = _write(tmp_path / "models_extra" / "b.yml", "b: 1\n")
        assert iter_project_yaml_files(tmp_path, ["models", "models_extra"]) == [a.resolve(), b.resolve()]

    def test_missing_paths_are_ignored(self, tmp_path: Path):
        a = _write(tmp_path / "models" / "a.yml", "a: 1\n")
        assert iter_project_yaml_files(tmp_path, ["models", "does_not_exist"]) == [a.resolve()]


class TestSemanticDefinitionsCollection:
    def test_parses_each_file_once(self, tmp_path: Path):
        _write(tmp_path / "models" / "a.yml", "models:\n  - name: a\n")
        _write(tmp_path / "models" / "b.yml", "metrics:\n  - name: m\n")
        with patch.object(semantic_definitions, "load_yaml", wraps=semantic_definitions.load_yaml) as load_yaml:
            SemanticDefinitions(tmp_path, ["models", "models"])
        assert load_yaml.call_count == 2

    def test_collects_semantic_models_models_and_metrics(self, tmp_path: Path):
        _write(
            tmp_path / "models" / "schema.yml",
            """
            models:
              - name: orders
              - name: customers
                versions:
                  - v: 1
                  - v: 2
            semantic_models:
              - name: orders_sm
                model: ref('orders')
            metrics:
              - name: revenue
            """,
        )
        sd = SemanticDefinitions(tmp_path, ["models"])
        assert sd.model_yml_keys == {("orders", None), ("customers", 1), ("customers", 2)}
        assert sd.get_semantic_model("orders")["name"] == "orders_sm"
        assert list(sd.initial_metrics) == ["revenue"]

    def test_versioned_model_key_matches_versioned_ref(self, tmp_path: Path):
        """`v: 2` in YAML and `ref('m', v=2)` must produce the same model key."""
        _write(
            tmp_path / "models" / "schema.yml",
            """
            models:
              - name: m
                versions:
                  - v: 2
            semantic_models:
              - name: m_sm
                model: ref('m', v=2)
            """,
        )
        sd = SemanticDefinitions(tmp_path, ["models"])
        [semantic_model] = sd.semantic_models.values()
        model_key = sd.get_model_key_for_semantic_model(semantic_model)
        assert model_key is not None
        assert sd.model_key_exists_for_semantic_model(model_key)

    @pytest.mark.parametrize("value", ["", "null", "foo", "{name: x}"])
    def test_non_list_top_level_values_are_ignored(self, tmp_path: Path, value: str):
        _write(
            tmp_path / "models" / "schema.yml",
            f"models: {value}\nsemantic_models: {value}\nmetrics: {value}\n",
        )
        sd = SemanticDefinitions(tmp_path, ["models"])
        assert (sd.semantic_models, sd.model_yml_keys, sd.initial_metrics) == ({}, set(), {})

    def test_semantic_model_without_parseable_ref_is_skipped(self, tmp_path: Path):
        _write(
            tmp_path / "models" / "schema.yml",
            """
            semantic_models:
              - name: no_model
              - name: bad_ref
                model: ref('unterminated
            """,
        )
        assert SemanticDefinitions(tmp_path, ["models"]).semantic_models == {}

    def test_unparseable_file_is_skipped_with_warning(self, tmp_path: Path, capsys: pytest.CaptureFixture):
        bad = _write(tmp_path / "models" / "bad.yml", "{ not closed\n")
        _write(tmp_path / "models" / "good.yml", "models:\n  - name: a\n")
        sd = SemanticDefinitions(tmp_path, ["models"])
        assert sd.model_yml_keys == {("a", None)}
        assert bad.name in capsys.readouterr().err


def test_statically_parse_ref_returns_none_on_extraction_error():
    assert statically_parse_ref("ref('unterminated") is None


class TestProcessYamlFiles:
    def test_overlapping_paths_apply_changesets_once_per_file(self, tmp_path: Path):
        a = _write(tmp_path / "models" / "a.yml", "a: 1\n").resolve()
        b = _write(tmp_path / "models" / "semantic" / "b.yml", "b: 1\n").resolve()
        with patch.object(YMLRefactorResult, "apply_changeset", autospec=True) as apply_changeset:
            results = process_yaml_files_except_dbt_project(
                tmp_path, ["models", "models/semantic"], schema_specs=MagicMock(), dry_run=True
            )
        calls_per_file = Counter(call.args[0].file_path for call in apply_changeset.call_args_list)
        assert [r.file_path for r in results] == [a, b]
        assert calls_per_file[a] == calls_per_file[b]

    def test_original_yaml_is_not_parsed_up_front(self, tmp_path: Path):
        _write(tmp_path / "models" / "a.yml", "a: 1\n")
        with (
            patch.object(YMLRefactorResult, "apply_changeset", autospec=True),
            patch("dbt_autofix.refactor.load_yaml") as load_yaml,
        ):
            process_yaml_files_except_dbt_project(tmp_path, ["models"], schema_specs=MagicMock(), dry_run=True)
        load_yaml.assert_not_called()

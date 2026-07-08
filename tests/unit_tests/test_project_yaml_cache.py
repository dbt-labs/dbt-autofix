"""Tests for project YAML cache, unified ``load_yaml(Path)``, and semantic list coercion."""

import logging
from pathlib import Path
from textwrap import dedent
from unittest.mock import patch

import pytest
from ruamel.yaml.comments import CommentedMap, CommentedSeq

from dbt_autofix.refactor import process_yaml_files_except_dbt_project
from dbt_autofix.refactors.yml import (
    _minimal_covering_path_roots,
    build_project_yaml_cache,
    iter_project_yaml_files,
    load_yaml,
)
from dbt_autofix.retrieve_schemas import SchemaSpecs
from dbt_autofix.semantic_definitions import SemanticDefinitions, _as_top_level_yaml_list


def _write_dbt_tree(root: Path) -> None:
    (root / "dbt_project.yml").write_text("name: p\nversion: 1.0.0\n", encoding="utf-8")


def test_load_yaml_path_matches_string_content(tmp_path: Path) -> None:
    p = tmp_path / "a.yml"
    p.write_text("a: 1\n", encoding="utf-8")
    from_path = load_yaml(p)
    from_str = load_yaml("a: 1\n")
    assert dict(from_path) == dict(from_str) == {"a": 1}


def test_build_project_yaml_cache_parses_schema_files(tmp_path: Path) -> None:
    _write_dbt_tree(tmp_path)
    m = tmp_path / "models"
    m.mkdir()
    f = m / "schema.yml"
    f.write_text("version: 2\nmodels:\n  - name: x\n", encoding="utf-8")
    cache = build_project_yaml_cache(tmp_path, ["models"])
    assert cache.ordered_paths == [f]
    assert cache.parsed_by_path[f].get("version") == 2
    assert cache.text_by_path is None


def test_build_project_yaml_cache_invalid_yaml_uses_empty_map_and_warning(
    caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    _write_dbt_tree(tmp_path)
    m = tmp_path / "models"
    m.mkdir()
    bad = m / "bad.yml"
    bad.write_text("{ not closed", encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        cache = build_project_yaml_cache(tmp_path, ["models"])
    assert cache.parsed_by_path[bad] == CommentedMap()
    assert "YAML parse failed" in caplog.text
    assert str(bad) in caplog.text or repr(bad) in caplog.text  # path appears in log message


def test_build_project_yaml_cache_text_by_path_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _write_dbt_tree(tmp_path)
    m = tmp_path / "models"
    m.mkdir()
    f = m / "a.yml"
    content = "v: 2\n"
    f.write_text(content, encoding="utf-8")
    monkeypatch.delenv("DBT_AUTOFIX_YAML_CACHE_INCLUDE_TEXT", raising=False)
    c0 = build_project_yaml_cache(tmp_path, ["models"])
    assert c0.text_by_path is None

    monkeypatch.setenv("DBT_AUTOFIX_YAML_CACHE_INCLUDE_TEXT", "1")
    c1 = build_project_yaml_cache(tmp_path, ["models"])
    assert c1.text_by_path is not None
    assert c1.text_by_path[f] == content


def test_text_cache_env_truthy_variants(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _write_dbt_tree(tmp_path)
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "a.yml").write_text("a: 1\n", encoding="utf-8")
    for v in ("true", "on", "yes", "1"):
        monkeypatch.setenv("DBT_AUTOFIX_YAML_CACHE_INCLUDE_TEXT", v)
        c = build_project_yaml_cache(tmp_path, ["models"])
        assert c.text_by_path is not None, f"env value {v!r}"


def test_iter_project_yaml_files_nested_model_paths_does_not_double_glob(
    tmp_path: Path,
) -> None:
    _write_dbt_tree(tmp_path)
    sem = tmp_path / "models" / "semantic"
    sem.mkdir(parents=True)
    (tmp_path / "models" / "a.yml").write_text("a: 1\n", encoding="utf-8")
    (sem / "b.yml").write_text("b: 1\n", encoding="utf-8")
    p1 = set(iter_project_yaml_files(tmp_path, ["models"]))
    p2 = set(
        iter_project_yaml_files(
            tmp_path,
            [
                "models",
                "models/semantic",
            ],
        )
    )
    assert p1 == p2
    assert len(p2) == 2


def test_minimal_covering_path_roots_drops_descendant_dir(tmp_path: Path) -> None:
    a = (tmp_path / "models").resolve()
    b = (tmp_path / "models" / "semantic").resolve()
    a.mkdir()
    b.mkdir()
    r = _minimal_covering_path_roots([b, a])
    assert r == [a]


def test_as_top_level_yaml_list_accepts_list_tuple_commented() -> None:
    c = CommentedMap()
    seq = CommentedSeq()
    seq.append(CommentedMap())
    c["models"] = seq
    c["models_list"] = [{"name": "a"}]
    c["models_tuple"] = ({"name": "b"},)
    assert len(_as_top_level_yaml_list(c, "models")) == 1
    assert len(_as_top_level_yaml_list(c, "models_list")) == 1
    assert len(_as_top_level_yaml_list(c, "models_tuple")) == 1


def test_as_top_level_yaml_list_rejects_mapping_set_and_scalar() -> None:
    c = CommentedMap()
    c["m1"] = {"a": 1}  # mapping, not a list
    c["m2"] = {1, 2}  # set — not a YAML sequence, must not be treated as child nodes
    c["m3"] = "nope"
    c["m4"] = 0
    assert _as_top_level_yaml_list(c, "m1") == []
    assert _as_top_level_yaml_list(c, "m2") == []
    assert _as_top_level_yaml_list(c, "m3") == []
    assert _as_top_level_yaml_list(c, "m4") == []


def test_semantic_definitions_uses_yaml_cache_index(tmp_path: Path) -> None:
    _write_dbt_tree(tmp_path)
    mdir = tmp_path / "models"
    mdir.mkdir()
    (mdir / "schema.yml").write_text(
        dedent(
            """
            version: 2
            models:
              - name: m
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    cache = build_project_yaml_cache(tmp_path, ["models"])
    sd = SemanticDefinitions(tmp_path, ["models"], yaml_cache=cache)
    assert ("m", None) in sd.model_yml_keys


def test_cache_parsed_by_path_keys_match_ordered_paths(tmp_path: Path) -> None:
    _write_dbt_tree(tmp_path)
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "a.yml").write_text("a: 1\n", encoding="utf-8")
    (tmp_path / "models" / "b.yml").write_text("b: 1\n", encoding="utf-8")
    cache = build_project_yaml_cache(tmp_path, ["models"])
    assert set(cache.ordered_paths) == set(cache.parsed_by_path.keys())


def test_process_yaml_skips_disk_read_when_text_in_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _write_dbt_tree(tmp_path)
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "a.yml").write_text("a: 1\n", encoding="utf-8")
    monkeypatch.setenv("DBT_AUTOFIX_YAML_CACHE_INCLUDE_TEXT", "1")
    cache = build_project_yaml_cache(tmp_path, ["models"])
    with patch("pathlib.Path.read_text") as m_read:
        process_yaml_files_except_dbt_project(tmp_path, ["models"], SchemaSpecs(), dry_run=True, yaml_cache=cache)
    m_read.assert_not_called()

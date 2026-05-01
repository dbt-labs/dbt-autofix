import json
import pytest
from pathlib import Path
from dbt_autofix.manifest_reader import (
    load_manifest,
    get_model_infos,
    match_compiled_paths_to_unique_ids,
    get_transitive_descendants,
    ModelInfo,
)

MANIFEST = {
    "metadata": {"dbt_schema_version": "v12"},
    "nodes": {
        "model.jaffle_shop.stg_orders": {
            "resource_type": "model",
            "unique_id": "model.jaffle_shop.stg_orders",
            "name": "stg_orders",
            "original_file_path": "models/staging/stg_orders.sql",
            "patch_path": "jaffle_shop://models/staging/schema.yml",
        },
        "model.jaffle_shop.orders": {
            "resource_type": "model",
            "unique_id": "model.jaffle_shop.orders",
            "name": "orders",
            "original_file_path": "models/orders.sql",
            "patch_path": None,
        },
        "model.jaffle_shop.customers": {
            "resource_type": "model",
            "unique_id": "model.jaffle_shop.customers",
            "name": "customers",
            "original_file_path": "models/customers.sql",
            "patch_path": "jaffle_shop://models/schema.yml",
        },
        "test.jaffle_shop.some_test.abc123": {
            "resource_type": "test",
            "unique_id": "test.jaffle_shop.some_test.abc123",
            "name": "some_test",
            "original_file_path": "tests/some_test.sql",
            "patch_path": None,
        },
    },
    "child_map": {
        "model.jaffle_shop.stg_orders": [
            "model.jaffle_shop.orders",
            "model.jaffle_shop.customers",
        ],
        "model.jaffle_shop.orders": [],
        "model.jaffle_shop.customers": [],
        "test.jaffle_shop.some_test.abc123": [],
    },
}


def test_load_manifest(tmp_path):
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "manifest.json").write_text(json.dumps(MANIFEST))
    result = load_manifest(tmp_path)
    assert "nodes" in result


def test_load_manifest_raises_when_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_manifest(tmp_path)


def test_get_model_infos_returns_only_models():
    infos = get_model_infos(MANIFEST)
    assert "test.jaffle_shop.some_test.abc123" not in infos
    assert len(infos) == 3


def test_get_model_infos_fields():
    infos = get_model_infos(MANIFEST)
    info = infos["model.jaffle_shop.orders"]
    assert info.name == "orders"
    assert info.original_file_path == "models/orders.sql"
    assert info.patch_path is None


def test_get_model_infos_preserves_patch_path():
    infos = get_model_infos(MANIFEST)
    info = infos["model.jaffle_shop.stg_orders"]
    assert info.patch_path == "jaffle_shop://models/staging/schema.yml"


def test_match_compiled_paths_to_unique_ids(tmp_path):
    compiled_root = tmp_path / "target" / "compiled" / "jaffle_shop" / "models"
    compiled_root.mkdir(parents=True)
    (compiled_root / "orders.sql").write_text("select 1")

    compiled_path = compiled_root / "orders.sql"
    result = match_compiled_paths_to_unique_ids(MANIFEST, [compiled_path], tmp_path)
    assert result[compiled_path] == "model.jaffle_shop.orders"


def test_match_compiled_paths_subdirectory(tmp_path):
    compiled_root = tmp_path / "target" / "compiled" / "jaffle_shop" / "models" / "staging"
    compiled_root.mkdir(parents=True)
    (compiled_root / "stg_orders.sql").write_text("select 1")

    compiled_path = compiled_root / "stg_orders.sql"
    result = match_compiled_paths_to_unique_ids(MANIFEST, [compiled_path], tmp_path)
    assert result[compiled_path] == "model.jaffle_shop.stg_orders"


def test_get_transitive_descendants_direct():
    result = get_transitive_descendants(MANIFEST, {"model.jaffle_shop.stg_orders"})
    assert result == {"model.jaffle_shop.orders", "model.jaffle_shop.customers"}


def test_get_transitive_descendants_excludes_input():
    result = get_transitive_descendants(MANIFEST, {"model.jaffle_shop.stg_orders"})
    assert "model.jaffle_shop.stg_orders" not in result


def test_get_transitive_descendants_excludes_non_models():
    manifest_with_test_child = {
        **MANIFEST,
        "child_map": {
            **MANIFEST["child_map"],
            "model.jaffle_shop.orders": ["test.jaffle_shop.some_test.abc123"],
        },
    }
    result = get_transitive_descendants(manifest_with_test_child, {"model.jaffle_shop.orders"})
    assert "test.jaffle_shop.some_test.abc123" not in result


def test_get_transitive_descendants_empty():
    result = get_transitive_descendants(MANIFEST, {"model.jaffle_shop.orders"})
    assert result == set()


def test_get_transitive_descendants_transitive():
    deep_manifest = {
        "nodes": {
            "model.p.a": {"resource_type": "model", "unique_id": "model.p.a", "name": "a", "original_file_path": "models/a.sql", "patch_path": None},
            "model.p.b": {"resource_type": "model", "unique_id": "model.p.b", "name": "b", "original_file_path": "models/b.sql", "patch_path": None},
            "model.p.c": {"resource_type": "model", "unique_id": "model.p.c", "name": "c", "original_file_path": "models/c.sql", "patch_path": None},
        },
        "child_map": {
            "model.p.a": ["model.p.b"],
            "model.p.b": ["model.p.c"],
            "model.p.c": [],
        },
    }
    result = get_transitive_descendants(deep_manifest, {"model.p.a"})
    assert result == {"model.p.b", "model.p.c"}

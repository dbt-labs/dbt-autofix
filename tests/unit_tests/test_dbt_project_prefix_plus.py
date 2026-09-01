"""Tests for changeset_dbt_project_prefix_plus_for_config.

Covers top-level key handling in dbt_project.yml:
- Recognized config keys get + prefix
- +prefixed unknown keys move to +meta as a unit
- Non-+ unknown dict keys recurse as project paths
- YAML anchors survive the migration
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from dbt_autofix.refactors.changesets.dbt_project_yml import (
    changeset_dbt_project_prefix_plus_for_config,
    load_yaml,
)
from dbt_autofix.refactors.results import (
    DbtProjectYMLRefactorConfig,
    YMLContent,
)
from dbt_autofix.retrieve_schemas import SchemaSpecs


@pytest.fixture(scope="module")
def real_schema():
    return SchemaSpecs()


@pytest.fixture
def temp_path():
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def _run(yml_str, schema, root_path):
    parsed = load_yaml(yml_str)
    content = YMLContent(
        original_str=yml_str,
        original_parsed=parsed,
        current_str=yml_str,
    )
    config = DbtProjectYMLRefactorConfig(
        schema_specs=schema,
        root_path=root_path,
    )
    return changeset_dbt_project_prefix_plus_for_config(content, config)


def test_plus_prefixed_unknown_key_moved_to_meta(real_schema, temp_path):
    """A +prefixed key NOT in the schema should be moved to +meta
    as a complete unit — its dict value must NOT be recursed into.
    """
    yml = (
        "name: my_project\n"
        "version: '1.0'\n"
        "models:\n"
        "  +materialized: table\n"
        "  +my_custom_setting:\n"
        "    owner: my-team\n"
        "    lifecycle: production\n"
        "  my_project:\n"
        "    staging:\n"
        "      +materialized: view\n"
    )
    result = _run(yml, real_schema, temp_path)

    assert result.refactored
    out = result.refactored_yaml
    parsed = load_yaml(out)
    models = parsed["models"]

    # Custom key should be under +meta, not at top level
    assert "+my_custom_setting" not in models
    assert "+meta" in models
    assert "my_custom_setting" in models["+meta"]
    # Inner keys must be untouched
    assert models["+meta"]["my_custom_setting"]["owner"] == "my-team"
    assert models["+meta"]["my_custom_setting"]["lifecycle"] == "production"


def test_plus_prefixed_unknown_leaf_moved_to_meta(real_schema, temp_path):
    """A +prefixed unknown key with a scalar value should also
    move to +meta.
    """
    yml = "name: my_project\nversion: '1.0'\nmodels:\n  +materialized: table\n  +register_hades: true\n"
    result = _run(yml, real_schema, temp_path)

    assert result.refactored
    parsed = load_yaml(result.refactored_yaml)
    models = parsed["models"]
    assert "+register_hades" not in models
    assert models["+meta"]["register_hades"] is True


def test_non_plus_unknown_leaf_moved_to_meta(real_schema, temp_path):
    """A non-+ unknown key with a scalar value at the top level
    should also move to +meta.
    """
    yml = "name: my_project\nversion: '1.0'\nmodels:\n  +materialized: table\n  shard_guard: true\n"
    result = _run(yml, real_schema, temp_path)

    assert result.refactored
    parsed = load_yaml(result.refactored_yaml)
    models = parsed["models"]
    assert "shard_guard" not in models
    assert models["+meta"]["shard_guard"] is True


def test_anchor_preserved_after_move(real_schema, temp_path):
    """YAML anchors on custom config values survive the migration."""
    yml = (
        "name: my_project\n"
        "version: '1.0'\n"
        "models:\n"
        "  +my_defaults: &defaults\n"
        "    owner: my-team\n"
        "    lifecycle: production\n"
        "  my_project:\n"
        "    reporting:\n"
        "      +my_defaults:\n"
        "        <<: *defaults\n"
        "        lifecycle: staging\n"
    )
    result = _run(yml, real_schema, temp_path)

    assert result.refactored
    out = result.refactored_yaml
    assert "&defaults" in out
    assert "*defaults" in out
    parsed = load_yaml(out)
    assert "my_defaults" in parsed["models"]["+meta"]


def test_recognized_config_not_moved(real_schema, temp_path):
    """Known config keys (like +materialized) must stay at the
    top level — they should NOT be moved to +meta.
    """
    yml = "name: my_project\nversion: '1.0'\nmodels:\n  +materialized: table\n  +schema: my_schema\n"
    result = _run(yml, real_schema, temp_path)

    # No changes needed — both are valid configs with + prefix
    assert not result.refactored


def test_multiple_unknown_keys_coexist_in_meta(real_schema, temp_path):
    """Two +prefixed unknowns in the same section should both end
    up under +meta without clobbering each other.
    """
    yml = (
        "name: my_project\n"
        "version: '1.0'\n"
        "models:\n"
        "  +endpoint_settings:\n"
        "    owner: my-team\n"
        "  +workflow_settings:\n"
        "    schedule: daily\n"
    )
    result = _run(yml, real_schema, temp_path)

    assert result.refactored
    parsed = load_yaml(result.refactored_yaml)
    meta = parsed["models"]["+meta"]
    assert meta["endpoint_settings"]["owner"] == "my-team"
    assert meta["workflow_settings"]["schedule"] == "daily"


def test_recognized_config_gets_plus_prefix(real_schema, temp_path):
    """Known config keys missing + prefix should get it added."""
    yml = "name: my_project\nversion: '1.0'\nmodels:\n  materialized: table\n"
    result = _run(yml, real_schema, temp_path)

    assert result.refactored
    parsed = load_yaml(result.refactored_yaml)
    assert "+materialized" in parsed["models"]
    assert "materialized" not in parsed["models"]

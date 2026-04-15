"""YAML changeset for injecting SAO build_after config into model YAMLs."""
from __future__ import annotations

from ruamel.yaml.comments import CommentedMap

from dbt_autofix.refactors.yml import DbtYAML


def changeset_add_sao_config(
    yml_str: str,
    model_sao_configs: dict[str, dict],
) -> tuple[str, bool]:
    """Inject config.freshness.build_after into matching models.

    Skips models already having freshness config or not in model_sao_configs.
    Returns (new_yml_str, changed).
    """
    yaml = DbtYAML()
    data = yaml.load(yml_str)
    if not data or "models" not in data:
        return yml_str, False

    changed = False
    for model in data["models"]:
        name = model.get("name")
        if not name:
            continue

        build_after = model_sao_configs.get(name)
        if build_after is None:
            continue

        config = model.get("config")
        if config and "freshness" in config:
            continue  # Already configured — idempotent

        # Build nested CommentedMaps to preserve formatting
        ba_map = CommentedMap()
        ba_map["count"] = build_after["count"]
        ba_map["period"] = build_after["period"]
        ba_map["updates_on"] = "all"

        fresh_map = CommentedMap({"build_after": ba_map})
        config_map = CommentedMap({"freshness": fresh_map})

        if config is not None:
            model["config"]["freshness"] = fresh_map
        else:
            # Insert config right after name to match dbt convention
            reordered = CommentedMap()
            for key, val in model.items():
                reordered[key] = val
                if key == "name":
                    reordered["config"] = config_map
            model.clear()
            model.update(reordered)

        changed = True

    if not changed:
        return yml_str, False

    return yaml.dump_to_string(data, add_final_eol=True), True

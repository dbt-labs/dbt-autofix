from pathlib import Path
from typing import Optional

from ruamel.yaml import YAML


def apply_strict_project_default(project_path: Path) -> None:
    """Set +static_analysis: strict at the project root in dbt_project.yml."""
    dbt_project_file = project_path / "dbt_project.yml"
    if not dbt_project_file.exists():
        raise FileNotFoundError(f"dbt_project.yml not found in {project_path}")

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=2, offset=2)
    with open(dbt_project_file) as f:
        data = yaml.load(f)

    project_name = data.get("name", "")
    if "models" not in data or data["models"] is None:
        data["models"] = {}
    models = data["models"]
    if project_name and project_name not in models:
        models[project_name] = {}
    target = models[project_name] if project_name else models
    target["+static_analysis"] = "strict"

    with open(dbt_project_file, "w") as f:
        yaml.dump(data, f)


def apply_baseline_to_model_schema(
    project_path: Path,
    patch_path: str,
    model_name: str,
) -> None:
    """Write static_analysis: baseline into the model's config in its schema YAML.

    patch_path looks like "jaffle_shop://models/schema.yml" — the part before "://"
    is the project name and is stripped to get the relative file path.
    """
    if "://" not in patch_path:
        return
    rel_path = patch_path.split("://", 1)[1]
    schema_file = project_path / rel_path
    if not schema_file.exists():
        return

    yaml = YAML()
    yaml.preserve_quotes = True
    with open(schema_file) as f:
        data = yaml.load(f)

    if not data or "models" not in data:
        return

    for model in data.get("models") or []:
        if model.get("name") == model_name:
            if "config" not in model or model["config"] is None:
                model["config"] = {}
            model["config"]["static_analysis"] = "baseline"
            break

    with open(schema_file, "w") as f:
        yaml.dump(data, f)


def apply_baseline_to_model_sql(
    project_path: Path,
    original_file_path: str,
) -> None:
    """Inject {{ config(static_analysis='baseline') }} at top of model SQL (fallback)."""
    sql_file = project_path / original_file_path
    if not sql_file.exists():
        return

    content = sql_file.read_text(encoding="utf-8")
    if "static_analysis" in content[:200]:
        return

    sql_file.write_text("{{ config(static_analysis='baseline') }}\n" + content)


# Compatibility shim used by existing tests and integration tests.
# Will be removed once main.py is updated to call the new functions directly.
def apply_static_analysis_config(project_path: Path, level: str) -> None:
    """Write +static_analysis: <level> at the project root in dbt_project.yml."""
    dbt_project_file = project_path / "dbt_project.yml"
    if not dbt_project_file.exists():
        raise FileNotFoundError(f"dbt_project.yml not found in {project_path}")

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=2, offset=2)
    with open(dbt_project_file) as f:
        data = yaml.load(f)

    project_name = data.get("name", "")
    if "models" not in data or data["models"] is None:
        data["models"] = {}
    models = data["models"]
    if project_name and project_name not in models:
        models[project_name] = {}
    target = models[project_name] if project_name else models
    target["+static_analysis"] = level

    with open(dbt_project_file, "w") as f:
        yaml.dump(data, f)

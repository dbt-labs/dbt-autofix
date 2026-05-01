from pathlib import Path

from ruamel.yaml import YAML


def apply_static_analysis_config(project_path: Path, level: str) -> None:
    """Write +static_analysis: <level> into the project's top-level models config in dbt_project.yml."""
    dbt_project_file = project_path / "dbt_project.yml"
    if not dbt_project_file.exists():
        raise FileNotFoundError(f"dbt_project.yml not found in {project_path}")

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=2, offset=2)  # preserves "  - item" sequence style
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

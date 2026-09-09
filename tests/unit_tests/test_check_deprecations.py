import tempfile
from pathlib import Path

from pre_commit_hooks.check_deprecations import main, parse_arguments


def test_main_on_empty_project():
    """Smoke test: main() runs without error on a minimal dbt project with no files to fix."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir)
        project_dir.joinpath("dbt_project.yml").write_text('model-paths: ["models"]\n')
        (project_dir / "models").mkdir()

        exit_code = main(["--dry-run", "--path", str(project_dir)])

    assert exit_code == 0


def test_parse_arguments_accepts_json_schema_version():
    args = parse_arguments(["--dry-run", "--json-schema-version", "v2.0.0-preview.208", "models/schema.yml"])
    assert args.json_schema_version == "v2.0.0-preview.208"
    assert args.dry_run is True
    assert args.filenames == ["models/schema.yml"]


def test_parse_arguments_json_schema_version_defaults_to_none():
    args = parse_arguments(["--dry-run"])
    assert args.json_schema_version is None

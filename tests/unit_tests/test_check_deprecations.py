import tempfile
from pathlib import Path

from pre_commit_hooks.check_deprecations import main


def test_main_on_empty_project():
    """Smoke test: main() runs without error on a minimal dbt project with no files to fix."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir)
        project_dir.joinpath("dbt_project.yml").write_text('model-paths: ["models"]\n')
        (project_dir / "models").mkdir()

        exit_code = main(["--dry-run", "--path", str(project_dir)])

    assert exit_code == 0

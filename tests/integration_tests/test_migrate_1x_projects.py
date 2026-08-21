"""Golden-file integration tests for the `migrate-1x` command.

Kept separate from ``test_full_dbt_projects.py`` (and using a separate fixtures directory) so
the Fusion ``refactor_yml`` harness does not pick up these 1.x-only projects.

Regenerate expected output with ``GOLDIE_UPDATE=1 pytest tests/integration_tests/test_migrate_1x_projects.py``.
"""

import os
import shutil
import tempfile
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

import pytest

from dbt_autofix.main import migrate_1x
from tests.integration_tests.test_full_dbt_projects import compare_dirs, compare_json_logs

dbt_projects_dir_name = "dbt_projects_migrate_1x"
postfix_expected = "_expected"


def get_project_folders():
    dbt_projects_dir = os.path.join(os.path.dirname(__file__), dbt_projects_dir_name)
    return [
        folder
        for folder in os.listdir(dbt_projects_dir)
        if os.path.isdir(os.path.join(dbt_projects_dir, folder)) and not folder.endswith(postfix_expected)
    ]


@pytest.mark.parametrize("project_folder", get_project_folders())
def test_migrate_1x_project(project_folder, request):
    dbt_projects_dir = os.path.join(os.path.dirname(__file__), dbt_projects_dir_name)
    source_dir = os.path.join(dbt_projects_dir, project_folder)

    temp_dir = tempfile.mkdtemp(prefix=f"dbt_autofix_migrate_1x_{project_folder}_")
    temp_project_path = os.path.join(temp_dir, project_folder)
    shutil.copytree(source_dir, temp_project_path, dirs_exist_ok=True)

    refactor_logs_io = StringIO()
    with redirect_stdout(refactor_logs_io):
        migrate_1x(path=Path(temp_project_path), dry_run=False, json_output=True)

    expected_dir = os.path.join(dbt_projects_dir, f"{project_folder}{postfix_expected}")

    if os.getenv("GOLDIE_UPDATE"):
        if os.path.exists(expected_dir):
            shutil.rmtree(expected_dir)
        shutil.copytree(temp_project_path, expected_dir)
    elif not os.path.exists(expected_dir):
        pytest.fail(f"Expected output directory not found: {expected_dir}")  # ty: ignore[invalid-argument-type]

    compare_dirs(temp_project_path, expected_dir)

    expected_logs_path = Path(dbt_projects_dir, f"{project_folder}_expected.stdout")
    compare_json_logs(refactor_logs_io, expected_logs_path, relative_to=Path(temp_project_path).parent)

    def cleanup_temp_dir():
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            print(f"Failed to clean up {temp_dir}: {e}")

    request.addfinalizer(cleanup_temp_dir)

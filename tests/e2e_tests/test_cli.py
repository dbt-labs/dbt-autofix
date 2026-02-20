import subprocess
import sys
from pathlib import Path


def test_run_cli():
    """Make sure the CLI runs correctly."""
    subprocess.run(["dbt-autofix", "--help"], check=True)


def test_run_cli_deprecations():
    """Make sure the deprecations CLI runs correctly."""
    path = Path(__file__).parent.parent / "integration_tests" / "dbt_projects" / "project1"
    try:
        subprocess.run(
            ["dbt-autofix", "deprecations", "--path", str(path.resolve())],
            check=True,
            capture_output=True,
            text=True,
        )
    finally:
        result = subprocess.run(["git", "restore", str(path.resolve())], check=False)
        if result.returncode != 0:
            print(f"Warning: git restore failed (exit {result.returncode})", file=sys.stderr)


def test_check_latest_schema():
    """Make sure the print-fields-matrix command runs correctly."""
    subprocess.run(["dbt-autofix", "print-fields-matrix"], check=True)

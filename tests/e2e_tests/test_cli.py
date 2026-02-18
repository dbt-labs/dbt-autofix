import subprocess


def test_run_cli():
    """Make sure the CLI runs correctly."""
    subprocess.run(["dbt-autofix", "--help"], check=True)


def test_run_cli_deprecations():
    """Make sure the deprecations CLI runs correctly."""
    subprocess.run(["dbt-autofix", "deprecations"], check=True)


def test_check_latest_schema():
    """Make sure the print-fields-matrix command runs correctly."""
    subprocess.run(["dbt-autofix", "print-fields-matrix"], check=True)

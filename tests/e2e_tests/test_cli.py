import subprocess


def test_run_cli():
    subprocess.run(["dbt-autofix", "--help"], check=True)


def test_run_cli_deprecations():
    subprocess.run(["dbt-autofix", "deprecations"], check=True)


def test_check_latest_schema():
    subprocess.run(["dbt-autofix", "print-fields-matrix"], check=True)

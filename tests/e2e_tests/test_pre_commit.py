import subprocess


def test_pre_commit_installation():
    subprocess.run(
        ["pre-commit", "try-repo", ".", "dbt-autofix-check", "--files", "non_existent_file", "--verbose"],
        check=True,
    )

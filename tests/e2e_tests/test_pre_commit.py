import subprocess


def test_pre_commit_installation():
    """Test the pre-commit hook installation flow as end users experience it.

    This is the only test that exercises pre-commit's own build-and-install
    machinery against the local repo. It catches issues that would prevent
    users from adding dbt-autofix to their .pre-commit-config.yaml, such as:
    - Build backend misconfiguration (hatchling + uv-dynamic-versioning)
    - Dependencies that fail to resolve outside the uv workspace
    - Missing pre_commit_hooks package in the wheel (the hook entry point
      is `python -m pre_commit_hooks.check_deprecations`, so import failure
      would surface here)

    Uses try-repo with a non-existent file to test installation without
    execution. This avoids the requirement for a dbt_project.yml file.
    """
    subprocess.run(
        ["pre-commit", "try-repo", ".", "dbt-autofix-check", "--files", "non_existent_file", "--verbose"],
        check=True,
    )

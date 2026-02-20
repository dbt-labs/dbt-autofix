import os
import subprocess
from pathlib import Path


def test_pre_commit_installation(tmp_path: Path):
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

    Builds local wheels first and passes PIP_FIND_LINKS to pre-commit so its
    isolated pip environment resolves workspace packages (e.g.
    dbt-fusion-package-tools) from the local build rather than PyPI.

    Version matching:
    pyproject.toml uses `dbt-fusion-package-tools=={{ version }}`, which pins
    the dep to the exact version of dbt-autofix at build time. pre-commit's
    try-repo *always* creates a synthetic shadow repo (1 commit, no tags) from
    the HEAD state, so uv-dynamic-versioning always computes 0.0.0.post1.dev0
    in that shadow — regardless of working-tree cleanliness or whether HEAD is
    on a tagged commit. Building from the real repo (which has actual tags like
    v0.20.0+N commits) would produce a mismatched version and cause pip to fail
    on the pinned dbt-fusion-package-tools dep. We therefore always build from
    a shallow no-tags clone so both wheels land at 0.0.0.post1.dev0.
    """
    dist_path = tmp_path / "dist"

    # Always build from a shallow, no-tags clone so the wheel version matches
    # the synthetic shadow that pre-commit will create.
    #
    # pre-commit's try-repo *always* archives HEAD into a fresh single-commit
    # repo with no tags, so uv-dynamic-versioning always computes
    # 0.0.0.post1.dev0 in that shadow — regardless of whether the working tree
    # is clean or dirty, and regardless of whether the current commit is tagged.
    # Building from a real repo with tags (e.g. v0.20.0+8 commits) would
    # produce a different version (e.g. 0.20.0.post8.dev0), causing pip to fail
    # on the dbt-fusion-package-tools=={{ version }} pin.
    repo_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    build_src = tmp_path / "build_src"
    subprocess.run(
        ["git", "clone", "--depth=1", "--no-tags", f"file://{repo_root}", str(build_src)],
        check=True,
    )
    subprocess.run(
        ["uv", "build", "--all", "--out-dir", str(dist_path)],
        check=True,
        cwd=build_src,
    )

    env = {**os.environ, "PIP_FIND_LINKS": str(dist_path)}
    subprocess.run(
        ["pre-commit", "try-repo", ".", "dbt-autofix-check", "--files", "non_existent_file", "--verbose"],
        check=True,
        env=env,
    )

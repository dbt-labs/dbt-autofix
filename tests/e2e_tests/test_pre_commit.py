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
    the dep to the exact version of dbt-autofix at build time. When pre-commit
    has uncommitted changes it creates a synthetic shadow repo (1 commit, no
    tags) whose dynamic version is typically `0.0.0.post1.dev0` — different
    from a local build that has real git tags. To keep the versions in sync we
    detect uncommitted changes and, when present, build from a shallow
    no-tags clone so both the wheel and the shadow report the same version.
    Without uncommitted changes (e.g. CI's shallow checkout or a clean local
    tree) both paths produce the same version naturally.
    """
    dist_path = tmp_path / "dist"

    has_uncommitted = bool(
        subprocess.run(
            ["git", "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )

    if has_uncommitted:
        # Build from a shallow, no-tags clone so the wheel version matches
        # the synthetic shadow that pre-commit will create.
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
    else:
        subprocess.run(["uv", "build", "--all", "--out-dir", str(dist_path)], check=True)

    env = {**os.environ, "PIP_FIND_LINKS": str(dist_path)}
    subprocess.run(
        ["pre-commit", "try-repo", ".", "dbt-autofix-check", "--files", "non_existent_file", "--verbose"],
        check=True,
        env=env,
    )

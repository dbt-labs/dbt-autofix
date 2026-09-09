import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

import nox

nox.options.default_venv_backend = "uv"


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def run_cli(session):
    """Make sure the CLI runs correctly"""
    session.run_install(
        "uv",
        "sync",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("dbt-autofix", "--help")


@nox.session(python=["3.13"], venv_backend="uv")
def check_latest_schema(session):
    """Make sure the CLI runs correctly"""
    session.run_install(
        "uv",
        "sync",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("dbt-autofix", "print-fields-matrix")


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def run_cli_deprecations(session):
    """Make sure the deprecations CLI runs (but fails)"""
    session.run_install(
        "uv",
        "sync",
        "--all-packages",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    integration_test_path = Path.cwd() / "tests" / "integration_tests" / "dbt_projects" / "project1"
    session.run("uv", "run", "dbt-autofix", "deprecations", "--path", f"{integration_test_path.resolve()}")


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_pre_commit_installation(session):
    """Test the pre-commit hook installation flow as end users experience it.

    This is the only test that exercises pre-commit's own build-and-install
    machinery against the local repo. It catches issues that would prevent
    users from adding dbt-autofix to their .pre-commit-config.yaml, such as:
    - Build backend misconfiguration (hatchling + uv-dynamic-versioning)
    - Dependencies that fail to resolve outside the uv workspace
    - Missing pre_commit_hooks package in the wheel (the hook entry point
      is `python -m pre_commit_hooks.check_deprecations`, so import failure
      would surface here)
    """
    session.run_install(
        "uv",
        "sync",
        "--all-packages",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    # Build wheels for all packages
    current_directory = Path.cwd()
    dist_path = (current_directory / "dist").resolve()
    session.run(
        "uv",
        "build",
        "--all",
        f"--out-dir={dist_path}",
    )

    # Use try-repo with a non-existent file to test installation without execution.
    # This avoids the requirement for a dbt_project.yml file.
    session.run(
        "pre-commit",
        "try-repo",
        ".",
        "dbt-autofix-check",
        "--files",
        "non_existent_file",
        "--verbose",
        env={"PIP_FIND_LINKS": dist_path, **os.environ},
    )


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_pre_commit_installation_shallow_clone(session):
    """Test pre-commit hook installation from a shallow clone without tags.

    pre-commit clones repos without tags, which causes uv-dynamic-versioning
    to fall back to the fallback-version (0.0.0). This test simulates that
    behavior to ensure the hook can still be installed correctly.

    See: https://github.com/dbt-labs/dbt-autofix/issues/337
    """
    session.run_install(
        "uv",
        "sync",
        "--all-packages",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )

    repo_root = Path.cwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        shallow_clone = Path(tmpdir) / "dbt-autofix"

        # Simulate pre-commit's clone behavior: shallow, no tags.
        session.run(
            "git",
            "clone",
            "--depth=1",
            "--no-tags",
            f"file://{repo_root}",
            str(shallow_clone),
            external=True,
        )

        # Build wheels from the shallow clone (where version will fall back)
        dist_path = (shallow_clone / "dist").resolve()
        session.run(
            "uv",
            "build",
            "--all",
            f"--out-dir={dist_path}",
            external=True,
            env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
        )

        # Use try-repo against the shallow clone, just like a real user would
        session.run(
            "pre-commit",
            "try-repo",
            str(shallow_clone),
            "dbt-autofix-check",
            "--files",
            "non_existent_file",
            "--verbose",
            env={"PIP_FIND_LINKS": str(dist_path), **os.environ},
        )


# Workspace packages that dbt-autofix depends on, pinned to the release version at build
# time by the uv-dynamic-versioning metadata hook in pyproject.toml. Keep in sync with
# [tool.uv.workspace] members.
_WORKSPACE_DEPS = ["dbt-fusion-package-tools", "dbt-package-tools"]


def _build_and_install_wheels(session):
    """Build all wheels, install them, verify entry points, and return metadata.

    Clears dist/, runs `uv build --all`, inspects the dbt-autofix wheel for
    structural correctness (pre_commit_hooks included, metadata present),
    installs every wheel, and verifies CLI entry points.

    Returns:
        A (version, tools_deps) tuple where version is the wheel's Version string
        and tools_deps maps each workspace package name to its Requires-Dist line.
    """
    dist = Path("dist")
    if dist.exists():
        shutil.rmtree(dist)

    session.run("uv", "build", "--all", external=True)

    autofix_wheels = sorted(dist.glob("dbt_autofix-*.whl"))
    assert autofix_wheels, "dbt-autofix wheel not found in dist/"
    autofix_whl = autofix_wheels[-1]

    # Every workspace dep must be installed from dist/ alongside dbt-autofix: a release
    # build pins them to a version that doesn't exist on PyPI yet.
    tools_whls = []
    for name in _WORKSPACE_DEPS:
        wheels = sorted(dist.glob(f"{name.replace('-', '_')}-*.whl"))
        assert wheels, f"{name} wheel not found in dist/"
        tools_whls.append(str(wheels[-1]))

    with zipfile.ZipFile(autofix_whl) as zf:
        wheel_files = zf.namelist()

        hook_files = [f for f in wheel_files if f.startswith("pre_commit_hooks/")]
        assert hook_files, "pre_commit_hooks/ not found in dbt-autofix wheel"

        metadata_files = [f for f in wheel_files if f.endswith("/METADATA")]
        assert metadata_files, "METADATA not found in wheel"
        metadata = zf.read(metadata_files[0]).decode()

        version = None
        for line in metadata.splitlines():
            if line.startswith("Version: "):
                version = line.split(": ", 1)[1]
                break
        assert version, "Version not found in wheel METADATA"

        requires_lines = [line for line in metadata.splitlines() if line.startswith("Requires-Dist:")]
        tools_deps = {}
        for name in _WORKSPACE_DEPS:
            # Match on the requirement name only, so dbt-package-tools doesn't pick up
            # the dbt-fusion-package-tools line.
            matches = [line for line in requires_lines if line.split(": ", 1)[1].startswith(name)]
            assert matches, f"{name} not found in wheel METADATA.\nRequires-Dist lines:\n" + "\n".join(requires_lines)
            tools_deps[name] = matches[0]

    session.install(*tools_whls, str(autofix_whl))
    session.run("dbt-autofix", "--help")
    session.run("python", "-m", "pre_commit_hooks.check_deprecations", "--help")

    return version, tools_deps


_SIMULATED_TAG = "v99.99.99"


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_wheel_installation(session):
    """Dry-run the release pipeline for a dev build.

    Builds both wheels from the current (untagged) git state, installs them,
    and verifies that dbt-fusion-package-tools is an unpinned dependency.
    """
    version, tools_deps = _build_and_install_wheels(session)
    for name, tools_dep in tools_deps.items():
        assert "==" not in tools_dep, f"Dev build: {name} should be unpinned but got: {tools_dep}"
    session.log(f"version={version}, dev build (unpinned deps)")


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_wheel_installation_release(session):
    """Dry-run the release pipeline for a simulated tagged release.

    Creates a temporary git tag on HEAD so uv-dynamic-versioning sees
    distance=0, builds both wheels, and verifies that dbt-fusion-package-tools
    is pinned to the exact release version.
    """
    subprocess.run(["git", "tag", _SIMULATED_TAG], check=True)
    try:
        version, tools_deps = _build_and_install_wheels(session)
    finally:
        subprocess.run(["git", "tag", "-d", _SIMULATED_TAG], check=True)
    for name, tools_dep in tools_deps.items():
        expected = f"Requires-Dist: {name}=={version}"
        assert expected in tools_dep, f"Release build: expected '{expected}' but got: {tools_dep}"
    session.log(f"version={version}, pin=={version}")


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_core_1_10_installation(session):
    """Test that dbt-autofix can be installed with dbt-core"""
    session.run("uv", "add", "--optional", "dbt", "dbt-core==1.10.6", f"--python={session.virtualenv.location}")
    session.run_install(
        "uv",
        "sync",
        "--extra=dbt",
        "--all-packages",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("dbt-autofix", "--help")


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_core_1_12_installation(session):
    """Test that dbt-autofix can be installed with dbt-core"""
    session.run("uv", "add", "--optional", "dbt", "dbt-core==1.11.2", f"--python={session.virtualenv.location}")
    session.run_install(
        "uv",
        "sync",
        "--extra=dbt",
        "--all-packages",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("dbt-autofix", "--help")

import shutil
import subprocess
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
def pytest(session):
    """Run the tests"""
    session.run_install(
        "uv",
        "sync",
        "--extra=test",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("pytest", *session.posargs)


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def run_cli_deprecations(session):
    """Make sure the deperecations CLI runs (but fails)"""
    session.run_install(
        "uv",
        "sync",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("dbt-autofix", "deprecations")


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
        "--extra=test",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
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
    )


def _build_and_install_wheels(session):
    """Build both wheels, install them, verify entry points, and return metadata.

    Clears dist/, runs `uv build --all`, inspects the dbt-autofix wheel for
    structural correctness (pre_commit_hooks included, metadata present),
    installs both wheels, and verifies CLI entry points.

    Returns:
        A (version, tools_dep) tuple where version is the wheel's Version string
        and tools_dep is the Requires-Dist line for dbt-fusion-package-tools.
    """
    dist = Path("dist")
    if dist.exists():
        shutil.rmtree(dist)

    session.run("uv", "build", "--all", external=True)

    autofix_wheels = sorted(dist.glob("dbt_autofix-*.whl"))
    tools_wheels = sorted(dist.glob("dbt_fusion_package_tools-*.whl"))
    assert autofix_wheels, "dbt-autofix wheel not found in dist/"
    assert tools_wheels, "dbt-fusion-package-tools wheel not found in dist/"
    autofix_whl = autofix_wheels[-1]
    tools_whl = tools_wheels[-1]

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
        tools_deps = [line for line in requires_lines if "dbt-fusion-package-tools" in line]
        assert tools_deps, "dbt-fusion-package-tools not found in wheel METADATA.\nRequires-Dist lines:\n" + "\n".join(
            requires_lines
        )

    session.install(str(tools_whl), str(autofix_whl))
    session.run("dbt-autofix", "--help")
    session.run("python", "-m", "pre_commit_hooks.check_deprecations", "--help")

    return version, tools_deps[0]


_SIMULATED_TAG = "v99.99.99"


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_wheel_installation(session):
    """Dry-run the release pipeline for a dev build.

    Builds both wheels from the current (untagged) git state, installs them,
    and verifies that dbt-fusion-package-tools is an unpinned dependency.
    """
    version, tools_dep = _build_and_install_wheels(session)
    assert "==" not in tools_dep, f"Dev build: expected unpinned dbt-fusion-package-tools but got: {tools_dep}"
    session.log(f"version={version}, unpinned (dev build)")


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_wheel_installation_release(session):
    """Dry-run the release pipeline for a simulated tagged release.

    Creates a temporary git tag on HEAD so uv-dynamic-versioning sees
    distance=0, builds both wheels, and verifies that dbt-fusion-package-tools
    is pinned to the exact release version.
    """
    subprocess.run(["git", "tag", _SIMULATED_TAG], check=True)
    try:
        version, tools_dep = _build_and_install_wheels(session)
    finally:
        subprocess.run(["git", "tag", "-d", _SIMULATED_TAG], check=True)
    expected = f"Requires-Dist: dbt-fusion-package-tools=={version}"
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

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


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_wheel_installation(session):
    """Dry-run the release pipeline: build, inspect, and install both wheels.

    Smoke test to prevent us from uploading broken wheels to PyPI. Exercises our dynamic versioning system.

    This mirrors what .github/workflows/release.yml does (`uv build --all`)
    and is the only test that explicitly inspects the built artifacts. Unlike
    test_pre_commit_installation (which relies on pre-commit's installer),
    this test directly examines wheel contents and metadata to catch:
    - Missing pre_commit_hooks package in the dbt-autofix wheel
    - Broken Jinja templating of the dbt-fusion-package-tools dependency
    - Wheels that fail to install together outside the uv workspace
    """
    dist = Path("dist")

    # Build both packages. This exercises the hatchling + uv-dynamic-versioning
    # build pipeline and the metadata hook that templates the dependency pin.
    session.run("uv", "build", "--all", external=True)

    # Find the built wheels.
    autofix_wheels = sorted(dist.glob("dbt_autofix-*.whl"))
    tools_wheels = sorted(dist.glob("dbt_fusion_package_tools-*.whl"))
    assert autofix_wheels, "dbt-autofix wheel not found in dist/"
    assert tools_wheels, "dbt-fusion-package-tools wheel not found in dist/"
    autofix_whl = autofix_wheels[-1]
    tools_whl = tools_wheels[-1]

    # Inspect the dbt-autofix wheel: verify pre_commit_hooks is included
    # and that the metadata contains the dbt-fusion-package-tools pin.
    with zipfile.ZipFile(autofix_whl) as zf:
        wheel_files = zf.namelist()

        # Check that pre_commit_hooks package is in the wheel.
        hook_files = [f for f in wheel_files if f.startswith("pre_commit_hooks/")]
        assert hook_files, "pre_commit_hooks/ not found in dbt-autofix wheel"

        # Read METADATA and check the dbt-fusion-package-tools dependency.
        metadata_files = [f for f in wheel_files if f.endswith("/METADATA")]
        assert metadata_files, "METADATA not found in wheel"
        metadata = zf.read(metadata_files[0]).decode()

        # Extract version from the metadata.
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

        # On a tagged release (no .dev or + in version), the dependency should
        # be pinned to the exact version. Off a tag, it should be unpinned.
        is_release = ".dev" not in version and "+" not in version and not version.startswith("0.0")
        if is_release:
            expected = f"Requires-Dist: dbt-fusion-package-tools=={version}"
            assert any(expected in line for line in tools_deps), (
                f"Release build: expected '{expected}' but got:\n" + "\n".join(tools_deps)
            )
            session.log(f"Wheel metadata looks good: version={version}, pin=={version}")
        else:
            assert any("==" not in line for line in tools_deps), (
                "Dev build: expected unpinned dbt-fusion-package-tools but got:\n" + "\n".join(tools_deps)
            )
            session.log(f"Wheel metadata looks good: version={version}, unpinned (dev build)")

    # Install both wheels into the nox venv (bypassing workspace resolution)
    # and verify the CLI and pre-commit hook entry point work.
    session.install(str(tools_whl), str(autofix_whl))
    session.run("dbt-autofix", "--help")
    session.run("python", "-m", "pre_commit_hooks.check_deprecations", "--help")


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_core_1_10_installation(session):
    """Test that dbt-autofix can be installed with dbt-core"""
    # Use `uv pip install` instead of `uv add` because [project].dependencies is
    # dynamic (managed by the uv-dynamic-versioning metadata hook), so `uv add`
    # would fail trying to write to it. We just need dbt-core in the venv to
    # verify compatibility, not as a permanent project dependency.
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

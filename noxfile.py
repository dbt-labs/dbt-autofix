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
def test_wheel_installation(session):
    """Test that both wheels build, install, and work outside the uv workspace.

    This replaces the old test_pre_commit_installation session which used
    `pre-commit try-repo`. That approach broke because the metadata hook pins
    dbt-fusion-package-tools=={version} and the dev version doesn't exist on
    PyPI. Instead, we build both wheels locally and install them together —
    which is what actually happens during a release.

    Verifies:
    1. Both packages build successfully via `uv build --all`
    2. The dbt-autofix wheel contains the pre_commit_hooks package
    3. The dbt-autofix wheel metadata pins dbt-fusion-package-tools=={version}
    4. Both wheels install together in an isolated (non-workspace) venv
    5. The CLI entry point works
    6. The pre-commit hook entry point is importable
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

        # Read METADATA and check for the version-pinned dependency.
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

        expected_pin = f"dbt-fusion-package-tools=={version}"
        assert expected_pin in metadata, (
            f"Expected '{expected_pin}' in wheel METADATA, but not found.\n"
            f"Metadata Requires-Dist lines:\n"
            + "\n".join(line for line in metadata.splitlines() if line.startswith("Requires-Dist:"))
        )

    session.log(f"Wheel metadata looks good: version={version}, pin={expected_pin}")

    # Install both wheels into the nox venv (bypassing workspace resolution)
    # and verify the CLI and pre-commit hook entry point work.
    session.install(str(tools_whl), str(autofix_whl))
    session.run("dbt-autofix", "--help")
    session.run("python", "-m", "pre_commit_hooks.check_deprecations", "--help")


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_core_1_10_installation(session):
    """Test that dbt-autofix can be installed with dbt-core"""
    session.run_install(
        "uv",
        "sync",
        "--all-packages",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    # Use `uv pip install` instead of `uv add` because [project].dependencies is
    # dynamic (managed by the uv-dynamic-versioning metadata hook), so `uv add`
    # would fail trying to write to it. We just need dbt-core in the venv to
    # verify compatibility, not as a permanent project dependency.
    session.run_install(
        "uv",
        "pip",
        "install",
        "dbt-core==1.10.6",
        f"--python={session.virtualenv.location}",
    )
    session.run("dbt-autofix", "--help")


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
def test_core_1_12_installation(session):
    """Test that dbt-autofix can be installed with dbt-core"""
    session.run_install(
        "uv",
        "sync",
        "--all-packages",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    # Use `uv pip install` instead of `uv add` — see comment in test_core_1_10_installation.
    session.run_install(
        "uv",
        "pip",
        "install",
        "dbt-core==1.11.2",
        f"--python={session.virtualenv.location}",
    )
    session.run("dbt-autofix", "--help")

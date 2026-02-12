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
    """Test that dbt-autofix can be installed as a pre-commit hook"""
    session.run_install(
        "uv",
        "sync",
        "--extra=test",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    # Clear the debug log before running, then use --all-files so the hook
    # actually executes and the DEBUG lines in check_deprecations.py fire.
    import os

    try:
        os.remove("/tmp/pdm_build_debug.log")
    except FileNotFoundError:
        pass

    session.run(
        "pre-commit",
        "try-repo",
        ".",
        "dbt-autofix-check",
        "--all-files",
        "--verbose",
    )
    # Dump the build-time debug log so it's visible in the nox output.
    try:
        with open("/tmp/pdm_build_debug.log") as f:
            session.log(f"pdm_build debug log:\n{f.read()}")
    except FileNotFoundError:
        session.log("No pdm_build_debug.log found")


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
    session.run_install(
        "uv",
        "add",
        "dbt-core==1.10.6",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
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
    session.run_install(
        "uv",
        "add",
        "dbt-core==1.11.2",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("dbt-autofix", "--help")

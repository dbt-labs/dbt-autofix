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
    # Use try-repo with a non-existent file to test installation without execution.
    # This avoids the requirement for a dbt_project.yml file while still
    # triggering the pdm_build.py logic we want to verify.
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

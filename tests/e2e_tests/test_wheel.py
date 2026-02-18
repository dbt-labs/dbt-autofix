import subprocess

import pytest

from tests.e2e_tests.wheel_helpers import build_wheels, inspect_autofix_wheel, install_wheels_in_venv, make_venv

_SIMULATED_TAG = "v99.99.99"


def _exact_head_tags() -> list[str]:
    result = subprocess.run(
        ["git", "tag", "--points-at", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [t for t in result.stdout.strip().splitlines() if t]


def test_wheel_installation_dev(tmp_path):
    """Dry-run the release pipeline for a dev build.

    Builds both wheels from the current (untagged) git state, installs them,
    and verifies that dbt-fusion-package-tools is an unpinned dependency.
    """
    if _exact_head_tags():
        pytest.skip("HEAD is exactly tagged; dev build test requires untagged commits above the last tag")
    autofix_whl, tools_whl = build_wheels(tmp_path / "dist")
    _version, tools_dep = inspect_autofix_wheel(autofix_whl)
    assert "==" not in tools_dep, f"Dev build: expected unpinned dbt-fusion-package-tools but got: {tools_dep}"
    venv = tmp_path / "venv"
    make_venv(venv)
    install_wheels_in_venv(venv, tools_whl, autofix_whl)


def test_wheel_installation_release(tmp_path):
    """Dry-run the release pipeline for a simulated tagged release.

    Creates a temporary git tag on HEAD so uv-dynamic-versioning sees
    distance=0, builds both wheels, and verifies that dbt-fusion-package-tools
    is pinned to the exact release version. Any competing HEAD tags are removed
    before tagging and restored in the finally block.
    """
    # Remove any existing HEAD tags so the simulated release tag is unambiguous
    # to uv-dynamic-versioning (competing tags cause it to pick the wrong one).
    existing_tags = _exact_head_tags()
    for tag in existing_tags:
        subprocess.run(["git", "tag", "-d", tag], check=True)
    subprocess.run(["git", "tag", _SIMULATED_TAG], check=True)
    try:
        autofix_whl, tools_whl = build_wheels(tmp_path / "dist")
        version, tools_dep = inspect_autofix_wheel(autofix_whl)
        expected = f"Requires-Dist: dbt-fusion-package-tools=={version}"
        assert tools_dep == expected, f"Release build: expected '{expected}' but got: {tools_dep}"
        venv = tmp_path / "venv"
        make_venv(venv)
        install_wheels_in_venv(venv, tools_whl, autofix_whl)
    finally:
        subprocess.run(["git", "tag", "-d", _SIMULATED_TAG], check=True)
        for tag in existing_tags:
            subprocess.run(["git", "tag", tag], check=True)

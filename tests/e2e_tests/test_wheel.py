import subprocess
from contextlib import contextmanager

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


@contextmanager
def _head_tag_override(tags_to_add: list[str]):
    """Temporarily override HEAD tags. Removes existing HEAD tags, adds the given
    tags, and restores the original state on exit — whether or not the body raises."""
    existing = _exact_head_tags()
    for t in existing:
        subprocess.run(["git", "tag", "-d", t], check=True)
    for t in tags_to_add:
        subprocess.run(["git", "tag", t], check=True)
    try:
        yield
    finally:
        for t in tags_to_add:
            subprocess.run(["git", "tag", "-d", t], check=True)
        for t in existing:
            subprocess.run(["git", "tag", t], check=True)


def test_wheel_installation_dev(tmp_path):
    """Dry-run the release pipeline for a dev build.

    Builds both wheels from the current (untagged) git state, installs them,
    and verifies that dbt-fusion-package-tools is pinned to a dev version.
    """
    with _head_tag_override([]):  # ensure HEAD is untagged
        autofix_whl, tools_whl = build_wheels(tmp_path / "dist")
        _version, tools_dep = inspect_autofix_wheel(autofix_whl)
        assert "dev" in tools_dep, f"Dev build: dbt-fusion-package-tools version should include dev but got: {tools_dep}"
        venv = tmp_path / "venv"
        make_venv(venv)
        install_wheels_in_venv(venv, tools_whl, autofix_whl)


def test_wheel_installation_release(tmp_path):
    """Dry-run the release pipeline for a simulated tagged release.

    Creates a temporary git tag on HEAD so uv-dynamic-versioning sees
    distance=0, builds both wheels, and verifies that dbt-fusion-package-tools
    is pinned to the exact release version.
    """
    with _head_tag_override([_SIMULATED_TAG]):  # ensure HEAD has exactly the simulated tag
        autofix_whl, tools_whl = build_wheels(tmp_path / "dist")
        version, tools_dep = inspect_autofix_wheel(autofix_whl)
        expected = f"Requires-Dist: dbt-fusion-package-tools=={version}"
        assert tools_dep == expected, f"Release build: expected '{expected}' but got: {tools_dep}"
        venv = tmp_path / "venv"
        make_venv(venv)
        install_wheels_in_venv(venv, tools_whl, autofix_whl)

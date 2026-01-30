from pathlib import Path

import pytest

from dbt_autofix.hub_packages import should_skip_package

PROJECT_WITH_PACKAGES_PATH = Path("tests/unit_tests/dbt_projects/project_with_packages")


@pytest.mark.parametrize(
    "package_path,include_private_packages,expected",
    [
        # Private package should _not_ be skipped when include_private_package is True
        (Path(f"{PROJECT_WITH_PACKAGES_PATH}/dbt_packages/private_package"), True, False),
        # Private package should be skipped when include_private_package is False
        (Path(f"{PROJECT_WITH_PACKAGES_PATH}/dbt_packages/private_package"), False, True),
        # Public package should be skipped when include_private_package is True
        (Path(f"{PROJECT_WITH_PACKAGES_PATH}/dbt_packages/dbt_utils"), True, True),
        # Public package should be skipped when include_private_package is False
        (Path(f"{PROJECT_WITH_PACKAGES_PATH}/dbt_packages/dbt_utils"), False, False),
    ],
)
def test_should_skip_package(package_path, include_private_packages, expected):
    assert should_skip_package(package_path, include_private_packages) == expected

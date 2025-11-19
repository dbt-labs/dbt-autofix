from pathlib import Path
from typing import Optional
import pytest

from dbt_autofix.package_upgrade import PackageUpgradeResult, PackageVersionUpgradeResult, check_for_package_upgrades, generate_package_dependencies, upgrade_package_versions
from dbt_autofix.packages.dbt_package_file import DbtPackageFile
from dbt_autofix.packages.dbt_package_version import FusionCompatibilityState


PROJECT_WITH_PACKAGES_PATH = Path("tests/integration_tests/package_upgrades/mixed_versions")
# update if count changes
PROJECT_DEPENDENCY_COUNT = 7

# cases to test:

# manual override: all versions compatible
# dbt-labs/dbt_utils 0.8.5: no upgrade needed

# manual override: all versions incompatible
# dbt-labs/logging 0.7.0: no upgrade needed (no versions compatible)

# user already has compatible version
# dbt-labs/snowplow 0.9.0: no upgrade needed (version 0.9.0 has compatible require dbt version)

# needs upgrade to version within config range (should always succeed)
# Matts52/dbt_set_similarity 0.2.1: upgrade needed (versions 0.2.2+ compatible)

# needs upgrade to version outside config range (only succeed if force upgrade)
# Matts52/dbt_stat_test 0.1.1: version 0.1.2 compatible but config is pineed to 0.1.1

# incompatible but all versions have unknown version
# avohq/avo_audit 1.0.1: no upgrade needed (all versions require dbt version unknown)

# incompatible but no compatible versions >= current version
# there are compatible versions < current version, but we shouldn't downgrade
# MaterializeInc/materialize_dbt_utils 0.6.0: no upgrade needed (versions <0.6.0 compatible)

def test_generate_package_dependencies():
    output: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert output is not None
    assert len(output.package_dependencies) == PROJECT_DEPENDENCY_COUNT
    assert len(output.get_private_package_names()) == 0
    for package in output.package_dependencies:
        assert output.package_dependencies[package].get_installed_package_version() != "unknown"
        fusion_compatibility_state = output.package_dependencies[package].is_installed_version_fusion_compatible()
        if package == 'dbt-labs/dbt_utils':
            assert fusion_compatibility_state == FusionCompatibilityState.EXPLICIT_ALLOW
        elif package == 'dbt-labs/snowplow':
            assert fusion_compatibility_state == FusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
        elif package == 'dbt-labs/logging':
            assert fusion_compatibility_state == FusionCompatibilityState.EXPLICIT_DISALLOW

def test_check_for_package_upgrades():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert package_file is not None
    output: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(output) == PROJECT_DEPENDENCY_COUNT

def test_upgrade_package_versions_no_force_update():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert package_file is not None
    upgrades: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(upgrades) == PROJECT_DEPENDENCY_COUNT
    output: PackageUpgradeResult = upgrade_package_versions(package_file, upgrades, dry_run=True, override_pinned_version=False)
    assert output
    output.print_to_console(json_output=False)
    output.print_to_console(json_output=True)

def test_upgrade_package_versions_with_force_update():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert package_file is not None
    upgrades: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(upgrades) == PROJECT_DEPENDENCY_COUNT
    output: PackageUpgradeResult = upgrade_package_versions(package_file, upgrades, dry_run=True, override_pinned_version=True)
    assert output
    output.print_to_console(json_output=False)
    output.print_to_console(json_output=True)
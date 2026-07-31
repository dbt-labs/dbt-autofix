from pathlib import Path
from typing import Optional

from dbt_fusion_package_tools.upgrade_status import (
    PackageFusionCompatibilityState,
    PackageVersionFusionCompatibilityState,
    PackageVersionUpgradeType,
)

from dbt_autofix.package_upgrade import (
    PackageUpgradeResult,
    PackageVersionUpgradeResult,
    check_for_package_upgrades,
    generate_package_dependencies,
    upgrade_package_versions,
)
from dbt_autofix.packages.dbt_package_file import DbtPackageFile

# project does not have a package lock file
PROJECT_WITH_PACKAGES_PATH = Path("tests/integration_tests/package_upgrades/datavault4dbt")
# update if count changes
PROJECT_DEPENDENCY_COUNT = 2
PROJECT_TRANSITIVE_DEPENDENCY_COUNT = 1

# none should have upgrades


def test_generate_package_dependencies():
    output: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert output is not None
    assert len(output.package_dependencies) == PROJECT_DEPENDENCY_COUNT
    assert len(output.get_private_package_names()) == 0
    assert len(output.transitive_dependencies) == PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    for package in output.package_dependencies:
        assert output.package_dependencies[package].get_installed_package_version() != "unknown"
        fusion_compatibility_state = output.package_dependencies[package].is_installed_version_fusion_compatible()
        package_fusion_compatibility_state: PackageFusionCompatibilityState = output.package_dependencies[
            package
        ].get_package_fusion_compatibility_state()
        if package == "dbt-labs/dbt_utils":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.ALL_VERSIONS_COMPATIBLE
        elif package == "dbt-labs/codegen":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.ALL_VERSIONS_COMPATIBLE
        elif package == "ScalefreeCOM/datavault4dbt":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE


def test_check_for_package_upgrades():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert package_file is not None
    assert package_file.has_lock_file
    output: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(output) == PROJECT_DEPENDENCY_COUNT + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    for package_result in output:
        print(f"test output: {package_result.id}, {package_result.version_reason}")
        package = package_result.id
        if package == "dbt-labs/dbt_utils":
            assert package_result.version_reason == PackageVersionUpgradeType.TRANSITIVE_DEPENDENCY
            assert (
                package_result.installed_version_compatibility_state
                == PackageVersionFusionCompatibilityState.EXPLICIT_ALLOW
            )
        elif package == "dbt-labs/codegen":
            assert package_result.version_reason == PackageVersionUpgradeType.NO_UPGRADE_REQUIRED
            assert (
                package_result.installed_version_compatibility_state
                == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
            )
        elif package == "ScalefreeCOM/datavault4dbt":
            assert package_result.version_reason == PackageVersionUpgradeType.NO_UPGRADE_REQUIRED
            assert (
                package_result.installed_version_compatibility_state
                == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
            )


def test_upgrade_package_versions_no_force_update():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert package_file is not None
    upgrades: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(upgrades) == PROJECT_DEPENDENCY_COUNT + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    output: PackageUpgradeResult = upgrade_package_versions(
        package_file, upgrades, dry_run=True, override_pinned_version=False
    )
    assert output
    assert not output.upgraded
    assert len(output.upgrades) == 0
    assert len(output.unchanged) == 3
    assert (
        len(output.upgrades) + len(output.unchanged) == PROJECT_DEPENDENCY_COUNT + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    )
    output.print_to_console(json_output=False)
    # output.print_to_console(json_output=True)


def test_upgrade_package_versions_with_force_update():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert package_file is not None
    upgrades: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(upgrades) == PROJECT_DEPENDENCY_COUNT + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    output: PackageUpgradeResult = upgrade_package_versions(
        package_file, upgrades, dry_run=True, override_pinned_version=True
    )
    assert output
    assert not output.upgraded
    assert len(output.upgrades) == 0
    assert len(output.unchanged) == 3
    assert (
        len(output.upgrades) + len(output.unchanged) == PROJECT_DEPENDENCY_COUNT + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    )
    output.print_to_console(json_output=False)
    # output.print_to_console(json_output=True)

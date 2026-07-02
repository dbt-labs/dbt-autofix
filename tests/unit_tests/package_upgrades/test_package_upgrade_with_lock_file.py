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

PROJECT_WITH_PACKAGE_LOCK_PATH = Path("tests/integration_tests/package_upgrades/transitive_dependencies")
# update if count changes
PROJECT_DEPENDENCY_COUNT = 3
PROJECT_TRANSITIVE_DEPENDENCY_COUNT = 13

# version is compatible based on require dbt version but has version override
# dbt_project_evaluator: upgrade to 1.1.2


def test_generate_package_dependencies():
    output: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGE_LOCK_PATH)
    assert output is not None
    assert len(output.package_dependencies) == PROJECT_DEPENDENCY_COUNT
    assert len(output.get_private_package_names()) == 0
    for package in output.package_dependencies:
        assert output.package_dependencies[package].get_installed_package_version() != "unknown"
        fusion_compatibility_state = output.package_dependencies[package].is_installed_version_fusion_compatible()
        package_fusion_compatibility_state: PackageFusionCompatibilityState = output.package_dependencies[
            package
        ].get_package_fusion_compatibility_state()
        if package == "dbt-labs/dbt_utils":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.EXPLICIT_ALLOW
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.ALL_VERSIONS_COMPATIBLE
        elif package == "dbt-labs/dbt_project_evaluator":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.EXPLICIT_DISALLOW
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE


def test_check_for_package_upgrades():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGE_LOCK_PATH)
    assert package_file is not None
    output: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(output) == PROJECT_DEPENDENCY_COUNT
    for package_result in output:
        print(f"test output: {package_result.id}, {package_result.version_reason}")
        package = package_result.id
        fusion_compatibility_state = package_result.version_reason
        if package == "dbt-labs/dbt_utils":
            assert fusion_compatibility_state == PackageVersionUpgradeType.NO_UPGRADE_REQUIRED
        elif package == "dbt-labs/dbt_project_evaluator":
            assert (
                fusion_compatibility_state
                == PackageVersionUpgradeType.PUBLIC_PACKAGE_FUSION_COMPATIBLE_VERSION_EXCEEDS_PROJECT_CONFIG
            )


def test_upgrade_package_versions_no_force_update():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGE_LOCK_PATH)
    assert package_file is not None
    upgrades: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(upgrades) == PROJECT_DEPENDENCY_COUNT
    output: PackageUpgradeResult = upgrade_package_versions(
        package_file, upgrades, dry_run=True, override_pinned_version=False
    )
    assert output
    assert not output.upgraded
    assert len(output.upgrades) == 0
    assert len(output.unchanged) == 3
    assert len(output.upgrades) + len(output.unchanged) == PROJECT_DEPENDENCY_COUNT
    output.print_to_console(json_output=False)
    output.print_to_console(json_output=True)


def test_upgrade_package_versions_with_force_update():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGE_LOCK_PATH)
    assert package_file is not None
    upgrades: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(upgrades) == PROJECT_DEPENDENCY_COUNT
    output: PackageUpgradeResult = upgrade_package_versions(
        package_file, upgrades, dry_run=True, override_pinned_version=True
    )
    assert output
    assert output.upgraded
    assert len(output.upgrades) == 1
    assert len(output.unchanged) == 2
    assert len(output.upgrades) + len(output.unchanged) == PROJECT_DEPENDENCY_COUNT
    output.print_to_console(json_output=False)
    output.print_to_console(json_output=True)

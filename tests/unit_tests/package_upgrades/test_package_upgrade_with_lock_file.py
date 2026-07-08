from pathlib import Path
from typing import Optional

from dbt_fusion_package_tools.dbt_package import DbtPackage
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
    assert isinstance(output.package_dependencies, dict)
    for package in output.package_dependencies:
        assert isinstance(output.package_dependencies[package], DbtPackage)
    assert len(output.package_dependencies) == PROJECT_DEPENDENCY_COUNT
    assert len(output.transitive_dependencies) == PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    assert len(output.get_private_package_names()) == 0
    for package in output.package_dependencies:
        # check that the correct package version is identified based on lock file
        assert output.package_dependencies[package].installed_package_version is not None
        assert output.package_dependencies[package].get_installed_package_version() != "unknown"
        assert output.package_dependencies[package].project_config_version_range is not None
        fusion_compatibility_state = output.package_dependencies[package].is_installed_version_fusion_compatible()
        package_fusion_compatibility_state: PackageFusionCompatibilityState = output.package_dependencies[
            package
        ].get_package_fusion_compatibility_state()
        # packages.yml: [">=1.0.0", "<2.0.0"]
        # package-lock resolves to 1.4.0
        if package == "dbt-labs/dbt_utils":
            assert output.package_dependencies[package].get_installed_package_version() == "1.4.0"
            assert (
                str(output.package_dependencies[package].project_config_raw_version_specifier)
                == "['>=1.0.0', '<2.0.0']"
            )
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.EXPLICIT_ALLOW
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.ALL_VERSIONS_COMPATIBLE
        # packages.yml: [">=1.1.0", "<1.1.2"]
        # package-lock resolves to 1.1.1
        elif package == "dbt-labs/dbt_project_evaluator":
            assert output.package_dependencies[package].get_installed_package_version() == "1.1.1"
            assert (
                str(output.package_dependencies[package].project_config_raw_version_specifier)
                == "['>=1.1.0', '<1.1.2']"
            )
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.EXPLICIT_DISALLOW
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE
        # packages.yml: "2.6.0"
        # package-lock resolves to 2.6.0
        elif package == "fivetran/ad_reporting":
            assert output.package_dependencies[package].get_installed_package_version() == "2.6.0"
            assert str(output.package_dependencies[package].project_config_raw_version_specifier) == "2.6.0"
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.EXPLICIT_ALLOW
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.ALL_VERSIONS_COMPATIBLE
        else:
            raise ValueError(f"Unknown package: {package}")


def test_check_for_package_upgrades():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGE_LOCK_PATH)
    assert package_file is not None
    output: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(output) == PROJECT_DEPENDENCY_COUNT + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
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
    assert len(upgrades) == PROJECT_DEPENDENCY_COUNT + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    output: PackageUpgradeResult = upgrade_package_versions(
        package_file, upgrades, dry_run=True, override_pinned_version=False
    )
    assert output
    assert not output.upgraded
    assert len(output.upgrades) == 0
    assert len(output.unchanged) == 3 + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    assert len(output.upgrades) + len(output.unchanged) == PROJECT_DEPENDENCY_COUNT + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    output.print_to_console(json_output=False)
    output.print_to_console(json_output=True)


def test_upgrade_package_versions_with_force_update():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGE_LOCK_PATH)
    assert package_file is not None
    upgrades: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(upgrades) == PROJECT_DEPENDENCY_COUNT + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    output: PackageUpgradeResult = upgrade_package_versions(
        package_file, upgrades, dry_run=True, override_pinned_version=True
    )
    assert output
    assert output.upgraded
    assert len(output.upgrades) == 1
    assert len(output.unchanged) == 2 + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    assert len(output.upgrades) + len(output.unchanged) == PROJECT_DEPENDENCY_COUNT + PROJECT_TRANSITIVE_DEPENDENCY_COUNT
    output.print_to_console(json_output=False)
    output.print_to_console(json_output=True)

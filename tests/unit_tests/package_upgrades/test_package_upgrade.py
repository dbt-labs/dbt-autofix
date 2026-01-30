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

PROJECT_WITH_PACKAGES_PATH = Path("tests/integration_tests/package_upgrades/mixed_versions")
# update if count changes
PROJECT_DEPENDENCY_COUNT = 10

# cases to test:

# manual override: all versions compatible
# dbt-labs/dbt_utils 0.8.5: no upgrade needed

# manual override: all versions incompatible
# dbt-labs/logging 0.7.0: no upgrade needed (no versions compatible)

# user already has compatible version
# dbt-labs/snowplow 0.9.0: no upgrade needed (version 0.9.0 has compatible require dbt version)

# needs upgrade to version within config range (should always succeed)
# Matts52/dbt_set_similarity 0.2.1: upgrade needed (versions 0.2.2+ compatible)
# dbt_project.yml require-dbt-version: [">=1.1.0", "<2.0.0"]

# needs upgrade to version outside config range (only succeed if force upgrade)
# Matts52/dbt_stat_test 0.1.1: version 0.1.2 compatible but config is pineed to 0.1.1
# dbt_project.yml require-dbt-version: [">=1.1.0", "<2.0.0"]

# incompatible but all versions have unknown version
# avohq/avo_audit 1.0.1: no upgrade needed (all versions require dbt version unknown)
# dbt_project.yml has no require dbt version

# incompatible but no compatible versions >= current version
# there are compatible versions < current version, but we shouldn't downgrade
# MaterializeInc/materialize_dbt_utils 0.6.0: no upgrade needed (no higher compatible version)
# dbt_project.yml require-dbt-version: [">=1.3.0", "<2.0.0"]
# "fusion_compatible_versions": ["=0.1.0", "=0.3.0", "=0.4.0", "=0.5.0"],
# "fusion_incompatible_versions": ["=0.6.0", "=0.7.0"],

# version is compatible based on require dbt version but has version override
# dbt_project_evaluator: upgrade to 1.1.1


def test_generate_package_dependencies():
    output: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
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
        elif package == "dbt-labs/snowplow":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE
        elif package == "dbt-labs/logging":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.EXPLICIT_DISALLOW
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.NO_VERSIONS_COMPATIBLE
        elif package == "Matts52/dbt_set_similarity":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_EXCLUDES_2_0
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE
        elif package == "Matts52/dbt_stat_test":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_EXCLUDES_2_0
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE
        elif package == "avohq/avo_audit":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.NO_DBT_VERSION_RANGE
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.MISSING_COMPATIBILITY
        elif package == "MaterializeInc/materialize_dbt_utils":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_EXCLUDES_2_0
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE
        elif package == "dbt-labs/dbt_project_evaluator":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.EXPLICIT_DISALLOW
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE
        elif package == "calogica/dbt_date":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.EXPLICIT_ALLOW
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.ALL_VERSIONS_COMPATIBLE
        elif package == "brooklyn-data/dbt_artifacts":
            assert fusion_compatibility_state == PackageVersionFusionCompatibilityState.EXPLICIT_DISALLOW
            assert package_fusion_compatibility_state == PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE


def test_check_for_package_upgrades():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert package_file is not None
    output: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(output) == PROJECT_DEPENDENCY_COUNT
    for package_result in output:
        print(f"test output: {package_result.id}, {package_result.version_reason}")
        package = package_result.id
        fusion_compatibility_state = package_result.version_reason
        if package == "dbt-labs/dbt_utils":
            assert fusion_compatibility_state == PackageVersionUpgradeType.NO_UPGRADE_REQUIRED
        elif package == "dbt-labs/snowplow":
            assert fusion_compatibility_state == PackageVersionUpgradeType.NO_UPGRADE_REQUIRED
        elif package == "dbt-labs/logging":
            assert fusion_compatibility_state == PackageVersionUpgradeType.PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION
        elif package == "Matts52/dbt_set_similarity":
            assert fusion_compatibility_state == PackageVersionUpgradeType.UPGRADE_AVAILABLE
        elif package == "Matts52/dbt_stat_test":
            assert (
                fusion_compatibility_state
                == PackageVersionUpgradeType.PUBLIC_PACKAGE_FUSION_COMPATIBLE_VERSION_EXCEEDS_PROJECT_CONFIG
            )
        elif package == "avohq/avo_audit":
            assert fusion_compatibility_state == PackageVersionUpgradeType.PUBLIC_PACKAGE_MISSING_FUSION_ELIGIBILITY
        elif package == "MaterializeInc/materialize_dbt_utils":
            assert fusion_compatibility_state == PackageVersionUpgradeType.PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION
        elif package == "dbt-labs/dbt_project_evaluator":
            assert (
                fusion_compatibility_state
                == PackageVersionUpgradeType.PUBLIC_PACKAGE_FUSION_COMPATIBLE_VERSION_EXCEEDS_PROJECT_CONFIG
            )
        elif package == "calogica/dbt_date":
            assert fusion_compatibility_state == PackageVersionUpgradeType.NO_UPGRADE_REQUIRED
        elif package == "brooklyn-data/dbt_artifacts":
            assert (
                fusion_compatibility_state
                == PackageVersionUpgradeType.PUBLIC_PACKAGE_FUSION_COMPATIBLE_VERSION_EXCEEDS_PROJECT_CONFIG
            )


def test_upgrade_package_versions_no_force_update():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert package_file is not None
    upgrades: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(upgrades) == PROJECT_DEPENDENCY_COUNT
    output: PackageUpgradeResult = upgrade_package_versions(
        package_file, upgrades, dry_run=True, override_pinned_version=False
    )
    assert output
    assert output.upgraded
    assert len(output.upgrades) == 1
    assert len(output.unchanged) == 9
    assert len(output.upgrades) + len(output.unchanged) == PROJECT_DEPENDENCY_COUNT
    output.print_to_console(json_output=False)
    output.print_to_console(json_output=True)


def test_upgrade_package_versions_with_force_update():
    package_file: Optional[DbtPackageFile] = generate_package_dependencies(PROJECT_WITH_PACKAGES_PATH)
    assert package_file is not None
    upgrades: list[PackageVersionUpgradeResult] = check_for_package_upgrades(package_file)
    assert len(upgrades) == PROJECT_DEPENDENCY_COUNT
    output: PackageUpgradeResult = upgrade_package_versions(
        package_file, upgrades, dry_run=True, override_pinned_version=True
    )
    assert output
    assert output.upgraded
    assert len(output.upgrades) == 4
    assert len(output.unchanged) == 6
    assert len(output.upgrades) + len(output.unchanged) == PROJECT_DEPENDENCY_COUNT
    output.print_to_console(json_output=False)
    output.print_to_console(json_output=True)

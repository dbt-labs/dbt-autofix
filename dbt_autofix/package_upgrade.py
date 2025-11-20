from dataclasses import dataclass
from enum import Enum
import json
from pathlib import Path
from typing import Any, Optional
from rich.console import Console

from dbt_autofix.packages.dbt_package import DbtPackage, PackageFusionCompatibilityState
from dbt_autofix.packages.dbt_package_file import (
    DbtPackageFile,
    find_package_yml_files,
    load_yaml_from_dependencies_yml,
    load_yaml_from_packages_yml,
    parse_package_dependencies_from_dependencies_yml,
    parse_package_dependencies_from_packages_yml,
)
from dbt_autofix.packages.dbt_package_text_file import DbtPackageTextFile
from dbt_autofix.packages.dbt_package_version import DbtPackageVersion, FusionCompatibilityState
from dbt_autofix.packages.installed_packages import get_current_installed_package_versions
from dbt_common.semver import VersionSpecifier, VersionRange, versions_compatible


console = Console()
error_console = Console(stderr=True)


class PackageVersionUpgradeType(str, Enum):
    """String enum for package upgrade types"""

    NO_UPGRADE_REQUIRED = "Package is already compatible with Fusion"
    UPGRADE_AVAILABLE = "Package has Fusion-compatible version available"
    PUBLIC_PACKAGE_MISSING_FUSION_ELIGIBILITY = "Public package has not defined fusion eligibility"
    PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION = "Public package is not compatible with fusion"
    PUBLIC_PACKAGE_FUSION_COMPATIBLE_VERSION_EXCEEDS_PROJECT_CONFIG = (
        "Public package has Fusion-compatible version that is outside the project's requested version range"
    )
    PRIVATE_PACKAGE_MISSING_REQUIRE_DBT_VERSION = "Private package requires a compatible require-dbt-version (>=2.0.0, <3.0.0) to be available on fusion. https://docs.getdbt.com/reference/project-configs/require-dbt-version"
    UNKNOWN = "Package's Fusion eligibility unknown"


@dataclass
class PackageVersionUpgradeResult:
    id: str
    public_package: bool
    installed_version: str
    version_reason: PackageVersionUpgradeType
    upgraded_version: Optional[str] = None
    compatible_version: Optional[str] = None
    version_range_config: Optional[str] = None

    def package_should_upgrade(self):
        return self.version_reason == PackageVersionUpgradeType.UPGRADE_AVAILABLE

    def package_final_version(self):
        if self.package_should_upgrade() and self.upgraded_version:
            return self.upgraded_version
        else:
            return self.installed_version

    @property
    def package_upgrade_logs(self):
        return [self.version_reason]

    def to_dict(self) -> dict:
        ret_dict = {"id": self.id, "version": self.package_final_version(), "log": [self.version_reason]}
        return ret_dict


@dataclass
class PackageUpgradeResult:
    dry_run: bool
    file_path: Optional[Path]
    upgraded: bool
    upgrades: list[PackageVersionUpgradeResult]
    unchanged: list[PackageVersionUpgradeResult]

    def print_to_console(self, json_output: bool = True):
        if json_output:
            to_print = {
                "mode": "dry_run" if self.dry_run else "applied",
                "file_path": str(self.file_path),
                "upgrades": [result.to_dict() for result in self.upgrades],
                "unchanged": [result.to_dict() for result in self.unchanged],
            }
            print(json.dumps(to_print))  # noqa: T201
            return

        console.print(
            f"\n{'DRY RUN - NOT APPLIED: ' if self.dry_run else ''}Refactored {self.file_path}:",
            style="green",
        )
        for result in self.upgrades:
            console.print(f"  package {result.id} upgraded to version {result.package_final_version()}", style="yellow")
            for log in result.package_upgrade_logs:
                console.print(f"    {log.value}")
        for result in self.unchanged:
            console.print(
                f"  package {result.id} unchanged from installed version {result.installed_version}", style="yellow"
            )
            for log in result.package_upgrade_logs:
                console.print(f"    {log.value}")
        return


def generate_package_dependencies(root_dir: Path) -> Optional[DbtPackageFile]:
    # check `dependencies.yml`
    # check `packages.yml`
    package_dependencies_yml_files: list[Path] = find_package_yml_files(root_dir)
    if len(package_dependencies_yml_files) != 1:
        package_yml_count = len([x for x in package_dependencies_yml_files if x.name == "packages.yml"])
        dependencies_yml_count = len([x for x in package_dependencies_yml_files if x.name == "dependencies.yml"])
        if package_yml_count > 1 or dependencies_yml_count > 1:
            error_console.log(
                f"Project must contain exactly one packages.yml or dependencies.yml, found {len(package_dependencies_yml_files)}"
            )
            return
        if package_yml_count == 1 and dependencies_yml_count == 1:
            error_console.log(
                f"Project contains both packages.yml and dependencies.yml, package dependencies will only be loaded from packages.yml"
            )
            package_dependencies_yml_files = [x for x in package_dependencies_yml_files if x.name == "packages.yml"]
    dependency_path: Path = package_dependencies_yml_files[0]
    if dependency_path.name == "packages.yml":
        dependency_yaml: dict[Any, Any] = load_yaml_from_packages_yml(dependency_path)
        deps_file: Optional[DbtPackageFile] = parse_package_dependencies_from_packages_yml(
            dependency_yaml, dependency_path
        )
    else:
        dependency_yaml: dict[Any, Any] = load_yaml_from_dependencies_yml(dependency_path)
        deps_file: Optional[DbtPackageFile] = parse_package_dependencies_from_dependencies_yml(
            dependency_yaml, dependency_path
        )
    if not deps_file:
        error_console.log(f"Project dependencies could not be parsed")
        return
    # check installed packages
    installed_packages: dict[str, DbtPackageVersion] = get_current_installed_package_versions(root_dir)

    # merge into dependency configs
    deps_file.merge_installed_versions(installed_packages)

    return deps_file


def check_for_package_upgrades(deps_file: DbtPackageFile) -> list[PackageVersionUpgradeResult]:
    # check all packages for upgrades
    # if dry run, write out package upgrades and exit
    package_version_upgrade_results: list[PackageVersionUpgradeResult] = []

    # create a set of all packages in file - packages will be removed once checked
    packages_to_check: set[str] = set([package for package in deps_file.package_dependencies])

    # the currently installed version fo each package
    installed_package_versions: dict[str, str] = {
        package: deps_file.package_dependencies[package].get_installed_package_version()
        for package in deps_file.package_dependencies
    }

    # all private packages in file
    private_packages: set[str] = set(deps_file.get_private_package_names())

    # packages where the currently installed version is compatible with Fusion
    # includes both manual overrides and require dbt version
    installed_version_compatible: set[str] = set(deps_file.get_installed_version_fusion_compatible())

    # check package level compatibility
    package_fusion_compatibility: dict[PackageFusionCompatibilityState, set[str]] = (
        deps_file.get_package_fusion_compatibility()
    )

    # packages that are fully incompatible, either explicitly or from require dbt version
    all_versions_compatible = package_fusion_compatibility.get(
        PackageFusionCompatibilityState.ALL_VERSIONS_COMPATIBLE, set()
    )
    some_versions_compatible = package_fusion_compatibility.get(
        PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE, set()
    )
    no_versions_compatible = package_fusion_compatibility.get(
        PackageFusionCompatibilityState.NO_VERSIONS_COMPATIBLE, set()
    )
    # packages that don't define dbt require version on any versions in package hub
    missing_compatibility = package_fusion_compatibility.get(
        PackageFusionCompatibilityState.MISSING_COMPATIBILITY, set()
    )

    # now, the actual work begins

    # private packages
    for package in private_packages:
        if package not in packages_to_check:
            continue
        package_version_upgrade_results.append(
            PackageVersionUpgradeResult(
                id=package,
                public_package=False,
                installed_version=installed_package_versions[package],
                version_reason=PackageVersionUpgradeType.PRIVATE_PACKAGE_MISSING_REQUIRE_DBT_VERSION,
            )
        )
        packages_to_check.remove(package)

    # already compatible

    # installed version is compatible
    for package in installed_version_compatible:
        if package not in packages_to_check:
            continue
        package_version_upgrade_results.append(
            PackageVersionUpgradeResult(
                id=package,
                public_package=True,
                installed_version=installed_package_versions[package],
                version_reason=PackageVersionUpgradeType.NO_UPGRADE_REQUIRED,
            )
        )
        packages_to_check.remove(package)

    # all public versions are compatible
    for package in all_versions_compatible:
        if package not in packages_to_check:
            continue
        package_version_upgrade_results.append(
            PackageVersionUpgradeResult(
                id=package,
                public_package=True,
                installed_version=installed_package_versions[package],
                version_reason=PackageVersionUpgradeType.NO_UPGRADE_REQUIRED,
            )
        )
        packages_to_check.remove(package)

    # all public versions are incompatible with Fusion
    for package in no_versions_compatible:
        if package not in packages_to_check:
            continue
        package_version_upgrade_results.append(
            PackageVersionUpgradeResult(
                id=package,
                public_package=True,
                installed_version=installed_package_versions[package],
                version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION,
            )
        )
        packages_to_check.remove(package)

    # all public versions don't define dbt version range
    for package in missing_compatibility:
        if package not in packages_to_check:
            continue
        package_version_upgrade_results.append(
            PackageVersionUpgradeResult(
                id=package,
                public_package=True,
                installed_version=installed_package_versions[package],
                version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_MISSING_FUSION_ELIGIBILITY,
            )
        )
        packages_to_check.remove(package)

    # exit if all packages are accounted for (optimistic)
    if len(packages_to_check) == 0:
        return package_version_upgrade_results

    # otherwise, check individual versions
    for package in deps_file.package_dependencies:
        if package not in packages_to_check or package not in some_versions_compatible:
            continue
        dbt_package = deps_file.package_dependencies[package]
        versions_within_config: list[VersionSpecifier] = (
            dbt_package.find_fusion_compatible_versions_in_requested_range()
        )
        versions_outside_config: list[VersionSpecifier] = (
            dbt_package.find_fusion_compatible_versions_above_requested_range()
        )
        # package has compatible version within config file range
        if len(versions_within_config) > 0:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    public_package=True,
                    installed_version=installed_package_versions[package],
                    compatible_version=versions_within_config[0].to_version_string(skip_matcher=True),
                    version_reason=PackageVersionUpgradeType.UPGRADE_AVAILABLE,
                )
            )
            packages_to_check.remove(package)
            continue
        # package has compatible version higher than config file range
        elif len(versions_outside_config) > 0:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    public_package=True,
                    installed_version=installed_package_versions[package],
                    compatible_version=versions_outside_config[0].to_version_string(skip_matcher=True),
                    version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_FUSION_COMPATIBLE_VERSION_EXCEEDS_PROJECT_CONFIG,
                )
            )
            packages_to_check.remove(package)
            continue
        # package has compatible version but it's lower than the config range
        # (avoids downgrading packages)
        else:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    public_package=True,
                    installed_version=installed_package_versions[package],
                    version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION,
                )
            )
            packages_to_check.remove(package)
            continue

    # fallback
    if len(packages_to_check) > 0:
        for package in packages_to_check:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    public_package=True,
                    installed_version=installed_package_versions[package],
                    version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_MISSING_FUSION_ELIGIBILITY,
                )
            )
    return package_version_upgrade_results


def upgrade_package_versions(
    deps_file: DbtPackageFile,
    package_dependencies_with_upgrades: list[PackageVersionUpgradeResult],
    dry_run: bool = True,
    override_pinned_version: bool = False,
    json_output: bool = False,
) -> PackageUpgradeResult:
    # if package dependencies have upgrades:
    # update dependencies.yml
    # update packages.yml
    # write out dependencies.yml (unless dry run)
    # write out packages.yml (unless dry run)
    if deps_file.file_path is None or len(package_dependencies_with_upgrades) == 0:
        return PackageUpgradeResult(
            dry_run=dry_run,
            file_path=deps_file.file_path,
            upgraded=False,
            upgrades=[],
            unchanged=package_dependencies_with_upgrades,
        )

    packages_with_upgrades: list[PackageVersionUpgradeResult] = []
    packages_with_forced_upgrades: list[PackageVersionUpgradeResult] = []
    packages_with_no_change: list[PackageVersionUpgradeResult] = []
    for package in package_dependencies_with_upgrades:
        if package.version_reason == PackageVersionUpgradeType.UPGRADE_AVAILABLE:
            packages_with_upgrades.append(package)
        elif (
            package.version_reason
            == PackageVersionUpgradeType.PUBLIC_PACKAGE_FUSION_COMPATIBLE_VERSION_EXCEEDS_PROJECT_CONFIG
        ):
            packages_with_forced_upgrades.append(package)
        else:
            packages_with_no_change.append(package)

    print(f"package with upgrades: {[x.id for x in packages_with_upgrades]}")
    print(f"package with forced upgrades: {[x.id for x in packages_with_forced_upgrades]}")
    packages_to_update: dict[str, str] = {}

    if override_pinned_version:
        for package in packages_with_forced_upgrades:
            if package.compatible_version:
                packages_to_update[package.id] = package.compatible_version
    for package in packages_with_upgrades:
        if package.compatible_version:
            packages_to_update[package.id] = package.compatible_version

    print(f"package to update: {[x for x in packages_to_update]}")

    if len(packages_to_update) == 0:
        return PackageUpgradeResult(
            dry_run=dry_run,
            file_path=deps_file.file_path,
            upgraded=False,
            upgrades=[],
            unchanged=package_dependencies_with_upgrades,
        )

    package_text_file = DbtPackageTextFile(file_path=deps_file.file_path)
    updated_packages: set[str] = package_text_file.update_config_file(
        packages_to_update, dry_run=dry_run, print_to_console=True
    )

    upgraded_package_results: list[PackageVersionUpgradeResult] = []
    unchanged_package_results: list[PackageVersionUpgradeResult] = []
    for package in package_dependencies_with_upgrades:
        if package.id in updated_packages:
            package.upgraded_version = package.compatible_version
            upgraded_package_results.append(package)
        else:
            unchanged_package_results.append(package)

    upgrade_result = PackageUpgradeResult(
        dry_run=dry_run,
        file_path=deps_file.file_path,
        upgraded=len(updated_packages) > 0,
        upgrades=upgraded_package_results,
        unchanged=unchanged_package_results,
    )
    upgrade_result.print_to_console(json_output)

    return upgrade_result

from dataclasses import dataclass
from enum import Enum
import json
from pathlib import Path
from typing import Any, Optional
from rich.console import Console

from dbt_autofix.packages.dbt_package import DbtPackage
from dbt_autofix.packages.dbt_package_file import (
    DbtPackageFile,
    find_package_yml_files,
    load_yaml_from_dependencies_yml,
    load_yaml_from_packages_yml,
    parse_package_dependencies_from_dependencies_yml,
    parse_package_dependencies_from_packages_yml,
)
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
    file_path: Path
    upgraded: bool
    upgrades: list[PackageVersionUpgradeResult]
    unchanged: list[PackageVersionUpgradeResult]

    def print_to_console(self, json_output: bool = True):
        if not self.upgraded and not self.dry_run:
            return

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
                console.print(f"    {log}")
        for result in self.unchanged:
            console.print(
                f"  package {result.id} unchanged from installed version {result.installed_version}", style="yellow"
            )
            for log in result.package_upgrade_logs:
                console.print(f"    {log}")
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
            f"Project must contain exactly one projects.yml or dependencies.yml, found {len(package_dependencies_yml_files)}"
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
    packages_to_check: set[str] = set([package for package in deps_file.package_dependencies])
    package_version_upgrade_results: list[PackageVersionUpgradeResult] = []

    # private packages
    for package in deps_file.get_private_package_names():
        installed_package_version = deps_file.package_dependencies[package].get_installed_package_version()
        package_version_upgrade_results.append(
            PackageVersionUpgradeResult(
                id=package,
                public_package=False,
                installed_version=installed_package_version,
                version_reason=PackageVersionUpgradeType.PRIVATE_PACKAGE_MISSING_REQUIRE_DBT_VERSION,
            )
        )
        packages_to_check.remove(package)

    # already compatible
    for package in deps_file.get_installed_version_fusion_compatible():
        if package not in packages_to_check:
            continue
        installed_package_version = deps_file.package_dependencies[package].get_installed_package_version()
        package_version_upgrade_results.append(
            PackageVersionUpgradeResult(
                id=package,
                public_package=True,
                installed_version=installed_package_version,
                version_reason=PackageVersionUpgradeType.NO_UPGRADE_REQUIRED,
            )
        )
        packages_to_check.remove(package)

    # find packages with no possible upgrades
    package_fusion_compatibility: dict[FusionCompatibilityState, list[str]] = (
        deps_file.get_package_fusion_compatibility()
    )
    # all public versions are incompatible with Fusion
    if FusionCompatibilityState.DBT_VERSION_RANGE_EXCLUDES_2_0 in package_fusion_compatibility:
        for package in package_fusion_compatibility[FusionCompatibilityState.DBT_VERSION_RANGE_EXCLUDES_2_0]:
            if package not in packages_to_check:
                continue
            installed_package_version = deps_file.package_dependencies[package].get_installed_package_version()
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    public_package=True,
                    installed_version=installed_package_version,
                    version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION,
                )
            )
            packages_to_check.remove(package)
    # all public versions don't define dbt version range
    if FusionCompatibilityState.NO_DBT_VERSION_RANGE in package_fusion_compatibility:
        for package in package_fusion_compatibility[FusionCompatibilityState.NO_DBT_VERSION_RANGE]:
            if package not in packages_to_check:
                continue
            installed_package_version = deps_file.package_dependencies[package].get_installed_package_version()
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    public_package=True,
                    installed_version=installed_package_version,
                    version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_MISSING_FUSION_ELIGIBILITY,
                )
            )
            packages_to_check.remove(package)

    # exit if all packages are accounted for (optimistic)
    if len(packages_to_check) == 0:
        return package_version_upgrade_results

    # otherwise, check individual versions
    for package in deps_file.package_dependencies:
        if package not in packages_to_check:
            continue
        dbt_package = deps_file.package_dependencies[package]
        installed_package_version = dbt_package.get_installed_package_version()
        versions_within_config: list[VersionSpecifier] = (
            dbt_package.find_fusion_compatible_versions_in_requested_range()
        )
        versions_outside_config: list[VersionSpecifier] = (
            dbt_package.find_fusion_compatible_versions_outside_requested_range()
        )
        if len(versions_within_config) > 0:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    public_package=True,
                    installed_version=installed_package_version,
                    compatible_version=versions_within_config[0].to_version_string(),
                    version_reason=PackageVersionUpgradeType.UPGRADE_AVAILABLE,
                )
            )
            packages_to_check.remove(package)
            continue
        if len(versions_outside_config) > 0:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    public_package=True,
                    installed_version=installed_package_version,
                    compatible_version=versions_outside_config[0].to_version_string(),
                    version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_FUSION_COMPATIBLE_VERSION_EXCEEDS_PROJECT_CONFIG,
                )
            )
            packages_to_check.remove(package)
            continue

    # fallback
    if len(packages_to_check) > 0:
        for package in packages_to_check:
            public_package: bool = deps_file.package_dependencies[package].is_public_package()
            installed_package_version = deps_file.package_dependencies[package].get_installed_package_version()
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    public_package=public_package,
                    installed_version=installed_package_version,
                    version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_MISSING_FUSION_ELIGIBILITY,
                )
            )
    return package_version_upgrade_results


def upgrade_package_versions(deps_file: DbtPackageFile, package_dependencies_with_upgrades: list[PackageVersionUpgradeResult], dry_run: bool, override_pinned_version: bool, json_output: bool) -> int:
    # if package dependencies have upgrades:
    # update dependencies.yml
    # update packages.yml
    # write out dependencies.yml (unless dry run)
    # write out packages.yml (unless dry run)
    packages_with_upgrades = []
    packages_with_forced_upgrades = []
    packages_with_no_change = []
    for package in package_dependencies_with_upgrades:
        if package.version_reason == PackageVersionUpgradeType.UPGRADE_AVAILABLE:
            packages_with_upgrades.append(package)
        elif package.version_reason == PackageVersionUpgradeType.PUBLIC_PACKAGE_FUSION_COMPATIBLE_VERSION_EXCEEDS_PROJECT_CONFIG:
            packages_with_forced_upgrades.append(package)
        else:
            packages_with_no_change.append(package)
    
    for package in packages_with_upgrades:
        pass
    return 0

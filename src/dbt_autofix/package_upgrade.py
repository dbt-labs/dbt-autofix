from dataclasses import dataclass
from importlib.metadata import version
import json
from pathlib import Path
from typing import Any, Optional
from rich.console import Console

from dbt_autofix.packages.dbt_package_file import (
    DbtPackageFile,
    find_package_yml_files,
    load_yaml_from_dependencies_yml,
    load_yaml_from_packages_yml,
    parse_package_dependencies_from_dependencies_yml,
    parse_package_dependencies_from_packages_yml,
)
from dbt_autofix.packages.dbt_package_text_file import DbtPackageTextFile
from dbt_fusion_package_tools.dbt_package_version import DbtPackageVersion
from dbt_autofix.packages.installed_packages import get_current_installed_package_versions
from dbt_fusion_package_tools.version_utils import VersionSpecifier, Matchers, VersionRange

from dbt_fusion_package_tools.manual_overrides import EXPLICIT_ALLOW_ALL_VERSIONS, EXPLICIT_DISALLOW_ALL_VERSIONS
from dbt_fusion_package_tools.upgrade_status import (
    PackageFusionCompatibilityState,
    PackageVersionUpgradeType,
    PackageVersionFusionCompatibilityState,
)


console = Console()
error_console = Console(stderr=True)


@dataclass
class PackageVersionUpgradeResult:
    id: str
    public_package: bool
    installed_version: str
    version_reason: PackageVersionUpgradeType
    installed_version_compatibility_state: PackageVersionFusionCompatibilityState
    upgraded_version_compatibility_state: Optional[PackageVersionFusionCompatibilityState]
    already_compatible: bool = False
    upgraded_version: Optional[str] = None
    compatible_version: Optional[str] = None
    version_range_config: Optional[str] = None
    upgraded: bool = False

    def package_should_upgrade(self):
        return self.version_reason == PackageVersionUpgradeType.UPGRADE_AVAILABLE

    def package_final_version(self):
        if self.package_should_upgrade() and self.upgraded_version:
            return self.upgraded_version
        elif self.upgraded and self.upgraded_version:
            return self.upgraded_version
        else:
            return self.installed_version

    @property
    def package_upgrade_logs(self):
        logs: list[str] = [self.version_reason.value]
        if self.already_compatible:
            current_version_compat = (
                f"Current version is compatible: {self.installed_version_compatibility_state.value}"
            )
        elif self.installed_version_compatibility_state == PackageVersionFusionCompatibilityState.UNKNOWN:
            current_version_compat = "Current version compatibility unknown"
        else:
            current_version_compat = (
                f"Current version is not compatible: {self.installed_version_compatibility_state.value}"
            )
        logs.append(current_version_compat)
        if self.upgraded and self.upgraded_version and self.upgraded_version_compatibility_state is not None:
            logs.append(f"Upgraded version is compatible: {self.upgraded_version_compatibility_state.value}")
        elif self.compatible_version is not None and self.upgraded_version_compatibility_state is not None:
            logs.append(
                f"Compatible version is available ({self.compatible_version}): {self.upgraded_version_compatibility_state.value}"
            )
        return logs

    def to_dict(self) -> dict:
        ret_dict = {"id": self.id, "version": self.package_final_version(), "log": self.package_upgrade_logs}
        return ret_dict


@dataclass
class PackageUpgradeResult:
    dry_run: bool
    force_upgrade: bool
    file_path: Optional[Path]
    upgraded: bool
    upgrades: list[PackageVersionUpgradeResult]
    unchanged: list[PackageVersionUpgradeResult]

    def print_to_console(self, json_output: bool = True):
        if json_output:
            to_print = {
                "command": "packages",
                "mode": "dry_run" if self.dry_run else "applied",
                "file_path": str(self.file_path),
                "force_upgrade": self.force_upgrade,
                "upgrades": [result.to_dict() for result in self.upgrades],
                "unchanged": [result.to_dict() for result in self.unchanged],
                "autofix_version": version("dbt-autofix"),
            }
            print(json.dumps(to_print))  # noqa: T201
            return

        console.print(
            f"\n{'DRY RUN - NOT APPLIED: ' if self.dry_run else ''}Packages updated in {self.file_path}:",
            style="green",
        )
        for result in self.upgrades:
            console.print(f"  package {result.id} upgraded to version {result.package_final_version()}", style="yellow")
            for log in result.package_upgrade_logs:
                console.print(f"    {log}")
        for result in self.unchanged:
            console.print(
                f"  package {result.id} unchanged",
                style="green" if result.version_reason == PackageVersionUpgradeType.NO_UPGRADE_REQUIRED else "bold red",
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

    # the currently installed version of each package
    installed_package_versions: dict[str, str] = {
        package: deps_file.package_dependencies[package].get_installed_package_version()
        for package in deps_file.package_dependencies
    }

    # check package level compatibility
    package_fusion_compatibility: dict[PackageFusionCompatibilityState, set[str]] = (
        deps_file.get_package_fusion_compatibility()
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

    # handle cases where we know we can't upgrade first
    for package in deps_file.package_dependencies:
        if package not in packages_to_check:
            continue
        public_package: bool = deps_file.package_dependencies[package].is_public_package()
        installed_version: str = deps_file.package_dependencies[package].get_installed_package_version()
        installed_version_compat: PackageVersionFusionCompatibilityState = deps_file.package_dependencies[
            package
        ].is_installed_version_fusion_compatible()

        # if version is compatible based on version range, include private packages
        if installed_version_compat == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    upgraded=False,
                    already_compatible=True,
                    public_package=public_package,
                    installed_version=installed_version,
                    version_reason=PackageVersionUpgradeType.NO_UPGRADE_REQUIRED,
                    installed_version_compatibility_state=installed_version_compat,
                    upgraded_version_compatibility_state=None,
                )
            )
            packages_to_check.remove(package)
        # otherwise skip private packages
        elif not public_package:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    upgraded=False,
                    public_package=False,
                    installed_version=installed_version,
                    version_reason=PackageVersionUpgradeType.PRIVATE_PACKAGE_MISSING_REQUIRE_DBT_VERSION,
                    installed_version_compatibility_state=PackageVersionFusionCompatibilityState.UNKNOWN,
                    upgraded_version_compatibility_state=None,
                )
            )
            packages_to_check.remove(package)
        # already compatible version - don't upgrade
        elif (
            installed_version_compat == PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
            or installed_version_compat == PackageVersionFusionCompatibilityState.EXPLICIT_ALLOW
        ):
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    upgraded=False,
                    already_compatible=True,
                    public_package=True,
                    installed_version=installed_version,
                    version_reason=PackageVersionUpgradeType.NO_UPGRADE_REQUIRED,
                    installed_version_compatibility_state=installed_version_compat,
                    upgraded_version_compatibility_state=None,
                )
            )
            packages_to_check.remove(package)
        # package has manual override for all versions - don't upgrade
        elif package in EXPLICIT_ALLOW_ALL_VERSIONS:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    upgraded=False,
                    already_compatible=True,
                    public_package=True,
                    installed_version=installed_version,
                    version_reason=PackageVersionUpgradeType.NO_UPGRADE_REQUIRED,
                    installed_version_compatibility_state=PackageVersionFusionCompatibilityState.EXPLICIT_ALLOW,
                    upgraded_version_compatibility_state=None,
                )
            )
            packages_to_check.remove(package)
        # package has manual override for all versions so can't upgrade
        elif package in EXPLICIT_DISALLOW_ALL_VERSIONS:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    upgraded=False,
                    public_package=True,
                    installed_version=installed_version,
                    version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION,
                    installed_version_compatibility_state=PackageVersionFusionCompatibilityState.EXPLICIT_DISALLOW,
                    upgraded_version_compatibility_state=None,
                )
            )
            packages_to_check.remove(package)
        # all versions have require-dbt-version < 2.0
        elif package in no_versions_compatible:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    upgraded=False,
                    public_package=True,
                    installed_version=installed_version,
                    version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION,
                    installed_version_compatibility_state=PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_EXCLUDES_2_0,
                    upgraded_version_compatibility_state=None,
                )
            )
            packages_to_check.remove(package)
        # all versions don't have require-dbt-version defined
        elif package in missing_compatibility:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    upgraded=False,
                    public_package=True,
                    installed_version=installed_version,
                    version_reason=PackageVersionUpgradeType.PUBLIC_PACKAGE_MISSING_FUSION_ELIGIBILITY,
                    installed_version_compatibility_state=PackageVersionFusionCompatibilityState.UNKNOWN,
                    upgraded_version_compatibility_state=None,
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
        installed_version: str = deps_file.package_dependencies[package].get_installed_package_version()
        installed_version_compat: PackageVersionFusionCompatibilityState = deps_file.package_dependencies[
            package
        ].is_installed_version_fusion_compatible()
        package_version_range: Optional[VersionRange] = deps_file.package_dependencies[
            package
        ].project_config_version_range

        installed_version_spec = dbt_package.installed_package_version
        # in case the user hadn't run dbt deps, estimate version
        if installed_version_spec is None:
            if package_version_range is not None:
                installed_version_spec = package_version_range.start
            else:
                installed_version_spec = VersionSpecifier("0", "0", "0")
        installed_version_spec.matcher = Matchers.EXACT

        versions_within_config: list[VersionSpecifier] = [
            x for x in dbt_package.find_fusion_compatible_versions_in_requested_range() if x > installed_version_spec
        ]
        versions_outside_config: list[VersionSpecifier] = [
            x for x in dbt_package.find_fusion_compatible_versions_above_requested_range() if x > installed_version_spec
        ]

        # package has compatible version within config file range
        if len(versions_within_config) > 0:
            package_version_upgrade_results.append(
                PackageVersionUpgradeResult(
                    id=package,
                    public_package=True,
                    installed_version=installed_package_versions[package],
                    compatible_version=versions_within_config[0].to_version_string(skip_matcher=True),
                    version_reason=PackageVersionUpgradeType.UPGRADE_AVAILABLE,
                    installed_version_compatibility_state=installed_version_compat,
                    upgraded_version_compatibility_state=PackageVersionFusionCompatibilityState.EXPLICIT_ALLOW
                    if package in EXPLICIT_ALLOW_ALL_VERSIONS
                    else PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0,
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
                    installed_version_compatibility_state=installed_version_compat,
                    upgraded_version_compatibility_state=PackageVersionFusionCompatibilityState.EXPLICIT_ALLOW
                    if package in EXPLICIT_ALLOW_ALL_VERSIONS
                    else PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0,
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
                    installed_version_compatibility_state=installed_version_compat,
                    upgraded_version_compatibility_state=PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_EXCLUDES_2_0,
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
                    installed_version_compatibility_state=PackageVersionFusionCompatibilityState.UNKNOWN,
                    upgraded_version_compatibility_state=None,
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
            force_upgrade=override_pinned_version,
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

    packages_to_update: dict[str, str] = {}

    if override_pinned_version:
        for package in packages_with_forced_upgrades:
            if package.compatible_version:
                packages_to_update[package.id] = package.compatible_version
    for package in packages_with_upgrades:
        if package.compatible_version:
            packages_to_update[package.id] = package.compatible_version

    if len(packages_to_update) == 0:
        return PackageUpgradeResult(
            dry_run=dry_run,
            file_path=deps_file.file_path,
            force_upgrade=override_pinned_version,
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

    for result in upgraded_package_results:
        result.upgraded = True
        result.version_reason = PackageVersionUpgradeType.UPGRADE_AVAILABLE

    upgrade_result = PackageUpgradeResult(
        dry_run=dry_run,
        file_path=deps_file.file_path,
        force_upgrade=override_pinned_version,
        upgraded=len(updated_packages) > 0,
        upgrades=upgraded_package_results,
        unchanged=unchanged_package_results,
    )

    return upgrade_result

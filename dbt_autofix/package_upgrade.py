from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from rich.console import Console

from dbt_autofix.packages.dbt_package import DbtPackage
from dbt_autofix.packages.dbt_package_file import DbtPackageFile, find_package_yml_files, load_yaml_from_dependencies_yml, load_yaml_from_packages_yml, parse_package_dependencies_from_dependencies_yml, parse_package_dependencies_from_packages_yml
from dbt_autofix.packages.installed_packages import DbtInstalledPackage, get_current_installed_package_versions


console = Console()


class PackageVersionUpgradeType(str, Enum):
    """String enum for package upgrade types"""

    NO_UPGRADE_REQUIRED = "Package is already compatible with Fusion"
    UPGRADE_AVAILABLE = "Package has Fusion-compatible version available"
    PUBLIC_PACKAGE_MISSING_FUSION_ELIGIBILITY = "Public package has not defined fusion eligibility"
    PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION = "Public package is not compatible with fusion"
    PRIVATE_PACKAGE_MISSING_REQUIRE_DBT_VERSION = "Private package requires a compatible require-dbt-version (>=2.0.0, <3.0.0) to be available on fusion. https://docs.getdbt.com/reference/project-configs/require-dbt-version"


@dataclass
class PackageVersionUpgradeResult:
    id: str
    public_package: bool
    previous_version: str
    compatible_version: str
    upgraded_version: str
    version_reason: PackageVersionUpgradeType

    @property
    def package_upgrade_logs(self):
        return [self.version_reason]

    def to_dict(self) -> dict:
        ret_dict = {"version_reason": [self.version_reason]}
        return ret_dict


@dataclass
class PackageUpgradeResult:
    dry_run: bool
    file_path: Path
    upgraded: bool
    upgrades: list[PackageVersionUpgradeResult]
    unchanged: list[PackageVersionUpgradeResult]

    def print_to_console(self, json_output: bool = True):
        # if not self.refactored:
        #     return

        # if json_output:
        #     flattened_refactors = []
        #     for refactor in self.refactors:
        #         if refactor.refactored:
        #             flattened_refactors.extend(refactor.to_dict()["deprecation_refactors"])

        #     to_print = {
        #         "mode": "dry_run" if self.dry_run else "applied",
        #         "file_path": str(self.file_path),
        #         "refactors": flattened_refactors,
        #     }
        #     print(json.dumps(to_print))  # noqa: T201
        #     return

        # console.print(
        #     f"\n{'DRY RUN - NOT APPLIED: ' if self.dry_run else ''}Refactored {self.file_path}:",
        #     style="green",
        # )
        # for refactor in self.refactors:
        #     if refactor.refactored:
        #         console.print(f"  {refactor.rule_name}", style="yellow")
        #         for log in refactor.refactor_logs:
        #             console.print(f"    {log}")
        return


def generate_package_dependencies(root_dir: Path) -> dict[str, DbtPackage]:
    # check `dependencies.yml`
    # check `packages.yml`
    package_dependencies_yml_files: list[Path] = find_package_yml_files(root_dir)
    if len(package_dependencies_yml_files) != 1:
        console.log(f"Project must contain exactly one projects.yml or dependencies.yml, found {len(package_dependencies_yml_files)}")
        return {}
    dependency_path: Path = package_dependencies_yml_files[0]
    if dependency_path.name == "packages.yml":
        dependency_yaml: dict[Any, Any] = load_yaml_from_packages_yml(dependency_path)
        deps_file: Optional[DbtPackageFile] = parse_package_dependencies_from_packages_yml(dependency_yaml, dependency_path)
    else:
        dependency_yaml: dict[Any, Any] = load_yaml_from_dependencies_yml(dependency_path)
        deps_file: Optional[DbtPackageFile] = parse_package_dependencies_from_dependencies_yml(dependency_yaml, dependency_path)
    if not deps_file:
        console.log(f"Project depedencies could not be parsed")
        return {}
    # check installed packages
    installed_packages: dict[str, DbtPackage] = get_current_installed_package_versions(root_dir)
    # merge into dependency configs
    package_lookup = deps_file.get_reverse_lookup_by_package_name()
    for package in installed_packages:
        package_id = package_lookup[package]
        deps_file.set_installed_version_for_package(package_id, installed_packages[package].installed_package_version)
    # return get_current_installed_package_versions(root_dir)
    return deps_file # ?
    


def check_for_package_upgrades(package_dependencies: dict[str, DbtPackage]) -> list[PackageUpgradeResult]:
    # check all packages for upgrades
    # if dry run, write out package upgrades and exit
    return []


def upgrade_package_versions(package_dependencies_with_upgrades: list[PackageUpgradeResult], json_output: bool):
    # if package dependencies have upgrades:
    # update dependencies.yml
    # update packages.yml
    # write out dependencies.yml (unless dry run)
    # write out packages.yml (unless dry run)
    pass


def delete_existing_packages():
    # delete package-lock.yml
    # delete `dbt_packages/` directory
    pass


def run_dbt_deps():
    # run dbt deps
    pass
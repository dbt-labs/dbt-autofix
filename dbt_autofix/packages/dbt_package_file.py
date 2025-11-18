from collections import defaultdict
from typing import Any, Optional
from dbt_autofix.packages.dbt_package import DbtPackage
from dbt_autofix.packages.dbt_package_version import (
    DbtPackageVersion,
    FusionCompatibilityState,
    convert_optional_version_string_to_spec,
    convert_version_string_list_to_spec,
)
from dataclasses import dataclass, field
from pathlib import Path
from rich.console import Console
from dbt_autofix.refactors.yml import read_file
from dbt_autofix.packages.fusion_version_compatibility_output import FUSION_VERSION_COMPATIBILITY_OUTPUT
from dbt_common.semver import VersionSpecifier, VersionRange, versions_compatible

console = Console()


VALID_PACKAGE_YML_NAMES: set[str] = set(["packages.yml", "dependencies.yml"])


def find_package_yml_files(
    root_dir: Path,
) -> list[Path]:
    """Get file paths for YML files that define package dependencies.

    Find YML files that define package dependencies for the project
    (packages.yml and dependencies.yml).

    Args:
        root_dir (Path): the root directory of the project

    Returns:
        list[Path]: the file path(s) for packages/dependencies.yml
    """
    yml_files = set(root_dir.glob("*.yml")).union(set(root_dir.glob("*.yaml")))

    package_yml_files = []

    for yml_file in yml_files:
        if yml_file.name in VALID_PACKAGE_YML_NAMES:
            package_yml_files.append(yml_file)

    if len(package_yml_files) == 0:
        console.log("No package YML files found")
    return package_yml_files


def load_yaml_from_packages_yml(packages_yml_path: Path) -> dict[Any, Any]:
    """Parse YAML from a packages.yml file.

    Args:
        packages_yml_path (Path): file path for packages.yml

    Returns:
        dict[Any, Any]: parsed YAML content

    Example:
        packages.yml content:
            packages:
              - name: dbt_external_tables
                package: dbt-labs/dbt_external_tables
                version: 0.8.7
        Output:
            {
                'packages': [
                    {
                        'name': 'dbt_external_tables',
                        'package': 'dbt-labs/dbt_external_tables',
                        'version': '0.8.7',
                    }
                ]
            }
    """
    if packages_yml_path.name != "packages.yml":
        console.log("File must be packages.yml")
        return {}

    try:
        parsed_package_file = read_file(packages_yml_path)
    except:
        console.log(f"Error when parsing package file {packages_yml_path}")
        return {}
    if parsed_package_file == {}:
        console.log("No content parsed")
        return {}
    else:
        return parsed_package_file


# Same as load_yaml_from_packages_yml
def load_yaml_from_dependencies_yml(dependencies_yml_path: Path) -> dict[Any, Any]:
    if dependencies_yml_path.name != "dependencies.yml":
        console.log("File must be dependencies.yml")
        return {}

    try:
        parsed_package_file = read_file(dependencies_yml_path)
    except:
        console.log(f"Error when parsing package file {dependencies_yml_path}")
        return {}
    if parsed_package_file == {}:
        console.log("No content parsed")
        return {}
    else:
        return parsed_package_file


@dataclass
class DbtPackageFile:
    package_file_name: str
    file_path: Optional[Path]
    yml_dependencies: dict[Any, Any]
    # this is indexed by package id for uniqueness (hopefully)
    package_dependencies: dict[str, DbtPackage] = field(default_factory=dict)

    def parse_file_path_to_string(self):
        if not self.file_path:
            console.log("No file path set")
            return
        try:
            self.yml_str = self.file_path.read_text()
            console.log(f"parsed yaml string: {self.yml_str}")
        except:
            console.log(f"Error when parsing package file {self.file_path}")

    def add_package_dependency(self, package_id: str, package: DbtPackage):
        if package_id in self.package_dependencies:
            console.log(f"{package_id} is already in the dependencies")
        else:
            self.package_dependencies[package_id] = package

    # the package dependencies are indexed by package ID, but packages'
    # dbt_project.yml only has the package name - so this reverse lookup
    # lets us match from the installed package to the project's deps.
    # this returns the lookup table so subsequent lookups are O(1) (hopefully)
    # and we can detect duplicate names in advance
    def get_reverse_lookup_by_package_name(self) -> dict[str, str]:
        lookup: dict[str, str] = {}
        for package_id in self.package_dependencies:
            package_name = self.package_dependencies[package_id].package_name
            if package_name in lookup:
                console.log(f"Duplicate package name {package_name} for package_id {package_id}")
            else:
                lookup[package_name] = package_id
        return lookup

    def set_installed_version_for_package(self, package_id: str, package_version: DbtPackageVersion) -> bool:
        return self.package_dependencies[package_id].add_package_version(package_version, installed=True)

    def add_version_for_package(self, package_id: str, package_version: DbtPackageVersion, installed=False) -> bool:
        return self.package_dependencies[package_id].add_package_version(package_version, installed=installed)

    def merge_installed_versions(self, installed_packages: dict[str, DbtPackageVersion]) -> int:
        package_lookup: dict[str, str] = self.get_reverse_lookup_by_package_name()
        installed_count: int = 0
        for package in installed_packages:
            if package not in package_lookup:
                console.log(f"Installed package name {package} not found in package deps")
                continue
            package_id = package_lookup[package]
            if self.set_installed_version_for_package(package_id, installed_packages[package]):
                installed_count += 1
        return installed_count

    def merge_fusion_compatibility_output(self) -> int:
        package_compat_count = 0
        for package in self.package_dependencies:
            output = FUSION_VERSION_COMPATIBILITY_OUTPUT.get(package)
            if output is None:
                continue
            oldest_fusion_compatible_version = convert_optional_version_string_to_spec(
                output["oldest_fusion_compatible_version"]
            )
            # latest_fusion_compatible_version = convert_optional_version_string_to_spec(output["latest_fusion_compatible_version"])
            fusion_compatible_versions = convert_version_string_list_to_spec(output["fusion_compatible_versions"])
            fusion_incompatible_versions = convert_version_string_list_to_spec(output["fusion_incompatible_versions"])
            unknown_compatibility_versions = convert_version_string_list_to_spec(output["fusion_incompatible_versions"])
            self.package_dependencies[package].lowest_fusion_compatible_version = oldest_fusion_compatible_version
            # self.package_dependencies[package].latest_fusion_compatible_version = latest_fusion_compatible_version
            self.package_dependencies[package].fusion_compatible_versions = fusion_compatible_versions
            self.package_dependencies[package].fusion_incompatible_versions = fusion_incompatible_versions
            self.package_dependencies[package].unknown_compatibility_versions = unknown_compatibility_versions
            package_compat_count += 1
        return package_compat_count

    def get_private_package_names(self) -> list[str]:
        return [
            package for package in self.package_dependencies if not self.package_dependencies[package].is_public_package
        ]

    def get_installed_version_fusion_compatible(self) -> list[str]:
        package_names = []
        for package in self.package_dependencies:
            installed_version_compatibility: FusionCompatibilityState = self.package_dependencies[
                package
            ].installed_version_fusion_compatibility
            if (
                installed_version_compatibility == FusionCompatibilityState.EXPLICIT_ALLOW
                or installed_version_compatibility == FusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
            ):
                package_names.append(package)
        return package_names

    def get_package_fusion_compatibility(self) -> dict[FusionCompatibilityState, list[str]]:
        compatibility: dict[FusionCompatibilityState, list[str]] = defaultdict(list)
        for package in self.package_dependencies:
            fusion_compatibility = self.package_dependencies[package].get_fusion_compatibility_state()
            compatibility[fusion_compatibility].append(package)
        return compatibility


def parse_package_dependencies_from_yml(
    parsed_yml: dict[Any, Any], package_file_name: str, package_file_path: Optional[Path]
) -> Optional[DbtPackageFile]:
    if "packages" not in parsed_yml:
        console.log("YML must contain packages key")
        return
    package_dict: dict[Any, Any] = parsed_yml["packages"]

    package_file = DbtPackageFile(
        package_file_name=package_file_name, file_path=package_file_path, yml_dependencies=package_dict
    )
    for idx, package in enumerate(package_dict):
        if "package" not in package:
            package_id = f"package_{idx}"
        else:
            package_id: str = str(package["package"])
        package_name: str = package_id.split("/")[-1]
        version: Optional[Any] = package.get("version")
        local: bool = "local" in package
        git: bool = "git" in package
        tarball = "tarball" in package
        new_package = DbtPackage(
            package_name=package_name,
            package_id=package_id,
            local=local,
            git=git,
            tarball=tarball,
            project_config_raw_version_specifier=version,
        )
        package_file.add_package_dependency(package_id, new_package)
    package_file.merge_fusion_compatibility_output()

    return package_file


# I think the parsing should be the same for packages.yml and dependencies.yml,
# but I'm making separate functions in case they need to be customized
def parse_package_dependencies_from_packages_yml(
    parsed_packages_yml: dict[Any, Any], package_file_path: Optional[Path]
) -> Optional[DbtPackageFile]:
    return parse_package_dependencies_from_yml(parsed_packages_yml, "packages.yml", package_file_path)


def parse_package_dependencies_from_dependencies_yml(
    parsed_dependencies_yml: dict[Any, Any], package_file_path: Optional[Path]
) -> Optional[DbtPackageFile]:
    return parse_package_dependencies_from_yml(parsed_dependencies_yml, "dependencies.yml", package_file_path)


def merge_installed_version_with_deps(deps_file: DbtPackageFile, installed_packages: dict[str, DbtPackageVersion]):
    package_lookup = deps_file.get_reverse_lookup_by_package_name()
    for package in installed_packages:
        package_id = package_lookup[package]
        deps_file.set_installed_version_for_package(package_id, installed_packages[package])

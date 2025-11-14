from typing import Any, Optional, Union
from dbt_autofix.packages.dbt_package import DbtPackage
from dbt_autofix.packages.dbt_package_version import get_versions
from dbt_autofix.packages.installed_packages import DbtInstalledPackage
from dbt_autofix.refactors.yml import DbtYAML
from dataclasses import dataclass, field
from pathlib import Path
from pprint import pprint
from rich.console import Console
from dbt_autofix.refactors.yml import DbtYAML, read_file

console = Console()


VALID_PACKAGE_YML_NAMES: set[str] = set(["packages.yml", "dependencies.yml"])


def find_package_yml_files(
    root_dir: Path,
) -> list[Path]:
    yml_files = set(root_dir.glob("**/*.yml")).union(set(root_dir.glob("**/*.yaml")))

    package_yml_files = []

    for yml_file in yml_files:
        if yml_file.name in VALID_PACKAGE_YML_NAMES:
            package_yml_files.append(yml_file)

    if len(package_yml_files) == 0:
        console.log("No package YML files found")
    return package_yml_files




def load_yaml_from_packages_yml(packages_yml_path: Path) -> dict[Any, Any]:
    if packages_yml_path.name != "packages.yml":
        console.log("File must be packages.yml")
        return {}
    # try:
    #     yml_str = package_project_yml_path.read_text()
    # except:
    #     console.log(f"Error when parsing package file {package_project_yml_path}")
    #     return
    try:
        # parsed_package_file = DbtYAML().load(yml_str) or {}
        parsed_package_file = read_file(packages_yml_path)
    except:
        console.log(f"Error when parsing package file {packages_yml_path}")
        return {}
    if parsed_package_file == {}:
        console.log("No content parsed")
        return {}
    else:
        return parsed_package_file


def load_yaml_from_dependencies_yml(dependencies_yml_path: Path) -> dict[Any, Any]:
    if dependencies_yml_path.name != "dependencies.yml":
        console.log("File must be dependencies.yml")
        return {}
    # try:
    #     yml_str = package_project_yml_path.read_text()
    # except:
    #     console.log(f"Error when parsing package file {package_project_yml_path}")
    #     return
    try:
        # parsed_package_file = DbtYAML().load(yml_str) or {}
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

    def set_installed_package_versions(self, installed_packages: dict[str, DbtInstalledPackage]):
        for package in installed_packages:
            if package not in self.package_dependencies:
                continue
            # self.package_dependencies[package].installed_version
    
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


def parse_package_dependencies_from_yml(parsed_yml: dict[Any, Any], package_file_name: str, package_file_path: Optional[Path]) -> Optional[DbtPackageFile]:
    if "packages" not in parsed_yml:
        console.log("YML must contain packages key")
        return
    package_dict: dict[Any, Any] = parsed_yml["packages"]
    
    package_file = DbtPackageFile(package_file_name=package_file_name, file_path=package_file_path, yml_dependencies=package_dict)
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
            project_config_raw_version_specifier=version
        )
        package_file.add_package_dependency(
            package_id,
            new_package
        )
        
    return package_file
            

# I think the parsing should be the same for packages.yml and dependencies.yml,
# but I'm making separate functions in case they need to be customized
def parse_package_dependencies_from_packages_yml(parsed_packages_yml: dict[Any, Any], package_file_path: Optional[Path]) -> Optional[DbtPackageFile]:
    return parse_package_dependencies_from_yml(parsed_packages_yml, "packages.yml", package_file_path)

def parse_package_dependencies_from_dependencies_yml(parsed_dependencies_yml: dict[Any, Any], package_file_path: Optional[Path]) -> Optional[DbtPackageFile]:
    return parse_package_dependencies_from_yml(parsed_dependencies_yml, "dependencies.yml", package_file_path)

# def add_package_info_from_installed_packages(root_directory: Path, package_file: DbtPackageFile) -> DbtPackageFile:
#     installed_packages: dict[str, DbtInstalledPackage] = get_current_installed_package_versions(root_directory)
#     package_file.set_installed_package_versions(installed_packages)
#     return package_file


# def parse_package_files(package_file_paths: list[Path]) -> list[DbtPackageFile]:
#     if package_file_paths == []:
#         return []

#     package_files: list[DbtPackageFile] = []
#     for path in package_file_paths:
#         console.log(f"package path: {path}")
#         package_file = DbtPackageFile(path)
#         package_file.parse_file_path_to_string()
#         package_file.parse_yml_string_to_dict()
#         package_file.parse_package_dependencies()
#         package_files.append(package_file)
#     return package_files

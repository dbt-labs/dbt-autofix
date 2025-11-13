from typing import Any, Optional, Union
from dbt_autofix.packages.dbt_package import DbtPackage
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
    file_path: Optional[Path]
    yml_str: str = ""
    yml_dict: list[dict[str, Union[str, list[str]]]] = field(default_factory=list)
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

    def parse_yml_string_to_dict(self):
        if not self.yml_str:
            console.log("No YML string found, use parse_file_path_to_string first")
        try:
            parsed_package_file = DbtYAML().load(self.yml_str) or {}
        except:
            console.log(f"Error when parsing package file {self.file_path}")
            return
        if parsed_package_file == {}:
            console.log("No content parsed")
            return
        if "packages" not in parsed_package_file:
            console.log("File does not contain packages key")
            return
        for package in parsed_package_file["packages"]:
            print(f"package: {package}")
            self.yml_dict.append(package)
        else:
            console.log(f"Package file {self.file_path} could not be parsed")
        console.log(pprint(self.yml_dict))

    def parse_package_dependencies(self):
        for k, v in self.yml_dict:
            console.log(f"k: {k}, v: {v}")

    def add_package_dependency(self, package_name: str, package: DbtPackage):
        if package_name in self.package_dependencies:
            console.log(f"{package_name} is already in the dependencies")
        else:
            self.package_dependencies[package_name] = package

    def set_installed_package_versions(self, installed_packages: dict[str, DbtInstalledPackage]):
        for package in installed_packages:
            if package not in self.package_dependencies:
                continue
            # self.package_dependencies[package].installed_version


def parse_package_dependencies_from_packages_yml(parsed_packages_yml: dict[Any, Any]) -> Optional[DbtPackageFile]:
    return

def parse_package_dependencies_from_dependencies_yml(parsed_dependencies_yml: dict[Any, Any]) -> Optional[DbtPackageFile]:
    return

# def add_package_info_from_installed_packages(root_directory: Path, package_file: DbtPackageFile) -> DbtPackageFile:
#     installed_packages: dict[str, DbtInstalledPackage] = get_current_installed_package_versions(root_directory)
#     package_file.set_installed_package_versions(installed_packages)
#     return package_file


def parse_package_files(package_file_paths: list[Path]) -> list[DbtPackageFile]:
    if package_file_paths == []:
        return []

    package_files: list[DbtPackageFile] = []
    for path in package_file_paths:
        console.log(f"package path: {path}")
        package_file = DbtPackageFile(path)
        package_file.parse_file_path_to_string()
        package_file.parse_yml_string_to_dict()
        package_file.parse_package_dependencies()
        package_files.append(package_file)
    return package_files
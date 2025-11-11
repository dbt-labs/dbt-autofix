from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from pprint import pprint
from typing import Any, Optional, Union
from semver.version import Version
import yaml
import yamllint
import yamllint.linter
import yamllint.config
from rich.console import Console
from dbt_autofix.refactors.yml import DbtYAML, read_file
from dbt_autofix.packages.installed_packages import DbtInstalledPackage, find_package_paths, parse_package_info_from_package_dbt_project_yml, get_current_installed_package_versions

console = Console()

config = """
rules:
  key-duplicates: enable
"""

yaml_config = yamllint.config.YamlLintConfig(config)

VALID_PACKAGE_YML_NAMES: set[str] = set(['packages.yml', 'dependencies.yml'])

@dataclass
class DbtPackage:
    package_dict: dict[str, Union[str,list[str]]]
    package_id: Optional[str] = None
    package_name: Optional[str] = None
    package_version_str: Optional[list[str]] = None
    package_version: Optional[Version] = None
    current_project_package_version_str: Optional[str] = None
    current_project_package_version: Optional[Version] = None
    current_project_package_version_range_str: Optional[list[str]] = None
    current_project_package_version_range: Optional[list[Version]] = None
    require_dbt_version: Optional[list[str]] = None
    min_upgradeable_version: Optional[str] = None
    max_upgradeable_version: Optional[str] = None
    lowest_fusion_compatible_version: Optional[str] = None
    fusion_compatible_versions: Optional[list[Version]] = None
    git_url: Optional[str] = None
    installed_version: Optional[DbtInstalledPackage] = None
    opt_in_prerelease: bool = False
    fusion_dbt_version: str = "2.0.0"

    def __post_init__(self):
        self.parse_package_dict()
        self.fusion_dbt_version_semver = Version.parse(self.fusion_dbt_version)

    def parse_package_dict(self):
        if "package" in self.package_dict and type(self.package_dict["package"]) == "str":
            self.package_name = str(self.package_dict["package"])
        if "version" in self.package_dict:
            if type(self.package_dict["version"] == "list"):
                self.package_version_str = [str(version) for version in self.package_dict["version"]]
            elif type(self.package_dict["version"] == "str"):
                self.package_version_str = [str(self.package_dict["version"])]
        if "git" in self.package_dict:
            self.git_url = str(self.package_dict["git"])
        if "install-prerelease" in self.package_dict:
            if str(self.package_dict["install-prerelease"]) == "true":
                self.opt_in_prerelease = True
            else:
                self.opt_in_prerelease = False

    def parse_current_package_version_range(self):
        pass

    def is_dbt_version_fusion_compatible(self, dbt_version_range: list[str]) -> bool:
        dbt_fusion_version = self.fusion_dbt_version_semver
        try:
            compatible_versions: list[bool] = [dbt_fusion_version.match(x) for x in dbt_version_range]
            return all(compatible_versions)
        except:
            return False


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
            self.package_dependencies[package].installed_version


def find_package_paths(
    root_dir: Path,
) -> list[Path]:
    packages_path = yaml.safe_load((root_dir / "dbt_project.yml").read_text()).get(
        "packages-install-path", "dbt_packages"
    )

    yml_files_packages = set((root_dir / packages_path).glob("**/*.yml")).union(
        set((root_dir / packages_path).glob("**/*.yaml"))
    )

    # this is a hack to avoid checking integration_tests. it won't work everywhere but it's good enough for now
    yml_files_packages_integration_tests = set((root_dir / packages_path).glob("**/integration_tests/**/*.yml")).union(
        set((root_dir / packages_path).glob("**/integration_tests/**/*.yaml"))
    )
    yml_files_packages_not_integration_tests = yml_files_packages - yml_files_packages_integration_tests

    return [Path(str(path)) for path in yml_files_packages_not_integration_tests if path.name == "dbt_project.yml"]


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


def add_package_info_from_installed_packages(root_directory: Path, package_file: DbtPackageFile) -> DbtPackageFile:
    installed_packages: dict[str, DbtInstalledPackage] = get_current_installed_package_versions(root_directory)
    package_file.set_installed_package_versions(installed_packages)
    return package_file
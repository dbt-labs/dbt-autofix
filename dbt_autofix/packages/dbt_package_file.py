from typing import Optional, Union
from dbt_autofix.packages.dbt_package import DbtPackage
from dbt_autofix.packages.installed_packages import DbtInstalledPackage
from dbt_autofix.packages.package_upgrade_versions import console
from dbt_autofix.refactors.yml import DbtYAML
from dataclasses import dataclass, field
from pathlib import Path
from pprint import pprint


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

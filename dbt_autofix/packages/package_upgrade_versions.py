from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from semver.version import Version
import yaml
import yamllint
import yamllint.linter
import yamllint.config
from rich.console import Console
from dbt_autofix.refactors.yml import DbtYAML, read_file

console = Console()

config = """
rules:
  key-duplicates: enable
"""

yaml_config = yamllint.config.YamlLintConfig(config)

@dataclass
class DbtPackage:
    package_id: str
    package_name: Optional[str]
    package_version_str: str
    package_version: Optional[Version]
    current_project_package_version_str: Optional[str]
    current_project_package_version: Optional[Version]
    current_project_package_version_range_str: Optional[list[str]]
    current_project_package_version_range: Optional[list[Version]]
    require_dbt_version: Optional[list[str]]
    min_upgradeable_version: Optional[str]
    max_upgradeable_version: Optional[str]
    lowest_fusion_compatible_version: Optional[str]
    fusion_compatible_versions: Optional[list[Version]]

    def __post_init__(self):
        pass

    def parse_current_package_version(self):
        pass


@dataclass
class DbtPackageFile:
    file_path: Path
    yml_str: str = ""
    yml_dict: dict[str, str] = field(default_factory=dict)
    package_dependencies: dict[str, DbtPackage] = field(default_factory=dict)

    def parse_file_path_to_string(self):
        try:
            self.yml_str = self.file_path.read_text()
        except:
            console.log(f"Error when parsing package file {self.file_path}")
    
    def parse_yml_string_to_dict(self):
        try:
            parsed_package_file = DbtYAML().load(self.yml_str) or {}
        except:
            console.log(f"Error when parsing package file {self.file_path}")
            return
        if parsed_package_file != {}:
            for k, v in parsed_package_file:
                self.yml_dict[str(k)] = str(v)
            else:
                console.log(f"Package file {self.file_path} could not be parsed")

    def parse_package_dependencies(self):
        for k, v in self.yml_dict:
            console.log(f"k: {k}, v: {v}")

    def add_package_dependency(self, package_name: str, package: DbtPackage):
        if package_name in self.package_dependencies:
            console.log(f"{package_name} is already in the dependencies")
        else:
            self.package_dependencies[package_name] = package


def find_package_files(
    root_dir: Path,
# ) -> Tuple[List[DuplicateFound], List[DuplicateFound]]:
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

    return [Path(str(path)) for path in yml_files_packages_not_integration_tests]


def parse_package_files(package_file_paths: list[Path]):
    if package_file_paths == []:
        return []
    
    package_files: list[DbtPackageFile] = []
    for path in package_file_paths:
        package_file = DbtPackageFile(path)
        package_file.parse_file_path_to_string()
        package_file.parse_yml_string_to_dict()
        package_file.parse_package_dependencies()
        package_files.append(package_file)
    return package_files


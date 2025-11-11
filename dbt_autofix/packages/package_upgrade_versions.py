from collections import defaultdict
from pathlib import Path
from typing import Any, Optional, Union
import yaml
import yamllint
import yamllint.linter
import yamllint.config
from rich.console import Console
from dbt_autofix.packages.dbt_package_file import DbtPackageFile
from dbt_autofix.packages.installed_packages import (
    DbtInstalledPackage,
    find_package_paths,
    parse_package_info_from_package_dbt_project_yml,
    get_current_installed_package_versions,
)

console = Console()

config = """
rules:
  key-duplicates: enable
"""

yaml_config = yamllint.config.YamlLintConfig(config)

VALID_PACKAGE_YML_NAMES: set[str] = set(["packages.yml", "dependencies.yml"])


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

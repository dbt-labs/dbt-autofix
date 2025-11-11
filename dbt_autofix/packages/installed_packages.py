

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union
from rich.console import Console
import yaml

from dbt_autofix.refactors.yml import DbtYAML

console = Console()


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


@dataclass
class DbtInstalledPackage:
    package_name: str
    version: str
    require_dbt_version: list[str]

def parse_package_info_from_package_dbt_project_yml(package_project_yml_path: Path) -> Optional[DbtInstalledPackage]:
    if package_project_yml_path.name != "dbt_project.yml":
        console.log("File must be dbt_project.yml")
        return
    try:
        yml_str = package_project_yml_path.read_text()
    except:
        console.log(f"Error when parsing package file {package_project_yml_path}")
        return
    try:
        parsed_package_file = DbtYAML().load(yml_str) or {}
    except:
        console.log(f"Error when parsing package file {package_project_yml_path}")
        return
    if parsed_package_file == {}:
        console.log("No content parsed")
        return

    if "name" in parsed_package_file:
        package_name = str(parsed_package_file["name"])
    else:
        console.log("Package must contain name")
        return
    
    if "version" in parsed_package_file:
        version = str(parsed_package_file["version"])
    else:
        console.log("Package must contain version")
        return
    
    if "require-dbt-version" in parsed_package_file:
        if type(parsed_package_file["require-dbt-version"]) == "list":
            require_dbt_version = [str(key) for key in parsed_package_file["require-dbt-version"]]
        elif type(parsed_package_file["require-dbt-version"]) == "str":
            require_dbt_version = [str(parsed_package_file["require-dbt-version"])]
    else:
        require_dbt_version = []
    
    return DbtInstalledPackage(package_name=package_name, version=version, require_dbt_version=require_dbt_version)



def get_current_installed_package_versions(root_dir: Path) -> dict[str, DbtInstalledPackage]:
    installed_package_paths = find_package_paths(root_dir)
    installed_package_versions: dict[str, DbtInstalledPackage] = {}
    if len(installed_package_paths) == 0:
        console.log("No packages installed. Please run dbt deps first")
        return installed_package_versions
    for package_path in installed_package_paths:
        package_info = parse_package_info_from_package_dbt_project_yml(package_path)
        if not package_info:
            console.log("Parsing failed on package")
            continue
        package_name = package_info.package_name
        installed_package_versions[package_name] = package_info
    return installed_package_versions

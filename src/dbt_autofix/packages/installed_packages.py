import os
from pathlib import Path
from typing import Any, Optional, Union
from rich.console import Console
import yaml

from dbt_fusion_package_tools.dbt_package_version import DbtPackageVersion
from dbt_autofix.refactors.yml import read_file

console = Console()

# Helper functions to find currently installed packages
# and construct DbtPackageVersions from the package's dbt_project.yml


def find_packages_within_directory(installed_packages_dir: Union[Path, str]) -> list[Path]:
    if type(installed_packages_dir) == str:
        installed_packages_path = Path(installed_packages_dir)
    elif isinstance(installed_packages_dir, Path):
        installed_packages_path = installed_packages_dir
    else:
        return []
    if not installed_packages_path.exists() or not installed_packages_path.is_dir():
        return []

    # we only go one level deep here to avoid parsing the dependencies of dependnecies
    yml_files_packages = set((installed_packages_path).glob("*/*.yml")).union(
        set((installed_packages_path).glob("*/*.yaml"))
    )

    # this is a hack to avoid checking integration_tests. it won't work everywhere but it's good enough for now
    yml_files_packages_integration_tests = set((installed_packages_path).glob("**/integration_tests/**/*.yml")).union(
        set((installed_packages_path).glob("**/integration_tests/**/*.yaml"))
    )
    yml_files_packages_not_integration_tests = yml_files_packages - yml_files_packages_integration_tests

    return [Path(str(path)) for path in yml_files_packages_not_integration_tests if path.name == "dbt_project.yml"]


def find_package_paths(
    root_dir: Path,
) -> list[Path]:
    """Find paths to installed packages' dbt_project.yml files.

    Find each installed package's root directory and return the file paths
    for each package's dbt_project.yml
    Note that this only returns paths for direct dependencies of the project.

    Args:
        root_dir (Path): the root directory of the project

    Returns:
        list[Path]: the file path(s) for all dbt_project.yml files for packages
    """
    packages_path = yaml.safe_load((root_dir / "dbt_project.yml").read_text()).get(
        "packages-install-path", "dbt_packages"
    )

    # check package path from project or default package path first
    installed_packages = find_packages_within_directory((root_dir / packages_path))

    # if we don't find any, fall back to default package directory
    if len(installed_packages) == 0:
        installed_packages = find_packages_within_directory((root_dir / "dbt_packages"))

    # if still don't have any, check for env var
    if len(installed_packages) == 0:
        package_dir_envvar = os.getenv("DBT_PACKAGES_INSTALL_PATH")
        if package_dir_envvar is not None:
            installed_packages = find_packages_within_directory((package_dir_envvar))

    return installed_packages


def load_yaml_from_package_dbt_project_yml_path(package_project_yml_path: Path) -> dict[Any, Any]:
    """Extracts YAML content from a package's dbt_project.yml file.

    Parses a dbt_project.yml file for an installed package into an untyped dict

    Args:
        package_project_yml_path (Path): the path for the package's dbt_project.yml file

    Returns:
        dict[Any, Any]: the result produced by the YAML parser
    """

    if package_project_yml_path.name != "dbt_project.yml":
        console.log("File must be dbt_project.yml")
        return {}
    try:
        parsed_package_file = read_file(package_project_yml_path)
    except:
        console.log(f"Error when parsing package file {package_project_yml_path}")
        return {}
    if parsed_package_file == {}:
        console.log("No content parsed")
        return {}
    else:
        return parsed_package_file


def parse_package_info_from_package_dbt_project_yml(parsed_package_file: dict[Any, Any]) -> Optional[DbtPackageVersion]:
    """Extracts package info from a dict parsed from a package's dbt_project.yml.

    Constructs a DbtPackageVersion by extracting required attributes from the dict
    containing the output from a package's parsed dbt_project.yml.

    Args:
        parsed_package_file (dict[Any, Any]): parsed dbt_project.yml

    Returns:
        DbtPackageVersion: object representing a single version of a package
    """
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
        require_dbt_version_raw: Any = parsed_package_file["require-dbt-version"]
    else:
        require_dbt_version_raw = None

    installed_package_version = DbtPackageVersion(
        package_name=package_name, package_version_str=version, raw_require_dbt_version_range=require_dbt_version_raw
    )

    return installed_package_version


def get_current_installed_package_versions(root_dir: Path) -> dict[str, DbtPackageVersion]:
    """Extract version metadata for all installed packages in a project.

    Finds all installed packages from a project's root directory and
    constructs DbtPackageVersions for each project.

    Args:
        root_dir (Path): the root directory of the project

    Returns:
        dict[str, DbtPackageVersion]: maps the name from dbt_project.yml to a specific package version
    """
    installed_package_paths: list[Path] = find_package_paths(root_dir)
    installed_package_versions: dict[str, DbtPackageVersion] = {}
    if len(installed_package_paths) == 0:
        console.log("No packages installed. Please run dbt deps first")
        return installed_package_versions
    for package_path in installed_package_paths:
        loaded_yaml: dict[Any, Any] = load_yaml_from_package_dbt_project_yml_path(package_path)
        package_info: Optional[DbtPackageVersion] = parse_package_info_from_package_dbt_project_yml(loaded_yaml)
        if not package_info:
            console.log("Parsing failed on package")
            continue
        package_name = package_info.package_name
        if package_name in installed_package_versions:
            console.log(f"Package name {package_name} already installed")
        installed_package_versions[package_name] = package_info
    return installed_package_versions

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Union

import yaml
from dbt_fusion_package_tools.dbt_package_version import DbtPackageVersion
from rich.console import Console

console = Console()


def load_yaml_from_package_lock_file_path(package_lock_yml_path: Path) -> dict[Any, Any]:
    """Extracts YAML content from a project's package-lock file.

    Parses a package-lock.yml file for an installed package into an untyped dict

    Args:
        package_lock_yml_path (Path): the path for the package's dbt_project.yml file

    Returns:
        dict[Any, Any]: the result produced by the YAML parser; {} if no results
    """

    if package_lock_yml_path.name != "package-lock.yml":
        console.log("File must be package-lock.yml")
        return {}
    try:
        parsed_package_lock_file = yaml.safe_load(package_lock_yml_path.read_text())
    except FileNotFoundError:
        console.log(f"File not found at {package_lock_yml_path}")
        return {}
    except Exception as e:
        console.log(f"Error when parsing package file {package_lock_yml_path}: {type(e)}, {e!s}")
        return {}
    if isinstance(parsed_package_lock_file, dict) and parsed_package_lock_file != {}:
        return parsed_package_lock_file
    else:
        console.log("No content parsed")
        return {}


@dataclass
class DbtPackageLockFile:
    """Represents a project's package-lock.yml file.

    Attributes:
        file_path (Path, optional): Path to the package-lock.yml file
        yml_dependencies (dict[Any, Any], optional): package-lock.yml parsed to dict
        installed_package_versions (dict[str, DbtPackageVersion]): maps package names to package version
        unknown_packages (set[str]): names of packages not available in Hub
    """

    # must provide either file path or parsed dependencies
    file_path: Optional[Path] = None
    yml_dependencies: Optional[dict[Any, Any]] = None
    # this is indexed by package id for uniqueness (hopefully)
    installed_package_versions: dict[str, DbtPackageVersion] = field(init=False, default_factory=dict)
    unknown_packages: set[str] = field(init=False, default_factory=set)

    def __post_init__(self):
        """Extracts project dependencies from a given path or dict."""
        self.installed_package_versions = {}
        self.unknown_packages = set()
        if self.yml_dependencies is not None:
            self.parse_packages_from_yml(self.yml_dependencies)
        elif self.file_path is not None:
            self.load_yaml_from_package_lock_yml_path(self.file_path)

    def load_yaml_from_package_lock_yml_path(self, package_lock_yml_path: Path) -> bool:
        """Loads package-lock.yml from a given path.

        This path is then set as file_path and the parsed yaml is set as yml_dependencies.

        Args:
            package_lock_yml_path (Path): path to package-lock.yml

        Returns:
            bool: true if file load was successful
        """
        parsed_yaml = load_yaml_from_package_lock_file_path(package_lock_yml_path)
        if len(parsed_yaml) == 0:
            return False
        parsed_packages = self.parse_packages_from_yml(parsed_yaml)
        if parsed_packages is True:
            self.file_path = package_lock_yml_path
            self.yml_dependencies = parsed_yaml
            return True
        else:
            return False

    @staticmethod
    def parse_package_version_from_block(block: dict[Any, Any]) -> Union[DbtPackageVersion, str]:
        """Extract package version from YAML block.

        Args:
            block (dict[Any, Any]): package version block from package-lock.yml

        Returns:
            Union[DbtPackageVersion, str, None]: DbtPackageVersion for public package, str for non-public package, None if not parsed
        """
        package_name = block.get("name")
        package_id = block.get("package")
        package_version = block.get("version")
        local = block.get("local")
        git = block.get("git")
        tarball = block.get("tarball")
        # "package" key indicates that it's a hub package
        if isinstance(package_id, str) and isinstance(package_version, str):
            # for old lock files that don't have a name, use the second half of the id
            short_package_name = package_name if isinstance(package_name, str) else package_id.split("/")[-1]
            return DbtPackageVersion(
                package_name=(short_package_name), package_id=package_id, package_version_str=package_version
            )
        # otherwise, it's a private package of some kind
        elif isinstance(package_name, str):
            return str(package_name)
        elif isinstance(local, str):
            return "local"
        elif isinstance(git, str):
            return "git"
        elif isinstance(tarball, str):
            return "tarball"
        else:
            return "unknown"

    def parse_packages_from_yml(self, yml_dependencies: dict[Any, Any]) -> bool:
        """Extracts package dependencies from YAML parsed to a dict.

        This method will set the yml_dependencies field equal to the input,
        then parse the dependencies into package versions. Successfully parsed versions will be
        set in installed_package_versions. For packages that are not public,
        the name only will be stored in unknown_packages.

        Args:
            yml_dependencies (dict[Any, Any]): parsed YAML from package-lock

        Returns:
            bool: true if package versions extracted
        """
        if "packages" in yml_dependencies:
            self.yml_dependencies = yml_dependencies
        else:
            console.log("YAML input does not contain packages")
            return False
        self.installed_package_versions = {}
        self.unknown_packages = set()
        for block in yml_dependencies["packages"]:
            parsed_block = self.parse_package_version_from_block(block)
            if isinstance(parsed_block, DbtPackageVersion):
                package_key = parsed_block.package_id or parsed_block.package_name
                self.installed_package_versions[package_key] = parsed_block
            else:
                self.unknown_packages.add(parsed_block)
        # true if some packages were loaded successfully, else false
        return len(self.installed_package_versions) > 0 or len(self.unknown_packages) > 0

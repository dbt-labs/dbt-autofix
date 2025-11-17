from pathlib import Path
import urllib.request
from typing import Optional, Set
from yaml import safe_load
import json


def should_skip_package(package_path: Path, include_private_packages: bool) -> bool:
    """Determine if a package should be skipped based on hub status and flags.

    Args:
        package_path: Path to the package directory
        include_private_packages: Whether to include private packages

    Returns:
        True if if the package is a public package and include_private_packages is True
    """
    if include_private_packages:
        return _is_hub_package(package_path)
    else:
        return not _is_hub_package(package_path)


def _is_hub_package(package_path: Path) -> bool:
    """Check if a package is a hub package by comparing its name.

    Args:
        package_path: Path to the package directory

    Returns:
        True if the package is a hub package, False otherwise
    """
    dbt_project_yml = package_path / "dbt_project.yml"

    if not dbt_project_yml.exists():
        return False

    try:
        with open(dbt_project_yml, "r") as f:
            package_config = safe_load(f)

        package_name = package_config.get("name")

        # If we don't have hub packages, assume it's not a hub package
        if _HUB_PACKAGES is None:
            return False
        elif package_name and package_name in _HUB_PACKAGES:
            return True
    except Exception:
        # If we can't read the package config, assume it's not a hub package
        pass

    return False


def _fetch_hub_packages() -> Optional[Set[str]]:
    hub_url = "https://hub.getdbt.com/api/v1/index.json"

    try:
        with urllib.request.urlopen(hub_url) as response:
            data = json.loads(response.read().decode())

        if isinstance(data, list):
            return set(map(lambda package: package.split("/")[-1], data))
        else:
            return None
    except Exception:
        return None


_HUB_PACKAGES: Optional[Set[str]] = _fetch_hub_packages()

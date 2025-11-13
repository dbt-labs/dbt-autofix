"""Utilities for handling dbt hub packages."""

import json
import urllib.request
from pathlib import Path
from typing import Optional, Set
from yaml import safe_load


def fetch_hub_packages() -> Optional[Set[str]]:
    """Fetch the list of hub package names from the dbt hub API.
        
    Returns:
        Set of hub package names
    """
    hub_url = "https://hub.getdbt.com/api/v1/index.json"
    
    try:
        with urllib.request.urlopen(hub_url) as response:
            data = json.loads(response.read().decode())
        
        # The API returns a list of package paths, we need to extract the package names
        if isinstance(data, list):
            return set(map(lambda package: package.split("/")[-1], data))
    
    except Exception:
        return None


def is_hub_package(package_path: Path, hub_packages: Set[str]) -> bool:
    """Check if a package is a hub package by comparing its name.
    
    Args:
        package_path: Path to the package directory
        hub_packages: Set of known hub package names
        
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
        if package_name and package_name in hub_packages:
            return True
    except Exception:
        # If we can't read the package config, assume it's not a hub package
        pass
    
    return False


def should_skip_package(
    package_path: Path, 
    hub_packages: Optional[Set[str]], 
    include_private_packages: bool
) -> bool:
    """Determine if a package should be skipped based on hub status and flags.
    
    Args:
        package_path: Path to the package directory
        hub_packages: Set of known hub package names
        include_private_packages: Whether to include private packages
        
    Returns:
        True if the package should be skipped, False otherwise
    """
    # If we don't have hub packages, we can't skip any packages
    if hub_packages is None:
        return False

    if is_hub_package(package_path, hub_packages):
        # This is a hub package - always skip it
        return True
    else:
        # This is a private package - skip it unless include_private_packages is True
        return not include_private_packages

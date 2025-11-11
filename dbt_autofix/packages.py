# import difflib
# import re
# from typing import List, Tuple, Dict, Any
# import yamllint.linter
# from copy import deepcopy

# from dbt_autofix.refactors.results import YMLRuleRefactorResult
# from dbt_autofix.refactors.results import DbtDeprecationRefactor
# from dbt_autofix.retrieve_schemas import SchemaSpecs
# from dbt_autofix.deprecations import DeprecationType
# from dbt_autofix.refactors.yml import DbtYAML, dict_to_yaml_str, yaml_config
# from dbt_autofix.refactors.constants import COMMON_PROPERTY_MISSPELLINGS, COMMON_CONFIG_MISSPELLINGS

# NUM_SPACES_TO_REPLACE_TAB = 2

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from rich.console import Console


console = Console()


def generate_package_dependencies():
    # check `dependencies.yml`
    # check `packages.yml`
    pass


def check_for_package_upgrades(package_dependencies):
    # check all packages for upgrades
    # if dry run, write out package upgrades and exit
    pass


def upgrade_package_versions(package_dependencies_with_upgrades):
    # if package dependencies have upgrades:
    # update dependencies.yml
    # update packages.yml
    # write out dependencies.yml (unless dry run)
    # write out packages.yml (unless dry run)
    pass


def delete_existing_packages():
    # delete package-lock.yml
    # delete `dbt_packages/` directory
    pass


def run_dbt_deps():
    # run dbt deps
    pass


class PackageVersionUpgradeType(str, Enum):
    """String enum for package upgrade types"""

    NO_UPGRADE_REQUIRED = "Package is already compatible with Fusion"
    UPGRADE_AVAILABLE = "Package has Fusion-compatible version available"
    PUBLIC_PACKAGE_MISSING_FUSION_ELIGIBILITY = "Public package has not defined fusion eligibility"
    PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION = "Public package is not compatible with fusion"
    PRIVATE_PACKAGE_MISSING_REQUIRE_DBT_VERSION = "Private package requires a compatible require-dbt-version (>=2.0.0, <3.0.0) to be available on fusion. https://docs.getdbt.com/reference/project-configs/require-dbt-version"


@dataclass
class PackageVersionUpgradeResult:
    id: str
    public_package: bool
    previous_version: str
    compatible_version: str
    upgraded_version: str
    version_reason: PackageVersionUpgradeType

    @property
    def package_upgrade_logs(self):
        return [self.version_reason]

    def to_dict(self) -> dict:
        ret_dict = {"version_reason": [self.version_reason]}
        return ret_dict


@dataclass
class PackageUpgradeResult:
    dry_run: bool
    file_path: Path
    upgraded: bool
    upgrades: list[PackageVersionUpgradeResult]
    unchanged: list[PackageVersionUpgradeResult]

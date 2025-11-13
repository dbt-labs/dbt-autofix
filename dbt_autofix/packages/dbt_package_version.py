from typing import Optional, Union
from dbt_fusion_package_manager.installed_packages import DbtInstalledPackage
from semver.version import Version
from dataclasses import dataclass, field
from rich.console import Console
from dbt_common.semver import VersionSpecifier, VersionRange


console = Console()

@dataclass
class DbtPackageVersion:
    package_name: str
    package_version_str: str
    require_dbt_version_range: list[str]
    version: VersionSpecifier = field(init=False)
    require_dbt_version: VersionRange = field(init=False)

    def __post_init__(self):
        pass

    def convert_version_range_from_list(self, version_range: list[str]) -> VersionRange:
        # version_specifiers = []
        # return VersionRange
        pass

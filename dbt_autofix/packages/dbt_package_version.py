from typing import Optional, Union
from dbt_fusion_package_manager.installed_packages import DbtInstalledPackage
from semver.version import Version
from dataclasses import dataclass, field
from rich.console import Console
from dbt_common.semver import VersionSpecifier, VersionRange, versions_compatible


console = Console()

FUSION_COMPATIBLE_VERSION: VersionSpecifier = VersionSpecifier.from_version_string("2.0.0")

@dataclass
class DbtPackageVersion:
    package_name: str
    package_version_str: str
    require_dbt_version_range: list[str] = field(default_factory=list)
    version: VersionSpecifier = field(init=False)
    require_dbt_version: Optional[VersionRange] = field(init=False)

    def __post_init__(self):
        pass

    def convert_version_range_from_list(self, version_range: list[str]) -> VersionRange:
        # version_specifiers = []
        # return VersionRange
        pass

    def is_version_fusion_compatible(self):
        if self.require_dbt_version:
            return versions_compatible(self.require_dbt_version, FUSION_COMPATIBLE_VERSION)
        else:
            return False

    def is_require_dbt_version_defined(self):
        return len(self.require_dbt_version_range) > 0

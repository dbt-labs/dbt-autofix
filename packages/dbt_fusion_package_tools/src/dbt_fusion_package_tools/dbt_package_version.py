from dataclasses import dataclass, field
from typing import Any, Optional

from rich.console import Console

from dbt_fusion_package_tools.manual_overrides import (
    EXPLICIT_ALLOW_ALL_VERSIONS,
    EXPLICIT_DISALLOW_ALL_VERSIONS,
    EXPLICIT_DISALLOW_VERSIONS,
)
from dbt_fusion_package_tools.upgrade_status import PackageVersionFusionCompatibilityState
from dbt_fusion_package_tools.version_utils import (
    FUSION_COMPATIBLE_VERSION,
    VersionRange,
    VersionSpecifier,
    construct_version_list_from_raw,
    convert_version_specifiers_to_range,
    get_version_specifiers,
    versions_compatible,
)

console = Console()


@dataclass
class DbtPackageVersion:
    package_name: str
    package_version_str: str
    require_dbt_version_range: list[str] = field(default_factory=list)
    # version: VersionSpecifier = field(init=False)
    require_dbt_version: Optional[VersionRange] = field(init=False)
    package_id_with_version: Optional[str] = None
    package_id: Optional[str] = None
    raw_require_dbt_version_range: Any = None

    @property
    def version(self) -> VersionSpecifier:
        return VersionSpecifier.from_version_string(self.package_version_str)

    @version.setter
    def version(self, new_version: VersionSpecifier) -> None:
        self.package_version_str = new_version.to_version_string(skip_matcher=True)

    def __post_init__(self):
        try:
            if self.raw_require_dbt_version_range is not None:
                self.require_dbt_version_range = construct_version_list_from_raw(self.raw_require_dbt_version_range)
            if self.require_dbt_version_range and len(self.require_dbt_version_range) > 0:
                version_specs: list[VersionSpecifier] = get_version_specifiers(self.require_dbt_version_range)
                self.require_dbt_version = convert_version_specifiers_to_range(version_specs)
            else:
                self.require_dbt_version = None
        except:
            self.require_dbt_version = None

    def __lt__(self, other) -> bool:
        if self.package_name != other.package_name:
            return False
        return self.version < other.version

    def __eq__(self, other) -> bool:
        return self.package_name != other.package_name or self.version != other.version

    def is_prerelease_version(self) -> bool:
        return self.version.prerelease is not None

    def is_require_dbt_version_fusion_compatible(self) -> bool:
        if self.require_dbt_version:
            return versions_compatible(self.require_dbt_version, FUSION_COMPATIBLE_VERSION)
        else:
            return False

    def is_require_dbt_version_defined(self) -> bool:
        return self.require_dbt_version_range != None and len(self.require_dbt_version_range) > 0

    def is_version_explicitly_disallowed_on_fusion(self) -> bool:
        return (
            self.package_id is not None
            and self.package_id in EXPLICIT_DISALLOW_VERSIONS
            and self.package_version_str in EXPLICIT_DISALLOW_VERSIONS[self.package_id]
        )

    def is_explicitly_disallowed_on_fusion(self) -> bool:
        if self.package_id is not None:
            if self.package_id in EXPLICIT_DISALLOW_ALL_VERSIONS:
                return True
            elif self.is_version_explicitly_disallowed_on_fusion():
                return True
        return False

    def is_explicitly_allowed_on_fusion(self) -> bool:
        if self.package_id is not None and self.package_id in EXPLICIT_ALLOW_ALL_VERSIONS:
            return True
        return False

    def get_fusion_compatibility_state(self) -> PackageVersionFusionCompatibilityState:
        if self.is_explicitly_allowed_on_fusion():
            return PackageVersionFusionCompatibilityState.EXPLICIT_ALLOW
        elif self.is_explicitly_disallowed_on_fusion():
            return PackageVersionFusionCompatibilityState.EXPLICIT_DISALLOW
        elif not self.is_require_dbt_version_defined():
            return PackageVersionFusionCompatibilityState.NO_DBT_VERSION_RANGE
        elif self.is_require_dbt_version_fusion_compatible():
            return PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
        elif not self.is_require_dbt_version_fusion_compatible():
            return PackageVersionFusionCompatibilityState.DBT_VERSION_RANGE_EXCLUDES_2_0
        else:
            return PackageVersionFusionCompatibilityState.UNKNOWN

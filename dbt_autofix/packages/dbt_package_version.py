from enum import Enum
from typing import Optional, Union
from dataclasses import dataclass, field
from rich.console import Console
from dbt_common.semver import VersionSpecifier, VersionRange, versions_compatible, reduce_versions


console = Console()

FUSION_COMPATIBLE_VERSION: VersionSpecifier = VersionSpecifier.from_version_string("2.0.0")

# `float` also allows `int`, according to PEP484 (and jsonschema!)
RawVersion = Union[str, float]

def get_versions(version: Union[RawVersion, list[RawVersion]]) -> list[str]:
    if isinstance(version, list):
        return [str(v) for v in version]
    else:
        return [str(version)]


def get_version_specifiers(raw_version: list[str]) -> list[VersionSpecifier]:
    return [VersionSpecifier.from_version_string(v) for v in raw_version]


def convert_version_specifiers_to_range(specs: list[VersionSpecifier]) -> Optional[VersionRange]:
    if len(specs) == 0 or len(specs) > 2:
        return
    elif len(specs) == 1:
        return VersionRange(specs[0], specs[0])
    elif specs[0] < specs[1]:
        return VersionRange(specs[0], specs[1])
    else:
        return VersionRange(specs[1], specs[0])

class FusionCompatibilityState(str, Enum):
    """String enum for deprecation types used in DbtDeprecationRefactor."""
    
    NO_DBT_VERSION_RANGE = "Package does not define required dbt version range"
    DBT_VERSION_RANGE_EXCLUDES_2_0 = "Package's DBT version range excludes version 2.0"
    DBT_VERSION_RANGE_INCLUDES_2_0 = "Package's DBT versions range include version 2.0"
    EXPLICIT_ALLOW = "Package version has been verified as Fusion-compatible"
    EXPLICIT_DISALLOW = "Package version has been verified as incompatible with Fusion"
    UNKNOWN = "Package version state unknown"


@dataclass
class DbtPackageVersion:
    package_name: str
    package_version_str: str
    require_dbt_version_range: list[str] = field(default_factory=list)
    version: VersionSpecifier = field(init=False)
    require_dbt_version: Optional[VersionRange] = field(init=False)

    def __post_init__(self):
        try:
            self.version = VersionSpecifier.from_version_string(self.package_version_str)
            if self.require_dbt_version_range:
                raw_versions: list[RawVersion] = [x for x in self.require_dbt_version_range]
                version_specs: list[VersionSpecifier] = get_version_specifiers(get_versions(raw_versions))
                self.require_dbt_version = convert_version_specifiers_to_range(version_specs)
            else:
                self.require_dbt_version = None
        except:
            pass

    def is_version_fusion_compatible(self) -> bool:
        if self.require_dbt_version:
            return versions_compatible(self.require_dbt_version, FUSION_COMPATIBLE_VERSION)
        else:
            return False

    def is_require_dbt_version_defined(self) -> bool:
        return len(self.require_dbt_version_range) > 0
    
    def is_explicitly_disallowed_on_fusion(self) -> bool:
        return False
    
    def is_explicitly_allowed_on_fusion(self) -> bool:
        return False
    
    def get_fusion_compatibility_state(self) -> FusionCompatibilityState:
        if self.is_explicitly_allowed_on_fusion():
            return FusionCompatibilityState.EXPLICIT_ALLOW
        elif self.is_explicitly_disallowed_on_fusion():
            return FusionCompatibilityState.EXPLICIT_DISALLOW
        elif not self.is_require_dbt_version_defined():
            return FusionCompatibilityState.NO_DBT_VERSION_RANGE
        elif self.is_version_fusion_compatible():
            return FusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
        elif not self.is_version_fusion_compatible():
            return FusionCompatibilityState.DBT_VERSION_RANGE_EXCLUDES_2_0
        else:
            return FusionCompatibilityState.UNKNOWN


from typing import Optional, Union
from dataclasses import dataclass, field
from rich.console import Console
from dbt_common.semver import VersionSpecifier, VersionRange, versions_compatible


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
        except:
            pass

    def is_version_fusion_compatible(self):
        if self.require_dbt_version:
            return versions_compatible(self.require_dbt_version, FUSION_COMPATIBLE_VERSION)
        else:
            return False

    def is_require_dbt_version_defined(self):
        return len(self.require_dbt_version_range) > 0

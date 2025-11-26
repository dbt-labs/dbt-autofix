from typing import Any, Optional, Union
from dataclasses import dataclass, field
from rich.console import Console
from dbt_common.semver import VersionSpecifier, VersionRange, versions_compatible
from dbt_autofix.packages.manual_overrides import EXPLICIT_ALLOW_ALL_VERSIONS, EXPLICIT_DISALLOW_ALL_VERSIONS

from dbt_autofix.packages.upgrade_status import PackageVersionFusionCompatibilityState

console = Console()

FUSION_COMPATIBLE_VERSION: VersionSpecifier = VersionSpecifier.from_version_string("2.0.0")

# `float` also allows `int`, according to PEP484 (and jsonschema!)
RawVersion = Union[str, float]


def get_versions(version: Union[RawVersion, list[RawVersion]]) -> list[str]:
    if isinstance(version, list):
        return [str(v) for v in version]
    else:
        return [str(version)]


def construct_version_list(raw_versions: Union[str, list[str], None]) -> list[str]:
    if raw_versions is None:
        return []
    elif type(raw_versions) == str:
        return [raw_versions]
    elif type(raw_versions) == list:
        return raw_versions
    else:
        return []


def construct_version_list_from_raw(raw_versions: Any) -> list[str]:
    if raw_versions is None:
        return []
    # isinstance is needed here because when ruyaml parses the YAML,
    # sometimes it stores it as an instance of a class that extend str or list
    elif isinstance(raw_versions, str):
        return [str(raw_versions)]
    elif isinstance(raw_versions, list):
        versions = []
        for version in raw_versions:
            if isinstance(version, str) or isinstance(version, float):
                versions.append(str(version))
        return versions
    else:
        return []


def get_version_specifiers(raw_version: list[str]) -> list[VersionSpecifier]:
    return [VersionSpecifier.from_version_string(v) for v in raw_version]


def convert_version_specifiers_to_range(specs: list[VersionSpecifier]) -> VersionRange:
    if len(specs) == 0 or len(specs) > 2:
        # assume any version compatible
        any_version = VersionSpecifier.from_version_string(">=0.0.0")
        return VersionRange(any_version, any_version)
    elif len(specs) == 1:
        return VersionRange(specs[0], specs[0])
    elif specs[0] < specs[1]:
        return VersionRange(specs[0], specs[1])
    else:
        return VersionRange(specs[1], specs[0])


def convert_optional_version_string_to_spec(version_string: Optional[str]) -> Optional[VersionSpecifier]:
    try:
        if type(version_string) == str:
            return VersionSpecifier.from_version_string(version_string)
        else:
            return None
    except:
        return None


def convert_version_string_list_to_spec(version_string: list[str]) -> list[VersionSpecifier]:
    if len(version_string) == 0:
        return []
    else:
        return [VersionSpecifier.from_version_string(x) for x in version_string]


@dataclass
class DbtPackageVersion:
    package_name: str
    package_version_str: str
    require_dbt_version_range: list[str] = field(default_factory=list)
    version: VersionSpecifier = field(init=False)
    require_dbt_version: Optional[VersionRange] = field(init=False)
    package_id_with_version: Optional[str] = None
    package_id: Optional[str] = None
    raw_require_dbt_version_range: Any = None

    def __post_init__(self):
        try:
            self.version = VersionSpecifier.from_version_string(self.package_version_str)
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

    def is_explicitly_disallowed_on_fusion(self) -> bool:
        if self.package_id is not None and self.package_id in EXPLICIT_DISALLOW_ALL_VERSIONS:
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

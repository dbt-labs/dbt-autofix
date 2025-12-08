from typing import Any, Optional, Union
from dbt_common.semver import VersionRange, VersionSpecifier

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

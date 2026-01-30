from typing import Any, Optional

import pytest
from dbt_fusion_package_tools.dbt_package_version import DbtPackageVersion
from dbt_fusion_package_tools.version_utils import (
    Matchers,
    UnboundedVersionSpecifier,
    VersionRange,
    VersionSpecifier,
    versions_compatible,
)


@pytest.mark.parametrize(
    "input_str,expected_match",
    [
        ("0.8.7", VersionSpecifier(major="0", minor="8", patch="7", matcher=Matchers.EXACT)),
        ("1.7.10-beta", VersionSpecifier(major="1", minor="7", patch="10", prerelease="beta", matcher=Matchers.EXACT)),
    ],
)
def test_convert_version_str(input_str: str, expected_match: VersionSpecifier):
    package_version = DbtPackageVersion(package_name="test_package", package_version_str=input_str)
    extracted_version = package_version.version
    assert extracted_version == expected_match
    assert extracted_version.major == expected_match.major
    assert extracted_version.minor == expected_match.minor
    assert extracted_version.patch == expected_match.patch
    assert extracted_version.prerelease == expected_match.prerelease
    assert extracted_version.matcher == expected_match.matcher
    assert extracted_version.is_exact == True
    assert versions_compatible(extracted_version, input_str) == True


# class Matchers(StrEnum):
#     GREATER_THAN = ">"
#     GREATER_THAN_OR_EQUAL = ">="
#     LESS_THAN = "<"
#     LESS_THAN_OR_EQUAL = "<="
#     EXACT = "="
version_1_1_0_eq: VersionSpecifier = VersionSpecifier(major="1", minor="1", patch="0", matcher=Matchers.EXACT)
version_1_1_0_gt: VersionSpecifier = VersionSpecifier(major="1", minor="1", patch="0", matcher=Matchers.GREATER_THAN)
version_1_1_0_ge: VersionSpecifier = VersionSpecifier(
    major="1", minor="1", patch="0", matcher=Matchers.GREATER_THAN_OR_EQUAL
)
version_1_1_0_lt: VersionSpecifier = VersionSpecifier(major="1", minor="1", patch="0", matcher=Matchers.LESS_THAN)
version_1_1_0_le: VersionSpecifier = VersionSpecifier(
    major="1", minor="1", patch="0", matcher=Matchers.LESS_THAN_OR_EQUAL
)
version_2_0_0_lt: VersionSpecifier = VersionSpecifier(major="2", minor="0", patch="0", matcher=Matchers.LESS_THAN)
version_2_0_0_le: VersionSpecifier = VersionSpecifier(
    major="2", minor="0", patch="0", matcher=Matchers.LESS_THAN_OR_EQUAL
)
version_3_0_0_lt: VersionSpecifier = VersionSpecifier(major="3", minor="0", patch="0", matcher=Matchers.LESS_THAN)
version_3_0_0_le: VersionSpecifier = VersionSpecifier(
    major="3", minor="0", patch="0", matcher=Matchers.LESS_THAN_OR_EQUAL
)
unbounded_version: VersionSpecifier = UnboundedVersionSpecifier()


@pytest.mark.parametrize(
    "input_yaml,expected_match",
    [
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": [">=1.1.0", "<2.0.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            VersionRange(version_1_1_0_ge, version_2_0_0_lt),
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            None,
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": [">=1.1.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            VersionRange(version_1_1_0_ge, unbounded_version),
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": [">1.1.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            VersionRange(version_1_1_0_gt, unbounded_version),
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": ["<1.1.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            VersionRange(unbounded_version, version_1_1_0_lt),
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": ["<=1.1.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            VersionRange(unbounded_version, version_1_1_0_le),
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": ["1.1.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            VersionRange(version_1_1_0_eq, version_1_1_0_eq),
        ),
    ],
)
def test_convert_require_dbt_version_range_raw(input_yaml: dict[Any, Any], expected_match: Optional[VersionRange]):
    package_name = str(input_yaml.get("name", ""))
    version = str(input_yaml.get("version", ""))
    require_dbt_version_raw: Any = input_yaml.get("require-dbt-version")

    package_version = DbtPackageVersion(
        package_name=package_name, package_version_str=version, raw_require_dbt_version_range=require_dbt_version_raw
    )

    extracted_version: Optional[VersionRange] = package_version.require_dbt_version
    assert extracted_version == expected_match


@pytest.mark.parametrize(
    "input_yaml,expected_match",
    [
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": [">=1.1.0", "<2.0.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            False,
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            False,
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": [">=1.1.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            True,
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": [">1.1.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            True,
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": ["<1.1.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            False,
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": ["<=1.1.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            False,
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": ["1.1.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            False,
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": [">=1.1.0", "<=2.0.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            True,
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": [">=1.1.0", "<3.0.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            True,
        ),
        (
            {
                "name": "dbt_set_similarity",
                "version": "0.1.0",
                "require-dbt-version": ["<3.0.0"],
                "config-version": 2,
                "model-paths": ["models"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_modules"],
                "macro-paths": ["macros"],
                "log-path": "logs",
            },
            True,
        ),
    ],
)
def test_fusion_compatible_from_raw(input_yaml: dict[Any, Any], expected_match: Optional[VersionRange]):
    package_name = str(input_yaml.get("name", ""))
    version = str(input_yaml.get("version", ""))
    require_dbt_version_raw: Any = input_yaml.get("require-dbt-version")

    package_version = DbtPackageVersion(
        package_name=package_name, package_version_str=version, raw_require_dbt_version_range=require_dbt_version_raw
    )

    fusion_compatible: bool = package_version.is_require_dbt_version_fusion_compatible()
    assert fusion_compatible == expected_match


def test_package_version_eq_expected_match():
    version1 = DbtPackageVersion("test/package", "1.0.0")
    version2 = DbtPackageVersion("test/package", "1.0.0")
    assert version1 == version2


def test_package_version_eq_expected_no_version_match():
    version1 = DbtPackageVersion("test/package", "1.0.0")
    version2 = DbtPackageVersion("test/package", "1.0.1")
    assert version1 != version2


def test_package_version_eq_expected_no_name_match():
    version1 = DbtPackageVersion("test/package", "1.0.0")
    version2 = DbtPackageVersion("test/package2", "1.0.0")
    assert version1 != version2

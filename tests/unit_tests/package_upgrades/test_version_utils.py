# from dbt_autofix.packages.version_utils import construct_version_list_from_raw, construct_version_list, get_version_specifiers
from typing import Any

import pytest
from dbt_fusion_package_tools.version_utils import (
    VersionRange,
    VersionSpecifier,
    construct_version_list_from_raw,
    get_version_specifiers,
)


@pytest.mark.parametrize(
    "input_yaml,expected_match",
    [
        ([">=1.1.0", "<2.0.0"], [">=1.1.0", "<2.0.0"]),
        (None, []),
        (
            [">=1.1.0"],
            [">=1.1.0"],
        ),
        (
            ">=1.1.0",
            [">=1.1.0"],
        ),
        (">0.2.1", [">0.2.1"]),
    ],
)
def test_convert_version_string_from_raw(input_yaml: Any, expected_match: list[str]):
    version_list = construct_version_list_from_raw(input_yaml)

    assert version_list == expected_match
    if input_yaml is not None:
        for i, version in enumerate(version_list):
            assert version == expected_match[i]
        get_version_specifiers(version_list)


def test_convert_string_to_range():
    upper_bound_only = VersionSpecifier.from_version_string("<1.0.0")
    lower_bound_only = VersionSpecifier.from_version_string(">1.0.0")
    upper_bound_only_range = VersionRange(upper_bound_only, upper_bound_only)
    lower_bound_only_range = VersionRange(lower_bound_only, lower_bound_only)
    print(upper_bound_only_range)
    print(lower_bound_only_range)
    print(upper_bound_only_range.to_version_string_pair())
    print(upper_bound_only.to_range())
    print(lower_bound_only.to_range())
    # print(VersionRange(UnboundedVersionSpecifier(), ))

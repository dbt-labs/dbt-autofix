from dbt_autofix.packages.version_utils import construct_version_list_from_raw


import pytest


from typing import Any


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

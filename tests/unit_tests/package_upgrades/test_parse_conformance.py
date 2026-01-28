from pathlib import Path
from dbt_fusion_package_tools.check_parse_conformance import (
    check_fusion_schema_compatibility,
    construct_download_url_from_latest,
)


def test_fusion_schema_compat():
    output = check_fusion_schema_compatibility(
        Path("tests/integration_tests/package_upgrades/dbt_utils_package_lookup_map_2")
    )
    print(output)
    print()
    print(
        check_fusion_schema_compatibility(
            Path("tests/integration_tests/package_upgrades/dbt_utils_package_lookup_map_2"), show_fusion_output=False
        )
    )


def test_construct_download_url_from_latest():
    old_version_url: str = "https://codeload.github.com/fishtown-analytics/dbt-utils/tar.gz/0.1.5"
    package_latest_url: str = "https://codeload.github.com/dbt-labs/dbt-utils/tar.gz/1.3.3"
    assert (
        construct_download_url_from_latest(package_latest_url, old_version_url)
        == "https://codeload.github.com/dbt-labs/dbt-utils/tar.gz/0.1.5"
    )

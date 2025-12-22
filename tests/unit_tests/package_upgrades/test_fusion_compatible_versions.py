import pytest
from dbt_fusion_package_tools.fusion_version_compatibility_output import FUSION_VERSION_COMPATIBILITY_OUTPUT


@pytest.mark.parametrize(
    "old_package_id,new_package_id",
    [
        ("calogica/dbt_date", "godatadriven/dbt_date"),
        ("calogica/dbt_expectations", "metaplane/dbt_expectations"),
        ("masthead-data/bq_reservations", "masthead-data/bq_reservations"),
    ],
)
def test_check_renames(old_package_id, new_package_id):
    package = FUSION_VERSION_COMPATIBILITY_OUTPUT[old_package_id]
    old_package_namespace, old_package_name = old_package_id.split("/")
    assert old_package_namespace is not None
    assert old_package_name is not None
    if package["package_redirect_name"] is None:
        package_redirect_name = old_package_name
    else:
        package_redirect_name = package["package_redirect_name"]
    if package["package_redirect_namespace"] is None:
        package_redirect_namespace = old_package_namespace
    else:
        package_redirect_namespace = package["package_redirect_namespace"]
    package_redirect_id = f"{package_redirect_namespace}/{package_redirect_name}"
    assert package_redirect_id == new_package_id


@pytest.mark.parametrize(
    "package_id,version,expected",
    [
        ("dbt-labs/dbt_project_evaluator", "=0.9.0", False),
        ("dbt-labs/dbt_project_evaluator", "=1.0.0", False),
        ("dbt-labs/dbt_project_evaluator", "=1.1.0", False),
        ("dbt-labs/dbt_project_evaluator", "=1.1.1", False),
        ("dbt-labs/dbt_project_evaluator", "=1.1.2", True),
        ("brooklyn-data/dbt_artifacts", "=2.10.0", True),
        ("brooklyn-data/dbt_artifacts", "=2.9.3", False),
        ("brooklyn-data/dbt_artifacts", "=0.6.0", False),
    ],
)
def test_check_explicit_override_version(package_id, version, expected):
    package = FUSION_VERSION_COMPATIBILITY_OUTPUT[package_id]
    fusion_compatibility = version in package["fusion_compatible_versions"]
    assert fusion_compatibility == expected
    if not expected:
        assert package["oldest_fusion_compatible_version"] != version
        assert package["latest_fusion_compatible_version"] != version

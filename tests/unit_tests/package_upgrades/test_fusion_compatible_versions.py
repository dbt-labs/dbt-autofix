import pytest
from dbt_autofix.packages.fusion_version_compatibility_output import FUSION_VERSION_COMPATIBILITY_OUTPUT


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

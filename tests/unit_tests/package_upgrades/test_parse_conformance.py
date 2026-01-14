from pathlib import Path
from dbt_fusion_package_tools.check_parse_conformance import (
    check_fusion_schema_compatibility,
    checkout_repo_and_run_conformance,
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


def test_checkout_repo_and_run_conformance():
    checkout_repo_and_run_conformance("dbt-labs", "dbt-project-evaluator", "dbt_project_evaluator", limit=1)

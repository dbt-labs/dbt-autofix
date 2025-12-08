from pathlib import Path
from dbt_fusion_package_tools.check_parse_conformance import check_fusion_schema_compatibility

def test_fusion_schema_compat():
    output = check_fusion_schema_compatibility(Path("tests/integration_tests/dbt_projects/project1_expected"))
    print(output)
    print()
    print(check_fusion_schema_compatibility(Path("tests/integration_tests/dbt_projects/project1_expected"), show_fusion_output=False))
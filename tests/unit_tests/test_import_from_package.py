from dbt_autofix.main import print_package
from dbt_fusion_package_tools.package_example import output_package_name

def test_print_package():
    print_package()
    output_package_name()
from pathlib import Path

from dbt_fusion_package_tools.yaml.loader import safe_load

PROJECT_WITH_PACKAGES_PATH = Path("tests/integration_tests/package_upgrades/mixed_versions")


def test_loader():
    input = (PROJECT_WITH_PACKAGES_PATH / "packages.yml").read_text()
    output = safe_load(input)
    assert output

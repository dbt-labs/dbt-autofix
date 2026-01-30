import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_project_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        project_dir = Path(tmpdirname)

        # Create dbt_project.yml
        project_dir.joinpath("dbt_project.yml").write_text("""
packages-install-path: dbt_packages
""")

        # Create project YAML files
        models_dir = project_dir / "models"
        models_dir.mkdir(parents=True, exist_ok=True)
        models_dir.joinpath("schema.yml").write_text("""
version: 2

models:
  - name: model1
    description: "First model"
    name: other_name
  - name: model2
    description: "Second model"
""")

        # Create package YAML file
        project_dir.joinpath("packages.yml").write_text("""
packages:
  - package: dbt-labs/dbt_external_tables
    version: [">=0.8.0", "<0.9.0"]
  
  - package: dbt-labs/dbt_utils
    version: [">=0.9.0", "<1.0.0"]

  - package: dbt-labs/codegen
    version: [">=0.8.0", "<0.9.0"]

  - package: dbt-labs/audit_helper
    version: [">=0.6.0", "<0.7.0"]

  - package: metaplane/dbt_expectations
    version: [">=0.10.8", "<1.0.0"]

  - git: "https://github.com/PrivateGitRepoPackage/gmi_common_dbt_utils.git"
    revision: main # use a branch or a tag name
""")

        # Create package lock file
        project_dir.joinpath("package-lock.yml").write_text("""
packages:
  - name: dbt_external_tables
    package: dbt-labs/dbt_external_tables
    version: 0.8.7
  - name: dbt_utils
    package: dbt-labs/dbt_utils
    version: 0.9.6
  - name: codegen
    package: dbt-labs/codegen
    version: 0.8.1
  - name: audit_helper
    package: dbt-labs/audit_helper
    version: 0.6.0
  - name: dbt_expectations
    package: metaplane/dbt_expectations
    version: 0.10.9
  - git: https://github.com/PrivateGitRepoPackage/gmi_common_dbt_utils.git
    name: gmi_common_dbt_utils
    revision: 067b588343e9c19dc8593b6b3cb06cc5b47822e1
  - name: dbt_date
    package: godatadriven/dbt_date
    version: 0.16.1
sha1_hash: f10149243aadecf4a289805e5892180d9fc50142

""")

        # Create package YAML files without duplicates
        package_dir = project_dir / "dbt_packages" / "test_package"
        package_dir.mkdir(parents=True, exist_ok=True)
        package_models_dir = package_dir / "models"
        package_models_dir.mkdir(parents=True, exist_ok=True)
        package_models_dir.joinpath("schema.yml").write_text("""
version: 2

models:
  - name: package_model1
    description: "First package model"
  - name: package_model2
    name: package_model3
    description: "Duplicate package model"
""")

        # Create integration test files (should be ignored)
        integration_dir = package_dir / "integration_tests"
        integration_dir.mkdir(parents=True, exist_ok=True)
        integration_dir.joinpath("schema.yml").write_text("""
version: 2

models:
  - name: integration_model1
    description: "First integration model"
    description: "First integration model duplicate"
  - name: integration_model2
    description: "Duplicate integration model"
    
""")

        yield project_dir


# @pytest.mark.parametrize(
#     "version_range,expected_result",
#     [
#         (
#             [">1.0.0"],
#             True,
#         ),
#         (
#             [">1.0.0", "<2.0.0"],
#             False,
#         ),
#         (
#             [">=2.0.0"],
#             True,
#         ),
#     ],
# )
# def test_fusion_compatible_dbt_version_ranges(version_range: list[str], expected_result: bool):
#     package = DbtPackage(package_name="test_package")
#     assert package.is_dbt_version_fusion_compatible(version_range) == expected_result

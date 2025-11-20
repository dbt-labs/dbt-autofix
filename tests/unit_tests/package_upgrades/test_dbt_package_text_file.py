from pprint import pprint
import tempfile
from pathlib import Path
from dbt_autofix.packages.dbt_package_file import (
    DbtPackageFile,
    load_yaml_from_packages_yml,
    parse_package_dependencies_from_packages_yml,
    parse_package_dependencies_from_yml,
    find_package_yml_files,
)

import pytest

from dbt_autofix.packages.dbt_package_text_file import DbtPackageTextFileLine


@pytest.fixture
def temp_project_dir_with_packages_yml():
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
  - version: 0.10.9
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


@pytest.mark.parametrize(
    "input_str,expected_match",
    [
        ("    version: 0.8.7", ["    version: ", "0.8.7", ""]),
        ("  - version: 0.10.9", ["  - version: ", "0.10.9", ""]),
        ("    version: 0.8.7\n", ["    version: ", "0.8.7", "\n"]),
        ("  - version: 0.10.9\n", ["  - version: ", "0.10.9", "\n"]),
    ],
)
def test_extract_version_from_line(input_str, expected_match):
    file_line = DbtPackageTextFileLine(input_str)
    extracted_version = file_line.extract_version_from_line()
    assert len(extracted_version) == len(expected_match)
    assert extracted_version[0] == expected_match[0]
    assert extracted_version[1] == expected_match[1]


@pytest.mark.parametrize(
    "input_str,expected_match",
    [
        ("  - package: dbt-labs/dbt_utils", True),
        ('    version: [">=0.9.0", "<1.0.0"]', False),
        ("", False),
        ("                  ", False),
        ('  - version: [">=0.9.0", "<1.0.0"]', True),
        ('# test comment - version: [">=0.9.0", "<1.0.0"]', False),
        ('# test comment - package: [">=0.9.0", "<1.0.0"]', False),
        ("# test comment version: test", False),
        ("# test comment package: test", False),
    ],
)
def test_match_key_in_line(input_str, expected_match):
    package_line = DbtPackageTextFileLine(line=input_str)
    assert package_line.line_contains_key() == expected_match


@pytest.mark.parametrize(
    "input_str,expected_match",
    [
        ("  - package: dbt-labs/dbt_utils", True),
        ('    version: [">=0.9.0", "<1.0.0"]', False),
        ("    package: dbt-labs/dbt_utils", True),
        ("", False),
        ("                  ", False),
        ('  - version: [">=0.9.0", "<1.0.0"]', False),
        ('# test comment - version: [">=0.9.0", "<1.0.0"]', False),
        ('# test comment - package: [">=0.9.0", "<1.0.0"]', False),
        ("# test comment version: test", False),
        ("# test comment package: test", False),
    ],
)
def test_match_package_in_line(input_str, expected_match):
    package_line = DbtPackageTextFileLine(line=input_str)
    assert package_line.line_contains_package() == expected_match


@pytest.mark.parametrize(
    "input_str,expected_match",
    [
        ("  - package: dbt-labs/dbt_utils", False),
        ('    version: [">=0.9.0", "<1.0.0"]', True),
        ("    package: dbt-labs/dbt_utils", False),
        ("", False),
        ("                  ", False),
        ('  - version: [">=0.9.0", "<1.0.0"]', True),
        ('# test comment - version: [">=0.9.0", "<1.0.0"]', False),
        ('# test comment - package: [">=0.9.0", "<1.0.0"]', False),
        ("# test comment version: test", False),
        ("# test comment package: test", False),
    ],
)
def test_match_version_in_line(input_str, expected_match):
    package_line = DbtPackageTextFileLine(line=input_str)
    assert package_line.line_contains_version() == expected_match


@pytest.mark.parametrize(
    "input_str,expected_match",
    [
        ("  - package: dbt-labs/dbt_utils", "dbt-labs/dbt_utils"),
        ('    version: [">=0.9.0", "<1.0.0"]', ""),
        ("    package: dbt-labs/dbt_utils", "dbt-labs/dbt_utils"),
        ("", ""),
        ("                  ", ""),
        ('  - version: [">=0.9.0", "<1.0.0"]', ""),
        ('# test comment - version: [">=0.9.0", "<1.0.0"]', ""),
        ('# test comment - package: [">=0.9.0", "<1.0.0"]', ""),
        ("# test comment version: test", ""),
        ("# test comment package: test", ""),
        ("    package: dbt-labs/dbt_utils # trailing comment", "dbt-labs/dbt_utils"),
        ("    package: dbt-labs/dbt_utils\n", "dbt-labs/dbt_utils"),
        ("    package: dbt-labs/dbt_utils  \n", "dbt-labs/dbt_utils"),
    ],
)
def test_extract_package_in_line(input_str, expected_match):
    package_line = DbtPackageTextFileLine(line=input_str)
    assert package_line.extract_package_from_line() == expected_match


@pytest.mark.parametrize(
    "input_str,expected_match",
    [
        ("    version: 0.8.7", "    version: 0.0.0"),
        ("  - version: 0.10.9", "  - version: 0.0.0"),
        ("    version: 0.8.7\n", "    version: 0.0.0\n"),
        ("  - version: 0.10.9\n", "  - version: 0.0.0\n"),
    ],
)
def test_replace_version_in_line(input_str, expected_match):
    file_line = DbtPackageTextFileLine(input_str)
    replaced = file_line.replace_version_string_in_line("0.0.0")
    assert replaced
    assert len(file_line.line) == len(expected_match)
    assert file_line.line == expected_match

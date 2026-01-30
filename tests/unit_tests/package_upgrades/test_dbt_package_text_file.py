import tempfile
from pathlib import Path

import pytest

from dbt_autofix.packages.dbt_package_text_file import DbtPackageTextFile, DbtPackageTextFileLine


@pytest.fixture
def temp_project_dir_with_packages_yml():
    with tempfile.TemporaryDirectory() as tmpdirname:
        project_dir = Path(tmpdirname)

        # Create package YAML file
        project_dir.joinpath("packages.yml").write_text("""
packages:
  - package: dbt-labs/dbt_external_tables
    version: [">=0.8.0", "<0.9.0"]
  
  - package: dbt-labs/dbt_utils
    version: [">=0.9.0", "<1.0.0"]

  - package: dbt-labs/codegen
    version: [">=0.8.0", "<0.9.0"]

  - package: "dbt-labs/audit_helper"
    version: [">=0.6.0", "<0.7.0"]
  
  - version: 0.9.6
    package: Datavault-UK/dbtvault  # example comment

  - package: metaplane/dbt_expectations
    version: [">=0.10.8", "<1.0.0"]                                       
                                                        
  - package: "elementary-data/elementary"
    version: "0.7.0"

  - git: "https://github.com/PrivateGitRepoPackage/gmi_common_dbt_utils.git"
    revision: main # use a branch or a tag name
""")

        yield project_dir


@pytest.mark.parametrize(
    "input_str,expected_match",
    [
        ("    version: 0.8.7", ["    version: ", "0.8.7", ""]),
        ("  - version: 0.10.9", ["  - version: ", "0.10.9", ""]),
        ("    version: 0.8.7\n", ["    version: ", "0.8.7", "\n"]),
        ("  - version: 0.10.9\n", ["  - version: ", "0.10.9", "\n"]),
        ('    version: [">=0.8.0", "<0.9.0"]', ["    version: ", '[">=0.8.0", "<0.9.0"]', ""]),
    ],
)
def test_extract_version_from_line(input_str, expected_match):
    file_line = DbtPackageTextFileLine(input_str)
    extracted_version = file_line.extract_version_from_line()
    assert len(extracted_version) == 3
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
        ('    package: "dbt-labs/dbt_utils"  \n', "dbt-labs/dbt_utils"),
        ("    package: 'dbt-labs/dbt_utils'  \n", "dbt-labs/dbt_utils"),
    ],
)
def test_extract_package_in_line(input_str, expected_match):
    package_line = DbtPackageTextFileLine(line=input_str)
    assert package_line.extract_package_name_from_line() == expected_match


@pytest.mark.parametrize(
    "input_str,expected_match",
    [
        ("    version: 0.8.7", "    version: 0.0.0"),
        ("  - version: 0.10.9", "  - version: 0.0.0"),
        ("    version: 0.8.7\n", "    version: 0.0.0\n"),
        ("  - version: 0.10.9\n", "  - version: 0.0.0\n"),
        ("  - version: 0.10.9 # example comment\n", "  - version: 0.0.0 # example comment\n"),
        ('  - version: "0.10.9" # example comment\n', "  - version: 0.0.0 # example comment\n"),
        ('    version: [">=0.8.0", "<0.9.0"]', "    version: 0.0.0"),
    ],
)
def test_replace_version_in_line(input_str, expected_match):
    file_line = DbtPackageTextFileLine(input_str)
    replaced = file_line.replace_version_string_in_line("0.0.0")
    assert replaced
    assert len(file_line.line) == len(expected_match)
    assert file_line.line == expected_match


@pytest.mark.parametrize(
    "input_str,expected_match",
    [
        ("    package: calogica/dbt_date", "    package: godatadriven/dbt_date"),
        ("  - package: calogica/dbt_date", "  - package: godatadriven/dbt_date"),
        ("    package: calogica/dbt_date\n", "    package: godatadriven/dbt_date\n"),
        ("  - package: calogica/dbt_date\n", "  - package: godatadriven/dbt_date\n"),
        (
            "  - package: calogica/dbt_date # example comment\n",
            "  - package: godatadriven/dbt_date # example comment\n",
        ),
    ],
)
def test_replace_package_name_in_line(input_str, expected_match):
    file_line = DbtPackageTextFileLine(input_str)
    replaced = file_line.replace_package_name_in_line("godatadriven/dbt_date")
    assert replaced
    assert len(file_line.line) == len(expected_match)
    assert file_line.line == expected_match


def test_rename_package(temp_project_dir_with_packages_yml):
    file = DbtPackageTextFile(temp_project_dir_with_packages_yml / "packages.yml")
    file.update_config_file(
        {"calogica/dbt_date": "0.17.0", "Datavault-UK/dbtvault": "0.9.7", "dbt-labs/dbt_utils": "1.3.1"},
        dry_run=True,
        print_to_console=True,
    )

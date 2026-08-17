import tempfile
from pathlib import Path

import pytest

from dbt_autofix.duplicate_keys import DuplicateFound, find_duplicate_keys


@pytest.fixture
def temp_project_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        project_dir = Path(tmpdirname)

        # Create dbt_project.yml
        project_dir.joinpath("dbt_project.yml").write_text("""
packages-install-path: dbt_packages
""")

        # Create project YAML files with duplicates
        models_dir = project_dir / "models"
        models_dir.mkdir(parents=True, exist_ok=True)
        models_dir.joinpath("schema.yml").write_text("""
version: 2

models:
  - name: model1
    description: "First model"
    name: other_name
  - name: model1
    description: "Duplicate model"
""")

        # Create package YAML files with duplicates
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
  - name: integration_model1
    description: "Duplicate integration model"
    
""")

        yield project_dir


def test_find_duplicate_keys(temp_project_dir: Path):
    project_duplicates, package_duplicates = find_duplicate_keys(temp_project_dir)

    # Debug prints
    print("\nProject duplicates found:", len(project_duplicates))
    for dup in project_duplicates:
        print(f"  {dup}")
    print("\nPackage duplicates found:", len(package_duplicates))
    for dup in package_duplicates:
        print(f"  {dup}")

    # Check project duplicates
    assert len(project_duplicates) == 1
    project_dup = project_duplicates[0]
    assert project_dup.file.name == "schema.yml"
    assert "name" in project_dup.value
    assert project_dup.line > 0

    # Check package duplicates
    assert len(package_duplicates) == 1
    package_dup = package_duplicates[0]
    assert package_dup.file.name == "schema.yml"
    assert "name" in package_dup.value
    assert package_dup.line > 0

    # Verify integration test duplicates are ignored
    integration_duplicates = [d for d in package_duplicates if "integration_tests" in str(d.file)]
    assert len(integration_duplicates) == 0


def test_find_duplicate_keys_empty_project():
    with tempfile.TemporaryDirectory() as tmpdirname:
        project_dir = Path(tmpdirname)
        project_dir.joinpath("dbt_project.yml").write_text("""
packages-install-path: dbt_packages
""")

        project_duplicates, package_duplicates = find_duplicate_keys(project_dir)
        assert len(project_duplicates) == 0
        assert len(package_duplicates) == 0


def test_find_duplicate_keys_yaml_extension(temp_project_dir: Path):
    # Create a .yaml file with duplicates
    models_dir = temp_project_dir / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    models_dir.joinpath("schema.yaml").write_text("""
version: 2

models:
  - name: yaml_model1
    description: "First yaml model"
  - name: yaml_model2
    description: "Other yaml model"
    description: "Other yaml description"
""")

    project_duplicates, _ = find_duplicate_keys(temp_project_dir)

    # Debug prints
    print("\nYAML extension test duplicates found:", len(project_duplicates))
    for dup in project_duplicates:
        print(f"  {dup}")

    # Check that .yaml files are also checked
    yaml_duplicates = [d for d in project_duplicates if d.file.suffix == ".yaml"]
    assert len(yaml_duplicates) == 1
    assert "description" in yaml_duplicates[0].value


def test_find_duplicate_keys_with_exclude(temp_project_dir: Path):
    # Create a target_foo directory with duplicate YAML
    target_foo_dir = temp_project_dir / "target_foo"
    target_foo_dir.mkdir(parents=True, exist_ok=True)
    target_foo_dir.joinpath("run_results.yml").write_text("""
version: 2

models:
  - name: log_model
    description: "First"
    description: "Duplicate"
""")

    project_duplicates, _ = find_duplicate_keys(temp_project_dir, excluded_paths=["target_foo"])

    # logs/ duplicates should be excluded, models/ duplicates should remain
    log_duplicates = [d for d in project_duplicates if "logs" in str(d.file)]
    assert len(log_duplicates) == 0
    assert len(project_duplicates) == 1
    assert project_duplicates[0].file.name == "schema.yml"


def test_find_duplicate_keys_with_exclude_multiple(temp_project_dir: Path):
    for dirname in ["logs", ".venv"]:
        d = temp_project_dir / dirname
        d.mkdir(parents=True, exist_ok=True)
        d.joinpath("schema.yml").write_text("""
version: 2

models:
  - name: m1
    description: "First"
    description: "Duplicate"
""")

    project_duplicates, _ = find_duplicate_keys(temp_project_dir, excluded_paths=["logs", ".venv"])

    excluded = [d for d in project_duplicates if "logs" in str(d.file) or ".venv" in str(d.file)]
    assert len(excluded) == 0
    assert len(project_duplicates) == 1


def test_find_duplicate_keys_with_exclude_no_match(temp_project_dir: Path):
    project_duplicates, package_duplicates = find_duplicate_keys(temp_project_dir, excluded_paths=["nonexistent_dir"])

    assert len(project_duplicates) == 1
    assert len(package_duplicates) == 1


def test_duplicate_found_str_representation():
    dup = DuplicateFound(file=Path("test.yml"), line=10, key="test_key", value="duplicate key: test_key")
    assert str(dup) == "test.yml:10 -- duplicate key: test_key"

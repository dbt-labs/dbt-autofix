from pathlib import Path

from dbt_fusion_package_tools.dbt_package_version import DbtPackageVersion

from dbt_autofix.packages.dbt_package_lock_file import DbtPackageLockFile, load_yaml_from_package_lock_file_path


def test_load_yaml():
    file_path = Path(
        "tests/integration_tests/package_upgrades/dbt_utils_package_lookup_map_2/package-lock.yml"
    ).resolve()
    output = load_yaml_from_package_lock_file_path(file_path)
    assert output == {
        "packages": [
            {"package": "dbt-labs/codegen", "name": "codegen", "version": "0.7.0"},
            {"package": "dbt-labs/dbt_utils", "name": "dbt_utils", "version": "0.8.5"},
        ],
        "sha1_hash": "9a01e56afedfedc66557a05b7f641750a70234e6",
    }


# test full load through constructor


def test_package_lock_file_construct_from_path_with_valid_file():
    file_path = Path(
        "tests/integration_tests/package_upgrades/dbt_utils_package_lookup_map_2/package-lock.yml"
    ).resolve()
    lock_file = DbtPackageLockFile(file_path=file_path)
    assert lock_file.yml_dependencies is not None
    assert lock_file.yml_dependencies != {}
    assert lock_file.file_path is not None
    assert lock_file.file_path == file_path
    assert isinstance(lock_file.installed_package_versions, dict)
    assert len(lock_file.installed_package_versions) == 2
    assert len(lock_file.unknown_packages) == 0


def test_package_lock_file_construct_from_dict_with_valid_yml():
    parsed_yml = {
        "packages": [
            {"package": "dbt-labs/codegen", "name": "codegen", "version": "0.7.0"},
            {"package": "dbt-labs/dbt_utils", "name": "dbt_utils", "version": "0.8.5"},
        ],
        "sha1_hash": "9a01e56afedfedc66557a05b7f641750a70234e6",
    }
    lock_file = DbtPackageLockFile(yml_dependencies=parsed_yml)
    assert lock_file.yml_dependencies is not None
    assert lock_file.yml_dependencies != {}
    assert "packages" in lock_file.yml_dependencies
    assert len(lock_file.yml_dependencies["packages"]) == 2
    assert lock_file.file_path is None
    assert isinstance(lock_file.installed_package_versions, dict)
    assert len(lock_file.installed_package_versions) == 2
    assert len(lock_file.unknown_packages) == 0


# test load at function level


def test_package_lock_file_load_yml_path():
    file_path = Path(
        "tests/integration_tests/package_upgrades/dbt_utils_package_lookup_map_2/package-lock.yml"
    ).resolve()
    lock_file = DbtPackageLockFile()
    loaded_dependencies = lock_file.load_yaml_from_package_lock_yml_path(file_path)
    assert loaded_dependencies
    assert lock_file.yml_dependencies is not None
    assert lock_file.yml_dependencies != {}
    assert "packages" in lock_file.yml_dependencies
    assert len(lock_file.yml_dependencies["packages"]) == 2


def test_package_lock_file_parse_from_yml():
    parsed_yml = {
        "packages": [
            {"package": "dbt-labs/codegen", "name": "codegen", "version": "0.7.0"},
            {"package": "dbt-labs/dbt_utils", "name": "dbt_utils", "version": "0.8.5"},
        ],
        "sha1_hash": "9a01e56afedfedc66557a05b7f641750a70234e6",
    }
    lock_file = DbtPackageLockFile()
    loaded_dependencies = lock_file.parse_packages_from_yml(parsed_yml)
    assert loaded_dependencies
    assert lock_file.yml_dependencies is not None
    assert lock_file.yml_dependencies != {}
    assert "packages" in lock_file.yml_dependencies
    assert len(lock_file.yml_dependencies["packages"]) == 2


# extract package version from parsed YML


# hub package in format used in Fusion and core >1.10
def test_package_version_from_hub_package_fusion():
    block = {"package": "dbt-labs/codegen", "name": "codegen", "version": "0.7.0"}
    parsed_block = DbtPackageLockFile.parse_package_version_from_block(block)
    assert parsed_block is not None
    assert isinstance(parsed_block, DbtPackageVersion)
    assert parsed_block.package_name == "codegen"
    assert parsed_block.package_id == "dbt-labs/codegen"
    assert parsed_block.package_version_str == "0.7.0"


# hub package in format used in older core versions
def test_package_version_from_hub_package_core_1_9():
    block = {"package": "dbt-labs/codegen", "version": "0.7.0"}
    parsed_block = DbtPackageLockFile.parse_package_version_from_block(block)
    assert parsed_block is not None
    assert isinstance(parsed_block, DbtPackageVersion)
    assert parsed_block.package_name == "codegen"
    assert parsed_block.package_id == "dbt-labs/codegen"
    assert parsed_block.package_version_str == "0.7.0"


def test_package_version_from_local_package():
    block = {"local": "../"}
    parsed_block = DbtPackageLockFile.parse_package_version_from_block(block)
    assert parsed_block is not None
    assert isinstance(parsed_block, str)
    assert parsed_block == "local"


def test_package_version_from_git_package_with_name():
    block = {
        "git": "https://github.com/PrivateGitRepoPackage/gmi_common_dbt_utils.git",
        "name": "gmi_common_dbt_utils",
        "revision": "067b588343e9c19dc8593b6b3cb06cc5b47822e1",
    }
    parsed_block = DbtPackageLockFile.parse_package_version_from_block(block)
    assert parsed_block is not None
    assert isinstance(parsed_block, str)
    assert parsed_block == "gmi_common_dbt_utils"


def test_package_version_from_git_package_without_name():
    block = {
        "git": "https://github.com/PrivateGitRepoPackage/gmi_common_dbt_utils.git",
        "revision": "067b588343e9c19dc8593b6b3cb06cc5b47822e1",
    }
    parsed_block = DbtPackageLockFile.parse_package_version_from_block(block)
    assert parsed_block is not None
    assert isinstance(parsed_block, str)
    assert parsed_block == "git"


def test_package_version_from_name():
    block = {"name": "name_only"}
    parsed_block = DbtPackageLockFile.parse_package_version_from_block(block)
    assert parsed_block is not None
    assert isinstance(parsed_block, str)
    assert parsed_block == "name_only"


def test_package_version_from_unknown_package():
    block = {"unknown_key": "nothing"}
    parsed_block = DbtPackageLockFile.parse_package_version_from_block(block)
    assert parsed_block is not None
    assert isinstance(parsed_block, str)
    assert parsed_block == "unknown"

import json
import tempfile
import warnings
from pathlib import Path
from tempfile import TemporaryDirectory

from dbt_fusion_package_tools.compatibility import (
    FusionConformanceResult,
    FusionLogMessage,
    ParseConformanceLogOutput,
)
from dbt_fusion_package_tools.scripts.package_hub_fusion_compatibility import (
    clean_version,
    extract_package_id_from_path,
    is_package_index_file,
    is_package_version_file,
    process_json,
    read_json_from_local_hub_repo,
    write_conformance_output_to_json,
    write_dict_to_json,
)


def test_write_conformance_output_to_json():
    test_data: dict[str, dict[str, FusionConformanceResult]] = {
        "package1": {
            "1.1.1": FusionConformanceResult(
                version="1.1.1",
                require_dbt_version_defined=True,
                require_dbt_version_compatible=False,
                parse_compatible=False,
                parse_compatibility_result=ParseConformanceLogOutput(
                    parse_exit_code=10,
                    total_errors=2,
                    total_warnings=0,
                    errors=[
                        FusionLogMessage("error 1", 1060, "ERROR"),
                        FusionLogMessage("error 2", 8999, "ERROR"),
                    ],
                ),
                manually_verified_compatible=False,
                manually_verified_incompatible=False,
            )
        }
    }
    with TemporaryDirectory() as tmpdir:
        write_conformance_output_to_json(test_data, Path(tmpdir))


class TestIsPackageIndexFile:
    """Tests for is_package_index_file function."""

    def test_valid_index_file(self):
        """Test valid package index file path."""
        assert is_package_index_file("data/packages/org/pkg/index.json") is True

    def test_invalid_index_file_wrong_filename(self):
        """Test index file path with wrong filename."""
        assert is_package_index_file("data/packages/org/pkg/version.json") is False

    def test_invalid_index_file_wrong_depth(self):
        """Test index file path with wrong directory depth."""
        assert is_package_index_file("data/packages/org/pkg/sub/index.json") is False

    def test_invalid_index_file_too_shallow(self):
        """Test index file path that's too shallow."""
        assert is_package_index_file("data/packages/index.json") is False


class TestIsPackageVersionFile:
    """Tests for is_package_version_file function."""

    def test_valid_version_file(self):
        """Test valid package version file path."""
        assert is_package_version_file("data/packages/org/pkg/versions/v1.0.0.json") is True

    def test_invalid_version_file_wrong_depth(self):
        """Test version file path with wrong directory depth."""
        assert is_package_version_file("data/packages/org/pkg/v1.0.0.json") is False

    def test_invalid_version_file_no_versions_dir(self):
        """Test version file path without versions directory."""
        assert is_package_version_file("data/packages/org/pkg/files/v1.0.0.json") is False

    def test_invalid_version_file_too_deep(self):
        """Test version file path that's too deep."""
        assert is_package_version_file("data/packages/org/pkg/versions/sub/v1.0.0.json") is False


class TestExtractPackageIdFromPath:
    """Tests for extract_package_id_from_path function."""

    def test_extract_from_index_file(self):
        """Test extracting package ID from index file path."""
        result = extract_package_id_from_path("data/packages/my-org/my-package/index.json")
        assert result == "my-org/my-package"

    def test_extract_from_version_file(self):
        """Test extracting package ID from version file path."""
        result = extract_package_id_from_path("data/packages/my-org/my-package/versions/v1.0.0.json")
        assert result == "my-org/my-package"

    def test_invalid_path_missing_data(self):
        """Test path missing 'data' directory."""
        result = extract_package_id_from_path("packages/org/pkg/index.json")
        assert result == ""

    def test_invalid_path_missing_packages(self):
        """Test path missing 'packages' directory."""
        result = extract_package_id_from_path("data/modules/org/pkg/index.json")
        assert result == ""

    def test_invalid_path_too_shallow(self):
        """Test path that's too shallow."""
        result = extract_package_id_from_path("data/packages/org")
        assert result == ""


class TestCleanVersion:
    """Tests for clean_version function."""

    def test_removes_leading_v(self):
        """Test that leading 'v' is removed."""
        assert clean_version("v1.0.0") == "1.0.0"

    def test_removes_leading_uppercase_v(self):
        """Test that leading 'V' is removed."""
        assert clean_version("V1.0.0") == "1.0.0"

    def test_preserves_version_without_v(self):
        """Test that versions without 'v' are unchanged."""
        assert clean_version("1.0.0") == "1.0.0"

    def test_handles_none(self):
        """Test that None returns empty string."""
        assert clean_version(None) == ""

    def test_preserves_v_in_middle(self):
        """Test that 'v' in the middle is preserved."""
        assert clean_version("1.0.0-version1") == "1.0.0-version1"


class TestProcessJson:
    """Tests for process_json function."""

    def test_process_index_file(self):
        """Test processing index JSON file."""
        file_path = "data/packages/org/pkg/index.json"
        parsed_json = {
            "latest": "v1.0.0",
            "name": "my-package",
            "namespace": "org",
            "redirectname": None,
            "redirectnamespace": None,
        }

        result = process_json(file_path, parsed_json)

        assert result["package_id_from_path"] == "org/pkg"
        assert result["package_latest_version_index_json"] == "1.0.0"
        assert result["package_name_index_json"] == "my-package"
        assert result["package_namespace_index_json"] == "org"

    def test_process_version_file_with_source(self):
        """Test processing version JSON file with source."""
        file_path = "data/packages/org/pkg/versions/v1.0.0.json"
        parsed_json = {
            "id": "org/pkg",
            "name": "my-package",
            "version": "v1.0.0",
            "require_dbt_version": ">=1.0.0",
            "_source": {"url": "https://github.com/org/pkg"},
            "downloads": {"tarball": "https://codeload.github.com/org/pkg/tar.gz/v1.0.0"},
        }

        result = process_json(file_path, parsed_json)

        assert result["package_id_from_path"] == "org/pkg"
        assert result["package_version_string"] == "1.0.0"
        assert result["package_version_github_url"] == "https://github.com/org/pkg"
        assert result["package_version_download_url"] == "https://codeload.github.com/org/pkg/tar.gz/v1.0.0"

    def test_process_version_file_without_source(self):
        """Test processing version JSON file without source."""
        file_path = "data/packages/org/pkg/versions/v1.0.0.json"
        parsed_json = {
            "id": "org/pkg",
            "name": "my-package",
            "version": "v1.0.0",
            "downloads": {"tarball": "https://example.com/tar.gz"},
        }

        result = process_json(file_path, parsed_json)

        assert result["package_version_github_url"] is None

    def test_process_version_file_without_downloads(self):
        """Test processing version JSON file without downloads."""
        file_path = "data/packages/org/pkg/versions/v1.0.0.json"
        parsed_json = {
            "id": "org/pkg",
            "name": "my-package",
            "version": "v1.0.0",
            "_source": {"url": "https://github.com/org/pkg"},
        }

        result = process_json(file_path, parsed_json)

        assert result["package_version_download_url"] is None

    def test_process_invalid_path(self):
        """Test processing file with invalid path."""
        result = process_json("invalid/path.json", {})
        assert result == {}


class TestWriteDictToJson:
    """Tests for write_dict_to_json function."""

    def test_write_dict_to_json(self):
        """Test writing dictionary to JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            data = {"name": "test", "version": "1.0.0"}

            write_dict_to_json(data, dest_dir)

            # Check file was created
            output_file = dest_dir / "package_output.json"
            assert output_file.exists()

            # Check contents
            with output_file.open("r") as f:
                written_data = json.load(f)
            assert written_data == data

    def test_write_dict_with_custom_indent(self):
        """Test writing dictionary with custom indentation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            data = {"key": "value"}

            write_dict_to_json(data, dest_dir, indent=4)

            output_file = dest_dir / "package_output.json"
            with output_file.open("r") as f:
                content = f.read()
            # 4-space indent should create longer output
            assert "    " in content

    def test_write_dict_sorted_keys(self):
        """Test that keys are sorted in output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            data = {"z": 1, "a": 2, "m": 3}

            write_dict_to_json(data, dest_dir, sort_keys=True)

            output_file = dest_dir / "package_output.json"
            with output_file.open("r") as f:
                content = f.read()
            # Check order in JSON string
            a_pos = content.find('"a"')
            m_pos = content.find('"m"')
            z_pos = content.find('"z"')
            assert a_pos < m_pos < z_pos


class TestReadJsonFromLocalHubRepo:
    """Tests for read_json_from_local_hub_repo function."""

    def test_read_single_json_file(self):
        """Test reading a single JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            packages_dir = base_dir / "data" / "packages" / "org" / "pkg"
            packages_dir.mkdir(parents=True)

            # Create a valid index file
            index_file = packages_dir / "index.json"
            index_file.write_text(json.dumps({"latest": "v1.0.0", "name": "pkg", "namespace": "org"}))

            result = read_json_from_local_hub_repo(str(index_file))

            assert "org/pkg" in result
            assert len(result["org/pkg"]) > 0

    def test_read_directory_recursively(self):
        """Test reading JSON files from directory recursively."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "data" / "packages"
            base_dir.mkdir(parents=True)

            # Create multiple package files
            for pkg_name in ["pkg1", "pkg2"]:
                pkg_dir = base_dir / "org" / pkg_name
                pkg_dir.mkdir(parents=True)
                index_file = pkg_dir / "index.json"
                index_file.write_text(json.dumps({"latest": "v1.0.0", "name": pkg_name, "namespace": "org"}))

            result = read_json_from_local_hub_repo(str(base_dir.parent))

            assert "org/pkg1" in result
            assert "org/pkg2" in result

    def test_read_with_file_count_limit(self):
        """Test reading with file count limit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "data" / "packages"
            base_dir.mkdir(parents=True)

            # Create multiple files
            for i in range(5):
                pkg_dir = base_dir / "org" / f"pkg{i}"
                pkg_dir.mkdir(parents=True)
                index_file = pkg_dir / "index.json"
                index_file.write_text(json.dumps({"latest": f"v{i}.0.0"}))

            result = read_json_from_local_hub_repo(str(base_dir.parent), file_count_limit=2)

            # Should have read at most 2 files
            total_files = sum(len(v) for v in result.values())
            assert total_files <= 2

    def test_read_nonexistent_path(self):
        """Test reading from nonexistent path."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = read_json_from_local_hub_repo("/nonexistent/path")

            assert len(w) == 1
            assert "does not exist" in str(w[0].message)
            assert len(result) == 0

    def test_read_invalid_json_file(self):
        """Test reading invalid JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "data" / "packages"
            base_dir.mkdir(parents=True)

            # Create invalid JSON
            invalid_file = base_dir / "invalid.json"
            invalid_file.write_text("{invalid json}")

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = read_json_from_local_hub_repo(str(invalid_file))

                assert len(w) > 0

    def test_read_empty_directory(self):
        """Test reading from empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = read_json_from_local_hub_repo(tmpdir)
            assert len(result) == 0

"""Unit tests for update_package_hub_json module."""

import json
import tempfile
from collections import defaultdict
from pathlib import Path

from dbt_fusion_package_tools.compatibility import (
    FusionConformanceResult,
)
from dbt_fusion_package_tools.scripts.update_package_hub_json import (
    check_for_rename,
    extract_output_from_json,
    find_package_hub_file,
    get_json_from_package_hub_file,
    reload_output_from_file,
    update_hub_json,
    write_dict_to_json,
)


class TestReloadOutputFromFile:
    """Tests for reload_output_from_file function."""

    def test_reload_valid_json_file(self):
        """Test reloading valid JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "output.json"
            test_data = {"package1": {"1.0.0": {"version": "1.0.0", "download_failed": False}}}
            with file_path.open("w") as f:
                json.dump(test_data, f)

            result = reload_output_from_file(file_path)

            assert "package1" in result
            assert "1.0.0" in result["package1"]

    def test_reload_empty_file(self):
        """Test reloading empty JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "output.json"
            with file_path.open("w") as f:
                json.dump({}, f)

            result = reload_output_from_file(file_path)

            assert result == {}

    def test_reload_nested_structure(self):
        """Test reloading deeply nested structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "output.json"
            test_data = {
                "org/package": {
                    "v1.0.0": {
                        "version": "v1.0.0",
                        "download_failed": False,
                        "parse_compatible": True,
                    }
                }
            }
            with file_path.open("w") as f:
                json.dump(test_data, f)

            result = reload_output_from_file(file_path)

            assert result["org/package"]["v1.0.0"]["parse_compatible"] is True


class TestExtractOutputFromJson:
    """Tests for extract_output_from_json function."""

    def test_extract_single_package_single_version(self):
        """Test extracting data for single package and version."""
        data = defaultdict(dict)
        data["package1"]["1.0.0"] = {
            "version": "1.0.0",
            "download_failed": False,
            "parse_compatible": True,
        }

        result = extract_output_from_json(data)

        assert "package1" in result
        assert "1.0.0" in result["package1"]
        assert isinstance(result["package1"]["1.0.0"], FusionConformanceResult)

    def test_extract_multiple_packages_multiple_versions(self):
        """Test extracting data for multiple packages and versions."""
        data = defaultdict(dict)
        data["package1"]["1.0.0"] = {
            "version": "1.0.0",
            "download_failed": False,
            "parse_compatible": True,
        }
        data["package1"]["2.0.0"] = {
            "version": "2.0.0",
            "download_failed": False,
            "parse_compatible": False,
        }
        data["package2"]["1.5.0"] = {
            "version": "1.5.0",
            "download_failed": True,
        }

        result = extract_output_from_json(data)

        assert len(result) == 2
        assert len(result["package1"]) == 2
        assert len(result["package2"]) == 1

    def test_extract_returns_conformance_result_objects(self):
        """Test that extracted data contains FusionConformanceResult objects."""
        data = defaultdict(dict)
        data["pkg"]["1.0.0"] = {
            "version": "1.0.0",
            "download_failed": False,
            "require_dbt_version_defined": True,
        }

        result = extract_output_from_json(data)

        conformance_result = result["pkg"]["1.0.0"]
        assert isinstance(conformance_result, FusionConformanceResult)
        assert conformance_result.version == "1.0.0"
        assert conformance_result.download_failed is False


class TestCheckForRename:
    """Tests for check_for_rename function."""

    def test_check_for_rename_valid_package(self):
        """Test checking rename for valid package."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hub_path = tmpdir
            package_name = "org/package"

            # Create the necessary directory structure
            pkg_dir = Path(hub_path) / "data" / "packages" / package_name
            pkg_dir.mkdir(parents=True)

            # Create index.json (version should not have 'v' prefix)
            index_file = pkg_dir / "index.json"
            index_file.write_text(json.dumps({"latest": "1.5.0"}))

            result = check_for_rename(hub_path, package_name)

            # VersionSpecifier includes operator prefix
            assert "1.5.0" in str(result)

    def test_check_for_rename_with_version_variations(self):
        """Test checking rename with different version formats."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hub_path = tmpdir
            package_name = "org/pkg"

            pkg_dir = Path(hub_path) / "data" / "packages" / package_name
            pkg_dir.mkdir(parents=True)

            index_file = pkg_dir / "index.json"
            index_file.write_text(json.dumps({"latest": "2.0.0-beta.1"}))

            result = check_for_rename(hub_path, package_name)

            assert "2.0.0" in str(result)


class TestFindPackageHubFile:
    """Tests for find_package_hub_file function."""

    def test_find_file_with_v_prefix(self):
        """Test finding file with 'v' prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hub_path = tmpdir
            package_name = "org/pkg"
            version = "1.0.0"

            # Create the necessary directory structure
            versions_dir = Path(hub_path) / "data" / "packages" / package_name / "versions"
            versions_dir.mkdir(parents=True)

            # Create file with 'v' prefix
            version_file = versions_dir / "v1.0.0.json"
            version_file.write_text("{}")

            result = find_package_hub_file(hub_path, package_name, version)

            assert result.name == "v1.0.0.json"

    def test_find_file_without_v_prefix(self):
        """Test finding file without 'v' prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hub_path = tmpdir
            package_name = "org/pkg"
            version = "1.0.0"

            versions_dir = Path(hub_path) / "data" / "packages" / package_name / "versions"
            versions_dir.mkdir(parents=True)

            # Create file without 'v' prefix
            version_file = versions_dir / "1.0.0.json"
            version_file.write_text("{}")

            result = find_package_hub_file(hub_path, package_name, version)

            assert result.name == "1.0.0.json"

    def test_find_file_prefers_no_v_prefix(self):
        """Test that function prefers file without 'v' prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hub_path = tmpdir
            package_name = "org/pkg"
            version = "1.0.0"

            versions_dir = Path(hub_path) / "data" / "packages" / package_name / "versions"
            versions_dir.mkdir(parents=True)

            # Create both versions
            (versions_dir / "1.0.0.json").write_text("{}")
            (versions_dir / "v1.0.0.json").write_text("{}")

            result = find_package_hub_file(hub_path, package_name, version)

            # Should find the one without 'v' first
            assert result.name == "1.0.0.json"


class TestGetJsonFromPackageHubFile:
    """Tests for get_json_from_package_hub_file function."""

    def test_get_json_valid_file(self):
        """Test getting JSON from valid file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "version.json"
            test_json = {
                "id": "org/pkg",
                "name": "package",
                "version": "1.0.0",
            }
            file_path.write_text(json.dumps(test_json))

            result = get_json_from_package_hub_file(file_path, "org/pkg", "1.0.0")

            assert result == test_json

    def test_get_json_file_not_found(self):
        """Test getting JSON from nonexistent file."""
        file_path = Path("/nonexistent/file.json")

        result = get_json_from_package_hub_file(file_path, "org/pkg", "1.0.0")

        assert result == {}

    def test_get_json_complex_structure(self):
        """Test getting JSON from file with complex structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "version.json"
            test_json = {
                "id": "org/pkg",
                "name": "package",
                "version": "1.0.0",
                "packages": [{"package": "dependency", "version": "1.0.0"}],
                "require_dbt_version": ">=1.0.0",
                "_source": {"url": "https://github.com/org/pkg"},
                "downloads": {"tarball": "https://example.com/tar.gz"},
            }
            file_path.write_text(json.dumps(test_json))

            result = get_json_from_package_hub_file(file_path, "org/pkg", "1.0.0")

            assert result["packages"] == test_json["packages"]
            assert result["_source"] == test_json["_source"]


class TestUpdateHubJson:
    """Tests for update_hub_json function."""

    def test_update_preserves_original_structure(self):
        """Test that update preserves original JSON structure."""
        original_json = {
            "id": "org/pkg",
            "name": "package",
            "version": "1.0.0",
            "published_at": "2023-01-01",
            "packages": [],
            "works_with": [],
            "_source": {"url": "https://github.com/org/pkg"},
            "downloads": {"tarball": "https://example.com/tar.gz"},
        }
        conformance = FusionConformanceResult(
            version="1.0.0",
            parse_compatible=True,
        )

        result = update_hub_json(original_json, conformance, "2.0.0")

        # Check that original fields are preserved
        assert result["id"] == original_json["id"]
        assert result["name"] == original_json["name"]
        assert result["version"] == original_json["version"]

    def test_update_adds_fusion_compatibility(self):
        """Test that update adds fusion compatibility field."""
        original_json = {
            "id": "org/pkg",
            "name": "package",
            "version": "1.0.0",
            "published_at": "2023-01-01",
            "packages": [],
            "works_with": [],
            "_source": {"url": "https://github.com/org/pkg"},
            "downloads": {"tarball": "https://example.com/tar.gz"},
        }
        conformance = FusionConformanceResult(
            version="1.0.0",
            parse_compatible=True,
            require_dbt_version_defined=True,
        )

        result = update_hub_json(original_json, conformance, "2.0.0")

        assert "fusion_compatibility" in result
        assert result["fusion_compatibility"]["version"] == "1.0.0"

    def test_update_preserves_manual_verification(self):
        """Test that update preserves manual verification flags."""
        original_json = {
            "id": "org/pkg",
            "name": "package",
            "version": "1.0.0",
            "published_at": "2023-01-01",
            "packages": [],
            "works_with": [],
            "_source": {"url": "https://github.com/org/pkg"},
            "downloads": {"tarball": "https://example.com/tar.gz"},
            "fusion_compatibility": {
                "manually_verified_compatible": True,
            },
        }
        conformance = FusionConformanceResult(
            version="1.0.0",
            parse_compatible=False,
        )

        result = update_hub_json(original_json, conformance, "2.0.0")

        # Should preserve the manual verification flag
        assert result["fusion_compatibility"]["manually_verified_compatible"] is True

    def test_update_with_require_dbt_version(self):
        """Test update with require_dbt_version field."""
        original_json = {
            "id": "org/pkg",
            "name": "package",
            "version": "1.0.0",
            "published_at": "2023-01-01",
            "packages": [],
            "require_dbt_version": ">=1.5.0",
            "works_with": [],
            "_source": {"url": "https://github.com/org/pkg"},
            "downloads": {"tarball": "https://example.com/tar.gz"},
        }
        conformance = FusionConformanceResult(version="1.0.0")

        result = update_hub_json(original_json, conformance, "2.0.0")

        assert result["require_dbt_version"] == ">=1.5.0"

    def test_update_without_require_dbt_version(self):
        """Test update when require_dbt_version is not in original."""
        original_json = {
            "id": "org/pkg",
            "name": "package",
            "version": "1.0.0",
            "published_at": "2023-01-01",
            "packages": [],
            "works_with": [],
            "_source": {"url": "https://github.com/org/pkg"},
            "downloads": {"tarball": "https://example.com/tar.gz"},
        }
        conformance = FusionConformanceResult(version="1.0.0")

        result = update_hub_json(original_json, conformance, "2.0.0")

        # Should not have require_dbt_version if not in original
        assert "require_dbt_version" not in result


class TestWriteDictToJson:
    """Tests for write_dict_to_json function."""

    def test_write_dict_to_file(self):
        """Test writing dictionary to JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir) / "output.json"
            data = {
                "id": "org/pkg",
                "name": "package",
                "version": "1.0.0",
            }

            write_dict_to_json(data, dest_path)

            assert dest_path.exists()
            with dest_path.open("r") as f:
                written_data = json.load(f)
            assert written_data == data

    def test_write_dict_with_custom_indent(self):
        """Test writing with custom indentation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir) / "output.json"
            data = {"key": "value"}

            write_dict_to_json(data, dest_path, indent=4)

            with dest_path.open("r") as f:
                content = f.read()
            # 4-space indent should be in the output
            assert "    " in content

    def test_write_preserves_unicode(self):
        """Test that Unicode characters are preserved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir) / "output.json"
            data = {"name": "café", "emoji": "🚀"}

            write_dict_to_json(data, dest_path)

            with dest_path.open("r", encoding="utf-8") as f:
                written_data = json.load(f)
            assert written_data["name"] == "café"
            assert written_data["emoji"] == "🚀"

    def test_write_preserves_order_when_sort_keys_false(self):
        """Test that key order is preserved when sort_keys=False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir) / "output.json"
            # Use a regular dict which preserves insertion order in Python 3.7+
            data = {"z": 1, "a": 2, "m": 3}

            write_dict_to_json(data, dest_path, sort_keys=False)

            with dest_path.open("r") as f:
                content = f.read()
            # Check order in the file
            z_pos = content.find('"z"')
            a_pos = content.find('"a"')
            m_pos = content.find('"m"')
            assert z_pos < a_pos < m_pos


class TestIntegrationUpdatePackageHubJson:
    """Integration tests for the update_package_hub_json workflow."""

    def test_full_workflow(self):
        """Test the full workflow of updating package hub JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create conformance output file
            conformance_output = {
                "org/pkg": {
                    "1.0.0": {
                        "version": "1.0.0",
                        "download_failed": False,
                        "parse_compatible": True,
                        "require_dbt_version_defined": True,
                        "require_dbt_version_compatible": True,
                    }
                }
            }
            conformance_file = Path(tmpdir) / "conformance.json"
            conformance_file.write_text(json.dumps(conformance_output))

            # Load and extract
            loaded = reload_output_from_file(conformance_file)
            extracted = extract_output_from_json(loaded)

            assert "org/pkg" in extracted
            assert "1.0.0" in extracted["org/pkg"]

            # Verify the extracted data is correct type
            conformance_result = extracted["org/pkg"]["1.0.0"]
            assert isinstance(conformance_result, FusionConformanceResult)
            assert conformance_result.parse_compatible is True

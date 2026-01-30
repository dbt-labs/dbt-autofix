"""Unit tests for check_parse_conformance module."""

import io
import json
import subprocess
import tarfile
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from dbt_fusion_package_tools.check_parse_conformance import (
    check_binary_name,
    check_fusion_schema_compatibility,
    check_fusion_version,
    construct_download_url_from_latest,
    download_tarball_and_run_conformance,
    find_fusion_binary,
    parse_log_output,
)
from dbt_fusion_package_tools.compatibility import (
    FusionConformanceResult,
    FusionLogMessage,
    ParseConformanceLogOutput,
)


# Basic integration test mainly for local debugging
def test_fusion_schema_compat():
    output = check_fusion_schema_compatibility(
        Path("tests/integration_tests/package_upgrades/dbt_utils_package_lookup_map_2")
    )
    print(output)
    print()
    print(
        check_fusion_schema_compatibility(
            Path("tests/integration_tests/package_upgrades/dbt_utils_package_lookup_map_2"), show_fusion_output=False
        )
    )


class TestConstructDownloadUrlFromLatest:
    """Tests for construct_download_url_from_latest function."""

    def test_replaces_version_tag(self):
        """Test that version tag is correctly replaced."""
        latest_url = "https://codeload.github.com/dbt-labs/dbt-utils/tar.gz/v1.0.0"
        target_url = "https://codeload.github.com/dbt-labs/dbt-utils/tar.gz/v0.9.0"

        result = construct_download_url_from_latest(latest_url, target_url)

        assert result == "https://codeload.github.com/dbt-labs/dbt-utils/tar.gz/v0.9.0"

    def test_preserves_url_structure(self):
        """Test that URL structure is preserved."""
        latest_url = "https://codeload.github.com/package/name/tar.gz/main"
        target_url = "https://codeload.github.com/package/name/tar.gz/develop"

        result = construct_download_url_from_latest(latest_url, target_url)

        assert result == "https://codeload.github.com/package/name/tar.gz/develop"
        assert result.count("/") == latest_url.count("/")

    def test_different_paths(self):
        """Test with different path structures."""
        latest_url = "https://github.com/org/repo/archive/refs/tags/v2.0.0.tar.gz"
        target_url = "https://github.com/org/repo/archive/refs/tags/v1.5.0.tar.gz"

        result = construct_download_url_from_latest(latest_url, target_url)

        assert result.endswith("v1.5.0.tar.gz")
        assert result.startswith("https://github.com/org/repo/archive/refs/tags/")


class TestCheckBinaryName:
    """Tests for check_binary_name function."""

    @patch("dbt_fusion_package_tools.check_parse_conformance.subprocess.run")
    def test_binary_exists(self, mock_run):
        """Test when binary exists and runs successfully."""
        mock_run.return_value = Mock(returncode=0)

        result = check_binary_name("dbtf")

        assert result is True
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == ["dbtf", "--version"]

    @patch("dbt_fusion_package_tools.check_parse_conformance.subprocess.run")
    def test_binary_not_found(self, mock_run):
        """Test when binary is not found."""
        mock_run.side_effect = FileNotFoundError()

        result = check_binary_name("dbtf")

        assert result is False

    @patch("dbt_fusion_package_tools.check_parse_conformance.subprocess.run")
    def test_binary_call_error(self, mock_run):
        """Test when binary call fails."""
        mock_run.side_effect = subprocess.CalledProcessError(1, "dbtf")

        result = check_binary_name("dbtf")

        assert result is False

    @patch("dbt_fusion_package_tools.check_parse_conformance.subprocess.run")
    def test_unknown_exception(self, mock_run):
        """Test when unknown exception occurs."""
        mock_run.side_effect = Exception("Unknown error")

        result = check_binary_name("dbtf")

        assert result is False


class TestCheckFusionVersion:
    """Tests for check_fusion_version function."""

    @patch("dbt_fusion_package_tools.check_parse_conformance.subprocess.run")
    def test_returns_version_for_fusion(self, mock_run):
        """Test that version is extracted for dbt-fusion."""
        mock_run.return_value = Mock(stdout="dbt-fusion version v2.0.0-beta.34", returncode=0)

        result = check_fusion_version("dbtf")

        assert result == "v2.0.0-beta.34"

    @patch("dbt_fusion_package_tools.check_parse_conformance.subprocess.run")
    def test_returns_none_for_non_fusion(self, mock_run):
        """Test that None is returned for non-fusion versions."""
        mock_run.return_value = Mock(stdout="dbt version 1.7.0", returncode=0)

        result = check_fusion_version("dbt")

        assert result is None

    @patch("dbt_fusion_package_tools.check_parse_conformance.subprocess.run")
    def test_exception_handling(self, mock_run):
        """Test exception handling."""
        mock_run.side_effect = Exception("Timeout")

        result = check_fusion_version("dbtf")

        assert result is None


class TestFindFusionBinary:
    """Tests for find_fusion_binary function."""

    @patch("dbt_fusion_package_tools.check_parse_conformance.check_fusion_version")
    @patch("dbt_fusion_package_tools.check_parse_conformance.check_binary_name")
    def test_finds_dbtf_binary(self, mock_check_binary, mock_check_version):
        """Test finding dbtf binary."""
        # Both binaries exist, but check dbtf first
        mock_check_binary.return_value = True

        def version_side_effect(binary):
            if binary == "dbtf":
                return "v2.0.0"
            return None

        mock_check_version.side_effect = version_side_effect

        result = find_fusion_binary()

        # Should find dbtf (checked first)
        assert result == "dbtf"

    @patch("dbt_fusion_package_tools.check_parse_conformance.check_fusion_version")
    @patch("dbt_fusion_package_tools.check_parse_conformance.check_binary_name")
    def test_finds_custom_binary(self, mock_check_binary, mock_check_version):
        """Test finding custom named binary."""
        mock_check_binary.return_value = True
        mock_check_version.return_value = "v1.0.0"

        result = find_fusion_binary(custom_name="dbt-fusion")

        assert result == "dbt-fusion"

    @patch("dbt_fusion_package_tools.check_parse_conformance.check_fusion_version")
    @patch("dbt_fusion_package_tools.check_parse_conformance.check_binary_name")
    def test_no_binary_found(self, mock_check_binary, mock_check_version):
        """Test when no binary is found."""
        mock_check_binary.return_value = False

        result = find_fusion_binary()

        assert result is None


class TestParseLogOutput:
    """Tests for parse_log_output function."""

    def test_parses_error_messages(self):
        """Test parsing error messages from log output."""
        log_output = json.dumps(
            {
                "event_type": "v1.public.events.fusion.log.LogMessage",
                "severity_text": "ERROR",
                "body": "Schema error",
                "attributes": {"code": 100, "message": "error message", "original_severity_text": "ERROR"},
            }
        )

        result = parse_log_output(log_output, exit_code=1)

        assert result.parse_exit_code == 1
        assert len(result.errors) == 1
        assert result.errors[0].severity_text == "ERROR"
        assert "Schema error" in result.errors[0].body

    def test_parses_warning_messages(self):
        """Test parsing warning messages from log output."""
        log_output = json.dumps(
            {
                "event_type": "v1.public.events.fusion.log.LogMessage",
                "severity_text": "WARNING",
                "body": "Deprecation warning",
                "attributes": {"code": 50, "message": "warning message", "original_severity_text": "WARNING"},
            }
        )

        result = parse_log_output(log_output, exit_code=0)

        assert result.parse_exit_code == 0
        assert len(result.warnings) == 1
        assert result.warnings[0].severity_text == "WARNING"

    def test_removes_temp_directory_from_body(self):
        """Test that temporary directory paths are removed from log messages."""
        temp_path = "/tmp/xyz123"
        log_output = json.dumps(
            {
                "event_type": "v1.public.events.fusion.log.LogMessage",
                "severity_text": "ERROR",
                "body": f"{temp_path}/models/model.sql",
                "attributes": {"code": 100, "message": "error", "original_severity_text": "ERROR"},
            }
        )

        result = parse_log_output(log_output, exit_code=1, repo_path=Path(temp_path))

        assert all(temp_path not in error.body for error in result.errors)

    def test_sets_fusion_version(self):
        """Test that fusion version is set in output."""
        log_output = json.dumps(
            {
                "event_type": "v1.public.events.fusion.log.LogMessage",
                "severity_text": "ERROR",
                "body": "error",
                "attributes": {"code": 100, "message": "error", "original_severity_text": "ERROR"},
            }
        )

        result = parse_log_output(log_output, exit_code=1, fusion_version="v2.0.0-beta.34")

        assert result.fusion_version == "v2.0.0-beta.34"

    def test_parses_multiple_messages(self):
        """Test parsing multiple log messages."""
        log_output = (
            json.dumps(
                {
                    "event_type": "v1.public.events.fusion.log.LogMessage",
                    "severity_text": "ERROR",
                    "body": "error 1",
                    "attributes": {"code": 100, "message": "error", "original_severity_text": "ERROR"},
                }
            )
            + "\n"
            + json.dumps(
                {
                    "event_type": "v1.public.events.fusion.log.LogMessage",
                    "severity_text": "WARNING",
                    "body": "warning 1",
                    "attributes": {"code": 50, "message": "warning", "original_severity_text": "WARNING"},
                }
            )
        )

        result = parse_log_output(log_output, exit_code=1)

        assert len(result.errors) == 1
        assert len(result.warnings) == 1

    def test_ignores_unknown_event_types(self):
        """Test that unknown event types are ignored."""
        log_output = json.dumps(
            {
                "event_type": "unknown.event.type",
                "severity_text": "ERROR",
                "body": "should be ignored",
                "attributes": {},
            }
        )

        result = parse_log_output(log_output, exit_code=0)

        assert len(result.errors) == 0
        assert len(result.warnings) == 0

    def test_parses_invocation_metrics(self):
        """Test parsing invocation metrics."""
        log_output = json.dumps(
            {
                "record_type": "SpanEnd",
                "event_type": "v1.public.events.fusion.invocation.Invocation",
                "attributes": {"metrics": {"total_errors": 5, "total_warnings": 3}},
            }
        )

        result = parse_log_output(log_output, exit_code=1)

        assert result.total_errors == 5
        assert result.total_warnings == 3


class TestDownloadTarballAndRunConformance:
    """Tests for download_tarball_and_run_conformance function."""

    @patch("dbt_fusion_package_tools.check_parse_conformance.run_conformance_for_version")
    @patch("dbt_fusion_package_tools.check_parse_conformance.requests.get")
    def test_successful_download_and_extract(self, mock_get, mock_conformance):
        """Test successful download and extraction of tarball."""
        # Mock the tarball download
        mock_response = Mock()
        mock_response.iter_content = Mock(return_value=[b"mock_tar_data"])
        mock_get.return_value = mock_response

        # Create a real tar file for testing
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a mock tarball content

            tar_buffer = io.BytesIO()
            with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
                # Add a simple directory structure
                tarinfo = tarfile.TarInfo(name="test-package-v1.0.0/dbt_project.yml")
                tarinfo.size = 10
                tar.addfile(tarinfo, io.BytesIO(b"test yaml\n"))

            tar_buffer.seek(0)
            mock_response.iter_content = Mock(return_value=[tar_buffer.read()])

            # Mock conformance check
            mock_conformance.return_value = FusionConformanceResult(version="v1.0.0", parse_compatible=True)

            result = download_tarball_and_run_conformance(
                package_name="test_package",
                package_id="test-org/test-package",
                package_version_str="v1.0.0",
                package_version_download_url="https://codeload.github.com/test/tar.gz/v1.0.0",
                latest_package_version_download_url="https://codeload.github.com/test/tar.gz/main",
            )

            assert result is not None
            assert result.version == "v1.0.0"

    @patch("dbt_fusion_package_tools.check_parse_conformance.requests.get")
    def test_download_failure(self, mock_get):
        """Test handling of download failure."""
        mock_get.side_effect = Exception("Network error")

        result = download_tarball_and_run_conformance(
            package_name="test_package",
            package_id="test-org/test-package",
            package_version_str="v1.0.0",
            package_version_download_url="https://codeload.github.com/test/tar.gz/v1.0.0",
            latest_package_version_download_url=None,
        )

        assert result is not None
        assert result.download_failed is True

    @patch("dbt_fusion_package_tools.check_parse_conformance.run_conformance_for_version")
    @patch("dbt_fusion_package_tools.check_parse_conformance.requests.get")
    def test_fallback_to_latest_url(self, mock_get, mock_conformance):
        """Test fallback to latest URL when primary fails."""
        # First call fails, second succeeds
        mock_response_fail = Mock()
        mock_response_fail.raise_for_status.side_effect = Exception("Not found")

        mock_response_success = Mock()
        mock_response_success.iter_content = Mock(return_value=[b"mock_tar"])

        mock_get.side_effect = [mock_response_fail, mock_response_success]

        mock_conformance.return_value = FusionConformanceResult(version="v1.0.0")

        # This should try the fallback URL
        result = download_tarball_and_run_conformance(
            package_name="test_package",
            package_id="test-org/test-package",
            package_version_str="v1.0.0",
            package_version_download_url="https://codeload.github.com/test/tar.gz/v1.0.0",
            latest_package_version_download_url="https://codeload.github.com/test/tar.gz/main",
        )

        # Should have attempted both URLs
        assert mock_get.call_count >= 1


class TestFusionConformanceResult:
    """Tests for FusionConformanceResult dataclass."""

    def test_create_with_defaults(self):
        """Test creating result with default values."""
        result = FusionConformanceResult(version="v1.0.0")

        assert result.version == "v1.0.0"
        assert result.download_failed is False
        assert result.require_dbt_version_defined is None

    def test_create_with_all_fields(self):
        """Test creating result with all fields."""
        parse_output = ParseConformanceLogOutput(
            parse_exit_code=0, total_errors=0, total_warnings=0, fusion_version="v2.0.0"
        )

        result = FusionConformanceResult(
            version="v1.0.0",
            require_dbt_version_defined=True,
            require_dbt_version_compatible=True,
            parse_compatible=True,
            parse_compatibility_result=parse_output,
            manually_verified_compatible=False,
            manually_verified_incompatible=False,
            download_failed=False,
        )

        assert result.version == "v1.0.0"
        assert result.parse_compatible is True
        assert result.parse_compatibility_result.fusion_version == "v2.0.0"


class TestParseConformanceLogOutput:
    """Tests for ParseConformanceLogOutput dataclass."""

    def test_create_with_defaults(self):
        """Test creating output with default values."""
        result = ParseConformanceLogOutput()

        assert result.parse_exit_code == 0
        assert result.total_errors == 0
        assert result.total_warnings == 0
        assert result.errors == []
        assert result.warnings == []
        assert result.fusion_version == "unknown"

    def test_add_error_messages(self):
        """Test adding error messages."""
        result = ParseConformanceLogOutput()
        error = FusionLogMessage(body="test error", severity_text="ERROR", error_code=100)
        result.errors.append(error)

        assert len(result.errors) == 1
        assert result.errors[0].body == "test error"

    def test_add_warning_messages(self):
        """Test adding warning messages."""
        result = ParseConformanceLogOutput()
        warning = FusionLogMessage(body="test warning", severity_text="WARNING", error_code=50)
        result.warnings.append(warning)

        assert len(result.warnings) == 1
        assert result.warnings[0].body == "test warning"

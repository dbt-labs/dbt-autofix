"""Interface for objects useful to processing hub entries"""

from dataclasses import dataclass, field
import json
import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Optional

from rich.console import Console

from dbt_fusion_package_tools.exceptions import FusionBinaryNotAvailable
from dbt_fusion_package_tools.git.package_repo import DbtPackageRepo
from dbt_fusion_package_tools.yaml.loader import safe_load
from dbt_fusion_package_tools.dbt_package import DbtPackage
from dbt_fusion_package_tools.dbt_package_version import DbtPackageVersion
from dbt_fusion_package_tools.version_utils import VersionSpecifier
from dbtlabs.proto.public.v1.events.fusion.invocation.invocation_pb2 import Invocation
from dbtlabs.proto.public.v1.events.fusion.log.log_pb2 import LogMessage
from google.protobuf import json_format

console = Console()
error_console = Console(stderr=True)


@dataclass
class FusionLogMessage:
    body: str
    message: LogMessage


@dataclass
class ParseConformanceLogOutput:
    parse_exit_code: int = 0
    total_errors: int = 0
    total_warnings: int = 0
    errors: list[FusionLogMessage] = field(default_factory=list)
    warnings: list[FusionLogMessage] = field(default_factory=list)


@dataclass
class FusionConformanceResult:
    version: Optional[str] = None
    require_dbt_version_defined: Optional[bool] = None
    require_dbt_version_compatible: Optional[bool] = None
    parse_compatible: Optional[bool] = None
    parse_compatibility_result: Optional[ParseConformanceLogOutput] = None
    manually_verified_compatible: Optional[bool] = None
    manually_verified_incompatible: Optional[bool] = None


def checkout_repo_and_run_conformance(
    github_organization: str, github_repo_name: str, package_name: str, limit: int = 0
) -> dict[str, FusionConformanceResult]:
    results: dict[str, FusionConformanceResult] = {}
    with TemporaryDirectory() as tmpdir:
        print(f"writing to {tmpdir}")
        repo = DbtPackageRepo(
            repo_name=package_name,
            github_organization=github_organization,
            github_repo_name=github_repo_name,
            local_path=tmpdir,
        )
        package_id = f"{github_organization}/{github_repo_name}"
        # package: DbtPackage = DbtPackage(package_name=package_name, package_id=package_id, project_config_raw_version_specifier=None)
        tags = repo.get_tags()
        for i, tag in enumerate(tags):
            if limit > 0 and i > limit:
                break
            checked_out = repo.checkout_tag(tag, stash_changes=True)
            console.log(f"tag {tag.name} checked out: {checked_out}")
            tag_version = tag.name[1:] if tag.name[0] == "v" else tag.name
            result = run_conformance_for_version(tmpdir, package_name, tag_version, package_id)
            if result:
                results[tag_version] = result
    return results


def run_conformance_for_version(path, package_name, tag_version, package_id) -> Optional[FusionConformanceResult]:
    result = FusionConformanceResult(version=tag_version)
    # check require dbt version
    try:
        dbt_project_yml = safe_load((Path(f"{path}/dbt_project.yml")).read_text()) or ({}, {})
        require_dbt_version_string = dbt_project_yml[1].get("require_dbt_version")
    except Exception as e:
        error_console.log(f"dbt_project.yml load failed for {package_id} {tag_version}: {e}")
        return
    # try to add profile to suppress warning about profiles
    if "profile" not in dbt_project_yml[1]:
        console.log("Adding profile to dbt project")
        try:
            with open(Path(f"{path}/dbt_project.yml"), "a") as f:
                f.write("\nprofile: test_schema_compat\n")
        except Exception as e:
            error_console.log(f"failed when adding profile to dbt_project.yml for {package_id} {tag.name}: {e}")
    new_version: DbtPackageVersion = DbtPackageVersion(
        package_name, tag_version, package_id=package_id, raw_require_dbt_version_range=require_dbt_version_string
    )
    parse_conformance = check_fusion_schema_compatibility(Path(path), show_fusion_output=True)
    result.require_dbt_version_compatible = new_version.is_require_dbt_version_fusion_compatible()
    result.require_dbt_version_defined = new_version.is_require_dbt_version_defined()
    result.manually_verified_compatible = new_version.is_explicitly_allowed_on_fusion()
    result.manually_verified_incompatible = new_version.is_explicitly_disallowed_on_fusion()
    result.parse_compatibility_result = parse_conformance
    if parse_conformance:
        result.parse_compatible = parse_conformance.parse_exit_code == 0
        result.parse_compatibility_result = parse_conformance
    return result


def check_binary_name(binary_name: str) -> bool:
    try:
        subprocess.run(
            [
                binary_name,
                "--version",
            ],
            capture_output=True,
            timeout=60,
            check=True,
        )
        return True
    # indicates that the binary name is not found
    except FileNotFoundError:
        return False
    # indicates that an error occurred when running command
    except subprocess.CalledProcessError as process_error:
        error_console.log(
            f"CalledProcessError: {binary_name} --version exited with return code {process_error.returncode}"
        )
        error_console.log(process_error.stderr)
        return False
    except Exception as other_error:
        error_console.log(f"{other_error}: An unknown exception occured when running {binary_name} --version")
        return False


def find_fusion_binary(custom_name: Optional[str] = None) -> Optional[str]:
    possible_binary_names: list[str] = ["dbtf", "dbt"] if custom_name is None else [custom_name]

    binary_names_found: set[str] = set()
    # test each name
    for binary_name in possible_binary_names:
        # first check if name exists at all
        binary_exists = check_binary_name(binary_name)
        if binary_exists:
            binary_names_found.add(binary_name)

    if len(binary_names_found) == 0:
        error_console.log("No fusion binaries found on system path, please install first")
        return None

    # now check version returned by each and use first one
    # don't need exception handling here because previous step already did it
    for valid_binary_name in binary_names_found:
        version_result = subprocess.run(
            [
                valid_binary_name,
                "--version",
            ],
            check=False,
            capture_output=True,
            timeout=60,
            text=True,
        )
        if "dbt-fusion" in version_result.stdout:
            return valid_binary_name

    # if we got to the end, then no fusion version has been found
    error_console.log(f"Could not find Fusion binary, latest version output is {version_result.stdout}")
    return None


def parse_log_output(output: str, exit_code: int) -> ParseConformanceLogOutput:
    log_output = [json.loads(x) for x in output.splitlines()]
    result = ParseConformanceLogOutput(parse_exit_code=exit_code)
    for line in log_output:
        if line.get("event_type") == "v1.public.events.fusion.log.LogMessage":
            severity_text = line.get("severity_text")
            body = line.get("body")
            log_message = LogMessage()
            json_format.ParseDict(line.get("attributes"), log_message, ignore_unknown_fields=True)
            fusion_log_message = FusionLogMessage(body, log_message)
            if severity_text == "ERROR":
                result.errors.append(fusion_log_message)
            elif severity_text == "WARNING":
                result.warnings.append(fusion_log_message)
        elif (
            line.get("record_type") == "SpanEnd"
            and line.get("event_type") == "v1.public.events.fusion.invocation.Invocation"
        ):
            invocation = Invocation()
            json_format.ParseDict(line.get("attributes"), invocation, ignore_unknown_fields=True)
            result.total_errors += invocation.metrics.total_errors
            result.total_warnings += invocation.metrics.total_warnings

    return result


def check_fusion_schema_compatibility(
    repo_path: Path = Path.cwd(), show_fusion_output=True
) -> Optional[ParseConformanceLogOutput]:
    """
    Check if a dbt package is fusion schema compatible by running 'dbtf parse'.

    Args:
        repo_path: Path to the dbt package repository

    Returns:
        True if fusion compatible (dbtf parse exits with code 0), False otherwise
    """
    # Add a test profiles.yml to the current directory
    profiles_path = repo_path / Path("profiles.yml")
    try:
        with open(profiles_path, "a") as f:
            f.write(
                "\n"
                "test_schema_compat:\n"
                "  target: dev\n"
                "  outputs:\n"
                "    dev:\n"
                "      type: postgres\n"
                "      host: localhost\n"
                "      port: 5432\n"
                "      user: postgres\n"
                "      password: postgres\n"
                "      dbname: postgres\n"
                "      schema: public\n"
            )

        # Ensure the `_DBT_FUSION_STRICT_MODE` is set (this will ensure fusion errors on schema violations)
        os.environ["_DBT_FUSION_STRICT_MODE"] = "1"

        # Find correct name for Fusion binary
        fusion_binary_name: Optional[str] = find_fusion_binary()
        if fusion_binary_name is None:
            raise FusionBinaryNotAvailable()

        try:
            # Run dbt deps to install package dependencies
            if show_fusion_output:
                console.log("\n\nRunning dbt deps", style="green")
            deps_result = subprocess.run(
                [
                    fusion_binary_name,
                    "deps",
                    "--profile",
                    "test_schema_compat",
                    "--project-dir",
                    str(repo_path),
                ],
                check=False,
                text=True,
                capture_output=True,
                timeout=60,
            )
            if deps_result.returncode != 0:
                error_console.log(f"dbt deps returned errors")
                error_console.log(parse_log_output(deps_result.stdout, deps_result.returncode))

            # Now try parse
            if show_fusion_output:
                console.log("\n\nRunning dbt parse", style="green")
            parse_result = subprocess.run(
                [
                    fusion_binary_name,
                    "parse",
                    "--profile",
                    "test_schema_compat",
                    "--project-dir",
                    str(repo_path),
                    "--log-format",
                    "otel",
                ],
                check=False,
                text=True,
                capture_output=True,
                timeout=60,
            )
            parse_output = parse_log_output(parse_result.stdout, parse_result.returncode)
            if parse_result.returncode != 0:
                error_console.log(f"dbt parse returned errors")
                error_console.log(parse_output)
        except Exception as e:
            error_console.log(f"{e}: An unknown error occurred when running dbt parse")
            return

        # Return True if exit code is 0 (success)
        is_compatible = parse_result.returncode == 0

        if show_fusion_output:
            if is_compatible:
                console.log(f"Package at {repo_path} is fusion schema compatible")
            else:
                console.log(f"Package at {repo_path} is not fusion schema compatible")

        # Clean up deps
        if show_fusion_output:
            console.log("\n\nRunning dbt clean", style="green")
        subprocess.run(
            [
                fusion_binary_name,
                "clean",
                "--profile",
                "test_schema_compat",
                "--project-dir",
                str(repo_path),
            ],
            check=False,
            timeout=60,
            text=True,
            capture_output=True,
        )
        # Remove the test profile
        os.remove(profiles_path)

        return parse_output

    except Exception as e:
        error_console.log(f"Error checking fusion compatibility for {repo_path}: {e!s}")
        try:
            os.remove(profiles_path)
        except Exception:
            pass
        return


def main():
    check_fusion_schema_compatibility(Path.cwd())


if __name__ == "__main__":
    main()

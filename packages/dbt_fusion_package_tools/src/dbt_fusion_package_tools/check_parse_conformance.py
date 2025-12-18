"""Interface for objects useful to processing hub entries"""

import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

from rich.console import Console

from dbt_fusion_package_tools.exceptions import FusionBinaryNotAvailable
from dbt_fusion_package_tools.git.package_repo import DbtPackageRepo

console = Console()
error_console = Console(stderr=True)


def checkout_repo_and_run_conformance(
    github_organization: str, github_repo_name: str, package_name: str, limit: int = 0
):
    with TemporaryDirectory() as tmpdir:
        print(f"writing to {tmpdir}")
        repo = DbtPackageRepo(
            repo_name=package_name,
            github_organization=github_organization,
            github_repo_name=github_repo_name,
            local_path=tmpdir,
        )
        tags = repo.get_tags()
        for i, tag in enumerate(tags):
            if limit > 0 and i > limit:
                break
            checked_out = repo.checkout_tag(tag, stash_changes=True)
            print(f"tag {tag.name} checked out: {checked_out}")
            check_fusion_schema_compatibility(tmpdir, show_fusion_output=True)


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
            check=False, capture_output=True,
            timeout=60,
            text=True,
        )
        if "dbt-fusion" in version_result.stdout:
            return valid_binary_name

    # if we got to the end, then no fusion version has been found
    error_console.log(f"Could not find Fusion binary, latest version output is {version_result.stdout}")
    return None


def check_fusion_schema_compatibility(repo_path: Path = Path.cwd(), show_fusion_output=True) -> bool:
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
            subprocess.run(
                [
                    fusion_binary_name,
                    "deps",
                    "--profile",
                    "test_schema_compat",
                    "--project-dir",
                    str(repo_path),
                ],
                check=False, text=True,
                capture_output=(not show_fusion_output),
                timeout=60,
            )
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
                ],
                check=False, text=True,
                capture_output=(not show_fusion_output),
                timeout=60,
            )
        except Exception as e:
            error_console.log(f"{e}: An unknown error occurred when running dbt parse")
            return False

        # Return True if exit code is 0 (success)
        is_compatible = parse_result.returncode == 0

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
            check=False, timeout=60,
            text=True,
            capture_output=(not show_fusion_output),
        )
        # Remove the test profile
        os.remove(profiles_path)

        return is_compatible

    except Exception as e:
        error_console.log(f"Error checking fusion compatibility for {repo_path}: {e!s}")
        try:
            os.remove(profiles_path)
        except Exception:
            pass
        return False


def main():
    check_fusion_schema_compatibility(Path.cwd())


if __name__ == "__main__":
    main()

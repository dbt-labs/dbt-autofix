import os
from pathlib import Path
import subprocess


def check_multiple_version_tags(repo_path: Path) -> bool:
    git_tags_output = subprocess.run(
        [
            "git",
            "tag",
        ],
        capture_output=True,
        timeout=60,
        cwd=repo_path,
        text=True,
    )
    git_tags = git_tags_output.stdout.splitlines()

    for tag in git_tags:
        checkout_tag_output = subprocess.run(
            [
                "git",
                "checkout",
                tag,
            ],
            capture_output=True,
            timeout=60,
            cwd=repo_path,
            text=True,
        )
        if checkout_tag_output.returncode != 0:
            print(checkout_tag_output.stderr)
        print(f"version: {tag}")
        check_fusion_schema_compatibility(repo_path)
        subprocess.run(
            [
                "git",
                "stash",
                "-u",
            ],
            capture_output=True,
            timeout=60,
            cwd=repo_path,
            text=True,
        )

    return True


def check_fusion_schema_compatibility(repo_path: Path) -> bool:
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
                "integration_tests:\n"
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

        # Run dbtf parse command (try dbtf first, fall back to dbt)
        try:
            # Try dbtf first (without shell=True to get proper FileNotFoundError)
            result = subprocess.run(
                [
                    "dbtf",
                    "parse",
                    "--profile",
                    "test_schema_compat",
                    "--project-dir",
                    str(repo_path),
                ],
                capture_output=True,
                timeout=60,
            )
            # If dbtf command exists but returns error mentioning it's not found, fall back to dbt
            if result.returncode != 0 and result.stderr and b"not found" in result.stderr:
                print("dbtf not found")
                raise FileNotFoundError("dbtf command not found")
        except FileNotFoundError:
            # Fall back to dbt command, but validate that this is dbt-fusion
            version_result = subprocess.run(["dbt", "--version"], capture_output=True, timeout=60)
            # print(version_result)
            if b"dbt-fusion" not in version_result.stdout:
                raise FileNotFoundError("dbt-fusion command not found - regular dbt-core detected instead")

            dbt_deps_output = subprocess.run(
                [
                    "dbt",
                    "deps",
                    "--profile",
                    "test_schema_compat"
                ],
                capture_output=True,
                timeout=60,
                cwd=repo_path,
                text=True,
            )
            if dbt_deps_output.returncode != 0:
                print(dbt_deps_output.stderr)

            # Run dbt parse since we have dbt-fusion
            result = subprocess.run(
                [
                    "dbt",
                    "parse",
                    "--profile",
                    "test_schema_compat",
                    "--project-dir",
                    str(repo_path),
                ],
                capture_output=True,
                timeout=60,
                text=True,
            )

        # Return True if exit code is 0 (success)
        is_compatible = result.returncode == 0

        if is_compatible:
            print(f"Package at {repo_path} is fusion schema compatible")
        else:
            print(f"Package at {repo_path} is not fusion schema compatible")
            print(result.stderr)

        # Remove the test profile
        os.remove(profiles_path)

        return is_compatible

    except subprocess.TimeoutExpired:
        print(f"dbtf parse timed out for package at {repo_path}")
        try:
            os.remove(profiles_path)
        except Exception:
            pass
        return False
    except FileNotFoundError:
        print(f"dbtf command not found - skipping fusion compatibility check for {repo_path}")
        try:
            os.remove(profiles_path)
        except Exception:
            pass
        return False
    except Exception as e:
        print(f"Error checking fusion compatibility for {repo_path}: {str(e)}")
        try:
            os.remove(profiles_path)
        except Exception:
            pass
        return False


def main():
    print("Hello from parse conformance!")
    check_fusion_schema_compatibility(Path("/Users/chaya/workplace/packages/calogica/dbt-date"))
    # check_multiple_version_tags(Path("/Users/chaya/workplace/packages/calogica/dbt-date"))
    check_multiple_version_tags(Path("/Users/chaya/workplace/packages/dbt-labs/dbt-project-evaluator"))


if __name__ == "__main__":
    main()

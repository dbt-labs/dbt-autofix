"""Runs autofix on incompatible packages to see if that fixes parse errors."""

import json
import os
import subprocess
import tarfile
import warnings
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, List, Optional

import requests
import typer
from rich.console import Console
from typing_extensions import Annotated

from dbt_fusion_package_tools.check_parse_conformance import (
    check_fusion_schema_compatibility,
    construct_download_url_from_latest,
)
from dbt_fusion_package_tools.compatibility import (
    FusionConformanceResult,
    ParseConformanceLogOutput,
)

# from dbt_fusion_package_tools.dbt_package_version import DbtPackageVersion
from dbt_fusion_package_tools.scripts.package_hub_fusion_compatibility import (
    DEFAULT_FUSION_BINARY_PATH,
    DEFAULT_HUB_PATH,
    DEFAULT_OUTPUT_PATH,
    clean_version,
    extract_package_id_from_path,
    get_latest_github_tarball_urls,
    is_package_index_file,
    is_package_version_file,
)
from dbt_fusion_package_tools.yaml.loader import safe_load

console = Console()
error_console = Console(stderr=True)

app = typer.Typer()


# Notes:
# The package_id from path (which is the key in results)
# is the organization + package name
# This is not related to the Github URL
# Example:
# package_id_from_path = AxelThevenot/dbt_star
# package_version_download_url = https://codeload.github.com/AxelThevenot/dbt-star/tar.gz/0.1.0
# Package Hub page: https://hub.getdbt.com/AxelThevenot/dbt_star/latest/
# package name in packages.yml: AxelThevenot/dbt_star
def process_json_with_conformance(file_path: str, parsed_json: Any) -> dict[str, Any]:
    package_id = extract_package_id_from_path(file_path)
    if package_id == "":
        return {}
    if is_package_index_file(file_path):
        return {
            "package_id_from_path": package_id,
            "package_latest_version_index_json": clean_version(parsed_json.get("latest")),
            "package_name_index_json": parsed_json.get("name"),
            "package_namespace_index_json": parsed_json.get("namespace"),
            "package_redirect_name": parsed_json.get("redirectname"),
            "package_redirect_namespace": parsed_json.get("redirectnamespace"),
        }
    elif is_package_version_file(file_path):
        # console.log(parsed_json)
        if "_source" in parsed_json:
            github_url = parsed_json["_source"].get("url", "")
        else:
            github_url = None
        if "downloads" in parsed_json:
            tarball_url = parsed_json["downloads"].get("tarball")
        else:
            tarball_url = None
        if parsed_json != {}:
            if "fusion_compatibility" not in parsed_json:
                # console.log(f"No Fusion compatibility info found for {parsed_json.get('id')}")
                return {}
            else:
                if parsed_json["fusion_compatibility"].get("download_failed", False):
                    # console.log(f"Download previously failed for {parsed_json['id']}, skipping")
                    return {}
                parse_conformant = parsed_json["fusion_compatibility"].get("parse_compatible")
                manually_verified_compatible = parsed_json["fusion_compatibility"].get(
                    "manually_verified_compatible", False
                )
                manually_verified_incompatible = parsed_json["fusion_compatibility"].get(
                    "manually_verified_incompatible", False
                )
                # if parse_conformant is None:
                #     # console.log(f"{parsed_json['id']} does not have parse conformance")
                #     return {}
                # if parse_conformant is not None and parse_conformant is True:
                #     return {}
                #     console.log(f"{parsed_json['id']} is already compatible")
                # elif parse_conformant is not None and parse_conformant is False:
                #     # console.log(f"{parsed_json['id']} is not conformant")
                #     pass
                # else:
                #     error_console.log(f"Something wrong with {parsed_json['id']}")
            return {
                "package_id_from_path": package_id,
                "package_id_with_version": parsed_json.get("id"),
                "package_name_version_json": parsed_json.get("name"),
                "package_version_string": clean_version(parsed_json.get("version")),
                "package_version_require_dbt_version": parsed_json.get("require_dbt_version"),
                "package_version_github_url": github_url,
                "package_version_download_url": tarball_url,
                "parse_compatible": parse_conformant,
                "manually_verified_compatible": manually_verified_compatible,
                "manually_verified_incompatible": manually_verified_incompatible,
            }
    return {}


def read_json_from_local_hub_repo_with_conformance(path: str, file_count_limit: int = 0):
    """Read JSON files from a local copy of the hub repo.

    The `path` argument may be either:
      - the repository root (so files are found under `data/packages/...`),
      - or the `data/packages` directory itself, or
      - a single JSON file path.

    Behavior mirrors `download_package_jsons_from_hub_repo` where possible:
      - JSON files are found recursively
      - each file is parsed and passed to `process_json(file_path, parsed_json)`
      - parsing/IO errors are warned and skipped

    Return a defaultdict mapping package_id -> list[parsed outputs].
    """
    base = Path(path)
    packages: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)

    if not base.exists():
        warnings.warn(f"Path does not exist: {path}")
        return packages

    # Collect JSON files
    json_files: List[Path]
    if base.is_file():
        if base.suffix.lower() == ".json":
            json_files = [base]
        else:
            return packages
    else:
        json_files = sorted(base.rglob("*.json"), key=lambda p: str(p))

    if file_count_limit > 0:
        json_files = json_files[:file_count_limit]

    if not json_files:
        return packages

    for file in json_files:
        try:
            with file.open("r", encoding="utf-8") as fh:
                parsed = json.load(fh)

            # Try to produce a repo-style path like 'data/packages/...'
            file_path: str | Path
            parts = list(file.parts)
            if "data" in parts:
                idx = parts.index("data")
                file_path = Path(*parts[idx:]).as_posix()
            else:
                try:
                    # prefer path relative to provided base
                    rel = file.relative_to(base)
                    file_path = rel.as_posix()
                except Exception:
                    file_path = file.as_posix()

            # If the user passed the `data/packages` directory itself,
            # ensure returned path still starts with 'data/packages'
            if not file_path.startswith("data/packages") and base.name == "packages" and base.parent.name == "data":
                rel = file.relative_to(base)
                file_path = Path("data") / "packages" / rel
                file_path = file_path.as_posix()

            output = process_json_with_conformance(file_path, parsed)
            # console.log(output)
            # console.log(output)
            # if output != {} and "fusion_compatibility" in output:
            #     parse_conformant = output["fusion_compatibility"].get("parse_compatible")
            #     if parse_conformant is None:
            #         pass
            #         # console.log(f"{output['package_id_from_path']} does not have parse conformance")
            #     if parse_conformant is not None and parse_conformant is True:
            #         # console.log(f"{output['package_id_from_path']} is already compatible")
            #         pass
            #     elif parse_conformant is not None and parse_conformant is False:
            #         # console.log(f"{output['package_id_from_path']} is not conformant")
            if output != {}:
                packages[output["package_id_from_path"]].append(output)
        except Exception as exc:
            # warnings.warn(f"Failed to read/parse {file}: {exc}")
            error_console.log(f"Failed to read/parse {file}: {exc.with_traceback}")

    return packages


def create_tarball_from_directory(
    source_dir: Path,
    dest_dir: Path,
    file_name: str,
) -> Optional[Path]:
    """Create a gzipped tarball of a directory and write it to another directory.

    Args:
        source_dir: Path to the directory to archive
        dest_dir: Path to the directory where the tarball should be written
        file_name: Tarball name

    Returns:
        Path to the created tarball, or None if archiving failed
    """
    source_dir = Path(source_dir)
    dest_dir = Path(dest_dir)
    if not source_dir.is_dir():
        error_console.log(f"Source is not a directory: {source_dir}")
        return None
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        tar_path = dest_dir / f"{file_name}.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(source_dir, arcname=source_dir.name)
        return tar_path
    except Exception as e:
        error_console.log(f"Error when creating tarball for {source_dir}: {e}")
        return None


def run_autofix(
    repo_path: Path = Path.cwd(),
    fusion_binary: Optional[str] = None,
    show_fusion_output=True,
) -> Optional[dict[str, list[Any]]]:
    """Run autofix deprecations on a package.

    Args:
        repo_path: Path to the dbt package repository
        fusion_binary: name of a valid Fusion binary
        show_fusion_output: display command output from Fusion

    Returns:
        Dict if run succeeds, None otherwise
    """
    # Add a test profiles.yml to the current directory
    # profiles_path = repo_path / Path("profiles.yml")
    try:
        autofix_result = subprocess.run(
            [
                "uv",
                "tool",
                "run",
                "dbt-autofix@latest",
                "deprecations",
                "--all",
                "--json",
                "--path",
                str(repo_path),
            ],
            check=False,
            text=True,
            capture_output=True,
            timeout=60,
        )
        # console.log(f"autofix stdout: {autofix_result.stdout}")
        # console.log(f"autofix stderr: {autofix_result.stderr}")
        # console.log(f"autofix stdout: {[json.loads(x) for x in autofix_result.stdout.splitlines()]}")
        autofix_stdout = [json.loads(x) for x in autofix_result.stdout.splitlines()]
        autofix_stderr = [x for x in autofix_result.stderr.splitlines()]
        # save autofixed version

        return {
            "autofix_stdout": autofix_stdout,
            "autofix_stderr": autofix_stderr,
        }
        # try:
        #     # Run dbt deps to install package dependencies
        #     if show_fusion_output:
        #         console.log("\n\nRunning dbt deps", style="green")
        #     deps_result = subprocess.run(
        #         [
        #             fusion_binary_name,
        #             "deps",
        #             "--profile",
        #             "test_schema_compat",
        #             "--project-dir",
        #             str(repo_path),
        #             "--log-format",
        #             "otel",
        #         ],
        #         check=False,
        #         text=True,
        #         capture_output=True,
        #         timeout=60,
        #     )
        #     deps_output = parse_log_output(
        #         deps_result.stdout, deps_result.returncode, repo_path, fusion_version=fusion_version
        #     )
        #     if deps_result.returncode != 0:
        #         error_console.log("dbt deps returned errors")
        #         error_console.log(deps_output)

        #     # Now try parse
        #     if show_fusion_output:
        #         console.log("\n\nRunning dbt parse", style="green")
        #     parse_result = subprocess.run(
        #         [
        #             fusion_binary_name,
        #             "parse",
        #             "--profile",
        #             "test_schema_compat",
        #             "--project-dir",
        #             str(repo_path),
        #             "--log-format",
        #             "otel",
        #         ],
        #         check=False,
        #         text=True,
        #         capture_output=True,
        #         timeout=60,
        #     )
        #     parse_output = parse_log_output(
        #         parse_result.stdout, parse_result.returncode, repo_path, fusion_version=fusion_version
        #     )
        #     if parse_result.returncode != 0:
        #         error_console.log("dbt parse returned errors")
        #         error_console.log(parse_output)
        #         if len(deps_output.errors) > 0:
        #             parse_output.errors.extend(deps_output.errors)
        # except Exception as e:
        #     error_console.log(f"{e}: An unknown error occurred when running dbt parse")
        #     return

        # # Return True if exit code is 0 (success)
        # is_compatible = parse_result.returncode == 0

        # if show_fusion_output:
        #     if is_compatible:
        #         console.log(f"Package at {repo_path} is fusion schema compatible")
        #     else:
        #         console.log(f"Package at {repo_path} is not fusion schema compatible")

        # # Clean up deps
        # if show_fusion_output:
        #     console.log("\n\nRunning dbt clean", style="green")
        # subprocess.run(
        #     [
        #         fusion_binary_name,
        #         "clean",
        #         "--profile",
        #         "test_schema_compat",
        #         "--project-dir",
        #         str(repo_path),
        #         "--log-format",
        #         "otel",
        #     ],
        #     check=False,
        #     timeout=60,
        #     text=True,
        #     capture_output=True,
        # )
        # Remove the test profile
        # os.remove(profiles_path)

        # return parse_output
        return ParseConformanceLogOutput()

    except Exception as e:
        error_console.log(f"Error running autofix for {repo_path}: {e!s}")
        # try:
        #     os.remove(profiles_path)
        # except Exception:
        #     pass
        return


def run_autofix_for_version(
    path, package_name, tag_version, package_id, fusion_binary=None
) -> Optional[dict[str, Any]]:
    # result = FusionConformanceResult(version=tag_version, download_failed=False)
    result = {}
    # check require dbt version
    try:
        dbt_project_yml = safe_load((Path(f"{path}/dbt_project.yml")).read_text()) or (
            {},
            {},
        )
    except Exception as e:
        error_console.log(f"dbt_project.yml load failed for {package_id} {tag_version}: {e}")
        return
    # try to add profile to suppress warning about profiles
    if "profile" not in dbt_project_yml[1]:
        # console.log("Adding profile to dbt project")
        try:
            with open(Path(f"{path}/dbt_project.yml"), "a") as f:
                f.write("\nprofile: test_schema_compat\n")
        except Exception as e:
            error_console.log(f"failed when adding profile to dbt_project.yml for {package_id} {tag_version}: {e}")
    # new_version: DbtPackageVersion = DbtPackageVersion(
    #     package_name,
    #     tag_version,
    #     package_id=package_id,
    #     raw_require_dbt_version_range=require_dbt_version_string,
    # )
    # run parse conformance pre autofix
    pre_autofix_parse_conformance = check_fusion_schema_compatibility(
        Path(path), fusion_binary=fusion_binary, show_fusion_output=False
    )
    if pre_autofix_parse_conformance:
        result["pre_autofix_parse_conformance"] = pre_autofix_parse_conformance.to_dict()
    else:
        result["pre_autofix_parse_conformance"] = {}
    # run autofix on version
    autofix_output = run_autofix(Path(path), fusion_binary=fusion_binary, show_fusion_output=True)
    # don't rerun if autofix failed
    if autofix_output is None:
        console.log("Autofix failed")
        return
    result["autofix_output"] = autofix_output
    # otherwise, run parse conformance again
    post_autofix_parse_conformance = check_fusion_schema_compatibility(
        Path(path), fusion_binary=fusion_binary, show_fusion_output=False
    )
    # result["post_autofix_parse_conformance"] = post_autofix_parse_conformance
    if post_autofix_parse_conformance:
        result["post_autofix_parse_conformance"] = post_autofix_parse_conformance.to_dict()
    else:
        result["post_autofix_parse_conformance"] = {}
    # save autofixed version
    tarball_name = f"{'_'.join(package_id.split('/'))}_{tag_version}"
    create_tarball_from_directory(Path(path), DEFAULT_OUTPUT_PATH / "autofixed_versions", tarball_name)
    result["autofixed_version_file_name"] = f"{tarball_name}.tar.gz"
    # result.require_dbt_version_defined = new_version.is_require_dbt_version_defined()
    # if result.require_dbt_version_defined:
    #     result.require_dbt_version_compatible = new_version.is_require_dbt_version_fusion_compatible()
    # result.manually_verified_compatible = new_version.is_explicitly_allowed_on_fusion()
    # result.manually_verified_incompatible = new_version.is_explicitly_disallowed_on_fusion()
    # if post_autofix_parse_conformance:
    #     result.parse_compatible = post_autofix_parse_conformance.parse_exit_code == 0
    #     result.parse_compatibility_result = post_autofix_parse_conformance
    #     if result.parse_compatible:
    #         console.log("Version conformant after autofix")
    #     else:
    #         console.log("Version not conformant after autofix")
    return result


def download_tarball_and_run_autofix(
    package_name: str,
    package_id: str,
    package_version_str: str,
    package_version_download_url: str,
    latest_package_version_download_url: Optional[str],
    fusion_binary: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    with TemporaryDirectory() as tmpdir:
        # download tarball from version json
        tar_path: Optional[Path] = None
        # track exceptions across both checks
        exceptions: list[str] = []
        # use explicitly provided version first
        try:
            # Download the tarball
            response = requests.get(package_version_download_url, stream=True)
            response.raise_for_status()

            # Save to a temporary file
            tar_path = Path(tmpdir) / "archive.tar.gz"
            with open(tar_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        except requests.exceptions.HTTPError as http_error:
            exceptions.append(
                f"{http_error.request.url}: {http_error.response.status_code}, {http_error.response.reason}"
            )
        except Exception as other_error:
            exceptions.append(f"Error when downloading tarball: {other_error}")
        # if that errors or doesn't exist, construct from the latest version
        if not tar_path and latest_package_version_download_url:
            constructed_url: str = construct_download_url_from_latest(
                latest_package_version_download_url, package_version_download_url
            )
            try:
                # Download the tarball
                response = requests.get(constructed_url, stream=True)
                response.raise_for_status()

                # Save to a temporary file
                tar_path = Path(tmpdir) / "archive.tar.gz"
                with open(tar_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            except requests.exceptions.HTTPError as http_error:
                exceptions.append(
                    f"{http_error.request.url}: {http_error.response.status_code}, {http_error.response.reason}"
                )
            except Exception as other_error:
                exceptions.append(f"Error when downloading tarball: {other_error}")
        # if still no download, error
        if not tar_path:
            console.log(f"Could not download {package_name} {package_version_str}")
            for exception in exceptions:
                console.log(exception)
            return {"version": package_version_str, "download_failed": "true"}
            return FusionConformanceResult(version=package_version_str, download_failed=True)

        # if we do have a file, extract the archive
        try:
            extract_dir = Path(tmpdir) / "extracted"
            extract_dir.mkdir(parents=True, exist_ok=True)

            with tarfile.open(tar_path, "r:gz") as tar:
                for entry in tar:
                    if os.path.isabs(entry.name) or ".." in entry.name:
                        raise ValueError("Illegal tar archive entry")
                    tar.extract(entry, extract_dir)

            # Clean up the tar file
            tar_path.unlink()

            # Check that only 1 directory is inside
            tar_contents = os.listdir(extract_dir)
            if len(tar_contents) != 1:
                console.log("Error downloading tar")
            extracted_package = extract_dir / tar_contents[0]
        except Exception as e:
            console.log(f"Error when extracting tarball: {e}")
            return

        # run conformance if possible
        try:
            # console.log(f"Running parse conformance for {package_id} version {package_version_str}")
            conformance_result: Optional[dict[str, Any]] = run_autofix_for_version(
                extracted_package, package_name, package_version_str, package_id, fusion_binary=fusion_binary
            )
            return conformance_result
        except Exception as e:
            console.log(f"Error when running autofix: {e}")
            return


def run_autofix_from_tarballs(
    output: defaultdict[str, list[dict[str, Any]]],
    package_latest_version_urls: dict[str, str],
    package_limit: int = 0,
    fusion_binary=None,
) -> dict[str, dict[str, dict[str, Any]]]:
    results: dict[str, dict[str, dict[str, Any]]] = {}

    for i, package in enumerate(output):
        # temporarily skip fivetran packages
        if package.split("/")[0] != "fivetran" and package.split("/")[0] != "fishtown-analytics":
            continue
        if package_limit > 0 and i > package_limit:
            break
        results[package] = {}
        for version in output[package]:
            package_version_download_url = version.get("package_version_download_url")
            package_version_string = version.get("package_version_string")
            if package_version_string is None:
                continue
            if package_version_download_url is None:
                console.log(f"No download URL found for {package} version {package_version_string}")
                continue
            # console.log(version)
            parse_compatible = version.get("parse_compatible", True)
            if parse_compatible:
                continue
            # else:
            #     console.log(version)
            #     continue
            version_output = {
                "hub_data": version,
                "parse_compatible_hub": version["parse_compatible"],
                "manually_verified_incompatible": version["manually_verified_incompatible"],
                "manually_verified_compatible": version["manually_verified_compatible"],
            }
            console.log(version.get("package_id_from_path"))
            conformance_output: Optional[dict[str, Any]] = download_tarball_and_run_autofix(
                package_name=package,
                package_id=version["package_id_from_path"],
                package_version_str=str(package_version_string),
                package_version_download_url=package_version_download_url,
                latest_package_version_download_url=package_latest_version_urls.get(package),
                fusion_binary=fusion_binary,
            )
            if (
                not conformance_output
                or not conformance_output.get("pre_autofix_parse_conformance")
                or not conformance_output.get("post_autofix_parse_conformance")
                or not conformance_output.get("autofix_output")
            ):
                console.log(f"Could not run autofix for {package} version {package_version_string}\n")
                continue
            else:
                version_output["pre_autofix_parse_conformance"] = conformance_output["pre_autofix_parse_conformance"]
                version_output["autofix_output"] = conformance_output["autofix_output"]
                if conformance_output["autofix_output"] is not None and conformance_output["autofix_output"].get(
                    "autofix_stdout"
                ):
                    version_output["autofix_stdout_count"] = len(
                        conformance_output["autofix_output"].get("autofix_stdout")
                    )
                if conformance_output["autofix_output"] is not None and conformance_output["autofix_output"].get(
                    "autofix_stderr"
                ):
                    version_output["autofix_stderr_count"] = len(
                        conformance_output["autofix_output"].get("autofix_stderr")
                    )
                version_output["post_autofix_parse_conformance"] = conformance_output["post_autofix_parse_conformance"]
                parse_compatible_pre_autofix = (
                    conformance_output["pre_autofix_parse_conformance"]["parse_exit_code"] == 0
                )
                parse_compatible_post_autofix = (
                    conformance_output["post_autofix_parse_conformance"]["parse_exit_code"] == 0
                )
                version_output["parse_compatible_pre_autofix"] = parse_compatible_pre_autofix
                version_output["parse_compatible_post_autofix"] = parse_compatible_post_autofix
                version_output["autofixed_version_file_name"] = conformance_output.get("autofixed_version_file_name")
                if version_output["parse_compatible_hub"] != parse_compatible_pre_autofix:
                    error_console.log(
                        f"Inconsistent parse results for {package} version {package_version_string}: hub {version_output['parse_compatible_hub']}, pre {parse_compatible_pre_autofix}"
                    )
                console.log()
            results[package][package_version_string] = version_output

    return results


def refine_output(original_output: defaultdict[str, list[dict[str, Any]]]) -> defaultdict[str, list[dict[str, Any]]]:
    return original_output
    packages: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for package, versions in original_output.items():
        # console.log(f"package in input: {package}, {v}")
        if len(versions) == 1:
            console.log(f"No versions to process for package {package}")
            continue
        else:
            for version in versions:
                # if "parse_compatible" in version:
                packages[package].append(version)
        # if package in original_output:
        #     console.log(original_output.get("package"))
        # package_version_count = len(input["package"])
    return packages


def write_autofix_output_to_json(
    data: dict[str, dict[str, dict[str, Any]]],
    dest_dir: Path,
    *,
    indent: int = 2,
    sort_keys: bool = True,
):
    # data_output = {}
    # for k, v in data.items():
    #     data_output[k] = {version: result.to_dict() for version, result in v.items()}
    out_file = Path(dest_dir) / "conformance_autofix_output.json"
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=indent, sort_keys=sort_keys, ensure_ascii=False)


@app.command()
def main(
    local_hub_path: Annotated[
        str, typer.Option("--local-hub", help="Fully qualified path to local Package Hub clone")
    ] = str(DEFAULT_HUB_PATH),
    fusion_binary: Annotated[str, typer.Option("--fusion-binary", help="Name of fusion binary")] = str(
        DEFAULT_FUSION_BINARY_PATH
    ),
    output_path: Annotated[
        str, typer.Option("--output-path", help="Fully qualified path to directory for output")
    ] = str(DEFAULT_OUTPUT_PATH),
    package_limit: Annotated[
        int, typer.Option("--limit", help="Only run on first n packages (default = 0 to run all packages)")
    ] = 0,
):
    if output_path:
        output_dir = Path(output_path)
    else:
        output_dir = DEFAULT_OUTPUT_PATH
    console.log(f"Reading from local Hub repo: {local_hub_path}")
    console.log(f"Writing to output path: {output_dir}/conformance_output.json")
    console.log(f"Package limit: {package_limit}")
    console.log(f"Fusion binary: {fusion_binary}")

    original_output: defaultdict[str, list[dict[str, Any]]] = read_json_from_local_hub_repo_with_conformance(
        path=local_hub_path
    )
    # console.log(original_output)
    output = refine_output(original_output)
    # console.log(f"Packages to autofix: {len(output)}")
    # for package in output:
    #     console.log(package)

    package_latest_version_urls: dict[str, str] = get_latest_github_tarball_urls(output)
    autofix_results: dict[str, dict[str, dict[str, Any]]] = run_autofix_from_tarballs(
        output, package_latest_version_urls, package_limit, fusion_binary
    )
    write_autofix_output_to_json(autofix_results, output_path)  # ty: ignore[invalid-argument-type]
    console.log(f"Successfully wrote output to {output_path}/conformance_output.json")


if __name__ == "__main__":
    app()

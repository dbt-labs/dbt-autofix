"""Script to run parse conformance on all package versions in Package Hub.

Run as a CLI: `uv run package-hub-compat`

Reads JSON from a local clone of `hub.getdbt.com` and extracts all package versions,
then runs parse conformance using `check_parse_conformance`.
Writes output to `output/conformance_output.json`, which is used in two other scripts
* `update_package_hub_json`: adds compatibility info back to the original Package Hub files
* `conformance_output`: produces 2 CSV files that summarize compatibility info for further analysis
"""

import json
import os
import re
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import typer
from rich import print
from rich.console import Console
from typing_extensions import Annotated

from dbt_fusion_package_tools.check_parse_conformance import (
    download_tarball_and_run_conformance,
)
from dbt_fusion_package_tools.compatibility import FusionConformanceResult

console = Console()
error_console = Console(stderr=True)

app = typer.Typer()

current_dir = Path.cwd()
DEFAULT_OUTPUT_PATH = current_dir / "src" / "dbt_fusion_package_tools" / "scripts" / "output"
DEFAULT_HUB_PATH = Path.home() / "workplace" / "hub.getdbt.com"
DEFAULT_FUSION_BINARY_PATH = Path.home() / ".local" / "bin" / "dbt"


# Example package index path:
# data/packages/Aaron-Zhou/synapse_statistic/index.json
def is_package_index_file(file_path: str) -> bool:
    file_path_split = file_path.split("/")
    if len(file_path_split) != 5:
        return False
    return file_path_split[-1] == "index.json"


# Example package version path:
# data/packages/Aaron-Zhou/synapse_statistic/versions/v0.1.0.json
def is_package_version_file(file_path: str) -> bool:
    file_path_split = file_path.split("/")
    if len(file_path_split) != 6:
        return False
    return file_path_split[-2] == "versions"


# Example paths that resolve to Aaron-Zhou/synapse_statistic
# data/packages/Aaron-Zhou/synapse_statistic/index.json
# data/packages/Aaron-Zhou/synapse_statistic/versions/v0.1.0.json
def extract_package_id_from_path(file_path: str) -> str:
    file_path_split = file_path.split("/")
    if file_path_split[0] != "data" or file_path_split[1] != "packages" or len(file_path_split) < 4:
        return ""
    return f"{file_path_split[2]}/{file_path_split[3]}"


def clean_version(version_str: Optional[str]) -> str:
    """Remove leading 'v' or 'V' from version strings, if present."""
    if version_str is None:
        return ""
    elif version_str.startswith(("v", "V")):
        return version_str[1:]
    return version_str


# Notes:
# The package_id from path (which is the key in results)
# is the organization + package name
# This is not related to the Github URL
# Example:
# package_id_from_path = AxelThevenot/dbt_star
# package_version_download_url = https://codeload.github.com/AxelThevenot/dbt-star/tar.gz/0.1.0
# Package Hub page: https://hub.getdbt.com/AxelThevenot/dbt_star/latest/
# package name in packages.yml: AxelThevenot/dbt_star
def process_json(file_path: str, parsed_json: Any) -> dict[str, Any]:
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
        if "_source" in parsed_json:
            github_url = parsed_json["_source"].get("url", "")
        else:
            github_url = None
        if "downloads" in parsed_json:
            tarball_url = parsed_json["downloads"].get("tarball")
        else:
            tarball_url = None
        return {
            "package_id_from_path": package_id,
            "package_id_with_version": parsed_json.get("id"),
            "package_name_version_json": parsed_json.get("name"),
            "package_version_string": clean_version(parsed_json.get("version")),
            "package_version_require_dbt_version": parsed_json.get("require_dbt_version"),
            "package_version_github_url": github_url,
            "package_version_download_url": tarball_url,
        }
    else:
        return {}


def write_dict_to_json(data: Dict[str, Any], dest_dir: Path, *, indent: int = 2, sort_keys: bool = True) -> None:
    out_file = dest_dir / "package_output.json"
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=indent, sort_keys=sort_keys, ensure_ascii=False)


def read_json_from_local_hub_repo(path: str, file_count_limit: int = 0):
    """Read JSON files from a local copy of the hub repo and return a
    defaultdict mapping package_id -> list[parsed outputs].

    The `path` argument may be either:
      - the repository root (so files are found under `data/packages/...`),
      - or the `data/packages` directory itself, or
      - a single JSON file path.

    Behavior mirrors `download_package_jsons_from_hub_repo` where possible:
      - JSON files are found recursively
      - each file is parsed and passed to `process_json(file_path, parsed_json)`
      - parsing/IO errors are warned and skipped
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
            file_path: str
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

            output = process_json(file_path, parsed)
            if output != {}:
                packages[output["package_id_from_path"]].append(output)
        except Exception as exc:
            warnings.warn(f"Failed to read/parse {file}: {exc}")

    return packages


def reload_packages_from_file(
    file_path: Path,
) -> defaultdict[str, list[dict[str, Any]]]:
    with file_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def get_github_repos_from_file(file_path: Path) -> defaultdict[str, set[str]]:
    """Extracts the Github repo paths from package hub output.

    Note that the repos are in a set because in rare cases, a package
    may have more than 1 reported repo.

    Args:
        file_path (Path): path to output from read_json_from_local_hub_repo

    Returns:
        defaultdict[str, set[str]]: packages with repos

    Example:
        {
            "package_1":
                {"https://github.com/example/package-1"}
        }
    """
    with file_path.open("r", encoding="utf-8") as fh:
        output = json.load(fh)
    package_repos: defaultdict[str, set[str]] = defaultdict(set)
    for package in output:
        for version in output[package]:
            repo = version.get("package_version_github_url", "")
            m = re.match(r"^(https?://github\.com/[^/]+/[^/]+)", repo)
            if m:
                package_repos[package].add(m.group(1))
    for repos in package_repos:
        repo_count = len(package_repos[repos])
        if repo_count > 1:
            print(repos, package_repos[repos])
    return package_repos


def check_github_url(
    url: str, timeout: int = 10, github_token: Optional[str] = os.getenv("GITHUB_TOKEN")
) -> Dict[str, Any]:
    """Check a GitHub URL and return status info.
    Returns a dict with keys: status (int|None), is_404 (bool), is_301 (bool),
    location (redirect target or None), error (str|None).
    """
    headers = {"User-Agent": "dbt-autofix-agent"}
    if github_token:
        headers["Authorization"] = f"token {github_token}"

    try:
        resp = requests.head(url, headers=headers, timeout=timeout, allow_redirects=False)
        if resp.status_code in (405, 501):  # HEAD not allowed => try GET
            resp = requests.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=False,
                stream=True,
            )
        status = resp.status_code
        location = resp.headers.get("Location")
        return {
            "status": status,
            "is_404": status == 404,
            "is_301": status == 301,
            "location": location,
            "error": None,
        }
    except requests.RequestException as exc:
        return {
            "status": None,
            "is_404": False,
            "is_301": False,
            "location": None,
            "error": str(exc),
        }


def validate_github_urls(packages: defaultdict[str, set[str]], package_limit: int = 0) -> dict[str, str]:
    # Returning a single string here because literally only 1 package has more than 1 valid repo
    # and it looks like a mistake (Saras-Daton/Walmart)
    valid_repos: dict[str, str] = {}
    for i, package in enumerate(packages):
        if package_limit > 0 and i > package_limit:
            break
        for github_url in packages[package]:
            response = check_github_url(github_url)
            if not response:
                error_console.log(f"No response for {package} {github_url}")
            if (response["status"] != 200 and not response["is_301"]) or response.get("error"):
                error_console.log(response)
            if response["is_404"]:
                continue
            elif response["is_301"]:
                valid_repos[package] = response["location"]
            else:
                valid_repos[package] = github_url
    return valid_repos


def follow_redirects(package_name: str, packages: dict[str, dict[str, Optional[str]]]) -> str:
    package_redirect_name: Optional[str] = packages[package_name].get("package_redirect_name")
    package_redirect_namespace: Optional[str] = packages[package_name].get("package_redirect_namespace")
    # base case: package does not have any redirects
    if not package_redirect_name and not package_redirect_namespace:
        return package_name
    # recursive case: follow redirect
    original_namespace = package_name.split("/")[0]
    original_name = package_name.split("/")[1]
    if package_redirect_name and package_redirect_namespace:
        package_after_redirect: str = f"{package_redirect_namespace}/{package_redirect_name}"
    elif package_redirect_name:
        package_after_redirect: str = f"{original_namespace}/{package_redirect_name}"
    else:
        package_after_redirect: str = f"{package_redirect_namespace}/{original_name}"
    next_redirect_name = follow_redirects(package_after_redirect, packages)
    return next_redirect_name


def get_latest_github_tarball_urls(hub_data: defaultdict[str, list[dict[str, Any]]]) -> dict[str, str]:
    # first load in all packages and get latest version + redirects
    # but don't actually follow the redirects yet
    packages_no_redirects: dict[str, dict[str, Optional[str]]] = {}
    for package in hub_data:
        # index file should always be first
        package_latest_version: str = hub_data[package][0]["package_latest_version_index_json"]
        package_redirect_name: Optional[str] = hub_data[package][0].get("package_redirect_name")
        package_redirect_namespace: Optional[str] = hub_data[package][0].get("package_redirect_namespace")
        package_latest_version_download_url: Optional[str] = None

        for version in hub_data[package][1:]:
            package_version_string = version.get("package_version_string")

            # get the tarball url for the latest version only
            if package_version_string == package_latest_version:
                package_latest_version_download_url = version.get("package_version_download_url")
                break

        if not package_latest_version_download_url:
            console.log(f"No download available for {package}")
            continue
        else:
            packages_no_redirects[package] = {
                "package_latest_version": package_latest_version,
                "package_redirect_name": package_redirect_name,
                "package_redirect_namespace": package_redirect_namespace,
                "package_latest_version_download_url": package_latest_version_download_url,
            }

    # get final latest version url after following redirect
    package_latest_version_urls: dict[str, str] = {}
    for package in packages_no_redirects:
        package_latest_name: str = follow_redirects(package, packages_no_redirects)
        package_latest_url: Optional[str] = packages_no_redirects[package_latest_name].get(
            "package_latest_version_download_url"
        )
        if package_latest_url is not None:
            package_latest_version_urls[package] = package_latest_url
    return package_latest_version_urls


def run_conformance_from_tarballs(
    output: defaultdict[str, list[dict[str, Any]]],
    package_latest_version_urls: dict[str, str],
    package_limit: int = 0,
    fusion_binary=None,
) -> dict[str, dict[str, FusionConformanceResult]]:
    results: dict[str, dict[str, FusionConformanceResult]] = {}

    for i, package in enumerate(output):
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
            conformance_output = download_tarball_and_run_conformance(
                package_name=package,
                package_id=version["package_id_from_path"],
                package_version_str=str(package_version_string),
                package_version_download_url=package_version_download_url,
                latest_package_version_download_url=package_latest_version_urls.get(package),
                fusion_binary=fusion_binary,
            )
            if not conformance_output:
                console.log(f"Could not run conformance for {package} version {package_version_string}\n")
                continue
            else:
                results[package][package_version_string] = conformance_output
                console.log()

    return results


def write_conformance_output_to_json(
    data: dict[str, dict[str, FusionConformanceResult]],
    dest_dir: Path,
    *,
    indent: int = 2,
    sort_keys: bool = True,
):
    data_output = {}
    for k, v in data.items():
        data_output[k] = {version: result.to_dict() for version, result in v.items()}
    out_file = Path(dest_dir) / "conformance_output.json"
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data_output, fh, indent=indent, sort_keys=sort_keys, ensure_ascii=False)


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

    output: defaultdict[str, list[dict[str, Any]]] = read_json_from_local_hub_repo(path=local_hub_path)
    package_latest_version_urls: dict[str, str] = get_latest_github_tarball_urls(output)
    parse_conformance_results = run_conformance_from_tarballs(
        output, package_latest_version_urls, package_limit, fusion_binary
    )
    write_conformance_output_to_json(parse_conformance_results, output_path)
    console.log(f"Successfully wrote output to {output_path}/conformance_output.json")


if __name__ == "__main__":
    app()

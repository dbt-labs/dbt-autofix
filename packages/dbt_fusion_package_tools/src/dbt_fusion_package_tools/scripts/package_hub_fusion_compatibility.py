import json
import os
import re
import time
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional
from dbt_fusion_package_tools.check_parse_conformance import (
    checkout_repo_and_run_conformance,
)
from dbt_fusion_package_tools.compatibility import FusionConformanceResult

import requests
from requests import HTTPError


def _http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Any:
    try:
        resp = requests.get(url, headers=headers or {}, timeout=timeout)
        resp.raise_for_status()
        # requests already decodes JSON when using .json(), but in case
        # the content is not JSON, fall back to decoding manually.
        try:
            return resp.json()
        except ValueError:
            return json.loads(resp.text)
    except HTTPError:
        # re-raise HTTP errors to be handled by callers
        raise
    except requests.RequestException as exc:
        # Convert other request exceptions to a RuntimeError for clarity
        raise RuntimeError(f"Network error when fetching {url}: {exc}")


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


def download_package_jsons_from_hub_repo(
    owner: str = "dbt-labs",
    repo: str = "hub.getdbt.com",
    path: str = "data/packages",
    branch: Optional[str] = "master",
    github_token: Optional[str] = None,
    file_count_limit: int = 0,
    # ) -> List[PackageJSON]:
) -> defaultdict[str, list[dict[str, Any]]]:
    """Download and parse all JSON files under `data/packages` in a GitHub repo.

    This function performs the following steps:
    - Discover the repository's default branch (if `branch` is not provided).
    - Fetch the git tree for the branch recursively and find all files under
      ``{path}`` that end with ``.json``.
    - Download each JSON file via the raw.githubusercontent.com URL and parse
      it into Python objects.

    Returns:
        A list of parsed JSON objects typed as ``PackageJSON``.

    Args:
        owner: GitHub repo owner (default: "dbt-labs").
        repo: GitHub repository name (default: "hub.getdbt.com").
        path: Path within the repo to search (default: "data/packages").
        branch: Branch name to use; if omitted the repository default branch is
            discovered via the GitHub API.
        github_token: Optional GitHub token to increase rate limits.
    """
    base_api = "https://api.github.com"
    headers: Dict[str, str] = {"User-Agent": "dbt-autofix-agent"}
    if github_token:
        headers["Authorization"] = f"token {github_token}"

    # 1) Find default branch if not provided
    if not branch:
        repo_url = f"{base_api}/repos/{owner}/{repo}"
        try:
            repo_info = _http_get_json(repo_url, headers=headers)
            branch = repo_info.get("default_branch")
        except Exception as exc:  # pragma: no cover - network error handling
            raise RuntimeError(f"Failed to get repo info for {owner}/{repo}: {exc}")
        if not branch:
            raise RuntimeError("Could not determine repository default branch")

    # 2) Get the git tree recursively
    tree_url = f"{base_api}/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
    try:
        tree_json = _http_get_json(tree_url, headers=headers)
    except Exception as exc:  # pragma: no cover - network error handling
        raise RuntimeError(f"Failed to fetch git tree for {owner}/{repo}@{branch}: {exc}")

    if "tree" not in tree_json:
        raise RuntimeError("Unexpected response from GitHub API when fetching tree")

    files: List[Dict[str, Any]] = []
    prefix = path.rstrip("/") + "/"
    for entry in tree_json["tree"]:
        # entry has keys: path, mode, type (blob/tree), sha, url
        if entry.get("type") != "blob":
            continue
        p = entry.get("path", "")
        if p.startswith(prefix) and p.endswith(".json"):
            files.append(entry)
        if file_count_limit > 0 and len(files) >= file_count_limit:
            break

    packages: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    if not files:
        # No files found; return empty list rather than error.
        return packages

    packages: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    # 3) Download each JSON using raw.githubusercontent.com
    for entry in files:
        file_path = entry["path"]
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path}"
        # Use simple GET; raw.githubusercontent does not require auth for public repos.
        try:
            parsed = _http_get_json(raw_url, headers={"User-Agent": headers["User-Agent"]})
            output = process_json(file_path, parsed)
            if output != {}:
                packages[output["package_id_from_path"]].append(output)
            time.sleep(1)
        except Exception as exc:  # network/file parsing issues
            warnings.warn(f"Failed to fetch/parse {file_path}: {exc}")
            time.sleep(5)

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
        # print(repos)
        repo_count = len(package_repos[repos])
        if repo_count > 1:
            print(repos, package_repos[repos])
    return package_repos


def check_github_url(
    url: str, timeout: int = 10, github_token: Optional[str] = os.getenv("GITHUB_TOKEN")
) -> Dict[str, Any]:
    """
    Check a GitHub URL and return status info.
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
                print(f"No response for {package} {github_url}")
            if response["status"] != 200 or response.get("error"):
                print(response)
            if response["is_404"]:
                continue
            elif response["is_301"]:
                valid_repos[package] = response["location"]
            else:
                valid_repos[package] = github_url
    return valid_repos


def run_conformance(file_path: Path, package_limit: int = 0) -> dict[str, dict[str, FusionConformanceResult]]:
    output: defaultdict[str, list[dict[str, Any]]] = read_json_from_local_hub_repo(
        path="/Users/chaya/workplace/hub.getdbt.com"
    )
    results: dict[str, dict[str, FusionConformanceResult]] = {}
    github_repos: defaultdict[str, set[str]] = get_github_repos_from_file(file_path)
    github_urls: dict[str, str] = validate_github_urls(github_repos, package_limit)
    for i, package in enumerate(output):
        if package_limit > 0 and i > package_limit:
            break
        if package not in github_urls:
            continue
        results[package] = {}
        package_github_url = github_urls[package]
        m = re.match(r"^(https?://github\.com/[^/]+/[^/]+)", package_github_url)
        if m:
            parsed_url = (m.group(1)).split("/")
            package_github_org = parsed_url[-2]
            package_github_repo = parsed_url[-1]
            package_name = output[package][0]["package_name_index_json"]
            results[package] = checkout_repo_and_run_conformance(package_github_org, package_github_repo, package_name)

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
    out_file = dest_dir / "conformance_output.json"
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data_output, fh, indent=indent, sort_keys=sort_keys, ensure_ascii=False)


def main():
    package_limit = 0
    output_path: Path = Path.cwd() / "src" / "dbt_fusion_package_tools" / "scripts" / "output"
    parse_conformance_results = run_conformance(output_path / "package_output.json", package_limit)
    write_conformance_output_to_json(parse_conformance_results, output_path)


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import warnings

import requests
from requests import HTTPError


@dataclass
class PackageJSON:
    """Dataclass wrapper for parsed package JSON content.

    The content of each package JSON file can vary; we keep a single `data`
    field containing the parsed JSON as a dictionary (or other JSON value).
    """

    data: Any


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


def download_package_jsons_from_hub_repo(
    owner: str = "dbt-labs",
    repo: str = "hub.getdbt.com",
    path: str = "data/packages",
    branch: Optional[str] = None,
    github_token: Optional[str] = None,
) -> List[PackageJSON]:
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

    results: List[PackageJSON] = []
    if not files:
        # No files found; return empty list rather than error.
        return results

    # 3) Download each JSON using raw.githubusercontent.com
    for entry in files:
        file_path = entry["path"]
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path}"
        # Use simple GET; raw.githubusercontent does not require auth for public repos.
        try:
            parsed = _http_get_json(raw_url, headers={"User-Agent": headers["User-Agent"]})
            results.append(PackageJSON(parsed))
        except Exception as exc:  # pragma: no cover - network/file parsing issues
            warnings.warn(f"Failed to fetch/parse {file_path}: {exc}")

    return results


__all__ = ["download_package_jsons_from_hub_repo", "PackageJSON"]

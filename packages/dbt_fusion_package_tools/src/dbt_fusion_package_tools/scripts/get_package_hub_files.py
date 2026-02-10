import json
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

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
        if "fusion_compatibility" in parsed_json:
            fusion_compatibility = {
                "manually_verified_compatible": parsed_json["fusion_compatibility"].get("manually_verified_compatible"),
                "manually_verified_incompatible": parsed_json["fusion_compatibility"].get(
                    "manually_verified_incompatible"
                ),
                "require_dbt_version_defined": parsed_json["fusion_compatibility"].get("require_dbt_version_defined"),
                "require_dbt_version_compatible": parsed_json["fusion_compatibility"].get(
                    "require_dbt_version_compatible"
                ),
                "parse_compatible": parsed_json["fusion_compatibility"].get("parse_compatible"),
                "download_failed": parsed_json["fusion_compatibility"].get("download_failed"),
            }
            return {
                "package_id_from_path": package_id,
                "package_id_with_version": parsed_json.get("id"),
                "package_name_version_json": parsed_json.get("name"),
                "package_version_string": clean_version(parsed_json.get("version")),
                "package_version_require_dbt_version": parsed_json.get("require_dbt_version"),
                "package_version_github_url": github_url,
                "package_version_download_url": tarball_url,
                "fusion_compatibility": fusion_compatibility,
            }
        else:
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


def main():
    file_count_limit = 0
    # assumes hub repo and dbt-autofix repo are cloned in the same directory
    results = read_json_from_local_hub_repo(path="../../../hub.getdbt.com", file_count_limit=file_count_limit)
    print(f"Downloaded {len(results)} packages from hub.getdbt.com")
    output_path: Path = Path.cwd() / "src" / "dbt_fusion_package_tools" / "scripts" / "output"
    write_dict_to_json(results, output_path)
    print(f"Output written to {output_path / 'package_output.json'}")
    reload_packages = reload_packages_from_file(output_path / "package_output.json")
    print(f"Reloaded {len(reload_packages)} packages from file")


if __name__ == "__main__":
    main()

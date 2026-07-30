"""Centralizes functions used in multiple scripts."""

import json
import subprocess
import tarfile
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any, List, Optional

from rich.console import Console

# from dbt_fusion_package_tools.dbt_package_version import DbtPackageVersion
from dbt_fusion_package_tools.scripts.package_hub_fusion_compatibility import (
    clean_version,
    extract_package_id_from_path,
    is_package_index_file,
    is_package_version_file,
)

console = Console()
error_console = Console(stderr=True)


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

            if output != {}:
                packages[output["package_id_from_path"]].append(output)
        except Exception as exc:
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
) -> Optional[dict[str, list[Any]]]:
    """Run autofix deprecations on a package.

    Args:
        repo_path: Path to the dbt package repository
        fusion_binary: name of a valid Fusion binary
        show_fusion_output: display command output from Fusion

    Returns:
        Dict if run succeeds, None otherwise
    """
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
        autofix_stdout = [json.loads(x) for x in autofix_result.stdout.splitlines()]
        autofix_stderr = [x for x in autofix_result.stderr.splitlines()]

        return {
            "autofix_stdout": autofix_stdout,
            "autofix_stderr": autofix_stderr,
        }

    except Exception as e:
        error_console.log(f"Error running autofix for {repo_path}: {e!s}")
        return

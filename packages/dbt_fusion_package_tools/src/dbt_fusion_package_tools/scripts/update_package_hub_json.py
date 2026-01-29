"""Script to add parse conformance data to Package Hub.

Run as a CLI: `uv run update-package-hub`

Reads the `conformance_output.json` output from `package_hub_fusion_compatibility`.
Adds or updates the Fusion compatibility info for package versions in a local Hub repo.
"""

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from typing_extensions import Annotated

from dbt_fusion_package_tools.compatibility import FusionConformanceResult
from dbt_fusion_package_tools.exceptions import SemverError
from dbt_fusion_package_tools.version_utils import VersionSpecifier

console = Console()
error_console = Console(stderr=True)

app = typer.Typer()

current_dir = Path.cwd()
DEFAULT_OUTPUT_PATH = current_dir / "src" / "dbt_fusion_package_tools" / "scripts" / "output"
DEFAULT_HUB_PATH = Path.home() / "workplace" / "hub.getdbt.com"
DEFAULT_FUSION_BINARY_PATH = Path.home() / ".local" / "bin" / "dbt"
DEFAULT_CONFORMANCE_OUTPUT_FILE = (
    current_dir / "src" / "dbt_fusion_package_tools" / "scripts" / "output" / "conformance_output.json"
)


def reload_output_from_file(
    file_path: Path,
) -> defaultdict[str, dict[str, Any]]:
    with file_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def extract_output_from_json(
    data: defaultdict[str, dict[str, Any]],
) -> dict[str, dict[str, FusionConformanceResult]]:
    output: dict[str, dict[str, FusionConformanceResult]] = {}
    for package in data:
        output[package] = {}
        for version_id in data[package]:
            version_data: dict[str, Any] = data[package][version_id]
            version_data_conformance_result = FusionConformanceResult.from_dict(version_data)
            output[package][version_id] = version_data_conformance_result
    return output


def check_for_rename(hub_path: str, package_name: str) -> VersionSpecifier:
    dir_path = Path(hub_path) / "data" / "packages" / package_name
    file_path = dir_path / "index.json"
    with file_path.open("r", encoding="utf-8") as fh:
        index_json = json.load(fh)
    # if "redirectname" in index_json or "redirectnamespace" in index_json:
    #     print(f"package {package_name} renamed after version {index_json['latest']}")
    return VersionSpecifier.from_version_string(index_json["latest"])


def find_package_hub_file(hub_path: str, package_name: str, version: str) -> Path:
    dir_path = Path(hub_path) / "data" / "packages" / package_name / "versions"
    if (dir_path / f"{version}.json").is_file():
        file_path = dir_path / f"{version}.json"
    else:
        file_path = dir_path / f"v{version}.json"
    return file_path


def get_json_from_package_hub_file(file_path: Path, package_name: str, version: str) -> dict[str, Any]:
    try:
        with file_path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        error_console.log(f"No package hub output found for {package_name} version {version}")
        return {}


def update_hub_json(
    original_json: dict[str, Any],
    conformance_output: FusionConformanceResult,
    fusion_version: str,
) -> dict[str, Any]:
    # this is done to ensure that the ordering matches the original so the git diff is minimized
    updated_json = {
        "id": original_json["id"],
        "name": original_json["name"],
        "version": original_json["version"],
        "published_at": original_json["published_at"],
        "packages": original_json["packages"],
    }
    if "require_dbt_version" in original_json:
        updated_json["require_dbt_version"] = original_json["require_dbt_version"]
    updated_json["works_with"] = original_json["works_with"]
    updated_json["_source"] = original_json["_source"]
    updated_json["downloads"] = original_json["downloads"]

    new_conformance = conformance_output.to_dict()
    if "fusion_compatibility" in original_json:
        manually_verified_compatible = original_json["fusion_compatibility"].get("manually_verified_compatible")
        manually_verified_incompatible = original_json["fusion_compatibility"].get("manually_verified_incompatible")
        if manually_verified_compatible:
            new_conformance["manually_verified_compatible"] = manually_verified_compatible
        if manually_verified_incompatible:
            new_conformance["manually_verified_incompatible"] = manually_verified_incompatible
    updated_json["fusion_compatibility"] = new_conformance
    return updated_json


def write_dict_to_json(data: dict[str, Any], dest_path: Path, *, indent: int = 2, sort_keys: bool = True) -> None:
    out_file = dest_path
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=indent, sort_keys=False, ensure_ascii=False)


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
    fusion_version: Annotated[str, typer.Option("--fusion-version", help="Version of Fusion used for testing")] = "89",
    conformance_output_path: Annotated[
        str, typer.Option("--conformance-output", help="Path to conformance output")
    ] = str(DEFAULT_CONFORMANCE_OUTPUT_FILE),
):
    file_path: Path = Path(conformance_output_path)
    console.log(f"Writing to local Hub repo: {local_hub_path}")
    console.log(f"Reading from output path: {conformance_output_path}")
    console.log(f"Package limit: {str(package_limit) if package_limit > 0 else 'none'}")
    console.log(f"Fusion version: 2.0.0-preview.{fusion_version}")

    conformance_output = reload_output_from_file(file_path)
    conformance_data = extract_output_from_json(conformance_output)
    package_count = 0
    success_count = 0
    error_count = 0
    i = 0
    for package_name in conformance_data:
        if package_limit > 0 and i > package_limit:
            break
        console.log(f"Updating package {package_name} with {len(conformance_data[package_name])} versions")
        latest_version = check_for_rename(local_hub_path, package_name)
        package_count += 1
        for version in conformance_data[package_name]:
            try:
                version_spec = VersionSpecifier.from_version_string(version)
                if version_spec > latest_version:
                    continue
            except SemverError:
                error_console.log(f"Can't parse version spec {version} for package {package_name}")
                error_count += 1
                continue
            version_file_path = find_package_hub_file(local_hub_path, package_name, version)
            package_hub_json = get_json_from_package_hub_file(version_file_path, package_name, version)
            if package_hub_json == {}:
                error_count += 1
                continue
            updated_json = update_hub_json(
                package_hub_json,
                conformance_data[package_name][version],
                f"2.0.0-preview.{fusion_version}",
            )
            write_dict_to_json(updated_json, version_file_path, indent=4)
            success_count += 1
        i += 1
    console.log("Package Hub update complete", style="green")
    console.log(f"Packages processed: {package_count}")
    console.log(f"Success count: {success_count}")
    console.log(f"Error count: {error_count}")


if __name__ == "__main__":
    app()

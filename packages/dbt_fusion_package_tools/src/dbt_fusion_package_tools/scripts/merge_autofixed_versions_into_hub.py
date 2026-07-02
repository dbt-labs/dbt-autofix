
import json
from pathlib import Path
from typing import Any, Optional
from typing_extensions import Annotated
from rich.console import Console
import typer
from dbt_fusion_package_tools.scripts.update_package_hub_json import DEFAULT_FUSION_BINARY_PATH, DEFAULT_HUB_PATH, DEFAULT_CONFORMANCE_OUTPUT_FILE, DEFAULT_OUTPUT_PATH

console = Console()
error_console = Console(stderr=True)

app = typer.Typer()

def merge_autofix_output(file_names: list[str], path: Path) -> dict[str, dict[str, dict[str, Any]]]:
    output: dict[str, dict[str, dict[str, Any]]] = {}

    for file in file_names:
        try:
            with (path / file).open("r", encoding="utf-8") as fh:
                parsed = json.load(fh)
                # console.log(parsed)
            if isinstance(parsed, dict) and parsed != {}:
                console.log(f"{len(parsed)} items found in {file}")
                for k, v in parsed.items():
                    output[k] = v
        except Exception as exc:
            # warnings.warn(f"Failed to read/parse {file}: {exc}")
            error_console.log(f"Failed to read/parse {file}: {exc.with_traceback}")

    return output

def get_json_from_package_hub_file(file_path: Path, package_name: str, version: str) -> dict[str, Any]:
    try:
        with file_path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        error_console.log(f"No package hub output found for {package_name} version {version} at {file_path}")
        return {}

def update_hub_json(
    autofix_output: dict[str, Any],
    package_name: str,
    package_version: str,
) -> Optional[dict[str, Any]]:
    # get hub json
    if "hub_data" not in autofix_output:
        error_console.log(f"Hub data missing for {id}")
        return
    package_id_with_version = (autofix_output["hub_data"]["package_id_with_version"]).split("/")
    original_json = get_json_from_package_hub_file(DEFAULT_HUB_PATH / "data" / "packages" / package_id_with_version[0] / package_id_with_version[1] / "versions" / f"{package_id_with_version[2]}.json", package_name, package_version)
    # skip if fusion compatibility not in original
    if "fusion_compatibility" not in original_json:
        error_console.log(f"Fusion compatibility missing for {id}")
        return
    
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
    updated_json["fusion_compatibility"] = original_json["fusion_compatibility"]
    if "fusion_compatible_download" in original_json["fusion_compatibility"] and "tarball" in original_json["fusion_compatibility"]["fusion_compatible_download"]:
        console.log(f"{package_id_with_version} already has fusion-compatible download: {original_json['fusion_compatibility']['fusion_compatible_download']['tarball']}")
        pass
    else:
        v2_compatible_file_name: str = autofix_output["autofixed_version_file_name"]
        v2_compatible_file_path: Path = DEFAULT_HUB_PATH / "source" / "v2_compatible_versions" / v2_compatible_file_name
        if not v2_compatible_file_path.exists():
            error_console.log(f"v2-compatible file not found at {v2_compatible_file_path}, skipping")
            return
        v2_compatible_download_url = f"https://hub.getdbt.com/v2_compatible_versions/{v2_compatible_file_name}"
        updated_json["fusion_compatibility"]["fusion_compatible_download"] = {
            "tarball": v2_compatible_download_url
        }
    
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

    conformance_output: dict[str, dict[str, dict[str, Any]]] = merge_autofix_output(["conformance_autofix_output_fivetran_fishtown.json", "conformance_autofix_output_no_fivetran_fishtown_20260623.json"], DEFAULT_OUTPUT_PATH)
    console.log(f"conformance_output: {len(conformance_output)}")
    for package in conformance_output:
        for version in conformance_output[package]:
            updated_json = update_hub_json(conformance_output[package][version], package, version)
            if updated_json is None:
                error_console.log(f"Unable to update hub json for {package}/{version}")
            else:
                console.log(f"{package}/{version}")
                console.log(f"updated json: {updated_json['fusion_compatibility']['fusion_compatible_download']}")
    
    # conformance_output = reload_output_from_file(file_path)
    # conformance_data = extract_output_from_json(conformance_output)
    # package_count = 0
    # success_count = 0
    # error_count = 0
    # i = 0
    # for package_name in conformance_data:
    #     if package_limit > 0 and i > package_limit:
    #         break
    #     console.log(f"Updating package {package_name} with {len(conformance_data[package_name])} versions")
    #     latest_version = check_for_rename(local_hub_path, package_name)
    #     package_count += 1
    #     for version in conformance_data[package_name]:
    #         try:
    #             version_spec = VersionSpecifier.from_version_string(version)
    #             if version_spec > latest_version:
    #                 continue
    #         except SemverError:
    #             error_console.log(f"Can't parse version spec {version} for package {package_name}")
    #             error_count += 1
    #             continue
    #         version_file_path = find_package_hub_file(local_hub_path, package_name, version)
    #         package_hub_json = get_json_from_package_hub_file(version_file_path, package_name, version)
    #         if package_hub_json == {}:
    #             error_count += 1
    #             continue
    #         updated_json = update_hub_json(
    #             package_hub_json,
    #             conformance_data[package_name][version],
    #             f"2.0.0-preview.{fusion_version}",
    #         )
    #         write_dict_to_json(updated_json, version_file_path, indent=4)
    #         success_count += 1
    #     i += 1
    # console.log("Package Hub update complete", style="green")
    # console.log(f"Packages processed: {package_count}")
    # console.log(f"Success count: {success_count}")
    # console.log(f"Error count: {error_count}")


if __name__ == "__main__":
    app()

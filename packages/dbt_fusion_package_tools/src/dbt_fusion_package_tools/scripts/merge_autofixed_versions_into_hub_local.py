import json
from pathlib import Path
from typing import Any, Optional

import typer
from rich.console import Console
from typing_extensions import Annotated

from dbt_fusion_package_tools.scripts.constants import DEFAULT_FUSION_BINARY_PATH, DEFAULT_HUB_PATH, DEFAULT_OUTPUT_PATH
from dbt_fusion_package_tools.scripts.update_package_hub_json import (
    DEFAULT_CONFORMANCE_OUTPUT_FILE,
)

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
    original_json = get_json_from_package_hub_file(
        DEFAULT_HUB_PATH
        / "data"
        / "packages"
        / package_id_with_version[0]
        / package_id_with_version[1]
        / "versions"
        / f"{package_id_with_version[2]}.json",
        package_name,
        package_version,
    )
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
    if (
        "fusion_compatible_download" in original_json["fusion_compatibility"]
        and "tarball" in original_json["fusion_compatibility"]["fusion_compatible_download"]
    ):
        console.log(
            f"{package_id_with_version} already has fusion-compatible download: {original_json['fusion_compatibility']['fusion_compatible_download']['tarball']}"
        )
        pass
    else:
        v2_compatible_file_name: str = autofix_output["autofixed_version_file_name"]
        # v2_compatible_file_path: Path = DEFAULT_HUB_PATH / "source" / "v2_compatible_versions" / v2_compatible_file_name
        # if not v2_compatible_file_path.exists():
        #     error_console.log(f"v2-compatible file not found at {v2_compatible_file_path}, skipping")
        #     return
        v2_compatible_download_url = f"https://public.cdn.getdbt.com/package-hub/{v2_compatible_file_name}"
        updated_json["fusion_compatibility"]["fusion_compatible_download"] = {
            "tarball": v2_compatible_download_url,
            "format": "tgz",
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
    # file_path: Path = Path(conformance_output_path)
    console.log(f"Writing to local Hub repo: {local_hub_path}")
    console.log(f"Reading from output path: {conformance_output_path}")
    console.log(f"Package limit: {str(package_limit) if package_limit > 0 else 'none'}")
    console.log(f"Fusion version: 2.0.0-preview.{fusion_version}")

    conformance_output: dict[str, dict[str, dict[str, Any]]] = merge_autofix_output(
        [
            "conformance_output_after_autofix_local.json",
        ],
        DEFAULT_OUTPUT_PATH,
    )
    console.log(f"conformance_output: {len(conformance_output)}")
    # only upload subset
    orgs = set(
        [
            "brooklyn-data",
            "calogica",
            "dbt-labs",
            "elementary-data",
            "fishtown-analytics",
            "fivetran",
            "godatadriven",
            "metaplane",
            "tnightengale",
        ]
    )
    for package, versions in conformance_output.items():
        package_org = package.split("/")[0]
        # console.log(package_org, package)
        if package_org not in orgs:
            continue
        # console.log(package_org, package)
        for version, version_output in versions.items():
            # console.log(package_org, package, version, version_output.get("parse_compatible_hub", False), version_output.get(
            #     "parse_compatible_post_autofix", False
            # ))
            # only update versions that weren't parse compatible but are now
            parse_compatible_before_autofix = version_output.get("parse_compatible_hub", False)
            manually_verified_incompatible = version_output.get("manually_verified_incompatible", False)
            autofix_stdout = version_output.get("autofix_output", {}).get("autofix_stdout", [])
            autofix_made_changes = len(autofix_stdout) > 1 and len(autofix_stdout[0].get("refactors", [])) > 1
            parse_compatible_after_autofix = version_output.get("parse_compatible_after_autofix", False)
            if parse_compatible_after_autofix and (
                not parse_compatible_before_autofix or manually_verified_incompatible or autofix_made_changes
            ):
                console.log(package_org, package, version)
                updated_json = update_hub_json(version_output, package, version)
                if updated_json is None:
                    error_console.log(f"Unable to update hub json for {package}/{version}")
                else:
                    console.log(f"{package}/{version}")
                    console.log(f"updated json: {updated_json['fusion_compatibility']['fusion_compatible_download']}")
                    package_id_with_version = updated_json["id"].split("/")
                    version_file_path = (
                        DEFAULT_HUB_PATH
                        / "data"
                        / "packages"
                        / package_id_with_version[0]
                        / package_id_with_version[1]
                        / "versions"
                        / f"{package_id_with_version[2]}.json"
                    )
                    write_dict_to_json(updated_json, version_file_path, indent=4)


if __name__ == "__main__":
    app()

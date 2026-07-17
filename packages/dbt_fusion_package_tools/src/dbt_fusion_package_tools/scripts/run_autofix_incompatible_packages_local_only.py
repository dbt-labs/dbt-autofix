"""Runs autofix on incompatible packages to see if that fixes parse errors."""

import json
import os
import tarfile
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Optional

import typer
from rich.console import Console
from typing_extensions import Annotated

from dbt_fusion_package_tools.scripts.constants import (
    DEFAULT_AUTOFIXED_TARBALL_PATH,
    DEFAULT_HUB_PATH,
    DEFAULT_OUTPUT_PATH,
)
from dbt_fusion_package_tools.scripts.download_package_version_tarballs import (
    DEFAULT_DOWNLOAD_PATH,
    DownloadedTarballOutput,
)
from dbt_fusion_package_tools.scripts.helpers import (
    create_tarball_from_directory,
    read_json_from_local_hub_repo_with_conformance,
    run_autofix,
)

console = Console()
error_console = Console(stderr=True)

app = typer.Typer()


def reload_package_tarball_output_from_file(
    file_path: Path,
) -> defaultdict[str, dict[str, DownloadedTarballOutput]]:
    with file_path.open("r", encoding="utf-8") as fh:
        file_output = json.load(fh)
    output: defaultdict[str, dict[str, DownloadedTarballOutput]] = defaultdict(dict)
    for package, version in file_output.items():
        for version_str, tarball_output in version.items():
            output[package][version_str] = tarball_output
    return output


def run_autofix_for_version(
    path, package_name, tag_version, package_id, fusion_binary=None
) -> Optional[dict[str, Any]]:
    result = {}

    # run autofix on version
    autofix_output = run_autofix(Path(path))
    # don't rerun if autofix failed
    if autofix_output is None:
        console.log("Autofix failed")
        return
    result["autofix_output"] = autofix_output

    # save autofixed version
    tarball_name = f"{'_'.join(package_id.split('/'))}_{tag_version}"
    create_tarball_from_directory(Path(path), DEFAULT_AUTOFIXED_TARBALL_PATH, tarball_name)
    result["autofixed_version_file_name"] = f"{tarball_name}.tar.gz"

    return result


def extract_tarball_and_run_autofix(
    package_name: str, package_id: str, package_version_str: str, tar_path: Path
) -> Optional[dict[str, Any]]:
    with TemporaryDirectory() as tmpdir:
        try:
            extract_dir = Path(tmpdir) / "extracted"
            extract_dir.mkdir(parents=True, exist_ok=True)

            with tarfile.open(tar_path, "r:gz") as tar:
                for entry in tar:
                    if os.path.isabs(entry.name) or ".." in entry.name:
                        raise ValueError("Illegal tar archive entry")
                    tar.extract(entry, extract_dir)

            # Clean up the tar file
            # tar_path.unlink()

            # Check that only 1 directory is inside
            tar_contents = os.listdir(extract_dir)
            if len(tar_contents) != 1:
                console.log("Error downloading tar")
            extracted_package = extract_dir / tar_contents[0]
        except Exception as e:
            console.log(f"Error when extracting tarball: {e}")
            return

        # run autofix if possible
        try:
            conformance_result: Optional[dict[str, Any]] = run_autofix_for_version(
                extracted_package, package_name, package_version_str, package_id
            )
            return conformance_result
        except Exception as e:
            console.log(f"Error when running autofix: {e}")
            return


def run_autofix_from_tarballs(
    output: defaultdict[str, list[dict[str, Any]]],
    tarball_data: defaultdict[str, dict[str, DownloadedTarballOutput]],
    package_limit: int = 0,
) -> dict[str, dict[str, dict[str, Any]]]:
    results: dict[str, dict[str, dict[str, Any]]] = {}

    for i, package in enumerate(output):
        if package_limit > 0 and i > package_limit:
            break
        results[package] = {}
        for version in output[package]:
            package_version_string = version.get("package_version_string")
            if package_version_string is None:
                continue
            if (
                package not in tarball_data
                or package_version_string not in tarball_data[package]
                or "tarball_file_path" not in tarball_data[package][package_version_string]
            ):
                console.log(f"No tarball found for {package} version {package_version_string}")
                continue

            # parse_compatible = version.get("parse_compatible", True)
            # if parse_compatible:
            #     continue

            version_output = {
                "hub_data": version,
                "parse_compatible_hub": version["parse_compatible"],
                "manually_verified_incompatible": version["manually_verified_incompatible"],
                "manually_verified_compatible": version["manually_verified_compatible"],
            }
            console.log(version.get("package_id_from_path"))
            autofix_output: Optional[dict[str, Any]] = extract_tarball_and_run_autofix(
                package_name=package,
                package_id=version["package_id_from_path"],
                package_version_str=str(package_version_string),
                tar_path=Path(tarball_data[package][package_version_string]["tarball_file_path"]),
            )
            if not autofix_output:
                console.log(f"Could not run autofix for {package} version {package_version_string}\n")
                continue
            else:
                version_output["autofix_output"] = autofix_output["autofix_output"]
                if autofix_output["autofix_output"] is not None and autofix_output["autofix_output"].get(
                    "autofix_stdout"
                ):
                    version_output["autofix_stdout_count"] = len(autofix_output["autofix_output"].get("autofix_stdout"))
                if autofix_output["autofix_output"] is not None and autofix_output["autofix_output"].get(
                    "autofix_stderr"
                ):
                    version_output["autofix_stderr_count"] = len(autofix_output["autofix_output"].get("autofix_stderr"))

                version_output["autofixed_version_file_name"] = autofix_output.get("autofixed_version_file_name")
                console.log()
            results[package][package_version_string] = version_output

    return results


def write_autofix_output_to_json(
    data: dict[str, dict[str, dict[str, Any]]],
    dest_dir: Path,
    *,
    indent: int = 2,
    sort_keys: bool = True,
):
    out_file = Path(dest_dir) / "conformance_autofix_output_local.json"
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=indent, sort_keys=sort_keys, ensure_ascii=False)


@app.command()
def main(
    local_hub_path: Annotated[
        str, typer.Option("--local-hub", help="Fully qualified path to local Package Hub clone")
    ] = str(DEFAULT_HUB_PATH),
    output_path: Annotated[
        str, typer.Option("--output-path", help="Fully qualified path to directory for output")
    ] = str(DEFAULT_OUTPUT_PATH),
    package_limit: Annotated[
        int, typer.Option("--limit", help="Only run on first n packages (default = 0 to run all packages)")
    ] = 0,
    download_path: Annotated[
        str, typer.Option("--download-path", help="Fully qualified path to directory for output")
    ] = str(DEFAULT_DOWNLOAD_PATH),
):
    if output_path:
        output_dir = Path(output_path)
    else:
        output_dir = DEFAULT_OUTPUT_PATH

    console.log(f"Reading from local Hub repo: {local_hub_path}")
    console.log(f"Writing to output path: {output_dir}/conformance_autofix_output_local.json")
    console.log(f"Package limit: {package_limit}")

    # get package version data from hub
    output: defaultdict[str, list[dict[str, Any]]] = read_json_from_local_hub_repo_with_conformance(path=local_hub_path)
    console.log(len(output))
    # get paths to locally stored tarballs
    local_tarball_output: defaultdict[str, dict[str, DownloadedTarballOutput]] = (
        reload_package_tarball_output_from_file(output_dir / "package_tarball_downloads.json")
    )
    console.log(len(local_tarball_output))

    autofix_results: dict[str, dict[str, dict[str, Any]]] = run_autofix_from_tarballs(
        output, local_tarball_output, package_limit
    )
    write_autofix_output_to_json(autofix_results, output_dir)
    console.log(f"Successfully wrote output to {output_dir}/conformance_autofix_output_local.json")


if __name__ == "__main__":
    app()

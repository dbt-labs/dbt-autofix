import json
import os
import tarfile
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Optional

import typer
from rich.console import Console
from typing_extensions import Annotated

from dbt_fusion_package_tools.check_parse_conformance import (
    run_conformance_for_version,
)
from dbt_fusion_package_tools.compatibility import FusionConformanceResult
from dbt_fusion_package_tools.scripts.constants import (
    DEFAULT_AUTOFIXED_TARBALL_PATH,
    DEFAULT_FUSION_BINARY_PATH,
    DEFAULT_HUB_PATH,
    DEFAULT_OUTPUT_PATH,
)

console = Console()
error_console = Console(stderr=True)

app = typer.Typer()


def reload_autofixed_tarball_output_from_file(
    file_path: Path,
) -> defaultdict[str, dict[str, dict[str, Any]]]:
    with (file_path / "conformance_autofix_output_local.json").open("r", encoding="utf-8") as fh:
        file_output = json.load(fh)
    output: defaultdict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for package, version in file_output.items():
        for version_str, autofixed_tarball_output in version.items():
            output[package][version_str] = autofixed_tarball_output
    return output


def write_dict_to_json(data: Dict[str, Any], dest_dir: Path, *, indent: int = 2, sort_keys: bool = True) -> None:
    out_file = dest_dir / "package_autofixed_parse_output.json"
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=indent, sort_keys=sort_keys, ensure_ascii=False)


def reload_packages_from_file(
    file_path: Path,
) -> defaultdict[str, list[dict[str, Any]]]:
    with file_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def extract_tarball_and_run_conformance(
    package_name: str,
    package_id: str,
    package_version_str: str,
    tar_path: Path,
) -> Optional[FusionConformanceResult]:
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

        # run conformance if possible
        try:
            console.log(f"Running parse conformance for {package_id} version {package_version_str}")
            return run_conformance_for_version(
                extracted_package,
                package_name,
                package_version_str,
                package_id,
                fusion_binary=DEFAULT_FUSION_BINARY_PATH,
            )
        except Exception as e:
            console.log(f"Error when running conformance: {e}")
            return


def run_conformance_from_local_tarballs(
    output: defaultdict[str, dict[str, dict[str, Any]]],
    package_limit: int = 0,
) -> defaultdict[str, dict[str, dict[str, Any]]]:
    results: defaultdict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

    for i, package in enumerate(output):
        if package_limit > 0 and i > package_limit:
            break
        results[package] = {}
        for package_version_string, version_output in output[package].items():
            try:
                if package_version_string is None:
                    continue
                autofixed_version_file_name = version_output.get("autofixed_version_file_name")
                if autofixed_version_file_name is None:
                    continue
                tar_path = Path(DEFAULT_AUTOFIXED_TARBALL_PATH / autofixed_version_file_name)
                conformance_output = extract_tarball_and_run_conformance(
                    package_name=package,
                    package_id=version_output["hub_data"]["package_id_from_path"],
                    package_version_str=package_version_string,
                    tar_path=tar_path,
                )
                if not conformance_output:
                    console.log(f"Could not run conformance for {package} version {package_version_string}\n")
                    continue
                else:
                    results[package][package_version_string] = version_output
                    results[package][package_version_string]["conformance_output"] = conformance_output.to_dict()
                    results[package][package_version_string]["parse_compatible_before_autofix"] = version_output[
                        "parse_compatible_hub"
                    ]
                    results[package][package_version_string]["parse_compatible_after_autofix"] = (
                        conformance_output.parse_compatible
                    )
                    console.log()
            except Exception as e:
                error_console.log(f"Error when running conformance for {package} {package_version_string}: {e}")

    return results


def write_conformance_output_to_json(
    data: defaultdict[str, dict[str, dict[str, Any]]],
    dest_dir: Path,
    *,
    indent: int = 2,
    sort_keys: bool = True,
):
    # data_output = {}
    # for k, v in data.items():
    #     data_output[k] = {version: result.to_dict() for version, result in v.items()}
    out_file = Path(dest_dir) / "conformance_output_after_autofix_local.json"
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

    output: defaultdict[str, dict[str, dict[str, Any]]] = reload_autofixed_tarball_output_from_file(
        file_path=DEFAULT_OUTPUT_PATH
    )

    parse_conformance_results = run_conformance_from_local_tarballs(output, package_limit)
    write_conformance_output_to_json(parse_conformance_results, output_dir)
    console.log(f"Successfully wrote output to {output_path}/conformance_output_after_autofix_local.json")


if __name__ == "__main__":
    app()

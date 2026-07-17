"""Script to download code for all package versions.

Reads JSON from a local clone of `hub.getdbt.com` and extracts all package versions,
then downloads the tarballs.
Writes output to `output/package_version_downloads.json`
"""

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional, TypedDict

import requests
import typer
from rich import print
from rich.console import Console
from typing_extensions import Annotated

from dbt_fusion_package_tools.check_parse_conformance import construct_download_url_from_latest
from dbt_fusion_package_tools.scripts.constants import DEFAULT_HUB_PATH, DEFAULT_OUTPUT_PATH
from dbt_fusion_package_tools.scripts.package_hub_fusion_compatibility import (
    get_latest_github_tarball_urls,
    read_json_from_local_hub_repo,
)

console = Console()
error_console = Console(stderr=True)

app = typer.Typer()

current_dir = Path.cwd()
DEFAULT_OUTPUT_FILE_NAME = "package_tarball_downloads.json"
DEFAULT_DOWNLOAD_PATH = Path.home() / "workplace" / "package-tarballs"


class DownloadedTarballOutput(TypedDict):
    package_name: str
    package_version: str
    tarball_file_name: str
    tarball_file_path: str


def download_tarball(
    package_name: str,
    package_id: str,
    package_version_str: str,
    package_version_download_url: str,
    latest_package_version_download_url: Optional[str],
    download_path: Path = DEFAULT_DOWNLOAD_PATH,
) -> Optional[DownloadedTarballOutput]:
    tarball_name: str = f"{'_'.join(package_id.split('/'))}_{package_version_str}.tar.gz"
    console.log(f"{package_name}, {package_id}, {package_version_str}, {tarball_name}")

    # download tarball from version json
    tar_path: Optional[Path] = None
    # track exceptions across both checks
    exceptions: list[str] = []
    # use explicitly provided version first
    try:
        # Download the tarball
        response = requests.get(package_version_download_url, stream=True)
        response.raise_for_status()

        # Save to output directory
        tar_path = download_path / tarball_name
        with open(tar_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    except requests.exceptions.HTTPError as http_error:
        exceptions.append(f"{http_error.request.url}: {http_error.response.status_code}, {http_error.response.reason}")
    except Exception as other_error:
        exceptions.append(f"Error when downloading tarball: {other_error}")
    # if that errors or doesn't exist, construct from the latest version
    if not tar_path and latest_package_version_download_url:
        constructed_url: str = construct_download_url_from_latest(
            latest_package_version_download_url, package_version_download_url
        )
        try:
            # Download the tarball
            response = requests.get(constructed_url, stream=True)
            response.raise_for_status()

            # Save to a temporary file
            tar_path = download_path / tarball_name
            with open(tar_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        except requests.exceptions.HTTPError as http_error:
            exceptions.append(
                f"{http_error.request.url}: {http_error.response.status_code}, {http_error.response.reason}"
            )
        except Exception as other_error:
            exceptions.append(f"Error when downloading tarball: {other_error}")
    # if still no download, error
    if not tar_path:
        console.log(f"Could not download {package_name} {package_version_str}")
        for exception in exceptions:
            console.log(exception)
        return
    else:
        return DownloadedTarballOutput(
            package_name=package_name,
            package_version=package_version_str,
            tarball_file_name=tar_path.name,
            tarball_file_path=str(tar_path),
        )


def download_from_tarballs(
    output: defaultdict[str, list[dict[str, Any]]],
    package_latest_version_urls: dict[str, str],
    package_limit: int = 0,
    download_path: Path = DEFAULT_DOWNLOAD_PATH,
) -> dict[str, dict[str, DownloadedTarballOutput]]:
    results: dict[str, dict[str, DownloadedTarballOutput]] = {}

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
            downloaded_tarball_path = download_tarball(
                package_name=package,
                package_id=version["package_id_from_path"],
                package_version_str=str(package_version_string),
                package_version_download_url=package_version_download_url,
                latest_package_version_download_url=package_latest_version_urls.get(package),
                download_path=download_path,
            )
            if not downloaded_tarball_path:
                console.log(f"Could not download tarball for {package} version {package_version_string}\n")
                continue
            else:
                results[package][package_version_string] = downloaded_tarball_path
                console.log()

    return results


def write_package_download_output_to_json(
    data: dict[str, dict[str, DownloadedTarballOutput]],
    dest_dir: Path,
    *,
    indent: int = 2,
    sort_keys: bool = True,
):
    # data_output = {}
    # for k, v in data.items():
    #     data_output[k] = {version: result for version, result in v.items()}
    out_file = Path(dest_dir) / DEFAULT_OUTPUT_FILE_NAME
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=indent, sort_keys=sort_keys, ensure_ascii=False)


def write_package_download_output_to_csv(data: dict[str, dict[str, DownloadedTarballOutput]], dest_dir: Path):
    tarball_output_only = []
    for _package_name, version in data.items():
        for _version_str, tarball_output in version.items():
            tarball_output_only.append(tarball_output)
    with open(dest_dir / "package_tarball_downloads.csv", mode="w") as file:
        writer = csv.DictWriter(file, fieldnames=list(DownloadedTarballOutput.__annotations__.keys()))
        writer.writeheader()
        writer.writerows(tarball_output_only)
    print(f"Wrote {len(tarball_output_only)} rows to {dest_dir}/package_tarball_downloads.csv")


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
    if download_path:
        download_dir = Path(download_path)
    else:
        download_dir = DEFAULT_DOWNLOAD_PATH
    console.log(f"Reading from local Hub repo: {local_hub_path}")
    console.log(f"Writing to output path: {output_dir}/{DEFAULT_OUTPUT_FILE_NAME}")
    console.log(f"Package limit: {package_limit}")

    output: defaultdict[str, list[dict[str, Any]]] = read_json_from_local_hub_repo(path=local_hub_path)
    package_latest_version_urls: dict[str, str] = get_latest_github_tarball_urls(output)
    downloaded_tarballs = download_from_tarballs(output, package_latest_version_urls, package_limit, download_dir)
    write_package_download_output_to_json(downloaded_tarballs, output_dir)
    write_package_download_output_to_csv(downloaded_tarballs, output_dir)
    console.log(f"Successfully wrote output to {output_dir}/{DEFAULT_OUTPUT_FILE_NAME}")


if __name__ == "__main__":
    app()

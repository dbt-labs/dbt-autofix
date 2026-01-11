from collections import defaultdict
import csv
import json
from pathlib import Path
from typing import Any, Optional

from dbt_fusion_package_tools.fusion_version_compatibility_output import (
    FUSION_VERSION_COMPATIBILITY_OUTPUT,
)
from dbt_fusion_package_tools.compatibility import FusionConformanceResult
from dbt_fusion_package_tools.version_utils import VersionSpecifier
from dbt_fusion_package_tools.exceptions import SemverError

from pprint import pprint


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
    file_path = dir_path / f"index.json"
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
        print(f"******************** No package hub output found for {package_name} version {version}")
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
    new_conformance["fusion_version_tested"] = fusion_version
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


def main():
    file_path: Path = (
        Path.cwd() / "src" / "dbt_fusion_package_tools" / "scripts" / "output" / "v89_conformance_output_20251222.json"
    )
    fusion_version = "89"
    conformance_output = reload_output_from_file(file_path)
    conformance_data = extract_output_from_json(conformance_output)
    package_count = 0
    success_count = 0
    error_count = 0
    i = 0
    for package_name in conformance_data:
        # if i > 5:
        #     break
        latest_version = check_for_rename("/Users/chaya/workplace/hub.getdbt.com", package_name)
        package_count += 1
        for version in conformance_data[package_name]:
            try:
                version_spec = VersionSpecifier.from_version_string(version)
                if version_spec > latest_version:
                    continue
            except SemverError:
                print(f"Can't parse version spec {version} for package {package_name}")
                error_count += 1
                continue
            version_file_path = find_package_hub_file("/Users/chaya/workplace/hub.getdbt.com", package_name, version)
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
    print(package_count, success_count, error_count)


if __name__ == "__main__":
    main()

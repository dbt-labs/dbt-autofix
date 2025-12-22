from collections import defaultdict
import csv
import json
from pathlib import Path
from typing import Any

from dbt_fusion_package_tools.fusion_version_compatibility_output import FUSION_VERSION_COMPATIBILITY_OUTPUT
from dbt_fusion_package_tools.compatibility import FusionConformanceResult


def reload_output_from_file(
    file_path: Path,
) -> defaultdict[str, list[dict[str, Any]]]:
    with file_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)

    # data_output = {}
    # for k, v in data.items():
    #     data_output[k] = {version: result.to_dict() for version, result in v.items()}
    # out_file = dest_dir / "conformance_output.json"
    # with out_file.open("w", encoding="utf-8") as fh:
    #     json.dump(data_output, fh, indent=indent, sort_keys=sort_keys, ensure_ascii=False)


def main():
    file_path: Path = (
        Path.cwd() / "src" / "dbt_fusion_package_tools" / "scripts" / "output" / "v89_conformance_output_20251222.json"
    )
    fusion_version = "89"
    conformance_output = reload_output_from_file(file_path)
    package_summary = []
    unique_error_codes: set[str] = set()
    for package_name in conformance_output:
        package: dict[str, Any] = conformance_output[package_name]
        for version in package:
            package_version: dict[str, Any] = package[version]
            parse_errors = []
            parse_warnings = []
            parse_compatibility_result = package_version.get("parse_compatibility_result") or {}
            if parse_compatibility_result.get("errors"):
                for error in parse_compatibility_result["errors"]:
                    parse_errors.append(error["error_code"])
                    unique_error_codes.add(str(error["error_code"]))
            if parse_compatibility_result.get("warnings"):
                for warning in parse_compatibility_result["warnings"]:
                    parse_errors.append(warning["error_code"])
                    unique_error_codes.add(str(warning["error_code"]))
            package_summary.append(
                {
                    "package_name": package_name,
                    "package_version": version,
                    "manually_verified_compatible": package[version]["manually_verified_compatible"],
                    "manually_verified_incompatible": package[version]["manually_verified_incompatible"],
                    "parse_errors": parse_errors,
                    "parse_warnings": parse_warnings,
                    "parse_exit_code": parse_compatibility_result.get("parse_exit_code"),
                    "parse_total_errors": parse_compatibility_result.get("total_errors"),
                    "parse_total_warnings": parse_compatibility_result.get("total_warnings"),
                    "parse_compatible": package[version]["parse_compatible"],
                    "require_dbt_version_compatible": package[version]["require_dbt_version_compatible"],
                    "require_dbt_version_defined": package[version]["require_dbt_version_defined"],
                    "fusion_version": f"v{fusion_version}",
                }
            )
    print(f"unique error codes:")
    for error_code in unique_error_codes:
        print(error_code)
    field_names = [field for field in package_summary[0]]
    output_file = (
        Path.cwd()
        / "src"
        / "dbt_fusion_package_tools"
        / "scripts"
        / "output"
        / f"package_conformance_v{fusion_version}.csv"
    )
    with open(output_file, mode="w") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(package_summary)
    print(f"Wrote {len(package_summary)} rows to conformance_output_v{fusion_version}.csv")


if __name__ == "__main__":
    main()

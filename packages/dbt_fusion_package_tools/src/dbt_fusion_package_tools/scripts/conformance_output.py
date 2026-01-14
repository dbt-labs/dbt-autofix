from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
from typing import Any
from datetime import datetime


def reload_output_from_file(
    file_path: Path,
) -> defaultdict[str, list[dict[str, Any]]]:
    with file_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def main():
    output_path: Path = Path.cwd() / "src" / "dbt_fusion_package_tools" / "scripts" / "output"
    conformance_output_file_name: str = "v92_conformance_output_20260112.json"
    fusion_version = "92"
    run_date: str = datetime.today().strftime("%Y%m%d")
    package_summary_file_name: str = f"package_conformance_summary_v{fusion_version}_{run_date}.csv"
    package_errors_file_name: str = f"package_conformance_all_errors_v{fusion_version}_{run_date}.csv"
    conformance_output = reload_output_from_file(output_path / conformance_output_file_name)
    package_summary = []
    unique_error_codes: Counter[str] = Counter()
    unique_warning_codes: Counter[str] = Counter()
    all_errors = []
    for package_name in conformance_output:
        package: dict[str, Any] = conformance_output[package_name]
        for version in package:
            package_version: dict[str, Any] = package[version]
            parse_errors: set[str] = set()
            parse_warnings: set[str] = set()
            parse_compatibility_result = package_version.get("parse_compatibility_result") or {}
            if parse_compatibility_result.get("errors"):
                for error in parse_compatibility_result["errors"]:
                    parse_errors.add(str(error["error_code"]))
                    unique_error_codes[str(error["error_code"])] += 1
                    all_errors.append(
                        {
                            "package_name": package_name,
                            "package_version": version,
                            "manually_verified_compatible": package[version]["manually_verified_compatible"],
                            "manually_verified_incompatible": package[version]["manually_verified_incompatible"],
                            "parse_compatible": package[version]["parse_compatible"],
                            "parse_error_code": str(error["error_code"]),
                            "parse_error": error["body"],
                            "parse_exit_code": parse_compatibility_result.get("parse_exit_code"),
                            "parse_total_errors": parse_compatibility_result.get("total_errors"),
                            "parse_total_warnings": parse_compatibility_result.get("total_warnings"),
                            "require_dbt_version_compatible": package[version]["require_dbt_version_compatible"],
                            "require_dbt_version_defined": package[version]["require_dbt_version_defined"],
                            "fusion_version": f"v{fusion_version}",
                        }
                    )
            if parse_compatibility_result.get("warnings"):
                for warning in parse_compatibility_result["warnings"]:
                    parse_warnings.add(str(warning["error_code"]))
                    unique_warning_codes[str(error["error_code"])] += 1
            package_summary.append(
                {
                    "package_name": package_name,
                    "package_version": version,
                    "manually_verified_compatible": package[version]["manually_verified_compatible"],
                    "manually_verified_incompatible": package[version]["manually_verified_incompatible"],
                    "parse_errors": ",".join(sorted(parse_errors)) if len(parse_errors) > 0 else "",
                    "parse_warnings": ",".join(sorted(parse_warnings)) if len(parse_warnings) > 0 else "",
                    "parse_exit_code": parse_compatibility_result.get("parse_exit_code"),
                    "parse_total_errors": parse_compatibility_result.get("total_errors"),
                    "parse_total_warnings": parse_compatibility_result.get("total_warnings"),
                    "parse_compatible": package[version]["parse_compatible"],
                    "require_dbt_version_compatible": package[version]["require_dbt_version_compatible"],
                    "require_dbt_version_defined": package[version]["require_dbt_version_defined"],
                    "fusion_version": f"v{fusion_version}",
                }
            )
    print(f"unique error codes: {unique_error_codes}")
    print(f"unique warning codes: {unique_warning_codes}")

    with open(output_path / package_summary_file_name, mode="w") as file:
        writer = csv.DictWriter(file, fieldnames=[field for field in package_summary[0]])
        writer.writeheader()
        writer.writerows(package_summary)
    print(f"Wrote {len(package_summary)} rows to {output_path / package_summary_file_name}")

    with open(output_path / package_errors_file_name, mode="w") as file:
        writer = csv.DictWriter(file, fieldnames=[field for field in all_errors[0]])
        writer.writeheader()
        writer.writerows(all_errors)
    print(f"Wrote {len(all_errors)} rows to {output_path / package_errors_file_name}")


if __name__ == "__main__":
    main()

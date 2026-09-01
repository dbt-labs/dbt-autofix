import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

OUTPUT_PATH: Path = Path.cwd() / "src" / "dbt_fusion_package_tools" / "scripts" / "output"
CONFORMANCE_OUTPUT_FILE_NAME: str = "conformance_output_after_autofix_local.json"
FUSION_VERSION = "200"


def reload_output_from_file(
    file_path: Path,
) -> dict[str, dict[str, dict[str, Any]]]:
    with file_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def main():
    output_path: Path = OUTPUT_PATH
    conformance_output_file_name: str = CONFORMANCE_OUTPUT_FILE_NAME
    fusion_version = FUSION_VERSION
    run_date: str = datetime.today().strftime("%Y%m%d")
    package_summary_file_name: str = f"conformance_after_autofix_v{fusion_version}_{run_date}.csv"
    package_errors_file_name: str = f"conformance_errors_after_autofix_v{fusion_version}_{run_date}.csv"
    conformance_output = reload_output_from_file(output_path / conformance_output_file_name)
    package_summary = []
    parse_errors = []
    for package_name in conformance_output:
        package: dict[str, dict[str, Any]] = conformance_output[package_name]
        for version, version_output in package.items():
            autofix_output = version_output.get("autofix_output", {})
            autofix_stdout = autofix_output.get("autofix_stdout", [])
            parse_compatible_hub = version_output["parse_compatible_hub"]
            parse_compatible_pre_autofix = version_output["parse_compatible_before_autofix"]
            parse_compatible_post_autofix = version_output["parse_compatible_after_autofix"]
            parse_compatible_hub_pre_mismatch = parse_compatible_hub != parse_compatible_pre_autofix
            parse_compatible_hub_true_pre_false = parse_compatible_hub is True and parse_compatible_pre_autofix is False
            parse_compatible_hub_true_post_false = (
                parse_compatible_hub is True and parse_compatible_post_autofix is False
            )
            parse_compatible_hub_false_pre_true = parse_compatible_hub is False and parse_compatible_pre_autofix is True
            parse_compatible_hub_false_post_true = (
                parse_compatible_hub is False and parse_compatible_post_autofix is True
            )
            v2_compatible_eligible = (
                parse_compatible_hub is False or parse_compatible_pre_autofix is False
            ) and parse_compatible_post_autofix is True
            parse_results = version_output["conformance_output"].get("parse_compatibility_result", {})
            parse_errors_after_autofix = max(parse_results.get("total_errors", 0), len(parse_results.get("errors", [])))
            parse_warnings_after_autofix = max(
                parse_results.get("total_warnings", 0), len(parse_results.get("warnings", []))
            )
            if len(autofix_stdout) > 1:
                autofix_change_count = len(autofix_stdout[0].get("refactors", []))
            else:
                autofix_change_count = 0
            package_summary.append(
                {
                    "package_name": package_name,
                    "package_version": version,
                    "manually_verified_compatible": version_output["manually_verified_compatible"],
                    "manually_verified_incompatible": version_output["manually_verified_incompatible"],
                    "parse_compatible_hub": version_output["parse_compatible_hub"],
                    "parse_compatible_pre_autofix": parse_compatible_pre_autofix,
                    "parse_compatible_post_autofix": parse_compatible_post_autofix,
                    "autofix_stdout_count": version_output.get("autofix_stdout_count", 0),
                    "autofix_stderr_count": version_output.get("autofix_stderr_count", 0),
                    "autofix_change_count": autofix_change_count,
                    "parse_errors_post_autofix": parse_errors_after_autofix,
                    "parse_warnings_post_autofix": parse_warnings_after_autofix,
                    "parse_compatible_hub_pre_mismatch": parse_compatible_hub_pre_mismatch,
                    "parse_compatible_hub_true_pre_false": parse_compatible_hub_true_pre_false,
                    "parse_compatible_hub_true_post_false": parse_compatible_hub_true_post_false,
                    "parse_compatible_hub_false_pre_true": parse_compatible_hub_false_pre_true,
                    "parse_compatible_hub_false_post_true": parse_compatible_hub_false_post_true,
                    "v2_compatible_eligible": v2_compatible_eligible,
                    "autofixed_version_file_name": version_output.get("autofixed_version_file_name"),
                }
            )
            if len(parse_results.get("errors", [])) > 0:
                for error in parse_results.get("errors", []):
                    parse_errors.append(
                        {
                            "package_name": package_name,
                            "package_version": version,
                            "manually_verified_compatible": version_output["manually_verified_compatible"],
                            "manually_verified_incompatible": version_output["manually_verified_incompatible"],
                            "parse_compatible_hub": version_output["parse_compatible_hub"],
                            "parse_compatible_pre_autofix": parse_compatible_pre_autofix,
                            "parse_compatible_post_autofix": parse_compatible_post_autofix,
                            "autofix_stdout_count": version_output.get("autofix_stdout_count", 0),
                            "autofix_stderr_count": version_output.get("autofix_stderr_count", 0),
                            "autofix_change_count": autofix_change_count,
                            "parse_errors_post_autofix": parse_errors_after_autofix,
                            "parse_warnings_post_autofix": parse_warnings_after_autofix,
                            "parse_compatible_hub_pre_mismatch": parse_compatible_hub_pre_mismatch,
                            "parse_compatible_hub_true_pre_false": parse_compatible_hub_true_pre_false,
                            "parse_compatible_hub_true_post_false": parse_compatible_hub_true_post_false,
                            "parse_compatible_hub_false_pre_true": parse_compatible_hub_false_pre_true,
                            "parse_compatible_hub_false_post_true": parse_compatible_hub_false_post_true,
                            "v2_compatible_eligible": v2_compatible_eligible,
                            "autofixed_version_file_name": version_output.get("autofixed_version_file_name"),
                            "parse_error": error,
                        }
                    )

    with open(output_path / package_summary_file_name, mode="w") as file:
        writer = csv.DictWriter(file, fieldnames=[field for field in package_summary[0]])
        writer.writeheader()
        writer.writerows(package_summary)

    with open(output_path / package_errors_file_name, mode="w") as file:
        writer = csv.DictWriter(file, fieldnames=[field for field in parse_errors[0]])
        writer.writeheader()
        writer.writerows(parse_errors)
    print(f"Wrote {len(package_summary)} rows to {output_path / package_summary_file_name}")


if __name__ == "__main__":
    main()

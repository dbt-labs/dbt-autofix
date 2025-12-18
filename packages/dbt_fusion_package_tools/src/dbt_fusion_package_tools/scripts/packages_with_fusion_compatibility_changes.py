import csv
from pathlib import Path
from typing import Any

from dbt_fusion_package_tools.fusion_version_compatibility_output import FUSION_VERSION_COMPATIBILITY_OUTPUT


def main():
    package_summary = []
    for package_name in FUSION_VERSION_COMPATIBILITY_OUTPUT:
        package: dict[str, Any] = FUSION_VERSION_COMPATIBILITY_OUTPUT[package_name]
        package_summary.append(
            {
                "package_name": package_name,
                "fusion_compatible_version": len(package["fusion_compatible_versions"]),
                "fusion_incompatible_versions": len(package["fusion_incompatible_versions"]),
                "unknown_versions": len(package["unknown_compatibility_versions"]),
                "redirect": package["package_redirect_name"] is not None
                or package["package_redirect_namespace"] is not None,
                "latest_is_prerelease": package["latest_version"] != package["latest_version_incl_prerelease"],
                "latest_version": package["latest_version"],
                "oldest_fusion_compatible_version": package["oldest_fusion_compatible_version"],
                "latest_fusion_compatible_version": package["latest_fusion_compatible_version"],
            }
        )
    field_names = [field for field in package_summary[0]]
    output_file = Path.cwd() / "src" / "dbt_fusion_package_tools" / "scripts" / "output" / "packages.csv"
    with open(output_file, mode="w") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(package_summary)
    print(f"Wrote {len(package_summary)} rows to packages.csv")


if __name__ == "__main__":
    main()

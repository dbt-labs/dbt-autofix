import json
from pathlib import Path
from typing import Any, Optional

from dbt_fusion_package_tools.dbt_package_version import DbtPackageVersion
from dbt_fusion_package_tools.version_utils import VersionSpecifier


def read_package_output_json(file_path: Path) -> dict[str, Any]:
    """Read a JSON file containing package output data.

    Args:
        file_path: Path to the JSON file.

    Returns:
        A dictionary with the contents of the JSON file.
    """
    with file_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    return data


def convert_version_spec_to_string(version_spec: Optional[VersionSpecifier]) -> Optional[str]:
    if version_spec is None:
        return None
    else:
        return version_spec.to_version_string()


def new_name_from_redirect(redirect_name, redirect_namespace, current_name, current_namespace) -> str:
    if redirect_name and redirect_namespace:
        return f"{redirect_namespace}/{redirect_name}"
    elif redirect_namespace == None:
        return f"{current_namespace}/{redirect_name}"
    else:
        return f"{redirect_namespace}/{current_name}"


def get_versions_for_package(package_versions) -> dict[str, Any]:
    versions: list[DbtPackageVersion] = []
    latest_fusion_version: Optional[VersionSpecifier] = None
    latest_version: Optional[VersionSpecifier] = None
    latest_version_incl_prerelease: Optional[VersionSpecifier] = None
    oldest_fusion_compatible_version: Optional[VersionSpecifier] = None
    fusion_compatible_versions: list[VersionSpecifier] = []
    fusion_incompatible_versions: list[VersionSpecifier] = []
    unknown_compatibility_versions: list[VersionSpecifier] = []
    package_latest_version_index_json: Optional[VersionSpecifier] = None
    package_redirect_name: Optional[str] = None
    package_redirect_namespace: Optional[str] = None
    for version in package_versions:
        # skip record for package index
        if "package_latest_version_index_json" in version:
            package_latest_version_index_json = VersionSpecifier.from_version_string(
                version["package_latest_version_index_json"]
            )
            if version["package_redirect_name"] != None or version["package_redirect_namespace"] != None:
                package_redirect_name = version["package_redirect_name"]
                package_redirect_namespace = version["package_redirect_namespace"]
            continue
        package_version = DbtPackageVersion(
            package_name=version["package_name_version_json"],
            package_version_str=version["package_version_string"],
            package_id_with_version=version["package_id_with_version"],
            raw_require_dbt_version_range=version["package_version_require_dbt_version"],
            package_id=version["package_id_from_path"],
        )
        versions.append(version)
        dbt_version_defined = package_version.is_require_dbt_version_defined()
        fusion_compatible_version: bool = (
            dbt_version_defined and package_version.is_require_dbt_version_fusion_compatible()
        )
        if package_version.is_version_explicitly_disallowed_on_fusion():
            fusion_compatible_version = False
        if fusion_compatible_version:
            fusion_compatible_versions.append(package_version.version)
            if not latest_fusion_version or package_version.version > latest_fusion_version:
                latest_fusion_version = package_version.version
            if not oldest_fusion_compatible_version or package_version.version < oldest_fusion_compatible_version:
                oldest_fusion_compatible_version = package_version.version
        elif not dbt_version_defined:
            unknown_compatibility_versions.append(package_version.version)
        elif dbt_version_defined and not fusion_compatible_version:
            fusion_incompatible_versions.append(package_version.version)
        if package_version.is_prerelease_version():
            if not latest_version_incl_prerelease or package_version.version > latest_version_incl_prerelease:
                latest_version_incl_prerelease = package_version.version
        elif not latest_version or package_version.version > latest_version:
            latest_version = package_version.version
    if latest_version_incl_prerelease is None:
        latest_version_incl_prerelease = latest_version
    if latest_version is None and latest_version_incl_prerelease:
        latest_version = latest_version_incl_prerelease
    assert latest_version is not None
    assert latest_version_incl_prerelease is not None
    latest_version_incl_prerelease = max(latest_version_incl_prerelease, latest_version)
    if not package_redirect_name and not package_redirect_namespace:
        assert package_latest_version_index_json == latest_version
        assert len(fusion_compatible_versions) + len(fusion_incompatible_versions) + len(
            unknown_compatibility_versions
        ) == len(versions)
        if latest_fusion_version:
            assert oldest_fusion_compatible_version is not None
            assert len(fusion_compatible_versions) > 0
        if not latest_fusion_version:
            assert oldest_fusion_compatible_version == None
            assert len(fusion_compatible_versions) == 0
    return {
        # "versions": versions,
        "latest_version": convert_version_spec_to_string(latest_version),
        "oldest_fusion_compatible_version": convert_version_spec_to_string(oldest_fusion_compatible_version),
        "latest_fusion_compatible_version": convert_version_spec_to_string(latest_fusion_version),
        "fusion_compatible_versions": [convert_version_spec_to_string(x) for x in fusion_compatible_versions],
        "fusion_incompatible_versions": [convert_version_spec_to_string(x) for x in fusion_incompatible_versions],
        "unknown_compatibility_versions": [convert_version_spec_to_string(x) for x in unknown_compatibility_versions],
        "package_latest_version_index_json": convert_version_spec_to_string(package_latest_version_index_json),
        "package_redirect_name": package_redirect_name,
        "package_redirect_namespace": package_redirect_namespace,
        "latest_version_incl_prerelease": convert_version_spec_to_string(latest_version_incl_prerelease),
    }


def get_versions(packages):
    # skip over renamed packages and come back once the new package is done
    renamed_packages: list[tuple[str, str]] = []
    packages_with_versions: dict[str, dict[str, Any]] = {}
    for package in packages:
        versions = get_versions_for_package(packages[package])
        if versions["package_redirect_name"] is not None or versions["package_redirect_namespace"] is not None:
            old_package_namespace = package.split("/")[0]
            old_package_name = package.split("/")[1]
            new_name = new_name_from_redirect(
                versions["package_redirect_name"],
                versions["package_redirect_namespace"],
                old_package_name,
                old_package_namespace,
            )
            renamed_packages.append((package, new_name))
            print(f"renamed package: {package}, {new_name}")
            continue
        else:
            packages_with_versions[package] = versions
    for package in renamed_packages:
        old_package_name = package[0]
        new_package_name = package[1]
        packages_with_versions[old_package_name] = packages_with_versions[new_package_name]
        redirect_namespace, redirect_name = new_package_name.split("/")
        packages_with_versions[old_package_name]["package_redirect_name"] = redirect_name
        packages_with_versions[old_package_name]["package_redirect_namespace"] = redirect_namespace
        packages_with_versions[old_package_name]["package_redirect_id"] = new_package_name
    assert len(packages_with_versions) == len(packages)
    return packages_with_versions


def write_dict_to_json(data: dict[str, Any], dest_dir: Path, *, indent: int = 2, sort_keys: bool = True) -> None:
    out_file = dest_dir / "fusion_version_compatibility_output.json"
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=indent, sort_keys=sort_keys, ensure_ascii=False)


def main():
    input: Path = Path.cwd() / "src" / "dbt_fusion_package_tools" / "scripts" / "output"
    data = read_package_output_json(input / "package_output.json")
    # check_package_names(data)
    packages_with_versions: dict[str, dict[str, Any]] = get_versions(data)
    print(f"Read {len(packages_with_versions)} packages from file")
    write_dict_to_json(packages_with_versions, input)
    print("Output written to fusion_version_compatibility_output.json")
    with open(
        Path.cwd() / "src" / "dbt_fusion_package_tools" / "fusion_version_compatibility_output.py", "w"
    ) as output_py_file:
        output_py_file.write(
            f"from typing import Any\n\nFUSION_VERSION_COMPATIBILITY_OUTPUT: dict[str, dict[str, Any]] = {packages_with_versions}"
        )


if __name__ == "__main__":
    main()

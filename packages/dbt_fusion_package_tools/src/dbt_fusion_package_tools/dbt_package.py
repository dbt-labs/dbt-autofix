from dataclasses import dataclass, field
from typing import Any, Optional

from rich.console import Console

from dbt_fusion_package_tools.dbt_package_version import (
    DbtPackageVersion,
)
from dbt_fusion_package_tools.fusion_version_compatibility_output import FUSION_VERSION_COMPATIBILITY_OUTPUT
from dbt_fusion_package_tools.manual_overrides import EXPLICIT_ALLOW_ALL_VERSIONS, EXPLICIT_DISALLOW_ALL_VERSIONS
from dbt_fusion_package_tools.upgrade_status import (
    PackageFusionCompatibilityState,
    PackageVersionFusionCompatibilityState,
)
from dbt_fusion_package_tools.version_utils import (
    VersionRange,
    VersionSpecifier,
    construct_version_list_from_raw,
    convert_optional_version_string_to_spec,
    convert_version_specifiers_to_range,
    convert_version_string_list_to_spec,
    get_version_specifiers,
    versions_compatible,
)
from dbt_fusion_package_tools.git.package_repo import DbtPackageRepo

console = Console()
error_console = Console(stderr=True)


@dataclass
class DbtPackage:
    # name in project yml
    package_name: str
    # org/package_name used in deps and package hub
    package_id: str
    # version range specified in deps config (packages.yml)
    project_config_raw_version_specifier: Any
    project_config_version_range_list: list[str] = field(default_factory=list)
    project_config_version_range: Optional[VersionRange] = field(init=False)
    # package versions indexed by version string
    package_versions: dict[str, DbtPackageVersion] = field(default_factory=dict)
    installed_package_version: Optional[VersionSpecifier] = None
    latest_package_version: Optional[VersionSpecifier] = None
    latest_package_version_incl_prerelease: Optional[VersionSpecifier] = None
    # misc parameters from deps config
    git_url: Optional[str] = None
    opt_in_prerelease: bool = False
    local: bool = False
    tarball: bool = False
    git: bool = False
    private: bool = False
    # represent git repo if needed (such as for package hub)
    git_repo: Optional[DbtPackageRepo] = None

    # fields from FUSION_VERSION_COMPATIBILITY_OUTPUT
    package_redirect_id: Optional[str] = None
    lowest_fusion_compatible_version: Optional[VersionSpecifier] = None
    fusion_compatible_versions: list[VersionSpecifier] = field(default_factory=list)
    fusion_incompatible_versions: list[VersionSpecifier] = field(default_factory=list)
    unknown_compatibility_versions: list[VersionSpecifier] = field(default_factory=list)

    # check compatibility of latest and installed versions when loading
    latest_version_fusion_compatibility: PackageVersionFusionCompatibilityState = (
        PackageVersionFusionCompatibilityState.UNKNOWN
    )
    installed_version_fusion_compatibility: PackageVersionFusionCompatibilityState = (
        PackageVersionFusionCompatibilityState.UNKNOWN
    )

    def merge_fusion_compatibility_output(self) -> bool:
        output = FUSION_VERSION_COMPATIBILITY_OUTPUT.get(self.package_id)
        if output is None:
            return False
        self.package_redirect_id = output.get("package_redirect_id")
        oldest_fusion_compatible_version = convert_optional_version_string_to_spec(
            output["oldest_fusion_compatible_version"]
        )
        fusion_compatible_versions = convert_version_string_list_to_spec(output["fusion_compatible_versions"])
        fusion_incompatible_versions = convert_version_string_list_to_spec(output["fusion_incompatible_versions"])
        unknown_compatibility_versions = convert_version_string_list_to_spec(output["unknown_compatibility_versions"])
        self.lowest_fusion_compatible_version = oldest_fusion_compatible_version
        self.fusion_compatible_versions = fusion_compatible_versions
        self.fusion_incompatible_versions = fusion_incompatible_versions
        self.unknown_compatibility_versions = unknown_compatibility_versions
        return True

    def __post_init__(self):
        try:
            if self.project_config_raw_version_specifier is not None:
                self.project_config_version_range_list = construct_version_list_from_raw(
                    self.project_config_raw_version_specifier
                )
            if self.project_config_version_range_list and len(self.project_config_version_range_list) > 0:
                version_specs: list[VersionSpecifier] = get_version_specifiers(self.project_config_version_range_list)
                self.project_config_version_range = convert_version_specifiers_to_range(version_specs)
            else:
                self.project_config_version_range = None
        except:
            self.project_config_version_range = None
            error_console.print("exception calculating config version range ")
        self.merge_fusion_compatibility_output()

    def add_package_version(self, new_package_version: DbtPackageVersion, installed=False, latest=False) -> bool:
        new_package_version.package_id = self.package_id
        if latest:
            self.latest_package_version = new_package_version.version
            self.latest_version_fusion_compatibility = new_package_version.get_fusion_compatibility_state()
        if new_package_version.package_version_str in self.package_versions:
            console.log(f"Package version {new_package_version.package_version_str} already exists in package versions")
            return False
        else:
            self.package_versions[new_package_version.package_version_str] = new_package_version
        if installed:
            self.installed_package_version = new_package_version.version
            self.installed_version_fusion_compatibility = new_package_version.get_fusion_compatibility_state()
        return True

    def set_latest_package_version(self, version_str: str, raw_require_dbt_version: Any = None):
        try:
            return self.add_package_version(
                DbtPackageVersion(
                    package_name=self.package_name,
                    package_version_str=version_str,
                    raw_require_dbt_version_range=raw_require_dbt_version,
                ),
                latest=True,
            )
        except:
            return False

    def is_public_package(self) -> bool:
        return not (self.git or self.tarball or self.local or self.private)

    def is_installed_version_fusion_compatible(self) -> PackageVersionFusionCompatibilityState:
        if self.installed_package_version is None:
            return PackageVersionFusionCompatibilityState.UNKNOWN
        else:
            installed_version_string = self.installed_package_version.to_version_string(skip_matcher=True)
            if installed_version_string not in self.package_versions:
                return PackageVersionFusionCompatibilityState.UNKNOWN
            else:
                return self.package_versions[installed_version_string].get_fusion_compatibility_state()

    def find_fusion_compatible_versions_in_requested_range(self) -> list[VersionSpecifier]:
        """Find package versions that are compatible with Fusion AND the version range specified in the project config.

        A project can upgrade to one of these version without updating their project's packages.yml.

        Returns:
            list[VersionSpecifier]: Fusion-compatible versions
        """
        if self.project_config_version_range is None:
            return []
        compatible_versions = []
        if self.fusion_compatible_versions is None or len(self.fusion_compatible_versions) == 0:
            return compatible_versions
        for version in self.fusion_compatible_versions:
            if versions_compatible(
                version, self.project_config_version_range.start, self.project_config_version_range.end
            ):
                compatible_versions.append(version)
        sorted_versions = sorted(compatible_versions)
        return sorted_versions

    def find_fusion_compatible_versions_above_requested_range(self) -> list[VersionSpecifier]:
        """Find package versions that are compatible with Fusion but NOT the version range specified in the project config.

        The project's packages.yml/dependencies.yml MUST be updated in order to upgrade to one of these version.

        Returns:
            list[VersionSpecifier]: Fusion-compatible versions
        """
        if self.project_config_version_range is None:
            return []
        compatible_versions = []
        if self.fusion_compatible_versions is None or len(self.fusion_compatible_versions) == 0:
            return compatible_versions
        for version in self.fusion_compatible_versions:
            if (
                not versions_compatible(
                    version,
                    self.project_config_version_range.start,
                    self.project_config_version_range.end,
                    # make sure we only count versions newer than the current version
                    # so we don't recommend downgrades
                )
                and version > self.project_config_version_range.start
            ):
                compatible_versions.append(version)
        sorted_versions = sorted(compatible_versions)
        return sorted_versions

    def find_fusion_incompatible_versions_in_requested_range(self) -> list[VersionSpecifier]:
        incompatible_versions = []
        if self.project_config_version_range is None:
            return []
        if self.fusion_incompatible_versions is None or len(self.fusion_incompatible_versions) == 0:
            return incompatible_versions
        for version in self.fusion_incompatible_versions:
            if versions_compatible(
                version, self.project_config_version_range.start, self.project_config_version_range.end
            ):
                incompatible_versions.append(version)
        sorted_versions = sorted(incompatible_versions)
        return sorted_versions

    def find_fusion_unknown_versions_in_requested_range(self) -> list[VersionSpecifier]:
        unknown_compatibility_versions = []
        if self.project_config_version_range is None:
            return []
        if self.unknown_compatibility_versions is None or len(self.unknown_compatibility_versions) == 0:
            return unknown_compatibility_versions
        for version in self.unknown_compatibility_versions:
            if versions_compatible(
                version, self.project_config_version_range.start, self.project_config_version_range.end
            ):
                unknown_compatibility_versions.append(version)
        sorted_versions = sorted(unknown_compatibility_versions)
        return sorted_versions

    def get_installed_package_version(self) -> str:
        if self.installed_package_version:
            return self.installed_package_version.to_version_string(skip_matcher=True)
        elif self.installed_package_version is None and self.project_config_version_range is not None:
            return self.project_config_version_range.start.to_version_string(skip_matcher=True)
        else:
            return "unknown"

    def get_package_fusion_compatibility_state(self) -> PackageFusionCompatibilityState:
        if not self.is_public_package():
            return PackageFusionCompatibilityState.UNKNOWN
        if self.package_id in EXPLICIT_DISALLOW_ALL_VERSIONS:
            return PackageFusionCompatibilityState.NO_VERSIONS_COMPATIBLE
        if self.package_id in EXPLICIT_ALLOW_ALL_VERSIONS:
            return PackageFusionCompatibilityState.ALL_VERSIONS_COMPATIBLE

        fusion_compatible_version_count = (
            0 if self.fusion_compatible_versions is None else len(self.fusion_compatible_versions)
        )
        fusion_incompatible_version_count = (
            0 if self.fusion_incompatible_versions is None else len(self.fusion_incompatible_versions)
        )
        unknown_compatibility_version_count = (
            0 if self.unknown_compatibility_versions is None else len(self.unknown_compatibility_versions)
        )
        total_version_count = (
            fusion_compatible_version_count + fusion_incompatible_version_count + unknown_compatibility_version_count
        )
        # cases where we can determine compatibility across all versions
        if total_version_count == 0:
            return PackageFusionCompatibilityState.UNKNOWN
        elif fusion_compatible_version_count == total_version_count:
            return PackageFusionCompatibilityState.ALL_VERSIONS_COMPATIBLE
        elif unknown_compatibility_version_count == total_version_count:
            return PackageFusionCompatibilityState.MISSING_COMPATIBILITY
        elif fusion_incompatible_version_count == total_version_count:
            return PackageFusionCompatibilityState.NO_VERSIONS_COMPATIBLE
        # case where we have to look at individual versions to determine compatibility
        elif total_version_count > 0 and fusion_compatible_version_count > 0:
            return PackageFusionCompatibilityState.SOME_VERSIONS_COMPATIBLE
        # fallback case where some versions have incompatible version and some have no version defined
        elif total_version_count > 0 and fusion_compatible_version_count == 0:
            return PackageFusionCompatibilityState.NO_VERSIONS_COMPATIBLE
        # hopefully nothing is left but if so
        else:
            return PackageFusionCompatibilityState.UNKNOWN

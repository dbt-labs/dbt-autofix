from typing import Any, Optional, Union
from dataclasses import dataclass, field
from rich.console import Console
from dbt_autofix.packages.dbt_package_version import (
    DbtPackageVersion,
    FusionCompatibilityState,
    RawVersion,
    construct_version_list,
    convert_version_specifiers_to_range,
    get_version_specifiers,
)
from dbt_common.semver import VersionSpecifier, VersionRange, versions_compatible
from dbt_autofix.packages.manual_overrides import EXPLICIT_DISALLOW_ALL_VERSIONS, EXPLICIT_ALLOW_ALL_VERSIONS


console = Console()


@dataclass
class DbtPackage:
    # name in project yml
    package_name: str
    # org/package_name used in deps and package hub
    package_id: str
    # version range specified in deps config (packages.yml)
    project_config_raw_version_specifier: Union[str, list[str], None]
    project_config_version_range: VersionRange = field(init=False)
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

    # fields for hardcoding Fusion-specific info
    min_upgradeable_version: Optional[str] = None
    max_upgradeable_version: Optional[str] = None
    lowest_fusion_compatible_version: Optional[VersionSpecifier] = None
    fusion_compatible_versions: Optional[list[VersionSpecifier]] = None
    fusion_incompatible_versions: Optional[list[VersionSpecifier]] = None
    unknown_compatibility_versions: Optional[list[VersionSpecifier]] = None

    # check compatibility of latest and installed versions when loading
    latest_version_fusion_compatibility: FusionCompatibilityState = FusionCompatibilityState.UNKNOWN
    installed_version_fusion_compatibility: FusionCompatibilityState = FusionCompatibilityState.UNKNOWN

    def __post_init__(self):
        project_config_raw_version_specifier_parsed = construct_version_list(self.project_config_raw_version_specifier)
        version_specs: list[VersionSpecifier] = get_version_specifiers(project_config_raw_version_specifier_parsed)
        self.project_config_version_range = convert_version_specifiers_to_range(version_specs)

    def add_package_version(self, new_package_version: DbtPackageVersion, installed=False, latest=False) -> bool:
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

    def set_latest_package_version(self, version_str: str, require_dbt_version_range: list[str] = []):
        try:
            return self.add_package_version(
                DbtPackageVersion(
                    package_name=self.package_name,
                    package_version_str=version_str,
                    require_dbt_version_range=require_dbt_version_range,
                ),
                latest=True,
            )
        except:
            return False

    def is_public_package(self) -> bool:
        return not (self.git or self.tarball or self.local)

    def is_installed_version_fusion_compatible(self) -> FusionCompatibilityState:
        if self.package_id in EXPLICIT_DISALLOW_ALL_VERSIONS:
            return FusionCompatibilityState.EXPLICIT_DISALLOW
        if self.package_id in EXPLICIT_ALLOW_ALL_VERSIONS:
            return FusionCompatibilityState.EXPLICIT_ALLOW
        if self.installed_package_version is None:
            return FusionCompatibilityState.UNKNOWN
        else:
            installed_version_string = self.installed_package_version.to_version_string()
            if installed_version_string not in self.package_versions:
                return FusionCompatibilityState.UNKNOWN
            else:
                return self.package_versions[installed_version_string].get_fusion_compatibility_state()

    def find_fusion_compatible_versions_in_requested_range(self) -> list[VersionSpecifier]:
        """Find package versions that are compatible with Fusion AND the version range specified in the project config.

        A project can upgrade to one of these version without updating their project's packages.yml.

        Returns:
            list[VersionSpecifier]: Fusion-compatible versions
        """
        compatible_versions = []
        if self.fusion_compatible_versions is None or len(self.fusion_compatible_versions) == 0:
            return compatible_versions
        for version in self.fusion_compatible_versions:
            if versions_compatible(
                version, self.project_config_version_range.start, self.project_config_version_range.end
            ):
                compatible_versions.append(version)
        sorted_versions = sorted(compatible_versions, reverse=True)
        return sorted_versions

    def find_fusion_compatible_versions_outside_requested_range(self) -> list[VersionSpecifier]:
        """Find package versions that are compatible with Fusion but NOT the version range specified in the project config.

        The project's packages.yml/dependencies.yml MUST be updated in order to upgrade to one of these version.

        Returns:
            list[VersionSpecifier]: Fusion-compatible versions
        """
        compatible_versions = []
        if self.fusion_compatible_versions is None or len(self.fusion_compatible_versions) == 0:
            return compatible_versions
        for version in self.fusion_compatible_versions:
            if not versions_compatible(
                version, self.project_config_version_range.start, self.project_config_version_range.end
            ):
                compatible_versions.append(version)
        sorted_versions = sorted(compatible_versions, reverse=True)
        return sorted_versions

    def find_fusion_incompatible_versions_in_requested_range(self) -> list[VersionSpecifier]:
        incompatible_versions = []
        if self.fusion_incompatible_versions is None or len(self.fusion_incompatible_versions) == 0:
            return incompatible_versions
        for version in self.fusion_incompatible_versions:
            if versions_compatible(
                version, self.project_config_version_range.start, self.project_config_version_range.end
            ):
                incompatible_versions.append(version)
        sorted_versions = sorted(incompatible_versions, reverse=True)
        return sorted_versions

    def find_fusion_unknown_versions_in_requested_range(self) -> list[VersionSpecifier]:
        unknown_compatibility_versions = []
        if self.unknown_compatibility_versions is None or len(self.unknown_compatibility_versions) == 0:
            return unknown_compatibility_versions
        for version in self.unknown_compatibility_versions:
            if versions_compatible(
                version, self.project_config_version_range.start, self.project_config_version_range.end
            ):
                unknown_compatibility_versions.append(version)
        sorted_versions = sorted(unknown_compatibility_versions, reverse=True)
        return sorted_versions

    def get_installed_package_version(self) -> str:
        if self.installed_package_version:
            return self.installed_package_version.to_version_string()
        else:
            return "unknown"

    def get_fusion_compatibility_state(self) -> FusionCompatibilityState:
        if not self.is_public_package():
            return FusionCompatibilityState.UNKNOWN
        if self.package_id in EXPLICIT_DISALLOW_ALL_VERSIONS:
            return FusionCompatibilityState.EXPLICIT_DISALLOW
        if self.package_id in EXPLICIT_ALLOW_ALL_VERSIONS:
            return FusionCompatibilityState.EXPLICIT_ALLOW
        installed_version_fusion_compatibility = self.is_installed_version_fusion_compatible()
        if (
            installed_version_fusion_compatibility == FusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
            or installed_version_fusion_compatibility == FusionCompatibilityState.EXPLICIT_ALLOW
        ):
            return installed_version_fusion_compatibility
        if self.fusion_compatible_versions is not None and len(self.fusion_compatible_versions) > 0:
            return FusionCompatibilityState.DBT_VERSION_RANGE_INCLUDES_2_0
        elif self.fusion_incompatible_versions is not None and len(self.fusion_incompatible_versions) > 0:
            if self.unknown_compatibility_versions is None or len(self.unknown_compatibility_versions) == 0:
                return FusionCompatibilityState.DBT_VERSION_RANGE_EXCLUDES_2_0
        return FusionCompatibilityState.NO_DBT_VERSION_RANGE

from typing import Any, Optional, Union
from dataclasses import dataclass, field
from rich.console import Console
from dbt_autofix.packages.dbt_package_version import DbtPackageVersion, FusionCompatibilityState, RawVersion, get_version_specifiers
from dbt_common.semver import VersionSpecifier, VersionRange, versions_compatible


console = Console()

@dataclass
class DbtPackage:
    # name in project yml
    package_name: str
    # org/package_name used in deps and package hub
    package_id: str
    # package versions indexed by version string
    package_versions: dict[str, DbtPackageVersion] = field(default_factory=dict)
    installed_package_version: Optional[VersionSpecifier] = None
    latest_package_version: Optional[VersionSpecifier] = None
    # version range specified in deps config (packages.yml)
    project_config_raw_version_specifier: Optional[Any] = None
    # misc parameters from deps config
    git_url: Optional[str] = None
    opt_in_prerelease: bool = False
    local: bool = False
    tarball: bool = False
    git: bool = False

    # fields for hardcoding Fusion-specific info
    min_upgradeable_version: Optional[str] = None
    max_upgradeable_version: Optional[str] = None
    lowest_fusion_compatible_version: Optional[str] = None
    fusion_compatible_versions: Optional[list[VersionSpecifier]] = None

    # check compatibility of latest and installed versions when loading
    latest_version_fusion_compatibility: FusionCompatibilityState = FusionCompatibilityState.UNKNOWN
    installed_version_fusion_compatibility: FusionCompatibilityState = FusionCompatibilityState.UNKNOWN
    
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
            return self.add_package_version(DbtPackageVersion(package_name=self.package_name, package_version_str=version_str, require_dbt_version_range=require_dbt_version_range), latest=True)
        except:
            return False

    def is_public_package(self) -> bool:
        return not (self.git or self.tarball or self.local)

from typing import Optional, Union
from dbt_autofix.packages.installed_packages import DbtInstalledPackage


from semver.version import Version


from dataclasses import dataclass


@dataclass
class DbtPackage:
    package_dict: dict[str, Union[str, list[str]]]
    package_id: Optional[str] = None
    package_name: Optional[str] = None
    package_version_str: Optional[list[str]] = None
    package_version: Optional[Version] = None
    current_project_package_version_str: Optional[str] = None
    current_project_package_version: Optional[Version] = None
    current_project_package_version_range_str: Optional[list[str]] = None
    current_project_package_version_range: Optional[list[Version]] = None
    require_dbt_version: Optional[list[str]] = None
    min_upgradeable_version: Optional[str] = None
    max_upgradeable_version: Optional[str] = None
    lowest_fusion_compatible_version: Optional[str] = None
    fusion_compatible_versions: Optional[list[Version]] = None
    git_url: Optional[str] = None
    installed_version: Optional[DbtInstalledPackage] = None
    opt_in_prerelease: bool = False
    fusion_dbt_version: str = "2.0.0"

    def __post_init__(self):
        self.parse_package_dict()
        self.fusion_dbt_version_semver = Version.parse(self.fusion_dbt_version)

    def parse_package_dict(self):
        if "package" in self.package_dict and type(self.package_dict["package"]) == "str":
            self.package_name = str(self.package_dict["package"])
        if "version" in self.package_dict:
            if type(self.package_dict["version"] == "list"):
                self.package_version_str = [str(version) for version in self.package_dict["version"]]
            elif type(self.package_dict["version"] == "str"):
                self.package_version_str = [str(self.package_dict["version"])]
        if "git" in self.package_dict:
            self.git_url = str(self.package_dict["git"])
        if "install-prerelease" in self.package_dict:
            if str(self.package_dict["install-prerelease"]) == "true":
                self.opt_in_prerelease = True
            else:
                self.opt_in_prerelease = False

    def parse_current_package_version_range(self):
        pass

    def is_dbt_version_fusion_compatible(self, dbt_version_range: list[str]) -> bool:
        dbt_fusion_version = self.fusion_dbt_version_semver
        try:
            compatible_versions: list[bool] = [dbt_fusion_version.match(x) for x in dbt_version_range]
            return all(compatible_versions)
        except:
            return False

from enum import Enum


class PackageVersionFusionCompatibilityState(str, Enum):
    """String enum for Fusion compatibility of a specific version of a package."""

    NO_DBT_VERSION_RANGE = "require-dbt-version is not defined"
    DBT_VERSION_RANGE_EXCLUDES_2_0 = "require-dbt-version excludes version 2.0"
    DBT_VERSION_RANGE_INCLUDES_2_0 = "require-dbt-version includes version 2.0"
    EXPLICIT_ALLOW = "Version has been verified by dbt as Fusion-compatible even though its declared require-dbt-version may not include 2.0"
    EXPLICIT_DISALLOW = "Version has been verified by dbt as incompatible with Fusion"
    UNKNOWN = "Version state unknown"


class PackageFusionCompatibilityState(str, Enum):
    """String enum for Fusion compatibility at the package level."""

    ALL_VERSIONS_COMPATIBLE = "All package versions are Fusion compatible"
    SOME_VERSIONS_COMPATIBLE = "A subset of package versions are Fusion compatible"
    NO_VERSIONS_COMPATIBLE = "No versions are Fusion compatible"
    MISSING_COMPATIBILITY = "All package versions are missing require dbt version"
    UNKNOWN = "Package version state unknown"


class PackageVersionUpgradeType(str, Enum):
    """String enum for package upgrade types"""

    NO_UPGRADE_REQUIRED = "Package is already compatible with Fusion"
    UPGRADE_AVAILABLE = "Package has Fusion-compatible version available"
    PUBLIC_PACKAGE_MISSING_FUSION_ELIGIBILITY = (
        "Public package has not defined require-dbt-version so Fusion eligibility cannot be determined"
    )
    PUBLIC_PACKAGE_NOT_COMPATIBLE_WITH_FUSION = "Public package is not compatible with Fusion"
    PUBLIC_PACKAGE_FUSION_COMPATIBLE_VERSION_EXCEEDS_PROJECT_CONFIG = (
        "Public package has Fusion-compatible version that is outside the project's requested version range"
    )
    PRIVATE_PACKAGE_MISSING_REQUIRE_DBT_VERSION = "Private package requires a compatible require-dbt-version (>=2.0.0, <3.0.0) to be available on Fusion. https://docs.getdbt.com/reference/project-configs/require-dbt-version"
    UNKNOWN = "Package's Fusion eligibility unknown"

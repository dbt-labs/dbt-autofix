# Package Management

This directory contains code used by the `packages` option in the CLI that upgrades packages in a project to a Fusion-compatible version. The code is centered on four classes:
* DbtPackageFile: represents a file (currently packages.yml or dependencies.yml) that contains package dependencies for a project
* DbtPackage: represents a package that is installed as a dependency for the project
* DbtPackageVersion: represents a specific version of a package
* DbtPackageTextFile: contains the raw lines of text from package dependency files. This is used when upgrading packages so we can replace just the version strings within a file without affecting the rest of the file layout (such as comments).

## Scripts

Two scripts are used to pull data from the public package registry (hub.getdbt.com) and extract Fusion compatibility information from available versions. This is basically a local cache of package information to bootstrap autofix. We need to know the lower bound of Fusion-compatible versions for a package but we also know that older versions of packages will not change, so caching this locally removes a lot of repetitive network calls and text parsing. Which means faster run times and fewer failures due to network issues. 

The output from these two scripts produces `fusion_version_compatibility_output.py` that contains a single constant, `FUSION_VERSION_COMPATIBILITY_OUTPUT`. This is then used in `DbtPackageFile`'s `merge_fusion_compatibility_output` to populate compatible versions within all package dependencies.

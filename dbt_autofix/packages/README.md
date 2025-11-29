# Package Management

This directory contains code used by the `packages` option in the CLI that upgrades packages in a project to a Fusion-compatible version. The code is centered on four classes:
* DbtPackageFile: represents a file (currently packages.yml or dependencies.yml) that contains package dependencies for a project
* DbtPackage: represents a package that is installed as a dependency for the project
* DbtPackageVersion: represents a specific version of a package
* DbtPackageTextFile: contains the raw lines of text from package dependency files. This is used when upgrading packages so we can replace just the version strings within a file without affecting the rest of the file layout (such as comments).

## How the CLI works
The `packages` command calls the `upgrade_packages` function in `main.py`. This then calls:
* `generate_package_dependencies`: extracts dependencies from project's packages.yml/dependencies.yml file and identifies installed package versions in `dbt_packages`
  * Returns `DbtPackageFile` if a packages.yml/dependencies.yml file is found and specifies at least one package; otherwise, None
* `check_for_package_upgrades`: traverses the dependencies in `DbtPackageFile` and for each package, determines if the current installed versions is Fusion compatible; if not, it looks for any Fusion-compatible versions of the package
  * Returns list of `PackageVersionUpgradeResult` 
    * Length should exactly match the number of packages in the `DbtPackageFile`'s dependencies
* `upgrade_package_versions`: takes the `PackageVersionUpgradeResult` list and if any packages need updates, it identifies the required changes in packages.yml. For a dry run, it prints out the new packages.yml; otherwise, it actually makes the changes in the file.
  * Returns a single `PackageUpgradeResult`
* `print_to_console` on the `PackageUpgradeResult`

`upgrade_packages` will generate an error if:
* the path specified in `--path` does not exist or isn't a directory
* `generate_package_dependencies` can't find a packages.yml or dependencies.yml
* `generate_package_dependencies` found a packages.yml or dependencies.yml but it didn't contain any package dependencies

## Scripts

* Used to extract info used in package upgrade CLI:
  * `get_package_hub_files.py`: download package information from package hub (hub.getdbt.com) for all versions of all packages
    * Output: `package_output.json`
  * `get_fusion_compatible_versions.py`: loads `package_output.json` and summarizes Fusion compatibility across all versions for each package
    * Output: `fusion_version_compatibility_output.json` and `fusion_version_compatibility_output.py`
* Not used as an input to the package upgrade CLI:
  * `packages_with_fusion_compatibility_changes.py`: reads `fusion_version_compatibility_output.py` and generates a CSV summary of packages for analytics use
    * Output: `packages.csv`

`get_package_hub_files.py` and `get_fusion_compatible_version.py` are used to pull data from the public package registry (hub.getdbt.com) and extract Fusion compatibility information from available versions. This is basically a local cache of package information to bootstrap autofix. We need to know the lower bound of Fusion-compatible versions for a package but we also know that older versions of packages will not change, so caching this locally removes a lot of repetitive network calls and text parsing. Which means faster run times and fewer failures due to network issues. 

The output from these two scripts produces `fusion_version_compatibility_output.py` that contains a single constant, `FUSION_VERSION_COMPATIBILITY_OUTPUT`. This is then used in `DbtPackage`'s `merge_fusion_compatibility_output` to populate compatible versions.

## TODO
* Private packages
  * Check require_dbt_version in installed private packages
  * Need a way to match the dependency in packages.yml (since it doesn't have the name which is used for public packages)
* Match the version specifier type when upgrading packages
  * Currently if the package config specifies a version like ">1.0.1" and we need to upgrade to 1.0.2, it gets replaced with "1.0.2"
  * Should instead replace with same format like ">1.0.2"
* Get latest versions from package hub instead of using cache
* Better handling for version in package's dbt_project.yml
  * Sometimes the version number in the package's dbt_project.yml doesn't actually match the release version because package hub only checks the release tag on Github, so the installed version check will set an incorrect version
  * Added logic in DbtPackageFile will override the installed version if it's less than the config's version range, but this isn't 100% reliable
  * Could instead refer to the package lock file to find the exact version
  * But probably not a huge problem since we are only looking for the require dbt version anyway and only look for upgrades if it's missing/incompatible
* Move package parsing logic to hubcap or package hub where appropriate
* Explicit overrides at version level
  * Currently in scripts/get_fusion_compatible_versions and DbtPackageVersion.is_version_explicitly_disallowed_on_fusion, but should refine logic
# Package Management

This directory contains code used by the `packages` option in the CLI that upgrades packages in a project to a Fusion-compatible version. The code is centered on three classes:
* DbtPackageFile: represents a file (currently packages.yml or dependencies.yml) that contains package dependencies for a project
* DbtPackage: represents a package that is installed as a dependency for the project
* DbtPackageVersion: represents a specific version of a package


from pdm.backend.hooks.version import SCMVersion


def format_version(version: SCMVersion) -> str:
    if version.distance is None:
        return str(version.version)
    else:
        return f"{version.version}.post{version.distance}"


# Pin a specific version of dbt-fusion-package-tools when building dbt-autofix
# Required because uv workspace doesn't use version for workspace members
# but the version is needed when installing the package independently
def pdm_build_initialize(context):
    metadata = context.config.metadata
    if metadata["name"] == "dbt-autofix" and "dbt-fusion-package-tools" in metadata["dependencies"]:
        new_package_version = metadata["version"]
        dependency_idx = metadata["dependencies"].index("dbt-fusion-package-tools")
        metadata["dependencies"][dependency_idx] = f"dbt-fusion-package-tools=={new_package_version}"

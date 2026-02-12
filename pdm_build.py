import sys

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
        version = str(metadata["version"])
        # Use stderr to ensure visibility in pip output
        sys.stderr.write(f"[dbt-autofix build] Detected version: {version}\n")

        # Only pin strictly for real releases.
        # Fallback versions (0.0...) or development versions (.post, .dev, .rc, +, alpha, beta, etc.)
        # should not be pinned as they likely won't exist on PyPI.
        is_release = not (
            ".post" in version
            or ".dev" in version
            or "rc" in version
            or "+" in version
            or "a" in version
            or "b" in version
            or "alpha" in version
            or "beta" in version
            or version.startswith("0.0")
        )

        if is_release:
            sys.stderr.write(
                f"[dbt-autofix build] Release version detected. Pinning dbt-fusion-package-tools to =={version}\n"
            )
            dependency_idx = metadata["dependencies"].index("dbt-fusion-package-tools")
            metadata["dependencies"][dependency_idx] = f"dbt-fusion-package-tools=={version}"
        else:
            sys.stderr.write(
                "[dbt-autofix build] Non-release/fallback version detected. Skipping strict dependency pin.\n"
            )

        # DEBUG: write to file since pre-commit captures stderr
        with open("/tmp/pdm_build_debug.log", "a") as f:
            f.write(f"version={version} is_release={is_release}\n")
            f.write(f"dependencies={metadata['dependencies']}\n")

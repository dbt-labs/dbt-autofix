from pathlib import Path

DEFAULT_OUTPUT_PATH = Path.cwd() / "src" / "dbt_fusion_package_tools" / "scripts" / "output"
DEFAULT_HUB_PATH = Path.home() / "workplace" / "hub.getdbt.com"
DEFAULT_FUSION_BINARY_PATH = Path.home() / ".local" / "bin" / "dbt"

# save location for tarballs of autofixed versions of packages
DEFAULT_AUTOFIXED_TARBALL_PATH = DEFAULT_OUTPUT_PATH / "autofixed_versions"

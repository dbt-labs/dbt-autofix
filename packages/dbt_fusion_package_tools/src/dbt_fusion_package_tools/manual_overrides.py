EXPLICIT_DISALLOW_ALL_VERSIONS: set[str] = set([])

# https://docs.getdbt.com/docs/fusion/supported-features#package-support
EXPLICIT_ALLOW_ALL_VERSIONS: set[str] = set([])

# TODO: Currently this is used in scripts/get_fusion_compatible_versions
# to set compatibility when parsing the raw package files and also in
# DbtPackageVersion.is_version_explicitly_disallowed_on_fusion,
# but need to refine logic
EXPLICIT_DISALLOW_VERSIONS: dict[str, set[str]] = {}

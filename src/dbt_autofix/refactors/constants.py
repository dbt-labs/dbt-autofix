COMMON_PROPERTY_MISSPELLINGS = {
    "desciption": "description",
    "descrption": "description",
    "descritption": "description",
    "desscription": "description",
}

# Used for schema.yml and SQL files - convert hyphen to underscore
COMMON_CONFIG_MISSPELLINGS = {"post-hook": "post_hook", "pre-hook": "pre_hook"}

# Used for dbt_project.yml - convert underscore to hyphen.
# dbt_project.yml expects the hyphenated hook keys (+pre-hook / +post-hook), unlike node-level
# configs (schema.yml / SQL config()) where the underscore form is canonical. Users frequently use
# the underscore form in dbt_project.yml as well; without this normalization it is treated as an
# unknown config and moved to +meta, which silently disables a functional hook. Map it back to the
# hyphenated form instead.
DBT_PROJECT_CONFIG_MISSPELLINGS = {"pre_hook": "pre-hook", "post_hook": "post-hook"}

# dbt config files that are only meaningful at a project root, never resource properties.
# Superset of VALID_PACKAGE_YML_NAMES (dbt_package_file.py), which is the inverse notion -
# the dependency manifests the packages feature parses - and must not be reused here.
PROJECT_ROOT_ONLY_YAML_FILENAMES: frozenset[str] = frozenset(
    {
        "dbt_project.yml",
        "dependencies.yml",
        "packages.yml",
        "package-lock.yml",
        "catalogs.yml",
        "vars.yml",
        "profiles.yml",
        "selectors.yml",
    }
)

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

# Adapter-specific configs that are functional in dbt-core but absent from the Fusion JSON schema,
# which only describes core configs. Without this allowlist they look like unknown configs and get
# moved to +meta, where the adapter can no longer read them - silently changing behaviour.
#
# - `options`: dbt-duckdb's write options for the `external` materialization. Read via
#   `config.get('options')` in its `render_write_options` macro, so moving it under +meta silently
#   drops `partition_by`, `overwrite_or_ignore`, csv `delimiter`, etc. from the emitted COPY TO.
#   https://github.com/duckdb/dbt-duckdb#writing-to-external-files
ADAPTER_SPECIFIC_CONFIGS = {"options"}

EXPLICIT_DISALLOW_ALL_VERSIONS: set[str] = set(
    [
        "Snowflake-Labs/dbt_constraints",
        "get-select/dbt_snowflake_query_tags",
        "data-mie/dbt_profiler",
        "dbt-labs/logging",
        "get-select/dbt_snowflake_monitoring",
        "dbt-labs/dbt_external_tables",
        "tnightengale/dbt_meta_testing",
        "yu-iskw/dbt_airflow_macros",
        "everpeace/dbt_models_metadata",
    ]
)

# https://docs.getdbt.com/docs/fusion/supported-features#package-support
EXPLICIT_ALLOW_ALL_VERSIONS: set[str] = set(
    [
        "fivetran/fivetran_utils",
        "fivetran/hubspot",
        "fivetran/linkedin",
        "fivetran/microsoft_ads",
        "fivetran/salesforce_formula_utils",
        "AxelThevenot/dbt_assertions",
        "Datavault-UK/automate_dv",
        "entechlog/dbt_snow_mask",
        "fivetran/ad_reporting",
        "fivetran/facebook_ads",
        "fivetran/fivetran_log",
        "fivetran/google_ads",
        "fivetran/jira",
        "fivetran/pendo",
        "fivetran/qualtrics",
        "fivetran/salesforce",
        "fivetran/social_media_reporting",
        "fivetran/zendesk",
        "godatadriven/dbt_date",
        "kristeligt-dagblad/dbt_ml",
        "metaplane/dbt_expectations",
        "Montreal-Analytics/snowflake_utils",
        "Snowflake-Labs/dbt_semantic_view",
        "dbt-labs/dbt_utils",
        "dbt-labs/audit_helper",
        "GJMcClintock/dbt_tld",
        "dbt-labs/codegen",
        "calogica/dbt_date",
    ]
)

# TODO: Currently this is used in scripts/get_fusion_compatible_versions
# to set compatibility when parsing the raw package files and also in
# DbtPackageVersion.is_version_explicitly_disallowed_on_fusion,
# but need to refine logic
EXPLICIT_DISALLOW_VERSIONS: dict[str, set[str]] = {
    # dbt_project_evaluator version 1.1.0 has compatible
    # require dbt version but actually has bug that makes
    # package incompatible until fixed in 1.1.2
    "dbt-labs/dbt_project_evaluator": set(["1.1.0", "1.1.1"]),
    "brooklyn-data/dbt_artifacts": set(
        [
            "0.1.0",
            "0.2.0",
            "0.2.1",
            "0.3.0",
            "0.4.0",
            "0.4.1",
            "0.4.2",
            "0.4.3",
            "0.4.4",
            "0.5.0",
            "0.6.0",
            "0.7.0",
            "0.8.0",
            "1.0.0",
            "1.1.0",
            "1.1.1",
            "1.1.2",
            "1.2.0",
            "2.0.0",
            "2.1.0",
            "2.1.1",
            "2.2.0",
            "2.2.1",
            "2.2.2",
            "2.2.3",
            "2.3.0",
            "2.4.0",
            "2.4.1",
            "2.4.2",
            "2.4.3",
            "2.5.0",
            "2.6.0",
            "2.6.1",
            "2.6.2",
            "2.6.3",
            "2.6.4",
            "2.7.0",
            "2.8.0",
            "2.9.0",
            "2.9.1",
            "2.9.2",
            "2.9.3",
        ]
    ),
}

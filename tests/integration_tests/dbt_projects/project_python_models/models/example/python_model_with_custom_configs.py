def model(dbt, session):
    """Python model with custom configs that should be transformed."""
    dbt.config(materialized="table", meta={"owner": "data-team", "priority": "high", "custom_setting": "value"})

    # These should be transformed to dbt.config.meta_get()
    owner = dbt.config.get("owner")
    priority = dbt.config.get("priority", "low")
    custom_setting = dbt.config.get("custom_setting", "default")

    # These should NOT be transformed (dbt-native configs)
    mat = dbt.config.get("materialized")
    schema = dbt.config.get("schema", "public")

    return session.sql(f"""
        SELECT
            '{owner}' as owner,
            '{priority}' as priority,
            '{custom_setting}' as custom_setting,
            '{mat}' as materialized
    """)

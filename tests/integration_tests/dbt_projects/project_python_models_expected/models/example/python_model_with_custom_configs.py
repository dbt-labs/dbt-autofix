def model(dbt, session):
    """Python model with custom configs that should be transformed."""
    dbt.config(materialized='table', meta={'owner': 'data-team', 'priority': 'high', 'custom_setting': 'value'})
    owner = dbt.config.meta_get('owner')
    priority = dbt.config.meta_get('priority', 'low')
    custom_setting = dbt.config.meta_get('custom_setting', 'default')
    mat = dbt.config.get('materialized')
    schema = dbt.config.get('schema', 'public')
    return session.sql(f"\n        SELECT\n            '{owner}' as owner,\n            '{priority}' as priority,\n            '{custom_setting}' as custom_setting,\n            '{mat}' as materialized\n    ")
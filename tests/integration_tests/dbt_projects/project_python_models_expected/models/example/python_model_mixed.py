def model(dbt, session):
    """Python model with mix of custom and native configs."""
    dbt.config(materialized='incremental', unique_key='id', meta={'pipeline_name': 'etl', 'team': 'analytics'})
    pipeline = dbt.meta_get('pipeline_name', 'default')
    team = dbt.meta_get('team')
    unique_key = dbt.config.get('unique_key')
    mat = dbt.config.get('materialized')
    return session.sql("\n        SELECT 1 as id, 'test' as name\n    ")
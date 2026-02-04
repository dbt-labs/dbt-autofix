def model(dbt, session):
    """Python model with mix of custom and native configs."""
    dbt.config(
        materialized='incremental',
        unique_key='id',
        meta={'pipeline_name': 'etl', 'team': 'analytics'}
    )

    # Custom configs - should transform
    pipeline = dbt.config.get('pipeline_name', 'default')
    team = dbt.config.get('team')

    # Native configs - should NOT transform
    unique_key = dbt.config.get('unique_key')
    mat = dbt.config.get('materialized')

    return session.sql("""
        SELECT 1 as id, 'test' as name
    """)

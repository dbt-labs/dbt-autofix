def model(dbt, session):
    """Python model with only native configs - should NOT be transformed."""
    dbt.config(materialized='view', schema='analytics', tags=['daily'])

    # Only native configs - none should be transformed
    mat = dbt.config.get('materialized', 'table')
    schema = dbt.config.get('schema')
    tags = dbt.config.get('tags', [])

    return session.sql("""
        SELECT 1 as id
    """)

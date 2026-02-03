def model(dbt, session):
    dbt.config(materialized="table", meta={"random_config": "AR"})
    random_config = dbt.config.get("meta").get("random_config")
    # Do something with the config
    return session.sql(f"SELECT '{random_config}' as config_value")

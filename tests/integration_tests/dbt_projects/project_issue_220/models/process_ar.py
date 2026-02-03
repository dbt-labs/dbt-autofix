def model(dbt, session):
    dbt.config(materialized="table", random_config="AR")
    random_config = dbt.config.get("random_config")
    # Do something with the config
    return session.sql(f"SELECT '{random_config}' as config_value")

def model(dbt, session):
    dbt.config(materialized="table", meta={"refresh_frequency": "daily"})
    classification = dbt.config.meta_get("data_classification")
    return session.sql(f"SELECT '{classification}' as data_class, current_date as report_date")

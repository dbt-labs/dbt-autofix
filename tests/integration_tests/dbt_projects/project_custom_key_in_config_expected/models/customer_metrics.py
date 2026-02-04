def model(dbt, session):
    dbt.config(materialized="table", meta={"refresh_frequency": "daily"})
    classification = dbt.config.get("meta").get("data_classification")
    return session.sql(f"SELECT '{classification}' as data_class, current_date as report_date")

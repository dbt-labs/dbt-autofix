def model(dbt, session):
    dbt.config(materialized="table", refresh_frequency="daily")
    classification = dbt.config.get("data_classification")
    return session.sql(f"SELECT '{classification}' as data_class, current_date as report_date")

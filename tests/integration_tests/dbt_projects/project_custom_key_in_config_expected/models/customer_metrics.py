import pandas as pd  # noqa: F401

# Report configuration
REPORT_NAME = "customer_metrics"


def model(dbt, session):
    """Calculates customer metrics.

    This model aggregates data from multiple sources
    and applies custom classification logic.
    """
    # Configure the model
    dbt.config(materialized="table", meta={"refresh_frequency": "daily"})

    # Get custom classification
    classification = dbt.config.meta_get("data_classification")

    # Build the query
    query = f"SELECT '{classification}' as data_class, current_date as report_date"

    return session.sql(query)

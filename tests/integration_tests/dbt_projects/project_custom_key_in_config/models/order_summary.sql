{{
    config(
        materialized='table',
        sla_hours=24
    )
}}

SELECT
    current_date as report_date,
    'order_summary' as model_name

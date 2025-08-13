{% set filter_date = (run_started_at - modules.datetime.timedelta(days=8)).strftime('%Y-%m-%d') %}

{{ config(
    materialized = 'table',
    myconf = run_started_at - 1,
) }}

{% set filter_date = (run_started_at - modules.datetime.timedelta(days=8)).strftime('%Y-%m-%d') %}

{{ config(
    pre_hook='select 1', 
    post_hook='select 2', 
    meta={'myconf': run_started_at - 1}
) }}

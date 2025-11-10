{% set filter_date = (run_started_at - modules.datetime.timedelta(days=8)).strftime('%Y-%m-%d') %}

{{ config({
    'pre-hook': 'select 1',
    'post-hook': 'select 2',
    'myconf': run_started_at - 1,
}) }}

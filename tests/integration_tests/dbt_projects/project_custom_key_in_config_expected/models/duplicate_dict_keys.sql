-- the hook rename collapses the config() dict; the repeated 'tags' key must keep its last
-- value, which is the one dbt itself resolves to
{{ config(
    materialized="table", 
    tags=["weekly"], 
    post_hook=["select 1"]
) }}

select 1 as id

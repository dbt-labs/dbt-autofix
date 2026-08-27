-- the hook rename collapses the config() dict; the repeated 'tags' key must keep its last
-- value, which is the one dbt itself resolves to
{{ config({"materialized": "table", "tags": "mapping_tables", "post-hook": ["select 1"], "tags": ["weekly"]}) }}

select 1 as id

{{ config(
    materialized='table',
    static_analysis=True
) }}

select 1 as id

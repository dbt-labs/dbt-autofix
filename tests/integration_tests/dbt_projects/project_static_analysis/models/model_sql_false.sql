{{ config(
    materialized='view',
    static_analysis=False
) }}

select 2 as id

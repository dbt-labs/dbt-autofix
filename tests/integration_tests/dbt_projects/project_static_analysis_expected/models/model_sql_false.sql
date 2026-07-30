{{ config(
    materialized='view', 
    static_analysis='off'
) }}

select 2 as id

{{ config(
    materialized='table', 
    static_analysis='baseline'
) }}

select 1 as id

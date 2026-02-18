{{
    config(
        materialized='table',
        snowflake_warehouse='BI_XXL_WH'
    )
}}

select 1 as id

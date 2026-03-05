{{ config(
    severity='warn', 
    warn_if='== "never true"', 
    meta={'type_of_dq_test': 'base'}
) }}

select 1 as id

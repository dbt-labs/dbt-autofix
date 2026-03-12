{{ config(
    severity='warn', 
    warn_if='== "never true"',
    error_if="!= 'always true'",
    meta={'type_of_dq_test': 'base'}
) }}

select 1 as id

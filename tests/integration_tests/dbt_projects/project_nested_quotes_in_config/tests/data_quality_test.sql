{{ config(
    type_of_dq_test = 'base',
    severity = 'warn',
    warn_if = '== "never true"'
) }}

select 1 as id

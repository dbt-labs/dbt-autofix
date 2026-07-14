with a as (
    select * from {{ ref('model_with_spaces') }}
),

b as (
    select * from {{ ref("python_model_with_spaces") }}
),

c as (
    select * from {{ source('source_with_spaces', 'my_table') }}
),

d as (
    select * from {{ ref('unrelated_model') }}
)

select * from a

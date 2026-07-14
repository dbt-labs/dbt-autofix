with a as (
    select * from {{ ref('model with spaces') }}
),

b as (
    select * from {{ ref("python model with spaces") }}
),

c as (
    select * from {{ source('source with spaces', 'my_table') }}
),

d as (
    select * from {{ ref('unrelated_model') }}
)

select * from a

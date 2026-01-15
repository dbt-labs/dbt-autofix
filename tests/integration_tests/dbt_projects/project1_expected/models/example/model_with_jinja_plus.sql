-- Uses plus in Jinja for whitespace control
select *
from {{ ref('my_first_dbt_model') }}
{%+ if (1 = 1) +%}
where 1 = 1
{% endif %}
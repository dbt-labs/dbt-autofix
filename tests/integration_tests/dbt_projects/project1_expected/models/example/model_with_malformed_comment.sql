{% macro get_filtered_columns(exclude_list) %}
{% if execute -%}
    {%- set columns = adapter.get_columns_in_relation(ref('my_first_dbt_model')) -%}

    {#%- if column_name not in excluded_columns -%}
        {%- do column_names.append(column_name) -%}
    {%- endif -%#}

    {%- for column in columns -%}
        {%- if column.name not in exclude_list and column.name != '_FIVETRAN_DELETED' -%}
            {{ column.name }}{% if not loop.last %}, {% endif %}
        {%- endif -%}
    {%- endfor -%}
{%-endif-%}
{%- endmacro -%}

select
    {{ get_filtered_columns(['id']) }}
from {{ ref('my_first_dbt_model') }}

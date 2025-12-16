-- This model tests edge cases for unmatched endings in Jinja comments

select 1 as id

-- Test case 1: An actual unmatched endif that SHOULD be removed
{% endif %}

-- Test case 2: An actual unmatched endmacro that SHOULD be removed
{% endmacro %}

-- Test case 3: Properly commented block (should NOT be modified)
# if not adapter.check_schema_exists(model.database, model.schema) %}
# if not adapter.check_schema_exists(model.database, model.schema) %}
{# if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif #}

-- Test case 4: Malformed comment with {#% ... %#} (should NOT be modified)
#% if not adapter.check_schema_exists(model.database, model.schema) %}
#% if not adapter.check_schema_exists(model.database, model.schema) %}
{#% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %#}

-- The query continues here
union all select 4 as id

-- Test case 5: Unclosed {# comment with endif inside (should be preserved)
-- Everything after this point is commented out due to unclosed {#
# This is a commented out section:
# This is a commented out section:
{# This is a commented out section:
  {% if should_run %}
    select 3 as result
  {% endif %}
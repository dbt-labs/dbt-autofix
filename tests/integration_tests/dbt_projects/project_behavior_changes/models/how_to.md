<!-- models/how_to.md -->

# Example Documentation

## Handling the Foobar Feature

If the `FOOBAR` feature is enabled in the environment, we want to include only the `baz` rows.

```sql
SELECT *
FROM dataset.example
{% if env_var("DBT_FOO") == 'BAR' %}
WHERE baz IS TRUE
{% endif %}
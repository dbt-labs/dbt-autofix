# Deprecation Types Reference

See the main [README.md](../../../README.md) for the complete, authoritative list of deprecations that dbt-autofix handles.

## Discovering Existing Test Projects

To see what test projects already exist:

```bash
ls tests/integration_tests/dbt_projects/ | grep -v _expected
```

## Common Test Patterns

### Testing CustomKeyInConfigDeprecation

Input model:
```sql
{{
    config(
        materialized='table',
        my_custom_key='some_value'
    )
}}
SELECT 1
```

Expected output:
```sql
{{
    config(
        materialized='table',
        meta={'my_custom_key': 'some_value'}
    )
}}
SELECT 1
```

### Testing MissingGenericTestArgumentsPropertyDeprecation

This is automatically applied to `dbt_project.yml` when the tool runs.

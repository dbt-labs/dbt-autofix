# Custom configurations

## PROBLEM

Any config key that's not a part of the new authoring layer will cause Fusion to fail to parse.

Unsupported dbt configs need to be moved into `meta:`, or the Fusion will fail to parse the user project.

However, often a user project depends on these keys existing, especially in the case of:
- custom materializations
- custom incremental strategies

For example, it's easy enough to move these cold storage keys into `meta:` like below. However, it doesn't completely solve the issue.

```sql
{{
    config(
        materialized = 'incremental',
        unique_key = 'event_id',
        cold_storage = true,
        cold_storage_date_type = 'relative',
        cold_storage_period = var('cold_storage_default_period'),
        cold_storage_value = var('cold_storage_default_value')
    )
}}
```


```sql
{{
    config(
        materialized = 'incremental',
        unique_key = 'event_id',
        meta = {
            'cold_storage': true,
            'cold_storage_date_type': 'relative',
            'cold_storage_period': var('cold_storage_default_period'),
            'cold_storage_value': var('cold_storage_default_value')
        }
    )
}}
```


## SOLUTION

In these instances, not only do the custom configs need to be moved within `meta:`, but also any macro, or materialization that references those configs in jinja, need to be updated.

for example:

this code
```sql
{% if config.get('cold_storage_date_type') == 'date' %}
```

needs to be changed to be this
```sql
{% if config.get('meta').cold_storage_date_type == 'date' %}
```

When you have many files (50+) with the same custom config pattern, use systematic approaches:

1. **Search for all affected files**: Use `grep` or similar to find all files with the custom config pattern
2. **Use Agent tools**: For bulk operations, use automation tools to apply the same transformation pattern across many files
3. **Verify the pattern**: Test the transformation on a few files first to ensure the pattern works
4. **Common patterns to move to meta:**
   - Any custom materialization configs
   - Custom incremental strategy configs


## CHALLENGES

Often upgrading packages isn't as simple as increasing the version number.

There's often changes to the project that are needed. To learn more about required changes check the release notes for the package on GitHub. The url format for package release notes is
```
https://github.com/{package_owner}/{package_name}/releases
```

For the the latest Fusion compatible releases of Fivetran packages, the source (`_source`) packages have deprecated and rolled into the main packages.

The result is that there may be reference in `dbt_project.yml` to `*_source` package models, sources, and variables that have to be adjusted.

If the user project has an explicit, non-transitive dependency on a Fivetran package whose name ends in `_source`, know that the dependency has to be changed to be the main package

e.g. `fivetran/microsoft_ads_source` no longer exists as it's own package, it lives within `fivetran/microsoft_ads` now.

After doing so, the models from the package not originating from the source package need to be explicitly disabled.

## CHALLENGES

### `config.get('user_custom_config)` returns `None`

When referencing custom configs that have been moved to `meta`, you may encounter Jinja errors like:
```
unknown method: none has no method named get
```

This happens when `config.get('meta')` returns `None` instead of an empty dictionary for models that don't have a `meta` section.

Update macro references to be null-safe:

Instead of:
```sql
{% set config_value = config.get('meta', {}).get('custom_key') %}
```

Use:
```sql
{% set config_value = config.get('meta', {}).get('custom_key', false) if config.get('meta') else false %}
```

Or set a local variable for cleaner code:
```sql
{% set meta_config = config.get('meta', {}) %}
{% if meta_config and meta_config.get('custom_key') %}
    -- do something
{% endif %}
```

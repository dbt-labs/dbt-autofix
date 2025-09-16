# `AGENTS.md`

## Goals

Make a pre-existing dbt project compatible with the new authoring layer while preserving existing functinality as much as possible.

As more issues are discovered, this AGENTS.md will be augmented to include more manual processes that need changing.

## Migrating to the new authoring layer and the dbt Fusion engine

if a user says "migrate me to the new authoring layer" what that really means is that they want to have the following commands complete with no errors (i.e. exit code 1).

1. `dbtf debug`
2. `dbtf parse --show-all-deprecations`
3. `dbtf compile --static-analysis off`
4. `dbtf compile`
6. `dbtf test`

the best way to do this today is to start with a migration wizard that's build into the dbt Fusion CLI.
```
dbtf init --fusion-upgrade
```

This command will first make sure you have a `profiles.yml`, before attempting the above commands in order.

If steps two and below fail, it will try to use [dbt-autofix](https://github.com/dbt-labs/dbt-autofix) to fix the packages.

If it still fails to parse after autofix, consider if the problem is one of the below problems that has to be manually resolved.

If a parse can be successful, it's worth returning to `dbtf init --fusion-upgrade` that will help you try the next step.


## Problems that cannot be automatically fixed

### Misspelled config keys

**PROBLEM**

In the new authoring layer, new unsupported dbt configs need to be moved into `meta:`, or the Fusion will fail to parse the user project.

This is fine and well, except when a user has misspelled a real config (e.g. `materailized:` instead of `materialized:`).

**SOLUTION**

The answer is to fix the config's spelling!

Before moving any custom configs into `meta:` check to see if they are misspellings of real configurations. If so, correct the spelling to that config.

Using the available keys in the JSON schema can be helpful here.


### Package incompatibility

**PROBLEM**

Fusion cannot parse (`dbtf parse`) a user project if the project itself is not compatilbe with the authoring layer.

**SOLUTION**

Upgrade any packages with known error to the latest version wherever possible.

to do upgrade a package look up the latest version on https://hub.getdbt.com/

then modify either one of these files:
- `dependencies.yml`
- `packages.yml`

Rather than providing just the newest version, you should provide a compatibility range where so that patches can automaticall be included

```
  - package: fivetran/social_media_reporting
    # not this
    version: 1.0.0
    # this
    version: [">=1.0.0", "<1.1.0"]


```

Additionally, the `package-lock.yml` will also have to be upgraded. To do so, delete both the `package-lock.yml` and the `dbt_packages/` directory, before running `dbt deps` again which will recreate them both again.

If new package versions introduce new parse or compile errors, check the package's release notes, in case there's breaking changes that have to be accommodated.


If a latest version still throws an error at parse time but is already on the latest version, `dbt-autofix deprecations` has a `--include-packages` flag that may help resolve the issue.

**CHALLENGES**

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


**Success criteria**

the whole project can parse and compile, and the data models work as before.

### bespoke configurations 

**PROBLEM**

Any config key that's not a part of the new authoring layer will cause Fusion to fail to parse.

Unsupported dbt configs need to be moved into `meta:`, or the Fusion will fail to parse the user project.

However, often a user project depends on these keys existing, especially in the case of:
- custom materializations
- custom incremental strategies

For example, it's easy enough to move these cold storage keys into `meta:` like below.

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

But that doesn't solve the issue.

**SOLUTION**

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

### Macro null-safety issues

**PROBLEM**

When referencing custom configs that have been moved to `meta`, you may encounter Jinja errors like:
```
unknown method: none has no method named get
```

This happens when `config.get('meta')` returns `None` instead of an empty dictionary for models that don't have a `meta` section.

**SOLUTION**

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

### sources without source tables

**PROBLEM**
if there's a dbt YAML file that has a source defined but the source has no models defined, you'll see this error

```
warning: dbt0102: No tables defined for source 'my_source' in file 'models/staging/my_source/_my_source__sources.yml'
```

**SOLUTION**

1. you can delete it if there's no config on the source
2. if there is config for the source (e.g. freshness) you have to move the config to `dbt_project.yml` under the `sources:` key. after that you can delete that source definition in YAML.


### Dynamic SQL limitations

**PROBLEM**

Some SQL patterns that work in legacy dbt may not be supported by Fusion's static analysis, such as:
```sql
PIVOT(...) FOR column_name IN (ANY)
```

**SOLUTION**

option 1:

refactor away any instances of `PIVOT(...) FOR column_name IN (ANY)` to have hard-coded values.

option 2:

For models with unsupported dynamic SQL:
1. Add `static_analysis = 'off'` to the model config:
```sql
{{
    config(
        materialized = 'table',
        meta = {
            'static_analysis': 'off'
        }
    )
}}
```

2. Or disable static analysis globally for the model in `dbt_project.yml`

## Resources


### dbt docs


https://docs.getdbt.com/reference/deprecations#list-of-deprecation-warnings

https://github.com/dbt-labs/dbt-fusion/discussions/401

https://docs.getdbt.com/docs/fusion/supported-features

https://docs.getdbt.com/docs/fusion/new-concepts


### JSON schemas

Almost certainly, dbt-autofix should have everything you need, but if you need to see the actual JSON Schema, or are just curious, you can do so. They form the basis of the dbt-autofix behavior.

One great use case for these would be to check if unrecognized config keys are actually mispellings

Here’s one example:

```
https://public.cdn.getdbt.com/fs/schemas/fs-schema-dbt-yaml-files-v2.0.0-beta.34.json
```

They follow the below template format where:
- `RESOURCE` is either `dbt-yaml-files` or `dbt-project`
- `VERSION` is the fusion version
- e.g. `v2.0.0-beta.34`, but `https://public.cdn.getdbt.com/fs/latest.json` gives you the latest version

`https://public.cdn.getdbt.com/fs/schemas/fs-schema-{RESOURCE}-{VERSION}.json`


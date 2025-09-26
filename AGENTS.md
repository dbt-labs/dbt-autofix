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

## Resources

### Problems that cannot be automatically fixed

- [Custom configurations](./manual_fixes/custom_configuration.md)
- [Limitations of Dynamic SQL](./manual_fixes/dynamic_sql.md)
- [Misspelled config keys](./manual_fixes/misspelled_config_keys.md)
- [package incompatibility](./manual_fixes/package_incompatibility.md)
- [sources defined in YAML without tables defined](./manual_fixes/sources_without_tables.md)


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


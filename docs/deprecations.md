# dbt-fusion Deprecation Warnings — Autofix Reference

This document is an autofix development reference. Each deprecation is grouped by how completely
dbt-autofix handles it, and includes:

- which file types the deprecation can fire for (from source-code call-site research)
- what autofix currently does or should do
- before/after examples drawn from the [official deprecations docs](https://docs.getdbt.com/reference/deprecations)
  (supplemented by source-code research where the docs omit Python model applicability)

> **Note on docs vs. source:** The official docs list 31 deprecations. The source code defines
> 35 active ones. The 4 not in the docs are marked accordingly.

## Summary

| Category                                | Count  |
| --------------------------------------- | :----: |
| Fully supported in autofix              |   14   |
| Partially supported in autofix          |   2    |
| Not supported in autofix (documented)   |   15   |
| Not supported in autofix (undocumented) |   4    |
| **Total active deprecations**           | **35** |

## Sources

- [dbt-core's deprecations.py](https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/deprecations.py): Source of truth for the deprecations (DBTDeprecation subclasses) core emits
- [Developer docs on deprecations](https://docs.getdbt.com/reference/deprecations#customoutputpathinsourcefreshnessdeprecation-warning-resolution): This is our guidance to users on how to fix deprecations. Core may emit deprecations that are not documented here. Provides useful before/after code patterns.
- Readme for this repo: Documentation on what deprecations we fix. May occasionally be incorrect or out of date.
- This repo: Ground truth on what deprecations we fix

## Fully supported in autofix

Autofix handles every file type this deprecation can fire for.

### ConfigDataPathDeprecation

> **Fires for:** `dbt_project.yml`

`data-paths` was renamed to `seed-paths` in v1.0.

**Before:**

```yaml
# dbt_project.yml
data-paths: ["seeds"]
```

**After:**

```yaml
# dbt_project.yml
seed-paths: ["seeds"]
```

**Autofix:** Renames the key `data-paths` → `seed-paths` in `dbt_project.yml`.

### ConfigLogPathDeprecation

> **Fires for:** `dbt_project.yml`

Specifying `log-path` in `dbt_project.yml` was deprecated in v1.5. Use the `--log-path` CLI
flag or `DBT_LOG_PATH` environment variable instead.

**Before:**

```yaml
# dbt_project.yml
log-path: "logs"
```

**After:**

```yaml
# dbt_project.yml
# log-path removed; set via --log-path flag or DBT_LOG_PATH env var
```

**Autofix:** Removes the `log-path` key from `dbt_project.yml`.

### ConfigSourcePathDeprecation

> **Fires for:** `dbt_project.yml`

`source-paths` was renamed to `model-paths` in v1.0.

**Before:**

```yaml
# dbt_project.yml
source-paths: ["models"]
```

**After:**

```yaml
# dbt_project.yml
model-paths: ["models"]
```

**Autofix:** Renames the key `source-paths` → `model-paths` in `dbt_project.yml`.

### ConfigTargetPathDeprecation

> **Fires for:** `dbt_project.yml`

Specifying `target-path` in `dbt_project.yml` was deprecated in v1.5. Use the `--target-path`
CLI flag or `DBT_TARGET_PATH` environment variable instead.

**Before:**

```yaml
# dbt_project.yml
target-path: "target"
```

**After:**

```yaml
# dbt_project.yml
# target-path removed; set via --target-path flag or DBT_TARGET_PATH env var
```

**Autofix:** Removes the `target-path` key from `dbt_project.yml`.

### CustomOutputPathInSourceFreshnessDeprecation

> **Fires for:** `cli`

The `-o` / `--output` flag for overriding the source freshness results file location is
deprecated. Use `--target-path` instead if you need to control artifact output location.

**Autofix:** Removes `-o`/`--output` from `dbt source freshness` invocations in dbt Platform jobs

### CustomTopLevelKeyDeprecation

> **Fires for:** `schema yaml`

Custom top-level keys in YAML schema files are not supported. Custom metadata belongs under
`config.meta`.

**Before:**

```yaml
models:
  - name: my_model
    columns:
      - name: id

custom_metadata:
  owner: "data_team"
  last_updated: "2025-07-01"
```

**After:**

```yaml
models:
  - name: my_model
    config:
      meta:
        custom_metadata:
          owner: "data_team"
          last_updated: "2025-07-01"
    columns:
      - name: id
```

**Autofix:** Deletes unrecognised top-level key-value pairs from YAML files.

> [!CAUTION] **Is this desired?**
> Autofix does not move the custom config, like it does for`CustomKeyInObjectDeprecation`

### DuplicateYAMLKeysDeprecation

> **Fires for:** `schema yaml`, `dbt_project.yml`

Identical keys appear more than once in a YAML file. dbt currently uses the last occurrence;
this behavior will become an error in dbt-fusion.

**Before:**

```yaml
models:
  - name: my_model
    description: "first description"
    description: "second description"
```

**After:**

```yaml
models:
  - name: my_model
    description: "second description"
```

**Autofix:** Removes all but the last occurrence of each duplicate key.

### ExposureNameDeprecation

> **Fires for:** `schema yaml`

Exposure names must match `[a-zA-Z0-9_]+` since v1.3. Human-readable labels belong in the
`label` property.

**Before:**

```yaml
exposures:
  - name: "Weekly Revenue Report"
```

**After:**

```yaml
exposures:
  - name: Weekly_Revenue_Report
    label: "Weekly Revenue Report"
```

**Autofix:** Replaces spaces with underscores in the `name` field of exposures.

> [!WARNING] Should we improve this?
> I'm thinking we should add a label if the original did not have one preserving the original whitespace-formatted name that we convert

### MissingArgumentsPropertyInGenericTestDeprecation

> **Fires for:** `schema yaml`

Keyword arguments for custom generic tests must be nested under an `arguments` property when
`require_generic_test_arguments_property` is `true`. Specifying them as sibling properties is
deprecated.

**Before:**

```yaml
models:
  - name: my_model
    data_tests:
      - dbt_utils.expression_is_true:
          expression: "subtotal > 0"
          where: "1=1"
```

**After:**

```yaml
models:
  - name: my_model
    data_tests:
      - dbt_utils.expression_is_true:
          arguments:
            expression: "subtotal > 0"
          config:
            where: "1=1"
```

**Autofix:** Moves keyword arguments into the `arguments:` property, separating framework configs (e.g. `where`, `severity`) into `config:`.

### MissingPlusPrefixDeprecation

> **Fires for:** `dbt_project.yml`

Built-in config keys specified inside `dbt_project.yml` hierarchical config blocks must be
prefixed with `+` to distinguish them from subdirectory selectors.

**Before:**

```yaml
models:
  marts:
    materialized: table
```

**After:**

```yaml
models:
  marts:
    +materialized: table
```

**Autofix:** Adds the `+` prefix to unambiguous built-in config keys in `dbt_project.yml`.

### ModelParamUsageDeprecation

> **Fires for:** `cli`

The `--models` / `--model` / `-m` flag was renamed to `--select` / `-s` in v0.21 (Oct 2021).
Silently skipping the flag causes incorrect DAG behaviour.

**Before:**

```bash
dbt run --model my_model
dbt run -m my_model
```

**After:**

```bash
dbt run --select my_model
dbt run -s my_model
```

**Autofix:** Replaces `-m`/`--model`/`--models` with `-s`/`--select` in dbt Platform jobs

> [!WARNING] 2 possible bugs
>
> 1. This is the default fix — it runs when you call jobs without --behavior-change. With behavior--change flag on, you _only_ get the CustomOutputPathInSourceFreshnessDeprecation and not this one.
> 2. the rule_name hardcoded into the DBTCloudRefactor at line 306 is "m_selector_deprecated" regardless of which fix was actually applied — so if --behavior-change triggered the source freshness fix, the logged rule name would still say m_selector_deprecated.

### PropertyMovedToConfigDeprecation

> **Fires for:** `schema yaml`

Several properties that historically lived at the resource level (`freshness`, `meta`, `tags`,
`docs`, `group`, `access`) are moving entirely into `config:`.

**Before:**

```yaml
sources:
  - name: ecom
    schema: raw
    freshness:
      warn_after:
        count: 24
        period: hour
```

**After:**

```yaml
sources:
  - name: ecom
    schema: raw
    config:
      freshness:
        warn_after:
          count: 24
          period: hour
```

**Autofix:** Moves affected property-level keys under `config:` in schema YAML files.

### SourceFreshnessProjectHooksNotRun

> **Fires for:** `dbt_project.yml`

`on-run-start` / `on-run-end` hooks are defined but `source_freshness_run_project_hooks` is
`false`. Hooks will run by default in future versions; opt in now to avoid unexpected behaviour
changes.

**Before:**

```yaml
# dbt_project.yml
flags:
  source_freshness_run_project_hooks: false
```

**After:**

```yaml
# dbt_project.yml
flags:
  source_freshness_run_project_hooks: true
```

**Autofix:** Sets `source_freshness_run_project_hooks: true` in `dbt_project.yml`.

### UnexpectedJinjaBlockDeprecation

> **Fires for:** `sql`, `py`

Orphaned or out-of-context Jinja block tags (e.g., a stray `{% endmacro %}` before the opening
`{% macro %}`) are currently silently ignored. dbt-fusion will treat them as errors.

**Before:**

```jinja2
{% endmacro %}

{% macro hello() %}
  hello!
{% endmacro %}
```

**After:**

```jinja2
{% macro hello() %}
  hello!
{% endmacro %}
```

**Autofix:** Removes unexpected `{% endmacro %}` and `{% endif %}` blocks from `.sql` files

## Partially supported in autofix

Autofix handles the deprecation for some file types but not all.

### CustomKeyInConfigDeprecation

> **Fires for:** `schema yaml`, `sql`, `py`

An unrecognised key appears at the top level of a `config:` block. The fix is to move it under
`meta:`.

This deprecation fires from two distinct code paths:

- **Schema YAML** — JSON schema validation detects an extra key inside a `config:` block
- **SQL / Python inline configs** — `validate_model_config()` detects an extra top-level key in
  a `{{ config(...) }}` call

> **Note:** The official docs only show a YAML example. This deprecation also fires for
> SQL and Python models that pass unrecognised keys in `{{ config(...) }}`.

**Before (YAML):**

````yaml
models:
  - name: my_model
    config:
      custom_config_key: value

**After (YAML):**
```yaml
models:
  - name: my_model
    config:
      meta:
        custom_config_key: value
````

**Before (SQL / Python):**

```sql
{{ config(materialized='table', custom_config_key='value') }}
```

**After (SQL / Python):**

```sql
{{ config(materialized='table', meta={'custom_config_key': 'value'}) }}
```

**Autofix covers:** Move any custom key into `meta:` within the config block in yaml and sql jinja
**Autofix does not cover:** Moving custom keys into meta in python models

### CustomKeyInObjectDeprecation

> **Fires for:** `schema yaml`, `sql`, `py`

An unrecognised key appears inside a nested object within a resource or its config. This includes
custom keys on column definitions and `meta` placed at non-config levels. The fix is to move
custom keys under `meta:` (which itself should live under `config:`).

This deprecation fires from two distinct code paths:

- **Schema YAML** — JSON schema validation detects the unknown key at the resource-object level
- **SQL / Python inline configs** — `validate_model_config()` detects unknown nested keys inside
  `{{ config(...) }}` call arguments

**Before:**

```yaml
models:
  - name: my_model
    config:
      custom_config_key: value
    columns:
      - name: my_column
        meta:
          some_key: some_value
```

**After:**

```yaml
models:
  - name: my_model
    config:
      meta:
        custom_config_key: value
    columns:
      - name: my_column
        config:
          meta:
            some_key: some_value
```

For SQL models with inline config:
**Before:**

```sql
{{ config(
    materialized='table',
    some_nested_obj={'custom_key': 'value'}
) }}
```

**After:**

```sql
{{ config(
    materialized='table',
    meta={'some_nested_obj': {'custom_key': 'value'}}
) }}
```

**Autofix covers:** `schema yaml` — relocates custom keys under `meta:` and `meta:` under
`config:` in YAML files.

**Autofix does NOT cover:** `sql`, `py` — inline `{{ config(...) }}` blocks in SQL and Python
model files are not rewritten. Users must migrate these manually.

> [!WARNING] validate claim above
> I need to better understand if our existing config re-writes cover this or not, even they they are filed under CustomKeyInConfigDeprecation (I think?)

### ResourceNamesWithSpacesDeprecation

> **Fires for:** `manifest` (any resource type — the check runs after all files are parsed)

Resource names containing spaces have been deprecated since v1.8. dbt-fusion will reject them
outright.

**Resolution:** Remove spaces from the `name:` field in the YAML definition and rename the
corresponding model file to match.

**Autofix covers:** `schema yaml` (updates the `name:` field) and `sql` filenames (renames the
file on disk to match the new name).

**Autofix does NOT cover:** `py` filenames — the autofix README does not document renaming
Python model files. Verify manually if your project uses Python models with spaces in their
names.

## Not supported in autofix

These deprecations require manual remediation. They are ordered alphabetically, with undocumented
deprecations (not in the official docs) listed at the end.

### ArgumentsPropertyInGenericTestDeprecation

> **Fires for:** `schema yaml`

The ability to specify a custom top-level `arguments` property on generic tests is deprecated in
favour of nesting arguments under the standard `arguments:` key expected by the framework.

**Before:**

```yaml
models:
  - name: my_model
    data_tests:
      - my_custom_generic_test:
          arguments: [1, 2, 3]
          expression: "subtotal > 0"
```

**After:**

```yaml
models:
  - name: my_model
    data_tests:
      - my_custom_generic_test:
          arguments:
            arguments: [1, 2, 3]
            expression: "subtotal > 0"
```

**Manual fix:** Nest all test arguments under `arguments:`.

### DuplicateNameDistinctNodeTypesDeprecation

> **Fires for:** `manifest`

Two unversioned resources in the same package share the same name but have different node types
(e.g., a model and a seed both named `sales`). This is allowed while
`require_unique_project_resource_names` is `false`, but will become an error.

**Manual fix:** Rename one of the conflicting resources so all resource names are unique across
types within the package.

### EnvironmentVariableNamespaceDeprecation

> **Fires for:** `env` (not a file — checked at runtime)

A custom environment variable name conflicts with dbt's reserved `DBT_ENGINE` namespace prefix.

**Manual fix:** Rename any custom environment variables that begin with `DBT_ENGINE` to avoid
the reserved prefix.

### GenerateSchemaNameNullValueDeprecation

> **Fires for:** `sql`, `py`

A custom `generate_schema_name` macro returns `null`, causing invalid schema resolution
behaviour.

**Manual fix:** Update the macro to always return a non-null string:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ return(target.schema) }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
```

### GenericJSONSchemaValidationDeprecation

> **Fires for:** `schema yaml`
> **Status:** Preview (`_is_preview = True`) — surfaced as a `Note`, not a warning

Catch-all for JSON schema validation errors not covered by a more specific deprecation type.
This signals a structural problem in a YAML file that dbt-fusion will reject.

**Manual fix:** Review the note message for the specific violation; consult the [community Slack](https://getdbt.slack.com) or docs for guidance.

### MFCumulativeTypeParamsDeprecation

> **Fires for:** `schema yaml`

`window` and `time_to_grain` specified directly on a metric's `type_params` were deprecated in
v1.9. They must be nested under `cumulative_type_params`.

**Manual fix:**

```yaml
# Before
metrics:
  - name: cumulative_metric
    type_params:
      window: 7
      grain_to_date: day

# After
metrics:
  - name: cumulative_metric
    type_params:
      cumulative_type_params:
        window: 7
        grain_to_date: day
```

### MFTimespineWithoutYamlConfigurationDeprecation

> **Fires for:** `schema yaml`

A MetricFlow timespine is configured via a SQL file rather than a YAML definition.

**Manual fix:** Define the timespine in YAML format alongside the model definition.

### ModulesItertoolsUsageDeprecation

> **Fires for:** `sql`, `py`
>
> **Note:** The official docs only show a SQL example. This also fires for Python models that
> access `modules.itertools` through the dbt Jinja context.

Using `modules.itertools` in Jinja is deprecated. Use built-in Jinja/Python equivalents instead.

**Before (SQL):**

```sql
{%- set AB_cartesian = modules.itertools.product([1, 2], ['x', 'y']) -%}
{%- for item in AB_cartesian %}
  {{ item }}
{%- endfor -%}
```

**After (SQL) — replace with a custom macro:**

```sql
-- macros/cartesian_product.sql
{%- macro cartesian_product(list1, list2) -%}
  {%- set result = [] -%}
  {%- for item1 in list1 -%}
    {%- for item2 in list2 -%}
      {%- set _ = result.append((item1, item2)) -%}
    {%- endfor -%}
  {%- endfor -%}
  {{ return(result) }}
{%- endmacro -%}

-- models/my_model.sql
{%- set AB_cartesian = cartesian_product([1, 2], ['x', 'y']) -%}
{%- for item in AB_cartesian %}
  {{ item }}
{%- endfor -%}
```

**After (Python models):** Use Python's built-in `itertools` directly — no Jinja context needed:

```python
import itertools
AB_cartesian = list(itertools.product([1, 2], ['x', 'y']))
```

**Manual fix:** Replace `modules.itertools.*` calls with custom macros (SQL) or native Python
imports (Python models).

### PackageInstallPathDeprecation

> **Fires for:** `dbt_project.yml`

The default package install path changed from `dbt_modules` to `dbt_packages`. If
`clean-targets` or `.gitignore` still references `dbt_modules`, this deprecation fires.

**Manual fix (option 1):** Update references from `dbt_modules` to `dbt_packages` in
`clean-targets` and `.gitignore`.

**Manual fix (option 2):** Explicitly set `packages-install-path: dbt_modules` in
`dbt_project.yml` to pin the old path.

### PackageMaterializationOverrideDeprecation

> **Fires for:** `manifest`

An installed package overrides a built-in dbt materialisation without explicit opt-in. This
requires `require_explicit_package_overrides_for_builtin_materializations: false` to suppress,
which will be removed.

**Manual fix:** Add an explicit adapter-specific materialization in your project that delegates
to the package:

```sql
{% materialization table, snowflake %}
  {{ return(my_package.materialization_table_snowflake()) }}
{% endmaterialization %}
```

Then remove the behaviour flag from `dbt_project.yml`.

### PackageRedirectDeprecation

> **Fires for:** `packages.yml`

A package referenced in `packages.yml` has been renamed; the original package is no longer
actively maintained.

**Manual fix:** Update `packages.yml` to reference the new package name as indicated in the
deprecation message.

### ProjectFlagsMovedDeprecation

> **Fires for:** `dbt_project.yml`

The `config:` property in `profiles.yml` was deprecated in favour of a `flags:` block in
`dbt_project.yml`.

**Before (`profiles.yml`):**

```yaml
my_profile:
  config:
    use_colors: true
```

**After (`dbt_project.yml`):**

```yaml
flags:
  use_colors: true
```

**Manual fix:** Remove the `config:` block from `profiles.yml` and add the equivalent settings
under `flags:` in `dbt_project.yml`.

### SourceOverrideDeprecation

> **Fires for:** `schema yaml`

The `overrides:` property on source definitions is deprecated.

**Manual fix:** Remove the `overrides:` property. To enable or disable sources from a package,
use the package's own source configuration instead.

### WEOInlcudeExcludeDeprecation

> **Fires for:** `dbt_project.yml`
>
> **Note:** The class name has a typo — `WEOInlcudeExcludeDeprecation` — but the `_name` string
> (`weo-include-exclude-deprecation`) is spelled correctly.

The `include:` and `exclude:` options for `warn_error_options` have been replaced by `error:`
and `warn:`.

**Before:**

```yaml
flags:
  warn_error_options:
    include:
      - NoNodesForSelectionCriteria
    exclude:
      - SomeOtherWarning
```

**After:**

```yaml
flags:
  warn_error_options:
    error:
      - NoNodesForSelectionCriteria
    warn:
      - SomeOtherWarning
```

**Manual fix:** Replace `include:` → `error:` and `exclude:` → `warn:` in `dbt_project.yml`.

### Undocumented deprecations

The following four deprecations exist in the source code but are not listed in the official
docs. They have no autofix support.

#### CollectFreshnessReturnSignature

> **Fires for:** `sql` (adapter macro files)

The return signature of the freshness collection macro/adapter interface has changed. Old
implementations returning data in the previous format are deprecated.

**Manual fix:** Update any custom adapter freshness macros to match the current expected return
signature.

#### GenericSemanticLayerDeprecation

> **Fires for:** `schema yaml` (semantic manifest files)

A catch-all fired during semantic manifest validation when the DSI (Data Services Interface)
reports an upcoming deprecation not covered by a more specific warning.

**Manual fix:** Review the deprecation message for specifics; update semantic layer YAML
definitions accordingly.

#### MicrobatchMacroOutsideOfBatchesDeprecation

> **Fires for:** `manifest`

A microbatch model is defined but the `get_batch_relation` macro is being called outside of a
batch context.

**Manual fix:** Ensure microbatch macro usage is scoped correctly within batch execution
contexts.

#### TimeDimensionsRequireGranularityDeprecation

> **Fires for:** `schema yaml`

Time dimensions defined in semantic models are missing the required `granularity` field, which
will become mandatory.

**Manual fix:** Add the `granularity:` field to all time dimension definitions in semantic model
YAML files.

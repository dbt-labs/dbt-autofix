# Deprecation Types Reference

These are the deprecation types that dbt-autofix can detect and fix. Use this as a reference when creating integration tests.

## Available Deprecations

| Deprecation | Description |
|-------------|-------------|
| `UnexpectedJinjaBlockDeprecation` | Jinja blocks in unexpected locations |
| `ResourceNamesWithSpacesDeprecation` | Resource names containing spaces |
| `PropertyMovedToConfigDeprecation` | Properties that should be in config block |
| `CustomKeyInObjectDeprecation` | Custom keys in object definitions |
| `MissingGenericTestArgumentsPropertyDeprecation` | Generic tests missing `arguments` property |
| `DuplicateYAMLKeysDeprecation` | Duplicate keys in YAML files |
| `ExposureNameDeprecation` | Exposure naming issues |
| `ConfigLogPathDeprecation` | Deprecated `log-path` config |
| `ConfigTargetPathDeprecation` | Deprecated `target-path` config |
| `ConfigDataPathDeprecation` | Deprecated `data-paths` config |
| `ConfigSourcePathDeprecation` | Deprecated `source-paths` config |
| `MissingPlusPrefixDeprecation` | Config keys missing `+` prefix |
| `CustomTopLevelKeyDeprecation` | Custom keys at top level of YAML |
| `CustomKeyInConfigDeprecation` | Custom keys in config blocks (moved to `meta`) |

## Existing Test Projects

| Project | Tests | Special Mode |
|---------|-------|--------------|
| `project1` | General refactoring | - |
| `project_behavior_changes` | Behavior flag changes | `--behavior-change` |
| `project_jinja_templates` | Jinja template handling | `--behavior-change` |
| `project_semantic_layer` | Semantic layer configs | `--semantic-layer` |
| `project_config_quoted_strings` | Quoted strings in configs (#221) | - |

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

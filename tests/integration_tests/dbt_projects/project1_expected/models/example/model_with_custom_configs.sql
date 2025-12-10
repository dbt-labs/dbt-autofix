{{ config(
    materialized="table", 
    meta={'existing_meta_config': 'existing_meta_config', 'custom_config': 'custom_config', 'custom_config_int': 2, 'custom_config_list': ['a', 'b', 'c'], 'custom_config_dict': {'a': 1, 'b': 2, 'c': 3}}
) }}

{{ config.meta_get('custom_config') }}

{{ config.meta_get('custom_config', 'default_value') }}

{{ config.get('materialized') }}

{{ config.get('materialized', 'default_value') }}

{{ config.meta_get('custom_config_dict').a }}

{{ config.meta_get('custom_config_dict').get('a', 'default_value') }}
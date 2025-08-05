{{ config(materialized="table", meta={'existing_meta_config': 'existing_meta_config', 'custom_config': 'custom_config', 'custom_config_int': 2, 'custom_config_list': ['a', 'b', 'c'], 'custom_config_dict': {'a': 1, 'b': 2, 'c': 3}}) }}

select 1 as id
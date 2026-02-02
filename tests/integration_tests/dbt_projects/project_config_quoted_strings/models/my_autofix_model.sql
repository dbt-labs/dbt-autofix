{{
    config(
        materialized='view',
        my_custom_config_1='"database"."schema"."identifier"',
        my_custom_config_2='"some_quoted_string"',
        my_custom_config_3='some_unquoted_string',
    )
}}

select 1 as id

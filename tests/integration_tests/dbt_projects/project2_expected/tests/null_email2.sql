
{{ config(
    severity="error", 
    meta={'custom_config': 'test'}
) }}
select * from {{ ref("sample_model") }} where email is null

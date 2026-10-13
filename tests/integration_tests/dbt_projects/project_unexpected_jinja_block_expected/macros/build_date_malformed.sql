{# Malformed duplicate opener + stray %}; reported as invalid Jinja (warnings), file not auto-edited. #}
{% {% macro build_date_jago_cbas(start_date, end_date, base_table_ingestion_time_column, watermark=1) %}
  {% set date_selector = "DATE(" ~ base_table_ingestion_time_column ~ ") >= 1" %}
  {% set upper_date_selector = "2" %}
  {{ return(date_selector ~ ' AND ' ~ upper_date_selector) }}
{% endmacro %}%}

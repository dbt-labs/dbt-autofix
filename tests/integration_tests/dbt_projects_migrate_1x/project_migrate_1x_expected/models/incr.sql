{{ config(materialized='incremental', incremental_predicates=["1=1"]) }}
select {{ dbt.current_timestamp() }} as ts, {{ dbt_utils.current_timestamp_in_utc() }} as ts_utc

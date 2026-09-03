{{ config(materialized='incremental', predicates=["1=1"]) }}
select {{ dbt_utils.current_timestamp() }} as ts, {{ dbt_utils.current_timestamp_in_utc() }} as ts_utc

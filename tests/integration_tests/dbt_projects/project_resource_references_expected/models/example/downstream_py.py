def model(dbt, session):
    upstream = dbt.ref("model_with_spaces")
    src = dbt.source("source_with_spaces", "my_table")
    return upstream.union(src)

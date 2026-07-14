def model(dbt, session):
    upstream = dbt.ref("model with spaces")
    src = dbt.source("source with spaces", "my_table")
    return upstream.union(src)

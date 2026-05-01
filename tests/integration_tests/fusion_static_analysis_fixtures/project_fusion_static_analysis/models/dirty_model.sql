select
    AGG(revenue) as total_revenue,
    PERCENTILE_CONT(0.5) within group (order by amount) as median_amount,
    TRIM(name) as name
from orders
group by name

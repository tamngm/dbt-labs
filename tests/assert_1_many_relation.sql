-- checking the cardinality betwee 2 columns
-- to make sure there's 1-many relationship 
select
    order_id,
    count(order_date) as count_order_date
from {{ ref('stg_jaffle_shop__orders') }}
group by 1
having count_order_date > 1 
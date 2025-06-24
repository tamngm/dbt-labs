select
    id as order_id,
    user_id as customer_id,
    order_date,
    status,
    row_number() over (
            partition by user_id 
            order by order_date, id
        ) as user_order_seq,
    case when status not in ('returned', 'pending') then order_date end valid_order_date,
    case when status not in ('returned','return_pending') then 1 else 0 end valid_order_status

from {{ source("jaffle_shop_datasrc", "orders")}}
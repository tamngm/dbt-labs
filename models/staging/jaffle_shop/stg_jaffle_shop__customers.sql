select
    id as customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as name
from {{ source("jaffle_shop_datasrc", "customers")}}

with
-- Staging
customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

order_values_joined as (
    select * from {{ ref('int_orders')}}
)
,
employee as (
    select * from {{ ref('employee')}}
)
,
customer_order_history as (

    select 

        b.customer_id,
        b.name as full_name,
        b.last_name as surname,
        b.first_name as givenname,
        employee.employee_id is not null as is_employee,

        a.valid_order_date,
        min(valid_order_date) as first_order_date,

        min(valid_order_date) as first_non_returned_order_date,
        max(valid_order_date) as most_recent_non_returned_order_date,

        coalesce(max(user_order_seq),0) as order_count,

        coalesce(count(case 
            when a.valid_order_status = 1
            then 1 end),
            0
        ) as non_returned_order_count,

        sum(case 
            when a.valid_order_status = 1
            then round(amount/100.0,2) 
            else 0 
        end) as total_lifetime_value,

        sum(case 
            when a.valid_order_status = 1
            then round(amount/100.0,2) 
            else 0 
        end)
        / nullif(count(case 
            when a.valid_order_status = 1
            then 1 end),
            0
        ) as avg_non_returned_order_value,

        array_agg(distinct a.order_id) as order_ids

    from order_values_joined a

    join customers b
    on a.customer_id = b.customer_id
    join employee on employee.employee_id = b.customer_id
    group by all

),

-- Final CTEs 
final as (

    select 

        orders.order_id,
        orders.customer_id,
        customer_order_history.surname,
        customer_order_history.givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        round(amount/100.0,2) as order_value_dollars,
        orders.status as order_status

    from order_values_joined orders

    left join customer_order_history on orders.customer_id = customer_order_history.customer_id


)

-- Simple Select Statement
select * from final
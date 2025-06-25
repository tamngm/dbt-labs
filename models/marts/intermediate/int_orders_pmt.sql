WITH 
    customers as (select * from {{ ref("stg_jaffle_shop__customers") }}),
    orders as (select * from {{ ref("stg_jaffle_shop__orders") }}),
    payment as (select * from {{ ref("stg_stripe__payments") }}),
    -- Logical CTEs
    completed_payment as (
        select
            order_id,
            max(created_at) as payment_finalized_date,
            sum(amount) as total_amount_paid
        from payment
        where status <> 'fail'
        group by 1
    ),

    paid_orders as (
        select
            orders.order_id,
            orders.customer_id,
            orders.order_date as order_placed_at,
            orders.status as order_status,
            p.total_amount_paid,
            p.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from orders
        left join completed_payment p on orders.order_id = p.order_id
        left join customers on orders.customer_id = customers.customer_id
    )
select * from paid_orders